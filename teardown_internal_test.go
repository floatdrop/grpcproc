package grpcproc

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/floatdrop/grpcproc/internal/testpb"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// bufconnPair is nodes a and b, each behind an in-memory listener and
// dialing the other there; hooks, if any, are a's, and dialing, if any, runs
// as a connects to b. b runs "echo", which answers every call with a Ping,
// watched by the caller if it asked to be (CallMonitor).
func bufconnPair(t *testing.T, hooks Hooks, dialing func()) (a, b *Node) {
	t.Helper()
	lns := map[string]*bufconn.Listener{"a": bufconn.Listen(1 << 20), "b": bufconn.Listen(1 << 20)}
	node := func(name string, hooks Hooks) *Node {
		dial := grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			if name == "a" && dialing != nil {
				dialing()
			}
			return lns[addr].DialContext(ctx)
		})
		n, err := NewNode(Config{
			Name: name, Incarnation: 1, Hooks: hooks,
			Resolver:    StaticResolver{"a": "passthrough:///a", "b": "passthrough:///b"},
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), dial},
		})
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer()
		n.Register(srv)
		go func() { _ = srv.Serve(lns[name]) }()
		t.Cleanup(func() { _ = n.Stop(context.Background()); srv.Stop() })
		return n
	}
	a, b = node("a", hooks), node("b", nil)
	if _, err := b.Spawn[*testpb.Ping](func(p *Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			_ = m.Watch(p.PID())
			_ = m.Reply(&testpb.Ping{}, nil)
		}
	}, WithName("echo")); err != nil {
		t.Fatal(err)
	}
	return a, b
}

var echo = Named[*testpb.Ping]("b", "echo")

// lateSessionEnd starts a session between a and b, then ends it on a in two
// steps, as Disconnect does: the links are taken, and act runs while the end
// is under way, its calls not yet failed nor its monitors fired; then the
// end is carried out.
func lateSessionEnd(t *testing.T, a *Node, act func()) {
	t.Helper()
	if _, err := echo.Call[*testpb.Ping](t.Context(), a, &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
	a.mu.Lock()
	out, in := a.takeLinks("b")
	a.mu.Unlock()
	act()
	synctest.Wait()
	a.linksLost(out, in, errDisconnected)
	synctest.Wait()
}

// A call made while a session's end is under way goes on the next session's
// link, which comes up only once that end is carried out: the end does not
// fail it.
func TestALateSessionEndSparesTheNextSessionsCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, _ := bufconnPair(t, nil, nil)
		errs := make(chan error, 1)
		lateSessionEnd(t, a, func() {
			go func() {
				_, err := echo.Call[*testpb.Ping](context.Background(), a, &testpb.Ping{})
				errs <- err
			}()
		})
		if err := <-errs; err != nil {
			t.Fatalf("the call failed with the session before it: %v", err)
		}
	})
}

// The same holds for a monitor: the end of a session it never went on does
// not fire it.
func TestALateSessionEndSparesTheNextSessionsMonitor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, _ := bufconnPair(t, nil, nil)
		downs := make(chan *Down, 1)
		lateSessionEnd(t, a, func() {
			if _, err := a.Spawn[*testpb.Ping](func(p *Process[*testpb.Ping]) error {
				p.Monitor(echo)
				m, err := p.ReceiveTimeout(time.Minute)
				if err == nil {
					downs <- m.Down
				}
				close(downs)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
		if d := <-downs; d != nil {
			t.Fatalf("the monitor fired with the session before it: %+v", d)
		}
	})
}

// The same holds for the watch a CallMonitor asks for, which echo places:
// the end of a session before it does not fire it.
func TestALateSessionEndSparesTheNextSessionsCallMonitor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, _ := bufconnPair(t, nil, nil)
		got := make(chan string, 1)
		lateSessionEnd(t, a, func() {
			if _, err := a.Spawn[*testpb.Ping](func(p *Process[*testpb.Ping]) error {
				if _, _, err := echo.CallMonitor[*testpb.Ping](context.Background(), p, &testpb.Ping{}); err != nil {
					got <- err.Error()
					return nil
				}
				m, err := p.ReceiveTimeout(time.Minute)
				if err != nil {
					got <- "no Down"
					return nil
				}
				got <- "Down " + m.Down.Reason
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
		if g := <-got; g != "no Down" {
			t.Fatalf("the CallMonitor got %s", g)
		}
	})
}

// A call that joins a dial begun before a session ended, whose link the end
// makes stale, is dialed again for: the session was not its.
func TestACallJoiningAStaleDialIsDialedAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		var gated atomic.Bool
		a, b := bufconnPair(t, nil, func() {
			if gated.Load() {
				<-gate
			}
		})
		// A session in which only b sent: a has only b's link.
		if err := b.SendTo(t.Context(), Named[*testpb.Ping]("a", "nobody"), &testpb.Ping{}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		gated.Store(true)
		go func() { _ = echo.Send(context.Background(), a, &testpb.Ping{}) }()
		synctest.Wait() // the dial has counted the session as current, and waits at the gate
		a.mu.Lock()
		out, in := a.takeLinks("b")
		a.mu.Unlock()
		errs := make(chan error, 1)
		go func() {
			_, err := echo.Call[*testpb.Ping](context.Background(), a, &testpb.Ping{})
			errs <- err
		}()
		synctest.Wait() // the call waits for the dial
		gated.Store(false)
		close(gate)
		synctest.Wait()
		a.linksLost(out, in, errDisconnected)
		if err := <-errs; err != nil {
			t.Fatalf("the call failed with the dial it joined: %v", err)
		}
	})
}

// cutLinks are an inbound link from b that a request came by, and the one
// that has replaced it since, which is n.in["b"].
func cutLinks(n *Node) (old, replaced *inLink) {
	peer := NodeID{Name: "b", Incarnation: 1}
	old = &inLink{peer: peer, done: make(chan struct{})}
	replaced = &inLink{peer: peer, done: make(chan struct{})}
	n.mu.Lock()
	n.in["b"] = replaced
	n.mu.Unlock()
	return old, replaced
}

// An answer dispatch makes itself, queued on a dial that then fails, cuts
// the link its request came by, not the one that has replaced it.
func TestADispatchedAnswerCutsOnlyTheLinkItCameBy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a") // its Resolver knows no b: the dial fails
		old, replaced := cutLinks(n)
		env := wire(grpcprocv1.Kind_KIND_CALL, PID{Node: "b", Incarnation: 1, ID: 1}, PID{Node: "a", Incarnation: 1, ID: 99}, "")
		env.Ref = 1
		n.dispatch(old, "b", nil, env) // no such process: the answer goes on a dial
		synctest.Wait()
		select {
		case <-old.done:
		default:
			t.Fatal("the link the call came by was not cut")
		}
		select {
		case <-replaced.done:
			t.Fatalf("the answer cut the new session's link: %v", replaced.why)
		default:
		}
		n.mu.Lock()
		delete(n.in, "b")
		n.mu.Unlock()
	})
}

// A Down that cannot go back cuts the link its watch came by.
func TestADownCutsOnlyTheLinkItsWatchCameBy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		addr, err := n.Spawn[*testpb.Ping](func(p *Process[*testpb.Ping]) error {
			_, err := p.Receive()
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		old, replaced := cutLinks(n)
		env := wire(grpcprocv1.Kind_KIND_MONITOR, PID{Node: "b", Incarnation: 1, ID: 1}, addr.PID(), "")
		env.Ref = 1
		n.dispatch(old, "b", nil, env)
		n.mu.Lock()
		n.backoff["b"] = &redial{at: time.Now().Add(time.Hour), err: errors.New("unreachable")}
		n.mu.Unlock()
		if err := n.Exit(t.Context(), addr.PID(), ReasonKilled); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case <-replaced.done:
			t.Fatalf("the Down cut the new session's link: %v", replaced.why)
		default:
		}
		select {
		case <-old.done:
		default:
			t.Fatal("the link the watch came by was not cut")
		}
		n.mu.Lock()
		delete(n.in, "b")
		n.mu.Unlock()
	})
}

// An answer that cannot go back cuts the link its call came by, not one that
// has replaced it since: the new session is owed nothing.
func TestAnAnswerCutsOnlyTheLinkItsCallCameBy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		msgs := make(chan Msg[*testpb.Ping], 1)
		addr, err := n.Spawn[*testpb.Ping](func(p *Process[*testpb.Ping]) error {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			msgs <- m
			<-p.Context().Done()
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		peer := NodeID{Name: "b", Incarnation: 1}
		old := &inLink{peer: peer, done: make(chan struct{})}
		env := wire(grpcprocv1.Kind_KIND_CALL, PID{Node: "b", Incarnation: 1, ID: 1}, addr.PID(), "")
		env.Ref = 1
		if err := encodeBody(env, &testpb.Ping{}); err != nil {
			t.Fatal(err)
		}
		n.dispatch(old, "b", nil, env)
		m := <-msgs

		// The session ends, and a new one comes in, while the call waits;
		// the peer cannot be dialed for the answer.
		old.close()
		replaced := &inLink{peer: peer, done: make(chan struct{})}
		n.mu.Lock()
		n.in["b"] = replaced
		n.backoff["b"] = &redial{at: time.Now().Add(time.Hour), err: errors.New("unreachable")}
		n.mu.Unlock()
		if err := m.Reply(&testpb.Ping{}, nil); !errors.Is(err, ErrNoConnection) {
			t.Fatalf("the answer went: %v", err)
		}
		select {
		case <-replaced.done:
			t.Fatalf("the answer cut the new session's link: %v", replaced.why)
		default:
		}
		n.mu.Lock()
		delete(n.in, "b")
		n.mu.Unlock()
	})
}

// gatedHooks records link events, and holds OnLinkUp until it is let go.
type gatedHooks struct {
	NopHooks
	mu     sync.Mutex
	events []string
	gate   chan struct{}
}

func (h *gatedHooks) OnLinkUp(peer NodeID) {
	<-h.gate
	h.mu.Lock()
	h.events = append(h.events, "up "+peer.Name)
	h.mu.Unlock()
}

func (h *gatedHooks) OnLinkDown(peer NodeID, _ error) {
	h.mu.Lock()
	h.events = append(h.events, "down "+peer.Name)
	h.mu.Unlock()
}

func (h *gatedHooks) seen() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.events)
}

// Stop waits for a dial that has installed its link and is announcing it:
// no hook runs after Stop has returned.
func TestStopWaitsForADialAnnouncingItsLink(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &gatedHooks{gate: make(chan struct{})}
		a, _ := bufconnPair(t, h, nil)
		go func() { _ = echo.Send(context.Background(), a, &testpb.Ping{}) }()
		synctest.Wait() // the dial's OnLinkUp waits on the gate
		stopped := make(chan struct{})
		go func() { _ = a.Stop(context.Background()); close(stopped) }()
		synctest.Wait()
		select {
		case <-stopped:
			t.Fatal("Stop returned while a dial ran OnLinkUp")
		default:
		}
		close(h.gate)
		<-stopped
	})
}

// Stop's ctx bounds its wait for a link-up being announced, as its others.
func TestStopGivesUpOnALinkUpWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &gatedHooks{gate: make(chan struct{})}
		a, _ := bufconnPair(t, h, nil)
		go func() { _ = echo.Send(context.Background(), a, &testpb.Ping{}) }()
		synctest.Wait() // the dial's OnLinkUp waits on the gate
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := a.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop: %v", err)
		}
		close(h.gate)
	})
}

// A session's end is announced after its start, even when the session ends
// while its start is still being announced.
func TestLinkDownFollowsItsLinkUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &gatedHooks{gate: make(chan struct{})}
		a, _ := bufconnPair(t, h, nil)
		go func() { _ = echo.Send(context.Background(), a, &testpb.Ping{}) }()
		synctest.Wait() // the dial's OnLinkUp waits on the gate
		go a.Disconnect("b")
		synctest.Wait()
		close(h.gate)
		synctest.Wait()
		if got := h.seen(); len(got) < 2 || !slices.Equal(got[:2], []string{"up b", "down b"}) {
			t.Fatalf("link events %v", got)
		}
	})
}

// Link events come once per session, whichever way its links go: a call
// and its answer bring up a link each way, and the session one link-up.
func TestLinkEventsComeOncePerSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &gatedHooks{gate: make(chan struct{})}
		close(h.gate)
		a, _ := bufconnPair(t, h, nil)
		if _, err := echo.Call[*testpb.Ping](t.Context(), a, &testpb.Ping{}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		a.Disconnect("b")
		synctest.Wait()
		if got := h.seen(); !slices.Equal(got, []string{"up b", "down b"}) {
			t.Fatalf("link events %v", got)
		}
	})
}

// A remote call whose caller exits before it leaves, once its link is known,
// records no watch and is not sent.
func TestRemoteCallOfAnExitedCallerIsNotSent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		l := queuedLink(t, n, NodeID{Name: "b", Incarnation: 1})
		r := n.callRemote(t.Context(), n.PID(), PID{Node: "b", Incarnation: 1, ID: 1}, "", &testpb.Ping{}, nil, 7, func() bool { return false })
		if !errors.Is(r.err, ErrNoProc) || l.info().Queued != 0 {
			t.Fatalf("got %v, with %d queued", r.err, l.info().Queued)
		}
	})
}
