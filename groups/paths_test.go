package groups_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/groups"
	groupsv1 "github.com/floatdrop/grpcproc/groups/proto/grpcproc/groups/v1"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// members is a Membership the test drives.
type members struct {
	mu       sync.Mutex
	failures int // Watch fails this many times first
	watches  []chan grpcproc.MemberEvent
	up       []grpcproc.Member
}

func (m *members) Watch(ctx context.Context) (<-chan grpcproc.MemberEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failures > 0 {
		m.failures--
		return nil, errors.New("etcd is down")
	}
	ch := make(chan grpcproc.MemberEvent, 16)
	for _, mb := range m.up {
		ch <- grpcproc.MemberEvent{Member: mb, Up: true}
	}
	m.watches = append(m.watches, ch)
	context.AfterFunc(ctx, func() { m.end(ch) })
	return ch, nil
}

// emit reports mb up or down to every watch.
func (m *members) emit(mb grpcproc.Member, up bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.up = slices.DeleteFunc(m.up, func(x grpcproc.Member) bool { return x.Name == mb.Name })
	if up {
		m.up = append(m.up, mb)
	}
	for _, ch := range m.watches {
		ch <- grpcproc.MemberEvent{Member: mb, Up: up}
	}
}

// vanish takes name out of what a new watch lists, telling no watch.
func (m *members) vanish(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.up = slices.DeleteFunc(m.up, func(x grpcproc.Member) bool { return x.Name == name })
}

// end ends ch, or every watch.
func (m *members) end(ch chan grpcproc.MemberEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, w := range m.watches {
		if ch == nil || w == ch {
			close(w)
			m.watches = slices.Delete(m.watches, i, i+1)
			return
		}
	}
}

func selected(name string) grpcproc.Member {
	return grpcproc.Member{Name: name, Metadata: map[string]string{"groups": "yes"}}
}

// Membership says which nodes to ask, Select which of them; a node that
// leaves the cluster is forgotten, and a watch that fails or ends is
// started again.
func TestMembership(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b", "c", "d")
		a, b := c.Node("a"), c.Node("b")
		mem := &members{failures: 1, up: []grpcproc.Member{selected("a"), selected("b"), {Name: "c"}}}
		sa := start(t, a, groups.Config{Membership: mem, Select: func(m grpcproc.Member) bool { return m.Metadata["groups"] == "yes" }})
		sb := start(t, b, groups.Config{})
		start(t, c.Node("c"), peers("a")) // not selected, but it says hello
		pb := idle(t, b)
		join(t, sb, b, "g", pb)
		synctest.Wait()
		wantPeers(t, a, sa, "c synced") // the watch failed; c said hello
		time.Sleep(time.Second)
		synctest.Wait()
		wantPeers(t, a, sa, "b synced, c synced")
		same(t, "b's", sa.Members("g"), pb)
		mem.emit(grpcproc.Member{Name: "c"}, true) // still not selected: a goes on answering it
		synctest.Wait()
		wantPeers(t, a, sa, "b synced, c synced")
		wantPeers(t, b, sb, "a synced")

		mem.emit(selected("b"), true) // known
		mem.emit(selected("d"), true) // runs no scope
		synctest.Wait()
		wantPeers(t, a, sa, "b synced, c synced, d absent")
		mem.emit(selected("d"), false)
		mem.emit(selected("b"), false)
		synctest.Wait()
		wantPeers(t, a, sa, "c synced")
		if got := sa.Members("g"); len(got) != 0 {
			t.Fatalf("members of a node that left: %v", got)
		}

		mem.emit(selected("b"), true)
		mem.emit(selected("d"), true)
		synctest.Wait()
		wantPeers(t, a, sa, "b synced, c synced, d absent")
		mem.vanish("d") // it leaves while the watch is down
		mem.end(nil)
		synctest.Wait()
		time.Sleep(time.Second) // watched again, which lists b and not d
		synctest.Wait()
		wantPeers(t, a, sa, "b synced, c synced")
	})
}

// tell sends m from n to to.
func tell(t *testing.T, n *grpcproc.Node, to grpcproc.Target, m proto.Message) {
	t.Helper()
	if err := n.SendTo(t.Context(), to, m); err != nil {
		t.Fatal(err)
	}
}

// A scope ignores what does not come from where it should.
func TestStrays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b", "c", "d")
		a, b, cn := c.Node("a"), c.Node("b"), c.Node("c")
		sa, sb := start(t, a, peers("b", "c")), start(t, b, peers("a"))
		pb := idle(t, b)
		join(t, sb, b, "g", pb)
		synctest.Wait()
		unchanged := func(what string) {
			t.Helper()
			synctest.Wait()
			wantPeers(t, a, sa, "b synced, c absent")
			same(t, "g after "+what, sa.Members("g"), pb)
			if got := sa.Groups(); !slices.Equal(got, []string{"g"}) {
				t.Fatalf("groups after %s: %v", what, got)
			}
		}

		tell(t, a, sa.PID(), &testpb.Ping{})
		if _, err := a.CallTo[*emptypb.Empty](t.Context(), sa.PID(), &testpb.Ping{}); !errors.Is(err, grpcproc.ErrType) {
			t.Fatalf("a call of another type: %v", err)
		}
		if _, err := a.CallTo[*groupsv1.Hello](t.Context(), sa.PID(), &groupsv1.Hello{}); err != nil {
			t.Fatalf("a hello from its own node: %v", err)
		}
		unchanged("a hello from its own node")
		tell(t, a, sa.PID(), &groupsv1.Found{Scope: sb.PID().Proto()})
		tell(t, a, sa.PID(), &groupsv1.Lost{Reason: grpcproc.ReasonNoConnection})
		unchanged("a Found and a Lost from no relay")
		tell(t, cn, sa.PID(), &groupsv1.Sync{First: true, Groups: []*groupsv1.Members{{Group: "x", Ids: []uint64{1}}}})
		tell(t, cn, sa.PID(), &groupsv1.Update{Joined: []*groupsv1.Members{{Group: "x", Ids: []uint64{1}}}})
		unchanged("a Sync and an Update from a node that runs no scope, asked again")
		tell(t, b, sa.PID(), &groupsv1.Sync{Groups: []*groupsv1.Members{{Group: "x", Ids: []uint64{1}}}})
		unchanged("a part of a Sync whose start a did not take")
		tell(t, c.Node("d"), sa.PID(), &groupsv1.Update{Joined: []*groupsv1.Members{{Group: "x", Ids: []uint64{1}}}})
		tell(t, a, sa.PID(), &groupsv1.Update{Joined: []*groupsv1.Members{{Group: "x", Ids: []uint64{1}}}})
		unchanged("an Update from a node it never heard of, asked and forgotten, and one from its own")
		tell(t, a, sa.PID(), &groupsv1.MemberEvent{Node: "b", Up: false})
		tell(t, b, sa.PID(), &groupsv1.MemberEvent{Node: "x", Up: true})
		tell(t, a, sa.PID(), &groupsv1.Retry{})
		tell(t, a, sa.PID(), &groupsv1.Unwatch{Group: "g"})
		unchanged("membership news and a retry from strangers, and no watch")
		tell(t, a, relays(a)[0], &groupsv1.Update{Joined: []*groupsv1.Members{{Group: "x", Ids: []uint64{1}}}})
		synctest.Wait()
		if got := sb.Groups(); !slices.Equal(got, []string{"g"}) {
			t.Fatalf("b's groups after a stranger wrote to a's relay: %v", got)
		}

		// An Update that says nothing new changes nothing.
		tell(t, b, sa.PID(), &groupsv1.Update{Joined: []*groupsv1.Members{{Group: "g", Ids: []uint64{pb.ID}}}, Left: []*groupsv1.Members{{Group: "g", Ids: []uint64{pb.ID + 100}}}})
		unchanged("an Update of what a knows")
	})
}

// A scope keeps up with a peer whose relay ends, whose scope says hello
// anew, and whose scope is not the one it watched.
func TestReplaced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		sa, sb := start(t, a, peers("b")), start(t, b, peers("a"))
		pb := idle(t, b)
		join(t, sb, b, "g", pb)
		synctest.Wait()

		// a's relay ends without a word: b is asked again a Retry later.
		exit(t, a, relays(a)[0])
		wantPeers(t, a, sa, "b unreachable")
		time.Sleep(time.Second)
		synctest.Wait()
		wantPeers(t, a, sa, "b synced")
		same(t, "g once a's relay is back", sa.Members("g"), pb)

		// A hello from an older scope than the one a's relay reaches changes
		// nothing.
		before := relays(a)
		if _, err := b.CallTo[*groupsv1.Hello](t.Context(), sa.PID(), &groupsv1.Hello{Scope: grpcproc.PID{Node: "b", Incarnation: sb.PID().Incarnation}.Proto()}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if after := relays(a); !slices.Equal(before, after) {
			t.Fatalf("a stale hello replaced a's relay: %v, then %v", before, after)
		}

		// A hello from a scope a's relay does not reach: a reaches it anew.
		other := idle(t, b)
		if _, err := b.CallTo[*groupsv1.Hello](t.Context(), sa.PID(), &groupsv1.Hello{Scope: other.Proto()}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		wantPeers(t, a, sa, "b synced")
		same(t, "g once a reached b anew", sa.Members("g"), pb)

		// A Sync names a scope that is not the one a's relay watches: when
		// that one goes, a looks again, and finds none.
		tell(t, b, sa.PID(), &groupsv1.Sync{Scope: other.Proto(), First: true})
		synctest.Wait()
		exit(t, b, sb.PID())
		wantPeers(t, a, sa, "b absent")
	})
}

// A node that asked this one, and cannot be reached for long, is forgotten,
// and revived when it says hello again.
func TestAskerLostAndRevived(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		sa, sb := start(t, a, groups.Config{}), start(t, b, peers("a"))
		pb := idle(t, b)
		join(t, sb, b, "g", pb)
		synctest.Wait()
		wantPeers(t, a, sa, "b synced")
		c.Partition("a", "b")
		synctest.Wait()
		wantPeers(t, a, sa, "b unreachable")
		time.Sleep(time.Minute) // asked again at 1, 3, 7, 15 and 31s, in vain
		synctest.Wait()
		wantPeers(t, a, sa, "")
		c.Heal("a", "b")
		time.Sleep(time.Minute) // b, which asks a, says hello again
		synctest.Wait()
		wantPeers(t, a, sa, "b synced")
		same(t, "b's member, back", sa.Members("g"), pb)
	})
}

// A Join of many processes goes to the other nodes in parts.
func TestUpdateInParts(t *testing.T) {
	defer groups.SetSyncBudget(64)()
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		sa, sb := start(t, a, peers("b")), start(t, b, peers("a"))
		synctest.Wait()
		var want []grpcproc.PID
		for range 20 {
			want = append(want, idle(t, a))
		}
		join(t, sa, a, "g", want...)
		join(t, sa, a, "h", want[:2]...)
		synctest.Wait()
		same(t, "g", sb.Members("g"), want...)
		exit(t, a, want[0]) // in two groups: one Update
		same(t, "h", sb.Members("h"), want[1])
		same(t, "g", sb.Members("g"), want[1:]...)
	})
}

// Peers are asked whatever Membership says; a node Select stops taking is
// asked no more; the end of an incarnation older than the one reported up
// changes nothing.
func TestPeersAndMembership(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b", "c", "d")
		a := c.Node("a")
		inc := func(name string, older uint64) grpcproc.Member {
			m := selected(name)
			m.Incarnation = c.Node(name).PID().Incarnation - older
			return m
		}
		mem := &members{up: []grpcproc.Member{inc("b", 0), inc("c", 0), inc("d", 0)}}
		sa := start(t, a, groups.Config{Peers: []string{"b"}, Membership: mem, Select: func(m grpcproc.Member) bool { return m.Metadata["groups"] == "yes" }})
		start(t, c.Node("b"), groups.Config{})
		start(t, c.Node("c"), groups.Config{})
		synctest.Wait()
		wantPeers(t, a, sa, "b synced, c synced, d absent")

		mem.emit(inc("c", 1), false) // an older incarnation's end
		mem.emit(inc("b", 0), false) // gone, but Peers names it
		synctest.Wait()
		wantPeers(t, a, sa, "b synced, c synced, d absent")
		mem.emit(grpcproc.Member{Name: "d", Incarnation: c.Node("d").PID().Incarnation}, true) // up, and not taken
		mem.emit(grpcproc.Member{Name: "e"}, true)                                             // never asked
		synctest.Wait()
		wantPeers(t, a, sa, "b synced, c synced")
		mem.emit(inc("c", 0), false)
		synctest.Wait()
		wantPeers(t, a, sa, "b synced")
	})
}

// A scope that answers its Hello with an error refuses it: that node is
// absent, not unreachable.
func TestHelloRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		if _, err := b.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				_ = m.Reply(nil, errors.New("not a scope"))
			}
		}, grpcproc.WithName(groups.ScopeName(groups.DefaultScope))); err != nil {
			t.Fatal(err)
		}
		sa := start(t, a, peers("b"))
		synctest.Wait()
		wantPeers(t, a, sa, "b absent")
	})
}

// An Update from a node whose Sync has not come yet changes nothing.
func TestUpdateBeforeSync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		if _, err := b.Spawn(func(p *grpcproc.Process[proto.Message]) error { // takes a's hello, and never answers
			<-p.Context().Done()
			return nil
		}, grpcproc.WithName(groups.ScopeName(groups.DefaultScope))); err != nil {
			t.Fatal(err)
		}
		sa := start(t, a, peers("b"))
		synctest.Wait()
		tell(t, b, sa.PID(), &groupsv1.Update{Joined: []*groupsv1.Members{{Group: "x", Ids: []uint64{1}}}})
		synctest.Wait()
		wantPeers(t, a, sa, "b syncing")
		if got := sa.Groups(); len(got) != 0 {
			t.Fatalf("groups after an Update before a Sync: %v", got)
		}
	})
}

// A Hello and a Sync of an incarnation older than one the node is known to
// have change nothing: here Membership says b is newer than it is.
func TestOlderIncarnation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		newer := selected("b")
		newer.Incarnation = b.PID().Incarnation + 1
		sa := start(t, a, groups.Config{Membership: &members{up: []grpcproc.Member{newer}}})
		sb := start(t, b, peers("a"))
		join(t, sb, b, "g", idle(t, b))
		synctest.Wait()
		wantPeers(t, a, sa, "b syncing")
		if got := sa.Members("g"); len(got) != 0 {
			t.Fatalf("members of an older incarnation: %v", got)
		}
	})
}
