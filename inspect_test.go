package grpcproc_test

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func TestInspectAndInfo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		type state struct{ handled int }
		s := &state{}
		release := make(chan struct{})
		e, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				s.handled++
				if m.Body.N == 7 {
					<-release // simulate a long handler
				}
			}
		}, grpcproc.WithName("insp"), grpcproc.WithLabel("order"), grpcproc.WithInspect(func() map[string]string {
			return map[string]string{"handled": strconv.Itoa(s.handled)}
		}))
		for range 3 {
			_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 1})
		}
		time.Sleep(20 * time.Millisecond)
		got, err := a.Inspect(ctx(t), e.PID())
		if err != nil || got["handled"] != "3" {
			t.Fatalf("inspect: %v %v", got, err)
		}
		info, _ := a.Process(e.PID())
		if info.Label != "order" || info.Name != "insp" || info.Received != 3 || info.State != grpcproc.StateIdle ||
			info.LastMessage != "grpcproc.test.v1.Ping" || info.Type != "*testpb.Ping" {
			t.Fatalf("info %+v", info)
		}
		// Busy process: inspect times out with a reason, and the mailbox shows the backlog.
		_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 7})
		_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 1})
		time.Sleep(20 * time.Millisecond)
		short, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		if _, err := a.Inspect(short, e.PID()); err == nil || !strings.Contains(err.Error(), "busy") {
			t.Fatalf("want busy error, got %v", err)
		}
		info, _ = a.Process(e.PID())
		if info.State != grpcproc.StateRunning || info.Mailbox.Depth != 1 || info.Mailbox.OldestAge <= 0 {
			t.Fatalf("busy info %+v", info)
		}
		close(release)
		ni := a.Info()
		if ni.ID.Name != "a" || ni.Processes != 1 || ni.Spawned != 1 {
			t.Fatalf("node info %+v", ni)
		}
		if all := a.Processes(); len(all) != 1 || all[0].PID != e.PID() {
			t.Fatalf("processes %+v", all)
		}
	})
}

func TestInspectWhileBacklogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		release := make(chan struct{})
		entered := make(chan struct{}, 1)
		handled := 0
		e, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				handled++
				if m.Body.N == 7 {
					entered <- struct{}{}
					<-release
				}
			}
		}, grpcproc.WithInspect(func() map[string]string { return map[string]string{"n": "x"} }))
		_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 7})
		<-entered
		_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 1}) // queued behind the busy handler
		got := make(chan error, 1)
		go func() { _, err := a.Inspect(ctx(t), e.PID()); got <- err }()
		time.Sleep(20 * time.Millisecond)
		close(release) // the pending inspect is served before the queued message
		if err := <-got; err != nil {
			t.Fatal(err)
		}
		// The inspect function itself blocking: the caller's ctx bounds the wait.
		blockInspect := make(chan struct{})
		e2, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			_, err := p.Receive()
			return err
		}, grpcproc.WithInspect(func() map[string]string { <-blockInspect; return nil }))
		short, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()
		if _, err := a.Inspect(short, e2.PID()); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
		close(blockInspect)
	})
}

func TestBusyMeasuresTheCurrentMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		release := make(chan struct{})
		defer close(release)
		p, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				if m.Body.GetN() == 1 {
					<-release
				}
			}
		})
		time.Sleep(300 * time.Millisecond) // old, but idle
		_ = p.Send(t.Context(), a, &testpb.Ping{N: 1})
		time.Sleep(20 * time.Millisecond)
		short, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()
		_, err := a.Inspect(short, p.PID())
		if d := busyFor(t, err); d > 200*time.Millisecond {
			t.Fatalf("busy must count from the message, not the start: %v", err)
		}
		// A process that never took a message counts from its start.
		stuck, _ := a.Spawn[*testpb.Ping](func(*grpcproc.Process[*testpb.Ping]) error { <-release; return nil })
		time.Sleep(50 * time.Millisecond)
		short2, cancel2 := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel2()
		_, err = a.Inspect(short2, stuck.PID())
		if d := busyFor(t, err); d < 50*time.Millisecond {
			t.Fatalf("busy since start: %v", err)
		}
		// Untyped processes are named proto.Message, not by the alias's target.
		u, _ := a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error { _, err := p.Receive(); return err })
		if info, _ := a.Process(u.PID()); info.Type != "proto.Message" || info.Label != "proto.Message" {
			t.Fatalf("%+v", info)
		}
	})
}

// Busy counts the time a process spends anywhere but waiting in Receive,
// waits in Call included, and BusyFor the time since it took the messages
// queued for it at once.
func TestBusyTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		events := a.Subscribe(t.Context(), 16)
		slow, _ := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				time.Sleep(3 * time.Second)
				_ = m.Reply(&testpb.Pong{}, nil)
			}
		})
		w, _ := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			time.Sleep(2 * time.Second)
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				if m.Body.N == 1 {
					if _, err := slow.Call[*testpb.Pong](p.Context(), p, &testpb.Ping{}); err != nil {
						return err
					}
				} else {
					time.Sleep(time.Second)
				}
			}
		})
		check := func(when string, busy, busyFor time.Duration) {
			t.Helper()
			info, _ := a.Process(w.PID())
			if info.Busy != busy || info.BusyFor != busyFor {
				t.Fatalf("%s: busy %v, for %v; want %v, for %v", when, info.Busy, info.BusyFor, busy, busyFor)
			}
		}
		time.Sleep(5 * time.Second)
		check("idle after its start", 2*time.Second, 0)
		_ = w.Send(t.Context(), a, &testpb.Ping{N: 1})
		time.Sleep(time.Second)
		check("in a Call", 3*time.Second, time.Second)
		time.Sleep(4 * time.Second)
		check("idle after the Call", 5*time.Second, 0)
		_ = w.Send(t.Context(), a, &testpb.Ping{N: 2})
		_ = w.Send(t.Context(), a, &testpb.Ping{N: 2})
		time.Sleep(1500 * time.Millisecond)
		check("on the second of two queued", 6500*time.Millisecond, 1500*time.Millisecond)
		_ = w.Send(t.Context(), a, &testpb.Ping{N: 2})
		time.Sleep(750 * time.Millisecond)
		check("on one queued while it was busy", 7250*time.Millisecond, 250*time.Millisecond)
		time.Sleep(3 * time.Second)
		if err := a.Exit(t.Context(), w.PID(), "stop"); err != nil {
			t.Fatal(err)
		}
		ev := nextEvent(t, events, grpcproc.EventExit)
		for ev.Process.PID != w.PID() {
			ev = nextEvent(t, events, grpcproc.EventExit)
		}
		if ev.Process.Busy != 8*time.Second || ev.Process.BusyFor != 0 {
			t.Fatalf("exited from a wait: busy %v, for %v", ev.Process.Busy, ev.Process.BusyFor)
		}
	})
}

// A wait ends the same whether a message, a timeout or an exit ends it: what
// the process does next is busy, and BusyFor counts from then.
func TestBusyTimeAfterAWaitEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		timer, _ := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				if _, err := p.ReceiveTimeout(time.Second); !errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				time.Sleep(500 * time.Millisecond)
			}
		})
		teardown, _ := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			_, err := p.Receive()
			time.Sleep(2 * time.Second)
			return err
		})
		// The timer works from 1s to 1.5s, 2.5s to 3s, and so on.
		time.Sleep(10*time.Second + 250*time.Millisecond)
		if info, _ := a.Process(timer.PID()); info.Busy != 3250*time.Millisecond || info.BusyFor != 250*time.Millisecond {
			t.Fatalf("timer: busy %v, for %v", info.Busy, info.BusyFor)
		}
		if err := a.Exit(t.Context(), teardown.PID(), "stop"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if info, _ := a.Process(teardown.PID()); info.Busy != time.Second || info.BusyFor != time.Second {
			t.Fatalf("teardown: busy %v, for %v", info.Busy, info.BusyFor)
		}
	})
}

// slowReceive takes a second over every message a process takes.
type slowReceive struct{ grpcproc.NopHooks }

func (slowReceive) OnReceive(_ grpcproc.ReceiveInfo, md grpcproc.Metadata) (grpcproc.Metadata, grpcproc.Done) {
	time.Sleep(time.Second)
	return md, nil
}

// What OnReceive takes of a message that ended a wait is busy time.
func TestBusyTimeOfOnReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, err := grpcproc.NewNode(grpcproc.Config{Admit: grpcproc.AdmitAll, Name: "a", Resolver: grpcproc.StaticResolver{}, Hooks: slowReceive{}, Logger: slog.New(slog.DiscardHandler)})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Stop(context.Background()) })
		addr, _ := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				if _, err := p.Receive(); err != nil {
					return err
				}
			}
		})
		time.Sleep(time.Second)
		_ = addr.Send(t.Context(), n, &testpb.Ping{})
		time.Sleep(2 * time.Second)
		if info, _ := n.Process(addr.PID()); info.Busy != time.Second || info.BusyFor != 0 {
			t.Fatalf("busy %v, for %v", info.Busy, info.BusyFor)
		}
	})
}

// A Receive inside an inspect function, served while its process waits,
// leaves the wait as it found it.
func TestBusyTimeOfAReceiveInAnInspect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		var self *grpcproc.Process[*testpb.Ping]
		addr, _ := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			self = p
			time.Sleep(time.Second)
			_, err := p.Receive()
			return err
		}, grpcproc.WithInspect(func() map[string]string {
			_, _ = self.ReceiveTimeout(time.Second)
			return nil
		}))
		time.Sleep(2 * time.Second)
		if _, err := a.Inspect(t.Context(), addr.PID()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if info, _ := a.Process(addr.PID()); info.Busy != time.Second || info.BusyFor != 0 {
			t.Fatalf("busy %v, for %v", info.Busy, info.BusyFor)
		}
	})
}

// An inspect function that panics in a wait ends the wait, for a process
// that recovers the panic and goes on as for one it ends.
func TestBusyTimeAfterAnInspectPanics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		panicked := false
		addr, _ := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			for p.Context().Err() == nil {
				func() {
					defer func() { _ = recover() }()
					if _, err := p.Receive(); err == nil {
						time.Sleep(time.Second)
					}
				}()
			}
			return nil
		}, grpcproc.WithInspect(func() map[string]string {
			if !panicked {
				panicked = true
				panic("once")
			}
			return nil
		}))
		time.Sleep(time.Second)
		_, _ = a.Inspect(t.Context(), addr.PID())
		time.Sleep(time.Second)
		_ = addr.Send(t.Context(), a, &testpb.Ping{})
		time.Sleep(2 * time.Second)
		if info, _ := a.Process(addr.PID()); info.Busy != time.Second || info.BusyFor != 0 {
			t.Fatalf("busy %v, for %v", info.Busy, info.BusyFor)
		}
	})
}

// An inspect that cannot be delivered to a process waiting in Receive, as it
// serves another, says no busy time.
func TestInspectOfAProcessServingAnother(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		p, _ := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			_, err := p.Receive()
			return err
		}, grpcproc.WithInspect(func() map[string]string { time.Sleep(2 * time.Second); return nil }))
		synctest.Wait()
		go func() { _, _ = a.Inspect(t.Context(), p.PID()) }()
		synctest.Wait()
		short, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if _, err := a.Inspect(short, p.PID()); err == nil || strings.Contains(err.Error(), "busy for") || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("inspect of a waiting process: %v", err)
		}
	})
}

func busyFor(t *testing.T, err error) time.Duration {
	t.Helper()
	if err == nil {
		t.Fatal("inspect of a busy process answered")
	}
	_, rest, ok := strings.Cut(err.Error(), "busy for ")
	d, perr := time.ParseDuration(strings.SplitN(rest, ":", 2)[0])
	if !ok || perr != nil {
		t.Fatalf("no duration in %v", err)
	}
	return d
}

// An inspect function that panics ends its process, as any panic in it
// does, and Inspect says so at once rather than wait on an answer that will
// not come.
func TestInspectOfAProcessThatPanicsAnswering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		events := a.Subscribe(t.Context(), 16)
		addr, err := a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			_, err := p.Receive()
			return err
		}, grpcproc.WithInspect(func() map[string]string { panic("inspect") }))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Inspect(ctx(t), addr.PID()); !errors.Is(err, grpcproc.ErrNoProc) || !strings.Contains(err.Error(), "inspect function panicked") {
			t.Fatalf("got %v", err)
		}
		for e := range events {
			if e.Kind == grpcproc.EventExit && e.Process.PID == addr.PID() {
				if e.Reason != "panic: inspect" {
					t.Fatalf("exited with %q", e.Reason)
				}
				return
			}
		}
	})
}

func TestOrderingOfSnapshots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b", "c")
		a := c.Node("a")
		e1, _ := a.Spawn(echo)
		e2, _ := a.Spawn(echo)
		if all := a.Processes(); len(all) != 2 || all[0].PID != e1.PID() || all[1].PID != e2.PID() {
			t.Fatalf("%+v", all)
		}
		for _, peer := range []string{"c", "b"} {
			e, _ := c.Node(peer).Spawn(echo)
			if _, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
		}
		links := a.Info().Links
		if len(links) != 4 || links[0].Peer.Name != "b" || !links[0].Outbound || links[1].Outbound || links[2].Peer.Name != "c" {
			t.Fatalf("%+v", links)
		}
		if peers := a.Peers(); len(peers) != 2 || peers[0] != "b" {
			t.Fatalf("%v", peers)
		}
		// A local monitor, demonitored.
		w, ch := watcher(t, a)
		ref := w.Monitor(e1)
		if info, _ := a.Process(e1.PID()); info.Watchers != 1 {
			t.Fatalf("%+v", info)
		}
		w.Demonitor(ref)
		if info, _ := a.Process(e1.PID()); info.Watchers != 0 {
			t.Fatalf("%+v", info)
		}
		_ = a.SendTo(t.Context(), e1, &testpb.Ping{N: 0})
		select {
		case m := <-ch:
			t.Fatalf("unexpected %+v", m)
		case <-time.After(100 * time.Millisecond):
		}
	})
}

// A process spawned with WithQuery is asked on the asker's goroutine, with
// its ctx, whatever the process is doing; one spawned without answers none.
func TestQuery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		busy := make(chan struct{})
		defer close(busy)
		ask, err := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			<-busy // never in Receive: a query does not need it to be
			return nil
		}, grpcproc.WithQuery(func(ctx context.Context, q proto.Message) (proto.Message, error) {
			switch q.(*testpb.Ping).GetN() {
			case 1:
				return nil, errors.New("refused")
			case 2:
				panic("boom")
			}
			if _, ok := ctx.Deadline(); !ok {
				return nil, errors.New("not the asker's ctx")
			}
			return &testpb.Pong{N: 7}, nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		if pong, err := a.Query(ctx, ask.PID(), &testpb.Ping{}); err != nil || pong.(*testpb.Pong).GetN() != 7 {
			t.Fatalf("%v %v", pong, err)
		}
		if _, err := a.Query(ctx, ask.PID(), &testpb.Ping{N: 1}); err == nil || err.Error() != "refused" {
			t.Errorf("an answer that is an error: %v", err)
		}
		if _, err := a.Query(ctx, ask.PID(), &testpb.Ping{N: 2}); err == nil || !strings.Contains(err.Error(), "panicked: boom") {
			t.Errorf("a query that panics: %v", err)
		}
		mute, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error { <-busy; return nil })
		if _, err := a.Query(ctx, mute.PID(), &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoQuery) {
			t.Errorf("a process with no queries: %v", err)
		}
		if _, err := a.Query(ctx, grpcproc.PID{Node: "a", Incarnation: a.ID().Incarnation, ID: 999}, &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Errorf("no process: %v", err)
		}
	})
}

// A query answered with nothing is answered with Empty.
func TestQueryOfNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		e, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error { <-p.Context().Done(); return nil },
			grpcproc.WithQuery(func(context.Context, proto.Message) (proto.Message, error) { return nil, nil }))
		if answer, err := a.Query(t.Context(), e.PID(), &testpb.Ping{}); err != nil || answer == nil {
			t.Fatalf("%v %v", answer, err)
		}
	})
}
