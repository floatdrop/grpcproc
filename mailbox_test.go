package grpcproc_test

import (
	"errors"
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func TestMailboxLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &countingHooks{}
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(h)}, "a", "b")
		a, b := c.Node("a"), c.Node("b")

		// gated monitors victim, then takes nothing until gate opens.
		victim, err := b.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error { _, err := p.Receive(); return err })
		if err != nil {
			t.Fatal(err)
		}
		gate := make(chan struct{})
		got := make(chan grpcproc.Msg[*testpb.Ping], 8)
		gated, err := b.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			p.Monitor(victim)
			<-gate
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				got <- m
				if m.IsCall() {
					_ = m.Reply(&testpb.Pong{N: m.Body.N}, nil)
				}
			}
		}, grpcproc.WithMailboxLimit(2))
		if err != nil {
			t.Fatal(err)
		}
		for n := range int64(2) {
			if err := gated.Send(t.Context(), b, &testpb.Ping{N: n + 1}); err != nil {
				t.Fatal(err)
			}
		}

		// A full mailbox refuses messages and calls, local or remote; a send
		// still succeeds, its message a dead letter.
		if err := gated.Send(t.Context(), b, &testpb.Ping{N: 10}); err != nil {
			t.Fatal(err)
		}
		if err := gated.Send(t.Context(), a, &testpb.Ping{N: 11}); err != nil {
			t.Fatal(err)
		}
		for _, from := range []*grpcproc.Node{b, a} {
			_, err := gated.Call[*testpb.Pong](ctx(t), from, &testpb.Ping{N: 12})
			if _, remote := errors.AsType[*grpcproc.RemoteError](err); !errors.Is(err, grpcproc.ErrMailboxFull) || remote {
				t.Fatalf("call from %s: %v", from.Name(), err)
			}
		}
		// A refused CallMonitor leaves no monitor behind.
		called := make(chan error, 1)
		watched := make(chan grpcproc.Msg[proto.Message], 4)
		_, err = a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			_, _, err := gated.CallMonitor[*testpb.Pong](ctx(t), p, &testpb.Ping{N: 14})
			called <- err
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				watched <- m
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := within(t, called, "CallMonitor's answer"); !errors.Is(err, grpcproc.ErrMailboxFull) {
			t.Fatalf("CallMonitor: %v", err)
		}
		synctest.Wait()
		if got := b.Info().DeadLetters; got != 5 {
			t.Fatalf("dead letters = %d, want 5", got)
		}
		if r := h.lastDead.Load(); r == nil || *r != grpcproc.ReasonMailboxFull {
			t.Fatalf("reason %v", r)
		}

		// A Down gets in all the same, and counts.
		_ = victim.Send(t.Context(), b, &testpb.Ping{})
		synctest.Wait()
		info, _ := b.Process(gated.PID())
		if info.Mailbox.Limit != 2 || info.Mailbox.Depth != 3 {
			t.Fatalf("mailbox %+v, want limit 2, depth 3", info.Mailbox)
		}

		// Once the process takes them, there is room again.
		close(gate)
		for _, want := range []int64{1, 2} {
			if m := recv(t, got); m.Body.N != want {
				t.Fatalf("got %v, want %d", m.Body, want)
			}
		}
		if m := recv(t, got); m.Down == nil || m.Down.PID != victim.PID() {
			t.Fatalf("want victim's Down, got %+v", m)
		}
		if r, err := gated.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 20}); err != nil || r.N != 20 {
			t.Fatalf("%v %v", r, err)
		}
		recv(t, got)
		noMore(t, got)
		noMore(t, watched)
	})
}

func TestMailboxFullFromAHandlerIsItsAnswer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		// A handler that passes a downstream ErrMailboxFull on has run: its
		// caller gets a RemoteError, not the refusal that was never handled.
		relay, err := c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				_ = m.Reply(nil, grpcproc.ErrMailboxFull)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, from := range []*grpcproc.Node{c.Node("b"), c.Node("a")} {
			_, err := relay.Call[*testpb.Pong](ctx(t), from, &testpb.Ping{N: 1})
			if _, answered := errors.AsType[*grpcproc.RemoteError](err); !answered || !errors.Is(err, grpcproc.ErrMailboxFull) {
				t.Fatalf("from %s: %v", from.Name(), err)
			}
		}
	})
}

func TestNegativeMailboxLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		if _, err := n.Spawn(echo, grpcproc.WithMailboxLimit(-1)); err == nil {
			t.Fatal("spawned with a negative mailbox limit")
		}
	})
}
