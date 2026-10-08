package grpcproc_test

import (
	"testing"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func BenchmarkLocalCall(b *testing.B) {
	c := grpcproctest.New(b, "a")
	a := c.Node("a")
	e, _ := a.Spawn(echo)
	for b.Loop() {
		if _, err := e.Call[*testpb.Pong](b.Context(), a, &testpb.Ping{N: 1}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRemoteCall(b *testing.B) {
	c := grpcproctest.New(b, "a", "b")
	a := c.Node("a")
	e, _ := c.Node("b").Spawn(echo)
	_, _ = e.Call[*testpb.Pong](b.Context(), a, &testpb.Ping{N: 1}) // establish the link
	for b.Loop() {
		if _, err := e.Call[*testpb.Pong](b.Context(), a, &testpb.Ping{N: 1}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRemoteCallParallel(b *testing.B) {
	c := grpcproctest.New(b, "a", "b")
	a := c.Node("a")
	e, _ := c.Node("b").Spawn(echo)
	_, _ = e.Call[*testpb.Pong](b.Context(), a, &testpb.Ping{N: 1})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := e.Call[*testpb.Pong](b.Context(), a, &testpb.Ping{N: 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// benchSend measures sends until every one is received, so the number is
// end-to-end throughput, not the cost of enqueueing. It uses b.N rather than
// b.Loop on purpose: b.Loop stops the timer when the loop ends, before the
// wait for the receiver, which would time only the enqueue.
func benchSend(b *testing.B, from, to *grpcproc.Node) {
	done := make(chan struct{})
	total := b.N
	addr, err := to.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		for range total + 1 {
			if _, err := p.Receive(); err != nil {
				return err
			}
		}
		close(done)
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	msg := &testpb.Ping{}
	_ = addr.Send(b.Context(), from, msg) // establish the link before timing
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := addr.Send(b.Context(), from, msg); err != nil {
			b.Fatal(err)
		}
	}
	<-done
}

func BenchmarkLocalSend(b *testing.B) {
	c := grpcproctest.New(b, "a")
	benchSend(b, c.Node("a"), c.Node("a"))
}

func BenchmarkRemoteSend(b *testing.B) {
	c := grpcproctest.New(b, "a", "b")
	benchSend(b, c.Node("a"), c.Node("b"))
}

func BenchmarkSpawn(b *testing.B) {
	n := grpcproctest.New(b, "a").Node("a")
	b.ReportAllocs()
	for b.Loop() {
		if _, err := n.Spawn(func(*grpcproc.Process[*testpb.Ping]) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReceive measures taking messages from a mailbox that already holds
// them, with no sender racing the receiver: what a process that is behind
// pays per message. It uses b.N rather than b.Loop to leave each batch's
// sends untimed.
func BenchmarkReceive(b *testing.B) {
	const batch = 1000
	n := grpcproctest.New(b, "a").Node("a")
	start, done := make(chan int), make(chan struct{})
	addr, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			select {
			case k := <-start:
				for range k {
					if _, err := p.Receive(); err != nil {
						return err
					}
				}
				done <- struct{}{}
			case <-p.Context().Done():
				return nil
			}
		}
	})
	if err != nil {
		b.Fatal(err)
	}
	msg := &testpb.Ping{}
	b.ReportAllocs()
	b.ResetTimer()
	for left := b.N; left > 0; left -= batch {
		b.StopTimer()
		k := min(batch, left)
		for range k {
			if err := addr.Send(b.Context(), n, msg); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		start <- k
		<-done
	}
}
