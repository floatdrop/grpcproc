package actor_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// reports answers Ping{N: 0} itself and every other call from a worker, with
// what fn returns for N.
type reports struct {
	w        *actor.Workers
	fn       func(ctx context.Context, wp *grpcproc.Process[proto.Message], n int64) (*testpb.Pong, error)
	gate     chan struct{} // if set, received from before ReplyLater
	returned chan error    // if set, given what ReplyLater returned
}

func (r *reports) HandleCall(p *P, m M) (proto.Message, error) {
	if m.Body.GetN() == 0 {
		return &testpb.Pong{}, nil
	}
	if r.gate != nil {
		<-r.gate
	}
	err := r.w.ReplyLater(p, m, func(ctx context.Context, wp *grpcproc.Process[proto.Message]) (*testpb.Pong, error) {
		return r.fn(ctx, wp, m.Body.GetN())
	})
	if r.returned != nil {
		r.returned <- err
	}
	return nil, err
}

func (r *reports) HandleMessage(p *P, m M) error {
	return r.w.ReplyLater(p, m, func(context.Context, *grpcproc.Process[proto.Message]) (*testpb.Pong, error) {
		panic("fn ran for a message that is not a call")
	})
}

func spawnReports(t *testing.T, n *grpcproc.Node, r *reports) grpcproc.Addr[*testpb.Ping] {
	t.Helper()
	addr, err := n.Spawn(actor.Run[*testpb.Ping](r))
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestReplyLaterAnswersBesideTheActor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		addr := spawnReports(t, n, &reports{w: actor.NewWorkers(2), fn: func(_ context.Context, _ *grpcproc.Process[proto.Message], v int64) (*testpb.Pong, error) {
			time.Sleep(time.Second)
			return &testpb.Pong{N: v}, nil
		}})
		start := time.Now()
		answers := make(chan int64, 2)
		for v := range int64(2) {
			go func() {
				r, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: v + 1})
				if err != nil || time.Since(start) != time.Second {
					t.Errorf("call %d answered after %v: %v", v+1, time.Since(start), err)
				}
				answers <- r.GetN()
			}()
		}
		synctest.Wait()
		// Both run on workers, so the actor answers at once.
		if _, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{}); err != nil || time.Since(start) != 0 {
			t.Fatalf("the actor answered after %v: %v", time.Since(start), err)
		}
		// A third has no worker, and is refused at once.
		if _, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 3}); !errors.Is(err, actor.ErrWorkersBusy) || time.Since(start) != 0 {
			t.Fatalf("a third call after %v: %v", time.Since(start), err)
		}
		if got := []int64{within(t, answers), within(t, answers)}; got[0]+got[1] != 3 {
			t.Fatalf("answers %v", got)
		}
		// Their places are free again.
		if r, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 3}); err != nil || r.GetN() != 3 {
			t.Fatalf("after: %v %v", r, err)
		}
	})
}

func TestReplyLaterWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		var addr grpcproc.Addr[*testpb.Ping]
		addr = spawnReports(t, n, &reports{w: actor.NewWorkers(1).Label("report"), fn: func(ctx context.Context, wp *grpcproc.Process[proto.Message], v int64) (*testpb.Pong, error) {
			info, ok := n.Process(wp.PID())
			if !ok || info.Label != "report" || info.Parent != addr.PID() {
				return nil, fmt.Errorf("worker %+v", info)
			}
			// The actor is free to answer its own worker.
			if _, err := addr.Call[*testpb.Pong](ctx, wp, &testpb.Ping{}); err != nil {
				return nil, err
			}
			return &testpb.Pong{N: v}, nil
		}})
		if r, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1}); err != nil || r.GetN() != 1 {
			t.Fatalf("call: %v %v", r, err)
		}
	})
}

// labelled answers each call from a worker of w.Label(label); the worker
// sends its label on labels, then waits for release.
type labelled struct {
	w       *actor.Workers
	label   string
	labels  chan string
	release chan struct{}
}

func (*labelled) HandleMessage(*P, M) error { return nil }

func (l *labelled) HandleCall(p *P, m M) (proto.Message, error) {
	return nil, l.w.Label(l.label).ReplyLater(p, m, func(_ context.Context, wp *grpcproc.Process[proto.Message]) (*testpb.Pong, error) {
		info, _ := p.Node().Process(wp.PID())
		l.labels <- info.Label
		<-l.release
		return &testpb.Pong{N: m.Body.GetN()}, nil
	})
}

func TestWorkersLabel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		l := &labelled{w: actor.NewWorkers(1), label: "report", labels: make(chan string, 1), release: make(chan struct{})}
		addr, err := n.Spawn(actor.Run[*testpb.Ping](l))
		if err != nil {
			t.Fatal(err)
		}
		answered := make(chan error, 1)
		go func() {
			_, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1})
			answered <- err
		}()
		if got := within(t, l.labels); got != "report" {
			t.Fatalf("label %q", got)
		}
		// The labelled Workers share their place.
		if _, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 2}); !errors.Is(err, actor.ErrWorkersBusy) {
			t.Fatalf("a second call: %v", err)
		}
		close(l.release)
		if err := within(t, answered); err != nil {
			t.Fatal(err)
		}
		// Label made a copy: the actor's own Workers keep their label.
		addr = spawnReports(t, n, &reports{w: l.w, fn: func(_ context.Context, wp *grpcproc.Process[proto.Message], v int64) (*testpb.Pong, error) {
			if info, _ := n.Process(wp.PID()); info.Label != "reply" {
				return nil, fmt.Errorf("label %q", info.Label)
			}
			return &testpb.Pong{N: v}, nil
		}})
		if _, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		// A second call finds the one place free: the copy's worker gave it back.
		if _, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWorkersLabelNotMade(t *testing.T) {
	defer func() {
		if r := recover(); r != "actor: Workers not made by NewWorkers" {
			t.Fatalf("recovered %v", r)
		}
	}()
	var w *actor.Workers
	w.Label("x")
}

func TestReplyLaterError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		addr := spawnReports(t, n, &reports{w: actor.NewWorkers(1), fn: func(context.Context, *grpcproc.Process[proto.Message], int64) (*testpb.Pong, error) {
			return nil, errors.New("no report")
		}})
		if _, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1}); err == nil || err.Error() != "no report" {
			t.Fatalf("call: %v", err)
		}
	})
}

func TestReplyLaterPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, logs := logged(t)
		addr := spawnReports(t, n, &reports{w: actor.NewWorkers(1), fn: func(_ context.Context, _ *grpcproc.Process[proto.Message], v int64) (*testpb.Pong, error) {
			if v == 1 {
				panic("boom")
			}
			return &testpb.Pong{N: v}, nil
		}})
		if _, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1}); err == nil || err.Error() != "panic: boom" {
			t.Fatalf("call: %v", err)
		}
		// The actor carries on, and the worker's slot is free again.
		if r, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 2}); err != nil || r.GetN() != 2 {
			t.Fatalf("after the panic: %v %v", r, err)
		}
		synctest.Wait()
		if !strings.Contains(logs.String(), "process panicked") {
			t.Fatalf("the panic was not reported:\n%s", logs)
		}
	})
}

func TestReplyLaterGoexit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		addr := spawnReports(t, n, &reports{w: actor.NewWorkers(1), fn: func(_ context.Context, _ *grpcproc.Process[proto.Message], v int64) (*testpb.Pong, error) {
			if v == 1 {
				runtime.Goexit()
			}
			return &testpb.Pong{N: v}, nil
		}})
		if _, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1}); err == nil || err.Error() != "actor: the worker ended without answering" {
			t.Fatalf("call: %v", err)
		}
		if r, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 2}); err != nil || r.GetN() != 2 {
			t.Fatalf("after: %v %v", r, err)
		}
	})
}

func TestReplyLaterEndsWithTheActor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		ended := make(chan error, 1)
		addr := spawnReports(t, n, &reports{w: actor.NewWorkers(1), fn: func(ctx context.Context, _ *grpcproc.Process[proto.Message], _ int64) (*testpb.Pong, error) {
			<-ctx.Done()
			ended <- context.Cause(ctx)
			return nil, ctx.Err()
		}})
		answered := make(chan error, 1)
		go func() {
			_, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1})
			answered <- err
		}()
		synctest.Wait()
		if err := n.Exit(t.Context(), addr, "bye"); err != nil {
			t.Fatal(err)
		}
		if err := within(t, answered); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("call: %v", err)
		}
		if ee, ok := errors.AsType[*grpcproc.ExitError](within(t, ended)); !ok || ee.Reason != "bye" {
			t.Fatalf("the worker ended with %v", ee)
		}
	})
}

func TestReplyLaterSpawnFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		r := &reports{
			w: actor.NewWorkers(1),
			fn: func(context.Context, *grpcproc.Process[proto.Message], int64) (*testpb.Pong, error) {
				panic("fn ran with no worker")
			},
			gate:     make(chan struct{}),
			returned: make(chan error, 1),
		}
		addr := spawnReports(t, n, r)
		go func() { _, _ = addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1}) }()
		synctest.Wait()
		go func() { _ = n.Stop(context.Background()) }()
		synctest.Wait()
		close(r.gate)
		if err := within(t, r.returned); !errors.Is(err, grpcproc.ErrNodeStopped) {
			t.Fatalf("ReplyLater: %v", err)
		}
	})
}

func TestReplyLaterNotACall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		addr := spawnReports(t, n, &reports{w: actor.NewWorkers(1)})
		downs := watch(t, n, addr)
		_ = addr.Send(t.Context(), n, &testpb.Ping{N: 1})
		if d := within(t, downs); d.Reason != grpcproc.ErrNotCall.Error() {
			t.Fatalf("%+v", d)
		}
	})
}

func TestReplyLaterZeroWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, logs := logged(t)
		addr := spawnReports(t, n, &reports{w: &actor.Workers{}})
		downs := watch(t, n, addr)
		_, _ = addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1})
		if d := within(t, downs); d.Reason != "panic: actor: Workers not made by NewWorkers" {
			t.Fatalf("%+v\n%s", d, logs)
		}
	})
}

func TestNewWorkersBelowOne(t *testing.T) {
	defer func() {
		if r := recover(); r != "actor: NewWorkers(0): n must be at least 1" {
			t.Fatalf("recovered %v", r)
		}
	}()
	actor.NewWorkers(0)
}
