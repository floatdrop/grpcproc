package actor

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
)

// ErrWorkersBusy answers a call ReplyLater has no free worker for. It was
// never handled, so making the call again cannot run it twice.
var ErrWorkersBusy = errors.New("actor: every worker is busy; try again")

// Workers answers an actor's calls on processes of their own, at most n at
// once, so a slow answer does not hold up the actor or the messages behind
// it. Each answer is a process: cheap, but with the hooks and events of one.
// An actor makes its own, in Init or in its factory, and uses it from
// HandleCall:
//
//	type Inventory struct {
//		left    map[string]int64
//		reports *actor.Workers // actor.NewWorkers(4).Label("report")
//	}
//
//	func (i *Inventory) HandleCall(p *grpcproc.Process[*pb.Stock], m grpcproc.Msg[*pb.Stock]) (proto.Message, error) {
//		switch op := m.Body.Op.(type) {
//		case *pb.Stock_Report:
//			left := maps.Clone(i.left) // the state as this call found it
//			return nil, i.reports.ReplyLater(p, m, func(ctx context.Context, _ *grpcproc.Process[proto.Message]) (*pb.Report, error) {
//				return render(ctx, left, op.Report)
//			})
//		…
//	}
type Workers struct {
	slots chan struct{}
	label string
}

// NewWorkers returns Workers that run at most n answers at once, each a
// process labelled "reply" (see Label). It panics for n < 1.
func NewWorkers(n int) *Workers {
	if n < 1 {
		panic(fmt.Sprintf("actor: NewWorkers(%d): n must be at least 1", n))
	}
	return &Workers{slots: make(chan struct{}, n), label: "reply"}
}

// Label returns Workers whose processes are labelled label ("" is "reply"),
// the key the Inspector and metrics tell them apart by; w is unchanged. The
// two share their n places: the bound counts every label. A label is a
// metrics key, so it comes from a small set, a tool's name, say, never
// from what a caller sends.
func (w *Workers) Label(label string) *Workers {
	w.made()
	return &Workers{slots: w.slots, label: cmp.Or(label, "reply")}
}

// made panics for Workers not made by NewWorkers, nil included.
func (w *Workers) made() {
	if w == nil || w.slots == nil {
		panic("actor: Workers not made by NewWorkers")
	}
}

// ReplyLater answers the call m with what fn returns, from a worker process
// spawned by p and linked to it. fn gets the worker, to send and call from,
// and m's context (see grpcproc.Msg.Context), which also ends when p does;
// a caller that cancels with no deadline is not seen. A goroutine cannot be
// killed: a fn that does not watch ctx runs on after p, holding its place.
// The actor carries on at once, so fn must not touch the actor's state, only
// what nothing changes: a copy, or a value replaced, never modified.
//
// fn's response and error are the reply, as HandleCall's are.
//
// HandleCall returns what ReplyLater returns, and the caller is answered
// with it unless it is ErrNoReply: that once a worker has m;
// ErrWorkersBusy, without running fn, while n workers are answering; or the
// error spawning the worker. A worker's place is free before its answer is
// sent, so a caller that has the answer finds it free. A panic in fn
// answers m with "panic: …" and then ends the worker as panics do; the
// actor carries on. For a message that is not a call it is
// grpcproc.ErrNotCall. It panics for Workers not made by NewWorkers.
func (w *Workers) ReplyLater[M, R proto.Message](p *grpcproc.Process[M], m grpcproc.Msg[M], fn func(ctx context.Context, wp *grpcproc.Process[proto.Message]) (R, error)) error {
	w.made()
	if !m.IsCall() {
		return grpcproc.ErrNotCall
	}
	select {
	case w.slots <- struct{}{}:
	default:
		return ErrWorkersBusy
	}
	spawned := false
	defer func() {
		if !spawned {
			<-w.slots
		}
	}()
	_, err := p.Spawn(func(wp *grpcproc.Process[proto.Message]) error {
		answered := false
		answer := func(resp proto.Message, err error) {
			answered = true
			<-w.slots
			_ = m.Reply(resp, err)
		}
		defer func() {
			if answered {
				return
			}
			r := recover()
			if r == nil { // runtime.Goexit
				answer(nil, errNoAnswer)
				return
			}
			answer(nil, fmt.Errorf("panic: %v", r))
			panic(r)
		}()
		ctx, cancel := m.Context(wp.Context())
		defer cancel()
		answer(fn(ctx, wp))
		return nil
	}, grpcproc.WithLabel(w.label), grpcproc.LinkParent())
	if err != nil {
		return err
	}
	spawned = true
	return ErrNoReply
}

var errNoAnswer = errors.New("actor: the worker ended without answering")
