package saga

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/floatdrop/grpcproc"
	sagav1 "github.com/floatdrop/grpcproc/saga/proto/grpcproc/saga/v1"
)

// maxAsking is how many queries, and how many controls, an engine answers
// at once; one past that is refused at once, to be asked again.
const maxAsking = 4

// askTimeout bounds a query or a control whose asker set no deadline, so a
// store that hangs does not hold its place for ever.
const askTimeout = 30 * time.Second

// bounded is ctx, with askTimeout if it has no deadline.
func bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, askTimeout)
}

// ErrBusy answers a query or a control that comes while an engine answers
// as many as it does at once.
var ErrBusy = errors.New("saga: the engine is answering other questions; ask again")

// query answers a sagav1.Query, through grpcproc.WithQuery: on the asker's
// goroutine, from the store, so never from the loop that claims runs.
func (e *Engine) query(ctx context.Context, q proto.Message) (proto.Message, error) {
	select {
	case e.querying <- struct{}{}:
		defer func() { <-e.querying }()
	default:
		return nil, ErrBusy
	}
	ctx, cancel := bounded(ctx)
	defer cancel()
	query, ok := q.(*sagav1.Query)
	if !ok {
		return nil, fmt.Errorf("saga: the engine answers a grpcproc.saga.v1.Query, not %T", q)
	}
	switch op := query.GetOp().(type) {
	case *sagav1.Query_List:
		return e.list(ctx, op.List)
	case *sagav1.Query_Get:
		return e.get(ctx, op.Get)
	}
	return nil, errors.New("saga: a Query without an op")
}

// ask answers a call, a sagav1.Control from the Inspector, in a process of
// its own, since it writes to the store; any other call is refused here.
func (e *Engine) ask(p *grpcproc.Process[proto.Message], m grpcproc.Msg[proto.Message]) {
	ctl, ok := m.Body.(*sagav1.Control)
	switch {
	case !ok:
		_ = m.Reply(nil, fmt.Errorf("saga: the engine is called with a grpcproc.saga.v1.Control, not %T", m.Body))
		return
	case len(e.asking) >= maxAsking:
		_ = m.Reply(nil, ErrBusy)
		return
	}
	// It fails only as the node stops, and the engine's exit then fails the
	// call.
	if _, ref, err := p.SpawnMonitor(func(w *grpcproc.Process[proto.Message]) error {
		ctx, cancel := m.Context(w.Context())
		defer cancel()
		ctx, stop := bounded(ctx)
		defer stop()
		return m.Reply(e.control(ctx, ctl))
	}, grpcproc.WithLabel("saga control")); err == nil {
		e.asking[ref] = true
	}
}

func (e *Engine) control(ctx context.Context, ctl *sagav1.Control) (proto.Message, error) {
	r := ctl.GetResume()
	if r == nil {
		return nil, errors.New("saga: a Control without an op")
	}
	if err := e.resume(ctx, r.GetSaga(), r.GetId()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// list answers a ListRuns from Store.List, saga by saga: the one it names,
// or every one the engine runs. Each page reads each saga's runs whole, and
// ends at its limit or at pageBytes, whichever comes first.
func (e *Engine) list(ctx context.Context, q *sagav1.ListRuns) (*sagav1.Runs, error) {
	sagas := []string{q.GetSaga()}
	if q.GetSaga() == "" {
		sagas = slices.Sorted(maps.Keys(e.sagas))
	}
	limit := int(min(cmp.Or(q.GetLimit(), 100), 1000))
	after := q.GetAfter()
	out := &sagav1.Runs{}
	size := 0
	for _, name := range sagas {
		if after != nil && name < after.GetSaga() {
			continue
		}
		runs, err := e.cfg.Store.List(ctx, name)
		if err != nil {
			return nil, err
		}
		for _, r := range runs {
			if after != nil && name == after.GetSaga() && r.ID <= after.GetId() {
				continue
			}
			if len(q.GetStatus()) > 0 && !slices.Contains(q.GetStatus(), status(r.Status)) {
				continue
			}
			v := e.run(r, false)
			if len(out.Runs) == limit || len(out.Runs) > 0 && size+proto.Size(v) > pageBytes {
				out.More = true
				return out, nil
			}
			out.Runs = append(out.Runs, v)
			size += proto.Size(v)
		}
	}
	return out, nil
}

// get answers a run, its data and its signals' payloads as JSON.
func (e *Engine) get(ctx context.Context, ref *sagav1.RunRef) (*sagav1.Run, error) {
	r, ok, err := e.cfg.Store.Get(ctx, ref.GetSaga(), ref.GetId())
	switch {
	case err != nil:
		return nil, err
	case !ok:
		return nil, ErrNoRun
	}
	return e.run(r, true), nil
}

// The most an answer carries: a page, a run's data, and a payload, of
// which a run holds MaxInbox, a megabyte at most; well under a message's
// 4 MiB.
const (
	pageBytes    = 2 << 20
	dataBytes    = 1 << 20
	payloadBytes = 16 << 10
)

// run is r as a query answers it: in full, with its data and payloads as
// JSON if the engine runs its saga, or as a page lists it. Its saga and ID
// are as stored, made valid UTF-8, which a proto string must be; its other
// text is clipped too, and what an answer cannot carry is left out, with its
// size.
func (e *Engine) run(r Record, full bool) *sagav1.Run {
	textLimit := 64 << 10
	if !full {
		textLimit = 1 << 10
	}
	out := &sagav1.Run{
		Saga: strings.ToValidUTF8(r.Saga, "\uFFFD"), Id: strings.ToValidUTF8(r.ID, "\uFFFD"),
		SagaVersion: r.SagaVersion, State: text(r.State, 1<<10),
		Status: status(r.Status), Visit: r.Visit, EffectDone: r.EffectDone, Attempts: int64(r.Attempts),
		Error: text(r.Error, textLimit), Cause: text(r.Cause, textLimit),
		Wake: stamp(r.Wake), RetryAt: stamp(r.RetryAt), Deadline: stamp(r.Deadline),
		Owner: text(r.Owner, 1<<10), LeaseUntil: stamp(r.LeaseUntil), Epoch: r.Epoch, Seen: r.Seen,
		Revision: r.Revision, CreatedAt: stamp(r.CreatedAt), UpdatedAt: stamp(r.UpdatedAt),
	}
	if !full {
		out.Waiting = uint32(len(r.Inbox))
		return out
	}
	s, known := e.sagas[r.Saga]
	if known {
		out.Data, out.DataOmitted = fit(r.Data, dataBytes, s.dataJSON)
	}
	for _, sig := range r.Inbox {
		v := &sagav1.Signal{Seq: sig.Seq, Event: text(sig.Event, 1<<10), Version: sig.Version}
		if known {
			v.Payload, v.PayloadOmitted = fit(sig.Payload, payloadBytes, func(b []byte) string { return s.payloadJSON(sig.Event, b) })
		}
		out.Inbox = append(out.Inbox, v)
	}
	return out
}

// fit is raw, rendered as JSON, if both are at most n bytes; past that, no
// JSON, and the size of what was too large.
func fit(raw []byte, n int, render func([]byte) string) (string, uint64) {
	if len(raw) > n {
		return "", uint64(len(raw))
	}
	json := render(raw)
	if len(json) > n {
		return "", uint64(len(json))
	}
	return json, 0
}

// text is s as an answer carries it: cut to n bytes at a character, and
// marked so if cut, then made valid UTF-8.
func text(s string, n int) string {
	if len(s) > n {
		cut := n
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut] + "…"
	}
	return strings.ToValidUTF8(s, "\uFFFD")
}

// inspect is what the engine publishes about itself: each saga it runs, as
// "saga <name>", with its version, and how many runs it works on.
func (e *Engine) inspect() map[string]string {
	out := map[string]string{"working": strconv.Itoa(e.active)}
	for name, v := range e.versions {
		out["saga "+name] = "v" + strconv.FormatUint(v, 10)
	}
	return out
}

func status(s Status) sagav1.Status {
	switch s {
	case Done:
		return sagav1.Status_STATUS_DONE
	case Stuck:
		return sagav1.Status_STATUS_STUCK
	}
	return sagav1.Status_STATUS_ACTIVE
}

func stamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
