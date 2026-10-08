package saga

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/floatdrop/grpcproc"
)

// Config configures an Engine.
type Config struct {
	// Store keeps the runs. Required. Engines that share one share the runs.
	Store Store
	// Name is the name the engine's process is registered under on its node.
	// Default "saga".
	Name string
	// Poll is how often the engine asks the store for runs that are due, and
	// how often Wait asks about a run. A run begun, signalled or resumed
	// through this engine is claimed at once. Default 1s.
	Poll time.Duration
	// Lease is how long a claim holds a run without being renewed: how long
	// a run whose engine died waits for another. The engine renews it a
	// quarter of that after each claim or renewal, and tries one that failed
	// again a sixteenth later; an effect whose engine could not renew it,
	// with a quarter of it left, has its ctx ended. Default 30s; at least a
	// millisecond.
	Lease time.Duration
	// Concurrency is how many runs the engine works on at once. Default 64.
	Concurrency int
	// InspectData lets the engine show, to a query through the Inspector,
	// a run's data and its signals' payloads. Off, a query sees a run's
	// state, status, timers, and its error and cause, which say why it is
	// stuck, and not what the run carries, which is often a customer's: a
	// read-only Inspector is not a private one. An effect whose error would
	// tell a customer's data should not put it there.
	InspectData bool
}

// Saga is a Definition, of any state and data type, as Start takes it.
type Saga interface {
	sagaName() string
	sagaVersion() uint64
	validate() error
	work(ctx context.Context, e *Engine, p *grpcproc.Process[proto.Message], r Record)
	// dataJSON and payloadJSON render a run's data, and the payload of a
	// signal for event, as JSON; "" for what does not decode.
	dataJSON(data []byte) string
	payloadJSON(event string, payload []byte) string
}

// Engine runs sagas on a node: it claims runs that are due from the store
// and works on each in a process of its own.
type Engine struct {
	n     *grpcproc.Node
	cfg   Config
	sagas map[string]Saga
	// versions is the sagas the engine runs, and the version of each here:
	// what it claims.
	versions map[string]uint64
	// pid is the engine's process, set by its loop before it claims a run.
	pid atomic.Pointer[grpcproc.PID]
	// active is the runs at work, and asking the controls being answered,
	// by the monitors of their processes. The loop alone, and its
	// inspect, which runs on the loop's goroutine, touch them.
	active int
	asking map[grpcproc.Ref]bool
	// querying holds a place for each query being answered.
	querying chan struct{}
}

// Start runs sagas on n, from cfg.Store, until n stops. The engine is a
// process, named cfg.Name; each run it works on is one too, labelled
// "saga:" and the saga's name, for as long as the run has work.
func Start(n *grpcproc.Node, cfg Config, sagas ...Saga) (*Engine, error) {
	if cfg.Store == nil {
		return nil, errors.New("saga: Config.Store is required")
	}
	cfg.Name = cmp.Or(cfg.Name, "saga")
	cfg.Poll = cmp.Or(cfg.Poll, time.Second)
	cfg.Lease = cmp.Or(cfg.Lease, 30*time.Second)
	cfg.Concurrency = cmp.Or(cfg.Concurrency, 64)
	if cfg.Poll < 0 || cfg.Lease < time.Millisecond || cfg.Concurrency < 0 {
		return nil, errors.New("saga: Config.Poll and Concurrency must not be negative, and Lease is at least a millisecond")
	}
	e := &Engine{n: n, cfg: cfg, sagas: map[string]Saga{}, versions: map[string]uint64{},
		asking: map[grpcproc.Ref]bool{}, querying: make(chan struct{}, maxAsking)}
	var errs []error
	for _, s := range sagas {
		if _, dup := e.sagas[s.sagaName()]; dup {
			errs = append(errs, fmt.Errorf("saga: two sagas named %q", s.sagaName()))
		}
		e.sagas[s.sagaName()], e.versions[s.sagaName()] = s, s.sagaVersion()
		errs = append(errs, s.validate())
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	addr, err := n.Spawn(e.loop, grpcproc.WithName(cfg.Name), grpcproc.WithLabel("saga engine"),
		grpcproc.WithInspect(e.inspect), grpcproc.WithQuery(e.query))
	if err != nil {
		return nil, err
	}
	pid := addr.PID()
	e.pid.Store(&pid)
	return e, nil
}

// PID is the engine's process.
func (e *Engine) PID() grpcproc.PID { return *e.pid.Load() }

// resume makes a stuck run active, and has the engine claim it at once.
func (e *Engine) resume(ctx context.Context, saga, id string) error {
	if err := e.cfg.Store.Resume(ctx, saga, id); err != nil {
		return err
	}
	e.nudge()
	return nil
}

// nudge has the engine ask the store now rather than at its next tick.
func (e *Engine) nudge() {
	_ = e.n.SendTo(context.Background(), e.PID(), &emptypb.Empty{})
}

// wakeAt has the engine ask the store at t, when a run it let go is due,
// rather than at the tick after.
func (e *Engine) wakeAt(t time.Time) {
	_ = e.n.SendTo(context.Background(), e.PID(), timestamppb.New(t))
}

// loop is the engine's process: it claims what is due, on a tick, when
// nudged, and when a run it let go says it will be within the tick, and
// counts the runs at work by the Downs of their processes. It hands a call,
// a control from the Inspector, to a process of its own, so the store is
// never asked from here; a query does not come here at all (see query).
func (e *Engine) loop(p *grpcproc.Process[proto.Message]) error {
	pid := p.PID()
	e.pid.Store(&pid)
	var wakes []time.Time // when the runs let go are due, soonest first
	for {
		// Read before the claim: a wake the store's clock has not reached
		// by then is kept.
		now := time.Now()
		wakes = slices.DeleteFunc(wakes, func(t time.Time) bool { return !t.After(now) })
		e.active += e.claim(p, e.cfg.Concurrency-e.active)
		wait := e.cfg.Poll
		if len(wakes) > 0 {
			wait = min(wait, time.Until(wakes[0]))
		}
		// Everything queued is taken before the store is asked again: a
		// burst of nudges is one claim.
		for m, err := p.ReceiveTimeout(wait); ; m, err = p.ReceiveTimeout(0) {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			if err != nil {
				return err
			}
			switch at, ok := m.Body.(*timestamppb.Timestamp); {
			case m.Down != nil && e.asking[m.Down.Ref]:
				delete(e.asking, m.Down.Ref)
			case m.Down != nil:
				e.active--
			case m.IsCall():
				e.ask(p, m)
			case ok && time.Until(at.AsTime()) < e.cfg.Poll:
				// Later than that, the tick is in time.
				i, _ := slices.BinarySearchFunc(wakes, at.AsTime(), time.Time.Compare)
				wakes = slices.Insert(wakes, i, at.AsTime())
			}
		}
	}
}

// claim takes up to n due runs and starts a process for each; it returns how
// many it started.
func (e *Engine) claim(p *grpcproc.Process[proto.Message], n int) int {
	if n <= 0 || p.Context().Err() != nil {
		return 0
	}
	// A claim the store made as the node stops comes back, to be let go,
	// rather than ending with ctx and leaving its runs to their leases.
	ctx, done := outlive(p.Context(), time.Second)
	defer done()
	// A lease is counted from before the call, by this clock, not the store's.
	claimed := time.Now()
	runs, err := e.cfg.Store.Claim(ctx, e.n.Name(), e.versions, e.cfg.Lease, n)
	if err != nil {
		if p.Context().Err() == nil { // not cut short by the stop
			p.Log().Warn("saga: claim", "err", err)
		}
		return 0
	}
	for i, r := range runs {
		if _, _, err := p.SpawnMonitor(func(w *grpcproc.Process[proto.Message]) error {
			e.work(w, r, claimed)
			return nil
		}, grpcproc.WithLabel("saga:"+r.Saga), grpcproc.LinkParent()); err != nil {
			// It fails only once the engine is told to exit or its node
			// stops, which may have let r go just before the claim took it.
			e.letGo(ctx, runs[i:]...)
			return i
		}
	}
	return len(runs)
}

// outlive returns a context that ends d after parent does, or when done is
// called.
func outlive(parent context.Context, d time.Duration) (ctx context.Context, done func()) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	grace := time.AfterFunc(d, cancel)
	grace.Stop()
	stop := context.AfterFunc(parent, func() { grace.Reset(d) })
	return ctx, func() { stop(); grace.Stop(); cancel() }
}

// letGo gives up the leases on runs, within ctx, so that another engine need
// not wait them out.
func (e *Engine) letGo(ctx context.Context, runs ...Record) {
	for _, r := range runs {
		_ = e.cfg.Store.Renew(ctx, r.Saga, r.ID, r.Epoch, 0)
	}
}

// work is a run's process, its lease counted from claimed: it keeps the
// lease while the saga works on the run, and ends the work once the lease is
// lost, or has run out unrenewed.
func (e *Engine) work(p *grpcproc.Process[proto.Message], r Record, claimed time.Time) {
	ctx, cancel := context.WithCancelCause(p.Context())
	var renewing sync.WaitGroup
	renewing.Go(func() {
		every := e.cfg.Lease / 4
		// Given up at last, a quarter before the lease ends, unless renewed.
		last := claimed.Add(e.cfg.Lease - every)
		next := time.Now().Add(min(every, time.Until(last)/2))
		wake := time.NewTimer(time.Until(soonest(next, last)))
		defer wake.Stop()
		failing := false // since the last renewal that got through
		for {
			select {
			case <-ctx.Done():
				return
			case <-wake.C:
			}
			asked := time.Now()
			if !asked.Before(last) {
				cancel(ErrLost)
				return
			}
			rctx, done := context.WithDeadline(ctx, soonest(asked.Add(every), last))
			err := e.cfg.Store.Renew(rctx, r.Saga, r.ID, r.Epoch, e.cfg.Lease)
			done()
			switch {
			case err == nil:
				last, next, failing = asked.Add(e.cfg.Lease-every), asked.Add(every), false
			case errors.Is(err, ErrLost):
				cancel(ErrLost)
				return
			case ctx.Err() != nil:
				return // cut short by the end of the work, not failed
			default:
				if !failing {
					p.Log().Warn("saga: renew", "saga", r.Saga, "run", r.ID, "err", err)
				}
				failing, next = true, time.Now().Add(every/4)
			}
			wake.Reset(time.Until(soonest(next, last)))
		}
	})
	e.sagas[r.Saga].work(ctx, e, p, r)
	lost := errors.Is(context.Cause(ctx), ErrLost)
	cancel(nil)
	renewing.Wait()
	if p.Context().Err() != nil && !lost {
		// Told to exit with the run in hand.
		ctx, done := outlive(p.Context(), time.Second)
		defer done()
		e.letGo(ctx, r)
	}
}

// save writes r, which its caller owns, and takes the record as the store
// keeps it. A store that fails is asked again, a Poll apart, for as long as
// the run is the caller's. It reports false when it no longer is.
func (e *Engine) save(ctx context.Context, p *grpcproc.Process[proto.Message], r *Record) bool {
	for {
		kept, err := e.cfg.Store.Save(ctx, *r)
		if err == nil {
			*r = kept
			return true
		}
		if errors.Is(err, ErrLost) || ctx.Err() != nil {
			return false
		}
		p.Log().Warn("saga: save", "saga", r.Saga, "run", r.ID, "err", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(e.cfg.Poll):
		}
	}
}

// release saves r and lets the run go: it has no work until its Wake, or a
// signal.
func (e *Engine) release(ctx context.Context, p *grpcproc.Process[proto.Message], r *Record) {
	r.Owner, r.LeaseUntil = "", time.Time{}
	if !e.save(ctx, p, r) || r.Status != Active {
		return
	}
	switch {
	case r.unseen(): // a signal came meanwhile
		e.nudge()
	case !r.Wake.IsZero() && time.Until(r.Wake) < e.cfg.Poll: // sooner than the tick
		e.wakeAt(r.Wake)
	}
}

// soonest is the earlier of two times, of which zero is none.
func soonest(a, b time.Time) time.Time {
	if a.IsZero() || !b.IsZero() && b.Before(a) {
		return b
	}
	return a
}

// work takes a run its engine has claimed as far as it goes: through the
// effects of the states it enters, the signals its machine takes and its
// timer, until it waits, ends, is stuck, or is lost. It saves before each
// effect, so that what came before is kept, and when it lets the run go.
func (d *Definition[S, D]) work(ctx context.Context, e *Engine, p *grpcproc.Process[proto.Message], r Record) {
	r.SagaVersion = max(r.SagaVersion, d.version)
	unsaved := false
	for ctx.Err() == nil {
		if r.SagaVersion > d.version {
			// A newer program signalled it since it was claimed: the run is
			// that program's now, its signals with it.
			e.release(ctx, p, &r)
			return
		}
		now := time.Now()
		var again time.Time // when to look again at a signal whose taking failed
		st, data, err := d.decode(&r)
		if err != nil {
			// Its version says this program can read it, and it cannot.
			r.Status, r.Error, r.Wake = Stuck, err.Error(), time.Time{}
			e.release(ctx, p, &r)
			return
		}
		effect := d.effects[st]
		pending := effect != nil && !r.EffectDone
		// The effect of a visit runs before anything moves the run on from
		// it; once it has, a signal or the timer may come before its next
		// attempt.
		entered := pending && r.Attempts == 0
		switch {
		case d.ends[st]:
			r.Status, r.Wake, r.RetryAt, r.Deadline = Done, time.Time{}, time.Time{}, time.Time{}
			e.release(ctx, p, &r)
			return
		case !entered && d.takeSignal(ctx, e, p, &r, st, data, now, &again):
			unsaved = true
		case !entered && !r.Deadline.IsZero() && !r.Deadline.After(now):
			d.timeout(ctx, e, p, &r, st, now)
			unsaved = true
		case pending && !r.RetryAt.After(now):
			if unsaved { // and look again: the save brings the signals that came
				if !e.save(ctx, p, &r) {
					return
				}
				unsaved = false
				continue
			}
			if !d.run(ctx, e, p, &r, st, data, effect) {
				return
			}
			unsaved = true
		default: // it waits: for its next attempt, its timer, or a signal
			r.Wake = soonest(r.Deadline, again)
			if pending {
				r.Wake = soonest(r.Wake, r.RetryAt)
			}
			e.release(ctx, p, &r)
			return
		}
	}
}

// timeout fires the timer of st, which is due. It fires once: a machine
// that refuses it, having no such transition or by a guard, stays in st with
// no timer, and so does a state this program gives none, and a machine that
// takes it by an internal transition. One whose action fails, or panics,
// keeps the timer, and is asked again a Poll later.
func (d *Definition[S, D]) timeout(ctx context.Context, e *Engine, p *grpcproc.Process[proto.Message], r *Record, st S, now time.Time) {
	t, timed := d.timers[st]
	ok, err := false, error(nil)
	stays := timed && d.stays(st, t.ev)
	if timed {
		ok, err = safely(func() (bool, error) {
			_, ok, err := d.m.TrySend(ctx, &st, t.ev)
			return ok, err
		})
	}
	switch {
	case err != nil:
		p.Log().Warn("saga: timer", "saga", d.name, "run", r.ID, "err", err)
		r.Deadline = now.Add(e.cfg.Poll)
	case ok && !stays:
		d.enter(r, st, now)
	default:
		r.Deadline = time.Time{}
	}
}

// takeSignal gives the machine the first signal waiting that it takes in st,
// and reports whether the run's record changed. Signals the machine does not
// take now stay. One that cannot be delivered, of an event the saga does not
// accept, with a payload that does not decode, or whose taking panics, is
// dropped. One whose taking fails otherwise, by the machine's action, stays,
// with those behind it, and again is set to when it is next tried.
func (d *Definition[S, D]) takeSignal(ctx context.Context, e *Engine, p *grpcproc.Process[proto.Message], r *Record, st S, data D, now time.Time, again *time.Time) bool {
	for i, s := range r.Inbox {
		r.Seen = max(r.Seen, s.Seq)
		ok, stays, err := false, false, error(undeliverable{errors.New("not an event the saga accepts")})
		if t, known := d.signals[s.Event]; known {
			ok, err = safely(func() (took bool, err error) {
				took, stays, err = t(ctx, &st, data, s.Payload)
				return took, err
			})
		}
		var b []byte
		if err == nil && ok {
			if b, err = proto.Marshal(data); err != nil {
				err = undeliverable{err}
			}
		}
		if _, drop := errors.AsType[undeliverable](err); drop {
			// st and data may be half changed: the pass ends here, and the
			// next reads the record again.
			p.Log().Warn("saga: signal dropped", "saga", d.name, "run", r.ID, "event", s.Event, "err", err)
			r.consume(i)
			return true
		}
		if err != nil {
			// The machine's action failed, and changed nothing. Those behind
			// it wait for it: signals are taken in the order they came.
			p.Log().Warn("saga: signal", "saga", d.name, "run", r.ID, "event", s.Event, "err", err)
			r.Seen = r.Inbox[len(r.Inbox)-1].Seq
			*again = now.Add(e.cfg.Poll)
			return false
		}
		if !ok {
			continue
		}
		r.consume(i)
		r.Data = b
		if !stays { // an internal transition begins no visit
			d.enter(r, st, now)
		}
		return true
	}
	return false
}

// safely runs f, which calls into the machine and the saga's own functions,
// and makes an error of its panic, one that asking again will not get past.
func safely(f func() (bool, error)) (ok bool, err error) {
	defer func() {
		if v := recover(); v != nil {
			ok, err = false, undeliverable{fmt.Errorf("panic: %v", v)}
		}
	}()
	return f()
}

// run runs the effect of st once and writes what came of it to r. It
// reports false when the work on the run ends here: the run is stuck and was
// let go, or is no longer this engine's.
func (d *Definition[S, D]) run(ctx context.Context, e *Engine, p *grpcproc.Process[proto.Message], r *Record, st S, data D, effect Effect[S, D]) bool {
	run := &Run[S, D]{Data: data, def: d, rec: r, state: st, p: p}
	ectx := grpcproc.WithMetadata(ctx, grpcproc.Metadata{
		MetadataKey: keyOf(r), MetadataFence: strconv.FormatUint(r.Epoch, 10),
	})
	_, err := safely(func() (bool, error) { return true, effect(ectx, run) })
	if ctx.Err() != nil {
		return false // told to exit, or the run is another's: nothing is saved
	}
	now := time.Now()
	var b []byte
	if err == nil {
		if b, err = proto.Marshal(run.Data); err != nil {
			err = Permanent(fmt.Errorf("encode: %w", err))
		}
	}
	if err == nil {
		r.Data, r.Error, r.RetryAt = b, "", time.Time{}
		if run.moved {
			d.enter(r, run.state, now)
		} else {
			r.EffectDone = true
		}
		return true
	}

	s := d.steps[st]
	r.Attempts++
	r.Error = err.Error()
	_, forGood := errors.AsType[permanent](err)
	if !forGood && (s.attempts == 0 || r.Attempts < s.attempts) {
		r.RetryAt = now.Add(s.wait(r.Attempts))
		return true // it waits for that, unless a signal or its timer comes first
	}
	if s.otherwise != nil {
		stays := d.stays(st, *s.otherwise)
		if ok, ferr := safely(func() (bool, error) {
			_, ok, err := d.m.TryFire(ctx, &st, *s.otherwise, r.Error)
			return ok, err
		}); ferr == nil && ok {
			r.Cause = r.Error
			if stays { // an internal transition: the run waits in this visit
				r.EffectDone, r.Attempts, r.Error, r.RetryAt = true, 0, "", time.Time{}
			} else {
				d.enter(r, st, now)
			}
			return true
		}
	}
	r.Status, r.Wake, r.RetryAt = Stuck, time.Time{}, time.Time{}
	e.release(ctx, p, r)
	return false
}
