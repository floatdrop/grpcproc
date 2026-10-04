package saga

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/floatdrop/fsm"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
)

// Effect is what runs when a run enters a state. It fires the event that
// moves the run on, with Run.Fire, or returns nil without firing, and the run
// waits in the state for a signal or its timer. An error has it run again,
// after a backoff; what it wrote to Run.Data is then dropped.
//
// It runs at least once per visit: after a crash, or a lost lease, the next
// owner runs it again, with the same Run.Key. ctx ends when the run's process
// is told to exit or its engine loses the run, and carries the key and the
// fence as metadata, so what the effect sends with ctx carries them too.
type Effect[S comparable, D proto.Message] func(ctx context.Context, r *Run[S, D]) error

// Run is a run as its effect sees it.
type Run[S comparable, D proto.Message] struct {
	// Data is the run's data. What an effect writes to it is saved when the
	// effect returns nil, with the state it fired to.
	Data D

	def   *Definition[S, D]
	rec   *Record
	state S
	fired bool // the effect fired an event the machine took
	moved bool // and it was not an internal transition: the run enters a state
	p     *grpcproc.Process[proto.Message]
}

// ID is the run's, as Begin was given it.
func (r *Run[S, D]) ID() string { return r.rec.ID }

// State is the state the run is in.
func (r *Run[S, D]) State() S { return r.state }

// Attempt is which attempt of the effect this is in this visit, from 1.
func (r *Run[S, D]) Attempt() int { return r.rec.Attempts + 1 }

// Key names this visit to this state, the same for every attempt: the
// idempotency key of what the effect asks of others.
func (r *Run[S, D]) Key() string { return keyOf(r.rec) }

// Fence grows with each owner the run has. A participant that keeps the
// highest it has seen for a key can refuse an old owner's late request.
func (r *Run[S, D]) Fence() uint64 { return r.rec.Epoch }

// Process is the process the run is worked on by: the Caller of the effect's
// sends and calls.
func (r *Run[S, D]) Process() *grpcproc.Process[proto.Message] { return r.p }

// Fire moves the run on by ev, as the machine's Fire does. An effect fires
// once. An event the machine refuses in this state is a Permanent error:
// trying again will not change the machine. An internal transition (fsm's
// Stay) is taken without leaving the state: the run then waits in it, as if
// the effect had not fired.
func (r *Run[S, D]) Fire[A any](ctx context.Context, ev fsm.Event[A], arg A) error {
	if r.fired {
		return ErrFired
	}
	stays := r.def.stays(r.state, ev)
	if _, err := r.def.m.Fire(ctx, &r.state, ev, arg); err != nil {
		return Permanent(err)
	}
	r.fired, r.moved = true, !stays
	return nil
}

func keyOf(r *Record) string {
	return fmt.Sprintf("%s/%s/%s/%d", r.Saga, r.ID, r.State, r.Visit)
}

// StepOption says how a state's effect is tried.
type StepOption func(*step)

// step is a state's effect and how it is tried, without its types.
type step struct {
	attempts  int
	min, max  time.Duration
	otherwise *fsm.Event[string]
}

// Attempts bounds how many times an effect is run in one visit before it has
// failed for good. The default, 0, is no bound.
func Attempts(n int) StepOption { return func(s *step) { s.attempts = n } }

// Backoff is the wait before an effect that failed is run again: lo after
// the first failure, doubling to hi. The default is 100ms to a minute.
func Backoff(lo, hi time.Duration) StepOption { return func(s *step) { s.min, s.max = lo, hi } }

// Otherwise fires ev, with the error's text, when the effect has failed for
// good: it returned a Permanent error, or ran out of Attempts. With none, or
// if the machine refuses ev, the run is Stuck.
func Otherwise(ev fsm.Event[string]) StepOption { return func(s *step) { s.otherwise = &ev } }

// wait is how long to wait before attempt n+1, after n failures.
func (s *step) wait(n int) time.Duration {
	d := s.min
	for range min(n-1, 62) { // more doublings than a Duration holds
		d = min(2*d, s.max)
	}
	return d
}

// Definition is a saga: an fsm machine, the type of its runs' data, the
// effects of its states, their timers, and the events it takes from outside.
// Build it with Define and its methods, then give it to Start. Its other
// methods begin runs and ask about them, through an Engine.
type Definition[S comparable, D proto.Message] struct {
	name    string
	version uint64
	m       *fsm.Machine[S]
	states  map[string]S
	names   map[S]string
	ends    map[S]bool
	initial S

	effects map[S]Effect[S, D]
	steps   map[S]*step
	timers  map[S]timer
	signals map[string]take[S, D]
	errs    []error
}

type timer struct {
	after time.Duration
	ev    fsm.Event[fsm.Unit]
}

// take applies a signal's payload to a run: whether the machine took it, and
// whether it stays in its state for it, by an internal transition.
type take[S comparable, D proto.Message] func(ctx context.Context, st *S, data D, payload []byte) (took, stays bool, err error)

// Define begins a saga named name, whose runs are states of m with a D
// beside them. m must have an Initial state, where a run begins, and its
// states must print differently, since a run's state is kept as fmt prints
// it. Mistakes here and in the methods are reported by Start.
func Define[D proto.Message, S comparable](name string, m *fsm.Machine[S]) *Definition[S, D] {
	d := &Definition[S, D]{
		name: name, m: m,
		states: map[string]S{}, names: map[S]string{}, ends: map[S]bool{},
		effects: map[S]Effect[S, D]{}, steps: map[S]*step{}, timers: map[S]timer{}, signals: map[string]take[S, D]{},
	}
	if name == "" {
		d.errs = append(d.errs, errors.New("saga: a saga needs a name"))
	}
	initial, ok := m.Initial()
	if !ok {
		d.errs = append(d.errs, fmt.Errorf("saga %s: machine %s has no Initial state", name, m.Name()))
	}
	d.initial = initial
	for _, s := range m.States() {
		n := fmt.Sprint(s)
		if _, dup := d.states[n]; dup {
			d.errs = append(d.errs, fmt.Errorf("saga %s: two states print as %q", name, n))
		}
		d.states[n], d.names[s] = s, n
	}
	for _, s := range m.Terminals() {
		d.ends[s] = true
	}
	return d
}

// known reports a state of the machine that is not one of its ends, or
// records why it is not.
func (d *Definition[S, D]) known(what string, s S) bool {
	_, ok := d.names[s]
	switch {
	case !ok:
		d.errs = append(d.errs, fmt.Errorf("saga %s: %s for %v, which is not a state of the machine", d.name, what, s))
	case d.ends[s]:
		d.errs = append(d.errs, fmt.Errorf("saga %s: %s for %v, which ends the run", d.name, what, s))
	default:
		return true
	}
	return false
}

// Version says which version of the saga this program has; 0 if it never
// says. A run keeps the highest version that began, signalled or worked on
// it, and an engine works only on runs its version reaches. So raise it when
// the machine gains a state, an event or a timer: in a rolling deploy, the
// old program then leaves alone the runs the new one has touched, which it
// could not read. Runs of an older version are taken up as they are.
func (d *Definition[S, D]) Version(v uint64) *Definition[S, D] {
	d.version = v
	return d
}

// Do gives state its effect: what runs when a run enters it. It runs before
// any signal or timer moves the run on from this visit.
func (d *Definition[S, D]) Do(state S, effect Effect[S, D], opts ...StepOption) *Definition[S, D] {
	if !d.known("an effect", state) {
		return d
	}
	if _, dup := d.effects[state]; dup {
		d.errs = append(d.errs, fmt.Errorf("saga %s: two effects for %v", d.name, state))
	}
	st := &step{min: 100 * time.Millisecond, max: time.Minute}
	for _, opt := range opts {
		opt(st)
	}
	if st.min <= 0 || st.max < st.min || st.attempts < 0 {
		d.errs = append(d.errs, fmt.Errorf("saga %s: %v: Backoff needs 0 < lo <= hi, Attempts n >= 0", d.name, state))
	}
	d.effects[state], d.steps[state] = effect, st
	return d
}

// After fires ev on a run that is still in the same visit to state once
// after has passed since it entered it. The time is kept in the run's record,
// so it outlasts the engine that set it.
func (d *Definition[S, D]) After(state S, after time.Duration, ev fsm.Event[fsm.Unit]) *Definition[S, D] {
	if !d.known("a timer", state) {
		return d
	}
	if _, dup := d.timers[state]; dup || after <= 0 {
		d.errs = append(d.errs, fmt.Errorf("saga %s: %v: one timer a state, of a positive duration", d.name, state))
	}
	d.timers[state] = timer{after, ev}
	return d
}

// Accept lets ev reach a run from outside, by Signal. apply, unless nil,
// writes the payload to the run's data when the machine takes the event. A
// state that takes it by an internal transition (fsm's Stay) keeps its visit:
// its effect is not run again, and its timer stands.
func (d *Definition[S, D]) Accept[P proto.Message](ev fsm.Event[P], apply func(data D, p P)) *Definition[S, D] {
	d.accept(ev.Name(), func(ctx context.Context, st *S, data D, payload []byte) (bool, bool, error) {
		p := newMessage[P]()
		if err := proto.Unmarshal(payload, p); err != nil {
			return false, false, undeliverable{err}
		}
		stays := d.stays(*st, ev)
		_, ok, err := d.m.TryFire(ctx, st, ev, p)
		if ok && apply != nil {
			apply(data, p)
		}
		return ok, stays, err
	})
	return d
}

// AcceptSignal is Accept for an event with no payload, sent by Notify.
func (d *Definition[S, D]) AcceptSignal(ev fsm.Event[fsm.Unit]) *Definition[S, D] {
	d.accept(ev.Name(), func(ctx context.Context, st *S, _ D, _ []byte) (bool, bool, error) {
		stays := d.stays(*st, ev)
		_, ok, err := d.m.TrySend(ctx, st, ev)
		return ok, stays, err
	})
	return d
}

// stays reports whether the machine takes ev in from by an internal
// transition: one that does not leave the state, so begins no visit to it.
func (d *Definition[S, D]) stays[A any](from S, ev fsm.Event[A]) bool {
	return slices.ContainsFunc(d.m.Edges(), func(e fsm.Edge[S]) bool {
		return e.Internal && e.From == from && e.Is(ev)
	})
}

func (d *Definition[S, D]) accept(name string, t take[S, D]) {
	if _, dup := d.signals[name]; dup {
		d.errs = append(d.errs, fmt.Errorf("saga %s: event %q accepted twice", d.name, name))
	}
	d.signals[name] = t
}

// undeliverable marks an error that asking again will not get past: a
// signal's payload that does not decode, a panic in what takes it.
type undeliverable struct{ error }

// newMessage returns an empty M: M is a pointer to a generated message.
func newMessage[M proto.Message]() M {
	var zero M
	return zero.ProtoReflect().New().Interface().(M)
}

func (d *Definition[S, D]) sagaName() string { return d.name }

func (d *Definition[S, D]) sagaVersion() uint64 { return d.version }

func (d *Definition[S, D]) validate() error { return errors.Join(d.errs...) }

// Snapshot is a run as Get and Wait report it.
type Snapshot[S comparable, D proto.Message] struct {
	ID       string
	State    S
	Data     D
	Status   Status
	Attempts int    // failures of the state's effect in this visit
	Error    string // the last, or why the run is Stuck
	Cause    string // the error an Otherwise last fired with
	Updated  time.Time
}

// Begin starts a run with id and data, in the machine's Initial state, and
// reports whether it did: a saga has one run of an id, so beginning it again
// is no error and changes nothing. id is the run's idempotency key for
// whoever begins it.
func (d *Definition[S, D]) Begin(ctx context.Context, e *Engine, id string, data D) (bool, error) {
	if id == "" {
		return false, errors.New("saga: a run needs an id")
	}
	b, err := proto.Marshal(data)
	if err != nil {
		return false, fmt.Errorf("saga %s: encode: %w", d.name, err)
	}
	now := time.Now()
	r := Record{Saga: d.name, ID: id, SagaVersion: d.version, Data: b, Wake: now}
	d.enter(&r, d.initial, now)
	_, created, err := e.cfg.Store.Create(ctx, r)
	if err != nil {
		return false, err
	}
	e.nudge()
	return created, nil
}

// Signal sends ev with its payload to a run. It is kept with the run, and
// its machine takes it once it is in a state that accepts it, after those
// sent before it. ev must be one the saga Accepts.
func (d *Definition[S, D]) Signal[P proto.Message](ctx context.Context, e *Engine, id string, ev fsm.Event[P], p P) error {
	b, err := proto.Marshal(p)
	if err != nil {
		return fmt.Errorf("saga %s: encode: %w", d.name, err)
	}
	return d.signal(ctx, e, id, Signal{Event: ev.Name(), Payload: b, Version: d.version})
}

// Notify sends ev, an event with no payload, to a run, as Signal does.
func (d *Definition[S, D]) Notify(ctx context.Context, e *Engine, id string, ev fsm.Event[fsm.Unit]) error {
	return d.signal(ctx, e, id, Signal{Event: ev.Name(), Version: d.version})
}

func (d *Definition[S, D]) signal(ctx context.Context, e *Engine, id string, s Signal) error {
	if _, ok := d.signals[s.Event]; !ok {
		return fmt.Errorf("saga %s: event %q is not one it accepts", d.name, s.Event)
	}
	if err := e.cfg.Store.Signal(ctx, d.name, id, s); err != nil {
		return err
	}
	e.nudge()
	return nil
}

// Resume has a Stuck run tried again, from the state it is in.
func (d *Definition[S, D]) Resume(ctx context.Context, e *Engine, id string) error {
	if err := e.cfg.Store.Resume(ctx, d.name, id); err != nil {
		return err
	}
	e.nudge()
	return nil
}

// Get returns a run as it stands; ErrNoRun if there is none.
func (d *Definition[S, D]) Get(ctx context.Context, e *Engine, id string) (Snapshot[S, D], error) {
	r, ok, err := e.cfg.Store.Get(ctx, d.name, id)
	if err != nil {
		return Snapshot[S, D]{}, err
	}
	if !ok {
		return Snapshot[S, D]{}, ErrNoRun
	}
	st, data, err := d.decode(&r)
	if err != nil {
		return Snapshot[S, D]{}, err
	}
	return Snapshot[S, D]{ID: r.ID, State: st, Data: data, Status: r.Status, Attempts: r.Attempts, Error: r.Error, Cause: r.Cause, Updated: r.UpdatedAt}, nil
}

// Wait returns a run once it is Done or Stuck, asking the store every
// Config.Poll until then, or until ctx ends. Like Get, it fails for a run
// whose record this program cannot read: one a later Version wrote.
func (d *Definition[S, D]) Wait(ctx context.Context, e *Engine, id string) (Snapshot[S, D], error) {
	for {
		s, err := d.Get(ctx, e, id)
		if err != nil || s.Status != Active {
			return s, err
		}
		select {
		case <-ctx.Done():
			return s, ctx.Err()
		case <-time.After(e.cfg.Poll):
		}
	}
}

// decode reads a record's state and data.
func (d *Definition[S, D]) decode(r *Record) (S, D, error) {
	st, ok := d.states[r.State]
	data := newMessage[D]()
	if !ok {
		return st, data, fmt.Errorf("saga %s: run %s is in state %q, which the machine does not have", d.name, r.ID, r.State)
	}
	if err := proto.Unmarshal(r.Data, data); err != nil {
		return st, data, fmt.Errorf("saga %s: run %s: decode: %w", d.name, r.ID, err)
	}
	return st, data, nil
}

// enter puts a run in st: a new visit, its effect to run, its timer set.
func (d *Definition[S, D]) enter(r *Record, st S, now time.Time) {
	r.State = d.names[st]
	r.Visit++
	r.EffectDone, r.Attempts, r.Error = false, 0, ""
	r.Wake, r.RetryAt, r.Deadline = now, time.Time{}, time.Time{}
	if t, ok := d.timers[st]; ok {
		r.Deadline = now.Add(t.after)
	}
}
