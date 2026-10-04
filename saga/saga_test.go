package saga_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/fsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/saga"
)

// The states and events of the tests' machines.
type state string

const (
	first    state = "first"
	second   state = "second"
	done     state = "done"
	failed   state = "failed"
	timedOut state = "timed out"
)

var (
	evNext    = fsm.Signal("next")
	evGo      = fsm.Signal("go")
	evTimeout = fsm.Signal("timeout")
	evFailed  = fsm.Define[string]("failed")
	evPaid    = fsm.Define[*wrapperspb.StringValue]("paid")
)

// text is the data of the tests' runs: what happened to them, in order.
type text = *wrapperspb.StringValue

type run = saga.Run[state, text]

// twoSteps is first -> second -> done, with a way from each to failed and
// from second to timed out.
func twoSteps() *fsm.Machine[state] {
	return fsm.MustNew("two steps",
		fsm.Initial(first),
		fsm.From(first).On(evNext).To(second),
		fsm.From(second).On(evNext).To(done),
		fsm.From(second).On(evGo).To(done),
		fsm.From(second).On(evPaid).To(done),
		fsm.From(first).On(evFailed).To(failed),
		fsm.From(second).On(evTimeout).To(timedOut),
	)
}

// step is an effect that notes its name in the run's data and fires next.
func step(name string) saga.Effect[state, text] {
	return func(ctx context.Context, r *run) error {
		r.Data.Value += name
		return r.Fire(ctx, evNext, fsm.Unit{})
	}
}

// start runs sagas on node of a new cluster, from a new memory store.
func start(t *testing.T, sagas ...saga.Saga) (*saga.Engine, saga.Store) {
	t.Helper()
	store := saga.Memory()
	return engine(t, grpcproctest.New(t, "a").Node("a"), store, sagas...), store
}

func engine(t *testing.T, n *grpcproc.Node, store saga.Store, sagas ...saga.Saga) *saga.Engine {
	t.Helper()
	e, err := saga.Start(n, saga.Config{Store: store}, sagas...)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func begin(t *testing.T, d *saga.Definition[state, text], e *saga.Engine, id string) {
	t.Helper()
	if created, err := d.Begin(t.Context(), e, id, wrapperspb.String("")); err != nil || !created {
		t.Fatalf("begin %s: %v %v", id, created, err)
	}
}

func wait(t *testing.T, d *saga.Definition[state, text], e *saga.Engine, id string) saga.Snapshot[state, text] {
	t.Helper()
	s, err := d.Wait(t.Context(), e, id)
	if err != nil {
		t.Fatalf("wait for %s: %v", id, err)
	}
	return s
}

func TestARunGoesThroughItsStates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		// A participant on another node, which notes the key and the fence
		// each request came with.
		var mu sync.Mutex
		var keys []string
		var fences []uint64
		_, err := c.Node("b").Spawn(func(p *grpcproc.Process[proto.Message]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				key, fence, ok := saga.KeyOf(m.Metadata)
				if !ok {
					t.Errorf("no key in %v", m.Metadata)
				}
				mu.Lock()
				keys, fences = append(keys, key), append(fences, fence)
				mu.Unlock()
				_ = m.Reply(&emptypb.Empty{}, nil)
			}
		}, grpcproc.WithName("stock"))
		if err != nil {
			t.Fatal(err)
		}
		ask := func(name string) saga.Effect[state, text] {
			return func(ctx context.Context, r *run) error {
				if r.ID() != "1" || r.Attempt() != 1 || r.Fence() != 1 || string(r.State()) != name {
					t.Errorf("run: %s %d %d %s", r.ID(), r.Attempt(), r.Fence(), r.State())
				}
				if _, err := r.Process().CallTo[*emptypb.Empty](ctx, grpcproc.Name{Node: "b", Name: "stock"}, &emptypb.Empty{}); err != nil {
					return err
				}
				if got := r.Key(); got != "orders/1/"+name+"/"+map[string]string{"first": "1", "second": "2"}[name] {
					t.Errorf("key %q", got)
				}
				return step(name+" ")(ctx, r)
			}
		}
		d := saga.Define[text]("orders", twoSteps()).Do(first, ask("first")).Do(second, ask("second"))
		e := engine(t, c.Node("a"), saga.Memory(), d)
		if e.PID().Node != "a" {
			t.Fatalf("engine %v", e.PID())
		}

		begin(t, d, e, "1")
		if created, err := d.Begin(t.Context(), e, "1", wrapperspb.String("again")); err != nil || created {
			t.Fatalf("begun twice: %v %v", created, err)
		}
		s := wait(t, d, e, "1")
		if s.State != done || s.Status != saga.Done || s.Data.Value != "first second " || s.ID != "1" || s.Updated.IsZero() {
			t.Fatalf("%+v", s)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(keys) != 2 || keys[0] != "orders/1/first/1" || keys[1] != "orders/1/second/2" || fences[0] != 1 || fences[1] != 1 {
			t.Fatalf("keys %v, fences %v", keys, fences)
		}
	})
}

func TestAnEffectThatFailsIsRunAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var tries atomic.Int32
		d := saga.Define[text]("retry", twoSteps()).
			Do(first, func(ctx context.Context, r *run) error {
				r.Data.Value += "dropped "
				switch tries.Add(1) {
				case 1:
					return errors.New("busy")
				case 2:
					panic("boom")
				}
				if r.Attempt() != 3 || r.Data.Value != "dropped " {
					t.Errorf("attempt %d, data %q", r.Attempt(), r.Data.Value)
				}
				return r.Fire(ctx, evNext, fsm.Unit{})
			}).
			Do(second, step("second"))
		e, _ := start(t, d)
		began := time.Now()
		begin(t, d, e, "1")
		synctest.Wait()
		if s, err := d.Get(t.Context(), e, "1"); err != nil || s.Attempts != 1 || s.Error != "busy" || s.Status != saga.Active || s.State != first {
			t.Fatalf("after one failure: %+v %v", s, err)
		}
		time.Sleep(150 * time.Millisecond)
		synctest.Wait()
		if s, _ := d.Get(t.Context(), e, "1"); s.Attempts != 2 || s.Error != "panic: boom" {
			t.Fatalf("after a panic: %+v", s)
		}
		s := wait(t, d, e, "1")
		// The waits were 100ms and 200ms; what a failed attempt wrote is gone.
		if s.Status != saga.Done || s.Data.Value != "dropped second" || s.Error != "" || time.Since(began) < 300*time.Millisecond {
			t.Fatalf("%+v after %v", s, time.Since(began))
		}
	})
}

func TestAnEffectThatFailsForGood(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var tries atomic.Int32
		boom := func(context.Context, *run) error { tries.Add(1); return errors.New("boom") }
		// With an Otherwise the machine takes, the run goes that way.
		bounded := saga.Define[text]("bounded", twoSteps()).
			Do(first, boom, saga.Attempts(3), saga.Backoff(time.Second, 2*time.Second), saga.Otherwise(evFailed))
		// With one it refuses, or none, the run is stuck.
		refused := saga.Define[text]("refused", twoSteps()).
			Do(first, step("first")).
			Do(second, func(context.Context, *run) error { return saga.Permanent(errors.New("no")) }, saga.Otherwise(evFailed))
		var fixed atomic.Bool
		stuck := saga.Define[text]("stuck", twoSteps()).
			Do(first, func(ctx context.Context, r *run) error {
				if !fixed.Load() {
					return saga.Permanent(errors.New("broken"))
				}
				return step("first")(ctx, r)
			}).
			Do(second, step("second"))
		e, _ := start(t, bounded, refused, stuck)

		began := time.Now()
		begin(t, bounded, e, "1")
		s := wait(t, bounded, e, "1")
		if s.State != failed || s.Status != saga.Done || s.Cause != "boom" || s.Error != "" || tries.Load() != 3 {
			t.Fatalf("%+v after %d tries", s, tries.Load())
		}
		if took := time.Since(began); took < 3*time.Second || took > 5*time.Second { // 1s, then 2s
			t.Fatalf("took %v", took)
		}

		begin(t, refused, e, "1")
		if s := wait(t, refused, e, "1"); s.State != second || s.Status != saga.Stuck || s.Error != "no" {
			t.Fatalf("%+v", s)
		}

		begin(t, stuck, e, "1")
		if s := wait(t, stuck, e, "1"); s.Status != saga.Stuck || s.Error != "broken" || s.Attempts != 1 {
			t.Fatalf("%+v", s)
		}
		if !errors.Is(errors.Unwrap(saga.Permanent(context.Canceled)), context.Canceled) {
			t.Fatal("Permanent does not unwrap")
		}
		fixed.Store(true)
		if err := stuck.Resume(t.Context(), e, "1"); err != nil {
			t.Fatal(err)
		}
		if s := wait(t, stuck, e, "1"); s.Status != saga.Done || s.Data.Value != "firstsecond" {
			t.Fatalf("after resume: %+v", s)
		}
	})
}

func TestFire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		twice := saga.Define[text]("twice", twoSteps()).Do(first, func(ctx context.Context, r *run) error {
			if err := r.Fire(ctx, evNext, fsm.Unit{}); err != nil {
				return err
			}
			return saga.Permanent(r.Fire(ctx, evNext, fsm.Unit{}))
		})
		refused := saga.Define[text]("refused", twoSteps()).Do(first, func(ctx context.Context, r *run) error {
			return r.Fire(ctx, evGo, fsm.Unit{}) // not an event of first
		})
		e, _ := start(t, twice, refused)
		begin(t, twice, e, "1")
		if s := wait(t, twice, e, "1"); s.Status != saga.Stuck || s.State != first || s.Error != saga.ErrFired.Error() {
			t.Fatalf("%+v", s)
		}
		begin(t, refused, e, "1")
		if s := wait(t, refused, e, "1"); s.Status != saga.Stuck || !strings.Contains(s.Error, "go") {
			t.Fatalf("%+v", s)
		}
	})
}

func TestSignals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var entered atomic.Int32
		d := saga.Define[text]("signals", twoSteps()).
			// first waits once its effect has run: for a next that comes from outside.
			Do(first, func(context.Context, *run) error { entered.Add(1); return nil }).
			AcceptSignal(evNext).
			AcceptSignal(evGo).
			Accept(evPaid, func(data text, p *wrapperspb.StringValue) { data.Value += "paid " + p.Value })
		e, store := start(t, d)
		ctx := t.Context()

		begin(t, d, e, "1")
		// These come early: first takes neither, and they are kept.
		if err := d.Signal(ctx, e, "1", evPaid, wrapperspb.String("42")); err != nil {
			t.Fatal(err)
		}
		// Two the saga cannot read, as another version of it might leave.
		for _, s := range []saga.Signal{{Event: "bogus"}, {Event: "paid", Payload: []byte{0xff}}} {
			if err := store.Signal(ctx, "signals", "1", s); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if s, _ := d.Get(ctx, e, "1"); s.State != first || s.Status != saga.Active {
			t.Fatalf("%+v", s)
		}
		// The payment waits; the two that cannot be delivered are dropped.
		if r, _, _ := store.Get(ctx, "signals", "1"); len(r.Inbox) != 1 || !r.EffectDone || !r.Wake.IsZero() || r.Owner != "" {
			t.Fatalf("waiting: %+v", r)
		}
		// next moves it to second, which takes the payment that waited.
		if err := d.Notify(ctx, e, "1", evNext); err != nil {
			t.Fatal(err)
		}
		s := wait(t, d, e, "1")
		if s.State != done || s.Data.Value != "paid 42" || entered.Load() != 1 {
			t.Fatalf("%+v, entered %d times", s, entered.Load())
		}

		if err := d.Notify(ctx, e, "1", evGo); !errors.Is(err, saga.ErrEnded) {
			t.Fatalf("signal to a run that ended: %v", err)
		}
		if err := d.Notify(ctx, e, "none", evGo); !errors.Is(err, saga.ErrNoRun) {
			t.Fatalf("signal to no run: %v", err)
		}
		if err := d.Notify(ctx, e, "1", evTimeout); err == nil {
			t.Fatal("sent an event the saga does not accept")
		}
		if err := d.Signal(ctx, e, "1", evPaid, wrapperspb.String("\xff")); err == nil {
			t.Fatal("sent a payload that does not encode")
		}
	})
}

func TestTimers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// second waits an hour for a go, and then times out.
		waits := saga.Define[text]("waits", twoSteps()).
			Do(first, step("first")).
			After(second, time.Hour, evTimeout).
			AcceptSignal(evGo)
		// first has a timer its machine does not take: it is spent, and the run stays.
		spent := saga.Define[text]("spent", twoSteps()).
			After(first, time.Minute, evTimeout).
			AcceptSignal(evNext)
		// second fails for ever: its timer fires between two attempts.
		failing := saga.Define[text]("failing", twoSteps()).
			Do(first, step("first")).
			Do(second, func(context.Context, *run) error { return errors.New("down") }).
			After(second, 250*time.Millisecond, evTimeout)
		e, store := start(t, waits, spent, failing)
		ctx := t.Context()

		began := time.Now()
		begin(t, waits, e, "late")
		begin(t, waits, e, "in time")
		synctest.Wait()
		if err := waits.Notify(ctx, e, "in time", evGo); err != nil {
			t.Fatal(err)
		}
		if s := wait(t, waits, e, "in time"); s.State != done || time.Since(began) > time.Minute {
			t.Fatalf("%+v after %v", s, time.Since(began))
		}
		if s := wait(t, waits, e, "late"); s.State != timedOut || time.Since(began) < time.Hour {
			t.Fatalf("%+v after %v", s, time.Since(began))
		}

		begin(t, spent, e, "1")
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if r, _, _ := store.Get(ctx, "spent", "1"); r.State != "first" || !r.Deadline.IsZero() || !r.Wake.IsZero() || r.Status != saga.Active {
			t.Fatalf("after its timer: %+v", r)
		}
		if err := spent.Notify(ctx, e, "1", evNext); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if s, _ := spent.Get(ctx, e, "1"); s.State != second {
			t.Fatalf("%+v", s)
		}

		began = time.Now()
		begin(t, failing, e, "1")
		if s := wait(t, failing, e, "1"); s.State != timedOut || s.Updated.Sub(began) != 250*time.Millisecond {
			t.Fatalf("%+v, %v after it began", s, s.Updated.Sub(began))
		}
	})
}

func TestStatusAndKeyOf(t *testing.T) {
	for s, want := range map[saga.Status]string{saga.Active: "active", saga.Done: "done", saga.Stuck: "stuck", 9: "status(9)"} {
		if s.String() != want {
			t.Errorf("%d prints as %q", s, s)
		}
	}
	if key, fence, ok := saga.KeyOf(grpcproc.Metadata{saga.MetadataKey: "k", saga.MetadataFence: "7"}); !ok || key != "k" || fence != 7 {
		t.Fatalf("%q %d %v", key, fence, ok)
	}
	if _, _, ok := saga.KeyOf(grpcproc.Metadata{saga.MetadataKey: "k", saga.MetadataFence: "x"}); ok {
		t.Fatal("a fence that is no number")
	}
	if _, _, ok := saga.KeyOf(nil); ok {
		t.Fatal("no key")
	}
}

// An internal transition takes its event without a new visit to the state:
// the effect is not run again, its key is not changed, and the timer stands.
func TestInternalTransitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		evNote := fsm.Define[*wrapperspb.StringValue]("note")
		evPing := fsm.Signal("ping")
		m := fsm.MustNew("stays",
			fsm.Initial(first),
			fsm.From(first).On(evNote).Stay(),
			fsm.From(first).On(evPing).Stay(),
			fsm.From(first).On(evTimeout).To(timedOut),
			fsm.From(first).On(evGo).To(second),
			fsm.From(second).On(evTimeout).Stay(),
			fsm.From(second).On(evNext).To(done).Guard("never", func(context.Context, fsm.Unit) error { return errors.New("not yet") }),
		)
		var entered atomic.Int32
		d := saga.Define[text]("stays", m).
			Do(first, func(ctx context.Context, r *run) error {
				entered.Add(1)
				return r.Fire(ctx, evPing, fsm.Unit{}) // it stays: the run waits here
			}).
			After(first, time.Minute, evTimeout).
			After(second, time.Minute, evTimeout).
			Accept(evNote, func(data text, p *wrapperspb.StringValue) { data.Value += p.Value }).
			AcceptSignal(evGo)
		e, store := start(t, d)
		ctx := t.Context()
		began := time.Now()

		begin(t, d, e, "noted")
		for _, note := range []string{"a", "b"} {
			time.Sleep(20 * time.Second)
			if err := d.Signal(ctx, e, "noted", evNote, wrapperspb.String(note)); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		r, _, _ := store.Get(ctx, "stays", "noted")
		if entered.Load() != 1 || r.Visit != 1 || !r.EffectDone || !r.Deadline.Equal(began.Add(time.Minute)) || len(r.Inbox) != 0 {
			t.Fatalf("entered %d times: %+v", entered.Load(), r)
		}
		// The timer, set when the run entered, still fires on time.
		if s := wait(t, d, e, "noted"); s.State != timedOut || s.Data.Value != "ab" || s.Updated.Sub(began) != time.Minute {
			t.Fatalf("%+v, %v after it began", s, s.Updated.Sub(began))
		}

		// A timer taken by an internal transition, or refused by a guard, is spent.
		begin(t, d, e, "second")
		if err := d.Notify(ctx, e, "second", evGo); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if r, _, _ := store.Get(ctx, "stays", "second"); r.State != "second" || r.Visit != 2 || !r.Deadline.IsZero() || !r.Wake.IsZero() {
			t.Fatalf("after its timer: %+v", r)
		}
	})
}
