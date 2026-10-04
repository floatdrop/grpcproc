package saga_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/fsm"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/saga"
)

// flaky is a store whose methods fail while their name is in failing.
type flaky struct {
	saga.Store
	mu      sync.Mutex
	failing map[string]error
	// onRelease, if set, runs once, just before a save that lets a run go.
	onRelease func()
	// hang, if set, holds every Renew until it is closed, or its ctx ends.
	hang chan struct{}
	// slow, if set, is how long every Renew takes, unless its ctx ends first.
	slow time.Duration
	// late, if set, is how long after the store claimed runs Claim answers.
	late time.Duration
	// behind, if set, is how far the leases Claim reports lag, as on a store
	// whose clock is behind.
	behind time.Duration
}

func (f *flaky) fail(method string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing == nil {
		f.failing = map[string]error{}
	}
	if err == nil {
		delete(f.failing, method)
		return
	}
	f.failing[method] = err
}

func (f *flaky) err(method string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failing[method]
}

func (f *flaky) Create(ctx context.Context, r saga.Record) (saga.Record, bool, error) {
	if err := f.err("Create"); err != nil {
		return saga.Record{}, false, err
	}
	return f.Store.Create(ctx, r)
}

func (f *flaky) Claim(ctx context.Context, owner string, sagas map[string]uint64, ttl time.Duration, n int) ([]saga.Record, error) {
	if err := f.err("Claim"); err != nil {
		return nil, err
	}
	runs, err := f.Store.Claim(ctx, owner, sagas, ttl, n)
	for i := range runs {
		runs[i].LeaseUntil = runs[i].LeaseUntil.Add(-f.behind)
	}
	f.mu.Lock()
	late := f.late
	f.mu.Unlock()
	select {
	case <-time.After(late):
	case <-ctx.Done():
	}
	return runs, err
}

func (f *flaky) Save(ctx context.Context, r saga.Record) (saga.Record, error) {
	if err := f.err("Save"); err != nil {
		return saga.Record{}, err
	}
	f.mu.Lock()
	hook := f.onRelease
	if r.Owner == "" {
		f.onRelease = nil
	}
	f.mu.Unlock()
	if hook != nil && r.Owner == "" {
		hook()
	}
	return f.Store.Save(ctx, r)
}

func (f *flaky) Renew(ctx context.Context, name, id string, epoch uint64, ttl time.Duration) error {
	f.mu.Lock()
	hang := f.hang
	f.mu.Unlock()
	if hang != nil {
		select {
		case <-hang:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	slow := f.slow
	f.mu.Unlock()
	if slow > 0 {
		select {
		case <-time.After(slow):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := f.err("Renew"); err != nil {
		return err
	}
	return f.Store.Renew(ctx, name, id, epoch, ttl)
}

func (f *flaky) Signal(ctx context.Context, name, id string, s saga.Signal) error {
	if err := f.err("Signal"); err != nil {
		return err
	}
	return f.Store.Signal(ctx, name, id, s)
}

func (f *flaky) Resume(ctx context.Context, name, id string) error {
	if err := f.err("Resume"); err != nil {
		return err
	}
	return f.Store.Resume(ctx, name, id)
}

func (f *flaky) Get(ctx context.Context, name, id string) (saga.Record, bool, error) {
	if err := f.err("Get"); err != nil {
		return saga.Record{}, false, err
	}
	return f.Store.Get(ctx, name, id)
}

// held is an effect that notes each run of it, and holds the first until its
// ctx ends; later ones fire next.
type held struct {
	mu     sync.Mutex
	keys   []string
	fences []uint64
	causes []error
}

func (h *held) effect(ctx context.Context, r *run) error {
	h.mu.Lock()
	h.keys, h.fences = append(h.keys, r.Key()), append(h.fences, r.Fence())
	n := len(h.keys)
	h.mu.Unlock()
	if n == 1 {
		<-ctx.Done()
		h.mu.Lock()
		h.causes = append(h.causes, context.Cause(ctx))
		h.mu.Unlock()
		return ctx.Err()
	}
	return step("first")(ctx, r)
}

func (h *held) seen() ([]string, []uint64, []error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.keys, h.fences, h.causes
}

func heldSaga(h *held) *saga.Definition[state, text] {
	return saga.Define[text]("held", twoSteps()).Do(first, h.effect).Do(second, step("second"))
}

// An engine whose lease ran out, and was taken by another, has its effect's
// ctx ended, and saves nothing; the other runs the effect again, with the
// same key and a higher fence.
func TestALostLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		store := &flaky{Store: saga.Memory()}
		h := &held{}
		d := heldSaga(h)
		a := engine(t, c.Node("a"), store, d)
		begin(t, d, a, "1")
		synctest.Wait()

		// a cannot renew, as if cut off from the store; b starts, and takes
		// the run when a's lease ends.
		store.fail("Renew", errors.New("store unreachable"))
		began := time.Now()
		b := engine(t, c.Node("b"), store, d)
		s := wait(t, d, b, "1")
		if s.Status != saga.Done || s.Data.Value != "firstsecond" || time.Since(began) < 30*time.Second {
			t.Fatalf("%+v after %v", s, time.Since(began))
		}
		// a's effect ended with its lease, which it could not renew.
		keys, fences, causes := h.seen()
		if len(keys) != 2 || keys[0] != keys[1] || fences[0] != 1 || fences[1] != 2 || len(causes) != 1 || !errors.Is(causes[0], saga.ErrLost) {
			t.Fatalf("keys %v, fences %v, causes %v", keys, fences, causes)
		}
		store.fail("Renew", nil)
		time.Sleep(time.Minute)
		synctest.Wait()
		if s, _ := d.Get(t.Context(), a, "1"); s.Status != saga.Done || s.Data.Value != "firstsecond" {
			t.Fatalf("the old owner wrote: %+v", s)
		}
	})
}

// A node that stops lets its runs go, so another takes them at once; one
// that is killed leaves them to their leases.
func TestANodeThatStopsOrDies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		end     func(c *grpcproctest.Cluster)
		atLeast time.Duration
		atMost  time.Duration
	}{
		{"stops", func(c *grpcproctest.Cluster) { c.Stop("a") }, 0, 2 * time.Second},
		{"is killed", func(c *grpcproctest.Cluster) { c.Kill("a") }, 0, 31 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := grpcproctest.New(t, "a", "b")
				store := saga.Memory()
				h := &held{}
				d := heldSaga(h)
				a := engine(t, c.Node("a"), store, d)
				begin(t, d, a, "1")
				synctest.Wait()

				b := engine(t, c.Node("b"), store, d)
				began := time.Now()
				tc.end(c)
				s := wait(t, d, b, "1")
				if took := time.Since(began); s.Status != saga.Done || took < tc.atLeast || took > tc.atMost {
					t.Fatalf("%+v after %v", s, took)
				}
				if keys, fences, _ := h.seen(); len(keys) != 2 || keys[0] != keys[1] || fences[1] != 2 {
					t.Fatalf("keys %v, fences %v", keys, fences)
				}
			})
		})
	}
}

func TestAStoreThatFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &flaky{Store: saga.Memory()}
		down := errors.New("store down")
		d := saga.Define[text]("flaky", twoSteps()).Do(first, step("first")).Do(second, step("second")).AcceptSignal(evGo)
		e := engine(t, grpcproctest.New(t, "a").Node("a"), store, d)
		ctx := t.Context()

		for _, method := range []string{"Create", "Get", "Signal", "Resume"} {
			store.fail(method, down)
		}
		if _, err := d.Begin(ctx, e, "1", wrapperspb.String("")); !errors.Is(err, down) {
			t.Fatalf("begin: %v", err)
		}
		if _, err := d.Get(ctx, e, "1"); !errors.Is(err, down) {
			t.Fatalf("get: %v", err)
		}
		if err := d.Notify(ctx, e, "1", evGo); !errors.Is(err, down) {
			t.Fatalf("signal: %v", err)
		}
		if err := d.Resume(ctx, e, "1"); !errors.Is(err, down) {
			t.Fatalf("resume: %v", err)
		}
		for _, method := range []string{"Create", "Get", "Signal", "Resume"} {
			store.fail(method, nil)
		}
		if err := d.Resume(ctx, e, "none"); !errors.Is(err, saga.ErrNoRun) {
			t.Fatalf("resume of no run: %v", err)
		}
		if _, err := d.Get(ctx, e, "none"); !errors.Is(err, saga.ErrNoRun) {
			t.Fatalf("get of no run: %v", err)
		}

		// A run begun while the store refuses claims and saves goes on once
		// it takes them, a tick later, not a lease: a save is asked again.
		synctest.Wait() // no claim of the engine's is under way
		store.fail("Claim", down)
		began := time.Now()
		begin(t, d, e, "1")
		time.Sleep(5 * time.Second)
		store.fail("Claim", nil)
		store.fail("Save", down)
		time.Sleep(5 * time.Second)
		store.fail("Save", nil)
		s := wait(t, d, e, "1")
		if took := s.Updated.Sub(began); s.Status != saga.Done || s.Data.Value != "firstsecond" || took < 10*time.Second || took > 12*time.Second {
			t.Fatalf("%+v, %v after it began", s, took)
		}
	})
}

// A record its saga cannot read, though its version says it can, is stuck,
// with why, rather than dropped; a timer in a state the saga gives none is
// spent.
func TestARecordTheSagaCannotRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := saga.Define[text]("old", twoSteps()).Do(first, step("first")).AcceptSignal(evGo)
		e, store := start(t, d)
		ctx := t.Context()
		now := time.Now()
		for _, r := range []saga.Record{
			{Saga: "old", ID: "state", State: "removed", Wake: now},
			{Saga: "old", ID: "data", State: "first", Data: []byte{0xff}, Wake: now},
			{Saga: "old", ID: "timer", State: "second", Visit: 1, Wake: now, Deadline: now},
		} {
			if _, _, err := store.Create(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		for id, why := range map[string]string{"state": `state "removed"`, "data": "decode"} {
			r, _, _ := store.Get(ctx, "old", id)
			if r.Status != saga.Stuck || !strings.Contains(r.Error, why) || r.Owner != "" || r.Epoch != 1 {
				t.Errorf("%s: %+v", id, r)
			}
			if _, err := d.Get(ctx, e, id); err == nil || !strings.Contains(err.Error(), why) {
				t.Errorf("get %s: %v", id, err)
			}
		}
		if r, _, _ := store.Get(ctx, "old", "timer"); r.Status != saga.Active || !r.Deadline.IsZero() || !r.Wake.IsZero() || r.Owner != "" {
			t.Fatalf("a deadline with no timer: %+v", r)
		}
	})
}

// An engine works only on runs its saga's version reaches: an older program
// leaves alone what a newer one began, signalled or worked on.
func TestVersions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "old", "new")
		store := saga.Memory()
		ctx := t.Context()
		var ranOn []string
		var mu sync.Mutex
		def := func(node string, version uint64) *saga.Definition[state, text] {
			return saga.Define[text]("orders", twoSteps()).Version(version).
				Do(first, func(ctx context.Context, r *run) error {
					mu.Lock()
					ranOn = append(ranOn, r.ID()+" on "+node)
					mu.Unlock()
					return nil // it waits for a next
				}).
				AcceptSignal(evNext).AcceptSignal(evGo)
		}
		v1, v2 := def("old", 1), def("new", 2)
		old := engine(t, c.Node("old"), store, v1)

		// A run the old program began is the old engine's.
		begin(t, v1, old, "a")
		synctest.Wait()
		// The new program signals it: from then on it is not.
		newer, err := saga.Start(c.Node("new"), saga.Config{Store: store, Poll: time.Hour}, v2)
		if err != nil {
			t.Fatal(err)
		}
		if err := v2.Notify(ctx, newer, "a", evNext); err != nil {
			t.Fatal(err)
		}
		// And a run the new program begins never is.
		begin(t, v2, newer, "b")
		time.Sleep(5 * time.Second) // the old engine ticks; the new one was nudged
		synctest.Wait()

		mu.Lock()
		defer mu.Unlock()
		if len(ranOn) != 2 || ranOn[0] != "a on old" || ranOn[1] != "b on new" {
			t.Fatalf("ran %v", ranOn)
		}
		for id, want := range map[string]state{"a": second, "b": first} {
			r, _, _ := store.Get(ctx, "orders", id)
			if r.SagaVersion != 2 || r.State != string(want) || r.Owner != "" {
				t.Errorf("%s: %+v", id, r)
			}
		}
	})
}

// The effect of a visit runs before a signal that waited, or the timer,
// moves the run on.
func TestTheEffectRunsFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := fsm.MustNew("first things",
			fsm.Initial(first),
			fsm.From(first).On(evGo).To(second),
			fsm.From(second).On(evGo).To(done),
		)
		d := saga.Define[text]("first things", m).
			Do(first, func(_ context.Context, r *run) error { r.Data.Value += "first "; return nil }).
			Do(second, func(_ context.Context, r *run) error { r.Data.Value += "second "; return nil }).
			AcceptSignal(evGo)
		store := saga.Memory()
		ctx := t.Context()
		// Two signals wait before any engine has seen the run.
		n := grpcproctest.New(t, "a").Node("a")
		if _, _, err := store.Create(ctx, saga.Record{Saga: "first things", ID: "1", State: "first", Visit: 1, Wake: time.Now()}); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if err := store.Signal(ctx, "first things", "1", saga.Signal{Event: evGo.Name()}); err != nil {
				t.Fatal(err)
			}
		}
		e := engine(t, n, store, d)
		if s := wait(t, d, e, "1"); s.State != done || s.Data.Value != "first second " {
			t.Fatalf("%+v", s)
		}
	})
}

// A signal whose taking fails by the machine's action stays, and is tried
// again; the run is not held for it.
func TestASignalWhoseActionFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var fails atomic.Bool
		fails.Store(true)
		m := fsm.MustNew("acting",
			fsm.Initial(first),
			fsm.From(first).On(evGo).To(done).Action(func(context.Context, fsm.Unit) error {
				if fails.Load() {
					return errors.New("ledger down")
				}
				return nil
			}),
		)
		d := saga.Define[text]("acting", m).AcceptSignal(evGo)
		e, store := start(t, d)
		begin(t, d, e, "1")
		if err := d.Notify(t.Context(), e, "1", evGo); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if r, _, _ := store.Get(t.Context(), "acting", "1"); r.State != "first" || len(r.Inbox) != 1 || r.Owner != "" || r.Wake.IsZero() {
			t.Fatalf("while its action fails: %+v", r)
		}
		fails.Store(false)
		if s := wait(t, d, e, "1"); s.State != done {
			t.Fatalf("%+v", s)
		}
	})
}

// Data that does not encode fails the effect, or drops the signal, that
// wrote it.
func TestDataThatDoesNotEncode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := saga.Define[text]("bad", twoSteps()).
			Do(first, func(ctx context.Context, r *run) error {
				if r.ID() == "effect" {
					r.Data.Value = "\xff"
				}
				return r.Fire(ctx, evNext, fsm.Unit{})
			}).
			Accept(evPaid, func(data text, p *wrapperspb.StringValue) { data.Value = "\xff" }).
			AcceptSignal(evGo)
		e, _ := start(t, d)
		ctx := t.Context()

		begin(t, d, e, "effect")
		if s := wait(t, d, e, "effect"); s.Status != saga.Stuck || !strings.Contains(s.Error, "encode") || s.State != first {
			t.Fatalf("%+v", s)
		}
		if _, err := d.Begin(ctx, e, "begin", wrapperspb.String("\xff")); err == nil {
			t.Fatal("began with data that does not encode")
		}
		if _, err := d.Begin(ctx, e, "", wrapperspb.String("")); err == nil {
			t.Fatal("began with no id")
		}

		begin(t, d, e, "signal")
		if err := d.Signal(ctx, e, "signal", evPaid, wrapperspb.String("42")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if s, _ := d.Get(ctx, e, "signal"); s.State != second || s.Data.Value != "" { // dropped: it stays
			t.Fatalf("%+v", s)
		}
		if err := d.Notify(ctx, e, "signal", evGo); err != nil {
			t.Fatal(err)
		}
		if s := wait(t, d, e, "signal"); s.State != done {
			t.Fatalf("%+v", s)
		}
	})
}

func TestConcurrencyAndWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var running, most atomic.Int32
		d := saga.Define[text]("slow", twoSteps()).
			Do(first, func(ctx context.Context, r *run) error {
				most.Store(max(most.Load(), running.Add(1)))
				defer running.Add(-1)
				time.Sleep(time.Second)
				return r.Fire(ctx, evNext, fsm.Unit{})
			}).
			Do(second, step("second"))
		n := grpcproctest.New(t, "a").Node("a")
		e, err := saga.Start(n, saga.Config{Store: saga.Memory(), Concurrency: 2, Name: "sagas", Poll: 100 * time.Millisecond, Lease: time.Minute}, d)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := n.Whereis("sagas"); !ok {
			t.Fatal("the engine is not registered under its name")
		}
		for _, id := range []string{"1", "2", "3", "4"} {
			begin(t, d, e, id)
		}
		// A wait that gives up says where the run was.
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		if s, err := d.Wait(ctx, e, "4"); !errors.Is(err, context.DeadlineExceeded) || s.Status != saga.Active {
			t.Fatalf("%+v %v", s, err)
		}
		for _, id := range []string{"1", "2", "3", "4"} {
			if s := wait(t, d, e, id); s.Status != saga.Done {
				t.Fatalf("%s: %+v", id, s)
			}
		}
		if most.Load() != 2 {
			t.Fatalf("%d ran at once", most.Load())
		}
		// The processes of runs are labelled by their saga, and gone once it waits.
		synctest.Wait()
		for _, p := range n.Processes() {
			if strings.HasPrefix(p.Label, "saga:") {
				t.Fatalf("a run's process outlived its work: %+v", p)
			}
		}
	})
}

func TestStartRefuses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		ok := func() *saga.Definition[state, text] { return saga.Define[text]("ok", twoSteps()) }
		noInitial := fsm.MustNew("no initial", fsm.From(first).On(evNext).To(done))

		cases := map[string]struct {
			cfg   saga.Config
			sagas []saga.Saga
		}{
			"Config.Store is required":          {saga.Config{}, nil},
			"must not be negative":              {saga.Config{Store: saga.Memory(), Poll: -1}, nil},
			"Lease is at least a millisecond":   {saga.Config{Store: saga.Memory(), Lease: time.Microsecond}, nil},
			`two sagas named "ok"`:              {saga.Config{Store: saga.Memory()}, []saga.Saga{ok(), ok()}},
			"needs a name":                      {saga.Config{Store: saga.Memory()}, []saga.Saga{saga.Define[text]("", twoSteps())}},
			"has no Initial state":              {saga.Config{Store: saga.Memory()}, []saga.Saga{saga.Define[text]("x", noInitial)}},
			"which is not a state":              {saga.Config{Store: saga.Memory()}, []saga.Saga{ok().Do("nowhere", step(""))}},
			"an effect for done, which ends":    {saga.Config{Store: saga.Memory()}, []saga.Saga{ok().Do(done, step(""))}},
			"two effects for first":             {saga.Config{Store: saga.Memory()}, []saga.Saga{ok().Do(first, step("")).Do(first, step(""))}},
			"Backoff needs":                     {saga.Config{Store: saga.Memory()}, []saga.Saga{ok().Do(first, step(""), saga.Backoff(time.Second, time.Millisecond))}},
			"a timer for failed, which ends":    {saga.Config{Store: saga.Memory()}, []saga.Saga{ok().After(failed, time.Second, evTimeout)}},
			"one timer a state":                 {saga.Config{Store: saga.Memory()}, []saga.Saga{ok().After(second, time.Second, evTimeout).After(second, time.Second, evTimeout)}},
			"of a positive duration":            {saga.Config{Store: saga.Memory()}, []saga.Saga{ok().After(second, 0, evTimeout)}},
			`event "go" accepted twice`:         {saga.Config{Store: saga.Memory()}, []saga.Saga{ok().AcceptSignal(evGo).AcceptSignal(evGo)}},
			"two states print as":               {saga.Config{Store: saga.Memory()}, []saga.Saga{saga.Define[text]("x", printsAlike())}},
			"grpcproc: name already registered": {saga.Config{Store: saga.Memory(), Name: "taken"}, nil},
		}
		if _, err := n.Spawn(func(p *grpcproc.Process[text]) error { _, err := p.Receive(); return err }, grpcproc.WithName("taken")); err != nil {
			t.Fatal(err)
		}
		for want, tc := range cases {
			if _, err := saga.Start(n, tc.cfg, tc.sagas...); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("want %q, got %v", want, err)
			}
		}
	})
}

// alike is a state type two of whose values print the same.
type alike struct{ n int }

func (alike) String() string { return "same" }

func printsAlike() *fsm.Machine[alike] {
	return fsm.MustNew("alike", fsm.Initial(alike{1}), fsm.From(alike{1}).On(evNext).To(alike{2}))
}

// A signal that comes while an effect waits for its next attempt is looked
// at, and the effect is not run before its time.
func TestASignalDoesNotCutABackoffShort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var tries atomic.Int32
		d := saga.Define[text]("backoff", twoSteps()).
			Do(first, func(ctx context.Context, r *run) error {
				if tries.Add(1) == 1 {
					return errors.New("busy")
				}
				return r.Fire(ctx, evNext, fsm.Unit{})
			}, saga.Backoff(time.Minute, time.Minute)).
			AcceptSignal(evGo)
		e, store := start(t, d)
		began := time.Now()
		begin(t, d, e, "1")
		synctest.Wait()
		for range 3 { // first does not take a go: they wait
			if err := d.Notify(t.Context(), e, "1", evGo); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
			synctest.Wait()
		}
		r, _, _ := store.Get(t.Context(), "backoff", "1")
		if tries.Load() != 1 || r.Attempts != 1 || len(r.Inbox) != 3 || r.Seen != r.Inbox[2].Seq || !r.Wake.Equal(began.Add(time.Minute)) {
			t.Fatalf("%d tries, %+v", tries.Load(), r)
		}
		// Then the attempt, and second takes the first go.
		s := wait(t, d, e, "1")
		if s.State != done || tries.Load() != 2 || s.Updated.Sub(began) != time.Minute {
			t.Fatalf("%+v after %v, %d tries", s, s.Updated.Sub(began), tries.Load())
		}
	})
}

// What a saga's own functions do wrong outside an effect costs a signal or
// a tick, not the run.
func TestPanicsAndErrorsOutsideAnEffect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var guardFails atomic.Bool
		guardFails.Store(true)
		m := fsm.MustNew("guarded",
			fsm.Initial(first),
			fsm.From(first).On(evPaid).To(done),
			fsm.From(first).On(evGo).To(done),
			fsm.From(first).On(evTimeout).To(timedOut).Guard("the bank answers", func(context.Context, fsm.Unit) error {
				if guardFails.Load() {
					panic("the bank is down")
				}
				return nil
			}),
		)
		d := saga.Define[text]("guarded", m).
			Accept(evPaid, func(text, *wrapperspb.StringValue) { panic("apply") }).
			AcceptSignal(evGo).
			After(first, time.Minute, evTimeout)
		e, store := start(t, d)
		ctx := t.Context()

		begin(t, d, e, "signal")
		if err := d.Signal(ctx, e, "signal", evPaid, wrapperspb.String("1")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if r, _, _ := store.Get(ctx, "guarded", "signal"); r.State != "first" || len(r.Inbox) != 0 || r.Status != saga.Active || r.Owner != "" {
			t.Fatalf("after a signal whose apply panics: %+v", r)
		}

		// The timer's guard panics: the timer is kept, and tried again.
		began := time.Now()
		begin(t, d, e, "timer")
		time.Sleep(time.Minute + 3*time.Second)
		synctest.Wait()
		if r, _, _ := store.Get(ctx, "guarded", "timer"); r.State != "first" || r.Deadline.IsZero() || r.Owner != "" {
			t.Fatalf("after a timer whose guard panics: %+v", r)
		}
		guardFails.Store(false)
		if s := wait(t, d, e, "timer"); s.State != timedOut || s.Updated.Sub(began) > time.Minute+5*time.Second {
			t.Fatalf("%+v, %v after it began", s, s.Updated.Sub(began))
		}
	})
}

// A run whose effects take longer than a lease stays its engine's: the
// lease is renewed, and a save in between does not undo that.
func TestALongEffectKeepsItsLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		store := saga.Memory()
		var mu sync.Mutex
		var fences []uint64
		slow := func(ctx context.Context, r *run) error {
			mu.Lock()
			fences = append(fences, r.Fence())
			mu.Unlock()
			time.Sleep(45 * time.Second) // a lease is 30s
			return r.Fire(ctx, evNext, fsm.Unit{})
		}
		d := saga.Define[text]("slow", twoSteps()).Do(first, slow).Do(second, slow)
		a := engine(t, c.Node("a"), store, d)
		engine(t, c.Node("b"), store, d) // it asks for work every second
		begin(t, d, a, "1")
		if s := wait(t, d, a, "1"); s.Status != saga.Done {
			t.Fatalf("%+v", s)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(fences) != 2 || fences[0] != 1 || fences[1] != 1 {
			t.Fatalf("the run changed hands: fences %v", fences)
		}
	})
}

// A signal that comes just as its run is let go is not left for the next
// tick.
func TestASignalAsTheRunIsLetGo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &flaky{Store: saga.Memory()}
		d := saga.Define[text]("race", twoSteps()).AcceptSignal(evNext)
		n := grpcproctest.New(t, "a").Node("a")
		e, err := saga.Start(n, saga.Config{Store: store, Poll: time.Hour}, d)
		if err != nil {
			t.Fatal(err)
		}
		store.onRelease = func() {
			if err := store.Store.Signal(t.Context(), "race", "1", saga.Signal{Event: evNext.Name()}); err != nil {
				t.Error(err)
			}
		}
		began := time.Now()
		begin(t, d, e, "1")
		synctest.Wait()
		if s, _ := d.Get(t.Context(), e, "1"); s.State != second || time.Since(began) != 0 {
			t.Fatalf("%+v after %v", s, time.Since(began))
		}
	})
}

// A store whose Renew never answers is one that failed: the effect's ctx
// ends before the lease does.
func TestARenewThatHangs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &flaky{Store: saga.Memory(), hang: make(chan struct{})}
		h := &held{}
		d := heldSaga(h)
		e := engine(t, grpcproctest.New(t, "a").Node("a"), store, d)
		began := time.Now()
		begin(t, d, e, "1")
		time.Sleep(25 * time.Second) // a lease is 30s
		synctest.Wait()
		if _, _, causes := h.seen(); len(causes) != 1 || !errors.Is(causes[0], saga.ErrLost) {
			t.Fatalf("after %v: causes %v", time.Since(began), causes)
		}
		close(store.hang)
		store.mu.Lock()
		store.hang = nil
		store.mu.Unlock()
		if s := wait(t, d, e, "1"); s.Status != saga.Done {
			t.Fatalf("%+v", s)
		}
	})
}

// An engine that cannot renew gives the run up a quarter of its lease
// before the lease ends, counted from before it asked for the claim, and not
// before: whether its renewals fail slowly, or at once after a claim that
// answered late. It gives the run up at once when the store says the run is
// another's.
func TestTheLeaseIsGivenUpAQuarterBeforeItEnds(t *testing.T) {
	unreachable := errors.New("store unreachable")
	for name, tc := range map[string]struct {
		slow, late time.Duration
		err        error
		by         time.Duration // when the effect's ctx ends, from the claim
	}{
		"slow renewals": {slow: 7*time.Second + 400*time.Millisecond, err: unreachable, by: 22500 * time.Millisecond},
		"a late claim":  {late: 6 * time.Second, err: unreachable, by: 22500 * time.Millisecond},
		"another's":     {err: saga.ErrLost, by: 7500 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := &flaky{Store: saga.Memory(), slow: tc.slow}
				store.fail("Renew", tc.err)
				h := &held{}
				d := heldSaga(h)
				e := engine(t, grpcproctest.New(t, "a").Node("a"), store, d)
				synctest.Wait() // the engine has claimed nothing: the next claim is the run's
				store.mu.Lock()
				store.late = tc.late
				store.mu.Unlock()
				begin(t, d, e, "1")
				time.Sleep(tc.by - time.Second)
				synctest.Wait()
				if _, _, causes := h.seen(); len(causes) != 0 {
					t.Fatalf("given up early: %v", causes)
				}
				time.Sleep(2 * time.Second)
				synctest.Wait()
				if _, _, causes := h.seen(); len(causes) != 1 || !errors.Is(causes[0], saga.ErrLost) {
					t.Fatalf("causes %v", causes)
				}
				store.fail("Renew", nil)
				if s := wait(t, d, e, "1"); s.Status != saga.Done {
					t.Fatalf("%+v", s)
				}
			})
		})
	}
}

// An engine keeps a run whose claim answered after half its lease had gone,
// one whose store was unreachable for a while, since a renewal that failed
// is tried again soon, and one whose store reports a lease end by a clock
// behind its own, since it counts by its own.
func TestTheLeaseIsKept(t *testing.T) {
	for name, tc := range map[string]struct {
		late, down, behind time.Duration
	}{
		"a claim, past half its lease": {late: 16 * time.Second},
		"a store down for a while":     {down: 16 * time.Second},
		"a store's clock behind":       {behind: time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := &flaky{Store: saga.Memory(), behind: tc.behind}
				h := &held{}
				d := heldSaga(h)
				e := engine(t, grpcproctest.New(t, "a").Node("a"), store, d)
				synctest.Wait()
				store.mu.Lock()
				store.late = tc.late
				store.mu.Unlock()
				store.fail("Renew", errors.New("store unreachable"))
				begin(t, d, e, "1")
				time.Sleep(tc.down)
				store.fail("Renew", nil)
				time.Sleep(2 * time.Minute)
				synctest.Wait()
				if _, _, causes := h.seen(); len(causes) != 0 {
					t.Fatalf("given up: %v", causes)
				}
			})
		})
	}
}

// logs is a log a node writes to, and its tests read.
type logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logs) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(b)
}

func (l *logs) count(s string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Count(l.buf.String(), s)
}

// A renewal that fails is logged once until one gets through, and one cut
// short by the end of the work is not logged at all.
func TestRenewalWarnings(t *testing.T) {
	for name, tc := range map[string]struct {
		hang     bool          // renewals hang, and the node stops while the first does
		after    time.Duration // when the log is read, from the claim
		warnings int
	}{
		"failing":                {after: 20 * time.Second, warnings: 1}, // from 7.5s, a sixteenth of the lease apart
		"stopped while it hangs": {hang: true, after: 10 * time.Second},  // from 7.5s, until 15s
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := &flaky{Store: saga.Memory()}
				if tc.hang {
					store.hang = make(chan struct{}) // in the bubble, which waits on it
				} else {
					store.fail("Renew", errors.New("store unreachable"))
				}
				var log logs
				c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithLogger(slog.New(slog.NewTextHandler(&log, nil)))}, "a")
				d := heldSaga(&held{})
				e := engine(t, c.Node("a"), store, d)
				begin(t, d, e, "1")
				time.Sleep(tc.after)
				if tc.hang {
					if err := c.Node("a").Stop(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				synctest.Wait()
				if n := log.count("saga: renew"); n != tc.warnings {
					t.Fatalf("%d warnings:\n%s", n, log.buf.String())
				}
			})
		})
	}
}

// A save the store refuses as another owner's ends the work on the run; one
// it fails is asked again until the engine is told to stop.
func TestASaveThatIsRefusedOrFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		store := &flaky{Store: saga.Memory()}
		var runs atomic.Int32
		d := saga.Define[text]("saves", twoSteps()).
			Do(first, func(ctx context.Context, r *run) error { runs.Add(1); return step("first")(ctx, r) }).
			Do(second, step("second"))
		e := engine(t, c.Node("a"), store, d)

		store.fail("Save", saga.ErrLost)
		begin(t, d, e, "lost")
		synctest.Wait()
		if s, _ := d.Get(t.Context(), e, "lost"); s.State != first || runs.Load() != 1 {
			t.Fatalf("%+v after %d runs", s, runs.Load())
		}
		// The lease runs out, and the run is done over.
		store.fail("Save", nil)
		if s := wait(t, d, e, "lost"); s.Status != saga.Done || runs.Load() != 2 {
			t.Fatalf("%+v after %d runs", s, runs.Load())
		}

		store.fail("Save", errors.New("store down"))
		begin(t, d, e, "down")
		time.Sleep(3500 * time.Millisecond) // between two asks, which are a second apart
		c.Stop("a")
		if s, _ := d.Get(t.Context(), e, "down"); s.State != first || s.Status != saga.Active {
			t.Fatalf("%+v", s)
		}
	})
}

// A run a newer program signals while an older engine works on it is let
// go, its signal untouched, for the newer program's engine.
func TestANewerSignalDuringWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "old", "new")
		store := saga.Memory()
		ctx := t.Context()
		evApproved := fsm.Signal("approved")
		machine := func(approved bool) *fsm.Machine[state] {
			rules := []fsm.Rule[state]{fsm.Initial(first), fsm.From(first).On(evNext).To(second), fsm.From(second).On(evGo).To(done)}
			if approved {
				rules = append(rules, fsm.From(second).On(evApproved).To(done))
			}
			return fsm.MustNew("approvals", rules...)
		}
		slow := func(ctx context.Context, r *run) error {
			time.Sleep(2 * time.Second)
			return r.Fire(ctx, evNext, fsm.Unit{})
		}
		// second has an effect too, so the old engine saves before it, and
		// learns there that the run is no longer its own.
		var ranSecond []string
		var mu sync.Mutex
		waits := func(on string) saga.Effect[state, text] {
			return func(context.Context, *run) error {
				mu.Lock()
				defer mu.Unlock()
				ranSecond = append(ranSecond, on)
				return nil
			}
		}
		v1 := saga.Define[text]("approvals", machine(false)).Version(1).Do(first, slow).Do(second, waits("old")).AcceptSignal(evGo)
		v2 := saga.Define[text]("approvals", machine(true)).Version(2).Do(first, slow).Do(second, waits("new")).AcceptSignal(evGo).AcceptSignal(evApproved)
		old := engine(t, c.Node("old"), store, v1)
		begin(t, v1, old, "1")
		synctest.Wait() // the old engine is in its effect
		newer := engine(t, c.Node("new"), store, v2)
		if err := v2.Notify(ctx, newer, "1", evApproved); err != nil {
			t.Fatal(err)
		}
		s := wait(t, v2, newer, "1")
		r, _, _ := store.Get(ctx, "approvals", "1")
		mu.Lock()
		defer mu.Unlock()
		if s.State != done || r.SagaVersion != 2 || len(r.Inbox) != 0 || len(ranSecond) != 1 || ranSecond[0] != "new" {
			t.Fatalf("%+v, %+v, second ran on %v", s, r, ranSecond)
		}
	})
}

// Signals are taken in the order they came: one whose action fails holds
// those behind it.
func TestAFailingSignalHoldsThoseBehindIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var fails atomic.Bool
		fails.Store(true)
		m := fsm.MustNew("ordered",
			fsm.Initial(first),
			fsm.From(first).On(evPaid).To(done).Action(func(context.Context, *wrapperspb.StringValue) error {
				if fails.Load() {
					return errors.New("ledger down")
				}
				return nil
			}),
			fsm.From(first).On(evGo).To(failed),
		)
		d := saga.Define[text]("ordered", m).Accept(evPaid, nil).AcceptSignal(evGo)
		e, store := start(t, d)
		ctx := t.Context()
		begin(t, d, e, "1")
		if err := d.Signal(ctx, e, "1", evPaid, wrapperspb.String("1")); err != nil {
			t.Fatal(err)
		}
		if err := d.Notify(ctx, e, "1", evGo); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if r, _, _ := store.Get(ctx, "ordered", "1"); r.State != "first" || len(r.Inbox) != 2 || r.Epoch > 8 {
			t.Fatalf("while the first fails: %+v", r)
		}
		fails.Store(false)
		if s := wait(t, d, e, "1"); s.State != done {
			t.Fatalf("the second overtook the first: %+v", s)
		}
	})
}

// An engine started on a store with runs already due works on them: its
// workers reach it from their first moment.
func TestRunsDueAtStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := saga.Memory()
		for _, id := range []string{"1", "2", "3", "4"} {
			if _, _, err := store.Create(t.Context(), saga.Record{Saga: "due", ID: id, State: "first", Visit: 1, Wake: time.Now()}); err != nil {
				t.Fatal(err)
			}
		}
		var tries atomic.Int32
		d := saga.Define[text]("due", twoSteps()).
			Do(first, func(ctx context.Context, r *run) error {
				if tries.Add(1) <= 4 {
					return errors.New("busy")
				}
				return r.Fire(ctx, evNext, fsm.Unit{})
			}).
			Do(second, step("second"))
		e := engine(t, grpcproctest.New(t, "a").Node("a"), store, d)
		for _, id := range []string{"1", "2", "3", "4"} {
			if s := wait(t, d, e, id); s.Status != saga.Done {
				t.Fatalf("%s: %+v", id, s)
			}
		}
	})
}
