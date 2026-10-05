package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/saga"
	"github.com/floatdrop/grpcproc/saga/postgres"
	"github.com/floatdrop/grpcproc/saga/sagatest"
)

// db is the database every test shares: see TestDSN.
var db = sync.OnceValue(func() *sql.DB {
	db, err := sql.Open("pgx", postgres.TestDSN)
	if err != nil {
		panic(err)
	}
	return db
})

var tables atomic.Int64

// open returns a store in tables no other has.
func open(t *testing.T) *postgres.Store {
	s, _ := named(t)
	return s
}

// named is open, and the name of the store's table.
func named(t *testing.T) (*postgres.Store, string) {
	t.Helper()
	name := fmt.Sprintf("sagas_%d", tables.Add(1))
	s, err := postgres.NewStore(db(), name)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s, name
}

func TestStore(t *testing.T) {
	sagatest.Store(t, func(t *testing.T) saga.Store { return open(t) })
}

// The store asks of its driver only what database/sql does: lib/pq, which
// knows no Go type beyond database/sql's, keeps it as well as pgx.
func TestStoreOnAnotherDriver(t *testing.T) {
	pq, err := sql.Open("postgres", postgres.TestDSN+"?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pq.Close() })
	sagatest.Store(t, func(t *testing.T) saga.Store {
		s, err := postgres.NewStore(pq, fmt.Sprintf("sagas_%d", tables.Add(1)))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Migrate(t.Context()); err != nil {
			t.Fatal(err)
		}
		return s
	})
}

// A signal with no payload, and a run with no data, read back as nil
// through either driver.
func TestNothingIsNil(t *testing.T) {
	for _, driver := range []struct{ name, dsn string }{
		{"pgx", postgres.TestDSN},
		{"postgres", postgres.TestDSN + "?sslmode=disable"},
	} {
		conn, err := sql.Open(driver.name, driver.dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		s, err := postgres.NewStore(conn, fmt.Sprintf("sagas_%d", tables.Add(1)))
		if err != nil {
			t.Fatal(err)
		}
		ctx := t.Context()
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", Data: []byte{}}); err != nil {
			t.Fatal(err)
		}
		if err := s.Signal(ctx, "a", "1", saga.Signal{Event: "x", Payload: []byte{}}); err != nil {
			t.Fatal(err)
		}
		r, _, err := s.Get(ctx, "a", "1")
		if err != nil || r.Data != nil || r.Inbox[0].Payload != nil {
			t.Fatalf("%s: data %#v, payload %#v, %v", driver.name, r.Data, r.Inbox[0].Payload, err)
		}
	}
}

// The point of the store: a run outlasts the program that began it. The
// first program reserves, and is stopped while it charges; the second, a
// new node with a store of its own on the same tables, goes on from the
// charge, and does not reserve again.
func TestARunOutlastsItsProgram(t *testing.T) {
	store, table := named(t)
	var reserved, charged atomic.Int32
	charging := make(chan struct{}, 1)
	order := func(charge saga.Effect[saga.Stage, *wrapperspb.StringValue]) *saga.Definition[saga.Stage, *wrapperspb.StringValue] {
		d, err := saga.Sequence("order",
			saga.Step("reserve", func(context.Context, *saga.Run[saga.Stage, *wrapperspb.StringValue]) error {
				reserved.Add(1)
				return nil
			}),
			saga.Step("charge", charge),
		)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	cfg := func(s saga.Store) saga.Config {
		return saga.Config{Store: s, Poll: 20 * time.Millisecond, Lease: time.Second}
	}

	first := order(func(ctx context.Context, _ *saga.Run[saga.Stage, *wrapperspb.StringValue]) error {
		charging <- struct{}{}
		<-ctx.Done() // the program stops in the middle of it
		return ctx.Err()
	})
	n1 := node(t, "one")
	eng, err := saga.Start(n1, cfg(store), first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Begin(t.Context(), eng, "order-1", wrapperspb.String("4242")); err != nil {
		t.Fatal(err)
	}
	<-charging
	if err := n1.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	again, err := postgres.NewStore(db(), table)
	if err != nil {
		t.Fatal(err)
	}
	second := order(func(context.Context, *saga.Run[saga.Stage, *wrapperspb.StringValue]) error {
		charged.Add(1)
		return nil
	})
	eng, err = saga.Start(node(t, "two"), cfg(again), second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	snap, err := second.Wait(ctx, eng, "order-1")
	if err != nil || snap.State != saga.StageDone {
		t.Fatalf("%+v %v", snap, err)
	}
	if reserved.Load() != 1 || charged.Load() != 1 {
		t.Fatalf("reserved %d times, charged %d", reserved.Load(), charged.Load())
	}
}

func node(t *testing.T, name string) *grpcproc.Node {
	t.Helper()
	n, err := grpcproc.NewNode(grpcproc.Config{Name: name, Resolver: grpcproc.StaticResolver{}, Admit: grpcproc.AdmitAll})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	return n
}

// Signals sent at once are numbered apart, and no more than MaxInbox are
// kept.
func TestSignalsAtOnce(t *testing.T) {
	s, ctx := open(t), t.Context()
	if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1"}); err != nil {
		t.Fatal(err)
	}
	const sent = saga.MaxInbox + 36
	var kept, full atomic.Int32
	var wg sync.WaitGroup
	for range sent {
		wg.Go(func() {
			switch err := s.Signal(ctx, "a", "1", saga.Signal{Event: "x"}); {
			case err == nil:
				kept.Add(1)
			case errors.Is(err, saga.ErrInboxFull):
				full.Add(1)
			default:
				t.Error(err)
			}
		})
	}
	wg.Wait()
	r, _, err := s.Get(ctx, "a", "1")
	if err != nil || kept.Load() != saga.MaxInbox || full.Load() != sent-saga.MaxInbox || len(r.Inbox) != saga.MaxInbox {
		t.Fatalf("kept %d, refused %d, inbox %d, %v", kept.Load(), full.Load(), len(r.Inbox), err)
	}
	for i, sig := range r.Inbox {
		if sig.Seq != uint64(i+1) {
			t.Fatalf("signal %d numbered %d", i, sig.Seq)
		}
	}
}

// Engines that claim at once share the runs: none is claimed twice.
func TestClaimsAtOnce(t *testing.T) {
	s, ctx := open(t), t.Context()
	const runs = 50
	for i := range runs {
		if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: fmt.Sprint(i), Wake: time.Now().Add(-time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	claimed := map[string]string{}
	var wg sync.WaitGroup
	for e := range 5 {
		owner := fmt.Sprint("e", e)
		wg.Go(func() {
			for {
				got, err := s.Claim(ctx, owner, map[string]uint64{"a": 0}, time.Minute, 3)
				if err != nil {
					t.Error(err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, r := range got {
					if by, twice := claimed[r.ID]; twice {
						t.Errorf("%s claimed by %s and %s", r.ID, by, owner)
					}
					claimed[r.ID] = owner
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(claimed) != runs {
		t.Fatalf("claimed %d of %d", len(claimed), runs)
	}
}

// What a store refuses before it asks the database, and what the database
// refuses: each is an error, never a panic or a wrong answer.
func TestErrors(t *testing.T) {
	if _, err := postgres.NewStore(db(), "Not-A-Table"); err == nil {
		t.Fatal("a bad table name was taken")
	}
	s, table := named(t)
	ctx := t.Context()
	huge := uint64(1) << 63
	if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", SagaVersion: huge}); err == nil {
		t.Fatal("create: a version past a bigint was taken")
	}
	if _, err := s.Save(ctx, saga.Record{Saga: "a", ID: "1", SagaVersion: huge}); err == nil {
		t.Fatal("save: a version past a bigint was taken")
	}
	if err := s.Signal(ctx, "a", "1", saga.Signal{Event: "x", Version: huge}); err == nil {
		t.Fatal("signal: a version past a bigint was taken")
	}
	if got, err := s.Claim(ctx, "e", map[string]uint64{"a": 0}, time.Minute, 0); got != nil || err != nil {
		t.Fatalf("claim of none: %v %v", got, err)
	}
	if got, err := s.Claim(ctx, "e", nil, time.Minute, 1); got != nil || err != nil {
		t.Fatalf("claim of no saga: %v %v", got, err)
	}
	if _, err := s.Claim(ctx, "e", map[string]uint64{"a": huge}, time.Minute, 1); err == nil {
		t.Fatal("claim: a version past a bigint was taken")
	}

	// Text that is not UTF-8 is refused by the database, as each query runs.
	bad := "\xff"
	if _, _, err := s.Create(ctx, saga.Record{Saga: bad, ID: "1"}); err == nil {
		t.Fatal("create: bad text taken")
	}
	if _, err := s.Claim(ctx, bad, map[string]uint64{"a": 0}, time.Minute, 1); err == nil {
		t.Fatal("claim: bad owner taken")
	}
	if _, err := s.Claim(ctx, "e", map[string]uint64{bad: 0}, time.Minute, 1); err == nil {
		t.Fatal("claim: bad saga taken")
	}
	if _, err := s.Save(ctx, saga.Record{Saga: bad, ID: "1"}); err == nil {
		t.Fatal("save: bad text taken")
	}
	if _, _, err := s.Get(ctx, bad, "1"); err == nil {
		t.Fatal("get: bad text taken")
	}
	if _, err := s.List(ctx, bad); err == nil {
		t.Fatal("list: bad text taken")
	}

	// A run whose inbox is not what the store wrote is an error, not a run.
	if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "spoilt", Wake: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := db().ExecContext(ctx, `UPDATE "`+table+`" SET inbox = '[{"seq": 1, "payload": "not base64"}]' WHERE id = 'spoilt'`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get(ctx, "a", "spoilt"); err == nil {
		t.Fatal("get: a spoilt inbox was read")
	}
	if _, err := s.List(ctx, "a"); err == nil {
		t.Fatal("list: a spoilt inbox was read")
	}

	// A database that cannot be reached fails every method.
	closed, err := sql.Open("pgx", postgres.TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Close()
	gone, err := postgres.NewStore(closed, "sagas_gone")
	if err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() error{
		"migrate": func() error { return gone.Migrate(ctx) },
		"create":  func() error { _, _, err := gone.Create(ctx, saga.Record{Saga: "a", ID: "1"}); return err },
		"claim":   func() error { _, err := gone.Claim(ctx, "e", map[string]uint64{"a": 0}, time.Minute, 1); return err },
		"save":    func() error { _, err := gone.Save(ctx, saga.Record{Saga: "a", ID: "1"}); return err },
		"renew":   func() error { return gone.Renew(ctx, "a", "1", 1, time.Minute) },
		"signal":  func() error { return gone.Signal(ctx, "a", "1", saga.Signal{Event: "x"}) },
		"resume":  func() error { return gone.Resume(ctx, "a", "1") },
		"get":     func() error { _, _, err := gone.Get(ctx, "a", "1"); return err },
		"list":    func() error { _, err := gone.List(ctx, "a"); return err },
	} {
		if err := call(); err == nil || errors.Is(err, saga.ErrLost) || errors.Is(err, saga.ErrNoRun) {
			t.Errorf("%s on a closed database: %v", name, err)
		}
	}
}

// An effect's error is kept, whatever its code wrote; PostgreSQL text takes
// no NUL and only UTF-8.
func TestAnyErrorIsKept(t *testing.T) {
	s, ctx := open(t), t.Context()
	if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", Wake: time.Now(), Error: "\x00"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, "e", map[string]uint64{"a": 0}, time.Minute, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("%v %v", got, err)
	}
	got[0].Error, got[0].Cause = "bad reply: \xff\x00", "\xfe"
	kept, err := s.Save(ctx, got[0])
	if err != nil || kept.Error != "bad reply: \ufffd" || kept.Cause != "\ufffd" {
		t.Fatalf("%q %q %v", kept.Error, kept.Cause, err)
	}
}

// A table may be named as SQL names a keyword, and programs that start at
// once may each create it.
func TestTables(t *testing.T) {
	ctx := t.Context()
	s, err := postgres.NewStore(db(), "order")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := s.Migrate(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	// The table outlasts a test run: the run is new each time.
	if _, created, err := s.Create(ctx, saga.Record{Saga: "a", ID: time.Now().String()}); err != nil || !created {
		t.Fatal(created, err)
	}
}

// Runs created at once: one is made, and every call returns it.
func TestCreatesAtOnce(t *testing.T) {
	s, ctx := open(t), t.Context()
	var made atomic.Int32
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			r, created, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", State: fmt.Sprint(i)})
			if err != nil || r.ID != "1" || r.Revision == 0 {
				t.Errorf("%+v %v", r, err)
			}
			if created {
				made.Add(1)
			}
		})
	}
	wg.Wait()
	if made.Load() != 1 {
		t.Fatalf("made %d times", made.Load())
	}
}

// Signals that come while the owner saves are kept: none is lost, and none
// is taken that the owner did not consume.
func TestSignalsWhileSaving(t *testing.T) {
	s, ctx := open(t), t.Context()
	if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", Wake: time.Now()}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, "e", map[string]uint64{"a": 0}, time.Minute, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("%v %v", got, err)
	}
	r := got[0]
	const sent = 200
	var wg sync.WaitGroup
	wg.Go(func() {
		for range sent {
			for {
				err := s.Signal(ctx, "a", "1", saga.Signal{Event: "x"})
				if err == nil {
					break
				}
				if !errors.Is(err, saga.ErrInboxFull) {
					t.Error(err)
					return
				}
			}
		}
	})
	consumed := map[uint64]bool{}
	for len(consumed) < sent {
		// The owner takes what it was given, and saves; the save says what
		// came meanwhile.
		r.Consumed = nil
		for _, sig := range r.Inbox {
			if consumed[sig.Seq] {
				t.Fatalf("signal %d given again", sig.Seq)
			}
			consumed[sig.Seq] = true
			r.Consumed = append(r.Consumed, sig.Seq)
		}
		if r, err = s.Save(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if len(r.Inbox) != 0 || len(consumed) != sent {
		t.Fatalf("consumed %d, left %d", len(consumed), len(r.Inbox))
	}
}

// Migrate takes the tables to this version once, and leaves them be after;
// what Schema says makes the same tables.
func TestMigrations(t *testing.T) {
	ctx := t.Context()
	s, table := named(t)
	if err := s.Migrate(ctx); err != nil { // again: nothing to do
		t.Fatal(err)
	}
	// Tables a later version made are left as they are.
	if _, err := db().ExecContext(ctx, `UPDATE "`+table+`_schema" SET version = 99`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db().QueryRowContext(ctx, `SELECT version FROM "`+table+`_schema"`).Scan(&version); err != nil || version != 99 {
		t.Fatalf("version %d, %v", version, err)
	}

	// Applied by hand, as a migration tool would.
	name := fmt.Sprintf("sagas_%d", tables.Add(1))
	by, err := postgres.NewStore(db(), name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db().ExecContext(ctx, by.Schema()); err != nil {
		t.Fatal(err)
	}
	if err := by.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, created, err := by.Create(ctx, saga.Record{Saga: "a", ID: "1"}); err != nil || !created {
		t.Fatal(created, err)
	}

	// A step that fails leaves nothing behind.
	viewName := fmt.Sprintf("sagas_%d", tables.Add(1))
	if _, err := db().ExecContext(ctx, `CREATE VIEW "`+viewName+`" AS SELECT 1 AS x`); err != nil {
		t.Fatal(err)
	}
	view, err := postgres.NewStore(db(), viewName)
	if err != nil {
		t.Fatal(err)
	}
	if err := view.Migrate(ctx); err == nil {
		t.Fatal("a store was made over a view")
	}
	var left bool
	if err := db().QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, `"`+viewName+`_schema"`).Scan(&left); err != nil || left {
		t.Fatalf("left %v, %v", left, err)
	}
}

// IDs are ordered by their bytes, as Go orders strings, whatever the
// database's collation.
func TestOrder(t *testing.T) {
	s, ctx := open(t), t.Context()
	for _, id := range []string{"a", "B", "order-10", "order10"} {
		if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: id, Wake: time.Unix(0, 0)}); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"B", "a", "order-10", "order10"}
	ids := func(runs []saga.Record) []string {
		var out []string
		for _, r := range runs {
			out = append(out, r.ID)
		}
		return out
	}
	list, err := s.List(ctx, "a")
	if err != nil || !slices.Equal(ids(list), want) {
		t.Fatalf("list %v, %v", ids(list), err)
	}
	claimed, err := s.Claim(ctx, "e", map[string]uint64{"a": 0}, time.Minute, 2)
	if err != nil || !slices.Equal(ids(claimed), want[:2]) {
		t.Fatalf("claim %v, %v", ids(claimed), err)
	}
}

// A run is due for a signal only while one waits that its owner has not
// seen, as saga.Memory has it: one it took without looking makes it due no
// more.
func TestTakenSignalsMakeNothingDue(t *testing.T) {
	s, ctx := open(t), t.Context()
	if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Signal(ctx, "a", "1", saga.Signal{Event: "x"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, "e", map[string]uint64{"a": 0}, time.Minute, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("%v %v", got, err)
	}
	r := got[0]
	r.Consumed, r.Owner = []uint64{r.Inbox[0].Seq}, ""
	if _, err := s.Save(ctx, r); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Claim(ctx, "e", map[string]uint64{"a": 0}, time.Minute, 1); err != nil || len(got) != 0 {
		t.Fatalf("claimed with nothing waiting: %v %v", got, err)
	}
}

// A signal that waits for the run's row while the run ends is told it
// ended, not that the inbox is full.
func TestASignalThatLosesToTheEnd(t *testing.T) {
	s, table := named(t)
	ctx := t.Context()
	if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1"}); err != nil {
		t.Fatal(err)
	}
	// The run ends in a transaction that holds its row.
	tx, err := db().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE "`+table+`" SET status = $1`, int16(saga.Done)); err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	go func() { sent <- s.Signal(ctx, "a", "1", saga.Signal{Event: "x"}) }()
	for waiting := 0; waiting == 0; time.Sleep(5 * time.Millisecond) {
		if err := db().QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE '%jsonb_build_object%'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-sent; !errors.Is(err, saga.ErrEnded) {
		t.Fatalf("got %v", err)
	}
}
