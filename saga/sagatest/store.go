// Package sagatest checks a saga.Store: what every implementation must do
// for an engine to be right on it.
package sagatest

import (
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/grpcproc/saga"
)

// Store runs the checks, each a subtest, on stores that open returns, a new
// and empty one each time. Its leases are under half a second, on the clock
// the store reads.
func Store(t *testing.T, open func(t *testing.T) saga.Store) {
	t.Run("create is if absent", func(t *testing.T) {
		s, ctx := open(t), t.Context()
		r, created, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", State: "x", Data: []byte("d"), Wake: time.Now()})
		if err != nil || !created || r.Revision == 0 || r.CreatedAt.IsZero() {
			t.Fatalf("create: %+v %v %v", r, created, err)
		}
		again, created, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", State: "y"})
		if err != nil || created || again.State != "x" {
			t.Fatalf("create again: %+v %v %v", again, created, err)
		}
		if _, created, _ := s.Create(ctx, saga.Record{Saga: "b", ID: "1", State: "x"}); !created {
			t.Fatal("an id is per saga")
		}
		got, ok, err := s.Get(ctx, "a", "1")
		if err != nil || !ok || got.State != "x" || string(got.Data) != "d" {
			t.Fatalf("get: %+v %v %v", got, ok, err)
		}
		if _, ok, err := s.Get(ctx, "a", "none"); ok || err != nil {
			t.Fatalf("get none: %v %v", ok, err)
		}
		if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "0"}); err != nil {
			t.Fatal(err)
		}
		list, err := s.List(ctx, "a")
		if err != nil || len(list) != 2 || list[0].ID != "0" || list[1].ID != "1" {
			t.Fatalf("list: %+v %v", list, err)
		}
	})

	t.Run("claim takes what is due, once", func(t *testing.T) {
		s, ctx := open(t), t.Context()
		now := time.Now()
		for _, r := range []saga.Record{
			{Saga: "a", ID: "later", Wake: now.Add(time.Hour)},
			{Saga: "a", ID: "never"},
			{Saga: "a", ID: "second", Wake: now},
			{Saga: "a", ID: "first", Wake: now.Add(-time.Second)},
			{Saga: "a", ID: "done", Wake: now, Status: saga.Done},
			{Saga: "a", ID: "stuck", Wake: now, Status: saga.Stuck},
			{Saga: "other", ID: "x", Wake: now},
		} {
			if _, _, err := s.Create(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		if got, err := s.Claim(ctx, "e1", map[string]uint64{"a": 0}, time.Minute, 0); err != nil || len(got) != 0 {
			t.Fatalf("claim none: %+v %v", got, err)
		}
		got, err := s.Claim(ctx, "e1", map[string]uint64{"a": 0}, time.Minute, 1)
		if err != nil || len(got) != 1 || got[0].ID != "first" || got[0].Owner != "e1" || got[0].Epoch != 1 {
			t.Fatalf("claim one: %+v %v", got, err)
		}
		got, err = s.Claim(ctx, "e2", map[string]uint64{"a": 0}, time.Minute, 10)
		if err != nil || len(got) != 1 || got[0].ID != "second" {
			t.Fatalf("claim the rest: %+v %v", got, err)
		}
		if got, _ := s.Claim(ctx, "e2", map[string]uint64{"a": 0}, time.Minute, 10); len(got) != 0 {
			t.Fatalf("claimed twice: %+v", got)
		}
	})

	t.Run("claim leaves a newer version's runs", func(t *testing.T) {
		s, ctx := open(t), t.Context()
		now := time.Now()
		for _, r := range []saga.Record{{Saga: "a", ID: "v1", SagaVersion: 1, Wake: now}, {Saga: "a", ID: "v3", SagaVersion: 3, Wake: now}} {
			if _, _, err := s.Create(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.Claim(ctx, "e1", map[string]uint64{"a": 2}, time.Minute, 10)
		if err != nil || len(got) != 1 || got[0].ID != "v1" {
			t.Fatalf("claim at version 2: %+v %v", got, err)
		}
		// A signal from a newer program makes the run that program's.
		got[0].Owner = ""
		if _, err := s.Save(ctx, got[0]); err != nil {
			t.Fatal(err)
		}
		if err := s.Signal(ctx, "a", "v1", saga.Signal{Event: "x", Version: 3}); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.Claim(ctx, "e1", map[string]uint64{"a": 2}, time.Minute, 10); len(got) != 0 {
			t.Fatalf("claimed a run a newer version signalled: %+v", got)
		}
		got, _ = s.Claim(ctx, "e1", map[string]uint64{"a": 3}, time.Minute, 10)
		if len(got) != 2 || got[0].SagaVersion != 3 || got[1].SagaVersion != 3 {
			t.Fatalf("claim at version 3: %+v", got)
		}
		// Nor does a save by an owner that read the run before the signal.
		older := got[0]
		older.SagaVersion = 1
		if kept, err := s.Save(ctx, older); err != nil || kept.SagaVersion != 3 {
			t.Fatalf("a save lowered the version: %+v %v", kept, err)
		}
		// An older signal does not lower it.
		if err := s.Signal(ctx, "a", "v3", saga.Signal{Event: "x", Version: 1}); err != nil {
			t.Fatal(err)
		}
		if r, _, _ := s.Get(ctx, "a", "v3"); r.SagaVersion != 3 {
			t.Fatalf("version lowered: %+v", r)
		}
	})

	t.Run("a save is fenced, and a release frees", func(t *testing.T) {
		s, ctx := open(t), t.Context()
		if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", State: "x", Wake: time.Now()}); err != nil {
			t.Fatal(err)
		}
		mine := claimOne(t, s, "e1")
		mine.State, mine.Data, mine.Attempts, mine.Wake = "y", []byte("new"), 2, time.Now()
		mine.LeaseUntil = time.Time{} // a save leaves the lease as it is
		kept, err := s.Save(ctx, mine)
		if err != nil || kept.State != "y" || kept.Revision <= mine.Revision || kept.CreatedAt.IsZero() || kept.Owner != "e1" || kept.LeaseUntil.IsZero() {
			t.Fatalf("save: %+v %v", kept, err)
		}
		stale := kept
		stale.Epoch--
		if _, err := s.Save(ctx, stale); !errors.Is(err, saga.ErrLost) {
			t.Fatalf("stale save: %v", err)
		}
		if _, err := s.Save(ctx, saga.Record{Saga: "a", ID: "none"}); !errors.Is(err, saga.ErrLost) {
			t.Fatalf("save of no run: %v", err)
		}
		// Owned, so not claimed, though it is due.
		if got, _ := s.Claim(ctx, "e2", map[string]uint64{"a": 0}, time.Minute, 1); len(got) != 0 {
			t.Fatalf("claimed an owned run: %+v", got)
		}
		kept.Owner = ""
		if _, err := s.Save(ctx, kept); err != nil {
			t.Fatal(err)
		}
		// Let go: its lease is no more, though its epoch is the same.
		if err := s.Renew(ctx, "a", "1", kept.Epoch, time.Minute); !errors.Is(err, saga.ErrLost) {
			t.Fatalf("renew of a run let go: %v", err)
		}
		next := claimOne(t, s, "e2")
		if next.Epoch != kept.Epoch+1 || next.State != "y" || string(next.Data) != "new" || next.Attempts != 2 {
			t.Fatalf("after release: %+v", next)
		}
		// The first owner's epoch is gone for good.
		if _, err := s.Save(ctx, kept); !errors.Is(err, saga.ErrLost) {
			t.Fatalf("old owner's save: %v", err)
		}
	})

	t.Run("a lease ends, unless renewed", func(t *testing.T) {
		s, ctx := open(t), t.Context()
		if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", Wake: time.Now()}); err != nil {
			t.Fatal(err)
		}
		mine := claimOne(t, s, "e1")
		time.Sleep(150 * time.Millisecond)
		if err := s.Renew(ctx, "a", "1", mine.Epoch, lease); err != nil {
			t.Fatal(err)
		}
		time.Sleep(300 * time.Millisecond) // past the first lease, within the renewed one
		if got, _ := s.Claim(ctx, "e2", map[string]uint64{"a": 0}, time.Minute, 1); len(got) != 0 {
			t.Fatalf("claimed under a renewed lease: %+v", got)
		}
		time.Sleep(200 * time.Millisecond)
		theirs := claimOne(t, s, "e2")
		if theirs.Epoch != mine.Epoch+1 {
			t.Fatalf("epoch %d after %d", theirs.Epoch, mine.Epoch)
		}
		if err := s.Renew(ctx, "a", "1", mine.Epoch, time.Minute); !errors.Is(err, saga.ErrLost) {
			t.Fatalf("old owner's renew: %v", err)
		}
		if err := s.Renew(ctx, "a", "none", 1, time.Minute); !errors.Is(err, saga.ErrLost) {
			t.Fatalf("renew of no run: %v", err)
		}
		// A lease renewed to nothing is free at once.
		if err := s.Renew(ctx, "a", "1", theirs.Epoch, 0); err != nil {
			t.Fatal(err)
		}
		claimOne(t, s, "e3")
	})

	t.Run("signals are kept in order, and wake", func(t *testing.T) {
		s, ctx := open(t), t.Context()
		later := time.Now().Add(time.Hour).Truncate(time.Millisecond)
		if _, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", Wake: later}); err != nil { // not due
			t.Fatal(err)
		}
		if got, _ := s.Claim(ctx, "e1", map[string]uint64{"a": 0}, lease, 1); len(got) != 0 {
			t.Fatalf("claimed a run that waits: %+v", got)
		}
		for _, ev := range []string{"one", "two"} {
			if err := s.Signal(ctx, "a", "1", saga.Signal{Event: ev, Payload: []byte(ev)}); err != nil {
				t.Fatal(err)
			}
		}
		// The signals make it due, and leave its Wake as it was.
		mine := claimOne(t, s, "e1")
		if len(mine.Inbox) != 2 || mine.Inbox[0].Event != "one" || mine.Inbox[1].Seq <= mine.Inbox[0].Seq || string(mine.Inbox[1].Payload) != "two" || !mine.Wake.Equal(later) {
			t.Fatalf("inbox: %+v, wake %v", mine.Inbox, mine.Wake)
		}
		// A third comes while the owner works: it has seen two, takes the first.
		if err := s.Signal(ctx, "a", "1", saga.Signal{Event: "three"}); err != nil {
			t.Fatal(err)
		}
		mine.Consumed, mine.Seen = []uint64{mine.Inbox[0].Seq}, mine.Inbox[1].Seq
		mine.Owner, mine.Wake = "", time.Time{} // let go, to wait
		kept, err := s.Save(ctx, mine)
		if err != nil || len(kept.Inbox) != 2 || kept.Inbox[0].Event != "two" || kept.Inbox[1].Event != "three" {
			t.Fatalf("after save: %+v %v", kept.Inbox, err)
		}
		// The unseen signal keeps it due.
		again := claimOne(t, s, "e1")
		again.Seen = again.Inbox[1].Seq
		again.Owner = ""
		if _, err := s.Save(ctx, again); err != nil {
			t.Fatal(err)
		}
		// Seen, and not taken: it waits again.
		if got, _ := s.Claim(ctx, "e1", map[string]uint64{"a": 0}, time.Minute, 1); len(got) != 0 {
			t.Fatalf("claimed with nothing unseen: %+v", got)
		}
		// A new one is numbered past those seen.
		if err := s.Signal(ctx, "a", "1", saga.Signal{Event: "four"}); err != nil {
			t.Fatal(err)
		}
		last := claimOne(t, s, "e1")
		if n := len(last.Inbox); n != 3 || last.Inbox[2].Seq <= again.Seen {
			t.Fatalf("inbox: %+v", last.Inbox)
		}

		if err := s.Signal(ctx, "a", "none", saga.Signal{Event: "x"}); !errors.Is(err, saga.ErrNoRun) {
			t.Fatalf("signal to no run: %v", err)
		}
	})

	t.Run("signals to a full or ended run are refused", func(t *testing.T) {
		s, ctx := open(t), t.Context()
		for _, r := range []saga.Record{{Saga: "a", ID: "full"}, {Saga: "a", ID: "done", Status: saga.Done}, {Saga: "a", ID: "stuck", Status: saga.Stuck}} {
			if _, _, err := s.Create(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		for range saga.MaxInbox {
			if err := s.Signal(ctx, "a", "full", saga.Signal{Event: "x"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Signal(ctx, "a", "full", saga.Signal{Event: "x"}); !errors.Is(err, saga.ErrInboxFull) {
			t.Fatalf("signal to a full inbox: %v", err)
		}
		if err := s.Signal(ctx, "a", "done", saga.Signal{Event: "x"}); !errors.Is(err, saga.ErrEnded) {
			t.Fatalf("signal to a done run: %v", err)
		}
		// A stuck run keeps its signals, and is not made due by them.
		if err := s.Signal(ctx, "a", "stuck", saga.Signal{Event: "x"}); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.Claim(ctx, "e1", map[string]uint64{"a": 0}, time.Minute, 10); len(got) != 1 || got[0].ID != "full" {
			t.Fatalf("claim: %+v", got)
		}
	})

	t.Run("resume makes a stuck run due", func(t *testing.T) {
		s, ctx := open(t), t.Context()
		for _, r := range []saga.Record{{Saga: "a", ID: "stuck", Status: saga.Stuck, Attempts: 3, RetryAt: time.Now().Add(time.Hour)}, {Saga: "a", ID: "waits"}} {
			if _, _, err := s.Create(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Resume(ctx, "a", "waits"); err != nil { // not stuck: left as it is
			t.Fatal(err)
		}
		if err := s.Resume(ctx, "a", "none"); !errors.Is(err, saga.ErrNoRun) {
			t.Fatalf("resume of no run: %v", err)
		}
		if err := s.Resume(ctx, "a", "stuck"); err != nil {
			t.Fatal(err)
		}
		got, _ := s.Claim(ctx, "e1", map[string]uint64{"a": 0}, time.Minute, 10)
		if len(got) != 1 || got[0].ID != "stuck" || got[0].Status != saga.Active || got[0].Attempts != 0 || !got[0].RetryAt.IsZero() {
			t.Fatalf("after resume: %+v", got)
		}
	})

	t.Run("what it returns is a copy", func(t *testing.T) {
		s, ctx := open(t), t.Context()
		data := []byte("abc")
		r, _, err := s.Create(ctx, saga.Record{Saga: "a", ID: "1", Data: data})
		if err != nil {
			t.Fatal(err)
		}
		data[0], r.Data[1] = 'X', 'Y'
		if got, _, _ := s.Get(ctx, "a", "1"); string(got.Data) != "abc" {
			t.Fatalf("data shared with the store: %q", got.Data)
		}
	})
}

// lease is how long claimOne's claims hold.
const lease = 400 * time.Millisecond

// claimOne claims the one run that is due, of saga "a".
func claimOne(t *testing.T, s saga.Store, owner string) saga.Record {
	t.Helper()
	got, err := s.Claim(t.Context(), owner, map[string]uint64{"a": 0}, lease, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim by %s: %+v %v", owner, got, err)
	}
	return got[0]
}
