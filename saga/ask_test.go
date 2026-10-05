package saga_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/fsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/saga"
	sagav1 "github.com/floatdrop/grpcproc/saga/proto/grpcproc/saga/v1"
)

// query asks the engine q, as the Inspector's Query does.
func query[R proto.Message](t *testing.T, n *grpcproc.Node, e *saga.Engine, q proto.Message) (R, error) {
	t.Helper()
	answer, err := n.Query(t.Context(), e.PID(), q)
	if err != nil {
		var none R
		return none, err
	}
	return answer.(R), nil
}

func ids(runs []*sagav1.Run) string {
	var out []string
	for _, r := range runs {
		out = append(out, r.GetSaga()+"/"+r.GetId())
	}
	return strings.Join(out, " ")
}

// What grpcprocctl asks an engine: the runs of its sagas, one with its data
// and signals as JSON, and to resume one that is stuck.
func TestAnEngineAnswersQueries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		evAny := fsm.Define[*anypb.Any]("any")
		orders := saga.Define[text]("orders", twoSteps()).Accept(evPaid, nil).Accept(evAny, nil)
		refunds := saga.Define[*anypb.Any]("refunds", twoSteps()).Version(2)
		n := grpcproctest.New(t, "a").Node("a")
		store := saga.Memory()
		longIDs := saga.Define[text]("long ids", twoSteps())
		e, err := saga.Start(n, saga.Config{Store: store, InspectData: true}, orders, refunds, longIDs)
		if err != nil {
			t.Fatal(err)
		}
		ctx := t.Context()

		data := func(m proto.Message) []byte { b, _ := proto.Marshal(m); return b }
		unknown := data(&anypb.Any{TypeUrl: "type.googleapis.com/no.Such"})
		// Runs with no wake: the engine leaves them be.
		for _, r := range []saga.Record{
			{Saga: "orders", ID: "1", State: "first", Data: data(wrapperspb.String("a"))},
			{Saga: "orders", ID: "2", State: "second", Status: saga.Stuck, Error: "boom\xff", Data: []byte{0xff}},
			{Saga: "orders", ID: "3", State: "done", Status: saga.Done},
			{Saga: "refunds", ID: "1", State: "first", Data: unknown},
			{Saga: "other", ID: "x", State: "s", Data: data(wrapperspb.String("x"))},
			// Text too long for an answer.
			{Saga: "orders", ID: "4", State: "first", Status: saga.Stuck, Error: "a" + strings.Repeat("é", 1000),
				Data: data(wrapperspb.String(strings.Repeat("é", 1<<20)))},
		} {
			if _, _, err := store.Create(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		for _, s := range []saga.Signal{
			{Event: "paid", Payload: data(wrapperspb.String("42"))},
			{Event: "any", Payload: unknown},
			{Event: "bogus"},
		} {
			if err := store.Signal(ctx, "orders", "1", s); err != nil {
				t.Fatal(err)
			}
		}
		// A stuck run keeps a signal its saga does not accept, and one too
		// large to carry.
		for _, s := range []saga.Signal{
			{Event: "bogus"},
			{Event: "paid", Payload: data(wrapperspb.String(strings.Repeat("x", 20<<10)))},
			// Small, but six times as long as JSON.
			{Event: "paid", Payload: data(wrapperspb.String(strings.Repeat("\x01", 10<<10)))},
		} {
			if err := store.Signal(ctx, "orders", "2", s); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()

		list := func(l *sagav1.ListRuns) *sagav1.Runs {
			t.Helper()
			runs, err := query[*sagav1.Runs](t, n, e, &sagav1.Query{Op: &sagav1.Query_List{List: l}})
			if err != nil {
				t.Fatal(err)
			}
			return runs
		}
		for _, c := range []struct {
			q    *sagav1.ListRuns
			want string
			more bool
		}{
			{&sagav1.ListRuns{Status: []sagav1.Status{sagav1.Status_STATUS_ACTIVE}, After: &sagav1.RunRef{Saga: "long ids"}}, "orders/1 refunds/1", false},
			{&sagav1.ListRuns{Saga: "orders"}, "orders/1 orders/2 orders/3 orders/4", false},
			{&sagav1.ListRuns{Status: []sagav1.Status{sagav1.Status_STATUS_STUCK, sagav1.Status_STATUS_DONE}, After: &sagav1.RunRef{Saga: "long ids"}}, "orders/2 orders/3 orders/4", false},
			{&sagav1.ListRuns{Saga: "orders", Limit: 2}, "orders/1 orders/2", true},
			{&sagav1.ListRuns{Limit: 2, After: &sagav1.RunRef{Saga: "orders", Id: "2"}}, "orders/3 orders/4", true},
			{&sagav1.ListRuns{After: &sagav1.RunRef{Saga: "refunds"}}, "refunds/1", false},
		} {
			got := list(c.q)
			if ids(got.GetRuns()) != c.want || got.GetMore() != c.more {
				t.Errorf("%v: %s, more %v", c.q, ids(got.GetRuns()), got.GetMore())
			}
		}
		if r := list(&sagav1.ListRuns{Saga: "orders", Limit: 1}).GetRuns()[0]; r.GetData() != "" || len(r.GetInbox()) != 0 || r.GetWaiting() == 0 {
			t.Errorf("a listed run carries its data: %v", r)
		}
		// Text that is not UTF-8 is made so: a proto string must be.
		if r := list(&sagav1.ListRuns{Saga: "orders", Status: []sagav1.Status{sagav1.Status_STATUS_STUCK}, Limit: 1}).GetRuns()[0]; r.GetError() != "boom\uFFFD" {
			t.Errorf("error %q", r.GetError())
		}

		get := func(sagaName, id string) (*sagav1.Run, error) {
			return query[*sagav1.Run](t, n, e, &sagav1.Query{Op: &sagav1.Query_Get{Get: &sagav1.RunRef{Saga: sagaName, Id: id}}})
		}
		r, err := get("orders", "1")
		if err != nil || r.GetData() != `"a"` || r.GetStatus() != sagav1.Status_STATUS_ACTIVE {
			t.Fatalf("%v %v", r, err)
		}
		payloads := []string{r.GetInbox()[0].GetPayload(), r.GetInbox()[1].GetPayload()}
		if payloads[0] != `"42"` || payloads[1] != "" {
			t.Errorf("payloads %q", payloads)
		}
		// Data that does not decode, or holds what JSON cannot name, is none,
		// and so is the payload of an event the saga does not accept.
		for _, ref := range [][2]string{{"orders", "2"}, {"refunds", "1"}} {
			if r, err := get(ref[0], ref[1]); err != nil || r.GetData() != "" {
				t.Errorf("%v: %v %v", ref, r, err)
			}
		}
		if r, _ := get("orders", "2"); len(r.GetInbox()) != 3 || r.GetInbox()[0].GetPayload() != "" ||
			r.GetInbox()[1].GetPayload() != "" || r.GetInbox()[1].GetPayloadOmitted() < 20<<10 ||
			r.GetInbox()[2].GetPayload() != "" || r.GetInbox()[2].GetPayloadOmitted() < 60<<10 {
			t.Errorf("a signal no state takes, and one too large: %v", r.GetInbox())
		}
		// A page carries a kilobyte of an error, cut at a character; a run
		// says how long data it cannot carry is.
		page := list(&sagav1.ListRuns{Saga: "orders", After: &sagav1.RunRef{Saga: "orders", Id: "3"}}).GetRuns()[0]
		if e := page.GetError(); len(e) > 1<<10+3 || !strings.HasSuffix(e, "é…") {
			t.Errorf("a long error on a page: %d bytes, ends %q", len(e), e[len(e)-8:])
		}
		if r, err := get("orders", "4"); err != nil || r.GetData() != "" || r.GetDataOmitted() < 2<<20 || r.GetError() != "a"+strings.Repeat("é", 1000) {
			t.Errorf("a run's long data: %d bytes, omitted %d, %v", len(r.GetData()), r.GetDataOmitted(), err)
		}
		if _, err := get("orders", "none"); !errors.Is(err, saga.ErrNoRun) {
			t.Errorf("get of no run: %v", err)
		}
		// A saga the engine does not run is not its to show, though its
		// runs are in the store it shares.
		if _, err := get("other", "x"); !errors.Is(err, saga.ErrNoSaga) {
			t.Errorf("get of another's run: %v", err)
		}
		if _, err := query[*sagav1.Runs](t, n, e, &sagav1.Query{Op: &sagav1.Query_List{List: &sagav1.ListRuns{Saga: "other"}}}); !errors.Is(err, saga.ErrNoSaga) {
			t.Errorf("list of another's runs: %v", err)
		}
		if _, err := n.CallTo[*emptypb.Empty](ctx, e.PID(), &sagav1.Control{Op: &sagav1.Control_Resume{Resume: &sagav1.RunRef{Saga: "other", Id: "x"}}}); err == nil || !strings.Contains(err.Error(), "runs no such saga") {
			t.Errorf("resume of another's run: %v", err)
		}

		// Resumed, the stuck run is active again.
		resume := func(id string) error {
			_, err := n.CallTo[*emptypb.Empty](ctx, e.PID(), &sagav1.Control{Op: &sagav1.Control_Resume{Resume: &sagav1.RunRef{Saga: "orders", Id: id}}})
			return err
		}
		if err := resume("2"); err != nil {
			t.Fatal(err)
		}
		if r, _, _ := store.Get(ctx, "orders", "2"); r.Status != saga.Active {
			t.Fatalf("resumed: %+v", r)
		}
		if err := resume("none"); !errors.Is(err, saga.ErrNoRun) {
			t.Errorf("resume of no run: %v", err)
		}

		// A page ends at two megabytes, though its limit is not reached.
		for _, id := range []string{"a", "b"} {
			if _, _, err := store.Create(ctx, saga.Record{Saga: "long ids", ID: strings.Repeat(id, 3<<19)}); err != nil {
				t.Fatal(err)
			}
		}
		if page := list(&sagav1.ListRuns{Saga: "long ids"}); len(page.GetRuns()) != 1 || !page.GetMore() {
			t.Errorf("a page past two megabytes: %d runs, more %v", len(page.GetRuns()), page.GetMore())
		}

		// What it does not answer.
		for name, ask := range map[string]func() error{
			"control without an op": func() error { _, err := n.CallTo[*emptypb.Empty](ctx, e.PID(), &sagav1.Control{}); return err },
			"query without an op":   func() error { _, err := query[*sagav1.Runs](t, n, e, &sagav1.Query{}); return err },
			"query of another kind": func() error { _, err := query[*sagav1.Runs](t, n, e, wrapperspb.String("x")); return err },
			"call of another kind":  func() error { _, err := n.CallTo[*sagav1.Runs](ctx, e.PID(), wrapperspb.String("x")); return err },
		} {
			if err := ask(); err == nil {
				t.Errorf("%s answered", name)
			}
		}

		info, err := n.Inspect(ctx, e.PID())
		if err != nil || info["saga orders"] != "v0" || info["saga refunds"] != "v2" || info["working"] != "0" {
			t.Errorf("%v %v", info, err)
		}
	})
}

// listHeld is a store whose List and Resume wait for release.
type listHeld struct {
	saga.Store
	release chan struct{}
}

func (h listHeld) List(ctx context.Context, sagaName string) ([]saga.Record, error) {
	<-h.release
	return h.Store.List(ctx, sagaName)
}

func (h listHeld) Resume(ctx context.Context, sagaName, id string) error {
	<-h.release
	return h.Store.Resume(ctx, sagaName, id)
}

// An engine answers so many queries, and so many controls, at once, and
// refuses the next at once, rather than hold up the runs it works on.
func TestAnEngineAnswersSoManyAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		store := listHeld{saga.Memory(), make(chan struct{})}
		e := engine(t, n, store, saga.Define[text]("orders", twoSteps()))
		ctx := t.Context()
		if _, _, err := store.Create(ctx, saga.Record{Saga: "orders", ID: "1", Status: saga.Stuck}); err != nil {
			t.Fatal(err)
		}
		list := func() error {
			_, err := query[*sagav1.Runs](t, n, e, &sagav1.Query{Op: &sagav1.Query_List{List: &sagav1.ListRuns{}}})
			return err
		}
		resume := func() error {
			_, err := n.CallTo[*emptypb.Empty](ctx, e.PID(), &sagav1.Control{Op: &sagav1.Control_Resume{Resume: &sagav1.RunRef{Saga: "orders", Id: "1"}}})
			return err
		}
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				if err := list(); err != nil {
					t.Error(err)
				}
			})
			wg.Go(func() {
				if err := resume(); err != nil {
					t.Error(err)
				}
			})
		}
		synctest.Wait()
		if err := list(); !errors.Is(err, saga.ErrBusy) {
			t.Fatalf("a fifth query: %v", err)
		}
		if err := resume(); !errors.Is(err, saga.ErrBusy) {
			t.Fatalf("a fifth control: %v", err)
		}
		close(store.release)
		wg.Wait()
		synctest.Wait()
		if err := list(); err != nil {
			t.Fatalf("once answered: %v", err)
		}
		// A control with a deadline of its own keeps it.
		dl, stop := context.WithTimeout(ctx, time.Minute)
		defer stop()
		if _, err := n.CallTo[*emptypb.Empty](dl, e.PID(), &sagav1.Control{Op: &sagav1.Control_Resume{Resume: &sagav1.RunRef{Saga: "orders", Id: "1"}}}); err != nil {
			t.Fatal(err)
		}
		// A query with a deadline of its own keeps it.
		short, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		if _, err := n.Query(short, e.PID(), &sagav1.Query{Op: &sagav1.Query_List{List: &sagav1.ListRuns{}}}); err != nil {
			t.Fatal(err)
		}
	})
}

// broken is a store that cannot be read.
type broken struct{ saga.Store }

func (broken) List(context.Context, string) ([]saga.Record, error) {
	return nil, errors.New("store down")
}

func (broken) Get(context.Context, string, string) (saga.Record, bool, error) {
	return saga.Record{}, false, errors.New("store down")
}

// A store that fails fails the question, with its error.
func TestAnEngineWhoseStoreFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		e := engine(t, n, broken{saga.Memory()}, saga.Define[text]("orders", twoSteps()))
		for _, q := range []*sagav1.Query{
			{Op: &sagav1.Query_List{List: &sagav1.ListRuns{}}},
			{Op: &sagav1.Query_Get{Get: &sagav1.RunRef{Saga: "orders", Id: "1"}}},
		} {
			if _, err := query[proto.Message](t, n, e, q); err == nil || !strings.Contains(err.Error(), "store down") {
				t.Errorf("%v: %v", q, err)
			}
		}
	})
}

// An engine shows a run's data, and its signals' payloads, only when told
// to: by default a query sees the rest of the run.
func TestAnEngineKeepsDataToItself(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		store := saga.Memory()
		ctx := t.Context()
		evPaid := fsm.Define[*wrapperspb.StringValue]("paid")
		e := engine(t, n, store, saga.Define[text]("orders", twoSteps()).Accept(evPaid, nil))
		b, _ := proto.Marshal(wrapperspb.String("card 4242"))
		if _, _, err := store.Create(ctx, saga.Record{Saga: "orders", ID: "1", State: "first", Status: saga.Stuck, Error: "declined", Data: b}); err != nil {
			t.Fatal(err)
		}
		if err := store.Signal(ctx, "orders", "1", saga.Signal{Event: "paid", Payload: b}); err != nil {
			t.Fatal(err)
		}
		r, err := query[*sagav1.Run](t, n, e, &sagav1.Query{Op: &sagav1.Query_Get{Get: &sagav1.RunRef{Saga: "orders", Id: "1"}}})
		if err != nil || r.GetData() != "" || r.GetDataOmitted() != 0 || len(r.GetInbox()) != 1 || r.GetInbox()[0].GetPayload() != "" || r.GetError() != "declined" {
			t.Fatalf("%v %v", r, err)
		}
	})
}
