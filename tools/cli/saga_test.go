package cli_test

import (
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

func TestSaga(t *testing.T) {
	f := testcluster.Start(t)
	f.Saga(t, "a", "orders")

	has(t, ok(t, run(t, f, "saga")), "NODE ENGINE SAGAS WORKING ERROR", "a saga orders v2 0")
	has(t, ok(t, run(t, f, "saga", "runs", "--status", "stuck, done")), "orders 2 ", "orders 3 ")
	has(t, ok(t, run(t, f, "saga", "engines", "--node", "a")), "a saga orders v2")

	table := ok(t, run(t, f, "saga", "runs"))
	has(t, table, "SAGA ID STATE STATUS ATTEMPTS SIGNALS OWNER UPDATED ERROR",
		"orders 1 charging active 0 0", "orders 2 charging stuck 3 2", "card declined", "orders 3 done done 0 0")
	// flat is a table with its columns one space apart.
	flat := func(r result) string { return strings.Join(strings.Fields(ok(t, r)), " ") }
	if got := flat(run(t, f, "saga", "runs", "--status", "stuck,done", "--limit", "1", "orders")); !strings.Contains(got, "orders 2 ") ||
		strings.Contains(got, "orders 3 ") || !strings.Contains(got, `more: --after "2"`) {
		t.Errorf("a page of stuck and done:\n%s", got)
	}
	if got := flat(run(t, f, "saga", "runs", "--limit", "1")); !strings.Contains(got, `more: --after-saga "orders" --after "1"`) {
		t.Errorf("a page of every saga:\n%s", got)
	}
	if got := flat(run(t, f, "saga", "runs", "--after-saga", "orders", "--after", "2")); strings.Contains(got, "orders 2 ") || !strings.Contains(got, "orders 3 ") {
		t.Errorf("after 2:\n%s", got)
	}
	var page struct {
		Runs []client.SagaRunView `json:"runs"`
		More bool                 `json:"more"`
	}
	if err := json.Unmarshal([]byte(ok(t, run(t, f, "--json", "saga", "runs"))), &page); err != nil || len(page.Runs) != 3 || page.More {
		t.Fatalf("%+v %v", page, err)
	}

	has(t, ok(t, run(t, f, "saga", "get", "orders", "1")), "saga: orders", "status: active", `data: "apples"`)
	has(t, ok(t, run(t, f, "saga", "get", "orders", "2")), "error: card declined", `signal 1: paid "42"`, "signal 2: refund")
	has(t, ok(t, run(t, f, "saga", "get", "orders", "3")), "bytes, too large to show")
	var engines []client.SagaEngineView
	if err := json.Unmarshal([]byte(ok(t, run(t, f, "--json", "saga"))), &engines); err != nil || len(engines) != 1 {
		t.Fatalf("%+v %v", engines, err)
	}
	var r client.SagaRunView
	if err := json.Unmarshal([]byte(ok(t, run(t, f, "--json", "saga", "get", "orders", "1"))), &r); err != nil || r.Data != "apples" {
		t.Fatalf("%+v %v", r, err)
	}
	// A resume prints the run as it now is.
	if got := ok(t, run(t, f, "saga", "resume", "orders", "2")); strings.Contains(got, "status: stuck") {
		t.Errorf("resumed, still stuck:\n%s", got)
	}
}

func TestSagaErrors(t *testing.T) {
	f := testcluster.Start(t)
	f.Saga(t, "a", "orders")
	for _, args := range [][]string{
		{"saga", "engines", "x"},
		{"saga", "runs", "a", "b"},
		{"saga", "get", "orders"},
		{"saga", "resume", "orders", "1", "2"},
		{"saga", "--bogus"},
	} {
		if r := run(t, f, args...); r.code != 2 {
			t.Errorf("%v: exit %d, %s", args, r.code, r.stderr)
		}
	}
	for args, want := range map[string]string{
		"saga get orders none":        "no such run",
		"saga resume orders none":     "no such run",
		"saga runs --status lost":     "bad saga status",
		"saga runs --node b":          "no saga engine on node b",
		"saga engines --node nowhere": "node nowhere",
	} {
		r := run(t, f, strings.Fields(args)...)
		if r.code != 1 || !strings.Contains(r.stderr, want) {
			t.Errorf("%s: exit %d, %q, want %q", args, r.code, r.stderr, want)
		}
	}
}

// An engine that shows no data: get says so rather than print nothing.
func TestSagaDataNotShown(t *testing.T) {
	f := testcluster.Start(t)
	f.SagaHidingData(t, "a", "orders")
	has(t, ok(t, run(t, f, "saga", "get", "orders", "1")), "data: (not shown)", "status: active")
}
