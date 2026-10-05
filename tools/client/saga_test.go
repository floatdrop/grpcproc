package client_test

import (
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

func runIDs(runs []client.SagaRunView) string {
	var out []string
	for _, r := range runs {
		out = append(out, r.ID+":"+r.Status)
	}
	return strings.Join(out, " ")
}

func TestSagas(t *testing.T) {
	f := testcluster.Start(t)
	f.Saga(t, "a", "orders")
	c := client.New(f.C.Conn("a"))
	ctx := t.Context()

	engines, err := c.SagaEngines(ctx, "")
	if err != nil || len(engines) != 1 || engines[0].Node != "a" || engines[0].Process != "saga" || strings.Join(engines[0].Sagas, ",") != "orders v2" || engines[0].Error != "" {
		t.Fatalf("%+v %v", engines, err)
	}
	if engines, err := c.SagaEngines(ctx, "b"); err != nil || len(engines) != 0 {
		t.Fatalf("on b: %+v %v", engines, err)
	}

	for _, tc := range []struct {
		q    client.SagaQuery
		want string
		more bool
	}{
		{client.SagaQuery{}, "1:active 2:stuck 3:done", false},
		{client.SagaQuery{Saga: "orders", Status: []string{"stuck", "done"}}, "2:stuck 3:done", false},
		{client.SagaQuery{Limit: 1}, "1:active", true},
		{client.SagaQuery{AfterSaga: "orders", AfterID: "1"}, "2:stuck 3:done", false},
		{client.SagaQuery{Saga: "orders", AfterID: "2"}, "3:done", false},
	} {
		runs, more, err := c.SagaRuns(ctx, "", tc.q)
		if err != nil || runIDs(runs) != tc.want || more != tc.more {
			t.Errorf("%+v: %s, more %v, %v", tc.q, runIDs(runs), more, err)
		}
	}
	// Asked of a named node; a listed run carries no data.
	runs, _, err := c.SagaRuns(ctx, "a", client.SagaQuery{Status: []string{"stuck"}})
	if err != nil || len(runs) != 1 || runs[0].Error != "card declined" || runs[0].Attempts != 3 || runs[0].Data != nil || len(runs[0].Inbox) != 0 || runs[0].Waiting != 2 {
		t.Fatalf("%+v %v", runs, err)
	}

	r, err := c.SagaRun(ctx, "", "orders", "2")
	if err != nil || r.Status != "stuck" || r.Inbox[0].Event != "paid" || r.Inbox[0].Payload != "42" || r.Revision == 0 || r.Created == "" {
		t.Fatalf("%+v %v", r, err)
	}
	if r, err := c.SagaRun(ctx, "", "orders", "1"); err != nil || r.Data != "apples" {
		t.Fatalf("%+v %v", r, err)
	}
	if r, err := c.SagaRun(ctx, "", "orders", "3"); err != nil || r.Data != nil || r.DataOmitted < 2<<20 {
		t.Fatalf("data too large: %+v %v", r.DataOmitted, err)
	}
	if r, err := c.ResumeSaga(ctx, "", "orders", "2"); err != nil || r.ID != "2" {
		t.Fatalf("%+v %v", r, err)
	}
	// Resumed, it is claimed at once, and takes the signal it kept.
	if r, err := c.SagaRun(ctx, "a", "orders", "2"); err != nil || r.Status == "stuck" {
		t.Fatalf("resumed: %+v %v", r, err)
	}

	for name, err := range map[string]error{
		"a bad status":       func() error { _, _, err := c.SagaRuns(ctx, "", client.SagaQuery{Status: []string{"lost"}}); return err }(),
		"no such run":        func() error { _, err := c.SagaRun(ctx, "", "orders", "none"); return err }(),
		"a bad saga":         func() error { _, err := c.SagaRun(ctx, "", "orders", "\xff"); return err }(),
		"no engine":          func() error { _, err := c.SagaRun(ctx, "b", "orders", "1"); return err }(),
		"resume none":        func() error { _, err := c.ResumeSaga(ctx, "", "orders", "none"); return err }(),
		"resume bad":         func() error { _, err := c.ResumeSaga(ctx, "", "orders", "\xff"); return err }(),
		"resume on b":        func() error { _, err := c.ResumeSaga(ctx, "b", "orders", "1"); return err }(),
		"no node":            func() error { _, _, err := c.SagaRuns(ctx, "nowhere", client.SagaQuery{}); return err }(),
		"engines of no node": func() error { _, err := c.SagaEngines(ctx, "nowhere"); return err }(),
		"runs on b":          func() error { _, _, err := c.SagaRuns(ctx, "b", client.SagaQuery{}); return err }(),
		"runs of a bad saga": func() error {
			_, _, err := c.SagaRuns(ctx, "", client.SagaQuery{Saga: "orders", AfterID: "\xff"})
			return err
		}(),
		"no engine anywhere": func() error {
			_, err := client.New(testcluster.Start(t).C.Conn("a")).SagaRun(ctx, "", "orders", "1")
			return err
		}(),
	} {
		if err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// With engines of several stores, a run is asked of an engine that runs its
// saga.
func TestSagasOfSeveralEngines(t *testing.T) {
	f := testcluster.Start(t)
	f.Saga(t, "a", "orders")
	f.Saga(t, "b", "refunds")
	c := client.New(f.C.Conn("a"))
	ctx := t.Context()
	for _, s := range []string{"orders", "refunds"} {
		if r, err := c.SagaRun(ctx, "", s, "1"); err != nil || r.Saga != s || r.Data != "apples" {
			t.Errorf("%s: %+v %v", s, r, err)
		}
		if runs, _, err := c.SagaRuns(ctx, "", client.SagaQuery{Saga: s}); err != nil || len(runs) != 3 || runs[0].Saga != s {
			t.Errorf("%s: %+v %v", s, runs, err)
		}
	}
	// A saga no engine runs is asked of none.
	if _, err := c.SagaRun(ctx, "", "unknown", "1"); err == nil || err.Error() != `no saga engine runs saga "unknown"` {
		t.Errorf("a saga nobody runs: %v", err)
	}
	// Asked on a node whose engine does not run it, the engine says so.
	if _, err := c.SagaRun(ctx, "b", "orders", "1"); err == nil || err.Error() != `no saga engine on node b runs saga "orders"` {
		t.Errorf("orders asked on b: %v", err)
	}
}
