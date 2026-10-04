package saga_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc/saga"
)

type stage = saga.Run[saga.Stage, text]

// note is a step, or an undo, that notes its name in the run's data, and
// fails for good if the data, as the run began, names it.
func note(name string) saga.Effect[saga.Stage, text] {
	return func(_ context.Context, r *stage) error {
		if strings.Contains(r.Data.Value, "!"+name+" ") {
			return saga.Permanent(errors.New(name + " refused"))
		}
		r.Data.Value += name + " "
		return nil
	}
}

func order(t *testing.T) *saga.Definition[saga.Stage, text] {
	t.Helper()
	d, err := saga.Sequence("order",
		saga.Step("reserve", note("reserve")).Undo(note("release")),
		saga.Step("check", note("check")), // nothing to undo
		saga.Step("charge", note("charge"), saga.Attempts(2), saga.Backoff(time.Second, time.Second)).Undo(note("refund"), saga.Attempts(1)).Pivot(),
		saga.Step("ship", note("ship")).Undo(note("never")), // past the pivot: not undone
	)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestSequence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := order(t)
		e, _ := start(t, d)
		for id, tc := range map[string]struct {
			data   string // what refuses, as "!name "
			stage  saga.Stage
			status saga.Status
			did    string
			cause  string
		}{
			"done":               {"", saga.StageDone, saga.Done, "reserve check charge ship ", ""},
			"first refused":      {"!reserve ", saga.StageFailed, saga.Done, "release ", "reserve refused"},
			"second refused":     {"!check ", saga.StageFailed, saga.Done, "reserve release ", "check refused"},
			"pivot refused":      {"!charge ", saga.StageFailed, saga.Done, "reserve check refund release ", "charge refused"},
			"after the pivot":    {"!ship ", "ship", saga.Stuck, "reserve check charge ", ""},
			"an undo that fails": {"!charge !refund ", "undo charge", saga.Stuck, "reserve check ", "charge refused"},
		} {
			if _, err := d.Begin(t.Context(), e, id, wrapperspb.String(tc.data)); err != nil {
				t.Fatal(err)
			}
			s, err := d.Wait(t.Context(), e, id)
			if err != nil {
				t.Fatal(err)
			}
			if did := strings.TrimPrefix(s.Data.Value, tc.data); s.State != tc.stage || s.Status != tc.status || did != tc.did || s.Cause != tc.cause {
				t.Errorf("%s: %+v, did %q", id, s, did)
			}
		}
	})
}

func TestSequenceRefuses(t *testing.T) {
	ok := note("x")
	for want, steps := range map[string][]*saga.StepSpec[text]{
		"needs a step":          nil,
		`"" cannot name a step`: {saga.Step("", ok)},
		`"done" cannot name`:    {saga.Step("done", ok)},
		`"failed" cannot name`:  {saga.Step("failed", ok)},
		`"undo x" cannot name`:  {saga.Step("undo x", ok)},
		`two steps named "a"`:   {saga.Step("a", ok), saga.Step("a", ok)},
		"Backoff needs":         {saga.Step("a", ok, saga.Backoff(0, 0))},
	} {
		if _, err := saga.Sequence("bad", steps...); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want %q, got %v", want, err)
		}
	}
}
