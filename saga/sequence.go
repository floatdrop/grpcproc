package saga

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/floatdrop/fsm"
	"google.golang.org/protobuf/proto"
)

// Stage is a state of a Sequence's machine: a step's name while the step is
// done, "undo " and its name while it is undone, StageDone or StageFailed.
type Stage string

const (
	// StageDone ends a run whose every step was done.
	StageDone Stage = "done"
	// StageFailed ends a run whose steps were undone.
	StageFailed Stage = "failed"
)

// The events of every Sequence's machine: a step, or an undo, is done; a
// step has failed for good, with why.
var (
	stepDone   = fsm.Signal("done")
	stepFailed = fsm.Define[string]("failed")
)

// StepSpec is a step of a Sequence; see Step.
type StepSpec[D proto.Message] struct {
	name     string
	do, undo Effect[Stage, D]
	doOpts   []StepOption
	undoOpts []StepOption
	pivot    bool
}

// Step is a step of a Sequence, named name: do does it. It is an Effect that
// does not fire: returning nil is the step done, and an error has it tried
// again, as opts say, like any effect.
func Step[D proto.Message](name string, do Effect[Stage, D], opts ...StepOption) *StepSpec[D] {
	return &StepSpec[D]{name: name, do: do, doOpts: opts}
}

// Undo gives the step what undoes it, run if a later step, or this one,
// fails for good before the pivot. The step may not have happened, or not
// wholly: undo must then do nothing, or the rest. An undo that fails for good
// leaves the run Stuck.
func (s *StepSpec[D]) Undo(undo Effect[Stage, D], opts ...StepOption) *StepSpec[D] {
	s.undo, s.undoOpts = undo, opts
	return s
}

// Pivot marks the step after which the run only goes forward: the steps
// after it are not undone, nor is anything undone for them. One that fails
// for good leaves the run Stuck. Without a pivot, the last step is it.
func (s *StepSpec[D]) Pivot() *StepSpec[D] {
	s.pivot = true
	return s
}

// Sequence is a saga of steps done in order, named name. A step that fails
// for good, by a Permanent error or by running out of Attempts, up to the
// pivot, has the steps done so far undone, last first, itself included, since
// it may have happened; the run then ends in StageFailed, with why in its
// Cause. After the pivot there is no way back. A run whose steps are all done
// ends in StageDone.
//
// It is a Definition over a machine built here, so a run is begun, asked
// about and drawn like any other.
func Sequence[D proto.Message](name string, steps ...*StepSpec[D]) (*Definition[Stage, D], error) {
	if len(steps) == 0 {
		return nil, fmt.Errorf("saga %s: a sequence needs a step", name)
	}
	pivot := len(steps) - 1
	seen := map[string]bool{}
	var errs []error
	for i, s := range steps {
		switch {
		case s.name == "" || Stage(s.name) == StageDone || Stage(s.name) == StageFailed || strings.HasPrefix(s.name, "undo "):
			errs = append(errs, fmt.Errorf("saga %s: step %d: %q cannot name a step", name, i, s.name))
		case seen[s.name]:
			errs = append(errs, fmt.Errorf("saga %s: two steps named %q", name, s.name))
		}
		seen[s.name] = true
		if s.pivot {
			pivot = i
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	undoOf := func(s *StepSpec[D]) Stage { return Stage("undo " + s.name) }
	// back is where a run goes from step i, undoing: the undo of the nearest
	// step at or before i that has one, or StageFailed.
	back := func(i int) Stage {
		for ; i >= 0; i-- {
			if steps[i].undo != nil {
				return undoOf(steps[i])
			}
		}
		return StageFailed
	}
	rules := []fsm.Rule[Stage]{fsm.Initial(Stage(steps[0].name))}
	for i, s := range steps {
		next := StageDone
		if i+1 < len(steps) {
			next = Stage(steps[i+1].name)
		}
		rules = append(rules, fsm.From(Stage(s.name)).On(stepDone).To(next))
		if i <= pivot {
			rules = append(rules, fsm.From(Stage(s.name)).On(stepFailed).To(back(i)))
			if s.undo != nil {
				rules = append(rules, fsm.From(undoOf(s)).On(stepDone).To(back(i-1)))
			}
		}
	}
	// The names were checked above, so the rules are a machine.
	m := fsm.MustNew(name, rules...)

	d := Define[D](name, m)
	then := func(do Effect[Stage, D]) Effect[Stage, D] {
		return func(ctx context.Context, r *Run[Stage, D]) error {
			if err := do(ctx, r); err != nil {
				return err
			}
			return r.Fire(ctx, stepDone, fsm.Unit{})
		}
	}
	for i, s := range steps {
		opts := s.doOpts
		if i <= pivot {
			opts = append(opts[:len(opts):len(opts)], Otherwise(stepFailed))
			if s.undo != nil {
				d.Do(undoOf(s), then(s.undo), s.undoOpts...)
			}
		}
		d.Do(Stage(s.name), then(s.do), opts...)
	}
	return d, d.validate()
}
