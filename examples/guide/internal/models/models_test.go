package models_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"golang.yandex/di"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/models"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	modelsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/models/v1"
	toolsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/tools/v1"
)

// gated is a model that says what it is told to, when it is told to.
type gated struct {
	began chan struct{}
	say   chan error
}

func (g gated) Generate(context.Context, []*modelsv1.Turn, func(string) error) (*toolsv1.Run, error) {
	g.began <- struct{}{}
	return nil, <-g.say
}

// A node with one slot: while it generates, a second request is refused at
// once, and the slot is free again when the generation ends, however it does.
func TestASchedulerWithNoSlotFreeSaysSo(t *testing.T) {
	model := gated{began: make(chan struct{}), say: make(chan error)}
	s := di.Test(t)
	platform.Compose(s, platform.Config{Node: "local", Listen: "127.0.0.1:0"}, slog.New(slog.DiscardHandler), models.Module)
	s.Value[models.Model](model).Override()
	s.Value(models.Slots(1)).Override()
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := s.Get[*grpcproc.Node]()
	scheduler := modelsv1.Scheduler("local")

	for _, failure := range []error{nil, errors.New("out of memory")} {
		answered := make(chan error, 1)
		go func() {
			// The slot of the generation before is free a moment after its
			// answer: when the scheduler hears that its process ended.
			for {
				out, err := scheduler.Generate(t.Context(), n, &modelsv1.Generate{})
				if err != nil || !out.Busy {
					answered <- err
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
		<-model.began
		if out, err := scheduler.Generate(t.Context(), n, &modelsv1.Generate{}); err != nil || !out.Busy {
			t.Fatalf("%v %v", out, err)
		}
		model.say <- failure
		// A generation that failed answers its call with why.
		if err := <-answered; (err == nil) != (failure == nil) || err != nil && !strings.Contains(err.Error(), "out of memory") {
			t.Fatalf("answered %v", err)
		}
	}
}
