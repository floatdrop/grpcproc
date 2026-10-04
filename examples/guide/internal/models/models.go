// Package models is the service that runs the model, on a node with a GPU.
// Its scheduler takes requests to generate while a slot is free, and runs
// each as a process of its own, which publishes the tokens as the model
// produces them. The package exports its Module, the Model it runs, and how
// many it runs at once.
package models

import (
	"context"
	"fmt"
	"time"

	"golang.yandex/di"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	conversationsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/conversations/v1"
	modelsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/models/v1"
	toolsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/tools/v1"
	"github.com/floatdrop/grpcproc/pubsub"
)

// Model says the assistant's next turn of a conversation: it gives emit
// each token as it has it, and returns the tool it wants called before it
// goes on, if any. It is called from several processes at once. The guide's
// is a script, so that what it says can be pinned by a test; a real one is a
// client of an inference server, which the container builds the same way.
type Model interface {
	Generate(ctx context.Context, history []*modelsv1.Turn, emit func(token string) error) (*toolsv1.Run, error)
}

// Slots is how many generations a node runs at once: what its GPU holds.
type Slots int

// scheduler is the actor. It answers a request with no slot free at once,
// and hands every other to a process started for it, which answers the call
// when the model is done. So the scheduler is never busy generating, and a
// generation that fails takes down nothing else.
type scheduler struct {
	actor.CallsOnly[*modelsv1.Generate]
	model Model
	slots Slots
	// running is the calls being answered, by the monitor of the process
	// that answers each.
	running map[grpcproc.Ref]grpcproc.Msg[*modelsv1.Generate]
}

func (s *scheduler) HandleCall(p *grpcproc.Process[*modelsv1.Generate], m grpcproc.Msg[*modelsv1.Generate]) (proto.Message, error) {
	if len(s.running) >= int(s.slots) {
		return &modelsv1.Generated{Busy: true}, nil
	}
	// Monitored from before it runs, and linked to the scheduler, which
	// takes its generations with it if it ends.
	_, ref, err := p.SpawnMonitor(func(g *grpcproc.Process[proto.Message]) error {
		return generate(g, s.model, m)
	}, grpcproc.WithLabel("generation"), grpcproc.LinkParent())
	if err != nil {
		return nil, err
	}
	s.running[ref] = m
	return nil, actor.ErrNoReply // the generation answers
}

// HandleDown frees the slot of a generation that ended. One that ended
// without answering, by an error or a panic, has its call answered here.
func (s *scheduler) HandleDown(_ *grpcproc.Process[*modelsv1.Generate], d grpcproc.Down) error {
	m := s.running[d.Ref]
	delete(s.running, d.Ref)
	if d.Reason != grpcproc.ReasonNormal {
		_ = m.Reply(nil, fmt.Errorf("models: the generation failed: %s", d.Reason))
	}
	return nil
}

// generate is the process of one generation. The call it answers was made
// to the scheduler, which handed it over: a call is answered by whoever
// holds its message. Its tokens go straight to the topic the request names,
// and its answer after them on the same link, so a subscriber has every
// token before whoever called has the answer.
func generate(p *grpcproc.Process[proto.Message], model Model, m grpcproc.Msg[*modelsv1.Generate]) error {
	// The caller's deadline is the generation's, and so is an exit.
	ctx, cancel := m.Context(p.Context())
	defer cancel()
	topic := pubsub.Named[*conversationsv1.Event](m.Body.TopicNode, m.Body.Topic)
	var text string
	tool, err := model.Generate(ctx, m.Body.History, func(token string) error {
		text += token
		return topic.Publish(ctx, p, &conversationsv1.Event{
			Turn: m.Body.Turn, Attempt: m.Body.Attempt, Kind: &conversationsv1.Event_Token{Token: token},
		})
	})
	if err != nil {
		return err // its exit reason, which HandleDown passes on
	}
	return m.Reply(&modelsv1.Generated{Text: text, Tool: tool}, nil)
}

// tree is the service's supervision tree. The scheduler's dependencies come
// from the container; Child builds a fresh actor around them at every start.
func tree(model Model, slots Slots) actor.ChildSpec {
	return actor.ChildSupervisor(modelsv1.Service+"-sup", actor.Spec{
		Children: []actor.ChildSpec{
			actor.Child(modelsv1.SchedulerName, func() *scheduler {
				return &scheduler{model: model, slots: slots, running: map[grpcproc.Ref]grpcproc.Msg[*modelsv1.Generate]{}}
			}),
		},
	})
}

// Module registers the model, how many it runs at once, and adds the tree
// to the node's root.
func Module(s *di.Scope) {
	s.Wire[Model](newScript)
	s.Value(Pace(40 * time.Millisecond))
	s.Value(Slots(2))
	s.Wire[actor.ChildSpec](tree).Group()
}

var _ actor.DownHandler[*modelsv1.Generate] = (*scheduler)(nil)
