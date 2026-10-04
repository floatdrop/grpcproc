// Package conversations is the service that holds the conversations: a
// process for each, started when it is first spoken to and ended when it
// has been idle. A conversation takes what the user says, has a model answer
// and the tools it asks for run, and publishes all of it, as it comes, on a
// topic of its own. What was said is kept in a History, so a conversation
// whose process ended, or crashed, goes on from there. The package exports
// its Module and the History it depends on.
package conversations

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.yandex/di"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	conversationsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/conversations/v1"
	modelsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/models/v1"
	toolsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/tools/v1"
	"github.com/floatdrop/grpcproc/pubsub"
)

// History keeps what was said in each conversation, outside the process
// that holds it. The guide keeps it in memory; a real History is a database,
// which the container builds, starts and stops the same way.
type History interface {
	Load(ctx context.Context, id string) ([]*modelsv1.Turn, error)
	Append(ctx context.Context, id string, turn *modelsv1.Turn) error
}

type memory struct {
	mu    sync.Mutex
	turns map[string][]*modelsv1.Turn
}

func newMemory() *memory { return &memory{turns: map[string][]*modelsv1.Turn{}} }

func (m *memory) Load(_ context.Context, id string) ([]*modelsv1.Turn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.turns[id]), nil
}

func (m *memory) Append(_ context.Context, id string, turn *modelsv1.Turn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.turns[id] = append(m.turns[id], turn)
	return nil
}

// Limits is how long a conversation waits: for something to be said before
// its process ends, for a model to answer, for a tool, and before it asks
// again the nodes that were all busy.
type Limits struct {
	Idle, Generate, Tool, Busy time.Duration
}

// conversation is one conversation: a process function over what the
// service was given, and the conversation's own state.
type conversation struct {
	id      string
	node    string // this one: where the topic is
	history History
	models  []string // the nodes that run a model, in the order they are tried
	tools   toolsv1.RunnerAddr
	limits  Limits

	turns   []*modelsv1.Turn
	events  pubsub.Topic[*conversationsv1.Event]
	turn    uint64
	attempt uint64 // the last try at an answer: see modelsv1.Generate
}

// run is the conversation's process: a receive loop, since all it takes is
// what the user says, one thing at a time. With nothing said for a while it
// returns, and the process ends; its history stays, for the next one.
func (c *conversation) run(p *grpcproc.Process[*conversationsv1.Request]) (err error) {
	if c.turns, err = c.history.Load(p.Context(), c.id); err != nil {
		return err
	}
	// The topic is the conversation's: linked to it, it ends when this
	// process does, and whoever follows it is told.
	// The topic of the process before this one ends a moment after it, and
	// has the name until then.
	for {
		c.events, err = pubsub.SpawnOwned[*conversationsv1.Event](p, pubsub.Config{Buffer: 1024},
			grpcproc.WithName(conversationsv1.TopicName(c.id)))
		if !errors.Is(err, grpcproc.ErrNameTaken) {
			break
		}
		select {
		case <-p.Context().Done():
			return p.Context().Err()
		case <-time.After(time.Millisecond):
		}
	}
	if err != nil {
		return err
	}
	c.replay(p)
	for {
		m, err := p.ReceiveTimeout(c.limits.Idle)
		if errors.Is(err, context.DeadlineExceeded) {
			return nil // idle: a normal end, so the supervisor leaves it at that
		}
		if err != nil {
			return err
		}
		if m.Body.GetSay().GetText() == "" { // a Down too: nothing here waits for one
			_ = m.Reply(nil, errors.New("conversations: nothing was said"))
			continue
		}
		// Answered as the turn begins: the caller is told its place, and
		// does not wait for the model. What is said meanwhile waits in the
		// mailbox, and is answered in its turn.
		c.turn++
		_ = m.Reply(&conversationsv1.Said{Turn: c.turn}, nil)
		if err := c.answer(p, m.Body.GetSay().GetText()); err != nil {
			return err
		}
	}
}

// replay publishes what the history has, as the events it was: the topic is
// new, and whoever follows it is sent the conversation so far. It counts the
// turns as it goes: one for each thing the user said. A turn the history has
// no answer for failed, or was cut short with its process.
func (c *conversation) replay(p *grpcproc.Process[*conversationsv1.Request]) {
	c.turn = 0 // a process started again runs this conversation again
	for i, t := range c.turns {
		switch t.Role {
		case modelsv1.Role_ROLE_USER:
			c.turn++
			_ = c.publish(p, &conversationsv1.Event{Kind: &conversationsv1.Event_Said{Said: t.Text}})
		case modelsv1.Role_ROLE_TOOL:
			ran := &toolsv1.Ran{Output: t.Text}
			if why, failed := strings.CutPrefix(t.Text, modelsv1.ToolFailed); failed {
				ran = &toolsv1.Ran{Failed: why}
			}
			_ = c.publish(p, &conversationsv1.Event{Kind: &conversationsv1.Event_Ran{Ran: ran}})
		default:
			_ = c.publish(p, &conversationsv1.Event{Kind: &conversationsv1.Event_Token{Token: t.Text}})
			if t.Tool != nil {
				_ = c.publish(p, &conversationsv1.Event{Kind: &conversationsv1.Event_Tool{Tool: t.Tool}})
			}
		}
		if i+1 < len(c.turns) && c.turns[i+1].Role != modelsv1.Role_ROLE_USER {
			continue // the turn goes on
		}
		end := &conversationsv1.Event{Kind: &conversationsv1.Event_Done{Done: true}}
		if t.Role != modelsv1.Role_ROLE_ASSISTANT || t.Tool != nil {
			end = &conversationsv1.Event{Kind: &conversationsv1.Event_Failed{Failed: "the turn was not finished"}}
		}
		_ = c.publish(p, end)
	}
}

// answer is one turn: the model answers, and for as long as it asks for a
// tool, the tool is run and the model answers again. A turn that cannot be
// finished is published as failed, and the conversation goes on; an error is
// the history's, and ends the process.
func (c *conversation) answer(p *grpcproc.Process[*conversationsv1.Request], text string) error {
	if err := c.said(p, &modelsv1.Turn{Role: modelsv1.Role_ROLE_USER, Text: text}); err != nil {
		return err
	}
	_ = c.publish(p, &conversationsv1.Event{Kind: &conversationsv1.Event_Said{Said: text}})
	for range 4 { // a model that keeps asking for tools is stopped
		out, err := c.generate(p)
		if err != nil {
			_ = c.publish(p, &conversationsv1.Event{Kind: &conversationsv1.Event_Failed{Failed: err.Error()}})
			return nil
		}
		if err := c.said(p, &modelsv1.Turn{Role: modelsv1.Role_ROLE_ASSISTANT, Text: out.Text, Tool: out.Tool}); err != nil {
			return err
		}
		if out.Tool == nil {
			_ = c.publish(p, &conversationsv1.Event{Kind: &conversationsv1.Event_Done{Done: true}})
			return nil
		}
		ran := c.call(p, out.Tool)
		result := ran.Output
		if ran.Failed != "" {
			result = modelsv1.ToolFailed + ran.Failed
		}
		if err := c.said(p, &modelsv1.Turn{Role: modelsv1.Role_ROLE_TOOL, Text: result}); err != nil {
			return err
		}
	}
	_ = c.publish(p, &conversationsv1.Event{Kind: &conversationsv1.Event_Failed{Failed: "the model asked for too many tools"}})
	return nil
}

// generate has a model answer, on the first node that takes the request. A
// node that is busy says so, and one that cannot be reached never had the
// request: the next is asked. One that is lost with the request, or fails
// it, may have published tokens: if another node is asked, the subscribers
// are first told the answer starts over. With every node busy, it waits for
// a slot, for as long as a model is given to answer.
func (c *conversation) generate(p *grpcproc.Process[*conversationsv1.Request]) (*modelsv1.Generated, error) {
	waited := time.Now().Add(c.limits.Generate)
	lost := "" // the node that took the request and did not finish
	for {
		var errs []error
		busy := false
		for _, node := range c.models {
			if p.Context().Err() != nil { // the conversation is told to exit
				return nil, p.Context().Err()
			}
			// A try has a number larger than the one before it; the time
			// makes it larger than those of the process before this one too.
			c.attempt = max(c.attempt+1, uint64(time.Now().UnixNano()))
			if lost != "" {
				_ = c.publish(p, &conversationsv1.Event{Attempt: c.attempt, Kind: &conversationsv1.Event_Retrying{Retrying: lost + " did not finish"}})
				lost = ""
			}
			// A message of its own for each try: a local send passes the
			// pointer, and the try before may still be reading its own.
			req := &modelsv1.Generate{
				History: c.turns, TopicNode: c.node, Topic: conversationsv1.TopicName(c.id),
				Turn: c.turn, Attempt: c.attempt,
			}
			ctx, cancel := context.WithTimeout(p.Context(), c.limits.Generate)
			out, err := modelsv1.Scheduler(node).Generate(ctx, p, req)
			cancel()
			switch {
			case err != nil:
				errs = append(errs, fmt.Errorf("%s: %w", node, err))
				// It may have taken the request and published tokens, unless
				// the error says the request never left.
				if link, ok := errors.AsType[*grpcproc.LinkError](err); !ok || !link.Unsent {
					lost = node
				}
			case out.Busy:
				errs = append(errs, fmt.Errorf("%s: busy", node))
				busy = true
			default:
				return out, nil
			}
		}
		if !busy || time.Now().After(waited) {
			return nil, fmt.Errorf("no model answered: %w", errors.Join(errs...))
		}
		select {
		case <-p.Context().Done():
			return nil, p.Context().Err()
		case <-time.After(c.limits.Busy):
		}
	}
}

// call runs the tool the model asked for, and says what came of it. A tool
// that fails, or a sandbox that does not answer in time, is an answer too:
// the model is told, and says so.
func (c *conversation) call(p *grpcproc.Process[*conversationsv1.Request], tool *toolsv1.Run) *toolsv1.Ran {
	_ = c.publish(p, &conversationsv1.Event{Kind: &conversationsv1.Event_Tool{Tool: tool}})
	ctx, cancel := context.WithTimeout(p.Context(), c.limits.Tool)
	defer cancel()
	ran, err := c.tools.Run(ctx, p, tool)
	if err != nil {
		ran = &toolsv1.Ran{Failed: "no answer from the sandbox: " + err.Error()}
	}
	_ = c.publish(p, &conversationsv1.Event{Kind: &conversationsv1.Event_Ran{Ran: ran}})
	return ran
}

// said adds a turn to the history, and to what the model is shown.
func (c *conversation) said(p *grpcproc.Process[*conversationsv1.Request], turn *modelsv1.Turn) error {
	if err := c.history.Append(p.Context(), c.id, turn); err != nil {
		return err
	}
	c.turns = append(c.turns, turn)
	return nil
}

// publish sends an event of this turn to the topic, and so to whoever
// follows the conversation.
func (c *conversation) publish(p *grpcproc.Process[*conversationsv1.Request], e *conversationsv1.Event) error {
	e.Turn = c.turn
	return c.events.Publish(p.Context(), p, e)
}

// tree is the service's supervision tree: a supervisor with no children of
// its own, and a factory for the one kind it is asked for. A conversation is
// Transient: started again if it crashes, which loads its history back, and
// left alone once it ends for being idle.
func tree(cfg platform.Config, history History, limits Limits) actor.ChildSpec {
	return actor.ChildSupervisor(conversationsv1.Service, actor.Spec{
		Factories: map[string]actor.Factory{
			conversationsv1.Factory: actor.ChildFactory(func(open *conversationsv1.Open) (actor.ChildSpec, error) {
				if !conversationsv1.ValidID(open.Id) {
					return actor.ChildSpec{}, fmt.Errorf("conversations: %q cannot name a conversation", open.Id)
				}
				c := &conversation{
					id: open.Id, node: cfg.Node, history: history, limits: limits,
					models: cfg.All(modelsv1.Service),
					tools:  toolsv1.Runner(cfg.Where(toolsv1.Service)),
				}
				inspect := grpcproc.WithInspect(func() map[string]string {
					return map[string]string{"turns": strconv.FormatUint(c.turn, 10), "said": strconv.Itoa(len(c.turns))}
				})
				name := conversationsv1.Conversation(cfg.Node, open.Id).Name()
				return actor.ChildFunc(name, c.run, inspect, grpcproc.WithLabel("conversation")).WithRestart(actor.Transient), nil
			}),
		},
	})
}

// Module registers the history, the limits, and adds the tree to the node's
// root.
func Module(s *di.Scope) {
	s.Wire[History](newMemory)
	s.Value(Limits{Idle: 10 * time.Minute, Generate: time.Minute, Tool: 5 * time.Second, Busy: 100 * time.Millisecond})
	s.Wire[actor.ChildSpec](tree).Group()
}
