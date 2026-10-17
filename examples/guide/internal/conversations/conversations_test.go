package conversations_test

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.yandex/di"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/conversations"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	conversationsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/conversations/v1"
	modelsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/models/v1"
	toolsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/tools/v1"
	"github.com/floatdrop/grpcproc/groups"
)

// The conversations alone. Their collaborators are addresses, so a test
// puts fakes behind them: processes registered under the names the
// contracts give, on the node the placement points at, which with none is
// this one, and a scheduler in the models group. What the model is asked
// comes out on a channel, for the test to read.
func service(t *testing.T, limits conversations.Limits, model func(*modelsv1.Generate) *modelsv1.Generated) (*grpcproc.Node, <-chan *modelsv1.Generate) {
	t.Helper()
	s := di.Test(t)
	platform.Compose(s, platform.Config{Node: "local", Listen: "127.0.0.1:0"}, slog.New(slog.DiscardHandler), conversations.Module)
	s.Value(limits).Override()
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := s.Get[*grpcproc.Node]()
	asked := make(chan *modelsv1.Generate, 8)
	scheduler := func(p *grpcproc.Process[*modelsv1.Generate]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			asked <- m.Body
			_ = m.Reply(model(m.Body), nil)
		}
	}
	a, err := n.Spawn(scheduler, grpcproc.WithName(modelsv1.SchedulerProcess))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Get[*groups.Scope]().Join(t.Context(), n, modelsv1.Group, a.PID()); err != nil {
		t.Fatal(err)
	}
	// A sandbox that takes every call and answers none.
	runner := func(p *grpcproc.Process[*toolsv1.Run]) error {
		for {
			if _, err := p.Receive(); err != nil {
				return err
			}
		}
	}
	if _, err := n.Spawn(runner, grpcproc.WithName(toolsv1.RunnerProcess)); err != nil {
		t.Fatal(err)
	}
	return n, asked
}

func next(t *testing.T, asked <-chan *modelsv1.Generate) *modelsv1.Generate {
	t.Helper()
	select {
	case g := <-asked:
		return g
	case <-time.After(5 * time.Second):
		t.Fatal("the model was not asked")
		return nil
	}
}

// A tool that does not answer in time is an answer: the model is asked
// again, and told.
func TestAToolWithNoAnswerIsToldToTheModel(t *testing.T) {
	limits := conversations.Limits{Idle: time.Minute, Generate: time.Second, Tool: 50 * time.Millisecond, Busy: time.Millisecond}
	n, asked := service(t, limits, func(g *modelsv1.Generate) *modelsv1.Generated {
		if last := g.History[len(g.History)-1]; last.Role == modelsv1.Role_ROLE_TOOL {
			return &modelsv1.Generated{Text: "It did not answer."}
		}
		return &modelsv1.Generated{Text: "One moment.", Tool: &toolsv1.Run{Name: "sleep", Args: "1h"}}
	})
	if _, err := conversationsv1.ConversationAt("local", "1").Say(t.Context(), n, "wait an hour"); err != nil {
		t.Fatal(err)
	}
	next(t, asked)
	last := next(t, asked).History[2] // the user, the model, the tool
	if last.Role != modelsv1.Role_ROLE_TOOL || !strings.Contains(last.Text, "no answer from the sandbox") {
		t.Fatalf("the model was told %v", last)
	}
}

// A conversation nobody speaks to ends its process, and the next thing said
// starts one that knows what was said before.
func TestAnIdleConversationEndsAndComesBack(t *testing.T) {
	limits := conversations.Limits{Idle: 50 * time.Millisecond, Generate: time.Second, Tool: time.Second, Busy: time.Millisecond}
	n, asked := service(t, limits, func(*modelsv1.Generate) *modelsv1.Generated {
		return &modelsv1.Generated{Text: "Noted."}
	})
	c := conversationsv1.ConversationAt("local", "1")
	if said, err := c.Say(t.Context(), n, "one"); err != nil || said.Turn != 1 {
		t.Fatalf("%v %v", said, err)
	}
	next(t, asked)
	// A call of a type the conversation has no rpc for is answered ErrType,
	// as the node answered it when the mailbox held one type, and does not
	// keep the conversation from going idle.
	if _, err := n.CallTo[*conversationsv1.Said](t.Context(), c.ConversationAddr, &conversationsv1.Open{Id: "1"}); !errors.Is(err, grpcproc.ErrType) {
		t.Fatalf("a stranger's call: %v", err)
	}
	for range 500 {
		if _, running := n.Whereis(c.Name()); !running {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, running := n.Whereis(c.Name()); running {
		t.Fatal("the conversation is still running")
	}
	if said, err := c.Say(t.Context(), n, "two"); err != nil || said.Turn != 2 {
		t.Fatalf("%v %v", said, err)
	}
	if h := next(t, asked).History; len(h) != 3 || h[0].Text != "one" || h[1].Text != "Noted." || h[2].Text != "two" {
		t.Fatalf("the model was shown %v", h)
	}
}

// With every node busy the conversation waits for a slot, and asks again.
func TestABusyModelIsAskedAgain(t *testing.T) {
	limits := conversations.Limits{Idle: time.Minute, Generate: time.Second, Tool: time.Second, Busy: time.Millisecond}
	asks := 0
	n, asked := service(t, limits, func(*modelsv1.Generate) *modelsv1.Generated {
		asks++ // by the one process that is the model here
		return &modelsv1.Generated{Text: "Here.", Busy: asks < 3}
	})
	if _, err := conversationsv1.ConversationAt("local", "1").Say(t.Context(), n, "anyone?"); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		next(t, asked)
	}
}

// A conversation whose process crashed is started again on what the history
// has: its turns are counted from there, and whoever follows it is sent the
// conversation so far, as the events it was.
func TestARestartedConversationReplaysItsHistory(t *testing.T) {
	limits := conversations.Limits{Idle: time.Minute, Generate: time.Second, Tool: 20 * time.Millisecond, Busy: time.Millisecond}
	n, asked := service(t, limits, func(g *modelsv1.Generate) *modelsv1.Generated {
		switch last := g.History[len(g.History)-1]; {
		case last.Role == modelsv1.Role_ROLE_TOOL:
			return &modelsv1.Generated{Text: "It did not answer."}
		case last.Text == "and again":
			<-t.Context().Done() // no answer while the test runs
		}
		return &modelsv1.Generated{Text: "One moment.", Tool: &toolsv1.Run{Name: "sleep", Args: "1h"}}
	})
	c := conversationsv1.ConversationAt("local", "1")
	if _, err := c.Say(t.Context(), n, "wait an hour"); err != nil {
		t.Fatal(err)
	}
	next(t, asked)
	next(t, asked)
	// Its second turn is cut short: the model is asked, and the process is
	// told to exit while it waits.
	if _, err := c.Say(t.Context(), n, "and again"); err != nil {
		t.Fatal(err)
	}
	next(t, asked)
	before, _ := n.Whereis(c.Name())
	if err := n.Exit(t.Context(), c, "crashed"); err != nil {
		t.Fatal(err)
	}
	for range 500 {
		if now, running := n.Whereis(c.Name()); running && now != before {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	events := make(chan *conversationsv1.Event, 16)
	if _, err := n.Spawn(func(p *grpcproc.Process[*conversationsv1.Event]) error {
		if _, err := c.Follow(p.Context(), p); err != nil {
			return err
		}
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Body != nil {
				events <- m.Body
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for len(kinds) < 8 {
		select {
		case e := <-events:
			kinds = append(kinds, strings.TrimPrefix(fmt.Sprintf("%T", e.Kind), "*conversationsv1.Event_"))
		case <-time.After(5 * time.Second):
			t.Fatalf("replayed %v", kinds)
		}
	}
	if want := []string{"Said", "Token", "Tool", "Ran", "Token", "Done", "Said", "Failed"}; !slices.Equal(kinds, want) {
		t.Fatalf("replayed %v", kinds)
	}
	if said, err := c.Say(t.Context(), n, "three"); err != nil || said.Turn != 3 {
		t.Fatalf("%v %v", said, err)
	}
}

// A request that says nothing is refused, and is no turn.
func TestARequestThatSaysNothingIsRefused(t *testing.T) {
	limits := conversations.Limits{Idle: time.Minute, Generate: time.Second, Tool: time.Second, Busy: time.Millisecond}
	n, _ := service(t, limits, func(*modelsv1.Generate) *modelsv1.Generated { return &modelsv1.Generated{Text: "Noted."} })
	c := conversationsv1.ConversationAt("local", "1")
	if _, err := c.Say(t.Context(), n, ""); err == nil || !strings.Contains(err.Error(), "nothing was said") {
		t.Fatalf("got %v", err)
	}
	if said, err := c.Say(t.Context(), n, "one"); err != nil || said.Turn != 1 {
		t.Fatalf("%v %v", said, err)
	}
}

// With no node in the models group, a turn waits as if every node were
// busy, then fails, and says why.
func TestNoNodeRunsAModel(t *testing.T) {
	s := di.Test(t)
	platform.Compose(s, platform.Config{Node: "local", Listen: "127.0.0.1:0"}, slog.New(slog.DiscardHandler), conversations.Module)
	s.Value(conversations.Limits{Idle: time.Minute, Generate: 50 * time.Millisecond, Tool: time.Second, Busy: time.Millisecond}).Override()
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := s.Get[*grpcproc.Node]()
	c := conversationsv1.ConversationAt("local", "1")
	failed := make(chan string, 1)
	if _, err := n.Spawn(func(p *grpcproc.Process[*conversationsv1.Event]) error {
		if _, err := c.Follow(p.Context(), p); err != nil {
			return err
		}
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if f := m.Body.GetFailed(); f != "" {
				failed <- f
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Say(t.Context(), n, "anyone?"); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-failed:
		if !strings.Contains(f, "no node runs a model") {
			t.Fatalf("failed with %q", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the turn did not fail")
	}
}
