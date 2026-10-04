// Package tools is the service that runs the tools a model asks for, on a
// node kept apart from the others: what a tool does is decided by what a
// model wrote. Its runner starts a process for every call, so a tool that
// crashes, or does not return, costs that call and nothing else. The package
// exports its Module and the Tools it runs.
package tools

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.yandex/di"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	toolsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/tools/v1"
)

// Tool is one tool: it returns what it makes of args, within ctx.
type Tool func(ctx context.Context, args string) (string, error)

// Tools is the tools the runner has, by name.
type Tools map[string]Tool

// builtin is the guide's tools: a calculator, which crashes on a division
// by zero, and one that waits, for as long as it is told or allowed.
func builtin() Tools { return Tools{"calc": calc, "sleep": sleep} }

func calc(_ context.Context, args string) (string, error) {
	f := strings.Fields(args)
	if len(f) != 3 {
		return "", fmt.Errorf("calc wants a number, an operator and a number, not %q", args)
	}
	a, errA := strconv.ParseInt(f[0], 10, 64)
	b, errB := strconv.ParseInt(f[2], 10, 64)
	if err := errors.Join(errA, errB); err != nil {
		return "", err
	}
	switch f[1] {
	case "+":
		a += b
	case "-":
		a -= b
	case "*":
		a *= b
	case "/":
		a /= b // by zero, it panics: the process of this call ends, with that as its reason
	default:
		return "", fmt.Errorf("calc does not know %q", f[1])
	}
	return strconv.FormatInt(a, 10), nil
}

func sleep(ctx context.Context, args string) (string, error) {
	d, err := time.ParseDuration(args)
	if err != nil {
		return "", err
	}
	select {
	case <-time.After(d):
		return "waited " + d.String(), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// runner is the actor. Like the models' scheduler, it hands each call to a
// process started for it, and answers for one that ended without answering:
// a tool that failed is part of the answer, since the model is told of it.
type runner struct {
	actor.CallsOnly[*toolsv1.Run]
	tools   Tools
	running map[grpcproc.Ref]grpcproc.Msg[*toolsv1.Run]
}

func (r *runner) HandleCall(p *grpcproc.Process[*toolsv1.Run], m grpcproc.Msg[*toolsv1.Run]) (proto.Message, error) {
	tool, ok := r.tools[m.Body.Name]
	if !ok {
		return &toolsv1.Ran{Failed: "no tool named " + m.Body.Name}, nil
	}
	_, ref, err := p.SpawnMonitor(func(c *grpcproc.Process[proto.Message]) error {
		// The caller's deadline is the tool's. A goroutine cannot be
		// killed: a tool that does not watch ctx runs on after it.
		ctx, cancel := m.Context(c.Context())
		defer cancel()
		out, err := tool(ctx, m.Body.Args)
		if err != nil {
			return m.Reply(&toolsv1.Ran{Failed: err.Error()}, nil)
		}
		return m.Reply(&toolsv1.Ran{Output: out}, nil)
	}, grpcproc.WithLabel("tool:"+m.Body.Name), grpcproc.LinkParent())
	if err != nil {
		return nil, err
	}
	r.running[ref] = m
	return nil, actor.ErrNoReply // the call's process answers
}

// HandleDown answers the call of a process that ended without answering: a
// tool that panicked. Its reason is the answer.
func (r *runner) HandleDown(_ *grpcproc.Process[*toolsv1.Run], d grpcproc.Down) error {
	m := r.running[d.Ref]
	delete(r.running, d.Ref)
	if d.Reason != grpcproc.ReasonNormal {
		_ = m.Reply(&toolsv1.Ran{Failed: d.Reason}, nil)
	}
	return nil
}

func tree(tools Tools) actor.ChildSpec {
	return actor.ChildSupervisor(toolsv1.Service+"-sup", actor.Spec{
		Children: []actor.ChildSpec{
			actor.Child(toolsv1.RunnerName, func() *runner {
				return &runner{tools: tools, running: map[grpcproc.Ref]grpcproc.Msg[*toolsv1.Run]{}}
			}),
		},
	})
}

// Module registers the tools and adds the tree to the node's root.
func Module(s *di.Scope) {
	s.Wire[Tools](builtin)
	s.Wire[actor.ChildSpec](tree).Group()
}

var _ actor.DownHandler[*toolsv1.Run] = (*runner)(nil)
