// Package tools is the service that runs the tools a model asks for, on a
// node kept apart from the others: what a tool does is decided by what a
// model wrote. Its runner starts a process for every call, so a tool that
// crashes costs that call and nothing else; one that does not return holds
// one of the runner's places until the node stops. The package exports its
// Module and the Tools it runs.
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
		a /= b // by zero, it panics: the worker of this call answers with that, and ends
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

// runner is the actor. It hands each call to a worker, a process started
// for it, and is free again at once; the worker's answer is the call's. A
// tool that failed is part of the answer, since the model is told of it,
// and so is one that panicked: the worker answers with the panic.
type runner struct {
	actor.CallsOnly[*toolsv1.Run]
	tools   Tools
	workers *actor.Workers
}

func (r *runner) HandleCall(p *grpcproc.Process[*toolsv1.Run], m grpcproc.Msg[*toolsv1.Run]) (proto.Message, error) {
	tool, ok := r.tools[m.Body.Name]
	if !ok {
		return &toolsv1.Ran{Failed: "no tool named " + m.Body.Name}, nil
	}
	return nil, r.workers.Label("tool:"+m.Body.Name).ReplyLater(p, m, func(ctx context.Context, _ *grpcproc.Process[proto.Message]) (*toolsv1.Ran, error) {
		// The caller's deadline is the tool's. A goroutine cannot be
		// killed: a tool that does not watch ctx runs on after it, in its
		// worker's place.
		out, err := tool(ctx, m.Body.Args)
		if err != nil {
			return &toolsv1.Ran{Failed: err.Error()}, nil
		}
		return &toolsv1.Ran{Output: out}, nil
	})
}

// running is how many tools the runner runs at once; a call beyond them is
// answered actor.ErrWorkersBusy.
const running = 16

// tree is the service's supervision tree.
func tree(tools Tools) actor.ChildSpec {
	return actor.ChildSupervisor(toolsv1.Service+"-sup", actor.Spec{
		Children: []actor.ChildSpec{
			actor.Child(toolsv1.RunnerName, func() *runner {
				return &runner{tools: tools, workers: actor.NewWorkers(running)}
			}),
		},
	})
}

// Module registers the tools and adds the tree to the node's root.
func Module(s *di.Scope) {
	s.Wire[Tools](builtin)
	s.Wire[actor.ChildSpec](tree).Group()
}
