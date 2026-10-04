package toolsv1

import (
	"context"

	"github.com/floatdrop/grpcproc"
)

const (
	// Service is the tools service's name in a deployment's placement.
	Service = "tools"
	// RunnerName is the name its runner process is registered under.
	RunnerName = "tools"
)

// RunnerAddr addresses the runner; Run is its protocol.
type RunnerAddr struct{ grpcproc.Addr[*Run] }

// Runner addresses the runner on node, the node that runs the tools service.
func Runner(node string) RunnerAddr {
	return RunnerAddr{Addr: grpcproc.Named[*Run](node, RunnerName)}
}

// Run calls a tool, and waits for it no longer than ctx allows. A tool that
// fails is part of the answer; an error means no answer.
func (r RunnerAddr) Run(ctx context.Context, from grpcproc.Caller, run *Run) (*Ran, error) {
	return r.Call[*Ran](ctx, from, run)
}
