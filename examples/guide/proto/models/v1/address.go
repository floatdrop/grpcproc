package modelsv1

import (
	"context"

	"github.com/floatdrop/grpcproc"
)

const (
	// Service is the models service's name in a deployment's placement.
	Service = "models"
	// SchedulerName is the name a GPU node's scheduler is registered under.
	SchedulerName = "models"
	// ToolFailed begins the text of a tool's turn when the tool failed: what
	// follows is why.
	ToolFailed = "failed: "
)

// SchedulerAddr addresses a GPU node's scheduler; Generate is its protocol.
type SchedulerAddr struct{ grpcproc.Addr[*Generate] }

// Scheduler addresses the scheduler on node, one of the nodes that run the
// models service.
func Scheduler(node string) SchedulerAddr {
	return SchedulerAddr{Addr: grpcproc.Named[*Generate](node, SchedulerName)}
}

// Generate has the model answer, and returns once it is done; the tokens
// are published meanwhile on the topic g names. A node with no slot free
// answers Busy; an error means no answer: the node is gone, or ctx ended.
func (s SchedulerAddr) Generate(ctx context.Context, from grpcproc.Caller, g *Generate) (*Generated, error) {
	return s.Call[*Generated](ctx, from, g)
}
