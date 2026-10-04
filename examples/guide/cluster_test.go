package guide

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"

	"golang.yandex/di"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/models"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	conversationsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/conversations/v1"
	modelsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/models/v1"
	toolsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/tools/v1"
)

// stalling is a model that says two words and nothing more: a GPU node
// about to be lost.
type stalling struct{}

func (stalling) Generate(ctx context.Context, _ []*modelsv1.Turn, emit func(string) error) (*toolsv1.Run, error) {
	if err := emit("Let me think"); err != nil {
		return nil, err
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// The distributed deployment: a gateway, two GPU nodes and a sandbox, the
// same modules split between them, and the configuration a deployment would
// give each. gpu-1 is lost in the middle of an answer.
func TestCluster(t *testing.T) {
	stall := func(s *di.Scope) { s.Value[models.Model](stalling{}).Override() }
	nodes := []struct {
		name     string
		services []di.Module
		more     []di.Module
		ln       net.Listener
		app      *di.Scope
	}{
		{name: "gateway", services: gateway},
		{name: "gpu-1", services: gpu, more: []di.Module{stall}},
		{name: "gpu-2", services: gpu, more: []di.Module{fast}},
		{name: "sandbox", services: sandbox},
	}
	// Each node's port is bound first, so every node can be told the others'.
	peers := map[string]string{}
	for i := range nodes {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		nodes[i].ln, peers[nodes[i].name] = ln, ln.Addr().String()
	}
	apps := map[string]*di.Scope{}
	for i := range nodes {
		n := &nodes[i]
		cfg := platform.Config{Node: n.name, Listen: peers[n.name], Peers: peers}
		switch n.name {
		case "gateway":
			// Where the other services run, and whom not to trust.
			cfg.Placement = map[string]string{modelsv1.Service: "gpu-1+gpu-2", toolsv1.Service: "sandbox"}
			cfg.Untrusted = []string{"sandbox"}
		case "sandbox":
			cfg.Export = []string{toolsv1.RunnerName}
		default:
			cfg.Export = []string{modelsv1.SchedulerName}
			cfg.Untrusted = []string{"sandbox"}
		}
		n.app = compose(cfg, n.services, n.more...)
		n.app.Value(n.ln).Override() // the port bound above
		start(t, n.app)
		apps[n.name] = n.app
	}
	front := apps["gateway"]
	events := follow(t, front.Get[*grpcproc.Node](), "gateway", "1")

	if code, body := say(t, front, "1", "what is 6 * 7?"); code != http.StatusAccepted {
		t.Fatalf("%d %s", code, body)
	}
	// gpu-1 has begun the answer: what the nodes run now is the drawing the
	// guide shows, a generation in flight included.
	if said, token := <-events, <-events; said.GetSaid() == "" || token.GetToken() != "Let me think" {
		t.Fatalf("got %v, %v", said, token)
	}
	var trees []string
	for _, n := range nodes {
		trees = append(trees, tree(n.app.Get[*grpcproc.Node]()))
	}
	golden(t, "cluster.txt", strings.Join(trees, "\n"))

	// gpu-1 is lost. The conversation's call fails, and it asks gpu-2, which
	// starts the answer over; the tool runs in the sandbox.
	if err := apps["gpu-1"].Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	chat := "> what is 6 * 7?\nLet me think\n" + transcript(t, events, 1)
	// From then on gpu-1 is asked first and found gone, at once.
	if code, body := say(t, front, "1", "thanks"); code != http.StatusAccepted {
		t.Fatalf("%d %s", code, body)
	}
	chat += transcript(t, events, 1)
	golden(t, "failover.txt", chat)

	// The sandbox answers the gateway, and may ask nothing of it or of the
	// GPU nodes: to the sandbox, their processes do not exist.
	denied := make(chan error, 2)
	if _, err := apps["sandbox"].Get[*grpcproc.Node]().Spawn(func(p *grpcproc.Process[proto.Message]) error {
		_, err := conversationsv1.Conversation("gateway", "1").Say(p.Context(), p, "let me in")
		denied <- err
		// Nor a GPU node's scheduler, which would publish where it is told.
		_, err = modelsv1.Scheduler("gpu-2").Generate(p.Context(), p, &modelsv1.Generate{})
		denied <- err
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-denied; !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("the sandbox was answered %v", err)
		}
	}
}
