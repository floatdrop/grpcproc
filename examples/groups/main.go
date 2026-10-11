// Process groups: workers on two nodes join a group, and a third node finds
// them, sends each a job, and watches the group change as a worker exits
// and a node stops. Three nodes, in one binary, on loopback.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/groups"
	groupsv1 "github.com/floatdrop/grpcproc/groups/proto/grpcproc/groups/v1"
)

// worker resizes the photos it is called with. It joins the group "resize"
// on its own node's scope, and is in it until it exits.
func worker(scope *groups.Scope, n int) func(*grpcproc.Process[*wrapperspb.StringValue]) error {
	return func(p *grpcproc.Process[*wrapperspb.StringValue]) error {
		if err := scope.Join(p.Context(), p, "resize", p.PID()); err != nil {
			return err
		}
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			done := fmt.Sprintf("%s resized by worker %d on %s", m.Body.GetValue(), n, p.Node().Name())
			_ = m.Reply(wrapperspb.String(done), nil)
		}
	}
}

// watcher prints the first change to the group "resize", then stops
// watching. Its mailbox takes any message, *groupsv1.Changed among them.
func watcher(scope *groups.Scope, watching, done chan<- struct{}) func(*grpcproc.Process[proto.Message]) error {
	return func(p *grpcproc.Process[proto.Message]) error {
		defer close(done)
		w, err := scope.Watch(p.Context(), p, "resize")
		if err != nil {
			return err
		}
		close(watching)
		members := len(w.Members)
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if c, ok := m.Body.(*groupsv1.Changed); ok {
				members += len(c.GetJoined()) - len(c.GetLeft())
				for _, pid := range c.GetLeft() {
					fmt.Printf("watcher: a worker on %s left, %d remain\n", pid.GetNode(), members)
				}
				w.Cancel(p)
				return nil
			}
		}
	}
}

func main() {
	ctx := context.Background()
	names := []string{"dispatch", "east", "west"}
	nodes, scopes := map[string]*grpcproc.Node{}, map[string]*groups.Scope{}
	for _, name := range names {
		nodes[name] = node(ctx, name)
	}
	// Every node runs the scope, and asks the others for their members.
	for _, name := range names {
		var err error
		scopes[name], err = groups.Start(nodes[name], groups.Config{Peers: slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n == name })})
		check(err)
	}

	// Two workers on each of east and west.
	var crashy grpcproc.PID
	for _, name := range []string{"east", "west"} {
		for n := 1; n <= 2; n++ {
			a, err := nodes[name].Spawn(worker(scopes[name], n))
			check(err)
			crashy = a.PID()
		}
	}

	// The dispatch node finds them in its own copy of the group, and sends
	// each a photo.
	resize := scopes["dispatch"]
	await(resize, 4)
	members := resize.Members("resize")
	fmt.Printf("workers: %d, on %s\n", len(members), nodesOf(members))
	for i, pid := range members {
		worker := grpcproc.AddrOf[*wrapperspb.StringValue](pid)
		photo := wrapperspb.String(fmt.Sprintf("photo-%d.jpg", i+1))
		done, err := worker.Call[*wrapperspb.StringValue](ctx, nodes["dispatch"], photo)
		check(err)
		fmt.Println(done.GetValue())
	}

	// A watcher hears of a worker that exits.
	watching, watched := make(chan struct{}), make(chan struct{})
	_, err := nodes["dispatch"].Spawn(watcher(resize, watching, watched))
	check(err)
	<-watching
	check(nodes["west"].Exit(ctx, crashy, "crash"))
	<-watched

	// A node that stops takes its workers with it.
	check(nodes["east"].Stop(ctx))
	await(resize, 1)
	left := resize.Members("resize")
	fmt.Printf("after east stopped: %d, on %s\n", len(left), nodesOf(left))

	check(nodes["west"].Stop(ctx))
	check(nodes["dispatch"].Stop(ctx))
}

// await waits until scope's copy of the group "resize" has n members.
func await(scope *groups.Scope, n int) {
	for deadline := time.Now().Add(5 * time.Second); len(scope.Members("resize")) != n; {
		if time.Now().After(deadline) {
			log.Fatalf("the group has %d members, not %d", len(scope.Members("resize")), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func nodesOf(pids []grpcproc.PID) string {
	var out []string
	for _, pid := range pids {
		out = append(out, pid.Node)
	}
	return strings.Join(out, ", ")
}

// peers is where each node's gRPC server listens: a static address book
// here, grpcproc/etcd in a real cluster.
var peers = grpcproc.StaticResolver{}

// node runs a node on its own gRPC server.
func node(ctx context.Context, name string) *grpcproc.Node {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	check(err)
	peers[name] = lis.Addr().String()

	n, err := grpcproc.NewNode(grpcproc.Config{
		Admit:       grpcproc.AdmitAll,
		Name:        name,
		Resolver:    peers,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	check(err)
	srv := grpc.NewServer()
	n.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	check(n.Start(ctx))
	return n
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
