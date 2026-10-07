// Package testcluster builds the cluster the tool tests look at.
package testcluster

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/fsm"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/cron"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/inspect"
	"github.com/floatdrop/grpcproc/internal/testpb"
	"github.com/floatdrop/grpcproc/leader"
	"github.com/floatdrop/grpcproc/saga"
)

// Fixture is what Start leaves running on node "a".
type Fixture struct {
	C        *grpcproctest.Cluster
	Sup      grpcproc.PID // a supervisor with children "w1" and "w2"
	Stuck    grpcproc.PID // "stuck": busy in a handler, 3 messages waiting
	Talker   grpcproc.PID // "talker": publishes state=ready through WithInspect, holds the global name "room:talker"
	Echo     grpcproc.PID // "echo" on node "b"
	Release  func()       // unblocks "stuck"
	Resolver grpcproc.Resolver
}

func worker(p *grpcproc.Process[*testpb.Ping]) error {
	for {
		if _, err := p.Receive(); err != nil {
			return err
		}
	}
}

// members is a Membership that reports each of its nodes up once.
type members []string

func (ms members) Watch(ctx context.Context) (<-chan grpcproc.MemberEvent, error) {
	ch := make(chan grpcproc.MemberEvent, len(ms))
	for _, name := range ms {
		ch <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: name, Addr: name}, Up: true}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

// unlisted is a store of global names that cannot list them.
type unlisted struct{ grpcproc.Names }

// Start runs nodes a and b (and any others named) with Inspectors that
// forward to each other, and links a to b. Node a backs off from a peer whose
// dials fail (Config.DialBackoff), so a test can show it a down link, and
// says version=1.0 in its metadata. A node named "nonames" has no global
// names, and one named "unlisted" has names it cannot list. With a node named
// "quiet", which nothing links to, a's Membership reports every node up,
// and "ghost", which is not running.
func Start(t *testing.T, more ...string) *Fixture {
	t.Helper()
	names := append([]string{"a", "b"}, more...)
	// Each Inspector reaches the others as its node does, through the node's
	// Dial (so a Partition cuts it off too).
	c := grpcproctest.NewWith(t, []grpcproctest.Option{
		grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
			switch name {
			case "a":
				cfg.DialBackoff = time.Hour
				cfg.Metadata = map[string]string{"version": "1.0"}
				if slices.Contains(names, "quiet") {
					cfg.Membership = members(append(slices.Clone(names), "ghost"))
				}
			case "nonames":
				cfg.Names = nil
			case "unlisted":
				cfg.Names = unlisted{cfg.Names}
			}
		}),
		grpcproctest.WithServices(func(n *grpcproc.Node, s *grpc.Server) {
			insp := inspect.New(n)
			insp.Register(s)
			t.Cleanup(func() { _ = insp.Close() })
		}),
	}, names...)
	a, b := c.Node("a"), c.Node("b")

	f := &Fixture{C: c, Resolver: c.Resolver()}
	var err error
	f.Sup, err = actor.Supervise(a, actor.Spec{Children: []actor.ChildSpec{
		actor.ChildFunc("w1", worker), actor.ChildFunc("w2", worker),
	}}, grpcproc.WithName("sup"))
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	f.Release = func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	t.Cleanup(f.Release)
	stuck, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		if _, err := p.Receive(); err != nil {
			return err
		}
		<-release
		return worker(p)
	}, grpcproc.WithName("stuck"), grpcproc.WithLabel("stuck"))
	f.Stuck = stuck.PID()
	for range 4 {
		_ = stuck.Send(t.Context(), a, &testpb.Ping{})
	}
	claimed := make(chan error, 1)
	talker, _ := a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		_, err := p.Claim(p.Context(), "room:talker")
		claimed <- err
		for {
			if _, err := p.Receive(); err != nil {
				return err
			}
		}
	}, grpcproc.WithName("talker"), grpcproc.WithInspect(func() map[string]string { return map[string]string{"state": "ready"} }))
	f.Talker = talker.PID()
	if err := <-claimed; err != nil {
		t.Fatal(err)
	}
	echo, _ := b.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.IsCall() {
				_ = m.Reply(&testpb.Pong{}, nil)
			}
		}
	}, grpcproc.WithName("echo"))
	f.Echo = echo.PID()
	if _, err := echo.Call[*testpb.Pong](t.Context(), a, &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // the stuck process takes its first message
	return f
}

// Elect runs a grpcproc/leader election called cluster among nodes, which
// are its voters, with short timeouts and an idle singleton, and waits until
// one of them leads.
func (f *Fixture) Elect(t *testing.T, cluster string, nodes ...string) {
	t.Helper()
	spec := leader.Spec[proto.Message]{
		Cluster:           cluster,
		Voters:            nodes,
		ElectionTimeout:   100 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
		Singleton: func(*leader.Lease[proto.Message], proto.Message) (actor.ChildSpec, error) {
			return actor.ChildFunc("singleton", worker), nil
		},
	}
	for _, n := range nodes {
		if _, err := leader.Start(f.C.Node(n), spec); err != nil {
			t.Fatal(err)
		}
	}
	f.Leading(t, cluster, nodes[0], func(lead string) bool { return lead != "" })
}

// Leading waits until node's elector for cluster names a leader ok
// accepts, and returns it.
func (f *Fixture) Leading(t *testing.T, cluster, node string, ok func(string) bool) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		info, err := leader.Status(t.Context(), f.C.Node(node), cluster)
		if err == nil && info.Leader != "" && ok(info.Leader) {
			return info.Leader
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: no leader as wanted: %+v, %v", cluster, info, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Cron starts a grpcproc/cron process registered as name on node, with
// jobs that do not come due while a test runs: "yearly", "leap" (February
// 29) and "paused", disabled.
func (f *Fixture) Cron(t *testing.T, node, name string) grpcproc.PID {
	t.Helper()
	nothing := cron.Func(func(context.Context, cron.Run) error { return nil })
	c, err := cron.Start(f.C.Node(node), cron.Spec{Jobs: []cron.Job{
		{Name: "yearly", Spec: "@yearly", Action: nothing},
		{Name: "leap", Spec: "0 0 29 2 *", Action: nothing},
		{Name: "paused", Spec: "0 * * * *", Action: nothing, Disabled: true},
	}}, grpcproc.WithName(name))
	if err != nil {
		t.Fatal(err)
	}
	return c.Addr().PID()
}

// Saga runs a grpcproc/saga engine on node, for a saga called name, from a
// store of its own with three runs no engine works on: 1 waits; 2 is stuck, with two
// signals it keeps, one of an event the saga does not accept; 3 is done,
// with data too large to show.
func (f *Fixture) Saga(t *testing.T, node, name string) *saga.Engine {
	t.Helper()
	return f.saga(t, node, name, true)
}

// SagaHidingData is Saga with an engine that shows no run's data.
func (f *Fixture) SagaHidingData(t *testing.T, node, name string) *saga.Engine {
	t.Helper()
	return f.saga(t, node, name, false)
}

func (f *Fixture) saga(t *testing.T, node, name string, inspect bool) *saga.Engine {
	t.Helper()
	paid := fsm.Define[*wrapperspb.StringValue]("paid")
	m := fsm.MustNew(name, fsm.Initial("charging"), fsm.From("charging").On(paid).To("done"))
	orders := saga.Define[*wrapperspb.StringValue](name, m).Accept(paid, nil).Version(2)
	store := saga.Memory()
	ctx := t.Context()
	for _, r := range []saga.Record{
		{Saga: name, ID: "1", State: "charging", Data: encode(wrapperspb.String("apples"))},
		{Saga: name, ID: "2", State: "charging", Status: saga.Stuck, Error: "card declined", Attempts: 3},
		{Saga: name, ID: "3", State: "done", Status: saga.Done, Data: encode(wrapperspb.String(strings.Repeat("x", 2<<20)))},
	} {
		if _, _, err := store.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	e, err := saga.Start(f.C.Node(node), saga.Config{Store: store, InspectData: inspect}, orders)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []saga.Signal{{Event: "paid", Payload: encode(wrapperspb.String("42"))}, {Event: "refund"}} {
		if err := store.Signal(ctx, name, "2", s); err != nil {
			t.Fatal(err)
		}
	}
	return e
}

func encode(m proto.Message) []byte {
	b, _ := proto.Marshal(m)
	return b
}
