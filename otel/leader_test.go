package grpcprocotel_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/leader"
	grpcprocotel "github.com/floatdrop/grpcproc/otel"
)

func idle(*leader.Lease[*emptypb.Empty], *emptypb.Empty) (actor.ChildSpec, error) {
	return actor.ChildFunc("singleton", func(p *grpcproc.Process[proto.Message]) error {
		for {
			if _, err := p.Receive(); err != nil {
				return err
			}
		}
	}), nil
}

// Each node's gauges say what its elector believes: one leader among them,
// running the singleton, in the term they all agree on, and a view that
// shrinks to what the leader hears from once a node is gone.
func TestObserveLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b", "c")
		envs := map[string]env{}
		for _, name := range []string{"a", "b", "c"} {
			e := setup(t)
			reg, err := e.hooks.ObserveLeader(c.Node(name))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reg.Unregister() })
			if _, err := leader.Start(c.Node(name), leader.Spec[*emptypb.Empty]{Cluster: "test", Voters: []string{"a", "b", "c"}, Singleton: idle}); err != nil {
				t.Fatal(err)
			}
			envs[name] = e
		}
		// None is an elector: one is not labelled as one, one publishes
		// nothing, and one exits when asked.
		for _, opts := range [][]grpcproc.SpawnOption{
			{grpcproc.WithName(leader.ElectorName("unlabelled"))},
			{grpcproc.WithName(leader.ElectorName("silent")), grpcproc.WithLabel("leader")},
			{grpcproc.WithName(leader.ElectorName("exits")), grpcproc.WithLabel("leader"), grpcproc.WithInspect(func() map[string]string { panic("asked") })},
		} {
			if _, err := c.Node("a").Spawn(func(p *grpcproc.Process[proto.Message]) error {
				_, err := p.Receive()
				return err
			}, opts...); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		for _, other := range []string{"unlabelled", "silent", "exits"} {
			rm := collect(t, envs["a"].reader)
			if got := intValue(rm, "grpcproc.leader.electors", grpcprocotel.AttrCluster.String(other)) + intValue(rm, "grpcproc.leader.stalled", grpcprocotel.AttrCluster.String(other)); got != 0 {
				t.Errorf("%s observed as an elector: %d", other, got)
			}
		}

		cluster := grpcprocotel.AttrCluster.String("test")
		gauge := func(node, name string, attrs ...attribute.KeyValue) int64 {
			return intValue(collect(t, envs[node].reader), name, append(attrs, cluster)...)
		}
		info, err := leader.Status(t.Context(), c.Node("a"), "test")
		if err != nil || info.Leader == "" {
			t.Fatalf("status %+v, %v", info, err)
		}
		for _, node := range []string{"a", "b", "c"} {
			role := leader.Follower
			if node == info.Leader {
				role = leader.Leader
			}
			running := int64(0)
			if role == leader.Leader {
				running = 1
			}
			checks := []struct {
				name  string
				attrs []attribute.KeyValue
				want  int64
			}{
				{"grpcproc.leader.electors", []attribute.KeyValue{grpcprocotel.AttrRole.String(role.String())}, 1},
				{"grpcproc.leader.electors", nil, 1}, // one role at a time
				{"grpcproc.leader.stalled", nil, 0},
				{"grpcproc.leader.term", nil, int64(info.Term)},
				{"grpcproc.leader.view", nil, 3},
				{"grpcproc.leader.quorum", nil, 2},
				{"grpcproc.leader.reachable", nil, 3},
				{"grpcproc.leader.singleton", nil, running},
				{"grpcproc.leader.checkpoints.waiting", nil, 0},
			}
			for _, ch := range checks {
				if got := gauge(node, ch.name, ch.attrs...); got != ch.want {
					t.Errorf("%s: %s%v = %d, want %d", node, ch.name, ch.attrs, got, ch.want)
				}
			}
		}

		gone := "a"
		if info.Leader == "a" {
			gone = "b"
		}
		c.Kill(gone)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if got := gauge(info.Leader, "grpcproc.leader.reachable"); got != 2 {
			t.Errorf("reachable with %s gone = %d, want 2", gone, got)
		}
		if got := gauge(info.Leader, "grpcproc.leader.view"); got != 3 {
			t.Errorf("view with %s gone = %d, want 3", gone, got)
		}
	})
}

// stalled is a Store whose Save waits while it is held.
type stalled struct {
	held    atomic.Bool
	release chan struct{}
}

func (*stalled) Load() ([]byte, error) { return nil, nil }

func (s *stalled) Save([]byte) error {
	if s.held.Load() {
		<-s.release
	}
	return nil
}

// An elector held up by its Store is reported as stalled, with no other
// gauge, rather than holding the collection up: the first time after the
// second it is given to answer, then at once, as the node's snapshot says
// how long it has been on one message. A collection that ends sooner
// reports nothing of it.
func TestObserveLeaderStalled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setup(t)
		c := grpcproctest.New(t, "a")
		reg, err := e.hooks.ObserveLeader(c.Node("a"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reg.Unregister() }()
		store := &stalled{release: make(chan struct{})}
		if _, err := leader.Start(c.Node("a"), leader.Spec[*emptypb.Empty]{Cluster: "test", Voters: []string{"a"}, Store: store, Singleton: idle}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		gauges := func(when string, electors, stall int64, wait time.Duration) {
			t.Helper()
			start := time.Now()
			rm := collect(t, e.reader)
			if got := intValue(rm, "grpcproc.leader.electors"); got != electors {
				t.Errorf("%s: electors = %d, want %d", when, got, electors)
			}
			if got := intValue(rm, "grpcproc.leader.stalled"); got != stall {
				t.Errorf("%s: stalled = %d, want %d", when, got, stall)
			}
			if waited := time.Since(start); waited != wait {
				t.Errorf("%s: the collection waited %v, want %v", when, waited, wait)
			}
		}
		gauges("answering", 1, 0, 0)

		// It steps down, campaigns again after a backoff, and stalls saving
		// its vote.
		store.held.Store(true)
		if err := leader.Resign(t.Context(), c.Node("a"), "test"); !errors.Is(err, leader.ErrNoSuccessor) {
			t.Fatalf("resign: %v", err)
		}
		for i := 0; ; i++ {
			if p, ok := c.Node("a").Process(pidOf(t, c.Node("a"))); ok && p.BusyFor > 0 && p.State == grpcproc.StateRunning {
				break
			}
			if i == 1000 {
				t.Fatal("the elector did not stall")
			}
			time.Sleep(10 * time.Millisecond)
		}
		// A collection that ends first reports it as nothing.
		short, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		var rm metricdata.ResourceMetrics
		_ = e.reader.Collect(short, &rm)
		cancel()
		if got := intValue(rm, "grpcproc.leader.stalled") + intValue(rm, "grpcproc.leader.electors"); got != 0 {
			t.Errorf("a collection that ended first: %d", got)
		}
		gauges("stalled, asked", 0, 1, time.Second)
		gauges("stalled, from the snapshot", 0, 1, 0)

		store.held.Store(false)
		close(store.release)
		time.Sleep(time.Second)
		synctest.Wait()
		gauges("answering again", 1, 0, 0)
	})
}

func pidOf(t *testing.T, n *grpcproc.Node) grpcproc.PID {
	t.Helper()
	pid, ok := n.Whereis(leader.ElectorName("test"))
	if !ok {
		t.Fatal("no elector")
	}
	return pid
}
