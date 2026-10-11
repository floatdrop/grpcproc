package groups_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/groups"
)

func scaleEnv(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv("GROUPS_SCALE_" + name)); err == nil {
		return v
	}
	return def
}

// everyone is a Membership that reports each of its nodes up once.
type everyone []grpcproc.Member

func (ms everyone) Watch(ctx context.Context) (<-chan grpcproc.MemberEvent, error) {
	ch := make(chan grpcproc.MemberEvent, len(ms))
	for _, m := range ms {
		ch <- grpcproc.MemberEvent{Member: m, Up: true}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func heapMB() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1 << 20)
}

func cpu() time.Duration {
	var u syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	return time.Duration(u.Utime.Nano() + u.Stime.Nano())
}

// TestScale measures a scope of N nodes, every one in it, in one process:
//
//	GROUPS_SCALE_N=100 go test -run '^TestScale$' -v ./groups
//
// Each node has M members in group "g". The nodes are linked to each other
// first, so what a scope adds is told apart from what the links cost.
func TestScale(t *testing.T) {
	n := scaleEnv("N", 0)
	if n == 0 {
		t.Skip("GROUPS_SCALE_N not set")
	}
	m, churn := scaleEnv("M", 10), scaleEnv("CHURN", 200)
	names := make([]string, n)
	ms := make(everyone, n)
	for i := range names {
		names[i] = fmt.Sprintf("n%04d", i)
		ms[i] = grpcproc.Member{Name: names[i], Addr: names[i]}
	}
	// In-memory connections with small buffers, gRPC's own small too: with
	// a link each way between every two nodes, the defaults would hold
	// gigabytes.
	lns := map[string]*bufconn.Listener{}
	for _, name := range names {
		lns[name] = bufconn.Listen(8 << 10)
	}
	dial := grpc.WithContextDialer(func(ctx context.Context, to string) (net.Conn, error) { return lns[to].DialContext(ctx) })
	base := heapMB()
	nodes := make([]*grpcproc.Node, n)
	for i, name := range names {
		node, err := grpcproc.NewNode(grpcproc.Config{
			Admit: grpcproc.AdmitAll, Name: name, Advertise: name,
			Resolver: grpcproc.ResolverFunc(func(_ context.Context, node string) (string, error) { return "passthrough:///" + node, nil }),
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), dial,
				grpc.WithReadBufferSize(4 << 10), grpc.WithWriteBufferSize(4 << 10)},
			Logger: slog.New(slog.DiscardHandler),
		})
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer(grpc.ReadBufferSize(4<<10), grpc.WriteBufferSize(4<<10))
		node.Register(srv)
		go func() { _ = srv.Serve(lns[name]) }()
		if err := node.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = node.Stop(ctx)
			srv.Stop()
		})
		nodes[i] = node
		_, _ = node.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				_ = m.Reply(m.Body, nil)
			}
		}, grpcproc.WithName("echo"))
	}
	// A full mesh of links, before any scope.
	var wg sync.WaitGroup
	slots := make(chan struct{}, 512)
	for i := range nodes {
		for j := range nodes {
			if i == j {
				continue
			}
			wg.Go(func() {
				slots <- struct{}{}
				defer func() { <-slots }()
				if _, err := grpcproc.Named[proto.Message](names[j], "echo").Call[proto.Message](t.Context(), nodes[i], &emptypb.Empty{}); err != nil {
					t.Errorf("%s -> %s: %v", names[i], names[j], err)
				}
			})
		}
	}
	wg.Wait()
	meshed := heapMB()
	goroutines := runtime.NumGoroutine()

	// Members first, then the scopes: every scope then syncs every other at
	// once, as a cluster that starts, or comes back, does.
	scopes := make([]*groups.Scope, n)
	idle := func(node *grpcproc.Node) grpcproc.PID {
		a, _ := node.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			<-p.Context().Done()
			return nil
		})
		return a.PID()
	}
	members := make([][]grpcproc.PID, n)
	for i, node := range nodes {
		for range m {
			members[i] = append(members[i], idle(node))
		}
	}
	cpu0, start := cpu(), time.Now()
	for i, node := range nodes {
		sc, err := groups.Start(node, groups.Config{Membership: ms})
		if err != nil {
			t.Fatal(err)
		}
		scopes[i] = sc
		if err := sc.Join(t.Context(), node, "g", members[i]...); err != nil {
			t.Fatal(err)
		}
	}
	all := func(want int) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Minute); ; {
			done := true
			for _, sc := range scopes {
				if len(sc.Members("g")) != want {
					done = false
					break
				}
			}
			if done {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("not every node has %d members", want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	all(n * m)
	synced, syncCPU := time.Since(start), cpu()-cpu0
	runtime.GC()
	scoped := heapMB()
	scopeGoroutines := runtime.NumGoroutine() - goroutines

	// One join on one node, until every node has it.
	extra := idle(nodes[0])
	start = time.Now()
	if err := scopes[0].Join(t.Context(), nodes[0], "g", extra); err != nil {
		t.Fatal(err)
	}
	all(n*m + 1)
	one := time.Since(start)

	// A burst of churn on one node: CHURN joins and leaves, one at a time.
	cpu0, start = cpu(), time.Now()
	for range churn / 2 {
		if err := scopes[0].Leave(t.Context(), nodes[0], "g", extra); err != nil {
			t.Fatal(err)
		}
		if err := scopes[0].Join(t.Context(), nodes[0], "g", extra); err != nil {
			t.Fatal(err)
		}
	}
	// A last join of one more: its Update comes after the others, in order.
	if err := scopes[0].Join(t.Context(), nodes[0], "g", idle(nodes[0])); err != nil {
		t.Fatal(err)
	}
	all(n*m + 2)
	burst, burstCPU := time.Since(start), cpu()-cpu0

	t.Logf("nodes %d, members %d each (%d in all)", n, m, n*m)
	t.Logf("heap: %.0f MB for the nodes and their full mesh of links, %.0f MB more for the scopes: %.2f MB a node", meshed-base, scoped-meshed, (scoped-meshed)/float64(n))
	t.Logf("goroutines: %d for the nodes and links, %d more for the scopes: %.0f a node", goroutines, scopeGoroutines, float64(scopeGoroutines)/float64(n))
	t.Logf("every scope syncing every other at once: %v, %v of CPU", synced.Round(time.Millisecond), syncCPU.Round(time.Millisecond))
	t.Logf("one join, until every node has it: %v", one.Round(time.Microsecond))
	t.Logf("%d changes on one node, until every node has the last: %v, %v of CPU, %.1f ms of CPU a change across the cluster", churn, burst.Round(time.Millisecond), burstCPU.Round(time.Millisecond), float64(burstCPU.Microseconds())/1000/float64(churn))
}
