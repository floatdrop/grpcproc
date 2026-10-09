package client_test

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/inspect"
	"github.com/floatdrop/grpcproc/internal/testpb"
	"github.com/floatdrop/grpcproc/tools/client"
)

func scaleEnv(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv("GRPCPROCCTL_SCALE_" + name)); err == nil {
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

// TestScale measures Cluster over a large cluster, as the web UI walks it:
//
//	GRPCPROCCTL_SCALE_N=900 go test -run '^TestScale$' -v ./client
//
// N nodes, over DCS data centers, each linked to K nodes of its own; an
// Inspector request to a node costs RTT_DC ms inside the inspected node's
// data center and RTT ms across.
func TestScale(t *testing.T) {
	n := scaleEnv("N", 0)
	if n == 0 {
		t.Skip("GRPCPROCCTL_SCALE_N not set")
	}
	dcs, k := scaleEnv("DCS", 3), scaleEnv("K", 8)
	rtt, rttDC := time.Duration(scaleEnv("RTT", 50))*time.Millisecond, time.Duration(scaleEnv("RTT_DC", 1))*time.Millisecond
	names := make([]string, n)
	ms := make(everyone, n)
	dcOf := map[string]string{}
	for i := range names {
		names[i] = fmt.Sprintf("n%04d", i)
		dcOf[names[i]] = "dc" + strconv.Itoa(i%dcs)
		ms[i] = grpcproc.Member{Name: names[i], Addr: names[i], Metadata: map[string]string{"dc": dcOf[names[i]]}}
	}
	// In-memory connections with small buffers: grpcproctest's would hold
	// gigabytes at this size.
	lns := map[string]*bufconn.Listener{}
	for _, name := range names {
		lns[name] = bufconn.Listen(16 << 10)
	}
	dial := grpc.WithContextDialer(func(ctx context.Context, to string) (net.Conn, error) { return lns[to].DialContext(ctx) })
	delay := func(from string) grpc.DialOption {
		return grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			d := rtt
			if _, to, _ := strings.Cut(cc.CanonicalTarget(), ":///"); dcOf[to] == dcOf[from] {
				d = rttDC
			}
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		})
	}
	start := time.Now()
	nodes := map[string]*grpcproc.Node{}
	for _, name := range names {
		node, err := grpcproc.NewNode(grpcproc.Config{
			Admit: grpcproc.AdmitAll, Name: name, Advertise: name,
			Resolver:    grpcproc.ResolverFunc(func(_ context.Context, node string) (string, error) { return "passthrough:///" + node, nil }),
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), dial, delay(name)},
			Logger:      slog.New(slog.DiscardHandler),
			Metadata:    map[string]string{"dc": dcOf[name]},
			Membership:  ms,
		})
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer()
		node.Register(srv)
		insp := inspect.New(node)
		insp.Register(srv)
		go func() { _ = srv.Serve(lns[name]) }()
		if err := node.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = insp.Close()
			_ = node.Stop(ctx)
			srv.Stop()
		})
		nodes[name] = node
		_, _ = node.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				_ = m.Reply(&testpb.Pong{}, nil)
			}
		}, grpcproc.WithName("echo"))
	}
	var wg sync.WaitGroup
	slots := make(chan struct{}, 256)
	for i, name := range names {
		for j := 1; j <= min(k, n/dcs-1); j++ {
			wg.Go(func() {
				slots <- struct{}{}
				defer func() { <-slots }()
				peer := names[(i+j*dcs)%n] // the same data center
				if _, err := grpcproc.Named[*testpb.Ping](peer, "echo").Call[*testpb.Pong](t.Context(), nodes[name], &testpb.Ping{}); err != nil {
					t.Errorf("%s -> %s: %v", name, peer, err)
				}
			})
		}
	}
	wg.Wait()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	t.Logf("%d nodes in %d data centers, %d links out each, RTT %v (%v inside one): up in %v, heap %d MB",
		n, dcs, k, rtt, rttDC, time.Since(start).Round(time.Millisecond), mem.HeapAlloc>>20)

	cc, err := grpc.NewClient("passthrough:///"+names[0], grpc.WithTransportCredentials(insecure.NewCredentials()), dial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	c := client.New(cc)
	walk := func(label string, budget time.Duration, o client.ClusterOptions) {
		ctx, cancel := context.WithTimeout(t.Context(), budget)
		defer cancel()
		begin := time.Now()
		views, err := c.Cluster(ctx, o)
		took := time.Since(begin)
		if err != nil {
			t.Fatal(err)
		}
		var ok, failed, unanswered int
		for _, v := range views {
			switch {
			case v.Unanswered:
				unanswered++
			case v.Error != "":
				failed++
			default:
				ok++
			}
		}
		b, _ := json.Marshal(views)
		t.Logf("%s: %v, %d ok, %d failed, %d unanswered, %d missing; %d KB as JSON", label, took.Round(time.Millisecond), ok, failed, unanswered, n-len(views), len(b)>>10)
	}
	o := client.ClusterOptions{LinksUpTo: 32, NodeTimeout: 5 * time.Second}
	walk("first walk", time.Minute, o)
	walk("walk", time.Minute, o)
	walk("walk within 5s", 5*time.Second, o)
	o.GroupLinksBy = "dc"
	walk("walk, links by dc", time.Minute, o)
}
