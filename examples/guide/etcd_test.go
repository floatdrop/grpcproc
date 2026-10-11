package guide

import (
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"golang.yandex/di"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	modelsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/models/v1"
	toolsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/tools/v1"
	"github.com/floatdrop/grpcproc/groups"
)

// startEtcd runs a single-member etcd in the test process, and returns the
// address its clients dial. etcd binds its client port itself, so no other
// test can take it in between; the peer port, which a one-member cluster
// must name before it starts, is the one picked.
func startEtcd(t *testing.T) string {
	t.Helper()
	free := func() url.URL {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ln.Close() }()
		return url.URL{Scheme: "http", Host: ln.Addr().String()}
	}
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	client, peer := url.URL{Scheme: "http", Host: "127.0.0.1:0"}, free()
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{client}, []url.URL{client}
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{peer}, []url.URL{peer}
	cfg.InitialCluster = cfg.Name + "=" + peer.String()
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(20 * time.Second):
		t.Fatal("etcd did not start")
	}
	return e.Clients[0].Addr().String()
}

// The distributed deployment with nodes that find each other in etcd: no
// node is told another's address, and the gateway is told nothing of the GPU
// nodes. A GPU node that starts later is asked as soon as the gateway learns
// of it, and one that stops is asked no more.
func TestEtcd(t *testing.T) {
	endpoint := startEtcd(t)
	run := func(name string, services []di.Module, more ...di.Module) *di.Scope {
		t.Helper()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		// Each node advertises the address it listens on: what the others
		// find in etcd.
		cfg := platform.Config{Node: name, Listen: ln.Addr().String(), Etcd: []string{endpoint}}
		switch name {
		case "gateway":
			cfg.Placement = map[string]string{toolsv1.Service: "sandbox"}
			cfg.Untrusted = []string{"sandbox"}
		case "sandbox":
			cfg.Export = []string{toolsv1.RunnerName}
		default:
			cfg.Export = []string{modelsv1.SchedulerName}
			cfg.Untrusted = []string{"sandbox"}
		}
		app := compose(cfg, services, more...)
		app.Value(ln).Override()
		start(t, app)
		return app
	}
	front := run("gateway", gateway)
	run("sandbox", sandbox)
	gpu1 := run("gpu-1", gpu, fast)
	run("gpu-2", gpu, fast)

	var log []string
	note := func(what string, n int) {
		t.Helper()
		awaitModels(t, front, n)
		var nodes []string
		for _, pid := range front.Get[*groups.Scope]().Members(modelsv1.Group) {
			nodes = append(nodes, pid.Node)
		}
		log = append(log, what+": "+strings.Join(nodes, ", "))
	}
	note("the models group, as the gateway has it", 2)

	events := follow(t, front.Get[*grpcproc.Node](), "gateway", "1")
	if code, body := say(t, front, "1", "what is 6 * 7?"); code != http.StatusAccepted {
		t.Fatalf("%d %s", code, body)
	}
	log = append(log, strings.TrimSuffix(transcript(t, events, 1), "\n"))

	run("gpu-3", gpu, fast)
	note("gpu-3 starts", 3)
	if err := gpu1.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	note("gpu-1 stops", 2)
	golden(t, "etcd.txt", strings.Join(log, "\n")+"\n")
}
