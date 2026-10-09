package cli

import (
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
)

func TestEventLine(t *testing.T) {
	got := eventLine(client.EventView{
		Time: "not a time", Kind: "exit", Missed: 3, Reason: "boom",
		Process: &client.ProcessView{PID: "<a.1.2>", Label: "order", Name: "orders"},
	})
	if got != `not a time exit <a.1.2> label=order name=orders reason="boom" missed=3` {
		t.Fatal(got)
	}
}

func TestBusy(t *testing.T) {
	for secs, want := range map[float64]string{0: "0s", 0.0004: "400µs", 1.5: "1.5s", 90.4: "1m30s"} {
		if got := busy(client.ProcessView{BusySeconds: secs}); got != want {
			t.Errorf("busy(%v) = %q, want %q", secs, got, want)
		}
	}
}

func TestPeersAndProblem(t *testing.T) {
	many := client.NodeView{LinkTotals: []client.LinkTotalsView{
		{Group: "east", Peers: 20, PeersDown: 18, Down: []string{"e1", "e2"}},
		{Group: "west", Peers: 3},
	}}
	for n, want := range map[*client.NodeView]string{
		{}: "", {LinkTotals: []client.LinkTotalsView{{Peers: 2}}}: "2 up", &many: "5 up, 18 down: e1,e2,…",
	} {
		if got := peers(*n); got != want {
			t.Errorf("peers(%+v) = %q, want %q", n.LinkTotals, got, want)
		}
	}
	if got := (client.NodeView{Unanswered: true}).Problem(); got != client.NotAnswered {
		t.Error(got)
	}
	if got := (client.NodeView{Error: "refused"}).Problem(); got != "refused" {
		t.Error(got)
	}
}

func TestLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"localhost:9911": true, "127.0.0.1:9911": true, "[::1]:9911": true,
		":9911": false, "0.0.0.0:9911": false, "10.0.0.5:9911": false, "observer.internal:80": false, "nonsense": false,
	} {
		if got := loopback(addr); got != want {
			t.Errorf("%s: %v", addr, got)
		}
	}
}
