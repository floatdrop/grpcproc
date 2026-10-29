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
	got = eventLine(client.EventView{Time: "t", Kind: "member-down", Member: &client.MemberView{Name: "b", Incarnation: 2, Addr: "b:1"}})
	if got != `t member-down member="b#2" addr="b:1"` {
		t.Fatal(got)
	}
	if got = eventLine(client.EventView{Time: "t", Kind: "member-up", Member: &client.MemberView{Name: "c"}}); got != `t member-up member="c"` {
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

func TestAgreed(t *testing.T) {
	silent := client.ElectorView{Node: "u", Error: client.NotInspected}
	for _, c := range []struct {
		views []client.ElectorView
		want  string
	}{
		{[]client.ElectorView{{Node: "a", Leader: "b"}, {Node: "b", Leader: "b"}}, "b"},
		{[]client.ElectorView{{Node: "a", Leader: "b"}, {Node: "b", Leader: "a"}}, ""},
		{[]client.ElectorView{{Node: "a", Leader: "b"}, {Node: "b", Error: "busy"}}, ""},
		{[]client.ElectorView{{Node: "a"}, {Node: "b", Leader: "b"}}, ""},
		// A node that serves no Inspector names none, whether it leads or not.
		{[]client.ElectorView{silent, {Node: "a", Leader: "b"}, {Node: "b", Leader: "b"}}, "b"},
		{[]client.ElectorView{{Node: "a", Leader: "u"}, silent, {Node: "b", Leader: "u"}}, "u"},
		{[]client.ElectorView{silent}, ""},
	} {
		if got := agreed(c.views); got != c.want {
			t.Errorf("agreed(%+v) = %q, want %q", c.views, got, c.want)
		}
	}
}
