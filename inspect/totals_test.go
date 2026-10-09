package inspect_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/inspect"
)

// A peer is down when its every link is, and Down names the first 16 of
// them; the rest add up by direction.
func TestSumLinks(t *testing.T) {
	var links []grpcproc.LinkInfo
	for i := range 20 {
		links = append(links, grpcproc.LinkInfo{Peer: grpcproc.NodeID{Name: fmt.Sprintf("p%02d", 19-i)}, Outbound: true, State: grpcproc.LinkDown, Reconnects: 1})
	}
	links = append(links,
		grpcproc.LinkInfo{Peer: grpcproc.NodeID{Name: "q"}, Outbound: true, State: grpcproc.LinkUp, Messages: 5, Bytes: 50, Queued: 3, QueuedBytes: 30, Reconnects: 2},
		grpcproc.LinkInfo{Peer: grpcproc.NodeID{Name: "q"}, State: grpcproc.LinkUp, Messages: 7, Bytes: 70},
		grpcproc.LinkInfo{Peer: grpcproc.NodeID{Name: "r"}, Outbound: true, State: grpcproc.LinkDown},
		grpcproc.LinkInfo{Peer: grpcproc.NodeID{Name: "r"}, State: grpcproc.LinkUp, Messages: 1, Bytes: 10},
		grpcproc.LinkInfo{Peer: grpcproc.NodeID{Name: "s"}, Outbound: true, State: grpcproc.LinkConnecting},
	)
	group := func(peer string) string {
		if peer[0] == 'p' {
			return "down"
		}
		return "up"
	}
	var down []string
	for i := range 16 {
		down = append(down, fmt.Sprintf("p%02d", i))
	}
	want := []inspect.LinkTotals{
		{Group: "down", Peers: 20, PeersDown: 20, Outbound: 20, OutboundDown: 20, Reconnects: 20, Down: down},
		{
			Group: "up", Peers: 3, Outbound: 3, OutboundUp: 1, OutboundDown: 1, Inbound: 2,
			MessagesSent: 5, BytesSent: 50, MessagesReceived: 8, BytesReceived: 80, Queued: 3, QueuedBytes: 30, Reconnects: 2,
		},
	}
	if got := inspect.SumLinks(links, group); !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
	if got := inspect.SumLinks(nil, group); len(got) != 0 {
		t.Fatalf("no links: %+v", got)
	}
}
