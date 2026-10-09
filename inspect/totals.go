package inspect

import (
	"maps"
	"slices"

	"github.com/floatdrop/grpcproc"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// maxDown bounds LinkTotals.Down.
const maxDown = 16

// LinkTotals is a node's links with a group of its peers, added up: what
// GetNode answers in place of the links themselves, for a node with many
// peers.
type LinkTotals struct {
	Group     string // the peers' value of the key they were grouped by
	Peers     int    // peers with a link either way
	PeersDown int    // peers whose every link is down

	Outbound, OutboundUp, OutboundDown, Inbound int

	// Sent over the outbound links, received over the inbound ones.
	MessagesSent, BytesSent, MessagesReceived, BytesReceived uint64

	Queued, QueuedBytes int      // outbound, as LinkInfo has them
	Reconnects          uint64   // outbound, as LinkInfo has them
	Down                []string // the first of the PeersDown peers by name, at most 16
}

// SumLinks adds links up by the group each peer is in, ordered by group,
// with one entry per group that has any.
func SumLinks(links []grpcproc.LinkInfo, group func(peer string) string) []LinkTotals {
	type peer struct {
		group string
		down  bool // every link with it is down
	}
	peers := map[string]*peer{}
	totals := map[string]*LinkTotals{}
	for _, l := range links {
		p := peers[l.Peer.Name]
		if p == nil {
			p = &peer{group: group(l.Peer.Name), down: true}
			peers[l.Peer.Name] = p
		}
		p.down = p.down && l.State == grpcproc.LinkDown
		t := totals[p.group]
		if t == nil {
			t = &LinkTotals{Group: p.group}
			totals[p.group] = t
		}
		if !l.Outbound {
			t.Inbound++
			t.MessagesReceived += l.Messages
			t.BytesReceived += l.Bytes
			continue
		}
		t.Outbound++
		switch l.State {
		case grpcproc.LinkUp:
			t.OutboundUp++
		case grpcproc.LinkDown:
			t.OutboundDown++
		}
		t.MessagesSent += l.Messages
		t.BytesSent += l.Bytes
		t.Queued += l.Queued
		t.QueuedBytes += l.QueuedBytes
		t.Reconnects += l.Reconnects
	}
	for _, name := range slices.Sorted(maps.Keys(peers)) {
		p := peers[name]
		t := totals[p.group]
		t.Peers++
		if p.down {
			t.PeersDown++
			if len(t.Down) < maxDown {
				t.Down = append(t.Down, name)
			}
		}
	}
	out := make([]LinkTotals, 0, len(totals))
	for _, g := range slices.Sorted(maps.Keys(totals)) {
		out = append(out, *totals[g])
	}
	return out
}

func totalsTo(ts []LinkTotals) []*inspectv1.LinkTotals {
	out := make([]*inspectv1.LinkTotals, 0, len(ts))
	for _, t := range ts {
		out = append(out, &inspectv1.LinkTotals{
			Group: t.Group, Peers: uint32(t.Peers), PeersDown: uint32(t.PeersDown),
			Outbound: uint32(t.Outbound), OutboundUp: uint32(t.OutboundUp), OutboundDown: uint32(t.OutboundDown), Inbound: uint32(t.Inbound),
			MessagesSent: t.MessagesSent, BytesSent: t.BytesSent, MessagesReceived: t.MessagesReceived, BytesReceived: t.BytesReceived,
			Queued: uint32(t.Queued), QueuedBytes: uint64(t.QueuedBytes), Reconnects: t.Reconnects, Down: t.Down,
		})
	}
	return out
}

// Totals converts the link totals of a GetNode answer back.
func Totals(r *inspectv1.GetNodeResponse) []LinkTotals {
	var out []LinkTotals
	for _, t := range r.GetLinkTotals() {
		out = append(out, LinkTotals{
			Group: t.GetGroup(), Peers: int(t.GetPeers()), PeersDown: int(t.GetPeersDown()),
			Outbound: int(t.GetOutbound()), OutboundUp: int(t.GetOutboundUp()), OutboundDown: int(t.GetOutboundDown()), Inbound: int(t.GetInbound()),
			MessagesSent: t.GetMessagesSent(), BytesSent: t.GetBytesSent(), MessagesReceived: t.GetMessagesReceived(), BytesReceived: t.GetBytesReceived(),
			Queued: int(t.GetQueued()), QueuedBytes: int(t.GetQueuedBytes()), Reconnects: t.GetReconnects(), Down: t.GetDown(),
		})
	}
	return out
}
