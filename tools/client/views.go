package client

import (
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/inspect"
)

// NodeView is a node as grpcprocctl shows it and its MCP tools return it.
type NodeView struct {
	Name        string            `json:"name" jsonschema:"node name"`
	Incarnation uint64            `json:"incarnation,omitzero" jsonschema:"changes every time the node starts"`
	Advertise   string            `json:"advertise,omitempty" jsonschema:"address peers dial"`
	Metadata    map[string]string `json:"metadata,omitempty" jsonschema:"what the node says about itself to the cluster: its version, its zone (Config.Metadata)"`
	Uptime      string            `json:"uptime,omitempty"`
	Processes   int               `json:"processes"`
	Spawned     uint64            `json:"spawned"`
	Exited      uint64            `json:"exited"`
	DeadLetters uint64            `json:"dead_letters" jsonschema:"messages that could not be delivered"`
	Links       []LinkView        `json:"links,omitzero" jsonschema:"absent when only added up, in link_totals"`
	LinkTotals  []LinkTotalsView  `json:"link_totals,omitempty" jsonschema:"its links added up, by a key of its peers' metadata or all together"`
	Members     []MemberView      `json:"members,omitempty" jsonschema:"the nodes its Config.Membership reports up, by name: the cluster as its discovery sees it, linked or not; none without a Membership"`
	Error       string            `json:"error,omitempty" jsonschema:"why the node could not be inspected"`
	Unanswered  bool              `json:"unanswered,omitzero" jsonschema:"the time to describe the cluster ran out before this node was asked, or answered: not known to be down"`

	raw []grpcproc.LinkInfo // Links, for Cluster to add up again
}

// NotAnswered is what to show for an Unanswered node where an error goes.
const NotAnswered = "not answered in time"

// Problem says why the node is not described: its Error, or NotAnswered.
func (v NodeView) Problem() string {
	if v.Unanswered {
		return NotAnswered
	}
	return v.Error
}

// Reached is whether the node answered.
func (v NodeView) Reached() bool { return v.Error == "" && !v.Unanswered }

// LinkTotalsView is a node's links with a group of its peers, added up.
type LinkTotalsView struct {
	Group            string   `json:"group,omitempty" jsonschema:"the peers' value of the metadata key the links are grouped by; empty for peers without it, and for all of them when not grouped"`
	Peers            int      `json:"peers" jsonschema:"peers with a link either way"`
	PeersDown        int      `json:"peers_down,omitzero" jsonschema:"peers whose every link is down"`
	Outbound         int      `json:"outbound"`
	OutboundUp       int      `json:"outbound_up"`
	OutboundDown     int      `json:"outbound_down,omitzero"`
	Inbound          int      `json:"inbound"`
	MessagesSent     uint64   `json:"messages_sent"`
	BytesSent        uint64   `json:"bytes_sent"`
	MessagesReceived uint64   `json:"messages_received"`
	BytesReceived    uint64   `json:"bytes_received"`
	Queued           int      `json:"queued,omitzero" jsonschema:"messages waiting to be written to these peers"`
	QueuedBytes      int      `json:"queued_bytes,omitzero"`
	Reconnects       uint64   `json:"reconnects,omitzero"`
	Down             []string `json:"down,omitempty" jsonschema:"the first of the peers that are down, by name, at most 16"`
}

func linkTotalsView(t inspect.LinkTotals) LinkTotalsView {
	return LinkTotalsView{
		Group: t.Group, Peers: t.Peers, PeersDown: t.PeersDown,
		Outbound: t.Outbound, OutboundUp: t.OutboundUp, OutboundDown: t.OutboundDown, Inbound: t.Inbound,
		MessagesSent: t.MessagesSent, BytesSent: t.BytesSent, MessagesReceived: t.MessagesReceived, BytesReceived: t.BytesReceived,
		Queued: t.Queued, QueuedBytes: t.QueuedBytes, Reconnects: t.Reconnects, Down: t.Down,
	}
}

// MemberView is a node as a node's Membership reports it.
type MemberView struct {
	Name        string            `json:"name"`
	Incarnation uint64            `json:"incarnation,omitzero" jsonschema:"0 when the Membership does not know it"`
	Addr        string            `json:"addr,omitempty" jsonschema:"where peers dial it"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// LinkView is one direction of traffic with a peer.
type LinkView struct {
	Peer        string `json:"peer"`
	Incarnation uint64 `json:"incarnation" jsonschema:"the peer's incarnation; 0 until a dial to it succeeds"`
	Direction   string `json:"direction" jsonschema:"out: this node sends on it; in: the peer does"`
	State       string `json:"state"`
	Age         string `json:"age,omitempty"`
	Reconnects  uint64 `json:"reconnects"`
	Messages    uint64 `json:"messages"`
	Bytes       uint64 `json:"bytes"`
	Queued      int    `json:"queued,omitzero" jsonschema:"on an out link: messages waiting to be written to the peer; a growing queue means the peer or the network cannot keep up, and with Config.MaxQueued set, sends to the peer fail at once while it holds that many"`
	QueuedBytes int    `json:"queued_bytes,omitzero" jsonschema:"on an out link: the bytes of the queued messages' bodies, which Config.MaxQueuedBytes bounds"`
	RetryIn     string `json:"retry_in,omitempty" jsonschema:"on a down out link, whose dials failed: how long sends to the peer keep failing at once; empty when the next send dials again. Measured against this tool's clock"`
	LastError   string `json:"last_error,omitempty"`
}

// ProcessView is a process as grpcprocctl shows it and its MCP tools return it.
type ProcessView struct {
	PID           string            `json:"pid"`
	Name          string            `json:"name,omitempty" jsonschema:"the name it is registered under, if any"`
	Label         string            `json:"label" jsonschema:"what metrics aggregate by; the message type by default"`
	Type          string            `json:"type" jsonschema:"Go type of the messages it accepts"`
	Parent        string            `json:"parent,omitempty" jsonschema:"the process that started it, such as its supervisor"`
	Globals       []string          `json:"globals,omitempty" jsonschema:"the installation's global names it holds"`
	State         string            `json:"state" jsonschema:"idle (waiting in Receive), running, waiting-reply (in a Call), exiting"`
	Uptime        string            `json:"uptime"`
	Mailbox       int               `json:"mailbox" jsonschema:"messages waiting"`
	MailboxPeak   int               `json:"mailbox_peak"`
	OldestWait    string            `json:"oldest_wait,omitempty" jsonschema:"how long the oldest waiting message has waited"`
	Received      uint64            `json:"received"`
	Sent          uint64            `json:"sent"`
	CallsInFlight uint32            `json:"calls_in_flight,omitzero"`
	BusySeconds   float64           `json:"busy_seconds" jsonschema:"seconds it has spent anywhere but waiting for a message since it started, waits in calls included; its growth between two reads over the seconds between them is the share of that time it was busy, near 1 for a process that is never idle"`
	BusyFor       string            `json:"busy_for,omitempty" jsonschema:"how long since it last stopped waiting for a message or took a batch of them: at least how long it has been on its current one; empty while it waits"`
	LastMessage   string            `json:"last_message,omitempty" jsonschema:"type of the last message it took"`
	Monitors      int               `json:"monitors,omitzero" jsonschema:"processes it watches"`
	Links         int               `json:"links,omitzero" jsonschema:"processes it is linked to: whose exit ends it, or reaches it as a message if it traps exits"`
	TrapExit      bool              `json:"trap_exit,omitzero" jsonschema:"whether the exits of processes it is linked to reach it as messages"`
	Watchers      int               `json:"watchers,omitzero" jsonschema:"processes monitoring or linked to it"`
	LogLevel      string            `json:"log_level"`
	Inspect       map[string]string `json:"inspect,omitempty" jsonschema:"what the process says about itself"`
	InspectError  string            `json:"inspect_error,omitempty" jsonschema:"why inspect is empty: busy, or gone"`
}

// NameView is a global name and the process that holds it.
type NameView struct {
	Name string `json:"name"`
	PID  string `json:"pid" jsonschema:"the process that holds it"`
}

// EventView is something that happened on a node.
type EventView struct {
	Time    string       `json:"time"`
	Kind    string       `json:"kind" jsonschema:"spawn, exit, link-up, link-down or dead-letter"`
	Missed  uint64       `json:"missed,omitzero" jsonschema:"events lost before this one"`
	Process *ProcessView `json:"process,omitempty"`
	Reason  string       `json:"reason,omitempty"`
	Peer    string       `json:"peer,omitempty"`
	Error   string       `json:"error,omitempty"`
	From    string       `json:"from,omitempty"`
	To      string       `json:"to,omitempty"`
	Type    string       `json:"type,omitempty" jsonschema:"message type of a dead letter"`
}

// short renders a duration for people: whole seconds past a minute,
// milliseconds below.
func short(d time.Duration) string {
	switch {
	case d <= 0:
		return ""
	case d >= time.Minute:
		return d.Round(time.Second).String()
	case d >= time.Second:
		return d.Round(time.Millisecond).String()
	}
	return d.Round(time.Microsecond).String()
}

func (c *Client) since(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return short(c.now().Sub(t))
}

func pidString(p grpcproc.PID) string {
	if p.IsZero() {
		return ""
	}
	return p.String()
}

func (c *Client) nodeView(n grpcproc.NodeInfo) NodeView {
	v := NodeView{
		Name: n.ID.Name, Incarnation: n.ID.Incarnation, Advertise: n.Advertise, Metadata: n.Metadata,
		Uptime: c.since(n.StartedAt), Processes: n.Processes,
		Spawned: n.Spawned, Exited: n.Exited, DeadLetters: n.DeadLetters,
		Links: []LinkView{},
	}
	for _, l := range n.Links {
		dir := "in"
		if l.Outbound {
			dir = "out"
		}
		v.Links = append(v.Links, LinkView{
			Peer: l.Peer.Name, Incarnation: l.Peer.Incarnation, Direction: dir, State: l.State.String(),
			Age: c.since(l.EstablishedAt), Reconnects: l.Reconnects, Messages: l.Messages, Bytes: l.Bytes,
			Queued: l.Queued, QueuedBytes: l.QueuedBytes, RetryIn: short(l.RetryAt.Sub(c.now())), LastError: l.LastError,
		})
	}
	return v
}

func (c *Client) processView(p grpcproc.ProcessInfo) ProcessView {
	return ProcessView{
		PID: p.PID.String(), Name: p.Name, Label: p.Label, Type: p.Type, Parent: pidString(p.Parent), Globals: p.Globals,
		State: p.State.String(), Uptime: c.since(p.StartedAt),
		Mailbox: p.Mailbox.Depth, MailboxPeak: p.Mailbox.Peak, OldestWait: short(p.Mailbox.OldestAge),
		Received: p.Received, Sent: p.Sent, CallsInFlight: p.CallsInFlight, LastMessage: p.LastMessage,
		BusySeconds: p.Busy.Seconds(), BusyFor: short(p.BusyFor),
		Monitors: p.Monitors, Links: p.Links, TrapExit: p.TrapExit, Watchers: p.Watchers, LogLevel: p.LogLevel.String(),
	}
}

func (c *Client) eventView(e grpcproc.Event) EventView {
	v := EventView{Time: e.Time.Format(time.RFC3339Nano), Kind: e.Kind.String(), Missed: e.Missed,
		Reason: e.Reason, Error: e.Err, From: pidString(e.From), To: pidString(e.To), Type: e.Type}
	if e.Kind == grpcproc.EventSpawn || e.Kind == grpcproc.EventExit {
		p := c.processView(e.Process)
		v.Process = &p
	}
	if e.Peer.Name != "" {
		v.Peer = e.Peer.String()
	}
	return v
}
