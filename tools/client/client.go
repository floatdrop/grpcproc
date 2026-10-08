// Package client is a typed wrapper over grpcproc's Inspector, shared by
// grpcprocctl and its MCP server: flat, readable views of nodes, processes and
// events, target parsing, and a walk over every node of a cluster.
package client

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/inspect"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// Client talks to one Inspector; requests about other nodes are forwarded
// by it.
type Client struct {
	rpc inspectv1.InspectorClient
	now func() time.Time
}

// New wraps a connection to a node serving grpcproc.inspect.v1.Inspector.
func New(cc grpc.ClientConnInterface) *Client {
	return &Client{rpc: inspectv1.NewInspectorClient(cc), now: time.Now}
}

// parseTarget reads a process reference: a PID as grpcproc prints it,
// "<node.incarnation.id>", or a registered name. A name is looked up on node.
func parseTarget(s string) (*inspectv1.Target, error) {
	if inner, ok := strings.CutPrefix(s, "<"); ok {
		inner, ok = strings.CutSuffix(inner, ">")
		if !ok {
			return nil, fmt.Errorf("bad pid %q: want <node.incarnation.id>", s)
		}
		// Node names may contain dots; incarnations and ids do not.
		rest, id, ok1 := strings.CutLast(inner, ".")
		node, inc, ok2 := strings.CutLast(rest, ".")
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("bad pid %q: want <node.incarnation.id>", s)
		}
		pid := &grpcprocv1.PID{Node: node}
		var err1, err2 error
		pid.Incarnation, err1 = strconv.ParseUint(inc, 10, 64)
		pid.Id, err2 = strconv.ParseUint(id, 10, 64)
		if err1 != nil || err2 != nil || pid.Node == "" {
			return nil, fmt.Errorf("bad pid %q: want <node.incarnation.id>", s)
		}
		return &inspectv1.Target{Kind: &inspectv1.Target_Pid{Pid: pid}}, nil
	}
	if s == "" {
		return nil, fmt.Errorf("a process is required: <node.incarnation.id> or a name")
	}
	return &inspectv1.Target{Kind: &inspectv1.Target_Name{Name: s}}, nil
}

// ParseLevel reads a log level: debug, info, warn, error (each with an
// optional offset, as in debug-4), or a number. It must fit the Inspector's
// int32.
func ParseLevel(s string) (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err == nil && l >= math.MinInt32 && l <= math.MaxInt32 {
		return l, nil
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad level %q: want debug, info, warn, error or a number", s)
	}
	return slog.Level(n), nil
}

// ParseKinds checks names of event kinds (spawn, exit, link-up, link-down,
// dead-letter), trimming spaces, and returns them.
func ParseKinds(names []string) ([]string, error) {
	var out []string
	for _, name := range names {
		name = strings.TrimSpace(name)
		if !slices.Contains(kinds, name) {
			return nil, fmt.Errorf("bad event kind %q: want %s", name, strings.Join(kinds, ", "))
		}
		out = append(out, name)
	}
	return out, nil
}

var kinds = func() []string {
	var out []string
	for k := grpcproc.EventSpawn; k <= grpcproc.EventDeadLetter; k++ {
		out = append(out, k.String())
	}
	return out
}()

// Node describes one node; "" is the node serving the Inspector.
func (c *Client) Node(ctx context.Context, node string) (NodeView, error) {
	resp, err := c.rpc.GetNode(ctx, &inspectv1.GetNodeRequest{Node: node})
	if err != nil {
		return NodeView{}, err
	}
	v := c.nodeView(inspect.NodeInfo(resp.GetNode()))
	for _, m := range inspect.Members(resp.GetNode()) {
		v.Members = append(v.Members, MemberView{Name: m.Name, Incarnation: m.Incarnation, Addr: m.Addr, Metadata: m.Metadata})
	}
	return v, nil
}

// Cluster describes every node reachable from this one by following links,
// and the members each node's Config.Membership reports, linked or not. A
// node that cannot be reached is reported with Error, and so is a peer a
// node only fails to dial (a down link), which this Inspector may reach all
// the same. Those peers, and members no link leads to, are asked last, in
// order of name, laterAtOnce at a time: they are the likeliest to hang until
// ctx ends, and one that does must not use up the time the others need. A
// member that cannot be reached is shown with the address and metadata its
// Membership reports.
func (c *Client) Cluster(ctx context.Context) ([]NodeView, error) {
	first, err := c.Node(ctx, "")
	if err != nil {
		return nil, err
	}
	out := []NodeView{first}
	seen := map[string]bool{first.Name: true}
	var later []string // peers seen only over down links, and members no link leads to
	queued := map[string]bool{}
	members := map[string]MemberView{} // as the first Membership to report each has it
	queue := func(peer string) {
		if !seen[peer] && !queued[peer] {
			queued[peer] = true
			later = append(later, peer)
		}
	}
	for i := 0; ; {
		for ; i < len(out); i++ {
			for _, l := range out[i].Links {
				switch {
				case seen[l.Peer]:
				case l.State == "down":
					queue(l.Peer)
				default:
					seen[l.Peer] = true
					out = append(out, c.probe(ctx, l.Peer))
				}
			}
			for _, m := range out[i].Members {
				if _, ok := members[m.Name]; !ok {
					members[m.Name] = m
				}
				queue(m.Name)
			}
		}
		later = slices.DeleteFunc(later, func(peer string) bool { return seen[peer] })
		if len(later) == 0 {
			return out, nil
		}
		slices.Sort(later)
		found := make([]NodeView, len(later))
		slots := make(chan struct{}, laterAtOnce)
		var wg sync.WaitGroup
		for j, peer := range later {
			seen[peer] = true
			wg.Go(func() {
				slots <- struct{}{}
				defer func() { <-slots }()
				found[j] = c.probe(ctx, peer)
				if m, ok := members[peer]; ok && found[j].Error != "" {
					found[j].Incarnation, found[j].Advertise, found[j].Metadata = m.Incarnation, m.Addr, m.Metadata
				}
			})
		}
		wg.Wait()
		out, later = append(out, found...), nil
	}
}

// laterAtOnce bounds how many of the peers Cluster asks last it asks at once.
const laterAtOnce = 16

// probe describes node, or says why it could not.
func (c *Client) probe(ctx context.Context, node string) NodeView {
	v, err := c.Node(ctx, node)
	if err != nil {
		return NodeView{Name: node, Error: err.Error()}
	}
	return v
}

// Filter narrows Processes; zero fields match everything.
type Filter struct {
	Name       string // substring of a registered name
	Label      string
	State      string // idle, running, waiting-reply, exiting
	MinMailbox int
}

// Processes lists the processes of node that match f, ordered by PID.
func (c *Client) Processes(ctx context.Context, node string, f Filter) ([]ProcessView, error) {
	req := &inspectv1.ListProcessesRequest{Node: node, Name: f.Name, Label: f.Label, MinMailbox: uint32(min(max(int64(f.MinMailbox), 0), math.MaxUint32))}
	if f.State != "" {
		s, err := parseState(f.State)
		if err != nil {
			return nil, err
		}
		req.State = s
	}
	resp, err := c.rpc.ListProcesses(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make([]ProcessView, 0, len(resp.GetProcesses()))
	for _, p := range resp.GetProcesses() {
		out = append(out, c.processView(inspect.ProcessInfo(p)))
	}
	return out, nil
}

func parseState(s string) (inspectv1.ProcessState, error) {
	for st := grpcproc.StateIdle; st <= grpcproc.StateExiting; st++ {
		if st.String() == s {
			return inspectv1.ProcessState(st + 1), nil
		}
	}
	return 0, fmt.Errorf("bad state %q: want idle, running, waiting-reply or exiting", s)
}

// Process describes one process; with ask, also what it publishes about
// itself, waiting up to wait for a busy process to answer.
func (c *Client) Process(ctx context.Context, node, target string, ask bool, wait time.Duration) (ProcessView, error) {
	t, err := parseTarget(target)
	if err != nil {
		return ProcessView{}, err
	}
	req := &inspectv1.GetProcessRequest{Node: node, Target: t, Inspect: ask}
	if wait > 0 {
		req.InspectTimeout = durationpb.New(wait)
	}
	resp, err := c.rpc.GetProcess(ctx, req)
	if err != nil {
		return ProcessView{}, err
	}
	v := c.processView(inspect.ProcessInfo(resp.GetProcess()))
	v.Inspect, v.InspectError = resp.GetInspect(), resp.GetInspectError()
	return v, nil
}

// Exit asks a process to exit with reason ("killed" when empty).
func (c *Client) Exit(ctx context.Context, node, target, reason string) error {
	t, err := parseTarget(target)
	if err != nil {
		return err
	}
	_, err = c.rpc.Exit(ctx, &inspectv1.ExitRequest{Node: node, Target: t, Reason: reason})
	return err
}

// SetLogLevel sets a process's log threshold.
func (c *Client) SetLogLevel(ctx context.Context, node, target string, level slog.Level) error {
	t, err := parseTarget(target)
	if err != nil {
		return err
	}
	_, err = c.rpc.SetLogLevel(ctx, &inspectv1.SetLogLevelRequest{Node: node, Target: t, Level: int32(level)})
	return err
}

// deadlinePassed reports whether err is ctx's own deadline, before ctx says
// so itself: the server's deadline timer ends the stream (RST_STREAM
// CANCEL), which the client reports as DeadlineExceeded once the deadline
// has passed.
func deadlinePassed(ctx context.Context, err error) bool {
	d, ok := ctx.Deadline()
	return ok && !time.Now().Before(d) && status.Code(err) == codes.DeadlineExceeded
}

// Watch streams node's events to fn until ctx is done or fn returns false.
// It returns nil when either ends it, and when the Inspector ends the stream
// cleanly (EOF), which it does once it sees ctx done.
func (c *Client) Watch(ctx context.Context, node string, fn func(EventView) bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.rpc.Watch(ctx, &inspectv1.WatchRequest{Node: node})
	if err != nil {
		// Not EOF: opening a stream reports a failed write as EOF.
		if ctx.Err() != nil || deadlinePassed(ctx, err) {
			return nil
		}
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) || deadlinePassed(ctx, err) {
				return nil
			}
			return err
		}
		if !fn(c.eventView(inspect.Event(resp.GetEvent()))) {
			return nil
		}
	}
}

// SortProcesses orders ps by pid (as listed), or by mailbox, received, sent
// or busy (BusySeconds), largest first.
func SortProcesses(ps []ProcessView, by string) error {
	var key func(ProcessView) float64
	switch by {
	case "", "pid":
		return nil
	case "mailbox":
		key = func(p ProcessView) float64 { return float64(p.Mailbox) }
	case "received":
		key = func(p ProcessView) float64 { return float64(p.Received) }
	case "sent":
		key = func(p ProcessView) float64 { return float64(p.Sent) }
	case "busy":
		key = func(p ProcessView) float64 { return p.BusySeconds }
	default:
		return fmt.Errorf("bad sort %q: want pid, mailbox, received, sent or busy", by)
	}
	slices.SortStableFunc(ps, func(a, b ProcessView) int { return cmp.Compare(key(b), key(a)) })
	return nil
}
