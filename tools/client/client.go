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
	v, _, err := c.node(ctx, &inspectv1.GetNodeRequest{Node: node}, nil)
	return v, err
}

// node asks for a node as req says, and returns it with the peers its links
// and members name. Totals a node of an earlier release left out are added
// up from its links, by req's key of the peers' metadata as members has it,
// or as the node's own members do when members is nil.
func (c *Client) node(ctx context.Context, req *inspectv1.GetNodeRequest, members map[string]MemberView) (NodeView, []string, error) {
	resp, err := c.rpc.GetNode(ctx, req)
	if err != nil {
		return NodeView{}, nil, err
	}
	info := inspect.NodeInfo(resp.GetNode())
	v := c.nodeView(info)
	var peers []string
	for _, l := range info.Links {
		peers = append(peers, l.Peer.Name)
	}
	for _, m := range inspect.Members(resp.GetNode()) {
		v.Members = append(v.Members, MemberView{Name: m.Name, Incarnation: m.Incarnation, Addr: m.Addr, Metadata: m.Metadata})
		peers = append(peers, m.Name)
	}
	totals := inspect.Totals(resp)
	if len(totals) == 0 && len(info.Links) > 0 {
		if members == nil {
			members = byName(v.Members)
		}
		key := req.GetLinkTotalsBy()
		totals = inspect.SumLinks(info.Links, func(peer string) string {
			if key == "" {
				return ""
			}
			return members[peer].Metadata[key]
		})
	}
	for _, t := range totals {
		v.LinkTotals = append(v.LinkTotals, linkTotalsView(t))
	}
	if req.GetExcludeLinks() {
		v.Links = nil
	}
	return v, peers, nil
}

func byName(ms []MemberView) map[string]MemberView {
	out := make(map[string]MemberView, len(ms))
	for _, m := range ms {
		out[m.Name] = m
	}
	return out
}

const (
	// WalkTime is how long grpcprocctl's commands, MCP tools and web UI
	// give Cluster, unless their limit for each request is longer.
	WalkTime = 30 * time.Second
	// LinksUpTo is the ClusterOptions.LinksUpTo they pass.
	LinksUpTo = 32
)

// ClusterOptions says what Cluster asks each node for.
type ClusterOptions struct {
	// LinksUpTo lists each node's links (NodeView.Links) when Cluster finds
	// at most this many nodes; past it, a node's links are only added up, as
	// a node of a large cluster has many.
	LinksUpTo int
	// GroupLinksBy adds each node's links up by this key of its peers'
	// metadata (NodeView.LinkTotals); empty adds them all up together.
	GroupLinksBy string
	// NodeTimeout bounds the request to each node, so that one that hangs
	// only fails itself; 0 leaves only ctx, and one that hangs holds one of
	// the Parallel requests until ctx ends.
	NodeTimeout time.Duration
	// Parallel is how many nodes are asked at once: 32 when 0.
	Parallel int
}

// Cluster describes the node serving the Inspector, first, and the others
// it knows of, ordered by name: those its Config.Membership reports up, and
// those it is linked to. When it reports none, as without a Membership, it
// walks the links of every node it finds. Only the first node lists its
// members. A node that
// cannot be asked is listed with Error, and a member that cannot be with
// the address and metadata its Membership reports; one that ctx ended
// before it was asked or answered is listed as Unanswered.
func (c *Client) Cluster(ctx context.Context, o ClusterOptions) ([]NodeView, error) {
	rctx, cancel := withTimeout(ctx, o.NodeTimeout)
	first, peers, err := c.node(rctx, &inspectv1.GetNodeRequest{LinkTotalsBy: o.GroupLinksBy}, nil)
	cancel()
	if err != nil {
		return nil, err
	}
	members := byName(first.Members)
	seen := map[string]bool{first.Name: true}
	var todo []string
	for _, p := range peers {
		if !seen[p] {
			seen[p] = true
			todo = append(todo, p)
		}
	}
	// A walk follows every node's links and members, and drops the links
	// past LinksUpTo once done.
	walk := len(first.Members) == 0
	links := walk || 1+len(todo) <= o.LinksUpTo
	groupBy := members // a walk's old-release nodes group by their own
	if walk {
		groupBy = nil
	}
	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		found []NodeView
		slots = make(chan struct{}, cmp.Or(max(o.Parallel, 0), 32))
	)
	var ask func(peer string)
	ask = func(peer string) {
		wg.Go(func() {
			v := NodeView{Name: peer, Unanswered: true}
			var more []string
			select {
			case slots <- struct{}{}:
				v, more = c.ask(ctx, peer, o, links, groupBy)
				<-slots
			case <-ctx.Done():
			}
			if m, ok := members[peer]; ok && (v.Error != "" || v.Unanswered) {
				v.Incarnation, v.Advertise, v.Metadata = m.Incarnation, m.Addr, m.Metadata
			}
			mu.Lock()
			defer mu.Unlock()
			found = append(found, v)
			if !walk {
				return
			}
			for _, p := range more {
				if !seen[p] {
					seen[p] = true
					ask(p)
				}
			}
		})
	}
	mu.Lock()
	for _, p := range todo {
		ask(p)
	}
	mu.Unlock()
	wg.Wait()
	slices.SortFunc(found, func(a, b NodeView) int { return cmp.Compare(a.Name, b.Name) })
	all := append([]NodeView{first}, found...)
	if len(all) > o.LinksUpTo {
		for i := range all {
			all[i].Links = nil
		}
	}
	return all, nil
}

// withTimeout bounds ctx by d, unless d is 0.
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d > 0 {
		return context.WithTimeout(ctx, d)
	}
	return ctx, func() {}
}

// walkFor runs Cluster in half the time ctx has left, a quarter for each
// node, so that nodes that hang leave the rest to what the caller asks the
// others next.
func (c *Client) walkFor(ctx context.Context) ([]NodeView, error) {
	var o ClusterOptions
	if d, ok := ctx.Deadline(); ok {
		left := time.Until(d)
		o.NodeTimeout = max(left/4, time.Nanosecond)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, left/2)
		defer cancel()
	}
	return c.Cluster(ctx, o)
}

// ask describes peer for Cluster: Unanswered when ctx ended first.
func (c *Client) ask(ctx context.Context, peer string, o ClusterOptions, links bool, members map[string]MemberView) (NodeView, []string) {
	rctx, cancel := withTimeout(ctx, o.NodeTimeout)
	defer cancel()
	req := &inspectv1.GetNodeRequest{Node: peer, ExcludeLinks: !links, ExcludeMembers: true, LinkTotalsBy: o.GroupLinksBy}
	v, more, err := c.node(rctx, req, members)
	switch code := status.Code(err); {
	case (code == codes.DeadlineExceeded || code == codes.Canceled) && ctx.Err() != nil:
		return NodeView{Name: peer, Unanswered: true}, nil
	case err != nil:
		return NodeView{Name: peer, Error: err.Error()}, nil
	}
	v.Members = nil
	return v, more
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
