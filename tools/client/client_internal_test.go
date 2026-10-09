package client

import (
	"cmp"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/floatdrop/grpcproc/leader"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// fake answers GetNode for "a" (linked to "b", and failing to dial "c"
// and "d") and fails everything else, or hands out a scripted event stream
// that then ends with end (errFake by default).
type fake struct {
	inspectv1.InspectorClient
	events []*inspectv1.Event
	end    error
}

var errFake = errors.New("unavailable")

func (f *fake) GetNode(_ context.Context, req *inspectv1.GetNodeRequest, _ ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	switch req.GetNode() {
	case "":
		return &inspectv1.GetNodeResponse{Node: &inspectv1.NodeInfo{
			Id: &inspectv1.NodeID{Name: "a", Incarnation: 1},
			Links: []*inspectv1.Link{
				{Peer: &inspectv1.NodeID{Name: "b"}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_UP, Queued: 3, QueuedBytes: 7},
				{Peer: &inspectv1.NodeID{Name: "c"}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_DOWN, LastError: "refused",
					RetryAt: timestamppb.New(time.Now().Add(time.Hour))},
				{Peer: &inspectv1.NodeID{Name: "d"}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_DOWN, LastError: "refused",
					RetryAt: timestamppb.New(time.Now().Add(-time.Second))},
				// b again, as a down link: it is asked once, as a live peer.
				{Peer: &inspectv1.NodeID{Name: "b"}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_DOWN},
			},
		}}, nil
	}
	return nil, errFake
}

func (f *fake) ListProcesses(context.Context, *inspectv1.ListProcessesRequest, ...grpc.CallOption) (*inspectv1.ListProcessesResponse, error) {
	return nil, errFake
}

func (f *fake) Watch(context.Context, *inspectv1.WatchRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[inspectv1.WatchResponse], error) {
	if f.events == nil {
		return nil, errFake
	}
	return &stream{events: f.events, end: cmp.Or(f.end, errFake)}, nil
}

// noProcesses lists no processes on any node fake reaches.
type noProcesses struct{ *fake }

func (noProcesses) ListProcesses(context.Context, *inspectv1.ListProcessesRequest, ...grpc.CallOption) (*inspectv1.ListProcessesResponse, error) {
	return &inspectv1.ListProcessesResponse{}, nil
}

type stream struct {
	grpc.ClientStream
	events []*inspectv1.Event
	end    error
}

func (s *stream) Recv() (*inspectv1.WatchResponse, error) {
	if len(s.events) == 0 {
		return nil, s.end
	}
	e := s.events[0]
	s.events = s.events[1:]
	return &inspectv1.WatchResponse{Event: e}, nil
}

func TestFailuresAndOddities(t *testing.T) {
	c := &Client{rpc: &fake{}, now: time.Now}
	ctx := t.Context()
	// A node that never started has no uptime; a peer that cannot be
	// reached is listed with why.
	nodes, err := c.Cluster(ctx, ClusterOptions{LinksUpTo: 4})
	if err != nil || len(nodes) != 4 || nodes[0].Uptime != "" || nodes[0].Links[0].Age != "" || nodes[1].Name != "b" || nodes[1].Error == "" ||
		nodes[2].Name != "c" || nodes[2].Error == "" || nodes[3].Name != "d" || nodes[3].Error == "" {
		t.Fatalf("%+v %v", nodes, err)
	}
	// An Inspector of an earlier release adds no totals up: they are added
	// up from its links.
	if ts := nodes[0].LinkTotals; len(ts) != 1 || ts[0].Peers != 3 || ts[0].PeersDown != 2 || !slices.Equal(ts[0].Down, []string{"c", "d"}) || ts[0].Queued != 3 {
		t.Fatalf("%+v", ts)
	}
	// An outbound link's queue, and when a down one is dialed again: not
	// at all once that time has passed.
	if up, down, due := nodes[0].Links[0], nodes[0].Links[1], nodes[0].Links[2]; up.Queued != 3 || up.QueuedBytes != 7 || up.RetryIn != "" || down.State != "down" || down.RetryIn == "" || due.RetryIn != "" {
		t.Fatalf("%+v %+v %+v", up, down, due)
	}
	// A walk follows the links, and drops them past LinksUpTo.
	if nodes, _ := c.Cluster(ctx, ClusterOptions{LinksUpTo: 3}); len(nodes) != 4 || nodes[0].Links != nil {
		t.Fatalf("%+v", nodes)
	}
	if _, err := c.Node(ctx, "b"); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	if _, err := c.Processes(ctx, "", Filter{}); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	// When no node can say which elections it runs, that is the error.
	if _, err := c.Elections(ctx); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	// A node the walk could not ask may run it: that is the error.
	if _, _, err := c.engine(ctx, "", "orders"); err == nil || !strings.Contains(err.Error(), "b: ") {
		t.Fatal(err)
	}
	// Nodes that cannot be reached are not searched for electors, but may
	// run one: with none found elsewhere, that is the error.
	c.rpc = noProcesses{&fake{}}
	if all, err := c.Elections(ctx); err == nil || !strings.Contains(err.Error(), "b: ") {
		t.Fatal(all, err)
	}
	c.rpc = &fake{}
	if err := c.Watch(ctx, "", func(EventView) bool { return true }); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	// A stream that breaks is an error; a link-up event names its peer.
	c.rpc = &fake{events: []*inspectv1.Event{{Time: timestamppb.Now(), Kind: &inspectv1.Event_LinkUp{LinkUp: &inspectv1.NodeID{Name: "b", Incarnation: 2}}}}}
	var got []EventView
	if err := c.Watch(ctx, "", func(e EventView) bool { got = append(got, e); return true }); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Kind != "link-up" || got[0].Peer != "b#2" {
		t.Fatalf("%+v", got)
	}
	// A stream the Inspector ends cleanly is a clean end; a watch that
	// cannot open because ctx is done, too.
	c.rpc = &fake{events: []*inspectv1.Event{}, end: io.EOF}
	if err := c.Watch(ctx, "", func(EventView) bool { return true }); err != nil {
		t.Fatal(err)
	}
	done, cancel := context.WithCancel(ctx)
	cancel()
	c.rpc = &fake{}
	if err := c.Watch(done, "", func(EventView) bool { return true }); err != nil {
		t.Fatal(err)
	}
	// A cluster whose first node fails is an error.
	c.rpc = unreachable{}
	if _, err := c.Cluster(ctx, ClusterOptions{}); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	if _, err := c.Elections(ctx); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
}

// members answers GetNode as an Inspector of an earlier release would,
// listing links and members whatever it is asked, for "" (node a, whose
// Membership reports b, c and d, unless none) and b (linked to e, which
// only b's reports), and blocks until the request ends for the others.
type members struct {
	inspectv1.InspectorClient
	none bool
}

func (f members) GetNode(ctx context.Context, req *inspectv1.GetNodeRequest, _ ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	member := func(name, dc string) *inspectv1.Member {
		return &inspectv1.Member{Id: &inspectv1.NodeID{Name: name, Incarnation: 7}, Addr: name + ":1", Metadata: map[string]string{"dc": dc}}
	}
	up := inspectv1.LinkState_LINK_STATE_UP
	switch req.GetNode() {
	case "":
		info := &inspectv1.NodeInfo{
			Id:      &inspectv1.NodeID{Name: "a"},
			Links:   []*inspectv1.Link{{Peer: &inspectv1.NodeID{Name: "b"}, Outbound: true, State: up}},
			Members: []*inspectv1.Member{member("a", "west"), member("b", "west"), member("c", "east"), member("d", "east")},
		}
		if f.none {
			info.Members, info.Metadata = nil, map[string]string{"dc": "west"}
		}
		return &inspectv1.GetNodeResponse{Node: info}, nil
	case "b":
		return &inspectv1.GetNodeResponse{Node: &inspectv1.NodeInfo{
			Id: &inspectv1.NodeID{Name: "b"},
			Links: []*inspectv1.Link{
				{Peer: &inspectv1.NodeID{Name: "a"}, State: up, Messages: 2},
				{Peer: &inspectv1.NodeID{Name: "e"}, Outbound: true, State: up, Messages: 5},
			},
			Members: []*inspectv1.Member{member("b", "west"), member("e", "east")},
		}}, nil
	}
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func TestClusterOfMembers(t *testing.T) {
	c := &Client{rpc: members{}, now: time.Now}
	// Totals left out are added up by the peers' metadata as the first
	// node's Membership reports it; only the first node lists members, and
	// links only up to LinksUpTo nodes.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	// Nor are a node's peers followed, only the first's: e is not listed.
	nodes, err := c.Cluster(ctx, ClusterOptions{GroupLinksBy: "dc", NodeTimeout: time.Millisecond, Parallel: 2})
	if err != nil || len(nodes) != 4 {
		t.Fatalf("%+v %v", nodes, err)
	}
	a, b, cn := nodes[0], nodes[1], nodes[2]
	if len(a.Members) != 4 || a.Links != nil || len(b.Members) != 0 || b.Links != nil {
		t.Fatalf("a %+v\nb %+v", a, b)
	}
	if len(a.LinkTotals) != 1 || a.LinkTotals[0].Group != "west" {
		t.Fatalf("a's totals, by its own members: %+v", a.LinkTotals)
	}
	// e is in no group: the Membership does not report it.
	if len(b.LinkTotals) != 2 || b.LinkTotals[0].Group != "" || b.LinkTotals[0].MessagesSent != 5 || b.LinkTotals[1].Group != "west" || b.LinkTotals[1].MessagesReceived != 2 {
		t.Fatalf("%+v", b.LinkTotals)
	}
	// A node that does not answer in NodeTimeout is an error, shown as its
	// Membership reports it.
	if cn.Name != "c" || cn.Unanswered || cn.Error == "" || cn.Reached() || cn.Advertise != "c:1" || cn.Metadata["dc"] != "east" {
		t.Fatalf("%+v", cn)
	}
	// One ctx ended on, asked or waiting to be, is unanswered.
	ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	nodes, err = c.Cluster(ctx, ClusterOptions{LinksUpTo: 5, Parallel: 1})
	// b answers with its links, unless c or d took the one slot first.
	if err != nil || len(nodes) < 4 || nodes[0].Links == nil || nodes[1].Reached() && nodes[1].Links == nil || !nodes[1].Reached() && !nodes[1].Unanswered {
		t.Fatalf("%+v %v", nodes, err)
	}
	for _, n := range nodes[2:] {
		if !n.Unanswered || n.Error != "" || n.Reached() || n.Name != "e" && n.Incarnation != 7 {
			t.Fatalf("%+v", n)
		}
	}
	// Without a Membership, a walk: b's peer e is found, and asked, and the
	// links are grouped by what each node walked says of itself: a's dc,
	// and nothing for e, which did not answer.
	c.rpc = members{none: true}
	nodes, err = c.Cluster(t.Context(), ClusterOptions{GroupLinksBy: "dc", NodeTimeout: time.Millisecond, Parallel: -1})
	if err != nil || len(nodes) != 3 || nodes[2].Name != "e" || nodes[2].Error == "" {
		t.Fatalf("%+v %v", nodes, err)
	}
	if ts := nodes[1].LinkTotals; len(ts) != 2 || ts[0].Group != "" || ts[0].MessagesSent != 5 || ts[1].Group != "west" || ts[1].MessagesReceived != 2 {
		t.Fatalf("%+v", ts)
	}
}

// A walk for a caller takes half of what ctx has left, and a node that
// hangs a quarter, so that what the caller does next has the other half.
func TestWalkFor(t *testing.T) {
	c := &Client{rpc: members{}, now: time.Now}
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	begin := time.Now()
	nodes, err := c.walkFor(ctx)
	if took := time.Since(begin); err != nil || len(nodes) != 4 || took < 100*time.Millisecond || took > 300*time.Millisecond {
		t.Fatalf("%v: %+v %v", took, nodes, err)
	}
	for _, n := range nodes[2:] {
		if n.Reached() {
			t.Fatalf("%+v", n)
		}
	}
}

func TestDeadlinePassed(t *testing.T) {
	passed, cancelPassed := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelPassed()
	later, cancelLater := context.WithTimeout(t.Context(), time.Hour)
	defer cancelLater()
	deadline := status.Error(codes.DeadlineExceeded, "deadline")
	for i, tc := range []struct {
		ctx  context.Context
		err  error
		want bool
	}{
		{passed, deadline, true},       // gRPC reported our deadline first
		{later, deadline, false},       // not our deadline: it has not passed
		{t.Context(), deadline, false}, // no deadline of ours
		{passed, status.Error(codes.Unavailable, "gone"), false},
		{passed, io.EOF, false},
	} {
		if got := deadlinePassed(tc.ctx, tc.err); got != tc.want {
			t.Errorf("case %d (%v): got %v", i, tc.err, got)
		}
	}
}

// halfDown answers for a cluster of a, which cannot list its processes, and
// b, which runs an elector of "x" (leading it), a cron and a saga engine.
type halfDown struct{ inspectv1.InspectorClient }

func (halfDown) GetNode(_ context.Context, req *inspectv1.GetNodeRequest, _ ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	if req.GetNode() == "" {
		return &inspectv1.GetNodeResponse{Node: &inspectv1.NodeInfo{
			Id:    &inspectv1.NodeID{Name: "a"},
			Links: []*inspectv1.Link{{Peer: &inspectv1.NodeID{Name: "b"}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_UP}},
		}}, nil
	}
	return &inspectv1.GetNodeResponse{Node: &inspectv1.NodeInfo{Id: &inspectv1.NodeID{Name: req.GetNode()}}}, nil
}

func (halfDown) ListProcesses(_ context.Context, req *inspectv1.ListProcessesRequest, _ ...grpc.CallOption) (*inspectv1.ListProcessesResponse, error) {
	if req.GetNode() != "b" {
		return nil, errFake
	}
	pid := func(id uint64) *grpcprocv1.PID { return &grpcprocv1.PID{Node: "b", Incarnation: 1, Id: id} }
	var ps []*inspectv1.ProcessInfo
	switch {
	case req.GetName() != "":
		ps = append(ps, &inspectv1.ProcessInfo{Pid: pid(1), Name: leader.ElectorName("x")})
	case req.GetLabel() == cronLabel:
		ps = append(ps, &inspectv1.ProcessInfo{Pid: pid(2), Type: cronType})
	case req.GetLabel() == sagaLabel:
		ps = append(ps, &inspectv1.ProcessInfo{Pid: pid(3)})
	}
	return &inspectv1.ListProcessesResponse{Processes: ps}, nil
}

func (halfDown) GetProcess(_ context.Context, req *inspectv1.GetProcessRequest, _ ...grpc.CallOption) (*inspectv1.GetProcessResponse, error) {
	resp := &inspectv1.GetProcessResponse{Process: &inspectv1.ProcessInfo{Pid: &grpcprocv1.PID{Node: "b", Incarnation: 1, Id: 9}}}
	if req.GetTarget().GetName() == leader.ElectorName("x") {
		resp.Inspect = map[string]string{"role": "leader", "term": "2"}
	}
	return resp, nil
}

// A node that cannot be asked fails only itself: it is listed in each
// election with why, and the others' crons and saga engines are found.
func TestOneNodeFailing(t *testing.T) {
	c := &Client{rpc: halfDown{}, now: time.Now}
	ctx := t.Context()
	all, err := c.Elections(ctx)
	if err != nil || len(all) != 1 || all[0].Leading != "b" || len(all[0].Electors) != 2 || all[0].Electors[0].Error != errFake.Error() {
		t.Fatalf("%+v %v", all, err)
	}
	if crons, err := c.Crons(ctx, ""); err != nil || len(crons) != 2 || crons[0].Node != "a" || crons[0].Error == "" || crons[1].Node != "b" {
		t.Fatalf("%+v %v", crons, err)
	}
	if engines, err := c.SagaEngines(ctx, ""); err != nil || len(engines) != 2 || engines[0].Node != "a" || engines[0].Error == "" || engines[1].Node != "b" {
		t.Fatalf("%+v %v", engines, err)
	}
	if engines, err := c.SagaEngines(ctx, "b"); err != nil || len(engines) != 1 {
		t.Fatalf("%+v %v", engines, err)
	}
	if _, err := c.SagaEngines(ctx, "a"); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	if _, node, err := c.engine(ctx, "", ""); err != nil || node != "b" {
		t.Fatal(node, err)
	}
	if got := (NodeView{Unanswered: true}).Problem(); got != NotAnswered {
		t.Fatal(got)
	}
	// Nodes the walk could not ask: why is the first's, and they are listed
	// with it.
	gone := []NodeView{{Name: "x", Unanswered: true}}
	if _, err := c.ElectionsOf(ctx, gone, 0); err == nil || err.Error() != "x: "+NotAnswered {
		t.Fatal(err)
	}
	if crons := c.CronsOf(ctx, gone, 0); len(crons) != 1 || crons[0].Error != NotAnswered {
		t.Fatalf("%+v", crons)
	}
	if engines := c.SagaEnginesOf(ctx, gone, 0); len(engines) != 1 || engines[0].Error != NotAnswered {
		t.Fatalf("%+v", engines)
	}
}
