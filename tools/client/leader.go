package client

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/floatdrop/grpcproc/leader"
	leaderv1 "github.com/floatdrop/grpcproc/leader/proto/grpcproc/leader/v1"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// ElectorView is one node's part in a grpcproc/leader election, as its
// elector says: comparing the nodes is how a split view shows.
type ElectorView struct {
	Node        string   `json:"node"`
	Role        string   `json:"role,omitempty" jsonschema:"leader, candidate, follower, or unclustered (its view is too small to elect)"`
	Term        uint64   `json:"term,omitzero"`
	Leader      string   `json:"leader,omitempty" jsonschema:"the node it follows; empty while it knows none"`
	VotedFor    string   `json:"voted_for,omitempty"`
	View        []string `json:"view,omitempty" jsonschema:"the nodes whose majority elects"`
	Quorum      int      `json:"quorum,omitzero"`
	State       string   `json:"state,omitempty" jsonschema:"the version of the replicated state it holds: the term of the leader that made it, then its sequence"`
	Cordoned    []string `json:"cordoned,omitempty" jsonschema:"nodes that may not lead"`
	Unreachable []string `json:"unreachable,omitempty" jsonschema:"nodes of its view it has not heard from since it lost them"`
	Singleton   string   `json:"singleton,omitempty" jsonschema:"the singleton: its pid on the leader, or none, starting, stopping"`
	Backoff     string   `json:"backoff,omitempty" jsonschema:"how long it holds off campaigning: after its singleton failed, or after it handed over"`
	Error       string   `json:"error,omitempty" jsonschema:"why the node could not be asked"`
}

// Election describes the election called cluster on every node Cluster
// finds that takes part in it, in Cluster's order, asking them at once. A
// node that runs no elector for it is left out; one that cannot be asked is
// listed with its Error.
func (c *Client) Election(ctx context.Context, cluster string) ([]ElectorView, error) {
	nodes, err := c.walkFor(ctx)
	if err != nil {
		return nil, err
	}
	timeout := quarter(ctx)
	asked := each(ctx, nodes, timeout, func(ctx context.Context, n NodeView) *ElectorView {
		if !n.Reached() {
			return &ElectorView{Node: n.Name, Error: n.Problem()}
		}
		return c.elector(ctx, n.Name, cluster)
	})
	out := []ElectorView{}
	for _, v := range asked {
		if v != nil {
			out = append(out, *v)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no node runs an election called %q", cluster)
	}
	return out, nil
}

// elector describes node's elector of cluster: nil when it runs none.
func (c *Client) elector(ctx context.Context, node, cluster string) *ElectorView {
	resp, err := c.rpc.GetProcess(ctx, &inspectv1.GetProcessRequest{Node: node, Target: electorOf(cluster), Inspect: true})
	var v ElectorView
	switch {
	case status.Code(err) == codes.NotFound:
		return nil
	case err != nil:
		v = ElectorView{Node: node, Error: status.Convert(err).Message()}
	case resp.GetInspectError() != "":
		v = ElectorView{Node: node, Error: resp.GetInspectError()}
	default:
		v = electorView(node, resp.GetInspect())
	}
	return &v
}

// ElectionView is one grpcproc/leader election, as each node that takes
// part sees it.
type ElectionView struct {
	Cluster  string        `json:"cluster"`
	Leading  string        `json:"leading,omitempty" jsonschema:"the leader of the highest term any node knows; empty while none leads"`
	Electors []ElectorView `json:"electors"`
}

// Elections is ElectionsOf the nodes Cluster finds.
func (c *Client) Elections(ctx context.Context) ([]ElectionView, error) {
	nodes, err := c.walkFor(ctx)
	if err != nil {
		return nil, err
	}
	return c.ElectionsOf(ctx, nodes, quarter(ctx))
}

// ElectionsOf finds every grpcproc/leader election that one of nodes takes
// part in, by the names its electors are registered under, and describes
// each, in order of name, as the nodes that run it see it, in nodes'
// order. It asks each node at once, each within timeout (0: ctx alone). A
// node that cannot be asked is listed in each election with its Error; an
// election whose electors all exit while it is asked, with none. When no
// election is found and a node could not be asked, which may run one, that
// is the error.
func (c *Client) ElectionsOf(ctx context.Context, nodes []NodeView, timeout time.Duration) ([]ElectionView, error) {
	prefix := leader.ElectorName("")
	type listed struct {
		elections []string
		err       string
		cause     error
	}
	lists := each(ctx, nodes, timeout, func(ctx context.Context, n NodeView) listed {
		if !n.Reached() {
			return listed{err: n.Problem()}
		}
		ps, err := c.Processes(ctx, n.Name, Filter{Name: prefix})
		if err != nil {
			return listed{err: message(err), cause: err}
		}
		var l listed
		for _, p := range ps {
			// The filter matches a substring: "myleader/x" is not an elector.
			if cluster, ok := strings.CutPrefix(p.Name, prefix); ok {
				l.elections = append(l.elections, cluster)
			}
		}
		return l
	})
	type part struct{ node, cluster string }
	var parts []part
	for i, l := range lists {
		for _, e := range l.elections {
			parts = append(parts, part{nodes[i].Name, e})
		}
	}
	views := each(ctx, parts, timeout, func(ctx context.Context, p part) *ElectorView { return c.elector(ctx, p.node, p.cluster) })
	byPart := map[part]*ElectorView{}
	names := map[string]bool{}
	for i, p := range parts {
		byPart[p] = views[i]
		names[p.cluster] = true
	}
	if len(names) == 0 {
		if i := slices.IndexFunc(lists, func(l listed) bool { return l.err != "" }); i >= 0 {
			if lists[i].cause != nil {
				return nil, lists[i].cause
			}
			return nil, fmt.Errorf("%s: %s", nodes[i].Name, lists[i].err)
		}
	}
	out := make([]ElectionView, 0, len(names))
	for _, name := range slices.Sorted(maps.Keys(names)) {
		e := ElectionView{Cluster: name, Electors: []ElectorView{}}
		for i, n := range nodes {
			switch v := byPart[part{n.Name, name}]; {
			case lists[i].err != "":
				e.Electors = append(e.Electors, ElectorView{Node: n.Name, Error: lists[i].err})
			case v != nil:
				e.Electors = append(e.Electors, *v)
			}
		}
		e.Leading = Leading(e.Electors)
		out = append(out, e)
	}
	return out, nil
}

// Leading is the node views say leads: the leader of the highest term, or
// empty while none does.
func Leading(views []ElectorView) string {
	var lead string
	var term uint64
	for _, v := range views {
		if v.Role == leader.Leader.String() && v.Term >= term {
			lead, term = v.Node, v.Term
		}
	}
	return lead
}

// MoveLeader asks the leader of cluster to hand over: to the node to, or,
// with to empty, to the follower with the latest state. It returns the node
// that led, once that node has told its successor to campaign.
func (c *Client) MoveLeader(ctx context.Context, cluster, to string) (string, error) {
	return c.toLeader(ctx, cluster, &leaderv1.Resign{To: to})
}

// Cordon keeps node from leading cluster, or, with off, lets it lead again.
// It returns the node that led, once a majority holds the change; if node
// was that leader, it is handing over.
func (c *Client) Cordon(ctx context.Context, cluster, node string, off bool) (string, error) {
	return c.toLeader(ctx, cluster, &leaderv1.Cordon{Node: node, Off: off})
}

// toLeader calls the elector of cluster's leader with m.
func (c *Client) toLeader(ctx context.Context, cluster string, m proto.Message) (string, error) {
	views, err := c.Election(ctx, cluster)
	if err != nil {
		return "", err
	}
	lead := Leading(views)
	if lead == "" {
		return "", fmt.Errorf("no node leads %q now: try again once one does", cluster)
	}
	body, err := anypb.New(m)
	if err != nil {
		return "", err
	}
	_, err = c.rpc.Call(ctx, &inspectv1.CallRequest{Node: lead, Target: electorOf(cluster), Body: body})
	return lead, err
}

func electorOf(cluster string) *inspectv1.Target {
	return &inspectv1.Target{Kind: &inspectv1.Target_Name{Name: leader.ElectorName(cluster)}}
}

// electorView reads what an elector publishes about itself.
func electorView(node string, m map[string]string) ElectorView {
	term, _ := strconv.ParseUint(m["term"], 10, 64)
	quorum, _ := strconv.Atoi(m["quorum"])
	return ElectorView{
		Node: node, Role: m["role"], Term: term, Leader: m["leader"], VotedFor: m["voted_for"],
		View: list(m["view"]), Quorum: quorum, State: m["state"], Cordoned: list(m["cordoned"]),
		Unreachable: list(m["unreachable"]), Singleton: m["singleton"], Backoff: m["backoff"],
	}
}

func list(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
