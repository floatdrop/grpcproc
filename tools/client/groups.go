package client

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/groups"
	groupsv1 "github.com/floatdrop/grpcproc/groups/proto/grpcproc/groups/v1"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// ErrNoGroups is the error of a node that runs no grpcproc/groups scope, or
// none of the name asked.
var ErrNoGroups = errors.New("no grpcproc/groups scope")

// GroupsView is a grpcproc/groups scope's groups, as one node's copy has
// them.
type GroupsView struct {
	Node    string          `json:"node" jsonschema:"the node whose copy of the groups this is"`
	Scope   string          `json:"scope"`
	PID     string          `json:"pid" jsonschema:"the scope's process"`
	Scopes  []string        `json:"scopes" jsonschema:"every scope the node runs"`
	Groups  []GroupView     `json:"groups"`
	Omitted int             `json:"omitted,omitzero" jsonschema:"groups left out past the limit"`
	Peers   []GroupPeerView `json:"peers" jsonschema:"the other nodes the scope knows"`
}

// GroupView is one group's counts.
type GroupView struct {
	Name    string `json:"name"`
	Members int    `json:"members" jsonschema:"its members on every node"`
	Local   int    `json:"local" jsonschema:"its members on the node whose copy this is"`
	Nodes   int    `json:"nodes" jsonschema:"nodes with a member"`
}

// GroupPeerView is another node's scope, as this one knows it.
type GroupPeerView struct {
	Node    string `json:"node"`
	State   string `json:"state" jsonschema:"syncing: asked, its members not yet known; synced: its members are known; absent: it runs no such scope; unreachable: lost, and asked again later"`
	Scope   string `json:"scope,omitempty" jsonschema:"its scope's pid, once synced"`
	Members int    `json:"members" jsonschema:"its processes' memberships, in every group"`
}

// GroupMembersView is one group's members, as one node's copy has them.
type GroupMembersView struct {
	Node    string   `json:"node"`
	Scope   string   `json:"scope"`
	Group   string   `json:"group"`
	Members []string `json:"members" jsonschema:"their pids, ordered by node"`
	Omitted int      `json:"omitted,omitzero" jsonschema:"members left out past the limit"`
}

// Groups lists the groups of scope ("" for the default scope, or the
// node's first) whose names
// start with prefix, at most limit of them (1000 when 0), as node's copy
// has them; "" is the node serving the Inspector.
func (c *Client) Groups(ctx context.Context, node, scope, prefix string, limit int) (GroupsView, error) {
	t, v, err := c.scope(ctx, node, scope)
	if err != nil {
		return GroupsView{}, err
	}
	r, err := c.askGroups(ctx, t, &groupsv1.Query{Prefix: prefix, Limit: clampLimit(limit)})
	if err != nil {
		return GroupsView{}, err
	}
	v.Groups, v.Peers, v.Omitted = []GroupView{}, []GroupPeerView{}, int(r.GetOmitted())
	for _, g := range r.GetGroups() {
		v.Groups = append(v.Groups, GroupView{Name: g.GetName(), Members: int(g.GetMembers()), Local: int(g.GetLocal()), Nodes: int(g.GetNodes())})
	}
	for _, p := range r.GetPeers() {
		pv := GroupPeerView{Node: p.GetNode(), State: p.GetState(), Members: int(p.GetMembers())}
		if p.GetScope() != nil {
			pv.Scope = grpcproc.PIDFromProto(p.GetScope()).String()
		}
		v.Peers = append(v.Peers, pv)
	}
	return v, nil
}

// GroupMembers lists the members of group in scope, at most limit of them
// (1000 when 0), as node's copy has them.
func (c *Client) GroupMembers(ctx context.Context, node, scope, group string, limit int) (GroupMembersView, error) {
	if group == "" {
		return GroupMembersView{}, errors.New("a group is required")
	}
	t, v, err := c.scope(ctx, node, scope)
	if err != nil {
		return GroupMembersView{}, err
	}
	r, err := c.askGroups(ctx, t, &groupsv1.Query{Group: group, Limit: clampLimit(limit)})
	if err != nil {
		return GroupMembersView{}, err
	}
	out := GroupMembersView{Node: v.Node, Scope: v.Scope, Group: group, Members: []string{}, Omitted: int(r.GetOmitted())}
	for _, p := range r.GetMembers() {
		out.Members = append(out.Members, grpcproc.PIDFromProto(p).String())
	}
	return out, nil
}

func clampLimit(n int) uint32 { return uint32(min(max(n, 0), 1<<20)) }

// scope finds the scope named scope on node, or the default one, or its
// first, and the view of it to fill in.
func (c *Client) scope(ctx context.Context, node, scope string) (*inspectv1.Target, GroupsView, error) {
	ps, err := c.Processes(ctx, node, Filter{Label: groups.Label})
	if err != nil {
		return nil, GroupsView{}, err
	}
	byScope := map[string]ProcessView{}
	for _, p := range ps {
		if s, ok := strings.CutPrefix(p.Name, groups.ScopeName("")); ok {
			byScope[s] = p
		}
	}
	scopes := slices.Sorted(maps.Keys(byScope))
	if len(scopes) == 0 {
		return nil, GroupsView{}, fmt.Errorf("%w on %s", ErrNoGroups, cmp.Or(node, "the node serving the Inspector"))
	}
	if scope == "" {
		scope = scopes[0]
		if _, ok := byScope[groups.DefaultScope]; ok {
			scope = groups.DefaultScope
		}
	}
	p, ok := byScope[scope]
	if !ok {
		return nil, GroupsView{}, fmt.Errorf("%w named %q on %s: it runs %s", ErrNoGroups, scope, cmp.Or(node, "the node serving the Inspector"), strings.Join(scopes, ", "))
	}
	t, _ := parseTarget(p.PID) // as the Inspector wrote it
	return t, GroupsView{Node: t.GetPid().GetNode(), Scope: scope, PID: p.PID, Scopes: scopes}, nil
}

// askGroups puts q to the scope t, as an Inspector Query.
func (c *Client) askGroups(ctx context.Context, t *inspectv1.Target, q *groupsv1.Query) (*groupsv1.Report, error) {
	body, _ := anypb.New(q) // a Query always marshals
	resp, err := c.rpc.Query(ctx, &inspectv1.QueryRequest{Target: t, Body: body})
	if err != nil {
		return nil, err
	}
	r := &groupsv1.Report{}
	return r, resp.GetBody().UnmarshalTo(r)
}
