// Package groups is process groups on grpcproc, after Erlang's pg: named
// sets of processes, on any node, that any process can list and watch. A
// process joins a group on its own node; every node that runs the same
// scope learns of it, and of its leaving, or its exit, or its node going
// away. It uses only grpcproc's public API, so it can be ignored or
// replaced.
//
// Every node that takes part runs a scope:
//
//	workers, err := groups.Start(node, groups.Config{Scope: "jobs"})
//
// A process joins, and any process on any node of the scope finds the
// members, read from the node's own copy without a message:
//
//	err = workers.Join(ctx, p, "resize", p.PID())
//	…
//	members := workers.Members("resize") // every node's, ordered
//
// A process that must hear of changes watches a group:
//
//	w, err := workers.Watch(ctx, p, "resize")
//	for {
//		m, err := p.Receive()
//		…	// a *groupsv1.Changed from w.From; a Down with w.Ref, the end of them
//	}
//
// The copies are eventually consistent: a node's own joins are in its copy
// once Join returns, another node's as soon as its message arrives. A node
// that cannot be reached has its members dropped until it can again.
package groups

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	groupsv1 "github.com/floatdrop/grpcproc/groups/proto/grpcproc/groups/v1"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// DefaultScope is the scope of a Config that names none.
const DefaultScope = "default"

// Label is the label of a scope's process, by which tools find it;
// RelayLabel is its relays'.
const (
	Label      = "groups"
	RelayLabel = "groups relay"
)

// maxGroup bounds a group's name, in bytes.
const maxGroup = 1024

var (
	// ErrNotLocal is what Join, Leave and Watch answer for a process of
	// another node than the scope's.
	ErrNotLocal = errors.New("groups: not a process of the scope's node")
	// ErrGroup is what they answer for a group's name that is empty, not
	// valid UTF-8, or longer than 1 KiB.
	ErrGroup = errors.New("groups: a group's name must be valid UTF-8, of 1 to 1024 bytes")
)

// ScopeName is the name a scope's process is registered under, on every
// node that runs the scope.
func ScopeName(scope string) string { return "groups/" + scope }

// Config configures a node's scope.
type Config struct {
	// Scope names the groups: the nodes whose scopes share a name share
	// their groups. Default DefaultScope.
	Scope string
	// Peers are nodes asked for their members from the start, whatever
	// Membership reports.
	Peers []string
	// Membership reports the nodes of the cluster: each one it reports up
	// that Select takes is asked for its members, and each one it reports
	// down is forgotten, unless Peers names it. Unset, it is the node's own
	// Config.Membership. With neither, only Peers are asked, and the nodes
	// that ask this one.
	Membership grpcproc.Membership
	// Select, if set, picks which nodes Membership reports up are asked:
	// those that run the scope, by their Metadata, say; one it stops taking
	// is only answered, as a node that asks this one is.
	// It runs on the scope's membership watch, not on its process.
	Select func(grpcproc.Member) bool
	// Retry is how long after a node could not be reached, or ran no such
	// scope, it is asked again, doubling with each loss in a row up to 32
	// times as long, and how long after the membership watch ends it is
	// started again. Default 1s.
	Retry time.Duration
}

// Scope is a node's part of a scope: its process, and its copy of every
// node's groups.
type Scope struct {
	s   *scope
	pid grpcproc.PID
}

// Start runs n's part of the scope cfg names: a process registered as
// ScopeName(cfg.Scope), which lasts until it is asked to exit or n stops.
// The groups of a scope that ended are empty, and its processes left them.
func Start(n *grpcproc.Node, cfg Config) (*Scope, error) {
	cfg.Scope = cmp.Or(cfg.Scope, DefaultScope)
	if cfg.Retry <= 0 {
		cfg.Retry = time.Second
	}
	if cfg.Membership == nil {
		cfg.Membership = n.Membership()
	}
	s := newScope(n, cfg)
	a, err := n.Spawn(s.run, grpcproc.WithName(s.name), grpcproc.WithLabel(Label),
		grpcproc.WithInspect(s.inspect), grpcproc.WithQuery(s.query))
	if err != nil {
		return nil, fmt.Errorf("groups: start scope %s: %w", cfg.Scope, err)
	}
	return &Scope{s, a.PID()}, nil
}

// PID is the scope's process.
func (sc *Scope) PID() grpcproc.PID { return sc.pid }

// Join adds pids, processes of the scope's node, to group, from the Node or
// a Process, as for grpcproc.Addr.Call. A process joins a group once, and
// is in it until it leaves it or exits. When Join returns, Members on this
// node has them; the other nodes hear of it after.
func (sc *Scope) Join(ctx context.Context, from grpcproc.Caller, group string, pids ...grpcproc.PID) error {
	return sc.call(ctx, from, &groupsv1.Join{Group: group, Pids: protos(pids)})
}

// Leave removes pids, processes of the scope's node, from group. A process
// that is not in it is left alone.
func (sc *Scope) Leave(ctx context.Context, from grpcproc.Caller, group string, pids ...grpcproc.PID) error {
	return sc.call(ctx, from, &groupsv1.Leave{Group: group, Pids: protos(pids)})
}

func (sc *Scope) call(ctx context.Context, from grpcproc.Caller, req proto.Message) error {
	_, err := grpcproc.AddrOf[proto.Message](sc.pid).Call[*emptypb.Empty](ctx, from, req)
	return err
}

// Members returns the members of group on every node of the scope, as this
// node knows them, ordered by node, incarnation and id.
func (sc *Scope) Members(group string) []grpcproc.PID {
	sc.s.mu.RLock()
	defer sc.s.mu.RUnlock()
	return sorted(sc.s.groups[group])
}

// Local returns the members of group on the scope's node, ordered.
func (sc *Scope) Local(group string) []grpcproc.PID {
	return slices.DeleteFunc(sc.Members(group), func(pid grpcproc.PID) bool { return pid.Node != sc.pid.Node })
}

// Groups returns the groups with members, on any node, ordered.
func (sc *Scope) Groups() []string {
	sc.s.mu.RLock()
	defer sc.s.mu.RUnlock()
	return slices.Sorted(maps.Keys(sc.s.groups))
}

// Watch is a process's watch of a group.
type Watch struct {
	Group string
	// From is the scope the changes come from, as *groupsv1.Changed.
	From grpcproc.PID
	// Ref is the watcher's monitor of From. The Down with Ref means no more
	// changes come.
	Ref grpcproc.Ref
	// Members are the group's members when the watch began; the changes
	// that follow are to them.
	Members []grpcproc.PID

	id uint64 // the scope's key for it
}

// watchIDs tells a process's watches of a group apart.
var watchIDs atomic.Uint64

// Watch has the scope send p a *groupsv1.Changed for each change to
// group's members, until p cancels the watch or exits. p must be a process
// of the scope's node, and its mailbox must accept *groupsv1.Changed. Each
// Watch is ended by its own Cancel; a process that holds several of one
// group gets each change once.
func (sc *Scope) Watch[M proto.Message](ctx context.Context, p *grpcproc.Process[M], group string) (Watch, error) {
	if !reflect.TypeFor[*groupsv1.Changed]().AssignableTo(reflect.TypeFor[M]()) {
		return Watch{}, fmt.Errorf("groups: a process of %s cannot receive *groupsv1.Changed: %w", reflect.TypeFor[M](), grpcproc.ErrType)
	}
	id := watchIDs.Add(1)
	r, err := p.CallTo[*groupsv1.Watching](ctx, sc.pid, &groupsv1.Watch{Group: group, Id: id})
	if err != nil {
		if ctx.Err() != nil {
			// The scope may have taken it all the same.
			_ = p.SendTo(sc.pid, &groupsv1.Unwatch{Group: group, Id: id})
		}
		return Watch{}, err
	}
	// After the answer, on this node: a scope that exits in between is a
	// Down all the same, and a call that fails leaves none behind.
	return Watch{Group: group, From: sc.pid, Ref: p.Monitor(sc.pid), Members: pids(r.GetMembers()), id: id}, nil
}

// Cancel ends the watch: p stops monitoring the scope and tells it to send
// no more. Changes already in p's mailbox stay there.
func (w Watch) Cancel[M proto.Message](p *grpcproc.Process[M]) {
	p.Demonitor(w.Ref)
	_ = p.SendTo(w.From, &groupsv1.Unwatch{Group: w.Group, Id: w.id})
}

// comparePID orders PIDs by node, incarnation and id.
func comparePID(a, b grpcproc.PID) int {
	return cmp.Or(cmp.Compare(a.Node, b.Node), cmp.Compare(a.Incarnation, b.Incarnation), cmp.Compare(a.ID, b.ID))
}

func sorted(set map[grpcproc.PID]struct{}) []grpcproc.PID {
	return slices.SortedFunc(maps.Keys(set), comparePID)
}

func protos(ps []grpcproc.PID) []*grpcprocv1.PID {
	out := make([]*grpcprocv1.PID, len(ps))
	for i, p := range ps {
		out[i] = p.Proto()
	}
	return out
}

func pids(ps []*grpcprocv1.PID) []grpcproc.PID {
	out := make([]grpcproc.PID, len(ps))
	for i, p := range ps {
		out[i] = grpcproc.PIDFromProto(p)
	}
	return out
}
