package groups_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/groups"
	groupsv1 "github.com/floatdrop/grpcproc/groups/proto/grpcproc/groups/v1"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

func start(t *testing.T, n *grpcproc.Node, cfg groups.Config) *groups.Scope {
	t.Helper()
	sc, err := groups.Start(n, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func peers(names ...string) groups.Config { return groups.Config{Peers: names} }

// idle is a process on n that waits until it is asked to exit.
func idle(t *testing.T, n *grpcproc.Node) grpcproc.PID {
	t.Helper()
	a, err := n.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		<-p.Context().Done()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return a.PID()
}

func same(t *testing.T, what string, got []grpcproc.PID, want ...grpcproc.PID) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: %v, want %v", what, got, want)
	}
}

func join(t *testing.T, sc *groups.Scope, n *grpcproc.Node, group string, pids ...grpcproc.PID) {
	t.Helper()
	if err := sc.Join(t.Context(), n, group, pids...); err != nil {
		t.Fatal(err)
	}
}

func leave(t *testing.T, sc *groups.Scope, n *grpcproc.Node, group string, pids ...grpcproc.PID) {
	t.Helper()
	if err := sc.Leave(t.Context(), n, group, pids...); err != nil {
		t.Fatal(err)
	}
}

func exit(t *testing.T, n *grpcproc.Node, pid grpcproc.PID) {
	t.Helper()
	if err := n.Exit(t.Context(), pid, "done"); err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
}

func report(t *testing.T, n *grpcproc.Node, sc *groups.Scope, q *groupsv1.Query) *groupsv1.Report {
	t.Helper()
	r, err := n.Query(t.Context(), sc.PID(), q)
	if err != nil {
		t.Fatal(err)
	}
	return r.(*groupsv1.Report)
}

// wantPeers checks each peer's state as n's scope reports it: "b synced".
func wantPeers(t *testing.T, n *grpcproc.Node, sc *groups.Scope, want string) {
	t.Helper()
	var got []string
	for _, p := range report(t, n, sc, &groupsv1.Query{}).GetPeers() {
		got = append(got, p.GetNode()+" "+p.GetState())
	}
	if s := strings.Join(got, ", "); s != want {
		t.Fatalf("%s's peers: %q, want %q", n.Name(), s, want)
	}
}

// relays are the relays of n's scope.
func relays(n *grpcproc.Node) []grpcproc.PID {
	var out []grpcproc.PID
	for _, p := range n.Processes() {
		if p.Label == groups.RelayLabel {
			out = append(out, p.PID)
		}
	}
	return out
}

// A member joins on its node, and every node of the scope lists it, in
// order, until it leaves or exits.
func TestMembersAcrossNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		sa, sb := start(t, a, peers("b")), start(t, b, peers("a"))
		p1, p2, p3 := idle(t, a), idle(t, b), idle(t, b)
		join(t, sa, a, "g", p1, p1) // once
		same(t, "a's own join, at once", sa.Members("g"), p1)
		join(t, sb, b, "g", p3, p2)
		join(t, sb, b, "h", p2)
		synctest.Wait()
		for _, sc := range []*groups.Scope{sa, sb} {
			same(t, "g", sc.Members("g"), p1, p2, p3)
			if got := sc.Groups(); !slices.Equal(got, []string{"g", "h"}) {
				t.Fatalf("groups %v", got)
			}
		}
		same(t, "a's", sa.Local("g"), p1)
		same(t, "b's", sb.Local("g"), p2, p3)
		wantPeers(t, a, sa, "b synced")

		leave(t, sb, b, "g", p2)
		leave(t, sb, b, "g", p2) // not in it any more
		synctest.Wait()
		same(t, "g after p2 left it", sa.Members("g"), p1, p3)
		same(t, "h, which p2 is still in", sa.Members("h"), p2)
		leave(t, sb, b, "h", p2)
		exit(t, a, p1)
		exit(t, b, p3)
		for _, sc := range []*groups.Scope{sa, sb} {
			if got := sc.Groups(); len(got) != 0 {
				t.Fatalf("groups once every member left: %v", got)
			}
		}
		if got := sb.PID(); got.Node != "b" {
			t.Fatalf("b's scope is %v", got)
		}
	})
}

// Join and Leave take processes of the scope's node that run, in groups
// with names.
func TestRefusals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		sa := start(t, a, groups.Config{})
		pa, pb := idle(t, a), idle(t, b)
		gone := idle(t, a)
		exit(t, a, gone)
		for _, tc := range []struct {
			name  string
			err   error
			group string
			pid   grpcproc.PID
		}{
			{"no name", groups.ErrGroup, "", pa},
			{"a long name", groups.ErrGroup, strings.Repeat("x", 1025), pa},
			{"not UTF-8", groups.ErrGroup, "\xff", pa},
			{"another node's", groups.ErrNotLocal, "g", pb},
			{"one that exited", grpcproc.ErrNoProc, "g", gone},
		} {
			if err := sa.Join(t.Context(), a, tc.group, tc.pid); !errors.Is(err, tc.err) {
				t.Errorf("Join of %s: %v, want %v", tc.name, err, tc.err)
			}
		}
		if err := sa.Leave(t.Context(), a, "", pa); !errors.Is(err, groups.ErrGroup) {
			t.Errorf("Leave of no group: %v", err)
		}
		if err := sa.Leave(t.Context(), a, "g", pb); !errors.Is(err, groups.ErrNotLocal) {
			t.Errorf("Leave of another node's: %v", err)
		}
		if got := sa.Groups(); len(got) != 0 {
			t.Fatalf("groups after refusals: %v", got)
		}
		if _, err := groups.Start(a, groups.Config{}); !errors.Is(err, grpcproc.ErrNameTaken) {
			t.Errorf("a second scope of one name on a node: %v", err)
		}
	})
}

// watcher is a process that watches groups as the test tells it, with
// Watch and Unwatch messages (which cancels the group's latest watch), and reports each Changed on got, and its
// scope's end as a Changed of group "down".
type watcher struct {
	pid   grpcproc.PID
	got   chan *groupsv1.Changed
	watch chan groups.Watch
}

func watch(t *testing.T, n *grpcproc.Node, sc *groups.Scope) *watcher {
	t.Helper()
	w := &watcher{got: make(chan *groupsv1.Changed, 64), watch: make(chan groups.Watch, 1)}
	a, err := n.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		watches := map[string][]groups.Watch{}
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			switch b := m.Body.(type) {
			case nil:
				w.got <- &groupsv1.Changed{Group: "down"}
			case *groupsv1.Watch:
				v, err := sc.Watch(t.Context(), p, b.GetGroup())
				if err != nil {
					t.Errorf("watch %s: %v", b.GetGroup(), err)
				}
				watches[b.GetGroup()] = append(watches[b.GetGroup()], v)
				w.watch <- v
			case *groupsv1.Unwatch: // the latest watch of the group
				ws := watches[b.GetGroup()]
				ws[len(ws)-1].Cancel(p)
				watches[b.GetGroup()] = ws[:len(ws)-1]
			case *groupsv1.Changed:
				w.got <- b
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	w.pid = a.PID()
	return w
}

func (w *watcher) start(t *testing.T, n *grpcproc.Node, group string) groups.Watch {
	t.Helper()
	if err := n.SendTo(t.Context(), w.pid, &groupsv1.Watch{Group: group}); err != nil {
		t.Fatal(err)
	}
	return <-w.watch
}

func (w *watcher) stop(t *testing.T, n *grpcproc.Node, group string) {
	t.Helper()
	if err := n.SendTo(t.Context(), w.pid, &groupsv1.Unwatch{Group: group}); err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
}

func (w *watcher) next(t *testing.T) *groupsv1.Changed {
	t.Helper()
	synctest.Wait()
	select {
	case c := <-w.got:
		return c
	default:
		t.Fatal("no change")
		return nil
	}
}

func (w *watcher) none(t *testing.T) {
	t.Helper()
	synctest.Wait()
	select {
	case c := <-w.got:
		t.Fatalf("a change: %v", c)
	default:
	}
}

func wantChange(t *testing.T, c *groupsv1.Changed, group string, joined, left []grpcproc.PID) {
	t.Helper()
	if c.GetGroup() != group {
		t.Fatalf("a change to %s, want %s", c.GetGroup(), group)
	}
	same(t, group+" joined", pidsOf(c.GetJoined()), joined...)
	same(t, group+" left", pidsOf(c.GetLeft()), left...)
}

func pidsOf(ps []*grpcprocv1.PID) []grpcproc.PID {
	var out []grpcproc.PID
	for _, p := range ps {
		out = append(out, grpcproc.PIDFromProto(p))
	}
	return out
}

// A watcher hears of every join and leave of the group it watches, on any
// node, until it cancels or exits.
func TestWatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		sa, sb := start(t, a, peers("b")), start(t, b, peers("a"))
		p1, p2 := idle(t, a), idle(t, b)
		join(t, sb, b, "g", p2)
		synctest.Wait()
		w := watch(t, b, sb)
		v := w.start(t, b, "g")
		same(t, "the members when the watch began", v.Members, p2)
		if v.From != sb.PID() || v.Group != "g" {
			t.Fatalf("watch %+v", v)
		}
		w.start(t, b, "g") // a second watch: still one change each
		w.start(t, b, "h")

		join(t, sa, a, "g", p1)
		wantChange(t, w.next(t), "g", []grpcproc.PID{p1}, nil)
		w.none(t)
		w.stop(t, b, "g") // the first watch goes on
		join(t, sa, a, "g2", p1)
		w.none(t)
		join(t, sa, a, "other", p1) // a group it does not watch
		w.none(t)
		exit(t, a, p1) // in g, and in groups it does not watch
		wantChange(t, w.next(t), "g", nil, []grpcproc.PID{p1})
		w.none(t)
		w.stop(t, b, "g")
		leave(t, sb, b, "g", p2)
		w.none(t)
		join(t, sb, b, "h", p2)
		wantChange(t, w.next(t), "h", []grpcproc.PID{p2}, nil)

		// A watcher that exits is forgotten.
		info, _ := b.Inspect(t.Context(), sb.PID())
		if info["watchers"] != "1" {
			t.Fatalf("watchers %s", info["watchers"])
		}
		exit(t, b, w.pid)
		info, _ = b.Inspect(t.Context(), sb.PID())
		if info["watchers"] != "0" {
			t.Fatalf("watchers once it exited: %s", info["watchers"])
		}

		// The scope's end ends the watch.
		w = watch(t, b, sb)
		w.start(t, b, "g")
		w.stop(t, b, "g")
		w.start(t, b, "g")
		exit(t, b, sb.PID())
		if c := w.next(t); c.GetGroup() != "down" {
			t.Fatalf("after the scope's end: %v", c)
		}
		if got := sb.Members("h"); len(got) != 0 {
			t.Fatalf("an ended scope's members: %v", got)
		}
	})
}

// Watch refuses a process that cannot take the changes, a group with no
// name, and a process of another node; a watch whose ctx ends is taken back.
func TestWatchRefusals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		sa := start(t, a, groups.Config{})
		errs := make(chan error, 4)
		if _, err := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			_, err := sa.Watch(t.Context(), p, "g")
			errs <- err
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := <-errs; !errors.Is(err, grpcproc.ErrType) {
			t.Errorf("a process of Ping: %v", err)
		}
		try := func(n *grpcproc.Node, ctx context.Context, group string) error {
			if _, err := n.Spawn(func(p *grpcproc.Process[proto.Message]) error {
				_, err := sa.Watch(ctx, p, group)
				errs <- err
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			return <-errs
		}
		if err := try(a, t.Context(), ""); !errors.Is(err, groups.ErrGroup) {
			t.Errorf("no group: %v", err)
		}
		if err := try(b, t.Context(), "g"); !errors.Is(err, groups.ErrNotLocal) {
			t.Errorf("another node's process: %v", err)
		}
		done, cancel := context.WithCancel(t.Context())
		cancel()
		if err := try(a, done, "g"); !errors.Is(err, context.Canceled) {
			t.Errorf("a done ctx: %v", err)
		}
		synctest.Wait()
		info, _ := a.Inspect(t.Context(), sa.PID())
		if info["watchers"] != "0" {
			t.Fatalf("watchers %s", info["watchers"])
		}

		// A Watch that fails takes back only itself.
		changed := make(chan *groupsv1.Changed, 1)
		if _, err := a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			if _, err := sa.Watch(t.Context(), p, "g"); err != nil {
				return err
			}
			_, err := sa.Watch(done, p, "g")
			errs <- err
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				if c, ok := m.Body.(*groupsv1.Changed); ok {
					changed <- c
				}
			}
		}); err != nil {
			t.Fatal(err)
		}
		if err := <-errs; !errors.Is(err, context.Canceled) {
			t.Fatalf("a second Watch with a done ctx: %v", err)
		}
		synctest.Wait()
		p := idle(t, a)
		join(t, sa, a, "g", p)
		synctest.Wait()
		select {
		case c := <-changed:
			wantChange(t, c, "g", []grpcproc.PID{p}, nil)
		default:
			t.Fatal("the first watch ended with the second")
		}
	})
}

// A node that cannot be reached has its members dropped, and asked again,
// less often each time, until it answers.
func TestUnreachable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		sa, sb := start(t, a, peers("b")), start(t, b, peers("a"))
		p1, p2 := idle(t, a), idle(t, b)
		join(t, sa, a, "g", p1)
		join(t, sb, b, "g", p2)
		synctest.Wait()
		same(t, "g", sa.Members("g"), p1, p2)

		c.Partition("a", "b")
		synctest.Wait()
		same(t, "a's g, cut off", sa.Members("g"), p1)
		same(t, "b's g, cut off", sb.Members("g"), p2)
		wantPeers(t, a, sa, "b unreachable")
		time.Sleep(3500 * time.Millisecond) // asked again at 1s and 3s, in vain
		synctest.Wait()
		wantPeers(t, a, sa, "b unreachable")
		c.Heal("a", "b")
		time.Sleep(4 * time.Second) // and at 7s
		synctest.Wait()
		wantPeers(t, a, sa, "b synced")
		same(t, "a's g, healed", sa.Members("g"), p1, p2)
		same(t, "b's g, healed", sb.Members("g"), p1, p2)

		// A relay that lasted longer than the wait before it starts the
		// count over: the next loss waits a Retry again.
		time.Sleep(10 * time.Second)
		c.Partition("a", "b")
		synctest.Wait()
		c.Heal("a", "b")
		time.Sleep(1500 * time.Millisecond)
		synctest.Wait()
		wantPeers(t, a, sa, "b synced")
	})
}

// A node that restarts comes back with its new members.
func TestRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a := c.Node("a")
		sa := start(t, a, peers("b"))
		sb := start(t, c.Node("b"), peers("a"))
		join(t, sb, c.Node("b"), "g", idle(t, c.Node("b")))
		synctest.Wait()
		c.Kill("b")
		synctest.Wait()
		if got := sa.Members("g"); len(got) != 0 {
			t.Fatalf("a dead node's members: %v", got)
		}
		b := c.Restart("b")
		sb = start(t, b, peers("a"))
		p := idle(t, b)
		join(t, sb, b, "g", p)
		synctest.Wait()
		same(t, "the restarted node's", sa.Members("g"), p)
	})
}

// A node that runs no scope is absent, and asked again, less often each
// time; a node that said hello first, and is not asked, is forgotten with
// its scope.
func TestAbsent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b", "c")
		a := c.Node("a")
		sa := start(t, a, peers("b"))
		synctest.Wait()
		wantPeers(t, a, sa, "b absent")
		sb := start(t, c.Node("b"), groups.Config{}) // asks no one
		synctest.Wait()
		wantPeers(t, a, sa, "b absent")
		time.Sleep(time.Second) // a asks again
		synctest.Wait()
		wantPeers(t, a, sa, "b synced")
		sc := start(t, c.Node("c"), peers("a"))
		pc := idle(t, c.Node("c"))
		join(t, sc, c.Node("c"), "g", pc)
		synctest.Wait()
		wantPeers(t, a, sa, "b synced, c synced")
		same(t, "c's member", sa.Members("g"), pc)
		wantPeers(t, c.Node("b"), sb, "a synced")
		exit(t, c.Node("c"), sc.PID())
		wantPeers(t, a, sa, "b synced")
		if got := sa.Members("g"); len(got) != 0 {
			t.Fatalf("an ended scope's members: %v", got)
		}
		exit(t, c.Node("b"), sb.PID())
		wantPeers(t, a, sa, "b absent")
	})
}

// A long list of members goes in several Syncs.
func TestSyncInParts(t *testing.T) {
	defer groups.SetSyncBudget(64)()
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		sa := start(t, a, groups.Config{})
		var want []grpcproc.PID
		for range 20 {
			want = append(want, idle(t, a))
		}
		join(t, sa, a, "g", want...)
		join(t, sa, a, "h", want[:3]...)
		sb := start(t, b, peers("a"))
		synctest.Wait()
		same(t, "g", sb.Members("g"), want...)
		same(t, "h", sb.Members("h"), want[:3]...)
	})
}

// A scope answers a query with its groups, or one group's members, and its
// peers; it is inspected with its counts.
func TestQuery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b", "c")
		a, b := c.Node("a"), c.Node("b")
		sa, sb := start(t, a, peers("b", "c")), start(t, b, peers("a"))
		p1, p2, p3 := idle(t, a), idle(t, b), idle(t, b)
		join(t, sa, a, "room:1", p1)
		join(t, sb, b, "room:1", p2, p3)
		join(t, sb, b, "room:2", p2)
		join(t, sb, b, "worker", p3)
		synctest.Wait()

		r := report(t, a, sa, &groupsv1.Query{})
		var got []string
		for _, g := range r.GetGroups() {
			got = append(got, fmt.Sprintf("%s %d %d %d", g.GetName(), g.GetMembers(), g.GetLocal(), g.GetNodes()))
		}
		want := []string{
			"room:1 3 1 2",
			"room:2 1 0 1",
			"worker 1 0 1",
		}
		if !slices.Equal(got, want) || r.GetScope() != groups.DefaultScope {
			t.Fatalf("groups\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		ps := r.GetPeers()
		if len(ps) != 2 || ps[0].GetScope() == nil || ps[0].GetMembers() != 4 || ps[1].GetState() != "absent" || ps[1].GetScope() != nil {
			t.Fatalf("peers %v", ps)
		}
		r = report(t, a, sa, &groupsv1.Query{Prefix: "room:", Limit: 1})
		if len(r.GetGroups()) != 1 || r.GetGroups()[0].GetName() != "room:1" || r.GetOmitted() != 1 {
			t.Fatalf("rooms, one of them: %v", r)
		}
		r = report(t, a, sa, &groupsv1.Query{Group: "room:1", Limit: 2})
		if same(t, "room:1's first two", pidsOf(r.GetMembers()), p1, p2); r.GetOmitted() != 1 {
			t.Fatalf("omitted %d", r.GetOmitted())
		}
		if _, err := a.Query(t.Context(), sa.PID(), &testpb.Ping{}); !errors.Is(err, grpcproc.ErrType) {
			t.Fatalf("a query of another type: %v", err)
		}
		info, err := a.Inspect(t.Context(), sa.PID())
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range map[string]string{"scope": "default", "groups": "3", "members": "5", "local": "1", "peers": "2", "peers synced": "1", "peers absent": "1"} {
			if info[k] != v {
				t.Errorf("inspect %s = %q, want %q (%v)", k, info[k], v, info)
			}
		}
	})
}
