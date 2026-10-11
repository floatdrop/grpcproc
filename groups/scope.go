package groups

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	groupsv1 "github.com/floatdrop/grpcproc/groups/proto/grpcproc/groups/v1"
)

const (
	// helloTimeout bounds a relay's Hello, its dial included.
	helloTimeout = 10 * time.Second
	// defaultLimit is a Query's limit when it sets none.
	defaultLimit = 1000
	// maxBackoff is how many times Retry doubles at most.
	maxBackoff = 5
)

// syncBudget bounds a Sync or an Update, roughly, in bytes: well under the
// smallest Config.MaxMessageSize a node may have.
var syncBudget = 32 << 10

// state is where a peer stands with this node's scope.
type state uint8

const (
	syncing state = iota + 1
	synced
	absent
	unreachable
)

var states = [...]string{"", "syncing", "synced", "absent", "unreachable"}

func (s state) String() string { return states[s] }

// peer is another node's scope, as this one knows it. Its members are what
// its last Sync said, and the Updates since.
type peer struct {
	state       state
	pinned      bool         // Peers names it
	listed      bool         // Membership reports it up, and Select takes it
	incarnation uint64       // the newest it is known to have
	found       time.Time    // when its relay reached its scope
	relay       grpcproc.PID // the relay to it, while one runs
	reached     grpcproc.PID // its scope, as the relay found it
	scope       grpcproc.PID // its scope, as its last Sync named it
	members     map[string]map[grpcproc.PID]struct{}
	size        int                                  // memberships in members
	incoming    map[string]map[grpcproc.PID]struct{} // a Sync under way
}

// asked says whether this node asks pr for its members, rather than only
// answering it.
func (pr *peer) asked() bool { return pr.pinned || pr.listed }

// saw records that pr's node has an incarnation of inc, and says whether
// that is older than the newest it is known to have.
func (pr *peer) saw(inc uint64) (older bool) {
	pr.incarnation = max(pr.incarnation, inc)
	return inc != 0 && inc < pr.incarnation
}

// wait is a node's backoff: it outlives the node's peer, so that a peer
// forgotten and made again waits as long as the last one would have.
type wait struct {
	failures int             // losses in a row
	timer    *grpcproc.Timer // asks it again; nil when nothing waits
	seq      uint64          // the timer's, in its Retry
	until    time.Time       // when the timer fires, or fired
	state    state           // what it was when lost
	inc      uint64          // the newest incarnation it was known to have
}

// stop ends w's wait early.
func (w *wait) stop() {
	if w != nil && w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
}

// member is a monitor of a local member, and the groups it is in.
type member struct {
	ref    grpcproc.Ref
	groups map[string]struct{}
}

// watcher is a monitor of a local watcher, and its watches by group.
type watcher struct {
	ref grpcproc.Ref
	ids map[string]map[uint64]struct{}
}

// scope is the state of a scope's process. Only its process writes it;
// mu guards what the Scope's methods and queries read on other goroutines.
type scope struct {
	cfg  Config
	n    *grpcproc.Node
	name string
	p    *grpcproc.Process[proto.Message]

	bridge   grpcproc.PID                         // the membership watch, while it runs
	joined   map[grpcproc.PID]*member             // a local member
	own      map[string]map[grpcproc.PID]struct{} // the local members, by group
	watchers map[grpcproc.PID]*watcher
	watching map[string]map[grpcproc.PID]struct{} // a group's watchers
	relays   map[grpcproc.PID]string              // a relay, and the node it is to
	waits    map[string]*wait                     // nodes lost, by name

	mu     sync.RWMutex
	groups map[string]map[grpcproc.PID]struct{} // every node's members, by group
	byNode map[string]map[string]int            // each group's members per node
	peers  map[string]*peer
}

func newScope(n *grpcproc.Node, cfg Config) *scope {
	return &scope{
		cfg: cfg, n: n, name: ScopeName(cfg.Scope),
		joined: map[grpcproc.PID]*member{}, own: map[string]map[grpcproc.PID]struct{}{},
		watchers: map[grpcproc.PID]*watcher{}, watching: map[string]map[grpcproc.PID]struct{}{},
		relays: map[grpcproc.PID]string{}, waits: map[string]*wait{},
		groups: map[string]map[grpcproc.PID]struct{}{}, byNode: map[string]map[string]int{}, peers: map[string]*peer{},
	}
}

func (s *scope) run(p *grpcproc.Process[proto.Message]) error {
	s.p = p
	defer s.locked(func() {
		clear(s.groups)
		clear(s.byNode)
		clear(s.peers)
	})
	if s.cfg.Membership != nil {
		s.watchMembers()
	}
	for _, node := range s.cfg.Peers {
		s.declare(node, true, false)
	}
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		s.handle(m)
	}
}

func (s *scope) handle(m grpcproc.Msg[proto.Message]) {
	if m.Down != nil {
		s.down(*m.Down)
		return
	}
	switch b := m.Body.(type) {
	case *groupsv1.Join:
		s.reply(m, nil, s.join(b.GetGroup(), pids(b.GetPids())))
	case *groupsv1.Leave:
		s.reply(m, nil, s.leave(b.GetGroup(), pids(b.GetPids())))
	case *groupsv1.Watch:
		s.watch(m, b.GetGroup(), b.GetId())
	case *groupsv1.Unwatch:
		s.unwatch(m.From, b.GetGroup(), b.GetId())
	case *groupsv1.Hello:
		s.hello(m, grpcproc.PIDFromProto(b.GetScope()))
	case *groupsv1.Found:
		if _, pr := s.relayed(m.From); pr != nil {
			s.found(pr, grpcproc.PIDFromProto(b.GetScope()))
		}
	case *groupsv1.Lost:
		if node, pr := s.relayed(m.From); pr != nil {
			s.lost(node, pr, grpcproc.PIDFromProto(b.GetScope()), b.GetReason())
		}
	case *groupsv1.Sync:
		s.synced(m.From, b)
	case *groupsv1.Update:
		s.updated(m.From, b)
	case *groupsv1.Listing:
		if m.From == s.bridge {
			s.listing()
		}
	case *groupsv1.MemberEvent:
		if m.From == s.bridge {
			s.member(b)
		}
	case *groupsv1.Retry:
		if m.From == s.p.PID() {
			s.retry(b.GetNode(), b.GetSeq())
		}
	default:
		s.p.Log().Warn("groups: dropped a message a scope does not take", "from", m.From, "type", proto.MessageName(m.Body))
		s.reply(m, nil, grpcproc.ErrType)
	}
}

// reply answers m, if it is a call, with resp or err; nil and nil answer
// nothing. A caller of another node is answered from a process of its own:
// an answer to a node this one has no link to yet waits for a dial.
func (s *scope) reply(m grpcproc.Msg[proto.Message], resp proto.Message, err error) {
	if !m.IsCall() {
		return
	}
	answer := func() {
		if err != nil {
			_ = m.Reply(nil, err)
			return
		}
		_ = m.Reply(cmp.Or[proto.Message](resp, &emptypb.Empty{}), nil)
	}
	if m.From.Node == s.n.Name() {
		answer()
		return
	}
	_, _ = s.p.Spawn(func(*grpcproc.Process[proto.Message]) error {
		answer()
		return nil
	}, grpcproc.WithLabel("groups reply"))
}

func (s *scope) local(pid grpcproc.PID) bool {
	return pid.Node == s.n.Name() && pid.Incarnation == s.p.PID().Incarnation
}

func validGroup(g string) bool { return g != "" && len(g) <= maxGroup && utf8.ValidString(g) }

// ---- local members ----

func (s *scope) join(group string, pids []grpcproc.PID) error {
	if !validGroup(group) {
		return ErrGroup
	}
	for _, pid := range pids {
		if !s.local(pid) {
			return ErrNotLocal
		}
		if _, ok := s.n.Process(pid); !ok {
			return grpcproc.ErrNoProc
		}
	}
	var joined []grpcproc.PID
	for _, pid := range pids {
		w := s.joined[pid]
		if w == nil {
			w = &member{ref: s.p.Monitor(pid), groups: map[string]struct{}{}}
			s.joined[pid] = w
		}
		if _, in := w.groups[group]; in {
			continue
		}
		w.groups[group] = struct{}{}
		add(s.own, group, pid)
		joined = append(joined, pid)
	}
	s.changed(group, joined, nil)
	s.tell(true, map[string][]uint64{group: ids(joined)})
	return nil
}

func (s *scope) leave(group string, pids []grpcproc.PID) error {
	if !validGroup(group) {
		return ErrGroup
	}
	if slices.ContainsFunc(pids, func(pid grpcproc.PID) bool { return !s.local(pid) }) {
		return ErrNotLocal
	}
	var left []grpcproc.PID
	for _, pid := range pids {
		if w := s.joined[pid]; w != nil && s.drop(pid, w, group) {
			left = append(left, pid)
		}
	}
	s.changed(group, nil, left)
	s.tell(false, map[string][]uint64{group: ids(left)})
	return nil
}

// drop takes pid, whose monitor is w, out of group, and reports whether it
// was in it. One in no group is no longer watched.
func (s *scope) drop(pid grpcproc.PID, w *member, group string) bool {
	if _, in := w.groups[group]; !in {
		return false
	}
	delete(w.groups, group)
	remove(s.own, group, pid)
	if len(w.groups) == 0 {
		s.p.Demonitor(w.ref)
		delete(s.joined, pid)
	}
	return true
}

// tell sends the other scopes the ids that joined groups, or left them.
func (s *scope) tell(joined bool, byGroup map[string][]uint64) {
	for _, part := range split(byGroup) {
		up := &groupsv1.Update{Left: part}
		if joined {
			up = &groupsv1.Update{Joined: part}
		}
		for _, pr := range s.peers {
			if !pr.reached.IsZero() {
				_ = s.p.SendTo(pr.relay, up)
			}
		}
	}
}

// split puts ids by group into parts of about syncBudget bytes each.
func split(byGroup map[string][]uint64) [][]*groupsv1.Members {
	var parts [][]*groupsv1.Members
	var cur []*groupsv1.Members
	size := 0
	for _, group := range slices.Sorted(maps.Keys(byGroup)) {
		for rest := byGroup[group]; len(rest) > 0; {
			if size >= syncBudget {
				parts, cur, size = append(parts, cur), nil, 0
			}
			n := min(len(rest), max(1, (syncBudget-size-len(group))/10)) // an id is at most 10 bytes
			cur = append(cur, &groupsv1.Members{Group: group, Ids: rest[:n]})
			size += len(group) + 10*n
			rest = rest[n:]
		}
	}
	if cur != nil {
		parts = append(parts, cur)
	}
	return parts
}

// ---- watchers ----

func (s *scope) watch(m grpcproc.Msg[proto.Message], group string, id uint64) {
	switch {
	case !validGroup(group):
		s.reply(m, nil, ErrGroup)
		return
	case !s.local(m.From):
		s.reply(m, nil, ErrNotLocal)
		return
	}
	w := s.watchers[m.From]
	if w == nil {
		w = &watcher{ref: s.p.Monitor(m.From), ids: map[string]map[uint64]struct{}{}}
		s.watchers[m.From] = w
	}
	if w.ids[group] == nil {
		w.ids[group] = map[uint64]struct{}{}
		add(s.watching, group, m.From)
	}
	w.ids[group][id] = struct{}{}
	s.reply(m, &groupsv1.Watching{Members: protos(sorted(s.groups[group]))}, nil)
}

func (s *scope) unwatch(from grpcproc.PID, group string, id uint64) {
	w := s.watchers[from]
	if w == nil {
		return
	}
	delete(w.ids[group], id)
	if len(w.ids[group]) == 0 {
		delete(w.ids, group)
		remove(s.watching, group, from)
	}
	if len(w.ids) == 0 {
		s.p.Demonitor(w.ref)
		delete(s.watchers, from)
	}
}

// changed records that joined and left, which changed, joined and left
// group, and tells the group's watchers.
func (s *scope) changed(group string, joined, left []grpcproc.PID) {
	if len(joined)+len(left) == 0 {
		return
	}
	s.locked(func() {
		nodes := s.byNode[group]
		if nodes == nil {
			nodes = map[string]int{}
			s.byNode[group] = nodes
		}
		for _, pid := range joined {
			add(s.groups, group, pid)
			nodes[pid.Node]++
		}
		for _, pid := range left {
			remove(s.groups, group, pid)
			if nodes[pid.Node]--; nodes[pid.Node] == 0 {
				delete(nodes, pid.Node)
			}
		}
		if len(nodes) == 0 {
			delete(s.byNode, group)
		}
	})
	note := &groupsv1.Changed{Group: group, Joined: protos(joined), Left: protos(left)}
	for pid := range s.watching[group] {
		_ = s.p.SendTo(pid, note)
	}
}

// ---- other nodes' scopes ----

// declare has node asked for its members: pinned, by Peers, or listed, by
// Membership. It returns the peer, nil for this node.
func (s *scope) declare(node string, pinned, now bool) *peer {
	pr := s.reach(node, now)
	if pr != nil && pinned {
		pr.pinned = true
	} else if pr != nil {
		pr.listed = true
	}
	return pr
}

// reach has a relay to node run, starting one if none does, unless node
// waits to be asked again and now is false, and returns node's peer, nil
// for this node.
func (s *scope) reach(node string, now bool) *peer {
	if node == s.n.Name() {
		return nil
	}
	pr := s.peers[node]
	if pr == nil {
		pr = s.add(node)
	}
	switch w := s.waits[node]; {
	case !pr.relay.IsZero():
	case !now && w != nil && w.timer != nil:
		s.locked(func() { pr.state = cmp.Or(pr.state, w.state) })
	default:
		s.connect(node, pr)
	}
	return pr
}

// member is Membership's word on a node: up, and taken by Select or not,
// or gone.
func (s *scope) member(b *groupsv1.MemberEvent) {
	node, inc := b.GetNode(), b.GetIncarnation()
	pr := s.peers[node]
	known := uint64(0)
	if pr != nil {
		known = pr.incarnation
	}
	if w := s.waits[node]; w != nil {
		known = max(known, w.inc)
	}
	switch {
	case inc != 0 && inc < known:
		// News of an incarnation older than one the node is known to have.
	case b.GetUp() && b.GetSelected():
		// A newer incarnation ends its wait, as it ends the core's backoff.
		if pr = s.declare(node, false, inc > known); pr != nil {
			pr.saw(inc)
		}
	case pr == nil:
	case !b.GetUp() && !pr.pinned:
		s.forget(node, pr)
	default:
		// Not taken by Select any more, or gone though Peers names it.
		s.unlist(node, pr)
	}
}

// listing is the start of a new membership watch's list: a node it does
// not list again is asked for no more.
func (s *scope) listing() {
	for node, pr := range s.peers {
		s.unlist(node, pr)
	}
}

// unlist takes Membership's word for node back. A node that is then not
// asked, and has no relay to it running, is forgotten.
func (s *scope) unlist(node string, pr *peer) {
	pr.listed = false
	if !pr.asked() && pr.relay.IsZero() {
		s.locked(func() { delete(s.peers, node) })
	}
}

func (s *scope) add(node string) *peer {
	pr := &peer{}
	s.locked(func() { s.peers[node] = pr })
	return pr
}

// connect starts a relay to node's scope, which says hello to it.
func (s *scope) connect(node string, pr *peer) {
	s.waits[node].stop()
	a, _, _ := s.p.SpawnMonitor(relay(node, s.name, s.p.PID()), grpcproc.LinkParent(), grpcproc.WithLabel(RelayLabel))
	s.relays[a.PID()] = node
	pr.relay, pr.reached = a.PID(), grpcproc.PID{}
	if pr.scope.IsZero() {
		s.locked(func() { pr.state = syncing })
	}
}

// relayed is the peer whose relay from is, if it still is.
func (s *scope) relayed(from grpcproc.PID) (string, *peer) {
	node := s.relays[from]
	if pr := s.peers[node]; pr != nil && pr.relay == from {
		return node, pr
	}
	return "", nil
}

// hello answers another node's relay, and sends that node this one's
// members: at once over a relay that reaches its scope, or once one does.
func (s *scope) hello(m grpcproc.Msg[proto.Message], from grpcproc.PID) {
	s.reply(m, &groupsv1.Hello{Scope: s.p.PID().Proto()}, nil)
	node := m.From.Node
	if node == s.n.Name() {
		return
	}
	pr := s.peers[node]
	switch {
	case pr != nil && pr.saw(from.Incarnation):
		// A late hello from an incarnation older than one it is known to have.
	case pr == nil || pr.relay.IsZero():
		s.reach(node, true).saw(from.Incarnation)
	case pr.reached == from:
		s.sync(pr)
	case !pr.reached.IsZero() && newer(from, pr.reached):
		// Its relay reaches an older scope of node's: reach this one.
		_ = s.p.Exit(pr.relay, "replaced")
		s.connect(node, pr)
	}
}

// newer says whether a is a later process of its node than b: of a later
// incarnation, or spawned after it.
func newer(a, b grpcproc.PID) bool {
	return a.Incarnation > b.Incarnation || a.Incarnation == b.Incarnation && a.ID > b.ID
}

// wrote is node writing to this scope with no relay to it running: one
// forgotten, or lost. It is asked again, at once unless it waits to be,
// and syncs again.
func (s *scope) wrote(node string) { s.reach(node, false) }

// found is pr's relay reaching its scope: it is sent this one's members.
func (s *scope) found(pr *peer, scope grpcproc.PID) {
	pr.reached, pr.found = scope, time.Now()
	pr.saw(scope.Incarnation)
	s.sync(pr)
}

// sync sends this scope's members over pr's relay, in parts.
func (s *scope) sync(pr *peer) {
	byGroup := map[string][]uint64{}
	for group, set := range s.own {
		for pid := range set {
			byGroup[group] = append(byGroup[group], pid.ID)
		}
	}
	parts := split(byGroup)
	if len(parts) == 0 {
		parts = append(parts, nil)
	}
	for i, part := range parts {
		_ = s.p.SendTo(pr.relay, &groupsv1.Sync{Scope: s.p.PID().Proto(), Groups: part, First: i == 0, More: i < len(parts)-1})
	}
}

// lost is a relay's end: node's scope did not answer it, or the one it
// watched is gone, or it could not send.
func (s *scope) lost(node string, pr *peer, scope grpcproc.PID, reason string) {
	pr.relay, pr.reached = grpcproc.PID{}, grpcproc.PID{}
	if !scope.IsZero() && !pr.scope.IsZero() && scope != pr.scope {
		// It watched an older scope than the one whose members these are.
		s.connect(node, pr)
		return
	}
	s.dropPeer(pr)
	longest := s.cfg.Retry << maxBackoff
	for n, w := range s.waits {
		if w.timer == nil && time.Since(w.until) > longest {
			delete(s.waits, n) // over for long: it tells nothing more
		}
	}
	w := s.waits[node]
	if w == nil {
		w = &wait{}
		s.waits[node] = w
	}
	if w.failures > 0 && !pr.found.IsZero() && time.Since(pr.found) >= s.cfg.Retry<<min(w.failures-1, maxBackoff) {
		w.failures = 0 // its relay lasted longer than the wait before it
	}
	pr.found = time.Time{}
	cut := reason == grpcproc.ReasonNoConnection
	w.state, w.inc = absent, max(w.inc, pr.incarnation)
	if cut {
		w.state = unreachable
	}
	delay := s.cfg.Retry << min(w.failures, maxBackoff)
	w.seq, w.until = w.seq+1, time.Now().Add(delay)
	w.timer = s.p.SendAfter[proto.Message](delay, s.p.Addr(), &groupsv1.Retry{Node: node, Seq: w.seq})
	w.failures++
	if !pr.asked() && (!cut || w.failures > maxBackoff) {
		// It asked this one, and is gone, or was asked in vain five times:
		// it is asked again when it writes, once its wait is over.
		s.locked(func() { delete(s.peers, node) })
		return
	}
	s.locked(func() { pr.state = w.state })
}

// locked runs f with s.mu held: for what other goroutines read.
func (s *scope) locked(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f()
}

// retry ends node's wait seq, if it still waits: it is asked again, or,
// forgotten while it waited, its wait is dropped. With no node, the
// membership watch starts again.
func (s *scope) retry(node string, seq uint64) {
	if node == "" {
		if s.bridge.IsZero() {
			s.watchMembers()
		}
		return
	}
	if w := s.waits[node]; w != nil && w.timer != nil && w.seq == seq {
		w.timer = nil
		if pr := s.peers[node]; pr != nil && pr.relay.IsZero() {
			s.connect(node, pr)
		}
	}
}

// forget drops node, which left the cluster; its wait stays, so that a
// node that comes back as the same incarnation waits it out.
func (s *scope) forget(node string, pr *peer) {
	if !pr.relay.IsZero() {
		_ = s.p.Exit(pr.relay, grpcproc.ReasonShutdown)
	}
	s.dropPeer(pr)
	s.locked(func() { delete(s.peers, node) })
}

// dropPeer forgets pr's members.
func (s *scope) dropPeer(pr *peer) {
	var old map[string]map[grpcproc.PID]struct{}
	s.locked(func() {
		old = pr.members
		pr.members, pr.size, pr.incoming, pr.scope = nil, 0, nil, grpcproc.PID{}
	})
	for _, group := range slices.Sorted(maps.Keys(old)) {
		s.changed(group, nil, sorted(old[group]))
	}
}

// synced takes a Sync from another node's scope.
func (s *scope) synced(from grpcproc.PID, b *groupsv1.Sync) {
	pr := s.peers[from.Node]
	if pr == nil || pr.relay.IsZero() {
		s.wrote(from.Node)
		return
	}
	switch {
	case b.GetFirst():
		pr.incoming = map[string]map[grpcproc.PID]struct{}{}
	case pr.incoming == nil:
		return // a part of a Sync whose start this one did not take: another follows the relay's hello
	}
	for _, ms := range b.GetGroups() {
		for _, id := range ms.GetIds() {
			add(pr.incoming, ms.GetGroup(), grpcproc.PID{Node: from.Node, Incarnation: from.Incarnation, ID: id})
		}
	}
	if b.GetMore() {
		return
	}
	if pr.saw(from.Incarnation) {
		pr.incoming = nil
		return // a Sync of an incarnation older than one it is known to have
	}
	old, now, size := pr.members, pr.incoming, 0
	for _, set := range now {
		size += len(set)
	}
	s.locked(func() {
		pr.members, pr.size, pr.incoming, pr.scope, pr.state = now, size, nil, grpcproc.PIDFromProto(b.GetScope()), synced
	})
	names := slices.AppendSeq(slices.Collect(maps.Keys(old)), maps.Keys(now))
	slices.Sort(names)
	for _, group := range slices.Compact(names) {
		joined := slices.DeleteFunc(sorted(now[group]), func(pid grpcproc.PID) bool { return has(old, group, pid) })
		left := slices.DeleteFunc(sorted(old[group]), func(pid grpcproc.PID) bool { return has(now, group, pid) })
		s.changed(group, joined, left)
	}
}

// updated takes an Update from another node's scope, after its Sync.
func (s *scope) updated(from grpcproc.PID, b *groupsv1.Update) {
	pr := s.peers[from.Node]
	if pr == nil || pr.relay.IsZero() {
		s.wrote(from.Node)
		return
	}
	if pr.scope.Incarnation != from.Incarnation {
		return // before its Sync, or from an incarnation it no longer is
	}
	apply := func(parts []*groupsv1.Members, join bool) {
		for _, ms := range parts {
			group := ms.GetGroup()
			var changes []grpcproc.PID
			s.locked(func() {
				for _, id := range ms.GetIds() {
					pid := grpcproc.PID{Node: from.Node, Incarnation: from.Incarnation, ID: id}
					if has(pr.members, group, pid) == join {
						continue
					}
					if join {
						add(pr.members, group, pid)
						pr.size++
					} else {
						remove(pr.members, group, pid)
						pr.size--
					}
					changes = append(changes, pid)
				}
			})
			if join {
				s.changed(group, changes, nil)
			} else {
				s.changed(group, nil, changes)
			}
		}
	}
	apply(b.GetJoined(), true)
	apply(b.GetLeft(), false)
}

func (s *scope) down(d grpcproc.Down) {
	if w := s.joined[d.PID]; w != nil && w.ref == d.Ref {
		delete(s.joined, d.PID)
		left := map[string][]uint64{}
		for _, group := range slices.Sorted(maps.Keys(w.groups)) {
			remove(s.own, group, d.PID)
			s.changed(group, nil, []grpcproc.PID{d.PID})
			left[group] = []uint64{d.PID.ID}
		}
		s.tell(false, left)
		return
	}
	if w := s.watchers[d.PID]; w != nil && w.ref == d.Ref {
		delete(s.watchers, d.PID)
		for group := range w.ids {
			remove(s.watching, group, d.PID)
		}
		return
	}
	if node, ok := s.relays[d.PID]; ok {
		delete(s.relays, d.PID)
		if pr := s.peers[node]; pr != nil && pr.relay == d.PID {
			// It ended without a word: asked to, or it panicked.
			s.lost(node, pr, grpcproc.PID{}, grpcproc.ReasonNoConnection)
		}
		return
	}
	if d.PID == s.bridge {
		s.bridge = grpcproc.PID{}
		s.p.Log().Warn("groups: the Membership watch ended; watching again later", "reason", d.Reason)
		s.p.SendAfter[proto.Message](s.cfg.Retry, s.p.Addr(), &groupsv1.Retry{})
	}
}

// watchMembers starts the membership watch, which reports each node
// Membership reports up, and whether Select takes it, and each it reports
// gone. Once watching, it starts a new list, so a node an earlier watch
// listed and this one does not is asked for no more.
func (s *scope) watchMembers() {
	membership, sel := s.cfg.Membership, s.cfg.Select
	a, _, _ := s.p.SpawnMonitor(func(p *grpcproc.Process[proto.Message]) error {
		events, err := membership.Watch(p.Context())
		if err != nil {
			return err
		}
		_ = p.SendTo(p.Parent(), &groupsv1.Listing{})
		for ev := range events {
			selected := ev.Up && (sel == nil || sel(ev.Member))
			_ = p.SendTo(p.Parent(), &groupsv1.MemberEvent{Node: ev.Member.Name, Up: ev.Up, Selected: selected, Incarnation: ev.Member.Incarnation})
		}
		return p.Context().Err()
	}, grpcproc.LinkParent(), grpcproc.WithLabel("groups membership"))
	s.bridge = a.PID()
}

// relay is the process through which a scope reaches the scope of node: a
// send or a monitor that waits for a dial holds up the relay, not the
// scope. It says hello, watches the scope that answers, sends on what its
// own scope gives it, and ends with a Lost when the other is gone or a send
// to it fails, since what it could not send is missing there.
func relay(node, name string, scope grpcproc.PID) func(*grpcproc.Process[proto.Message]) error {
	return func(p *grpcproc.Process[proto.Message]) error {
		ctx, cancel := context.WithTimeout(p.Context(), helloTimeout)
		r, err := p.CallTo[*groupsv1.Hello](ctx, grpcproc.Name{Node: node, Name: name}, &groupsv1.Hello{Scope: scope.Proto()})
		cancel()
		if err != nil {
			reason := err.Error() // it answered, and refused
			switch {
			case errors.Is(err, grpcproc.ErrNoProc):
				reason = grpcproc.ReasonNoProc
			case errors.Is(err, grpcproc.ErrNoConnection), errors.Is(err, context.DeadlineExceeded):
				reason = grpcproc.ReasonNoConnection
			}
			return p.SendTo(scope, &groupsv1.Lost{Reason: reason})
		}
		to := grpcproc.PIDFromProto(r.GetScope())
		p.Monitor(to)
		_ = p.SendTo(scope, &groupsv1.Found{Scope: r.GetScope()})
		for {
			m, err := p.Receive()
			switch {
			case err != nil:
				return err
			case m.Down != nil: // its one monitor's
				return p.SendTo(scope, &groupsv1.Lost{Scope: m.Down.PID.Proto(), Reason: m.Down.Reason})
			case m.From != scope:
			case p.SendTo(to, m.Body) != nil:
				return p.SendTo(scope, &groupsv1.Lost{Scope: r.GetScope(), Reason: grpcproc.ReasonNoConnection})
			}
		}
	}
}

// query answers a Query, on the asker's goroutine.
func (s *scope) query(_ context.Context, q proto.Message) (proto.Message, error) {
	b, ok := q.(*groupsv1.Query)
	if !ok {
		return nil, grpcproc.ErrType
	}
	limit := cmp.Or(int(b.GetLimit()), defaultLimit)
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := &groupsv1.Report{Scope: s.cfg.Scope}
	if group := b.GetGroup(); group != "" {
		members := sorted(s.groups[group])
		r.Omitted = uint32(max(len(members)-limit, 0))
		r.Members = protos(members[:min(len(members), limit)])
	} else {
		for _, name := range slices.Sorted(maps.Keys(s.groups)) {
			switch {
			case !strings.HasPrefix(name, b.GetPrefix()):
			case len(r.Groups) == limit:
				r.Omitted++
			default:
				r.Groups = append(r.Groups, s.report(name))
			}
		}
	}
	for _, node := range slices.Sorted(maps.Keys(s.peers)) {
		pr := s.peers[node]
		v := &groupsv1.Peer{Node: node, State: pr.state.String(), Members: uint32(pr.size)}
		if !pr.scope.IsZero() {
			v.Scope = pr.scope.Proto()
		}
		r.Peers = append(r.Peers, v)
	}
	return r, nil
}

// report is a group's counts. s.mu held.
func (s *scope) report(name string) *groupsv1.Group {
	nodes := s.byNode[name]
	return &groupsv1.Group{Name: name, Members: uint32(len(s.groups[name])), Local: uint32(nodes[s.n.Name()]), Nodes: uint32(len(nodes))}
}

func (s *scope) inspect() map[string]string {
	n := 0
	for _, set := range s.groups {
		n += len(set)
	}
	info := map[string]string{
		"scope":    s.cfg.Scope,
		"groups":   strconv.Itoa(len(s.groups)),
		"members":  strconv.Itoa(n),
		"local":    strconv.Itoa(len(s.joined)),
		"watchers": strconv.Itoa(len(s.watchers)),
		"peers":    strconv.Itoa(len(s.peers)),
	}
	count := map[state]int{}
	for _, pr := range s.peers {
		count[pr.state]++
	}
	for st, c := range count {
		info["peers "+st.String()] = strconv.Itoa(c)
	}
	return info
}

func add(m map[string]map[grpcproc.PID]struct{}, group string, pid grpcproc.PID) {
	if m[group] == nil {
		m[group] = map[grpcproc.PID]struct{}{}
	}
	m[group][pid] = struct{}{}
}

func remove(m map[string]map[grpcproc.PID]struct{}, group string, pid grpcproc.PID) {
	delete(m[group], pid)
	if len(m[group]) == 0 {
		delete(m, group)
	}
}

func has(m map[string]map[grpcproc.PID]struct{}, group string, pid grpcproc.PID) bool {
	_, ok := m[group][pid]
	return ok
}

func ids(ps []grpcproc.PID) []uint64 {
	out := make([]uint64, len(ps))
	for i, p := range ps {
		out[i] = p.ID
	}
	return out
}
