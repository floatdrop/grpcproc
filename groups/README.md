# grpcproc/groups

Process groups for [grpcproc](https://floatdrop.github.io/grpcproc/), after
Erlang's `pg`: named sets of processes, on any node, that any process can
list and watch. A process joins a group on its own node; every node that
runs the same scope learns of it, and of its leaving, its exit, or its node
going away. A package of grpcproc's, built on its public API alone.

```sh
go get github.com/floatdrop/grpcproc # groups is a package of the core module
```

```go
// every node that takes part runs the scope
workers, err := groups.Start(node, groups.Config{Scope: "jobs"})

// a process joins on its own node
err = workers.Join(ctx, p, "resize", p.PID())

// any node lists the members, from its own copy, without a message
members := workers.Members("resize") // every node's, ordered by node

// a process that must hear of changes watches the group
w, err := workers.Watch(ctx, p, "resize")
for {
    m, err := p.Receive()
    // a *groupsv1.Changed from w.From; the Down with w.Ref is the end of them
}
```

## Scopes

A scope is a process on each node that takes part, registered as
`groups/<scope>` and labelled `groups`. Nodes whose scopes share a name share
their groups; `Config.Scope` is `default` when empty. A node finds the other
nodes of its scope through:

| Config | The nodes it asks |
| --- | --- |
| `Peers` | these, from the start, whatever Membership reports |
| `Membership` | each node it reports up, and forgets each it reports down, unless `Peers` names it; unset, the node's own `Config.Membership` |
| `Select` | of those Membership reports up, only the ones it takes: by their `Metadata`, say, to keep a scope to the nodes that run it. One it stops taking is only answered, as a node that asks this one is |

A node that asks this one is answered whatever these say, and so is one
that writes to it with no relay to it running: it is asked again, unless it
waits to be, and syncs again. With neither `Peers` nor a `Membership`, a scope knows only the nodes
that ask it.

`Start` fails when the node already runs a scope of that name. A scope that
ends, asked to exit or with its node, takes its node's memberships with it:
the other nodes drop them, and its copy is empty.

## Joining and leaving

| Call | What it does |
| --- | --- |
| `scope.Join(ctx, from, group, pids…)` | adds processes of the scope's node to `group`; once each, however often joined |
| `scope.Leave(ctx, from, group, pids…)` | takes them out of it; a process that is not in it is left alone |

`from` is the `Node` or a `Process`, as for `Addr.Call`. A process stays in a
group until it leaves it or exits. Both refuse a process of another node
(`ErrNotLocal`), and a group's name that is empty, not valid UTF-8, or longer
than 1 KiB (`ErrGroup`); `Join` refuses a process that does not run
(`grpcproc.ErrNoProc`). A refusal changes nothing.

## Reading and watching

`scope.Members(group)`, `scope.Local(group)` and `scope.Groups()` read the
node's copy under a lock, from any goroutine, without a message: every
node's members, this node's own, and the groups with members. The copy is
eventually consistent. This node's joins are in it when `Join` returns;
another node's, once that node's message arrives.

`scope.Watch(ctx, p, group)` has the scope send `p`, a process of the
scope's node, a `*groupsv1.Changed` for each change to the group: who joined
and who left. `Watch.Members` are the members when the watch began, and the
changes follow from there. `p`'s mailbox must accept `*groupsv1.Changed`
(`grpcproc.ErrType` otherwise). Each watch is ended by its own `w.Cancel(p)`,
or by `p`'s exit; a process with several watches of one group gets each
change once. The `Down` with `w.Ref` says the scope ended.

## Between nodes

A scope reaches each other node's scope through a relay, a process of its
own labelled `groups relay`, so that a send or a monitor that waits for a
dial holds up the relay and not the scope; a call from another node is
answered from a process of its own, for the same reason. The relay says
hello to the other scope, watches the one that answers, and carries this
node's members to it: the whole list once (a `Sync`), then each change (an
`Update`), both in parts of about 32 KiB. A relay that fails to send ends,
and the next one syncs again.

| What happens to another node | Its members on this node |
| --- | --- |
| its scope answers, and syncs | are its list, then its changes |
| it runs no such scope, or its scope ends | dropped: it is `absent` |
| it cannot be reached | dropped: it is `unreachable` |
| Membership reports it gone | dropped, and the node forgotten, unless `Peers` names it; news of an incarnation older than one the node is known to have changes nothing |

An `absent` or `unreachable` node is asked again after `Config.Retry` (1s),
doubling with each loss in a row up to 32 times as long; a relay that lasts
longer than the wait before it starts the count over. A Hello from the node,
or Membership reporting a newer incarnation of it, ends the wait; the wait
is the node's, so one forgotten and made again, as the same incarnation,
waits it out. A node that is only answered is forgotten instead when
its scope ends, or after five tries in vain, and asked again when it writes
once its wait is over: one that writes while it waits waits. A node whose
scope answers the Hello with an error is `absent`, not `unreachable`.

## Inspection

A scope's `WithInspect` map counts its groups, members, watchers and peers
by state. It answers a `groupsv1.Query` through `grpcproc.WithQuery`, which
a read-only Inspector serves: its groups whose names start with a prefix,
with each one's members on every node and on this one, or one group's
members, and the other nodes it knows with their states. `grpcprocctl
groups`, the MCP tools `process_groups` and `group_members`, and the web UI's
Groups page read it. See [`docs/DESIGN.md`](../docs/DESIGN.md#process-groups-grpcprocgroups)
for why it works as it does.
