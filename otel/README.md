# grpcproc/otel

OpenTelemetry for [grpcproc](https://floatdrop.github.io/grpcproc/guides/observability/): a span for every send, call and
handled message, chained across processes and nodes, and metrics keyed by
process label. A separate module, so grpcproc itself does not depend on
OpenTelemetry.

```sh
go get github.com/floatdrop/grpcproc/otel
```

```go
h, err := grpcprocotel.New() // global meter/tracer providers and propagator; see Options
node, err := grpcproc.NewNode(grpcproc.Config{…, Hooks: grpcproc.JoinHooks(h, myHooks)})
reg, err := h.Observe(node) // gauges read from node snapshots at each collection
defer reg.Unregister()
lreg, err := h.ObserveLeader(node) // gauges of the node's grpcproc/leader elections
defer lreg.Unregister()
```

## Traces

| Where | Span | Kind | Parent |
| --- | --- | --- | --- |
| `Send` | `send <message type>` | producer | the context the sender carries |
| `Call` | `call <message type>` | client | the context the sender carries |
| a process takes a message | `process <message type>` | consumer (server for a call) | the sender's span |
| a process takes a `Down` | `process grpcproc.Down` | consumer | — |
| a process that traps exits takes an `Exited` | `process grpcproc.Exited` | consumer | — |

A handling span lasts until the process calls `Receive` again, or exits (with
an error status if abnormally). Everything the process sends meanwhile is
its child, because grpcproc hands a process's sends the metadata of the
message it is handling, and these hooks put the span there. No context has
to be threaded through handlers for the chain to form.

For a handler's own spans (a database call), take the handling span from the
message, in the message's context, which ends when its caller stops waiting:

```go
ctx, cancel := m.Context(p.Context())
defer cancel()
ctx, span := tracer.Start(h.Extract(ctx, m.Metadata), "load order")
```

Spans carry `messaging.system=grpcproc`, `messaging.operation.type`,
`messaging.destination.name`, `grpcproc.message.type`, `grpcproc.label`,
`grpcproc.pid`; failed sends and calls carry `error.type`.

## Metrics

No metric carries a PID: the process label (`grpcproc.WithLabel`, default the
message type) is the unit, and every other attribute is bounded.

| Name | Type | Attributes |
| --- | --- | --- |
| `grpcproc.messages.sent` | counter | `grpcproc.label` (sender), `grpcproc.call`, `grpcproc.remote` |
| `grpcproc.messages.received` | counter | `grpcproc.label` |
| `grpcproc.mailbox.wait` | histogram, s | `grpcproc.label` |
| `grpcproc.process.duration` | histogram, s | `grpcproc.label` — time from taking a message to the next `Receive` |
| `grpcproc.call.duration` | histogram, s | `grpcproc.label`, `grpcproc.remote`, `error.type` on failure |
| `grpcproc.processes.spawned` | counter | `grpcproc.label` |
| `grpcproc.processes.exited` | counter | `grpcproc.label`, `grpcproc.reason` (`normal`, `shutdown`, `killed`, `noproc`, `noconnection`, `type`, `name lost`, `name conflict`; `max restarts` from actor, `timeout` and `replaced` from cron, `demoted` from leader; `panic`, `error`) |
| `grpcproc.dead_letters` | counter | `grpcproc.reason`, `grpcproc.message.type` |
| `grpcproc.links.up`, `grpcproc.links.down` | counter | `grpcproc.peer` — sessions with the peer begun and ended (`OnLinkUp`, `OnLinkDown`) |
| `grpcproc.processes` | gauge (Observe) | `grpcproc.label` |
| `grpcproc.mailbox.depth` | gauge (Observe) | `grpcproc.label`, summed |
| `grpcproc.mailbox.oldest` | gauge (Observe), s | `grpcproc.label`, maximum |
| `grpcproc.link.messages`, `grpcproc.link.bytes` | counter (Observe) | `grpcproc.peer`, `grpcproc.direction` — what each link had carried at the collections that saw it, added up across links, so a broken link does not take it back |

`error.type` is one of `noproc`, `type`, `mailbox_full` (the callee's
mailbox was full: `WithMailboxLimit`), `too_large` (the message, or the
reply, was over `Config.MaxMessageSize`), `busy` (the link to the peer was
full: `Config.MaxQueued`), `noconnection`, `timeout`, `canceled`, `remote`
(the handler returned an error), `other`.

## Leader elections

`ObserveLeader` reads each [grpcproc/leader](../leader/README.md) elector on
the node, the processes named `leader.ElectorName(cluster)` with its label,
at each collection, from its inspect map, and reports one set of gauges per
election, by `grpcproc.leader.cluster`. The electors are asked at once. One
that the node's snapshot shows on one message for a second (busy in a slow
`Store`, say), or that does not answer within a second of a collection that
lasts that long, is reported as stalled, with no other gauge.

| Name | What it says |
| --- | --- |
| `grpcproc.leader.electors` | 1, with `grpcproc.leader.role`: `leader`, `candidate`, `follower` or `unclustered` |
| `grpcproc.leader.stalled` | 1 while the elector is on one message for a second or more, or does not answer within one; 0 while it answers |
| `grpcproc.leader.term` | the elector's term |
| `grpcproc.leader.view`, `grpcproc.leader.quorum` | the nodes whose majority elects, and how many that majority is |
| `grpcproc.leader.reachable` | the nodes of the view it hears from, itself included |
| `grpcproc.leader.singleton` | 1 while the singleton runs on this node |
| `grpcproc.leader.checkpoints.waiting` | `Checkpoint` calls waiting for a majority to hold their state |

Alerts per cluster, in PromQL, with the names as the Prometheus exporter
writes them: dots turned to underscores, and a unit suffix where there is one:

```promql
# No leader: the leaders, with a 0 for every elector, so that none adds up to 0.
# A cluster below MinClusterSize has none by design: its electors are unclustered.
sum by (grpcproc_leader_cluster) (grpcproc_leader_electors{grpcproc_leader_role="leader"} or grpcproc_leader_electors * 0) < 1
# No node of a cluster reporting at all: one rule per cluster.
absent(grpcproc_leader_electors{grpcproc_leader_cluster="scheduler"})
# Two leaders: a leader cut off from the others has not noticed.
sum by (grpcproc_leader_cluster) (grpcproc_leader_electors{grpcproc_leader_role="leader"}) > 1
# The singleton runs nowhere, or twice.
sum by (grpcproc_leader_cluster) (grpcproc_leader_singleton) != 1
# One more node lost and this side has no majority; below it, none can be elected.
# A view of one or two never has a node to spare.
grpcproc_leader_reachable <= grpcproc_leader_quorum and grpcproc_leader_view >= 3
# Elections keep happening.
changes(max by (grpcproc_leader_cluster) (grpcproc_leader_term)[15m:]) > 3
# A majority is not taking the leader's state.
grpcproc_leader_checkpoints_waiting > 0
# An elector held up for a second or more, which sends no heartbeat or vote meanwhile.
grpcproc_leader_stalled > 0
```

Give each a `for:` of a few scrape intervals, a minute say: an election, and
a hand-over (`Resign` stops the old singleton before the next leader starts
its own), leave no leader or no singleton for a moment, and a leader cut off
from the others leads for up to two election timeouts after they elect
another. A stalled elector is in none of the sums: when the leader is the one
held up, the other nodes' gauges show no leader, and with no other node
reporting the sums are empty, which only "stalled" alerts on. A second is
fixed: with an `ElectionTimeout` above it, a `Store` slow enough to be stalled
can still be within it, so give "stalled" a `for:` longer than the timeout.
