# grpcproc/pubsub

Publish/subscribe for [grpcproc](https://floatdrop.github.io/grpcproc/):
topics a process publishes to and any node subscribes to, with the last few
events kept for whoever comes late, and each event sent once to each node. A
topic is a process, so it has a PID, a name, an exit reason and an inspect
map. A package of grpcproc's, built on its public API alone.

```sh
go get github.com/floatdrop/grpcproc # pubsub is a package of the core module
```

```go
// the publisher owns the topic: it ends when the publisher does
placed, err := pubsub.SpawnOwned[*orderspb.Placed](p, pubsub.Config{Buffer: 100}, grpcproc.WithName("placed"))
err = placed.Publish(ctx, p, &orderspb.Placed{Id: id})

// a process on any node subscribes by the topic's name
sub, err := pubsub.Named[*orderspb.Placed]("orders", "placed").Subscribe(ctx, p)
for {
    m, err := p.Receive()
    // an event is a message from sub.From; the Down with sub.Ref is the end of them
}
```

The [guide](https://floatdrop.github.io/grpcproc/guides/pubsub/) walks
through [`examples/pubsub`](../examples/pubsub/main.go): a topic of stock
levels on one node, a dashboard subscribed from another.

## Topics

| Spawned with | The topic |
| --- | --- |
| `pubsub.SpawnOwned[E](p, cfg, opts…)` | is linked to `p`: it ends when `p` does, with `p`'s reason, which its subscribers receive in their `Down` |
| `pubsub.Spawn[E](node, cfg, opts…)` | is the node's, whoever publishes to it: it lasts until it is asked to exit or the node stops |

`opts` are `grpcproc.SpawnOption`s: a name, a label (`pubsub.topic` by
default). `Config.Buffer` is how many of the last events the topic keeps for
a new subscriber; zero keeps none.

`Topic[E]` is an address that carries the event type, as `Addr[M]` carries a
mailbox's: `pubsub.Named[E](node, name)` reaches a topic by its name,
`pubsub.TopicOf[E](pid)` by its PID or `Name`, and `topic.Addr()` is its
process, to monitor, link to or ask to exit. A topic whose `E` is an
interface, `proto.Message` say, takes any message of it.

`topic.Publish(ctx, from, e)` is a send to the topic, from the `Node` or a
`Process`, as `Addr.Send` is. The topic sends each event on in the order it
received them, with the metadata it was published with, trace context
included. A call to `topic.Addr()` publishes too, and is answered with
`emptypb.Empty` once the event is sent on. A message that is not an `E` is
answered with `ErrType` when it is a call, and logged and dropped when it is
a send.

## Subscribing

`topic.Subscribe(ctx, p)` subscribes `p`, from inside its handler. When it
returns, the events the topic kept are in `p`'s mailbox, and everything
published after them follows: nothing is missed between the two, and nothing
arrives twice. Events are plain messages from `sub.From`, so `p`'s mailbox
must accept `E`; `Subscribe` fails with `ErrType` when it does not, or when
the topic publishes another concrete type (a topic or subscriber whose `E` is
an interface is not checked). A process that subscribes twice still gets each
event once, but each `Subscribe` monitors the topic anew, with a `Ref` of its
own.

| When | The subscriber |
| --- | --- |
| the topic exits | receives the `Down` with `sub.Ref`, with the topic's reason: an owned topic's is its owner's |
| the topic's node becomes unreachable | receives that `Down` with `noconnection` |
| it calls `sub.Cancel(p)` | stops monitoring the topic and tells it to send no more; what the topic sent before it heard may still arrive, and what is in the mailbox, a `Down` included, stays |
| it exits | is forgotten: the topic monitors its subscribers |

A subscriber that should end with the topic, rather than be told, links to
it as well: `p.Link(topic.Addr())`.

`ctx` bounds the whole of `Subscribe`. If it ends first, `Subscribe` tells
the topic to forget `p`, in case the request reached it all the same; events
sent before that may still reach `p`. A topic that does not exist is
`ErrNoProc`, a node that cannot be reached `ErrNoConnection`, as for a call.

## Demand

`Config.Notify` names a process the topic tells whether anyone listens: a
`*pubsubv1.Demand` with `Subscribed` true when the first subscriber comes,
and false when the last one leaves, by `Cancel` or by exiting. A producer
whose events are costly to make makes them only while someone wants them:

```go
topic, err := pubsub.SpawnOwned[*pricespb.Quote](p, pubsub.Config{Notify: p.PID()})
…
if d, ok := m.Body.(*pubsubv1.Demand); ok {
    polling = d.GetSubscribed()
}
```

The `Demand` is a message like any other, so the notified process's mailbox
must accept it: `proto.Message`, or an interface both types implement.

## Across nodes

A process that subscribes to a topic of another node does so through a relay
on its own node, a process named after the topic's address
(`pubsub:{stock@warehouse}`, labelled `pubsub.relay`), which the first such
subscriber starts. The relay is a topic whose events are the upstream
topic's, and it keeps as many as the upstream does, so the topic sends each
event once to each node, however many subscribers the node has, and a later
subscriber gets the kept events without a trip across the network.

For a subscriber there, `sub.From` is the relay. The relay ends with the
topic, for the topic's reason, which its subscribers see in their `Down`, and
when its last subscriber leaves, after which the topic forgets it.

The relay subscribes to the topic while its first subscriber waits, within
that subscriber's deadline, which the call carries: give `Subscribe` a
deadline, or the relay waits until the topic answers or its node becomes
unreachable. A relay that fails ends, and the subscribers queued behind the
first start another.

## Inspecting

Every topic and relay publishes `subscribers`, `kept` and `published` in its
inspect map, and a relay its `upstream` topic, which
[`grpcprocctl inspect`](../tools/README.md) shows with the process.

## Limits

- Delivery is at most once, and in order from one sender, as for any
  message. A subscriber that must not miss an event needs acknowledgements
  built above this.
- Nothing pushes back on a publisher: a subscriber slower than its topic has
  a mailbox that grows, as any process's does.
- A node's topic that a supervisor restarts comes back with no subscribers.
  They receive a `Down`, and subscribe again.
- A topic addressed by its name and by its PID gets two relays on a node, one
  for each.
- A topic is for an application's events between processes. What happens on
  the node itself, spawns, exits, links up and down, is `node.Subscribe`.
