# grpcproc/saga

Sagas for [grpcproc](https://floatdrop.github.io/grpcproc/): work that spans
services and outlasts a process. Reserve, charge, ship, and undo what was
done when a later step cannot be. Each run is kept in a store, so with a
store that outlasts the program, a crash, a restart or a lost node is
followed by the run going on from where it was. The stores are in memory,
which the nodes of one program can share, so a run outlasts a node of that
program but not the program's restart, and in PostgreSQL, in
[`grpcproc/saga/postgres`](postgres/README.md) (see [Stores](#stores)). A
package of grpcproc's, built on its public API and
[fsm](https://github.com/floatdrop/fsm).

```sh
go get github.com/floatdrop/grpcproc # saga is a package of the core module
```

```go
var order = fsm.MustNew("order",
    fsm.Initial(Reserving),
    fsm.From(Reserving).On(reserved).To(Charging),
    fsm.From(Reserving).On(refused).To(Refused),
    fsm.From(Charging).On(charged).To(Done),
    fsm.From(Charging).On(declined).To(Releasing),
    fsm.From(Releasing).On(released).To(Refused),
)

orders := saga.Define[*ordersv1.Order]("order", order).
    Do(Reserving, func(ctx context.Context, r *saga.Run[State, *ordersv1.Order]) error {
        res, err := stock.Reserve(ctx, r.Process(), &inventoryv1.Reserve{Order: r.ID(), Sku: r.Data.Sku})
        if err != nil {
            return err // tried again, with the same key
        }
        if res.Refused != "" {
            return r.Fire(ctx, refused, res.Refused)
        }
        return r.Fire(ctx, reserved, fsm.Unit{})
    }).
    Do(Charging, charge).
    Do(Releasing, release)

eng, err := saga.Start(node, saga.Config{Store: store}, orders)
created, err := orders.Begin(ctx, eng, "order-123", &ordersv1.Order{Sku: "apple"})
snap, err := orders.Wait(ctx, eng, "order-123") // once it is Done or Stuck
```

A saga that is a list of steps, each with what undoes it, needs no machine
of its own: see [Sequence](#sequence).

## A run is a state and its data

A saga is an fsm machine and a protobuf message. A run is one state of the
machine and one value of the message, kept together as one record. `Begin`
starts a run in the machine's `Initial` state; its id is unique within the
saga, so beginning it again is no error and changes nothing, and the id is
the idempotency key of whoever begins it.

`Do` gives a state its *effect*, the function that runs when a run enters
the state. The effect does its work and fires the event that moves the run
on, with `Run.Fire`; what it wrote to `Run.Data` is saved with the new
state. An effect that returns `nil` without firing has done its part, and
the run waits in the state, for a signal or its timer. A state with no
effect waits from the start. A state with no way out ends the run: it is
`Done`.

Nothing is replayed. After a crash, the code that runs is the effect of the
state the record is in, so an effect is ordinary Go, goroutines and
`select` included, and a new version of the program takes a run up where
the record says it is. That is the difference from Temporal, which replays
a history through the workflow's code and forbids both, and from DBOS,
which runs the function again and asks for the same steps in the same
order.

| `Run` | |
| --- | --- |
| `Data` | the run's data; saved when the effect returns `nil`, dropped when it fails |
| `ID()`, `State()` | as `Begin` was given it, and the state it is in |
| `Fire(ctx, ev, arg)` | moves the run on; once the machine has taken an event, another `Fire` is `ErrFired` |
| `Process()` | the run's process: the `Caller` of the effect's sends and calls |
| `Key()`, `Fence()` | the idempotency key of this visit, and the run's epoch, raised by each claim of it |
| `Attempt()` | which attempt of the effect this is in this visit, from 1 |

An event the machine refuses in the state is a `Permanent` error: trying
again would not change the machine. An internal transition (fsm's `Stay`)
takes its event without leaving the state: the effect is not run again, and
the state's timer stands.

## An effect runs at least once

The record is saved before each effect and when the run waits, not as an
effect returns, so a crash after an effect and before the next save runs the
effect again, on whichever node takes the run up. That cannot be avoided,
only made safe: what an effect asks of others must be safe to ask twice.

`Run.Key` names the visit to the state, the same for every attempt of it:
the saga's name, the run's id, the state and how many states the run has
entered, as in `order/order-123/Charging/2`. The effect's `ctx` carries the
key and the fence as metadata (`grpcproc-saga-key`, `grpcproc-saga-fence`),
so every send and call it makes with that `ctx` carries them too. A
participant reads them with `KeyOf`, keeps the key with what it did, and
answers a repeat with what it answered before:

```go
key, fence, ok := saga.KeyOf(m.Metadata)
if ok {
    prev, seen := payments.Find(key)
    switch {
    case seen && fence < prev.Fence:
        return m.Reply(nil, errStale) // an earlier owner's request, late
    case seen:
        payments.RaiseFence(key, fence) // the highest seen for key
        return m.Reply(prev.Answer, nil)
    }
}
// charge, then keep key, fence and the answer
```

The fence grows with each engine that claims the run, so a participant
that keeps the highest it has seen for a key can refuse a request a
previous owner sent before it lost the run.

## Failures

An effect that returns an error is run again after a backoff, with the same
key; how many times it failed, and why, are in the record. A call that
timed out may have been handled, so the effect returns its error and is
tried again; it is never taken for a refusal. A refusal is an answer, and
the effect fires an event for it.

| Option of `Do` | |
| --- | --- |
| `Backoff(lo, hi)` | the wait after the first failure, doubling to `hi`; 100ms to a minute by default |
| `Attempts(n)` | the tries in one visit before the effect has failed for good; 0, the default, is no bound |
| `Otherwise(ev)` | fires `ev`, with the error's text, once the effect has failed for good |

`Permanent(err)` marks an error trying again cannot get past: the effect is
not run again. Once an effect has failed for good, by a `Permanent` error or
by running out of `Attempts`, its `Otherwise` fires and the run goes on from
where the machine takes it; the error is kept as the run's `Cause`. One the
machine takes by an internal transition leaves the run waiting in the state,
for a signal or its timer. With no `Otherwise`, or one the machine refuses,
the run is `Stuck`: kept as it is, and shown as such, until `Resume` has its
effect tried again, its attempts counted from none. A compensation that
keeps failing has no automatic answer, and a run that is dropped is worse
than one that waits for a person.

## Signals

A signal is an event sent to a run from outside it: a payment confirmed, a
person who approved, a parcel delivered. `Accept` names the events a saga
takes from outside, and how a payload is written to the run's data;
`AcceptSignal` takes one with no payload.

```go
orders.
    Accept(paid, func(o *ordersv1.Order, p *paymentsv1.Paid) { o.PaymentId = p.Id }).
    AcceptSignal(cancelled)

err := orders.Signal(ctx, eng, "order-123", paid, &paymentsv1.Paid{Id: "pay-9"})
err = orders.Notify(ctx, eng, "order-123", cancelled) // no payload
```

A signal is kept with the run until its machine is in a state that takes it.
In each state the machine takes the first signal waiting that the state
accepts, so of the signals a state accepts, the earlier is taken first; one
that comes early waits for its state while later ones go by. The effect of a
visit runs before any signal moves the run on from it. A run holds
`MaxInbox` (64) signals that wait, and `Signal` refuses more with
`ErrInboxFull`; one that no state takes waits until the run ends, so a
sender that repeats itself can fill it. A run that has ended refuses signals
with `ErrEnded`, and an id with no run is `ErrNoRun`; an event the saga does
not `Accept` is refused before it is sent.

A signal that cannot be delivered, with a payload that does not decode or
whose taking panics, is dropped and logged, and the run stays as it was.
One whose taking fails by the machine's action stays, those behind it
waiting, and is tried again a `Poll` later.

## Timers

```go
orders.After(AwaitingPayment, 24*time.Hour, expired)
```

`After` fires an event on a run still in the same visit to the state once
the time has passed since it entered it, between attempts if its effect is
failing. The time is a field of the record, and the store finds the runs
whose time has come, so a timer outlasts the engine that set it, and no
process waits for it. It fires once: a machine that refuses it stays in the
state with no timer, and one whose action fails is asked again a `Poll`
later.

## Sequence

The saga of the textbooks is steps done in order, each with what undoes
it. `Sequence` builds one as a machine:

```go
orders, err := saga.Sequence("order",
    saga.Step("reserve", reserve).Undo(release),
    saga.Step("charge", charge).Undo(refund).Pivot(),
    saga.Step("ship", ship),
)
```

A step's effect does not fire: returning `nil` is the step done. A step
that fails for good up to the pivot has the steps done so far undone, last
first, itself included, since it may have happened; the run then ends in
`StageFailed`, with why in its `Cause`. After the pivot the run only goes
forward, and a step that fails for good leaves it `Stuck`. Without a
pivot, the last step is it. A step fails for good only by a `Permanent`
error or by running out of its `Attempts`; with neither, it is tried until
it works.

An undo must do nothing for a step that never happened, or the rest of one
that happened in part. The run's stages are the steps' names, `undo <name>`
while that step is undone, `StageDone` and `StageFailed`.
[`example_test.go`](example_test.go) runs two orders through this sequence,
one whose card is declined.

## Engines

`Start` runs sagas on a node, from a store, until the node stops. Engines
on several nodes that share a store share the runs.

| `Config` | |
| --- | --- |
| `Store` | keeps the runs; required |
| `Name` | the engine's process's registered name; `saga` by default |
| `Poll` | how often the engine asks the store for runs that are due, and `Wait` about a run; 1s |
| `Lease` | how long a claim holds a run without being renewed; 30s |
| `Concurrency` | how many runs the engine works on at once; 64 |
| `InspectData` | show a run's data and its signals' payloads to a query through the Inspector; off |

The engine is a process. It claims the runs that have something to do on
each tick, at once when a run is begun, signalled or resumed through it,
and when a run it let go is due before the next tick. For each run it
claims it spawns a process, labelled `saga:<name>`, which lives while the
run has work and exits when it waits. A run at work is in the process list
and the Inspector; a run that waits is a record and no more.

Each claim is for a lease, and raises the run's epoch. While the run is
worked on, the engine renews the lease a quarter of it after the claim and
after each renewal, and tries a renewal that failed again a sixteenth of it
later, logging the first failure. It counts the lease by its own clock,
from before it asked for the claim. Only the owner writes the run's state,
and the store refuses a write with an older epoch, so an engine that was
cut off, and whose lease another took, cannot save over it. An effect's
`ctx` ends when the store says the run is another's, or when a quarter of
the lease is left and no renewal got through, before another engine may
have it; the new owner runs the effect again, with the same key.

When the node stops, a run's `ctx` is cancelled, nothing is saved, and its
lease is let go, so another engine claims it at its next `Poll`, without
waiting the lease out. A node that dies leaves the lease to run out.

## Two versions of a program

In a rolling deploy the old program meets the new one's runs, in states its
machine lacks. So a saga has a version, `Definition.Version(v)`, and a run
keeps the highest that began, signalled or worked on it. An engine claims
only the runs its version reaches: the old program leaves alone what the
new one has touched, and the new one takes up the old one's runs as they
are. Raise the version when the machine gains a state, an event or a timer.
A record a program cannot read though its version says it can is `Stuck`,
with why.

## Asking about a run

`Get` returns a run as it stands, `Wait` once it is `Done` or `Stuck`, both
as a `Snapshot`: its state, data, status, the failures of the state's
effect in this visit, the last error and the `Cause`. A run is `Active`
while it goes on, at work or waiting for a signal, a timer or its next
attempt. `Store.List` returns the records of a saga, by id.

From outside the program, an engine answers through the node's
[Inspector](https://floatdrop.github.io/grpcproc/guides/inspector/), which
is what `grpcprocctl saga` and its MCP tools ask
([grpcprocctl](../tools/README.md#sagas)):

- **What it runs.** It publishes, through `WithInspect`, each saga it runs
  with its version (`saga order: v2`) and how many runs it works on
  (`working`).
- **Queries.** It is spawned with `grpcproc.WithQuery`, so an Inspector
  `Query` of a [`grpcproc.saga.v1.Query`](proto/grpcproc/saga/v1/saga.proto)
  lists the runs of the sagas it runs, by saga and status, a page at a
  time, or gets one. A run's data and its signals' payloads are shown, as
  JSON, only with `Config.InspectData`: a read-only Inspector is not a
  private one, and what a run carries is often a customer's. Its error and
  cause are shown all the same, since they say why it is stuck, so an
  effect should not put a customer's data in its error. A saga the
  engine does not run is `ErrNoSaga`, though its runs are in the store. A
  query only reads, so a read-only Inspector allows it, and
  `inspect.NoQueries()` refuses it.
- **Controls.** An Inspector `Call` with a `grpcproc.saga.v1.Control`
  resumes a stuck run, as `Definition.Resume` does. A read-only Inspector
  refuses it.

A query is answered on the asker's goroutine, with the asker's ctx, and a
control in a process of its own, labelled `saga control`, so the store is
never asked from the loop that claims runs. An engine answers four of each
at once, and refuses the next with `ErrBusy`. A list reads each saga's runs
whole. An answer carries a run's data up to a megabyte of JSON and a
payload up to sixteen kilobytes, and says how long one it leaves out is; a
page carries a kilobyte of each run's error and how many signals wait.

## Stores

`Store` is one interface: `Create` if absent, `Claim`, `Save` (fenced by
the epoch), `Renew`, `Signal`, `Resume`, `Get` and `List`. The engine has no
state of its own: ownership is a lease in the store, a timer is a field of
the record, and nothing is published, so the store is the only part that
must outlast a process.

`saga.Memory()` keeps the runs in memory: for tests, and for sagas that
need not outlast their program.
[`grpcproc/saga/postgres`](postgres/README.md) keeps them in PostgreSQL:

```go
store, err := postgres.NewStore(db, "grpcproc_sagas") // db is a *sql.DB
err = store.Migrate(ctx)                               // or apply store.Schema() yourself
eng, err := saga.Start(node, saga.Config{Store: store}, orders)
```

A store of your own must pass
[`sagatest.Store`](sagatest/store.go), which checks what an engine relies
on:

```go
func TestStore(t *testing.T) {
    sagatest.Store(t, func(t *testing.T) saga.Store { return openEmpty(t) })
}
```

Every method returns when its `ctx` ends, and `Save` and `Renew` with
another epoch than the run's return `ErrLost`.

## Not built yet

Stores in etcd and in a leader's checkpoint, a
wrapper for participants that answers a repeated key and refuses a lower
fence, steps that run side by side, retention of finished runs, and a view
of the runs in grpcprocctl's web UI. [DESIGN.md](../docs/DESIGN.md#sagas-grpcprocsaga) has
the reasons behind the design, and its open work the rest.
