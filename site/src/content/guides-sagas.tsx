import example from '../../../saga/example_test.go?raw';

import { Code, Output, region } from '../code.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

/** What ExampleSequence prints, as its Output comment pins it. */
const exampleOutput = region(example, /^\t\/\/ Output:$/, /^}$/)
	.split('\n')
	.slice(1, -1)
	.map((l) => l.replace(/^\/\/ ?/, ''))
	.join('\n');

export const guidesSagas: Doc = {
	path: 'guides/sagas/',
	title: 'Sagas',
	description:
		'Work that spans services and outlasts a process: an fsm machine and its data per run, kept in a store, with retries, signals, durable timers and compensations.',
	lead: (
		<p>
			<C>grpcproc/saga</C> keeps each run of a saga in a store, so that, with a store that outlasts
			the program, a crash, a restart or a lost node is followed by the run going on from where it
			was: <A to="guides/sagas/#stores">a store</A> in PostgreSQL is one. It is
			a package of the core module,
			built on its public API and <Ext href="https://github.com/floatdrop/fsm">fsm</Ext>.
		</p>
	),
	sections: [
		{
			id: 'start',
			title: 'Install and start',
			body: (
				<>
					<Code lang="sh">{'go get github.com/floatdrop/grpcproc # saga is a package of it'}</Code>
					<Code>{`var order = fsm.MustNew("order",
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
snap, err := orders.Wait(ctx, eng, "order-123") // once it is Done or Stuck`}</Code>
					<p>
						<C>Define</C> names the saga and gives it a machine; <C>Start</C> checks every
						definition, and runs them on the node until it stops. A saga that is a list of steps,
						each with what undoes it, needs no machine of its own: see{' '}
						<A to="guides/sagas/#sequence">Sequence</A>.
					</p>
				</>
			)
		},
		{
			id: 'runs',
			title: 'A run is a state and its data',
			body: (
				<>
					<p>
						A saga is an fsm machine and a protobuf message. A run is one state of the machine and
						one value of the message, kept together as one record. <C>Begin</C> starts a run in the
						machine's <C>Initial</C> state. Its id is unique within the saga, so beginning it again
						is no error and changes nothing: the id is the idempotency key of whoever begins it.
					</p>
					<p>
						<C>Do</C> gives a state its <em>effect</em>, the function that runs when a run enters
						the state. The effect does its work and fires the event that moves the run on, with{' '}
						<C>Run.Fire</C>; what it wrote to <C>Run.Data</C> is saved with the new state. An
						effect that returns <C>nil</C> without firing has done its part, and the run waits in
						the state for a signal or its timer. A state with no effect waits from the start, and a
						state with no way out ends the run: it is <C>Done</C>.
					</p>
					<p>
						Nothing is replayed. After a crash the code that runs is the effect of the state the
						record is in, so an effect is ordinary Go, goroutines and <C>select</C> included, and a
						new version of the program takes a run up where the record says it is. Temporal replays
						a history through the workflow's code, which forbids both; DBOS runs the function again
						and asks for the same steps in the same order.
					</p>
					<Table
						head={['Run', 'Is']}
						rows={[
							[<C>Data</C>, <>The run's data: saved when the effect returns <C>nil</C>, dropped when it fails.</>],
							[<C>Fire(ctx, ev, arg)</C>, <>Moves the run on. Once the machine has taken an event, another is <C>ErrFired</C>.</>],
							[<C>Process()</C>, <>The run's process: the <C>Caller</C> of the effect's sends and calls.</>],
							[<C>Key()</C>, 'The idempotency key of this visit to the state.'],
							[<C>Fence()</C>, 'The run’s epoch, raised by each claim of it: it orders the run’s owners.'],
							[<C>Attempt()</C>, 'Which attempt of the effect this is in this visit, from 1.']
						]}
					/>
					<p>
						An event the machine refuses in the state is a <C>Permanent</C> error, since trying
						again would not change the machine. An internal transition (fsm's <C>Stay</C>) takes its
						event without leaving the state: the effect is not run again, and the state's timer
						stands.
					</p>
				</>
			)
		},
		{
			id: 'once',
			title: 'An effect runs at least once',
			body: (
				<>
					<p>
						The record is saved before each effect and when the run waits, not as an effect
						returns, so a crash after an effect and before the next save runs it again, on
						whichever node takes the run up. That cannot be avoided, only made safe: what an effect
						asks of others must be safe to ask twice.
					</p>
					<p>
						<C>Run.Key</C> names the visit to the state, the same for every attempt of it: the
						saga's name, the run's id, the state, and how many states the run has entered, as in{' '}
						<C>order/order-123/Charging/2</C>. The effect's <C>ctx</C> carries the key and the
						fence as metadata, so every send and call made with it carries them too. A participant
						reads them with <C>KeyOf</C>, keeps the key with what it did, and answers a repeat with
						what it answered before:
					</p>
					<Code>{`key, fence, ok := saga.KeyOf(m.Metadata)
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
// charge, then keep key, fence and the answer`}</Code>
					<p>
						The fence grows with each engine that claims the run, so a participant that keeps the
						highest it has seen for a key can refuse what an earlier owner sent before it lost the
						run.
					</p>
				</>
			)
		},
		{
			id: 'failures',
			title: 'Failures',
			body: (
				<>
					<p>
						An effect that returns an error is run again after a backoff, with the same key, and the
						record keeps how many times it failed and why. A call that timed out may have been
						handled, so the effect returns its error and is tried again; it is never taken for a
						refusal. A refusal is an answer, and the effect fires an event for it.
					</p>
					<Table
						head={['Option of Do', 'Means']}
						rows={[
							[<C>Backoff(lo, hi)</C>, <>The wait after the first failure, doubling to <C>hi</C>. 100ms to a minute by default.</>],
							[<C>Attempts(n)</C>, 'The tries in one visit before the effect has failed for good. 0, the default, is no bound.'],
							[<C>Otherwise(ev)</C>, <>Fires <C>ev</C>, with the error's text, once the effect has failed for good.</>]
						]}
					/>
					<p>
						<C>Permanent(err)</C> marks an error that trying again cannot get past. Once an effect has
						failed for good, by a <C>Permanent</C> error or by running out of <C>Attempts</C>, its{' '}
						<C>Otherwise</C> fires and the error is kept as the run's <C>Cause</C>; one the machine
						takes by an internal transition leaves the run waiting in the state, for a signal or its
						timer. With no{' '}
						<C>Otherwise</C>, or one the machine refuses, the run is <C>Stuck</C>: kept as it is
						until <C>Resume</C> has its effect tried again, its attempts counted from none. A
						compensation that keeps failing has no automatic answer, and a run that is dropped is
						worse than one that waits for a person.
					</p>
				</>
			)
		},
		{
			id: 'signals',
			title: 'Signals and timers',
			body: (
				<>
					<p>
						A signal is an event sent to a run from outside it: a payment confirmed, an approval, a
						parcel delivered. <C>Accept</C> names the events a saga takes from outside, and how
						their payload is written to the run's data; <C>AcceptSignal</C> takes one with no
						payload. <C>After</C> gives a state a timer.
					</p>
					<Code>{`orders.
	Accept(paid, func(o *ordersv1.Order, p *paymentsv1.Paid) { o.PaymentId = p.Id }).
	AcceptSignal(cancelled).
	After(AwaitingPayment, 24*time.Hour, expired)

err := orders.Signal(ctx, eng, "order-123", paid, &paymentsv1.Paid{Id: "pay-9"})
err = orders.Notify(ctx, eng, "order-123", cancelled)`}</Code>
					<p>
						A signal is kept with the run until its machine is in a state that takes it. In each
						state the machine takes the first signal waiting that the state accepts, so of the
						signals a state accepts, the earlier is taken first; one that comes early waits for its
						state while later ones go by.
						The effect of a visit runs before any signal or timer moves the run on from it. A run
						holds 64 signals that wait (<C>MaxInbox</C>) and refuses more with{' '}
						<C>ErrInboxFull</C>; one that no state takes waits until the run ends. A run that ended
						refuses signals with <C>ErrEnded</C>. A signal whose payload does not decode, or whose
						taking panics, is dropped and logged.
					</p>
					<p>
						A timer is a time in the record: the store finds the runs whose time has come, so it
						outlasts the engine that set it, and no process waits for it. It fires on a run still in
						the same visit to its state, between attempts if the effect is failing, and it fires
						once: a machine that refuses it stays in the state with no timer.
					</p>
				</>
			)
		},
		{
			id: 'sequence',
			title: 'Sequence',
			body: (
				<>
					<p>
						The saga of the textbooks is steps done in order, each with what undoes it.{' '}
						<C>Sequence</C> builds one as a machine. A step's effect does not fire: returning{' '}
						<C>nil</C> is the step done. Here an order is reserved, charged and shipped, and a
						declined card is a <C>Permanent</C> error:
					</p>
					<Code caption="saga/example_test.go">{region(example, /A run's data is the card/, /^\t\)$/)}</Code>
					<Output>{exampleOutput}</Output>
					<p>
						A step that fails for good up to the pivot has the steps done so far undone, last first,
						itself included, since it may have happened: the declined charge is undone before the
						stock is released, and its undo does nothing, since nothing was charged. The run then
						ends in <C>StageFailed</C>, with why in its <C>Cause</C>. After the pivot the run only
						goes forward, and a step that fails for good leaves it <C>Stuck</C>. Without a pivot,
						the last step is it.
					</p>
					<p>
						A step fails for good only by a <C>Permanent</C> error or by running out of its{' '}
						<C>Attempts</C>; with neither, it is tried until it works. The run's stages are the
						steps' names, <C>{'undo <name>'}</C> while that step is undone, <C>StageDone</C> and{' '}
						<C>StageFailed</C>.
					</p>
				</>
			)
		},
		{
			id: 'engines',
			title: 'Engines and leases',
			body: (
				<>
					<p>
						<C>Start</C> runs sagas on a node from a store. Engines on several nodes that share a
						store share the runs.
					</p>
					<Table
						head={['Config', 'Means']}
						rows={[
							[<C>Store</C>, 'Keeps the runs. Required.'],
							[<C>Name</C>, <>The engine process's registered name; <C>saga</C> by default.</>],
							[<C>Poll</C>, <>How often the engine asks the store for runs that are due, and <C>Wait</C> about a run. 1s.</>],
							[<C>Lease</C>, 'How long a claim holds a run without being renewed. 30s.'],
							[<C>Concurrency</C>, 'How many runs the engine works on at once. 64.'],
							[
								<C>InspectData</C>,
								<>
									Show a run's data and its signals' payloads to a query through the{' '}
									<A to="guides/inspector/">Inspector</A>. Off: what a run carries is often a
									customer's, and a read-only Inspector is not a private one.
								</>
							]
						]}
					/>
					<p>
						The engine is a process. It claims the runs that have something to do on each tick, at
						once when a run is begun, signalled or resumed through it, and when a run it let go is
						due before the next tick. For each run it claims it spawns a process, labelled{' '}
						<C>{'saga:<name>'}</C>, which lives while the run has work and exits when it waits. A
						run at work is in the process list and the <A to="guides/inspector/">Inspector</A>; a
						run that waits is a record and no more. The engine answers for the runs of its sagas,
						which <A to="guides/grpcprocctl/">grpcprocctl saga</A> and the Sagas page of the{' '}
						<A to="guides/web/">Web UI</A> list, show and resume.
					</p>
					<p>
						Each claim is for a lease, and raises the run's epoch. While the run is worked on, the
						engine renews the lease a quarter of it after the claim and after each renewal, and tries
						a renewal that failed again a sixteenth of it later. It counts the lease by its own
						clock, from before it asked for the claim. Only the owner writes the run's state, and
						the store refuses a write with an older epoch, so an engine that was cut off, and whose lease another took,
						cannot save over it. An effect's <C>ctx</C> ends when the store says the run is
						another's, or when a quarter of the lease is left and no renewal got through, before
						another engine may have it.
					</p>
					<p>
						When the node stops, a run's <C>ctx</C> is cancelled, nothing is saved, and its lease is
						let go, so another engine claims it at its next <C>Poll</C>, without waiting the lease
						out. A claim under way as it stops has a second, in all, to answer and to let its runs
						go likewise; past that they wait out their leases. A node that dies leaves the lease to
						run out.
					</p>
				</>
			)
		},
		{
			id: 'versions',
			title: 'Two versions of a program',
			body: (
				<p>
					In a rolling deploy the old program meets the new one's runs, in states its machine lacks.
					So a saga has a version, <C>Definition.Version(v)</C>, and a run keeps the highest that
					began, signalled or worked on it. An engine claims only the runs its version reaches: the
					old program leaves alone what the new one has touched, and the new one takes up the old
					one's runs as they are. Raise the version when the machine gains a state, an event or a
					timer.
				</p>
			)
		},
		{
			id: 'stores',
			title: 'Stores',
			body: (
				<>
					<p>
						<C>Store</C> is one interface: <C>Create</C> if absent, <C>Claim</C>, <C>Save</C>{' '}
						(fenced by the epoch), <C>Renew</C>, <C>Signal</C>, <C>Resume</C>, <C>Get</C> and{' '}
						<C>List</C>. The engine keeps no state of its own: ownership is a lease in the store,
						a timer is a field of the record, and nothing is published, so the store is the only
						part that must outlast a process.
					</p>
					<p>
						<C>saga.Memory()</C> keeps the runs in memory, for tests and for sagas that need not
						outlast their program.{' '}
						<Ext href={file('saga/postgres/README.md')}>
							<C>grpcproc/saga/postgres</C>
						</Ext>{' '}
						keeps them in PostgreSQL, in a <C>*sql.DB</C> the application opens, with the driver
						it chooses: a row per run with its inbox on it, each change one statement on the row.
					</p>
					<Code>{`store, err := postgres.NewStore(db, "grpcproc_sagas")
err = store.Migrate(ctx) // or apply store.Schema() with your migrations
eng, err := saga.Start(node, saga.Config{Store: store}, orders)`}</Code>
					<p>
						Leases and due times are read on the database's clock, which every engine shares. A
						store of your own must pass{' '}
						<Ext href={file('saga/sagatest/store.go')}>
							<C>sagatest.Store</C>
						</Ext>
						, which checks what an engine relies on:
					</p>
					<Code>{`func TestStore(t *testing.T) {
	sagatest.Store(t, func(t *testing.T) saga.Store { return openEmpty(t) })
}`}</Code>
				</>
			)
		},
		{
			id: 'limits',
			title: 'Limits',
			body: (
				<ul>
					<li>
						Stores in etcd and in a leader's checkpoint, for an installation with no database, are
						open work.
					</li>
					<li>
						A participant keeps keys and fences itself: a wrapper that answers a repeated key and
						refuses a lower fence is open work.
					</li>
					<li>Steps run one at a time; a run does not do two at once.</li>
					<li>Finished runs are kept until the store drops them: there is no retention yet.</li>
					<li>
						A signal no state takes waits until the run ends, so a sender that repeats itself can
						fill the run's inbox.
					</li>
				</ul>
			)
		}
	]
};
