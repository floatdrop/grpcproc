import supervisor from '../../../examples/supervisor/main.go?raw';
import supervisorOutput from '../../../examples/supervisor/output.txt?raw';

import { Code, Output, region } from '../code.tsx';
import { Escalation, Strategies } from '../components/diagrams-supervision.tsx';
import { Drawing } from '../components/Figure.tsx';
import { A, Aside, C, Table } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const conceptsSupervision: Doc = {
	path: 'concepts/supervision/',
	title: 'Supervision',
	description:
		'How a supervisor restarts the processes under it, which ones and when, and what happens when it gives up.',
	sections: [
		{
			id: 'let-it-crash',
			title: 'Let it crash',
			body: (
				<>
					<p>
						A process that meets a state it cannot handle returns an error, and a supervisor above
						it starts a new one from a known good state: the bug is logged, and it costs one process
						its state rather than leaving one running with a corrupted map.{' '}
						<A to="concepts/actors/#supervision">The actor model</A> has the reasoning. It is a rule
						about the unexpected, not a licence to skip validation: a bad message from another
						service is expected input, answered with a refusal, as{' '}
						<A to="shop/services/">the tutorial</A> shows.
					</p>
				</>
			)
		},
		{
			id: 'supervisor',
			title: 'Supervisors',
			body: (
				<>
					<p>
						A supervisor is a process built on{' '}
						<A to="concepts/monitors-and-links/">monitors and links</A>. It has a list of children,
						each described by how to start it and when to restart it. It starts them in order,
						monitoring each from before it runs, with <C>SpawnMonitor</C>, so no exit is missed or
						misreported as <C>noproc</C>. Each child is also linked to it, with <C>LinkParent</C>, so
						a supervisor that is gone takes its children with it. Then it waits for <C>Down</C>{' '}
						messages.
					</p>
					<p>
						When one arrives, the supervisor reads the reason and the child's restart type, and
						either starts the child again, and perhaps its siblings, or counts one failure too many
						and exits itself. A supervisor holds no application state and answers no application
						calls.
					</p>
					<Code>{`sup, err := actor.Supervise(node, actor.Spec{
	Strategy: actor.OneForOne,
	Children: []actor.ChildSpec{
		actor.Child("inventory", func() *Inventory { return &Inventory{ledger: ledger} }),
		actor.Child("pricing", func() *Pricing { return &Pricing{table: table} }),
	},
}, grpcproc.WithName("shop"))`}</Code>
					<p>
						<C>actor.Child</C> takes a name and a function that builds the actor. The supervisor
						calls the function at every start, so a restart begins from a fresh struct, not from the
						one that crashed. <C>actor.ChildFunc</C> does the same for a plain process
						function, and <C>actor.ChildSupervisor</C> makes a child that is itself a supervisor.{' '}
						<A to="guides/supervisors/">Supervisors</A> has the API in full.
					</p>
				</>
			)
		},
		{
			id: 'state',
			title: 'State across restarts',
			body: (
				<>
					<p>
						A restarted actor is a new struct, and whatever the old one held in its fields is gone.
						What must outlive a crash lives outside the actor, in something the actor loads when it
						starts and writes to as it goes. In the example that is a <C>Ledger</C>, a stand-in for
						a database:
					</p>
					<Code caption="examples/supervisor/main.go">
						{region(supervisor, /\/\/ Ledger stands for a database/, /^func \(i \*Inventory\) Init/).replace(/\n[^\n]*$/, '\n') +
							region(supervisor, /^func \(i \*Inventory\) Init/, /^}/)}
					</Code>
					<p>
						The actor's <C>Init</C> runs before its first message and loads the levels back; the
						supervisor built the actor around the same <C>Ledger</C> it built the last one around.
						So an actor may hold what is cheap to rebuild in <C>Init</C>, a cache, a cursor, a
						connection, and never the one copy of an order.
					</p>
				</>
			)
		},
		{
			id: 'strategies',
			title: 'Strategies',
			body: (
				<>
					<p>
						When a child exits, the supervisor's <C>Strategy</C> says who else is affected. Children
						that share state or a protocol with the one that failed cannot be trusted to carry on with
						a fresh replacement; children that only share a supervisor can.
					</p>
					<Drawing caption="the three strategies, when the middle child exits">
						<Strategies />
					</Drawing>
					<Table
						head={['Strategy', 'Restarts']}
						rows={[
							[<C>OneForOne</C>, 'Only the child that exited. The default, and right for children that do not depend on each other: the services of a node.'],
							[<C>OneForAll</C>, 'Every child. The others are stopped, in reverse order, then all are started again, in order. For children that share fate: a connection and the session that speaks over it.'],
							[<C>RestForOne</C>, 'The child and every child started after it. For children that depend on the ones before them: a store, then the workers that use it.']
						]}
					/>
					<p>
						Only children that were running come back with their group. A transient child that had
						finished stays finished.
					</p>
				</>
			)
		},
		{
			id: 'restart-types',
			title: 'Restart types',
			body: (
				<>
					<p>
						Each child has a <C>Restart</C> type, which says whether an exit calls for a restart at
						all. The question is whether the exit was abnormal: <C>normal</C> and <C>shutdown</C> are
						not, and every other reason is, an error's text, a panic, <C>noconnection</C>,{' '}
						<C>max restarts</C>.
					</p>
					<Table
						head={['Restart', 'Started again']}
						rows={[
							[<C>Permanent</C>, 'Always, however it exited. The default: a service that should be running as long as the program is.'],
							[<C>Transient</C>, 'After an abnormal exit only. A job that should run to completion: a normal end is success, a crash is not.'],
							[<C>Temporary</C>, 'Never. A one-off; the supervisor forgets it once it ends.']
						]}
					/>
					<Code>{`actor.Child("importer", newImporter).WithRestart(actor.Transient)
actor.ChildFunc("", oneOff).WithRestart(actor.Temporary) // anonymous: no name`}</Code>
					<p>
						A child that is stopped by its supervisor, with{' '}
						<C>StopChild</C> or when the supervisor ends, is not restarted whatever its type: the
						supervisor asked for that exit.
					</p>
				</>
			)
		},
		{
			id: 'escalation',
			title: 'Restart intensity and escalation',
			body: (
				<>
					<p>
						A child that keeps failing has a deterministic bug, or depends on something that is
						down, and a fresh struct fixes neither. So a supervisor keeps count. More than{' '}
						<C>MaxRestarts</C> restarts within <C>Within</C>, 3 in 5 seconds by default, and it
						stops trying: it stops its other children and exits with reason <C>max restarts</C>.
					</p>
					<p>
						That reason is abnormal. A supervisor is usually a child of another supervisor, and to
						that one, <C>max restarts</C> is a <C>Down</C> like any other: it
						restarts the child supervisor, which starts its whole subtree again from scratch. If
						that fails too, the failure moves up one more level. At the top it reaches the root,
						and a root that gives up means the program has given up; the tutorial's platform stops
						the program then, for whatever runs it to start it again.
					</p>
					<Drawing caption="a failure moving up the tree">
						<Escalation />
					</Drawing>
					<p>
						Each level restarts what it can and passes on what it cannot, so a restart widens as
						the failure persists: one actor, then one service, then the program.
					</p>
				</>
			)
		},
		{
			id: 'stopping',
			title: 'Stopping children',
			body: (
				<>
					<p>
						A supervisor that ends, because it was told to or because it gave up, stops its children
						first, in reverse order of their start, so a child never outlives one it was started
						after and might depend on. Each child gets an <C>Exit</C> with reason <C>shutdown</C>{' '}
						and a time to go: <C>Spec.Shutdown</C>, 5 seconds by default, or its own{' '}
						<C>WithShutdown</C>. A child that is itself a supervisor gets as long as it takes,{' '}
						<C>actor.Infinity</C>, since it has a subtree of its own to stop in order.
					</p>
					<p>
						A goroutine cannot be killed, so a child that ignores its exit request and outlives its{' '}
						<C>Shutdown</C> is logged and left behind. A named child keeps its name until it exits,
						and nothing is started under a name an old process still holds: the supervisor monitors
						the stubborn child and starts its replacement, and the children after it, only once it
						is gone, handling its mailbox meanwhile. Its own end waits the same way. A child that
						never exits therefore holds its restart for good.
					</p>
					<p>
						So a process function must return when <C>Receive</C> returns an error, and a handler
						must not block forever.
					</p>
				</>
			)
		},
		{
			id: 'names',
			title: 'Names across restarts',
			body: (
				<>
					<p>
						A child with a name is registered under it on the node, at every start. The processes
						that call it hold an address made from the node's name and that name, and the address
						reaches whichever process runs the child right now. A caller does not learn that the
						child was restarted, other than by a call that failed while it was down, which fails with{' '}
						<C>ErrNoProc</C> and can be tried again.
					</p>
					<p>
						The example has one child, restarted alone when it exits, up to 3 times a minute:
					</p>
					<Code caption="examples/supervisor/main.go">{region(supervisor, /\/\/ tree is the supervision tree/, /^}/)}</Code>
					<p>
						The program restocks, reserves, then sends a restock of zero, which the actor treats as a
						bug and exits on. The supervisor starts a new actor under the same name; the next call by
						that name reaches it, with levels loaded back from the ledger:
					</p>
					<Output>{supervisorOutput}</Output>
				</>
			)
		},
		{
			id: 'significant',
			title: 'Significant children',
			body: (
				<>
					<p>
						A job made of several processes is done when its processes are done, and the supervisor
						over them should end then. <C>WithSignificant(true)</C> marks the children whose end counts, and{' '}
						<C>Spec.AutoShutdown</C> says how many: <C>AnySignificant</C> ends the supervisor when the
						first of them ends by itself, <C>AllSignificant</C> when the last does. A child the
						supervisor stopped does not count, and a permanent child cannot be significant, since it
						is never allowed to end.
					</p>
					<p>
						The supervisor exits with <C>shutdown</C>, which is not abnormal: its own supervisor
						restarts it only if it is permanent there, which a job's supervisor is not.
					</p>
				</>
			)
		},
		{
			id: 'design',
			title: 'Designing a tree',
			body: (
				<>
					<ul>
						<li>
							<strong>One supervisor per service</strong>, under a root with <C>OneForOne</C>. A
							service that gives up is restarted alone, and the others keep serving. That is the shape{' '}
							<A to="shop/platform/">the tutorial's platform</A> builds from the services' modules.
						</li>
						<li>
							<strong>Group by fate.</strong> Children that cannot work with a fresh replacement of
							one another go under a supervisor of their own, with <C>OneForAll</C> or{' '}
							<C>RestForOne</C>. A supervisor is cheap; a wrong strategy over unrelated children
							restarts what did not fail.
						</li>
						<li>
							<strong>Pools are anonymous children.</strong> A <C>OneForOne</C> supervisor with no
							children in its spec, and workers added with <C>actor.StartChild</C> as they are needed,
							each with an empty name. Once one ends for good the supervisor forgets it, so the pool
							does not grow with its history.
						</li>
						<li>
							<strong>Keep state out of actors and logic out of supervisors.</strong> The first makes
							restarts safe; the second keeps supervisors from failing themselves.
						</li>
						<li>
							<strong>Set the intensity for the failure you expect.</strong> A dependency that comes
							back in a minute wants a wider window and more restarts than the default 3 in 5 seconds;
							a child whose crash means a bug wants to escalate fast.
						</li>
					</ul>
					<Aside title="Coming from Erlang/OTP">
						<p>
							The strategies, restart types, intensity and shutdown are OTP's, with the same names
							and defaults. Three things differ. There is no <C>simple_one_for_one</C>: a pool is a{' '}
							<C>OneForOne</C> supervisor plus <C>StartChild</C>. A child that outlives its shutdown is
							not killed, since a goroutine cannot be, but waited for before anything starts under its
							name. And links are one way, so a supervisor's children are linked to it, not it to
							them; it learns of their exits through the monitors it holds.
						</p>
					</Aside>
				</>
			)
		}
	]
};
