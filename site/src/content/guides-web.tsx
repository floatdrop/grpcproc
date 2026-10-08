import { Code } from '../code.tsx';
import { Screenshot } from '../components/Figure.tsx';
import { A, C, Table } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const guidesWeb: Doc = {
	path: 'guides/web/',
	title: 'Web UI',
	description: 'A live view of the cluster in a browser: nodes and links, processes, supervision trees and events.',
	sections: [
		{
			id: 'start',
			title: 'Starting it',
			body: (
				<>
					<Code lang="sh">{`go install github.com/floatdrop/grpcproc/tools/cmd/grpcprocctl@latest
grpcprocctl --plaintext --addr 10.0.0.5:9000 web   # http://localhost:9911`}</Code>
					<p>
						<C>grpcprocctl web</C> serves a page that reads the cluster through one node's{' '}
						<A to="guides/inspector/">Inspector</A> and refreshes every second. The node must serve
						the Inspector; the connection flags are those of the{' '}
						<A to="guides/grpcprocctl/#connecting">command line</A>.
					</p>
					<Screenshot
						caption="grpcprocctl web: the cluster"
						name="grpcprocctl-web-cluster"
						alt="The Cluster view: three nodes a, b and c, each with its process count, joined by arrows labelled with messages per second, above a table of the nodes with their uptime, processes, spawns and exits per second, dead letters and peers."
					/>
				</>
			)
		},
		{
			id: 'views',
			title: 'Views',
			body: (
				<>
					<Table
						head={['View', 'What it shows']}
						rows={[
							['Cluster', 'Every node, and a map of the links between them with messages per second on each, dead letters and queues.'],
							['Node', "One node's counters and links, charted over the last minute."],
							[
								'Processes',
								<>
									The table <C>grpcprocctl ps</C> prints, with messages in and out per second and the
									share of each second each process was busy. The scope (name, label, state, mailbox) is
									sent to the node; search and order stay in the page.
								</>
							],
							[
								'Global names',
								<>
									Who holds each <A to="concepts/addressing/#global">global name</A>.
								</>
							],
							['Supervision', 'Each process under the one that started it, coloured by state, mailbox or activity.'],
							['Events', 'Spawns, exits with their reasons, links and dead letters, streamed as they happen.'],
							[
								'Cron, Elections',
								<>
									Every <A to="guides/cron/">cron job</A> and{' '}
									<A to="guides/leader/">leader election</A> reachable from the node.
								</>
							],
							[
								'Sagas',
								<>
									The <A to="guides/sagas/">saga</A> engines reachable from the node, and a saga's
									runs a page at a time, by status. A run opens beside them: why it is stuck, its
									timers and owner, its data when the engine shows it, and the signals waiting.
								</>
							]
						]}
					/>
					<p>The address holds what is open, so a view can be shared as a link.</p>
				</>
			)
		},
		{
			id: 'process',
			title: 'A process',
			body: (
				<>
					<p>
						Clicking a process opens it beside any view: its mailbox, messages per second and how
						busy it is charted, what it <A to="guides/observability/#inspect">says about itself</A>, and what
						it started. Asking a process what it says takes it a turn, so the page asks when told
						to, or on an interval you choose. Here <C>ledger</C> handles about 20 messages a second
						while more arrive, and its mailbox climbs:
					</p>
					<Screenshot
						caption="grpcprocctl web: a backlog"
						name="grpcprocctl-web-processes"
						alt="The Processes view sorted by mailbox, ledger first with a deep mailbox, and ledger open beside it: running, receiving about 20 a second, its oldest message waiting tens of seconds, a chart of its mailbox climbing, and what it publishes about itself."
					/>
				</>
			)
		},
		{
			id: 'writes',
			title: 'Changing things',
			body: (
				<>
					<p>
						The page is read-only unless started with <C>--allow-writes</C>. With it, the page can
						ask a process to exit, set its log level, enable, disable and remove cron jobs, move
						or cordon a leader, and resume a stuck saga run. Each change is confirmed first. An Inspector built with{' '}
						<C>inspect.ReadOnly()</C> refuses them whatever the page allows.
					</p>
				</>
			)
		},
		{
			id: 'exposure',
			title: 'Where it listens',
			body: (
				<>
					<p>
						It listens on <C>localhost:9911</C> unless <C>--listen</C> says otherwise. On a loopback
						address it answers only requests whose <C>Host</C> is localhost or a loopback address,
						so a page on another
						site cannot read it through your browser. On any other address, put it behind something
						that authenticates.
					</p>
					<p>
						The API behind the page returns the objects <C>grpcprocctl --json</C> prints:{' '}
						<C>/api/info</C>, <C>/api/nodes</C>, <C>/api/node</C>, <C>/api/processes</C>,{' '}
						<C>/api/process</C>, <C>/api/names</C>, <C>/api/crons</C>, <C>/api/elections</C>,{' '}
						<C>/api/sagas</C>, <C>/api/saga/runs</C>, <C>/api/saga/run</C>, and{' '}
						<C>/api/events</C> as server-sent events.
					</p>
				</>
			)
		}
	]
};
