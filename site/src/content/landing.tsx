import { Drawing, Screenshot } from '../components/Figure.tsx';
import { CallAnywhere, CrashRestart, NodeLost } from '../components/diagrams-overview.tsx';
import { A, C, Cards, Ext } from '../components/prose.tsx';
import { PKG_DOC, file } from '../config.ts';
import type { Doc } from './types.ts';

/** The landing page's opening. */
export const hero = {
	title: 'Erlang-style processes for Go, on the gRPC server you already run.',
	lead: (
		<>
			A Go library that gives goroutines typed mailboxes, addresses that work across nodes,
			monitors and supervision trees.
		</>
	),
	install: 'go get github.com/floatdrop/grpcproc',
	actions: [
		{ to: 'start/', title: 'Quick start' },
		{ to: 'concepts/actors/', title: 'Concepts' }
	]
};

export const landing: Doc = {
	path: '',
	title: 'Overview',
	description:
		'Erlang-style processes for Go: typed mailboxes, calls, monitors and supervision trees that work the same within a node and across nodes.',
	sections: [
		{
			id: 'calls',
			title: 'One API, local or remote',
			body: (
				<>
					<Drawing caption="a call within a node, and the same call across nodes">
						<CallAnywhere />
					</Drawing>
					<p>
						A process is a goroutine with a mailbox and an address. <C>Send</C> and <C>Call</C>{' '}
						take the address and are written the same way wherever the process runs: within a
						program the message is passed as a pointer, and between nodes it travels a gRPC stream,
						in order. The address carries the message type, so the compiler checks every send.
					</p>
					<p>
						<A to="concepts/processes/">Processes and messages</A> ·{' '}
						<A to="concepts/addressing/">Addresses</A>
					</p>
				</>
			)
		},
		{
			id: 'failure',
			title: 'Failure as a message',
			body: (
				<>
					<Drawing caption="a crash, and the restart">
						<CrashRestart />
					</Drawing>
					<p>
						A process that returns an error or panics exits with a reason. Every process that
						monitors it receives a <C>Down</C> carrying that reason, after the last message the
						process sent. A supervisor is a process that monitors its children and restarts them
						by a strategy: only the one that exited, all of them, or it and those started after
						it.
					</p>
					<p>
						<A to="concepts/monitors-and-links/">Monitors and links</A> ·{' '}
						<A to="concepts/supervision/">Supervision</A>
					</p>
				</>
			)
		},
		{
			id: 'node-down',
			title: 'A lost node',
			body: (
				<>
					<Drawing caption="a node that cannot be reached">
						<NodeLost />
					</Drawing>
					<p>
						When a node cannot be reached, monitors of its processes fire with reason{' '}
						<C>noconnection</C>, and calls waiting on it fail with <C>ErrNoConnection</C>. A node
						that restarts is a new incarnation: an address from before the restart reaches no
						process, never a different one.
					</p>
					<p>
						<A to="concepts/nodes/">Nodes and links</A> ·{' '}
						<A to="reference/errors/">Errors and exit reasons</A>
					</p>
				</>
			)
		},
		{
			id: 'inspect',
			title: 'Inspection',
			body: (
				<>
					<Screenshot
						caption="grpcprocctl web: a process whose mailbox grows"
						name="grpcprocctl-web-processes"
						alt="The Processes view of the Web UI sorted by mailbox, with one process open beside it: its mailbox depth charted, its message rates, and what it publishes about itself."
					/>
					<p>
						Every process keeps counters, its mailbox depth and its state, and can publish what it
						holds. The Inspector serves that for the whole cluster from one endpoint, to a{' '}
						<A to="guides/grpcprocctl/">command line</A>, a <A to="guides/web/">Web UI</A> and{' '}
						<A to="guides/mcp/">AI agents over MCP</A>. Metrics and traces go to{' '}
						<A to="guides/observability/#otel">OpenTelemetry</A>.
					</p>
				</>
			)
		},
		{
			id: 'requirements',
			title: 'Requirements',
			body: (
				<ul>
					<li>Go 1.27 or later.</li>
					<li>
						A <C>*grpc.Server</C> of yours. grpcproc registers one service on it and opens no
						listener; credentials, discovery and logging stay the application's.
					</li>
					<li>Messages are protobuf messages.</li>
					<li>
						The core depends on gRPC, protobuf and{' '}
						<Ext href="https://github.com/floatdrop/fsm">fsm</Ext>, which has no dependencies. etcd
						and OpenTelemetry support are separate modules.
					</li>
				</ul>
			)
		},
		{
			id: 'map',
			title: 'Documentation',
			body: (
				<>
					<Cards
						items={[
							{
								to: 'start/',
								title: 'Quick start',
								text: 'Two nodes, a process on one, a call and a monitor from the other.'
							},
							{
								to: 'concepts/actors/',
								title: 'Concepts',
								text: 'Processes, addresses, monitors, links, supervision and nodes, for a reader with no Erlang.'
							},
							{
								to: 'guides/actors/',
								title: 'Build',
								text: 'Actors, supervisors, pub/sub, cron jobs, leader election, and testing a cluster in go test.'
							},
							{
								to: 'guides/operations/',
								title: 'Run',
								text: 'Configuration, etcd membership, and what a production deployment sets.'
							},
							{
								to: 'guides/observability/',
								title: 'Inspect',
								text: 'Snapshots and events, the Inspector, the command line, the Web UI, MCP, OpenTelemetry.'
							},
							{
								to: 'shop/',
								title: 'Tutorial',
								text: 'A shop of three services, run as one program or as three nodes from the same code.'
							},
							{
								to: PKG_DOC,
								title: 'API reference',
								text: 'Every type and function, on pkg.go.dev.'
							},
							{
								to: file('docs/DESIGN.md'),
								title: 'Design notes',
								text: 'The wire protocol, the reasons behind each choice, and what was rejected.'
							}
						]}
					/>
					<p>
						<A to="reference/performance/">Performance</A> compares grpcproc with GoAkt, Hollywood,
						Proto.Actor and Ergo.
					</p>
				</>
			)
		}
	]
};
