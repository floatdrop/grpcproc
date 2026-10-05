import { Code } from '../code.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesInspector: Doc = {
	path: 'guides/inspector/',
	title: 'Inspector',
	description:
		'A gRPC service on each node that serves its processes, links and events, and forwards requests to the other nodes.',
	lead: (
		<p>
			The Inspector serves the Go API that <A to="guides/observability/">Observability</A>{' '}
			describes over gRPC, on the node's own server, and forwards to every other node's, so one
			endpoint reaches the whole cluster.
		</p>
	),
	sections: [
		{
			id: 'serving',
			title: 'Serving',
			body: (
				<>
					<p>
						<C>grpcproc/inspect</C> is part of the core module. It registers a second service,{' '}
						<C>grpcproc.inspect.v1.Inspector</C>, next to the node's own:
					</p>
					<Code>{`node.Register(grpcServer)
insp := inspect.New(node) // reaches other nodes' Inspectors as the node reaches the nodes
insp.Register(grpcServer)
defer insp.Close()`}</Code>
					<p>
						Every request names a node. One that is not this node is forwarded to that node's
						Inspector, which the server dials as the node dials its peers, <C>node.Dial</C>, through
						the node's own resolver and dial options: one connection per peer, closed by{' '}
						<C>Close</C>. <C>WithResolver</C> takes another resolver or other dial options, for
						Inspectors served elsewhere than on the port the nodes link through, and{' '}
						<C>WithPeers</C> any other way of reaching a peer's Inspector; <C>WithPeers(nil)</C>{' '}
						keeps a server to its own node. A process targeted by PID routes to the PID's node when
						the request names none.
					</p>
					<p>
						<C>inspect.ReadOnly()</C> refuses the four writes: <C>Send</C>, <C>Call</C>,{' '}
						<C>Exit</C> and <C>SetLogLevel</C>. It allows <C>Query</C>, which only a process
						spawned with <C>grpcproc.WithQuery</C> answers, and which changes nothing.{' '}
						<C>inspect.NoQueries()</C> refuses it too. Anything finer is the job of the interceptors
						and transport credentials that guard your other services. The tutorial's platform serves
						the Inspector read-only because it shares the node's port and nothing there
						authenticates.
					</p>
					<p>
						Read-only is not private. Whoever reaches the Inspector sees every node's processes,
						their names and what they publish, and through <C>Query</C> what a saga engine reads from
						its store: its runs, their state and errors, and with the engine's{' '}
						<C>Config.InspectData</C> their data. A node's <C>Admit</C> does not cover it, since it
						is another service on the port, and one Inspector forwards to the others. Put it behind
						authentication wherever the network is not trusted, and limit its requests with an
						interceptor: a query reads from the store.
					</p>
				</>
			)
		},
		{
			id: 'service',
			title: 'Methods',
			body: (
				<>
					<Table
						head={['Method', 'What it answers']}
						rows={[
							[<C>GetNode</C>, <>The node's <C>NodeInfo</C>: counters, dead letters, and its links.</>],
							[
								<C>ListProcesses</C>,
								'Every process of a node, filtered by name, label, state or minimum mailbox depth.'
							],
							[
								<C>GetProcess</C>,
								<>
									One process's snapshot, and with <C>inspect: true</C> what it publishes through{' '}
									<C>WithInspect</C>. A process too busy to answer still gets its snapshot, with{' '}
									<C>inspect_error</C> saying so.
								</>
							],
							[<C>SetLogLevel</C>, "One process's log threshold."],
							[<C>Send</C>, <>A message, as an <C>Any</C>, from a tool.</>],
							[
								<C>Call</C>,
								<>
									A call, as <C>Node.CallTo</C> makes it: the answer as an <C>Any</C>, the request's
									deadline as the call's, and an answer that is an error as <C>Unknown</C> with its
									text.
								</>
							],
							[
								<C>Query</C>,
								<>
									A question to a process spawned with <C>WithQuery</C>, as <C>Node.Query</C> asks
									it: its function answers on the Inspector's goroutine, without the mailbox. A
									process spawned without one is <C>Unimplemented</C>. A saga engine answers queries
									about its runs.
								</>
							],
							[<C>Exit</C>, <>A request to exit; the reason defaults to <C>killed</C>.</>],
							[<C>Watch</C>, <><C>Node.Subscribe</C> over the wire: spawns, exits, links, dead letters.</>]
						]}
					/>
					<p>
						A watch holds a buffer of up to 4096 events, about 1.7 MB, and the client picks the
						size below that. The server allocates it, so limit how many streams a client may open,
						with <C>grpc.MaxConcurrentStreams</C> or an interceptor.
					</p>
					<p>It is a plain gRPC service, so <C>grpcurl</C> works on it too.</p>
				</>
			)
		},
		{
			id: 'clients',
			title: 'Clients',
			body: (
				<>
					<ul>
						<li>
							<A to="guides/grpcprocctl/">Command line</A>: <C>grpcprocctl</C>, in a terminal.
						</li>
						<li>
							<A to="guides/web/">Web UI</A>: <C>grpcprocctl web</C>, the cluster live in a browser.
						</li>
						<li>
							<A to="guides/mcp/">AI agents</A>: <C>grpcprocctl mcp</C>, the same questions as MCP tools.
						</li>
					</ul>
					<p>
						All three live in <Ext href={file('tools/README.md')}>grpcproc/tools</Ext>, a separate
						module.
					</p>
				</>
			)
		}
	]
};
