import config from '../../../examples/guide/internal/platform/config.go?raw';
import platform from '../../../examples/guide/internal/platform/platform.go?raw';
import root from '../../../examples/guide/internal/platform/root.go?raw';
import run from '../../../examples/guide/internal/platform/run.go?raw';
import modules from '../../../examples/guide/testdata/modules.txt?raw';

import { Code, region } from '../code.tsx';
import { Drawing } from '../components/Figure.tsx';
import { Lifecycle, Trust } from '../components/diagrams.tsx';
import { A, C } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const tutorialPlatform: Doc = {
	path: 'tutorial/platform/',
	title: 'Platform',
	description:
		'What every program of the runtime runs: configuration, a gRPC server with the node and the Inspector on it, what each peer may ask, and the root supervisor.',
	sections: [
		{
			id: 'config',
			title: 'Configuration',
			body: (
				<>
					<p>
						Every program runs the same platform. It starts from the configuration, the one thing
						the programs of a deployment differ in: which node this is, where it listens, where the
						other nodes are, which nodes run each service it does not, and what its peers may ask of
						it. <C>Where</C> and <C>All</C> are what the conversations and the front ask.
					</p>
					<Code caption="internal/platform/config.go">{config}</Code>
				</>
			)
		},
		{
			id: 'platform',
			title: 'The node and the server',
			body: (
				<>
					<p>
						Then it builds a gRPC server, the node registered on it, the Inspector beside the node,
						and the root supervisor. <C>grpcprocctl</C> talks to the Inspector. The node is a
						dependency like any other: a constructor that needs it takes it as a parameter.
					</p>
					<Code caption="internal/platform/platform.go">{platform}</Code>
				</>
			)
		},
		{
			id: 'admit',
			title: 'What a peer may ask',
			body: (
				<>
					<p>
						A node decides once per link what the peer on it may do: <C>Config.Admit</C> refuses
						the link, or returns the <A to="guides/configuration/">policy</A> that judges every
						send, call and monitor that arrives on it. The platform makes it from the
						configuration. A peer named untrusted gets <C>grpcproc.Export()</C> with no names,
						which allows nothing; any other peer may reach the processes this node exports, or all
						of them if it names none.
					</p>
					<Code caption="internal/platform/platform.go">{region(platform, /^\/\/ admit says what each peer may ask/, /^}/)}</Code>
					<Drawing caption="a peer that answers, and may ask nothing">
						<Trust />
					</Drawing>
					<p>
						Answers are never judged: a reply, or the <C>Down</C> of a monitor the node placed
						itself, always comes back. So the gateway, which does not trust the sandbox, still
						calls its tools and gets their results, while a sandbox that tries to call a
						conversation is told <C>ErrNoProc</C>, as for a process that does not exist. The GPU
						nodes name the sandbox untrusted too: a scheduler publishes to whatever topic a
						request names, so it takes requests from the gateway only.
					</p>
					<p>
						All of this rests on a peer being who it says it is, and here nothing checks: the
						links are plaintext, to keep the tutorial short, and a sandbox could call itself{' '}
						<C>gpu-1</C>. A real deployment uses mutual TLS and admits a peer only under the node
						name its certificate carries, as <C>grpcproc.AdmitTLS</C> does; then what a tool that
						takes over the sandbox's node can do through grpcproc is answer the calls made to it.
					</p>
				</>
			)
		},
		{
			id: 'lifecycle',
			title: 'Start and stop order',
			body: (
				<>
					<p>
						The container builds eager services in registration order and starts them in build
						order, so the node and the Inspector are registered on the server before it serves.
						Stopping runs the other way: the root stops the trees first, then the node closes its
						links, and the server goes last.
					</p>
					<Drawing caption="start and stop">
						<Lifecycle />
					</Drawing>
					<p>
						Keepalive, in the dial options and on the server, is what turns a silent peer into a
						broken link, and so into <C>Down</C>s and failed calls. The
						Inspector is read-only, since it shares the node's port and nothing here checks who
						calls it.
					</p>
				</>
			)
		},
		{
			id: 'root',
			title: 'The root supervisor',
			body: (
				<>
					<p>
						The root is built from the group the service modules add their trees to: one supervisor
						per node, <C>actor.OneForOne</C>, so a service whose supervisor gives up is restarted
						without the others. It starts the node once the services run, which with a registry is
						when peers learn of the node. A worker watches it: a root that exits on its own, past
						its restart limit, means the services have given up, and the worker's error stops the
						program for whatever runs it to start again.
					</p>
					<Code caption="internal/platform/root.go">{root}</Code>
				</>
			)
		},
		{
			id: 'run',
			title: 'Run and Compose',
			body: (
				<>
					<p>
						<C>Run</C> is every entry point's <C>main</C>: it loads the configuration, composes the
						platform with the services, validates the graph before building anything, and runs until
						a signal arrives or a worker fails. <C>Compose</C> is the part the tests use.
					</p>
					<Code caption="internal/platform/run.go">{run}</Code>
					<p>
						Each module declares what it provides and what its constructors need, so the container
						can report a composition without building it. This is <C>cmd/local</C>'s, as the tests
						compose it; another test checks that each <C>main</C> passes the modules its test
						composes.
					</p>
					<Code lang="txt" caption="app.Modules(), as the tests compose cmd/local">
						{modules}
					</Code>
				</>
			)
		}
	]
};
