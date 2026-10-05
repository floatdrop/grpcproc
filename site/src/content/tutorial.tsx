import conversationsAddress from '../../../examples/guide/proto/conversations/v1/address.go?raw';
import conversationsProto from '../../../examples/guide/proto/conversations/v1/conversations.proto?raw';
import modelsProto from '../../../examples/guide/proto/models/v1/models.proto?raw';
import chat from '../../../examples/guide/testdata/chat.txt?raw';

import { Code, region } from '../code.tsx';
import { Adaptive, Drawing } from '../components/Figure.tsx';
import { Layout } from '../components/diagrams.tsx';
import { AgentRuntime } from '../components/diagrams-overview.tsx';
import { A, C, Ext } from '../components/prose.tsx';
import { REPO } from '../config.ts';
import type { Doc } from './types.ts';

/** The annotated directory listing of examples/guide. */
const tree = [
	{ path: 'cmd/local/', comment: 'one program: every service on one node' },
	{ path: 'cmd/gateway/', comment: 'or several: the web front and the conversations,' },
	{ path: 'cmd/gpu/', comment: 'the model, on as many nodes as there are GPUs,' },
	{ path: 'cmd/sandbox/', comment: 'and the tools' },
	{ path: 'internal/platform/', comment: 'what every program runs: config, node, root supervisor' },
	{ path: 'internal/conversations/', comment: 'a process per conversation' },
	{ path: 'internal/models/', comment: 'the scheduler, and a process per generation' },
	{ path: 'internal/tools/', comment: 'the runner, and a process per tool call' },
	{ path: 'internal/web/', comment: 'the HTTP front and the chat page' },
	{ path: 'proto/<service>/v1/', comment: "a service's contract: messages, names, an address" },
	{ path: '*_test.go, testdata/', comment: 'the tests, and the output this tutorial shows' }
];

const pad = Math.max(...tree.map((row) => row.path.length)) + 2;
const treeText = tree.map((row) => `${row.path.padEnd(pad)}${row.comment}`).join('\n');

export const tutorial: Doc = {
	path: 'tutorial/',
	title: 'An agent runtime',
	description:
		'A tutorial that builds one application: a runtime for AI agents, with a process per conversation, the model on GPU nodes, tools on a sandbox node, and tokens streamed to the browser.',
	sections: [
		{
			id: 'build',
			title: 'The application',
			body: (
				<>
					<p>
						The runtime holds conversations with an assistant. A user says something over HTTP; a
						model answers a token at a time, and the browser shows each as it comes; when the model
						asks for a tool, the tool is run and the model goes on with what it returned.
					</p>
					<Adaptive caption="one turn, across the four nodes" wide={<AgentRuntime />} narrow={<AgentRuntime compact />} />
					<ul>
						<li>
							Every conversation is a process, started when it is first spoken to and ended when
							it has been idle.
						</li>
						<li>
							The model runs on GPU nodes. Each takes as many generations as it has slots, and a
							generation is a process that publishes its tokens.
						</li>
						<li>
							Tools run on a sandbox node, a process per call, under the caller's deadline. The
							sandbox answers and may ask nothing.
						</li>
						<li>
							A GPU node lost in the middle of an answer costs that answer's first try: another
							node starts it over.
						</li>
					</ul>
					<p>
						There is no model here. A script stands in for it, behind the interface a client of an
						inference server would implement, so that the tests know what it says. This is a
						conversation with it, as whoever follows the conversation is sent it; one of the tools
						crashes:
					</p>
					<Code lang="txt" caption="testdata/chat.txt">
						{chat}
					</Code>
					<p>
						The services are built and wired by{' '}
						<Ext href="https://github.com/yandex/di">golang.yandex/di</Ext>, a dependency-injection
						container. The same code runs as one program, for development, or as a program per
						node, as deployed. Every file shown is from{' '}
						<Ext href={`${REPO}/tree/main/examples/guide`}>
							<C>examples/guide</C>
						</Ext>{' '}
						in the repository, and every output is pinned by its tests.
					</p>
				</>
			)
		},
		{
			id: 'shape',
			title: 'Package layout',
			body: (
				<>
					<p>
						The code comes in four kinds of package. A <em>service</em> exports one function,{' '}
						<C>Module</C>, which registers with the container what the service needs and, if it runs
						processes, their supervision tree. A <em>contract</em> is the messages a service's
						process accepts and the name it is registered under; it is all another service may
						import. The <em>platform</em> is what every program runs. An <em>entry point</em> is a{' '}
						<C>main</C> that picks the services.
					</p>
					<Code lang="txt" caption="examples/guide">
						{treeText}
					</Code>
					<p>
						No service knows where the others run. A conversation reaches the model through the
						models' contract and the node names the placement gives, which is the part of the
						configuration that says which nodes run each service. The runtime as one program and as
						four differ in two things only: which services each entry point composes, and what each
						program is told.
					</p>
					<Drawing caption="the code, not the nodes: four kinds of package, and what imports the contracts">
						<Layout />
					</Drawing>
				</>
			)
		},
		{
			id: 'contracts',
			title: 'Contracts',
			body: (
				<>
					<p>
						A contract is a protobuf package. A mailbox holds one message type, so a conversation
						takes a <C>Request</C> whose oneof carries each operation. What it publishes is part of
						its contract too: an <C>Event</C> is what the user said, a token, a tool being called,
						what the tool returned, or the end of a turn.
					</p>
					<Code lang="proto" caption="proto/conversations/v1/conversations.proto">
						{conversationsProto}
					</Code>
					<p>
						The package exports the address as a type of its own. It embeds{' '}
						<C>Addr[*Request]</C>, so the compiler checks every send to it, wherever the process
						runs, and its methods are the protocol: the <C>Request</C> around each operation is
						wrapped here, once. Each takes its sender, a node or a process, as a{' '}
						<C>grpcproc.Caller</C>. <A to="tutorial/conversations/">The next page</A> has what{' '}
						<C>open</C> and <C>Follow</C> do.
					</p>
					<Code caption="proto/conversations/v1/address.go">
						{`${region(conversationsAddress, /^\/\/ ConversationAddr addresses/, /^}/)}

${region(conversationsAddress, /^\/\/ Conversation addresses the conversation/, /^}/)}

${region(conversationsAddress, /^\/\/ Say tells the conversation/, /^}/)}`}
					</Code>
					<p>
						A refusal is part of an answer. A GPU node with no slot free answers <C>busy</C>, and a
						tool that fails answers with why, so an error means one thing: no answer came. The
						request to generate names the topic its tokens go to, which is how a process on a GPU
						node streams to a conversation it knows nothing else about.
					</p>
					<Code lang="proto" caption="proto/models/v1/models.proto">
						{region(modelsProto, /^\/\/ Generate asks for the next thing/, /^}/) + '\n\n' + region(modelsProto, /^\/\/ Generated is the scheduler's answer/, /^}/)}
					</Code>
				</>
			)
		}
	]
};
