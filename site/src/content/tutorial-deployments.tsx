import gateway from '../../../examples/guide/cmd/gateway/main.go?raw';
import gpu from '../../../examples/guide/cmd/gpu/main.go?raw';
import local from '../../../examples/guide/cmd/local/main.go?raw';
import sandbox from '../../../examples/guide/cmd/sandbox/main.go?raw';
import clusterTree from '../../../examples/guide/testdata/cluster.txt?raw';
import localTree from '../../../examples/guide/testdata/local.txt?raw';

import { Code } from '../code.tsx';
import { Drawing } from '../components/Figure.tsx';
import { ClusterPicture, LocalPicture } from '../components/diagrams.tsx';
import { A, C, Ext } from '../components/prose.tsx';
import { REPO } from '../config.ts';
import type { Doc } from './types.ts';

const localShell = `git clone https://github.com/floatdrop/grpcproc && cd grpcproc/examples
go build -o bin/ ./guide/cmd/...
bin/local &
sleep 1
curl -N localhost:8080/conversations/1/events &     # follow the conversation
curl -d 'what is 6 * 7?' localhost:8080/conversations/1
kill %1 %2`;

const clusterShell = `export PEERS=gateway=127.0.0.1:9101,gpu-1=127.0.0.1:9102,gpu-2=127.0.0.1:9103,sandbox=127.0.0.1:9104
NODE=gpu-1 LISTEN=127.0.0.1:9102 EXPORT=models UNTRUSTED=sandbox bin/gpu &
NODE=gpu-2 LISTEN=127.0.0.1:9103 EXPORT=models UNTRUSTED=sandbox bin/gpu &
NODE=sandbox LISTEN=127.0.0.1:9104 EXPORT=tools bin/sandbox &
NODE=gateway LISTEN=127.0.0.1:9101 UNTRUSTED=sandbox \\
  PLACEMENT=models=gpu-1+gpu-2,tools=sandbox bin/gateway &
sleep 1
curl -N localhost:8080/conversations/1/events &
curl -d 'what is 6 * 7?' localhost:8080/conversations/1`;

export const tutorialDeployments: Doc = {
	path: 'tutorial/deployments/',
	title: 'Deployments',
	description:
		'The same modules composed two ways: every service on one node, or split between a gateway, GPU nodes and a sandbox.',
	sections: [
		{
			id: 'local',
			title: 'One program',
			body: (
				<>
					<p>
						<C>cmd/local</C> composes every service and needs no configuration: with no placement,
						every service runs on this node, called <C>local</C>.
					</p>
					<Code caption="cmd/local/main.go">{local}</Code>
					<Drawing caption="one program">
						<LocalPicture />
					</Drawing>
					<p>
						A test starts <C>cmd/local</C>'s composition, has a conversation, and draws what the
						node runs from the processes' parents: the root, a supervisor per service, the
						conversation with its topic under it, and the follower the test subscribed, as the web
						front does for a browser.
					</p>
					<Code lang="txt" caption="what the node runs">
						{localTree}
					</Code>
					<p>
						A call from the conversation to the scheduler never leaves the program. A local send
						appends to the process's mailbox, and the message is not encoded.
					</p>
					<Code lang="sh" caption="shell">
						{localShell}
					</Code>
					<p>
						The front also serves a chat page at <C>localhost:8080</C>, which follows a conversation
						and shows the tokens as they come.
					</p>
				</>
			)
		},
		{
			id: 'cluster',
			title: 'A program per node',
			body: (
				<>
					<p>
						The distributed deployment splits the same modules between three entry points: the
						gateway faces browsers and holds the conversations, a GPU node runs the model, and the
						sandbox runs the tools. Each <C>main</C> is one line.
					</p>
					<Code caption="cmd/gateway/main.go">{gateway}</Code>
					<Code caption="cmd/gpu/main.go">{gpu}</Code>
					<Code caption="cmd/sandbox/main.go">{sandbox}</Code>
					<p>
						The gateway's placement puts the models on <C>gpu-1</C> and <C>gpu-2</C> and the tools
						on <C>sandbox</C>, so a conversation's addresses name those nodes; nothing in the
						conversation changed. A second GPU is the same program started under another name. The
						other nodes are told the gateway's address because a reply travels back on the
						replier's own link, and a generation publishes its tokens to a topic there. A placement
						naming a node not among the peers is refused at start.
					</p>
					<Drawing caption="four programs">
						<ClusterPicture />
					</Drawing>
					<p>
						Each node's tree holds only the services its entry point composed. This is the cluster
						in the middle of an answer: the conversation on the gateway, and its generation in a
						slot of <C>gpu-1</C>.
					</p>
					<Code lang="txt" caption="what each node runs">
						{clusterTree}
					</Code>
					<p>
						<C>EXPORT</C> and <C>UNTRUSTED</C> say what each node lets its peers ask, as{' '}
						<A to="tutorial/platform/#admit">the platform</A> reads them: a GPU node offers its
						scheduler, the sandbox its runner, and the gateway and the GPU nodes take no requests
						from the sandbox.
					</p>
					<Code lang="sh" caption="shell">
						{clusterShell}
					</Code>
					<p>
						Stop <C>gpu-1</C> with <C>kill %1</C> and the next answer comes from <C>gpu-2</C>; stop
						it while an answer streams, and the answer starts over there. While dials to a node
						fail, calls to it fail at once, and it is dialed again within five seconds, so a
						restarted GPU node takes generations again soon after it starts.
					</p>
					<p>
						Here the peers are a static list. For nodes that come and go,{' '}
						<Ext href={`${REPO}/tree/main/etcd`}>
							<C>grpcproc/etcd</C>
						</Ext>{' '}
						publishes each node under an etcd lease and resolves the others through it; the platform
						takes its resolver, registrar and membership from there, and the services do not
						change. The <A to="guides/etcd/">etcd</A> guide shows how.
					</p>
				</>
			)
		}
	]
};
