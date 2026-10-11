import gateway from '../../../examples/guide/cmd/gateway/main.go?raw';
import gpu from '../../../examples/guide/cmd/gpu/main.go?raw';
import local from '../../../examples/guide/cmd/local/main.go?raw';
import sandbox from '../../../examples/guide/cmd/sandbox/main.go?raw';
import conversations from '../../../examples/guide/internal/conversations/conversations.go?raw';
import platformCluster from '../../../examples/guide/internal/platform/cluster.go?raw';
import clusterTree from '../../../examples/guide/testdata/cluster.txt?raw';
import etcdLog from '../../../examples/guide/testdata/etcd.txt?raw';
import localTree from '../../../examples/guide/testdata/local.txt?raw';

import { Code, region } from '../code.tsx';
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
  PLACEMENT=tools=sandbox bin/gateway &
sleep 1
curl -N localhost:8080/conversations/1/events &
curl -d 'what is 6 * 7?' localhost:8080/conversations/1`;

const etcdShell = `etcd --data-dir /tmp/agents-etcd &      # or any etcd you have
export ETCD=127.0.0.1:2379
NODE=gpu-1 LISTEN=127.0.0.1:9102 EXPORT=models UNTRUSTED=sandbox bin/gpu &
NODE=gpu-2 LISTEN=127.0.0.1:9103 EXPORT=models UNTRUSTED=sandbox bin/gpu &
NODE=sandbox LISTEN=127.0.0.1:9104 EXPORT=tools bin/sandbox &
NODE=gateway LISTEN=127.0.0.1:9101 UNTRUSTED=sandbox \\
  PLACEMENT=tools=sandbox bin/gateway &
sleep 1
curl -d 'what is 6 * 7?' localhost:8080/conversations/1
# one GPU node more, which the gateway finds in etcd
NODE=gpu-3 LISTEN=127.0.0.1:9105 EXPORT=models UNTRUSTED=sandbox bin/gpu &`;

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
						The gateway's placement puts the tools on <C>sandbox</C>, so a conversation's runner
						names that node. The GPU nodes are not placed: each one's scheduler joins the{' '}
						<C>models</C> group as it starts, and the conversations find them in it, as{' '}
						<A to="#groups">below</A>. A second GPU is the same program started under another name.
						The other nodes are told the gateway's address because a reply travels back on the
						replier's own link, and a generation publishes its tokens to a topic there. A placement
						naming a node not among the peers is refused at start.
					</p>
					<Drawing caption="four programs">
						<ClusterPicture />
					</Drawing>
					<p>
						Each node's tree holds only the services its entry point composed, and, where a service
						joins or reads a group, the node's groups scope, with a relay to each node it asks. This
						is the cluster in the middle of an answer: the conversation on the gateway, and its
						generation in a slot of <C>gpu-1</C>.
					</p>
					<Code lang="txt" caption="what each node runs">
						{clusterTree}
					</Code>
					<p>
						<C>EXPORT</C> and <C>UNTRUSTED</C> say what each node lets its peers ask, as{' '}
						<A to="tutorial/platform/#admit">the platform</A> reads them: a GPU node offers its
						scheduler and its groups scope, the sandbox its runner, and the gateway and the GPU nodes
						take no requests from the sandbox.
					</p>
					<Code lang="sh" caption="shell">
						{clusterShell}
					</Code>
					<p>
						Stop <C>gpu-1</C> with <C>kill %1</C> and the next answer comes from <C>gpu-2</C>; stop
						it while an answer streams, and the answer starts over there. A stopped GPU node leaves
						the gateway's copy of the <C>models</C> group, and is asked nothing more; restarted, its
						scope says hello to the gateway's, its scheduler joins the group again, and it takes
						generations from then on.
					</p>
				</>
			)
		},
		{
			id: 'groups',
			title: 'Finding the GPU nodes',
			body: (
				<>
					<p>
						The nodes that run a model are a <A to="guides/groups/">process group</A>. The platform
						starts the node's groups scope for the services that need one, the models, which join,
						and the conversations, which read: the sandbox runs none. A scope asks the nodes the
						node knows of, but those it does not trust, which may ask it nothing anyway.
					</p>
					<Code caption="internal/platform/cluster.go">{region(platformCluster, /^\/\/ newScope is the node's part/, /^}/)}</Code>
					<p>
						A conversation takes the group's nodes as it starts each answer, in the order it tries
						them: those it knew, as it left them, so a node that failed stays at the back, then any
						that joined since. With none known yet, as for a gateway that has just started, it waits
						as if every one were busy.
					</p>
					<Code caption="internal/conversations/conversations.go">{region(conversations, /^\/\/ nodes is the nodes that run a model/, /^}/)}</Code>
				</>
			)
		},
		{
			id: 'etcd',
			title: 'With etcd',
			body: (
				<>
					<p>
						With a static list, a node is still told every other node's address. With{' '}
						<C>ETCD</C>, the nodes find each other in etcd instead:{' '}
						<Ext href={`${REPO}/tree/main/etcd`}>
							<C>grpcproc/etcd</C>
						</Ext>{' '}
						registers each node's name and <C>LISTEN</C> address under a lease as it starts,
						resolves the others from there, and reports them coming and going, and <C>PEERS</C> is not
						read. The platform takes the node's resolver, registrar and membership from it; the
						services do not change. The <A to="guides/etcd/">etcd</A> guide has the rest.
					</p>
					<Code caption="internal/platform/cluster.go">{region(platformCluster, /^\/\/ cluster is how the node finds the others/, /^}/) + '\n\n' + region(platformCluster, /^func newCluster/, /^}/)}</Code>
					<p>
						The gateway is told nothing of the GPU nodes now. Its scope learns of each from etcd and
						asks it for its members, so a GPU node started later is asked as soon as it registers,
						and one that stops leaves the group with its scheduler.
					</p>
					<Code lang="sh" caption="shell">
						{etcdShell}
					</Code>
					<p>
						A test runs the same four programs against an etcd in the test process, starts a third
						GPU node once the first answer is in, and stops the first:
					</p>
					<Code lang="txt" caption="testdata/etcd.txt">
						{etcdLog}
					</Code>
				</>
			)
		}
	]
};
