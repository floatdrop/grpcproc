import clusterTest from '../../../examples/guide/cluster_test.go?raw';
import conversationsTest from '../../../examples/guide/internal/conversations/conversations_test.go?raw';

import { Code, region } from '../code.tsx';
import { A, C } from '../components/prose.tsx';
import type { Doc } from './types.ts';

const inspect = `go install github.com/floatdrop/grpcproc/tools/cmd/grpcprocctl@latest
export GRPCPROC_ADDR=127.0.0.1:9101          # the gateway's Inspector
grpcprocctl --plaintext nodes                # the nodes it is linked to, and their links
grpcprocctl --plaintext ps --node gpu-1      # gpu-1's processes, asked there: a generation, perhaps
grpcprocctl --plaintext inspect conversation/1   # how many turns it has had
grpcprocctl --plaintext inspect root         # the gateway's root: its children, its restarts
grpcprocctl --plaintext dot --cluster | dot -Tsvg -o runtime.svg
kill %1 %2 %3 %4 %5                          # the four programs, and the curl that follows`;

export const tutorialTesting: Doc = {
	path: 'tutorial/testing/',
	title: 'Tests and inspection',
	description:
		'A service tested alone with fakes, the deployments tested whole with a node lost on purpose, and the running cluster inspected with grpcprocctl.',
	sections: [
		{
			id: 'testing',
			title: 'A service alone',
			body: (
				<>
					<p>
						A service is tested alone with fakes behind the addresses it calls: processes registered
						under the contracts' names on the node the placement points at, which with no placement
						is this one. <C>di.Test</C> gives a scope stopped when the test ends; the test composes
						the platform and the conversations module in it, overrides the limits with short ones,
						and spawns a model and a sandbox of its own.
					</p>
					<Code caption="internal/conversations/conversations_test.go">
						{region(conversationsTest, /^\/\/ The conversations alone/, /^}/)}
					</Code>
					<p>
						The fake model reports what it is asked on a channel, so a test reads what a
						conversation sent it. This one has the model ask for a tool the sandbox never answers,
						and checks that the model is asked again and told so:
					</p>
					<Code caption="internal/conversations/conversations_test.go">
						{region(conversationsTest, /^\/\/ A tool that does not answer in time/, /^}/)}
					</Code>
					<p>
						And this one waits for an idle conversation's process to end, speaks to it again, and
						checks that the new process shows the model what was said to the old one:
					</p>
					<Code caption="internal/conversations/conversations_test.go">
						{region(conversationsTest, /^\/\/ A conversation nobody speaks to/, /^}/)}
					</Code>
				</>
			)
		},
		{
			id: 'whole',
			title: 'The deployment whole',
			body: (
				<>
					<p>
						The deployments are tested whole. Each node is composed as its entry point composes it
						and told what a deployment would: its peers, where the services it does not run are,
						what it exports and whom it does not trust. The test binds the ports first, so it knows
						them, and hands each node its listener with an override, which di requires to be marked
						as one. <C>gpu-1</C> gets a model that says two words and stalls.
					</p>
					<p>
						The test follows a conversation, says something, and waits for those two words. It pins
						what each node runs at that moment, stops <C>gpu-1</C>, and pins what the follower is
						sent from then on. Last, a process on the sandbox tries to speak to the conversation,
						and must be told there is none.
					</p>
					<Code caption="cluster_test.go">{clusterTest}</Code>
				</>
			)
		},
		{
			id: 'inspect',
			title: 'Inspecting the running cluster',
			body: (
				<>
					<p>
						Every node serves the Inspector, and one asked about another node forwards the question
						there, so <C>grpcprocctl</C> pointed at one node can ask about any. <C>nodes</C> and{' '}
						<C>dot --cluster</C> list the nodes it is linked to, their links and processes, and who
						started whom; for the gateway, once a conversation has used a tool, that is all four.{' '}
						<C>inspect</C> shows what a process says about itself: a conversation its turns, a
						supervisor its children and restarts. The platform's Inspector is read-only, so{' '}
						<C>exit</C> and <C>loglevel</C> are refused.
					</p>
					<Code lang="sh" caption="shell">
						{inspect}
					</Code>
					<p>
						<A to="guides/grpcprocctl/">Command line</A> has the rest of what <C>grpcprocctl</C> does,
						and <A to="guides/web/">Web UI</A> shows the same cluster in a browser.
					</p>
				</>
			)
		}
	]
};
