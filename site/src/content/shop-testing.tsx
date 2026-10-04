import clusterTest from '../../../examples/guide/cluster_test.go?raw';
import ordersTest from '../../../examples/guide/internal/orders/orders_test.go?raw';

import { Code } from '../code.tsx';
import { A, C } from '../components/prose.tsx';
import type { Doc } from './types.ts';

const inspect = `go install github.com/floatdrop/grpcproc/tools/cmd/grpcprocctl@latest
export GRPCPROC_ADDR=127.0.0.1:9101          # the front's Inspector
grpcprocctl --plaintext nodes                # the nodes it is linked to, and their links
grpcprocctl --plaintext ps --node warehouse  # the warehouse's processes, asked there
grpcprocctl --plaintext inspect root         # the front's root: its children, its restarts
grpcprocctl --plaintext dot --cluster | dot -Tsvg -o shop.svg
kill %1 %2 %3                                # the three programs`;

export const shopTesting: Doc = {
	path: 'shop/testing/',
	title: 'Tests and inspection',
	description:
		'A service tested alone with fakes, the deployments tested whole, and the running cluster inspected with grpcprocctl.',
	sections: [
		{
			id: 'testing',
			title: 'Tests',
			body: (
				<>
					<p>
						A service is tested alone with fakes behind the addresses it calls: processes registered
						under the contracts' names on the node the placement points at, which with no placement
						is this one. <C>di.Test</C> gives a scope stopped when the test ends; the test composes
						the platform and the orders module in it.
					</p>
					<Code caption="internal/orders/orders_test.go">{ordersTest}</Code>
					<p>
						The deployments are tested whole. Each node is composed as its entry point composes it
						and told what a deployment would: its peers, and where the services it does not run are.
						The test binds the three ports first, so it knows them, and hands each node its listener
						with an override, which di requires to be marked as one. It places orders through the
						front, pins what each node runs, and stops billing to see the front answer 503.
					</p>
					<Code caption="cluster_test.go">{clusterTest}</Code>
				</>
			)
		},
		{
			id: 'inspect',
			title: 'Inspecting the running shop',
			body: (
				<>
					<p>
						Every node serves the Inspector, and one asked about another node forwards the question
						there, so <C>grpcprocctl</C> pointed at one node can ask about any. <C>nodes</C> and{' '}
						<C>dot --cluster</C> list the nodes it is linked to, their links and processes, and who
						started whom; for the front, once it has taken an order, that is all three.{' '}
						<C>inspect</C> shows what a process says about itself; a supervisor lists its children
						and restarts. The platform's Inspector is read-only, so <C>exit</C> and{' '}
						<C>loglevel</C> are refused.
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
