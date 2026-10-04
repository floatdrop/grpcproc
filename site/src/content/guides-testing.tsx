import ordersTest from '../../../examples/guide/internal/orders/orders_test.go?raw';
import supervisor from '../../../examples/supervisor/main.go?raw';
import restartTest from '../../../examples/supervisor/restart_test.go?raw';
import shopTest from '../../../examples/testing/shop_test.go?raw';

import { Code, region } from '../code.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesTesting: Doc = {
	path: 'guides/testing/',
	title: 'Testing',
	description:
		'Run a cluster inside one go test, break it on purpose, test one actor with fakes, and make timers deterministic.',
	lead: (
		<p>
			A multi-node scenario runs as a plain <C>go test</C>, with no sockets and no registry. The
			code is from <Ext href={file('examples/testing/shop_test.go')}>examples/testing</Ext> and the
			tutorial's{' '}
			<Ext href={file('examples/guide/internal/orders/orders_test.go')}>orders service</Ext>.
		</p>
	),
	sections: [
		{
			id: 'cluster',
			title: 'A cluster in a test',
			body: (
				<>
					<p>
						<C>grpcproctest.New(t, names...)</C> starts a node per name, each with its own gRPC
						server, talking over in-memory connections. The nodes stop when the test ends. Every
						node resolves the others by name, so a process on one is reached from another through{' '}
						<C>grpcproc.Named</C>, as in production.
					</p>
					<Code>{`c := grpcproctest.New(t, "shop", "warehouse")
shop, warehouse := c.Node("shop"), c.Node("warehouse")`}</Code>
					<p>
						<C>NewWith(t, opts, names...)</C> takes options that apply to every node, each time it
						starts, restarts included:
					</p>
					<Table
						rows={[
							[<C>WithHooks(h)</C>, <>Installs an observability tap on every node.</>],
							[<C>WithLogger(l)</C>, <>The logger every node uses. By default the nodes log nothing.</>],
							[
								<C>WithConfig(fn)</C>,
								<>
									Adjusts a node's <C>Config</C> before the node is created, by name: a{' '}
									<C>Registrar</C>, a <C>Membership</C>, a <C>DialBackoff</C>, hooks for one node
									only.
								</>
							],
							[
								<C>WithServices(fn)</C>,
								<>
									Registers extra gRPC services on every node's server, an{' '}
									<A to="guides/inspector/">Inspector</A> say. <C>c.Conn(name)</C> dials a node's
									server from outside the cluster to call them.
								</>
							]
						]}
					/>
					<p>
						Two things differ from a production node. The cluster's nodes dial a peer again at once
						after a failed dial, since <C>DialBackoff</C> is negative, so the send right after a
						partition heals reaches the peer; a test of the backoff itself sets one with{' '}
						<C>WithConfig</C>. And the connections are in memory, so keepalive never fires: the
						cluster breaks links itself.
					</p>
				</>
			)
		},
		{
			id: 'faults',
			title: 'Partitions, crashes and restarts',
			body: (
				<>
					<p>
						The cluster is also the network between its nodes, and can break it. Processes see
						each fault as they would in production:
					</p>
					<Table
						rows={[
							[
								<C>Partition(a, b)</C>,
								<>
									Cuts the network between two nodes both ways. Existing links break, so monitors
									across them fire <C>Down{'{'}noconnection{'}'}</C> and pending calls fail with{' '}
									<C>ErrNoConnection</C>; new dials are refused until <C>Heal(a, b)</C>.
								</>
							],
							[
								<C>Kill(name)</C>,
								<>
									Stops a node abruptly, as a crash would: no <C>Down{'{'}shutdown{'}'}</C> reaches
									anyone, peers see their links break. By the time it returns, monitors have fired
									and calls have failed.
								</>
							],
							[
								<C>Stop(name)</C>,
								<>
									Stops a node gracefully: its processes exit with <C>shutdown</C>, and their
									watchers get <C>Down{'{'}shutdown{'}'}</C> before the links close.
								</>
							],
							[
								<C>Restart(name)</C>,
								<>
									Starts a stopped or killed node again, with a new incarnation and none of the
									processes the old one ran. An address from before points at a process that no
									longer exists.
								</>
							]
						]}
					/>
					<p>
						The example's test uses each. The process under test is the quick start's stock; the
						test reserves across nodes, then breaks things:
					</p>
					<Code caption="examples/testing/shop_test.go">{region(shopTest, /^func TestReserveAcrossNodes/, /^}/)}</Code>
					<p>
						Assert on the error, with <C>errors.Is</C>: <C>ErrNoConnection</C> during a
						partition, <C>ErrNoProc</C> after a restart, since the name is registered on nobody on
						the new incarnation. After <C>Stop</C>, a process monitoring the stock would receive{' '}
						<C>Down{'{'}shutdown{'}'}</C>; after <C>Kill</C> or a partition, the reason is{' '}
						<C>noconnection</C>. <A to="reference/errors/">Errors and exit reasons</A> lists every
						one.
					</p>
				</>
			)
		},
		{
			id: 'alone',
			title: 'One actor, with fakes',
			body: (
				<>
					<p>
						Most tests need one node. An actor that calls other actors takes their addresses, and
						a test puts fakes behind them: plain process functions, registered under the names the
						contracts give, on the node the actor was told to use. The tutorial's orders desk calls
						the inventory and the payments; its test composes the platform and the orders module
						alone, then spawns a stock and a cashier of its own:
					</p>
					<Code caption="examples/guide/internal/orders/orders_test.go">{region(ordersTest, /^\/\/ The desk alone/, /^}/)}</Code>
					<p>
						The stock always says yes, and reports what it is asked to release on a channel; the
						cashier is the test's parameter. One test gives it a cashier that declines, and expects
						the items back:
					</p>
					<Code caption="examples/guide/internal/orders/orders_test.go">{region(ordersTest, /^func TestADeclinedCardGivesTheItemsBack/, /^}/)}</Code>
					<p>
						The other gives it a cashier that exits without answering, so the desk's call fails
						with <C>ErrNoProc</C>. The card may have been charged, so the desk must keep the items
						reserved, and the test checks that nothing was released:
					</p>
					<Code caption="examples/guide/internal/orders/orders_test.go">{region(ordersTest, /^\/\/ A cashier that takes the charge and answers nothing/, /^}/)}</Code>
					<p>
						A fake is a process function, a dozen lines here. One that must answer on another node
						is the same function spawned on another node of a <C>grpcproctest</C> cluster.
					</p>
				</>
			)
		},
		{
			id: 'events',
			title: 'Waiting for an exit or a spawn',
			body: (
				<>
					<p>
						A test that crashes a supervised actor needs to know when the replacement runs.{' '}
						<C>node.Subscribe(ctx, buffer)</C> returns a channel of events: spawns, exits with
						their reasons, links up and down, dead letters. Subscribe before the fault, so the exit
						cannot be missed, then read until the spawn:
					</p>
					<Code caption="examples/supervisor/main.go">{region(supervisor, /events := node.Subscribe\(ctx, 16\)/, /^\t\t\tbreak/) + '\n\t\t}\n\t}'}</Code>
					<p>
						A subscriber that falls behind loses events rather than block the node; the next event
						it gets says how many in <C>Missed</C>. A test buffer of a few dozen is plenty. For a
						process that must be gone, <C>node.Process(pid)</C> answers whether it still runs, and{' '}
						<C>node.Whereis(name)</C> whether a name is registered.
					</p>
				</>
			)
		},
		{
			id: 'deterministic',
			title: 'Deterministic tests',
			body: (
				<>
					<p>
						Go's <C>testing/synctest</C> makes tests that involve time deterministic. Inside{' '}
						<C>synctest.Test</C>, the time
						package runs on a fake clock that moves only when every goroutine in the test's bubble
						waits, and <C>synctest.Wait</C> returns once they all do. grpcproc runs in a bubble as
						it is: a process waits on channels the bubble sees, and <C>SendAfter</C>,{' '}
						<C>ReceiveTimeout</C>, a caller's deadline and a supervisor's restart window and{' '}
						<C>Shutdown</C> all keep the bubble's time. A <C>grpcproctest</C> cluster runs there too,
						since its connections are in memory, and so do its partitions, crashes and restarts.
					</p>
					<Code caption="examples/supervisor/restart_test.go">{region(restartTest, /^\/\/ The tree restarts the inventory/, /^}/)}</Code>
					<p>
						<C>time.Sleep(time.Minute)</C> takes no real time, and <C>synctest.Wait</C> after a
						crash returns once the supervisor has restarted the child, or given up: every process
						waits, so the test can look. Outside a bubble,{' '}
						<A to="guides/testing/#events">subscribing</A> does that for one event.
					</p>
					<p>
						Two rules keep a test in its bubble. Everything it uses is made inside{' '}
						<C>synctest.Test</C>: nodes, clusters, channels, contexts; a channel or a timer made
						outside cannot be used inside. And every process has ended by the time the test does, or
						synctest reports a deadlock with the goroutines still waiting: stopping the node in{' '}
						<C>t.Cleanup</C>, as <C>grpcproctest</C> does, ends them all, and the report shows a
						process that ignores its exit. Real sockets stay outside: a goroutine blocked on
						the network is not waiting where the bubble can see it, so a test that listens on a
						port, as the <A to="guides/blocking-io/">blocking I/O</A> example does, or talks to
						etcd, runs on real time.
					</p>
				</>
			)
		},
		{
			id: 'ci',
			title: 'In CI',
			body: (
				<>
					<p>
						A cluster test needs no ports and no external services, and takes a few hundred
						milliseconds. Run it under the race detector: a test that reaches into an actor's
						fields from the test goroutine is a race, and should talk to the actor through its
						mailbox instead.
					</p>
					<Code lang="sh">{'go test -race -count=1 ./...'}</Code>
					<p>
						Timeouts on calls in tests come from <C>t.Context()</C>, which is cancelled when the
						test ends, or a <C>context.WithTimeout</C> around it for a call that is expected to
						fail; a call with no deadline to a process that never answers is a test that never ends.
					</p>
				</>
			)
		}
	]
};
