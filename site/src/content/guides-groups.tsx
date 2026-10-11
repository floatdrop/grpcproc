import example from '../../../examples/groups/main.go?raw';
import exampleOutput from '../../../examples/groups/output.txt?raw';

import { Code, Output, region } from '../code.tsx';
import { A, Aside, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

// What groups/scale_test.go measured: nodes, memory a node, a full sync's
// CPU, one change's CPU.
const scale = [
	['25', '0.21 MB', '79 ms', '0.1 ms'],
	['50', '0.38 MB', '254 ms', '0.2 ms'],
	['100', '0.78 MB', '1.04 s', '0.5 ms'],
	['200', '1.57 MB', '4.7 s', '1.1 ms']
];

export const guidesGroups: Doc = {
	path: 'guides/groups/',
	title: 'Process groups',
	description:
		'Named sets of processes on any node, after Erlang’s pg: joined on their own node, listed from every node’s copy, and watched for joins and leaves.',
	lead: (
		<p>
			<C>grpcproc/groups</C> is optional, and built on the public API only. The code is from{' '}
			<Ext href={file('examples/groups/main.go')}>examples/groups</Ext>.
		</p>
	),
	sections: [
		{
			id: 'scopes',
			title: 'Groups and scopes',
			body: (
				<>
					<p>
						A group is a name that many processes hold at once: the workers that resize photos, the
						nodes of a shard that serve rooms. Any process on any node lists its members and sends to
						the ones it picks. A <A to="concepts/addressing/#global">global name</A> has one holder,
						and a <A to="guides/pubsub/">pub/sub</A> topic keeps its subscribers to itself; a group
						is the list.
					</p>
					<p>
						Each node that takes part runs a scope: a process registered as{' '}
						<C>groups/&lt;scope&gt;</C>, which keeps a copy of every node's members of every group.
						Nodes whose scopes share a name share their groups, and a node may run several scopes,
						one per purpose.
					</p>
					<Code>{`workers, err := groups.Start(node, groups.Config{Scope: "jobs"})`}</Code>
					<p>A scope finds the other nodes that run it through its config:</p>
					<Table
						head={['Config', 'The nodes it asks for their members']}
						rows={[
							[<C>Peers</C>, 'These, from the start, whatever Membership reports.'],
							[
								<C>Membership</C>,
								<>
									Each node it reports up, and it forgets each one it reports down, unless{' '}
									<C>Peers</C> names it. Unset, the node's own <C>Config.Membership</C>.
								</>
							],
							[
								<C>Select</C>,
								<>
									Of the nodes Membership reports up, only those it takes: by their metadata, say,
									so that a scope of one shard does not dial the whole cluster.
								</>
							],
							[
								<C>Retry</C>,
								'How long after a node is lost it is asked again, doubling with each loss in a row up to 32 times as long. Default 1s.'
							]
						]}
					/>
					<p>
						A node that asks this one is answered whatever these say. With neither <C>Peers</C> nor
						a <C>Membership</C>, a scope knows only the nodes that ask it.
					</p>
				</>
			)
		},
		{
			id: 'joining',
			title: 'Joining and leaving',
			body: (
				<>
					<p>
						A process joins a group on its own node's scope, from the <C>Node</C> or a{' '}
						<C>Process</C>, as for <C>Addr.Call</C>. It stays in the group until it leaves it or
						exits: the scope monitors it. The example's workers join as they start:
					</p>
					<Code caption="examples/groups/main.go">{region(example, /^\/\/ worker resizes the photos/, /^}/)}</Code>
					<Table
						head={['Call', 'What it does']}
						rows={[
							[
								<C>scope.Join(ctx, from, group, pids…)</C>,
								'Adds processes of the scope’s node to the group, each once however often joined. When it returns, this node’s copy has them; the other nodes hear of it after.'
							],
							[<C>scope.Leave(ctx, from, group, pids…)</C>, 'Takes them out of it. A process that is not in it is left alone.']
						]}
					/>
					<p>
						Both refuse a process of another node with <C>ErrNotLocal</C>, and a group's name that is
						empty, not valid UTF-8 or longer than 1 KiB with <C>ErrGroup</C>; <C>Join</C> refuses a
						process that does not run with <C>ErrNoProc</C>. A refusal changes nothing.
					</p>
				</>
			)
		},
		{
			id: 'members',
			title: 'Finding members',
			body: (
				<>
					<p>
						<C>scope.Members(group)</C> lists the members on every node, ordered by node,
						incarnation and id. <C>scope.Local(group)</C> lists this node's, and{' '}
						<C>scope.Groups()</C> the groups with members. All three read the node's copy under a lock,
						from any goroutine, without a message. The example's dispatch node runs no worker, and
						sends each worker it finds a photo:
					</p>
					<Code caption="examples/groups/main.go">{region(example, /The dispatch node finds them/, /^\t}$/)}</Code>
					<p>
						The copy is eventually consistent. This node's own joins are in it when <C>Join</C>{' '}
						returns, and another node's once that node's message arrives. A member that exits or a node
						that goes away may still be listed for a moment, so a call to a member can fail as any call
						can, with <C>ErrNoProc</C> or <C>ErrNoConnection</C>: pick another and try again.
					</p>
				</>
			)
		},
		{
			id: 'watching',
			title: 'Watching a group',
			body: (
				<>
					<p>
						<C>scope.Watch(ctx, p, group)</C> has the scope send <C>p</C> a{' '}
						<C>*groupsv1.Changed</C> for each change to the group: who joined, and who left.{' '}
						<C>Watch.Members</C> are the members when the watch began, and the changes follow from
						there. <C>p</C> must be a process of the scope's node, and its mailbox must accept{' '}
						<C>*groupsv1.Changed</C>.
					</p>
					<Code caption="examples/groups/main.go">{region(example, /^\/\/ watcher prints the first change/, /^}/)}</Code>
					<Table
						head={['When', 'What the watcher sees']}
						rows={[
							['A member joins or leaves, or exits, on any node', <>A <C>Changed</C> with it.</>],
							["A node can no longer be reached, or its scope ends", <>A <C>Changed</C> with that node's members as left.</>],
							[<C>w.Cancel(p)</C>, 'No more changes for that watch. What is already in the mailbox stays.'],
							['The scope ends', <>The <C>Down</C> with <C>w.Ref</C>.</>]
						]}
					/>
					<p>
						Each <C>Watch</C> is ended by its own <C>Cancel</C>. A process that holds several watches of
						one group still gets each change once.
					</p>
				</>
			)
		},
		{
			id: 'nodes',
			title: 'Across nodes',
			body: (
				<>
					<p>
						A scope reaches each other node's scope through a relay, a process of its own labelled{' '}
						<C>groups relay</C>, so that a send that waits for a dial to an unreachable node holds up
						the relay and not the scope. The relay says hello to the other scope, watches the scope
						that answers, and carries this node's members to it: the whole list once, then each change.
						What one node says to another goes over one link in order, so no change overtakes the list
						it changes.
					</p>
					<Table
						head={['What happens to another node', 'Its members on this node']}
						rows={[
							['Its scope answers', 'Its list, then its changes.'],
							['It runs no such scope, or its scope ends', <>Dropped. It is <C>absent</C>.</>],
							['It cannot be reached', <>Dropped. It is <C>unreachable</C>.</>],
							['Membership reports it gone', <>Dropped, and the node forgotten, unless <C>Peers</C> names it.</>]
						]}
					/>
					<p>
						An absent or unreachable node is asked again after <C>Retry</C>, doubling with each loss in a
						row, and at once when it says hello or Membership reports a newer incarnation of it. News of
						an incarnation older than one a node is known to have is ignored, as the core ignores it.
					</p>
				</>
			)
		},
		{
			id: 'example',
			title: 'Example',
			body: (
				<>
					<p>
						Three nodes in one binary, as in the <A to="start/">quick start</A>: two workers on each of{' '}
						<C>east</C> and <C>west</C>, and a <C>dispatch</C> node that finds them and sends each a
						photo. A watcher on <C>dispatch</C> sees a worker exit on <C>west</C>; then <C>east</C>{' '}
						stops, and its workers leave the group with it.
					</p>
					<Code caption="examples/groups/main.go">{region(example, /A watcher hears of a worker that exits/, /after east stopped/)}</Code>
					<Output>{exampleOutput}</Output>
				</>
			)
		},
		{
			id: 'scale',
			title: 'How far it scales',
			body: (
				<>
					<p>
						A scope is a full mesh, as Erlang's <C>pg</C> is: every node holds a relay to every other
						node of the scope and a copy of every member, and sends each change to every other node.{' '}
						<Ext href={file('groups/scale_test.go')}>groups/scale_test.go</Ext> measured it in one process
						over in-memory connections, with ten members a node:
					</p>
					<div className="gp-table-wrap">
						<table className="gp-table">
							<thead>
								<tr>
									<th>Nodes in the scope</th>
									<th>Memory a node</th>
									<th>Every node syncing at once, CPU in all</th>
									<th>One change, CPU in all</th>
								</tr>
							</thead>
							<tbody>
								{scale.map((row) => (
									<tr key={row[0]}>
										{row.map((cell, j) => (
											<td key={j}>{cell}</td>
										))}
									</tr>
								))}
							</tbody>
						</table>
					</div>
					<p>
						That is about 8 KB a node of the scope, and about 125 bytes a membership, on every node; a
						change costs each node that receives it about 5.5 µs. What grows with the square of the
						nodes is churn: if each of 900 nodes made ten changes a second, each node would take about
						9,000 a second, some 5% of a core. Keep a big scope to roles that change slowly, and use{' '}
						<C>Select</C> and a scope per purpose to keep each scope to the nodes that need it. The
						links between the nodes cost more than the scope does.
					</p>
				</>
			)
		},
		{
			id: 'limits',
			title: 'Limits',
			body: (
				<>
					<ul>
						<li>
							A group is a directory of live processes, not a store. A scope that restarts has no
							members until its processes join again.
						</li>
						<li>
							Membership is eventually consistent: two nodes may list a group differently for a moment.
							A choice that must be the same everywhere, one leader, one owner, is{' '}
							<A to="guides/leader/">leader election</A> or a global name.
						</li>
						<li>
							Every join and leave goes to every node of the scope. A group per room or per order would
							send each one's opening and closing to all of them: rooms are better found by a global
							name.
						</li>
					</ul>
					<Aside title="Groups, pub/sub and global names">
						A group lists who holds a role, and the caller picks and calls. A{' '}
						<A to="guides/pubsub/">pub/sub</A> topic carries events to subscribers the publisher never
						sees. A <A to="concepts/addressing/#global">global name</A> has exactly one holder in the
						installation. The roomcontroller of a shard might find its live SFUs through a group, tell
						them of a change through a topic, and own a room by its global name.
					</Aside>
				</>
			)
		},
		{
			id: 'inspecting',
			title: 'Inspecting',
			body: (
				<>
					<p>
						A scope answers a <C>groupsv1.Query</C> through <C>grpcproc.WithQuery</C>, which a read-only{' '}
						<A to="guides/inspector/">Inspector</A> serves: its groups by prefix, with each one's members
						on every node and on this one, one group's members, and the other nodes it knows, by state.{' '}
						<A to="guides/grpcprocctl/">grpcprocctl</A> reads it with <C>grpcprocctl groups</C>, the{' '}
						<A to="guides/mcp/">MCP server</A> with <C>process_groups</C> and <C>group_members</C>, and the{' '}
						<A to="guides/web/">web UI</A> on its Groups page, each as the copy of the node it is pointed
						at has the groups.
					</p>
				</>
			)
		}
	]
};
