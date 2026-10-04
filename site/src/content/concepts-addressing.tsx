import inventoryAddress from '../../../examples/guide/proto/inventory/v1/address.go?raw';
import quickstart from '../../../examples/quickstart/main.go?raw';

import { Code, region } from '../code.tsx';
import { A, Aside, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const conceptsAddressing: Doc = {
	path: 'concepts/addressing/',
	title: 'Addresses',
	description:
		'How a message finds a process: PIDs, names on a node, global names, and the typed address over them.',
	lead: (
		<p>
			A message goes to an address, never to a process value. An address is small, comparable, and means
			the same on every node, so it can be stored and sent inside a message. There are two kinds, a
			PID and a name, and a typed wrapper over both.
		</p>
	),
	sections: [
		{
			id: 'pid',
			title: 'PID',
			body: (
				<>
					<p>
						A <C>PID</C> identifies one process, anywhere in the cluster, for the whole of its life:
					</p>
					<Code>{`type PID struct {
	Node        string // the node it runs on
	Incarnation uint64 // that node's start
	ID          uint64 // the process, on that start
}`}</Code>
					<p>
						It prints as <C>{'<shop.1718.4>'}</C>. <C>Spawn</C> returns its address, <C>p.PID()</C> is
						the process's own, and <C>Msg.From</C> is the sender's. A PID is a value: it can be
						compared, used as a map key, and put inside a protobuf message to tell another process
						whom to answer.
					</p>
					<p>
						The incarnation makes a PID safe to keep. A node that restarts numbers its processes
						from one again, so without it the PID of a process from before the restart would point
						at whichever process now has that number. With it, the PID names a process that no longer
						exists: a send to it is a dead letter, a call fails with <C>ErrNoProc</C>, and a monitor
						on it fires <C>Down{'{noproc}'}</C>. By
						default the incarnation is the node's start time; <A to="concepts/nodes/">Nodes</A> says
						what else it fences.
					</p>
					<Aside title="Coming from Erlang">
						<p>
							The incarnation is the creation number of a pid, kept for the same reason. It is a
							field rather than a hidden part of the value, and it grows with each start rather than
							counting modulo four, so a peer can tell which of two incarnations is the newer.
						</p>
					</Aside>
				</>
			)
		},
		{
			id: 'name',
			title: 'Name',
			body: (
				<>
					<p>
						A process may register a name on its node when it is spawned, and hold it until it
						exits. A <C>Name</C> is the node and that name; it prints as <C>{'{stock@warehouse}'}</C>.
					</p>
					<Code>{`_, err := warehouse.Spawn(inventory, grpcproc.WithName("stock"))
// ErrNameTaken while another process holds the name.

pid, ok := warehouse.Whereis("stock") // on this node, now`}</Code>
					<p>
						Names are per node: two nodes can each have a <C>stock</C>, told apart by the node
						name. A name is freed when its process exits and taken by whatever
						is spawned under it next, so an address by name outlives any one process: a supervisor
						restarts a child under the same name and its callers notice only the gap. A
						message sent by name while nobody holds it is a dead letter, and a monitor placed by
						name fires at once with <C>noproc</C>.
					</p>
					<p>
						A local name is found by knowing which node it is on, from configuration or from a
						message that carried its PID. A name of the whole installation is a global name, as
						Erlang's <C>global</C> is.
					</p>
				</>
			)
		},
		{
			id: 'global',
			title: 'Global names',
			body: (
				<>
					<p>
						A global name belongs to the installation, not to a node: whichever process holds it,
						wherever it runs, is what <C>{'Global{"room:42"}'}</C> reaches. It is for a process that
						moves, restarted on another node or placed where there is room, and for many of them:
						one per room, per tenant, per order. A process claims the name itself, and holds it until
						it exits or releases it:
					</p>
					<Code>{`func room(p *grpcproc.Process[*roomspb.Command]) error {
	claim, err := p.Claim(p.Context(), "room:42", grpcproc.KeepOnLoss())
	if err != nil {
		return err // a *grpcproc.TakenError names the holder; errors.Is(err, grpcproc.ErrTaken)
	}
	_ = claim.Revision() // the fencing token for what it writes elsewhere
	// … serve the room; the name goes when this function returns
}

// From any node: the node looks the name up and sends to the holder's PID.
r, err := grpcproc.AddrOf[*roomspb.Command](grpcproc.Global{Name: "room:42"}).
	Call[*roomspb.Joined](ctx, node, join)`}</Code>
					<p>
						The names live in a store, <C>Config.Names</C>: etcd in production, an in-memory one in
						every <C>grpcproctest</C> cluster. Each node keeps a copy, so a send never waits on the
						store; a message sent a moment after the holder moved can reach the old one, and fail as
						a process that does not exist would. For a name no one holds, a call fails
						with <C>ErrNoProc</C>, a monitor gets <C>Down{'{'}noproc{'}'}</C> with the global name
						in <C>Down.Name</C>, a message is a dead letter. Where the copy's lag matters, before
						starting what another node may have started, <C>node.Names().Resolve</C> asks the store
						itself.
					</p>
					<Table
						head={['Option', 'What it changes']}
						rows={[
							['none', <>If the store loses the claim, the node cut off from etcd past its lease, the holder ends with <C>name lost</C> before another node can claim the name: never two holders. For what must be unique, a ledger.</>],
							[<C>KeepOnLoss()</C>, <>The holder runs on, <C>Held()</C> false, and holds the name again once the store is back. If another process claimed it meanwhile, the old holder ends with <C>name conflict</C>. For what must stay up, a room whose calls go on while etcd is away.</>],
							[<C>WaitForName()</C>, <>A held name does not fail the claim: it waits, until its ctx is done, for the name to be free, and claims it then. A standby: the same process on every node, one active.</>]
						]}
					/>
					<p>
						In a test, <C>c.CutNames("b")</C> cuts node b off from the store as a partition from
						etcd would, and <C>c.RestoreNames("b")</C> lets it back.{' '}
						<C>ProcessInfo.Globals</C>, and the Inspector, show what a process holds. The{' '}
						<Ext href={file('docs/DESIGN.md')}>design notes</Ext> have the rest: what the store does,
						the release on exit, the cost at a hundred thousand names.
					</p>
				</>
			)
		},
		{
			id: 'addr',
			title: 'Typed addresses and targets',
			body: (
				<>
					<p>
						An <C>Addr[M]</C> is a PID or a Name together with the message type the process behind
						it accepts. It is what every typed send and call takes, and it comes from three places:
					</p>
					<Table
						rows={[
							[
								<C>Spawn</C>,
								<>
									Returns the new process's address, typed by the function it was given. <C>p.Addr()</C>{' '}
									is the process's own.
								</>
							],
							[
								<C>Named[M](node, name)</C>,
								<>
									An address by name, with the type the caller asserts. One service reaches another
									this way: the name and the type are the contract, exported by the package that owns
									the process.
								</>
							],
							[
								<C>AddrOf[M](target)</C>,
								<>
									Types an untyped target: a PID that came in a message, say. Nothing checks the
									assertion until a message is delivered.
								</>
							]
						]}
					/>
					<Code caption="examples/quickstart/main.go">
						{region(quickstart, /From shop it is a node name/, /stock := grpcproc.Named/)}
					</Code>
					<p>
						A <C>Target</C> is anything a message can go to: a <C>PID</C>, a <C>Name</C>, or any{' '}
						<C>Addr</C>. <C>Monitor</C>, <C>Link</C> and <C>Exit</C> take a target, since they do not
						care about the mailbox's type; <C>SendTo</C> and <C>CallTo</C> take one too, and the type
						is checked on delivery only.
					</p>
				</>
			)
		},
		{
			id: 'protocol',
			title: 'Address types with methods',
			body: (
				<>
					<p>
						A typed send or call goes through the address, which takes the sender as an argument, a{' '}
						<C>Caller</C>: the <C>*Node</C>, or a <C>*Process</C> from inside a handler. Every call
						site names its reply, <C>stock.Call[*shoppb.Reserved](ctx, node, req)</C>, and wraps the
						request in the mailbox's oneof. The package that owns the process can do both once,
						wrapping <C>Call</C> and <C>Send</C> in a method per operation on an address type of its
						own:
					</p>
					<Code caption="examples/guide/proto/inventory/v1/address.go">
						{`${region(inventoryAddress, /^\/\/ StockAddr addresses/, /^type StockAddr/)}

${region(inventoryAddress, /^\/\/ Reserve takes items/, /^}/)}`}
					</Code>
					<p>
						A caller then writes <C>stock.Reserve(ctx, p, req)</C> from a handler, or{' '}
						<C>stock.Reserve(ctx, node, req)</C> from anywhere else, and names neither the reply nor
						the oneof. The sender is an argument rather than part of the address because who sends
						matters: a process's call carries the metadata of the message it is handling, and an
						actor keeps its addresses in fields but has its process only inside a handler. The type
						embeds <C>Addr</C>, so it is still a <C>Target</C> with its raw <C>Call</C> and{' '}
						<C>Send</C>.{' '}
						<A to="shop/#contracts">The shop</A>'s contracts are written this way.
					</p>
				</>
			)
		},
		{
			id: 'types',
			title: 'Type checks',
			body: (
				<>
					<p>
						The type on an address is the sender's claim. On the sending node the compiler enforces
						it: a <C>Send</C> to an <C>Addr[*shoppb.Reserve]</C> takes a <C>*shoppb.Reserve</C> and
						nothing else. The claim does not travel: on the wire a message is its type's full
						name and its encoding, and the receiving node decodes it and checks, on delivery, that
						the process's mailbox accepts it.
					</p>
					<p>
						A remote sender that names the right process with the wrong type does not reach it. The
						message never enters the mailbox: it is a dead letter with reason <C>type</C>, and a
						caller gets <C>ErrType</C>. The same check runs for a local send. An address that{' '}
						<C>Spawn</C> returned always passes it; <C>Named</C> or <C>AddrOf</C> with the wrong
						type, or an untyped <C>SendTo</C>, is a dead letter on the same node too.
					</p>
				</>
			)
		},
		{
			id: 'dead-letters',
			title: 'Dead letters',
			body: (
				<>
					<p>
						A message that cannot be delivered is a dead letter: there is no process at the PID,
						nobody holds the name, the type is wrong, or the mailbox is full. For a <C>Send</C>{' '}
						that is not an error: the node counts the dead letter and reports it to{' '}
						<C>Hooks.OnDeadLetter</C> and to subscribers of its events.{' '}
						<A to="reference/errors/#dead-letters">Errors and exit reasons</A> lists the reasons.
					</p>
					<p>
						A <C>Call</C> waits, so it fails: with <C>ErrNoProc</C> when there is no such process,
						or when the process exits before answering, and with <C>ErrType</C> when the process
						does not take the request or the reply is not what the caller asked for. A send returns
						an error only for what its own node could not do: the message could not be encoded or
						was too large, the peer could not be reached or its link is full, or the context ended
						while waiting for a first connection to it.
					</p>
					<p>
						<A to="guides/observability/">Observability</A> shows where dead letters are counted and
						watched; a steady trickle of them under one name is usually a caller with a stale
						configuration.
					</p>
				</>
			)
		},
		{
			id: 'metadata',
			title: 'Metadata',
			body: (
				<>
					<p>
						Every message carries a <C>Metadata</C>, a <C>map[string]string</C> that grpcproc passes
						along and never reads: trace context, a tenant, a request id, whatever the application's
						interceptors would carry. From outside a process it is set on the context:
					</p>
					<Code>{`ctx = grpcproc.WithMetadata(ctx, grpcproc.Metadata{"tenant": "acme"})
r, err := stock.Call[*shoppb.Reserved](ctx, node, reserve)

md := grpcproc.MetadataFrom(ctx) // reads it back`}</Code>
					<p>
						Inside a process it needs no context. A process remembers the metadata of the
						message it is handling, and every <C>Send</C>, <C>Call</C> and <C>SendAfter</C> it makes
						meanwhile inherits it. A request that enters at the edge with a trace id leaves the same
						id on every message its handling causes, across processes and nodes. A call's context can add to what is inherited, and{' '}
						<C>m.Context(parent)</C> puts a message's metadata into a context for code that wants
						one, a database client say; for a call it also ends at the caller's deadline, as{' '}
						<A to="concepts/processes/#send-call">Send and Call</A> describes.
					</p>
					<p>
						<A to="guides/observability/">grpcproc/otel</A> builds its traces on this: a span
						opened when a message is taken, and every message sent while handling it a child of that
						span.
					</p>
				</>
			)
		},
		{
			id: 'node-pid',
			title: "The node's own PID",
			body: (
				<>
					<p>
						A message sent from outside any process still has a sender: the node's pseudo-process,{' '}
						<C>node.PID()</C>, the node's name and incarnation with ID zero. It has no mailbox, so a
						process cannot reply to it with a send; a call from it is answered through the call, as
						any other. It shows up in <C>Msg.From</C>, in dead letters and in traces.
					</p>
				</>
			)
		}
	]
};
