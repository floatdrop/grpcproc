import quickstart from '../../../examples/quickstart/main.go?raw';

import { Code, region } from '../code.tsx';
import { ActorsPicture } from '../components/diagrams-actors.tsx';
import { Drawing } from '../components/Figure.tsx';
import { A, Aside, C, Ext } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const conceptsActors: Doc = {
	path: 'concepts/actors/',
	title: 'The actor model',
	description:
		'The model behind grpcproc, for a Go developer who has not met it: processes, messages, addresses, and supervision.',
	lead: (
		<p>
			grpcproc takes its shape from Erlang, where the unit of a program is a process: state of its
			own, a queue of messages in front of it, and an address anyone can send to.
		</p>
	),
	sections: [
		{
			id: 'process',
			title: 'Processes',
			body: (
				<>
					<p>
						A process is a Go function, run on a goroutine of its own, that takes messages from a
						mailbox one at a time. Anything can append to the mailbox, from any goroutine or any
						node; only the process reads from it. Its state lives in local variables nothing else
						touches, so it needs no lock.
					</p>
					<Code caption="examples/quickstart/main.go">{region(quickstart, /^func inventory/, /^}/)}</Code>
					<p>
						The only way to change the state is to send a message. A caller that wants an answer
						sends a call and waits; one that does not sends and moves on. Nothing interrupts a
						process between two messages, so every handler sees a consistent state and leaves one.
					</p>
					<p>
						A mailbox is typed: this one holds <C>*shoppb.Reserve</C> and nothing else, and the
						compiler checks every send to it. The type is a protobuf message, so it has a wire
						encoding already: a process gets the same value from its neighbour in the binary and
						from a process on another machine.
					</p>
				</>
			)
		},
		{
			id: 'messages',
			title: 'Messages, not shared memory',
			body: (
				<>
					<p>
						Go already says "do not communicate by sharing memory; share memory by
						communicating". A process is a channel with an owner, an address, and a lifetime you
						can observe.
					</p>
					<p>
						A channel has no name: to send on it you need the channel value, so sender and receiver
						were wired together by whoever created both. A channel does not cross a program
						boundary, and it cannot tell you that the goroutine reading it has died; a send to it
						blocks for ever, or never happens, and nothing reports which.
					</p>
					<p>
						A process's address, a PID or a registered name, can be passed around, stored and sent
						inside a message, and works from any node in the cluster. And a process can be
						monitored: when it exits, or when its node is lost, whoever asked is told, with a
						reason.
					</p>
					<p>
						Two processes never share a variable; they share only the messages between them. A
						value that many parts of a program read and write becomes a process that owns it and
						answers questions about it. That is more typing than a mutex, and it is why the process
						can move to another machine without anything else changing.
					</p>
				</>
			)
		},
		{
			id: 'address',
			title: 'Addresses',
			body: (
				<>
					<p>
						A message is sent to an address, never to a process value. The address is a PID, which
						names a node and a process on it, or a name the process registered on its node. Both
						are small comparable values that mean the same thing on every node.
					</p>
					<p>
						<C>Send</C>, <C>Call</C>, <C>Monitor</C> and <C>Exit</C> take an address, look at the
						node in it, and either deliver within the program or write to the link with that node.
						The code on both ends is the same either way.
					</p>
					<Drawing caption="the same call, within a node and across nodes">
						<ActorsPicture />
					</Drawing>
					<p>
						What differs is what can go wrong: a remote call fails when the link breaks, and a
						monitor on a remote process fires when the node cannot be reached, with a reason that
						says so. The{' '}
						<A to="shop/">tutorial</A> runs one application as one program and as three, from the
						same code: only the configuration changes.
					</p>
				</>
			)
		},
		{
			id: 'rpc',
			title: 'Processes and RPC services',
			body: (
				<>
					<p>
						A gRPC service method runs once per request, on whatever goroutine the server gives it,
						concurrently with every other request. It has no state between calls but what it keeps
						in a database or behind a mutex. That suits a stateless handler, and grpcproc leaves
						your services as they are.
					</p>
					<p>
						A process has state and a queue, and handles one message at a time. A stock that must
						not be reserved twice, a session that must see its requests in order, a job that runs in
						steps and can be asked how far it got: each is one process.
					</p>
					<p>
						In the tutorial an HTTP handler is not a process: it calls one through the node, waits
						with the request's context, and turns the answer into a status code.
					</p>
				</>
			)
		},
		{
			id: 'failure',
			title: 'Failure as a message',
			body: (
				<>
					<p>
						A process ends when its function returns, and it has a reason: <C>normal</C> for a nil
						return, the error's text otherwise, <C>panic: …</C> for a panic, or whatever an{' '}
						<C>Exit</C> request asked for. Any process that monitored it receives a <C>Down</C>{' '}
						carrying the reason, after the last message the exiting process sent.
					</p>
					<p>
						A node that cannot be reached is reported the same way: every monitor on a process there
						gets a <C>Down</C> with reason <C>noconnection</C>, and every call waiting on it fails.
						So the code that handles a crash handles a lost machine too.
					</p>
					<p>
						A goroutine that panics takes the program down, and one that returns leaves no trace.{' '}
						<A to="concepts/monitors-and-links/">Monitors and links</A> has the rest.
					</p>
				</>
			)
		},
		{
			id: 'supervision',
			title: 'Supervision',
			body: (
				<>
					<p>
						Erlang's answer to a process that fails is to let it die and have another one, a
						supervisor, start it again from a known state. Most
						failures come from a state the code did not anticipate, and the surest way out of such a
						state is to leave it. Code that recovers in place keeps the bad state and adds guesses
						to it.
					</p>
					<p>
						A supervisor is itself a process. It starts its children, monitors them, and when one
						exits it applies a policy: restart that child, restart all of them, or give up and exit
						itself, which its own supervisor then handles. Supervisors of supervisors form a tree.
					</p>
					<p>
						In grpcproc a supervisor is an ordinary process built on <C>SpawnMonitor</C>,{' '}
						<C>Down</C> and <C>Exit</C>, in the optional <C>grpcproc/actor</C> package.{' '}
						<A to="concepts/supervision/">Supervision</A> explains the strategies and what state
						should live outside the process so a restart can load it.
					</p>
				</>
			)
		},
		{
			id: 'grpcproc',
			title: 'What grpcproc adds to Go',
			body: (
				<>
					<ul>
						<li>
							<strong>Typed mailboxes.</strong> A process accepts one protobuf message type, or an
							interface over several, and its address carries that type. The compiler checks a send
							on the sending node, and the receiving node checks again on delivery, since types do not
							cross the wire.
						</li>
						<li>
							<strong>Your gRPC server as the transport.</strong> A node is one gRPC service
							registered next to your others, with your credentials, interceptors and keepalive. Two
							nodes hold one stream in each direction, and everything between them travels on it in
							order.
						</li>
						<li>
							<strong>Delivery guarantees.</strong> At most once, in order per sender, into
							mailboxes that never block the sender:{' '}
							<A to="concepts/processes/#delivery">Processes and messages</A> has the list.
						</li>
					</ul>
					<p>
						The reasons behind the choices, and what was rejected, are in the{' '}
						<Ext href="https://github.com/floatdrop/grpcproc/blob/main/docs/DESIGN.md">design notes</Ext>.
					</p>
					<Aside title="Coming from Erlang">
						<p>
							A process is a process, its pid is a <C>PID</C> with a creation number called an
							incarnation, and a registered name is per node, as in <C>register/2</C>. There is no
							selective receive: the mailbox's type is fixed at spawn, and a message of another type
							is a dead letter. <C>gen_server</C> is the <C>actor</C> package's <C>Handler</C>, with{' '}
							<C>HandleCall</C> and <C>HandleMessage</C> for <C>handle_call</C> and{' '}
							<C>handle_cast</C>; a supervisor is <C>actor.Supervise</C>, with the same strategies and
							restart types. Links are one way, as in ergo, and an exit signal is not trapped, since a
							goroutine cannot be killed. Nodes connect over gRPC streams rather than the
							distribution protocol, and <C>global</C> is{' '}
							<A to="concepts/addressing/#global">global names</A>.
						</p>
					</Aside>
				</>
			)
		}
	]
};
