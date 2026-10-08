import supervisor from '../../../examples/supervisor/main.go?raw';

import { Code, region } from '../code.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesObservability: Doc = {
	path: 'guides/observability/',
	title: 'Observability',
	description:
		'What a node reports about its processes and links: snapshots, events, hooks, logging and OpenTelemetry.',
	lead: (
		<>
			<p>
				Every process and every link keeps counters, a process can publish its own state, and the
				node reports all of it through one Go API. <A to="guides/inspector/">The Inspector</A>{' '}
				serves the same data over gRPC, to the <A to="guides/grpcprocctl/">command line</A>, the{' '}
				<A to="guides/web/">web UI</A> and <A to="guides/mcp/">AI agents</A>.
			</p>
			<ul>
				<li>
					Counters are always on: atomics on the process and the link, read by snapshot, with nothing
					to configure.
				</li>
				<li>
					The core imports no metrics or tracing library. The <C>Hooks</C> interface is the single
					tap; <C>grpcproc/otel</C> implements it, in a module of its own.
				</li>
				<li>
					The Go API is the source of truth. The Inspector and every tool built on it expose
					exactly what <C>node.Info()</C> and <C>node.Processes()</C> return.
				</li>
			</ul>
		</>
	),
	sections: [
		{
			id: 'snapshots',
			title: 'Snapshots',
			body: (
				<>
					<p>
						<C>node.Processes()</C> is every local process, ordered by PID; <C>node.Process(pid)</C>{' '}
						is one; <C>node.Info()</C> is the node with its links; <C>node.Peers()</C> the peers
						with a live link. Each is a copy taken at the call, in a stable order, so a test can
						assert on it and a tool can render it. A <C>ProcessInfo</C> has:
					</p>
					<Table
						head={['Field', 'Meaning']}
						rows={[
							[<C>PID</C>, 'The process, anywhere in the cluster.'],
							[<C>Name</C>, <>What <C>WithName</C> registered; empty for an unnamed process.</>],
							[
								<C>Label</C>,
								<>
									What <C>WithLabel</C> set, by default the type of the mailbox: the low-cardinality
									key metrics aggregate by.
								</>
							],
							[<C>Type</C>, 'The Go type of the mailbox, for display.'],
							[<C>Parent</C>, <>The process that spawned it; zero for <C>Node.Spawn</C>.</>],
							[
								<C>State</C>,
								<>
									<C>idle</C> in <C>Receive</C>, <C>running</C> between two, <C>waiting-reply</C> in a{' '}
									<C>Call</C>, or <C>exiting</C>.
								</>
							],
							[<C>StartedAt</C>, 'When it was spawned.'],
							[
								<C>Mailbox</C>,
								<>
									<C>Depth</C>, <C>Peak</C>, and <C>OldestAge</C>: how long the oldest queued message has
									waited. A depth that grows and an age that grows with it is a process that does not
									keep up.
								</>
							],
							[
								<>
									<C>Received</C>, <C>Sent</C>
								</>,
								'Messages taken from the mailbox, and messages sent.'
							],
							[<C>CallsInFlight</C>, 'Calls it has made and not yet been answered.'],
							[<C>LastMessage</C>, 'The protobuf full name of the last message taken from the mailbox.'],
							[
								<>
									<C>Monitors</C>, <C>Links</C>, <C>Watchers</C>
								</>,
								'Monitors and links it holds on others, and how many processes monitor or are linked to it.'
							],
							[<C>Wakeups</C>, <>How many times <C>Receive</C> returned.</>],
							[
								<>
									<C>Busy</C>, <C>BusyFor</C>
								</>,
								<>
									The time it has spent anywhere but waiting in <C>Receive</C>, waits in <C>Call</C>{' '}
									included, and at least how long it has been on its current message, or on the timeout
									that woke it, 0 while it waits. The growth of <C>Busy</C> between two snapshots, over the
									time between them, is the share of that time it was busy: near 1, it is never idle, and a
									growing <C>Depth</C> says it does not keep up.
								</>
							],
							[
								<>
									<C>LogLevel</C>, <C>TrapExit</C>
								</>,
								'The threshold of its logger, and whether it traps exits.'
							]
						]}
					/>
					<p>
						A <C>LinkInfo</C> describes one direction of traffic with a peer: <C>Peer</C>, whether
						this node opened the stream (<C>Outbound</C>), its <C>State</C>, when it was established
						and how many times it reconnected, how many sessions with the peer ended (
						<C>Sessions</C>, which keeps growing for a peer whose links keep breaking), envelopes
						and bytes carried, envelopes <C>Queued</C>{' '}
						to be written and their <C>QueuedBytes</C>, and for a peer whose dials fail, or whose
						last link ended soon after it came up, the{' '}
						<C>LastError</C> and the <C>RetryAt</C> before which every send to it fails at once. A{' '}
						<C>NodeInfo</C> holds the node's identity and address, when it started, how many
						processes it runs and has spawned and exited, its dead letters, and its links, ordered by
						peer name.
					</p>
				</>
			)
		},
		{
			id: 'inspect',
			title: 'Inspecting a process',
			body: (
				<>
					<p>
						<C>WithInspect</C> gives a process a function that returns a map of what it wants to
						publish, and <C>node.Inspect</C> asks for it:
					</p>
					<Code>{`node.Spawn(run, grpcproc.WithInspect(func() map[string]string {
	return map[string]string{
		"state":   rec.state.String(),
		"pending": strconv.Itoa(rec.pendingUploads),
	}
}))

state, err := node.Inspect(ctx, pid)`}</Code>
					<p>
						The function runs on the process's own goroutine, inside <C>Receive</C>, between two
						messages, so it reads the process's state with no lock. A process busy in a handler
						answers when it next receives. One that never does cannot answer, and <C>Inspect</C>{' '}
						fails with <C>busy for 12s</C>: the process is stuck in one message, and{' '}
						<C>LastMessage</C> says which kind.
					</p>
					<p>With a state machine the map is one line:</p>
					<Code>{`grpcproc.WithInspect(func() map[string]string { return map[string]string{"state": rec.state.String()} })`}</Code>
					<p>
						Supervisors publish their children, each child's PID and restart count, and how many
						restarts they have made against their limit; the{' '}
						<A to="guides/inspector/">Inspector</A> shows it as <C>grpcprocctl inspect</C>.
					</p>
				</>
			)
		},
		{
			id: 'events',
			title: 'Events',
			body: (
				<>
					<p>
						<C>node.Subscribe(ctx, buffer)</C> is a channel of what happens on the node: a spawn, an
						exit with its reason, a session with a peer beginning or ending (its first link up, the peer
						declared down), a dead letter. Each <C>Event</C> has a{' '}
						<C>Kind</C> and the fields that kind fills: the <C>Process</C> snapshot for a spawn or an
						exit, the <C>Peer</C> and an error for a link, <C>From</C>, <C>To</C>, the message{' '}
						<C>Type</C> and a reason for a dead letter.
					</p>
					<p>
						Publishing never blocks the node. A subscriber whose buffer is full loses the event, and
						the next one it receives carries <C>Missed</C>, how many were lost in between. With no
						subscriber the cost is one atomic load at each point that would publish. The channel
						closes when <C>ctx</C> ends or the node stops.
					</p>
					<p>
						These are the node's own events. An application's events, published by one process for
						others on any node, go through <A to="guides/pubsub/">Pub/sub</A>.
					</p>
					<p>
						The supervisor example crashes an actor and follows what the supervisor does through the
						events:
					</p>
					<Code caption="examples/supervisor/main.go">
						{region(supervisor, /Crash it, and follow what/, /^\t\t\tbreak/) + '\n\t\t}\n\t}'}
					</Code>
				</>
			)
		},
		{
			id: 'hooks',
			title: 'Hooks',
			body: (
				<>
					<p>
						<C>Config.Hooks</C> is the synchronous tap for everything else. Every method runs on the
						goroutine doing the work, so an implementation must be cheap and must not call back into
						the node:
					</p>
					<Code>{`type Hooks interface {
	OnSpawn(ProcessInfo)
	OnExit(info ProcessInfo, reason string)
	OnSend(s SendInfo, md Metadata) (Metadata, Done)
	OnReceive(r ReceiveInfo, md Metadata) (Metadata, Done)
	OnDeadLetter(from, to PID, body proto.Message, reason string)
	OnLinkUp(peer NodeID)
	OnLinkDown(peer NodeID, err error)
}

type Done func(err error)`}</Code>
					<p>
						<C>OnSend</C> runs before a send or a call leaves its sender, <C>OnReceive</C> when a
						process takes a message, a <C>Down</C> or an <C>Exited</C> from its mailbox. Both return
						the metadata to use from then on, and a <C>Done</C> that closes what they started: a send
						once it is handed to delivery, a call once it returns, the handling of a message when the
						process next calls <C>Receive</C> or exits, with the exit reason as the error.
					</p>
					<p>
						A process remembers the metadata of the message it is handling, as <C>OnReceive</C>{' '}
						returned it, and its own sends and calls inherit it. So a tracer that puts the handling
						span in the metadata sees everything the handler sends as that span's child, and trace
						chains form across processes and nodes without a context threaded through handlers.
					</p>
					<p>
						<C>Metadata</C> is a <C>map[string]string</C> carried with every message, untouched:
						trace context, a tenant, a request id. <C>WithMetadata(ctx, md)</C> sets it for a node's
						sends and every call, and <C>MetadataFrom(ctx)</C> reads it back. The hooks must not
						modify the map they are given, which may be shared; they return a copy to change it.
					</p>
					<p>
						Embed <C>NopHooks</C> and override what you need. <C>JoinHooks</C> combines several:
						metadata threads through them in order, and their <C>Done</C>s run in reverse. With no
						hooks, the cost is a nil check per message.
					</p>
				</>
			)
		},
		{
			id: 'logging',
			title: 'Logging',
			body: (
				<>
					<p>
						<C>p.Log()</C> is a <C>*slog.Logger</C> with the process's PID and label attached, built
						on <C>Config.Logger</C>, by default <C>slog.Default()</C>. Each process has a threshold
						of its own: <C>node.SetLogLevel(pid, level)</C> changes it at runtime, the Inspector's{' '}
						<C>SetLogLevel</C> does the same over gRPC, and <C>ProcessInfo.LogLevel</C> reports it.
					</p>
					<Code>{`p.Log().Info("charged", "order", m.Body.Order, "amount", m.Body.Amount)

if err := node.SetLogLevel(pid, slog.LevelDebug); err != nil {
	return err // ErrNoProc: no such local process
}`}</Code>
					<p>
						A panic in a process is logged with its stack, and the process exits with reason{' '}
						<C>panic: …</C>. Goroutine dumps and heap profiles are <C>net/http/pprof</C>'s job.
					</p>
					<p>
						A process's goroutine, and every goroutine its code starts, carries the pprof labels{' '}
						<C>grpcproc.label</C>, <C>grpcproc.pid</C> and, for a named process,{' '}
						<C>grpcproc.name</C>; the goroutines the node starts, a link's among them, carry none. So
						a CPU profile splits by
						process, and a goroutine dump says whose each goroutine is. <C>pprof.Do</C> over{' '}
						<C>p.Context()</C> adds labels of your own to them.
					</p>
					<Code lang="sh">{`go tool pprof -tags http://localhost:6060/debug/pprof/profile               # CPU by label and by PID
go tool pprof -tagfocus=grpcproc.label=ledger http://localhost:6060/debug/pprof/profile`}</Code>
				</>
			)
		},
		{
			id: 'otel',
			title: 'OpenTelemetry',
			body: (
				<>
					<p>
						<Ext href={file('otel/README.md')}>grpcproc/otel</Ext> implements <C>Hooks</C> with
						OpenTelemetry: a span for every send, call and handled message, chained across processes
						and nodes, and metrics keyed by process label. It is a separate module.
					</p>
					<Code lang="sh">{'go get github.com/floatdrop/grpcproc/otel'}</Code>
					<Code>{`h, err := grpcprocotel.New() // global meter and tracer providers and propagator; see Options
node, err := grpcproc.NewNode(grpcproc.Config{…, Hooks: grpcproc.JoinHooks(h, myHooks)})
reg, err := h.Observe(node) // gauges read from node snapshots at each collection
defer reg.Unregister()`}</Code>
					<h3>Traces</h3>
					<Table
						head={['Where', 'Span']}
						rows={[
							[<C>Send</C>, <>
								<C>send &lt;message type&gt;</C>, a producer span, child of the context the sender carries.
							</>],
							[<C>Call</C>, <>
								<C>call &lt;message type&gt;</C>, a client span, child of the context the sender carries.
							</>],
							['A process takes a message', <>
								<C>process &lt;message type&gt;</C>, a consumer span (server for a call), child of the sender's
								span.
							</>],
							['A process takes a Down', <>
								<C>process grpcproc.Down</C>, a consumer span with no parent.
							</>],
							['A process that traps exits takes an Exited', <>
								<C>process grpcproc.Exited</C>, a consumer span with no parent.
							</>]
						]}
					/>
					<p>
						A handling span lasts until the process calls <C>Receive</C> again, or exits, with an
						error status if abnormally. Everything the process sends meanwhile is its child, because
						the process's sends inherit the metadata of the message it is handling and the hooks put
						the span there. For a handler's own spans, a database call say, take the handling span
						from the message, in the message's context, which ends when its caller stops waiting:
					</p>
					<Code>{`ctx, cancel := m.Context(p.Context())
defer cancel()
ctx, span := tracer.Start(h.Extract(ctx, m.Metadata), "load order")
defer span.End()`}</Code>
					<p>
						Spans carry <C>messaging.system=grpcproc</C>, <C>messaging.operation.type</C>,{' '}
						<C>messaging.destination.name</C>, <C>grpcproc.message.type</C>, <C>grpcproc.label</C> and{' '}
						<C>grpcproc.pid</C>; failed sends and calls carry <C>error.type</C>.
					</p>
					<h3>Metrics</h3>
					<p>
						No metric carries a PID: the process label is the unit, and every other attribute is
						bounded, so a system that spawns a process per session does not make a series for each.
					</p>
					<Table
						head={['Name', 'Type and attributes']}
						rows={[
							[<C>grpcproc.messages.sent</C>, <>
								counter; <C>grpcproc.label</C> (the sender's), <C>grpcproc.call</C>, <C>grpcproc.remote</C>
							</>],
							[<C>grpcproc.messages.received</C>, <>counter; <C>grpcproc.label</C></>],
							[<C>grpcproc.mailbox.wait</C>, <>histogram, seconds; <C>grpcproc.label</C></>],
							[<C>grpcproc.process.duration</C>, <>
								histogram, seconds, from taking a message to the next <C>Receive</C>; <C>grpcproc.label</C>
							</>],
							[<C>grpcproc.call.duration</C>, <>
								histogram, seconds; <C>grpcproc.label</C>, <C>grpcproc.remote</C>, <C>error.type</C> on failure
							</>],
							[<C>grpcproc.processes.spawned</C>, <>counter; <C>grpcproc.label</C></>],
							[<C>grpcproc.processes.exited</C>, <>
								counter; <C>grpcproc.label</C>, <C>grpcproc.reason</C>: <C>normal</C>, <C>shutdown</C>,{' '}
								<C>killed</C>, <C>noproc</C>, <C>noconnection</C>, <C>type</C>; <C>max restarts</C> from
								actor, <C>timeout</C> and <C>replaced</C> from cron, <C>demoted</C> from leader;{' '}
								<C>panic</C> or <C>error</C>
							</>],
							[<C>grpcproc.dead_letters</C>, <>counter; <C>grpcproc.reason</C>, <C>grpcproc.message.type</C></>],
							[<>
								<C>grpcproc.links.up</C>, <C>grpcproc.links.down</C>
							</>, <>counter; <C>grpcproc.peer</C></>],
							[<C>grpcproc.processes</C>, <>gauge, from <C>Observe</C>; <C>grpcproc.label</C></>],
							[<C>grpcproc.mailbox.depth</C>, <>gauge, from <C>Observe</C>, summed; <C>grpcproc.label</C></>],
							[<C>grpcproc.mailbox.oldest</C>, <>gauge, from <C>Observe</C>, seconds, maximum; <C>grpcproc.label</C></>],
							[<>
								<C>grpcproc.link.messages</C>, <C>grpcproc.link.bytes</C>
							</>, <>counter, from <C>Observe</C>; <C>grpcproc.peer</C>, <C>grpcproc.direction</C></>]
						]}
					/>
					<p>
						<C>error.type</C> is one of <C>noproc</C>, <C>type</C>, <C>mailbox_full</C> (the
						callee's mailbox was full), <C>too_large</C> (the message or its reply was over{' '}
						<C>MaxMessageSize</C>), <C>busy</C> (the link to the peer was full),{' '}
						<C>noconnection</C>, <C>timeout</C>, <C>canceled</C>, <C>remote</C> (the
						handler returned an error) or <C>other</C>. <A to="reference/errors/">Errors and exit reasons</A> says what each means.
					</p>
				</>
			)
		}
	]
};
