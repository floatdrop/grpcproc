import conversationsAddress from '../../../examples/guide/proto/conversations/v1/address.go?raw';
import conversationsGenerated from '../../../examples/guide/proto/conversations/v1/conversations_grpcproc.pb.go?raw';
import conversationsProto from '../../../examples/guide/proto/conversations/v1/conversations.proto?raw';
import toolsGenerated from '../../../examples/guide/proto/tools/v1/tools_grpcproc.pb.go?raw';
import toolsProto from '../../../examples/guide/proto/tools/v1/tools.proto?raw';

import { Code, region } from '../code.tsx';
import { A, Aside, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesContracts: Doc = {
	path: 'guides/contracts/',
	title: 'Contracts from proto',
	description:
		'A process’s protocol is a proto service marked grpcproc.v1.process; protoc-gen-grpcproc writes its address, its handler and its topics, as gRPC’s stubs are written.',
	lead: (
		<p>
			The code is from <Ext href={file('examples/guide')}>examples/guide</Ext>, whose three contracts are
			generated this way. <A to="concepts/addressing/#protocol">Addressing</A> has why an address carries
			methods; this page has how they come from the proto.
		</p>
	),
	sections: [
		{
			id: 'service',
			title: 'A service is the contract',
			body: (
				<>
					<p>
						A process accepts a few message types and answers some of them. Written as a proto{' '}
						<C>service</C>, each rpc says which: a call, answered with its response, or a send, which
						the process takes and does not answer. One option on the service marks it as a process's
						rather than a gRPC service's; a service without it is left alone, so both can share a
						file.
					</p>
					<Code lang="proto" caption="proto/tools/v1/tools.proto">
						{region(toolsProto, /^\/\/ Runner is the process/, /^}/)}
					</Code>
					<p>
						<C>protoc-gen-grpcproc</C> writes <C>tools_grpcproc.pb.go</C> beside <C>protoc-gen-go</C>'s
						file. For a service with a name it declares:
					</p>
					<Table
						head={['Generated', 'What it is']}
						rows={[
							[<C>RunnerProcess</C>, 'The name the process is registered under on its node, a constant.'],
							[<C>RunnerAddr</C>, 'The address: an Addr[proto.Message] with a method per rpc.'],
							[<C>RunnerAddrOf(t Target)</C>, 'The address of a PID, a Name or a global name.'],
							[<C>RunnerAt(node)</C>, 'The process on node, by its name.'],
							[<C>RunnerGlobal()</C>, 'The process that holds the name globally, wherever it runs.'],
							[<C>RunnerHandler</C>, 'An interface with a method per call and per send, for the process to implement.'],
							[
								<>
									<C>HandleRunnerCall</C>, <C>HandleRunnerMessage</C>
								</>,
								'Dispatch a message to the handler, from an actor’s HandleCall and HandleMessage.'
							],
							[<C>UnimplementedRunnerHandler</C>, 'A handler that answers ErrType to every call and drops every send, to embed.']
						]}
					/>
					<Code caption="proto/tools/v1/tools_grpcproc.pb.go">
						{`${region(toolsGenerated, /^\/\/ RunnerAt addresses/, /^}/)}

${region(toolsGenerated, /^\/\/ Run calls a tool/, /^}/)}`}
					</Code>
					<p>
						An rpc whose request message has the rpc's name, <C>rpc Run(Run)</C>, needs the message's
						package-qualified name, <C>rpc Run(tools.v1.Run)</C>: protobuf resolves the bare name to
						the rpc itself.
					</p>
				</>
			)
		},
		{
			id: 'setup',
			title: 'Setting it up',
			body: (
				<>
					<p>
						The plugin is a command of the core module, so it is pinned to the core's version as a
						tool dependency, and <C>buf generate</C> runs it from there: the build is the same on
						every machine and in CI.
					</p>
					<Code lang="sh">{`go get -tool github.com/floatdrop/grpcproc/cmd/protoc-gen-grpcproc@latest`}</Code>
					<p>
						The options live in <C>proto/grpcproc/v1/options.proto</C> in the grpcproc repository. buf
						resolves imports from the Buf Schema Registry, where grpcproc is not published yet, so copy
						the file into your proto tree, one file importing only <C>descriptor.proto</C>, and keep it
						out of your own generation with an input that names your paths:
					</p>
					<Code lang="yaml" caption="buf.gen.yaml">
						{`version: v2
inputs:
  - directory: proto
    paths: [proto/app]            # not proto/grpcproc, which is only imported
plugins:
  - local: protoc-gen-go
    out: .
    opt: module=example.com/app
  - local: ["go", "tool", "protoc-gen-grpcproc"]
    out: .
    opt: module=example.com/app`}
					</Code>
					<p>
						buf's <C>STANDARD</C> lint rules are written for gRPC, and a process's service breaks four
						of them by design: a <C>Runner</C> is not a <C>RunnerService</C>; a request is the message,
						not a <C>RunRequest</C>, and a reply is its own message, not a <C>RunResponse</C>; and every
						send returns <C>Empty</C>, as every topic takes it.
					</p>
					<Code lang="yaml" caption="buf.yaml">
						{`lint:
  use:
    - STANDARD
  except:
    - SERVICE_SUFFIX
    - RPC_REQUEST_STANDARD_NAME
    - RPC_RESPONSE_STANDARD_NAME
    - RPC_REQUEST_RESPONSE_UNIQUE`}
					</Code>
					<p>
						buf compiles your protos against your copy of <C>options.proto</C>, and the plugin knows
						the options of the grpcproc version it is pinned to. Keep the two at one version: copy the
						file again when you upgrade, since a release may add fields to it, and a field your copy
						has but the pinned plugin does not is ignored without a word.
					</p>
				</>
			)
		},
		{
			id: 'rpcs',
			title: 'Calls, sends and topics',
			body: (
				<>
					<p>
						A call is a plain rpc: the address gets <C>Run(ctx, from, req) (*Ran, error)</C>, a{' '}
						<C>Call</C> that names the reply once. A send is an rpc marked{' '}
						<C>option (grpcproc.v1.send) = true</C>, returning <C>google.protobuf.Empty</C>: the
						address gets <C>Note(ctx, from, req) error</C>, a <C>Send</C>, and{' '}
						<C>NoteAfter(p, d, req) *Timer</C>, a timer to send it later, from a process, since{' '}
						<C>p.SendAfter</C> infers its type from the address and would need the message cast to{' '}
						<C>proto.Message</C>. A request with no fields is left out of every signature:{' '}
						<C>List(ctx, from)</C>, <C>SyncAfter(p, d)</C>, and <C>List(p, m)</C> on the handler. A
						field added later changes them, which is when callers should notice.
					</p>
					<p>
						A topic the process owns is an rpc that takes <C>Empty</C> and returns the event type,
						marked <C>option (grpcproc.v1.topic)</C>. It is not dispatched; it gives the address a
						typed way to the topic, and the process the name to register it under.
					</p>
					<Code lang="proto" caption="proto/conversations/v1/conversations.proto">
						{region(conversationsProto, /^\/\/ Conversation is one conversation's process/, /^}/)}
					</Code>
					<Code caption="proto/conversations/v1/conversations_grpcproc.pb.go">
						{`${region(conversationsGenerated, /^\/\/ ConversationEventsTopic is/, /^func ConversationEventsTopic/)}

${region(conversationsGenerated, /^\/\/ ConversationEventsAt is/, /^}/)}`}
					</Code>
					<p>
						The conversation spawns its topic with <C>pubsub.SpawnOwned</C> under{' '}
						<C>ConversationEventsTopic(id)</C>, and a follower subscribes to{' '}
						<C>ConversationEventsAt(node, id)</C>: <A to="tutorial/conversations/">the tutorial</A>{' '}
						shows both. A topic of a process with a name is registered under the name as given,{' '}
						<C>RelayStatsTopic</C>; one of a process named per instance under the process's name
						and the topic's, <C>"conversation/" + id + "/events"</C>.
					</p>
				</>
			)
		},
		{
			id: 'names',
			title: 'How processes are named',
			body: (
				<>
					<p>
						<C>{`{name: "tools"}`}</C> is a process registered under one name on each node that runs
						it. <C>{`{prefix: "conversation/"}`}</C> is one named per instance, the prefix and an id: it
						gets <C>ConversationProcess(id)</C>, <C>ConversationAt(node, id)</C> and{' '}
						<C>ConversationGlobal(id)</C> instead. A service with neither, one whose processes the
						application names itself or reaches by PID, gets only <C>&lt;Service&gt;AddrOf</C>, and may
						own no topic, since a topic is named after its process.
					</p>
					<Code caption="proto/conversations/v1/conversations_grpcproc.pb.go">
						{`${region(conversationsGenerated, /^\/\/ ConversationProcess is/, /^func ConversationProcess/)}

${region(conversationsGenerated, /^\/\/ ConversationAt addresses/, /^}/)}`}
					</Code>
					<p>
						What the service cannot say, the package writes by hand around the generated type. The
						guide's conversation is opened before it is called, so <C>NewConversation(node, id)</C>{' '}
						wraps the address with the node and id and does that first:
					</p>
					<Code caption="proto/conversations/v1/address.go">
						{`${region(conversationsAddress, /^\/\/ Conversation is one conversation/, /^}/)}

${region(conversationsAddress, /^\/\/ NewConversation is the conversation/, /^}/)}`}
					</Code>
					<p>
						A service may not share a name with a message in protobuf. <C>{`{go_name: "Relay"}`}</C>{' '}
						lets the proto say <C>RelayProcess</C> while the Go stays <C>RelayAddr</C>.
					</p>
				</>
			)
		},
		{
			id: 'handler',
			title: "The process's side",
			body: (
				<>
					<p>
						The handler interface has a method per call and per send, with the process, the message
						and the typed request; a topic gets none, since nothing is sent to it. An actor implements it and hands its <C>HandleCall</C> and <C>HandleMessage</C>{' '}
						to the generated dispatch, which switches on the message's type. A call of a type the
						service has no rpc for is answered <C>ErrType</C>; a send of one is logged and dropped, as{' '}
						<C>actor.CallsOnly</C> drops a message, since an error from <C>HandleMessage</C> would
						exit the actor.
					</p>
					<Code caption="proto/conversations/v1/conversations_grpcproc.pb.go">
						{`${region(conversationsGenerated, /^\/\/ ConversationHandler is what/, /^}/)}

${region(conversationsGenerated, /^\/\/ HandleConversationCall answers/, /^}/)}`}
					</Code>
					<Code caption="internal/tools/tools.go">
						{`// HandleCall hands a call to the Runner service's method for it: Run.
func (r *runner) HandleCall(p *grpcproc.Process[proto.Message], m grpcproc.Msg[proto.Message]) (proto.Message, error) {
	return toolsv1.HandleRunnerCall(r, p, m)
}

func (r *runner) Run(p *grpcproc.Process[proto.Message], m grpcproc.Msg[proto.Message], run *toolsv1.Run) (*toolsv1.Ran, error) {`}
					</Code>
					<p>
						A message the process takes from elsewhere, the events of a topic it subscribes to say, is
						declared as a send too, and dispatched like one. <C>UnimplementedRunnerHandler</C> is for
						an implementation, or a test's fake, to embed: it answers <C>ErrType</C> to every call and
						drops every send, so an rpc added to the service breaks nothing that embeds it.
					</p>
					<Aside title="The mailbox is proto.Message">
						The mailbox of a generated process is <C>proto.Message</C>: each request travels as its
						own message, and the type check moves from the node's delivery, which checked a mailbox's
						one type, to the generated dispatch. A caller's call sees the same, <C>ErrType</C>; a
						mistyped send, which the node counted as a dead letter with reason <C>type</C>, is now a
						warning in the process's log; and a message of another type enters the mailbox before
						it is refused. A receive loop rather than an actor, as the guide's conversation is,
						type-asserts the body itself.
					</Aside>
				</>
			)
		},
		{
			id: 'rules',
			title: 'What the plugin refuses',
			body: (
				<>
					<p>
						The plugin stops, naming the service or the rpc, rather than write code that would not
						compile or run:
					</p>
					<Table
						head={['Refused', 'Why']}
						rows={[
							['A service with both a name and a prefix', 'A process is named one way.'],
							['A streaming rpc', 'A process takes messages and answers calls.'],
							['A send returning anything but Empty', 'A send is answered nothing.'],
							['A topic without a name, or not taking Empty', 'It is registered under the name, and returns its events.'],
							['A topic on a service with neither name nor prefix', 'A topic is named after its process.'],
							['An rpc that is a send and a topic', 'One option or the other.'],
							['Two rpcs of one kind taking the same message', 'A message is dispatched by its type, to one rpc.'],
							[
								'An rpc named as a method of Addr or of an actor',
								'Send, Call, Name, PID, Init, HandleCall, HandleDown and the rest would be shadowed or collide.'
							],
							['An rpc named like another’s timer, NoteAfter beside the send Note', 'Two methods of one name.'],
							[
								'A generated identifier the file declares already',
								'A message named RunnerAddr, or two services generating one name.'
							],
							[
								'A topic named like its process, or two processes or topics of one file under one name',
								'The second to register would fail with ErrNameTaken.'
							],
							['A go_name that is not an exported identifier', 'It names the generated API.']
						]}
					/>
				</>
			)
		},
		{
			id: 'wire',
			title: 'What is on the wire',
			body: (
				<>
					<p>
						A body travels under its message's full protobuf name, which the receiving node decodes
						it by. So a message's name is part of the protocol: renaming <C>Relay</C> to{' '}
						<C>RelayEndpoint</C> breaks the wire between nodes of the two versions even with its
						fields unchanged, and so does moving it to another package. <C>buf breaking</C> with the{' '}
						<C>FILE</C> category reports a rename as a deleted message, which is the signal to watch
						for before a rolling upgrade. The rpc names, the service name and the Go names are not on
						the wire: those can change freely.
					</p>
				</>
			)
		}
	]
};
