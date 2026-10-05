import conversations from '../../../examples/guide/internal/conversations/conversations.go?raw';
import web from '../../../examples/guide/internal/web/web.go?raw';
import address from '../../../examples/guide/proto/conversations/v1/address.go?raw';

import { Code, region } from '../code.tsx';
import { Drawing } from '../components/Figure.tsx';
import { Flow, Lifetime } from '../components/diagrams.tsx';
import { A, C } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const tutorialConversations: Doc = {
	path: 'tutorial/conversations/',
	title: 'Conversations',
	description:
		'One process per conversation: started when it is spoken to, publishing its events on a topic of its own, ended when idle, with its history kept outside it.',
	sections: [
		{
			id: 'process',
			title: 'A process per conversation',
			body: (
				<>
					<p>
						A conversation is a receive loop. It loads what was said before, starts the topic its
						events go to, and then takes what the user says, one thing at a time. A turn can take
						as long as the model does; what is said meanwhile waits in the mailbox and is answered
						in its turn, so a conversation needs no lock and no queue of its own.
					</p>
					<Code caption="internal/conversations/conversations.go">
						{region(conversations, /^\/\/ run is the conversation's process/, /^}/)}
					</Code>
					<p>
						<C>Say</C> is a call, and it is answered when its turn begins, not when the model is
						done: the caller learns which turn it is, and the answer goes to whoever follows the
						conversation. With nothing said for <C>Limits.Idle</C>, <C>ReceiveTimeout</C> returns
						and so does the function: the process ends, normally. A million conversations nobody
						is having cost nothing. A browser left open on one does count as somebody: when the
						process ends, it follows again, which starts the process again.
					</p>
				</>
			)
		},
		{
			id: 'start',
			title: 'Started when spoken to',
			body: (
				<>
					<p>
						The service's supervisor has no children of its own. It has a <em>factory</em>: a
						function from a message to a child's spec, which <C>actor.StartChildFrom</C> calls by
						name, from this node or another. The child is registered under a name made from the
						conversation's id, so starting one that runs already fails with{' '}
						<C>ErrAlreadyStarted</C>, and two requests for one conversation can never start two
						processes. The id comes from a URL and becomes part of a registered name, so the
						factory takes only letters, digits, <C>-</C> and <C>_</C>.
					</p>
					<Code caption="internal/conversations/conversations.go">
						{region(conversations, /^\/\/ tree is the service's supervision tree/, /^}/)}
					</Code>
					<p>
						The contract's address hides this. <C>Say</C> opens the conversation and then calls it,
						so its callers never ask whether a process runs. A process may end for being idle
						between the two; the call then fails with <C>ErrNoProc</C>, and the conversation is
						opened once more.
					</p>
					<Code caption="proto/conversations/v1/address.go">
						{region(address, /^\/\/ open starts the conversation's process/, /^}/) + '\n\n' + region(address, /^\/\/ ask opens the conversation/, /^}/)}
					</Code>
					<p>
						A conversation is <C>Transient</C>: its supervisor starts it again if it crashes, and
						leaves it alone when it ends for being idle. <C>WithInspect</C> gives{' '}
						<C>grpcprocctl inspect</C> something to show for it: how many turns it has had.
					</p>
				</>
			)
		},
		{
			id: 'history',
			title: 'State outside the process',
			body: (
				<>
					<p>
						What was said lives in a <C>History</C>, not in the process. Each turn is appended as
						it is said, and a process that starts loads it and publishes it to its new topic, for
						whoever follows. So a conversation that was idle comes back knowing what was said, and
						so does one that crashed: the turn it was in is lost, the turns before it are not. The history is an interface the container serves; the
						tutorial's keeps it in memory, and a database would be built, started and stopped the
						same way.
					</p>
					<Drawing caption="a conversation's process, and what outlasts it">
						<Lifetime />
					</Drawing>
					<Code caption="internal/conversations/conversations.go">
						{region(conversations, /^\/\/ History keeps what was said/, /^}/)}
					</Code>
				</>
			)
		},
		{
			id: 'turn',
			title: 'A turn',
			body: (
				<>
					<p>
						A turn is a loop: the model answers, and for as long as it asks for a tool, the tool is
						run and the model answers again. The models and the tools belong to other services,
						perhaps on other nodes; the conversation calls them through the methods of their
						addresses, as its process <C>p</C>, under a deadline.
					</p>
					<Code caption="internal/conversations/conversations.go">
						{region(conversations, /^\/\/ answer is one turn/, /^}/)}
					</Code>
					<p>
						A turn that cannot be finished is published as failed and the conversation goes on. An
						error from the history ends the process, and the supervisor starts one that loads what
						the history has.
					</p>
					<Drawing caption="one turn, with one tool call">
						<Flow />
					</Drawing>
				</>
			)
		},
		{
			id: 'events',
			title: 'A topic of its own',
			body: (
				<>
					<p>
						Each conversation owns a <A to="guides/pubsub/">pub/sub</A> topic, started with{' '}
						<C>pubsub.SpawnOwned</C>: it is linked to the conversation and ends when it does, which
						its subscribers learn from a <C>Down</C>. The conversation publishes what the user
						said, the tools it calls and the end of each turn; the tokens are published by the process generating them, on
						the GPU node, straight to the topic. The topic keeps the last 1024 events, and a new
						subscriber receives those first, so a browser that connects in the middle of an
						answer is sent the answer so far. A process that starts with a history publishes it
						to its new topic first, as the events it was, so the same holds after the conversation
						was idle; of one longer than the topic keeps, a follower is sent the end.
					</p>
					<p>
						<C>Follow</C> asks the conversation nothing: in the middle of a turn it would not be
						answered until the turn is over. It opens the conversation and subscribes to the
						topic by name. <C>StartChildFrom</C> returns once the process is started, which is a
						moment before it has made its topic, so for that moment <C>Follow</C> subscribes
						again.
					</p>
					<Code caption="proto/conversations/v1/address.go">
						{region(address, /^\/\/ Follow subscribes p/, /^}/)}
					</Code>
				</>
			)
		},
		{
			id: 'edge',
			title: 'The HTTP front',
			body: (
				<>
					<p>
						The web front is not a process. To say something, a handler calls the conversation
						through the node, as any code outside a process does: the node is the sender it passes
						to <C>Say</C>. A failure becomes a 503, and what went wrong between the nodes goes to
						the log, not to the client.
					</p>
					<Code caption="internal/web/web.go">{region(web, /^\/\/ say is POST/, /^}/)}</Code>
					<p>
						To follow a conversation, a handler does need a process, since a topic's subscriber is
						one. It spawns a follower for as long as the browser listens: the follower subscribes
						and hands each event to the handler's goroutine, which writes it as a server-sent
						event. When the browser goes away the handler tells the follower to exit, and the topic
						forgets a subscriber that has. When the conversation ends, the follower receives the
						topic's <C>Down</C> and the stream closes; the browser asks again, which opens the
						conversation again.
					</p>
					<Code caption="internal/web/web.go">{region(web, /^\/\/ follow is GET/, /^}/)}</Code>
					<p>
						The server is run by <C>dihttp.Serve</C>: it listens when the program starts, serves
						once the whole start has succeeded, and drains before anything else stops, so what is
						said meanwhile still finds its conversation. A browser that follows would keep its
						request open, and the drain waiting; the server's shutdown ends <C>closing</C>, and
						every such handler returns.
					</p>
				</>
			)
		}
	]
};
