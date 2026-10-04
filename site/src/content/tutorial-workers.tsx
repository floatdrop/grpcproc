import conversations from '../../../examples/guide/internal/conversations/conversations.go?raw';
import models from '../../../examples/guide/internal/models/models.go?raw';
import tools from '../../../examples/guide/internal/tools/tools.go?raw';
import failover from '../../../examples/guide/testdata/failover.txt?raw';

import { Code, region } from '../code.tsx';
import { Drawing } from '../components/Figure.tsx';
import { Failover, Handoff } from '../components/diagrams.tsx';
import { A, C } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const tutorialWorkers: Doc = {
	path: 'tutorial/workers/',
	title: 'Models and tools',
	description:
		'The model on GPU nodes, a process per generation that streams its tokens, failover when a node is lost, and tools run a process per call on a sandbox node.',
	sections: [
		{
			id: 'scheduler',
			title: 'The scheduler',
			body: (
				<>
					<p>
						A GPU node runs one scheduler, an <A to="guides/actors/">actor</A>: a struct holding its
						dependencies, with a method per kind of message. It embeds <C>actor.CallsOnly</C>, so a
						plain send to it is logged and dropped, and <C>HandleCall</C> takes the requests to
						generate.
					</p>
					<p>
						A GPU holds so many generations at once: <C>Slots</C>. With none free, the scheduler
						answers <C>busy</C> at once, and the caller asks another node. Otherwise it starts a
						process for the generation with <C>SpawnMonitor</C>, keeps the call under the monitor's{' '}
						<C>Ref</C>, and returns <C>actor.ErrNoReply</C>: the call stays open, and the scheduler
						is free for the next request. It never generates anything itself, so it always answers.
					</p>
					<Drawing caption="a call handed to a process of its own">
						<Handoff />
					</Drawing>
					<Code caption="internal/models/models.go">
						{region(models, /^\/\/ scheduler is the actor/, /^\/\/ generate is the process/).split('\n').slice(0, -1).join('\n').trimEnd()}
					</Code>
					<p>
						The slot is freed by the <C>Down</C>, however the generation ended: done, failed, or
						panicked. One that ended without answering has its call answered here, with the reason.
						The generation is linked to the scheduler with <C>LinkParent</C>, so a scheduler that
						exits takes its generations with it, and its supervisor starts a scheduler with every
						slot free and nothing running behind its back.
					</p>
				</>
			)
		},
		{
			id: 'generation',
			title: 'A process per generation',
			body: (
				<>
					<p>
						The call was made to the scheduler and is answered by the generation: a call is
						answered by whoever holds its message, and the caller does not see the difference. The
						message brings the caller's deadline with it: <C>m.Context</C> gives a context that
						ends at that deadline, so the model does not run past it. Only the deadline travels:
						a caller that goes away sooner is not heard of.
					</p>
					<Code caption="internal/models/models.go">
						{region(models, /^\/\/ generate is the process of one generation/, /^}/)}
					</Code>
					<p>
						Each token is published to the topic the request names, from the GPU node straight to
						the conversation's node. The answer follows the tokens on the same link, and a link
						delivers in order: every token is with the topic before the conversation has the
						answer, so the conversation's <C>done</C> is published after the last of them.
					</p>
					<p>
						The model is an interface the container serves. The tutorial's is a script that asks
						for the calculator when it sees a sum and otherwise says back what it was told, a word
						at a time; a real one is a client of an inference server.
					</p>
					<Code caption="internal/models/models.go">{region(models, /^\/\/ Model says the assistant's next turn/, /^}/)}</Code>
				</>
			)
		},
		{
			id: 'failover',
			title: 'A node that is lost',
			body: (
				<>
					<p>
						The placement may name several nodes for the models, and a conversation asks them in
						order. What it does next depends on what it is told, and the three cases are three
						different answers:
					</p>
					<ul>
						<li>
							<C>busy</C>: nothing was generated. The next node is asked, and with every node busy
							the conversation waits a moment and asks again.
						</li>
						<li>
							A <C>LinkError</C> with <C>Unsent</C> set: the node could not be reached, and the
							request never left. The next node is asked.
						</li>
						<li>
							Any other error: the node was lost with the request, or its generation failed. It
							may have published tokens, so before another node is asked, the subscribers are told
							the answer starts over.
						</li>
					</ul>
					<Code caption="internal/conversations/conversations.go">
						{region(conversations, /^\/\/ generate has a model answer/, /^}/)}
					</Code>
					<p>
						A node that lost its link to the gateway, and not its power, goes on generating until
						the deadline and may publish again once it redials. So every request carries an{' '}
						<C>attempt</C>, larger than the one before, every token carries its request's, and
						the event that says an answer starts over carries the new one: the web front drops
						the tokens of an attempt older than the latest it has heard of.
					</p>
					<p>
						A call to a node that goes down does not wait for its deadline. The call fails when the
						link to that node breaks, which is what makes failing over quick. In this test{' '}
						<C>gpu-1</C> is stopped after its first token; the second turn finds it gone, at once,
						without a word to the subscribers:
					</p>
					<Drawing caption="gpu-1 is lost in the middle of an answer">
						<Failover />
					</Drawing>
					<Code lang="txt" caption="testdata/failover.txt">
						{failover}
					</Code>
				</>
			)
		},
		{
			id: 'tools',
			title: 'Tools, a process per call',
			body: (
				<>
					<p>
						What a tool does is decided by what a model wrote, so tools run on a node of their
						own. The runner is the scheduler's pattern again: a process per call, the call handed
						to it, the runner free at once. Here the isolation is the point. A tool that panics
						ends its own process; the runner hears of it in a <C>Down</C> and answers the call with
						the reason, and nothing else on the node notices. The calculator divides by zero when
						asked to.
					</p>
					<Code caption="internal/tools/tools.go">
						{region(tools, /^\/\/ runner is the actor/, /^\/\/ HandleDown answers the call/).split('\n').slice(0, -1).join('\n').trimEnd() + '\n\n' + region(tools, /^\/\/ HandleDown answers the call/, /^}/)}
					</Code>
					<p>
						The conversation calls with a deadline, <C>Limits.Tool</C>, and the tool's context ends
						with it. A tool that runs out of time is an answer too: the tool says its deadline
						passed, or the conversation's call ends first, and either way the model is told the
						tool failed and says so. A goroutine cannot be
						killed, so a tool that ignores its context runs on after its caller has gone; a real
						sandbox runs tools in something that can be, a subprocess or a container, started from
						this process.
					</p>
					<p>
						The sandbox answers the gateway and may ask nothing of it, or of the GPU nodes.{' '}
						<A to="tutorial/platform/#admit">The platform</A> has how.
					</p>
				</>
			)
		}
	]
};
