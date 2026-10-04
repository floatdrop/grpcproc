// The site's drawings, as inline SVG: the primitives, and the tutorial's
// figures. Other pages draw with the same primitives in their own files. Every colour is a Gravity UI token, set
// in main.css, so a drawing follows the theme the way the text does; the
// shapes are the only thing here. Coordinates are in the viewBox, which is
// drawn at the width of the column.
import type { ReactNode } from 'react';

export type Kind = 'plain' | 'actor' | 'supervisor' | 'platform' | 'contract' | 'outside';

interface BoxProps {
	x: number;
	y: number;
	w: number;
	h?: number;
	title: string;
	sub?: string | undefined;
	kind?: Kind;
}

/** A labelled box; x and y are its top-left corner. */
export function Box({ x, y, w, h = 36, title, sub, kind = 'plain' }: BoxProps) {
	const cx = x + w / 2;
	return (
		<g className={`gp-d-box gp-d-box_${kind}`}>
			<rect x={x} y={y} width={w} height={h} rx={6} />
			<text className="gp-d-title" x={cx} y={sub ? y + h / 2 - 3 : y + h / 2 + 4} textAnchor="middle">
				{title}
			</text>
			{sub && (
				<text className="gp-d-sub" x={cx} y={y + h / 2 + 12} textAnchor="middle">
					{sub}
				</text>
			)}
		</g>
	);
}

/** A process boundary: everything drawn inside runs in one OS process. */
export function Frame({ x, y, w, h, title }: { x: number; y: number; w: number; h: number; title: string }) {
	return (
		<g className="gp-d-frame">
			<rect x={x} y={y} width={w} height={h} rx={10} />
			<text className="gp-d-frame-title" x={x + 12} y={y + 20}>
				{title}
			</text>
		</g>
	);
}

interface LineProps {
	d: string;
	/** Which diagram's markers to use: marker ids are per drawing. */
	id: string;
	end?: boolean;
	start?: boolean;
	kind?: 'solid' | 'dashed' | 'tree';
}

export function Line({ d, id, end = true, start = false, kind = 'solid' }: LineProps) {
	return (
		<path
			className={`gp-d-line gp-d-line_${kind}`}
			d={d}
			markerEnd={end ? `url(#${id}-end)` : undefined}
			markerStart={start ? `url(#${id}-start)` : undefined}
		/>
	);
}

export function Note({ x, y, children, anchor = 'start' }: { x: number; y: number; children: ReactNode; anchor?: 'start' | 'middle' | 'end' }) {
	return (
		<text className="gp-d-note" x={x} y={y} textAnchor={anchor}>
			{children}
		</text>
	);
}

/**
 * The svg element, its accessible name, and this drawing's arrowheads. One
 * that moves takes the focus, so that the keyboard can hold it still, as the
 * pointer does (main.css).
 */
export function Diagram({ id, width, height, label, moving = false, compact = false, children }: { id: string; width: number; height: number; label: string; moving?: boolean; /** A portrait layout, for a narrow column: see Adaptive in Figure.tsx. */ compact?: boolean; children: ReactNode }) {
	return (
		<svg className={compact ? 'gp-diagram gp-diagram_compact' : 'gp-diagram'} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={label} tabIndex={moving ? 0 : undefined}>
			<defs>
				<marker id={`${id}-end`} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
					<path className="gp-d-head" d="M0,0 L10,5 L0,10 z" />
				</marker>
				<marker id={`${id}-start`} viewBox="0 0 10 10" refX="1" refY="5" markerWidth="7" markerHeight="7" orient="auto">
					<path className="gp-d-head" d="M10,0 L0,5 L10,10 z" />
				</marker>
			</defs>
			{children}
		</svg>
	);
}

/**
 * The four layers, and which way imports go: an entry point composes
 * modules, every process runs the platform, a service imports contracts and
 * never another service.
 */
export function Layout() {
	const id = 'layout';
	const services = ['conversations', 'models', 'tools', 'web'];
	const contracts = ['conversations/v1', 'models/v1', 'tools/v1'];
	const sx = (i: number) => 24 + i * 130;
	const cx = (i: number) => 34 + i * 170;
	const edges: [number, number][] = [
		[0, 0],
		[0, 1],
		[0, 2],
		[1, 0],
		[1, 1],
		[1, 2],
		[2, 2],
		[3, 0]
	];
	return (
		<Diagram id={id} width={720} height={292} label="Entry points, the platform, services and contracts, and the imports between them">
			{['cmd/local', 'cmd/gateway', 'cmd/gpu', 'cmd/sandbox'].map((c, i) => (
				<Box key={c} x={24 + i * 130} y={14} w={116} title={c} kind="outside" />
			))}
			<Note x={560} y={36}>which services</Note>
			<Box x={24} y={70} w={506} title="platform" sub="config · node · gRPC server · Inspector · root supervisor" kind="platform" />
			<Note x={560} y={92}>every program</Note>
			{services.map((s, i) => (
				<Box key={s} x={sx(i)} y={136} w={116} title={s} kind={s === 'web' ? 'plain' : 'actor'} />
			))}
			<Note x={560} y={158}>a Module each</Note>
			{contracts.map((c, i) => (
				<Box key={c} x={cx(i)} y={236} w={146} title={c} kind="contract" />
			))}
			<Note x={560} y={258}>messages, names</Note>
			{edges.map(([s, c]) => (
				<Line key={`${s}-${c}`} id={id} d={`M${sx(s) + 58},172 L${cx(c) + 73},234`} />
			))}
		</Diagram>
	);
}

interface Lane {
	x: number;
	title: string;
	sub?: string;
	kind: Kind;
	/** Where its lifeline ends, if not at the bottom. */
	until?: number;
}

/** A sequence drawing: lanes, and the messages between them, top to bottom. */
function Sequence({ id, width, height, label, lanes, children }: { id: string; width: number; height: number; label: string; lanes: Lane[]; children: (msg: (from: number, to: number, y: number, text: string, kind?: 'solid' | 'dashed') => ReactNode) => ReactNode }) {
	const x = (i: number) => lanes[i]!.x;
	const msg = (from: number, to: number, y: number, text: string, kind: 'solid' | 'dashed' = 'solid') => (
		<g key={`${from}-${to}-${y}`}>
			<Line id={id} kind={kind} d={`M${x(from) + (to > from ? 4 : -4)},${y} L${x(to) + (to > from ? -4 : 4)},${y}`} />
			<Note x={(x(from) + x(to)) / 2} y={y - 6} anchor="middle">
				{text}
			</Note>
		</g>
	);
	return (
		<Diagram id={id} width={width} height={height} label={label}>
			{lanes.map((l) => (
				<g key={l.title}>
					<Box x={l.x - 62} y={8} w={124} h={40} title={l.title} sub={l.sub} kind={l.kind} />
					<path className="gp-d-lifeline" d={`M${l.x},48 L${l.x},${l.until ?? height - 10}`} />
				</g>
			))}
			{children(msg)}
		</Diagram>
	);
}

/** One turn with one tool call, as the processes see it. */
export function Flow() {
	const lanes: Lane[] = [
		{ x: 70, title: 'browser', kind: 'outside' },
		{ x: 220, title: 'web front', sub: 'not a process', kind: 'plain' },
		{ x: 370, title: 'conversation', sub: 'and its topic', kind: 'actor' },
		{ x: 520, title: 'generation', sub: 'on a GPU node', kind: 'actor' },
		{ x: 660, title: 'tool call', sub: 'on the sandbox', kind: 'actor' }
	];
	return (
		<Sequence id="flow" width={730} height={476} label="The calls, the published events and the answers of one turn" lanes={lanes}>
			{(msg) => (
				<>
					{msg(0, 1, 82, 'POST what the user says')}
					{msg(1, 2, 114, 'Call Say')}
					{msg(2, 1, 146, 'Said: the turn has begun', 'dashed')}
					{msg(2, 3, 178, 'Call Generate')}
					{msg(3, 2, 210, 'Publish token, token…')}
					{msg(2, 0, 242, 'each event, to whoever follows', 'dashed')}
					{msg(3, 2, 274, 'Generated: run this tool', 'dashed')}
					{msg(2, 4, 306, 'Call Run, under a deadline')}
					{msg(4, 2, 338, 'Ran: the output, or why not', 'dashed')}
					{msg(2, 3, 370, 'Call Generate, again')}
					{msg(3, 2, 402, 'tokens, then Generated', 'dashed')}
					{msg(2, 0, 434, 'done', 'dashed')}
				</>
			)}
		</Sequence>
	);
}

/** A GPU node lost in the middle of an answer, and the answer started over. */
export function Failover() {
	const lanes: Lane[] = [
		{ x: 70, title: 'followers', sub: 'browsers', kind: 'outside' },
		{ x: 260, title: 'conversation', sub: 'and its topic', kind: 'actor' },
		{ x: 460, title: 'gpu-1', sub: 'a generation', kind: 'actor', until: 180 },
		{ x: 650, title: 'gpu-2', sub: 'a generation', kind: 'actor' }
	];
	return (
		<Sequence id="failover" width={730} height={412} label="A generation lost with its node, and started over on another" lanes={lanes}>
			{(msg) => (
				<>
					{msg(1, 2, 82, 'Call Generate')}
					{msg(2, 1, 114, 'Publish token')}
					{msg(1, 0, 146, 'token', 'dashed')}
					<Note x={460} y={198} anchor="middle">
						the node is lost
					</Note>
					{msg(2, 1, 230, 'the link breaks: the call fails', 'dashed')}
					{msg(1, 0, 262, 'retrying: the answer starts over', 'dashed')}
					{msg(1, 3, 294, 'Call Generate')}
					{msg(3, 1, 326, 'Publish every token, from the first')}
					{msg(1, 0, 358, 'tokens', 'dashed')}
					{msg(3, 1, 390, 'Generated', 'dashed')}
				</>
			)}
		</Sequence>
	);
}

/** A conversation's process comes and goes; its history stays. */
export function Lifetime() {
	const id = 'lifetime';
	return (
		<Diagram id={id} width={720} height={216} label="A conversation's process is started when spoken to and ends when idle; its history is kept outside it">
			<Box x={16} y={56} w={130} h={44} title="no process" sub="only its history" kind="outside" />
			<Box x={295} y={56} w={150} h={44} title="conversation/1" sub="and its topic" kind="actor" />
			<Box x={574} y={56} w={130} h={44} title="no process" sub="only its history" kind="outside" />
			<Line id={id} d="M146,78 L293,78" />
			<Note x={220} y={70} anchor="middle">
				Say or Follow
			</Note>
			<Note x={220} y={96} anchor="middle">
				StartChildFrom
			</Note>
			<Line id={id} d="M445,78 L572,78" />
			<Note x={509} y={70} anchor="middle">
				idle
			</Note>
			<Note x={509} y={96} anchor="middle">
				it returns nil
			</Note>
			<Line id={id} d="M410,56 C410,22 330,22 330,54" />
			<Note x={370} y={20} anchor="middle">
				a crash: its supervisor starts it again
			</Note>
			<Box x={16} y={164} w={688} h={40} title="History" sub="every turn, appended as it is said" kind="platform" />
			<Line id={id} d="M340,164 L340,102" />
			<Note x={332} y={138} anchor="end">
				Load, at every start
			</Note>
			<Line id={id} d="M400,100 L400,162" />
			<Note x={408} y={138}>Append, each turn</Note>
		</Diagram>
	);
}

/** The scheduler hands a call to a process, which streams and answers. */
export function Handoff() {
	const id = 'handoff';
	return (
		<Diagram id={id} width={720} height={250} label="The scheduler hands each call to a generation, which publishes its tokens and answers the call">
			<Box x={16} y={50} w={150} h={44} title="conversation/1" sub="on the gateway" kind="actor" />
			<Box x={16} y={170} w={150} h={44} title="its topic" sub="on the gateway" kind="plain" />
			<Frame x={300} y={8} w={412} h={234} title="node gpu-1" />
			<Box x={320} y={44} w={150} h={44} title="models" sub="scheduler · 2 slots" kind="actor" />
			<Box x={320} y={170} w={150} h={44} title="generation" sub="holds the call" kind="actor" />
			<Box x={542} y={170} w={150} h={44} title="generation" sub="of another call" kind="actor" />
			<path className="gp-d-line gp-d-line_tree" d="M395,88 L395,170" />
			<path className="gp-d-line gp-d-line_tree" d="M395,130 L617,130 L617,170" />
			<Note x={405} y={122}>SpawnMonitor, the call handed over</Note>
			<Note x={542} y={62}>a third call:</Note>
			<Note x={542} y={78}>busy, at once</Note>
			<Line id={id} d="M166,66 L318,66" />
			<Note x={242} y={58} anchor="middle">
				Call Generate
			</Note>
			<Line id={id} kind="dashed" d="M318,180 L168,90" />
			<Note x={200} y={146} anchor="end">
				Generated
			</Note>
			<Line id={id} d="M318,198 L168,198" />
			<Note x={242} y={228} anchor="middle">
				Publish each token
			</Note>
		</Diagram>
	);
}

/** What the gateway asks of the sandbox, and what the sandbox may ask back. */
export function Trust() {
	const id = 'trust';
	return (
		<Diagram id={id} width={720} height={190} label="The gateway calls the sandbox and is answered; what the sandbox asks of the gateway is refused">
			<Frame x={8} y={8} w={250} h={174} title="node gateway" />
			<Box x={98} y={44} w={140} h={40} title="conversation/1" kind="actor" />
			<Note x={24} y={166}>UNTRUSTED=sandbox</Note>
			<Frame x={462} y={8} w={250} h={174} title="node sandbox" />
			<Box x={482} y={44} w={140} h={40} title="tools" sub="runner" kind="actor" />
			<Box x={482} y={112} w={140} h={40} title="a tool" sub="gone wrong" kind="outside" />
			<Note x={696} y={166} anchor="end">
				EXPORT=tools
			</Note>
			<Line id={id} d="M238,56 L480,56" />
			<Note x={360} y={48} anchor="middle">
				Call Run
			</Note>
			<Line id={id} kind="dashed" d="M480,74 L240,74" />
			<Note x={360} y={92} anchor="middle">
				Ran: an answer is never judged
			</Note>
			<Line id={id} d="M480,132 L262,132" />
			<Note x={360} y={124} anchor="middle">
				Call, Send, Monitor
			</Note>
			<Note x={360} y={150} anchor="middle">
				refused: no such process
			</Note>
		</Diagram>
	);
}

/** What the platform builds, in order, and the order things stop in. */
export function Lifecycle() {
	const id = 'lifecycle';
	const steps: { title: string; sub: string; kind: Kind }[] = [
		{ title: 'gRPC server', sub: 'and its listener', kind: 'platform' },
		{ title: 'node', sub: 'served on it', kind: 'platform' },
		{ title: 'Inspector', sub: 'beside the node', kind: 'platform' },
		{ title: 'root', sub: "services' trees", kind: 'supervisor' },
		{ title: 'HTTP server', sub: 'web front', kind: 'plain' }
	];
	return (
		<Diagram id={id} width={720} height={170} label="Services start left to right and stop right to left">
			<Line id={id} d="M24,24 L696,24" />
			<Note x={24} y={16}>built, then started, in this order</Note>
			{steps.map((s, i) => (
				<Box key={s.title} x={24 + i * 136} y={46} w={120} h={44} title={s.title} sub={s.sub} kind={s.kind} />
			))}
			<Line id={id} d="M696,118 L24,118" />
			<Note x={696} y={140} anchor="end">
				stopped in reverse, once the HTTP server has drained
			</Note>
			<Note x={696} y={158} anchor="end">
				so the trees stop before the node, and the node before the server
			</Note>
		</Diagram>
	);
}

interface TreeProps {
	x: number;
	y: number;
	services: { name: string; actor: string }[];
}

/** A node's supervision tree: the root, a supervisor per service, its process. */
export function Tree({ x, y, services }: TreeProps) {
	const gap = 126;
	const left = x - ((services.length - 1) * gap) / 2;
	return (
		<g>
			<Box x={x - 44} y={y} w={88} h={30} title="root" kind="supervisor" />
			{services.map((s, i) => {
				const cx = left + i * gap;
				return (
					<g key={s.name}>
						<path className="gp-d-line gp-d-line_tree" d={`M${x},${y + 30} L${x},${y + 42} L${cx},${y + 42} L${cx},${y + 54}`} />
						<Box x={cx - 58} y={y + 54} w={116} h={30} title={s.name} kind="supervisor" />
						<path className="gp-d-line gp-d-line_tree" d={`M${cx},${y + 84} L${cx},${y + 100}`} />
						<Box x={cx - 58} y={y + 100} w={116} h={30} title={s.actor} kind="actor" />
					</g>
				);
			})}
		</g>
	);
}

/** The local deployment: one process, one node, every service. */
export function LocalPicture() {
	const id = 'local';
	return (
		<Diagram id={id} width={720} height={300} label="One program running one node with every service">
			<Box x={8} y={132} w={98} h={36} title="browser" kind="outside" />
			<Box x={8} y={236} w={98} h={36} title="grpcprocctl" kind="outside" />
			<Frame x={124} y={8} w={588} h={284} title="cmd/local · node local" />
			<Box x={144} y={132} w={150} h={36} title="web front" sub=":8080" kind="plain" />
			<Box x={144} y={236} w={150} h={36} title="gRPC server" sub=":9100 · Inspector" kind="platform" />
			<Tree
				x={522}
				y={40}
				services={[
					{ name: 'models-sup', actor: 'models' },
					{ name: 'tools-sup', actor: 'tools' },
					{ name: 'conversations', actor: 'conversation/1' }
				]}
			/>
			<Line id={id} d="M106,150 L142,150" />
			<Line id={id} d="M106,254 L142,254" />
			<Line id={id} d="M294,150 L322,150 L322,200 L648,200 L648,172" />
			<Note x={332} y={218}>Say, through the node: no network in between</Note>
		</Diagram>
	);
}

/** The distributed deployment: the same services on four nodes. */
export function ClusterPicture() {
	const id = 'cluster';
	const workers = [
		{ y: 8, cmd: 'cmd/gpu · node gpu-1', title: 'models', sub: 'scheduler', port: ':9102' },
		{ y: 104, cmd: 'cmd/gpu · node gpu-2', title: 'models', sub: 'scheduler', port: ':9103' },
		{ y: 200, cmd: 'cmd/sandbox · node sandbox', title: 'tools', sub: 'runner', port: ':9104' }
	];
	return (
		<Diagram id={id} width={720} height={372} label="Four programs, one node each, linked by gRPC streams">
			<Box x={8} y={116} w={80} h={36} title="browser" kind="outside" />
			<Frame x={100} y={8} w={250} h={250} title="cmd/gateway · node gateway" />
			<Box x={116} y={116} w={100} h={36} title="web front" sub=":8080" kind="plain" />
			<Tree x={286} y={46} services={[{ name: 'conversations', actor: 'conversation/1' }]} />
			<Box x={116} y={206} w={218} h={36} title="gRPC server" sub=":9101 · Inspector" kind="platform" />
			<Line id={id} d="M88,134 L114,134" />
			<Line id={id} d="M216,134 L222,134 L222,161 L228,161" />
			{workers.map((w) => (
				<g key={w.cmd}>
					<Frame x={470} y={w.y} w={242} h={84} title={w.cmd} />
					<Box x={486} y={w.y + 34} w={104} h={36} title={w.title} sub={w.sub} kind="actor" />
					<Box x={604} y={w.y + 34} w={92} h={36} title="gRPC server" sub={w.port} kind="platform" />
					<Line id={id} start d={`M350,${100 + (w.y / 96) * 50} L468,${w.y + 46}`} />
				</g>
			))}
			<Note x={470} y={306}>gRPC links, a stream each way;</Note>
			<Note x={470} y={322}>the sandbox answers, and asks nothing</Note>
			<Box x={100} y={290} w={120} h={36} title="grpcprocctl" kind="outside" />
			<Line id={id} kind="dashed" d="M160,290 L160,244" />
			<Note x={100} y={350}>an Inspector forwards to the node asked about</Note>
		</Diagram>
	);
}
