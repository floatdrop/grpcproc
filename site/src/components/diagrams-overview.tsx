// The overview's three drawings, which move: a call within a node and across
// a link, a crash and its restart, and a node that is lost. Drawn with the
// primitives of diagrams.tsx. The motion is CSS keyframes written here, next
// to the shapes they move, and emitted in a <style> of the drawing: the page
// runs no script for it. With prefers-reduced-motion nothing moves, and what
// is left is the still drawing with its arrows (main.css, .gp-a).
import type { ReactNode } from 'react';

import { Box, Diagram, Frame, Line, Note } from './diagrams.tsx';

type Pt = readonly [number, number];

const pct = (t: number, cycle: number) => `${((t / cycle) * 100).toFixed(2)}%`;
const at = ([x, y]: Pt) => `translate(${x}px, ${y}px)`;

/** Keyframes that show an element from `from` to `to` seconds of a cycle, moving it along pts at a steady speed. */
function travel(name: string, pts: readonly Pt[], from: number, to: number, cycle: number): string {
	const first = pts[0]!;
	const last = pts[pts.length - 1]!;
	const lengths = pts.slice(1).map((p, i) => Math.hypot(p[0] - pts[i]![0], p[1] - pts[i]![1]));
	const total = lengths.reduce((a, b) => a + b, 0);
	const frames = [
		`0%, ${pct(from, cycle)} { opacity: 0; transform: ${at(first)}; }`,
		`${pct(from + 0.02, cycle)} { opacity: 1; transform: ${at(first)}; }`
	];
	let done = 0;
	lengths.slice(0, -1).forEach((len, i) => {
		done += len;
		frames.push(`${pct(from + ((to - from) * done) / total, cycle)} { opacity: 1; transform: ${at(pts[i + 1]!)}; }`);
	});
	frames.push(
		`${pct(to, cycle)} { opacity: 1; transform: ${at(last)}; }`,
		`${pct(to + 0.02, cycle)}, 100% { opacity: 0; transform: ${at(last)}; }`
	);
	return `@keyframes ${name} { ${frames.join(' ')} }`;
}

/** Keyframes with one opacity from `from` to `to` seconds of a cycle, and another the rest of it. */
function during(name: string, from: number, to: number, cycle: number, inside: number, outside: number): string {
	return (
		`@keyframes ${name} { 0%, ${pct(from, cycle)} { opacity: ${outside}; } ` +
		`${pct(from + 0.25, cycle)}, ${pct(to, cycle)} { opacity: ${inside}; } ` +
		`${pct(to + 0.25, cycle)}, 100% { opacity: ${outside}; } }`
	);
}

/** An element of a drawing that a keyframes rule animates, once per cycle, for ever. */
function Moving({ name, cycle, className = '', children }: { name: string; cycle: number; className?: string; children: ReactNode }) {
	return (
		<g className={`gp-a ${className}`} style={{ animation: `${name} ${cycle}s linear infinite` }}>
			{children}
		</g>
	);
}

type MsgKind = 'request' | 'reply' | 'down' | 'spawn';

/** A message on its way: a dot, and what it carries. Drawn at the origin; its animation places it. */
function Msg({ name, cycle, kind, label, dx = 10, dy = 4 }: { name: string; cycle: number; kind: MsgKind; label: string; dx?: number; dy?: number }) {
	return (
		<Moving name={name} cycle={cycle} className={`gp-a-msg gp-a-msg_${kind}`}>
			<circle r={5} />
			<text className="gp-d-note" x={dx} y={dy} textAnchor={dx < 0 ? 'end' : 'start'}>
				{label}
			</text>
		</Moving>
	);
}

/** A ring around a box while a message is at it. */
function Ring({ name, cycle, x, y, w, h }: { name: string; cycle: number; x: number; y: number; w: number; h: number }) {
	return (
		<Moving name={name} cycle={cycle} className="gp-a-ring">
			<rect x={x - 3} y={y - 3} width={w + 6} height={h + 6} rx={8} />
		</Moving>
	);
}

/** One process calls another on its node, then one on another node, with the same line of code. */
export function CallAnywhere() {
	const id = 'ov-call';
	const cycle = 9;
	const local: Pt[] = [
		[100, 88],
		[100, 168],
		[238, 168]
	];
	const remote: Pt[] = [
		[160, 70],
		[538, 70]
	];
	const back = (pts: Pt[]) => [...pts].reverse();
	const css = [
		travel(`${id}-q1`, local, 0.5, 1.9, cycle),
		during(`${id}-r1`, 1.9, 2.3, cycle, 1, 0),
		travel(`${id}-a1`, back(local), 2.3, 3.7, cycle),
		travel(`${id}-q2`, remote, 4.6, 6.2, cycle),
		during(`${id}-r2`, 6.2, 6.6, cycle, 1, 0),
		travel(`${id}-a2`, back(remote), 6.6, 8.2, cycle)
	].join('\n');
	return (
		<Diagram id={id} moving width={720} height={232} label="A process calls one on its own node and one on another node; the call is written the same way, and the second travels a gRPC stream">
			<style dangerouslySetInnerHTML={{ __html: css }} />
			<Frame x={8} y={8} w={392} h={216} title="node shop" />
			<Frame x={448} y={8} w={264} h={216} title="node warehouse" />
			<Line id={id} d="M100,88 L100,168 L238,168" />
			<Line id={id} d="M160,70 L538,70" />
			<Note x={112} y={208}>
				cashier.Call[*Charged](ctx, p, req)
			</Note>
			<Note x={170} y={52}>
				stock.Call[*Reserved](ctx, p, req)
			</Note>
			<Note x={424} y={62} anchor="middle">
				gRPC
			</Note>
			<Box x={40} y={52} w={120} title="desk" kind="actor" />
			<Box x={240} y={150} w={120} title="cashier" kind="actor" />
			<Box x={540} y={52} w={120} title="stock" kind="actor" />
			<Ring name={`${id}-r1`} cycle={cycle} x={240} y={150} w={120} h={36} />
			<Ring name={`${id}-r2`} cycle={cycle} x={540} y={52} w={120} h={36} />
			<Msg name={`${id}-q1`} cycle={cycle} kind="request" label="Charge" dy={16} />
			<Msg name={`${id}-a1`} cycle={cycle} kind="reply" label="Charged" dy={16} />
			<Msg name={`${id}-q2`} cycle={cycle} kind="request" label="Reserve" dx={0} dy={26} />
			<Msg name={`${id}-a2`} cycle={cycle} kind="reply" label="Reserved" dx={0} dy={26} />
		</Diagram>
	);
}

/** A child exits with an error; its supervisor gets the Down and starts it again under the same name. */
export function CrashRestart() {
	const id = 'ov-crash';
	const cycle = 10;
	const kids = [
		{ cx: 200, title: 'stock' },
		{ cx: 360, title: 'cashier' },
		{ cx: 520, title: 'desk' }
	];
	const up: Pt[] = [
		[360, 118],
		[360, 48]
	];
	const css = [
		during(`${id}-alive`, 2, 6.4, cycle, 0, 1),
		during(`${id}-boom`, 2, 6.2, cycle, 1, 0),
		travel(`${id}-down`, up, 2.7, 4, cycle),
		during(`${id}-ring`, 4, 4.6, cycle, 1, 0),
		travel(`${id}-spawn`, [...up].reverse(), 5, 6.3, cycle),
		during(`${id}-new`, 6.5, 9.2, cycle, 1, 0)
	].join('\n');
	return (
		<Diagram id={id} moving width={720} height={196} label="A supervisor over three processes; the middle one exits with an error, the supervisor receives a Down with the reason and starts a new process under the same name">
			<style dangerouslySetInnerHTML={{ __html: css }} />
			{kids.map((k) => (
				<path key={k.cx} className="gp-d-line gp-d-line_tree" d={`M360,48 L360,82 L${k.cx},82 L${k.cx},118`} />
			))}
			<Box x={300} y={16} w={120} h={32} title="supervisor" kind="supervisor" />
			<Box x={152} y={118} w={96} h={32} title="stock" kind="actor" />
			<Box x={472} y={118} w={96} h={32} title="desk" kind="actor" />
			{/* The child that exits, and the slot it leaves until it is started again. */}
			<Moving name={`${id}-boom`} cycle={cycle} className="gp-a-off">
				<Box x={312} y={118} w={96} h={32} title="" kind="outside" />
			</Moving>
			<Moving name={`${id}-alive`} cycle={cycle}>
				<Box x={312} y={118} w={96} h={32} title="cashier" kind="actor" />
			</Moving>
			<Moving name={`${id}-boom`} cycle={cycle} className="gp-a-off gp-a-danger">
				<Note x={360} y={172} anchor="middle">
					exited: boom
				</Note>
			</Moving>
			<Moving name={`${id}-new`} cycle={cycle} className="gp-a-off">
				<Note x={360} y={172} anchor="middle">
					a new process, under the same name
				</Note>
			</Moving>
			<Ring name={`${id}-ring`} cycle={cycle} x={300} y={16} w={120} h={32} />
			<Msg name={`${id}-down`} cycle={cycle} kind="down" label="Down{boom}" />
			<Msg name={`${id}-spawn`} cycle={cycle} kind="spawn" label="start" />
		</Diagram>
	);
}

/** A node is lost; the monitor of a process on it fires on the node that is left. */
export function NodeLost() {
	const id = 'ov-lost';
	const cycle = 10;
	const css = [
		during(`${id}-gone`, 2.5, 7.6, cycle, 0.25, 1),
		during(`${id}-cut`, 2.5, 7.6, cycle, 1, 0),
		travel(
			`${id}-down`,
			[
				[338, 100],
				[182, 100]
			],
			3.4,
			4.7,
			cycle
		),
		during(`${id}-ring`, 4.7, 5.3, cycle, 1, 0)
	].join('\n');
	return (
		<Diagram id={id} moving width={720} height={200} label="A process monitors one on another node; the node is lost, and the monitor fires with reason noconnection">
			<style dangerouslySetInnerHTML={{ __html: css }} />
			<Frame x={8} y={8} w={330} h={184} title="node shop" />
			<Line id={id} kind="dashed" d="M180,100 L518,100" />
			<Note x={196} y={90}>
				p.Monitor(stock)
			</Note>
			<Box x={60} y={82} w={120} title="desk" kind="actor" />
			<Moving name={`${id}-gone`} cycle={cycle}>
				<Frame x={420} y={8} w={292} h={184} title="node warehouse" />
				<Box x={520} y={82} w={120} title="stock" kind="actor" />
			</Moving>
			<Moving name={`${id}-cut`} cycle={cycle} className="gp-a-off gp-a-danger">
				<Note x={379} y={150} anchor="middle">
					link lost
				</Note>
				<path className="gp-a-cross" d="M371,160 L387,176 M387,160 L371,176" />
			</Moving>
			<Ring name={`${id}-ring`} cycle={cycle} x={60} y={82} w={120} h={36} />
			<Msg name={`${id}-down`} cycle={cycle} kind="down" label="Down{noconnection}" dx={0} dy={20} />
		</Diagram>
	);
}
