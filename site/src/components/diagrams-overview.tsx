// The overview's three drawings, which move: a call within a node and across
// a link, a crash and its restart, and a node that is lost. Drawn with the
// primitives of diagrams.tsx. The motion is CSS keyframes written here, next
// to the shapes they move, and emitted in a <style> of the drawing: the page
// runs no script for it. With prefers-reduced-motion nothing moves, and what
// is left is the still drawing with its arrows (main.css, .gp-a).
//
// Each drawing has two layouts: a landscape one for the column at its full
// width, and a compact, portrait one that a phone shows at a size its labels
// still read at. The page renders both, and main.css shows the one that fits.
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
function Msg({ name, cycle, kind, label, dx = 10, dy = 4, anchor = 'start' }: { name: string; cycle: number; kind: MsgKind; label: string; dx?: number; dy?: number; anchor?: 'start' | 'end' }) {
	return (
		<Moving name={name} cycle={cycle} className={`gp-a-msg gp-a-msg_${kind}`}>
			<circle r={5} />
			<text className="gp-d-note" x={dx} y={dy} textAnchor={anchor}>
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

/** Which of a drawing's two layouts to draw. */
interface LayoutProps {
	compact?: boolean;
}

/** One process calls another on its node, then one on another node, with the same line of code. */
export function CallAnywhere({ compact = false }: LayoutProps) {
	const id = compact ? 'ov-call-c' : 'ov-call';
	const cycle = 9;
	const l = compact
		? {
				width: 360,
				height: 326,
				shop: { x: 8, y: 8, w: 344, h: 186 },
				warehouse: { x: 8, y: 222, w: 344, h: 96 },
				desk: { x: 24, y: 44 },
				cashier: { x: 210, y: 120 },
				stock: { x: 210, y: 250 },
				local: [[110, 80], [110, 138], [208, 138]] as Pt[],
				remote: [[70, 80], [70, 268], [208, 268]] as Pt[],
				localNote: { x: 110, y: 178 },
				remoteNote: { x: 24, y: 306 },
				grpc: { x: 120, y: 212 },
				remoteLabel: { dx: -56, dy: 16, anchor: 'start' as const }
			}
		: {
				width: 720,
				height: 232,
				shop: { x: 8, y: 8, w: 392, h: 216 },
				warehouse: { x: 448, y: 8, w: 264, h: 216 },
				desk: { x: 40, y: 52 },
				cashier: { x: 240, y: 150 },
				stock: { x: 540, y: 52 },
				local: [[100, 88], [100, 168], [238, 168]] as Pt[],
				remote: [[160, 70], [538, 70]] as Pt[],
				localNote: { x: 112, y: 208 },
				remoteNote: { x: 170, y: 52 },
				grpc: { x: 424, y: 62 },
				remoteLabel: { dx: 0, dy: 26, anchor: 'start' as const }
			};
	const back = (pts: Pt[]) => [...pts].reverse();
	const d = (pts: Pt[]) => pts.map(([x, y], i) => `${i ? 'L' : 'M'}${x},${y}`).join(' ');
	const css = [
		travel(`${id}-q1`, l.local, 0.5, 1.9, cycle),
		during(`${id}-r1`, 1.9, 2.3, cycle, 1, 0),
		travel(`${id}-a1`, back(l.local), 2.3, 3.7, cycle),
		travel(`${id}-q2`, l.remote, 4.6, 6.2, cycle),
		during(`${id}-r2`, 6.2, 6.6, cycle, 1, 0),
		travel(`${id}-a2`, back(l.remote), 6.6, 8.2, cycle)
	].join('\n');
	return (
		<Diagram id={id} moving compact={compact} width={l.width} height={l.height} label="A process calls one on its own node and one on another node; the call is written the same way, and the second travels a gRPC stream">
			<style dangerouslySetInnerHTML={{ __html: css }} />
			<Line id={id} d={d(l.local)} />
			<Line id={id} d={d(l.remote)} />
			{/* After the lines: a frame's title keeps a halo, and a line that crosses it passes behind. */}
			<Frame {...l.shop} title="node shop" />
			<Frame {...l.warehouse} title="node warehouse" />
			<Note {...l.localNote}>cashier.Call[*Charged](ctx, p, req)</Note>
			<Note {...l.remoteNote}>stock.Call[*Reserved](ctx, p, req)</Note>
			<Note {...l.grpc} anchor="middle">
				gRPC
			</Note>
			<Box {...l.desk} w={120} title="desk" kind="actor" />
			<Box {...l.cashier} w={120} title="cashier" kind="actor" />
			<Box {...l.stock} w={120} title="stock" kind="actor" />
			<Ring name={`${id}-r1`} cycle={cycle} {...l.cashier} w={120} h={36} />
			<Ring name={`${id}-r2`} cycle={cycle} {...l.stock} w={120} h={36} />
			<Msg name={`${id}-q1`} cycle={cycle} kind="request" label="Charge" dy={16} />
			<Msg name={`${id}-a1`} cycle={cycle} kind="reply" label="Charged" dy={16} />
			<Msg name={`${id}-q2`} cycle={cycle} kind="request" label="Reserve" {...l.remoteLabel} />
			<Msg name={`${id}-a2`} cycle={cycle} kind="reply" label="Reserved" {...l.remoteLabel} />
		</Diagram>
	);
}

/** A child exits with an error; its supervisor gets the Down and starts it again under the same name. */
export function CrashRestart({ compact = false }: LayoutProps) {
	const id = compact ? 'ov-crash-c' : 'ov-crash';
	const cycle = 10;
	// The supervisor's centre, its children's, and how wide a child is.
	const l = compact ? { width: 360, mid: 180, kids: [56, 180, 304], w: 84 } : { width: 720, mid: 360, kids: [200, 360, 520], w: 96 };
	const up: Pt[] = [
		[l.mid, 118],
		[l.mid, 48]
	];
	const css = [
		during(`${id}-alive`, 2, 6.4, cycle, 0, 1),
		during(`${id}-boom`, 2, 6.2, cycle, 1, 0),
		travel(`${id}-down`, up, 2.7, 4, cycle),
		during(`${id}-ring`, 4, 4.6, cycle, 1, 0),
		travel(`${id}-spawn`, [...up].reverse(), 5, 6.3, cycle),
		during(`${id}-new`, 6.5, 9.2, cycle, 1, 0)
	].join('\n');
	const child = (cx: number) => ({ x: cx - l.w / 2, y: 118, w: l.w, h: 32 });
	return (
		<Diagram id={id} moving compact={compact} width={l.width} height={196} label="A supervisor over three processes; the middle one exits with an error, the supervisor receives a Down with the reason and starts a new process under the same name">
			<style dangerouslySetInnerHTML={{ __html: css }} />
			{l.kids.map((cx) => (
				<path key={cx} className="gp-d-line gp-d-line_tree" d={`M${l.mid},48 L${l.mid},82 L${cx},82 L${cx},118`} />
			))}
			<Box x={l.mid - 60} y={16} w={120} h={32} title="supervisor" kind="supervisor" />
			<Box {...child(l.kids[0]!)} title="stock" kind="actor" />
			<Box {...child(l.kids[2]!)} title="desk" kind="actor" />
			{/* The child that exits, and the slot it leaves until it is started again. */}
			<Moving name={`${id}-boom`} cycle={cycle} className="gp-a-off">
				<Box {...child(l.mid)} title="" kind="outside" />
			</Moving>
			<Moving name={`${id}-alive`} cycle={cycle}>
				<Box {...child(l.mid)} title="cashier" kind="actor" />
			</Moving>
			<Moving name={`${id}-boom`} cycle={cycle} className="gp-a-off gp-a-danger">
				<Note x={l.mid} y={172} anchor="middle">
					exited: boom
				</Note>
			</Moving>
			<Moving name={`${id}-new`} cycle={cycle} className="gp-a-off">
				<Note x={l.mid} y={172} anchor="middle">
					a new process, under the same name
				</Note>
			</Moving>
			<Ring name={`${id}-ring`} cycle={cycle} x={l.mid - 60} y={16} w={120} h={32} />
			<Msg name={`${id}-down`} cycle={cycle} kind="down" label="Down{boom}" dx={8} />
			<Msg name={`${id}-spawn`} cycle={cycle} kind="spawn" label="start" />
		</Diagram>
	);
}

/** A node is lost; the monitor of a process on it fires on the node that is left. */
export function NodeLost({ compact = false }: LayoutProps) {
	const id = compact ? 'ov-lost-c' : 'ov-lost';
	const cycle = 10;
	const l = compact
		? {
				width: 360,
				height: 250,
				shop: { x: 8, y: 8, w: 344, h: 96 },
				warehouse: { x: 8, y: 146, w: 344, h: 96 },
				desk: { x: 120, y: 40 },
				stock: { x: 120, y: 178 },
				monitor: 'M180,76 L180,176',
				monitorNote: { x: 170, y: 94, anchor: 'end' as const },
				down: [[180, 146], [180, 78]] as Pt[],
				downLabel: { dx: 10, dy: 4 },
				lost: { x: 118, y: 130, anchor: 'end' as const },
				cross: 'M126,118 L142,134 M142,118 L126,134'
			}
		: {
				width: 720,
				height: 200,
				shop: { x: 8, y: 8, w: 330, h: 184 },
				warehouse: { x: 420, y: 8, w: 292, h: 184 },
				desk: { x: 60, y: 82 },
				stock: { x: 520, y: 82 },
				monitor: 'M180,100 L518,100',
				monitorNote: { x: 196, y: 90, anchor: 'start' as const },
				down: [[338, 100], [182, 100]] as Pt[],
				downLabel: { dx: 0, dy: 20 },
				lost: { x: 379, y: 150, anchor: 'middle' as const },
				cross: 'M371,160 L387,176 M387,160 L371,176'
			};
	const css = [
		during(`${id}-gone`, 2.5, 7.6, cycle, 0.25, 1),
		during(`${id}-cut`, 2.5, 7.6, cycle, 1, 0),
		travel(`${id}-down`, l.down, 3.4, 4.7, cycle),
		during(`${id}-ring`, 4.7, 5.3, cycle, 1, 0)
	].join('\n');
	return (
		<Diagram id={id} moving compact={compact} width={l.width} height={l.height} label="A process monitors one on another node; the node is lost, and the monitor fires with reason noconnection">
			<style dangerouslySetInnerHTML={{ __html: css }} />
			<Frame {...l.shop} title="node shop" />
			<Line id={id} kind="dashed" d={l.monitor} />
			<Note {...l.monitorNote}>p.Monitor(stock)</Note>
			<Box {...l.desk} w={120} title="desk" kind="actor" />
			<Moving name={`${id}-gone`} cycle={cycle}>
				<Frame {...l.warehouse} title="node warehouse" />
				<Box {...l.stock} w={120} title="stock" kind="actor" />
			</Moving>
			<Moving name={`${id}-cut`} cycle={cycle} className="gp-a-off gp-a-danger">
				<Note {...l.lost}>link lost</Note>
				<path className="gp-a-cross" d={l.cross} />
			</Moving>
			<Ring name={`${id}-ring`} cycle={cycle} {...l.desk} w={120} h={36} />
			<Msg name={`${id}-down`} cycle={cycle} kind="down" label="Down{noconnection}" {...l.downLabel} />
		</Diagram>
	);
}
