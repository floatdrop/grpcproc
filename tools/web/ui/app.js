'use strict';

// grpcprocctl web: a view per page of the sidebar, each fetching what it
// shows from the API on every tick, and a drawer for one process or one
// saga run. The URL says what is open (#/processes?node=a&proc=<a.1.4>), so
// a view can be shared. Nothing here uses innerHTML: what nodes and processes say about
// themselves is text, and stays text.

// ---- DOM ----------------------------------------------------------------

function h(tag, props, ...kids) {
	const el = document.createElement(tag);
	append(el, kids);
	setProps(el, props); // after the children, so a select's value finds its option
	return el;
}

function svg(tag, props, ...kids) {
	const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
	append(el, kids);
	setProps(el, props);
	return el;
}

function setProps(el, props) {
	for (const [k, v] of Object.entries(props || {})) {
		if (v == null || v === false) continue;
		if (k === 'style') Object.assign(el.style, v);
		else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
		else if (['value', 'checked', 'disabled', 'hidden'].includes(k)) el[k] = v;
		else el.setAttribute(k, v === true ? '' : v);
	}
}

function append(el, kids) {
	for (const kid of [kids].flat(Infinity)) {
		if (kid == null || kid === false || kid === '') continue;
		el.append(kid instanceof Node ? kid : String(kid));
	}
}

const $ = (sel) => document.querySelector(sel);
const cls = (...names) => names.filter(Boolean).join(' ');

// ---- formatting ---------------------------------------------------------

const nf = new Intl.NumberFormat('en-US');
const fmtInt = (n) => (n == null ? '' : nf.format(n));

function fmtRate(v) {
	if (v == null || !isFinite(v)) return '';
	if (v === 0) return '0';
	if (v >= 100) return nf.format(Math.round(v));
	if (v >= 10) return v.toFixed(1).replace(/\.0$/, '');
	return v.toFixed(2).replace(/\.?0+$/, '');
}

// share is a rate of busy_seconds held to 1: the browser's clock and the
// node's differ. fmtPct prints a share, with a decimal below 10%.
const share = (v) => (v == null ? v : Math.min(1, v));
const fmtPct = (v) => (v == null || !isFinite(v) ? '' : `${(share(v) * 100).toFixed(v < 0.1 && v > 0 ? 1 : 0)}%`);

function fmtBytes(n) {
	if (n == null || !isFinite(n)) return '';
	const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
	let i = 0;
	n = Number(n);
	while (n >= 1024 && i < units.length - 1) {
		n /= 1024;
		i++;
	}
	return (i ? n.toFixed(n >= 10 ? 0 : 1) : Math.round(n)) + ' ' + units[i];
}

// parseDur reads a Go duration as the tools print one (1.112s, 3m2s, 512µs)
// into seconds, for sorting.
function parseDur(s) {
	const unit = { h: 3600, m: 60, s: 1, ms: 1e-3, 'µs': 1e-6, us: 1e-6, ns: 1e-9 };
	let t = 0;
	for (const [, n, u] of (s || '').matchAll(/([\d.]+)(h|ms|m|s|µs|us|ns)/g)) t += Number(n) * unit[u];
	return t;
}

function clock(iso) {
	const d = new Date(iso);
	if (isNaN(d)) return iso;
	const p = (n, w = 2) => String(n).padStart(w, '0');
	return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

// A pid as grpcproc prints it: <node.incarnation.id>. Node names may hold
// dots; incarnations and ids do not.
function pidParts(pid) {
	const m = /^<(.*)\.(\d+)\.(\d+)>$/.exec(pid || '');
	return m ? { node: m[1], inc: Number(m[2]), id: Number(m[3]) } : null;
}
const nodeOf = (pid) => pidParts(pid)?.node || '';

// pidText shortens a pid for a table: the incarnation, which only tells a
// node's restarts apart, is left out, and the node's prefix is dimmed, as
// nameEl dims it; on a page about its node, the node is left out too. The
// full pid is its title.
function pidText(pid, whole) {
	const p = pidParts(pid);
	if (!p) return pid;
	return !whole && p.node === pageNode() ? [h('span', { class: 'prefix' }, '<….'), String(p.id), h('span', { class: 'prefix' }, '>')] : ['<', nameEl(p.node), `.….${p.id}>`];
}

// pageNode is the node the page shows, on the pages about one node; ''
// elsewhere. A drawer shows pids whole: it may be about another node.
function pageNode() {
	if (pageNode.hash !== location.hash + home()) {
		const r = route();
		pageNode.hash = location.hash + home();
		pageNode.node = NODE_PAGES.has(r.page) ? r.params.node || home() : '';
	}
	return pageNode.node;
}
const NODE_PAGES = new Set(['node', 'processes', 'tree', 'events']);
// rel says how far a time is from now, as in 3m or 2h ago; its title is the
// time itself.
function rel(iso) {
	if (!iso) return '';
	const d = new Date(iso);
	if (isNaN(d)) return iso;
	const s = (d - Date.now()) / 1000, a = Math.abs(s);
	const n = a < 60 ? `${Math.round(a)}s` : a < 3600 ? `${Math.round(a / 60)}m` : a < 86400 ? `${Math.round(a / 3600)}h` : `${Math.round(a / 86400)}d`;
	return h('span', { title: d.toLocaleString() }, a < 1 ? 'now' : s > 0 ? `in ${n}` : `${n} ago`);
}

const pidSpan = (pid) => h('span', { class: 'mono', title: pid }, pidText(pid));

function pidCmp(a, b) {
	const x = pidParts(a), y = pidParts(b);
	if (!x || !y) return String(a).localeCompare(String(b));
	return x.node.localeCompare(y.node) || x.inc - y.inc || x.id - y.id;
}

function debounce(fn, ms) {
	let t = 0;
	return (...args) => {
		clearTimeout(t);
		t = setTimeout(() => fn(...args), ms);
	};
}

// rates remembers the counters it last saw per key and turns the next into
// per-second rates: null for a key it sees the first time.
function rates() {
	let prev = new Map(), at = 0, last = new Map();
	return (items, key, fields, now = performance.now()) => {
		if (now === at) return last; // the same sample again
		const dt = (now - at) / 1000, next = new Map(), out = new Map();
		for (const it of items) {
			const k = key(it), vals = fields.map((f) => Number(it[f]) || 0), p = prev.get(k);
			next.set(k, vals);
			out.set(k, p && dt > 0 ? Object.fromEntries(fields.map((f, i) => [f, Math.max(0, (vals[i] - p[i]) / dt)])) : null);
		}
		prev = next;
		at = now;
		last = out;
		return out;
	};
}

const sum = (xs, f) => xs.reduce((a, x) => a + (Number(f(x)) || 0), 0);

// middle shortens s to n characters by its middle, where a hash or a
// domain is least telling: its start and its end, a port or a pod's own
// part, stay.
const middle = (s, n) => (s.length <= n ? s : `${s.slice(0, Math.ceil((n - 1) / 2))}…${s.slice(s.length - Math.floor((n - 1) / 2))}`);

// ---- API ----------------------------------------------------------------

function query(params) {
	const q = new URLSearchParams();
	for (const [k, v] of Object.entries(params || {})) if (v !== '' && v != null) q.set(k, v);
	return q.size ? '?' + q : '';
}

async function answer(res) {
	let body = null;
	try {
		body = await res.json();
	} catch {
		// not JSON: a proxy's error page
	}
	if (!res.ok) {
		const e = new Error(body?.error || `${res.status} ${res.statusText}`);
		e.status = res.status;
		throw e;
	}
	return body;
}

// Incarnations are nanoseconds since 1970, past what a JS number holds
// exactly: the page shows none, and tells processes apart by their pid
// strings.
const api = async (path, params) => answer(await fetch(path + query(params), { headers: { Accept: 'application/json' } }));

const post = async (path, body) =>
	answer(await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }));

// act changes something, once confirmed, and says how it went.
async function act(what, path, body, question) {
	if (question && !confirm(question)) return;
	try {
		await post(path, body);
		toast(what);
	} catch (e) {
		toast(`${what}: ${e.message}`, true);
	}
	refresh();
}

function toast(msg, bad) {
	const d = h('div', { class: bad ? 'bad' : '' }, msg);
	$('#toast').append(d);
	setTimeout(() => d.remove(), bad ? 7000 : 3000);
}

// ---- small components ---------------------------------------------------

// Gravity's xmark icon (@gravity-ui/icons, MIT).
const XMARK =
	'M3.47 3.47a.75.75 0 0 1 1.06 0L8 6.94l3.47-3.47a.75.75 0 1 1 1.06 1.06L9.06 8l3.47 3.47a.75.75 0 1 1-1.06 1.06L8 9.06l-3.47 3.47a.75.75 0 0 1-1.06-1.06L6.94 8 3.47 4.53a.75.75 0 0 1 0-1.06';
const icon = (d) =>
	svg('svg', { width: 16, height: 16, viewBox: '0 0 16 16', 'aria-hidden': 'true' }, svg('path', { fill: 'currentColor', 'fill-rule': 'evenodd', 'clip-rule': 'evenodd', d }));

// pill is Gravity's Label, in the theme that says what kind names: a state,
// a link's state, an elector's role, an event's kind, or one of bad, warn.
const LABEL_THEME = {
	idle: 'unknown', running: 'success', 'waiting-reply': 'warning', exiting: 'danger',
	up: 'success', connecting: 'warning', down: 'danger',
	leader: 'success', follower: 'info', candidate: 'warning', unclustered: 'unknown',
	active: 'info', stuck: 'danger', done: 'unknown',
	synced: 'success', syncing: 'warning', absent: 'unknown', unreachable: 'danger',
	spawn: 'success', exit: 'unknown', 'link-up': 'info', 'link-down': 'warning', 'dead-letter': 'danger', 'member-up': 'info', 'member-down': 'warning',
	info: 'info', bad: 'danger', warn: 'warning', uninspected: 'unknown',
};
const pill = (text, kind, title) => h('span', { class: `label label_${LABEL_THEME[kind ?? text] || 'unknown'}`, title }, text);

// stateKey says what the cluster's walk found of node n: down (its error
// says why), unanswered, uninspected (up, but it serves no Inspector), or
// described, its counters its own. STATE says how each shows: its pill
// (text, kind, title; down's text is the node's error), a word where there
// is little room (short), the picker's (word), its rank among what is wrong
// (trouble) and its class on the map.
const stateKey = (n) => (n.error ? 'down' : n.unanswered ? 'unanswered' : n.no_inspector ? 'uninspected' : 'described');
const STATE = {
	down: { kind: 'bad', short: 'down', word: 'unreachable', rank: 5, map: 'bad' },
	unanswered: { text: 'not answered in time', kind: 'warn', short: '—', word: 'not answered', rank: 4, map: 'warn' },
	uninspected: { text: 'no Inspector', kind: 'uninspected', title: 'up, but it serves no Inspector to describe it', short: '—', word: 'no Inspector', rank: 0 },
	described: {},
};
const described = (n) => stateKey(n) === 'described';

// statePill says what node n is, unless described.
function statePill(n) {
	const k = stateKey(n), st = STATE[k];
	return k === 'described' ? '' : k === 'down' ? h('span', { class: 'label label_danger', title: n.error }, errText(n.error, n.name, true)) : pill(st.text, st.kind, st.title);
}

// errText is an error as a table shows it, its whole text its title: Go's
// "rpc error: code = X desc = " left off, the code a tag before the rest
// unless bare, and "inspect: node N: ", which the Inspector puts before a
// peer's error, left off where the row names N.
function errText(msg, node, bare) {
	if (!msg) return '';
	let rest = msg, code = '';
	const g = /^rpc error: code = (\w+) desc = ([\s\S]*)$/.exec(rest);
	if (g) [, code, rest] = g;
	const by = `inspect: node ${node}: `;
	if (node && rest.startsWith(by)) rest = rest.slice(by.length);
	if (rest === msg) return msg;
	return h('span', { title: msg }, code && !bare ? [h('span', { class: 'code' }, code), ' '] : null, rest || code);
}

// head is a page's title and, under it, what the page shows.
const head = (title, lead) => h('header', { class: 'head' }, h('h1', null, title), h('p', { class: 'lead' }, lead));

// select wraps a select for Gravity's chevron.
const select = (el) => h('span', { class: 'select' }, el);
const delta = (v, fmt = fmtRate) => (v > 0 ? h('span', { class: 'delta' }, `+${fmt(v)}/s`) : null);
const card = (k, v, sub, kind) =>
	h('div', { class: cls('card', kind) }, h('div', { class: 'k' }, k), h('div', { class: 'v' }, v), h('div', { class: 's' }, sub));

function banner() {
	const el = h('div', { class: 'banner bad', hidden: true });
	return {
		el,
		set(msg) {
			el.hidden = !msg;
			el.textContent = msg || '';
		},
	};
}

// defs is Gravity's DefinitionList: each [key, value] pair, a missing value
// as none.
const defs = (pairs) => pairs.flatMap(([k, v]) => [h('span', { class: 'k' }, k), h('span', { class: 'v' }, v || h('span', { class: 'muted' }, 'none'))]);

// pidLink opens the process in the drawer, over whatever page is showing;
// whole, its node is named on the node's own page too.
function pidLink(pid, text, whole) {
	if (!pid) return null;
	const r = route();
	return h('a', { class: 'mono', title: pid, href: href(r.page, { ...r.params, proc: pid }), onclick: (e) => e.stopPropagation() }, text || pidText(pid, whole));
}

// linkify turns the pids in a line a process says about itself into links.
const linkify = (s, whole) => s.split(/(<[^<>\s]+\.\d+\.\d+>)/).map((part, i) => (i % 2 ? pidLink(part, null, whole) : part));

// table draws rows under cols: {key, t (title), num, desc, wrap, get(row), sort(row)}.
function table(cols, rows, opt = {}) {
	const { onRow, rowClass, sort, onSort, limit } = opt;
	const head = cols.map((c) =>
		h(
			'th',
			{ class: cls(c.num && 'num', c.sort && onSort && 'sort', sort && sort.key === c.key && 'on'), onclick: c.sort && onSort ? () => onSort(c) : null },
			c.t,
			sort && sort.key === c.key ? (sort.dir < 0 ? ' ↓' : ' ↑') : '',
		),
	);
	const shown = limit ? rows.slice(0, limit) : rows;
	const body = shown.map((r) =>
		h(
			'tr',
			{ class: cls(onRow && 'click', rowClass?.(r)), onclick: onRow ? () => onRow(r) : null },
			cols.map((c) => h('td', { class: cls(c.num && 'num', c.wrap && 'wrap'), style: c.min ? { minWidth: c.min } : null }, c.get(r))),
		),
	);
	return h(
		'div',
		{ class: 'tablewrap' },
		h('table', null, h('thead', null, h('tr', null, head)), h('tbody', null, body)),
		rows.length === 0 ? h('div', { class: 'empty' }, opt.empty || 'Nothing here.') : null,
		limit && rows.length > limit ? h('div', { class: 'more' }, `Showing ${limit} of ${fmtInt(rows.length)}: narrow the scope to see the rest.`) : null,
	);
}

// sorter orders rows by the column the params name: sort=<key>, dir=asc|desc.
function sorter(cols, params) {
	const col = cols.find((c) => c.key === params.sort && c.sort);
	if (!col) return { sort: null, apply: (rows) => rows };
	const dir = params.dir === 'asc' ? 1 : -1;
	const cmp = col.cmp || ((a, b) => (a < b ? -1 : a > b ? 1 : 0));
	return {
		sort: { key: col.key, dir },
		apply: (rows) => rows.map((r) => [col.sort(r), r]).sort((a, b) => dir * cmp(a[0], b[0])).map((x) => x[1]),
	};
}

// A click on a column sorts by it: numbers, and columns marked desc,
// largest first, text A to Z; a second click turns it round.
function sortBy(params) {
	return (c) => {
		const same = params().sort === c.key;
		const dir = same ? (params().dir === 'asc' ? 'desc' : 'asc') : c.num || c.desc ? 'desc' : 'asc';
		set({ sort: c.key, dir });
	};
}

// ---- charts -------------------------------------------------------------

const charts = new Set();

// Chart plots the last minute or so of one or more series, one point per
// tick. A null point (a rate before there are two samples) is a gap.
class Chart {
	constructor(title, series, opt = {}) {
		this.fmt = opt.fmt || fmtRate;
		this.n = opt.points || 60;
		this.series = series.map(([name, color]) => ({ name, color, data: [], val: h('b', null, '–') }));
		this.t = []; // when each sample was taken
		this.canvas = h('canvas');
		this.el = h(
			'div',
			{ class: 'chart' },
			h(
				'div',
				{ class: 'head' },
				h('span', { class: 'title' }, title),
				this.series.map((s) => h('span', { class: 'leg' }, h('i', { class: 'sw', style: { background: `var(${s.color})` } }), s.name, ' ', s.val)),
			),
			this.canvas,
		);
		charts.add(this);
	}

	push(...vals) {
		this.t.push(performance.now());
		if (this.t.length > this.n) this.t.shift();
		this.series.forEach((s, i) => {
			const v = vals[i] ?? null;
			s.data.push(v);
			if (s.data.length > this.n) s.data.shift();
			s.val.textContent = v == null ? '–' : this.fmt(v);
		});
		this.draw();
	}

	draw() {
		const c = this.canvas, w = c.clientWidth, ht = c.clientHeight;
		if (!w || !ht) return;
		const dpr = window.devicePixelRatio || 1;
		if (c.width !== Math.round(w * dpr) || c.height !== Math.round(ht * dpr)) {
			c.width = Math.round(w * dpr);
			c.height = Math.round(ht * dpr);
		}
		const g = c.getContext('2d');
		g.setTransform(dpr, 0, 0, dpr, 0, 0);
		g.clearRect(0, 0, w, ht);
		const css = getComputedStyle(document.documentElement);
		let max = 0, points = 0;
		for (const s of this.series) {
			let n = 0;
			for (const v of s.data) if (v != null) (n++, (max = Math.max(max, v)));
			points = Math.max(points, n);
		}
		// Nothing yet, or nothing but naughts: said in words, not drawn as a
		// scale of 1 over a flat line.
		const quiet = points < 2 ? 'charting from now' : max === 0 ? `none in the last ${fmtSpan((this.t.at(-1) - this.t[0]) / 1000)}` : '';
		max = niceCeil(max);
		const top = 12, bottom = ht - 2, span = bottom - top;
		g.strokeStyle = css.getPropertyValue('--g-color-line-generic');
		g.lineWidth = 1;
		for (const f of [0, 0.5, 1]) {
			const y = Math.round(bottom - f * span) + 0.5;
			g.beginPath();
			g.moveTo(0, y);
			g.lineTo(w, y);
			g.stroke();
		}
		g.fillStyle = css.getPropertyValue('--g-color-text-secondary');
		g.font = '10px ' + css.getPropertyValue('--g-font-family-monospace');
		if (quiet) {
			g.textAlign = 'center';
			g.fillText(quiet, w / 2, top + span / 2);
			g.textAlign = 'start';
		} else g.fillText(this.fmt(max), 2, top - 2);
		const step = w / (this.n - 1);
		for (const s of this.series) {
			g.strokeStyle = css.getPropertyValue(s.color).trim();
			g.lineWidth = 1.6;
			g.lineJoin = 'round';
			g.beginPath();
			const off = this.n - s.data.length;
			let pen = false;
			s.data.forEach((v, i) => {
				if (v == null) {
					pen = false;
					return;
				}
				const x = (off + i) * step, y = bottom - (v / max) * span;
				if (pen) g.lineTo(x, y);
				else g.moveTo(x, y);
				pen = true;
			});
			g.stroke();
		}
	}
}

// fmtSpan says s seconds as a chart's span: 45s, 2m.
const fmtSpan = (s) => (s < 60 ? `${Math.round(s)}s` : `${Math.round(s / 60)}m`);

function niceCeil(v) {
	if (v <= 0) return 1;
	const p = Math.pow(10, Math.floor(Math.log10(v)));
	for (const m of [1, 2, 2.5, 5, 10]) if (v <= m * p) return m * p;
	return 10 * p;
}

function redrawCharts() {
	for (const c of charts) {
		if (c.el.isConnected) c.draw();
		else charts.delete(c);
	}
}

// ---- pages --------------------------------------------------------------

function clusterPage(_node, params) {
	let p = params, data = null, nr = new Map(), lr = new Map(), tr = new Map();
	const b = banner();
	const cards = h('div', { class: 'cards' });
	const map = h('div', { class: 'map' });
	const groupsEl = h('div');
	const list = h('div');
	const count = h('span', { class: 'muted' });
	const groupSel = h('select', { 'aria-label': 'Group by', onchange: (e) => set({ group: e.target.value, q: '', page: '', open: '' }) });
	const filter = h('input', { type: 'search', placeholder: 'name, or key=value', value: p.q || '', oninput: debounce((e) => set({ q: e.target.value, page: '' }), 300) });
	const nodeRates = rates(), linkRates = rates(), totalRates = rates();
	const el = h(
		'div',
		null,
		head('Cluster', 'Every node this Inspector knows of, and what goes between them.'),
		b.el,
		cards,
		map,
		h('div', { class: 'scope' }, h('b', null, 'Group by'), select(groupSel), h('span', { class: 'sep' }), h('label', null, filter), h('span', { class: 'spacer' }), count),
		groupsEl,
		h('h2', null, 'Nodes'),
		list,
	);
	const linkKey = (l) => `${l.from}>${l.peer}>${l.direction}`;
	const totalKey = (t) => `${t.from}>${t.group || ''}`;

	const cols = [
		// The node, and under it where peers dial it and its metadata.
		{
			key: 'node',
			t: 'Node',
			wrap: true,
			get: (n) => [
				h('div', null, nameEl(n.name, true), n === data.nodes[0] ? [' ', pill('inspector', 'info')] : null),
				n.advertise || n.metadata ? h('div', { class: 'under', title: [n.advertise, ...Object.keys(n.metadata || {}).sort().map((k) => `${k}=${n.metadata[k]}`)].filter(Boolean).join('\n') }, n.advertise ? h('span', { class: 'mono' }, n.advertise) : null, chips(n.metadata)) : null,
			],
			sort: (n) => n.name,
		},
		{ key: 'uptime', t: 'Uptime', num: true, get: (n) => n.uptime, sort: (n) => parseDur(n.uptime) },
		{ key: 'processes', t: 'Processes', num: true, get: (n) => fmtInt(described(n) ? n.processes : null), sort: (n) => n.processes || 0 },
		{ key: 'spawns', t: 'Spawns / s', num: true, get: (n) => fmtRate(nr.get(n.name)?.spawned), sort: (n) => nr.get(n.name)?.spawned || 0 },
		{ key: 'exits', t: 'Exits / s', num: true, get: (n) => fmtRate(nr.get(n.name)?.exited), sort: (n) => nr.get(n.name)?.exited || 0 },
		{ key: 'dead', t: 'Dead letters', num: true, get: (n) => [fmtInt(described(n) ? n.dead_letters : null), delta(nr.get(n.name)?.dead_letters)], sort: (n) => nr.get(n.name)?.dead_letters || 0 },
		{ key: 'peers', t: 'Peers', get: (n) => peers(n), sort: (n) => n.peers_down || 0, desc: true },
		{ key: 'error', t: 'Error', wrap: true, get: statePill, sort: (n) => trouble(n, nr), desc: true },
	];

	function render() {
		const nodes = data.nodes, key = p.group || '';
		const states = nodes.map(stateKey), countOf = (k) => states.filter((x) => x === k).length;
		const up = nodes.filter((_, i) => states[i] === 'described' || states[i] === 'uninspected'), seen = nodes.filter((_, i) => states[i] === 'described');
		const unreachable = countOf('down'), unanswered = countOf('unanswered'), uninspected = countOf('uninspected');
		const out = sum(up, (n) => n.outbound), outUp = sum(up, (n) => n.outbound_up);
		const queued = sum(up, (n) => n.queued);
		const deadRate = sum(up, (n) => nr.get(n.name)?.dead_letters);
		cards.replaceChildren(
			card('Nodes', up.length, [unreachable && `${unreachable} unreachable`, unanswered && `${unanswered} not answered in time`, uninspected && `${uninspected} without an Inspector`].filter(Boolean).join(', ') || 'all reachable', unreachable ? 'bad' : unanswered && 'warn'),
			card('Processes', fmtInt(sum(up, (n) => n.processes)), `${fmtInt(sum(up, (n) => n.spawned))} spawned in all`),
			card('Spawns / s', fmtRate(sum(up, (n) => nr.get(n.name)?.spawned)), `exits ${fmtRate(sum(up, (n) => nr.get(n.name)?.exited))}/s`),
			card('Dead letters', fmtInt(sum(up, (n) => n.dead_letters)), deadRate > 0 ? `+${fmtRate(deadRate)}/s` : 'none lately', deadRate > 0 && 'warn'),
			card('Links out', `${outUp} / ${out} up`, queued ? `${fmtInt(queued)} queued` : 'nothing queued', outUp < out && 'warn'),
			card('Messages / s', fmtRate(sum(up, (n) => nr.get(n.name)?.sent)), `${fmtBytes(sum(up, (n) => nr.get(n.name)?.sent_bytes))}/s between nodes`),
		);

		const keys = [...new Set(nodes.flatMap((n) => Object.keys(n.metadata || {})))].sort();
		setKeys(groupSel, keys, key);

		const groups = key ? groupsOf(nodes, key, nr, tr) : [];
		const pick = (g) => set({ q: filterOf(key, g), page: '' });
		if (key && groups.length <= MAP_GROUPS) {
			const opened = openOf(p);
			const isOpen = (g) => opened === ALL || opened.has(g);
			const toggle = (g) => {
				const names = groups.map((x) => x.name);
				const now = new Set(opened === ALL ? names : names.filter((x) => opened.has(x)));
				if (isOpen(g)) now.delete(g);
				else now.add(g);
				set({ open: now.size === groups.length ? '*' : now.size ? JSON.stringify([...now]) : '' });
			};
			const some = groups.some((g) => isOpen(g.name));
			map.replaceChildren(
				h(
					'div',
					{ class: 'caption' },
					`groups by ${key}, and what goes between them`,
					h('span', { class: 'spacer' }),
					h('button', { type: 'button', class: 'flat', onclick: () => set({ open: '*' }) }, 'Open all'),
					h('button', { type: 'button', class: 'flat', disabled: !some, onclick: () => set({ open: '' }) }, 'Close all'),
				),
				groupMap(groups, nodes, key, isOpen, { lr, tr, hereGroup: nodes[0]?.metadata?.[key] ?? '', hereNode: nodes[0]?.name, toggle, pick }),
				h(
					'div',
					{ class: 'legend' },
					h('span', null, 'A circle is a group: click it to open it to its nodes, an opened one’s ring to close it.'),
					h('span', null, 'A line is what two send each other, a head at each end sent to; its width grows with messages per second. Dashed: a link of it is down.'),
				),
			);
		} else if (!key && seen.every((n) => n.links)) {
			const lines = familyLabels(nodes.map((n) => shortName(n.name)));
			map.replaceChildren(
				h('div', { class: 'caption' }, 'nodes and links'),
				drawMap(
					nodes.map((n) => ({ ...n, lines: lines.get(shortName(n.name)) })),
					(l) => lr.get(linkKey(l)),
				),
				h(
					'div',
					{ class: 'legend' },
					h('span', null, 'A line is what two nodes send each other, a head at each end sent to; its width grows with messages per second.'),
					h('span', null, 'Dashed: connecting or down. Click a node to open it.'),
					app.prefix ? h('span', null, 'Names without the ', h('span', { class: 'mono' }, app.prefix), ' they all begin with, and what a deployment’s pods share.') : null,
					nodes.length > 12 && keys.length ? h('span', null, 'Group by a key to fold the map.') : null,
				),
			);
		} else {
			const why = key
				? `${fmtInt(groups.length)} groups are too many to draw: the table below lists them.`
				: `${fmtInt(nodes.length)} nodes are too many to draw: ${keys.length ? 'group them by a key of their metadata to see what goes between the groups.' : "the table below adds each one's links up."}`;
			map.replaceChildren(h('div', { class: 'caption' }, 'nodes and links'), h('div', { class: 'empty' }, why));
		}

		groupsEl.replaceChildren(
			...(key
				? [
						h('h2', null, `Groups by ${key}`),
						table(
							[
								{ t: key, get: (g) => (g.name === '' ? h('span', { class: 'muted' }, 'none') : h('b', null, g.name)) },
								{ t: 'Nodes', get: (g) => [`${fmtInt(g.up)} / ${fmtInt(g.nodes)}`, g.unreachable ? [' ', pill(`${g.unreachable} unreachable`, 'bad')] : null, g.unanswered ? [' ', pill(`${g.unanswered} not answered`, 'warn')] : null, g.uninspected ? [' ', pill(`${g.uninspected} no Inspector`, 'uninspected')] : null] },
								{ t: 'Processes', num: true, get: (g) => fmtInt(g.processes) },
								{ t: 'Spawns / s', num: true, get: (g) => fmtRate(g.spawnRate) },
								{ t: 'Dead letters', num: true, get: (g) => [fmtInt(g.dead), delta(g.deadRate)] },
								{ t: 'Peers down', num: true, get: (g) => (g.peersDown ? pill(fmtInt(g.peersDown), 'down') : '') },
								{ t: 'Queued', num: true, get: (g) => (g.queued ? fmtInt(g.queued) : '') },
								{ t: 'Messages / s', num: true, get: (g) => fmtRate(g.sentRate) },
								{ t: 'Within', num: true, get: (g) => fmtRate(g.out.get(g.name)?.rate) },
							],
							groups,
							{ onRow: (g) => pick(g.name), rowClass: (g) => cls((g.unreachable || g.peersDown || g.queued) && 'backlog', p.q === filterOf(key, g.name) && 'sel') },
						),
					]
				: []),
		);

		const hits = p.q ? nodes.filter((n) => matches(n, p.q)) : nodes;
		const s = sorter(cols, p);
		const ordered = s.sort ? s.apply(hits) : [...hits].sort((a, b) => trouble(b, nr) - trouble(a, nr) || (a === nodes[0] ? -1 : b === nodes[0] ? 1 : natural(a.name, b.name)));
		const pg = pager(ordered, p);
		count.textContent = p.q ? `${fmtInt(hits.length)} of ${fmtInt(nodes.length)} nodes` : `${fmtInt(nodes.length)} nodes`;
		list.replaceChildren(table(cols, pg.rows, { sort: s.sort, onSort: sortBy(() => p), onRow: (n) => go('node', { node: n.name }), empty: nodes.length ? 'No node matches.' : 'No nodes.' }), ...pg.el);
	}

	const page = {
		el,
		banner: b.set,
		update(np) {
			const regroup = (np.group || '') !== (p.group || '');
			p = np;
			if (document.activeElement !== filter) filter.value = p.q || '';
			if (regroup) run(page);
			else if (data) render();
		},
		async tick(stale) {
			const got = await api('/api/nodes', { group: p.group });
			if (stale()) return; // a walk grouped otherwise
			data = got;
			setNodes(data.nodes);
			const at = data.taken_at;
			for (const n of data.nodes) Object.assign(n, linkSums(n));
			// Rates of the nodes described alone: the counters of others are
			// not theirs, and one described again starts its rates afresh.
			nr = nodeRates(data.nodes.filter(described), (n) => n.name, ['spawned', 'exited', 'dead_letters', 'sent', 'sent_bytes'], at);
			lr = linkRates(data.nodes.flatMap((n) => (n.links || []).map((l) => ({ ...l, from: n.name }))), linkKey, ['messages', 'bytes'], at);
			tr = totalRates(data.nodes.flatMap((n) => (n.link_totals || []).map((t) => ({ ...t, from: n.name }))), totalKey, ['messages_sent', 'bytes_sent'], at);
			render();
		},
	};
	return page;
}

// setKeys offers keys to group by in sel, and key, chosen. The options are
// replaced only when they change: replacing them closes a list being read.
function setKeys(sel, keys, key) {
	keys = keys.filter(Boolean); // '' is no grouping
	const want = ['', ...keys, ...(key && !keys.includes(key) ? [key] : [])];
	if (sel.options.length !== want.length || [...sel.options].some((o, i) => o.value !== want[i])) {
		sel.replaceChildren(...want.map((k) => h('option', { value: k }, k || 'nothing')));
	}
	sel.value = key;
}

// openOf is the groups params say the map opens: ALL, or a set of names.
function openOf(params) {
	if (params.open === '*') return ALL;
	try {
		const names = JSON.parse(params.open || '[]');
		return new Set(Array.isArray(names) ? names.map(String) : []);
	} catch {
		return new Set();
	}
}
const ALL = Symbol('all');

// MAP_GROUPS is the most groups the cluster map draws.
const MAP_GROUPS = 24;

// PAGE is how many rows a paged table shows at once.
const PAGE = 100;

// pager shows rows a page at a time, the one params name from 1.
function pager(rows, params) {
	const pages = Math.max(1, Math.ceil(rows.length / PAGE));
	const n = Math.min(pages, Math.max(1, Math.floor(Number(params.page)) || 1));
	const from = (n - 1) * PAGE;
	return {
		rows: rows.slice(from, from + PAGE),
		// none, or the one element: spread into replaceChildren, which would
		// show a null as text
		el:
			pages < 2
				? []
				: [h(
						'div',
						{ class: 'pager' },
						h('button', { type: 'button', disabled: n === 1, onclick: () => set({ page: n - 1 }) }, '‹ Previous'),
						h('span', { class: 'muted' }, `${fmtInt(from + 1)}–${fmtInt(Math.min(rows.length, from + PAGE))} of ${fmtInt(rows.length)}`),
						h('button', { type: 'button', disabled: n === pages, onclick: () => set({ page: n + 1 }) }, 'Next ›'),
					)],
	};
}

// trouble ranks what is wrong with a node, worst first: down, not
// answered, peers down, a queue, dead letters lately. One that serves no
// Inspector has none of these: it ranks as a healthy one.
function trouble(n, nr) {
	const k = stateKey(n);
	if (k !== 'described') return STATE[k].rank;
	return n.peers_down ? 3 : n.queued ? 2 : nr.get(n.name)?.dead_letters > 0 ? 1 : 0;
}

// matches says whether node n matches every word of q: a word with = in it
// is a key of its metadata and the value it must have, in double quotes if
// it holds a space, any other a part of its name. Values are compared
// without their own double quotes, which a filter cannot hold.
function matches(n, q) {
	const unquoted = (v) => v.replaceAll('"', '');
	return (q.match(/(?:[^\s"]+|"[^"]*")+/g) || []).every((w) => {
		const i = w.indexOf('=');
		if (i < 0) return n.name.toLowerCase().includes(w.toLowerCase());
		return unquoted(n.metadata?.[w.slice(0, i)] ?? '') === unquoted(w.slice(i + 1));
	});
}

// filterOf is the filter that matches key's value v, as matches reads it.
const filterOf = (key, v) => `${key}=${/[\s"]/.test(v) || v === '' ? `"${v.replaceAll('"', '')}"` : v}`;

// groupsOf adds nodes up by their value of key, ordered by it; out holds
// what each group's nodes send to each group, by the peers' value.
function groupsOf(nodes, key, nr, tr) {
	const by = new Map();
	for (const n of nodes) {
		const name = n.metadata?.[key] ?? '';
		let g = by.get(name);
		if (!g) by.set(name, (g = { name, nodes: 0, up: 0, unreachable: 0, unanswered: 0, uninspected: 0, processes: 0, dead: 0, deadRate: 0, spawnRate: 0, peersDown: 0, queued: 0, sentRate: 0, out: new Map() }));
		g.nodes++;
		const k = stateKey(n);
		if (k === 'down') g.unreachable++;
		else if (k === 'unanswered') g.unanswered++;
		else if (k === 'uninspected') {
			g.up++;
			g.uninspected++;
		} else {
			const r = nr.get(n.name);
			g.up++;
			g.processes += n.processes || 0;
			g.dead += n.dead_letters || 0;
			g.deadRate += r?.dead_letters || 0;
			g.spawnRate += r?.spawned || 0;
			g.peersDown += n.peers_down || 0;
			g.queued += n.queued || 0;
			g.sentRate += r?.sent || 0;
			for (const t of n.link_totals || []) {
				const to = t.group || '', e = g.out.get(to) || { messages: 0, bytes: 0, rate: 0, down: 0, queued: 0 };
				e.messages += t.messages_sent || 0;
				e.bytes += t.bytes_sent || 0;
				e.rate += tr.get(`${n.name}>${to}`)?.messages_sent || 0;
				e.down += t.outbound_down || 0;
				e.queued += t.queued || 0;
				g.out.set(to, e);
			}
		}
	}
	return [...by.values()].sort((a, b) => natural(a.name, b.name));
}

// natural orders text with the numbers in it by value: s2 before s10.
const natural = (a, b) => a.localeCompare(b, undefined, { numeric: true });

// twoParts says whether a name's rest, its prefix taken off, still has two
// parts around a separator, so relay-0 is never shown as 0.
const twoParts = (rest) => /^[^-._:/].*[-._:/].*[^-._:/]$/.test(rest);

// namePrefix is what every one of two or more names begins with, up to a
// separator, as the pods of one chart share yarco-moq-: shown once, the
// rest of each name tells them apart, and keeps two parts. '' when they
// share nothing.
function namePrefix(names) {
	if (names.length < 2) return '';
	let p = names[0];
	for (const n of names) while (!n.startsWith(p)) p = p.slice(0, -1);
	for (;;) {
		p = p.slice(0, p.search(/[-._:/][^-._:/]*$/) + 1); // through its last separator
		if (!p || names.every((n) => twoParts(n.slice(p.length)))) return p;
		p = p.slice(0, -1); // a name keeps too little: it needs a shorter one
	}
}

// shortName is name without app.prefix, when it begins with it and keeps
// two parts; a peer the prefix was not drawn from may not.
function shortName(name) {
	const rest = app.prefix && name.startsWith(app.prefix) ? name.slice(app.prefix.length) : name;
	return twoParts(rest) ? rest : name;
}

// nameList lists node names, each with its prefix dimmed, commas between.
const nameList = (list) => (list || []).flatMap((n, i) => [i ? ', ' : null, nameEl(n)]);

// nodeLink opens node's page, its prefix dimmed; a click on it goes nowhere
// else, whatever row it is in.
const nodeLink = (node) => h('a', { href: href('node', { node }), onclick: (e) => e.stopPropagation() }, nameEl(node));

// procName shows a process's name with the prefix of node names dimmed
// wherever one is in it, as a topic named for a node has it, and a line
// break allowed after each separator.
function procName(name) {
	if (!name) return '';
	const parts = app.prefix && name.includes(app.prefix) ? name.split(app.prefix) : [name];
	return parts.flatMap((part, i) => (i ? [h('span', { class: 'prefix' }, breakable(app.prefix)), breakable(part)] : [breakable(part)]));
}

// breakable lets a line break after each separator of s: a type's dots,
// a topic's braces and @.
const breakable = (s) => s.split(/(?<=[-._:/@{])/).flatMap((part, i) => (i ? [h('wbr'), part] : [part]));

// labelEl shows a process's label, muted when it is only the type of
// message it takes, which a process spawned without a label has for one.
const labelEl = (x) => (x.label && x.label === x.type ? h('span', { class: 'muted', title: 'no label: the type of message it takes' }, x.label) : x.label);

// nameEl shows a name with its prefix dimmed.
function nameEl(name, strong) {
	const short = shortName(name);
	const rest = strong ? h('b', null, short) : short;
	return short === name ? rest : h('span', null, h('span', { class: 'prefix' }, app.prefix), rest);
}

// MAP_CHARS is the most characters a line of a node's label on the map holds.
const MAP_CHARS = 10;

// fitLabel breaks a label into the one or two lines a node on the map holds:
// at a separator when the rest then fits, or when it fits no other way, and
// then shortened in the middle, since its end is what tells names apart, as
// a pod's does.
function fitLabel(label, width = MAP_CHARS) {
	if (label.length <= width) return [label];
	// the last separator with a line's worth before it and something after
	const at = [...label.slice(1, width + 1).matchAll(/[-._:/]/g)].map((m) => m.index + 1).findLast((i) => i < label.length - 1);
	const fits = (i) => label.length - i <= width; // what follows i fits a line
	const [cut, from] = at && (fits(at + 1) || !fits(width)) ? [at, at + 1] : [width, width];
	const rest = label.slice(from);
	return [label.slice(0, cut), rest.length > width ? middle(rest, width) : rest];
}

// groupState is the state a group is drawn in, as stateKey reads a node:
// described with a node described; else up, undescribed, with one that
// serves no Inspector; else unanswered with one unanswered, which is not
// known to be down; else down. One that is not down but has nodes down is
// marked warn.
function groupState(g) {
	const k = g.up > g.uninspected ? 'described' : g.uninspected ? 'uninspected' : g.unanswered ? 'unanswered' : 'down';
	return { error: k === 'down' ? 'down' : '', unanswered: k === 'unanswered', no_inspector: k === 'uninspected', warn: k !== 'down' && g.unreachable > 0 };
}

// peers says how many of a node's peers are up, and names those that are
// not, up to PEERS_NAMED, without prefix; with its links, its title lists
// them all.
function peers(n) {
	if (!described(n)) return ''; // nothing known of its peers
	const named = (p, s) => {
		const short = shortName(p);
		return pill(short, s, short === p ? null : p);
	};
	let up, down, more, title;
	if (n.links) {
		const seen = new Map();
		for (const l of n.links) {
			const s = seen.get(l.peer);
			seen.set(l.peer, s === 'up' || l.state === 'up' ? 'up' : s || l.state);
		}
		const all = [...seen].sort(([a], [b]) => natural(a, b));
		down = all.filter(([, s]) => s !== 'up');
		up = all.length - down.length;
		more = 0;
		title = all.map(([p, s]) => `${p}: ${s}`).join('\n');
	} else {
		const t = linkSums(n);
		up = t.peers - t.peers_down;
		down = t.down.map((p) => [p, 'down']);
		more = t.peers_down - t.down.length;
	}
	more += Math.max(0, down.length - PEERS_NAMED);
	return h(
		'span',
		{ class: 'row', title },
		up || !down.length ? h('span', null, `${fmtInt(up)} up`) : null,
		...down.slice(0, PEERS_NAMED).map(([p, s]) => named(p, s)),
		more > 0 ? h('span', { class: 'muted' }, `+${fmtInt(more)} down`) : null,
	);
}
const PEERS_NAMED = 3;

// linkSums adds a node's link totals up across groups.
function linkSums(n) {
	const ts = n.link_totals || [];
	const total = (f) => sum(ts, (t) => t[f]);
	return {
		peers: total('peers'), peers_down: total('peers_down'), outbound: total('outbound'), outbound_up: total('outbound_up'), queued: total('queued'),
		sent: total('messages_sent'), sent_bytes: total('bytes_sent'), down: ts.flatMap((t) => t.down || []),
	};
}

// drawMap lays the nodes out around an ellipse (ring), the inspector's
// node first, at the top, and draws what goes between each pair of them
// (pairArrows). A click on a node opens it.
function drawMap(nodes, rateOf) {
	const here = nodes[0]?.name, R = MAP_R;
	const xy = ring(nodes.map(() => R), 30);
	const pos = new Map(nodes.map((node, i) => [node.name, xy[i]]));
	const labelOf = new Map(nodes.map((node) => [node.name, node.label ?? node.name]));
	const ends = new Map(nodes.map((node) => [node.name, { id: node.name, ...pos.get(node.name), r: R }]));
	const directed = [];
	for (const node of nodes) {
		for (const l of node.links || []) {
			if (l.direction !== 'out' || !ends.has(l.peer)) continue;
			const r = rateOf({ ...l, from: node.name });
			const tip = `${labelOf.get(node.name)} → ${labelOf.get(l.peer)}: ${l.state}, ${fmtInt(l.messages)} messages, ${fmtBytes(l.bytes)}` + (l.queued ? `, ${l.queued} queued` : '') + (l.last_error ? `\nlast error: ${l.last_error}` : '');
			directed.push({ a: ends.get(node.name), b: ends.get(l.peer), rate: r?.messages || 0, queued: l.queued || 0, state: l.state, tip });
		}
	}
	const { arrows, labels } = pairArrows(directed, true);
	const circles = nodes.map((node) => {
		const p = pos.get(node.name);
		const queued = (node.links || []).some((l) => l.queued > 0);
		const k = stateKey(node), st = STATE[k];
		const lines = node.lines || fitLabel(node.label ?? shortName(node.name));
		const top = lines.length > 1 ? -8 : -1; // two lines sit higher, the count under them
		return svg(
			'g',
			{ class: cls('node', st.map, queued && 'warn', node.name === here && 'here'), transform: `translate(${p.x},${p.y})`, onclick: () => go('node', { node: node.name }) },
			svg('title', null, k === 'described' ? `${node.name}: ${node.processes} processes, up ${node.uptime}` : `${node.name}: ${k === 'down' ? node.error : st.title || st.text}`),
			svg('circle', { r: R }),
			lines.map((line, i) => svg('text', { y: top + i * 12 }, line)),
			svg('text', { class: 'sub', y: top + lines.length * 12 + 1 }, k === 'described' ? fmtInt(node.processes) : st.short),
		);
	});
	return mapSvg(
		'Nodes and the links between them',
		xy.map((p) => ({ ...p, r: R })),
		arrowMarker(),
		arrows,
		labels,
		circles,
	);
}

// mapSvg is a map: its view box around circles {x, y, r, over}, over the
// room a name takes above one, and at least as large as 800 by 340, so a
// few draw no larger than a map of many.
function mapSvg(label, circles, ...kids) {
	let x0 = Math.min(...circles.map((c) => c.x - c.r)), x1 = Math.max(...circles.map((c) => c.x + c.r));
	let y0 = Math.min(...circles.map((c) => c.y - c.r - (c.over || 0))), y1 = Math.max(...circles.map((c) => c.y + c.r));
	const W = Math.max(800, x1 - x0 + 40), H = Math.max(340, y1 - y0 + 40);
	x0 -= (W - (x1 - x0)) / 2;
	y0 -= (H - (y1 - y0)) / 2;
	return svg(
		'svg',
		{ viewBox: `${x0} ${y0} ${W} ${H}`, preserveAspectRatio: 'xMidYMid meet', role: 'img', 'aria-label': label, style: { aspectRatio: `${W} / ${H}`, maxHeight: `${Math.min(640, H)}px` } },
		...kids,
	);
}

// pairArrows draws what goes between two ends of a map as one line, a
// head at each end sent to, as wide as both ways add up to; its title says
// each way, its label, when labelled, the messages per second. Two curves
// a pair would make a star, every node sending to one and back, a tangle.
// An end is {id, x, y, r}; an edge {a, b, rate, queued, state, tip}.
function pairArrows(directed, labelled) {
	const pairs = new Map();
	for (const e of directed) {
		const [a, b] = e.a.id < e.b.id ? [e.a, e.b] : [e.b, e.a];
		const k = `${a.id}\n${b.id}`;
		if (!pairs.has(k)) pairs.set(k, { a, b, ways: [] });
		pairs.get(k).ways.push(e);
	}
	const rank = { up: 0, connecting: 1, down: 2 };
	const arrows = [], labels = [];
	for (const { a, b, ways } of pairs.values()) {
		const rate = sum(ways, (e) => e.rate), queued = sum(ways, (e) => e.queued);
		const state = ways.reduce((s, e) => ((rank[e.state] ?? 1) > rank[s] ? e.state : s), 'up');
		const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1, ux = dx / len, uy = dy / len;
		const sx = a.x + ux * (a.r + 3), sy = a.y + uy * (a.r + 3), ex = b.x - ux * (b.r + 3), ey = b.y - uy * (b.r + 3);
		arrows.push(
			svg(
				'path',
				{ class: cls('link', state), d: `M${sx},${sy} L${ex},${ey}`, 'stroke-width': arrowWidth(rate), 'marker-end': ways.some((e) => e.b === b) ? 'url(#arrow)' : null, 'marker-start': ways.some((e) => e.b === a) ? 'url(#arrow-back)' : null },
				svg('title', null, ways.map((e) => e.tip).join('\n')),
			),
		);
		if (labelled && (rate > 0 || queued)) labels.push(svg('text', { class: 'rate', x: (sx + ex) / 2 - uy * 9, y: (sy + ey) / 2 + ux * 9 + 4 }, `${fmtRate(rate)}/s` + (queued ? ` q${fmtInt(queued)}` : '')));
	}
	return { arrows, labels };
}

// MAP_LABELLED is the most arrows a map labels with their rates: more crowd
// it, and their titles still say.
const MAP_LABELLED = 16;

// arrowWidth grows with messages per second.
const arrowWidth = (rate) => 1.5 + Math.min(5, Math.log10(1 + (rate || 0)) * 1.4);

// arrowMarker is the heads arrows end in: at the end of a line, and at its
// start, where it is drawn the other way.
const arrowMarker = () =>
	svg(
		'defs',
		null,
		svg('marker', { id: 'arrow', viewBox: '0 0 10 10', refX: 8, refY: 5, markerWidth: 9, markerHeight: 9, markerUnits: 'userSpaceOnUse', orient: 'auto' }, svg('path', { d: 'M0,0 L10,5 L0,10 z' })),
		svg('marker', { id: 'arrow-back', viewBox: '0 0 10 10', refX: 2, refY: 5, markerWidth: 9, markerHeight: 9, markerUnits: 'userSpaceOnUse', orient: 'auto' }, svg('path', { d: 'M10,0 L0,5 L10,10 z' })),
	);

// familyLabels gives each of names the lines the map labels it with, of
// width characters at most. Names of as many parts that begin with the same
// word, as the pods of a deployment do, relay-74b96c87fb-zn57s and
// relay-74b96c87fb-q8hnh, are that word over the parts that tell them
// apart, the others left out: relay / zn57s; web-1.dc1.example.com is
// web / 1. Others, and names that begin with a number, as addresses do,
// are fitted as fitLabel fits them.
function familyLabels(names, width = MAP_CHARS) {
	const parsed = new Map(
		names.map((n) => {
			const t = n.split(/([-._:/])/); // parts, and the separator before each
			return [n, { parts: t.filter((_, i) => i % 2 === 0), seps: ['', ...t.filter((_, i) => i % 2)] }];
		}),
	);
	// A family is the names that begin with one word, all of as many parts:
	// relay-74b96c87fb-zn57s beside relay-edge-5f6d7c8b9a-x5l58 makes none,
	// as their first word would not tell them apart. The parts that tell a
	// family's names apart are those not alike in all of them.
	const kin = (n, parts) => parts.length > 1 && /^[a-z]/i.test(parts[0]) && !ADDRESS.test(n);
	const families = new Map();
	for (const [n, { parts }] of parsed) {
		if (!kin(n, parts)) continue;
		if (!families.has(parts[0])) families.set(parts[0], { first: parts, names: 0, mixed: false, told: new Set() });
		const f = families.get(parts[0]);
		f.names++;
		f.mixed ||= parts.length !== f.first.length;
		parts.forEach((x, i) => i && x !== f.first[i] && f.told.add(i));
	}
	const out = new Map();
	for (const n of names) {
		const { parts, seps } = parsed.get(n), f = kin(n, parts) && families.get(parts[0]);
		const told = f && f.names > 1 && !f.mixed ? [...f.told].sort((a, b) => a - b) : [];
		if (!told.length) {
			out.set(n, fitLabel(n, width));
			continue;
		}
		const text = told.map((i, j) => (j ? seps[i] : '') + parts[i]).join('');
		// family: the word the names begin with, which a ring of them may say
		out.set(n, Object.assign([middle(parts[0], width), tell(text, parts[told.at(-1)], width)], { family: parts[0] }));
	}
	return out;
}

// ADDRESS is a name that is an address, IPv4 or IPv6, with a port or not:
// its parts are numbers, no family.
const ADDRESS = /^(\[?[0-9a-f]*:[0-9a-f:.]*\]?|\d+(\.\d+){3})(:\d+)?$/i;

// tell fits what tells a name apart in width characters: its last part
// whole, a pod's own, and what goes before it shortened, a deployment's
// hash in a rollout.
function tell(text, last, width) {
	if (text.length <= width) return text;
	const room = width - last.length - 1;
	return room >= 2 ? `${text.slice(0, room - 1)}…${text.slice(text.length - last.length - 1)}` : middle(last, width);
}

// MAP_SMALL is the radius of a node in a group the map has opened, which
// opens to at most MAP_OPEN.
const MAP_SMALL = 24, MAP_OPEN = 32;
// MAP_R is the radius of a node, or a group, drawn whole.
const MAP_R = 34;

// groupMap draws the groups of nodes by a key, each a circle, and the ones
// open names opened to their nodes; and what goes between what it draws:
// by the nodes' links when the walk has them all, else by each node's
// links added up by the group of the peer, so to a whole opened group. A
// click on a group opens it, on an opened one's name or ring closes it; on
// a node opens its page.
function groupMap(groups, nodes, key, open, opt) {
	const { lr, tr, hereGroup, hereNode, toggle, pick } = opt;
	const gap = 44, pad = 14;
	const groupOf = new Map(nodes.map((n) => [n.name, n.metadata?.[key] ?? '']));
	const members = new Map(groups.map((g) => [g.name, []]));
	for (const n of nodes) members.get(groupOf.get(n.name))?.push(n);
	// Each group is a slot: a circle, or a ring around its nodes laid out
	// on a sunflower's spiral, which packs them evenly in a disc.
	const slots = groups.map((g) => {
		const ms = members.get(g.name).sort((a, b) => natural(a.name, b.name));
		if (!open(g.name) || ms.length > MAP_OPEN) return { g, r: MAP_R, ms: null };
		const step = (2 * MAP_SMALL + 10) / 1.77;
		const at = ms.map((n, i) => {
			const rr = ms.length === 1 ? 0 : step * Math.sqrt(i + 0.5), a = i * 2.39996;
			return { n, dx: rr * Math.cos(a), dy: rr * Math.sin(a) };
		});
		return { g, r: Math.max(...at.map((p) => Math.hypot(p.dx, p.dy))) + MAP_SMALL + pad, ms: at };
	});
	// An opened group's name is over its ring: room is made for it.
	const xy = ring(slots.map((s) => s.r + (s.ms ? OVER : 0)), gap);
	slots.forEach((s, i) => Object.assign(s, xy[i]));

	// What an arrow can end at: a node of an opened group, a group's circle,
	// or an opened group's ring.
	const nodeAt = new Map(), groupAt = new Map();
	for (const s of slots) {
		groupAt.set(s.g.name, { id: `g:${s.g.name}`, group: s.g.name, x: s.x, y: s.y, r: s.r, label: s.g.name || 'none' });
		for (const m of s.ms || []) nodeAt.set(m.n.name, { id: `n:${m.n.name}`, group: s.g.name, x: s.x + m.dx, y: s.y + m.dy, r: MAP_SMALL, label: shortName(m.n.name) });
	}
	const at = (name) => nodeAt.get(name) || (groupOf.has(name) ? groupAt.get(groupOf.get(name)) : undefined);
	const edges = new Map();
	const add = (a, b, f) => {
		// Not within a group drawn whole, nor from a node to its own ring.
		if (!a || !b || a === b || b.id === `g:${a.group}`) return;
		const k = `${a.id}>${b.id}`;
		if (!edges.has(k)) edges.set(k, { a, b, messages: 0, bytes: 0, rate: 0, queued: 0, up: 0, down: 0 });
		f(edges.get(k));
	};
	const linked = nodes.every((n) => !described(n) || n.links);
	for (const n of nodes) {
		if (linked) {
			for (const l of n.links || [])
				if (l.direction === 'out')
					add(at(n.name), at(l.peer), (e) => {
						e.messages += l.messages || 0;
						e.bytes += l.bytes || 0;
						e.rate += lr.get(`${n.name}>${l.peer}>out`)?.messages || 0;
						e.queued += l.queued || 0;
						l.state === 'up' ? e.up++ : e.down++;
					});
		} else {
			for (const t of n.link_totals || [])
				add(at(n.name), groupAt.get(t.group || ''), (e) => {
					e.messages += t.messages_sent || 0;
					e.bytes += t.bytes_sent || 0;
					e.rate += tr.get(`${n.name}>${t.group || ''}`)?.messages_sent || 0;
					e.queued += t.queued || 0;
					e.up += t.outbound_up || 0;
					e.down += t.outbound_down || 0;
				});
		}
	}

	const hulls = [], circles = [];
	const directed = [...edges.values()]
		.filter((e) => e.messages > 0 || e.down || e.queued)
		.map((e) => ({
			...e,
			state: e.down ? (e.up ? 'connecting' : 'down') : 'up',
			tip: `${e.a.label} → ${e.b.label}: ${fmtInt(e.messages)} messages, ${fmtBytes(e.bytes)}` + (e.down ? `, ${fmtInt(e.down)} of ${fmtInt(e.up + e.down)} links down` : '') + (e.queued ? `, ${fmtInt(e.queued)} queued` : ''),
		}));
	const { arrows, labels: rateLabels } = pairArrows(directed, directed.length <= MAP_LABELLED);
	const counted = (g) => `${fmtInt(g.up)} of ${fmtInt(g.nodes)} nodes up${g.uninspected ? `, ${fmtInt(g.uninspected)} without an Inspector` : ''}, ${fmtInt(g.processes)} processes`;
	const view = new Map(groups.map((g) => [g.name, groupState(g)]));
	// Nodes are labelled as on the map of nodes, over the cluster: in a
	// group of one deployment, what tells them apart alone.
	const labels = familyLabels(nodes.map((n) => shortName(n.name)), 7);
	for (const s of slots) {
		const name = s.g.name || 'none', v = view.get(s.g.name);
		if (!s.ms) {
			const k = stateKey(v), st = STATE[k], big = members.get(s.g.name).length > MAP_OPEN;
			const lines = fitLabel(name);
			const top = lines.length > 1 ? -8 : -1;
			circles.push(
				svg(
					'g',
					{ class: cls('node', st.map, v.warn && 'warn', s.g.name === hereGroup && 'here'), transform: `translate(${s.x},${s.y})`, onclick: () => (big ? pick(s.g.name) : toggle(s.g.name)) },
					svg('title', null, `${name}: ${counted(s.g)}. ${big ? `Too many to open on the map: click to list them.` : 'Click to open it.'}`),
					svg('circle', { r: MAP_R }),
					lines.map((line, i) => svg('text', { y: top + i * 12 }, line)),
					svg('text', { class: 'sub', y: top + lines.length * 12 + 1 }, `×${fmtInt(s.g.nodes)}`),
				),
			);
			continue;
		}
		// A group of one deployment, two or more nodes of one family, says
		// the family over its ring once, and its nodes what tells them apart.
		const fams = new Set(s.ms.map((m) => labels.get(shortName(m.n.name)).family));
		const one = s.ms.length > 1 && fams.size === 1 && !fams.has(undefined) ? [...fams][0] : '';
		// The ring's name fits its width: the group's value is shortened, the
		// family kept while there is room for both.
		const fit = Math.max(8, Math.floor((2 * s.r) / 6.6)), count = ` ×${fmtInt(s.g.nodes)}`;
		const also = one && one !== name ? ` · ${one}` : '';
		const keep = also && fit - also.length - count.length - 2 >= Math.min(4, name.length) ? also : '';
		const named = `▾ ${middle(name, Math.max(3, fit - keep.length - count.length - 2))}${keep}${count}`;
		const over = one && (keep || one === name); // the family is said over the ring
		hulls.push(
			svg(
				'g',
				{ class: cls('hull', v.warn && 'warn', stateKey(v) === 'down' && 'bad'), transform: `translate(${s.x},${s.y})`, onclick: () => toggle(s.g.name) },
				svg('title', null, `${name}: ${counted(s.g)}. Click to close it.`),
				svg('circle', { r: s.r }),
				svg('text', { y: -s.r - 6 }, named),
			),
		);
		for (const m of s.ms) {
			const n = m.n, k = stateKey(n), st = STATE[k], all = labels.get(shortName(n.name));
			const lines = over ? all.slice(1) : all; // the family once, over the ring, if it is
			circles.push(
				svg(
					'g',
					{ class: cls('node', 'small', st.map, (n.links || []).some((l) => l.queued > 0) && 'warn', n.name === hereNode && 'here'), transform: `translate(${s.x + m.dx},${s.y + m.dy})`, onclick: () => go('node', { node: n.name }) },
					svg('title', null, k === 'described' ? `${n.name}: ${n.processes} processes, up ${n.uptime}` : `${n.name}: ${k === 'down' ? n.error : st.title || st.text}`),
					svg('circle', { r: MAP_SMALL }),
					lines.map((line, i) => svg('text', { y: (lines.length > 1 ? -7 : -2) + i * 11 }, line)),
					svg('text', { class: 'sub', y: lines.length > 1 ? 15 : 10 }, k === 'described' ? fmtInt(n.processes) : st.short),
				),
			);
		}
	}
	return mapSvg(`Groups by ${key} and what goes between them`, slots.map((s) => ({ x: s.x, y: s.y, r: s.r, over: s.ms ? OVER : 0 })), arrowMarker(), hulls, arrows, rateLabels, circles);
}

// OVER is the room an opened group's name takes over its ring.
const OVER = 22;

// around places circles as ring says, on an ellipse grow times the least
// that holds them.
function around(radii, gap, grow) {
	const n = radii.length;
	if (n === 1) return [{ x: 0, y: 0 }];
	if (n === 2) {
		const d = Math.max(radii[0] + radii[1] + gap * 2, 320);
		return [{ x: -d / 2, y: 0 }, { x: d / 2, y: 0 }];
	}
	const need = radii.reduce((s, r) => s + 2 * r + gap, 0);
	const big = Math.max(...radii);
	// At least as wide as the map of nodes draws a few.
	let a = Math.max(need / 4.844, 2 * big + gap, 220), b = Math.max(a / 2, big + gap / 2, 110);
	// Ramanujan's perimeter; the ellipse grows until the circles fit on it.
	const perimeter = () => Math.PI * (3 * (a + b) - Math.sqrt((3 * a + b) * (a + 3 * b)));
	const k = Math.max(1, need / perimeter()) * grow;
	a *= k;
	b *= k;
	// Arc length, sampled, from the top clockwise.
	const N = 720, pts = [], len = [0];
	for (let i = 0; i <= N; i++) {
		const t = -Math.PI / 2 + (i * 2 * Math.PI) / N;
		pts.push({ x: a * Math.cos(t), y: b * Math.sin(t) });
		if (i) len.push(len[i - 1] + Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y));
	}
	const spare = (len[N] - need) / n;
	const out = [];
	let s = 0;
	for (let i = 0; i < n; i++) {
		if (i) s += radii[i - 1] + gap + spare + radii[i];
		const j = Math.min(N, len.findIndex((l) => l >= s));
		out.push(pts[j < 0 ? N : j]);
	}
	return out;
}

// ring lays circles of radii out around an ellipse twice as wide as it is
// tall, the first at the top, each a gap from the next along it, and grows
// the ellipse until no two are nearer than half a gap: on its bends a
// chord is shorter than the arc. One is centred, two side by side.
function ring(radii, gap) {
	if (radii.length < 3) return around(radii, gap, 1);
	for (let grow = 1; ; grow *= 1.08) {
		const out = around(radii, gap, grow);
		const apart = out.every((p, i) => out.every((q, j) => j <= i || Math.hypot(p.x - q.x, p.y - q.y) >= radii[i] + radii[j] + gap / 2));
		if (apart || grow > 4) return out;
	}
}

function nodePage(node, params) {
	let p = params, n = null, ld = new Map();
	const b = banner();
	const cards = h('div', { class: 'cards' });
	const links = h('div');
	const groupsEl = h('div', { class: 'groups' });
	const count = h('span', { class: 'muted' });
	const groupSel = h('select', { 'aria-label': 'Group links by', onchange: (e) => set({ group: e.target.value, q: '', page: '' }) });
	const filter = h('input', { type: 'search', placeholder: 'peer, or key=value', value: p.q || '', oninput: debounce((e) => set({ q: e.target.value, page: '' }), 300) });
	const cProc = new Chart('Processes', [['running', '--c1']], { fmt: fmtInt });
	const cLife = new Chart('Spawns and exits / s', [['spawns', '--c3'], ['exits', '--c2']]);
	const cDead = new Chart('Dead letters / s', [['dead letters', '--c5']]);
	const cMsgs = new Chart('Messages / s between nodes', [['out', '--c1'], ['in', '--c4']]);
	const cBytes = new Chart('Bytes / s between nodes', [['out', '--c1'], ['in', '--c4']], { fmt: fmtBytes });
	const nr = rates(), lr = rates();
	// What a node serving no Inspector has none of: hidden until the node
	// is known to serve one.
	const shown = h(
		'div',
		null,
		h('div', { class: 'charts' }, cProc.el, cLife.el, cDead.el, cMsgs.el, cBytes.el),
		h('h2', null, 'Links'),
		h('div', { class: 'scope' }, h('b', null, 'Group by'), select(groupSel), h('span', { class: 'sep' }), h('label', null, filter), h('span', { class: 'spacer' }), count),
		groupsEl,
		links,
		h('p', null, h('a', { href: href('processes', { node }) }, 'Processes on this node →'), '   ', h('a', { href: href('events', { node }) }, 'Events →')),
	);
	shown.hidden = true;
	const LEAD = 'Its counters and its links to peers, charted over the last minute.';
	const top = head(['Node ', h('span', { class: 'mono' }, node || '(inspector)')], LEAD);
	// Where peers dial it and its metadata, under the title.
	const about = h('div', { class: 'under about' });
	const el = h('div', null, top, about, b.el, cards, shown);
	const linkKey = (l) => `${l.peer}>${l.direction}`;
	const rate = (l, f) => (l ? ld.get(linkKey(l))?.[f] : undefined);
	// A row a peer: out is the link the node dialed and sends to it on, in
	// the one the peer dialed, which the node receives on.
	const cols = [
		{ key: 'peer', t: 'Peer', get: (r) => [nodeLink(r.peer), r.out?.incarnation || r.in?.incarnation ? null : h('span', { class: 'muted' }, ' never reached')], sort: (r) => r.peer },
		{ key: 'out', t: 'Out', get: (r) => (r.out ? pill(r.out.state) : ''), sort: (r) => r.out?.state || '' },
		{ key: 'in', t: 'In', get: (r) => (r.in ? pill(r.in.state) : ''), sort: (r) => r.in?.state || '' },
		{ key: 'age', t: 'Age', num: true, get: (r) => (r.out || r.in).age, sort: (r) => parseDur((r.out || r.in).age) },
		{ key: 'queued', t: 'Queued', num: true, get: (r) => (r.out?.queued ? [fmtInt(r.out.queued), h('span', { class: 'muted' }, ` ${fmtBytes(r.out.queued_bytes)}`)] : ''), sort: (r) => r.out?.queued || 0 },
		{ key: 'sent', t: 'Sent', num: true, get: (r) => (r.out ? [fmtInt(r.out.messages), delta(rate(r.out, 'messages'))] : ''), sort: (r) => rate(r.out, 'messages') || 0 },
		{ key: 'received', t: 'Received', num: true, get: (r) => (r.in ? [fmtInt(r.in.messages), delta(rate(r.in, 'messages'))] : ''), sort: (r) => rate(r.in, 'messages') || 0 },
		{ key: 'bytes', t: 'Bytes out / in', num: true, get: (r) => `${r.out ? fmtBytes(r.out.bytes) : '–'} / ${r.in ? fmtBytes(r.in.bytes) : '–'}`, sort: (r) => (rate(r.out, 'bytes') || 0) + (rate(r.in, 'bytes') || 0) },
		{ key: 'reconnects', t: 'Reconnects', num: true, get: (r) => fmtInt(r.out?.reconnects), sort: (r) => r.out?.reconnects || 0 },
		{ key: 'retry', t: 'Retry in', get: (r) => r.retry },
		{ key: 'error', t: 'Last error', wrap: true, get: (r) => errText(r.out?.last_error || r.in?.last_error, r.peer), sort: (r) => r.out?.last_error || r.in?.last_error || '' },
	];
	// peerRows pairs a node's links by peer, the one up each way if one is:
	// a peer dials failed to has a down entry, with its retry, beside the
	// link that came up since.
	function peerRows(ls) {
		const by = new Map();
		for (const l of ls) {
			let r = by.get(l.peer);
			if (!r) by.set(l.peer, (r = { peer: l.peer, out: null, in: null, retry: '' }));
			const way = l.direction === 'out' ? 'out' : 'in', was = r[way];
			if (!was || (was.state !== 'up' && l.state === 'up')) r[way] = l;
			if (l.retry_in) r.retry = l.retry_in;
		}
		return [...by.values()];
	}

	// renderLinks lists the node's links: grouped by a key of the peers'
	// metadata, as the node's Membership reports it, filtered, down and
	// queued ones first, a page at a time.
	function renderLinks() {
		const ls = n.links || [], key = p.group || '';
		const meta = new Map((n.members || []).map((m) => [m.name, m.metadata || {}]));
		setKeys(groupSel, [...new Set((n.members || []).flatMap((m) => Object.keys(m.metadata || {})))].sort(), key);
		const peerOf = (l) => ({ name: l.peer, metadata: meta.get(l.peer) });
		if (key) {
			const by = new Map();
			for (const l of ls) {
				const name = meta.get(l.peer)?.[key] ?? '';
				let g = by.get(name);
				if (!g) by.set(name, (g = { name, peers: new Set(), out: 0, outUp: 0, in: 0, queued: 0, messages: 0, bytes: 0 }));
				g.peers.add(l.peer);
				if (l.direction === 'out') {
					g.out++;
					g.outUp += l.state === 'up' ? 1 : 0;
					g.queued += l.queued || 0;
				} else g.in++;
				g.messages += ld.get(linkKey(l))?.messages || 0;
				g.bytes += ld.get(linkKey(l))?.bytes || 0;
			}
			const groups = [...by.values()].sort((a, b) => natural(a.name, b.name));
			groupsEl.replaceChildren(
				table(
					[
						{ t: key, get: (g) => (g.name === '' ? h('span', { class: 'muted' }, 'none') : h('b', null, g.name)) },
						{ t: 'Peers', num: true, get: (g) => fmtInt(g.peers.size) },
						{ t: 'Out', get: (g) => [`${fmtInt(g.outUp)} / ${fmtInt(g.out)} up`, g.outUp < g.out ? [' ', pill(`${g.out - g.outUp} down`, 'down')] : null] },
						{ t: 'In', num: true, get: (g) => fmtInt(g.in) },
						{ t: 'Queued', num: true, get: (g) => (g.queued ? fmtInt(g.queued) : '') },
						{ t: 'Messages / s', num: true, get: (g) => fmtRate(g.messages) },
						{ t: 'Bytes / s', num: true, get: (g) => fmtBytes(g.bytes) },
					],
					groups,
					{ onRow: (g) => set({ q: filterOf(key, g.name), page: '' }), rowClass: (g) => cls((g.outUp < g.out || g.queued) && 'backlog', p.q === filterOf(key, g.name) && 'sel') },
				),
			);
		} else groupsEl.replaceChildren();
		const rows = peerRows(ls);
		const hits = p.q ? rows.filter((r) => matches(peerOf(r), p.q)) : rows;
		const s = sorter(cols, p);
		const worst = (r) => (r.out?.state === 'down' || r.in?.state === 'down' ? 2 : r.out?.queued ? 1 : 0);
		const ordered = s.sort ? s.apply(hits) : [...hits].sort((a, b) => worst(b) - worst(a) || natural(a.peer, b.peer));
		const pg = pager(ordered, p);
		count.textContent = p.q ? `${fmtInt(hits.length)} of ${fmtInt(rows.length)} peers` : `${fmtInt(rows.length)} peers`;
		links.replaceChildren(
			table(cols, pg.rows, {
				sort: s.sort,
				onSort: sortBy(() => p),
				empty: rows.length ? 'No peer matches.' : 'No links: this node has not talked to another yet.',
				rowClass: (r) => r.out?.queued > 0 && 'backlog',
			}),
			...pg.el,
		);
	}

	const page = {
		el,
		banner: b.set,
		update(np) {
			p = np;
			if (document.activeElement !== filter) filter.value = p.q || '';
			if (n) renderLinks();
		},
		async tick() {
			n = await api('/api/node', { node });
			shown.hidden = !!n.no_inspector;
			top.lastChild.textContent = n.no_inspector ? 'Up, as the cluster sees it, but it serves no Inspector to describe it.' : LEAD;
			about.replaceChildren(...[n.advertise ? h('span', { class: 'mono', title: 'what peers dial' }, n.advertise) : null, chips(n.metadata)].filter(Boolean));
			if (n.no_inspector) {
				cards.replaceChildren(card('Inspector', 'none', 'its address and metadata as the cluster has them'));
				return;
			}
			const d = nr([n], (x) => x.name, ['spawned', 'exited', 'dead_letters']).get(n.name);
			const ls = n.links || [];
			ld = lr(ls, linkKey, ['messages', 'bytes']);
			const total = (dir, f) => (d ? sum(ls.filter((l) => l.direction === dir), (l) => ld.get(linkKey(l))?.[f]) : null);
			const queued = sum(ls, (l) => l.queued);
			cards.replaceChildren(
				card('Processes', fmtInt(n.processes), `${fmtInt(n.spawned)} spawned, ${fmtInt(n.exited)} exited`),
				card('Spawns / s', fmtRate(d?.spawned) || '–', `exits ${fmtRate(d?.exited) || '–'}/s`),
				card('Dead letters', fmtInt(n.dead_letters), d?.dead_letters ? `+${fmtRate(d.dead_letters)}/s` : 'none lately', d?.dead_letters > 0 && 'warn'),
				card('Queued to peers', fmtInt(queued), queued ? fmtBytes(sum(ls, (l) => l.queued_bytes)) : 'nothing waits', queued > 0 && 'warn'),
				card('Uptime', n.uptime || '–', 'since this incarnation started'),
			);
			cProc.push(n.processes);
			cLife.push(d?.spawned, d?.exited);
			cDead.push(d?.dead_letters);
			cMsgs.push(total('out', 'messages'), total('in', 'messages'));
			cBytes.push(total('out', 'bytes'), total('in', 'bytes'));
			renderLinks();
		},
	};
	return page;
}

const STATES = ['idle', 'running', 'waiting-reply', 'exiting'];

function processesPage(node, params) {
	let p = params;
	const b = banner();
	const labels = h('datalist', { id: 'labels' });
	const known = new Set();
	const name = h('input', { type: 'search', placeholder: 'name contains', value: p.name || '', oninput: debounce((e) => set({ name: e.target.value }), 300) });
	const label = h('input', { type: 'search', placeholder: 'label', list: 'labels', value: p.label || '', onchange: (e) => set({ label: e.target.value }) });
	const state = h('select', { value: p.state || '', onchange: (e) => set({ state: e.target.value }) }, ['', ...STATES].map((s) => h('option', { value: s }, s || 'any state')));
	const min = h('input', { type: 'number', min: 0, placeholder: '0', value: p.min || '', onchange: (e) => set({ min: e.target.value }) });
	const search = h('input', { type: 'search', placeholder: 'search pid, name, label, type, message', value: p.q || '', oninput: (e) => set({ q: e.target.value }) });
	const count = h('span', { class: 'muted' });
	const cMsgs = new Chart('Messages / s', [['in', '--c1'], ['out', '--c2']]);
	const cBox = new Chart('Waiting in mailboxes', [['all', '--c2'], ['deepest', '--c5']], { fmt: fmtInt });
	const cState = new Chart('Processes by state', [['running', '--c3'], ['waiting reply', '--c2'], ['idle', '--c1']], { fmt: fmtInt });
	const cBusy = new Chart('Busy, in processes', [['all', '--c3']]);
	const list = h('div');
	const pr = rates();
	let rows = [], r = new Map();

	const cols = [
		{ key: 'pid', t: 'PID', get: (x) => pidSpan(x.pid), sort: (x) => x.pid, cmp: pidCmp },
		{ key: 'name', t: 'Name', wrap: true, min: '14rem', get: (x) => procName(x.name), sort: (x) => x.name || '' },
		{ key: 'label', t: 'Label', get: labelEl, sort: (x) => x.label },
		{ key: 'state', t: 'State', get: (x) => pill(x.state), sort: (x) => x.state },
		{ key: 'mailbox', t: 'Mailbox', num: true, get: (x) => [fmtInt(x.mailbox), x.mailbox ? h('span', { class: 'bar', style: { width: `${Math.max(3, Math.min(60, 60 * Math.log10(1 + x.mailbox) / 3))}px` } }) : null], sort: (x) => x.mailbox },
		{ key: 'oldest', t: 'Oldest', num: true, get: (x) => x.oldest_wait, sort: (x) => parseDur(x.oldest_wait) },
		{ key: 'in', t: 'In / s', num: true, get: (x) => fmtRate(r.get(x.pid)?.received), sort: (x) => r.get(x.pid)?.received || 0 },
		{ key: 'out', t: 'Out / s', num: true, get: (x) => fmtRate(r.get(x.pid)?.sent), sort: (x) => r.get(x.pid)?.sent || 0 },
		{ key: 'busy', t: 'Busy', num: true, get: (x) => fmtPct(r.get(x.pid)?.busy_seconds), sort: (x) => share(r.get(x.pid)?.busy_seconds) || 0 },
		{ key: 'received', t: 'Received', num: true, get: (x) => fmtInt(x.received), sort: (x) => x.received },
		{ key: 'sent', t: 'Sent', num: true, get: (x) => fmtInt(x.sent), sort: (x) => x.sent },
		{ key: 'calls', t: 'Calls', num: true, get: (x) => (x.calls_in_flight ? fmtInt(x.calls_in_flight) : ''), sort: (x) => x.calls_in_flight || 0 },
		{ key: 'last', t: 'Last message', wrap: true, min: '9rem', get: (x) => h('span', { class: 'mono' }, breakable(x.last_message || '')), sort: (x) => x.last_message || '' },
		{ key: 'uptime', t: 'Uptime', num: true, get: (x) => x.uptime, sort: (x) => parseDur(x.uptime) },
		{ key: 'log', t: 'Log', get: (x) => h('span', { class: 'muted' }, x.log_level) },
	];

	function render() {
		const q = (p.q || '').toLowerCase();
		const hits = q ? rows.filter((x) => [x.pid, x.name, x.label, x.type, x.last_message].some((f) => f && f.toLowerCase().includes(q))) : rows;
		const s = sorter(cols, p);
		count.textContent = q ? `${fmtInt(hits.length)} of ${fmtInt(rows.length)}` : `${fmtInt(rows.length)} processes`;
		list.replaceChildren(
			table(cols, s.apply(hits), {
				sort: s.sort,
				onSort: sortBy(() => p),
				limit: 1000,
				onRow: (x) => openProc(x.pid),
				rowClass: (x) => cls(x.mailbox > 0 && 'backlog', x.pid === app.params.proc && 'sel'),
				empty: rows.length ? 'No process matches the search.' : 'No process matches the scope.',
			}),
		);
	}

	const page = {
		el: h(
			'div',
			null,
			head(['Processes ', h('span', { class: 'mono' }, node || '(inspector)')], 'Every process on the node. The node applies the scope; the search and the order apply here, to what it sent.'),
			b.el,
			h('div', { class: 'charts' }, cMsgs.el, cBox.el, cState.el, cBusy.el),
			h(
				'div',
				{ class: 'scope' },
				h('b', null, 'Scope'),
				h('label', null, name),
				h('label', null, label, labels),
				select(state),
				h('label', null, 'mailbox ≥', min),
				h('span', { class: 'sep' }),
				h('label', null, search),
				h('span', { class: 'spacer' }),
				count,
			),
			list,
		),
		banner: b.set,
		// The scope goes to the node, which sends only what matches; the search
		// and the order apply to what came back.
		update(np) {
			const refetch = ['name', 'label', 'state', 'min'].some((k) => (np[k] || '') !== (p[k] || ''));
			p = np;
			if (refetch) run(page);
			else render();
		},
		async tick(stale) {
			const got = await api('/api/processes', { node, name: p.name, label: p.label, state: p.state, min_mailbox: p.min });
			if (stale()) return;
			rows = got;
			r = pr(rows, (x) => x.pid, ['received', 'sent', 'busy_seconds']);
			for (const x of rows) {
				if (!known.has(x.label)) {
					known.add(x.label);
					labels.append(h('option', { value: x.label }));
				}
			}
			const ready = [...r.values()].some(Boolean);
			cMsgs.push(ready ? sum(rows, (x) => r.get(x.pid)?.received) : null, ready ? sum(rows, (x) => r.get(x.pid)?.sent) : null);
			cBox.push(sum(rows, (x) => x.mailbox), Math.max(0, ...rows.map((x) => x.mailbox)));
			const by = (st) => rows.filter((x) => x.state === st).length;
			cState.push(by('running'), by('waiting-reply'), by('idle'));
			cBusy.push(ready ? sum(rows, (x) => share(r.get(x.pid)?.busy_seconds)) : null);
			render();
		},
	};
	return page;
}

function treePage(node, params) {
	let p = params;
	const b = banner();
	const collapsed = new Set();
	const unfolded = new Set(); // runs of like processes opened, by parent and label
	const unfoldAll = () => {
		const pids = new Set(rows.map((x) => x.pid));
		for (const x of rows) unfolded.add(`${x.parent && pids.has(x.parent) ? x.parent : ''}>${x.label}`);
	};
	const pr = rates();
	let rows = [], r = new Map();
	const color = h(
		'select',
		{ value: p.color || 'state', onchange: (e) => set({ color: e.target.value }) },
		h('option', { value: 'state' }, 'colour by state'),
		h('option', { value: 'mailbox' }, 'colour by mailbox'),
		h('option', { value: 'activity' }, 'colour by messages / s'),
	);
	const search = h('input', { type: 'search', placeholder: 'find name, pid, label', value: p.q || '', oninput: (e) => set({ q: e.target.value }) });
	const legend = h('span', { class: 'muted' });
	const count = h('span', { class: 'muted' });
	const view = h('div', { class: 'tree' });

	const heat = (v, steps) => 'h' + steps.filter((s) => v >= s).length;
	function colour(x) {
		if (p.color === 'mailbox') return heat(x.mailbox, [1, 10, 100]);
		if (p.color === 'activity') return heat(r.get(x.pid)?.received || 0, [0.01, 1, 100]);
		return x.state;
	}
	const legends = {
		state: 'grey idle · green running · amber waiting on a call · red exiting',
		mailbox: 'pale: empty · amber: some waiting · red: 100 or more',
		activity: 'pale: quiet · amber: some messages / s · red: 100 / s or more',
	};

	function render() {
		const byPid = new Map(rows.map((x) => [x.pid, x]));
		const kids = new Map(), roots = [];
		for (const x of rows) {
			if (x.parent && byPid.has(x.parent)) {
				if (!kids.has(x.parent)) kids.set(x.parent, []);
				kids.get(x.parent).push(x);
			} else roots.push(x);
		}
		const q = (p.q || '').toLowerCase();
		const hit = (x) => q && [x.pid, x.name, x.label].some((f) => f && f.toLowerCase().includes(q));
		// With a search, only hits and the processes above them are shown.
		let keep = null;
		if (q) {
			keep = new Set();
			for (const x of rows) {
				for (let y = hit(x) ? x : null; y && !keep.has(y.pid); y = byPid.get(y.parent)) keep.add(y.pid);
			}
		}
		const order = (a, b) => (kids.has(b.pid) ? 1 : 0) - (kids.has(a.pid) ? 1 : 0) || pidCmp(a.pid, b.pid);
		// rowsOf draws siblings, FOLD or more leaves of one label as one row
		// that opens to them, unless a search shows only its hits.
		const rowsOf = (parent, xs) => {
			xs = xs.sort(order);
			if (keep) return xs.map(row);
			const like = new Map();
			for (const x of xs) if (x.label && !kids.has(x.pid)) like.set(x.label, [...(like.get(x.label) || []), x]);
			const out = [];
			for (const x of xs) {
				const run = !kids.has(x.pid) && like.get(x.label);
				if (!run || run.length < FOLD) out.push(row(x));
				else if (run[0] === x) out.push(fold(`${parent}>${x.label}`, run));
			}
			return out;
		};
		const fold = (id, run) => {
			const open = unfolded.has(id);
			const toggle = () => {
				if (open) unfolded.delete(id);
				else unfolded.add(id);
				render();
			};
			const waiting = sum(run, (x) => x.mailbox);
			const worst = STATES.findLast((s) => run.some((x) => x.state === s)) || 'idle';
			const dot = p.color === 'mailbox' ? heat(Math.max(...run.map((x) => x.mailbox)), [1, 10, 100]) : p.color === 'activity' ? heat(Math.max(...run.map((x) => r.get(x.pid)?.received || 0)), [0.01, 1, 100]) : worst;
			const common = commonStart(run.map((x) => x.name || ''));
			return [
				h(
					'div',
					{ class: 'trow fold', onclick: toggle },
					h('span', { class: 'tog' }, open ? '▾' : '▸'),
					h('span', { class: cls('dot', dot), title: `${fmtInt(waiting)} waiting; the busiest ${worst}` }),
					h('span', { class: 'tname' }, common ? [procName(common), '…'] : run[0].label),
					h('span', { class: 'muted' }, `× ${fmtInt(run.length)}`),
					h('span', { class: 'muted' }, run[0].label),
					waiting ? pill(`${fmtInt(waiting)} waiting`, 'warn') : null,
				),
				open ? h('div', { class: 'tkids' }, run.map(row)) : null,
			];
		};
		const row = (x) => {
			if (keep && !keep.has(x.pid)) return null;
			const ch = kids.get(x.pid) || [];
			const open = keep ? true : !collapsed.has(x.pid);
			const toggle = (e) => {
				e.stopPropagation();
				if (collapsed.has(x.pid)) collapsed.delete(x.pid);
				else collapsed.add(x.pid);
				render();
			};
			return [
				h(
					'div',
					{ class: cls('trow', hit(x) && 'hit'), onclick: () => openProc(x.pid) },
					h('span', { class: 'tog', onclick: ch.length ? toggle : null }, ch.length ? (open ? '▾' : '▸') : ''),
					h('span', { class: cls('dot', colour(x)), title: `${x.state}, ${x.mailbox} waiting` }),
					h('span', { class: 'tname' }, procName(x.name) || x.label),
					h('span', { class: 'mono muted', title: x.pid }, pidText(x.pid)),
					x.name ? h('span', { class: x.label === x.type ? 'muted faint' : 'muted' }, x.label) : null,
					x.mailbox ? pill(`${fmtInt(x.mailbox)} waiting`, 'warn') : null,
					!open && ch.length ? h('span', { class: 'muted' }, `${ch.length} below`) : null,
					x.parent && !byPid.has(x.parent) ? h('span', { class: 'muted' }, 'started by ', pidLink(x.parent)) : null,
				),
				open && ch.length ? h('div', { class: 'tkids' }, rowsOf(x.pid, ch)) : null,
			];
		};
		legend.textContent = legends[p.color] || legends.state;
		const trees = roots.filter((x) => kids.has(x.pid)).length;
		count.textContent = `${fmtInt(rows.length)} processes, ${fmtInt(trees)} ${trees === 1 ? 'tree' : 'trees'}`;
		view.replaceChildren();
		append(view, rowsOf('', roots));
		if (!view.childElementCount) view.append(h('div', { class: 'empty' }, rows.length ? 'Nothing matches.' : 'No processes.'));
	}

	const page = {
		el: h(
			'div',
			null,
			head(['Supervision ', h('span', { class: 'mono' }, node || '(inspector)')], ['Each process under the one that started it: a supervisor over its children. ', legend]),
			b.el,
			h(
				'div',
				{ class: 'scope' },
				select(color),
				h('label', null, search),
				h('button', { type: 'button', onclick: () => (collapsed.clear(), unfoldAll(), render()) }, 'Expand all'),
				h('button', { type: 'button', onclick: () => (rows.forEach((x) => x.parent && collapsed.add(x.parent)), unfolded.clear(), render()) }, 'Collapse all'),
				h('span', { class: 'spacer' }),
				count,
			),
			view,
		),
		banner: b.set,
		update(np) {
			p = np;
			render();
		},
		async tick() {
			rows = await api('/api/processes', { node });
			r = pr(rows, (x) => x.pid, ['received']);
			render();
		},
	};
	return page;
}

// FOLD is how many leaves of one label under one parent the tree draws as
// one row, as a pub/sub topic a peer each makes.
const FOLD = 5;


// commonStart is what every one of names begins with, up to a separator
// with something after it in each, or ''.
function commonStart(names) {
	if (names.length < 2) return '';
	let c = names[0];
	for (const n of names) while (!n.startsWith(c)) c = c.slice(0, -1);
	c = c.slice(0, Math.max(...[...'-._:/@{'].map((s) => c.lastIndexOf(s))) + 1); // through its last separator
	return c && names.every((n) => n.length > c.length) ? c : '';
}

const KINDS = ['spawn', 'exit', 'link-up', 'link-down', 'dead-letter', 'member-up', 'member-down'];

function eventsPage(node) {
	const b = banner();
	const on = new Set(KINDS);
	const counts = Object.fromEntries(KINDS.map((k) => [k, 0]));
	let buf = [], paused = false, held = 0, text = '', missed = 0, broke = '';
	const note = () => broke || (missed ? `${fmtInt(missed)} events were dropped: the node sent them faster than this page read them.` : '');
	const status = h('span', { class: 'muted' }, 'connecting…');
	const tbody = h('tbody');
	const since = clock(new Date().toISOString());
	// What the table says while it lists nothing.
	const none = h('div', { class: 'empty' });
	let ended = '', streaming = false, began = false; // began: streamed once
	const quiet = () => {
		none.hidden = tbody.childElementCount > 0;
		none.textContent = ended
			? `No events: ${node || 'the inspector'} ${ended}.`
			: buf.length
				? 'No event matches.'
				: streaming
					? `No events since ${since.slice(0, 8)}: they show here as they happen.`
					: broke || began
						? `No events since ${since.slice(0, 8)}.`
						: `Asking ${node || 'the inspector'}…`;
	};
	const chips = KINDS.map((k) => {
		const n = h('span', { class: 'n' }, '0');
		const btn = h('button', { type: 'button', class: 'kind on', 'aria-pressed': 'true', onclick: () => (on.has(k) ? on.delete(k) : on.add(k), btn.setAttribute('aria-pressed', String(btn.classList.toggle('on'))), redraw()) }, k, n);
		return { k, btn, n };
	});
	const pause = h('button', { type: 'button', onclick: () => ((paused = !paused), (held = 0), (pause.textContent = paused ? 'Resume' : 'Pause'), paused || redraw()) }, 'Pause');
	const chart = new Chart('Events / s', [['spawns', '--c3'], ['exits', '--c2'], ['dead letters', '--c5'], ['links', '--c4'], ['members', '--c6']]);
	const matches = (e) => on.has(e.kind) && (!text || JSON.stringify(e).toLowerCase().includes(text));

	function evRow(e, fresh) {
		const p = e.process, m = e.member;
		const detail = [];
		for (const [k, v] of [['reason', e.reason], ['peer', e.peer], ['addr', m?.addr], ['metadata', m?.metadata && metadata(m.metadata)], ['type', e.type], ['error', e.error]]) if (v) detail.push(`${k}: ${v}`);
		const who = p
			? [pidLink(p.pid), ' ', h('span', { class: 'muted' }, p.name || p.label)]
			: m
				? [nameEl(m.name, true), m.incarnation ? h('span', { class: 'muted' }, `#${m.incarnation}`) : null]
				: e.kind === 'dead-letter'
					? [pidLink(e.from) || h('span', { class: 'muted' }, 'outside'), ' → ', pidLink(e.to) || '?']
					: '';
		return h(
			'tr',
			{ class: fresh ? 'fresh' : '' },
			h('td', { class: 'mono' }, clock(e.time)),
			h('td', null, pill(e.kind)),
			h('td', null, who),
			h('td', { class: 'wrap' }, detail.join(' · '), e.missed ? [' ', pill(`${fmtInt(e.missed)} missed before this`, 'warn')] : null),
		);
	}
	function redraw() {
		const shown = buf.filter(matches).slice(-500).reverse();
		tbody.replaceChildren(...shown.map((e) => evRow(e, false)));
		quiet();
	}

	const src = new EventSource('/api/events' + query({ node }));
	// The stream opens before the node is asked; "ready" says it answered.
	src.onopen = () => {
		status.textContent = `asking ${node || 'the inspector'}…`;
		streaming = false;
		quiet();
	};
	src.addEventListener('ready', () => {
		status.textContent = `streaming from ${node || 'the inspector'}`;
		streaming = began = true;
		broke = '';
		b.set(note());
		quiet();
	});
	src.onerror = () => {
		status.textContent = 'reconnecting…';
		streaming = false;
		quiet();
	};
	src.addEventListener('failure', (e) => {
		streaming = false;
		b.set((broke = `The event stream broke: ${JSON.parse(e.data)}. Reconnecting.`));
		quiet();
	});
	// end: nothing to stream, ever, such as from a node without an Inspector.
	src.addEventListener('end', (e) => {
		src.close();
		ended = JSON.parse(e.data);
		status.textContent = `${node || 'the inspector'} ${ended}: no events to stream`;
		quiet();
	});
	src.onmessage = (m) => {
		const e = JSON.parse(m.data);
		counts[e.kind] = (counts[e.kind] || 0) + 1;
		missed += e.missed || 0;
		buf.push(e);
		if (buf.length > 5000) buf = buf.slice(-4000);
		if (paused) {
			held++;
			status.textContent = `paused: ${fmtInt(held)} new`;
		} else if (matches(e)) {
			tbody.prepend(evRow(e, true));
			while (tbody.childElementCount > 500) tbody.lastChild.remove();
			quiet();
		}
	};

	quiet();
	let prev = { ...counts }, at = performance.now();
	const filter = h('input', { type: 'search', placeholder: 'filter: pid, name, reason, type…', oninput: (e) => ((text = e.target.value.toLowerCase()), redraw()) });
	return {
		el: h(
			'div',
			null,
			head(['Events ', h('span', { class: 'mono' }, node || '(inspector)')], 'Spawns, exits with their reasons, links and dead letters, as they happen. The node keeps no history: the stream starts when the page opens.'),
			b.el,
			h('div', { class: 'charts wide' }, chart.el),
			h(
				'div',
				{ class: 'scope' },
				chips.map((c) => c.btn),
				h('span', { class: 'sep' }),
				h('label', null, filter),
				pause,
				h('button', { type: 'button', onclick: () => ((buf = []), redraw()) }, 'Clear'),
				h('span', { class: 'spacer' }),
				status,
			),
			h('div', { class: 'tablewrap' }, h('table', null, h('thead', null, h('tr', null, ['Time', 'Kind', 'Process', 'Detail'].map((t) => h('th', null, t)))), tbody), none),
		),
		banner: (msg) => b.set(msg || note()),
		tick() {
			const now = performance.now(), dt = (now - at) / 1000;
			const d = (k) => (counts[k] - prev[k]) / dt;
			if (dt > 0.2) {
				chart.push(d('spawn'), d('exit'), d('dead-letter'), d('link-up') + d('link-down'), d('member-up') + d('member-down'));
				prev = { ...counts };
				at = now;
			}
			for (const c of chips) c.n.textContent = fmtInt(counts[c.k]);
		},
		unmount: () => src.close(),
	};
}

// readOnlyNote says how to change things from here, on a read-only page;
// a page hides it while it lists nothing to change.
const readOnlyNote = () =>
	app.info.allow_writes ? null : h('p', { class: 'muted' }, 'Read-only. Start grpcprocctl web with --allow-writes to change things from here.');

function cronPage(_node, params) {
	let p = params;
	const b = banner();
	const where = h('select', { onchange: (e) => set({ on: e.target.value }) });
	const list = h('div');
	const ro = readOnlyNote();
	let crons = [];
	function render() {
		const nodes = [...new Set(crons.map((c) => c.node))];
		where.replaceChildren(h('option', { value: '' }, 'every node'), ...nodes.map((n) => h('option', { value: n }, n)));
		where.value = p.on || '';
		const rows = crons.filter((c) => !p.on || c.node === p.on).flatMap((c) => (c.jobs.length ? c.jobs.map((j) => ({ c, j })) : [{ c, j: null }]));
		const job = (x, op, label, danger) =>
			h('button', { type: 'button', class: danger ? 'danger' : '', onclick: () => act(`${op} ${x.j.name}`, '/api/cron', { node: x.c.node, target: x.c.pid, op, job: x.j.name }, danger && `Remove job ${x.j.name} from ${x.c.process} on ${x.c.node}? It comes back only when the cron process restarts from its spec.`) }, label);
		list.replaceChildren(
			table(
				[
					{ t: 'Node', get: (x) => nodeLink(x.c.node) },
					{ t: 'Cron', get: (x) => pidLink(x.c.pid, x.c.process) },
					{ t: 'Job', get: (x) => (x.j ? h('b', null, x.j.name) : '') },
					{ t: 'Spec', get: (x) => (x.j ? h('span', { class: 'mono' }, x.j.spec) : '') },
					{ t: 'Zone', get: (x) => x.j?.location },
					{ t: 'Next', get: (x) => (x.j?.disabled ? pill('disabled', 'idle') : x.j?.next) },
					{ t: 'Last', get: (x) => x.j?.last },
					{ t: 'Running', num: true, get: (x) => (x.j?.running ? pill(String(x.j.running), 'running') : '') },
					{ t: 'Last failure', wrap: true, get: (x) => (x.j?.failure ? pill(x.j.failure, 'bad') : '') },
					{ t: 'Error', wrap: true, get: (x) => errText(x.c.error, x.c.node) },
					...(app.info.allow_writes
						? [{ t: '', get: (x) => (x.j ? h('span', { class: 'actions' }, x.j.disabled ? job(x, 'enable', 'Enable') : job(x, 'disable', 'Disable'), job(x, 'remove', 'Remove', true)) : '') }]
						: []),
				],
				rows,
				{ empty: 'No grpcproc/cron process runs on the nodes reachable from here.' },
			),
		);
	}
	return {
		every: 5000,
		el: h(
			'div',
			null,
			head('Cron', 'Every grpcproc/cron job on the nodes reachable from here. Each run is a process of its own, labelled cron:<job>.'),
			b.el,
			h('div', { class: 'scope' }, h('label', null, 'on', select(where))),
			list,
			ro,
		),
		banner: b.set,
		update(np) {
			p = np;
			render();
		},
		async tick() {
			crons = await api('/api/crons');
			if (ro) ro.hidden = !crons.some((c) => c.jobs.length);
			render();
		},
	};
}

// metadata renders a node's metadata as key=value pairs, ordered by key.
const metadata = (md) =>
	Object.keys(md || {})
		.sort()
		.map((k) => `${k}=${md[k]}`)
		.join(' ');

// chips shows metadata a chip a key, a long value shortened by its middle,
// as a certificate's hash would be, and whole in the chip's title.
const chips = (md) =>
	h(
		'span',
		{ class: 'chips' },
		Object.keys(md || {})
			.sort()
			.map((k) => h('span', { class: 'chip mono', title: `${k}=${md[k]}` }, h('span', { class: 'muted' }, `${k}=`), middle(md[k], CHIP_VALUE))),
	);
// CHIP_VALUE is the most characters a chip shows of a value.
const CHIP_VALUE = 18;

function namesPage(node, params) {
	let p = params;
	const b = banner();
	const prefix = h('input', { type: 'search', placeholder: 'prefix, such as room:', value: p.prefix || '', oninput: debounce((e) => set({ prefix: e.target.value }), 300) });
	const list = h('div');
	const page = {
		every: 2000,
		el: h(
			'div',
			null,
			head('Global names', `The installation's global names, as ${node}'s copy has them, and the processes that hold them, wherever they run.`),
			b.el,
			h('div', { class: 'scope' }, h('label', null, 'names starting with', prefix)),
			list,
		),
		banner: b.set,
		update(np) {
			const refetch = (np.prefix || '') !== (p.prefix || '');
			p = np;
			if (refetch) run(page);
		},
		async tick(stale) {
			const names = await api('/api/names', { node, prefix: p.prefix || '' });
			if (stale()) return;
			list.replaceChildren(
				table(
					[
						{ t: 'Name', get: (x) => h('b', null, x.name) },
						{ t: 'Held by', get: (x) => pidLink(x.pid) },
					],
					names,
					{ empty: p.prefix ? `No global name starts with ${p.prefix}.` : 'No process holds a global name.' },
				),
			);
		},
	};
	return page;
}

function groupsPage(node, params) {
	let p = params;
	const b = banner();
	const scope = h('select', { onchange: (e) => set({ scope: e.target.value, group: '' }) });
	const prefix = h('input', { type: 'search', placeholder: 'prefix, such as room:', value: p.prefix || '', oninput: debounce((e) => set({ prefix: e.target.value }), 300) });
	const list = h('div');
	const members = h('div');
	const peers = h('div');
	const none = h('div', { class: 'empty', hidden: true });
	const body = h('div', { hidden: true }, h('div', { class: 'scope' }, h('label', null, 'scope', select(scope)), h('label', null, 'groups starting with', prefix)), list, members, h('h2', null, 'Nodes of the scope'), peers);
	let scopes = '', absent = '';
	const page = {
		every: 2000,
		el: h(
			'div',
			null,
			head('Process groups', `The grpcproc/groups scopes ${node} runs, as its copy has them: each group's members on every node, and the other nodes of the scope. Pick a group to see its members.`),
			b.el,
			none,
			body,
		),
		banner: (msg) => b.set(msg || absent),
		update(np) {
			const what = (x) => [x.scope || '', x.prefix || '', x.group || ''].join('\n');
			const refetch = what(np) !== what(p);
			p = np;
			if (refetch) run(page);
		},
		async tick(stale) {
			const q = { node, scope: p.scope || '' };
			let v, m, missing = '';
			try {
				try {
					[v, m] = await Promise.all([api('/api/groups', { ...q, prefix: p.prefix || '' }), p.group ? api('/api/group', { ...q, group: p.group }) : null]);
				} catch (e) {
					// The scope asked for, picked on another node or away a
					// while, is not here now: the banner says so, what the node
					// runs shows meanwhile, and the URL keeps the scope, asked
					// for again each tick.
					if (!(e.status === 400 && p.scope && e.message.startsWith('no grpcproc/groups scope named '))) throw e;
					missing = e.message;
					[v, m] = [await api('/api/groups', { node, prefix: p.prefix || '' }), null];
				}
			} catch (e) {
				// A node that runs no scope is not in trouble: it says so, as
				// the other modules' pages do.
				if (stale() || !(e.status === 400 && e.message.startsWith('no grpcproc/groups scope on '))) throw e;
				absent = '';
				none.textContent = `${node} runs no grpcproc/groups scope.`;
				none.hidden = false;
				body.hidden = true;
				return;
			}
			if (stale()) return; // a later tick says what is missing, not this one
			absent = missing;
			none.hidden = true;
			body.hidden = false;
			// Options are made again only when they change, so an open select
			// stays open; one asked for that is not here is among them, so
			// another can be picked.
			const offered = (absent ? [...v.scopes, p.scope] : v.scopes).map((s) => [s, absent && s === p.scope ? `${s}: not here` : s]);
			if (JSON.stringify(offered) !== scopes) {
				scopes = JSON.stringify(offered);
				scope.replaceChildren(...offered.map(([s, label]) => h('option', { value: s }, label)));
			}
			scope.value = absent ? p.scope : v.scope;
			list.replaceChildren(
				table(
					[
						{ t: 'Group', get: (g) => h('b', null, g.name) },
						{ t: 'Members', num: true, get: (g) => fmtInt(g.members) },
						{ t: `On ${node}`, num: true, get: (g) => fmtInt(g.local) },
						{ t: 'Nodes', num: true, get: (g) => fmtInt(g.nodes) },
					],
					v.groups,
					{
						// A group shown in place of a scope not here is of the
						// scope shown: picking it picks that scope too, and the
						// group the URL names, of the scope not here, is none of
						// these.
						onRow: (g) => set(absent ? { scope: v.scope, group: g.name } : { group: g.name === p.group ? '' : g.name }, false),
						rowClass: (g) => !absent && g.name === p.group && 'sel',
						empty: p.prefix ? `No group of ${v.scope} starts with ${p.prefix}.` : `No process is in a group of ${v.scope}.`,
					},
				),
				v.omitted ? h('p', { class: 'muted' }, `${fmtInt(v.omitted)} more groups: narrow them with a prefix.`) : '',
			);
			members.replaceChildren(
				...(m
					? [
							h('h2', null, `Members of ${m.group}`),
							table(
								[
									{ t: 'Process', get: (pid) => pidLink(pid) },
									{ t: 'Node', get: (pid) => nodeLink(nodeOf(pid)) },
								],
								m.members,
								{ empty: `${m.group} has no members.` },
							),
							m.omitted ? h('p', { class: 'muted' }, `${fmtInt(m.omitted)} more members.`) : '',
						]
					: []),
			);
			peers.replaceChildren(
				table(
					[
						{ t: 'Node', get: (x) => nodeLink(x.node) },
						{ t: 'State', get: (x) => pill(x.state) },
						{ t: 'Members', num: true, get: (x) => fmtInt(x.members) },
						{ t: 'Scope', get: (x) => pidLink(x.scope) },
					],
					v.peers,
					{ rowClass: (x) => x.state === 'unreachable' && 'backlog', empty: `${v.scope} on ${node} knows no other node.` },
				),
			);
		},
	};
	return page;
}

function electionsPage() {
	const b = banner();
	const list = h('div');
	const moveTo = new Map(); // what each election's "to" select was left at
	const ro = readOnlyNote();
	return {
		every: 5000,
		el: h('div', null, head('Elections', 'Every grpcproc/leader election reachable from here, as each node that takes part sees it. Nodes that follow different leaders, or lag a term behind, point at a partition.'), b.el, list, ro),
		banner: b.set,
		async tick() {
			const all = await api('/api/elections');
			if (ro) ro.hidden = !all.length;
			if (!all.length) {
				list.replaceChildren(h('div', { class: 'empty' }, 'No node reachable from here takes part in a grpcproc/leader election.'));
				return;
			}
			list.replaceChildren(...all.map((e) => election(e, moveTo)));
		},
	};
}

// NOT_INSPECTED is client.NotInspected: an elector's error when its node
// serves no Inspector, which is not trouble, and cannot be asked through.
const NOT_INSPECTED = 'serves no Inspector';

function election(e, moveTo) {
	const answered = e.electors.filter((v) => !v.error);
	// A leader that serves no Inspector cannot be asked to hand over or to
	// cordon: nothing here can change the election.
	const silent = e.electors.some((v) => v.node === e.leading && v.error === NOT_INSPECTED);
	const writes = app.info.allow_writes && e.leading && !silent;
	const leaders = new Set(answered.filter((v) => v.leader).map((v) => v.leader));
	const top = Math.max(0, ...answered.map((v) => v.term || 0));
	const cordoned = new Set(e.electors.flatMap((v) => v.cordoned || []));
	const head = [
		h('span', { class: 't' }, e.cluster),
		e.leading ? pill(`${shortName(e.leading)} leads, term ${top}`, 'leader', e.leading) : pill('no leader', 'bad'),
		leaders.size > 1 ? pill(['split: nodes follow ', nameList([...leaders])], 'bad') : answered.length && answered.every((v) => v.leader === e.leading) ? pill('all agree', 'up') : null,
		answered.some((v) => v.term < top) ? pill('some nodes a term behind', 'warn') : null,
		silent ? h('span', { class: 'muted' }, 'its leader serves no Inspector: it cannot be moved or cordoned from here') : null,
	];
	if (writes) {
		const to = h(
			'select',
			{ value: moveTo.get(e.cluster) || '', onchange: (ev) => moveTo.set(e.cluster, ev.target.value) },
			h('option', { value: '' }, 'to the most up-to-date follower'),
			answered.filter((v) => v.node !== e.leading).map((v) => h('option', { value: v.node }, `to ${shortName(v.node)}`)),
		);
		head.push(
			h('span', { class: 'spacer' }),
			h(
				'span',
				{ class: 'actions' },
				select(to),
				h('button', { type: 'button', onclick: () => act(`moved ${e.cluster}`, '/api/leader', { cluster: e.cluster, op: 'move', to: to.value }, `Hand leadership of ${e.cluster} over from ${e.leading} ${to.value ? 'to ' + to.value : 'to the most up-to-date follower'}? Its singleton stops, and starts on the new leader.`) }, 'Move leader'),
			),
		);
	}
	const cols = [
		{ t: 'Node', get: (v) => nodeLink(v.node) },
		{ t: 'Role', get: (v) => (v.role ? pill(v.role) : '') },
		{ t: 'Term', num: true, get: (v) => [v.term || '', v.term && v.term < top ? h('span', { class: 'muted' }, ' behind') : null] },
		{ t: 'Follows', get: (v) => (v.leader ? nameEl(v.leader) : '') },
		{ t: 'Voted for', get: (v) => (v.voted_for ? nameEl(v.voted_for) : '') },
		{ t: 'View', get: (v) => nameList(v.view) },
		{ t: 'Quorum', num: true, get: (v) => v.quorum || '' },
		{ t: 'State', get: (v) => h('span', { class: 'mono' }, v.state) },
		{ t: 'Cordoned', get: (v) => nameList(v.cordoned) },
		{ t: 'Unreachable', get: (v) => (v.unreachable || []).map((n) => pill(shortName(n), 'bad', n)) },
		{ t: 'Singleton', get: (v) => (v.singleton?.startsWith('<') ? pidLink(v.singleton) : v.singleton) },
		{ t: 'Backoff', get: (v) => v.backoff },
		{ t: 'Error', wrap: true, get: (v) => errText(v.error, v.node) },
	];
	if (writes) {
		cols.push({
			t: '',
			get: (v) =>
				cordoned.has(v.node)
					? h('button', { type: 'button', onclick: () => act(`uncordoned ${v.node}`, '/api/leader', { cluster: e.cluster, op: 'uncordon', node: v.node }) }, 'Uncordon')
					: h('button', { type: 'button', class: 'danger', onclick: () => act(`cordoned ${v.node}`, '/api/leader', { cluster: e.cluster, op: 'cordon', node: v.node }, `Keep ${v.node} from leading ${e.cluster} until uncordoned?${v.node === e.leading ? ' It leads now, so it hands over.' : ''} It still votes.`) }, 'Cordon'),
		});
	}
	return h('div', { class: 'election' }, h('div', { class: 'ehead' }, head), table(cols, e.electors, { rowClass: (v) => v.error && v.error !== NOT_INSPECTED && 'backlog' }));
}

const SAGA_STATUSES = ['active', 'stuck', 'done'];

// sagaName is the saga of an engine's "name vN".
const sagaName = (s) => s.slice(0, s.lastIndexOf(' '));

function sagasPage(_node, params) {
	let p = params;
	const b = banner();
	const engines = h('div');
	// A saga is asked of the engine on the node "on" names, or the first that
	// runs it; an option is a saga and that node.
	const which = h('select', {
		onchange: (e) => {
			const [saga, on] = e.target.value.split('\n');
			set({ saga, on, after: '', run: '' });
		},
	});
	const status = h('select', { onchange: (e) => set({ status: e.target.value, after: '' }) }, ['', ...SAGA_STATUSES].map((s) => h('option', { value: s }, s || 'any status')));
	const count = h('span', { class: 'muted' });
	const list = h('div');
	const pager = h('div', { class: 'actions' });
	const runsEl = h('div', { hidden: true }, h('h2', null, 'Runs'), h('div', { class: 'scope' }, h('label', null, 'saga', select(which)), select(status), h('span', { class: 'spacer' }), count, pager), list, readOnlyNote());
	let known = [], runs = [], more = false, from = '', options = '';
	const saga = () => p.saga || known[0]?.saga || '';
	const on = () => p.on || known.find((k) => k.saga === saga())?.node || '';

	const cols = [
		{ t: 'ID', get: (r) => h('b', { class: 'mono' }, r.id) },
		{ t: 'Status', get: (r) => pill(r.status) },
		{ t: 'State', get: (r) => h('span', { class: 'mono' }, r.state) },
		{ t: 'Version', num: true, get: (r) => (r.version ? `v${r.version}` : '') },
		{ t: 'Visit', num: true, get: (r) => fmtInt(r.visit) },
		{ t: 'Attempts', num: true, get: (r) => (r.attempts ? fmtInt(r.attempts) : '') },
		{ t: 'Signals', num: true, get: (r) => (r.waiting ? fmtInt(r.waiting) : '') },
		{ t: 'Due', get: (r) => (r.status !== 'active' ? '' : r.retry_at ? ['retry ', rel(r.retry_at)] : r.effect_done && !r.deadline ? h('span', { class: 'muted' }, 'on a signal') : rel(r.wake)) },
		{ t: 'Deadline', get: (r) => rel(r.deadline) },
		{ t: 'Owner', get: (r) => (r.owner ? nodeLink(r.owner) : '') },
		{ t: 'Error', wrap: true, get: (r) => [r.error ? (r.status === 'stuck' ? pill(r.error, 'bad') : r.error) : '', r.cause ? h('div', { class: 'muted' }, `left its way: ${r.cause}`) : null] },
		{ t: 'Updated', get: (r) => rel(r.updated) },
	];

	function render() {
		const opts = [...known];
		if (saga() && !opts.some((k) => k.saga === saga() && k.node === on())) opts.unshift({ saga: saga(), node: on() });
		const twice = (s) => opts.filter((k) => k.saga === s).length > 1;
		// Options are made again only when they change, so an open select stays open.
		if (JSON.stringify(opts) !== options) {
			options = JSON.stringify(opts);
			which.replaceChildren(...(opts.length ? opts.map((k) => h('option', { value: `${k.saga}\n${k.node}` }, twice(k.saga) ? `${k.saga} on ${k.node}` : k.saga)) : [h('option', { value: '' }, 'no saga')]));
		}
		which.value = saga() ? `${saga()}\n${on()}` : '';
		if (p.status && ![...status.options].some((o) => o.value === p.status)) status.append(h('option', { value: p.status }, p.status));
		status.value = p.status || '';
		const what = p.status ? `${p.status} run` : 'run';
		count.textContent = runs.length ? `${fmtInt(runs.length)} ${runs.length === 1 ? 'run' : 'runs'}${p.after ? ` after ${p.after}` : ''}${more ? ', more follow' : ''}` : '';
		list.replaceChildren(
			table(cols, runs, {
				onRow: (r) => set({ saga: r.saga, on: from, run: r.id }, false),
				rowClass: (r) => cls(r.status === 'stuck' && 'backlog', r.id === p.run && r.saga === saga() && 'sel'),
				empty: !saga() ? 'No saga engine reachable from here runs a saga.' : p.after ? `No ${what} of ${saga()} after ${p.after}.` : `${saga()} has no ${what}.`,
			}),
		);
		// Next adds to the history, so the browser's Back is the previous page.
		pager.replaceChildren(
			h('button', { type: 'button', disabled: !p.after, onclick: () => set({ after: '' }, false) }, 'First page'),
			h('button', { type: 'button', disabled: !more, onclick: () => set({ after: runs.at(-1).id }, false) }, 'Next →'),
		);
	}

	const page = {
		every: 5000,
		el: h(
			'div',
			null,
			head('Sagas', "The grpcproc/saga engines reachable from here, and the runs in their stores, a page at a time. An engine shows only the sagas it runs, and a run's data only with saga.Config.InspectData."),
			b.el,
			h('h2', null, 'Engines'),
			engines,
			runsEl,
		),
		banner: b.set,
		// The page is asked for again when what it shows changes, not when a
		// default (the first saga, the node of its engine) goes into the URL.
		update(np) {
			const scope = () => [saga(), on(), p.status || '', p.after || ''].join('\n');
			const was = scope();
			p = np;
			if (scope() !== was) run(page);
			else render();
		},
		async tick(stale) {
			const ask = () => (saga() ? api('/api/saga/runs', { node: on(), saga: saga(), status: p.status, after: p.after }) : null);
			// Once the saga and its node are known, the runs are asked for beside
			// the engines.
			const asked = saga() ? { saga: saga(), on: on() } : null;
			const [all, first] = await Promise.all([api('/api/sagas'), ask()]);
			if (stale()) return;
			known = all.flatMap((e) => e.sagas.map((s) => ({ saga: sagaName(s), node: e.node })));
			runsEl.hidden = !saga(); // no runs to page through without a saga
			engines.replaceChildren(
				table(
					[
						{ t: 'Node', get: (e) => nodeLink(e.node) },
						{ t: 'Engine', get: (e) => pidLink(e.pid, e.process) },
						{ t: 'Sagas', get: (e) => h('span', { class: 'row' }, e.sagas.map((s) => h('a', { href: href('sagas', { saga: sagaName(s), on: e.node, status: p.status }), onclick: (ev) => ev.stopPropagation() }, s))) },
						{ t: 'Working on', num: true, get: (e) => (e.error ? '' : fmtInt(e.working)) },
						{ t: 'Error', wrap: true, get: (e) => errText(e.error, e.node) },
					],
					all,
					{ empty: 'No grpcproc/saga engine runs on the nodes reachable from here.' },
				),
			);
			// What the engines say may change the defaults: ask again if so.
			const got = asked?.saga === saga() && asked?.on === on() ? first : await ask();
			if (stale()) return;
			runs = got?.runs || [];
			more = !!got?.more;
			from = on();
			render();
		},
	};
	return page;
}

// ---- one process, in the drawer ------------------------------------------

const LEVELS = ['debug', 'info', 'warn', 'error'];

function procDrawer(pid) {
	const node = nodeOf(pid);
	const b = banner();
	const title = h('span', { class: 't' }, pid);
	const sub = h('div', { class: 'muted' });
	const state = h('span');
	const cards = h('div', { class: 'cards' });
	const cMsgs = new Chart('Messages / s', [['in', '--c1'], ['out', '--c2']]);
	const cBox = new Chart('Mailbox', [['waiting', '--c2']], { fmt: fmtInt });
	const cBusy = new Chart('Busy', [['busy', '--c3']], { fmt: fmtPct });
	const facts = h('div', { class: 'kv' });
	const said = h('div', { class: 'kv' });
	const saidNote = h('span', { class: 'muted' });
	const kids = h('div');
	const pr = rates();
	let ticks = 0, gone = '', auto = false, every = 5000, asked = 0, last = null;

	async function ask() {
		asked = Date.now();
		saidNote.textContent = 'asking…';
		try {
			const v = await api('/api/process', { node, target: pid, inspect: 1, wait: '1s' });
			const keys = Object.keys(v.inspect || {}).sort();
			said.replaceChildren(...keys.flatMap((k) => [h('span', { class: 'k' }, k), h('span', { class: 'v' }, linkify(v.inspect[k], true))]));
			said.hidden = !keys.length;
			saidNote.textContent = v.inspect_error ? v.inspect_error : keys.length ? `answered at ${new Date().toLocaleTimeString()}` : 'It publishes nothing: see grpcproc.WithInspect.';
		} catch (e) {
			saidNote.textContent = e.message;
		}
	}

	const level = h('select', null, LEVELS.map((l) => h('option', { value: l }, l)));
	const reason = h('input', { type: 'text', placeholder: 'reason (killed)' });
	const actions = app.info.allow_writes
		? [
				h('h3', null, 'Act'),
				h(
					'div',
					{ class: 'actions' },
					h('label', null, 'log level', select(level)),
					h('button', { type: 'button', onclick: () => act(`log level ${level.value}`, '/api/loglevel', { node, target: pid, level: level.value }) }, 'Set'),
					h('span', { class: 'spacer' }),
					reason,
					h(
						'button',
						{
							type: 'button',
							class: 'danger',
							onclick: () =>
								act(`asked ${last?.name || pid} to exit`, '/api/exit', { node, target: pid, reason: reason.value }, `Ask ${last?.name || pid} to exit with reason "${reason.value || 'killed'}"? Its supervisor, if any, may restart it.`),
						},
						'Exit',
					),
				),
			]
		: [h('h3', null, 'Act'), readOnlyNote()];

	const autoBox = h('input', { type: 'checkbox', onchange: (e) => ((auto = e.target.checked), auto && ask()) });
	const everySel = h('select', { value: '5000', onchange: (e) => (every = Number(e.target.value)) }, [1000, 5000, 10000, 30000].map((ms) => h('option', { value: ms }, `every ${ms / 1000}s`)));

	const el = h(
		'div',
		null,
		h('div', { class: 'dhead' }, h('div', null, h('div', { class: 'row' }, title, state), sub), h('span', { class: 'spacer' }), h('button', { type: 'button', class: 'icon-button', title: 'Close (Esc)', 'aria-label': 'Close', onclick: closeDrawer }, icon(XMARK))),
		b.el,
		cards,
		h('div', { class: 'charts' }, cMsgs.el, cBox.el, cBusy.el),
		h('h3', null, 'What it says about itself'),
		h('div', { class: 'row' }, h('button', { type: 'button', onclick: ask }, 'Ask'), h('label', null, autoBox, 'keep asking'), select(everySel), saidNote),
		h('p', { class: 'muted' }, 'Asking takes the process a turn: it answers between messages, so a busy one keeps you waiting.'),
		said,
		h('h3', null, 'Process'),
		facts,
		h('h3', null, 'Started by it'),
		kids,
		actions,
	);

	return {
		key: `proc\n${pid}`,
		el,
		banner: (msg) => b.set(msg || gone),
		first: ask,
		async tick() {
			if (gone) return;
			let v;
			try {
				v = await api('/api/process', { node, target: pid });
			} catch (e) {
				if (e.status !== 404) throw e;
				gone = `${last?.name || pid} is not running on ${node} any more. Its exit, and why, shows on the Events page while that is open.`;
				state.replaceChildren(pill('gone', 'exiting'));
				return;
			}
			if (!last) level.value = LEVELS.find((l) => l === v.log_level.toLowerCase()) || 'info';
			last = v;
			const d = pr([v], (x) => x.pid, ['received', 'sent', 'busy_seconds']).get(pid);
			title.textContent = v.name || v.label;
			sub.replaceChildren(h('span', { class: 'mono' }, pid), v.name ? ` · ${v.label}` : '', ' · ', h('span', { class: 'mono' }, v.type));
			state.replaceChildren(pill(v.state));
			cards.replaceChildren(
				card('Mailbox', fmtInt(v.mailbox), v.mailbox ? `oldest ${v.oldest_wait}, peak ${fmtInt(v.mailbox_peak)}` : `peak ${fmtInt(v.mailbox_peak)}`, v.mailbox > 0 && 'warn'),
				card('Received', fmtInt(v.received), d ? `${fmtRate(d.received)}/s` : ''),
				card('Sent', fmtInt(v.sent), d ? `${fmtRate(d.sent)}/s` : ''),
				card('Calls out', fmtInt(v.calls_in_flight || 0), v.state === 'waiting-reply' ? 'waiting on a reply' : ''),
				card('Busy', d ? fmtPct(d.busy_seconds) : '–', v.busy_for ? `on this for ${v.busy_for}` : 'waiting for a message', d && d.busy_seconds >= 0.95 && 'warn'),
				card('Uptime', v.uptime, `log level ${v.log_level}`),
			);
			cMsgs.push(d?.received, d?.sent);
			cBox.push(v.mailbox);
			cBusy.push(share(d?.busy_seconds));
			const kv = [
				['parent', pidLink(v.parent, null, true) || h('span', { class: 'muted' }, 'none: spawned by the node')],
				['global names', v.globals?.length ? h('span', { class: 'mono' }, v.globals.join(', ')) : h('span', { class: 'muted' }, 'none')],
				['last message', v.last_message],
				['monitors', `${v.monitors || 0} held`],
				['links', `${v.links || 0}${v.trap_exit ? ', trapping exits' : ''}`],
				['watchers', `${v.watchers || 0} monitoring or linked to it`],
			];
			facts.replaceChildren(...defs(kv));
			if (ticks++ % 5 === 0) {
				const ps = await api('/api/processes', { node });
				const mine = ps.filter((x) => x.parent === pid).sort((a, c) => pidCmp(a.pid, c.pid));
				kids.replaceChildren(
					mine.length
						? h('div', { class: 'kv' }, mine.flatMap((x) => [h('span', { class: 'k' }, pidLink(x.pid, null, true)), h('span', { class: 'v' }, [x.name || x.label, ' ', pill(x.state), x.mailbox ? ` ${x.mailbox} waiting` : ''])]))
						: h('span', { class: 'muted' }, 'nothing on this node'),
				);
			}
			if (auto && Date.now() - asked >= every) await ask();
		},
	};
}

// ---- one saga run, in the drawer -----------------------------------------

function runDrawer(saga, id, on) {
	const b = banner();
	const status = h('span');
	const sub = h('div', { class: 'muted' });
	const cards = h('div', { class: 'cards' });
	const why = h('div');
	const timing = h('div', { class: 'kv' });
	const data = h('div');
	const inbox = h('div');
	const acts = h('div');
	const json = (v) => h('pre', { class: 'json' }, JSON.stringify(v, null, 2));
	const hidden = (what) => h('span', { class: 'muted' }, `Not shown: the engine shows ${what} only with saga.Config.InspectData, and only what it can read.`);

	const el = h(
		'div',
		null,
		h('div', { class: 'dhead' }, h('div', null, h('div', { class: 'row' }, h('span', { class: 't mono' }, id), status), sub), h('span', { class: 'spacer' }), h('button', { type: 'button', class: 'icon-button', title: 'Close (Esc)', 'aria-label': 'Close', onclick: closeDrawer }, icon(XMARK))),
		b.el,
		cards,
		why,
		h('h3', null, 'Timers and owner'),
		timing,
		h('h3', null, 'Data'),
		data,
		h('h3', null, 'Signals waiting'),
		inbox,
		acts,
	);

	return {
		key: `run\n${saga}\n${id}\n${on}`,
		every: 5000,
		el,
		banner: b.set,
		async tick() {
			const r = await api('/api/saga/run', { node: on, saga, id });
			status.replaceChildren(pill(r.status));
			sub.replaceChildren(r.saga, r.version ? ` · v${r.version}` : '', ` · revision ${fmtInt(r.revision)}`);
			cards.replaceChildren(
				card('State', h('span', { class: 'mono' }, r.state), `visit ${fmtInt(r.visit || 0)}${r.effect_done ? ', its effect done' : ''}`),
				card('Attempts', fmtInt(r.attempts || 0), r.retry_at ? ['retry ', rel(r.retry_at)] : 'failures of this visit', r.attempts > 0 && 'warn'),
				card('Signals', fmtInt(r.inbox?.length || 0), 'waiting for a state that takes them'),
				card('Epoch', fmtInt(r.epoch || 0), 'times claimed'),
			);
			why.replaceChildren(
				...(r.error ? [h('h3', null, r.status === 'stuck' ? 'Why it is stuck' : 'Last failure'), h('pre', { class: 'json bad' }, r.error)] : []),
				...(r.cause ? [h('h3', null, 'Why it left its way'), h('pre', { class: 'json' }, r.cause)] : []),
			);
			timing.replaceChildren(
				...defs([
					['due', r.status === 'active' ? rel(r.wake) : null],
					['retry at', rel(r.retry_at)],
					['deadline', rel(r.deadline)],
					['owner', r.owner ? nodeLink(r.owner) : null],
					['lease until', rel(r.lease_until)],
					['created', rel(r.created)],
					['updated', rel(r.updated)],
				]),
			);
			data.replaceChildren(r.data !== undefined ? json(r.data) : r.data_omitted ? h('span', { class: 'muted' }, `${fmtBytes(r.data_omitted)}: too large to show.`) : hidden("a run's data"));
			inbox.replaceChildren(
				table(
					[
						{ t: 'Seq', num: true, get: (s) => fmtInt(s.seq) },
						{ t: 'Event', get: (s) => h('b', null, s.event) },
						{ t: 'Version', num: true, get: (s) => (s.version ? `v${s.version}` : '') },
						{ t: 'Payload', wrap: true, get: (s) => (s.payload !== undefined ? h('span', { class: 'mono' }, JSON.stringify(s.payload)) : s.payload_omitted ? h('span', { class: 'muted' }, `${fmtBytes(s.payload_omitted)}: too large to show`) : '') },
					],
					r.inbox || [],
					{ empty: 'None.' },
				),
			);
			acts.replaceChildren(
				h('h3', null, 'Act'),
				!app.info.allow_writes
					? readOnlyNote()
					: r.status === 'stuck'
						? h('button', { type: 'button', class: 'danger', onclick: () => act(`resumed ${id}`, '/api/saga/resume', { node: on, saga, id }, `Resume run ${id} of ${saga}? It becomes active and due at once, its attempts counted from none, and its effect is tried again.`) }, 'Resume')
						: h('span', { class: 'muted' }, 'Only a stuck run can be resumed.'),
			);
		},
	};
}

// ---- the app ------------------------------------------------------------

const pages = {
	cluster: { make: clusterPage, title: 'Cluster' },
	node: { make: nodePage, node: true, title: 'Node' },
	processes: { make: processesPage, node: true, title: 'Processes' },
	names: { make: namesPage, node: true, title: 'Global names' },
	groups: { make: groupsPage, node: true, title: 'Process groups' },
	tree: { make: treePage, node: true, title: 'Supervision' },
	events: { make: eventsPage, node: true, title: 'Events' },
	cron: { make: cronPage, title: 'Cron' },
	elections: { make: electionsPage, title: 'Elections' },
	sagas: { make: sagasPage, title: 'Sagas' },
};

const app = { info: { allow_writes: false }, nodes: [], prefix: '', params: {}, page: null, key: '', drawer: null, interval: 1000, timer: 0, busy: false };

const home = () => app.nodes[0]?.name || '';

function route() {
	const [path, q] = location.hash.replace(/^#\/?/, '').split('?');
	return { page: pages[path] ? path : 'cluster', params: Object.fromEntries(new URLSearchParams(q || '')) };
}

const href = (page, params) => '#/' + page + query(params);

// go opens page with params. With replace, the address changes in place:
// typing in a filter does not fill the history.
function go(page, params, replace) {
	const url = href(page, params || {});
	if (replace) {
		history.replaceState(null, '', url);
		onRoute();
	} else location.hash = url;
}

function set(changes, replace = true) {
	const r = route();
	go(r.page, { ...r.params, ...changes }, replace);
}

const openProc = (pid) => set({ proc: pid }, false);
// The drawer shows a process, or on the Sagas page a run; a process opened
// over a run closes back to it.
const closeDrawer = () => set(app.params.proc ? { proc: '' } : { run: '' }, false);

function onRoute() {
	const { page, params } = route();
	app.params = params;
	const node = params.node || home();
	selectNode(node);
	// A page starts over when it, or the node it shows, changes; other params
	// it takes as they come.
	const key = page + '|' + (pages[page].node ? node : '');
	if (key !== app.key) {
		app.page?.unmount?.();
		app.key = key;
		app.page = pages[page].make(node, params);
		$('#main').replaceChildren(app.page.el);
		document.title = `${pages[page].title} · grpcprocctl web`;
		for (const a of document.querySelectorAll('.side a')) a.classList.toggle('on', a.dataset.page === page);
		run(app.page);
	} else app.page.update?.(params);
	const open = params.proc ? `proc\n${params.proc}` : page === 'sagas' && params.run ? `run\n${params.saga || ''}\n${params.run}\n${params.on || ''}` : '';
	if (open !== (app.drawer?.key || '')) openDrawer(open && (() => (params.proc ? procDrawer(params.proc) : runDrawer(params.saga || '', params.run, params.on || ''))));
}

// openDrawer shows what make makes in the drawer, or closes it.
function openDrawer(make) {
	const d = $('#drawer');
	app.drawer = null;
	d.hidden = !make;
	d.replaceChildren();
	if (!make) return;
	app.drawer = make();
	d.append(app.drawer.el);
	run(app.drawer);
	app.drawer.first?.();
}

// run ticks a view once, showing what went wrong in its banner. A tick that
// a later one of the view overtook is stale: once stale() says so it draws
// nothing, and how it went is not shown.
async function run(v) {
	v.last = Date.now();
	const seq = (v.seq = (v.seq || 0) + 1);
	const stale = () => v.seq !== seq;
	try {
		await v.tick(stale);
		if (stale()) return;
		v.banner(null);
		live(true);
	} catch (e) {
		if (stale()) return;
		v.banner(e.message);
		live(!offline(e), e.message);
	}
}

// offline says whether e is this page's server or the Inspector out of
// reach, as the header shows it. What the Inspector says begins with
// "inspect: ", a peer it could not reach or resolve among it: that is the
// view's own trouble, which its banner shows. A request that ran out of
// time unanswered is taken for the Inspector's.
const offline = (e) => !e.status || ((e.status === 502 || e.status === 504) && !e.message.startsWith('inspect: '));

async function tick() {
	if (app.busy || document.hidden) return;
	app.busy = true;
	const now = Date.now();
	await Promise.all([app.page, app.drawer].filter((v) => v && !(v.every && now - (v.last || 0) < v.every)).map(run));
	app.busy = false;
}

// refresh ticks now, as after a change: what it changed shows at once.
function refresh() {
	for (const v of [app.page, app.drawer]) if (v) run(v);
}

function live(ok, msg) {
	const el = $('#live');
	if (!app.interval) {
		el.className = 'live';
		el.textContent = 'paused';
		el.title = 'Click to refresh once';
		return;
	}
	el.className = ok ? 'live ok' : 'live bad';
	el.textContent = ok ? 'live' : 'unreachable';
	el.title = ok ? `Updated ${new Date().toLocaleTimeString()}. Click to refresh now.` : msg;
}

function startTimer() {
	clearInterval(app.timer);
	if (app.interval) app.timer = setInterval(tick, app.interval);
	live(true);
}

function setNodes(nodes) {
	app.nodes = nodes;
	app.prefix = namePrefix(nodes.map((n) => n.name));
	selectNode(app.params.node || home());
}

// pickerWords are the picker's options' words, as last put in.
let pickerWords = [];

// selectNode shows node in the picker, which offers every node the
// cluster's walk found, as the browser filters them by what is typed.
function selectNode(node) {
	const input = $('#node'), list = $('#node-list');
	const names = app.nodes.map((n) => n.name);
	if (!names.includes(node) && node) names.push(node);
	const words = app.nodes.map((n) => STATE[stateKey(n)].word || '');
	if (names.length > words.length) words.push('');
	if (list.options.length !== names.length || [...list.options].some((o, i) => o.value !== names[i]) || words.some((w, i) => w !== pickerWords[i])) {
		list.replaceChildren(...names.map((n, i) => h('option', { value: n, label: words[i] || null })));
		pickerWords = words;
	}
	if (document.activeElement !== input) input.value = node;
}

// pickNode opens the node typed or picked, if it is one; anything else is
// put back.
function pickNode(e) {
	const name = e.target.value;
	if (app.nodes.some((n) => n.name === name)) set({ node: name }, false);
	else e.target.value = app.params.node || home();
}

// The theme follows the site's: a g-root_theme_* class on the root, the one
// chosen last with the button, or the system's, as theme.js settled it
// before the page painted.
function setTheme(theme) {
	const root = document.documentElement;
	root.classList.toggle('g-root_theme_dark', theme === 'dark');
	root.classList.toggle('g-root_theme_light', theme !== 'dark');
	redrawCharts();
}

function chosenTheme() {
	try {
		return localStorage.getItem('gp-theme');
	} catch {
		return null; // storage is off: follow the system
	}
}

function toggleTheme() {
	const theme = document.documentElement.classList.contains('g-root_theme_dark') ? 'light' : 'dark';
	try {
		localStorage.setItem('gp-theme', theme);
	} catch {
		// storage is off: the choice lasts as long as the page
	}
	setTheme(theme);
}

async function boot() {
	try {
		app.info = await api('/api/info');
	} catch (e) {
		live(!offline(e), e.message);
	}
	const mode = $('#mode');
	mode.textContent = app.info.allow_writes ? 'writes allowed' : 'read-only';
	mode.className = `label label_${app.info.allow_writes ? 'warning' : 'unknown'}`;
	const version = `grpcprocctl ${app.info.version || ''}`;
	$('#about').replaceChildren(h('div', { title: version }, middle(version, 26)), h('div', { title: `The Inspector this page reads through: ${app.info.target || ''}` }, middle(app.info.target || '', 26)));
	try {
		setNodes((await api('/api/nodes')).nodes);
	} catch (e) {
		live(!offline(e), e.message);
	}
	$('#node').addEventListener('change', pickNode);
	$('#interval').addEventListener('change', (e) => {
		app.interval = Number(e.target.value);
		startTimer();
	});
	$('#live').addEventListener('click', refresh);
	$('#theme').addEventListener('click', toggleTheme);
	addEventListener('hashchange', onRoute);
	addEventListener('resize', debounce(redrawCharts, 100));
	addEventListener('keydown', (e) => e.key === 'Escape' && app.drawer && closeDrawer());
	matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => chosenTheme() || setTheme(e.matches ? 'dark' : 'light'));
	// The node list follows the cluster while pages other than Cluster show.
	setInterval(async () => {
		if (app.interval && !document.hidden && !app.key.startsWith('cluster')) {
			try {
				setNodes((await api('/api/nodes')).nodes);
			} catch {
				// the page's own tick says so
			}
		}
	}, 15000);
	onRoute();
	startTimer();
}

boot();
