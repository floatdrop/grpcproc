import { Code } from '../code.tsx';
import { A, Aside, C, Table } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const guidesOperations: Doc = {
	path: 'guides/operations/',
	title: 'Running in production',
	description:
		'What a deployment sets that a first program does not: admission, keepalive, limits, identity across restarts, the election, the order of a shutdown, and what to watch.',
	lead: (
		<p>
			A node's defaults suit a program on one machine. A deployment decides a few things more: who
			may link to its nodes, how fast a silent peer is noticed, how large a message may be and how
			much may queue, how nodes are told apart across restarts, how a leader hands over, and in
			what order a node stops. Each section says what to set, and what goes wrong without it.{' '}
			<A to="guides/configuration/">Configuring a node</A> has every field.
		</p>
	),
	sections: [
		{
			id: 'checklist',
			title: 'Checklist',
			body: (
				<Table
					head={['Set', 'Or else']}
					rows={[
						[
							<>
								<C>Admit: grpcproc.AdmitTLS(…)</C>, mTLS on the server, <C>ServerName</C> per peer
							</>,
							'Whoever reaches the server may stand in for a node and exit any process.'
						],
						[
							<>Keepalive on the server and on every dial</>,
							'A peer that dies silently is not declared down for minutes, and its monitors do not fire.'
						],
						[<>A <C>Membership</C> (etcd)</>, 'A crashed peer behind a half-open connection lingers until keepalive notices it.'],
						[
							<>
								<C>MaxQueued</C>, <C>WithMailboxLimit</C> where load is not shed by the process
							</>,
							'A slow peer or process holds what is sent to it in memory, without bound.'
						],
						[
							<>
								<C>MaxMessageSize</C> no larger than every peer's <C>grpc.MaxRecvMsgSize</C>
							</>,
							'The default, 4 MiB on both, already agrees; raised on one side only, links break.'
						],
						[<>Incarnations that grow across restarts</>, 'A restarted node is refused as older than itself.'],
						[<>A shutdown in order</>, 'Peers see noconnection rather than shutdown, and a leader\'s state since its last checkpoint is lost.'],
						[<>Alerts on the signals below</>, 'A link that keeps breaking, or a mailbox that keeps growing, goes unseen.']
					]}
				/>
			)
		},
		{
			id: 'admission',
			title: 'Admission',
			body: (
				<>
					<p>
						A node's service is mounted on the program's gRPC server, which may be reachable by more
						than the cluster, and a peer is who its stream's metadata says it is. So{' '}
						<C>Config.Admit</C> is required: with <C>grpcproc.AdmitAll</C>, anyone who reaches the
						server may claim a node's name, with a newer incarnation that keeps the real node out,
						and exit any process. Use mutual TLS and <C>grpcproc.AdmitTLS</C>, which admits a peer
						whose verified client certificate names its node exactly, as a DNS subject alternative
						name, or an IP one of the address the name is. A wildcard names no node.
					</p>
					<Code>{`creds := credentials.NewTLS(&tls.Config{
	Certificates: []tls.Certificate{cert}, // its SAN is this node's name
	ClientCAs:    pool,
	ClientAuth:   tls.RequireAndVerifyClientCert,
})
srv := grpc.NewServer(grpc.Creds(creds), /* keepalive, below */)

node, err := grpcproc.NewNode(grpcproc.Config{
	Name:  "orders-1",
	Admit: grpcproc.AdmitTLS(nil),
	DialOptionsFor: func(peer string) []grpc.DialOption {
		// The server dialed must present a certificate naming the node meant.
		return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: peer,
		}))}
	},
	// …
})`}</Code>
					<p>
						Each node's certificate is used both ways, as the server's and as the client's when it
						dials, so it needs both the <C>serverAuth</C> and the <C>clientAuth</C> extended key
						usage; a server certificate with the first alone fails every dial.{' '}
						<C>Admit</C> checks who dialed this node; the <C>ServerName</C> checks the node this one
						dials, which otherwise is whoever answers at the address the resolver gave. A node of
						another installation gets a <C>Policy</C> instead of everything:{' '}
						<C>grpcproc.Export</C> lets it send to, call and monitor the names listed, and exit
						nothing (<A to="guides/configuration/#security">Security</A>).
					</p>
					<p>
						The Inspector, if the node serves one, is a second service on the same server, and it
						can send, call and exit too. Guard it with the server's interceptors, or build it with{' '}
						<C>inspect.ReadOnly()</C>, which refuses all three.
					</p>
				</>
			)
		},
		{
			id: 'failure-detection',
			title: 'Noticing a silent peer',
			body: (
				<>
					<p>
						A peer is declared down when its link to this node ends, and only then do monitors of
						its processes fire <C>noconnection</C> and calls to it fail. A peer that crashes cleanly
						closes its connections, and that is seen at once. One that loses power, or is cut off
						by the network, closes nothing: its link ends only when something notices. gRPC
						keepalive is what notices, in seconds, and it has two halves. The dial's pings end
						this node's link to the peer; the server's end the peer's link to this node, the one
						whose end declares the peer down. Without the server's half, only the operating
						system's TCP timeouts, which take minutes, or a <C>Membership</C> end it.
					</p>
					<Code>{`kp := keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}
dial := grpc.WithKeepaliveParams(kp) // in DialOptions

srv := grpc.NewServer(
	grpc.KeepaliveParams(keepalive.ServerParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}),
	// Accept the dials' pings: by default a server allows one every 5 minutes,
	// and answers more with GOAWAY "too_many_pings", ending the connection.
	grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
)`}</Code>
					<p>
						With these, a silent peer is down within about fifteen seconds. A{' '}
						<C>Membership</C> is the second opinion, the cluster's rather than one link's:{' '}
						<C>grpcprocetcd</C> reports a node down when its lease ends, ten seconds after it last
						renewed by default (<C>WithTTL</C>), and every node then drops its links to it. It also
						catches what keepalive cannot, a peer that answers pings but has been replaced.
					</p>
					<p>
						A dial is bounded by <C>DialTimeout</C>, 5 s by default. After a dial fails, sends to the
						peer fail at once with <C>ErrNoConnection</C> for a while, doubling up to{' '}
						<C>DialBackoff</C>, 5 s, rather than each waiting for a dial of its own; so does a link
						that ends within <C>DialTimeout</C> of coming up. Lower them for a cluster that must
						notice a peer coming back sooner; raise <C>DialTimeout</C> across slow networks.
					</p>
				</>
			)
		},
		{
			id: 'limits',
			title: 'Limits',
			body: (
				<>
					<p>
						Everything a node holds for others is unbounded by default, as Erlang's is: a send
						never waits and never fails for a slow receiver. That is right until a receiver falls
						behind for good, and then memory grows until something gives. Three bounds refuse
						instead, each for one kind of slowness. A call always learns of a refusal; a send
						learns of a link's and a message's, while a full mailbox makes it a dead letter on the
						receiver's node, as a send to a missing process is.
					</p>
					<Table
						head={['Bound', 'When it is reached']}
						rows={[
							[
								<>
									<C>MaxQueued</C>, <C>MaxQueuedBytes</C>: a peer's link
								</>,
								<>
									Sends and calls to that peer fail at once with <C>ErrLinkBusy</C>, as{' '}
									<C>Unsent</C>: safe to send again later. Other peers are not affected.
								</>
							],
							[
								<>
									<C>WithMailboxLimit(n)</C>: a process
								</>,
								<>
									A message to it is a dead letter, <C>mailbox full</C>; a call fails with{' '}
									<C>ErrMailboxFull</C>, never handled. <C>Down</C>s and exits always get in.
								</>
							],
							[
								<>
									<C>MaxMessageSize</C>: one message
								</>,
								<>
									A send or a call to another node that encodes larger fails with{' '}
									<C>ErrTooLarge</C>, never sent; a reply that large reaches its caller as that
									error.
								</>
							]
						]}
					/>
					<p>
						Bound a process's mailbox when it cannot shed load itself, a writer to a database that
						may stall, say; a process that answers from memory rarely needs one. Size a link's
						bound for a burst, not for steady load: it is what the node may hold for a peer that
						stopped reading.
					</p>
					<p>
						<C>MaxMessageSize</C> must not exceed any peer's <C>grpc.MaxRecvMsgSize</C>: a peer ends
						a link that sends it more, with every call and monitor on it. Both are 4 MiB by
						default. Nothing tells a node its peers' limits, so to carry larger messages, raise{' '}
						<C>MaxRecvMsgSize</C> on every server first, and <C>MaxMessageSize</C> after, in a
						second rollout. A message that large holds up everything behind it on its link,{' '}
						<C>Down</C>s included; for blobs, send a reference to where they are stored instead.
					</p>
				</>
			)
		},
		{
			id: 'identity',
			title: 'Names and restarts',
			body: (
				<>
					<p>
						A node's name is its address and must be unique in the cluster. Two programs started
						with one name are taken as two incarnations of one node: the newer one replaces the
						older on every peer that hears from both, and the older one's links are refused. That
						is what a restart should do, and what a misconfigured second replica must not.
					</p>
					<p>
						The incarnation must grow with each start. The default is the start time in
						nanoseconds, which it does as long as clocks agree to within the time between two
						starts; a node whose clock went back is refused as older than itself, until its peers
						forget the newer one: <C>Membership</C> reports it gone, or <C>Disconnect</C>. Where
						clocks cannot be trusted, give{' '}
						<C>Config.Incarnation</C> from a counter that outlives the program: a generation in
						the orchestrator, or a number kept on disk.
					</p>
					<p>
						Incarnations fence grpcproc's traffic only. A replaced node that still runs may still
						write to a database; fence that with <C>Lease.Term</C> (below) or a global name's{' '}
						<C>Claim.Revision</C> (<A to="concepts/addressing/">Addressing</A>).
					</p>
					<Aside title="Rolling upgrades">
						Nodes link only if they speak one protocol version, which each release's notes give.
						Releases that keep it can share a cluster, so a cluster upgrades one node at a time;
						a release that changes it says so, and its nodes cannot link with older ones.
					</Aside>
				</>
			)
		},
		{
			id: 'election',
			title: 'The leader election',
			body: (
				<>
					<p>
						<C>Voters</C> is a fixed set of nodes; a majority of it elects, and two majorities of
						one set always share a node, so no partition elects two leaders. Use it where the
						nodes that may lead are known. Without it, the view is <C>Peers</C> and whatever{' '}
						<C>Membership</C> reports, which follows a cluster that scales, at the cost of a view
						that can change under an election; <C>MinClusterSize</C>, 3 by default, keeps a
						fragment of it from electing.
					</p>
					<p>
						<C>ElectionTimeout</C>, 150 ms by default, suits nodes on one network. Raise it where a
						pause, a slow round trip, or a slow disk under <C>Store</C> can approach it: saves
						hold up the elector's heartbeats. Give every node a <C>Store</C> (
						<C>leader.File</C>), so that a cluster that restarts whole goes on from its last
						checkpoint, and its terms go on growing.
					</p>
					<p>
						Two nodes can briefly both believe they lead: a leader cut off from the others leads
						until it notices, at most two election timeouts, while they elect another. Pass{' '}
						<C>Lease.Term</C> to anything the singleton writes to, which keeps the highest term it
						has seen and refuses lower ones. Where leadership must wait for an external lock, an
						etcd lease or a Kubernetes Lease, take it in <C>Confirm</C>.
					</p>
					<p>
						To work on a host, <C>leader.Cordon</C> it: it campaigns no more, and hands over if it
						leads, across its own restarts, until <C>Uncordon</C>. <C>grpcprocctl leader</C> does
						both from a terminal (<A to="guides/leader/">Leader election</A>).
					</p>
				</>
			)
		},
		{
			id: 'shutdown',
			title: 'Shutting down',
			body: (
				<>
					<p>
						A node that stops tells its peers: processes exit with reason <C>shutdown</C>, their
						watchers get <C>Down{'{'}shutdown{'}'}</C> over links that are still up, every link
						flushes before it closes, and the <C>Registrar</C>'s record is withdrawn last. That
						needs <C>Stop</C> to run before the gRPC server stops, and a few steps before it:
					</p>
					<Code>{`ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
defer cancel()

// 1. A leader hands over, so the next one starts from the singleton's last state.
if err := leader.Resign(ctx, node, "billing"); err != nil && !errors.Is(err, leader.ErrNotLeader) {
	log.Warn("resign", "err", err)
}
// 2. The top supervisor stops its children in reverse order of their start.
events := node.Subscribe(ctx, 64)
_ = node.Exit(ctx, root, grpcproc.ReasonShutdown)
for ev := range events { // closed when ctx ends
	if ev.Kind == grpcproc.EventExit && ev.Process.PID == root {
		break
	}
}
// 3. Every process left exits, links flush and close, the record is withdrawn.
_ = node.Stop(ctx)
// 4. Only now the server.
srv.GracefulStop()`}</Code>
					<p>
						<C>Stop</C> alone asks every process to exit at once, so a supervisor cannot stop its
						children one by one, the last started first; stopping the top supervisor first lets it.
						grpcproc does not handle signals: the program decides when this runs, on SIGTERM
						usually, and its orchestrator's grace period bounds the ctx.
					</p>
				</>
			)
		},
		{
			id: 'watching',
			title: 'What to watch',
			body: (
				<>
					<p>
						<C>grpcprocotel</C> exports the metrics below. A node's snapshots, which{' '}
						<C>grpcprocctl</C> and the Inspector read, hold the totals, the mailboxes and each
						link's state, and <C>Node.Subscribe</C> streams the events behind them (
						<A to="guides/observability/">Observability</A>). The signals worth an alert:
					</p>
					<Table
						head={['Signal', 'What it means']}
						rows={[
							[
								<>
									<C>grpcproc.links.down</C> rising for one peer; <C>LinkInfo.Sessions</C> growing
								</>,
								<>
									Its links keep breaking: keepalive refused (<C>too_many_pings</C>), a proxy
									cutting streams, a message over its server's limit. The link-down event (
									<C>Hooks.OnLinkDown</C>, <C>grpcprocctl watch</C>) carries why;{' '}
									<C>LinkInfo.LastError</C> does for a link that ended young.
								</>
							],
							[
								<>
									<C>grpcproc.dead_letters</C> by reason
								</>,
								<>
									<C>mailbox full</C>: a process is overloaded. <C>denied</C>: an export list is
									wrong. <C>noconnection</C>: messages lost to broken links. <C>type</C>: two
									versions disagree on a message.
								</>
							],
							[
								<>
									<C>grpcproc.mailbox.oldest</C> by label
								</>,
								'A process falling behind, before its mailbox fills.'
							],
							[
								<>
									<C>grpcproc.call.duration</C> with <C>error.type</C>
								</>,
								<>
									<C>busy</C> and <C>mailbox_full</C>: overload. <C>noconnection</C>: a peer
									gone. <C>timeout</C>: a callee too slow for its callers.
								</>
							],
							[
								<>
									<C>grpcproc.processes.exited</C> with reason <C>panic</C>, <C>error</C>,{' '}
									<C>max restarts</C>
								</>,
								'Crashes, and a supervisor that gave up.'
							],
							[
								<>A leader's term rising, from <C>leader.Status</C>: no metric carries it</>,
								<>
									Elections keep happening: <C>ElectionTimeout</C> too low for the network, or a
									node flapping. <C>grpcprocctl leader</C> shows each node's view.
								</>
							]
						]}
					/>
				</>
			)
		}
	]
};
