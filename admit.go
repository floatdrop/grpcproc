package grpcproc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// AdmitAll is a Config.Admit that admits every peer, with no Policy: for a
// network where whoever can reach the node's gRPC server is trusted, since
// such a peer may claim any node name and exit any process.
func AdmitAll(context.Context, NodeID) (Policy, error) { return nil, nil }

var errNoCertificate = errors.New("grpcproc: no verified client certificate")

// AdmitTLS is a Config.Admit that admits a peer whose verified TLS client
// certificate names the node it claims to be, and judges it by pol, nil for
// no Policy. The name is a DNS subject alternative name equal to the node's,
// or an IP one of the address the node's name is; a wildcard names no node,
// since a node name is a name, not a host to match. The server's
// credentials must verify client certificates
// (tls.RequireAndVerifyClientCert), or no peer is admitted.
//
// It proves who dialed this node. That the node this one dials is the one
// it meant is for the dial's own credentials: DialOptionsFor can give each
// peer's TLS a ServerName of the peer's node name.
func AdmitTLS(pol Policy) func(ctx context.Context, peer NodeID) (Policy, error) {
	return func(ctx context.Context, id NodeID) (Policy, error) {
		p, ok := peer.FromContext(ctx)
		if !ok {
			return nil, errNoCertificate
		}
		info, ok := p.AuthInfo.(credentials.TLSInfo)
		if !ok || len(info.State.VerifiedChains) == 0 {
			return nil, errNoCertificate
		}
		cert, ip := info.State.VerifiedChains[0][0], net.ParseIP(id.Name)
		if !slices.Contains(cert.DNSNames, id.Name) && (ip == nil || !slices.ContainsFunc(cert.IPAddresses, ip.Equal)) {
			return nil, fmt.Errorf("grpcproc: the certificate does not name node %q", id.Name)
		}
		return pol, nil
	}
}

// Op is what a peer asks of a process on this node, as a Policy sees it.
type Op uint8

// The requests a Policy judges. Replies, Downs and demonitors are not among
// them: they answer, or take back, what was asked, and always pass.
const (
	OpSend    Op = iota + 1 // a message
	OpCall                  // a call
	OpMonitor               // a monitor, or a link, which travels as one
	OpExit                  // a request to exit
)

func (o Op) String() string {
	switch o {
	case OpSend:
		return "send"
	case OpCall:
		return "call"
	case OpMonitor:
		return "monitor"
	case OpExit:
		return "exit"
	}
	return "op(" + itoa(uint64(o)) + ")"
}

// Policy says what a peer may ask of this node's processes: op of the process
// registered under name, or, for one the peer addresses by PID, under the
// name it was spawned with, "" if none. Config.Admit returns one per inbound
// link, which judges everything the peer sends over it for as long as it
// lasts. A request it refuses is answered as if the process did not exist:
// a call fails with ErrNoProc, a monitor or a link gets Down{noproc}, and a
// message or an exit is dropped. This node counts each as a dead letter with
// reason ReasonDenied.
//
// It runs on the goroutine that reads the link, for every request on it, so
// it should not block.
type Policy func(op Op, name string) bool

// Export is the Policy of a peer that may send to, call and monitor the
// processes registered under names, by name or by PID, and nothing else: no
// process without a name, and no exits. It suits a node of another
// installation, which reaches the services this one offers it and is not
// trusted to stop them.
func Export(names ...string) Policy {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	return func(op Op, name string) bool {
		_, ok := set[name]
		return ok && op != OpExit
	}
}

// opOf is the request an envelope of kind makes, or 0 for one a Policy does
// not judge.
func opOf(kind grpcprocv1.Kind) Op {
	switch kind {
	case grpcprocv1.Kind_KIND_SEND:
		return OpSend
	case grpcprocv1.Kind_KIND_CALL:
		return OpCall
	case grpcprocv1.Kind_KIND_MONITOR:
		return OpMonitor
	case grpcprocv1.Kind_KIND_EXIT:
		return OpExit
	}
	return 0
}

// admits says whether pol, which is not nil, lets the peer ask op of the
// process to, or name. A process addressed by PID is judged by the name it
// was spawned with; one that does not exist is let through, for delivery to
// answer as noproc, as it would anyway.
func (n *Node) admits(pol Policy, op Op, to PID, name string) bool {
	if name == "" {
		p := n.lookup(to, "")
		if p == nil {
			return true
		}
		name = p.name
	}
	return pol(op, name)
}
