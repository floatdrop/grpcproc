package grpcproc_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// testCA issues certificates naming nodes, valid from 1970, since the
// clock in a synctest bubble starts in 2000.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Unix(0, 0), NotAfter: time.Unix(1<<32, 0),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

// issue makes a certificate for a client and a server that names dnsName.
func (ca *testCA) issue(t *testing.T, dnsName string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: dnsName}, DNSNames: []string{dnsName},
		NotBefore: time.Unix(0, 0), NotAfter: time.Unix(1<<32, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// mtlsNode starts a node named name over bufconn, serving with mTLS that
// verifies clients against ca, presenting cert both ways, and admitting
// peers with AdmitTLS. lns are the other nodes' listeners, by name.
func mtlsNode(t *testing.T, name string, cert tls.Certificate, ca *testCA, lns map[string]*bufconn.Listener) *grpcproc.Node {
	t.Helper()
	ln := bufconn.Listen(1 << 20)
	lns[name] = ln
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, ClientCAs: ca.pool, ClientAuth: tls.RequireAndVerifyClientCert,
	})))
	resolver := grpcproc.StaticResolver{}
	for _, peer := range []string{"a", "b", "c"} {
		resolver[peer] = "passthrough:///" + peer
	}
	n, err := grpcproc.NewNode(grpcproc.Config{
		Name: name, Resolver: resolver, Admit: grpcproc.AdmitTLS(nil),
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: ca.pool})),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) { return lns[addr].DialContext(ctx) }),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Register(srv)
	go func() { _ = srv.Serve(ln) }()
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()); srv.Stop() })
	return n
}

func TestAdmitTLS(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ca := newTestCA(t)
		lns := map[string]*bufconn.Listener{}
		a := mtlsNode(t, "a", ca.issue(t, "a"), ca, lns)
		b := mtlsNode(t, "b", ca.issue(t, "b"), ca, lns)
		// c's certificate names another node: what it claims is not proven.
		c := mtlsNode(t, "c", ca.issue(t, "someone-else"), ca, lns)
		e := spawnEcho(t, b)

		// Each certificate names its node: admitted.
		if r, err := e.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
			t.Fatalf("a to b: %v %v", r, err)
		}
		// c's dial reaches b's server, whose TLS name checks out, and b
		// refuses c, whose certificate does not name it.
		_, err := e.Call[*testpb.Pong](ctx(t), c, &testpb.Ping{N: 1})
		if le, ok := errors.AsType[*grpcproc.LinkError](err); !ok || status.Code(le.Err) != codes.PermissionDenied {
			t.Fatalf("c to b: %v", err)
		}
	})
}

// A certificate names a node exactly: no wildcard, no other case.
func TestAdmitTLSMatchesExactly(t *testing.T) {
	admit := grpcproc.AdmitTLS(nil)
	with := func(cert *x509.Certificate) context.Context {
		return peer.NewContext(t.Context(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
			VerifiedChains: [][]*x509.Certificate{{cert}},
		}}})
	}
	cases := []struct {
		cert  *x509.Certificate
		node  string
		admit bool
	}{
		{&x509.Certificate{DNSNames: []string{"orders-1"}}, "orders-1", true},
		{&x509.Certificate{DNSNames: []string{"orders-1"}}, "ORDERS-1", false},
		{&x509.Certificate{DNSNames: []string{"orders-1"}}, "orders-1.", false},
		{&x509.Certificate{DNSNames: []string{"*.prod"}}, "orders.prod", false},
		{&x509.Certificate{IPAddresses: []net.IP{net.ParseIP("10.0.0.5")}}, "10.0.0.5", true},
		{&x509.Certificate{IPAddresses: []net.IP{net.ParseIP("10.0.0.5")}}, "10.0.0.6", false},
		{&x509.Certificate{IPAddresses: []net.IP{net.ParseIP("2001:db8::1")}}, "2001:DB8::1", true},
		{&x509.Certificate{IPAddresses: []net.IP{net.ParseIP("2001:db8::1")}}, "orders-1", false},
	}
	for _, tc := range cases {
		if _, err := admit(with(tc.cert), grpcproc.NodeID{Name: tc.node}); (err == nil) != tc.admit {
			t.Errorf("%v %v for %q: %v", tc.cert.DNSNames, tc.cert.IPAddresses, tc.node, err)
		}
	}
}

func TestAdmitTLSWithoutAVerifiedCertificate(t *testing.T) {
	admit := grpcproc.AdmitTLS(grpcproc.Export("x"))
	peerB := grpcproc.NodeID{Name: "b"}
	if _, err := admit(t.Context(), peerB); err == nil {
		t.Fatal("admitted with no peer")
	}
	plain := peer.NewContext(t.Context(), &peer.Peer{})
	if _, err := admit(plain, peerB); err == nil {
		t.Fatal("admitted without TLS")
	}
	unverified := peer.NewContext(t.Context(), &peer.Peer{AuthInfo: credentials.TLSInfo{}})
	if _, err := admit(unverified, peerB); err == nil {
		t.Fatal("admitted without a verified chain")
	}
	if pol, err := grpcproc.AdmitAll(t.Context(), peerB); pol != nil || err != nil {
		t.Fatalf("AdmitAll: %v %v", pol, err)
	}
}
