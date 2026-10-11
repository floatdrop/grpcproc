package platform

import (
	"maps"
	"slices"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/grpcproc"
	grpcprocetcd "github.com/floatdrop/grpcproc/etcd"
	"github.com/floatdrop/grpcproc/groups"
)

// Prefix is where the runtime's nodes are kept in etcd.
const Prefix = "/agents"

// scope is the name of the runtime's groups scope, which admit exports.
const scope = groups.DefaultScope

// cluster is how the node finds the others: the peers the configuration
// names, or, with ETCD, the nodes registered there, which this one joins as
// it starts and leaves as it stops.
type cluster struct {
	resolver   grpcproc.Resolver
	registrar  grpcproc.Registrar  // nil with a static list
	membership grpcproc.Membership // nil with a static list
	client     *clientv3.Client    // etcd's, closed after the node stops
}

func newCluster(cfg Config) (*cluster, error) {
	if len(cfg.Etcd) == 0 {
		return &cluster{resolver: grpcproc.StaticResolver(cfg.Peers)}, nil
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	c := grpcprocetcd.New(client, Prefix)
	return &cluster{resolver: c, registrar: c, membership: c, client: client}, nil
}

func (c *cluster) close() error {
	if c.client == nil {
		return nil
	}
	return c.client.Close()
}

// newScope is the node's part of the runtime's process groups: a service's
// processes join a group, and other services find them in it, on any node.
// The scope asks the nodes the node knows of, from etcd or the
// configuration, but those it does not trust, which may ask it nothing.
func newScope(cfg Config, n *grpcproc.Node) (*groups.Scope, error) {
	trusted := func(node string) bool { return !slices.Contains(cfg.Untrusted, node) }
	var peers []string
	for _, node := range slices.Sorted(maps.Keys(cfg.Peers)) {
		if trusted(node) {
			peers = append(peers, node)
		}
	}
	return groups.Start(n, groups.Config{
		Scope:  scope,
		Peers:  peers,
		Select: func(m grpcproc.Member) bool { return trusted(m.Name) },
	})
}
