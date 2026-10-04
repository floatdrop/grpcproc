package platform

import (
	"cmp"
	"fmt"
	"os"
	"strings"
)

// Config is what a program is told: which node it is, where it listens, and
// where the services it does not run are. Services address each other
// through Where, so moving a service to another node changes which entry
// point composes it and what the programs are told, and no service.
type Config struct {
	// Node is the node's name. NODE, "local" by default.
	Node string
	// Listen is where its gRPC server listens. LISTEN.
	Listen string
	// HTTP is where the web front listens, when the program runs it. HTTP.
	HTTP string
	// Peers are the gRPC addresses of the nodes it talks to, and of those
	// that talk to it: a reply travels back on the replier's own link.
	// PEERS, as node=addr,node=addr.
	Peers map[string]string
	// Placement names the node of each service that does not run here, or
	// its nodes, for one that runs on several. PLACEMENT, as
	// service=node,service=node+node.
	Placement map[string]string
	// Export names the processes this node offers its peers: they may send
	// to, call and monitor those, and nothing else. With none, they may ask
	// anything. EXPORT, as name,name.
	Export []string
	// Untrusted names the peers that may ask nothing of this node: they
	// answer what it asks them. UNTRUSTED, as node,node.
	Untrusted []string
}

// Where is the node that runs service: the one Placement names, the first
// if it names several, or this one.
func (c Config) Where(service string) string { return c.All(service)[0] }

// All is the nodes that run service: those Placement names, or this one.
func (c Config) All(service string) []string {
	return strings.Split(cmp.Or(c.Placement[service], c.Node), "+")
}

// Load reads the configuration from the environment. With none set, it is
// the local deployment: one node, every service on it.
func Load() (Config, error) {
	peers, err := pairs("PEERS")
	if err != nil {
		return Config{}, err
	}
	placement, err := pairs("PLACEMENT")
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Node:      env("NODE", "local"),
		Listen:    env("LISTEN", "127.0.0.1:9100"),
		HTTP:      env("HTTP", "127.0.0.1:8080"),
		Peers:     peers,
		Placement: placement,
		Export:    list("EXPORT"),
		Untrusted: list("UNTRUSTED"),
	}
	// A service placed on a node nobody can dial fails every call to it;
	// better to fail here.
	for service := range cfg.Placement {
		for _, node := range cfg.All(service) {
			if node != cfg.Node && cfg.Peers[node] == "" {
				return Config{}, fmt.Errorf("platform: PLACEMENT puts %s on %s, which PEERS does not name", service, node)
			}
		}
	}
	return cfg, nil
}

// list parses name,name.
func list(key string) []string {
	var out []string
	for name := range strings.SplitSeq(os.Getenv(key), ",") {
		if name = strings.TrimSpace(name); name != "" {
			out = append(out, name)
		}
	}
	return out
}

func env(key, fallback string) string { return cmp.Or(os.Getenv(key), fallback) }

// pairs parses key=value,key=value.
func pairs(key string) (map[string]string, error) {
	out := map[string]string{}
	for pair := range strings.SplitSeq(os.Getenv(key), ",") {
		if pair = strings.TrimSpace(pair); pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		switch {
		case !ok || k == "" || v == "":
			return nil, fmt.Errorf("platform: %s: %q is not name=value", key, pair)
		case out[k] != "":
			return nil, fmt.Errorf("platform: %s names %s twice", key, k)
		}
		out[k] = v
	}
	return out, nil
}
