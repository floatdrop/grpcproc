package grpcproc

import (
	"context"
	"errors"
	"testing"
)

// stoppingMembership marks its node stopping as its Watch is called, as a
// Stop that ran meanwhile would.
type stoppingMembership struct{ n *Node }

func (m stoppingMembership) Watch(ctx context.Context) (<-chan MemberEvent, error) {
	m.n.mu.Lock()
	m.n.stopping = true
	m.n.mu.Unlock()
	out := make(chan MemberEvent)
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out, nil
}

// A Stop that began while the Membership was being asked to watch would
// not wait for the watch: Start fails, and waits for it itself.
func TestStartFindsStopBeganWhileWatching(t *testing.T) {
	n, err := NewNode(Config{Admit: AdmitAll, Name: "a", Resolver: StaticResolver{}, Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	n.cfg.Membership = stoppingMembership{n}
	if err := n.Start(t.Context()); !errors.Is(err, ErrNodeStopped) {
		t.Fatalf("%v", err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.watched != nil {
		t.Fatal("a watch Stop would not wait for is kept")
	}
}
