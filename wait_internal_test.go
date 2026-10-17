package grpcproc

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
)

// A process that is exiting as Wait finds it, its watchers already taken,
// is ErrNoProc, as one that has exited is.
func TestWaitOnAProcessExiting(t *testing.T) {
	n, err := NewNode(Config{Admit: AdmitAll, Name: "a", Resolver: StaticResolver{}, Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(t.Context()) })
	block := make(chan struct{})
	defer close(block)
	a, err := n.Spawn[proto.Message](func(*Process[proto.Message]) error {
		<-block
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	p := n.lookup(a.PID(), "")
	p.mu.Lock()
	p.exited = true // as terminate marks it, before it leaves n.procs
	p.mu.Unlock()
	if _, err := n.Wait(t.Context(), a.PID()); !errors.Is(err, ErrNoProc) {
		t.Fatalf("%v", err)
	}
	n.waitersMu.Lock()
	defer n.waitersMu.Unlock()
	if len(n.waiters) != 0 {
		t.Fatalf("waiters left: %v", n.waiters)
	}
}

// A Down to the node's own PID answers a waiter only from a process of this
// node: one a peer sends, forged or astray, is dropped as it always was.
func TestWaitIgnoresAPeersDown(t *testing.T) {
	n, err := NewNode(Config{Admit: AdmitAll, Name: "a", Resolver: StaticResolver{}, Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(t.Context()) })
	ch := make(chan string, 1)
	n.waitersMu.Lock()
	n.waiters[7] = ch
	n.waitersMu.Unlock()
	n.deliverDown(PID{Node: "b", Incarnation: 1, ID: 3}, n.PID(), 7, "forged")
	select {
	case reason := <-ch:
		t.Fatalf("answered with %q", reason)
	default:
	}
	n.deliverDown(PID{Node: "a", Incarnation: 1, ID: 3}, n.PID(), 7, "exited")
	if reason := <-ch; reason != "exited" {
		t.Fatalf("answered with %q", reason)
	}
}
