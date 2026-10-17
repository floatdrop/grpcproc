package grpcproc_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
)

// Wait returns a process's exit reason to plain Go: by PID, by name, and
// for a node that stops; a process that has exited is ErrNoProc, and a
// peer's an error.
func TestWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a := c.Node("a")
		e, _ := a.Spawn(echo, grpcproc.WithName("echo"))
		type result struct {
			reason string
			err    error
		}
		wait := func(to grpcproc.Target) <-chan result {
			ch := make(chan result, 1)
			go func() {
				reason, err := a.Wait(t.Context(), to)
				ch <- result{reason, err}
			}()
			return ch
		}
		byName, byPID := wait(grpcproc.Name{Node: "a", Name: "echo"}), wait(e.PID())
		synctest.Wait()
		if err := a.Exit(t.Context(), e.PID(), "boom"); err != nil {
			t.Fatal(err)
		}
		for _, ch := range []<-chan result{byName, byPID} {
			if r := <-ch; r.reason != "boom" || r.err != nil {
				t.Fatalf("%+v", r)
			}
		}
		if _, err := a.Wait(t.Context(), e.PID()); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("exited: %v", err)
		}
		if _, err := a.Wait(t.Context(), grpcproc.Global{Name: "nobody"}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("a global name nobody holds: %v", err)
		}
		remote, _ := c.Node("b").Spawn(echo)
		if _, err := a.Wait(t.Context(), remote.PID()); err == nil || errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("a peer's process: %v", err)
		}

		// A wait given up leaves the process alone; one still on when the
		// node stops ends with the shutdown.
		e2, _ := a.Spawn(echo)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := a.Wait(ctx, e2.PID()); !errors.Is(err, context.Canceled) {
			t.Fatalf("given up: %v", err)
		}
		if _, ok := a.Process(e2.PID()); !ok {
			t.Fatal("the process is gone")
		}
		stopped := wait(e2.PID())
		synctest.Wait()
		c.Stop("a")
		if r := <-stopped; r.reason != grpcproc.ReasonShutdown || r.err != nil {
			t.Fatalf("%+v", r)
		}
	})
}
