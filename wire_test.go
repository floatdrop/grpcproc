package grpcproc_test

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// bystander watches a process of a from b: a link between them that breaks
// fires its Down.
func bystander(t *testing.T, a, b *grpcproc.Node) <-chan grpcproc.Msg[proto.Message] {
	t.Helper()
	victim, _ := collector(t, a)
	w, downs := watcher(t, b)
	w.Monitor(victim.PID())
	synctest.Wait()
	return downs
}

// What a peer would refuse is refused before it is sent, and the link to the
// peer, with everything else on it, stays up.
func TestUnsendableIsRefusedAndTheLinkStays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		downs := bystander(t, a, b)
		e := spawnEcho(t, b)
		big := grpcproc.AddrOf[proto.Message](e.PID())

		if err := big.Send(t.Context(), a, wrapperspb.Bytes(make([]byte, 5<<20))); !errors.Is(err, grpcproc.ErrTooLarge) {
			t.Fatalf("send: %v", err)
		}
		if _, err := big.Call[proto.Message](ctx(t), a, wrapperspb.Bytes(make([]byte, 5<<20))); !errors.Is(err, grpcproc.ErrTooLarge) {
			t.Fatalf("call: %v", err)
		}
		bad := grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"k": "\xff"})
		if err := e.Send(bad, a, &testpb.Ping{N: 1}); err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
			t.Fatalf("metadata: %v", err)
		}
		// Locally nothing travels, so nothing is refused.
		if err := big.Send(t.Context(), b, wrapperspb.Bytes(make([]byte, 5<<20))); err != nil {
			t.Fatalf("local: %v", err)
		}

		synctest.Wait()
		noMore(t, downs)
		if r, err := e.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
			t.Fatalf("after: %v %v", r, err)
		}
	})
}

// A reply that cannot be carried reaches its caller as an error, and its
// Reply says why.
func TestUnsendableReplyIsAnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		downs := bystander(t, a, b)
		replied := make(chan error, 2)
		answers, err := b.Spawn(func(p *grpcproc.Process[*wrapperspb.StringValue]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				var resp proto.Message = wrapperspb.Bytes(make([]byte, 5<<20))
				if m.Body.GetValue() == "utf8" {
					resp = wrapperspb.String("\xff")
				}
				replied <- m.Reply(resp, nil)
			}
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = answers.Call[*wrapperspb.BytesValue](ctx(t), a, wrapperspb.String("big"))
		if _, handled := errors.AsType[*grpcproc.RemoteError](err); !handled || !errors.Is(err, grpcproc.ErrTooLarge) {
			t.Fatalf("big: %v", err)
		}
		if err := within(t, replied, "Reply's error"); !errors.Is(err, grpcproc.ErrTooLarge) {
			t.Fatalf("big Reply: %v", err)
		}
		_, err = answers.Call[*wrapperspb.StringValue](ctx(t), a, wrapperspb.String("utf8"))
		if _, handled := errors.AsType[*grpcproc.RemoteError](err); !handled || !strings.Contains(err.Error(), "encode") {
			t.Fatalf("utf8: %v", err)
		}
		if err := within(t, replied, "Reply's error"); err == nil {
			t.Fatal("utf8 Reply: no error")
		}

		synctest.Wait()
		noMore(t, downs)
	})
}

// Names and reasons that protobuf could not carry, or that would outgrow a
// frame, travel cut down to valid text.
func TestNamesAndReasonsTravelAsText(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		downs := bystander(t, a, b)
		long := strings.Repeat("x", 40<<10) + "\xff"

		// A Down's reason.
		quits := make(chan struct{})
		gone, err := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			<-quits
			return errors.New(long)
		})
		if err != nil {
			t.Fatal(err)
		}
		w, seen := watcher(t, b)
		w.Monitor(gone.PID())
		synctest.Wait()
		close(quits)
		d := recv(t, seen).Down
		if d == nil || len(d.Reason) > 20<<10 || !utf8.ValidString(d.Reason) || !strings.HasPrefix(d.Reason, "xxx") {
			t.Fatalf("down reason: %d bytes", len(d.Reason))
		}

		// An Exit's reason, which the Down of its target carries on.
		target := spawnEcho(t, b)
		ref := w.Monitor(target.PID())
		if err := a.Exit(t.Context(), target.PID(), long); err != nil {
			t.Fatal(err)
		}
		if d := recv(t, seen).Down; d == nil || d.Ref != ref || !utf8.ValidString(d.Reason) {
			t.Fatalf("exit reason: %+v", d)
		}

		// A name that cannot travel reaches no process, not one whose name
		// it would be, cleaned up: no process can hold it.
		if _, err := a.Spawn(echo, grpcproc.WithName("\xff")); err == nil {
			t.Fatal("spawned under a name that is not UTF-8")
		}
		if _, err := a.Spawn(echo, grpcproc.WithName(long[:20<<10])); err == nil {
			t.Fatal("spawned under a 20 KiB name")
		}
		spawnEcho(t, a, grpcproc.WithName("\uFFFD"))
		spawnEcho(t, a, grpcproc.WithName(long[:16<<10]))
		for _, name := range []string{"\xff", long[:16<<10] + "y"} {
			w.Monitor(grpcproc.Name{Node: "a", Name: name})
			if d := recv(t, seen).Down; d == nil || d.Reason != grpcproc.ReasonNoProc {
				t.Fatalf("monitor of %d bytes: %+v", len(name), d)
			}
			if _, err := grpcproc.Named[*testpb.Ping]("a", name).Call[*testpb.Pong](ctx(t), b, &testpb.Ping{N: 1}); !errors.Is(err, grpcproc.ErrNoProc) {
				t.Fatalf("call of %d bytes: %v", len(name), err)
			}
		}

		synctest.Wait()
		noMore(t, downs)
	})
}

// A smaller MaxMessageSize is what the sender holds itself to.
func TestMaxMessageSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(_ string, cfg *grpcproc.Config) {
			cfg.MaxMessageSize = 64 << 10
		})}, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		sink, got := collector(t, b)
		if err := sink.Send(t.Context(), a, wrapperspb.Bytes(make([]byte, 100<<10))); !errors.Is(err, grpcproc.ErrTooLarge) {
			t.Fatalf("100 KiB: %v", err)
		}
		for range 4 {
			if err := sink.Send(t.Context(), a, wrapperspb.Bytes(make([]byte, 30<<10))); err != nil {
				t.Fatalf("30 KiB: %v", err)
			}
		}
		for range 4 {
			recv(t, got)
		}
	})
}
