package groups

import (
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	groupsv1 "github.com/floatdrop/grpcproc/groups/proto/grpcproc/groups/v1"
	"github.com/floatdrop/grpcproc/grpcproctest"
)

// A relay that cannot send what its scope gives it ends with a Lost of the
// scope it reached: the other side is missing what it could not send.
func TestRelayThatCannotSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a := c.Node("a")
		sb, err := Start(c.Node("b"), Config{})
		if err != nil {
			t.Fatal(err)
		}
		got := make(chan proto.Message, 4)
		scope, err := a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			r, err := p.Spawn(relay("b", ScopeName(DefaultScope), p.PID()))
			if err != nil {
				return err
			}
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				got <- m.Body
				if _, ok := m.Body.(*groupsv1.Found); ok {
					// Not valid UTF-8: it does not encode.
					_ = p.SendTo(r, &groupsv1.Update{Joined: []*groupsv1.Members{{Group: "\xff"}}})
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if f, ok := (<-got).(*groupsv1.Found); !ok || grpcproc.PIDFromProto(f.GetScope()) != sb.PID() {
			t.Fatalf("first %v", f)
		}
		l, ok := (<-got).(*groupsv1.Lost)
		if !ok || grpcproc.PIDFromProto(l.GetScope()) != sb.PID() || l.GetReason() != grpcproc.ReasonNoConnection {
			t.Fatalf("then %v", l)
		}
		_ = a.Exit(t.Context(), scope.PID(), "done")
	})
}
