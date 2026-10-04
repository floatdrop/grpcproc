package saga_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/saga"
)

func ExampleSequence() {
	ctx := context.Background()
	node, err := grpcproc.NewNode(grpcproc.Config{Name: "shop", Resolver: grpcproc.StaticResolver{}, Admit: grpcproc.AdmitAll})
	if err != nil {
		panic(err)
	}
	if err := node.Start(ctx); err != nil {
		panic(err)
	}
	defer func() { _ = node.Stop(ctx) }()

	// A run's data is the card it is paid with.
	declined := func(card string) bool { return strings.HasPrefix(card, "4000") }
	type order = saga.Run[saga.Stage, *wrapperspb.StringValue]
	note := func(what string) saga.Effect[saga.Stage, *wrapperspb.StringValue] {
		return func(_ context.Context, r *order) error {
			fmt.Println(r.ID(), what)
			return nil
		}
	}
	charge := func(_ context.Context, r *order) error {
		if declined(r.Data.Value) {
			return saga.Permanent(errors.New("card declined"))
		}
		fmt.Println(r.ID(), "charge")
		return nil
	}
	refund := func(_ context.Context, r *order) error {
		// A charge that failed is undone too, since it may have happened.
		if declined(r.Data.Value) {
			fmt.Println(r.ID(), "refund: nothing was charged")
			return nil
		}
		fmt.Println(r.ID(), "refund")
		return nil
	}
	orders, err := saga.Sequence("order",
		saga.Step("reserve", note("reserve")).Undo(note("release")),
		saga.Step("charge", charge).Undo(refund).Pivot(),
		saga.Step("ship", note("ship")),
	)
	if err != nil {
		panic(err)
	}

	eng, err := saga.Start(node, saga.Config{Store: saga.Memory(), Poll: 10 * time.Millisecond}, orders)
	if err != nil {
		panic(err)
	}
	place(ctx, orders, eng, "order-1", "4242 4242")
	place(ctx, orders, eng, "order-2", "4000 0002")
	// Output:
	// order-1 reserve
	// order-1 charge
	// order-1 ship
	// order-1 ended in done
	// order-2 reserve
	// order-2 refund: nothing was charged
	// order-2 release
	// order-2 ended in failed: card declined
}

// place begins a run of orders paid with card and waits for its end.
func place(ctx context.Context, orders *saga.Definition[saga.Stage, *wrapperspb.StringValue], eng *saga.Engine, id, card string) {
	if _, err := orders.Begin(ctx, eng, id, wrapperspb.String(card)); err != nil {
		panic(err)
	}
	s, err := orders.Wait(ctx, eng, id)
	if err != nil {
		panic(err)
	}
	if s.Cause != "" {
		fmt.Printf("%s ended in %s: %s\n", id, s.State, s.Cause)
		return
	}
	fmt.Printf("%s ended in %s\n", id, s.State)
}
