package grpcprocotel

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/leader"
)

// The inspect map's keys, the label and the PID's form are leader's, not
// its API: TestObserveLeader, against the core this module requires, is what
// notices a change to them.
const (
	// inspectTimeout bounds how long a collection waits for the electors.
	inspectTimeout = time.Second
	// electorLabel is the label grpcproc/leader gives its electors.
	electorLabel = "leader"
)

// errStalled ends the wait for electors that do not answer in time.
var errStalled = errors.New("grpcprocotel: the elector did not answer in time")

// ObserveLeader registers gauges read at each collection from the electors
// of grpcproc/leader on node, by grpcproc.leader.cluster: one set for each
// election the node takes part in when the collection runs. They come from
// what each elector publishes through its inspect map: its role (1, with
// grpcproc.leader.role), its term, the nodes of its view, the quorum, how many
// of the view it hears from, itself included, whether the singleton runs on
// this node, and how many Checkpoint calls wait for a majority. The electors
// are asked at once. One that the node's snapshot shows on one message for a
// second, busy in a slow Store say, or that does not answer within a second
// of a collection that lasts that long, is reported as stalled, with no other
// gauge. Unregister the returned registration when the node stops.
func (h *Hooks) ObserveLeader(node *grpcproc.Node) (metric.Registration, error) {
	var errs []error
	electors := h.gauge(&errs, "grpcproc.leader.electors", "1 for this node's elector, by cluster and role.", "{elector}")
	stalled := h.gauge(&errs, "grpcproc.leader.stalled", "1 while the elector is on one message for a second or more, or does not answer within one, by cluster.", "{elector}")
	term := h.gauge(&errs, "grpcproc.leader.term", "The elector's term, by cluster.", "{term}")
	view := h.gauge(&errs, "grpcproc.leader.view", "Nodes whose majority elects, by cluster.", "{node}")
	quorum := h.gauge(&errs, "grpcproc.leader.quorum", "Nodes that make a majority of the view, by cluster.", "{node}")
	reachable := h.gauge(&errs, "grpcproc.leader.reachable", "Nodes of the view the elector hears from, itself included, by cluster.", "{node}")
	singleton := h.gauge(&errs, "grpcproc.leader.singleton", "1 while the singleton runs on this node, by cluster.", "{process}")
	waiting := h.gauge(&errs, "grpcproc.leader.checkpoints.waiting", "Checkpoint calls waiting for a majority to hold their state, by cluster.", "{checkpoint}")
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	type answer struct {
		cluster string
		stalled bool
		m       map[string]string
		err     error
	}
	prefix := leader.ElectorName("")
	return h.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ictx, cancel := context.WithTimeoutCause(ctx, inspectTimeout, errStalled)
		defer cancel()
		var answers []*answer
		var wg sync.WaitGroup
		for _, p := range node.Processes() {
			cluster, ok := strings.CutPrefix(p.Name, prefix)
			if !ok || p.Label != electorLabel || p.State == grpcproc.StateExiting {
				continue
			}
			a := &answer{cluster: cluster, stalled: p.BusyFor >= inspectTimeout}
			answers = append(answers, a)
			if !a.stalled {
				wg.Go(func() {
					a.m, a.err = node.Inspect(ictx, p.PID)
					a.stalled = a.err != nil && errors.Is(context.Cause(ictx), errStalled) // not ended by the collection
				})
			}
		}
		wg.Wait()
		for _, a := range answers {
			attrs := metric.WithAttributes(AttrCluster.String(a.cluster))
			switch {
			case a.stalled:
				o.ObserveInt64(stalled, 1, attrs)
				continue
			case a.err != nil:
				continue // exited since the snapshot, or the collection ended first
			case a.m["role"] == "":
				continue // a process of that name and label that is no elector
			}
			o.ObserveInt64(electors, 1, metric.WithAttributes(AttrCluster.String(a.cluster), AttrRole.String(a.m["role"])))
			o.ObserveInt64(stalled, 0, attrs)
			o.ObserveInt64(term, number(a.m["term"]), attrs)
			o.ObserveInt64(view, count(a.m["view"]), attrs)
			o.ObserveInt64(quorum, number(a.m["quorum"]), attrs)
			o.ObserveInt64(reachable, count(a.m["view"])-count(a.m["unreachable"]), attrs)
			running := int64(0)
			if strings.HasPrefix(a.m["singleton"], "<") { // its PID; a phase otherwise
				running = 1
			}
			o.ObserveInt64(singleton, running, attrs)
			o.ObserveInt64(waiting, number(a.m["checkpoints_waiting"]), attrs)
		}
		return nil
	}, electors, stalled, term, view, quorum, reachable, singleton, waiting)
}

// number is an inspect value as a number, 0 if absent.
func number(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// count is how many nodes an inspect list names.
func count(list string) int64 {
	if list == "" {
		return 0
	}
	return int64(strings.Count(list, ",") + 1)
}
