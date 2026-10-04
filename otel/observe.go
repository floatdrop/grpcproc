package grpcprocotel

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/floatdrop/grpcproc"
)

// Observe registers gauges read from node's snapshots at each collection:
// processes, mailbox depth and oldest message age per label, and counters of
// traffic per peer and direction. Those add up what each link to the peer
// had carried at the collections that saw it, so they never go back when a
// link breaks; what a link carries after the last collection before it
// breaks is not counted. A peer this node has neither a link to nor backs
// off from is forgotten, and counts from 0 if it comes back. Unregister the
// returned registration when the node stops.
func (h *Hooks) Observe(node *grpcproc.Node) (metric.Registration, error) {
	var errs []error
	gauge := func(name, desc, unit string) metric.Int64ObservableGauge {
		g, err := h.meter.Int64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
		errs = append(errs, err)
		return g
	}
	processes := gauge("grpcproc.processes", "Live processes, by label.", "{process}")
	depth := gauge("grpcproc.mailbox.depth", "Messages waiting in mailboxes, summed by label.", "{message}")
	oldest, err := h.meter.Float64ObservableGauge("grpcproc.mailbox.oldest",
		metric.WithDescription("Age of the oldest waiting message, the maximum by label."), metric.WithUnit("s"))
	errs = append(errs, err)
	linkMessages, err := h.meter.Int64ObservableCounter("grpcproc.link.messages",
		metric.WithDescription("Envelopes over links, by peer and direction."), metric.WithUnit("{message}"))
	errs = append(errs, err)
	linkBytes, err := h.meter.Int64ObservableCounter("grpcproc.link.bytes",
		metric.WithDescription("Bytes over links, by peer and direction."), metric.WithUnit("By"))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	type agg struct {
		count, depth int64
		oldest       float64
	}
	// A link's counts start at 0 and go when it does; the counters add up
	// what each link carried since the last collection, by peer and
	// direction, so they never go back.
	type way struct{ peer, dir string }
	type traffic struct {
		link              time.Time // the link counted last, by when it came up
		messages, bytes   uint64    // its counts then
		sumMsgs, sumBytes uint64
	}
	var mu sync.Mutex
	totals := map[way]*traffic{}
	return h.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		byLabel := map[string]*agg{}
		for _, p := range node.Processes() {
			a := byLabel[p.Label]
			if a == nil {
				a = &agg{}
				byLabel[p.Label] = a
			}
			a.count++
			a.depth += int64(p.Mailbox.Depth)
			a.oldest = max(a.oldest, p.Mailbox.OldestAge.Seconds())
		}
		for label, a := range byLabel {
			attrs := metric.WithAttributes(AttrLabel.String(label))
			o.ObserveInt64(processes, a.count, attrs)
			o.ObserveInt64(depth, a.depth, attrs)
			o.ObserveFloat64(oldest, a.oldest, attrs)
		}
		mu.Lock()
		defer mu.Unlock()
		links := node.Info().Links
		for w := range totals {
			if !slices.ContainsFunc(links, func(l grpcproc.LinkInfo) bool { return l.Peer.Name == w.peer }) {
				delete(totals, w)
			}
		}
		for _, l := range links {
			if l.State != grpcproc.LinkUp {
				continue // a peer backed off from: no link, no traffic
			}
			w := way{l.Peer.Name, "in"}
			if l.Outbound {
				w.dir = "out"
			}
			t := totals[w]
			if t == nil {
				t = &traffic{}
				totals[w] = t
			}
			if !t.link.Equal(l.EstablishedAt) || l.Messages < t.messages || l.Bytes < t.bytes {
				t.link, t.messages, t.bytes = l.EstablishedAt, 0, 0
			}
			t.sumMsgs += l.Messages - t.messages
			t.sumBytes += l.Bytes - t.bytes
			t.messages, t.bytes = l.Messages, l.Bytes
		}
		for w, t := range totals {
			attrs := metric.WithAttributes(AttrPeer.String(w.peer), AttrDirection.String(w.dir))
			o.ObserveInt64(linkMessages, int64(t.sumMsgs), attrs)
			o.ObserveInt64(linkBytes, int64(t.sumBytes), attrs)
		}
		return nil
	}, processes, depth, oldest, linkMessages, linkBytes)
}
