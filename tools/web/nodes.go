package web

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/floatdrop/grpcproc/tools/client"
)

const (
	// A walk is shared for walkShare times as long as it took, at least
	// minWalkAge and at most maxWalkAge.
	walkShare  = 5
	minWalkAge = 500 * time.Millisecond
	maxWalkAge = 15 * time.Second
)

var errWalkPanicked = errors.New("the walk over the cluster panicked")

// Nodes is what /api/nodes answers.
type Nodes struct {
	Nodes   []client.NodeView `json:"nodes"`
	TakenAt int64             `json:"taken_at"` // when the walk ended, in Unix milliseconds
}

// walks shares walks over the cluster between the pages that poll it, so
// that a large cluster is walked at a pace its size sets: a request takes
// the last walk while it is recent, and otherwise waits for a new one,
// which the first of them makes.
type walks struct {
	walk func(context.Context) ([]client.NodeView, error)

	mu    sync.Mutex
	last  Nodes
	taken time.Time // when last ended
	took  time.Duration
	err   error
	done  chan struct{} // closed when the walk under way ends; nil when none is
}

func (w *walks) get(ctx context.Context) (Nodes, error) {
	w.mu.Lock()
	if w.err == nil && !w.taken.IsZero() && time.Since(w.taken) < min(max(minWalkAge, walkShare*w.took), maxWalkAge) {
		defer w.mu.Unlock()
		return w.last, nil
	}
	if done := w.done; done != nil {
		w.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return Nodes{}, ctx.Err()
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.last, w.err
	}
	done := make(chan struct{})
	w.done = done
	w.mu.Unlock()
	// Ended however the walk ends, a panic on this goroutine included, or
	// the next request waits for it for ever.
	ended := false
	defer func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if !ended {
			w.err = errWalkPanicked
		}
		w.done = nil
		close(done)
	}()
	// The walk is every waiting page's: it outlives this request.
	start := time.Now()
	nodes, err := w.walk(context.WithoutCancel(ctx))
	w.mu.Lock()
	defer w.mu.Unlock()
	ended = true
	if w.err = err; err != nil {
		return Nodes{}, err
	}
	w.taken, w.took = time.Now(), time.Since(start)
	w.last = Nodes{Nodes: nodes, TakenAt: w.taken.UnixMilli()}
	return w.last, nil
}
