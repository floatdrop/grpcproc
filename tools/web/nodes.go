package web

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/floatdrop/grpcproc/tools/client"
)

const (
	// A walk is shared for walkShare times as long as it took, at least
	// minWalkAge and at most maxWalkAge.
	walkShare  = 5
	minWalkAge = 500 * time.Millisecond
	maxWalkAge = 15 * time.Second
)

// maxKey bounds a grouping key; maxGroupings, how many groupings' walks
// are kept.
const (
	maxKey       = 256
	maxGroupings = 8
)

// groupings keeps a shared walk for each key the links are grouped by.
type groupings struct {
	walk func(ctx context.Context, key string) ([]client.NodeView, error)

	mu sync.Mutex
	by map[string]*walks
}

func (g *groupings) get(ctx context.Context, key string) (Nodes, error) {
	g.mu.Lock()
	if key == "" {
		// Grouped or not, a walk lists the same nodes, and their totals add
		// up the same: the newest recent one serves.
		var newest Nodes
		var at time.Time
		for _, w := range g.by {
			if v, taken, ok := w.recent(); ok && taken.After(at) {
				newest, at = v, taken
			}
		}
		if !at.IsZero() {
			g.mu.Unlock()
			return newest, nil
		}
	}
	w := g.by[key]
	if w == nil {
		if g.by == nil {
			g.by = map[string]*walks{}
		}
		if len(g.by) >= maxGroupings {
			evicted := false
			for k, x := range g.by {
				if k != "" && x.idle() { // not every page's node list, nor a walk going
					delete(g.by, k)
					evicted = true
					break
				}
			}
			if !evicted {
				g.mu.Unlock()
				return Nodes{}, status.Errorf(codes.Unavailable, "%d groupings are being walked: try again", len(g.by))
			}
		}
		w = &walks{walk: func(ctx context.Context) ([]client.NodeView, error) { return g.walk(ctx, key) }}
		g.by[key] = w
	}
	g.mu.Unlock()
	return w.get(ctx)
}

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

// recent is the last walk, and when it ended, while it is recent enough to
// share.
func (w *walks) recent() (Nodes, time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last, w.taken, w.fresh()
}

// idle says whether no walk is under way.
func (w *walks) idle() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.done == nil
}

// fresh says whether the last walk is recent enough to share. w.mu held.
func (w *walks) fresh() bool {
	return w.err == nil && !w.taken.IsZero() && time.Since(w.taken) < min(max(minWalkAge, walkShare*w.took), maxWalkAge)
}

func (w *walks) get(ctx context.Context) (Nodes, error) {
	w.mu.Lock()
	if w.fresh() {
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
