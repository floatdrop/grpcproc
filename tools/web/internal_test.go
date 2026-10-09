package web

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/floatdrop/grpcproc/tools/client"
)

func TestHTTPStatus(t *testing.T) {
	for err, want := range map[error]int{
		badRequest{"bad"}:                          http.StatusBadRequest,
		errors.New("bad pid"):                      http.StatusBadRequest,
		status.Error(codes.NotFound, ""):           http.StatusNotFound,
		status.Error(codes.InvalidArgument, ""):    http.StatusBadRequest,
		status.Error(codes.FailedPrecondition, ""): http.StatusBadRequest,
		status.Error(codes.PermissionDenied, ""):   http.StatusForbidden,
		status.Error(codes.DeadlineExceeded, ""):   http.StatusGatewayTimeout,
		status.Error(codes.Unavailable, ""):        http.StatusBadGateway,
		status.Error(codes.Unknown, ""):            http.StatusConflict,
		status.Error(codes.Internal, ""):           http.StatusInternalServerError,
	} {
		if got := httpStatus(err); got != want {
			t.Errorf("%v: %d, want %d", err, got, want)
		}
	}
}

// Pages share a walk while it is recent, five times as long as it took and
// at least half a second, and wait for the one under way; the walk is
// theirs, not the request's that made it.
func TestWalksShared(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		walked, fail, panics, took := 0, false, false, 200*time.Millisecond
		w := &walks{walk: func(ctx context.Context) ([]client.NodeView, error) {
			walked++
			time.Sleep(took)
			if panics {
				panic("walk")
			}
			if err := ctx.Err(); err != nil {
				t.Errorf("the walk ended with its request: %v", err)
			}
			if fail {
				return nil, errors.New("down")
			}
			return []client.NodeView{{Name: "a"}}, nil
		}}
		get := func(ctx context.Context) (Nodes, error) {
			t.Helper()
			v, err := w.get(ctx)
			if err == nil && (len(v.Nodes) != 1 || v.TakenAt <= 0) {
				t.Fatalf("%+v", v)
			}
			return v, err
		}
		first, cancel := context.WithCancel(t.Context())
		results := make(chan error, 2)
		go func() { _, err := get(first); results <- err }()
		go func() { _, err := get(t.Context()); results <- err }()
		gone, leave := context.WithCancel(t.Context())
		go func() { _, err := get(gone); results <- err }()
		synctest.Wait()
		cancel()
		leave()
		for range 3 {
			if err := <-results; err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
		last, _ := get(t.Context())
		time.Sleep(900 * time.Millisecond) // within 5 × 200ms
		if v, _ := get(t.Context()); walked != 1 || v.TakenAt != last.TakenAt {
			t.Fatalf("walked %d times, %v then %v", walked, last.TakenAt, v.TakenAt)
		}
		time.Sleep(100 * time.Millisecond)
		fail = true
		if _, err := get(t.Context()); walked != 2 || err == nil {
			t.Fatalf("walked %d times: %v", walked, err)
		}
		// A failed walk is not shared, and one that panics fails its waiters.
		fail, panics = false, true
		waiter := make(chan error, 1)
		go func() {
			time.Sleep(100 * time.Millisecond)
			_, err := w.get(t.Context())
			waiter <- err
		}()
		func() {
			defer func() { _ = recover() }()
			_, _ = get(t.Context())
		}()
		if err := <-waiter; !errors.Is(err, errWalkPanicked) {
			t.Fatalf("waiting on a walk that panicked: %v", err)
		}
		panics = false
		if v, err := get(t.Context()); walked != 4 || err != nil || v.TakenAt <= last.TakenAt {
			t.Fatalf("walked %d times: %+v %v", walked, v, err)
		}
		// A slow walk is shared for 15 seconds at most.
		took = 10 * time.Second
		time.Sleep(time.Second)
		_, _ = get(t.Context())
		time.Sleep(15 * time.Second)
		if _, _ = get(t.Context()); walked != 6 {
			t.Fatalf("walked %d times", walked)
		}
	})
}
