package guide

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	conversationsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/conversations/v1"
	"github.com/floatdrop/grpcproc/pubsub"
)

// The local deployment: every service in one process, on one node. What is
// said, and what whoever follows the conversation is sent, is the chat the
// guide shows.
func TestLocal(t *testing.T) {
	app := compose(platform.Config{Node: "local"}, local, fast)
	start(t, app)
	n := app.Get[*grpcproc.Node]()

	events := follow(t, n, "local", "1")
	chat := ""
	for _, text := range []string{
		"hello there",
		"what is 6 * 7?",
		"and 7 / 0?", // the tool crashes: it is the tool's process that ends
	} {
		if code, body := say(t, app, "1", text); code != http.StatusAccepted {
			t.Fatalf("%d %s", code, body)
		}
		chat += transcript(t, events, 1)
	}
	golden(t, "chat.txt", chat)

	settled(t, n)
	golden(t, "local.txt", tree(n))
}

// What the front refuses, and what it does with a browser that is still
// following when the server shuts down.
func TestTheFront(t *testing.T) {
	app := compose(platform.Config{Node: "local"}, local, fast)
	start(t, app)
	// An id is part of a process's name: one that reads as another
	// conversation's topic names no conversation.
	if code, body := say(t, app, "1%2Fevents", "hello"); code != http.StatusNotFound {
		t.Fatalf("%d %s", code, body)
	}
	if code, body := say(t, app, "1", "\xff"); code != http.StatusBadRequest {
		t.Fatalf("%d %s", code, body)
	}

	srv := app.Get[*http.Server]()
	n := app.Get[*grpcproc.Node]()
	gone := make(chan struct{})
	rec := &stream{}
	go func() {
		defer close(gone)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/conversations/1/events", nil)
		srv.Handler.ServeHTTP(rec, req)
	}()
	following := func() bool {
		return slices.ContainsFunc(n.Processes(), func(p grpcproc.ProcessInfo) bool { return p.Label == "follower" })
	}
	for range 500 {
		if following() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A node that lost its caller may publish on, after another was asked:
	// the tokens of the earlier try are not for the browser.
	topic := pubsub.Named[*conversationsv1.Event]("local", conversationsv1.TopicName("1"))
	for _, e := range []*conversationsv1.Event{
		{Attempt: 2, Kind: &conversationsv1.Event_Token{Token: "new"}},
		{Attempt: 1, Kind: &conversationsv1.Event_Token{Token: "stale"}},
		{Attempt: 3, Kind: &conversationsv1.Event_Retrying{Retrying: "it starts over"}},
		{Attempt: 2, Kind: &conversationsv1.Event_Token{Token: "outdated"}},
		{Kind: &conversationsv1.Event_Done{Done: true}},
	} {
		if err := topic.Publish(t.Context(), n, e); err != nil {
			t.Fatal(err)
		}
	}
	for range 500 {
		if strings.Contains(rec.String(), "done") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sent := rec.String(); !strings.Contains(sent, "new") || strings.Contains(sent, "stale") || strings.Contains(sent, "outdated") || !strings.Contains(sent, "done") {
		t.Fatalf("the browser was sent %s", sent)
	}
	if err := srv.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gone:
	case <-time.After(5 * time.Second):
		t.Fatal("the browser's request outlived the server")
	}
}

// stream is a browser's end of a response that is read while it is written.
type stream struct {
	mu     sync.Mutex
	header http.Header
	body   strings.Builder
}

func (s *stream) Header() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.header == nil {
		s.header = http.Header{}
	}
	return s.header
}

func (s *stream) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body.Write(b)
}

func (*stream) WriteHeader(int) {}
func (*stream) Flush()          {}

func (s *stream) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body.String()
}
