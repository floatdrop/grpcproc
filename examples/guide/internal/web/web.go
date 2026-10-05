// Package web is the runtime's HTTP front: what a browser talks to. A
// handler is not a process: it opens a conversation and speaks to it through
// the node, as any code outside a process does, and the conversation may run
// on this node or another. To follow one, it starts a process for as long as
// the browser listens, since only a process subscribes to a topic.
package web

import (
	"context"
	_ "embed"
	"encoding/json/v2"
	"io"
	"log/slog"
	"net/http"
	"time"
	"unicode/utf8"

	"golang.yandex/di"
	"golang.yandex/di/dihttp"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	conversationsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/conversations/v1"
)

//go:embed chat.html
var page []byte

type front struct {
	node  *grpcproc.Node
	where string // the node that runs the conversations
	log   *slog.Logger
	// closing ends when the server shuts down: a browser that follows a
	// conversation would otherwise keep its request open, and the drain
	// waiting, for as long as it likes.
	closing context.Context
	close   context.CancelFunc
}

func newFront(n *grpcproc.Node, cfg platform.Config, log *slog.Logger) *front {
	closing, cancel := context.WithCancel(context.Background())
	return &front{node: n, where: cfg.Where(conversationsv1.Service), log: log, closing: closing, close: cancel}
}

// conversation addresses the conversation a request names, or answers 404
// for an id that cannot name one.
func (f *front) conversation(w http.ResponseWriter, r *http.Request) (conversationsv1.ConversationAddr, bool) {
	id := r.PathValue("id")
	if !conversationsv1.ValidID(id) {
		http.Error(w, "no such conversation", http.StatusNotFound)
		return conversationsv1.ConversationAddr{}, false
	}
	return conversationsv1.Conversation(f.where, id), true
}

// say is POST /conversations/{id}: the body is what the user says. It
// answers once the turn has begun; the answer itself goes to whoever follows
// the conversation.
func (f *front) say(w http.ResponseWriter, r *http.Request) {
	c, ok := f.conversation(w, r)
	if !ok {
		return
	}
	text, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
	if err != nil || len(text) == 0 || !utf8.Valid(text) {
		http.Error(w, "say something, as UTF-8, in at most 16 KiB", http.StatusBadRequest)
		return
	}
	said, err := c.Say(r.Context(), f.node, string(text))
	if err != nil {
		// What went wrong between the nodes is for the log, not the client.
		f.log.Error("say failed", "conversation", r.PathValue("id"), "err", err)
		http.Error(w, "the assistant cannot be reached right now", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.MarshalWrite(w, map[string]uint64{"turn": said.Turn})
}

// event is what a browser is sent for each of a conversation's events.
type event struct {
	Turn uint64 `json:"turn"`
	Kind string `json:"kind"`
	Text string `json:"text,omitempty"`
}

func describe(e *conversationsv1.Event) event {
	out := event{Turn: e.Turn}
	switch k := e.Kind.(type) {
	case *conversationsv1.Event_Said:
		out.Kind, out.Text = "said", k.Said
	case *conversationsv1.Event_Token:
		out.Kind, out.Text = "token", k.Token
	case *conversationsv1.Event_Tool:
		out.Kind, out.Text = "tool", k.Tool.Name+" "+k.Tool.Args
	case *conversationsv1.Event_Ran:
		out.Kind, out.Text = "ran", k.Ran.Output
		if k.Ran.Failed != "" {
			out.Kind, out.Text = "tool-failed", k.Ran.Failed
		}
	case *conversationsv1.Event_Retrying:
		out.Kind, out.Text = "retrying", k.Retrying
	case *conversationsv1.Event_Done:
		out.Kind = "done"
	case *conversationsv1.Event_Failed:
		out.Kind, out.Text = "failed", k.Failed
	}
	return out
}

// follow is GET /conversations/{id}/events: the conversation's events, as
// server-sent events, from the last the topic keeps on. It ends when the
// browser goes away, or the conversation's process does: the browser then
// asks again, which opens the conversation again.
func (f *front) follow(w http.ResponseWriter, r *http.Request) {
	c, ok := f.conversation(w, r)
	if !ok {
		return
	}
	// The handler's goroutine writes to the browser, and a process of its
	// own takes the events and hands them over: a subscriber is a process.
	events := make(chan *conversationsv1.Event)
	following := make(chan error, 1)
	// Until the browser goes away, or the server shuts down.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer context.AfterFunc(f.closing, cancel)()
	follower, err := f.node.Spawn(func(p *grpcproc.Process[*conversationsv1.Event]) error {
		defer close(events)
		sub, err := c.Follow(ctx, p)
		following <- err
		if err != nil {
			return nil // the handler says so
		}
		var attempt uint64 // the latest try at an answer it has heard of
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Down != nil && m.Down.Ref == sub.Ref {
				return nil // the topic ended, with its conversation
			}
			if m.Body == nil {
				continue
			}
			if _, token := m.Body.Kind.(*conversationsv1.Event_Token); token && m.Body.Attempt != 0 && m.Body.Attempt < attempt {
				continue // of a try that was given up
			}
			attempt = max(attempt, m.Body.Attempt)
			select {
			case events <- m.Body:
			case <-p.Context().Done():
			}
		}
	}, grpcproc.WithLabel("follower"))
	if err == nil {
		defer func() { _ = f.node.Exit(context.Background(), follower.PID(), grpcproc.ReasonNormal) }()
		err = <-following
	}
	if ctx.Err() != nil {
		return // nobody is there to tell
	}
	if err != nil {
		f.log.Error("follow failed", "conversation", r.PathValue("id"), "err", err)
		http.Error(w, "the assistant cannot be reached right now", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	out := http.NewResponseController(w)
	_ = out.Flush() // the browser's stream is open, whenever the first event comes
	for {
		select {
		case <-ctx.Done():
			return
		case e, open := <-events:
			if !open {
				return
			}
			// A browser that stopped reading is let go: the follower's
			// mailbox would otherwise keep every event meant for it.
			_ = out.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, _ = io.WriteString(w, "data: ")
			_ = json.MarshalWrite(w, describe(e))
			_, _ = io.WriteString(w, "\n\n")
			if out.Flush() != nil {
				return
			}
		}
	}
}

func newServer(cfg platform.Config, f *front) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /conversations/{id}", f.say)
	mux.HandleFunc("GET /conversations/{id}/events", f.follow)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(page)
	})
	srv := &http.Server{Addr: cfg.HTTP, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	srv.RegisterOnShutdown(f.close)
	return srv
}

// Module registers the front and its server, which listens as the program
// starts and serves once it has, and drains before anything stops, so what
// is said meanwhile still finds its conversation.
func Module(s *di.Scope) {
	s.Wire[*front](newFront)
	dihttp.Serve(s.Wire[*http.Server](newServer))
}
