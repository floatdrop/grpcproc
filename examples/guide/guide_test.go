package guide

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"golang.yandex/di"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/conversations"
	"github.com/floatdrop/grpcproc/examples/guide/internal/models"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	"github.com/floatdrop/grpcproc/examples/guide/internal/tools"
	"github.com/floatdrop/grpcproc/examples/guide/internal/web"
	conversationsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/conversations/v1"
)

var update = flag.Bool("update", false, "rewrite testdata/ from what the programs do")

// The compositions of the four entry points. A test cannot import package
// main, so **these are copies of cmd/*/main.go**: change a main and change
// it here, or the tests, and the output the guide shows, describe programs
// that are not the ones that run.
var (
	local   = []di.Module{models.Module, tools.Module, conversations.Module, web.Module}
	gateway = []di.Module{conversations.Module, web.Module}
	gpu     = []di.Module{models.Module}
	sandbox = []di.Module{tools.Module}
)

// compose is platform.Run without the program: the same composition, a
// silent logger, and an HTTP port the system picks. more adjusts it: a model
// that does not pause between tokens, say.
func compose(cfg platform.Config, services []di.Module, more ...di.Module) *di.Scope {
	cfg.Listen = cmp.Or(cfg.Listen, "127.0.0.1:0")
	cfg.HTTP = "127.0.0.1:0"
	app := di.New()
	platform.Compose(app, cfg, slog.New(slog.DiscardHandler), services...)
	app.Use(more...)
	return app
}

// fast has the script say its tokens with no wait between them.
func fast(s *di.Scope) { s.Value(models.Pace(0)).Override() }

func start(t *testing.T, app *di.Scope) {
	t.Helper()
	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Not t.Context(): it is cancelled before cleanups run.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := app.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
}

var mains = map[string][]di.Module{"local": local, "gateway": gateway, "gpu": gpu, "sandbox": sandbox}

func TestWiringValidates(t *testing.T) {
	for name, services := range mains {
		if err := compose(platform.Config{Node: name}, services).Validate().Err(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// The copies above are checked against the mains: each passes platform.Run
// the same modules, in the same order.
func TestCopiesMatchTheMains(t *testing.T) {
	for name, services := range mains {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("cmd", name, "main.go"), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var passed []string
		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && types.ExprString(call.Fun) == "platform.Run" {
				for _, arg := range call.Args {
					passed = append(passed, types.ExprString(arg))
				}
			}
			return true
		})
		var copied []string
		for _, m := range services {
			full := runtime.FuncForPC(reflect.ValueOf(m).Pointer()).Name() // .../internal/web.Module
			copied = append(copied, full[strings.LastIndex(full, "/")+1:])
		}
		if !slices.Equal(passed, copied) {
			t.Errorf("cmd/%s passes %v, the test composes %v", name, passed, copied)
		}
	}
}

// The module report the guide shows is this test's output.
func TestModulesMatchTheGuide(t *testing.T) {
	golden(t, "modules.txt", compose(platform.Config{Node: "local"}, local).Modules())
}

// say posts to the web front's handler, and returns the status and body.
func say(t *testing.T, app *di.Scope, id, text string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/conversations/"+id, strings.NewReader(text))
	app.Get[*http.Server]().Handler.ServeHTTP(rec, req)
	return rec.Code, strings.TrimSpace(rec.Body.String())
}

// follow subscribes a process of n to a conversation, as the web front does
// for a browser, and returns its events until the test ends.
func follow(t *testing.T, n *grpcproc.Node, where, id string) <-chan *conversationsv1.Event {
	t.Helper()
	events := make(chan *conversationsv1.Event, 256)
	ready := make(chan error, 1)
	follower, err := n.Spawn(func(p *grpcproc.Process[*conversationsv1.Event]) error {
		_, err := conversationsv1.Conversation(where, id).Follow(p.Context(), p)
		ready <- err
		for err == nil {
			var m grpcproc.Msg[*conversationsv1.Event]
			if m, err = p.Receive(); err == nil && m.Down == nil {
				events <- m.Body
			}
		}
		return err
	}, grpcproc.WithLabel("follower"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Exit(context.Background(), follower.PID(), grpcproc.ReasonNormal) })
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	return events
}

// transcript reads events until turns turns have ended, and writes them as
// a reader would see them: the tokens of an answer on one line, and a line
// for everything else.
func transcript(t *testing.T, events <-chan *conversationsv1.Event, turns int) string {
	t.Helper()
	var b strings.Builder
	line := false // in the middle of a line of tokens
	end := func() {
		if line {
			b.WriteString("\n")
		}
		line = false
	}
	for turns > 0 {
		select {
		case e := <-events:
			switch k := e.Kind.(type) {
			case *conversationsv1.Event_Said:
				end()
				fmt.Fprintf(&b, "> %s\n", k.Said)
			case *conversationsv1.Event_Token:
				b.WriteString(k.Token)
				line = true
				continue
			case *conversationsv1.Event_Tool:
				end()
				fmt.Fprintf(&b, "  [tool] %s %s\n", k.Tool.Name, k.Tool.Args)
			case *conversationsv1.Event_Ran:
				end()
				fmt.Fprintf(&b, "  [ran] %s%s\n", k.Ran.Output, failed(k.Ran.Failed))
			case *conversationsv1.Event_Retrying:
				end()
				fmt.Fprintf(&b, "  [starting over] %s\n", k.Retrying)
			case *conversationsv1.Event_Done:
				end()
				fmt.Fprintf(&b, "  [turn %d done]\n", e.Turn)
				turns--
			case *conversationsv1.Event_Failed:
				end()
				fmt.Fprintf(&b, "  [turn %d failed] %s\n", e.Turn, k.Failed)
				turns--
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("no more events; so far:\n%s", b.String())
		}
	}
	return b.String()
}

func failed(why string) string {
	if why == "" {
		return ""
	}
	return "failed: " + why
}

// settled waits until n runs no generation and no tool call: their
// processes end a moment after they answer.
func settled(t *testing.T, n *grpcproc.Node) {
	t.Helper()
	for range 500 {
		busy := slices.ContainsFunc(n.Processes(), func(p grpcproc.ProcessInfo) bool {
			return p.Label == "generation" || strings.HasPrefix(p.Label, "tool:")
		})
		if !busy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("a generation or a tool call is still running")
}

// tree draws what a node runs, from its processes' parents.
func tree(n *grpcproc.Node) string {
	children := map[grpcproc.PID][]grpcproc.ProcessInfo{}
	for _, p := range n.Processes() {
		children[p.Parent] = append(children[p.Parent], p)
	}
	var b strings.Builder
	b.WriteString(n.Name() + "\n")
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	var walk func(parent grpcproc.PID, indent string)
	walk = func(parent grpcproc.PID, indent string) {
		kids := children[parent]
		for i, p := range kids {
			branch, next := "├── ", "│   "
			if i == len(kids)-1 {
				branch, next = "└── ", "    "
			}
			_, _ = fmt.Fprintf(w, "%s%s%s\t%s\n", indent, branch, cmp.Or(p.Name, "·"), p.Label)
			walk(p.PID, indent+next)
		}
	}
	walk(grpcproc.PID{}, "")
	_ = w.Flush()
	return b.String()
}

// golden compares got with testdata/name, or rewrites it with -update.
func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("%s changed; run go test ./guide -update\n%s", path, got)
	}
}
