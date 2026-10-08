package grpcproc_test

import (
	"context"
	"log/slog"
	"maps"
	"regexp"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// goroutines reads a goroutine profile as records of how many goroutines
// share a stack and labels, the labels as the profile prints them.
func goroutines(t *testing.T) []goroutineRecord {
	t.Helper()
	var b strings.Builder
	if err := pprof.Lookup("goroutine").WriteTo(&b, 1); err != nil {
		t.Fatal(err)
	}
	head := regexp.MustCompile(`^(\d+) @`)
	var out []goroutineRecord
	for block := range strings.SplitSeq(b.String(), "\n\n") {
		m := head.FindStringSubmatch(block)
		if m == nil {
			continue
		}
		r := goroutineRecord{stack: block}
		r.count, _ = strconv.Atoi(m[1])
		for line := range strings.SplitSeq(block, "\n") {
			if labels, ok := strings.CutPrefix(line, "# labels: "); ok {
				r.labels = labels
			}
		}
		out = append(out, r)
	}
	return out
}

type goroutineRecord struct {
	count         int
	labels, stack string
}

// A process's goroutine and the goroutines it starts carry its pprof labels,
// which pprof.Do over its Context adds to; the link its first send to a peer
// dials carries none.
func TestProfileLabels(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	a := c.Node("a")
	sink, err := c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			if _, err := p.Receive(); err != nil {
				return err
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	defer close(release)
	ready := make(chan map[string]string, 1)
	addr, err := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		if err := sink.Send(p.Context(), p, &testpb.Ping{}); err != nil {
			return err
		}
		started := make(chan struct{})
		go func() {
			close(started)
			<-release
		}()
		<-started
		seen := map[string]string{}
		pprof.Do(p.Context(), pprof.Labels("handler", "ping"), func(ctx context.Context) {
			for _, k := range []string{grpcproc.ProfileLabel, grpcproc.ProfilePID, grpcproc.ProfileName, "handler"} {
				seen[k], _ = pprof.Label(ctx, k)
			}
		})
		ready <- seen
		<-release
		return nil
	}, grpcproc.WithName("labelled"), grpcproc.WithLabel("worker"))
	if err != nil {
		t.Fatal(err)
	}
	pid := addr.PID().String()
	want := map[string]string{
		grpcproc.ProfileLabel: "worker", grpcproc.ProfilePID: pid, grpcproc.ProfileName: "labelled", "handler": "ping",
	}
	if seen := <-ready; !maps.Equal(seen, want) {
		t.Fatalf("labels under pprof.Do: %v, want %v", seen, want)
	}

	// The process, and the goroutine it started.
	own, started := false, false
	for _, r := range goroutines(t) {
		if !strings.Contains(r.stack, "TestProfileLabels.func") {
			continue
		}
		mine := strings.Contains(r.labels, `"`+grpcproc.ProfilePID+`":"`+pid+`"`) &&
			strings.Contains(r.labels, `"`+grpcproc.ProfileName+`":"labelled"`) &&
			strings.Contains(r.labels, `"`+grpcproc.ProfileLabel+`":"worker"`)
		if strings.Contains(r.stack, "grpcproc.(*proc).run") {
			own = own || mine
		} else {
			started = started || mine
		}
	}
	if !own || !started {
		t.Fatalf("labelled: the process %v, the goroutine it started %v", own, started)
	}
	// The link the send dialed: its writer and the goroutine that reads it.
	var writer, reader []goroutineRecord
	eventually(t, "the link's goroutines in a profile", func() bool {
		writer, reader = nil, nil
		for _, r := range goroutines(t) {
			switch {
			case strings.Contains(r.stack, "grpcproc.(*outLink).writeLoop"):
				writer = append(writer, r)
			case strings.Contains(r.stack, "grpcproc.(*outLink).start.func"):
				reader = append(reader, r)
			}
		}
		return writer != nil && reader != nil
	})
	for _, r := range append(writer, reader...) {
		if strings.Contains(r.labels, "grpcproc.") {
			t.Errorf("a link's goroutine has labels %s:\n%s", r.labels, r.stack)
		}
	}
}

// unlabelledWhere fails t unless the goroutines whose stacks hold frame
// carry no grpcproc labels, and there is one.
func unlabelledWhere(t *testing.T, frame string) {
	t.Helper()
	found := false
	for _, r := range goroutines(t) {
		if strings.Contains(r.stack, frame) {
			found = true
			if strings.Contains(r.labels, "grpcproc.") {
				t.Errorf("labels %s:\n%s", r.labels, r.stack)
			}
		}
	}
	if !found {
		t.Fatalf("no goroutine in %s", frame)
	}
}

// The release of a name, which the process that held it starts as it exits,
// carries none of its labels.
func TestProfileLabelsOfARelease(t *testing.T) {
	f := newFakeNames()
	releasing, release := make(chan struct{}), make(chan struct{})
	f.setRelease(func(context.Context) error {
		close(releasing)
		<-release
		return nil
	})
	n := fakeNode(t, f)
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		_ = n.Stop(context.Background())
	})
	if _, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		_, err := p.Claim(p.Context(), "held")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	<-releasing
	unlabelledWhere(t, "grpcproc_test.fakeClaim.Release")
}

// sendHeld holds the sends of a Ping numbered 42 until release is closed.
type sendHeld struct {
	grpcproc.NopHooks
	sending, release chan struct{}
}

func (h *sendHeld) OnSend(s grpcproc.SendInfo, md grpcproc.Metadata) (grpcproc.Metadata, grpcproc.Done) {
	if ping, ok := s.Body.(*testpb.Ping); ok && ping.N == 42 {
		close(h.sending)
		<-h.release
	}
	return md, nil
}

// A SendAfter timer, which fires on a goroutine of the runtime's, sends under
// its process's labels.
func TestProfileLabelsOfATimer(t *testing.T) {
	h := &sendHeld{sending: make(chan struct{}), release: make(chan struct{})}
	n, err := grpcproc.NewNode(grpcproc.Config{Admit: grpcproc.AdmitAll, Name: "a", Resolver: grpcproc.StaticResolver{}, Hooks: h, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(h.release)
		_ = n.Stop(context.Background())
	})
	sink, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		_, err := p.Receive()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	timer, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		p.SendAfter(time.Millisecond, sink, &testpb.Ping{N: 42})
		_, err := p.Receive()
		return err
	}, grpcproc.WithLabel("timer"))
	if err != nil {
		t.Fatal(err)
	}
	<-h.sending
	found := false
	for _, r := range goroutines(t) {
		if strings.Contains(r.stack, "(*sendHeld).OnSend") {
			found = true
			if !strings.Contains(r.labels, `"`+grpcproc.ProfilePID+`":"`+timer.PID().String()+`"`) {
				t.Errorf("the timer's send has labels %s", r.labels)
			}
		}
	}
	if !found {
		t.Fatal("no goroutine in the timer's send")
	}
}
