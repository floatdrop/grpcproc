package platform_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.yandex/di"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
)

// A service that keeps crashing takes its supervisor past its restart
// limit, then the root past its own; the program stops, with the reason,
// rather than go on serving without its services.
func TestAProgramWhoseServicesGiveUpStops(t *testing.T) {
	crashing := func(s *di.Scope) {
		s.Value(actor.ChildSupervisor("crashing", actor.Spec{Children: []actor.ChildSpec{
			actor.ChildFunc("boom", func(*grpcproc.Process[proto.Message]) error { return errors.New("boom") }),
		}})).Group()
	}
	app := di.New()
	platform.Compose(app, platform.Config{Node: "local", Listen: "127.0.0.1:0"}, slog.New(slog.DiscardHandler), crashing)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err := app.Run(ctx, di.StopTimeout(5*time.Second))
	if err == nil || !strings.Contains(err.Error(), "root supervisor exited: max restarts") {
		t.Fatalf("got %v", err)
	}
}

func TestLoad(t *testing.T) {
	for _, tc := range []struct {
		env  map[string]string
		want string // an error, or where the models run
	}{
		{map[string]string{}, "local"},
		{map[string]string{"NODE": "gateway", "PEERS": " gpu-1 = 127.0.0.1:9102 ", "PLACEMENT": "models=gpu-1"}, "gpu-1"},
		{map[string]string{"NODE": "gateway", "PEERS": "gpu-1=:9102,gpu-2=:9103", "PLACEMENT": "models=gpu-1+gpu-2"}, "gpu-1 gpu-2"},
		{map[string]string{"PEERS": "gpu-1"}, `"gpu-1" is not name=value`},
		{map[string]string{"PEERS": "a=1,a=2"}, "names a twice"},
		{map[string]string{"PLACEMENT": "models=gpu-1"}, "PEERS does not name"},
		{map[string]string{"PEERS": "gpu-1=:9102", "PLACEMENT": "models=gpu-1+gpu-2"}, "puts models on gpu-2"},
	} {
		for _, key := range []string{"NODE", "PEERS", "PLACEMENT"} {
			t.Setenv(key, tc.env[key])
		}
		cfg, err := platform.Load()
		got := strings.Join(cfg.All("models"), " ")
		if err != nil {
			got = err.Error()
		}
		if !strings.Contains(got, tc.want) {
			t.Errorf("%v: got %q, want %q", tc.env, got, tc.want)
		}
	}
}

// A sandbox exports its runner, and a gateway does not trust the sandbox:
// what a peer is refused is, to it, a process that does not exist.
func TestWhatAPeerMayAsk(t *testing.T) {
	t.Setenv("EXPORT", "tools, ")
	t.Setenv("UNTRUSTED", "sandbox")
	cfg, err := platform.Load()
	if err != nil || !slices.Equal(cfg.Export, []string{"tools"}) || !slices.Equal(cfg.Untrusted, []string{"sandbox"}) {
		t.Fatalf("%+v %v", cfg, err)
	}
}
