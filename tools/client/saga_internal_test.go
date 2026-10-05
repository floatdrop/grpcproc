package client

import (
	"context"
	"encoding/json/v2"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/protobuf/types/known/anypb"

	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
	sagav1 "github.com/floatdrop/grpcproc/saga/proto/grpcproc/saga/v1"
)

// sagaFake is node a with one saga engine, which cannot be inspected.
type sagaFake struct{ inspectv1.InspectorClient }

func (sagaFake) GetNode(context.Context, *inspectv1.GetNodeRequest, ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	return &inspectv1.GetNodeResponse{Node: &inspectv1.NodeInfo{Id: &inspectv1.NodeID{Name: "a"}}}, nil
}

func (sagaFake) ListProcesses(context.Context, *inspectv1.ListProcessesRequest, ...grpc.CallOption) (*inspectv1.ListProcessesResponse, error) {
	return &inspectv1.ListProcessesResponse{Processes: []*inspectv1.ProcessInfo{
		{Pid: &grpcprocv1.PID{Node: "a", Incarnation: 1, Id: 7}, Label: sagaLabel},
	}}, nil
}

// GetProcess answers that the engine is too busy to say what it runs.
func (sagaFake) GetProcess(context.Context, *inspectv1.GetProcessRequest, ...grpc.CallOption) (*inspectv1.GetProcessResponse, error) {
	return &inspectv1.GetProcessResponse{
		Process:      &inspectv1.ProcessInfo{Pid: &grpcprocv1.PID{Node: "a", Incarnation: 1, Id: 7}, Label: sagaLabel},
		InspectError: "busy for 3s",
	}, nil
}

// goneFake is sagaFake whose engine is gone by the time it is asked.
type goneFake struct{ sagaFake }

func (goneFake) GetProcess(context.Context, *inspectv1.GetProcessRequest, ...grpc.CallOption) (*inspectv1.GetProcessResponse, error) {
	return nil, status.Error(codes.NotFound, "gone")
}

func (sagaFake) Query(context.Context, *inspectv1.QueryRequest, ...grpc.CallOption) (*inspectv1.QueryResponse, error) {
	body, _ := anypb.New(&sagav1.Runs{Runs: []*sagav1.Run{{Saga: "orders", Id: "1"}}})
	return &inspectv1.QueryResponse{Body: body}, nil
}

// An engine too busy to say what it runs is listed with why, and asked all
// the same: a query does not wait for it.
func TestASagaEngineThatCannotBeInspected(t *testing.T) {
	c := &Client{rpc: sagaFake{}, now: time.Now}
	engines, err := c.SagaEngines(t.Context(), "")
	if err != nil || len(engines) != 1 || engines[0].PID != "<a.1.7>" || engines[0].Error == "" {
		t.Fatalf("%+v %v", engines, err)
	}
	if runs, _, err := c.SagaRuns(t.Context(), "", SagaQuery{Saga: "orders"}); err != nil || len(runs) != 1 {
		t.Errorf("asked an engine that cannot be inspected: %+v %v", runs, err)
	}
	if _, err := (&Client{rpc: goneFake{}, now: time.Now}).SagaRun(t.Context(), "", "orders", "1"); err == nil || err.Error() != `no saga engine runs saga "orders"` {
		t.Errorf("an engine gone since it was listed was asked: %v", err)
	}
	if engines, err := (&Client{rpc: goneFake{}, now: time.Now}).SagaEngines(t.Context(), ""); err != nil || len(engines) != 1 || engines[0].Error == "" {
		t.Errorf("an engine gone since it was listed: %+v %v", engines, err)
	}
	gone := &Client{rpc: unreachable{}, now: time.Now}
	if _, err := gone.SagaEngines(t.Context(), ""); err == nil {
		t.Error("engines of a cluster that cannot be reached")
	}
	if _, _, err := gone.SagaRuns(t.Context(), "", SagaQuery{}); err == nil {
		t.Error("runs of a cluster that cannot be reached")
	}
}

func TestDecoded(t *testing.T) {
	if decoded("") != nil || decoded(`{"a":1}`).(map[string]any)["a"] != 1.0 || decoded("{bad") != "{bad" {
		t.Error("decoded")
	}
	if b, err := json.Marshal(SagaRunView{Data: decoded("null")}); err != nil || !strings.Contains(string(b), `"data":null`) {
		t.Errorf("null data: %s %v", b, err)
	}
}
