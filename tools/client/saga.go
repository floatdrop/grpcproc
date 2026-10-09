package client

import (
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
	sagav1 "github.com/floatdrop/grpcproc/saga/proto/grpcproc/saga/v1"
)

// sagaLabel is the label of a grpcproc/saga engine's process: how one is
// told from the others, whatever its name.
const sagaLabel = "saga engine"

// SagaEngineView is a grpcproc/saga engine and the sagas it runs.
type SagaEngineView struct {
	Node string `json:"node"`
	// Process is the engine's registered name, or its pid.
	Process string   `json:"process"`
	PID     string   `json:"pid"`
	Sagas   []string `json:"sagas" jsonschema:"the sagas it runs, each as name vN, its version"`
	Working int      `json:"working" jsonschema:"runs it works on now"`
	Error   string   `json:"error,omitempty" jsonschema:"why it could not be read: it is busy, or gone, or its node could not be asked"`
}

// SagaRunView is a run of a saga, as its store keeps it.
type SagaRunView struct {
	Saga    string `json:"saga"`
	ID      string `json:"id"`
	Version uint64 `json:"version,omitzero" jsonschema:"the highest version of the saga that began, signalled or worked on it"`
	State   string `json:"state" jsonschema:"the state of the saga's machine it is in"`
	Status  string `json:"status" jsonschema:"active: it goes on, at work or waiting; done: it ended; stuck: an effect failed for good with nothing to fire, and it waits for a resume"`
	// Data is the run's data as JSON, asked for one run of an engine that
	// shows data (saga.Config.InspectData); left out when it shows none, or
	// cannot read it.
	Data        any          `json:"data,omitzero"`
	DataOmitted uint64       `json:"data_omitted,omitzero" jsonschema:"the size of the data when it is too large to carry: data is then left out"`
	Waiting     uint32       `json:"waiting,omitzero" jsonschema:"how many signals wait, in a list, which carries the count and not the signals"`
	Visit       uint64       `json:"visit,omitzero" jsonschema:"how many states it has entered"`
	EffectDone  bool         `json:"effect_done,omitzero" jsonschema:"the state's effect is through for this visit, and the run waits"`
	Attempts    int64        `json:"attempts,omitzero" jsonschema:"how many times the effect failed in this visit"`
	Error       string       `json:"error,omitempty" jsonschema:"why the effect failed last, or why the run is stuck"`
	Cause       string       `json:"cause,omitempty" jsonschema:"the error an Otherwise last fired with: why the run left its way"`
	Wake        string       `json:"wake,omitempty" jsonschema:"when it is next due"`
	RetryAt     string       `json:"retry_at,omitempty" jsonschema:"when its failed effect is next tried"`
	Deadline    string       `json:"deadline,omitempty" jsonschema:"when the timer of its visit fires"`
	Owner       string       `json:"owner,omitempty" jsonschema:"the node of the engine working on it"`
	LeaseUntil  string       `json:"lease_until,omitempty"`
	Epoch       uint64       `json:"epoch,omitzero" jsonschema:"how many times it has been claimed"`
	Inbox       []SagaSignal `json:"inbox,omitempty" jsonschema:"signals waiting for a state that takes them"`
	Revision    uint64       `json:"revision"`
	Created     string       `json:"created,omitempty"`
	Updated     string       `json:"updated,omitempty"`
}

// SagaSignal is a signal waiting in a run's inbox.
type SagaSignal struct {
	Seq     uint64 `json:"seq"`
	Event   string `json:"event"`
	Payload any    `json:"payload,omitzero" jsonschema:"its payload as JSON, asked for one run of an engine that shows data and accepts the event; left out otherwise, and for a signal with none"`
	Omitted uint64 `json:"payload_omitted,omitzero" jsonschema:"the size of the payload when it is too large to carry: payload is then left out"`
	Version uint64 `json:"version,omitzero"`
}

// SagaQuery is which runs SagaRuns lists.
type SagaQuery struct {
	Saga   string   // one saga, or every one the engine runs
	Status []string // active, done, stuck; all of them when none
	Limit  int      // how many, 100 when 0, at most 1000
	// AfterSaga and AfterID name the last run of the previous page, for the
	// next; AfterSaga is Saga when empty.
	AfterSaga, AfterID string
}

// SagaEngines describes the saga engines of node, by PID, or when node is
// empty SagaEnginesOf the nodes Cluster finds.
func (c *Client) SagaEngines(ctx context.Context, node string) ([]SagaEngineView, error) {
	if node == "" {
		nodes, err := c.walkFor(ctx)
		if err != nil {
			return nil, err
		}
		return c.SagaEnginesOf(ctx, nodes, quarter(ctx)), nil
	}
	ps, err := c.Processes(ctx, node, Filter{Label: sagaLabel})
	if err != nil {
		return nil, err
	}
	out := []SagaEngineView{}
	for _, p := range ps {
		out = append(out, c.sagaEngine(ctx, node, p.PID))
	}
	return out, nil
}

// SagaEnginesOf describes the saga engines of nodes, in nodes' order and by
// PID on each, asking the nodes at once, each within timeout (0: ctx
// alone). A node that cannot be asked is listed with its Error.
func (c *Client) SagaEnginesOf(ctx context.Context, nodes []NodeView, timeout time.Duration) []SagaEngineView {
	found := each(ctx, nodes, timeout, func(ctx context.Context, n NodeView) []SagaEngineView {
		if !n.Reached() {
			return []SagaEngineView{{Node: n.Name, Sagas: []string{}, Error: n.Problem()}}
		}
		ps, err := c.Processes(ctx, n.Name, Filter{Label: sagaLabel})
		if err != nil {
			return []SagaEngineView{{Node: n.Name, Sagas: []string{}, Error: message(err)}}
		}
		var out []SagaEngineView
		for _, p := range ps {
			out = append(out, c.sagaEngine(ctx, n.Name, p.PID))
		}
		return out
	})
	out := []SagaEngineView{}
	for _, f := range found {
		out = append(out, f...)
	}
	return out
}

func (c *Client) sagaEngine(ctx context.Context, node, pid string) SagaEngineView {
	p, err := c.Process(ctx, node, pid, true, 0)
	if err != nil {
		return SagaEngineView{Node: node, Process: pid, PID: pid, Sagas: []string{}, Error: err.Error()}
	}
	v := SagaEngineView{Node: node, Process: cmp.Or(p.Name, p.PID), PID: p.PID, Sagas: []string{}, Error: p.InspectError}
	for k, version := range p.Inspect {
		if name, ok := strings.CutPrefix(k, "saga "); ok {
			v.Sagas = append(v.Sagas, name+" "+version)
		}
	}
	slices.Sort(v.Sagas)
	v.Working, _ = strconv.Atoi(p.Inspect["working"])
	return v
}

// engine is the saga engine to ask about saga: on node, or, when node is
// empty, the first of the cluster's that says it runs saga, since an engine
// answers for no other. One too busy to say what it runs is asked when none
// says so: a query does not wait for what keeps it busy. With no saga named,
// the first engine found is asked, for the sagas it runs.
func (c *Client) engine(ctx context.Context, node, saga string) (*inspectv1.Target, string, error) {
	nodes := []string{node}
	var failed error // the first node's that could not say: it may run saga
	if node == "" {
		all, err := c.walkFor(ctx)
		if err != nil {
			return nil, "", err
		}
		nodes = Reached(all)
		if i := slices.IndexFunc(all, func(n NodeView) bool { return !n.Reached() }); i >= 0 {
			failed = fmt.Errorf("%s: %s", all[i].Name, all[i].Problem())
		}
	}
	type listed struct {
		ps  []ProcessView
		err error
	}
	timeout := time.Duration(0) // one node, named: no walk to leave time for
	if node == "" {
		timeout = quarter(ctx)
	}
	lists := each(ctx, nodes, timeout, func(ctx context.Context, n string) listed {
		ps, err := c.Processes(ctx, n, Filter{Label: sagaLabel})
		return listed{ps, err}
	})
	var busy, busyNode string
	found := false
	for i, n := range nodes {
		switch {
		case lists[i].err != nil && node != "":
			return nil, "", lists[i].err
		case lists[i].err != nil: // one of the cluster's: the others may run it
			failed = cmp.Or(failed, lists[i].err)
			continue
		}
		for _, p := range lists[i].ps {
			found = true
			if saga == "" {
				t, _ := parseTarget(p.PID) // as the Inspector wrote it
				return t, n, nil
			}
			info, err := c.Process(ctx, n, p.PID, true, 0)
			switch {
			case err != nil: // gone since it was listed
			case info.Inspect["saga "+saga] != "":
				t, _ := parseTarget(p.PID)
				return t, n, nil
			case busy == "" && info.InspectError != "":
				busy, busyNode = p.PID, n
			}
		}
	}
	switch {
	case busy != "":
		t, _ := parseTarget(busy)
		return t, busyNode, nil
	case found && node != "":
		return nil, "", fmt.Errorf("no saga engine on node %s runs saga %q", node, saga)
	case failed != nil: // it may run there
		return nil, "", failed
	case found:
		return nil, "", fmt.Errorf("no saga engine runs saga %q", saga)
	case node == "":
		return nil, "", errors.New("no saga engine reachable from this node")
	}
	return nil, "", fmt.Errorf("no saga engine on node %s", node)
}

// SagaRuns lists the runs q asks for, from an engine found as engine finds
// one, by saga and then ID; more says whether there are more after the
// last. With no saga named, they are the runs of the sagas one engine runs.
func (c *Client) SagaRuns(ctx context.Context, node string, q SagaQuery) (runs []SagaRunView, more bool, err error) {
	list := &sagav1.ListRuns{Saga: q.Saga, Limit: uint32(min(max(q.Limit, 0), 1000))}
	for _, s := range q.Status {
		st, ok := sagav1.Status_value["STATUS_"+strings.ToUpper(s)]
		if !ok || st == 0 {
			return nil, false, fmt.Errorf("bad saga status %q: want active, done or stuck", s)
		}
		list.Status = append(list.Status, sagav1.Status(st))
	}
	if q.AfterID != "" {
		list.After = &sagav1.RunRef{Saga: cmp.Or(q.AfterSaga, q.Saga), Id: q.AfterID}
	}
	t, n, err := c.engine(ctx, node, q.Saga)
	if err != nil {
		return nil, false, err
	}
	answer := &sagav1.Runs{}
	if err := c.askSaga(ctx, n, t, &sagav1.Query{Op: &sagav1.Query_List{List: list}}, answer); err != nil {
		return nil, false, err
	}
	runs = []SagaRunView{}
	for _, r := range answer.GetRuns() {
		runs = append(runs, sagaRunView(r))
	}
	return runs, answer.GetMore(), nil
}

// SagaRun describes one run, with its data and its signals' payloads as
// JSON when the engine shows them (saga.Config.InspectData).
func (c *Client) SagaRun(ctx context.Context, node, saga, id string) (SagaRunView, error) {
	t, n, err := c.engine(ctx, node, saga)
	if err != nil {
		return SagaRunView{}, err
	}
	return c.sagaRun(ctx, n, t, saga, id)
}

func (c *Client) sagaRun(ctx context.Context, node string, t *inspectv1.Target, saga, id string) (SagaRunView, error) {
	r := &sagav1.Run{}
	if err := c.askSaga(ctx, node, t, &sagav1.Query{Op: &sagav1.Query_Get{Get: &sagav1.RunRef{Saga: saga, Id: id}}}, r); err != nil {
		return SagaRunView{}, err
	}
	return sagaRunView(r), nil
}

// ResumeSaga makes a stuck run active again, due at once, its attempts
// counted from none, and returns it as the same engine then has it.
func (c *Client) ResumeSaga(ctx context.Context, node, saga, id string) (SagaRunView, error) {
	t, n, err := c.engine(ctx, node, saga)
	if err != nil {
		return SagaRunView{}, err
	}
	body, err := anypb.New(&sagav1.Control{Op: &sagav1.Control_Resume{Resume: &sagav1.RunRef{Saga: saga, Id: id}}})
	if err != nil {
		return SagaRunView{}, err
	}
	if _, err := c.rpc.Call(ctx, &inspectv1.CallRequest{Node: n, Target: t, Body: body}); err != nil {
		return SagaRunView{}, err
	}
	return c.sagaRun(ctx, n, t, saga, id)
}

// askSaga puts q to the engine t on node, as an Inspector Query, and reads
// its answer into answer.
func (c *Client) askSaga(ctx context.Context, node string, t *inspectv1.Target, q *sagav1.Query, answer proto.Message) error {
	body, err := anypb.New(q)
	if err != nil {
		return err
	}
	resp, err := c.rpc.Query(ctx, &inspectv1.QueryRequest{Node: node, Target: t, Body: body})
	if err != nil {
		return err
	}
	return resp.GetBody().UnmarshalTo(answer)
}

func sagaRunView(r *sagav1.Run) SagaRunView {
	v := SagaRunView{
		Saga: r.GetSaga(), ID: r.GetId(), Version: r.GetSagaVersion(), State: r.GetState(),
		Status: strings.ToLower(strings.TrimPrefix(r.GetStatus().String(), "STATUS_")),
		Data:   decoded(r.GetData()), DataOmitted: r.GetDataOmitted(), Waiting: r.GetWaiting(),
		Visit: r.GetVisit(), EffectDone: r.GetEffectDone(),
		Attempts: r.GetAttempts(), Error: r.GetError(), Cause: r.GetCause(),
		Wake: when(r.GetWake()), RetryAt: when(r.GetRetryAt()), Deadline: when(r.GetDeadline()),
		Owner: r.GetOwner(), LeaseUntil: when(r.GetLeaseUntil()), Epoch: r.GetEpoch(),
		Revision: r.GetRevision(), Created: when(r.GetCreatedAt()), Updated: when(r.GetUpdatedAt()),
	}
	for _, s := range r.GetInbox() {
		v.Inbox = append(v.Inbox, SagaSignal{Seq: s.GetSeq(), Event: s.GetEvent(), Payload: decoded(s.GetPayload()), Omitted: s.GetPayloadOmitted(), Version: s.GetVersion()})
	}
	return v
}

// when is t as the views write a time; "" for none.
func when(t *timestamppb.Timestamp) string {
	if t == nil {
		return ""
	}
	return t.AsTime().Format(time.RFC3339Nano)
}

// decoded is the JSON an engine wrote, as a value; nil for none, and the
// text as it is should it not be JSON. JSON null is kept as such, since a
// nil value is left out as none.
func decoded(text string) any {
	if text == "" {
		return nil
	}
	var v any
	if json.Unmarshal([]byte(text), &v) != nil {
		return text
	}
	if v == nil {
		return jsontext.Value(text)
	}
	return v
}
