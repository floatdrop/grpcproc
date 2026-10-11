package mcpserver_test

import (
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
	"github.com/floatdrop/grpcproc/tools/mcpserver"
)

func TestGroups(t *testing.T) {
	f := testcluster.Start(t, "c")
	f.Groups(t)
	cs := connect(t, f, mcpserver.Options{})
	var v client.GroupsView
	if msg := call(t, cs, "process_groups", nil, &v); msg != "" || v.Scope != "default" || len(v.Groups) != 2 || len(v.Peers) != 2 {
		t.Fatalf("groups: %q %+v", msg, v)
	}
	if msg := call(t, cs, "process_groups", map[string]any{"node": "b", "prefix": "e", "limit": 1}, &v); msg != "" || v.Node != "b" || len(v.Groups) != 1 {
		t.Fatalf("b's, prefix e: %q %+v", msg, v)
	}
	if msg := call(t, cs, "process_groups", map[string]any{"node": "c"}, &v); !strings.Contains(msg, "no grpcproc/groups scope") {
		t.Fatalf("on c: %q", msg)
	}
	var m client.GroupMembersView
	if msg := call(t, cs, "group_members", map[string]any{"group": "chat"}, &m); msg != "" || len(m.Members) != 2 || m.Members[0] != f.Talker.String() {
		t.Fatalf("chat: %q %+v", msg, m)
	}
	if msg := call(t, cs, "group_members", map[string]any{"group": ""}, &m); !strings.Contains(msg, "a group is required") {
		t.Fatalf("no group: %q", msg)
	}
}
