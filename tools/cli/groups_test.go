package cli_test

import (
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

func TestGroups(t *testing.T) {
	f := testcluster.Start(t, "c")
	f.Groups(t)
	has(t, ok(t, run(t, f, "groups")),
		"scope default on a (it runs broken, default, rooms)",
		"GROUP MEMBERS LOCAL NODES", "chat 2 1 2", "echo 1 0 1",
		"PEER STATE MEMBERS SCOPE", "b synced 2 <b.", "c absent 0")
	has(t, ok(t, run(t, f, "groups", "--limit", "1")), "chat 2 1 2", "… 1 more groups past --limit")
	has(t, ok(t, run(t, f, "groups", "--node", "b", "--prefix", "e")), "scope default on b", "echo 1 1 1")
	has(t, ok(t, run(t, f, "groups", "chat")), "PID", f.Talker.String(), f.Echo.String())
	has(t, ok(t, run(t, f, "groups", "--limit", "1", "chat")), f.Talker.String(), "… 1 more members past --limit")
	has(t, ok(t, run(t, f, "--json", "groups")), `"scope": "default"`, `"name": "chat"`)
	has(t, ok(t, run(t, f, "--json", "groups", "chat")), `"group": "chat"`, `"members": [`)
	for _, tc := range []struct {
		args []string
		code int
		want string
	}{
		{[]string{"groups", "a", "b"}, 2, "at most one group"},
		{[]string{"groups", "--bogus"}, 2, ""},
		{[]string{"groups", "--node", "c"}, 1, "no grpcproc/groups scope on c"},
		{[]string{"groups", "--node", "c", "chat"}, 1, "no grpcproc/groups scope on c"},
	} {
		if r := run(t, f, tc.args...); r.code != tc.code || !strings.Contains(r.stderr, tc.want) {
			t.Errorf("%v: %+v", tc.args, r)
		}
	}
}
