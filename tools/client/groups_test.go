package client_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

func TestGroups(t *testing.T) {
	f := testcluster.Start(t, "c")
	f.Groups(t)
	c := client.New(f.C.Conn("a"))
	ctx := t.Context()

	v, err := c.Groups(ctx, "", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if v.Node != "a" || v.Scope != "default" || !slices.Equal(v.Scopes, []string{"broken", "default", "rooms"}) || v.PID == "" {
		t.Fatalf("scope: %+v", v)
	}
	want := []client.GroupView{{Name: "chat", Members: 2, Local: 1, Nodes: 2}, {Name: "echo", Members: 1, Nodes: 1}}
	if !slices.Equal(v.Groups, want) {
		t.Fatalf("groups %+v", v.Groups)
	}
	if len(v.Peers) != 2 || v.Peers[0].Node != "b" || v.Peers[0].State != "synced" || v.Peers[0].Scope == "" || v.Peers[0].Members != 2 ||
		v.Peers[1] != (client.GroupPeerView{Node: "c", State: "absent"}) {
		t.Fatalf("peers %+v", v.Peers)
	}
	v, err = c.Groups(ctx, "b", "", "ch", 1)
	if err != nil || v.Node != "b" || len(v.Groups) != 1 || v.Groups[0].Local != 1 || v.Omitted != 0 {
		t.Fatalf("b's copy, prefix ch: %+v %v", v, err)
	}
	v, err = c.Groups(ctx, "", "", "", 1)
	if err != nil || len(v.Groups) != 1 || v.Omitted != 1 {
		t.Fatalf("one group: %+v %v", v, err)
	}
	v, err = c.Groups(ctx, "a", "rooms", "", 0)
	if err != nil || len(v.Groups) != 0 || len(v.Peers) != 0 || v.Groups == nil {
		t.Fatalf("rooms: %+v %v", v, err)
	}

	m, err := c.GroupMembers(ctx, "", "", "chat", 0)
	if err != nil || !slices.Equal(m.Members, []string{f.Talker.String(), f.Echo.String()}) || m.Node != "a" || m.Scope != "default" {
		t.Fatalf("chat: %+v %v", m, err)
	}
	m, err = c.GroupMembers(ctx, "", "", "chat", 1)
	if err != nil || len(m.Members) != 1 || m.Omitted != 1 {
		t.Fatalf("chat, one: %+v %v", m, err)
	}
	m, err = c.GroupMembers(ctx, "", "", "nobody", 0)
	if err != nil || m.Members == nil || len(m.Members) != 0 {
		t.Fatalf("an empty group: %+v %v", m, err)
	}

	for _, tc := range []struct {
		name string
		err  func() error
		want string
	}{
		{"no group", func() error { _, err := c.GroupMembers(ctx, "", "", "", 0); return err }, "a group is required"},
		{"a scope a does not run", func() error { _, err := c.Groups(ctx, "", "nope", "", 0); return err }, `named "nope" on the node serving the Inspector: it runs broken, default, rooms`},
		{"a node with no scope", func() error { _, err := c.Groups(ctx, "c", "", "", 0); return err }, "no grpcproc/groups scope on c"},
		{"members on a node with no scope", func() error { _, err := c.GroupMembers(ctx, "c", "", "x", 0); return err }, "no grpcproc/groups scope on c"},
		{"a scope that answers no queries", func() error { _, err := c.Groups(ctx, "", "broken", "", 0); return err }, "answers no queries"},
		{"members of it", func() error { _, err := c.GroupMembers(ctx, "", "broken", "x", 0); return err }, "answers no queries"},
		{"a node that does not exist", func() error { _, err := c.Groups(ctx, "nowhere", "", "", 0); return err }, "nowhere"},
	} {
		err := tc.err()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want %q", tc.name, err, tc.want)
		}
		if strings.Contains(tc.want, "scope") && !errors.Is(err, client.ErrNoGroups) {
			t.Errorf("%s: %v is not ErrNoGroups", tc.name, err)
		}
	}
}
