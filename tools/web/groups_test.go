package web_test

import (
	"net/http"
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
	"github.com/floatdrop/grpcproc/tools/web"
)

func TestGroups(t *testing.T) {
	f := testcluster.Start(t, "c")
	f.Groups(t)
	s := serve(t, f, web.Options{})
	var v client.GroupsView
	if code := get(t, s, "/api/groups?prefix=ch&limit=5", &v); code != http.StatusOK || v.Scope != "default" || len(v.Groups) != 1 || v.Groups[0].Name != "chat" {
		t.Fatalf("%d %+v", code, v)
	}
	var m client.GroupMembersView
	if code := get(t, s, "/api/group?node=b&group=chat&limit=1", &m); code != http.StatusOK || m.Node != "b" || len(m.Members) != 1 || m.Omitted != 1 {
		t.Fatalf("%d %+v", code, m)
	}
	if code := get(t, s, "/api/groups?node=c", nil); code == http.StatusOK {
		t.Fatalf("on c: %d", code)
	}
}
