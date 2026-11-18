package biz

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

func entry(ns, name, state string, restarts int64, created int64, owners ...string) *Entry {
	return &Entry{
		Row: k8scontract.Row{
			Namespace: ns, Name: name, State: state, CreateTime: time.Unix(created, 0),
			Cells: map[string]any{"restarts": restarts, "status": k8scontract.StateCell{Text: state}},
		},
		Labels: map[string]string{"app": name}, Node: "n-" + ns, Owners: owners,
	}
}

func names(es []*Entry) string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Row.Namespace + "/" + e.Row.Name
	}
	return strings.Join(out, ",")
}

func TestSelectEntries(t *testing.T) {
	t.Parallel()
	all := []*Entry{
		entry("b", "web-2", "ok", 3, 30, "o1"), entry("a", "web-1", "abnormal", 10, 10, "o1"),
		entry("a", "db", "ok", 0, 20, "o2"), entry("b", "Web-3", "pending", 1, 40),
	}
	cases := []struct {
		name  string
		q     ListQuery
		want  string
		total int
	}{
		{"default sort by name then namespace", ListQuery{}, "b/Web-3,a/db,a/web-1,b/web-2", 4},
		{"keyword is case-insensitive substring", ListQuery{Keyword: " WEB "}, "b/Web-3,a/web-1,b/web-2", 3},
		{"state filter", ListQuery{State: "ok"}, "a/db,b/web-2", 2},
		{"node filter", ListQuery{Node: "n-a"}, "a/db,a/web-1", 2},
		{"owner filter", ListQuery{Owner: "o1"}, "a/web-1,b/web-2", 2},
		{"label match", ListQuery{Match: func(l map[string]string) bool { return l["app"] == "db" }}, "a/db", 1},
		{"numeric column sorts by value not text", ListQuery{SortBy: "restarts", SortDesc: true}, "a/web-1,b/web-2,b/Web-3,a/db", 4},
		{"sort by age", ListQuery{SortBy: "createTime"}, "a/web-1,a/db,b/web-2,b/Web-3", 4},
		{"sort by namespace desc keeps name tiebreak ascending", ListQuery{SortBy: "namespace", SortDesc: true}, "b/Web-3,b/web-2,a/db,a/web-1", 4},
		{"page 2 of size 3", ListQuery{Page: 2, PageSize: 3}, "b/web-2", 4},
		{"page beyond the end", ListQuery{Page: 9, PageSize: 3}, "", 4},
		{"filters combine and total counts filtered rows", ListQuery{Keyword: "web", State: "ok", PageSize: 1}, "b/web-2", 1},
	}
	for _, tc := range cases {
		page, total := SelectEntries(all, tc.q)
		if got := names(page); got != tc.want || total != tc.total {
			t.Errorf("%s: got %q total %d, want %q total %d", tc.name, got, total, tc.want, tc.total)
		}
	}
	if all[0].Row.Name != "web-2" {
		t.Fatal("SelectEntries must not reorder its input")
	}
	if page, _ := SelectEntries(all, ListQuery{PageSize: 100000}); len(page) != 4 {
		t.Fatal("page size must be clamped, not rejected")
	}
}

func TestLogCursorResume(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	// 同一纳秒三行，随后一行。
	stamps := []time.Time{t0, t0, t0, t0.Add(time.Millisecond)}
	first := NewLogTracker("")
	var cursors []string
	for _, ts := range stamps {
		c, emit := first.Next(ts)
		if !emit {
			t.Fatal("a fresh stream emits every line")
		}
		cursors = append(cursors, c)
	}
	if cursors[0] == cursors[1] || cursors[1] == cursors[2] {
		t.Fatalf("lines sharing a timestamp need distinct cursors: %v", cursors)
	}
	// 在同一时间戳的第二行处断开：续接时集群从该时间戳重放，前两行必须丢弃，其余必须保留。
	resumed := NewLogTracker(cursors[1])
	since, ok := resumed.Since()
	if !ok || !since.Equal(t0) {
		t.Fatalf("Since = %v %v", since, ok)
	}
	var emitted []int
	for i, ts := range stamps {
		c, emit := resumed.Next(ts)
		if c != cursors[i] {
			t.Fatalf("cursor for line %d changed across resume: %s vs %s", i, c, cursors[i])
		}
		if emit {
			emitted = append(emitted, i)
		}
	}
	if len(emitted) != 2 || emitted[0] != 2 || emitted[1] != 3 {
		t.Fatalf("resume emitted lines %v, want [2 3]", emitted)
	}
	// 重放里带着比游标更早的行（集群的 sinceTime 精度为秒）：同样丢弃。
	early := NewLogTracker(cursors[3])
	if _, emit := early.Next(t0.Add(-time.Second)); emit {
		t.Fatal("lines before the cursor must be dropped")
	}
	for _, bad := range []string{"", "x", "2026-01-02T03:04:05Z", "2026-01-02T03:04:05Z/-1", "nope/1"} {
		if _, ok := ParseLogCursor(bad); ok {
			t.Errorf("ParseLogCursor(%q) must fail", bad)
		}
		if _, ok := NewLogTracker(bad).Since(); ok {
			t.Errorf("invalid cursor %q must start from scratch", bad)
		}
	}
	ts, text, ok := ParseLogLine([]byte("2026-01-02T03:04:05.123456789Z hello world"))
	if !ok || string(text) != "hello world" || !ts.Equal(t0) {
		t.Fatalf("ParseLogLine: %v %q %v", ts, text, ok)
	}
	if _, text, ok := ParseLogLine([]byte("no timestamp here")); ok || string(text) != "no timestamp here" {
		t.Fatal("a line without timestamp is returned whole")
	}
	if _, text, ok := ParseLogLine([]byte("2026-01-02T03:04:05Z")); !ok || len(text) != 0 {
		t.Fatal("an empty line still has a timestamp")
	}
}

func TestSanitizeObject(t *testing.T) {
	t.Parallel()
	secret := map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{
			"name": "s", "managedFields": []any{"x"},
			"annotations": map[string]any{LastAppliedAnnotation: `{"data":{"password":"cGxhaW4="}}`, "keep": "1"},
		},
		"data": map[string]any{"password": "cGxhaW4="}, "stringData": map[string]any{"token": "plain"},
		"status": map[string]any{"x": 1},
	}
	out := SanitizeObject(secret)
	meta := out["metadata"].(map[string]any)
	if _, ok := meta["managedFields"]; ok {
		t.Error("managedFields must be removed")
	}
	if _, ok := out["status"]; ok {
		t.Error("status must be removed")
	}
	ann := meta["annotations"].(map[string]any)
	if _, ok := ann[LastAppliedAnnotation]; ok || ann["keep"] != "1" {
		t.Errorf("annotations: %v", ann)
	}
	if out["data"].(map[string]any)["password"] != k8scontract.MaskedValue || out["stringData"].(map[string]any)["token"] != k8scontract.MaskedValue {
		t.Errorf("secret values must be masked: %v %v", out["data"], out["stringData"])
	}
	// 入参不得被修改（它还要用于后续操作）。
	if secret["data"].(map[string]any)["password"] != "cGxhaW4=" || secret["status"] == nil ||
		secret["metadata"].(map[string]any)["managedFields"] == nil ||
		secret["metadata"].(map[string]any)["annotations"].(map[string]any)[LastAppliedAnnotation] == nil {
		t.Fatal("SanitizeObject mutated its input")
	}
	cm := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "data": map[string]any{"k": "v"}}
	if SanitizeObject(cm)["data"].(map[string]any)["k"] != "v" {
		t.Error("non-secret data must be kept")
	}
	// 不是核心组的同名 Kind 不当作 Secret。
	other := map[string]any{"apiVersion": "example.com/v1", "kind": "Secret", "data": map[string]any{"k": "v"}}
	if IsSecret(other) {
		t.Error("only core/v1 Secret is a Secret")
	}
}

type memAudit struct {
	mu   sync.Mutex
	rows []*AuditRecord
	err  error
}

func (m *memAudit) Insert(_ context.Context, r *AuditRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.rows = append(m.rows, r)
	return nil
}

type obs struct {
	mu      sync.Mutex
	audit   int
	streams map[string]int
}

func (o *obs) AuditFailed() { o.mu.Lock(); o.audit++; o.mu.Unlock() }
func (o *obs) StreamDelta(kind string, d int) {
	o.mu.Lock()
	if o.streams == nil {
		o.streams = map[string]int{}
	}
	o.streams[kind] += d
	o.mu.Unlock()
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestAuditor(t *testing.T) {
	t.Parallel()
	repo, o := &memAudit{}, &obs{}
	a := NewAuditor(repo, o, quiet)
	c := &Cluster{ID: "c1", Name: "prod"}
	id := Identity{UserID: "u1", Username: "alice"}
	big := map[string]any{"kind": "ConfigMap", "apiVersion": "v1", "data": map[string]any{"k": strings.Repeat("x", k8scontract.MaxAuditObjectBytes)}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 请求已取消也要落审计
	a.Record(ctx, c, id, []*AuditEvent{
		{Action: "update", Before: map[string]any{"kind": "Secret", "apiVersion": "v1", "data": map[string]any{"p": "c2VjcmV0"}}, After: big, Patch: map[string]any{"a": 1}},
		{Action: "delete", Err: NewError(403, k8scontract.ReasonForbidden, "pods is forbidden")},
		nil,
	})
	if len(repo.rows) != 2 {
		t.Fatalf("rows = %d", len(repo.rows))
	}
	r := repo.rows[0]
	if r.Result != k8scontract.AuditOK || bytes.Contains(r.Before, []byte("c2VjcmV0")) || !bytes.Contains(r.Before, []byte(k8scontract.MaskedValue)) {
		t.Errorf("before must be masked: %s", r.Before)
	}
	if r.After != nil || !r.Truncated || string(r.Patch) != `{"a":1}` {
		t.Errorf("oversized object must be dropped and flagged: after=%d truncated=%v patch=%s", len(r.After), r.Truncated, r.Patch)
	}
	if f := repo.rows[1]; f.Result != k8scontract.AuditFailed || f.Error != "pods is forbidden" || f.ClusterName != "prod" || f.Identity.Username != "alice" {
		t.Errorf("failed row: %+v", f)
	}
	repo.err = errors.New("db down")
	a.Record(context.Background(), c, id, []*AuditEvent{{Action: "scale"}})
	if o.audit != 1 {
		t.Fatalf("audit failure must be counted, got %d", o.audit)
	}
}

func TestDesiredAndDiffBindings(t *testing.T) {
	t.Parallel()
	scopes := []Scope{
		{RoleID: "r1", Namespace: "app", Level: k8scontract.LevelView},
		{RoleID: "r1", Namespace: "gone", Level: k8scontract.LevelEdit},
		{RoleID: "r2", Namespace: "*", Level: k8scontract.LevelEdit},
		{RoleID: "r3", Namespace: "*", Level: k8scontract.LevelClusterAdmin},
		{RoleID: "r4", Namespace: "app", Level: k8scontract.LevelClusterAdmin}, // 非法：集群管理员只能是全部命名空间
		{RoleID: "r5", Namespace: "app", Level: 9},
	}
	want, skipped := DesiredBindings(scopes, []string{"app", "default"})
	got := map[string]string{}
	for _, b := range want {
		got[b.Namespace+"|"+b.Name] = b.Group + "→" + b.Role
	}
	expect := map[string]string{
		"|reborn:admin":            "reborn:admin→cluster-admin",
		"app|reborn:r1:view":       "reborn:role:r1→view",
		"|reborn:r1:cluster-view":  "reborn:role:r1→reborn:cluster-view",
		"|reborn:r2:edit":          "reborn:role:r2→edit",
		"|reborn:r2:cluster-view":  "reborn:role:r2→reborn:cluster-view",
		"|reborn:r2:debug":         "reborn:role:r2→reborn:debug", // 开发及以上档位附带调试容器权限
		"|reborn:r3:cluster-admin": "reborn:role:r3→cluster-admin",
		"|reborn:r3:cluster-view":  "reborn:role:r3→reborn:cluster-view",
	}
	if len(got) != len(expect) {
		t.Fatalf("bindings = %v", got)
	}
	for k, v := range expect {
		if got[k] != v {
			t.Errorf("binding %s = %q, want %q", k, got[k], v)
		}
	}
	if len(skipped) != 1 || skipped[0] != "gone" {
		t.Errorf("skipped = %v", skipped)
	}
	// 没有任何授权时仍保留超管的绑定。
	if only, _ := DesiredBindings(nil, nil); len(only) != 1 || only[0].Group != k8scontract.AdminGroup {
		t.Fatalf("admin binding must always exist: %v", only)
	}

	have := []Binding{
		{Name: "reborn:admin", Group: k8scontract.AdminGroup, Role: "cluster-admin"},      // 一致：不动
		{Namespace: "app", Name: "reborn:r1:view", Group: "", Role: "view"},               // 形态被改坏：替换
		{Name: "reborn:r9:edit", Group: "reborn:role:r9", Role: "edit"},                   // 多余：删除
		{Namespace: "old", Name: "reborn:r1:edit", Group: "reborn:role:r1", Role: "edit"}, // 多余：删除
	}
	upsert, remove := DiffBindings(want, have)
	if len(upsert) != len(want)-1 {
		t.Errorf("upsert = %d, want %d", len(upsert), len(want)-1)
	}
	for _, b := range upsert {
		if b.Name == "reborn:admin" {
			t.Error("unchanged binding must not be rewritten")
		}
	}
	if len(remove) != 2 || remove[0].Name != "reborn:r9:edit" || remove[1].Namespace != "old" {
		t.Errorf("remove = %+v", remove)
	}
	// 幂等：对账后的状态再对账一次，没有任何动作。
	if u, r := DiffBindings(want, want); len(u)+len(r) != 0 {
		t.Errorf("reconcile is not idempotent: %v %v", u, r)
	}
}

func TestFrameAndRing(t *testing.T) {
	t.Parallel()
	f := EncodeFrame(k8scontract.FrameOutput, []byte("hi"))
	typ, payload, err := DecodeFrame(f)
	if err != nil || typ != k8scontract.FrameOutput || string(payload) != "hi" {
		t.Fatalf("frame round trip: %v %q %v", typ, payload, err)
	}
	if typ, payload, err := DecodeFrame([]byte{k8scontract.FramePing}); err != nil || typ != k8scontract.FramePing || len(payload) != 0 {
		t.Fatal("a frame may have an empty payload")
	}
	if _, _, err := DecodeFrame(nil); err == nil {
		t.Fatal("empty frame must be rejected")
	}
	if _, _, err := DecodeFrame(make([]byte, k8scontract.ExecMaxFrameSize+2)); err == nil {
		t.Fatal("oversized frame must be rejected")
	}

	r := NewRing(8)
	r.Write([]byte("abc"))
	r.Write([]byte("defgh"))
	if string(r.Bytes()) != "abcdefgh" {
		t.Fatalf("ring = %q", r.Bytes())
	}
	r.Write([]byte("ij")) // 溢出：丢最旧的
	if string(r.Bytes()) != "cdefghij" {
		t.Fatalf("ring after wrap = %q", r.Bytes())
	}
	r.Write([]byte("0123456789ABCDEF")) // 单次写入超过容量：只留尾部
	if string(r.Bytes()) != "89ABCDEF" {
		t.Fatalf("ring after oversized write = %q", r.Bytes())
	}
	r.Write([]byte("xy"))
	if string(r.Bytes()) != "ABCDEFxy" {
		t.Fatalf("ring keeps order after oversized write: %q", r.Bytes())
	}
	r.Reset()
	if len(r.Bytes()) != 0 {
		t.Fatal("reset")
	}
	NewRing(0).Write([]byte("x")) // 零容量不 panic
}

func TestAsError(t *testing.T) {
	t.Parallel()
	wrapped := errors.Join(errors.New("ctx"), NewError(409, k8scontract.ReasonConflict, "changed"))
	if e := AsError(wrapped); e.Code != 409 || e.Reason != k8scontract.ReasonConflict {
		t.Fatalf("wrapped business error: %+v", e)
	}
	if e := AsError(errors.New("pq: password authentication failed for user x")); e.Code != 500 || strings.Contains(e.Message, "password") {
		t.Fatalf("internal errors must not leak details: %+v", e)
	}
}
