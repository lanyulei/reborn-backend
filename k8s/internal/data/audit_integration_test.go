//go:build integration

package data

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// ptr 返回 v 的指针（测试里给「多久之后 / 之前」这类可选参数取址）。
func ptr[T any](v T) *T { return &v }

// jsonEqual 语义比较 jsonb 列读出的 JSON 与期望值：jsonb 会规范化键序与空白，不能按字节比。
func jsonEqual(t *testing.T, got []byte, want string) bool {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(got, &a); err != nil {
		t.Fatalf("decode stored json %s: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &b); err != nil {
		t.Fatalf("decode want json: %v", err)
	}
	return reflect.DeepEqual(a, b)
}

// auditRow 一条审计的断言视图；可空列用指针（NULL = nil）。
type auditRow struct {
	ID, Username, ClientIP, RequestID, ClusterName, Namespace string
	Group, Version, Resource, Kind, Name, Action              string
	UserID, ClusterID, ChangeID                               *string
	Result                                                    int32
	Error                                                     string
	Before, After, Patch                                      []byte
	Truncated, Emergency                                      bool
}

// auditRows 读出满足条件的审计（按操作时间）。
func (e *env) auditRows(where string, args ...any) []auditRow {
	e.t.Helper()
	rows, err := e.admin.Query(ctxb(), `SELECT id::text, user_id::text, username, client_ip, request_id, cluster_id::text, cluster_name,
		namespace, api_group, api_version, resource, kind, name, action, result, error, before_obj::text, after_obj::text, patch::text,
		truncated, emergency, change_id::text FROM k8s_audit `+where+" ORDER BY create_time, id", args...)
	if err != nil {
		e.t.Fatalf("read audit rows: %v", err)
	}
	defer rows.Close()
	var out []auditRow
	for rows.Next() {
		var r auditRow
		if err := rows.Scan(&r.ID, &r.UserID, &r.Username, &r.ClientIP, &r.RequestID, &r.ClusterID, &r.ClusterName, &r.Namespace,
			&r.Group, &r.Version, &r.Resource, &r.Kind, &r.Name, &r.Action, &r.Result, &r.Error, &r.Before, &r.After, &r.Patch,
			&r.Truncated, &r.Emergency, &r.ChangeID); err != nil {
			e.t.Fatalf("read audit rows: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		e.t.Fatalf("read audit rows: %v", err)
	}
	return out
}

// 一条完整的审计：身份、集群、对象、结果、对象快照与改动逐列落库；行落在当月分区。
func TestAuditInsert(t *testing.T) {
	e := newEnv(t)
	repo := NewAuditRepo(e.d)
	clusterID, changeID, user := e.cluster(clusterSpec{}), e.id(), e.id()
	const before = `{ "kind": "Deployment", "metadata": {"name": "web"}, "spec": {"replicas": 2} }`
	rec := &biz.AuditRecord{
		Identity:    biz.Identity{UserID: user, Username: "ops", ClientIP: "10.0.0.9", RequestID: "req-0001", Emergency: true, ChangeID: changeID},
		ClusterID:   clusterID,
		ClusterName: "c-prod",
		Ref:         k8scontract.Ref{Group: "apps", Version: "v1", Resource: "deployments", Namespace: "default", Name: "web"},
		Kind:        "Deployment",
		Action:      k8scontract.ActionScale,
		Result:      k8scontract.AuditFailed,
		Error:       "forbidden",
		Before:      []byte(before),
		After:       []byte(`{"spec":{"replicas":5}}`),
		Patch:       []byte(`{"replicas":5}`),
		Truncated:   true,
	}
	if err := repo.Insert(ctxb(), rec); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	rows := e.auditRows("WHERE cluster_id = $1", clusterID)
	if len(rows) != 1 {
		t.Fatalf("stored %d audit rows, want 1", len(rows))
	}
	r := rows[0]
	for name, col := range map[string]struct {
		got  *string
		want string
	}{
		"user_id":    {r.UserID, user},
		"cluster_id": {r.ClusterID, clusterID},
		"change_id":  {r.ChangeID, changeID},
	} {
		if col.got == nil || *col.got != col.want {
			t.Fatalf("%s = %v, want %s", name, col.got, col.want)
		}
	}
	if !idgen.IsUUID(r.ID) {
		t.Fatalf("id = %q, want a canonical UUID", r.ID)
	}
	if r.Username != "ops" || r.ClientIP != "10.0.0.9" || r.RequestID != "req-0001" || r.ClusterName != "c-prod" ||
		r.Group != "apps" || r.Version != "v1" || r.Resource != "deployments" || r.Namespace != "default" || r.Name != "web" ||
		r.Kind != "Deployment" || r.Action != "scale" || r.Result != k8scontract.AuditFailed || r.Error != "forbidden" ||
		!r.Truncated || !r.Emergency {
		t.Fatalf("audit row = %+v", r)
	}
	if !jsonEqual(t, r.Before, before) || !jsonEqual(t, r.After, `{"spec":{"replicas":5}}`) || !jsonEqual(t, r.Patch, `{"replicas":5}`) {
		t.Fatalf("object snapshots = %s / %s / %s", r.Before, r.After, r.Patch)
	}
	// 分区键取数据库时钟：行落在当月分区，经父表按主键可查。
	partition := partitionName(e.utcMonth())
	if n := e.count("SELECT count(*) FROM "+partition+" WHERE id = $1::uuid", r.ID); n != 1 {
		t.Fatalf("%d rows of this audit in %s, want 1", n, partition)
	}
	if n := e.count("SELECT count(*) FROM k8s_audit WHERE id = $1::uuid", r.ID); n != 1 {
		t.Fatalf("%d rows of this audit reachable through the parent table, want 1", n)
	}

	// 列截断：超过列宽的文本截到列宽（不切断多字节字符），审计行不会因 22001 整条丢失。
	long, longer := strings.Repeat("x", 400), strings.Repeat("y", 800)
	multi := strings.Repeat("版", 40) // 120 字节
	clip := &biz.AuditRecord{
		Identity:  biz.Identity{UserID: user, Username: multi, ClientIP: long, RequestID: long},
		ClusterID: clusterID,
		Ref:       k8scontract.Ref{Group: long, Version: long, Resource: long, Namespace: long, Name: long},
		Kind:      long,
		Action:    k8scontract.ActionUpdate,
		Result:    k8scontract.AuditOK,
		Error:     longer,
	}
	if err := repo.Insert(ctxb(), clip); err != nil {
		t.Fatalf("Insert() with overlong columns error = %v", err)
	}
	rows = e.auditRows("WHERE username <> 'ops'")
	if len(rows) != 1 {
		t.Fatalf("stored %d clipped audit rows, want 1", len(rows))
	}
	if r = rows[0]; len(r.ClientIP) != 64 || len(r.RequestID) != 64 || len(r.Namespace) != 63 || len(r.Group) != 128 ||
		len(r.Version) != 32 || len(r.Resource) != 128 || len(r.Kind) != 128 || len(r.Name) != 253 || len(r.Error) != 512 {
		t.Fatalf("ascii columns are not clipped to their widths: client_ip %d, request_id %d, namespace %d, group %d, version %d, resource %d, kind %d, name %d, error %d",
			len(r.ClientIP), len(r.RequestID), len(r.Namespace), len(r.Group), len(r.Version), len(r.Resource), len(r.Kind), len(r.Name), len(r.Error))
	}
	// 64 字节的上限装不下 22 个汉字（66 字节）：留 21 个（63 字节），字符完整。
	if !utf8.ValidString(r.Username) || strings.Trim(r.Username, "版") != "" || len([]rune(r.Username)) != 21 {
		t.Fatalf("multi-byte username clipped to %d runes (%d bytes), want 21 (63 bytes)", len([]rune(r.Username)), len(r.Username))
	}

	// 空值：用户 / 变更单为空落 NULL，其余 NOT NULL 列落空串，对象为空落 NULL（不是字符串 "null"）。
	blank := &biz.AuditRecord{ClusterID: clusterID, Action: k8scontract.ActionExec, Result: k8scontract.AuditOK}
	if err := repo.Insert(ctxb(), blank); err != nil {
		t.Fatalf("Insert() of a system audit error = %v", err)
	}
	if rows = e.auditRows("WHERE action = 'exec'"); len(rows) != 1 {
		t.Fatalf("stored %d system audits, want 1", len(rows))
	}
	if r = rows[0]; r.UserID != nil || r.ChangeID != nil || r.RequestID != "" || r.Username != "" || r.ClientIP != "" ||
		r.ClusterName != "" || r.Namespace != "" || r.Group != "" || r.Version != "" || r.Resource != "" || r.Kind != "" ||
		r.Name != "" || r.Error != "" || r.Before != nil || r.After != nil || r.Patch != nil || r.Truncated || r.Emergency ||
		r.Result != k8scontract.AuditOK {
		t.Fatalf("system audit = %+v", r)
	}

	// 边界：对象不是合法 JSON 时 jsonb 列拒收（22P02），整条不落库——审计对象由 biz.Auditor 序列化，仓储不兜底。
	bad := &biz.AuditRecord{ClusterID: clusterID, Action: k8scontract.ActionExec, Result: k8scontract.AuditOK, Before: []byte("{not json")}
	if err := repo.Insert(ctxb(), bad); pgCode(err) != "22P02" {
		t.Fatalf("Insert() of an invalid JSON object error = %v, want 22P02", err)
	}
	// 边界（现状）：集群 ID 为空串不是合法 uuid，整条写入被拒（22P02）。调用方 biz.Auditor 总是带着集群；
	// 这里钉住现状——哪天改成落 NULL（同 user_id / change_id 的做法），请同步改这条断言。
	if err := repo.Insert(ctxb(), &biz.AuditRecord{Action: k8scontract.ActionExec, Result: k8scontract.AuditOK}); pgCode(err) != "22P02" {
		t.Fatalf("Insert() without a cluster ID error = %v, want 22P02", err)
	}
	if n := e.count("SELECT count(*) FROM k8s_audit WHERE before_obj::text LIKE '%%not json%%' OR result IS NULL"); n != 0 {
		t.Fatalf("%d rejected audits were (partially) stored", n)
	}
	// 主键 (id, create_time)：同一时刻的同一 ID 只能有一行。
	e.exec(`INSERT INTO k8s_audit (id, action, result, create_time) VALUES ($1, 'exec', 1, '2026-01-01 00:00:00+00')`, r.ID)
	if err := e.try(`INSERT INTO k8s_audit (id, action, result, create_time) VALUES ($1, 'exec', 1, '2026-01-01 00:00:00+00')`, r.ID); pgCode(err) != "23505" {
		t.Fatalf("duplicate (id, create_time) error = %v, want 23505", err)
	}
}

// execRow 一条终端会话记录的断言视图。
type execRow struct {
	User, Username, ClientIP, Cluster, ClusterName, Namespace, Pod, Container, Reason string
	Kind                                                                              int32
	Start                                                                             time.Time
	Ended                                                                             bool
	End                                                                               time.Time
	BytesIn, BytesOut                                                                 int64
	Recorded, Truncated                                                               bool
}

func (e *env) execRow(id string) execRow {
	e.t.Helper()
	var r execRow
	e.scan(`SELECT user_id::text, username, client_ip, cluster_id::text, cluster_name, namespace, pod, container, kind, start_time,
		end_time IS NOT NULL, COALESCE(end_time, start_time), end_reason, bytes_in, bytes_out, recorded, record_truncated
		FROM k8s_exec_session WHERE id = $1`, []any{id},
		&r.User, &r.Username, &r.ClientIP, &r.Cluster, &r.ClusterName, &r.Namespace, &r.Pod, &r.Container, &r.Kind, &r.Start,
		&r.Ended, &r.End, &r.Reason, &r.BytesIn, &r.BytesOut, &r.Recorded, &r.Truncated)
	return r
}

// 终端会话的登记与收尾：登记写身份 / 集群 / 参数并截断过长的身份列；收尾只在会话尚未结束时生效一次。
func TestExecSessionStartFinish(t *testing.T) {
	e := newEnv(t)
	repo := NewAuditRepo(e.d)
	clusterID, user := e.cluster(clusterSpec{}), e.id()
	long := strings.Repeat("u", 100)
	s := &biz.ExecSession{
		ID: e.id(), Identity: biz.Identity{UserID: user, Username: long, ClientIP: long},
		ClusterID: clusterID, ClusterName: "c-prod",
		Params: k8scontract.ExecParams{Namespace: "default", Pod: "web-1", Container: "app", Kind: k8scontract.ExecKindDebug},
	}
	if err := repo.Start(ctxb(), s); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	r := e.execRow(s.ID)
	if r.User != user || len(r.Username) != 64 || len(r.ClientIP) != 64 || r.Cluster != clusterID || r.ClusterName != "c-prod" ||
		r.Namespace != "default" || r.Pod != "web-1" || r.Container != "app" || r.Kind != k8scontract.ExecKindDebug ||
		r.Ended || r.Reason != "" || r.BytesIn != 0 || r.BytesOut != 0 || r.Recorded || r.Truncated {
		t.Fatalf("after Start: %+v", r)
	}
	if before, after := e.dbNow().Add(-time.Second), e.dbNow(); r.Start.Before(before) || r.Start.After(after) {
		t.Fatalf("start_time = %v, want the database clock at Start", r.Start)
	}

	// 收尾：结束时间取数据库时钟，写入原因与流量。
	before := e.dbNow()
	if err := repo.Finish(ctxb(), s.ID, k8scontract.ExecEndClientClose, 120, 3400); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	after := e.dbNow()
	if r = e.execRow(s.ID); !r.Ended || r.End.Before(before) || r.End.After(after) || r.Reason != k8scontract.ExecEndClientClose ||
		r.BytesIn != 120 || r.BytesOut != 3400 {
		t.Fatalf("after Finish: %+v", r)
	}
	// 已结束的会话不被再次收尾：第一次的原因与流量保留。
	if err := repo.Finish(ctxb(), s.ID, k8scontract.ExecEndError, 1, 1); err != nil {
		t.Fatalf("second Finish() error = %v", err)
	}
	if r = e.execRow(s.ID); r.Reason != k8scontract.ExecEndClientClose || r.BytesIn != 120 || r.BytesOut != 3400 {
		t.Fatalf("a finished session was rewritten: %+v", r)
	}
	// 不存在的会话：空操作，不报错（会话记录写入失败不应影响终端本身）。
	if err := repo.Finish(ctxb(), e.id(), k8scontract.ExecEndError, 0, 0); err != nil {
		t.Fatalf("Finish() of an unknown session error = %v", err)
	}
	// 同一 ID 不能登记两次；没有用户 ID 的会话写不进去（空串不是合法 uuid，22P02）。
	if err := repo.Start(ctxb(), s); pgCode(err) != "23505" {
		t.Fatalf("second Start() error = %v, want 23505", err)
	}
	s.ID, s.Identity.UserID = e.id(), ""
	if err := repo.Start(ctxb(), s); pgCode(err) != "22P02" {
		t.Fatalf("Start() without a user error = %v, want 22P02", err)
	}
}

// 终端录制：按块追加，同一序号只认第一次；录制标记与「不完整」标记只升不降；空块只更新标记。
func TestExecRecord(t *testing.T) {
	e := newEnv(t)
	repo := NewAuditRepo(e.d)
	clusterID := e.cluster(clusterSpec{})
	live := e.execSession(execSpec{Cluster: clusterID}) // 进行中
	chunk := func(s string) []byte { return []byte("[0,\"o\",\"" + s + "\"]\n") }

	if err := repo.Record(ctxb(), live, 1, chunk("aGk="), false); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if r := e.execRow(live); !r.Recorded || r.Truncated || e.count("SELECT count(*) FROM k8s_exec_record WHERE session_id = $1", live) != 1 {
		t.Fatalf("after the first chunk: %+v", r)
	}
	// 第二块带上「不完整」标记。
	if err := repo.Record(ctxb(), live, 2, chunk("e29v"), true); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	// 同一序号再写是空操作（ON CONFLICT DO NOTHING）：内容保持第一次的，标记照常更新。
	if err := repo.Record(ctxb(), live, 2, chunk("REPLACED"), false); err != nil {
		t.Fatalf("duplicate Record() error = %v", err)
	}
	var data []byte
	e.scan("SELECT data FROM k8s_exec_record WHERE session_id = $1 AND seq = 2", []any{live}, &data)
	if !bytes.Equal(data, chunk("e29v")) {
		t.Fatalf("seq 2 was overwritten: %q", data)
	}
	// 空块：不写录制行，只把标记落库（recording 因超限停止后的那一次落库）。
	if err := repo.Record(ctxb(), live, 3, nil, true); err != nil {
		t.Fatalf("Record() of an empty chunk error = %v", err)
	}
	if r := e.execRow(live); !r.Recorded || !r.Truncated || e.count("SELECT count(*) FROM k8s_exec_record WHERE session_id = $1", live) != 2 {
		t.Fatalf("after the empty chunk: %+v, rows = %d", r, e.count("SELECT count(*) FROM k8s_exec_record WHERE session_id = $1", live))
	}
	// 从未录到内容的会话收到空块：仍是「没有录制内容」。
	quiet := e.execSession(execSpec{Cluster: clusterID})
	if err := repo.Record(ctxb(), quiet, 1, nil, false); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if r := e.execRow(quiet); r.Recorded || r.Truncated || e.count("SELECT count(*) FROM k8s_exec_record WHERE session_id = $1", quiet) != 0 {
		t.Fatalf("an empty chunk created a record: %+v", r)
	}
	// 二进制内容原样保存（含 0x00）。
	binary := []byte{0x00, 0xff, 0x1f, 0x8b, 0x00}
	if err := repo.Record(ctxb(), live, 4, binary, false); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	e.scan("SELECT data FROM k8s_exec_record WHERE session_id = $1 AND seq = 4", []any{live}, &data)
	if !bytes.Equal(data, binary) {
		t.Fatalf("binary chunk = %x, want %x", data, binary)
	}
	// 边界（现状）：会话行不存在（登记失败或已被保留期清理）时录制块照样落库，没有会话可更新——k8s_exec_record
	// 对会话没有外键，这些行 PurgeExpired 也不会清。钉住现状：改成「会话不存在就不写」时请同步改这条断言。
	orphan := e.id()
	if err := repo.Record(ctxb(), orphan, 1, chunk("aGk="), false); err != nil {
		t.Fatalf("Record() for an unknown session error = %v", err)
	}
	if n := e.count("SELECT count(*) FROM k8s_exec_record WHERE session_id = $1", orphan); n != 1 {
		t.Fatalf("%d record rows for an unknown session, want 1 (current behaviour)", n)
	}
}
