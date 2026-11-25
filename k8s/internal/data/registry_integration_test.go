//go:build integration

package data

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// sameSet 报告两个 ID 列表是否是同一个集合（领取顺序按 cluster_id，这里不关心顺序）。
func sameSet(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}

// runtimeRow 一条集群运行状态（k8s_cluster_runtime）的断言视图；可空列用指针。
type runtimeRow struct {
	Owner                                string
	LeaseEnds, AgentConnected, AgentSeen *time.Time
	Probed, SummaryAt                    *time.Time
	Status                               int32
	Since                                time.Time
	K8sVersion, AgentVersion, LastError  string
	Nodes, Ready                         int
	ScopeVersion                         int64
	ScopeError, Summary                  string
}

func (e *env) runtime(clusterID string) runtimeRow {
	e.t.Helper()
	var r runtimeRow
	e.scan(`SELECT owner_instance, lease_until, status, status_since, k8s_version, node_total, node_ready, agent_version,
		agent_connect_time, agent_last_seen, last_error, probe_time, scope_version, scope_error, summary::text, summary_time
		FROM k8s_cluster_runtime WHERE cluster_id = $1`, []any{clusterID},
		&r.Owner, &r.LeaseEnds, &r.Status, &r.Since, &r.K8sVersion, &r.Nodes, &r.Ready, &r.AgentVersion,
		&r.AgentConnected, &r.AgentSeen, &r.LastError, &r.Probed, &r.ScopeVersion, &r.ScopeError, &r.Summary, &r.SummaryAt)
	return r
}

// 实例登记：心跳是 upsert（首次登记、之后只刷新心跳 / 地址 / 集群数）；退出时删除登记。
func TestInstanceHeartbeat(t *testing.T) {
	e := newEnv(t)
	repo := NewRegistryRepo(e.d)
	ctx := ctxb()
	id := "plane-" + e.id()
	type row struct {
		addr        string
		clusters    int
		start, beat time.Time
	}
	read := func() row {
		var r row
		e.scan("SELECT addr, clusters, start_time, heartbeat_time FROM k8s_instance WHERE id = $1", []any{id},
			&r.addr, &r.clusters, &r.start, &r.beat)
		return r
	}
	if err := repo.Heartbeat(ctx, id, "http://a:8030", 0); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	r := read()
	if r.addr != "http://a:8030" || r.clusters != 0 || !r.start.Equal(r.beat) || r.beat.Before(e.dbNow().Add(-time.Minute)) {
		t.Fatalf("after the first heartbeat: %+v", r)
	}
	// 心跳过时后再写：只刷新心跳、地址与集群数，启动时间不变。
	e.exec(`UPDATE k8s_instance SET heartbeat_time = now() - interval '1 hour' WHERE id = $1`, id)
	start := read().start
	if err := repo.Heartbeat(ctx, id, "http://b:8030", 3); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	if r = read(); r.addr != "http://b:8030" || r.clusters != 3 || !r.start.Equal(start) || r.beat.Before(e.dbNow().Add(-time.Minute)) {
		t.Fatalf("after the next heartbeat: %+v (start was %v)", r, start)
	}
	// 退出：删除登记；重复删除不报错。
	for range 2 {
		if err := repo.Remove(ctx, id); err != nil {
			t.Fatalf("Remove() error = %v", err)
		}
	}
	if e.count("SELECT count(*) FROM k8s_instance") != 0 {
		t.Fatal("Remove() must delete the instance row")
	}
	// 边界：实例 ID 超过列宽（varchar(64)）时被数据库拒绝，不截断——实例 ID 由数据面自己生成。
	if err := repo.Heartbeat(ctx, strings.Repeat("x", 65), "http://a:8030", 0); pgCode(err) != "22001" {
		t.Fatalf("Heartbeat() with an overlong ID error = %v, want 22001", err)
	}
}

// 集群归属：领取 / 续租 / 放弃，公平份额与让出超额，停用 / 删除的集群不再持有。
func TestClusterOwnership(t *testing.T) {
	e := newEnv(t)
	repo := NewRegistryRepo(e.d)
	ctx := ctxb()
	const lease = time.Minute
	a, b, c := e.cluster(clusterSpec{}), e.cluster(clusterSpec{}), e.cluster(clusterSpec{})
	agent := e.cluster(clusterSpec{Mode: k8scontract.AccessAgent})
	e.cluster(clusterSpec{Disabled: true})
	e.cluster(clusterSpec{Deleted: true})
	direct := []string{a, b, c}
	held := func(instance string) []string { // 该实例此刻持有的集群（按 cluster_id）
		t.Helper()
		rows, err := e.admin.Query(ctxb(), "SELECT cluster_id::text FROM k8s_cluster_runtime WHERE owner_instance = $1 ORDER BY cluster_id", instance)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out = append(out, id)
		}
		return out
	}
	// claimed 领取并断言：不报错、新领到 n 个、库里登记的持有集合与「领取前 + 新领到」一致。
	claimed := func(instance string, limit, n int) []string {
		t.Helper()
		before := held(instance)
		got, err := repo.Claim(ctx, instance, lease, limit)
		if err != nil || len(got) != n || !sameSet(held(instance), append(before, got...)) {
			t.Fatalf("Claim(%s, %d) = %v, %v; want %d new clusters (held before: %v, held now: %v)",
				instance, limit, got, err, n, before, held(instance))
		}
		return got
	}
	e.instance("i1", "http://i1:8030", 0, 0)

	// 首次领取：缺的运行行补齐，领走全部可领的直连集群；Agent / 停用 / 已删除的集群不领。
	claimed("i1", 10, 3)
	for _, id := range direct {
		if r := e.runtime(id); r.Owner != "i1" || r.LeaseEnds == nil || r.LeaseEnds.Before(e.dbNow().Add(50*time.Second)) {
			t.Fatalf("runtime of %s after Claim: %+v", id, r)
		}
	}
	if r := e.runtime(agent); r.Owner != "" || r.LeaseEnds != nil {
		t.Fatalf("an Agent cluster must not be claimed: %+v", r)
	}
	if e.count("SELECT count(*) FROM k8s_cluster_runtime") != 4 {
		t.Fatal("runtime rows must exist for the enabled clusters only")
	}
	// 份额已满：再领取不到。
	claimed("i1", 10, 0)
	// 续租：返回仍持有的集群，租约前移。
	e.exec(`UPDATE k8s_cluster_runtime SET lease_until = lease_until - interval '30 seconds' WHERE owner_instance = 'i1'`)
	got, err := repo.Renew(ctx, "i1", lease)
	if err != nil || !sameSet(got, direct) {
		t.Fatalf("Renew(i1) = %v, %v; want %v", got, err, direct)
	}
	for _, id := range direct {
		if r := e.runtime(id); r.Owner != "i1" || r.LeaseEnds == nil || r.LeaseEnds.Before(e.dbNow().Add(50*time.Second)) {
			t.Fatalf("runtime of %s after Renew: %+v", id, r)
		}
	}
	// 另一个实例：租约有效期内领不到。
	e.instance("i2", "http://i2:8030", 0, 0)
	claimed("i2", 10, 0)
	// i1 的租约过期但实例仍存活：i2 只领到自己的公平份额（ceil(3 ÷ 2) = 2），第三个留给 i1。
	e.exec(`UPDATE k8s_cluster_runtime SET lease_until = now() - interval '1 second' WHERE owner_instance = 'i1'`)
	claimed("i2", 10, 2)
	// i1 已无心跳（下线）：份额 = 3，i2 把最后一个也接管；i1 的续租不再返回任何集群。
	e.exec(`UPDATE k8s_instance SET heartbeat_time = now() - interval '1 hour' WHERE id = 'i1'`)
	claimed("i2", 10, 1)
	if got, err = repo.Renew(ctx, "i1", lease); err != nil || len(got) != 0 || e.runtime(a).Owner != "i2" {
		t.Fatalf("Renew(i1) after takeover = %v, %v; owner of a = %q", got, err, e.runtime(a).Owner)
	}
	// Release：只放开自己持有的；重复执行不报错。
	for range 2 {
		if err := repo.Release(ctx, "i2"); err != nil {
			t.Fatalf("Release() error = %v", err)
		}
	}
	if e.count("SELECT count(*) FROM k8s_cluster_runtime WHERE owner_instance <> '' OR lease_until IS NOT NULL") != 0 {
		t.Fatal("Release() must give up every cluster")
	}
	// i1 回到线上。limit：一次至多领 limit 个。
	e.exec(`UPDATE k8s_instance SET heartbeat_time = now() WHERE id = 'i1'`)
	claimed("i1", 1, 1)
	// 公平份额：i2 存活且空闲 → 份额 = ceil(3 ÷ 2) = 2，i1 再领 1 个即满。
	claimed("i1", 10, 1)
	claimed("i1", 10, 0)
	// i2 没有心跳：份额 = 3，i1 把最后一个也领走。
	e.exec(`UPDATE k8s_instance SET heartbeat_time = now() - interval '1 hour' WHERE id = 'i2'`)
	claimed("i1", 10, 1)
	// i2 回来且空闲：i1 续租时让出超过份额的那一个（每轮至多 2 个），i2 随即领走。
	e.exec(`UPDATE k8s_instance SET heartbeat_time = now() WHERE id = 'i2'`)
	if got, err = repo.Renew(ctx, "i1", lease); err != nil || len(got) != 2 {
		t.Fatalf("Renew(i1) with an idle peer = %v, %v; want 2 clusters kept", got, err)
	}
	var shed []string
	for _, id := range direct {
		if !slices.Contains(got, id) {
			shed = append(shed, id)
		}
	}
	if len(shed) != 1 {
		t.Fatalf("i1 gave up %v, want exactly one cluster", shed)
	}
	if r := e.runtime(shed[0]); r.Owner != "" || r.LeaseEnds != nil {
		t.Fatalf("the shed cluster must be unowned: %+v", r)
	}
	claimed("i2", 10, 1)
	// 已停用 / 已删除的集群：续租时放弃归属，也不会再被任何实例领取。
	e.exec(`UPDATE k8s_cluster SET enabled = false WHERE id = $1`, a)
	e.exec(`UPDATE k8s_cluster SET delete_time = now() WHERE id = $1`, b)
	for _, id := range []string{a, b} {
		if got, err = repo.Renew(ctx, e.runtime(id).Owner, lease); err != nil || slices.Contains(got, id) {
			t.Fatalf("Renew() of the owner of %s = %v, %v; it must give up the cluster", id, got, err)
		}
		if r := e.runtime(id); r.Owner != "" || r.LeaseEnds != nil {
			t.Fatalf("disabled / deleted cluster %s must be unowned: %+v", id, r)
		}
	}
	claimed("i2", 10, 0)
}

// 配置表只读访问：集群清单与列映射、凭据密文、配置版本、角色数据范围。
func TestClusterRepo(t *testing.T) {
	e := newEnv(t)
	repo := NewClusterRepo(e.d)
	ctx := ctxb()
	const plaintext = `{"token":"PRIVATE-TOKEN"}`
	sealed := seal(t, plaintext)
	plain := e.cluster(clusterSpec{APIServer: "https://k8s.example.com:6443", ProxyURL: "http://proxy:3128"})
	secured := e.cluster(clusterSpec{Credential: sealed, ExecDisabled: true})
	agent := e.cluster(clusterSpec{Mode: k8scontract.AccessAgent})
	e.cluster(clusterSpec{Disabled: true})
	e.cluster(clusterSpec{Deleted: true})
	e.exec(`UPDATE k8s_cluster SET display_name = '生产集群', env = 'prod', version = 7 WHERE id = $1`, plain)

	all, err := repo.ListEnabled(ctx)
	if err != nil || len(all) != 3 {
		t.Fatalf("ListEnabled() = %d clusters, %v; want 3", len(all), err)
	}
	byID := map[string]*biz.Cluster{}
	for _, c := range all {
		byID[c.ID] = c
	}
	if c := byID[plain]; c == nil || c.Name == "" || c.DisplayName != "生产集群" || c.Env != "prod" || c.AccessMode != k8scontract.AccessDirect ||
		c.APIServer != "https://k8s.example.com:6443" || c.ProxyURL != "http://proxy:3128" || c.CACert != "" || c.Credential != nil ||
		c.ExecDisabled || c.Version != 7 || c.UpdateTime.After(e.dbNow()) {
		t.Fatalf("direct cluster = %+v", c)
	}
	if c := byID[secured]; c == nil || !bytes.Equal(c.Credential, sealed) || !c.ExecDisabled || c.ProxyURL != "" {
		t.Fatalf("secured cluster = %+v", c)
	}
	if c := byID[agent]; c == nil || c.AccessMode != k8scontract.AccessAgent || c.Credential != nil {
		t.Fatalf("agent cluster = %+v", c)
	}

	// Credential：密文与保存的连接地址；密文经 SecretBox 解出明文（与 core 写路径同一实现）。
	cred, ep, err := repo.Credential(ctx, secured)
	if err != nil || !bytes.Equal(cred, sealed) || ep.APIServer == "" || ep.ProxyURL != "" || ep.CACert != "" {
		t.Fatalf("Credential() = %d bytes, %+v, %v", len(cred), ep, err)
	}
	if got, err := secretBox(t).Open(cred); err != nil || got != plaintext {
		t.Fatalf("Open() = %q, %v", got, err)
	}
	// 没有保存凭据的集群：nil；软删除的集群与不存在的集群：同样 (nil, nil)。
	for _, id := range []string{plain, agent, e.id()} {
		if cred, _, err = repo.Credential(ctx, id); err != nil || cred != nil {
			t.Fatalf("Credential(%s) = %d bytes, %v; want none", id, len(cred), err)
		}
	}

	// Versions：配置版本表的全部行。
	v, err := repo.Versions(ctx)
	if err != nil || len(v) != 3 || v[k8scontract.CfgCluster] != 1 || v[k8scontract.CfgRoleScope] != 1 || v[k8scontract.CfgAgentToken] != 1 {
		t.Fatalf("Versions() = %v, %v", v, err)
	}
	e.exec(`UPDATE k8s_cfg_version SET version = version + 5 WHERE name = $1`, k8scontract.CfgRoleScope)
	if v, err = repo.Versions(ctx); err != nil || v[k8scontract.CfgRoleScope] != 6 {
		t.Fatalf("Versions() after a bump = %v, %v", v, err)
	}

	// Scopes：某集群的全部角色数据范围；没有授权的集群返回空。
	admin, dev := e.scope(plain, "*", k8scontract.LevelClusterAdmin), e.scope(plain, "default", k8scontract.LevelEdit)
	scopes, err := repo.Scopes(ctx, plain)
	if err != nil || len(scopes) != 2 {
		t.Fatalf("Scopes() = %+v, %v", scopes, err)
	}
	for _, want := range []biz.Scope{{RoleID: admin, Namespace: "*", Level: k8scontract.LevelClusterAdmin}, {RoleID: dev, Namespace: "default", Level: k8scontract.LevelEdit}} {
		if !slices.Contains(scopes, want) {
			t.Fatalf("Scopes() = %+v, missing %+v", scopes, want)
		}
	}
	if scopes, err = repo.Scopes(ctx, secured); err != nil || len(scopes) != 0 {
		t.Fatalf("Scopes() of a cluster without grants = %+v, %v", scopes, err)
	}
}

// Helm 仓库（含凭据密文）：只读未删除的仓库；不存在返回 404 业务错误。
func TestHelmRepo(t *testing.T) {
	e := newEnv(t)
	repo := NewClusterRepo(e.d)
	ctx := ctxb()
	sealed := seal(t, "helm-secret")
	id, gone := e.id(), e.id()
	e.exec(`INSERT INTO k8s_helm_repo (id, name, type, url, username, secret, ca_cert, insecure_skip_verify, enabled, version)
		VALUES ($1, 'bitnami', $2, 'oci://registry.example.com/bitnami', 'ops', $3, 'CA', true, true, 4)`,
		id, k8scontract.HelmRepoOCI, sealed)
	e.exec(`INSERT INTO k8s_helm_repo (id, name, type, url, delete_time) VALUES ($1, 'old', $2, 'https://charts.example.com', now())`,
		gone, k8scontract.HelmRepoHTTP)

	h, err := repo.HelmRepo(ctx, id)
	if err != nil || h.ID != id || h.Name != "bitnami" || h.Type != k8scontract.HelmRepoOCI || h.URL != "oci://registry.example.com/bitnami" ||
		h.Username != "ops" || !bytes.Equal(h.Secret, sealed) || h.CACert != "CA" || !h.Insecure || !h.Enabled || h.Version != 4 {
		t.Fatalf("HelmRepo() = %+v, %v", h, err)
	}
	if got, err := secretBox(t).Open(h.Secret); err != nil || got != "helm-secret" {
		t.Fatalf("Open() = %q, %v", got, err)
	}
	for _, missing := range []string{e.id(), gone} {
		var be *biz.Error
		if _, err = repo.HelmRepo(ctx, missing); !errors.As(err, &be) || be.Code != 404 {
			t.Fatalf("HelmRepo(%s) error = %v, want 404", missing, err)
		}
	}
}

// 集群运行状态与通知发件箱：探活结果以归属围栏、状态起始时间、失败保留上次读数、版本号截断；
// 授权同步回写；发件箱按 dedup_key 幂等。
func TestRuntime(t *testing.T) {
	e := newEnv(t)
	repo := NewRuntimeRepo(e.d)
	ctx := ctxb()
	c := e.cluster(clusterSpec{})
	if _, err := NewRegistryRepo(e.d).Claim(ctx, "i1", time.Minute, 10); err != nil {
		t.Fatal(err)
	}
	idle := e.cluster(clusterSpec{}) // 领取之后才登记：没有运行行
	online := biz.ProbeRecord{ClusterID: c, Status: k8scontract.ClusterOnline, Info: biz.ProbeInfo{Version: "v1.32.1", NodeTotal: 3, NodeReady: 2}}

	// 首次探活成功：之前是「未知」；进入在线状态的时刻取数据库时钟。
	before := e.dbNow()
	prev, since, err := repo.SaveProbe(ctx, "i1", online)
	if err != nil || prev != k8scontract.ClusterUnknown || since.Before(before) || since.After(e.dbNow()) {
		t.Fatalf("first SaveProbe() = prev %d, since %v, %v", prev, since, err)
	}
	if r := e.runtime(c); r.Status != k8scontract.ClusterOnline || r.K8sVersion != "v1.32.1" || r.Nodes != 3 || r.Ready != 2 ||
		r.LastError != "" || r.Probed == nil || !since.Equal(r.Since) {
		t.Fatalf("runtime after the first probe: %+v", r)
	}
	// 状态没变：起始时间不重置，返回写入前的状态。
	if p, s, err := repo.SaveProbe(ctx, "i1", online); err != nil || p != k8scontract.ClusterOnline || !s.Equal(since) {
		t.Fatalf("steady SaveProbe() = prev %d, since %v, %v; since must stay %v", p, s, err, since)
	}
	// 探活失败：状态翻为不可达并记下原因；上一次成功读到的版本与节点数保留。
	before = e.dbNow()
	if prev, since, err = repo.SaveProbe(ctx, "i1", biz.ProbeRecord{ClusterID: c, Status: k8scontract.ClusterUnreachable, Error: "dial tcp: i/o timeout"}); err != nil ||
		prev != k8scontract.ClusterOnline || since.Before(before) {
		t.Fatalf("failed SaveProbe() = prev %d, since %v, %v", prev, since, err)
	}
	if r := e.runtime(c); r.Status != k8scontract.ClusterUnreachable || !r.Since.Equal(since) || r.K8sVersion != "v1.32.1" || r.Nodes != 3 ||
		r.Ready != 2 || r.LastError != "dial tcp: i/o timeout" {
		t.Fatalf("runtime after a failed probe: %+v", r)
	}
	// 版本号超过列宽（varchar(32)）：截断后照常写入，整条探活结果不因 22001 丢失；成功的一轮清掉错误原因。
	long := "v1.35.1-eks-" + strings.Repeat("a", 40)
	if _, _, err = repo.SaveProbe(ctx, "i1", biz.ProbeRecord{
		ClusterID: c, Status: k8scontract.ClusterOnline,
		Info: biz.ProbeInfo{Version: long, NodeTotal: 5, NodeReady: 5},
	}); err != nil {
		t.Fatalf("SaveProbe() with an overlong version error = %v", err)
	}
	if r := e.runtime(c); r.K8sVersion != long[:32] || r.Nodes != 5 || r.Ready != 5 || r.LastError != "" {
		t.Fatalf("runtime after an overlong version: %+v", r)
	}
	// 围栏：归属已转移的实例写不进去（结果丢弃，errNotOwner）；没有运行行的集群同理。
	e.exec(`UPDATE k8s_cluster_runtime SET owner_instance = 'i2' WHERE cluster_id = $1`, c)
	for _, id := range []string{c, idle} {
		if _, _, err = repo.SaveProbe(ctx, "i1", biz.ProbeRecord{ClusterID: id, Status: k8scontract.ClusterUnreachable}); !errors.Is(err, errNotOwner) {
			t.Fatalf("SaveProbe(%s) by a non-owner error = %v, want errNotOwner", id, err)
		}
	}
	if r := e.runtime(c); r.Status != k8scontract.ClusterOnline {
		t.Fatalf("a non-owner changed the runtime row: %+v", r)
	}

	// SaveScope：成功时前移已同步版本并清掉错误；失败（version = 0）只更新错误；以归属围栏。
	save := func(instance, id string, version int64, msg string) {
		t.Helper()
		if err := repo.SaveScope(ctx, instance, id, version, msg); err != nil {
			t.Fatalf("SaveScope() error = %v", err)
		}
	}
	save("i2", c, 5, "")
	save("i2", c, 0, "forbidden")
	if r := e.runtime(c); r.ScopeVersion != 5 || r.ScopeError != "forbidden" {
		t.Fatalf("scope after a failed sync: %+v", r)
	}
	save("i2", c, 6, "")
	save("i1", c, 99, "x")
	save("i1", idle, 1, "")
	// 没有运行行的集群：围栏的写不报错，也不会凭空建出运行行。
	if r := e.runtime(c); r.ScopeVersion != 6 || r.ScopeError != "" || e.count("SELECT count(*) FROM k8s_cluster_runtime WHERE cluster_id = $1", idle) != 0 {
		t.Fatalf("scope after a successful sync and fenced writes: %+v", r)
	}

	// Enqueue：写发件箱（payload 落 jsonb）；dedup_key 已存在时为空操作。
	ev := biz.OutboxEvent{
		ClusterID: c, EventType: k8scontract.NotifyClusterUnreachable, Severity: 3,
		DedupKey: "k8s:it:unreachable:1", GroupKey: "k8s:cluster:" + c, Payload: map[string]any{"cluster_id": c, "recovering": false},
	}
	for range 2 {
		if err := repo.Enqueue(ctx, ev); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}
	var (
		eventType, group, payload  string
		severity, status, attempts int
		fresh                      bool
	)
	e.scan(`SELECT event_type, severity, group_key, payload::text, status, attempts, create_time > now() - interval '1 minute'
		FROM k8s_notify_outbox WHERE dedup_key = $1`, []any{ev.DedupKey}, &eventType, &severity, &group, &payload, &status, &attempts, &fresh)
	if e.count("SELECT count(*) FROM k8s_notify_outbox") != 1 || eventType != ev.EventType || severity != 3 || group != ev.GroupKey ||
		status != 1 || attempts != 0 || !fresh || !jsonEqual(t, []byte(payload), `{"cluster_id":"`+c+`","recovering":false}`) {
		t.Fatalf("outbox row = %s %d %s %s %d %d %v", eventType, severity, group, payload, status, attempts, fresh)
	}
	// 边界：级别超出取值范围（CHECK 1-4）时整行不落库。
	if err := repo.Enqueue(ctx, biz.OutboxEvent{ClusterID: c, EventType: "k8s.it", Severity: 9, DedupKey: e.id()}); pgCode(err) != "23514" {
		t.Fatalf("Enqueue() with an out-of-range severity error = %v, want 23514", err)
	}
	// 边界（现状）：runtimeRepo.Enqueue 不截断 dedup_key / group_key（列宽 255），超长时 22001——
	// 它的调用方（探活 / Agent 离线 / 巡检）用的键都很短；insightRepo.ClaimNotify 写同一张表时会截断。
	if err := repo.Enqueue(ctx, biz.OutboxEvent{ClusterID: c, EventType: "k8s.it", DedupKey: strings.Repeat("k", 256)}); pgCode(err) != "22001" {
		t.Fatalf("Enqueue() with an overlong dedup key error = %v, want 22001", err)
	}
}

// 健康检查与选主锁。
func TestLockerAndPing(t *testing.T) {
	e := newEnv(t)
	if err := e.d.Ping(); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	l := NewLocker(e.d)
	release, ok, err := l.TryLock(ctxb(), "k8s:it-lock")
	if err != nil || !ok {
		t.Fatalf("first lock: ok = %v, err = %v", ok, err)
	}
	// 同一把锁：另一条连接拿不到（会话级 advisory lock）。
	if _, ok, err = l.TryLock(ctxb(), "k8s:it-lock"); err != nil || ok {
		t.Fatalf("second lock must fail: ok = %v, err = %v", ok, err)
	}
	release()
	release2, ok, err := l.TryLock(ctxb(), "k8s:it-lock")
	if err != nil || !ok {
		t.Fatalf("lock after release: ok = %v, err = %v", ok, err)
	}
	release2()
}

// 数据面角色只能做迁移授权的事：越权语句被数据库拒绝（42501），授权范围内的语句可用。
// 这条测试同时确认夹具确实跑在受限角色上。
func TestPlaneRoleIsRestricted(t *testing.T) {
	e := newEnv(t)
	clusterID, user := e.cluster(clusterSpec{}), e.id()
	tokenID := e.token(clusterID, k8scontract.TokenLongTerm, "s", nil)
	sessionID := e.execSession(execSpec{Cluster: clusterID})
	e.exec(`INSERT INTO k8s_change (id, kind, cluster_id, action, requester_id, status) VALUES ($1, 1, $2, 'update', $3, 1)`,
		e.id(), clusterID, user)
	for name, stmt := range map[string]string{
		"insert cluster":        `INSERT INTO k8s_cluster (id, name, access_mode) VALUES ('` + e.id() + `', 'x', 2)`,
		"update cluster":        `UPDATE k8s_cluster SET enabled = false`,
		"update helm repo":      `UPDATE k8s_helm_repo SET enabled = false`,
		"insert role scope":     `INSERT INTO k8s_role_scope (role_id, cluster_id, namespace, level) VALUES ('` + e.id() + `', '` + clusterID + `', 'default', 1)`,
		"delete role scope":     `DELETE FROM k8s_role_scope`,
		"bump cfg version":      `UPDATE k8s_cfg_version SET version = version + 1`,
		"update agent token":    `UPDATE k8s_agent_token SET expire_time = NULL WHERE id = '` + tokenID + `'`,
		"read kube token":       `SELECT count(*) FROM k8s_kube_token`,
		"read exec ticket":      `SELECT count(*) FROM k8s_exec_ticket`,
		"read platform setting": `SELECT count(*) FROM k8s_setting`,
		"truncate exec session": `TRUNCATE k8s_exec_session`,
		"change beyond grant":   `UPDATE k8s_change SET summary = 'x'`,
		"insert change":         `INSERT INTO k8s_change (id, kind, cluster_id, action, requester_id) VALUES ('` + e.id() + `', 1, '` + clusterID + `', 'update', '` + user + `')`,
		"delete change":         `DELETE FROM k8s_change`,
		"update outbox status":  `UPDATE k8s_notify_outbox SET status = 2`,
		"drop runtime table":    `DROP TABLE k8s_cluster_runtime`,
		"read another domain":   `SELECT count(*) FROM "system_user"`,
		"create role":           `CREATE ROLE k8s_plane_it_intruder NOLOGIN`,
	} {
		if _, err := e.d.Pool.Exec(ctxb(), stmt); pgCode(err) != "42501" {
			t.Errorf("%s: want permission denied (42501), got %v", name, err)
		}
	}
	// 授权范围内的语句可用（其余的由本文件的其它用例在同一角色下覆盖）。
	for name, stmt := range map[string]string{
		"read cluster":           `SELECT count(*) FROM k8s_cluster`,
		"read helm repo":         `SELECT count(*) FROM k8s_helm_repo`,
		"read role scope":        `SELECT count(*) FROM k8s_role_scope`,
		"read cfg version":       `SELECT count(*) FROM k8s_cfg_version`,
		"delete agent token":     `DELETE FROM k8s_agent_token WHERE id = '` + tokenID + `'`,
		"insert agent token":     `INSERT INTO k8s_agent_token (id, cluster_id, kind, token_hash) VALUES ('` + e.id() + `', '` + clusterID + `', 2, '\x00')`,
		"insert audit":           `INSERT INTO k8s_audit (id, action, result) VALUES ('` + e.id() + `', 'update', 1)`,
		"delete exec session":    `DELETE FROM k8s_exec_session WHERE id = '` + sessionID + `'`,
		"insert runtime row":     `INSERT INTO k8s_cluster_runtime (cluster_id) VALUES ('` + clusterID + `') ON CONFLICT DO NOTHING`,
		"update runtime":         `UPDATE k8s_cluster_runtime SET last_error = ''`,
		"delete instance":        `DELETE FROM k8s_instance`,
		"insert outbox":          `INSERT INTO k8s_notify_outbox (id, cluster_id, event_type, dedup_key) VALUES ('` + e.id() + `', '` + clusterID + `', 'k8s.it', '` + e.id() + `')`,
		"delete outbox":          `DELETE FROM k8s_notify_outbox`,
		"change within grant":    `UPDATE k8s_change SET status = 5, message = 'x', attempts = 1, lease_owner = '', lease_until = NULL, result = '{}'`,
		"insert inspection run":  `INSERT INTO k8s_inspection_run (id, cluster_id, trigger, status) VALUES ('` + e.id() + `', '` + clusterID + `', 1, 1)`,
		"delete inspection run":  `DELETE FROM k8s_inspection_run`,
		"insert event":           `INSERT INTO k8s_event (id, cluster_id, first_time, last_time) VALUES ('` + e.id() + `', '` + clusterID + `', now(), now())`,
		"delete event":           `DELETE FROM k8s_event`,
		"create table in public": `CREATE TABLE IF NOT EXISTS k8s_plane_it_scratch (id int)`,
	} {
		if _, err := e.d.Pool.Exec(ctxb(), stmt); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	// 现状（钉住，不代表期望）：数据面角色是 k8s_audit 及其分区的属主（迁移 0072 为了分区维护把属主改成它），
	// 属主权限盖过了迁移里「只授 SELECT / INSERT」的本意——改、删、清空审计它都能执行，审计「只增不改」
	// 目前只靠代码约束。放在回滚的事务里验证，不动数据；哪天收紧了属主权限，这里会失败，届时改成断言 42501。
	tx, err := e.d.Pool.Begin(ctxb())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctxb()) }()
	for name, stmt := range map[string]string{
		"update audit":   `UPDATE k8s_audit SET username = 'x'`,
		"delete audit":   `DELETE FROM k8s_audit`,
		"truncate audit": `TRUNCATE k8s_audit`,
		"alter audit":    `ALTER TABLE k8s_audit ADD COLUMN IF NOT EXISTS it_probe int`,
	} {
		if _, err := tx.Exec(ctxb(), stmt); err != nil {
			t.Errorf("%s: the data plane role owns k8s_audit and can run this today, got %v", name, err)
		}
	}
	if err := tx.Rollback(ctxb()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.try(`DROP TABLE IF EXISTS k8s_plane_it_scratch`); err != nil {
			t.Errorf("drop scratch table: %v", err)
		}
	})
	e.exec(`DROP TABLE IF EXISTS k8s_plane_it_scratch`)
}
