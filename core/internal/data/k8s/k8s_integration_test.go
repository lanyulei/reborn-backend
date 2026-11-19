//go:build integration

package data

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/kitconf"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	uAlice = "018f3e1a-0f00-7000-8000-00000000000a"
	roleA  = "018f3e1a-0f00-7000-8000-0000000000a1"
	roleB  = "018f3e1a-0f00-7000-8000-0000000000b1"
)

type env struct {
	t        *testing.T
	pool     *pgxpool.Pool
	data     *basedata.Data
	clusters *clusterRepo
	scopes   *scopeRepo
	audits   *auditRepo
	tickets  *ticketRepo
	outbox   *outboxRepo
	resolver *instanceResolver
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	}
	if db := cfg.ConnConfig.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"", db)
	}
	d, cleanup, err := basedata.NewData(&conf.Data{Database: &kitconf.Database{Source: dsn}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	if _, err := d.Pool.Exec(context.Background(),
		`TRUNCATE TABLE k8s_notify_outbox, k8s_exec_ticket, k8s_exec_session, k8s_audit, k8s_cluster_runtime, k8s_instance,
		 k8s_role_scope, k8s_agent_token, k8s_cluster CASCADE`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	return &env{
		t: t, pool: d.Pool, data: d, clusters: NewClusterRepo(d), scopes: NewScopeRepo(d), audits: NewAuditRepo(d),
		tickets: NewTicketRepo(d), outbox: NewOutboxRepo(d), resolver: NewInstanceResolver(d),
	}
}

func (e *env) cfg(name string) int64 {
	e.t.Helper()
	var v int64
	if err := e.pool.QueryRow(context.Background(), "SELECT version FROM k8s_cfg_version WHERE name = $1", name).Scan(&v); err != nil {
		e.t.Fatalf("read cfg version: %v", err)
	}
	return v
}

func (e *env) create(name string, mode int32, credential []byte) *biz.Cluster {
	e.t.Helper()
	id, _ := idgen.NewID()
	c := &biz.Cluster{ID: id, Name: name, Env: "prod", AccessMode: mode, Enabled: true}
	if mode == kc.AccessAgent {
		c.AgentLocation = kc.AgentLocationInCluster
	} else {
		c.APIServer = "https://10.0.0.1:6443"
	}
	if err := e.clusters.Create(context.Background(), c, credential, entry(kc.AuditResourceCluster, kc.ActionCreate)); err != nil {
		e.t.Fatalf("create cluster %s: %v", name, err)
	}
	return c
}

func entry(resource, action string) *biz.AuditEntry {
	return &biz.AuditEntry{
		UserID: uAlice, Username: "alice", ClientIP: "10.1.1.1", RequestID: "r1", Resource: resource, Action: action,
		Detail: json.RawMessage(`{"k":"v"}`),
	}
}

func TestClusterRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	v0 := e.cfg(kc.CfgCluster)
	direct := e.create("dev-1", kc.AccessDirect, []byte("sealed-bytes"))
	agent := e.create("prod-1", kc.AccessAgent, nil)
	if e.cfg(kc.CfgCluster) != v0+2 {
		t.Fatalf("cfg version not bumped per write")
	}
	dup := &biz.Cluster{ID: agent.ID[:35] + "f", Name: "prod-1", AccessMode: kc.AccessAgent, AgentLocation: 1}
	if err := e.clusters.Create(ctx, dup, nil, nil); !kerrors.Is(err, biz.ErrClusterNameExists) {
		t.Fatalf("duplicate name: %v", err)
	}
	got, err := e.clusters.Get(ctx, direct.ID)
	if err != nil || !got.HasCredential || got.Version != 1 || got.Runtime.Status != kc.ClusterUnknown || got.AgentTokens != 0 {
		t.Fatalf("get = %+v, %v", got, err)
	}
	// 运行状态行随创建写入；数据范围版本未同步时 ScopeSynced = false。
	if got.Runtime.ScopeSynced {
		t.Fatal("scope must not be reported synced before the plane syncs it")
	}
	if _, err := e.pool.Exec(ctx, "UPDATE k8s_cluster_runtime SET status = 1, k8s_version = 'v1.35.1', scope_version = $2 WHERE cluster_id = $1",
		direct.ID, e.cfg(kc.CfgRoleScope)); err != nil {
		t.Fatal(err)
	}
	online := kc.ClusterOnline
	items, total, err := e.clusters.List(ctx, biz.ClusterFilter{Status: &online, Keyword: "dev"}, 1, 10)
	if err != nil || total != 1 || len(items) != 1 || !items[0].Runtime.ScopeSynced || items[0].Runtime.K8sVersion != "v1.35.1" {
		t.Fatalf("list = %+v total %d, %v", items, total, err)
	}
	if _, total, _ = e.clusters.List(ctx, biz.ClusterFilter{}, 1, 10); total != 2 {
		t.Fatalf("list all total = %d", total)
	}

	// 更新：乐观锁；不传凭据沿用；名称不变。
	got.DisplayName, got.Name = "开发", "renamed"
	if err := e.clusters.Update(ctx, got, nil, 9, nil); !kerrors.Is(err, biz.ErrVersionConflict) {
		t.Fatalf("stale version: %v", err)
	}
	if err := e.clusters.Update(ctx, got, nil, 1, entry(kc.AuditResourceCluster, kc.ActionUpdate)); err != nil {
		t.Fatalf("update: %v", err)
	}
	after, _ := e.clusters.Get(ctx, direct.ID)
	if after.Name != "dev-1" || after.DisplayName != "开发" || after.Version != 2 || !after.HasCredential {
		t.Fatalf("after update = %+v", after)
	}
	var stored []byte
	_ = e.pool.QueryRow(ctx, "SELECT credential FROM k8s_cluster WHERE id = $1", direct.ID).Scan(&stored)
	if string(stored) != "sealed-bytes" {
		t.Fatalf("credential changed: %q", stored)
	}

	// 令牌：重复签发只保留一个注册令牌；吊销清空。
	for i := range 2 {
		tid, _ := idgen.NewID()
		if err := e.clusters.ReplaceRegister(ctx, agent.ID, tid, []byte{byte(i)}, time.Now().Add(time.Hour), entry(kc.AuditResourceAgentToken, kc.ActionAgentInstall)); err != nil {
			t.Fatalf("replace register: %v", err)
		}
	}
	tid, _ := idgen.NewID()
	if _, err := e.pool.Exec(ctx, "INSERT INTO k8s_agent_token (id, cluster_id, kind, token_hash) VALUES ($1, $2, 2, 'x')", tid, agent.ID); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = e.pool.QueryRow(ctx, "SELECT count(*) FROM k8s_agent_token WHERE cluster_id = $1", agent.ID).Scan(&n)
	if a, _ := e.clusters.Get(ctx, agent.ID); n != 2 || a.AgentTokens != 1 {
		t.Fatalf("tokens = %d, long-term = %d", n, a.AgentTokens)
	}
	if err := e.clusters.RevokeAll(ctx, agent.ID, entry(kc.AuditResourceAgentToken, kc.ActionAgentRevoke)); err != nil {
		t.Fatal(err)
	}
	_ = e.pool.QueryRow(ctx, "SELECT count(*) FROM k8s_agent_token WHERE cluster_id = $1", agent.ID).Scan(&n)
	if n != 0 {
		t.Fatalf("tokens after revoke = %d", n)
	}
	if err := e.clusters.RevokeAll(ctx, uAlice, nil); !kerrors.Is(err, biz.ErrClusterNotFound) {
		t.Fatalf("revoke unknown cluster: %v", err)
	}

	// 数据范围 + 选项。
	if err := e.scopes.Replace(ctx, roleA, []*biz.RoleScope{
		{ClusterID: agent.ID, Namespace: "default", Level: 1}, {ClusterID: agent.ID, Namespace: "team-a", Level: 3},
	}, entry(kc.AuditResourceRoleScope, kc.ActionScopeReplace)); err != nil {
		t.Fatalf("replace scope: %v", err)
	}
	if err := e.scopes.Replace(ctx, roleB, []*biz.RoleScope{{ClusterID: uAlice, Namespace: "*", Level: 1}}, nil); !kerrors.Is(err, biz.ErrScopeInvalid) {
		t.Fatalf("unknown cluster in scope: %v", err)
	}
	sc, _ := e.scopes.Get(ctx, roleA)
	if len(sc) != 2 || sc[0].ClusterName != "prod-1" || sc[0].Namespace != "default" {
		t.Fatalf("scopes = %+v", sc)
	}
	opts, err := e.clusters.Options(ctx, false, []string{roleA, roleB})
	if err != nil || len(opts) != 1 || opts[0].Level != 3 || opts[0].Name != "prod-1" {
		t.Fatalf("options = %+v, %v", opts, err)
	}
	if opts, _ = e.clusters.Options(ctx, true, nil); len(opts) != 2 || opts[0].Level != kc.LevelClusterAdmin {
		t.Fatalf("admin options = %+v", opts)
	}
	if opts, _ = e.clusters.Options(ctx, false, nil); len(opts) != 0 {
		t.Fatalf("options without roles = %+v", opts)
	}

	// 审计：每次写入一条；列表不含内容，详情含 patch。
	list, total, err := e.audits.List(ctx, biz.AuditFilter{Start: time.Now().Add(-time.Hour), End: time.Now().Add(time.Hour), Access: biz.Access{All: true}}, 1, 50)
	if err != nil || total != 7 {
		t.Fatalf("audits total = %d, %v", total, err)
	}
	if list[0].Action != kc.ActionScopeReplace || list[0].Name != roleA || list[0].Username != "alice" || list[0].Result != kc.AuditOK {
		t.Fatalf("latest audit = %+v", list[0])
	}
	detail, err := e.audits.Get(ctx, list[0].ID)
	if err != nil || string(detail.Patch) == "" {
		t.Fatalf("audit detail = %+v, %v", detail, err)
	}
	if _, err := e.audits.Get(ctx, uAlice); !kerrors.Is(err, biz.ErrAuditNotFound) {
		t.Fatalf("unknown audit: %v", err)
	}
	if _, total, _ = e.audits.List(ctx, biz.AuditFilter{Start: time.Now().Add(-time.Hour), End: time.Now().Add(time.Hour), ClusterID: agent.ID, Action: kc.ActionAgentRevoke, Access: biz.Access{All: true}}, 1, 50); total != 1 {
		t.Fatalf("filtered audits = %d", total)
	}

	// 删除：软删、清凭据、清数据范围与令牌、三项配置版本都 bump；审计保留。
	sv := e.cfg(kc.CfgRoleScope)
	if err := e.clusters.Delete(ctx, agent.ID, entry(kc.AuditResourceCluster, kc.ActionDelete)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.clusters.Get(ctx, agent.ID); !kerrors.Is(err, biz.ErrClusterNotFound) {
		t.Fatalf("get deleted: %v", err)
	}
	if sc, _ = e.scopes.Get(ctx, roleA); len(sc) != 0 || e.cfg(kc.CfgRoleScope) != sv+1 {
		t.Fatalf("scopes after delete = %+v", sc)
	}
	if err := e.clusters.Delete(ctx, agent.ID, nil); !kerrors.Is(err, biz.ErrClusterNotFound) {
		t.Fatalf("double delete: %v", err)
	}
	// 同名可以重新登记。
	e.create("prod-1", kc.AccessAgent, nil)
}

func TestTicketRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c := e.create("prod-1", kc.AccessAgent, nil)
	ticket, hash, _ := kc.NewTicket()
	want := biz.ExecTicket{UserID: uAlice, ClusterID: c.ID, Params: kc.ExecParams{Namespace: "default", Pod: "web", Cols: 80, Rows: 24, Kind: 1}}
	if err := e.tickets.Create(ctx, hash, want, kc.ExecTicketTTL); err != nil {
		t.Fatal(err)
	}
	// 并发核销：恰好一个成功。
	var wg sync.WaitGroup
	var mu sync.Mutex
	okN := 0
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := e.tickets.Redeem(ctx, kc.HashSecret(ticket))
			if err == nil && got.UserID == uAlice && got.Params.Pod == "web" && got.Params.Cols == 80 {
				mu.Lock()
				okN++
				mu.Unlock()
			} else if err != nil && !kerrors.Is(err, biz.ErrTicketInvalid) {
				t.Errorf("redeem: %v", err)
			}
		}()
	}
	wg.Wait()
	if okN != 1 {
		t.Fatalf("successful redeems = %d, want 1", okN)
	}
	// 过期的票据不可核销。
	_, hash2, _ := kc.NewTicket()
	if err := e.tickets.Create(ctx, hash2, want, -time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := e.tickets.Redeem(ctx, hash2); !kerrors.Is(err, biz.ErrTicketInvalid) {
		t.Fatalf("expired ticket: %v", err)
	}
}

func TestInstanceResolver(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c := e.create("prod-1", kc.AccessAgent, nil)
	if _, err := e.resolver.Any(ctx); !kerrors.Is(err, biz.ErrPlaneUnavailable) {
		t.Fatalf("no instance: %v", err)
	}
	if _, err := e.resolver.Owner(ctx, c.ID); !kerrors.Is(err, biz.ErrPlaneUnavailable) {
		t.Fatalf("no owner: %v", err)
	}
	if _, err := e.resolver.Owner(ctx, uAlice); !kerrors.Is(err, biz.ErrClusterNotFound) {
		t.Fatalf("unknown cluster: %v", err)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := e.pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec("INSERT INTO k8s_instance (id, addr) VALUES ('i1', 'http://10.0.0.5:8060'), ('i2', 'http://10.0.0.6:8060')")
	exec("UPDATE k8s_instance SET heartbeat_time = now() - interval '1 minute' WHERE id = 'i2'")
	exec("UPDATE k8s_cluster_runtime SET owner_instance = 'i2' WHERE cluster_id = $1", c.ID)
	// 负责实例心跳已过期：视为没有负责实例。
	if _, err := e.resolver.Owner(ctx, c.ID); !kerrors.Is(err, biz.ErrPlaneUnavailable) {
		t.Fatalf("stale owner: %v", err)
	}
	if addr, err := e.resolver.Any(ctx); err != nil || addr != "http://10.0.0.5:8060" {
		t.Fatalf("any = %q, %v", addr, err)
	}
	exec("UPDATE k8s_cluster_runtime SET owner_instance = 'i1' WHERE cluster_id = $1", c.ID)
	if addr, err := e.resolver.Owner(ctx, c.ID); err != nil || addr != "http://10.0.0.5:8060" {
		t.Fatalf("owner = %q, %v", addr, err)
	}
	// 缓存：归属变了，缓存期内仍是旧地址；Invalidate 后立即改走新实例。
	exec("UPDATE k8s_instance SET heartbeat_time = now() WHERE id = 'i2'")
	exec("UPDATE k8s_cluster_runtime SET owner_instance = 'i2' WHERE cluster_id = $1", c.ID)
	if addr, _ := e.resolver.Owner(ctx, c.ID); addr != "http://10.0.0.5:8060" {
		t.Fatalf("cached owner = %q", addr)
	}
	e.resolver.Invalidate(c.ID)
	if addr, _ := e.resolver.Owner(ctx, c.ID); addr != "http://10.0.0.6:8060" {
		t.Fatalf("owner after invalidate = %q", addr)
	}
	// 停用的集群不可路由。
	e.resolver.Invalidate(c.ID)
	exec("UPDATE k8s_cluster SET enabled = false WHERE id = $1", c.ID)
	if _, err := e.resolver.Owner(ctx, c.ID); !kerrors.Is(err, biz.ErrClusterNotFound) {
		t.Fatalf("disabled cluster: %v", err)
	}
}

func TestOutboxRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c := e.create("prod-1", kc.AccessAgent, nil)
	ids := make([]string, 3)
	for i := range ids {
		ids[i], _ = idgen.NewID()
		if _, err := e.pool.Exec(ctx,
			`INSERT INTO k8s_notify_outbox (id, cluster_id, event_type, severity, dedup_key, group_key, recipients, payload, create_time)
			 VALUES ($1, $2, 'k8s.cluster.unreachable', 3, $3, 'g', $4::uuid[], '{"cluster_name":"prod-1"}', now() + make_interval(secs => $5))`,
			ids[i], c.ID, "d"+ids[i], []string{uAlice}, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	txm := basedata.NewTxManager(e.data)
	var first *biz.OutboxItem
	err := txm.WithinTx(ctx, func(tx txrun.Tx) error {
		var err error
		if first, err = e.outbox.ClaimTx(ctx, tx, nil); err != nil {
			return err
		}
		// 同一行在另一个事务里被跳过（SKIP LOCKED），领到的是下一行。
		return txm.WithinTx(ctx, func(tx2 txrun.Tx) error {
			second, err := e.outbox.ClaimTx(ctx, tx2, nil)
			if err != nil {
				return err
			}
			if second == nil || second.ID != ids[1] {
				return errors.New("concurrent claim must skip the locked row")
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if first.ID != ids[0] || first.EventType != kc.NotifyClusterUnreachable || first.Severity != 3 || len(first.Recipients) != 1 ||
		first.Recipients[0] != uAlice || !strings.Contains(string(first.Payload), "prod-1") {
		t.Fatalf("claimed = %+v", first)
	}
	if err := txm.WithinTx(ctx, func(tx txrun.Tx) error { return e.outbox.FinishTx(ctx, tx, ids[0], biz.OutboxDone, "") }); err != nil {
		t.Fatal(err)
	}
	if err := txm.WithinTx(ctx, func(tx txrun.Tx) error { return e.outbox.FinishTx(ctx, tx, ids[0], biz.OutboxDone, "") }); err == nil {
		t.Fatal("finishing a non-pending row must fail")
	}
	// 失败记账：未到上限仍待派发但进入退避；到上限即丢弃。
	if dropped, err := e.outbox.Fail(ctx, ids[1], "boom", 2); err != nil || dropped {
		t.Fatalf("first failure: %v %v", dropped, err)
	}
	_ = txm.WithinTx(ctx, func(tx txrun.Tx) error {
		it, err := e.outbox.ClaimTx(ctx, tx, nil)
		if err != nil || it == nil || it.ID != ids[2] {
			t.Errorf("row in backoff must be skipped, got %+v %v", it, err)
		}
		return nil
	})
	if dropped, err := e.outbox.Fail(ctx, ids[1], "boom", 2); err != nil || !dropped {
		t.Fatalf("second failure: %v %v", dropped, err)
	}
	_ = txm.WithinTx(ctx, func(tx txrun.Tx) error {
		if it, _ := e.outbox.ClaimTx(ctx, tx, []string{ids[2]}); it != nil {
			t.Errorf("nothing left to claim, got %+v", it)
		}
		return nil
	})
}
