//go:build integration

package data

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/jackc/pgx/v5/pgxpool"
)

// engineRole 引擎的 DB 角色（NOLOGIN 组角色）；memberRole 模拟部署时的引擎登录账号——仅仅是
// engineRole 的成员，自身没有任何授权。集成测试的全部仓储都跑在 SET ROLE 到 memberRole 的连接上：
// 任何超出迁移 0070 授权范围的读写都会直接报 permission denied。
const (
	engineRole = "job_engine_role"
	memberRole = "job_engine_it_member"
)

// env 一次集成测试的环境：admin 是 TEST_DATABASE_SOURCE 的原始账号（建单、改时间、断言——相当于 core）；
// d 是引擎视角的连接池（会话角色 = job_engine_role 的成员）。
type env struct {
	t     *testing.T
	admin *pgxpool.Pool
	d     *Data
	repo  *executionRepo
}

// newEnv 连接测试库并清空 job_* 表。未配置 TEST_DATABASE_SOURCE 时跳过；库名须含 "test"。
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
	ctx := context.Background()
	cfg.MaxConns = 8
	admin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(admin.Close)

	// 角色是集群级对象：用完即删（清理顺序为后进先出——先关引擎连接池，再删角色，最后关 admin）。
	if _, err := admin.Exec(ctx, `DO $$ BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '`+memberRole+`') THEN
			CREATE ROLE `+memberRole+` NOLOGIN IN ROLE `+engineRole+`;
		END IF; END $$`); err != nil {
		t.Fatalf("create member role: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+memberRole); err != nil {
			t.Errorf("drop member role: %v", err)
		}
	})

	engineCfg, _ := pgxpool.ParseConfig(dsn)
	engineCfg.MaxConns = 16
	// 启动参数里设置 role：该连接池的每个会话都以「引擎角色的成员」的权限执行。
	engineCfg.ConnConfig.RuntimeParams["role"] = memberRole
	engine, err := pgxpool.NewWithConfig(ctx, engineCfg)
	if err != nil {
		t.Fatalf("connect as engine role: %v", err)
	}
	t.Cleanup(engine.Close)
	var who string
	if err := engine.QueryRow(ctx, "SELECT current_user").Scan(&who); err != nil || who != memberRole {
		t.Fatalf("engine pool runs as %q (%v), want %s", who, err, memberRole)
	}

	e := &env{t: t, admin: admin, d: &Data{Pool: engine}}
	e.repo = NewExecutionRepo(e.d)
	e.exec(`TRUNCATE TABLE job_execution_payload, job_execution_log, job_execution_host, job_execution, job_template, job_runner, job_project, job_credential CASCADE`)
	return e
}

func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.admin.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatalf("exec %.60q: %v", sql, err)
	}
}

// scan 读一行到 dest。
func (e *env) scan(sql string, args []any, dest ...any) {
	e.t.Helper()
	if err := e.admin.QueryRow(context.Background(), sql, args...).Scan(dest...); err != nil {
		e.t.Fatalf("query %.60q: %v", sql, err)
	}
}

func (e *env) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	e.scan(sql, args, &n)
	return n
}

func (e *env) id() string {
	e.t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

// runner 建一个执行节点，返回 ID。
func (e *env) runner(zone string) string {
	e.t.Helper()
	id := e.id()
	e.exec(`INSERT INTO job_runner (id, name, zone, token_hash) VALUES ($1, $2, $3, $4)`,
		id, "runner-"+id[len(id)-12:], zone, jobcontract.HashRunnerSecret("secret-"+id))
	return id
}

// execSpec 建单参数。
type execSpec struct {
	Zone      string
	Template  string
	Exclusive bool
	Canceled  bool
	Hosts     []string // 缺省两台
	Age       time.Duration
	Status    int32 // 缺省排队中；9 = 待审批，10 = 审批未通过
	Kind      int32 // 缺省作业模板；2-4 = 快速执行（没有模板）
	Adhoc     *jobcontract.Adhoc
	Payload   []byte // 分发文件的内容（非 nil 时写 job_execution_payload）
}

// execution 以 core 的身份建一条执行（缺省排队中）及其主机快照，返回 ID。
func (e *env) execution(s execSpec) string {
	e.t.Helper()
	id := e.id()
	if s.Hosts == nil {
		s.Hosts = []string{"10.0.0.1", "10.0.0.2"}
	}
	if s.Status == 0 {
		s.Status = jobcontract.StatusQueued
	}
	sp := jobcontract.Spec{CredentialID: e.id(), Options: jobcontract.Options{Forks: 5, TimeoutS: 600, Port: 22}}
	var template any // 快速执行没有模板（ck_job_execution_template）
	if s.Kind == 0 || s.Kind == jobcontract.KindPlaybook {
		s.Kind = jobcontract.KindPlaybook
		if s.Template == "" {
			s.Template = e.id()
		}
		template = s.Template
		sp.ProjectID, sp.RepoURL, sp.Ref, sp.Playbook = e.id(), "https://git.example.com/ops/infra.git", "main", "site.yml"
		sp.ExtraVars = map[string]any{"version": "1.2.3"}
	} else {
		sp.Kind, sp.Adhoc = s.Kind, s.Adhoc
	}
	spec, err := json.Marshal(sp)
	if err != nil {
		e.t.Fatal(err)
	}
	e.exec(`INSERT INTO job_execution (id, template_id, template_name, template_version, creator, spec, runner_zone, exclusive,
			timeout_s, host_total, cancel_requested, create_time, status, kind, finish_time)
		VALUES ($1, $2, 'tpl', 1, $3, $4, $5, $6, 600, $7, $8, now() - $9 * interval '1 second', $10::smallint, $11,
			CASE WHEN $10::smallint IN (1, 2, 9) THEN NULL ELSE now() END)`,
		id, template, e.id(), spec, s.Zone, s.Exclusive, len(s.Hosts), s.Canceled, s.Age.Seconds(), s.Status, s.Kind)
	for i, addr := range s.Hosts {
		e.exec(`INSERT INTO job_execution_host (execution_id, seq, ci_id, name, env, address, port) VALUES ($1, $2, $3, $4, 'prod', $5, 22)`,
			id, i+1, e.id(), "host-"+addr, addr)
	}
	if s.Payload != nil {
		e.exec(`INSERT INTO job_execution_payload (execution_id, content) VALUES ($1, $2)`, id, s.Payload)
	}
	return id
}

// row 执行行的断言视图。
type row struct {
	Status       int32
	RunnerID     string
	LeaseToken   string
	Leased       bool // lease_until > now()
	Started      bool
	Finished     bool
	Error        string
	LogSeq       int64
	LogBytes     int64
	LogTruncated bool
	CommitID     string
	OK, Failed   int32
	Unreachable  int32
}

func (e *env) row(id string) row {
	e.t.Helper()
	var r row
	e.scan(`SELECT status, COALESCE(runner_id::text, ''), lease_token, COALESCE(lease_until > now(), false), start_time IS NOT NULL,
			finish_time IS NOT NULL, error, log_seq, log_bytes, log_truncated, commit_id, host_ok, host_failed, host_unreachable
		FROM job_execution WHERE id = $1`, []any{id},
		&r.Status, &r.RunnerID, &r.LeaseToken, &r.Leased, &r.Started, &r.Finished, &r.Error, &r.LogSeq, &r.LogBytes, &r.LogTruncated,
		&r.CommitID, &r.OK, &r.Failed, &r.Unreachable)
	return r
}

// claim 领取一条并断言领到的是 want（空串 = 应当领不到）。返回领取令牌。
func (e *env) claim(runnerID, zone, want string) string {
	e.t.Helper()
	token := "lease-" + e.id()
	c, err := e.repo.ClaimNext(context.Background(), runnerID, zone, token, time.Minute)
	if err != nil {
		e.t.Fatalf("claim: %v", err)
	}
	switch {
	case want == "" && c != nil:
		e.t.Fatalf("claimed %s, want nothing", c.ID)
	case want != "" && (c == nil || c.ID != want):
		e.t.Fatalf("claimed %+v, want %s", c, want)
	}
	return token
}

// expire 把执行的租约拨到过去。
func (e *env) expire(id string) {
	e.t.Helper()
	e.exec(`UPDATE job_execution SET lease_until = now() - interval '1 second' WHERE id = $1`, id)
}

func ctxb() context.Context { return context.Background() }

func TestRunnerRepo(t *testing.T) {
	e := newEnv(t)
	repo := NewRunnerRepo(e.d)
	id := e.runner("dmz")

	r, err := repo.Find(ctxb(), id)
	if err != nil || r.Zone != "dmz" || !r.Enabled || !jobcontract.RunnerSecretMatches("secret-"+id, r.TokenHash) {
		t.Fatalf("find: %+v %v", r, err)
	}
	if _, err := repo.Find(ctxb(), e.id()); !errors.Is(err, biz.ErrNotFound) {
		t.Fatalf("unknown runner: %v", err)
	}

	enabled, err := repo.Heartbeat(ctxb(), id, biz.Heartbeat{AgentVersion: "1.2.3", Hostname: "runner-host", Capacity: 4, Running: 1})
	if err != nil || !enabled {
		t.Fatalf("heartbeat: %v %v", enabled, err)
	}
	var (
		version, host string
		capacity, run int
		fresh         bool
		rowVersion    int
	)
	e.scan(`SELECT agent_version, hostname, capacity, running, last_seen > now() - interval '5 seconds', version FROM job_runner WHERE id = $1`,
		[]any{id}, &version, &host, &capacity, &run, &fresh, &rowVersion)
	if version != "1.2.3" || host != "runner-host" || capacity != 4 || run != 1 || !fresh || rowVersion != 1 {
		t.Fatalf("heartbeat columns: %q %q %d %d fresh=%v version=%d", version, host, capacity, run, fresh, rowVersion)
	}
	// 停用的节点：心跳照写，返回 enabled=false。
	e.exec(`UPDATE job_runner SET enabled = false WHERE id = $1`, id)
	if enabled, err := repo.Heartbeat(ctxb(), id, biz.Heartbeat{}); err != nil || enabled {
		t.Fatalf("disabled heartbeat: %v %v", enabled, err)
	}
	// 软删的节点：查不到、心跳不写。
	e.exec(`UPDATE job_runner SET delete_time = now() WHERE id = $1`, id)
	if _, err := repo.Find(ctxb(), id); !errors.Is(err, biz.ErrNotFound) {
		t.Fatalf("deleted runner: %v", err)
	}
	if _, err := repo.Heartbeat(ctxb(), id, biz.Heartbeat{}); !errors.Is(err, biz.ErrNotFound) {
		t.Fatalf("heartbeat of deleted runner: %v", err)
	}
}

func TestClaimOrderZoneAndFencing(t *testing.T) {
	e := newEnv(t)
	runnerA, runnerNone := e.runner("zone-a"), e.runner("")
	oldB := e.execution(execSpec{Zone: "zone-b", Age: 5 * time.Minute})
	oldCanceled := e.execution(execSpec{Canceled: true, Age: 4 * time.Minute})
	second := e.execution(execSpec{Age: 3 * time.Minute})
	third := e.execution(execSpec{Zone: "zone-a", Age: 2 * time.Minute})
	newest := e.execution(execSpec{Age: time.Minute})

	// 无区域的节点跳过指定了区域的与已请求取消的，按提交时间领最早的。
	token := e.claim(runnerNone, "", second)
	if n := e.count(`SELECT count(*) FROM job_execution WHERE status <> 1`); n != 1 {
		t.Fatalf("one claim moved %d executions out of the queue", n)
	}
	r := e.row(second)
	if r.Status != jobcontract.StatusRunning || r.RunnerID != runnerNone || r.LeaseToken != token || !r.Leased || !r.Started || r.Finished {
		t.Fatalf("after claim: %+v", r)
	}
	// zone-a 的节点：自己区域的与不限区域的都能领，仍按时间。
	e.claim(runnerA, "zone-a", third)
	tokenNewest := e.claim(runnerA, "zone-a", newest)
	e.claim(runnerA, "zone-a", "")
	e.claim(runnerNone, "", "")
	for _, id := range []string{oldB, oldCanceled} {
		if got := e.row(id).Status; got != jobcontract.StatusQueued {
			t.Fatalf("execution %s status %d, want queued", id, got)
		}
	}

	// 一次领取只动一条：其余排队中的执行原封不动（回归：IN 子查询 + SKIP LOCKED + LIMIT 1 曾一次领走多条）。
	if n := e.count(`SELECT count(*) FROM job_execution WHERE status = 2`); n != 3 {
		t.Fatalf("%d executions running after 3 claims", n)
	}

	// Fail / Release 以领取令牌围栏：令牌不对不生效。
	if err := e.repo.Fail(ctxb(), newest, "wrong-token", "nope"); err != nil {
		t.Fatal(err)
	}
	if err := e.repo.Release(ctxb(), newest, "wrong-token"); err != nil {
		t.Fatal(err)
	}
	if got := e.row(newest); got.Status != jobcontract.StatusRunning || got.Error != "" {
		t.Fatalf("fenced settle changed the row: %+v", got)
	}
	// Release：恢复成领取前的样子，可以再被领取。
	if err := e.repo.Release(ctxb(), newest, tokenNewest); err != nil {
		t.Fatal(err)
	}
	if got := e.row(newest); got.Status != jobcontract.StatusQueued || got.RunnerID != "" || got.LeaseToken != "" || got.Leased || got.Started {
		t.Fatalf("after release: %+v", got)
	}
	tokenNewest = e.claim(runnerNone, "", newest)
	// Fail：置为失败并写原因与结束时间。
	if err := e.repo.Fail(ctxb(), newest, tokenNewest, "凭据已被删除，无法执行"); err != nil {
		t.Fatal(err)
	}
	if got := e.row(newest); got.Status != jobcontract.StatusFailed || !got.Finished || got.Error != "凭据已被删除，无法执行" {
		t.Fatalf("after fail: %+v", got)
	}
}

// 并发领取：每条执行恰被领一次，没有重复、没有遗漏。
func TestClaimConcurrent(t *testing.T) {
	e := newEnv(t)
	const total, workers = 40, 8
	for range total {
		e.execution(execSpec{Hosts: []string{"10.0.0.1"}})
	}
	runners := make([]string, workers)
	for i := range runners {
		runners[i] = e.runner("")
	}
	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		wg      sync.WaitGroup
	)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c, err := e.repo.ClaimNext(ctxb(), runners[w], "", "lease-"+runners[w], time.Minute)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if c == nil {
					return
				}
				mu.Lock()
				claimed[c.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(claimed) != total {
		t.Fatalf("claimed %d distinct executions, want %d", len(claimed), total)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Fatalf("execution %s claimed %d times", id, n)
		}
	}
	if n := e.count(`SELECT count(*) FROM job_execution WHERE status = 1`); n != 0 {
		t.Fatalf("%d executions left queued", n)
	}
}

func TestMaterials(t *testing.T) {
	e := newEnv(t)
	box, err := secretbox.New("integration-master-key")
	if err != nil {
		t.Fatal(err)
	}
	seal := func(s string) []byte {
		out, err := box.Seal(s)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	credID, bareID, projID, anonID := e.id(), e.id(), e.id(), e.id()
	e.exec(`INSERT INTO job_credential (id, name, type, username, secret, become_method, become_user, become_secret)
		VALUES ($1, 'cred', 1, 'ops', $2, 'sudo', 'root', $3)`, credID, seal("PRIVATE-KEY"), seal("become-pass"))
	e.exec(`INSERT INTO job_credential (id, name, type, username, secret) VALUES ($1, 'bare', 2, 'ops', $2)`, bareID, seal("pw"))
	e.exec(`INSERT INTO job_project (id, name, repo_url, auth_type, username, secret) VALUES ($1, 'proj', 'https://git.example.com/x.git', 2, 'deploy', $2)`,
		projID, seal("git-token"))
	e.exec(`INSERT INTO job_project (id, name, repo_url) VALUES ($1, 'anon', 'https://git.example.com/y.git')`, anonID)

	// 密文经 SecretBox（与 core 写路径同一实现）解出明文。
	opener := &SecretBox{box: box}
	c, err := e.repo.Credential(ctxb(), credID)
	if err != nil || c.Type != jobcontract.CredentialSSHKey || c.Username != "ops" || c.BecomeMethod != "sudo" || c.BecomeUser != "root" {
		t.Fatalf("credential: %+v %v", c, err)
	}
	if s, err := opener.Open(c.Secret); err != nil || s != "PRIVATE-KEY" {
		t.Fatalf("open secret: %q %v", s, err)
	}
	if s, err := opener.Open(c.BecomeSecret); err != nil || s != "become-pass" {
		t.Fatalf("open become secret: %q %v", s, err)
	}
	if bare, err := e.repo.Credential(ctxb(), bareID); err != nil || bare.BecomeSecret != nil || bare.BecomeMethod != "" {
		t.Fatalf("bare credential: %+v %v", bare, err)
	}
	p, err := e.repo.Project(ctxb(), projID)
	if err != nil || p.AuthType != jobcontract.GitAuthToken || p.Username != "deploy" {
		t.Fatalf("project: %+v %v", p, err)
	}
	if s, err := opener.Open(p.Secret); err != nil || s != "git-token" {
		t.Fatalf("open project secret: %q %v", s, err)
	}
	if anon, err := e.repo.Project(ctxb(), anonID); err != nil || anon.AuthType != jobcontract.GitAuthNone || anon.Secret != nil {
		t.Fatalf("anonymous project: %+v %v", anon, err)
	}
	// 主密钥不对 / 未配置：解密失败，错误里没有密文与明文。
	wrong, _ := secretbox.New("another-key")
	if _, err := (&SecretBox{box: wrong}).Open(c.Secret); err == nil || strings.Contains(err.Error(), "PRIVATE-KEY") {
		t.Fatalf("wrong master key: %v", err)
	}
	none, _ := NewSecretBox(nil)
	if _, err := none.Open(c.Secret); !errors.Is(err, ErrMasterKeyUnavailable) {
		t.Fatalf("missing master key: %v", err)
	}
	// 软删后查不到。
	e.exec(`UPDATE job_credential SET delete_time = now() WHERE id = $1`, credID)
	e.exec(`UPDATE job_project SET delete_time = now() WHERE id = $1`, projID)
	if _, err := e.repo.Credential(ctxb(), credID); !errors.Is(err, biz.ErrNotFound) {
		t.Fatalf("deleted credential: %v", err)
	}
	if _, err := e.repo.Project(ctxb(), projID); !errors.Is(err, biz.ErrNotFound) {
		t.Fatalf("deleted project: %v", err)
	}

	id := e.execution(execSpec{Hosts: []string{"10.0.0.9", "2001:db8::1", "web-1.example.com"}})
	hosts, err := e.repo.Hosts(ctxb(), id)
	if err != nil || len(hosts) != 3 || hosts[0].Address != "10.0.0.9" || hosts[1].Address != "2001:db8::1" || hosts[2].Port != 22 ||
		hosts[0].Env != "prod" || hosts[0].Name != "host-10.0.0.9" || !idgen.IsUUID(hosts[0].CIID) {
		t.Fatalf("hosts: %+v %v", hosts, err)
	}
}

// report 用固定的变更落一次上报（仓储层测试：判定逻辑在 biz 的单元测试里）。
func (e *env) report(id string, m biz.ReportMutation) (revived bool, seen biz.ExecState, err error) {
	e.t.Helper()
	revived, err = e.repo.Report(ctxb(), id, func(cur biz.ExecState) (*biz.ReportMutation, error) {
		seen = cur
		return &m, nil
	})
	return revived, seen, err
}

func TestReportAndFinish(t *testing.T) {
	e := newEnv(t)
	runnerID := e.runner("")
	id := e.execution(execSpec{})
	token := e.claim(runnerID, "", id)
	e.exec(`UPDATE job_execution SET cancel_requested = true WHERE id = $1`, id)
	e.expire(id)

	revived, seen, err := e.report(id, biz.ReportMutation{
		Chunks: []jobcontract.LogChunk{{Seq: 1, Content: "line 1\n"}, {Seq: 2, Content: "第二行\n"}},
		LogSeq: 2, LogBytes: 17, CommitID: strings.Repeat("a", 40), Lease: time.Minute,
	})
	if err != nil || revived {
		t.Fatalf("report: revived=%v err=%v", revived, err)
	}
	if seen.Status != jobcontract.StatusRunning || seen.RunnerID != runnerID || seen.LeaseToken != token || !seen.CancelRequested || seen.LogSeq != 0 {
		t.Fatalf("locked state: %+v", seen)
	}
	r := e.row(id)
	if r.LogSeq != 2 || r.LogBytes != 17 || r.CommitID != strings.Repeat("a", 40) || !r.Leased || r.Status != jobcontract.StatusRunning {
		t.Fatalf("after report: %+v", r)
	}
	// 同一序号再写是空操作（ON CONFLICT DO NOTHING）：内容保持第一次的。
	if _, _, err := e.report(id, biz.ReportMutation{
		Chunks: []jobcontract.LogChunk{{Seq: 2, Content: "DIFFERENT"}, {Seq: 3, Content: "line 3\n"}},
		LogSeq: 3, LogBytes: 24, LogTruncated: true, CommitID: strings.Repeat("a", 40), Lease: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	var second string
	e.scan(`SELECT content FROM job_execution_log WHERE execution_id = $1 AND seq = 2`, []any{id}, &second)
	if second != "第二行\n" || e.count(`SELECT count(*) FROM job_execution_log WHERE execution_id = $1`, id) != 3 {
		t.Fatalf("log rows: seq 2 = %q", second)
	}
	if r := e.row(id); r.LogSeq != 3 || !r.LogTruncated {
		t.Fatalf("after second report: %+v", r)
	}
	// decide 返回错误：整个事务回滚，什么都不写。
	boom := errors.New("fenced")
	if _, err := e.repo.Report(ctxb(), id, func(biz.ExecState) (*biz.ReportMutation, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("decide error not propagated: %v", err)
	}
	if _, err := e.repo.Report(ctxb(), e.id(), func(biz.ExecState) (*biz.ReportMutation, error) { return &biz.ReportMutation{}, nil }); !errors.Is(err, biz.ErrNotFound) {
		t.Fatalf("unknown execution: %v", err)
	}

	// 结束：主机行按地址更新，未上报的保持未执行，统计列从主机行汇总。
	finish := func(m *biz.FinishMutation) error {
		return e.repo.Finish(ctxb(), id, func(biz.ExecState) (*biz.FinishMutation, error) { return m, nil })
	}
	if err := finish(&biz.FinishMutation{
		Status: jobcontract.StatusPartial, Error: "ansible-playbook 以退出码 2 结束", CommitID: strings.Repeat("b", 40),
		Hosts: []biz.HostUpdate{
			{Address: "10.0.0.1", Status: jobcontract.HostFailed, OK: 3, Changed: 1, Failed: 1, Skipped: 2},
			{Address: "10.9.9.9", Status: jobcontract.HostOK, OK: 1}, // 不属于本次执行：不命中任何行
		},
	}); err != nil {
		t.Fatal(err)
	}
	r = e.row(id)
	if r.Status != jobcontract.StatusPartial || !r.Finished || r.Error != "ansible-playbook 以退出码 2 结束" || r.CommitID != strings.Repeat("b", 40) ||
		r.OK != 0 || r.Failed != 1 || r.Unreachable != 0 {
		t.Fatalf("after finish: %+v", r)
	}
	var st, ok, changed, failed, skipped int32
	e.scan(`SELECT status, ok, changed, failed, skipped FROM job_execution_host WHERE execution_id = $1 AND address = '10.0.0.1'`, []any{id},
		&st, &ok, &changed, &failed, &skipped)
	if st != jobcontract.HostFailed || ok != 3 || changed != 1 || failed != 1 || skipped != 2 {
		t.Fatalf("host row: %d %d %d %d %d", st, ok, changed, failed, skipped)
	}
	if n := e.count(`SELECT count(*) FROM job_execution_host WHERE execution_id = $1 AND status = 0`, id); n != 1 {
		t.Fatalf("%d pending hosts, want 1", n)
	}
	// decide 返回 nil 变更（重复的结束请求）：空操作。
	if err := finish(nil); err != nil {
		t.Fatal(err)
	}
	// 空提交号不覆盖已有的提交号。
	if err := finish(&biz.FinishMutation{Status: jobcontract.StatusSuccess}); err != nil {
		t.Fatal(err)
	}
	if r := e.row(id); r.CommitID != strings.Repeat("b", 40) || r.Status != jobcontract.StatusSuccess || r.Error != "" {
		t.Fatalf("after second finish: %+v", r)
	}
}

func TestExpireLeasesAndRevive(t *testing.T) {
	e := newEnv(t)
	runnerID := e.runner("")
	lost, alive, queued := e.execution(execSpec{}), e.execution(execSpec{}), e.execution(execSpec{Zone: "elsewhere"})
	e.claim(runnerID, "", lost)
	e.claim(runnerID, "", alive)
	e.expire(lost)

	n, err := e.repo.ExpireLeases(ctxb(), biz.LeaseExpiredReason, 100)
	if err != nil || n != 1 {
		t.Fatalf("expire = %d, %v", n, err)
	}
	r := e.row(lost)
	if r.Status != jobcontract.StatusUnknown || !r.Finished || r.Error != biz.LeaseExpiredReason || r.RunnerID != runnerID || r.LeaseToken == "" {
		t.Fatalf("expired execution: %+v", r)
	}
	// 租约未到期的与排队中的不动；清扫幂等；结果未知的执行绝不重新排队。
	if e.row(alive).Status != jobcontract.StatusRunning || e.row(queued).Status != jobcontract.StatusQueued {
		t.Fatal("sweeper touched executions it must not touch")
	}
	if n, _ := e.repo.ExpireLeases(ctxb(), biz.LeaseExpiredReason, 100); n != 0 {
		t.Fatalf("second sweep = %d", n)
	}
	e.claim(runnerID, "", "")

	// 节点回来了：恢复为执行中，清掉结束时间与原因，续租；core 已为「结果未知」发过的通知标记一并清掉，
	// 真正结束时会再通知一次。
	e.exec(`UPDATE job_execution SET notify_time = now() WHERE id = $1`, lost)
	revived, seen, err := e.report(lost, biz.ReportMutation{Revive: true, Lease: time.Minute})
	if err != nil || !revived || seen.Status != jobcontract.StatusUnknown {
		t.Fatalf("revive: revived=%v seen=%+v err=%v", revived, seen, err)
	}
	if r := e.row(lost); r.Status != jobcontract.StatusRunning || r.Finished || r.Error != "" || !r.Leased {
		t.Fatalf("after revive: %+v", r)
	}
	if n := e.count(`SELECT count(*) FROM job_execution WHERE id = $1 AND notify_time IS NULL`, lost); n != 1 {
		t.Fatal("revive must clear notify_time")
	}
	// 批量上限：一轮至多 limit 条。
	for range 5 {
		id := e.execution(execSpec{Hosts: []string{"10.0.0.1"}})
		e.claim(runnerID, "", id)
		e.expire(id)
	}
	if n, _ := e.repo.ExpireLeases(ctxb(), biz.LeaseExpiredReason, 3); n != 3 {
		t.Fatalf("limited sweep = %d, want 3", n)
	}
	if n, _ := e.repo.ExpireLeases(ctxb(), biz.LeaseExpiredReason, 3); n != 2 {
		t.Fatalf("second limited sweep = %d, want 2", n)
	}
}

// 模板禁止并发：结果未知的执行在互斥位被新的执行占用后无法恢复——保持结果未知、什么都不清，
// 但上报照常收日志、续租、记提交号；清扫不再碰它；结束时直接落为终态。
func TestReviveBlockedByExclusiveSlot(t *testing.T) {
	e := newEnv(t)
	runnerID := e.runner("")
	tpl := e.id()
	lost := e.execution(execSpec{Template: tpl, Exclusive: true})
	e.claim(runnerID, "", lost)
	e.expire(lost)
	if n, _ := e.repo.ExpireLeases(ctxb(), biz.LeaseExpiredReason, 100); n != 1 {
		t.Fatal("sweep")
	}
	e.exec(`UPDATE job_execution SET notify_time = now() WHERE id = $1`, lost)
	// 互斥位已释放：core 为同一模板接受了新的执行。
	successor := e.execution(execSpec{Template: tpl, Exclusive: true, Zone: "elsewhere"})

	for seq := int64(1); seq <= 3; seq++ {
		revived, _, err := e.report(lost, biz.ReportMutation{
			Revive: true, Chunks: []jobcontract.LogChunk{{Seq: seq, Content: "still running\n"}},
			LogSeq: seq, LogBytes: seq * 14, CommitID: strings.Repeat("c", 40), Lease: time.Minute,
		})
		if err != nil || revived {
			t.Fatalf("report %d: revived=%v err=%v (must succeed without reviving)", seq, revived, err)
		}
		r := e.row(lost)
		if r.Status != jobcontract.StatusUnknown || !r.Finished || r.Error != biz.LeaseExpiredReason {
			t.Fatalf("report %d: execution must stay unknown with nothing cleared: %+v", seq, r)
		}
		if n := e.count(`SELECT count(*) FROM job_execution WHERE id = $1 AND notify_time IS NOT NULL`, lost); n != 1 {
			t.Fatalf("report %d: a blocked revive must not clear notify_time", seq)
		}
		if r.LogSeq != seq || r.CommitID != strings.Repeat("c", 40) || !r.Leased {
			t.Fatalf("report %d: logs / lease / commit must still be written: %+v", seq, r)
		}
		// 清扫只看执行中的行：这条不会被再次处理，哪怕它的租约又过期了。
		e.expire(lost)
		if n, _ := e.repo.ExpireLeases(ctxb(), "SECOND SWEEP", 100); n != 0 {
			t.Fatalf("sweeper touched an unknown execution (%d rows)", n)
		}
	}
	if n := e.count(`SELECT count(*) FROM job_execution_log WHERE execution_id = $1`, lost); n != 3 {
		t.Fatalf("%d log chunks, want 3", n)
	}
	if e.row(successor).Status != jobcontract.StatusQueued {
		t.Fatal("successor must be untouched")
	}
	// 结束：终态不占互斥位，直接落账。
	if err := e.repo.Finish(ctxb(), lost, func(cur biz.ExecState) (*biz.FinishMutation, error) {
		if cur.Status != jobcontract.StatusUnknown {
			t.Errorf("finish sees status %d, want unknown", cur.Status)
		}
		return &biz.FinishMutation{Status: jobcontract.StatusSuccess, Hosts: []biz.HostUpdate{
			{Address: "10.0.0.1", Status: jobcontract.HostOK, OK: 1}, {Address: "10.0.0.2", Status: jobcontract.HostChanged, OK: 1, Changed: 1},
		}}, nil
	}); err != nil {
		t.Fatalf("finish from unknown: %v", err)
	}
	if r := e.row(lost); r.Status != jobcontract.StatusSuccess || r.Error != "" || r.OK != 2 {
		t.Fatalf("after finish: %+v", r)
	}
	// core 已按「结果未知」通知过：真实结果落账时清掉通知标记，它会再按真实结果通知一次。
	if n := e.count(`SELECT count(*) FROM job_execution WHERE id = $1 AND notify_time IS NULL`, lost); n != 1 {
		t.Fatal("finishing from unknown must clear notify_time so the real result is notified")
	}

	// 互斥位空着时照常恢复。
	other := e.execution(execSpec{Exclusive: true})
	e.claim(runnerID, "", other)
	e.expire(other)
	if n, _ := e.repo.ExpireLeases(ctxb(), biz.LeaseExpiredReason, 100); n != 1 {
		t.Fatal("sweep")
	}
	if revived, _, err := e.report(other, biz.ReportMutation{Revive: true, Lease: time.Minute}); err != nil || !revived {
		t.Fatalf("revive with a free slot: revived=%v err=%v", revived, err)
	}
}

// 恢复与新执行建单之间的竞态：条件判定看不到对方尚未提交的行，最终由唯一索引裁决。恢复语句撞上
// 唯一约束时回滚到保存点，本次上报的其余写入照常提交。
func TestReviveLosesRaceToUniqueIndex(t *testing.T) {
	e := newEnv(t)
	runnerID := e.runner("")
	tpl := e.id()
	lost := e.execution(execSpec{Template: tpl, Exclusive: true})
	e.claim(runnerID, "", lost)
	e.expire(lost)
	if n, _ := e.repo.ExpireLeases(ctxb(), biz.LeaseExpiredReason, 100); n != 1 {
		t.Fatal("sweep")
	}

	// core 在一个尚未提交的事务里为同一模板建了新的互斥执行。
	ctx := ctxb()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	successor := e.id()
	if _, err := tx.Exec(ctx, `INSERT INTO job_execution (id, template_id, template_name, template_version, creator, spec, exclusive, timeout_s, status)
		VALUES ($1, $2, 'tpl', 1, $3, '{}', true, 600, 9)`, successor, tpl, e.id()); err != nil {
		t.Fatal(err)
	}

	type result struct {
		revived bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		revived, _, err := e.report(lost, biz.ReportMutation{
			Revive: true, Chunks: []jobcontract.LogChunk{{Seq: 1, Content: "back\n"}}, LogSeq: 1, LogBytes: 5, Lease: time.Minute,
		})
		done <- result{revived, err}
	}()
	// 恢复语句的条件看不到未提交的新行，于是去改状态，在唯一索引上等对方的事务。
	select {
	case r := <-done:
		t.Fatalf("report returned before the competing insert committed: %+v", r)
	case <-time.After(500 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var r result
	select {
	case r = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("report still blocked after the competing insert committed")
	}
	if r.err != nil || r.revived {
		t.Fatalf("report must succeed without reviving: %+v", r)
	}
	got := e.row(lost)
	if got.Status != jobcontract.StatusUnknown || !got.Finished || got.Error != biz.LeaseExpiredReason {
		t.Fatalf("execution must stay unknown: %+v", got)
	}
	if got.LogSeq != 1 || !got.Leased || e.count(`SELECT count(*) FROM job_execution_log WHERE execution_id = $1`, lost) != 1 {
		t.Fatalf("the rest of the report must still commit: %+v", got)
	}
	if e.row(successor).Status != jobcontract.StatusPending {
		t.Fatal("successor must be untouched")
	}
}

func TestPurgeLogs(t *testing.T) {
	e := newEnv(t)
	runnerID := e.runner("")
	finished, running := e.execution(execSpec{}), e.execution(execSpec{})
	e.claim(runnerID, "", finished)
	e.claim(runnerID, "", running)
	for _, id := range []string{finished, running} {
		if _, _, err := e.report(id, biz.ReportMutation{
			Chunks: []jobcontract.LogChunk{{Seq: 1, Content: "a"}, {Seq: 2, Content: "b"}, {Seq: 3, Content: "c"}},
			LogSeq: 3, LogBytes: 3, Lease: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.repo.Finish(ctxb(), finished, func(biz.ExecState) (*biz.FinishMutation, error) {
		return &biz.FinishMutation{Status: jobcontract.StatusSuccess}, nil
	}); err != nil {
		t.Fatal(err)
	}
	// 两条执行的前两块都已超过保留期。
	e.exec(`UPDATE job_execution_log SET create_time = now() - interval '100 days' WHERE seq <= 2`)

	if n, err := e.repo.PurgeLogs(ctxb(), 90*24*time.Hour, 1); err != nil || n != 1 {
		t.Fatalf("limited purge = %d, %v", n, err)
	}
	if n, err := e.repo.PurgeLogs(ctxb(), 90*24*time.Hour, 100); err != nil || n != 1 {
		t.Fatalf("purge = %d, %v", n, err)
	}
	if n, _ := e.repo.PurgeLogs(ctxb(), 90*24*time.Hour, 100); n != 0 {
		t.Fatalf("third purge = %d", n)
	}
	// 已结束的执行只剩保留期内的那块；未结束的执行一块不删。
	if n := e.count(`SELECT count(*) FROM job_execution_log WHERE execution_id = $1`, finished); n != 1 {
		t.Fatalf("finished execution keeps %d chunks, want 1", n)
	}
	if n := e.count(`SELECT count(*) FROM job_execution_log WHERE execution_id = $1`, running); n != 3 {
		t.Fatalf("running execution keeps %d chunks, want 3", n)
	}
}

// 引擎角色只能做迁移 0070 授权的事：越权语句被数据库拒绝。
func TestEngineRoleIsRestricted(t *testing.T) {
	e := newEnv(t)
	runnerID := e.runner("")
	id := e.execution(execSpec{})
	credID := e.id()
	e.exec(`INSERT INTO job_credential (id, name, type, username, secret) VALUES ($1, 'c', 2, 'ops', '\x00')`, credID)
	e.exec(`INSERT INTO job_template (id, name, project_id, playbook, credential_id, target) VALUES ($1, 't', $2, 'site.yml', $3, '{}')`,
		e.id(), e.id(), credID)

	forbidden := map[string]string{
		"read templates":           `SELECT count(*) FROM job_template`,
		"read script library":      `SELECT count(*) FROM job_script`,
		"read risk rules":          `SELECT count(*) FROM job_risk_rule`,
		"read policy":              `SELECT count(*) FROM job_policy`,
		"read schedules":           `SELECT count(*) FROM job_schedule`,
		"read triggers":            `SELECT count(*) FROM job_trigger`,
		"read trigger fires":       `SELECT count(*) FROM job_trigger_fire`,
		"write payload":            `INSERT INTO job_execution_payload (execution_id, content) VALUES ('` + id + `', '\x00')`,
		"delete payload":           `DELETE FROM job_execution_payload`,
		"insert execution":         `INSERT INTO job_execution (id, template_id, template_name, template_version, creator, spec, timeout_s) VALUES ('` + e.id() + `', '` + e.id() + `', 't', 1, '` + e.id() + `', '{}', 60)`,
		"delete execution":         `DELETE FROM job_execution WHERE id = '` + id + `'`,
		"insert execution host":    `INSERT INTO job_execution_host (execution_id, seq, ci_id, name, address, port) VALUES ('` + id + `', 9, '` + e.id() + `', 'x', '10.9.9.9', 22)`,
		"delete execution host":    `DELETE FROM job_execution_host WHERE execution_id = '` + id + `'`,
		"update log":               `UPDATE job_execution_log SET content = 'x'`,
		"update credential":        `UPDATE job_credential SET username = 'root' WHERE id = '` + credID + `'`,
		"delete credential":        `DELETE FROM job_credential WHERE id = '` + credID + `'`,
		"insert credential":        `INSERT INTO job_credential (id, name, type, username, secret) VALUES ('` + e.id() + `', 'x', 2, 'x', '\x00')`,
		"update project":           `UPDATE job_project SET repo_url = 'https://evil.example.com/x.git'`,
		"enable runner":            `UPDATE job_runner SET enabled = true WHERE id = '` + runnerID + `'`,
		"rotate runner token":      `UPDATE job_runner SET token_hash = '\x00' WHERE id = '` + runnerID + `'`,
		"move runner zone":         `UPDATE job_runner SET zone = 'prod' WHERE id = '` + runnerID + `'`,
		"insert runner":            `INSERT INTO job_runner (id, name, token_hash) VALUES ('` + e.id() + `', 'x', '\x00')`,
		"delete runner":            `DELETE FROM job_runner WHERE id = '` + runnerID + `'`,
		"truncate log":             `TRUNCATE job_execution_log`,
		"read another domain":      `SELECT count(*) FROM pg_authid`,
		"create table":             `CREATE TABLE job_engine_scratch (id int)`,
		"drop execution log table": `DROP TABLE job_execution_log`,
	}
	for name, stmt := range forbidden {
		_, err := e.d.Pool.Exec(ctxb(), stmt)
		if err == nil {
			t.Errorf("%s: statement succeeded, want permission denied", name)
			continue
		}
		if !strings.Contains(err.Error(), "permission denied") && !strings.Contains(err.Error(), "must be owner") {
			t.Errorf("%s: unexpected error %v", name, err)
		}
	}
	// 授权范围内的语句可用（其余的由本文件的其它用例在同一角色下覆盖）。
	allowed := map[string]string{
		"read credential":  `SELECT count(*) FROM job_credential`,
		"read project":     `SELECT count(*) FROM job_project`,
		"read runner":      `SELECT count(*) FROM job_runner`,
		"heartbeat column": `UPDATE job_runner SET last_seen = now() WHERE id = '` + runnerID + `'`,
		"read log":         `SELECT count(*) FROM job_execution_log`,
		"read payload":     `SELECT count(*) FROM job_execution_payload`,
		"delete log":       `DELETE FROM job_execution_log WHERE execution_id = '` + id + `'`,
	}
	for name, stmt := range allowed {
		if _, err := e.d.Pool.Exec(ctxb(), stmt); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestLockerAndPing(t *testing.T) {
	e := newEnv(t)
	if err := e.d.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	// 未连接数据库（runner 角色）的 Data 恒为健康。
	if err := (&Data{}).Ping(); err != nil {
		t.Fatalf("ping without pool: %v", err)
	}
	l := NewLocker(e.d)
	release, ok, err := l.TryLock(ctxb(), "job:it-lock")
	if err != nil || !ok {
		t.Fatalf("first lock: %v %v", ok, err)
	}
	if _, ok, err := l.TryLock(ctxb(), "job:it-lock"); err != nil || ok {
		t.Fatalf("second lock must fail: %v %v", ok, err)
	}
	release()
	release2, ok, err := l.TryLock(ctxb(), "job:it-lock")
	if err != nil || !ok {
		t.Fatalf("lock after release: %v %v", ok, err)
	}
	release2()
}

// 快速执行：没有模板与项目的执行照常领取；分发文件的内容从 job_execution_payload 读出（引擎只读）。
func TestQuickExecutionAndPayload(t *testing.T) {
	e := newEnv(t)
	runnerID := e.runner("")
	content := make([]byte, 256<<10)
	for i := range content {
		content[i] = byte(i * 13) // 含 0x00 与全部字节值
	}
	ids := map[int32]string{
		jobcontract.KindCommand: e.execution(execSpec{Kind: jobcontract.KindCommand, Adhoc: &jobcontract.Adhoc{Command: "uptime"}, Age: 3 * time.Minute}),
		jobcontract.KindScript:  e.execution(execSpec{Kind: jobcontract.KindScript, Adhoc: &jobcontract.Adhoc{Script: "id\n", Interpreter: "sh"}, Age: 2 * time.Minute}),
		jobcontract.KindFile: e.execution(execSpec{
			Kind: jobcontract.KindFile, Adhoc: &jobcontract.Adhoc{FileName: "a.bin", Dest: "/tmp", Mode: "0644", FileSize: int64(len(content))},
			Payload: content, Age: time.Minute,
		}),
	}
	for _, kind := range []int32{jobcontract.KindCommand, jobcontract.KindScript, jobcontract.KindFile} {
		token := "lease-" + e.id()
		c, err := e.repo.ClaimNext(ctxb(), runnerID, "", token, time.Minute)
		if err != nil || c == nil || c.ID != ids[kind] {
			t.Fatalf("kind %d: claimed %+v, %v", kind, c, err)
		}
		var spec jobcontract.Spec
		if err := json.Unmarshal(c.Spec, &spec); err != nil || spec.JobKind() != kind || spec.Adhoc == nil || spec.ProjectID != "" {
			t.Fatalf("kind %d: spec %+v, %v", kind, spec, err)
		}
		if hosts, err := e.repo.Hosts(ctxb(), c.ID); err != nil || len(hosts) != 2 {
			t.Fatalf("kind %d: hosts %v, %v", kind, hosts, err)
		}
	}
	got, err := e.repo.Payload(ctxb(), ids[jobcontract.KindFile])
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("payload: %d bytes, %v", len(got), err)
	}
	if _, err := e.repo.Payload(ctxb(), ids[jobcontract.KindCommand]); !errors.Is(err, biz.ErrNotFound) {
		t.Fatalf("payload of a command execution: %v", err)
	}
	// 空文件：读出的是长度为 0 的内容，不是「不存在」。
	empty := e.execution(execSpec{Kind: jobcontract.KindFile, Adhoc: &jobcontract.Adhoc{FileName: "e", Dest: "/tmp", Mode: "0644"}, Payload: []byte{}})
	if got, err := e.repo.Payload(ctxb(), empty); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty payload: %v, %v", got, err)
	}
	// 快速执行的上报与结束同作业模板。
	if err := e.repo.Finish(ctxb(), ids[jobcontract.KindCommand], func(biz.ExecState) (*biz.FinishMutation, error) {
		return &biz.FinishMutation{Status: jobcontract.StatusSuccess, Hosts: []biz.HostUpdate{{Address: "10.0.0.1", Status: jobcontract.HostOK, OK: 1}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if r := e.row(ids[jobcontract.KindCommand]); r.Status != jobcontract.StatusSuccess || r.OK != 1 || r.CommitID != "" {
		t.Fatalf("after finish: %+v", r)
	}
}

// 待审批（9）与审批未通过（10）的执行：不领取、不清扫、日志不按「已结束」清理（9）。
func TestPendingAndRejectedAreNeverTouched(t *testing.T) {
	e := newEnv(t)
	runnerID := e.runner("")
	pending := e.execution(execSpec{Status: jobcontract.StatusPending, Age: 10 * time.Minute})
	rejected := e.execution(execSpec{Status: jobcontract.StatusRejected, Age: 9 * time.Minute})
	queued := e.execution(execSpec{Age: time.Minute})
	// 即便它们带着一个已过期的租约（不应出现），清扫也只看执行中的行。
	e.exec(`UPDATE job_execution SET lease_until = now() - interval '1 hour' WHERE id IN ($1, $2)`, pending, rejected)

	e.claim(runnerID, "", queued)
	e.claim(runnerID, "", "")
	if n, err := e.repo.ExpireLeases(ctxb(), biz.LeaseExpiredReason, 100); err != nil || n != 0 {
		t.Fatalf("sweep = %d, %v", n, err)
	}
	for id, want := range map[string]int32{pending: jobcontract.StatusPending, rejected: jobcontract.StatusRejected} {
		if r := e.row(id); r.Status != want || r.RunnerID != "" || r.LeaseToken != "" || r.Error != "" || r.Started {
			t.Fatalf("status %d execution changed: %+v", want, r)
		}
	}
	// 日志清理：待审批的执行尚未结束，它的日志（不应有，这里人为写入）不删；审批未通过的按已结束处理。
	for _, id := range []string{pending, rejected} {
		e.exec(`INSERT INTO job_execution_log (execution_id, seq, content, create_time) VALUES ($1, 1, 'x', now() - interval '100 days')`, id)
	}
	if n, err := e.repo.PurgeLogs(ctxb(), 90*24*time.Hour, 100); err != nil || n != 1 {
		t.Fatalf("purge = %d, %v", n, err)
	}
	if e.count(`SELECT count(*) FROM job_execution_log WHERE execution_id = $1`, pending) != 1 {
		t.Fatal("log of a pending execution was purged")
	}
}

// 互斥位现在也被待审批（9）的执行占用：结果未知的执行在这种情况下同样不恢复。
func TestReviveBlockedByPendingSuccessor(t *testing.T) {
	e := newEnv(t)
	runnerID := e.runner("")
	tpl := e.id()
	lost := e.execution(execSpec{Template: tpl, Exclusive: true})
	e.claim(runnerID, "", lost)
	e.expire(lost)
	if n, _ := e.repo.ExpireLeases(ctxb(), biz.LeaseExpiredReason, 100); n != 1 {
		t.Fatal("sweep")
	}
	successor := e.execution(execSpec{Template: tpl, Exclusive: true, Status: jobcontract.StatusPending})
	revived, _, err := e.report(lost, biz.ReportMutation{
		Revive: true, Chunks: []jobcontract.LogChunk{{Seq: 1, Content: "back\n"}}, LogSeq: 1, LogBytes: 5, Lease: time.Minute,
	})
	if err != nil || revived {
		t.Fatalf("report: revived=%v err=%v", revived, err)
	}
	if r := e.row(lost); r.Status != jobcontract.StatusUnknown || !r.Finished || r.Error != biz.LeaseExpiredReason || r.LogSeq != 1 || !r.Leased {
		t.Fatalf("must stay unknown while still accepting the report: %+v", r)
	}
	// 审批未通过后互斥位释放：下一次上报即恢复。
	e.exec(`UPDATE job_execution SET status = 10, finish_time = now() WHERE id = $1`, successor)
	if revived, _, err := e.report(lost, biz.ReportMutation{Revive: true, LogSeq: 1, LogBytes: 5, Lease: time.Minute}); err != nil || !revived {
		t.Fatalf("revive after the slot was freed: revived=%v err=%v", revived, err)
	}
}
