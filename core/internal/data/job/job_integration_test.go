//go:build integration

package data

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/kitconf"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 固定的测试用户（作业中心域不读其他域的表，这些只是合法的 UUID）。
const (
	uAlice = "018f3e1a-0e00-7000-8000-00000000000a"
	uBob   = "018f3e1a-0e00-7000-8000-00000000000b"
)

type env struct {
	t           *testing.T
	pool        *pgxpool.Pool
	credentials *credentialRepo
	projects    *projectRepo
	templates   *templateRepo
	executions  *executionRepo
	runners     *runnerRepo
	txs         *basedata.TxManager
	scripts     *scriptRepo
	rules       *riskRuleRepo
	policies    *policyRepo
	schedules   *scheduleRepo
	triggers    *triggerRepo
	fires       *fireRepo
	stats       *statsRepo
}

// testDSN 取连接串并做护栏（库名须含 test）；接收 testing.TB，集成基准（bench_integration_test.go）共用。
func testDSN(t testing.TB) string {
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
	return dsn
}

func reposOf(t *testing.T, d *basedata.Data) *env {
	return &env{
		t: t, pool: d.Pool,
		credentials: NewCredentialRepo(d), projects: NewProjectRepo(d), templates: NewTemplateRepo(d),
		executions: NewExecutionRepo(d), runners: NewRunnerRepo(d),
		txs: basedata.NewTxManager(d), scripts: NewScriptRepo(d), rules: NewRiskRuleRepo(d), policies: NewPolicyRepo(d),
		schedules: NewScheduleRepo(d), triggers: NewTriggerRepo(d), fires: NewFireRepo(d), stats: NewStatsRepo(d),
	}
}

// newEnv 真实连接池上的全部仓储；未配置 TEST_DATABASE_SOURCE 时跳过。会清空作业中心域的表。
func newEnv(t *testing.T) *env {
	t.Helper()
	d, cleanup, err := basedata.NewData(&conf.Data{Database: &kitconf.Database{Source: testDSN(t)}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	if _, err := d.Pool.Exec(context.Background(),
		`TRUNCATE TABLE job_trigger_fire, job_trigger, job_schedule, job_script, job_risk_rule, job_execution_payload,
		 job_execution_log, job_execution_host, job_execution, job_template, job_runner, job_project, job_credential`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	if _, err := d.Pool.Exec(context.Background(),
		"UPDATE job_policy SET protected_envs = '{}', approval_workflow_key = '', adhoc_approval = false, max_hosts = 0, version = 1"); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	return reposOf(t, d)
}

func newID(t *testing.T) string {
	t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func is(err, target error) bool { return kerrors.Is(err, target) }

func (e *env) credential(name string) *biz.Credential {
	e.t.Helper()
	c := &biz.Credential{ID: newID(e.t), Name: name, Type: jc.CredentialSSHKey, Username: "deploy", BecomeMethod: jc.BecomeSudo, Fingerprint: "SHA256:abc"}
	if err := e.credentials.Create(context.Background(), c, []byte("sealed-key"), []byte("sealed-become"), uAlice); err != nil {
		e.t.Fatalf("create credential: %v", err)
	}
	return c
}

func (e *env) project(name string) *biz.Project {
	e.t.Helper()
	p := &biz.Project{ID: newID(e.t), Name: name, RepoURL: "https://git.example.com/ops/p.git", Ref: "main", AuthType: jc.GitAuthToken, Username: "ci"}
	if err := e.projects.Create(context.Background(), p, []byte("sealed-token"), uAlice); err != nil {
		e.t.Fatalf("create project: %v", err)
	}
	return p
}

func (e *env) template(name string, p *biz.Project, c *biz.Credential, exclusive bool) *biz.Template {
	e.t.Helper()
	t := &biz.Template{
		ID: newID(e.t), Name: name, ProjectID: p.ID, Playbook: "site.yml", CredentialID: c.ID,
		Target:     jc.Target{ModelCode: "host", Mode: jc.TargetModeCIs, CIIDs: []string{newID(e.t)}, AddressField: "ip"},
		Params:     []jc.Param{{Code: "version", Name: "版本", Type: jc.ParamString, Required: true}},
		Options:    jc.Options{Forks: 10, TimeoutS: 600, Port: 22, Become: true},
		RunnerZone: "idc-1", Exclusive: exclusive, Enabled: true,
	}
	if err := e.templates.Create(context.Background(), t, uAlice); err != nil {
		e.t.Fatalf("create template: %v", err)
	}
	return t
}

func (e *env) execution(tpl *biz.Template, creator, requestID string, hosts ...biz.Host) (*biz.Execution, string, bool, error) {
	e.t.Helper()
	if len(hosts) == 0 {
		hosts = []biz.Host{
			{CIID: newID(e.t), Name: "web-1", Env: "prod", Address: "10.0.0.1"},
			{CIID: newID(e.t), Name: "web-2", Env: "prod", Address: "10.0.0.2"},
		}
	}
	x := &biz.Execution{
		ID: newID(e.t), TemplateID: tpl.ID, TemplateName: tpl.Name, TemplateVersion: 1, Trigger: jc.TriggerManual,
		Creator: creator, RequestID: requestID,
		Spec: jc.Spec{
			ProjectID: tpl.ProjectID, ProjectName: "p", RepoURL: "https://git.example.com/ops/p.git", Ref: "main", Playbook: tpl.Playbook,
			CredentialID: tpl.CredentialID, CredentialName: "c", ExtraVars: map[string]any{"version": "1.0", "n": float64(2)}, Options: tpl.Options,
		},
		RunnerZone: tpl.RunnerZone, Exclusive: tpl.Exclusive, TimeoutS: tpl.Options.TimeoutS,
	}
	x.Kind, x.Status = jc.KindPlaybook, jc.StatusQueued
	var id string
	var created bool
	err := e.txs.WithinTx(context.Background(), func(tx biz.Tx) error {
		var err error
		id, created, err = e.executions.CreateTx(context.Background(), tx, []biz.NewExecution{{Execution: x, Hosts: hosts}})
		return err
	})
	return x, id, created, err
}

// 凭据：读取不含密文、提权密文的保留 / 清除、乐观锁、名称唯一、引用保护、软删清除密文、删除后名称可复用。
func TestCredentialRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c := e.credential("deploy-key")
	got, err := e.credentials.Get(ctx, c.ID)
	if err != nil || got.Name != "deploy-key" || !got.HasBecomeSecret || got.Fingerprint != "SHA256:abc" || got.Version != 1 || got.UpdateUser != uAlice {
		t.Fatalf("Get() = %+v, %v", got, err)
	}
	if err := e.credentials.Create(ctx, &biz.Credential{ID: newID(t), Name: "deploy-key", Type: 2, Username: "u"}, []byte("x"), nil, ""); !is(err, biz.ErrCredentialNameExists) {
		t.Fatalf("duplicate name: %v", err)
	}
	// 保留两个密文。
	got.Username = "deploy2"
	if err := e.credentials.Update(ctx, got, biz.SecretChange{}, biz.SecretChange{}, 1, uBob); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	var secret, become []byte
	if err := e.pool.QueryRow(ctx, "SELECT secret, become_secret FROM job_credential WHERE id = $1", c.ID).Scan(&secret, &become); err != nil {
		t.Fatal(err)
	}
	if string(secret) != "sealed-key" || string(become) != "sealed-become" {
		t.Fatalf("omitted secrets changed: %q %q", secret, become)
	}
	if err := e.credentials.Update(ctx, got, biz.SecretChange{}, biz.SecretChange{}, 1, uBob); !is(err, biz.ErrVersionConflict) {
		t.Fatalf("stale version: %v", err)
	}
	// 换主密文、清除提权密文。
	if err := e.credentials.Update(ctx, got, biz.SecretChange{Set: true, Sealed: []byte("sealed-new")}, biz.SecretChange{Set: true}, 2, uBob); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	after, _ := e.credentials.Get(ctx, c.ID)
	if after.HasBecomeSecret || after.Version != 3 || after.Username != "deploy2" || after.UpdateUser != uBob {
		t.Fatalf("after update = %+v", after)
	}
	if err := e.credentials.Update(ctx, got, biz.SecretChange{Set: true}, biz.SecretChange{}, 3, uBob); err == nil {
		t.Fatal("empty main secret accepted")
	}

	// 列表、选项、过滤。
	e.credential("other")
	pw := int32(jc.CredentialSSHPassword)
	if items, total, err := e.credentials.List(ctx, biz.CredentialFilter{Keyword: "DEPLOY"}, 1, 10); err != nil || total != 2 || len(items) != 2 {
		t.Fatalf("List(keyword) = %d items, total %d, %v", len(items), total, err)
	}
	if _, total, _ := e.credentials.List(ctx, biz.CredentialFilter{Type: &pw}, 1, 10); total != 0 {
		t.Fatalf("List(type) total = %d", total)
	}
	if opts, err := e.credentials.ListOptions(ctx); err != nil || len(opts) != 2 || opts[0].Name != "deploy-key" {
		t.Fatalf("ListOptions() = %v, %v", opts, err)
	}

	// 引用保护。
	tpl := e.template("deploy", e.project("p"), c, false)
	if got, _ := e.credentials.Get(ctx, c.ID); got.TemplateCount != 1 {
		t.Fatalf("TemplateCount = %d", got.TemplateCount)
	}
	if err := e.credentials.Delete(ctx, c.ID, uBob); !is(err, biz.ErrCredentialInUse) || !strings.Contains(err.Error(), "deploy") {
		t.Fatalf("delete in use: %v", err)
	}
	if err := e.templates.Delete(ctx, tpl.ID, uBob); err != nil {
		t.Fatal(err)
	}
	if err := e.credentials.Delete(ctx, c.ID, uBob); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := e.pool.QueryRow(ctx, "SELECT secret, become_secret FROM job_credential WHERE id = $1", c.ID).Scan(&secret, &become); err != nil {
		t.Fatal(err)
	}
	if len(secret) != 0 || become != nil {
		t.Fatalf("secrets survive deletion: %q %q", secret, become)
	}
	if _, err := e.credentials.Get(ctx, c.ID); !is(err, biz.ErrCredentialNotFound) {
		t.Fatalf("get deleted: %v", err)
	}
	if err := e.credentials.Delete(ctx, c.ID, uBob); !is(err, biz.ErrCredentialNotFound) {
		t.Fatalf("delete twice: %v", err)
	}
	e.credential("deploy-key") // 名称唯一只约束未删除行
}

// 项目：密文保留 / 清除、乐观锁、引用保护。
func TestProjectRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	p := e.project("playbooks")
	got, err := e.projects.Get(ctx, p.ID)
	if err != nil || !got.HasSecret || got.Ref != "main" || got.Username != "ci" {
		t.Fatalf("Get() = %+v, %v", got, err)
	}
	got.Ref = "v1.0"
	if err := e.projects.Update(ctx, got, biz.SecretChange{}, 1, uBob); err != nil {
		t.Fatal(err)
	}
	if after, _ := e.projects.Get(ctx, p.ID); !after.HasSecret || after.Ref != "v1.0" || after.Version != 2 {
		t.Fatalf("after keep = %+v", after)
	}
	if err := e.projects.Update(ctx, got, biz.SecretChange{Set: true}, 2, uBob); err != nil {
		t.Fatal(err)
	}
	if after, _ := e.projects.Get(ctx, p.ID); after.HasSecret {
		t.Fatal("secret not cleared")
	}
	if err := e.projects.Update(ctx, got, biz.SecretChange{}, 2, uBob); !is(err, biz.ErrVersionConflict) {
		t.Fatalf("stale version: %v", err)
	}
	if err := e.projects.Create(ctx, &biz.Project{ID: newID(t), Name: "playbooks", RepoURL: "https://x/y", Ref: "main", AuthType: 1}, nil, ""); !is(err, biz.ErrProjectNameExists) {
		t.Fatalf("duplicate: %v", err)
	}
	if items, total, err := e.projects.List(ctx, "example.com", 1, 10); err != nil || total != 1 || len(items) != 1 {
		t.Fatalf("List() = %d, %d, %v", len(items), total, err)
	}
	if opts, _ := e.projects.ListOptions(ctx); len(opts) != 1 || opts[0].Ref != "v1.0" {
		t.Fatalf("ListOptions() = %+v", opts)
	}
	e.template("t", p, e.credential("c"), false)
	if err := e.projects.Delete(ctx, p.ID, uBob); !is(err, biz.ErrProjectInUse) {
		t.Fatalf("delete in use: %v", err)
	}
}

// 模板：jsonb 往返、展示名随行返回、引用校验、乐观锁、过滤。
func TestTemplateRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	p, c := e.project("p"), e.credential("c")
	tpl := e.template("deploy", p, c, true)
	got, err := e.templates.Get(ctx, tpl.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProjectName != "p" || got.ProjectRef != "main" || got.CredentialName != "c" || !got.Exclusive || got.RunnerZone != "idc-1" ||
		got.Target.AddressField != "ip" || len(got.Target.CIIDs) != 1 || len(got.Params) != 1 || got.Params[0].Code != "version" ||
		got.Options.Forks != 10 || !got.Options.Become {
		t.Fatalf("Get() = %+v", got)
	}
	missing := *got
	missing.ID, missing.Name, missing.ProjectID = newID(t), "x", newID(t)
	if err := e.templates.Create(ctx, &missing, uAlice); !is(err, biz.ErrTemplateInvalid) {
		t.Fatalf("missing project: %v", err)
	}
	missing.ProjectID, missing.CredentialID = p.ID, newID(t)
	if err := e.templates.Create(ctx, &missing, uAlice); !is(err, biz.ErrTemplateInvalid) {
		t.Fatalf("missing credential: %v", err)
	}
	dup := *got
	dup.ID = newID(t)
	if err := e.templates.Create(ctx, &dup, uAlice); !is(err, biz.ErrTemplateNameExists) {
		t.Fatalf("duplicate: %v", err)
	}
	got.Enabled, got.Params = false, []jc.Param{}
	if err := e.templates.Update(ctx, got, 1, uBob); err != nil {
		t.Fatal(err)
	}
	if err := e.templates.Update(ctx, got, 1, uBob); !is(err, biz.ErrVersionConflict) {
		t.Fatalf("stale version: %v", err)
	}
	off := false
	items, total, err := e.templates.List(ctx, biz.TemplateFilter{Keyword: "site", ProjectID: p.ID, Enabled: &off}, 1, 10)
	if err != nil || total != 1 || len(items) != 1 || items[0].Version != 2 || len(items[0].Params) != 0 {
		t.Fatalf("List() = %+v, %d, %v", items, total, err)
	}
	if err := e.templates.Delete(ctx, newID(t), uBob); !is(err, biz.ErrTemplateNotFound) {
		t.Fatalf("delete missing: %v", err)
	}
}

// 执行：建单与主机快照、幂等键（含并发）、模板互斥位、过滤、主机列表、取消的各个分支。
func TestExecutionRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	p, c := e.project("p"), e.credential("c")
	shared, exclusive := e.template("shared", p, c, false), e.template("exclusive", p, c, true)

	x, id, created, err := e.execution(shared, uAlice, "req-1")
	if err != nil || !created || id != x.ID {
		t.Fatalf("Create() = %s, %v, %v", id, created, err)
	}
	got, err := e.executions.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != jc.StatusQueued || got.HostTotal != 2 || got.RequestID != "req-1" || got.Spec.ExtraVars["version"] != "1.0" ||
		got.Spec.ExtraVars["n"] != float64(2) || got.Spec.Options.Port != 22 || got.TimeoutS != 600 || got.RunnerZone != "idc-1" || got.FinishTime != nil {
		t.Fatalf("Get() = %+v", got)
	}
	// 同一执行人同一幂等键：返回先前那条，不新建、不重复写主机。
	if _, again, created, err := e.execution(shared, uAlice, "req-1"); err != nil || created || again != id {
		t.Fatalf("idempotent Create() = %s, %v, %v", again, created, err)
	}
	// 另一个执行人用同一个键：互不影响。
	if _, other, created, err := e.execution(shared, uBob, "req-1"); err != nil || !created || other == id {
		t.Fatalf("other creator = %s, %v, %v", other, created, err)
	}
	// 并发的同键提交只落一条。
	var wg sync.WaitGroup
	ids := make([]string, 8)
	for i := range ids {
		wg.Go(func() {
			if _, got, _, err := e.execution(shared, uAlice, "req-race"); err == nil {
				ids[i] = got
			} else {
				t.Errorf("concurrent create: %v", err)
			}
		})
	}
	wg.Wait()
	for _, got := range ids {
		if got != ids[0] {
			t.Fatalf("concurrent idempotent creates returned different ids: %v", ids)
		}
	}
	var n int
	if err := e.pool.QueryRow(ctx, "SELECT count(*) FROM job_execution WHERE request_id = 'req-race'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("rows for req-race = %d, %v", n, err)
	}
	if err := e.pool.QueryRow(ctx, "SELECT count(*) FROM job_execution_host WHERE execution_id = $1", ids[0]).Scan(&n); err != nil || n != 2 {
		t.Fatalf("hosts for req-race = %d, %v", n, err)
	}

	// 互斥模板：未结束期间拒绝第二条；结束后放行。
	_, first, _, err := e.execution(exclusive, uAlice, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := e.execution(exclusive, uBob, ""); !is(err, biz.ErrExecutionConflict) {
		t.Fatalf("exclusive conflict: %v", err)
	}
	if err := e.executions.Cancel(ctx, first, uBob); err != nil {
		t.Fatal(err)
	}
	canceled, _ := e.executions.Get(ctx, first)
	if canceled.Status != jc.StatusCanceled || !canceled.CancelRequested || canceled.CancelUser != uBob || canceled.FinishTime == nil {
		t.Fatalf("cancel queued = %+v", canceled)
	}
	if _, _, _, err := e.execution(exclusive, uBob, ""); err != nil {
		t.Fatalf("exclusive slot not released: %v", err)
	}
	if err := e.executions.Cancel(ctx, first, uBob); !is(err, biz.ErrExecutionFinished) {
		t.Fatalf("cancel finished: %v", err)
	}
	if err := e.executions.Cancel(ctx, newID(t), uBob); !is(err, biz.ErrExecutionNotFound) {
		t.Fatalf("cancel missing: %v", err)
	}

	// 执行中：只标记请求，保留首个请求人。
	if _, err := e.pool.Exec(ctx, "UPDATE job_execution SET status = 2, start_time = now(), lease_token = 'tok' WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if err := e.executions.Cancel(ctx, id, uBob); err != nil {
		t.Fatal(err)
	}
	if err := e.executions.Cancel(ctx, id, uAlice); err != nil {
		t.Fatal(err)
	}
	if running, _ := e.executions.Get(ctx, id); running.Status != jc.StatusRunning || !running.CancelRequested || running.CancelUser != uBob || running.FinishTime != nil {
		t.Fatalf("cancel running = %+v", running)
	}

	// 结果未知（节点失联）：登记取消请求，状态与结束时间不变。
	if _, err := e.pool.Exec(ctx, "UPDATE job_execution SET status = 8, finish_time = now(), cancel_requested = false, cancel_user = NULL WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if err := e.executions.Cancel(ctx, id, uAlice); err != nil {
		t.Fatalf("cancel unknown: %v", err)
	}
	if lost, _ := e.executions.Get(ctx, id); lost.Status != jc.StatusUnknown || !lost.CancelRequested || lost.CancelUser != uAlice || lost.FinishTime == nil {
		t.Fatalf("cancel unknown = %+v", lost)
	}

	// 地址重复违反唯一约束：整单回滚。
	dupHosts := []biz.Host{{CIID: newID(t), Name: "a", Address: "10.0.0.1"}, {CIID: newID(t), Name: "b", Address: "10.0.0.1"}}
	if _, _, _, err := e.execution(shared, uAlice, "req-dup", dupHosts...); err == nil {
		t.Fatal("duplicate addresses accepted")
	}
	if err := e.pool.QueryRow(ctx, "SELECT count(*) FROM job_execution WHERE request_id = 'req-dup'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("failed create left %d rows, %v", n, err)
	}

	// 过滤。
	if _, total, err := e.executions.List(ctx, biz.ExecutionFilter{}, 1, 10); err != nil || total != 5 {
		t.Fatalf("List(all) total = %d, %v", total, err)
	}
	if _, total, _ := e.executions.List(ctx, biz.ExecutionFilter{TemplateID: exclusive.ID, Statuses: []int32{jc.StatusCanceled}}, 1, 10); total != 1 {
		t.Fatalf("List(template,status) total = %d", total)
	}
	if _, total, _ := e.executions.List(ctx, biz.ExecutionFilter{Creator: uAlice, Keyword: "SHARED"}, 1, 10); total != 2 {
		t.Fatalf("List(creator,keyword) total = %d", total)
	}
	hosts, total, err := e.executions.ListHosts(ctx, id, biz.ExecutionHostFilter{}, 1, 10)
	if err != nil || total != 2 || hosts[0].Seq != 1 || hosts[0].Address != "10.0.0.1" || hosts[0].Port != 22 || hosts[1].Name != "web-2" {
		t.Fatalf("ListHosts() = %+v, %d, %v", hosts, total, err)
	}
	if _, total, _ := e.executions.List(ctx, biz.ExecutionFilter{CIID: hosts[0].CIID}, 1, 10); total != 1 {
		t.Fatalf("List(ci) total = %d", total)
	}
	if _, total, _ := e.executions.ListHosts(ctx, id, biz.ExecutionHostFilter{Statuses: []int32{jc.HostFailed}}, 1, 10); total != 0 {
		t.Fatalf("ListHosts(failed) total = %d", total)
	}
	if hosts, _, _ := e.executions.ListHosts(ctx, id, biz.ExecutionHostFilter{Keyword: "WEB-2", Statuses: []int32{jc.HostPending}}, 1, 10); len(hosts) != 1 {
		t.Fatalf("ListHosts(keyword) = %d", len(hosts))
	}
}

// 日志：按序号增量读取、分批、达到上限即停并可续读。
func TestExecutionLog(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, id, _, err := e.execution(e.template("t", e.project("p"), e.credential("c"), false), uAlice, "")
	if err != nil {
		t.Fatal(err)
	}
	const chunks = 25
	want := ""
	for i := 1; i <= chunks; i++ {
		line := fmt.Sprintf("line %02d\n", i)
		want += line
		if _, err := e.pool.Exec(ctx, "INSERT INTO job_execution_log (execution_id, seq, content) VALUES ($1, $2, $3)", id, i, line); err != nil {
			t.Fatal(err)
		}
	}
	content, last, err := e.executions.ReadLog(ctx, id, 0, 1<<20)
	if err != nil || content != want || last != chunks {
		t.Fatalf("ReadLog(all) last = %d, err = %v, content ok = %v", last, err, content == want)
	}
	// 上限 20 字节：每块 8 字节，读到第 3 块后达到上限。
	got, cursor := "", int64(0)
	for range chunks {
		part, last, err := e.executions.ReadLog(ctx, id, cursor, 20)
		if err != nil {
			t.Fatal(err)
		}
		if last == 0 {
			break
		}
		if len(part) > 24 {
			t.Fatalf("ReadLog returned %d bytes for a 20-byte cap", len(part))
		}
		got, cursor = got+part, last
	}
	if got != want || cursor != chunks {
		t.Fatalf("paged read mismatch: cursor = %d", cursor)
	}
	if part, last, _ := e.executions.ReadLog(ctx, id, chunks, 1<<20); part != "" || last != 0 {
		t.Fatalf("ReadLog(past end) = %q, %d", part, last)
	}
}

// 执行节点：在线判定、心跳列不被编辑覆盖、令牌更换、区域、名称（含已删除）。
func TestRunnerRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	r := &biz.Runner{ID: newID(t), Name: "r1", Zone: "idc-1", Enabled: true}
	if err := e.runners.Create(ctx, r, []byte("hash-1"), uAlice); err != nil {
		t.Fatal(err)
	}
	got, err := e.runners.Get(ctx, r.ID)
	if err != nil || got.Online || got.LastSeen != nil || got.Version != 1 {
		t.Fatalf("Get() = %+v, %v", got, err)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE job_runner SET last_seen = now(), hostname = 'h1', capacity = 4, running = 1, agent_version = '1.0' WHERE id = $1", r.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.runners.Get(ctx, r.ID); !got.Online || got.Capacity != 4 || got.Running != 1 || got.Hostname != "h1" {
		t.Fatalf("after heartbeat = %+v", got)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE job_runner SET last_seen = now() - interval '2 minutes' WHERE id = $1", r.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.runners.Get(ctx, r.ID); got.Online {
		t.Fatal("stale runner reported online")
	}
	// 编辑不动心跳列。
	if err := e.runners.Update(ctx, &biz.Runner{ID: r.ID, Name: "r1", Zone: "dmz", Enabled: false}, 1, uBob); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.runners.Get(ctx, r.ID); got.Zone != "dmz" || got.Enabled || got.Capacity != 4 || got.Version != 2 {
		t.Fatalf("after update = %+v", got)
	}
	if err := e.runners.Update(ctx, &biz.Runner{ID: r.ID, Name: "r1"}, 1, uBob); !is(err, biz.ErrVersionConflict) {
		t.Fatalf("stale version: %v", err)
	}
	if err := e.runners.SetToken(ctx, r.ID, []byte("hash-2"), uBob); err != nil {
		t.Fatal(err)
	}
	var hash []byte
	if err := e.pool.QueryRow(ctx, "SELECT token_hash FROM job_runner WHERE id = $1", r.ID).Scan(&hash); err != nil || string(hash) != "hash-2" {
		t.Fatalf("token hash = %q, %v", hash, err)
	}
	if err := e.runners.Create(ctx, &biz.Runner{ID: newID(t), Name: "r1"}, []byte("h"), ""); !is(err, biz.ErrRunnerNameExists) {
		t.Fatalf("duplicate: %v", err)
	}
	if err := e.runners.Create(ctx, &biz.Runner{ID: newID(t), Name: "r2", Zone: "idc-1"}, []byte("h"), ""); err != nil {
		t.Fatal(err)
	}
	if zones, err := e.runners.Zones(ctx); err != nil || len(zones) != 2 || zones[0] != "dmz" || zones[1] != "idc-1" {
		t.Fatalf("Zones() = %v, %v", zones, err)
	}
	if items, total, err := e.runners.List(ctx, "DMZ", 1, 10); err != nil || total != 1 || len(items) != 1 || items[0].Name != "r1" {
		t.Fatalf("List(keyword) = %d, %d, %v", len(items), total, err)
	}
	if _, total, _ := e.runners.List(ctx, "", 1, 10); total != 2 {
		t.Fatalf("List(all) total = %d", total)
	}
	if err := e.runners.Delete(ctx, r.ID, uBob); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(ctx, "SELECT token_hash FROM job_runner WHERE id = $1", r.ID).Scan(&hash); err != nil || len(hash) != 0 {
		t.Fatalf("token hash survives deletion: %q, %v", hash, err)
	}
	if err := e.runners.SetToken(ctx, r.ID, []byte("x"), uBob); !is(err, biz.ErrRunnerNotFound) {
		t.Fatalf("reset deleted: %v", err)
	}
	if names, err := e.runners.Names(ctx, []string{r.ID}); err != nil || names[r.ID] != "r1" {
		t.Fatalf("Names() = %v, %v", names, err)
	}
}

// 以「仅是 job_core_role 成员」的账号运行：core 用到的语句全部可用，越权语句被数据库拒绝
// （不能改写执行内容与主机结果、不能删执行记录、不能写日志）。
func TestCoreRoleIsRestricted(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	const role = "job_core_itest"
	if _, err := e.pool.Exec(ctx, "DROP ROLE IF EXISTS "+role); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, "CREATE ROLE "+role+" NOLOGIN IN ROLE job_core_role"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = e.pool.Exec(context.Background(), "DROP ROLE IF EXISTS "+role) })

	// 受限连接池：每条连接建立后 SET ROLE。
	cfg, err := pgxpool.ParseConfig(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET ROLE "+role)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	r := reposOf(t, &basedata.Data{Pool: pool})

	// core 的完整写路径在受限角色下可用。
	p, c := r.project("p"), r.credential("c")
	tpl := r.template("t", p, c, true)
	_, id, _, err := r.execution(tpl, uAlice, "req-1")
	if err != nil {
		t.Fatalf("create execution as core role: %v", err)
	}
	if _, _, err := r.executions.List(ctx, biz.ExecutionFilter{}, 1, 10); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.executions.ReadLog(ctx, id, 0, 1024); err != nil {
		t.Fatal(err)
	}
	if err := r.executions.Cancel(ctx, id, uBob); err != nil {
		t.Fatalf("cancel as core role: %v", err)
	}
	rn := &biz.Runner{ID: newID(t), Name: "r"}
	if err := r.runners.Create(ctx, rn, []byte("h"), ""); err != nil {
		t.Fatal(err)
	}
	if err := r.credentials.Delete(ctx, c.ID, ""); !is(err, biz.ErrCredentialInUse) {
		t.Fatalf("delete in use as core role: %v", err)
	}

	denied := map[string]string{
		"rewrite spec":        "UPDATE job_execution SET spec = '{}' WHERE id = '" + id + "'",
		"rewrite host counts": "UPDATE job_execution SET host_ok = 99 WHERE id = '" + id + "'",
		"take a lease":        "UPDATE job_execution SET lease_token = 'x', runner_id = NULL WHERE id = '" + id + "'",
		"delete execution":    "DELETE FROM job_execution WHERE id = '" + id + "'",
		"rewrite host result": "UPDATE job_execution_host SET status = 1 WHERE execution_id = '" + id + "'",
		"delete host":         "DELETE FROM job_execution_host WHERE execution_id = '" + id + "'",
		"write log":           "INSERT INTO job_execution_log (execution_id, seq, content) VALUES ('" + id + "', 1, 'x')",
		"delete log":          "DELETE FROM job_execution_log",
		"other domain":        `SELECT count(*) FROM "system_user"`,
	}
	for name, stmt := range denied {
		if _, err := pool.Exec(ctx, stmt); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: expected permission denied, got %v", name, err)
		}
	}
}
