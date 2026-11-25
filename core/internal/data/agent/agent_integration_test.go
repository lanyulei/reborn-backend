//go:build integration

package data

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/kitconf"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newIntegrationAgentData 连接 TEST_DATABASE_SOURCE（库名须含 test）并清空 agent 表。
// 接收 testing.TB：集成基准（bench_integration_test.go）复用同一份护栏与清库。
func newIntegrationAgentData(t testing.TB) *basedata.Data {
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
	if _, err := d.Pool.Exec(context.Background(), "TRUNCATE TABLE agent_budget, agent_run, agent_task, agent_profile_history, agent_profile CASCADE"); err != nil {
		t.Fatalf("truncate agent tables: %v", err)
	}
	return d
}

func mustID(t *testing.T) string {
	t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func sampleProfile(t *testing.T, name string) *biz.Profile {
	now := time.Now()
	return &biz.Profile{
		ID: mustID(t), Version: 1, CreateTime: now, UpdateTime: now, CreateUser: mustID(t), UpdateUser: mustID(t),
		Spec: biz.ProfileSpec{
			Name: name, Kind: biz.KindDiagnose, AgentUserID: mustID(t), Priority: 100, Enabled: true, Severities: []int32{1, 2, 3}, HourlyLimit: 30,
			Stages: biz.DefaultStages(), Budget: biz.DefaultBudget(), Auto: biz.DefaultAuto(),
		},
	}
}

func TestProfileRepo_CRUD(t *testing.T) {
	d := newIntegrationAgentData(t)
	ctx := context.Background()
	repo := NewProfileRepo(d)
	p := sampleProfile(t, "诊断-默认")
	if err := repo.Create(ctx, p); err != nil {
		t.Fatalf("Create: %v", err)
	}
	dup := sampleProfile(t, "诊断-默认")
	if err := repo.Create(ctx, dup); err != biz.ErrProfileNameExists {
		t.Fatalf("Create duplicate name: got %v, want ErrProfileNameExists", err)
	}
	got, err := repo.Get(ctx, p.ID)
	if err != nil || got.Spec.Name != "诊断-默认" || len(got.Spec.Stages) != 4 {
		t.Fatalf("Get: %+v, %v", got, err)
	}
	enabled, err := repo.ListEnabled(ctx, biz.KindDiagnose)
	if err != nil || len(enabled) != 1 {
		t.Fatalf("ListEnabled: %d, %v", len(enabled), err)
	}
	p.Spec.Description = "改过"
	if err := repo.Update(ctx, p, 1); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := repo.Update(ctx, p, 1); err != biz.ErrVersionConflict {
		t.Fatalf("Update stale version: got %v, want ErrVersionConflict", err)
	}
	if err := repo.SetBreaker(ctx, p.ID, true, "测试"); err != nil {
		t.Fatalf("SetBreaker: %v", err)
	}
	got, _ = repo.Get(ctx, p.ID)
	if !got.BreakerOpen || got.BreakerReason != "测试" || got.Version != 2 {
		t.Fatalf("after breaker: %+v", got)
	}
	before := p.Spec
	if err := repo.AppendHistory(ctx, &biz.ProfileHistory{ID: mustID(t), ProfileID: p.ID, Version: 2, ActorID: p.UpdateUser, Before: &before, After: p.Spec, Note: "编辑", CreateTime: time.Now()}); err != nil {
		t.Fatalf("AppendHistory: %v", err)
	}
	hist, total, err := repo.ListHistory(ctx, p.ID, 1, 20)
	if err != nil || total != 1 || hist[0].Before == nil || hist[0].After.Name != "诊断-默认" {
		t.Fatalf("ListHistory: %d %v %v", total, err, hist)
	}
	items, total, err := repo.List(ctx, "诊断", nil, 1, 20)
	if err != nil || total != 1 || len(items) != 1 {
		t.Fatalf("List: %d %v", total, err)
	}
	if err := repo.Delete(ctx, p.ID, 2, p.UpdateUser); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := repo.Get(ctx, p.ID); err != biz.ErrProfileNotFound {
		t.Fatalf("Get after delete: %v", err)
	}
	// 已删除的仍可按 ID 批量取名（任务列表显示）。
	byIDs, err := repo.ListByIDs(ctx, []string{p.ID})
	if err != nil || len(byIDs) != 1 {
		t.Fatalf("ListByIDs after delete: %d %v", len(byIDs), err)
	}
}

func TestTaskRepo_QueueLifecycle(t *testing.T) {
	d := newIntegrationAgentData(t)
	ctx := context.Background()
	repo := NewTaskRepo(d)
	txm := basedata.NewTxManager(d)
	incident := mustID(t)
	now := time.Now()
	newTask := func(sev int32, source int32, dry bool) *biz.Task {
		return &biz.Task{
			ID: mustID(t), Kind: biz.KindDiagnose, Source: source, DryRun: dry, IncidentID: incident, IncidentNumber: "INC-1", IncidentTitle: "t",
			Severity: sev, Reason: biz.ReasonOpened, Status: biz.TaskQueued, NextTime: now, QueuedTime: now, UpdateTime: now,
		}
	}
	// 同事务入队：第二次命中按故障唯一 → 只更新等级。
	if err := txm.WithinTx(ctx, func(tx txrun.Tx) error { return repo.EnqueueTx(ctx, tx, newTask(3, biz.SourceIncident, false)) }); err != nil {
		t.Fatalf("EnqueueTx: %v", err)
	}
	if err := txm.WithinTx(ctx, func(tx txrun.Tx) error { return repo.EnqueueTx(ctx, tx, newTask(1, biz.SourceIncident, false)) }); err != nil {
		t.Fatalf("EnqueueTx bump: %v", err)
	}
	active, err := repo.ActiveByIncident(ctx, incident, biz.KindDiagnose)
	if err != nil || active == nil || active.Severity != 1 {
		t.Fatalf("ActiveByIncident: %+v %v", active, err)
	}
	// 人工召唤撞上活跃任务 → ErrTaskExists；干跑不受限。
	if err := repo.Create(ctx, newTask(2, biz.SourceManual, false)); err != biz.ErrTaskExists {
		t.Fatalf("Create duplicate: got %v, want ErrTaskExists", err)
	}
	dry := newTask(2, biz.SourceDryRun, true)
	if err := repo.Create(ctx, dry); err != nil {
		t.Fatalf("Create dry run: %v", err)
	}
	// 领取：两条（S1 的在前）。
	claimed, err := repo.Claim(ctx, 5, time.Minute)
	if err != nil || len(claimed) != 2 || claimed[0].ID != active.ID || claimed[0].Status != biz.TaskRunning || claimed[0].LockToken == "" || claimed[0].Attempts != 1 {
		t.Fatalf("Claim: %d %v %+v", len(claimed), err, claimed)
	}
	again, _ := repo.Claim(ctx, 5, time.Minute)
	if len(again) != 0 {
		t.Fatalf("Claim again: %d, want 0", len(again))
	}
	first := claimed[0]
	if ok, err := repo.Renew(ctx, first.ID, first.LockToken, time.Minute); err != nil || !ok {
		t.Fatalf("Renew: %v %v", ok, err)
	}
	if ok, _ := repo.Renew(ctx, first.ID, mustID(t), time.Minute); ok {
		t.Fatal("Renew with wrong token should fail")
	}
	runID := mustID(t)
	if err := repo.BindRun(ctx, first.ID, first.LockToken, runID, mustID(t)); err != nil {
		t.Fatalf("BindRun: %v", err)
	}
	// 退避重试 → 回到队列，next_time 在未来，不会被立刻领取。
	if ok, err := repo.Requeue(ctx, first.ID, first.LockToken, time.Hour, "模型不可用"); err != nil || !ok {
		t.Fatalf("Requeue: %v %v", ok, err)
	}
	if more, _ := repo.Claim(ctx, 5, time.Minute); len(more) != 0 {
		t.Fatalf("Claim after requeue: %d, want 0", len(more))
	}
	got, _ := repo.Get(ctx, first.ID)
	if got.Status != biz.TaskQueued || got.RunID != runID || got.Result != "模型不可用" {
		t.Fatalf("after requeue: %+v", got)
	}
	// 取消排队中的任务。
	if ok, err := repo.Cancel(ctx, first.ID, "手动取消"); err != nil || !ok {
		t.Fatalf("Cancel: %v %v", ok, err)
	}
	if active, _ := repo.ActiveByIncident(ctx, incident, biz.KindDiagnose); active != nil {
		t.Fatalf("ActiveByIncident after cancel: %+v", active)
	}
	// 干跑任务完成。
	dryRun := claimed[1]
	if ok, err := repo.Finish(ctx, dryRun.ID, dryRun.LockToken, biz.TaskSucceeded, ""); err != nil || !ok {
		t.Fatalf("Finish: %v %v", ok, err)
	}
	if ok, _ := repo.Finish(ctx, dryRun.ID, dryRun.LockToken, biz.TaskFailed, ""); ok {
		t.Fatal("Finish twice should not match")
	}
	// 失联回收与排队超时。
	stale := newTask(2, biz.SourceManual, false)
	if err := repo.Create(ctx, stale); err != nil {
		t.Fatalf("Create stale: %v", err)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE agent_task SET status = 'running', lock_token = gen_random_uuid(), locked_until = now() - interval '1 minute' WHERE id = $1", stale.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := repo.ReleaseStale(ctx, 10); err != nil || n != 1 {
		t.Fatalf("ReleaseStale: %d %v", n, err)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE agent_task SET queued_time = now() - interval '1 hour', attempts = 0 WHERE id = $1", stale.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := repo.ExpireQueued(ctx, time.Now().Add(-10*time.Minute), 10); err != nil || n != 1 {
		t.Fatalf("ExpireQueued: %d %v", n, err)
	}
	counts, err := repo.CountByStatus(ctx, biz.StatsRange{Start: now.Add(-2 * time.Hour), End: now.Add(time.Hour)})
	if err != nil || counts[biz.TaskCancelled] != 1 || counts[biz.TaskSucceeded] != 1 || counts[biz.TaskExpired] != 1 {
		t.Fatalf("CountByStatus: %v %v", counts, err)
	}
	items, total, err := repo.List(ctx, biz.TaskFilter{IncidentID: incident}, 1, 10)
	if err != nil || total != 3 || len(items) != 3 {
		t.Fatalf("List: %d %v", total, err)
	}
	if n, err := repo.Purge(ctx, time.Now().Add(time.Minute), 10); err != nil || n != 3 {
		t.Fatalf("Purge: %d %v", n, err)
	}
}

func TestRunAndBudgetRepo(t *testing.T) {
	d := newIntegrationAgentData(t)
	ctx := context.Background()
	runs := NewRunRepo(d)
	budgets := NewBudgetRepo(d)
	profiles := NewProfileRepo(d)
	p := sampleProfile(t, "统计用")
	if err := profiles.Create(ctx, p); err != nil {
		t.Fatal(err)
	}
	incident := mustID(t)
	start := time.Now().Add(-time.Minute)
	run := &biz.Run{
		ID: mustID(t), TaskID: mustID(t), ProfileID: p.ID, IncidentID: incident, AgentUserID: p.Spec.AgentUserID, ConversationID: mustID(t), Status: biz.RunRunning,
		Stages: []biz.StageTrace{{Key: biz.StageCollect, Status: biz.StagePending}}, StartTime: start, CreateTime: start,
	}
	if err := runs.Create(ctx, run); err != nil {
		t.Fatalf("Create: %v", err)
	}
	end := time.Now()
	run.Status, run.EndReason, run.EndTime, run.PromptTokens, run.CompletionTokens = biz.RunSucceeded, "completed", &end, 1200, 300
	run.Finding = &biz.Finding{Title: "疑似变更引入", Confidence: 85, Evidence: []string{"a"}}
	run.FindingEventID = mustID(t)
	run.Proposals = []biz.Ref{{ID: mustID(t), Name: "重启"}}
	run.Stages[0].Status = biz.StageDone
	if ok, err := runs.SaveRunning(ctx, run); err != nil || !ok {
		t.Fatalf("SaveRunning: %v %v", ok, err)
	}
	// 已终态的运行不能再被 SaveRunning 覆盖；Close 也不再生效。
	if ok, err := runs.SaveRunning(ctx, run); err != nil || ok {
		t.Fatalf("SaveRunning on finished run should be a no-op: %v %v", ok, err)
	}
	if ok, err := runs.Close(ctx, run.ID, biz.RunFailed, "error", time.Now()); err != nil || ok {
		t.Fatalf("Close on finished run should be a no-op: %v %v", ok, err)
	}
	got, err := runs.Get(ctx, run.ID)
	if err != nil || got.Finding == nil || got.Finding.Confidence != 85 || len(got.Proposals) != 1 || got.Stages[0].Status != biz.StageDone || got.EndTime == nil {
		t.Fatalf("Get: %+v %v", got, err)
	}
	if err := runs.SetFeedback(ctx, run.ID, biz.FeedbackHelpful, mustID(t), "准", time.Now()); err != nil {
		t.Fatalf("SetFeedback: %v", err)
	}
	list, err := runs.ListByIncident(ctx, incident, 10)
	if err != nil || len(list) != 1 || list[0].Feedback != biz.FeedbackHelpful {
		t.Fatalf("ListByIncident: %v %v", list, err)
	}
	overview, byProfile, err := runs.Stats(ctx, biz.StatsRange{Start: start.Add(-time.Hour), End: end.Add(time.Hour)})
	if err != nil || overview.Runs != 1 || overview.Succeeded != 1 || overview.PromptTokens != 1200 || overview.Proposals != 1 || overview.Helpful != 1 || len(byProfile) != 1 || byProfile[0].ProfileName != "统计用" {
		t.Fatalf("Stats: %+v %+v %v", overview, byProfile, err)
	}
	if total, failed, err := runs.RecentAutoOutcome(ctx, p.ID, 24*time.Hour); err != nil || total != 0 || failed != 0 {
		t.Fatalf("RecentAutoOutcome: %d %d %v", total, failed, err)
	}
	day := "d:2026-01-01"
	for i := 0; i < 2; i++ {
		if err := budgets.Add(ctx, p.ID, day, 1, 100, 10); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	b, err := budgets.Get(ctx, p.ID, day)
	if err != nil || b.Tasks != 2 || b.PromptTokens != 200 || b.CompletionTokens != 20 {
		t.Fatalf("Get budget: %+v %v", b, err)
	}
	if empty, err := budgets.Get(ctx, p.ID, "d:1970-01-01"); err != nil || empty.Tasks != 0 {
		t.Fatalf("Get empty budget: %+v %v", empty, err)
	}
	total, err := budgets.Total(ctx, day)
	if err != nil || total.PromptTokens != 200 {
		t.Fatalf("Total: %+v %v", total, err)
	}
	purged, err := runs.PurgeBefore(ctx, time.Now().Add(time.Minute), 10)
	if err != nil || purged != 1 {
		t.Fatalf("PurgeBefore: %v %v", purged, err)
	}
}
