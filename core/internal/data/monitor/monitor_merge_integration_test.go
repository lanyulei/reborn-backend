//go:build integration

package data

import (
	"context"
	"errors"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 本文件是各功能合到一起之后才测得到的路径：事件动作循环在真实仓储上的全链路（动作行 → 外部域 →
// 事件的关联列 + 时间线 + 动作终态），以及规则保存与升级策略删除之间的互斥。

// mergeDesk 工单域的替身：按幂等键返回同一张工单，记下评论。
type mergeDesk struct {
	t        *testing.T
	byReq    map[string]*biz.TicketInfo
	created  int
	comments []string
}

func (d *mergeDesk) Create(_ context.Context, p biz.TicketCreate) (*biz.TicketInfo, error) {
	if got, ok := d.byReq[p.Creator+"|"+p.RequestID]; ok {
		return got, nil
	}
	d.created++
	info := &biz.TicketInfo{ID: newID(d.t), Number: "INC-20261118-0001", Title: p.Title, Status: 1, Creator: p.Creator}
	d.byReq[p.Creator+"|"+p.RequestID] = info
	return info, nil
}

func (d *mergeDesk) View(context.Context, string, string) (*biz.TicketInfo, error) { return nil, nil }

func (d *mergeDesk) Comment(_ context.Context, _, userID, content string) error {
	d.comments = append(d.comments, userID+": "+content)
	return nil
}

type mergeCatalog struct{}

func (mergeCatalog) Templates(context.Context) ([]biz.TicketTemplate, error) { return nil, nil }

func (mergeCatalog) Template(_ context.Context, id, _ string) (*biz.TicketTemplate, error) {
	return &biz.TicketTemplate{ID: id, Name: "故障单"}, nil
}

type mergeFlows struct{ instance string }

func (mergeFlows) Definitions(context.Context) ([]biz.WorkflowDef, error) { return nil, nil }

func (mergeFlows) Definition(_ context.Context, key string) (*biz.WorkflowDef, error) {
	return &biz.WorkflowDef{Key: key, Name: "自愈"}, nil
}

func (f mergeFlows) Start(context.Context, biz.WorkflowStart) (string, error) { return f.instance, nil }

type mergeCMDB struct{}

func (mergeCMDB) Card(context.Context, string, string) (*biz.CICard, bool, error) {
	return nil, false, nil
}

func (mergeCMDB) Impact(context.Context, string, string, int, int) ([]biz.CIImpact, bool, error) {
	return nil, false, nil
}

type mergeSummary struct{}

func (mergeSummary) Available() bool { return true }

func (mergeSummary) Summarize(context.Context, string, string) (string, error) {
	return "磁盘写满，先清理日志。", nil
}

func TestActionUsecase_EndToEndOnPostgres(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(mc.RootGroupID, "动作")
	r := e.rule(g.ID, "磁盘告警")
	alertID := e.alert(alertRow{ruleID: r.ID, ruleName: "磁盘告警", groupID: g.ID, severity: 3, status: mc.AlertFiring})
	ops, desk, flows := NewAlertOpsRepo(e.d), &mergeDesk{t: t, byReq: map[string]*biz.TicketInfo{}}, mergeFlows{instance: newID(t)}
	uc, err := biz.NewActionUsecase(biz.NewActionDeps(basedata.NewTxManager(e.d), NewActionRepo(e.d), e.alerts, ops, ops, ops, NewCIIndexRepo(e.d),
		biz.NewScopeUsecase(e.org, e.scopes, e.groups), e.org, mergeCatalog{}, desk, flows, mergeCMDB{}, mergeSummary{}, quietLogger(), &conf.Monitor{}))
	if err != nil {
		t.Fatalf("NewActionUsecase() error = %v", err)
	}
	agent := `{"template_id":"` + newID(t) + `","agent_user_id":"` + uAlice + `","definition_key":"heal"}`
	ticket := e.seedAction(alertID, mc.ActionTicket, agent)
	flow := e.seedAction(alertID, mc.ActionWorkflow, agent)
	ai := e.seedAction(alertID, mc.ActionAISummary, "{}")
	if n, err := uc.Run(ctx); err != nil || n != 3 {
		t.Fatalf("Run() = %d, %v; want 3 actions executed", n, err)
	}
	for name, id := range map[string]string{"ticket": ticket, "workflow": flow, "ai": ai} {
		if s := e.actionState(id); s.status != mc.ActionDone || !s.done {
			t.Fatalf("%s action = %+v, want done", name, s)
		}
	}
	a, err := e.alerts.Get(ctx, alertID)
	if err != nil || a.TicketID == "" || a.WorkflowInstanceID != flows.instance || a.AISummary != "磁盘写满，先清理日志。" {
		t.Fatalf("alert links = %+v, err=%v", a, err)
	}
	// 事件详情的工单卡片从时间线取编号：自动建单必须留下带编号的 ticket 条目。
	links, err := ops.TicketLinks(ctx, []string{alertID})
	if err != nil || links[alertID].TicketID != a.TicketID || links[alertID].Number != "INC-20261118-0001" || !links[alertID].Auto ||
		links[alertID].Creator != uAlice {
		t.Fatalf("ticket link = %+v, err=%v", links[alertID], err)
	}
	for _, typ := range []string{mc.AlertEventTicket, mc.AlertEventWorkflow, mc.AlertEventAISummary} {
		if n := e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = $2", alertID, typ); n != 1 {
			t.Fatalf("timeline entries of %s = %d, want 1", typ, n)
		}
	}
	// 再跑一轮：没有待执行的动作，不重复建单、不重复写时间线。
	if n, err := uc.Run(ctx); err != nil || n != 0 || desk.created != 1 {
		t.Fatalf("second Run() = %d, %v, tickets created = %d", n, err, desk.created)
	}
	// 告警恢复：在已关联的工单上以提单人的名义追加一条记录。
	note := e.seedAction(alertID, mc.ActionTicketNote, "{}")
	if n, err := uc.Run(ctx); err != nil || n != 1 || len(desk.comments) != 1 {
		t.Fatalf("ticket note Run() = %d, %v, comments = %v", n, err, desk.comments)
	}
	if s := e.actionState(note); s.status != mc.ActionDone {
		t.Fatalf("ticket note action = %+v", s)
	}
	if n := e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = $2", alertID, mc.AlertEventTicketNote); n != 1 {
		t.Fatalf("ticket_note timeline entries = %d", n)
	}
}

func TestRuleWrite_ExcludesEscalationDelete(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(mc.RootGroupID, "升级")
	esc := NewEscalationRepo(e.d)
	policy := func(name string) *biz.Escalation {
		p := &biz.Escalation{ID: newID(t), Spec: biz.EscalationSpec{
			GroupID: g.ID, Name: name, Mode: mc.EscalationModeStack, MinSeverity: 1, Enabled: true,
			Levels: []mc.EscalationLevel{{Trigger: mc.EscalationTriggerUnacked, AfterM: 10, Targets: mc.EscalationTargets{UserIDs: []string{uAlice}}}},
		}}
		if err := esc.Create(ctx, p, uAdmin); err != nil {
			t.Fatalf("create escalation: %v", err)
		}
		return p
	}
	gone := policy("已删除")
	if err := esc.Delete(ctx, gone.ID, 1, uAdmin); err != nil {
		t.Fatalf("delete escalation: %v", err)
	}
	// 新引用一个已删除的策略：写事务内拒绝（用例层的校验与删除之间有窗口）。
	fresh := &biz.Rule{ID: newID(t), Spec: ruleSpec(g.ID, "新引用")}
	fresh.Spec.EscalationID = gone.ID
	if err := e.rules.Create(ctx, fresh, biz.RuleChange{Action: mc.RuleActionCreate, Operator: uAlice}); !errors.Is(err, biz.ErrRuleEscalationMissing) {
		t.Fatalf("Create(deleted escalation) error = %v, want ErrRuleEscalationMissing", err)
	}
	// 规则原本就引用着的策略后来没了（历史数据）：不妨碍保存别的改动。
	live := policy("在用")
	kept := &biz.Rule{ID: newID(t), Spec: ruleSpec(g.ID, "已引用")}
	kept.Spec.EscalationID = live.ID
	if err := e.rules.Create(ctx, kept, biz.RuleChange{Action: mc.RuleActionCreate, Operator: uAlice}); err != nil {
		t.Fatalf("Create(live escalation) error = %v", err)
	}
	if err := esc.Delete(ctx, live.ID, 1, uAdmin); !errors.Is(err, biz.ErrEscalationInUse) {
		t.Fatalf("Delete(referenced) error = %v, want ErrEscalationInUse", err)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_escalation SET delete_time = now() WHERE id = $1", live.ID); err != nil {
		t.Fatal(err)
	}
	kept.Spec.Description = "改说明"
	if err := e.rules.Update(ctx, kept, kept.Version, biz.RuleChange{Action: mc.RuleActionUpdate, Operator: uAlice}); err != nil {
		t.Fatalf("Update(kept reference to a deleted escalation) error = %v", err)
	}
	// 互斥：规则写事务持有策略的共享锁期间，删除等待（这里以超时观察）。
	held := policy("锁住")
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockEscalation(ctx, tx, newID(t), held.ID); err != nil {
		t.Fatalf("lockEscalation() error = %v", err)
	}
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if err := esc.Delete(short, held.ID, 1, uAdmin); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Delete while a rule write holds the policy = %v, want it to wait", err)
	}
}

// 告警恢复时引擎只要规则开了自动建单就会入队「工单追加恢复记录」，而工单可能还没建出来：建单没有结论之前
// 不领取它；建单最终没有产生工单时它记为「无需执行」，不会无限重试。
func TestActionRepo_TicketNoteWaitsForTicket(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(mc.RootGroupID, "动作顺序")
	r := e.rule(g.ID, "磁盘告警")
	alertID := e.alert(alertRow{ruleID: r.ID, ruleName: "磁盘告警", groupID: g.ID, severity: 3, status: mc.AlertResolved})
	repo := NewActionRepo(e.d)
	ticket := e.seedAction(alertID, mc.ActionTicket, `{"template_id":"`+newID(t)+`","agent_user_id":"`+uCarol+`"}`)
	note := e.seedAction(alertID, mc.ActionTicketNote, "{}")
	// 让恢复记录排在建单之前到期：仍然只领到建单。
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert_action SET next_time = now() - interval '1 minute' WHERE id = $1", note); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Claim(ctx, 10, time.Minute)
	if err != nil || len(got) != 1 || got[0].ID != ticket {
		t.Fatalf("Claim() = %+v, %v; want only the ticket action", got, err)
	}
	// 建单以失败告终（没有工单）：恢复记录随后可领取，执行结论是「无需执行」。
	ops := NewAlertOpsRepo(e.d)
	uc, err := biz.NewActionUsecase(biz.NewActionDeps(basedata.NewTxManager(e.d), repo, e.alerts, ops, ops, ops, NewCIIndexRepo(e.d),
		biz.NewScopeUsecase(e.org, e.scopes, e.groups), e.org, mergeCatalog{}, &mergeDesk{t: t, byReq: map[string]*biz.TicketInfo{}}, mergeFlows{},
		mergeCMDB{}, mergeSummary{}, quietLogger(), &conf.Monitor{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert_action SET status = $2, done_time = now() WHERE id = $1", ticket, mc.ActionFailed); err != nil {
		t.Fatal(err)
	}
	if n, err := uc.Run(ctx); err != nil || n != 1 {
		t.Fatalf("Run() = %d, %v", n, err)
	}
	if s := e.actionState(note); s.status != mc.ActionSkipped || !s.done {
		t.Fatalf("ticket note without a ticket = %+v, want skipped", s)
	}
}
