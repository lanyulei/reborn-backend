package biz_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 第二、三期测试用的固定对象（需要落 uuid 列或经 UUID 校验的引用用真 UUID）。
const (
	escGlobal   = "018f3e1a-0e00-7000-8000-000000000001"
	escInfra    = "018f3e1a-0e00-7000-8000-000000000002"
	escApp      = "018f3e1a-0e00-7000-8000-000000000003"
	escDisabled = "018f3e1a-0e00-7000-8000-000000000004"
	escGone     = "018f3e1a-0e00-7000-8000-000000000005"
	tplOKD      = "018f3e1a-0e01-7000-8000-000000000001"
	tplOff      = "018f3e1a-0e01-7000-8000-000000000002"
	agentOK     = "018f3e1a-0e02-7000-8000-000000000001"
	agentGone   = "018f3e1a-0e02-7000-8000-000000000002"
	flowOK      = "restart-service"
	ciWebD      = "018f3e1a-0e03-7000-8000-000000000001"
	ciHostD     = "018f3e1a-0e03-7000-8000-000000000002"
	oncallOps   = "018f3e1a-0e04-7000-8000-000000000001"
)

// fakeEscalations biz.EscalationReader 的内存桩。
type fakeEscalations map[string]*biz.Escalation

func (f fakeEscalations) Get(_ context.Context, id string) (*biz.Escalation, error) {
	if e, ok := f[id]; ok {
		return e, nil
	}
	return nil, biz.ErrEscalationNotFound
}

func (f fakeEscalations) ListByIDs(_ context.Context, ids []string) ([]*biz.Escalation, error) {
	var out []*biz.Escalation
	for _, id := range ids {
		if e, ok := f[id]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f fakeEscalations) List(context.Context, biz.EscalationFilter, int64, int64) ([]*biz.Escalation, int64, error) {
	return nil, 0, nil
}

// fakeTickets biz.TicketCatalog：tplOKD 对任何人可用，其余不可用。
type fakeTickets struct{}

func (fakeTickets) Templates(context.Context) ([]biz.TicketTemplate, error) {
	return []biz.TicketTemplate{{ID: tplOKD, Name: "故障单", CategoryName: "运维"}}, nil
}

func (fakeTickets) Template(_ context.Context, templateID, _ string) (*biz.TicketTemplate, error) {
	if templateID == tplOKD {
		return &biz.TicketTemplate{ID: tplOKD, Name: "故障单", CategoryName: "运维"}, nil
	}
	return nil, nil
}

// fakeFlows biz.Workflows：只有 flowOK 可发起。
type fakeFlows struct{}

func (fakeFlows) Definitions(context.Context) ([]biz.WorkflowDef, error) {
	return []biz.WorkflowDef{{Key: flowOK, Name: "重启服务"}}, nil
}

func (fakeFlows) Definition(_ context.Context, key string) (*biz.WorkflowDef, error) {
	if key == flowOK {
		return &biz.WorkflowDef{Key: flowOK, Name: "重启服务"}, nil
	}
	return nil, nil
}

func (fakeFlows) Start(context.Context, biz.WorkflowStart) (string, error) { return "", nil }

type fakeCMDBCatalog struct{}

func (fakeCMDBCatalog) Models(context.Context) ([]biz.CIModel, error) {
	return []biz.CIModel{{Code: "host", Name: "主机"}}, nil
}

type fakeSummarizer bool

func (f fakeSummarizer) Available() bool { return bool(f) }

func (fakeSummarizer) Summarize(context.Context, string, string) (string, error) { return "", nil }

func level(after int32, users ...string) mc.EscalationLevel {
	return mc.EscalationLevel{Trigger: mc.EscalationTriggerUnacked, AfterM: after, Targets: mc.EscalationTargets{UserIDs: users}}
}

func escalation(id, groupID, name string, enabled bool) *biz.Escalation {
	return &biz.Escalation{ID: id, Spec: biz.EscalationSpec{
		GroupID: groupID, Name: name, Mode: mc.EscalationModeStack, MinSeverity: 1, Enabled: enabled, Levels: []mc.EscalationLevel{level(10, uAlice)},
	}}
}

// p2Env 在第一期的测试装置上挂第二、三期的协作者。
type p2Env struct {
	*env
	escalations fakeEscalations
}

func newP2EnvD(t *testing.T) *p2Env {
	t.Helper()
	e := newEnv(t)
	e.org.Users[agentOK] = "机器人"
	e.org.Disabled[agentGone] = "已离职的机器人"
	p := &p2Env{env: e, escalations: fakeEscalations{
		escGlobal: escalation(escGlobal, "", "全局升级", true), escInfra: escalation(escInfra, gInfra, "infra 升级", true),
		escApp: escalation(escApp, gApp, "app 升级", true), escDisabled: escalation(escDisabled, gInfra, "停用的", false),
	}}
	e.rules.WithP2(biz.RuleExt{Escalations: p.escalations, Tickets: fakeTickets{}, Workflows: fakeFlows{}, MatchAttrs: []string{"ip", "sn"}})
	return p
}

func TestRuleP2_PipelineValidation(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	bad := map[string]func(s *biz.RuleSpec){
		"relabel unknown action": func(s *biz.RuleSpec) { s.Relabel = []mc.RelabelStep{{Action: "hashmod"}} },
		"relabel bad regex":      func(s *biz.RuleSpec) { s.Relabel = []mc.RelabelStep{{Action: mc.RelabelLabelDrop, Regex: "("}} },
		"relabel replace no target": func(s *biz.RuleSpec) {
			s.Relabel = []mc.RelabelStep{{Action: mc.RelabelReplace, SourceLabels: []string{"a"}}}
		},
		"relabel too many steps": func(s *biz.RuleSpec) {
			s.Relabel = slices.Repeat([]mc.RelabelStep{{Action: mc.RelabelLabelDrop, Regex: "tmp_.*"}}, 11)
		},
		"drop_expr bad template":     func(s *biz.RuleSpec) { s.DropExpr = "{{ if }}" },
		"drop_expr too long":         func(s *biz.RuleSpec) { s.DropExpr = strings.Repeat("x", mc.MaxDropExprBytes+1) },
		"ci_match by without label":  func(s *biz.RuleSpec) { s.CIMatch = mc.CIMatch{By: mc.CIMatchByName} },
		"ci_match attr unregistered": func(s *biz.RuleSpec) { s.CIMatch = mc.CIMatch{Label: "instance", By: mc.CIMatchByAttr, Attr: "mac"} },
		"scope values when disabled": func(s *biz.RuleSpec) { s.ResourceScope = mc.ResourceScope{Envs: []string{"prod"}} },
		"effective mode with windows": func(s *biz.RuleSpec) {
			s.Effective = mc.Effective{Mode: mc.EffectiveModeWorkTime, Windows: []mc.TimeWindow{{Days: []int32{1}, Start: "09:00", End: "18:00"}}}
		},
		"escalation id not uuid":     func(s *biz.RuleSpec) { s.EscalationID = "nope" },
		"ticket enabled no template": func(s *biz.RuleSpec) { s.Actions.Ticket = mc.TicketAction{Enabled: true, AgentUserID: agentOK} },
		"workflow enabled no agent":  func(s *biz.RuleSpec) { s.Actions.Workflow = mc.WorkflowAction{Enabled: true, DefinitionKey: flowOK} },
	}
	for name, mutate := range bad {
		s := spec(gHost, "bad "+name)
		mutate(&s)
		if _, err := e.rules.Create(ctx, uAlice, s); !isReason(err, biz.ErrRuleInvalid) {
			t.Errorf("%s: err = %v, want MONITOR_RULE_INVALID", name, err)
		}
	}
	good := spec(gHost, "full pipeline")
	good.Relabel = []mc.RelabelStep{
		{Action: mc.RelabelReplace, SourceLabels: []string{"instance"}, Regex: `(.+):\d+`, TargetLabel: "host"},
		{Action: mc.RelabelLabelDrop, Regex: "tmp_.*"},
	}
	good.DropExpr = `  {{ if eq $labels.ci_env "dev" }}true{{ end }}  `
	good.CIMatch = mc.CIMatch{Label: "instance", By: mc.CIMatchByAttr, Attr: "sn", ModelCodes: []string{"host"}, EnrichTags: []string{"core"}}
	good.ResourceScope = mc.ResourceScope{Enabled: true, Envs: []string{"prod"}}
	good.Effective = mc.Effective{Mode: mc.EffectiveModeOffTime}
	good.TopologyInhibit, good.Notify.CIOwner = true, true
	r := e.mustRule(t, uAlice, good)
	if r.Spec.DropExpr != strings.TrimSpace(good.DropExpr) || len(r.Spec.Relabel) != 2 || !r.Spec.Notify.CIOwner || r.Spec.Effective.Mode != mc.EffectiveModeOffTime {
		t.Fatalf("saved spec = %+v", r.Spec)
	}
	// 版本快照带上新字段，回滚后原样恢复。
	upd := r.Spec
	upd.Relabel, upd.DropExpr, upd.TopologyInhibit = nil, "", false
	r2, err := e.rules.Update(ctx, uAlice, r.ID, upd, r.Version, "")
	if err != nil || len(r2.Spec.Relabel) != 0 {
		t.Fatalf("Update: %v %+v", err, r2)
	}
	if r2.Spec.Relabel == nil {
		t.Error("relabel must be normalised to an empty slice, not nil")
	}
	back, err := e.rules.Rollback(ctx, uAlice, r.ID, 1, r2.Version)
	if err != nil || len(back.Spec.Relabel) != 2 || back.Spec.DropExpr == "" || !back.Spec.TopologyInhibit || back.Spec.CIMatch.Attr != "sn" {
		t.Fatalf("Rollback: %v %+v", err, back)
	}
}

// isReason 报告 err 是否与哨兵错误同一 code + reason。
func isReason(err error, want *kerrors.Error) bool { return kerrors.Is(err, want) }

func TestRuleP2_SpecTimeBumpsOnlyForPipeline(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	r := e.mustRule(t, uAlice, spec(gHost, "bump"))
	step := func(what string, mutate func(s *biz.RuleSpec), wantBump bool) {
		t.Helper()
		before := r.SpecTime
		time.Sleep(2 * time.Millisecond)
		s := r.Spec
		mutate(&s)
		next, err := e.rules.Update(ctx, uAlice, r.ID, s, r.Version, "")
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if bumped := next.SpecTime.After(before); bumped != wantBump {
			t.Errorf("%s: spec_time bumped = %v, want %v", what, bumped, wantBump)
		}
		r = next
	}
	step("relabel", func(s *biz.RuleSpec) { s.Relabel = []mc.RelabelStep{{Action: mc.RelabelLabelDrop, Regex: "tmp_.*"}} }, true)
	step("drop_expr", func(s *biz.RuleSpec) { s.DropExpr = `{{ if eq $labels.env "dev" }}true{{ end }}` }, true)
	step("ci_match", func(s *biz.RuleSpec) { s.CIMatch = mc.CIMatch{Disabled: true} }, true)
	step("resource_scope", func(s *biz.RuleSpec) { s.ResourceScope = mc.ResourceScope{Enabled: true, ModelCodes: []string{"host"}} }, true)
	step("topology_inhibit", func(s *biz.RuleSpec) { s.TopologyInhibit = true }, false)
	step("escalation", func(s *biz.RuleSpec) { s.EscalationID = escGlobal }, false)
	step("actions", func(s *biz.RuleSpec) { s.Actions.AISummary = true }, false)
	step("ci_owner", func(s *biz.RuleSpec) { s.Notify.CIOwner = true }, false)
	step("no change", func(*biz.RuleSpec) {}, false)
}

func TestRuleP2_EscalationReference(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	create := func(group, esc string) error {
		s := spec(group, "esc "+group+esc)
		s.EscalationID = esc
		_, err := e.rules.Create(ctx, uAdmin, s)
		return err
	}
	for _, c := range []struct {
		group, esc string
		ok         bool
	}{
		{gHost, escGlobal, true},    // 全局策略
		{gHost, escInfra, true},     // 上级分组的策略
		{gInfra, escInfra, true},    // 本分组的策略
		{gHost, escApp, false},      // 别的分组的策略
		{gHost, escDisabled, false}, // 已停用
		{gHost, escGone, false},     // 不存在
	} {
		if err := create(c.group, c.esc); (err == nil) != c.ok || (!c.ok && !isReason(err, biz.ErrRuleInvalid)) {
			t.Errorf("create in %s with %s: err = %v, want ok=%v", c.group, c.esc, err, c.ok)
		}
	}
	// 不可用的三种情形给出同一句话：不泄露别的分组里有没有这条策略。
	msgs := map[string]bool{}
	for _, esc := range []string{escApp, escDisabled, escGone} {
		msgs[create(gHost, esc).Error()] = true
	}
	if len(msgs) != 1 {
		t.Errorf("unusable escalation messages differ: %v", msgs)
	}

	// 保存前就引用的策略被停用 / 删除后，不相关的编辑照常能保存。
	s := spec(gHost, "kept")
	s.EscalationID = escInfra
	r := e.mustRule(t, uAdmin, s)
	e.escalations[escInfra].Spec.Enabled = false
	edit := r.Spec
	edit.Description = "改个说明"
	r, err := e.rules.Update(ctx, uAdmin, r.ID, edit, r.Version, "")
	if err != nil {
		t.Fatalf("update with a disabled kept escalation: %v", err)
	}
	delete(e.escalations, escInfra)
	edit = r.Spec
	edit.Description = "再改"
	if r, err = e.rules.Update(ctx, uAdmin, r.ID, edit, r.Version, ""); err != nil {
		t.Fatalf("update with a deleted kept escalation: %v", err)
	}
	// 换成另一条不可用的策略仍被拒绝。
	edit = r.Spec
	edit.EscalationID = escDisabled
	if _, err := e.rules.Update(ctx, uAdmin, r.ID, edit, r.Version, ""); !isReason(err, biz.ErrRuleInvalid) {
		t.Errorf("switch to a disabled escalation: err = %v", err)
	}
	// 移到别的分组：保留的策略须能被新分组引用。
	e.escalations[escInfra] = escalation(escInfra, gInfra, "infra 升级", false)
	edit = r.Spec
	edit.GroupID = gApp
	if _, err := e.rules.Update(ctx, uAdmin, r.ID, edit, r.Version, ""); !isReason(err, biz.ErrRuleInvalid) {
		t.Errorf("move with an escalation the new group cannot use: err = %v", err)
	}
	edit.EscalationID = ""
	if _, err := e.rules.Update(ctx, uAdmin, r.ID, edit, r.Version, ""); err != nil {
		t.Errorf("move after clearing the escalation: %v", err)
	}
}

func TestRuleP2_CloneDropsUnusableEscalation(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	s := spec(gHost, "clone src")
	s.EscalationID = escInfra
	src := e.mustRule(t, uAdmin, s)
	same, err := e.rules.Clone(ctx, uAdmin, src.ID, "same group", "")
	if err != nil || same.Spec.EscalationID != escInfra {
		t.Fatalf("clone in group: %v %+v", err, same)
	}
	other, err := e.rules.Clone(ctx, uAdmin, src.ID, "other group", gApp)
	if err != nil || other.Spec.EscalationID != "" {
		t.Fatalf("clone to another group must drop the escalation: %v %+v", err, other)
	}
}

func TestRuleP2_Actions(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	with := func(name string, a mc.RuleActions) biz.RuleSpec {
		s := spec(gHost, name)
		s.Actions = a
		return s
	}
	for name, a := range map[string]mc.RuleActions{
		"template unusable":  {Ticket: mc.TicketAction{Enabled: true, TemplateID: tplOff, AgentUserID: agentOK}},
		"ticket agent gone":  {Ticket: mc.TicketAction{Enabled: true, TemplateID: tplOKD, AgentUserID: agentGone}},
		"workflow unknown":   {Workflow: mc.WorkflowAction{Enabled: true, DefinitionKey: "nope", AgentUserID: agentOK}},
		"workflow agent bad": {Workflow: mc.WorkflowAction{Enabled: true, DefinitionKey: flowOK, AgentUserID: agentGone}},
	} {
		if _, err := e.rules.Create(ctx, uAlice, with(name, a)); !isReason(err, biz.ErrRuleInvalid) {
			t.Errorf("%s: err = %v, want MONITOR_RULE_INVALID", name, err)
		}
	}
	ok := mc.RuleActions{
		Ticket:   mc.TicketAction{Enabled: true, TemplateID: tplOKD, AgentUserID: agentOK},
		Workflow: mc.WorkflowAction{Enabled: true, DefinitionKey: flowOK, AgentUserID: agentOK}, AISummary: true,
	}
	r := e.mustRule(t, uAlice, with("actions ok", ok))
	// 关闭的动作不校验引用。
	e.mustRule(t, uAlice, with("disabled action", mc.RuleActions{Ticket: mc.TicketAction{TemplateID: tplOff, AgentUserID: agentGone}}))
	// 代理人离职后，配置没动的规则照常能保存；改了动作配置才重新校验。
	delete(e.org.Users, agentOK)
	e.org.Disabled[agentOK] = "机器人"
	edit := r.Spec
	edit.Description = "x"
	r, err := e.rules.Update(ctx, uAlice, r.ID, edit, r.Version, "")
	if err != nil {
		t.Fatalf("update with unchanged actions: %v", err)
	}
	edit = r.Spec
	edit.Actions.Workflow.DefinitionKey = flowOK
	edit.Actions.Ticket.TemplateID = tplOff
	if _, err := e.rules.Update(ctx, uAlice, r.ID, edit, r.Version, ""); !isReason(err, biz.ErrRuleInvalid) {
		t.Errorf("changed ticket action with an inactive agent: err = %v", err)
	}
}

func TestRuleP2_Refs(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	s := spec(gHost, "refs")
	s.EscalationID = escGlobal
	s.Actions = mc.RuleActions{
		Ticket:   mc.TicketAction{Enabled: true, TemplateID: tplOKD, AgentUserID: agentOK},
		Workflow: mc.WorkflowAction{Enabled: true, DefinitionKey: flowOK, AgentUserID: agentOK},
	}
	a := e.mustRule(t, uAlice, s)
	b := e.mustRule(t, uAlice, spec(gHost, "no refs"))
	delete(e.escalations, escGlobal)
	refs, err := e.rules.Refs(ctx, []*biz.RuleView{a, b})
	if err != nil {
		t.Fatal(err)
	}
	ra, rb := refs[a.ID], refs[b.ID]
	if ra.Escalation == nil || !ra.Escalation.Missing || ra.Escalation.ID != escGlobal {
		t.Errorf("deleted escalation ref = %+v", ra.Escalation)
	}
	if ra.TicketTemplate == nil || ra.TicketTemplate.Name != "故障单" || ra.TicketAgent.Name != "机器人" ||
		ra.WorkflowDefinition.ID != flowOK || ra.WorkflowDefinition.Name != "重启服务" || ra.WorkflowAgent.Name != "机器人" {
		t.Errorf("refs = %+v", ra)
	}
	if rb.Escalation != nil || rb.TicketTemplate != nil || rb.TicketAgent != nil || rb.WorkflowDefinition != nil || rb.WorkflowAgent != nil {
		t.Errorf("rule without references must have nil refs: %+v", rb)
	}
}

func TestRuleP2_BatchSetEscalation(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	host := e.mustRule(t, uAdmin, spec(gHost, "host rule"))
	app := e.mustRule(t, uAdmin, spec(gApp, "app rule"))
	id := escInfra
	res, err := e.rules.Batch(ctx, uAdmin, biz.BatchInput{IDs: []string{host.ID, app.ID}, Action: biz.BatchSetEscalation, EscalationID: &id})
	if err != nil {
		t.Fatal(err)
	}
	if !res[0].OK || res[1].OK || res[1].Reason != biz.ErrRuleInvalid.Reason {
		t.Fatalf("results = %+v", res)
	}
	if got, _ := e.rules.Get(ctx, uAdmin, host.ID); got.Spec.EscalationID != escInfra || got.Version != host.Version+1 {
		t.Errorf("host rule = %+v", got.Spec)
	}
	// 不传 = 取消；已经没有策略的规则不产生新版本。
	res, err = e.rules.Batch(ctx, uAdmin, biz.BatchInput{IDs: []string{host.ID, app.ID}, Action: biz.BatchSetEscalation})
	if err != nil || !res[0].OK || !res[1].OK {
		t.Fatalf("clear: %v %+v", err, res)
	}
	if got, _ := e.rules.Get(ctx, uAdmin, host.ID); got.Spec.EscalationID != "" {
		t.Errorf("escalation not cleared: %+v", got.Spec)
	}
	if got, _ := e.rules.Get(ctx, uAdmin, app.ID); got.Version != app.Version {
		t.Errorf("no-op must not create a version: %d → %d", app.Version, got.Version)
	}
	// 参数与操作不匹配；只读用户逐条被拒。
	if _, err := e.rules.Batch(ctx, uAdmin, biz.BatchInput{IDs: []string{host.ID}, Action: biz.BatchEnable, EscalationID: &id}); !isReason(err, biz.ErrRuleBatchInvalid) {
		t.Errorf("escalation_id on enable: err = %v", err)
	}
	res, err = e.rules.Batch(ctx, uBob, biz.BatchInput{IDs: []string{host.ID}, Action: biz.BatchSetEscalation, EscalationID: &id})
	if err != nil || res[0].OK || res[0].Reason != biz.ErrForbidden.Reason {
		t.Errorf("read-only user: %v %+v", err, res)
	}
	// 移动分组时，规则上的策略须能被目标分组引用。
	if _, err := e.rules.Batch(ctx, uAdmin, biz.BatchInput{IDs: []string{host.ID}, Action: biz.BatchSetEscalation, EscalationID: &id}); err != nil {
		t.Fatal(err)
	}
	target := gApp
	res, err = e.rules.Batch(ctx, uAdmin, biz.BatchInput{IDs: []string{host.ID}, Action: biz.BatchMove, GroupID: &target})
	if err != nil || res[0].OK {
		t.Errorf("move with an escalation the target group cannot use: %v %+v", err, res)
	}
}

func TestRuleP2_ExportImportJSON(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	s := spec(gHost, "exported")
	s.Relabel = []mc.RelabelStep{{Action: mc.RelabelLabelDrop, Regex: "tmp_.*"}}
	s.DropExpr = `{{ if eq $labels.env "dev" }}true{{ end }}`
	s.CIMatch = mc.CIMatch{Label: "ident", By: mc.CIMatchByName, EnrichTags: []string{"core"}}
	s.ResourceScope = mc.ResourceScope{Enabled: true, ModelCodes: []string{"host"}}
	s.TopologyInhibit, s.EscalationID, s.Notify.CIOwner = true, escInfra, true
	s.Effective = mc.Effective{Mode: mc.EffectiveModeWorkTime}
	s.Actions = mc.RuleActions{
		Ticket:   mc.TicketAction{Enabled: true, TemplateID: tplOKD, AgentUserID: agentOK},
		Workflow: mc.WorkflowAction{Enabled: true, DefinitionKey: flowOK, AgentUserID: agentOK}, AISummary: true,
	}
	src := e.mustRule(t, uAdmin, s)
	content, n, err := e.rules.Export(ctx, uAdmin, []string{src.ID}, "", biz.ExportJSON)
	if err != nil || n != 1 {
		t.Fatalf("Export: %v n=%d", err, n)
	}
	var file struct {
		Format string
		Rules  []struct {
			Escalation struct{ ID, Name string }
			Actions    struct {
				Ticket struct {
					Enabled         bool
					Template, Agent struct{ ID, Name string }
				}
				Workflow struct{ Definition struct{ ID, Name string } }
			}
			Notify struct {
				CIOwner bool `json:"ci_owner"`
			}
			Effective struct{ Mode string }
		}
	}
	if err := json.Unmarshal([]byte(content), &file); err != nil {
		t.Fatal(err)
	}
	fr := file.Rules[0]
	if file.Format != biz.RuleExportFormat || fr.Escalation.Name != "infra 升级" || fr.Actions.Ticket.Template.Name != "故障单" ||
		fr.Actions.Ticket.Agent.Name != "机器人" || fr.Actions.Workflow.Definition.ID != flowOK || !fr.Notify.CIOwner || fr.Effective.Mode != mc.EffectiveModeWorkTime {
		t.Fatalf("exported file = %+v\n%s", fr, content)
	}
	opts := biz.ImportOptions{Content: content, GroupID: gInfra, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: 2}

	// 同环境再导入：全部保留。
	items, err := e.rules.PreviewImport(ctx, uAdmin, opts)
	if err != nil || len(items) != 1 || items[0].Status != biz.ImportNew || len(items[0].Warnings) != 0 {
		t.Fatalf("PreviewImport: %v %+v", err, items)
	}
	got := items[0].Spec
	if got.EscalationID != escInfra || !got.Actions.Ticket.Enabled || !got.Actions.Workflow.Enabled || !got.Actions.AISummary ||
		len(got.Relabel) != 1 || got.DropExpr != s.DropExpr || got.CIMatch.Label != "ident" || !got.ResourceScope.Enabled ||
		!got.TopologyInhibit || !got.Notify.CIOwner || got.Effective.Mode != mc.EffectiveModeWorkTime {
		t.Fatalf("round-tripped spec = %+v", got)
	}
	// 目标分组不能引用该升级策略、模板停用、流程代理人离职：引用被去掉并给出提示，规则仍可导入。
	opts.GroupID = gApp
	delete(e.org.Users, agentOK)
	e.org.Disabled[agentOK] = "机器人"
	items, err = e.rules.PreviewImport(ctx, uAdmin, opts)
	if err != nil || items[0].Status != biz.ImportNew || len(items[0].Warnings) != 3 {
		t.Fatalf("PreviewImport(unusable refs): %v %+v", err, items[0])
	}
	got = items[0].Spec
	if got.EscalationID != "" || got.Actions.Ticket != (mc.TicketAction{}) || got.Actions.Workflow != (mc.WorkflowAction{}) || !got.Actions.AISummary {
		t.Fatalf("unusable refs must be dropped: %+v", got)
	}
	res, err := e.rules.Import(ctx, uAdmin, opts, biz.ConflictSkip, false)
	if err != nil || res[0].Action != biz.ImportCreated {
		t.Fatalf("Import: %v %+v", err, res)
	}
	// 文件里的引用 ID 不合法：按没有引用处理，不报内部错误。
	bad := strings.Replace(content, escInfra, "not-a-uuid", 1)
	opts.Content = bad
	if items, err = e.rules.PreviewImport(ctx, uAdmin, opts); err != nil || items[0].Spec == nil || items[0].Spec.EscalationID != "" {
		t.Fatalf("PreviewImport(bad id): %v %+v", err, items)
	}
	// 关联属性没有在部署配置里登记：该条不合法。
	opts.Content = strings.Replace(content, `"label": "ident"`, `"label": "ident", "attr": "mac"`, 1)
	opts.Content = strings.Replace(opts.Content, `"by": "name"`, `"by": "attr"`, 1)
	if items, err = e.rules.PreviewImport(ctx, uAdmin, opts); err != nil || items[0].Status != biz.ImportInvalid {
		t.Fatalf("PreviewImport(unregistered attr): %v %+v", err, items[0])
	}
	// 第一期导出的文件（没有新键）照常导入，新字段为零值。
	old := `{"format":"reborn.monitor.rules/v1","export_time":"2026-11-17T12:00:00Z","rules":[{"name":"旧规则","enabled":true,
	  "group":{"id":"x","name":"y"},"labels":{},"datasource_scope":2,"datasources":[],"queries":[{"expr":"up == 0","severity":3}],
	  "level_inhibit":true,"eval_interval_s":60,"for_s":60,"recover_s":0,"nodata_keep_s":0,"effective":{"timezone":"","windows":[]},
	  "annotations":{},"recovery_expr":"","notify":{"users":[],"schedules":[],"depts":[]},"notify_recovery":true,
	  "repeat_interval_m":0,"max_notify":0,"group_key_labels":[]}]}`
	opts.Content = old
	items, err = e.rules.PreviewImport(ctx, uAdmin, opts)
	if err != nil || items[0].Status != biz.ImportNew || len(items[0].Warnings) != 0 || items[0].Spec.EscalationID != "" || len(items[0].Spec.Relabel) != 0 {
		t.Fatalf("PreviewImport(P1 file): %v %+v", err, items[0])
	}
}

// 每个内置模板包都能原样走导入预览与导入：没有不合法的条目。
func TestRuleP2_TemplatePacksImportCleanly(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	packs := e.rules.TemplatePacks()
	if len(packs) < 6 {
		t.Fatalf("packs = %d", len(packs))
	}
	total := 0
	for _, pack := range packs {
		d, err := e.rules.TemplatePack(pack.Code)
		if err != nil {
			t.Fatalf("TemplatePack(%s): %v", pack.Code, err)
		}
		opts := biz.ImportOptions{Content: d.Content, GroupID: gApp, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: 2}
		items, err := e.rules.PreviewImport(ctx, uCarol, opts)
		if err != nil {
			t.Fatalf("PreviewImport(%s): %v", pack.Code, err)
		}
		for _, it := range items {
			if it.Status != biz.ImportNew || len(it.Warnings) != 0 {
				t.Errorf("pack %s rule %q: status %s error %q warnings %v", pack.Code, it.Name, it.Status, it.Error, it.Warnings)
			}
		}
		res, err := e.rules.Import(ctx, uCarol, opts, biz.ConflictSkip, false)
		if err != nil || len(res) != int(pack.RuleCount) {
			t.Fatalf("Import(%s): %v, %d results for %d rules", pack.Code, err, len(res), pack.RuleCount)
		}
		for _, r := range res {
			if r.Action != biz.ImportCreated {
				t.Errorf("pack %s rule %q: action %s error %q", pack.Code, r.Name, r.Action, r.Error)
			}
		}
		total += len(res)
	}
	if _, n, err := e.rules.List(ctx, uCarol, biz.RuleListQuery{GroupID: gApp}, 1, 1); err != nil || int(n) != total {
		t.Errorf("rules in group = %d, want %d (err %v)", n, total, err)
	}
	if _, err := e.rules.TemplatePack("nope"); !isReason(err, biz.ErrRuleTemplateNotFound) {
		t.Errorf("unknown pack: err = %v", err)
	}
}

func TestRuleOptions(t *testing.T) {
	t.Parallel()
	envs := &testfake.MonitorCIEnvs{}
	envs.Set("prod", "", "dev", "prod")
	uc := biz.NewRuleOptionUsecase(fakeTickets{}, fakeFlows{}, fakeCMDBCatalog{}, envs, fakeSummarizer(true), &conf.Monitor{CiMatchAttrs: []string{" ip ", "sn", "ip"}})
	o, err := uc.Options(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(o.TicketTemplates) != 1 || len(o.Workflows) != 1 || len(o.CIModels) != 1 || !slices.Equal(o.CIEnvs, []string{"dev", "prod"}) ||
		!slices.Equal(o.CIMatchAttrs, []string{"ip", "sn"}) || !o.AIAvailable {
		t.Fatalf("options = %+v", o)
	}
	// 未配置匹配属性：缺省只有 ip；未配置模型密钥。
	o, err = biz.NewRuleOptionUsecase(fakeTickets{}, fakeFlows{}, fakeCMDBCatalog{}, envs, fakeSummarizer(false), nil).Options(context.Background())
	if err != nil || !slices.Equal(o.CIMatchAttrs, []string{"ip"}) || o.AIAvailable {
		t.Fatalf("default options = %+v err %v", o, err)
	}
}
