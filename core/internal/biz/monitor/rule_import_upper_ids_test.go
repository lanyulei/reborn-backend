package biz_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// JSON 导入文件里的引用 ID 是大写 UUID 时不再被当成「已不存在或不可用」而从规则里去掉（monitor 模块评审
// core-monitor-rules P3-3）：UUID 不区分大小写，目录与仓储按库里的小写 ID 比对。
func TestRuleP2_ImportJSONKeepsReferencesWrittenInUpperCase(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	ds := "018f3e1a-0d00-7000-8000-0000000000d1"
	user := "018f3e1a-0d00-7000-8000-0000000000d2"
	sched := "018f3e1a-0d00-7000-8000-0000000000d3"
	dept := "018f3e1a-0d00-7000-8000-0000000000d4"
	if err := e.store.Datasources().Create(ctx, &biz.Datasource{
		ID: ds, Name: "vm-prod", URL: "http://vm-prod", AuthType: mc.AuthNone, TimeoutMS: 5000, Enabled: true,
	}, nil, uAdmin); err != nil {
		t.Fatalf("seed datasource: %v", err)
	}
	e.org.Users[user] = "张三"
	e.org.Schedules[sched] = "一线值班"
	e.org.Depts[dept] = "平台部"

	s := spec(gHost, "upper-ids")
	s.DatasourceScope, s.DatasourceIDs = mc.DatasourceScopeSpecified, []string{ds}
	s.Notify = mc.NotifyTargets{UserIDs: []string{user}, ScheduleIDs: []string{sched}, DeptIDs: []string{dept}}
	s.EscalationID = escInfra
	s.Actions = mc.RuleActions{
		Ticket:   mc.TicketAction{Enabled: true, TemplateID: tplOKD, AgentUserID: agentOK},
		Workflow: mc.WorkflowAction{Enabled: true, DefinitionKey: flowOK, AgentUserID: agentOK},
	}
	src := e.mustRule(t, uAdmin, s)
	content, n, err := e.rules.Export(ctx, uAdmin, []string{src.ID}, "", biz.ExportJSON)
	if err != nil || n != 1 {
		t.Fatalf("Export: %v, n = %d", err, n)
	}
	// 文件经别的工具改写过：引用 ID 全部成了大写。
	ids := []string{ds, user, sched, dept, escInfra, tplOKD, agentOK}
	for _, id := range ids {
		if !strings.Contains(content, id) {
			t.Fatalf("exported file lacks %s:\n%s", id, content)
		}
		content = strings.ReplaceAll(content, id, strings.ToUpper(id))
	}

	items, err := e.rules.PreviewImport(ctx, uAdmin, biz.ImportOptions{
		Content: content, GroupID: gInfra, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: 2,
	})
	if err != nil || len(items) != 1 || items[0].Status != biz.ImportNew {
		t.Fatalf("PreviewImport: %v %+v", err, items)
	}
	if len(items[0].Warnings) != 0 {
		t.Fatalf("valid references must be kept, warnings = %v", items[0].Warnings)
	}
	got := items[0].Spec
	if !slices.Equal(got.DatasourceIDs, []string{ds}) || !slices.Equal(got.Notify.UserIDs, []string{user}) ||
		!slices.Equal(got.Notify.ScheduleIDs, []string{sched}) || !slices.Equal(got.Notify.DeptIDs, []string{dept}) {
		t.Fatalf("datasource / notify references = %+v", got)
	}
	if got.EscalationID != escInfra || got.Actions != s.Actions {
		t.Fatalf("escalation / action references = %+v", got)
	}
	// 流程标识不是 UUID，保持原样。
	if got.Actions.Workflow.DefinitionKey != flowOK {
		t.Fatalf("definition key = %q, want %q", got.Actions.Workflow.DefinitionKey, flowOK)
	}
}
