package service

import (
	"slices"
	"strings"
	"testing"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
)

// 规则接口入参里以 UUID 识别的 ID 一律转小写（monitor 模块评审 core-monitor-rules P3-3）：UUID 不区分
// 大小写（接口校验同样不区分），库里、目录与数据范围的比对却一律是小写。
func TestToRuleSpec_LowercasesReferencedIDs(t *testing.T) {
	const ds, user, sched, dept, esc, tpl, agent = "018f3e1a-0001-7000-8000-000000000001",
		"018f3e1a-0002-7000-8000-000000000002", "018f3e1a-0003-7000-8000-000000000003", "018f3e1a-0004-7000-8000-000000000004",
		"018f3e1a-0005-7000-8000-000000000005", "018f3e1a-0006-7000-8000-000000000006", "018f3e1a-0007-7000-8000-000000000007"
	upper := strings.ToUpper
	in := &v1.RuleSpec{
		GroupId: upper(ds), Name: "n", DatasourceIds: []string{upper(ds)},
		Notify:       &v1.RuleNotify{UserIds: []string{upper(user)}, ScheduleIds: []string{upper(sched)}, DeptIds: []string{upper(dept)}, CiOwner: true},
		EscalationId: upper(esc),
		Actions: &v1.RuleActions{
			Ticket:    &v1.RuleTicketAction{Enabled: true, TemplateId: upper(tpl), AgentUserId: upper(agent)},
			Workflow:  &v1.RuleWorkflowAction{Enabled: true, DefinitionKey: "Restart-Service", AgentUserId: upper(agent)},
			AiSummary: true,
		},
	}
	spec := toRuleSpec(in)
	if spec.GroupID != ds || !slices.Equal(spec.DatasourceIDs, []string{ds}) {
		t.Fatalf("group / datasources = %q %v, want lower-cased", spec.GroupID, spec.DatasourceIDs)
	}
	if !slices.Equal(spec.Notify.UserIDs, []string{user}) || !slices.Equal(spec.Notify.ScheduleIDs, []string{sched}) ||
		!slices.Equal(spec.Notify.DeptIDs, []string{dept}) || !spec.Notify.CIOwner {
		t.Fatalf("notify = %+v, want lower-cased", spec.Notify)
	}
	if spec.EscalationID != esc || spec.Actions.Ticket.TemplateID != tpl || spec.Actions.Ticket.AgentUserID != agent ||
		spec.Actions.Workflow.AgentUserID != agent {
		t.Fatalf("escalation / actions = %+v, want lower-cased", spec.Actions)
	}
	// 流程标识不是 UUID，保持原样。
	if spec.Actions.Workflow.DefinitionKey != "Restart-Service" {
		t.Fatalf("definition key = %q, must not be touched", spec.Actions.Workflow.DefinitionKey)
	}
	// 已是小写、空与缺省原样通过；空切片保持为空（nil 不变成空切片，归一化留给 biz）。
	if got := toNotifyTargets(&v1.RuleNotify{UserIds: []string{user}}); !slices.Equal(got.UserIDs, []string{user}) || got.ScheduleIDs != nil || got.DeptIDs != nil {
		t.Fatalf("toNotifyTargets = %+v", got)
	}
	if got := toRuleSpec(&v1.RuleSpec{Name: "x"}); got.GroupID != "" || got.DatasourceIDs != nil || got.EscalationID != "" {
		t.Fatalf("toRuleSpec(empty) = %+v", got)
	}

	// 导入选项入口同样转小写；可选的单个 ID（批量操作的分组 / 升级策略）nil 保持 nil。
	opts := toImportOptions(&v1.ImportRulesOptions{
		GroupId: upper(ds), DatasourceIds: []string{upper(ds)}, Notify: &v1.RuleNotify{UserIds: []string{upper(user)}},
	})
	if opts.GroupID != ds || !slices.Equal(opts.DatasourceIDs, []string{ds}) || !slices.Equal(opts.Notify.UserIDs, []string{user}) {
		t.Fatalf("toImportOptions = %+v, want lower-cased", opts)
	}
	if got := lowerOpt(nil); got != nil {
		t.Fatalf("lowerOpt(nil) = %v, want nil", got)
	}
	if got := lowerOpt(ptr(upper(esc))); got == nil || *got != esc {
		t.Fatalf("lowerOpt = %v, want %q", got, esc)
	}
	if got := lowerIDs(nil); got != nil {
		t.Fatalf("lowerIDs(nil) = %v, want nil", got)
	}
	if got := lowerIDs([]string{upper(user), agent}); !slices.Equal(got, []string{user, agent}) {
		t.Fatalf("lowerIDs = %v", got)
	}
}

func ptr(s string) *string { return &s }
