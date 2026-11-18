package v1_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func escalationSpec(mutate func(*v1.EscalationSpec)) *v1.EscalationSpec {
	s := &v1.EscalationSpec{
		GroupId: idA, Name: "运维升级策略", Mode: 1, MinSeverity: 3, Enabled: true,
		Levels: []*v1.EscalationLevel{
			{Trigger: 1, AfterM: 15, RepeatIntervalM: 15, MaxNotify: 3, Targets: &v1.EscalationTargets{Schedules: []*v1.EscalationSchedule{{Id: idA, BackupOnly: true}}}},
			{Trigger: 2, AfterM: 60, Targets: &v1.EscalationTargets{
				UserIds: []string{idA}, OncallGroupIds: []string{idB}, Depts: []*v1.EscalationDept{{Id: idA}, {Id: idA, Up: 1}},
			}},
		},
	}
	if mutate != nil {
		mutate(s)
	}
	return s
}

func inhibitSpec(mutate func(*v1.InhibitSpec)) *v1.InhibitSpec {
	s := &v1.InhibitSpec{
		GroupId: idA, Name: "主机失联抑制派生告警", Enabled: true,
		Source:      &v1.InhibitSelector{Matchers: []*v1.LabelMatcher{{Label: "alertname", Op: "==", Value: "HostDown"}}},
		EqualLabels: []string{"ident"},
	}
	if mutate != nil {
		mutate(s)
	}
	return s
}

func recordRuleSpec(mutate func(*v1.RecordRuleSpec)) *v1.RecordRuleSpec {
	s := &v1.RecordRuleSpec{
		GroupId: idA, Name: "job:http_requests:rate5m", DatasourceId: idB, Expr: "sum by (job) (rate(http_requests_total[5m]))",
		IntervalS: 60, Labels: map[string]string{"team": "ops"}, Enabled: true,
	}
	if mutate != nil {
		mutate(s)
	}
	return s
}

func repeatString(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "018f3e1a-0000-7000-8000-" + strings.Repeat("0", 8) + string(rune('a'+i/26)) + string(rune('a'+i%26)) + "00"
	}
	return out
}

// TestP2ValidationRules 第二、三期新增消息的校验规则：取值边界与迁移 0076 的 CHECK、kit/monitorcontract 的上限一致，
// 且每条 CEL 表达式可以编译。
func TestP2ValidationRules(t *testing.T) {
	t.Parallel()
	now := timestamppb.Now()
	later := timestamppb.New(now.AsTime().Add(time.Hour))
	days := func(n int) *timestamppb.Timestamp {
		return timestamppb.New(now.AsTime().Add(time.Duration(n) * 24 * time.Hour))
	}
	levels := func(n int) []*v1.EscalationLevel {
		out := make([]*v1.EscalationLevel, n)
		for i := range out {
			out[i] = &v1.EscalationLevel{Trigger: 1, AfterM: int32(i + 1), Targets: &v1.EscalationTargets{UserIds: []string{idA}}}
		}
		return out
	}
	steps := func(n int) []*v1.RuleRelabelStep {
		out := make([]*v1.RuleRelabelStep, n)
		for i := range out {
			out[i] = &v1.RuleRelabelStep{Action: "labeldrop", Regex: "tmp_.*"}
		}
		return out
	}
	cases := []struct {
		name string
		msg  proto.Message
		ok   bool
	}{
		// 规则的第二、三期字段。
		{"rule: full p2 spec", ruleSpec(func(s *v1.RuleSpec) {
			s.Effective = &v1.EffectiveTime{Mode: "work_time"}
			s.Notify.CiOwner = true
			s.CiMatch = &v1.RuleCIMatch{Label: "instance", By: "attr", Attr: "ip", ModelCodes: []string{"host"}, EnrichTags: []string{"核心"}}
			s.ResourceScope = &v1.RuleResourceScope{Enabled: true, ModelCodes: []string{"host"}, Envs: []string{"prod"}, Tags: []string{"core"}}
			s.Relabel = []*v1.RuleRelabelStep{
				{Action: "replace", SourceLabels: []string{"instance"}, Regex: "(.+):\\d+", TargetLabel: "host"},
				{Action: "labeldrop", Regex: "tmp_.*"},
			}
			s.DropExpr = `{{ if eq $labels.env "dev" }}true{{ end }}`
			s.TopologyInhibit = true
			s.EscalationId = idB
			s.Actions = &v1.RuleActions{
				Ticket:    &v1.RuleTicketAction{Enabled: true, TemplateId: idA, AgentUserId: idB},
				Workflow:  &v1.RuleWorkflowAction{Enabled: true, DefinitionKey: "alert_handle", AgentUserId: idB},
				AiSummary: true,
			}
		}), true},
		{"effective: off_time", &v1.EffectiveTime{Mode: "off_time"}, true},
		{"effective: windows mode", &v1.EffectiveTime{Mode: "windows", Windows: []*v1.TimeWindow{{Days: []int32{1}, Start: "09:00", End: "18:00"}}}, true},
		{"effective: work_time with windows", &v1.EffectiveTime{Mode: "work_time", Windows: []*v1.TimeWindow{{Days: []int32{1}, Start: "09:00", End: "18:00"}}}, false},
		{"effective: unknown mode", &v1.EffectiveTime{Mode: "holiday"}, false},
		{"ci match: empty", &v1.RuleCIMatch{}, true},
		{"ci match: disabled", &v1.RuleCIMatch{Disabled: true}, true},
		{"ci match: by name", &v1.RuleCIMatch{Label: "ident", By: "name"}, true},
		{"ci match: by attr default", &v1.RuleCIMatch{Label: "instance", By: "attr"}, true},
		{"ci match: label without by", &v1.RuleCIMatch{Label: "ident"}, false},
		{"ci match: by without label", &v1.RuleCIMatch{By: "name"}, false},
		{"ci match: attr without label", &v1.RuleCIMatch{Attr: "ip"}, false},
		{"ci match: name with attr", &v1.RuleCIMatch{Label: "ident", By: "name", Attr: "ip"}, false},
		{"ci match: bad label", &v1.RuleCIMatch{Label: "a-b", By: "name"}, false},
		{"ci match: bad attr", &v1.RuleCIMatch{Label: "ident", By: "attr", Attr: "Mgmt-IP"}, false},
		{"ci match: unknown by", &v1.RuleCIMatch{Label: "ident", By: "id"}, false},
		{"ci match: 21 models", &v1.RuleCIMatch{ModelCodes: repeatString(21)}, false},
		{"ci match: duplicate model", &v1.RuleCIMatch{ModelCodes: []string{"a", "a"}}, false},
		{"ci match: 11 enrich tags", &v1.RuleCIMatch{EnrichTags: repeatString(11)}, false},
		{"scope: empty", &v1.RuleResourceScope{}, true},
		{"scope: enabled only", &v1.RuleResourceScope{Enabled: true}, true},
		{"scope: conditions while disabled", &v1.RuleResourceScope{Envs: []string{"prod"}}, false},
		{"scope: 21 envs", &v1.RuleResourceScope{Enabled: true, Envs: repeatString(21)}, false},
		{"scope: empty tag", &v1.RuleResourceScope{Enabled: true, Tags: []string{""}}, false},
		{"relabel: unknown action", &v1.RuleRelabelStep{Action: "keep"}, false},
		{"relabel: six sources", &v1.RuleRelabelStep{Action: "replace", TargetLabel: "a", SourceLabels: []string{"a", "b", "c", "d", "e", "f"}}, false},
		{"relabel: bad source", &v1.RuleRelabelStep{Action: "replace", TargetLabel: "a", SourceLabels: []string{"a-b"}}, false},
		{"relabel: bad target", &v1.RuleRelabelStep{Action: "replace", TargetLabel: "1a"}, false},
		{"relabel: long regex", &v1.RuleRelabelStep{Action: "labeldrop", Regex: strings.Repeat("a", 257)}, false},
		{"relabel: long replacement", &v1.RuleRelabelStep{Action: "replace", TargetLabel: "a", Replacement: strings.Repeat("a", 257)}, false},
		{"rule: ten relabel steps", ruleSpec(func(s *v1.RuleSpec) { s.Relabel = steps(10) }), true},
		{"rule: eleven relabel steps", ruleSpec(func(s *v1.RuleSpec) { s.Relabel = steps(11) }), false},
		{"rule: drop expr at the limit", ruleSpec(func(s *v1.RuleSpec) { s.DropExpr = strings.Repeat("x", 2048) }), true},
		{"rule: drop expr too long", ruleSpec(func(s *v1.RuleSpec) { s.DropExpr = strings.Repeat("x", 2049) }), false},
		{"rule: bad escalation id", ruleSpec(func(s *v1.RuleSpec) { s.EscalationId = "x" }), false},
		{"actions: empty", &v1.RuleActions{}, true},
		{"actions: disabled ticket keeps blanks", &v1.RuleActions{Ticket: &v1.RuleTicketAction{}}, true},
		{"actions: ticket needs template", &v1.RuleActions{Ticket: &v1.RuleTicketAction{Enabled: true, AgentUserId: idA}}, false},
		{"actions: ticket needs agent", &v1.RuleActions{Ticket: &v1.RuleTicketAction{Enabled: true, TemplateId: idA}}, false},
		{"actions: ticket bad template id", &v1.RuleActions{Ticket: &v1.RuleTicketAction{TemplateId: "x"}}, false},
		{"actions: workflow needs key", &v1.RuleActions{Workflow: &v1.RuleWorkflowAction{Enabled: true, AgentUserId: idA}}, false},
		{"actions: workflow needs agent", &v1.RuleActions{Workflow: &v1.RuleWorkflowAction{Enabled: true, DefinitionKey: "k"}}, false},
		{"actions: ai only", &v1.RuleActions{AiSummary: true}, true},
		{"rule batch: set escalation", &v1.BatchRulesRequest{Ids: []string{idA}, Action: "set_escalation", EscalationId: proto.String(idB)}, true},
		{"rule batch: clear escalation", &v1.BatchRulesRequest{Ids: []string{idA}, Action: "set_escalation"}, true},
		{"rule list: by escalation", &v1.ListRulesRequest{Page: 1, PageSize: 20, EscalationId: proto.String(idA)}, true},
		{"target options: kinds unchanged from phase 1", &v1.ListMonitorTargetOptionsRequest{Kinds: []string{"oncall_group"}}, false},
		{"oncall group options: empty", &v1.ListOncallGroupOptionsRequest{}, true},
		{"rule template: valid code", &v1.GetRuleTemplatePackRequest{Code: "node-exporter"}, true},
		{"rule template: bad code", &v1.GetRuleTemplatePackRequest{Code: "../etc"}, false},
		{"rule template: empty code", &v1.GetRuleTemplatePackRequest{}, false},
		{"simulate: valid", &v1.SimulateRuleRequest{Spec: ruleSpec(nil), DatasourceId: idB, SampleLabels: map[string]string{"instance": "10.0.0.1:9100", "empty": ""}, Send: true}, true},
		{"simulate: spec required", &v1.SimulateRuleRequest{DatasourceId: idB}, false},
		{"simulate: datasource required", &v1.SimulateRuleRequest{Spec: ruleSpec(nil)}, false},
		{"simulate: bad sample label", &v1.SimulateRuleRequest{Spec: ruleSpec(nil), DatasourceId: idB, SampleLabels: map[string]string{"a-b": "v"}}, false},

		// 事件操作。
		{"ack: valid", &v1.AckAlertRequest{Id: idA}, true},
		{"ack: bad id", &v1.AckAlertRequest{Id: "x"}, false},
		{"unack: valid", &v1.UnackAlertRequest{Id: idA}, true},
		{"transfer: valid", &v1.TransferAlertRequest{Id: idA, UserId: idB, Note: "夜班交接"}, true},
		{"transfer: user required", &v1.TransferAlertRequest{Id: idA}, false},
		{"transfer: note too long", &v1.TransferAlertRequest{Id: idA, UserId: idB, Note: strings.Repeat("字", 501)}, false},
		{"comment: valid", &v1.CommentAlertRequest{Id: idA, Text: strings.Repeat("字", monitorcontract.MaxCommentLen)}, true},
		{"comment: empty", &v1.CommentAlertRequest{Id: idA}, false},
		{"comment: too long", &v1.CommentAlertRequest{Id: idA, Text: strings.Repeat("字", monitorcontract.MaxCommentLen+1)}, false},
		{"close: valid", &v1.CloseAlertRequest{Id: idA, Reason: strings.Repeat("字", monitorcontract.MaxCloseReasonLen)}, true},
		{"close: no reason", &v1.CloseAlertRequest{Id: idA}, true},
		{"close: reason too long", &v1.CloseAlertRequest{Id: idA, Reason: strings.Repeat("字", monitorcontract.MaxCloseReasonLen+1)}, false},
		{"batch ack: valid", &v1.BatchAckAlertsRequest{Ids: []string{idA, idB}}, true},
		{"batch ack: empty", &v1.BatchAckAlertsRequest{}, false},
		{"batch ack: duplicate", &v1.BatchAckAlertsRequest{Ids: []string{idA, idA}}, false},
		{"batch ack: 201 ids", &v1.BatchAckAlertsRequest{Ids: repeatString(201)}, false},
		{"batch close: valid", &v1.BatchCloseAlertsRequest{Ids: []string{idA}, Reason: "机器已下线"}, true},
		{"batch close: empty", &v1.BatchCloseAlertsRequest{}, false},
		{"ticket: valid", &v1.CreateAlertTicketRequest{Id: idA, TemplateId: idB, Priority: 3, RequestId: proto.String(idA)}, true},
		{"ticket: default priority", &v1.CreateAlertTicketRequest{Id: idA, TemplateId: idB}, true},
		{"ticket: template required", &v1.CreateAlertTicketRequest{Id: idA}, false},
		{"ticket: priority 5", &v1.CreateAlertTicketRequest{Id: idA, TemplateId: idB, Priority: 5}, false},
		{"ticket: title too long", &v1.CreateAlertTicketRequest{Id: idA, TemplateId: idB, Title: strings.Repeat("字", 256)}, false},
		{"alert list: p2 filters", &v1.ListAlertsRequest{
			Page: 1, PageSize: 20, CiId: proto.String(idA), Acked: proto.Bool(false), Inhibited: proto.Bool(true), AckUserId: proto.String(idB), HandleState: proto.Int32(4),
		}, true},
		{"alert list: handle state 0", &v1.ListAlertsRequest{Page: 1, PageSize: 20, HandleState: proto.Int32(0)}, false},
		{"alert list: handle state 5", &v1.ListAlertsRequest{Page: 1, PageSize: 20, HandleState: proto.Int32(5)}, false},
		{"alert list: bad ci id", &v1.ListAlertsRequest{Page: 1, PageSize: 20, CiId: proto.String("x")}, false},
		{"alert aggregate: p2 filters", &v1.AggregateAlertsRequest{By: "rule", Acked: proto.Bool(true), Inhibited: proto.Bool(false), Labels: []string{"env=prod"}}, true},
		{"alert aggregate: bad label filter", &v1.AggregateAlertsRequest{By: "rule", Labels: []string{"env"}}, false},
		{"shift counts: valid", &v1.ListShiftAlertCountsRequest{ScheduleId: idA, StartTime: now, EndTime: days(31)}, true},
		{"shift counts: 62 days", &v1.ListShiftAlertCountsRequest{ScheduleId: idA, StartTime: now, EndTime: days(62)}, true},
		{"shift counts: 63 days", &v1.ListShiftAlertCountsRequest{ScheduleId: idA, StartTime: now, EndTime: days(63)}, false},
		{"shift counts: reversed", &v1.ListShiftAlertCountsRequest{ScheduleId: idA, StartTime: later, EndTime: now}, false},
		{"shift counts: missing end", &v1.ListShiftAlertCountsRequest{ScheduleId: idA, StartTime: now}, false},
		{"shift counts: schedule required", &v1.ListShiftAlertCountsRequest{StartTime: now, EndTime: later}, false},

		// 升级策略。
		{"escalation: valid", escalationSpec(nil), true},
		{"escalation: global", escalationSpec(func(s *v1.EscalationSpec) { s.GroupId = "" }), true},
		{"escalation: bad group id", escalationSpec(func(s *v1.EscalationSpec) { s.GroupId = "x" }), false},
		{"escalation: name required", escalationSpec(func(s *v1.EscalationSpec) { s.Name = "" }), false},
		{"escalation: mode 0", escalationSpec(func(s *v1.EscalationSpec) { s.Mode = 0 }), false},
		{"escalation: mode 3", escalationSpec(func(s *v1.EscalationSpec) { s.Mode = 3 }), false},
		{"escalation: takeover", escalationSpec(func(s *v1.EscalationSpec) { s.Mode = 2 }), true},
		{"escalation: min severity 0", escalationSpec(func(s *v1.EscalationSpec) { s.MinSeverity = 0 }), false},
		{"escalation: min severity 5", escalationSpec(func(s *v1.EscalationSpec) { s.MinSeverity = 5 }), false},
		{"escalation: no levels", escalationSpec(func(s *v1.EscalationSpec) { s.Levels = nil }), false},
		{"escalation: ten levels", escalationSpec(func(s *v1.EscalationSpec) { s.Levels = levels(10) }), true},
		{"escalation: eleven levels", escalationSpec(func(s *v1.EscalationSpec) { s.Levels = levels(11) }), false},
		{"escalation: duplicate after_m", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[1].AfterM = 15 }), false},
		{"escalation: trigger 3", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[0].Trigger = 3 }), false},
		{"escalation: after 0", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[0].AfterM = 0 }), false},
		{"escalation: after 10081", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[1].AfterM = 10081 }), false},
		{"escalation: repeat 4", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[0].RepeatIntervalM = 4 }), false},
		{"escalation: repeat 5", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[0].RepeatIntervalM = 5 }), true},
		{"escalation: max notify 100", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[0].MaxNotify = 100 }), true},
		{"escalation: max notify 101", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[0].MaxNotify = 101 }), false},
		{"escalation: targets required", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[0].Targets = nil }), false},
		{"escalation: empty targets", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[0].Targets = &v1.EscalationTargets{} }), false},
		{"escalation: duplicate schedule", escalationSpec(func(s *v1.EscalationSpec) {
			s.Levels[0].Targets.Schedules = []*v1.EscalationSchedule{{Id: idA}, {Id: idA, BackupOnly: true}}
		}), false},
		{"escalation: dept up 5", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[1].Targets.Depts[1].Up = 5 }), true},
		{"escalation: dept up 6", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[1].Targets.Depts[1].Up = 6 }), false},
		{"escalation: 21 users", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[1].Targets.UserIds = repeatString(21) }), false},
		{"escalation: bad user id", escalationSpec(func(s *v1.EscalationSpec) { s.Levels[1].Targets.UserIds = []string{"x"} }), false},
		{"escalation create: spec required", &v1.CreateEscalationRequest{}, false},
		{"escalation update: valid", &v1.UpdateEscalationRequest{Id: idA, Spec: escalationSpec(nil), Version: 1}, true},
		{"escalation update: version required", &v1.UpdateEscalationRequest{Id: idA, Spec: escalationSpec(nil)}, false},
		{"escalation estimate: valid", &v1.EstimateEscalationRequest{Spec: escalationSpec(nil), RuleRepeatIntervalM: 30, RuleMaxNotify: 5}, true},
		{"escalation estimate: bad repeat", &v1.EstimateEscalationRequest{Spec: escalationSpec(nil), RuleRepeatIntervalM: 3}, false},
		{"escalation estimate: bad max", &v1.EstimateEscalationRequest{Spec: escalationSpec(nil), RuleMaxNotify: 1001}, false},
		{"escalation list: valid", &v1.ListEscalationsRequest{Page: 1, PageSize: 20, Global: proto.Bool(true)}, true},
		{"escalation list: page 0", &v1.ListEscalationsRequest{PageSize: 20}, false},
		{"escalation options: group required", &v1.ListEscalationOptionsRequest{}, false},
		{"escalation options: valid", &v1.ListEscalationOptionsRequest{GroupId: idA}, true},

		// 抑制规则。
		{"inhibit: valid", inhibitSpec(nil), true},
		{"inhibit: all groups", inhibitSpec(func(s *v1.InhibitSpec) { s.GroupId = "" }), true},
		{"inhibit: target instead of equal", inhibitSpec(func(s *v1.InhibitSpec) {
			s.EqualLabels, s.Target = nil, &v1.InhibitSelector{Severities: []int32{1, 2}}
		}), true},
		{"inhibit: source required", inhibitSpec(func(s *v1.InhibitSpec) { s.Source = nil }), false},
		{"inhibit: empty source", inhibitSpec(func(s *v1.InhibitSpec) { s.Source = &v1.InhibitSelector{} }), false},
		{"inhibit: no target and no equal", inhibitSpec(func(s *v1.InhibitSpec) { s.EqualLabels = nil }), false},
		{"inhibit: empty target and no equal", inhibitSpec(func(s *v1.InhibitSpec) { s.EqualLabels, s.Target = nil, &v1.InhibitSelector{} }), false},
		{"inhibit: eleven equal labels", inhibitSpec(func(s *v1.InhibitSpec) {
			s.EqualLabels = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}
		}), false},
		{"inhibit: bad equal label", inhibitSpec(func(s *v1.InhibitSpec) { s.EqualLabels = []string{"a-b"} }), false},
		{"inhibit: duplicate equal label", inhibitSpec(func(s *v1.InhibitSpec) { s.EqualLabels = []string{"a", "a"} }), false},
		{"inhibit: bad severity", inhibitSpec(func(s *v1.InhibitSpec) { s.Source.Severities = []int32{5} }), false},
		{"inhibit: bad matcher op", inhibitSpec(func(s *v1.InhibitSpec) { s.Source.Matchers[0].Op = "=" }), false},
		{"inhibit: 201 rules", inhibitSpec(func(s *v1.InhibitSpec) { s.Source.RuleIds = repeatString(201) }), false},
		{"inhibit update: valid", &v1.UpdateInhibitRequest{Id: idA, Spec: inhibitSpec(nil), Version: 1}, true},
		{"inhibit preview: spec required", &v1.PreviewInhibitRequest{}, false},
		{"inhibit preview: valid", &v1.PreviewInhibitRequest{Spec: inhibitSpec(nil)}, true},

		// 记录规则。
		{"record rule: valid", recordRuleSpec(nil), true},
		{"record rule: colon first", recordRuleSpec(func(s *v1.RecordRuleSpec) { s.Name = ":sum" }), true},
		{"record rule: bad name", recordRuleSpec(func(s *v1.RecordRuleSpec) { s.Name = "job-rate" }), false},
		{"record rule: digit first", recordRuleSpec(func(s *v1.RecordRuleSpec) { s.Name = "1m_rate" }), false},
		{"record rule: name too long", recordRuleSpec(func(s *v1.RecordRuleSpec) { s.Name = strings.Repeat("a", 129) }), false},
		{"record rule: expr required", recordRuleSpec(func(s *v1.RecordRuleSpec) { s.Expr = "" }), false},
		{"record rule: expr too long", recordRuleSpec(func(s *v1.RecordRuleSpec) { s.Expr = strings.Repeat("a", 8193) }), false},
		{"record rule: interval 14", recordRuleSpec(func(s *v1.RecordRuleSpec) { s.IntervalS = 14 }), false},
		{"record rule: interval 3601", recordRuleSpec(func(s *v1.RecordRuleSpec) { s.IntervalS = 3601 }), false},
		{"record rule: datasource required", recordRuleSpec(func(s *v1.RecordRuleSpec) { s.DatasourceId = "" }), false},
		{"record rule: bad label", recordRuleSpec(func(s *v1.RecordRuleSpec) { s.Labels = map[string]string{"a-b": "v"} }), false},
		{"record rule update: valid", &v1.UpdateRecordRuleRequest{Id: idA, Spec: recordRuleSpec(nil), Version: 1}, true},
		{"record rule validate: valid", &v1.ValidateRecordRuleRequest{Spec: recordRuleSpec(nil), Id: proto.String(idA)}, true},
		{"record rule list: status 5", &v1.ListRecordRulesRequest{Page: 1, PageSize: 20, LastStatus: proto.Int32(5)}, false},
		{"record rule list: valid", &v1.ListRecordRulesRequest{Page: 1, PageSize: 20, LastStatus: proto.Int32(4)}, true},
		{"datasource: remote write url", &v1.DatasourceSpec{Name: "p", Connection: connection(nil), RemoteWriteUrl: "http://vminsert:8480/insert/0/prometheus/api/v1/write"}, true},

		// 聚合视图。
		{"alert view: valid", &v1.CreateAlertViewRequest{Spec: &v1.AlertViewSpec{
			Name: "按机房", By: "label:idc", Filters: &v1.AlertViewFilter{GroupId: proto.String(idA), Severities: []int32{3, 4}, Acked: proto.Bool(false), Labels: []string{"env=prod"}},
		}}, true},
		{"alert view: name required", &v1.CreateAlertViewRequest{Spec: &v1.AlertViewSpec{By: "rule"}}, false},
		{"alert view: name too long", &v1.CreateAlertViewRequest{Spec: &v1.AlertViewSpec{Name: strings.Repeat("字", 65), By: "rule"}}, false},
		{"alert view: bad by", &v1.CreateAlertViewRequest{Spec: &v1.AlertViewSpec{Name: "v", By: "datasource"}}, false},
		{"alert view: bad filter label", &v1.CreateAlertViewRequest{Spec: &v1.AlertViewSpec{Name: "v", By: "rule", Filters: &v1.AlertViewFilter{Labels: []string{"x"}}}}, false},
		{"alert view update: valid", &v1.UpdateAlertViewRequest{Id: idA, Spec: &v1.AlertViewSpec{Name: "v", By: "severity", IsPublic: true}}, true},
		{"alert view delete: bad id", &v1.DeleteAlertViewRequest{Id: "x"}, false},

		// 统计。
		{"stats overview: empty", &v1.GetAlertOverviewRequest{}, true},
		{"stats overview: group", &v1.GetAlertOverviewRequest{GroupId: proto.String(idA), Timezone: "Asia/Shanghai"}, true},
		{"stats trend: valid", &v1.GetAlertTrendRequest{StartTime: now, EndTime: days(30), By: "group"}, true},
		{"stats trend: default by", &v1.GetAlertTrendRequest{StartTime: now, EndTime: later}, true},
		{"stats trend: reversed", &v1.GetAlertTrendRequest{StartTime: later, EndTime: now}, false},
		{"stats trend: missing start", &v1.GetAlertTrendRequest{EndTime: later}, false},
		{"stats trend: bad by", &v1.GetAlertTrendRequest{StartTime: now, EndTime: later, By: "rule"}, false},
		{"stats top: rule", &v1.GetAlertTopRequest{StartTime: now, EndTime: later, By: "rule", Limit: 50}, true},
		{"stats top: ci", &v1.GetAlertTopRequest{StartTime: now, EndTime: later, By: "ci"}, true},
		{"stats top: label", &v1.GetAlertTopRequest{StartTime: now, EndTime: later, By: "label:instance"}, true},
		{"stats top: by required", &v1.GetAlertTopRequest{StartTime: now, EndTime: later}, false},
		{"stats top: limit 51", &v1.GetAlertTopRequest{StartTime: now, EndTime: later, By: "rule", Limit: 51}, false},
		{"stats efficiency: valid", &v1.GetAlertEfficiencyRequest{StartTime: now, EndTime: later, By: "severity"}, true},
		{"stats efficiency: bad by", &v1.GetAlertEfficiencyRequest{StartTime: now, EndTime: later, By: "rule"}, false},
		{"stats noise: valid", &v1.GetAlertNoiseRequest{StartTime: now, EndTime: later}, true},
		{"stats noise: equal bounds", &v1.GetAlertNoiseRequest{StartTime: now, EndTime: now}, false},
		{"engine status: empty", &v1.GetEngineStatusRequest{}, true},
	}
	for _, c := range cases {
		err := httpx.ProtoValidate(c.msg)
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok=%v", c.name, err, c.ok)
		}
		if err != nil && strings.Contains(err.Error(), "compilation error") {
			t.Errorf("%s: CEL expression does not compile: %v", c.name, err)
		}
	}
}

// TestP2ProtoLiteralsMatchContract proto 里写死的第二、三期字面量与 kit/monitorcontract 一致。
func TestP2ProtoLiteralsMatchContract(t *testing.T) {
	t.Parallel()
	root, err := os.OpenRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	read := func(name string) string {
		raw, err := root.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	alert, rule, common := read("alert.proto"), read("rule.proto"), read("common.proto")
	// 时间线类型逐个在 AlertTimelineItem.type 的注释里说明。
	for _, typ := range monitorcontract.AlertEventTypes() {
		if !regexp.MustCompile(`// [^\n]*\b` + typ + ` \S`).MatchString(alert) {
			t.Errorf("timeline type %q is not documented in alert.proto", typ)
		}
	}
	for _, step := range []string{
		monitorcontract.StepDropped, monitorcontract.StepOutOfScope, monitorcontract.StepInhibitedRule,
		monitorcontract.StepInhibitedTopology, monitorcontract.StepMutedMaintenance,
	} {
		if !strings.Contains(rule, step+" ") {
			t.Errorf("eval step %q is not documented in rule.proto", step)
		}
	}
	for _, lit := range []string{
		`"` + monitorcontract.EffectiveModeWindows + `"`, `"` + monitorcontract.EffectiveModeWorkTime + `"`, `"` + monitorcontract.EffectiveModeOffTime + `"`,
	} {
		if !strings.Contains(common, lit) {
			t.Errorf("effective mode %s missing from common.proto", lit)
		}
	}
	for _, action := range []string{
		monitorcontract.RelabelReplace, monitorcontract.RelabelLabelMap, monitorcontract.RelabelLabelDrop, monitorcontract.RelabelLabelKeep,
	} {
		if err := httpx.ProtoValidate(&v1.RuleRelabelStep{Action: action}); err != nil {
			t.Errorf("contract relabel action %q rejected by proto: %v", action, err)
		}
	}
	for _, by := range []string{monitorcontract.CIMatchByName, monitorcontract.CIMatchByAttr} {
		if err := httpx.ProtoValidate(&v1.RuleCIMatch{Label: "a", By: by}); err != nil {
			t.Errorf("contract ci match %q rejected by proto: %v", by, err)
		}
	}
	if !strings.Contains(read("record_rule.proto"), `pattern: "`+strings.ReplaceAll(monitorcontract.MetricNamePattern, `\`, `\\`)+`"`) {
		t.Error("record_rule.proto does not carry the contract metric name pattern")
	}
	attr := strings.TrimSuffix(strings.TrimPrefix(monitorcontract.CIAttrPattern, "^"), "$")
	if !strings.Contains(rule, `"^(`+attr+`)?$"`) {
		t.Error("rule.proto does not carry the contract ci attribute pattern")
	}
	// 契约里的每个校验上限，proto 都不比它宽：用恰好越界一个的值去撞。
	over := func(n int) []string { return repeatString(n + 1) }
	for name, msg := range map[string]proto.Message{
		"MaxRelabelSourceLabels": &v1.RuleRelabelStep{Action: "replace", TargetLabel: "a", SourceLabels: make([]string, monitorcontract.MaxRelabelSourceLabels+1)},
		"MaxRelabelRegexLen":     &v1.RuleRelabelStep{Action: "labeldrop", Regex: strings.Repeat("a", monitorcontract.MaxRelabelRegexLen+1)},
		"MaxRelabelTextLen":      &v1.RuleRelabelStep{Action: "replace", TargetLabel: "a", Separator: strings.Repeat("a", monitorcontract.MaxRelabelTextLen+1)},
		"MaxCIMatchModels":       &v1.RuleCIMatch{ModelCodes: over(monitorcontract.MaxCIMatchModels)},
		"MaxCIEnrichTags":        &v1.RuleCIMatch{EnrichTags: over(monitorcontract.MaxCIEnrichTags)},
		"MaxScopeValues":         &v1.RuleResourceScope{Enabled: true, Tags: over(monitorcontract.MaxScopeValues)},
		"MaxCodeLen":             &v1.RuleResourceScope{Enabled: true, Envs: []string{strings.Repeat("a", monitorcontract.MaxCodeLen+1)}},
		"MaxInhibitRuleIDs":      &v1.InhibitSelector{RuleIds: over(monitorcontract.MaxInhibitRuleIDs)},
		"MaxEscalationTargets":   &v1.EscalationTargets{UserIds: over(monitorcontract.MaxEscalationTargets)},
		"MaxEscalationDeptUp":    &v1.EscalationDept{Id: idA, Up: monitorcontract.MaxEscalationDeptUp + 1},
		"MaxEscalationAfterM": &v1.EscalationLevel{
			Trigger: 1, AfterM: monitorcontract.MaxEscalationAfterM + 1, Targets: &v1.EscalationTargets{UserIds: []string{idA}},
		},
		"MaxEscalationNotify": &v1.EscalationLevel{
			Trigger: 1, AfterM: 1, MaxNotify: monitorcontract.MaxEscalationNotify + 1, Targets: &v1.EscalationTargets{UserIds: []string{idA}},
		},
	} {
		if err := httpx.ProtoValidate(msg); err == nil {
			t.Errorf("%s: proto accepts a value one past the contract limit", name)
		}
	}
}

// TestP2ErrorReasonsStable 错误码的数值是对外契约：第一期的保持不变，第二、三期新增的固定在这里。
func TestP2ErrorReasonsStable(t *testing.T) {
	t.Parallel()
	want := map[string]int32{
		"MONITOR_FORBIDDEN": 1, "MONITOR_VERSION_CONFLICT": 2, "MONITOR_TIME_RANGE_INVALID": 3, "MONITOR_SECRET_UNAVAILABLE": 4,
		"MONITOR_NOT_IMPLEMENTED": 5, "MONITOR_DATASOURCE_ADDRESS_BLOCKED": 19, "MONITOR_RULE_IMPORT_INVALID": 35,
		"MONITOR_RULE_TEMPLATE_NOT_FOUND": 36, "MONITOR_SIMULATE_INVALID": 37,
		"MONITOR_ALERT_NOT_FOUND": 40, "MONITOR_ALERT_NOT_FIRING": 41, "MONITOR_ALERT_ALREADY_ACKED": 42, "MONITOR_ALERT_ACTION_INVALID": 43,
		"MONITOR_ALERT_TICKET_EXISTS": 44, "MONITOR_ALERT_VIEW_NOT_FOUND": 45, "MONITOR_ALERT_VIEW_NAME_EXISTS": 46, "MONITOR_ALERT_VIEW_INVALID": 47,
		"MONITOR_MUTE_INVALID": 51, "MONITOR_SUBSCRIPTION_INVALID": 61,
		"MONITOR_ESCALATION_NOT_FOUND": 70, "MONITOR_ESCALATION_NAME_EXISTS": 71, "MONITOR_ESCALATION_INVALID": 72, "MONITOR_ESCALATION_IN_USE": 73,
		"MONITOR_INHIBIT_NOT_FOUND": 80, "MONITOR_INHIBIT_INVALID": 81,
		"MONITOR_RECORD_RULE_NOT_FOUND": 90, "MONITOR_RECORD_RULE_NAME_EXISTS": 91, "MONITOR_RECORD_RULE_INVALID": 92, "MONITOR_RECORD_RULE_NO_WRITE_URL": 93,
	}
	for name, number := range want {
		if got, ok := v1.ErrorReason_value[name]; !ok || got != number {
			t.Errorf("%s = %d (present %v), want %d", name, got, ok, number)
		}
	}
	if len(v1.ErrorReason_name) != 51 {
		t.Errorf("ErrorReason has %d values; a new reason must be pinned in this test", len(v1.ErrorReason_name))
	}
	if !v1.IsMonitorNotImplemented(v1.ErrorMonitorNotImplemented("x")) || v1.ErrorMonitorNotImplemented("x").Code != 501 {
		t.Error("MONITOR_NOT_IMPLEMENTED must map to HTTP 501")
	}
	if v1.ErrorMonitorAlertNotFiring("x").Code != 409 || v1.ErrorMonitorRecordRuleNoWriteUrl("x").Code != 400 || v1.ErrorMonitorEscalationInUse("x").Code != 409 {
		t.Error("HTTP codes of the phase 2 reasons")
	}
}

// TestP2ServicesAnnotated 新增的服务与接口都带 HTTP 路由，且路径在 /api/v1/monitor/ 下、彼此不重复
// （标题由 service/system 的可授权操作测试在登记后统一检查）。
func TestP2ServicesAnnotated(t *testing.T) {
	t.Parallel()
	files := []protoreflect.FileDescriptor{
		v1.File_monitor_v1_alert_proto, v1.File_monitor_v1_rule_proto, v1.File_monitor_v1_escalation_proto, v1.File_monitor_v1_inhibit_proto,
		v1.File_monitor_v1_record_rule_proto, v1.File_monitor_v1_alert_view_proto, v1.File_monitor_v1_stats_proto, v1.File_monitor_v1_engine_proto,
	}
	wantServices := map[string]int{
		"MonitorAlertService": 12, "MonitorRuleService": 21, "MonitorEscalationService": 8, "MonitorInhibitService": 6,
		"MonitorRecordRuleService": 7, "MonitorAlertViewService": 4, "MonitorStatsService": 5, "MonitorEngineService": 1,
	}
	for _, f := range files {
		services := f.Services()
		for i := range services.Len() {
			svc := services.Get(i)
			want, ok := wantServices[string(svc.Name())]
			if !ok {
				t.Errorf("unexpected service %s", svc.Name())
				continue
			}
			delete(wantServices, string(svc.Name()))
			if got := svc.Methods().Len(); got != want {
				t.Errorf("%s has %d methods, want %d (update the contract notes when adding RPCs)", svc.Name(), got, want)
			}
		}
	}
	for missing := range wantServices {
		t.Errorf("service %s not found", missing)
	}
}
