package v1_test

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	idA = "018f3e1a-0000-7000-8000-00000000000a"
	idB = "018f3e1a-0000-7000-8000-00000000000b"
)

func ruleSpec(mutate func(*v1.RuleSpec)) *v1.RuleSpec {
	s := &v1.RuleSpec{
		GroupId: idA, Name: "CPU 使用率过高", Enabled: true, Labels: map[string]string{"team": "ops"},
		DatasourceScope: 1, DatasourceIds: []string{idB},
		Queries:       []*v1.RuleQuery{{Expr: "cpu > 90", Severity: 3}, {Expr: "cpu > 80", Severity: 2}},
		LevelInhibit:  true,
		EvalIntervalS: 60, ForS: 60,
		Effective:   &v1.EffectiveTime{Timezone: "Asia/Shanghai", Windows: []*v1.TimeWindow{{Days: []int32{1, 2, 3, 4, 5}, Start: "09:00", End: "18:00"}}},
		Annotations: map[string]string{"summary": "{{ $labels.instance }} CPU {{ $value }}"},
		Notify:      &v1.RuleNotify{UserIds: []string{idA}},
	}
	if mutate != nil {
		mutate(s)
	}
	return s
}

// severityMapOf n 项的级别对照表。
func severityMapOf(n int) map[string]int32 {
	out := make(map[string]int32, n)
	for i := range n {
		out["k"+strconv.Itoa(i)] = 1
	}
	return out
}

func muteSpec(mutate func(*v1.MuteSpec)) *v1.MuteSpec {
	now := time.Now()
	s := &v1.MuteSpec{
		Name: "发布窗口", Reason: "版本发布", Mode: 1, TimeType: 1, Enabled: true,
		StartTime: timestamppb.New(now), EndTime: timestamppb.New(now.Add(time.Hour)),
		Matchers: []*v1.LabelMatcher{
			{Label: "instance", Op: "=~", Value: `10\..*`}, {Label: "env", Op: "in", Values: []string{"a", "b"}},
		},
	}
	if mutate != nil {
		mutate(s)
	}
	return s
}

func subscriptionSpec(mutate func(*v1.AlertSubscriptionSpec)) *v1.AlertSubscriptionSpec {
	s := &v1.AlertSubscriptionSpec{
		Name: "我关心的告警", GroupIds: []string{idA}, Severities: []int32{3, 4}, OnFiring: true,
		Receivers: &v1.SubscriptionReceivers{Self: true, Groups: []string{"$group:ops"}}, Enabled: true,
	}
	if mutate != nil {
		mutate(s)
	}
	return s
}

func connection(mutate func(*v1.DatasourceConnection)) *v1.DatasourceConnection {
	c := &v1.DatasourceConnection{
		Url: "http://vmselect:8481/select/0/prometheus", AuthType: 1, TimeoutMs: 10000,
		Headers: map[string]string{"X-Scope-OrgID": "tenant-1"},
	}
	if mutate != nil {
		mutate(c)
	}
	return c
}

// TestValidationRules 逐条执行 proto 里的校验规则：既验证取值边界与迁移 0069 的 CHECK 一致，
// 也保证每条 CEL 表达式可以编译（表达式写错只会在运行期首次校验时暴露）。
func TestValidationRules(t *testing.T) {
	t.Parallel()
	now := timestamppb.Now()
	later := timestamppb.New(now.AsTime().Add(time.Hour))
	cases := []struct {
		name string
		msg  proto.Message
		ok   bool
	}{
		{"rule: valid", ruleSpec(nil), true},
		{"rule: all datasources", ruleSpec(func(s *v1.RuleSpec) { s.DatasourceScope, s.DatasourceIds = 2, nil }), true},
		{"rule: specified scope needs ids", ruleSpec(func(s *v1.RuleSpec) { s.DatasourceIds = nil }), false},
		{"rule: all scope rejects ids", ruleSpec(func(s *v1.RuleSpec) { s.DatasourceScope = 2 }), false},
		{"rule: duplicate severity", ruleSpec(func(s *v1.RuleSpec) { s.Queries[1].Severity = 3 }), false},
		{"rule: no query", ruleSpec(func(s *v1.RuleSpec) { s.Queries = nil }), false},
		{"rule: five queries", ruleSpec(func(s *v1.RuleSpec) {
			s.Queries = append(s.Queries, &v1.RuleQuery{Expr: "a", Severity: 1}, &v1.RuleQuery{Expr: "b", Severity: 4}, &v1.RuleQuery{Expr: "c", Severity: 4})
		}), false},
		{"rule: severity 5", ruleSpec(func(s *v1.RuleSpec) { s.Queries[0].Severity = 5 }), false},
		{"rule: interval too small", ruleSpec(func(s *v1.RuleSpec) { s.EvalIntervalS = 14 }), false},
		{"rule: interval max", ruleSpec(func(s *v1.RuleSpec) { s.EvalIntervalS = 3600 }), true},
		{"rule: for too large", ruleSpec(func(s *v1.RuleSpec) { s.ForS = 86401 }), false},
		{"rule: repeat 0", ruleSpec(func(s *v1.RuleSpec) { s.RepeatIntervalM = 0 }), true},
		{"rule: repeat 4", ruleSpec(func(s *v1.RuleSpec) { s.RepeatIntervalM = 4 }), false},
		{"rule: repeat 5", ruleSpec(func(s *v1.RuleSpec) { s.RepeatIntervalM = 5 }), true},
		{"rule: repeat 10080", ruleSpec(func(s *v1.RuleSpec) { s.RepeatIntervalM = 10080 }), true},
		{"rule: repeat 10081", ruleSpec(func(s *v1.RuleSpec) { s.RepeatIntervalM = 10081 }), false},
		{"rule: max_notify 1001", ruleSpec(func(s *v1.RuleSpec) { s.MaxNotify = 1001 }), false},
		{"rule: bad label name", ruleSpec(func(s *v1.RuleSpec) { s.Labels = map[string]string{"a-b": "v"} }), false},
		{"rule: empty label value", ruleSpec(func(s *v1.RuleSpec) { s.Labels = map[string]string{"a": ""} }), false},
		{"rule: bad window clock", ruleSpec(func(s *v1.RuleSpec) { s.Effective.Windows[0].Start = "9:00" }), false},
		{"rule: bad window day", ruleSpec(func(s *v1.RuleSpec) { s.Effective.Windows[0].Days = []int32{0} }), false},
		{"rule: bad notify id", ruleSpec(func(s *v1.RuleSpec) { s.Notify.UserIds = []string{"x"} }), false},
		{"rule: bad group key label", ruleSpec(func(s *v1.RuleSpec) { s.GroupKeyLabels = []string{"a.b"} }), false},
		{"rule create: spec required", &v1.CreateRuleRequest{}, false},
		{"rule update: version required", &v1.UpdateRuleRequest{Id: idA, Spec: ruleSpec(nil)}, false},
		{"rule update: valid", &v1.UpdateRuleRequest{Id: idA, Spec: ruleSpec(nil), Version: 1}, true},
		{"rule list: label filter", &v1.ListRulesRequest{Page: 1, PageSize: 20, Labels: []string{"team=ops"}}, true},
		{"rule list: bad label filter", &v1.ListRulesRequest{Page: 1, PageSize: 20, Labels: []string{"team"}}, false},
		{"rule list: page size 101", &v1.ListRulesRequest{Page: 1, PageSize: 101}, false},
		{"rule batch: valid", &v1.BatchRulesRequest{Ids: []string{idA}, Action: "set_notify", RepeatIntervalM: proto.Int32(30)}, true},
		{"rule batch: bad repeat", &v1.BatchRulesRequest{Ids: []string{idA}, Action: "set_notify", RepeatIntervalM: proto.Int32(3)}, false},
		{"rule batch: bad action", &v1.BatchRulesRequest{Ids: []string{idA}, Action: "drop"}, false},
		{"rule export: needs target", &v1.ExportRulesRequest{}, false},
		{"rule export: by group", &v1.ExportRulesRequest{GroupId: proto.String(idA)}, true},
		{"rule export: by ids", &v1.ExportRulesRequest{Ids: []string{idA}}, true},
		{"rule import: valid", &v1.ImportRulesRequest{
			Options:    &v1.ImportRulesOptions{Content: "groups: []", GroupId: idA, DatasourceScope: 2, DefaultSeverity: 2},
			OnConflict: "skip",
		}, true},
		{"rule import: scope mismatch", &v1.PreviewImportRulesRequest{
			Options: &v1.ImportRulesOptions{Content: "groups: []", GroupId: idA, DatasourceScope: 1, DefaultSeverity: 2},
		}, false},
		{"rule export: json", &v1.ExportRulesRequest{Ids: []string{idA}, Format: "json"}, true},
		{"rule export: yaml", &v1.ExportRulesRequest{Ids: []string{idA}, Format: "yaml"}, true},
		{"rule export: unknown format", &v1.ExportRulesRequest{Ids: []string{idA}, Format: "xml"}, false},
		{"rule import: severity map", &v1.PreviewImportRulesRequest{Options: &v1.ImportRulesOptions{
			Content: "groups: []", GroupId: idA, DatasourceScope: 2, DefaultSeverity: 2, SeverityMap: map[string]int32{"P0": 4, "sev-3": 1},
		}}, true},
		{"rule import: severity map value 0", &v1.PreviewImportRulesRequest{Options: &v1.ImportRulesOptions{
			Content: "groups: []", GroupId: idA, DatasourceScope: 2, DefaultSeverity: 2, SeverityMap: map[string]int32{"x": 0},
		}}, false},
		{"rule import: severity map value 5", &v1.PreviewImportRulesRequest{Options: &v1.ImportRulesOptions{
			Content: "groups: []", GroupId: idA, DatasourceScope: 2, DefaultSeverity: 2, SeverityMap: map[string]int32{"x": 5},
		}}, false},
		{"rule import: severity map empty key", &v1.PreviewImportRulesRequest{Options: &v1.ImportRulesOptions{
			Content: "groups: []", GroupId: idA, DatasourceScope: 2, DefaultSeverity: 2, SeverityMap: map[string]int32{"": 1},
		}}, false},
		{"rule import: severity map 51 entries", &v1.PreviewImportRulesRequest{Options: &v1.ImportRulesOptions{
			Content: "groups: []", GroupId: idA, DatasourceScope: 2, DefaultSeverity: 2, SeverityMap: severityMapOf(51),
		}}, false},
		{"rule import: severity map 50 entries", &v1.PreviewImportRulesRequest{Options: &v1.ImportRulesOptions{
			Content: "groups: []", GroupId: idA, DatasourceScope: 2, DefaultSeverity: 2, SeverityMap: severityMapOf(50),
		}}, true},
		{"target options: empty", &v1.ListMonitorTargetOptionsRequest{}, true},
		{"target options: users by keyword", &v1.ListMonitorTargetOptionsRequest{Keyword: proto.String("ali"), Kinds: []string{"user"}}, true},
		{"target options: all kinds", &v1.ListMonitorTargetOptionsRequest{Kinds: []string{"schedule", "dept", "user"}}, true},
		{"target options: unknown kind", &v1.ListMonitorTargetOptionsRequest{Kinds: []string{"role"}}, false},
		{"target options: duplicate kind", &v1.ListMonitorTargetOptionsRequest{Kinds: []string{"user", "user"}}, false},
		{"target options: keyword too long", &v1.ListMonitorTargetOptionsRequest{Keyword: proto.String(strings.Repeat("x", 65))}, false},
		{"eval record: time order", &v1.ListRuleEvalRecordsRequest{Id: idA, Page: 1, PageSize: 20, EvalTimeStart: later, EvalTimeEnd: now}, false},
		{"eval record: valid", &v1.ListRuleEvalRecordsRequest{Id: idA, Page: 1, PageSize: 20, EvalTimeStart: now, EvalTimeEnd: later}, true},

		{"mute: valid one-off", muteSpec(nil), true},
		{"mute: one-off needs end", muteSpec(func(s *v1.MuteSpec) { s.EndTime = nil }), false},
		{"mute: one-off end before start", muteSpec(func(s *v1.MuteSpec) { s.EndTime = timestamppb.New(s.StartTime.AsTime().Add(-time.Minute)) }), false},
		{"mute: one-off rejects periodic", muteSpec(func(s *v1.MuteSpec) { s.Periodic = &v1.EffectiveTime{} }), false},
		{"mute: periodic", muteSpec(func(s *v1.MuteSpec) {
			s.TimeType, s.StartTime, s.EndTime = 2, nil, nil
			s.Periodic = &v1.EffectiveTime{Timezone: "Asia/Shanghai", Windows: []*v1.TimeWindow{{Days: []int32{6, 7}, Start: "00:00", End: "00:00"}}}
		}), true},
		{"mute: periodic needs windows object", muteSpec(func(s *v1.MuteSpec) { s.TimeType, s.StartTime, s.EndTime = 2, nil, nil }), false},
		{"mute: periodic bounded range reversed", muteSpec(func(s *v1.MuteSpec) {
			s.TimeType, s.Periodic = 2, &v1.EffectiveTime{}
			s.EndTime = timestamppb.New(s.StartTime.AsTime().Add(-time.Minute))
		}), false},
		{"mute: reason required", muteSpec(func(s *v1.MuteSpec) { s.Reason = "" }), false},
		{"mute: bad mode", muteSpec(func(s *v1.MuteSpec) { s.Mode = 3 }), false},
		{"mute: bad matcher op", muteSpec(func(s *v1.MuteSpec) { s.Matchers[0].Op = "=" }), false},
		{"mute: bad matcher label", muteSpec(func(s *v1.MuteSpec) { s.Matchers[0].Label = "a-b" }), false},
		{"mute: severity 0", muteSpec(func(s *v1.MuteSpec) { s.Severities = []int32{0} }), false},
		{"mute update: valid", &v1.UpdateMuteRequest{Id: idA, Spec: muteSpec(nil), Version: 2}, true},

		{"subscription: valid", subscriptionSpec(nil), true},
		{"subscription: no event", subscriptionSpec(func(s *v1.AlertSubscriptionSpec) { s.OnFiring = false }), false},
		{"subscription: resolved only", subscriptionSpec(func(s *v1.AlertSubscriptionSpec) { s.OnFiring, s.OnResolved = false, true }), true},
		{"subscription: no group", subscriptionSpec(func(s *v1.AlertSubscriptionSpec) { s.GroupIds = nil }), false},
		{"subscription: no severity", subscriptionSpec(func(s *v1.AlertSubscriptionSpec) { s.Severities = nil }), false},
		{"subscription: no receiver", subscriptionSpec(func(s *v1.AlertSubscriptionSpec) { s.Receivers = &v1.SubscriptionReceivers{} }), false},
		{"subscription: receivers required", subscriptionSpec(func(s *v1.AlertSubscriptionSpec) { s.Receivers = nil }), false},
		{"subscription: bad group mark", subscriptionSpec(func(s *v1.AlertSubscriptionSpec) { s.Receivers.Groups = []string{"ops"} }), false},
		{"subscription list: scope", &v1.ListAlertSubscriptionsRequest{Page: 1, PageSize: 20, Scope: "mine"}, true},
		{"subscription list: bad scope", &v1.ListAlertSubscriptionsRequest{Page: 1, PageSize: 20, Scope: "team"}, false},

		{"datasource: valid", &v1.CreateDatasourceRequest{Spec: &v1.DatasourceSpec{Name: "prod", Connection: connection(nil), Enabled: true}}, true},
		{"datasource: connection required", &v1.CreateDatasourceRequest{Spec: &v1.DatasourceSpec{Name: "prod"}}, false},
		{"datasource: ftp url", connection(func(c *v1.DatasourceConnection) { c.Url = "ftp://x" }), false},
		{"datasource: https url", connection(func(c *v1.DatasourceConnection) { c.Url = "https://prom.example.com" }), true},
		{"datasource: timeout too small", connection(func(c *v1.DatasourceConnection) { c.TimeoutMs = 999 }), false},
		{"datasource: timeout too large", connection(func(c *v1.DatasourceConnection) { c.TimeoutMs = 30001 }), false},
		{"datasource: bad auth type", connection(func(c *v1.DatasourceConnection) { c.AuthType = 4 }), false},
		{"datasource: bad header name", connection(func(c *v1.DatasourceConnection) { c.Headers = map[string]string{"X Y": "v"} }), false},
		{"datasource: empty secret", connection(func(c *v1.DatasourceConnection) { c.Secret = proto.String("") }), false},
		{"datasource: remote write url", &v1.DatasourceSpec{Name: "p", Connection: connection(nil), RemoteWriteUrl: "nope"}, false},
		{"datasource test: valid", &v1.TestDatasourceConfigRequest{Connection: connection(nil), Id: proto.String(idA)}, true},
		{"query range: valid", &v1.QueryDatasourceRangeRequest{DatasourceId: idA, Expr: "up", Start: now, End: later, StepS: 15}, true},
		{"query range: reversed", &v1.QueryDatasourceRangeRequest{DatasourceId: idA, Expr: "up", Start: later, End: now, StepS: 15}, false},
		{"query range: missing end", &v1.QueryDatasourceRangeRequest{DatasourceId: idA, Expr: "up", Start: now, StepS: 15}, false},
		{"query range: zero step", &v1.QueryDatasourceRangeRequest{DatasourceId: idA, Expr: "up", Start: now, End: later}, false},

		{"group: valid", &v1.CreateRuleGroupRequest{Name: "基础设施", ParentId: proto.String(idA)}, true},
		{"group: empty name", &v1.CreateRuleGroupRequest{}, false},
		{"role scope: valid", &v1.SetGroupRoleScopesRequest{GroupId: idA, Items: []*v1.GroupRoleScopeItem{{RoleId: idB, Level: 2}}}, true},
		{"role scope: clear", &v1.SetGroupRoleScopesRequest{GroupId: idA}, true},
		{"role scope: bad level", &v1.SetGroupRoleScopesRequest{GroupId: idA, Items: []*v1.GroupRoleScopeItem{{RoleId: idB, Level: 3}}}, false},

		{"alert list: valid", &v1.ListAlertsRequest{
			Page: 1, PageSize: 50, Statuses: []int32{2, 3}, Severities: []int32{3, 4}, Labels: []string{"instance=10.0.0.1:9100"},
			FireTimeStart: now, FireTimeEnd: later, Fingerprint: proto.String("ae837db6a2b9b950d66b6c0473c57168"),
		}, true},
		{"alert list: time order", &v1.ListAlertsRequest{Page: 1, PageSize: 50, FireTimeStart: later, FireTimeEnd: now}, false},
		{"alert list: bad status", &v1.ListAlertsRequest{Page: 1, PageSize: 50, Statuses: []int32{4}}, false},
		{"alert aggregate: rule", &v1.AggregateAlertsRequest{By: "rule"}, true},
		{"alert aggregate: label", &v1.AggregateAlertsRequest{By: "label:instance"}, true},
		{"alert aggregate: bad", &v1.AggregateAlertsRequest{By: "label:"}, false},
		{"alert aggregate: unknown", &v1.AggregateAlertsRequest{By: "datasource"}, false},
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

// TestProtoLiteralsMatchContract proto 里重复出现的标签名正则与匹配操作符必须与 kit/monitorcontract 一致
// （proto 无法引用 Go 常量，靠本测试守护）。
func TestProtoLiteralsMatchContract(t *testing.T) {
	t.Parallel()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".proto") {
			continue
		}
		raw, err := os.ReadFile(e.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			i := strings.Index(line, `pattern: "`)
			if i < 0 || !strings.Contains(line, "[a-zA-Z_]") {
				continue
			}
			lit := line[i+len(`pattern: "`):]
			lit = lit[:strings.LastIndex(lit, `"`)]
			found++
			// 允许在标签名正则外再包一层（name=value 过滤、label:<name> 聚合维度），但标签名部分须逐字一致。
			if !strings.Contains(lit, strings.TrimSuffix(strings.TrimPrefix(monitorcontract.LabelNamePattern, "^"), "$")) {
				t.Errorf("%s: pattern %q does not embed the contract label name pattern", e.Name(), lit)
			}
		}
	}
	if found < 8 {
		t.Fatalf("expected label name patterns in protos, found %d", found)
	}
	for _, op := range []string{
		monitorcontract.OpEqual, monitorcontract.OpNotEqual, monitorcontract.OpRegex, monitorcontract.OpNotRegex,
		monitorcontract.OpIn, monitorcontract.OpNotIn,
	} {
		if err := httpx.ProtoValidate(&v1.LabelMatcher{Label: "a", Op: op}); err != nil {
			t.Errorf("contract op %q rejected by proto: %v", op, err)
		}
	}
	for _, step := range []string{
		monitorcontract.StepPending, monitorcontract.StepFired, monitorcontract.StepFiring, monitorcontract.StepMutedEvent,
		monitorcontract.StepMutedNotify, monitorcontract.StepInhibited, monitorcontract.StepRecovering, monitorcontract.StepResolved,
	} {
		raw, err := os.ReadFile("rule.proto")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), step+" ") {
			t.Errorf("eval step %q is not documented in rule.proto", step)
		}
	}
}
