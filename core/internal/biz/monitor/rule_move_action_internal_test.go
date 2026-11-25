package biz

import (
	"encoding/json"
	"reflect"
	"testing"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// fullMoveSpec 每个字段都非零值的规则内容：把 GroupID 以外的任一字段清零，都必须让 specEqualExceptGroup
// 认为内容变了（这样「换分组 + 改这一项」才会记成 update 而不是 move）。
func fullMoveSpec() RuleSpec {
	return RuleSpec{
		GroupID: "g1", Name: "r", Description: "d", Enabled: true,
		Labels:          map[string]string{"team": "infra"},
		DatasourceScope: mc.DatasourceScopeSpecified, DatasourceIDs: []string{"ds-1"},
		Queries:      []mc.Query{{Expr: "up == 0", Severity: mc.SeverityCritical}},
		LevelInhibit: true, EvalIntervalS: 60, ForS: 30, RecoverS: 60, NodataKeepS: 120,
		Effective: mc.Effective{
			Timezone: "Asia/Shanghai", Mode: mc.EffectiveModeWindows,
			Windows: []mc.TimeWindow{{Days: []int32{1, 2}, Start: "09:00", End: "18:00"}},
		},
		Annotations:    map[string]string{"summary": "x"},
		RecoveryExpr:   "up",
		Notify:         mc.NotifyTargets{UserIDs: []string{"u1"}, ScheduleIDs: []string{"s1"}, DeptIDs: []string{"d1"}, CIOwner: true},
		NotifyRecovery: true, RepeatIntervalM: 30, MaxNotify: 5, GroupKeyLabels: []string{"env"},
		CIMatch:         mc.CIMatch{Disabled: true, Label: "ident", By: mc.CIMatchByName, Attr: "ip", ModelCodes: []string{"host"}, EnrichTags: []string{"sn"}},
		ResourceScope:   mc.ResourceScope{Enabled: true, ModelCodes: []string{"host"}, Envs: []string{"prod"}, Tags: []string{"a"}},
		Relabel:         []mc.RelabelStep{{Action: mc.RelabelLabelDrop, SourceLabels: []string{"tmp"}, Separator: ";", Regex: "x", TargetLabel: "t", Replacement: "$1"}},
		DropExpr:        `{{ if eq $labels.env "dev" }}true{{ end }}`,
		TopologyInhibit: true, EscalationID: "e1",
		Actions: mc.RuleActions{
			Ticket:   mc.TicketAction{Enabled: true, TemplateID: "t1", AgentUserID: "a1"},
			Workflow: mc.WorkflowAction{Enabled: true, DefinitionKey: "f1", AgentUserID: "a1"}, AISummary: true,
		},
	}
}

// jsonClone 经 JSON 深拷贝（切片与映射不与原值共享）。
func jsonClone(s RuleSpec) RuleSpec {
	raw, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	var out RuleSpec
	if err := json.Unmarshal(raw, &out); err != nil {
		panic(err)
	}
	return out
}

// specEqualExceptGroup 覆盖 GroupID 以外的每一个字段（monitor 模块评审 core-monitor-rules P3-4）：此前通知、
// 生效时间、附加信息、升级策略、事件动作等都不参与比较，「换分组 + 改通知对象」被记成了 move。
func TestSpecEqualExceptGroup_CoversEveryField(t *testing.T) {
	t.Parallel()
	base := fullMoveSpec()
	twin := jsonClone(fullMoveSpec())
	if !specEqualExceptGroup(&base, &twin) {
		t.Fatal("two identical specs must compare equal")
	}
	// 只有分组不同：是单纯的移动。
	moved := jsonClone(base)
	moved.GroupID = "g2"
	if !specEqualExceptGroup(&base, &moved) {
		t.Fatal("a pure group change must compare equal")
	}
	// 逐个字段清零：任何一个字段变了都不再相等。
	typ := reflect.TypeOf(RuleSpec{})
	for i := range typ.NumField() {
		field := typ.Field(i)
		if field.Name == "GroupID" {
			continue
		}
		changed := jsonClone(twin)
		reflect.ValueOf(&changed).Elem().Field(i).SetZero()
		if specEqualExceptGroup(&twin, &changed) {
			t.Errorf("zeroing %s must count as a change: a move plus this edit is an update, not a move", field.Name)
		}
	}
}

// 切片与映射不区分 nil 与空：接口没带的字段与库里存的空容器是同一个内容。
func TestSpecEqualExceptGroup_TreatsNilAndEmptyAlike(t *testing.T) {
	t.Parallel()
	a, b := jsonClone(fullMoveSpec()), jsonClone(fullMoveSpec())
	a.Annotations, a.Notify.UserIDs, a.Notify.ScheduleIDs, a.Notify.DeptIDs = nil, nil, nil, nil
	a.Effective.Windows, a.GroupKeyLabels = nil, nil
	b.Annotations, b.Notify.UserIDs, b.Notify.ScheduleIDs, b.Notify.DeptIDs = map[string]string{}, []string{}, []string{}, []string{}
	b.Effective.Windows, b.GroupKeyLabels = []mc.TimeWindow{}, []string{}
	if !specEqualExceptGroup(&a, &b) {
		t.Fatal("nil and empty containers must compare equal")
	}
	// 时间窗有多有少则不等（a 的已置空）。
	b.Effective.Windows = []mc.TimeWindow{{Days: []int32{1}, Start: "09:00", End: "18:00"}}
	if specEqualExceptGroup(&a, &b) {
		t.Fatal("an extra time window must count as a change")
	}
}
