package monitorcontract

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestEscalationSeq(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		level, n, want int32
	}{
		{1, 1, 100001},
		{1, 99999, 199999},
		{10, 1, 1000001},
		{10, 99999, 1099999},
		{0, 1, 0},
		{11, 1, 0},
		{1, 0, 0},
		{1, 100000, 0},
		{-1, 1, 0},
	} {
		got := EscalationOutboxSeq(tc.level, tc.n)
		if got != tc.want {
			t.Errorf("EscalationOutboxSeq(%d, %d) = %d, want %d", tc.level, tc.n, got, tc.want)
		}
		if tc.want == 0 {
			continue
		}
		level, n, ok := SplitEscalationSeq(got)
		if !ok || level != tc.level || n != tc.n {
			t.Errorf("SplitEscalationSeq(%d) = %d, %d, %v", got, level, n, ok)
		}
	}
	// 第一期的通知序号（1、2、3…）与越界的编码都不是升级通知。
	for _, seq := range []int32{0, 1, 2, 1000, 99999, 100000, 200000, 1100001, -100001} {
		if _, _, ok := SplitEscalationSeq(seq); ok {
			t.Errorf("SplitEscalationSeq(%d) ok, want not", seq)
		}
	}
}

func TestP2Literals(t *testing.T) {
	t.Parallel()
	// 这些字面量落在库里或已经交给通知管道，改动即破坏契约。
	ints := map[string][2]int32{
		"NotifyKindEscalation": {NotifyKindEscalation, 4}, "NotifySourceEscalation": {NotifySourceEscalation, 3},
		"InhibitNone": {InhibitNone, 0}, "InhibitRule": {InhibitRule, 1}, "InhibitTopology": {InhibitTopology, 2},
		"AlertCmdClose": {AlertCmdClose, 1}, "CmdPending": {CmdPending, 1}, "CmdDone": {CmdDone, 2}, "CmdRejected": {CmdRejected, 3},
		"ActionTicket": {ActionTicket, 1}, "ActionWorkflow": {ActionWorkflow, 2}, "ActionAISummary": {ActionAISummary, 3},
		"ActionTicketNote": {ActionTicketNote, 4},
		"ActionPending":    {ActionPending, 1}, "ActionDone": {ActionDone, 2}, "ActionFailed": {ActionFailed, 3}, "ActionSkipped": {ActionSkipped, 4},
		"EscalationModeStack": {EscalationModeStack, 1}, "EscalationModeTakeover": {EscalationModeTakeover, 2},
		"EscalationTriggerUnacked": {EscalationTriggerUnacked, 1}, "EscalationTriggerUnresolved": {EscalationTriggerUnresolved, 2},
		"RecordNever": {RecordNever, 0}, "RecordOK": {RecordOK, 1}, "RecordNoData": {RecordNoData, 2},
		"RecordQueryError": {RecordQueryError, 3}, "RecordWriteError": {RecordWriteError, 4},
		"EscalationSeqBase": {EscalationSeqBase, 100000},
	}
	for name, v := range ints {
		if v[0] != v[1] {
			t.Errorf("%s = %d, want %d", name, v[0], v[1])
		}
	}
	strs := map[string][2]string{
		"EventAlertEscalated":       {EventAlertEscalated, "monitor.alert.escalated"},
		"EventEngineDatasourceDown": {EventEngineDatasourceDown, "monitor.engine.datasource_down"},
		"EventEngineDatasourceUp":   {EventEngineDatasourceUp, "monitor.engine.datasource_recovered"},
		"EventEngineRuleFailing":    {EventEngineRuleFailing, "monitor.engine.rule_failing"},
		"EventEngineRuleRecovered":  {EventEngineRuleRecovered, "monitor.engine.rule_recovered"},
		"LockCISync":                {LockCISync, "monitor:ci-sync"}, "LockEngineWatch": {LockEngineWatch, "monitor:engine-watch"},
		"CfgEscalation": {CfgEscalation, "monitor_escalation"}, "CfgInhibit": {CfgInhibit, "monitor_inhibit"},
		"CfgCIIndex": {CfgCIIndex, "monitor_ci_index"}, "CfgCalendar": {CfgCalendar, "monitor_calendar"},
		"EffectiveModeWindows": {EffectiveModeWindows, "windows"}, "EffectiveModeWorkTime": {EffectiveModeWorkTime, "work_time"},
		"EffectiveModeOffTime": {EffectiveModeOffTime, "off_time"},
		"CIMatchByName":        {CIMatchByName, "name"}, "CIMatchByAttr": {CIMatchByAttr, "attr"}, "DefaultCIMatchAttr": {DefaultCIMatchAttr, "ip"},
		"CIStatusMaintenance": {CIStatusMaintenance, "maintenance"}, "CILabelPrefix": {CILabelPrefix, "ci_"},
		"RelabelReplace": {RelabelReplace, "replace"}, "RelabelLabelMap": {RelabelLabelMap, "labelmap"},
		"RelabelLabelDrop": {RelabelLabelDrop, "labeldrop"}, "RelabelLabelKeep": {RelabelLabelKeep, "labelkeep"},
		"WorkflowBusinessType": {WorkflowBusinessType, "monitor_alert"},
		"CloseCauseManual":     {CloseCauseManual, "manual"}, "CloseCauseRule": {CloseCauseRule, "rule"},
		"EngineObjectDatasource": {EngineObjectDatasource, "datasource"}, "EngineObjectRule": {EngineObjectRule, "rule"},
		"StepDropped": {StepDropped, "dropped"}, "StepOutOfScope": {StepOutOfScope, "out_of_scope"},
		"StepInhibitedRule": {StepInhibitedRule, "inhibited_rule"}, "StepInhibitedTopology": {StepInhibitedTopology, "inhibited_topology"},
		"StepMutedMaintenance": {StepMutedMaintenance, "muted_maintenance"},
	}
	for name, v := range strs {
		if v[0] != v[1] {
			t.Errorf("%s = %q, want %q", name, v[0], v[1])
		}
	}
	wantEvents := []string{
		"fire", "resolve", "close", "mute", "unmute", "inhibit", "uninhibit", "escalate", "notify", "ack", "unack", "transfer",
		"comment", "ticket", "ticket_note", "workflow", "ai_summary", "action_failed", "incident",
	}
	if got := AlertEventTypes(); !slices.Equal(got, wantEvents) {
		t.Errorf("AlertEventTypes() = %v", got)
	}
	for _, e := range wantEvents {
		if !ValidAlertEventType(e) || len(e) > 16 {
			t.Errorf("alert event type %q invalid or longer than the column", e)
		}
	}
	if ValidAlertEventType("") || ValidAlertEventType("Fire") {
		t.Error("ValidAlertEventType accepts unknown types")
	}
	for _, e := range EngineEventTypes() {
		if !ValidEngineEventType(e) || !strings.HasPrefix(e, "monitor.engine.") {
			t.Errorf("engine event type %q", e)
		}
	}
	if len(EngineEventTypes()) != 4 || ValidEngineEventType(EventAlertFiring) {
		t.Error("engine event types")
	}
	// 返回的是副本：改写不影响下一次调用。
	AlertEventTypes()[0], EngineEventTypes()[0], DefaultCIMatches()[0].Label = "x", "x", "x"
	if AlertEventTypes()[0] != AlertEventFire || EngineEventTypes()[0] != EventEngineDatasourceDown || DefaultCIMatches()[0].Label != "ident" {
		t.Error("list accessors must return copies")
	}
	for k := int32(-1); k <= 6; k++ {
		if ValidActionKind(k) != (k >= 1 && k <= 4) {
			t.Errorf("ValidActionKind(%d)", k)
		}
	}
	if !ValidEscalationMode(1) || !ValidEscalationMode(2) || ValidEscalationMode(0) || ValidEscalationMode(3) {
		t.Error("ValidEscalationMode")
	}
	if d := DefaultCIMatches(); len(d) != 2 || d[0].Label != "ident" || d[0].By != "name" || d[0].Attr != "" ||
		d[1].Label != "instance" || d[1].By != "attr" || d[1].Attr != "ip" {
		t.Errorf("DefaultCIMatches() = %+v", d)
	}
}

func TestP2JSONShape(t *testing.T) {
	t.Parallel()
	yes := true
	for _, tc := range []struct {
		name string
		v    any
		want string
	}{
		{"effective zero keeps the phase 1 shape", Effective{}, `{"timezone":"","windows":null}`},
		{"effective work time", Effective{Timezone: "Asia/Shanghai", Mode: EffectiveModeWorkTime}, `{"timezone":"Asia/Shanghai","windows":null,"mode":"work_time"}`},
		{"notify targets zero keeps the phase 1 shape", NotifyTargets{}, `{"user_ids":null,"schedule_ids":null,"dept_ids":null}`},
		{"notify targets ci owner", NotifyTargets{CIOwner: true}, `{"user_ids":null,"schedule_ids":null,"dept_ids":null,"ci_owner":true}`},
		{"rule actions zero", RuleActions{}, `{"ticket":{},"workflow":{}}`},
		{
			"rule actions",
			RuleActions{Ticket: TicketAction{Enabled: true, TemplateID: "t", AgentUserID: "u"}, Workflow: WorkflowAction{Enabled: true, DefinitionKey: "k", AgentUserID: "u"}, AISummary: true},
			`{"ticket":{"enabled":true,"template_id":"t","agent_user_id":"u"},"workflow":{"enabled":true,"definition_key":"k","agent_user_id":"u"},"ai_summary":true}`,
		},
		{"action payload", ActionPayload{TemplateID: "t", AgentUserID: "u", DefinitionKey: "k"}, `{"template_id":"t","agent_user_id":"u","definition_key":"k"}`},
		{"ci match zero", CIMatch{}, `{}`},
		{
			"ci match",
			CIMatch{Disabled: true, Label: "instance", By: "attr", Attr: "ip", ModelCodes: []string{"host"}, EnrichTags: []string{"core"}},
			`{"disabled":true,"label":"instance","by":"attr","attr":"ip","model_codes":["host"],"enrich_tags":["core"]}`,
		},
		{"resource scope zero", ResourceScope{}, `{}`},
		{"resource scope", ResourceScope{Enabled: true, ModelCodes: []string{"host"}, Envs: []string{"prod"}, Tags: []string{"core"}}, `{"enabled":true,"model_codes":["host"],"envs":["prod"],"tags":["core"]}`},
		{
			"relabel step",
			RelabelStep{Action: "replace", SourceLabels: []string{"a", "b"}, Separator: "-", Regex: "(.+)", TargetLabel: "c", Replacement: "$1"},
			`{"action":"replace","source_labels":["a","b"],"separator":"-","regex":"(.+)","target_label":"c","replacement":"$1"}`,
		},
		{"relabel drop", RelabelStep{Action: "labeldrop", Regex: "tmp_.*"}, `{"action":"labeldrop","regex":"tmp_.*"}`},
		{
			"inhibit selector",
			InhibitSelector{RuleIDs: []string{"r"}, Severities: []int32{4}, Matchers: []Matcher{{Label: "a", Op: "==", Value: "b"}}},
			`{"rule_ids":["r"],"severities":[4],"matchers":[{"label":"a","op":"==","value":"b"}]}`,
		},
		{"inhibit selector zero", InhibitSelector{}, `{}`},
		{
			"escalation level",
			EscalationLevel{Trigger: 1, AfterM: 15, RepeatIntervalM: 30, MaxNotify: 3, Targets: EscalationTargets{
				UserIDs: []string{"u"}, Schedules: []EscalationSchedule{{ID: "s", BackupOnly: true}, {ID: "s2"}},
				OncallGroupIDs: []string{"g"}, Depts: []EscalationDept{{ID: "d", Up: 1}, {ID: "d2"}},
			}},
			`{"trigger":1,"after_m":15,"targets":{"user_ids":["u"],"schedules":[{"id":"s","backup_only":true},{"id":"s2"}],"oncall_group_ids":["g"],"depts":[{"id":"d","up":1},{"id":"d2"}]},"repeat_interval_m":30,"max_notify":3}`,
		},
		{"alert event detail zero", AlertEventDetail{}, `{}`},
		{
			"alert event detail",
			AlertEventDetail{
				Text: "t", Cause: "manual", FromUser: "a", ToUser: "b", MuteID: "m", CIID: "c", InhibitKind: 1, InhibitID: "i", InhibitAlertID: "s",
				Level: 2, Severity: 3, TicketID: "tk", TicketNumber: "T-1", Auto: true, WorkflowInstanceID: "w", ActionKind: 1, Error: "e",
			},
			`{"text":"t","cause":"manual","from_user":"a","to_user":"b","mute_id":"m","ci_id":"c","inhibit_kind":1,"inhibit_id":"i","inhibit_alert_id":"s","level":2,"severity":3,"ticket_id":"tk","ticket_number":"T-1","auto":true,"workflow_instance_id":"w","action_kind":1,"error":"e"}`,
		},
		{
			"engine event payload",
			EngineEventPayload{ObjectType: "rule", ObjectID: "r", ObjectName: "n", DatasourceName: "d", Error: "e", Since: "s", Duration: "5m", Failures: 3, URL: "u"},
			`{"object_type":"rule","object_id":"r","object_name":"n","datasource_name":"d","error":"e","since":"s","duration":"5m","failures":3,"url":"u"}`,
		},
		{"alert view filter zero", AlertViewFilter{}, `{}`},
		{
			"alert view filter",
			AlertViewFilter{GroupID: "g", DatasourceID: "d", Severities: []int32{3}, Muted: &yes, Acked: &yes, Inhibited: &yes, Labels: []string{"a=b"}},
			`{"group_id":"g","datasource_id":"d","severities":[3],"muted":true,"acked":true,"inhibited":true,"labels":["a=b"]}`,
		},
		{
			"calendar snapshot",
			CalendarSnapshot{Timezone: "Asia/Shanghai", Weekdays: []int32{1}, Segments: []CalendarSegment{{Start: "09:00", End: "18:00"}}, Holidays: []string{"2026-10-01"}, Workdays: []string{"2026-10-10"}},
			`{"timezone":"Asia/Shanghai","weekdays":[1],"segments":[{"start":"09:00","end":"18:00"}],"holidays":["2026-10-01"],"workdays":["2026-10-10"]}`,
		},
		{
			"eval point with phase 2 keys",
			EvalPoint{Fingerprint: "f", Labels: map[string]string{}, Severity: 1, Value: "1", Step: StepInhibitedRule, InhibitAlertID: "a", CIID: "c"},
			`{"fingerprint":"f","labels":{},"severity":1,"value":"1","step":"inhibited_rule","inhibit_alert_id":"a","ci_id":"c"}`,
		},
	} {
		got, err := json.Marshal(tc.v)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if string(got) != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.name, got, tc.want)
		}
	}
	// 升级通知的载荷键：第一期的键原样保留，新键在不使用时不出现。
	raw, _ := json.Marshal(NotifyPayload{})
	for _, key := range []string{"escalation_level", "escalation_seq", "escalation_name", "ci_id", "ci_name", "test"} {
		if strings.Contains(string(raw), key) {
			t.Errorf("zero NotifyPayload carries %q: %s", key, raw)
		}
	}
	raw, _ = json.Marshal(NotifyPayload{EscalationLevel: 2, EscalationSeq: 3, EscalationName: "p", CIID: "c", CIName: "n", Test: true})
	for _, part := range []string{`"escalation_level":2`, `"escalation_seq":3`, `"escalation_name":"p"`, `"ci_id":"c"`, `"ci_name":"n"`, `"test":true`} {
		if !strings.Contains(string(raw), part) {
			t.Errorf("NotifyPayload lacks %s: %s", part, raw)
		}
	}
}

func TestEffectiveMode(t *testing.T) {
	t.Parallel()
	win := []TimeWindow{{Days: []int32{1}, Start: "09:00", End: "18:00"}}
	for _, tc := range []struct {
		name string
		e    Effective
		ok   bool
	}{
		{"empty mode", Effective{Windows: win}, true},
		{"windows", Effective{Mode: "windows", Windows: win}, true},
		{"work time", Effective{Mode: "work_time"}, true},
		{"off time", Effective{Mode: "off_time", Timezone: "Asia/Shanghai"}, true},
		{"work time with windows", Effective{Mode: "work_time", Windows: win}, false},
		{"off time with windows", Effective{Mode: "off_time", Windows: win}, false},
		{"unknown mode", Effective{Mode: "holiday"}, false},
		{"case sensitive", Effective{Mode: "Work_Time"}, false},
	} {
		err := tc.e.Validate()
		if (err == nil) != tc.ok {
			t.Errorf("%s: Validate() = %v, want ok=%v", tc.name, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error does not wrap ErrInvalid", tc.name)
		}
	}
	if (Effective{}).UsesCalendar() || (Effective{Mode: "windows"}).UsesCalendar() ||
		!(Effective{Mode: "work_time"}).UsesCalendar() || !(Effective{Mode: "off_time"}).UsesCalendar() {
		t.Error("UsesCalendar")
	}
}

func shanghai(t *testing.T, s string) time.Time {
	t.Helper()
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	v, err := time.ParseInLocation("2006-01-02 15:04:05", s, loc)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestCalendar(t *testing.T) {
	t.Parallel()
	snap := CalendarSnapshot{
		Timezone: "Asia/Shanghai", Weekdays: []int32{1, 2, 3, 4, 5},
		Segments: []CalendarSegment{{Start: "09:00", End: "12:00"}, {Start: "13:30", End: "18:00"}},
		Holidays: []string{"2026-10-01", "2026-10-10"}, // 周四放假；周六同时列在两边时调休上班优先
		Workdays: []string{"2026-10-10"},
	}
	cal, err := snap.Compile()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		at   string
		want bool
	}{
		{"2026-11-18 08:59:59", false}, // 周三，上班前
		{"2026-11-18 09:00:00", true},  // 含起点
		{"2026-11-18 11:59:59", true},
		{"2026-11-18 12:00:00", false}, // 不含终点
		{"2026-11-18 13:29:00", false}, // 午休
		{"2026-11-18 13:30:00", true},
		{"2026-11-18 17:59:59", true},
		{"2026-11-18 18:00:00", false},
		{"2026-11-21 10:00:00", false}, // 周六
		{"2026-11-22 10:00:00", false}, // 周日（Weekday 0）
		{"2026-10-01 10:00:00", false}, // 节假日（周四）
		{"2026-10-10 10:00:00", true},  // 调休上班日（周六），优先于节假日
		{"2026-10-10 20:00:00", false}, // 调休上班日也只在工作时段内
	} {
		at := shanghai(t, tc.at)
		if got := cal.Working(at); got != tc.want {
			t.Errorf("Working(%s) = %v, want %v", tc.at, got, tc.want)
		}
		// 判定按日历时区，与传入时刻所带的时区无关。
		if got := cal.Working(at.UTC()); got != tc.want {
			t.Errorf("Working(%s in UTC) = %v, want %v", tc.at, got, tc.want)
		}
		work, off := Effective{Mode: EffectiveModeWorkTime}, Effective{Mode: EffectiveModeOffTime}
		if work.ContainsWithCalendar(at, cal) != tc.want || off.ContainsWithCalendar(at, cal) == tc.want {
			t.Errorf("ContainsWithCalendar(%s) disagrees with Working", tc.at)
		}
	}
	// 日期按日历时区切换：UTC 的周五 16:30 已是上海的周六 00:30。
	if cal.Working(time.Date(2026, 11, 20, 16, 30, 0, 0, time.UTC)) {
		t.Error("Friday 16:30 UTC is Saturday in Shanghai")
	}
	// 24:00 结尾的时段覆盖到当天最后一分钟。
	night, err := CalendarSnapshot{Timezone: "UTC", Weekdays: []int32{0, 1, 2, 3, 4, 5, 6}, Segments: []CalendarSegment{{Start: "22:00", End: "24:00"}}}.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if !night.Working(time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC)) || night.Working(time.Date(2026, 1, 1, 21, 59, 0, 0, time.UTC)) ||
		night.Working(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Error("24:00 segment")
	}

	var none *Calendar
	now := shanghai(t, "2026-11-18 10:00:00")
	if none.Working(now) {
		t.Error("nil calendar must not be working")
	}
	// 没有日历时按工作日历的规则恒生效（两种方式都是）。
	for _, mode := range []string{EffectiveModeWorkTime, EffectiveModeOffTime} {
		if !(Effective{Mode: mode}).ContainsWithCalendar(now, nil) {
			t.Errorf("mode %s without a calendar must be effective", mode)
		}
	}
	// 时间窗方式不看日历；无法识别的方式按时间窗处理。
	win := Effective{Timezone: "Asia/Shanghai", Windows: []TimeWindow{{Days: []int32{3}, Start: "20:00", End: "21:00"}}}
	if win.ContainsWithCalendar(now, cal) || !win.ContainsWithCalendar(shanghai(t, "2026-11-18 20:30:00"), cal) {
		t.Error("windows mode must ignore the calendar")
	}
	win.Mode = "bogus"
	if win.ContainsWithCalendar(now, cal) != win.Contains(now) {
		t.Error("unknown mode must fall back to windows")
	}
	if !(Effective{}).ContainsWithCalendar(now, nil) {
		t.Error("zero Effective is always effective")
	}
}

func TestCalendarCompileErrors(t *testing.T) {
	t.Parallel()
	valid := func() CalendarSnapshot {
		return CalendarSnapshot{Timezone: "Asia/Shanghai", Weekdays: []int32{1}, Segments: []CalendarSegment{{Start: "09:00", End: "18:00"}}}
	}
	if _, err := valid().Compile(); err != nil {
		t.Fatal(err)
	}
	many := make([]string, 401)
	for i := range many {
		many[i] = "2026-01-01"
	}
	for name, mutate := range map[string]func(*CalendarSnapshot){
		"zero value":        func(s *CalendarSnapshot) { *s = CalendarSnapshot{} },
		"empty timezone":    func(s *CalendarSnapshot) { s.Timezone = "" },
		"local timezone":    func(s *CalendarSnapshot) { s.Timezone = "Local" },
		"unknown timezone":  func(s *CalendarSnapshot) { s.Timezone = "Mars/Olympus" },
		"no weekdays":       func(s *CalendarSnapshot) { s.Weekdays = nil },
		"weekday 7":         func(s *CalendarSnapshot) { s.Weekdays = []int32{7} },
		"negative weekday":  func(s *CalendarSnapshot) { s.Weekdays = []int32{-1} },
		"duplicate weekday": func(s *CalendarSnapshot) { s.Weekdays = []int32{1, 1} },
		"too many weekdays": func(s *CalendarSnapshot) { s.Weekdays = []int32{0, 1, 2, 3, 4, 5, 6, 0} },
		"no segments":       func(s *CalendarSnapshot) { s.Segments = nil },
		"too many segments": func(s *CalendarSnapshot) { s.Segments = make([]CalendarSegment, 7) },
		"bad clock":         func(s *CalendarSnapshot) { s.Segments[0].Start = "9:00" },
		"25 o'clock":        func(s *CalendarSnapshot) { s.Segments[0].End = "25:00" },
		"empty segment":     func(s *CalendarSnapshot) { s.Segments[0].End = "09:00" },
		"reversed segment":  func(s *CalendarSnapshot) { s.Segments[0].End = "08:00" },
		"overlapping": func(s *CalendarSnapshot) {
			s.Segments = append(s.Segments, CalendarSegment{Start: "17:00", End: "19:00"})
		},
		"bad holiday":        func(s *CalendarSnapshot) { s.Holidays = []string{"2026/10/01"} },
		"bad workday":        func(s *CalendarSnapshot) { s.Workdays = []string{"10-01"} },
		"too many holidays":  func(s *CalendarSnapshot) { s.Holidays = many },
		"too many work days": func(s *CalendarSnapshot) { s.Workdays = many },
	} {
		s := valid()
		mutate(&s)
		if cal, err := s.Compile(); err == nil || cal != nil || !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Compile() = %v, %v; want ErrInvalid", name, cal, err)
		}
	}
}

func TestCIMatchValidate(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("a", 65)
	models := make([]string, 21)
	tags := make([]string, 11)
	for i := range models {
		models[i] = "m" + string(rune('a'+i))
	}
	for i := range tags {
		tags[i] = "t" + string(rune('a'+i))
	}
	for _, tc := range []struct {
		name string
		m    CIMatch
		ok   bool
	}{
		{"zero", CIMatch{}, true},
		{"disabled", CIMatch{Disabled: true}, true},
		{"defaults with models and tags", CIMatch{ModelCodes: []string{"host"}, EnrichTags: []string{"核心"}}, true},
		{"by name", CIMatch{Label: "ident", By: "name"}, true},
		{"by attr default", CIMatch{Label: "instance", By: "attr"}, true},
		{"by attr named", CIMatch{Label: "instance", By: "attr", Attr: "mgmt_ip_v4"}, true},
		{"upper case attr", CIMatch{Label: "instance", By: "attr", Attr: "IP"}, false},
		{"by without label", CIMatch{By: "name"}, false},
		{"attr without label", CIMatch{Attr: "ip"}, false},
		{"label without by", CIMatch{Label: "ident"}, false},
		{"unknown by", CIMatch{Label: "ident", By: "id"}, false},
		{"bad label", CIMatch{Label: "1x", By: "name"}, false},
		{"name with attr", CIMatch{Label: "ident", By: "name", Attr: "ip"}, false},
		{"bad attr", CIMatch{Label: "ident", By: "attr", Attr: "a b"}, false},
		{"long attr", CIMatch{Label: "ident", By: "attr", Attr: long}, false},
		{"attr of 64", CIMatch{Label: "ident", By: "attr", Attr: long[:64]}, true},
		{"too many models", CIMatch{ModelCodes: models}, false},
		{"duplicate model", CIMatch{ModelCodes: []string{"a", "a"}}, false},
		{"empty model", CIMatch{ModelCodes: []string{""}}, false},
		{"long model", CIMatch{ModelCodes: []string{long}}, false},
		{"control char", CIMatch{ModelCodes: []string{"a\nb"}}, false},
		{"invalid utf8", CIMatch{EnrichTags: []string{"\xff"}}, false},
		{"too many tags", CIMatch{EnrichTags: tags}, false},
	} {
		err := tc.m.Validate()
		if (err == nil) != tc.ok {
			t.Errorf("%s: Validate() = %v, want ok=%v", tc.name, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error does not wrap ErrInvalid", tc.name)
		}
	}
}

func TestResourceScopeValidate(t *testing.T) {
	t.Parallel()
	many := make([]string, 21)
	for i := range many {
		many[i] = "v" + string(rune('a'+i))
	}
	for _, tc := range []struct {
		name string
		s    ResourceScope
		ok   bool
	}{
		{"zero", ResourceScope{}, true},
		{"enabled without conditions", ResourceScope{Enabled: true}, true},
		{"enabled", ResourceScope{Enabled: true, ModelCodes: []string{"host"}, Envs: []string{"prod"}, Tags: []string{"core"}}, true},
		{"conditions while disabled", ResourceScope{Envs: []string{"prod"}}, false},
		{"too many envs", ResourceScope{Enabled: true, Envs: many}, false},
		{"too many tags", ResourceScope{Enabled: true, Tags: many}, false},
		{"too many models", ResourceScope{Enabled: true, ModelCodes: many}, false},
		{"duplicate", ResourceScope{Enabled: true, Tags: []string{"a", "a"}}, false},
		{"empty value", ResourceScope{Enabled: true, Envs: []string{""}}, false},
	} {
		if err := tc.s.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: Validate() = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

func TestValidateInhibit(t *testing.T) {
	t.Parallel()
	src := InhibitSelector{Matchers: []Matcher{{Label: "alertname", Op: OpEqual, Value: "HostDown"}}}
	ids := make([]string, 201)
	equal := make([]string, 11)
	for i := range equal {
		equal[i] = "l" + string(rune('a'+i))
	}
	for _, tc := range []struct {
		name           string
		source, target InhibitSelector
		equal          []string
		ok             bool
	}{
		{"source with equal", src, InhibitSelector{}, []string{"ident"}, true},
		{"source with target", src, InhibitSelector{Severities: []int32{1, 2}}, nil, true},
		{"source by rule", InhibitSelector{RuleIDs: []string{"r"}}, InhibitSelector{RuleIDs: []string{"x"}}, []string{"a", "b"}, true},
		{"source by severity", InhibitSelector{Severities: []int32{4}}, InhibitSelector{}, []string{"ident"}, true},
		{"empty source", InhibitSelector{}, InhibitSelector{Severities: []int32{1}}, []string{"ident"}, false},
		{"empty target and equal", src, InhibitSelector{}, nil, false},
		{"bad equal label", src, InhibitSelector{}, []string{"1x"}, false},
		{"duplicate equal label", src, InhibitSelector{}, []string{"a", "a"}, false},
		{"too many equal labels", src, InhibitSelector{}, equal, false},
		{"bad source severity", InhibitSelector{Severities: []int32{5}}, InhibitSelector{}, []string{"a"}, false},
		{"duplicate target severity", src, InhibitSelector{Severities: []int32{1, 1}}, nil, false},
		{"bad source matcher", InhibitSelector{Matchers: []Matcher{{Label: "a", Op: "=~", Value: "("}}}, InhibitSelector{}, []string{"a"}, false},
		{"bad target matcher", src, InhibitSelector{Matchers: []Matcher{{Label: "a", Op: "?"}}}, nil, false},
		{"too many source rules", InhibitSelector{RuleIDs: ids}, InhibitSelector{}, []string{"a"}, false},
		{"too many target rules", src, InhibitSelector{RuleIDs: ids}, nil, false},
	} {
		err := ValidateInhibit(tc.source, tc.target, tc.equal)
		if (err == nil) != tc.ok {
			t.Errorf("%s: ValidateInhibit() = %v, want ok=%v", tc.name, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error does not wrap ErrInvalid", tc.name)
		}
	}
	if !(InhibitSelector{}).Empty() || src.Empty() || (InhibitSelector{RuleIDs: []string{"r"}}).Empty() || (InhibitSelector{Severities: []int32{1}}).Empty() {
		t.Error("InhibitSelector.Empty")
	}
}

func TestValidateEscalationLevels(t *testing.T) {
	t.Parallel()
	users := EscalationTargets{UserIDs: []string{"u1"}}
	level := func(mutate func(*EscalationLevel)) []EscalationLevel {
		l := EscalationLevel{Trigger: EscalationTriggerUnacked, AfterM: 10, Targets: users, RepeatIntervalM: 15, MaxNotify: 3}
		if mutate != nil {
			mutate(&l)
		}
		return []EscalationLevel{l}
	}
	eleven := make([]EscalationLevel, 11)
	for i := range eleven {
		eleven[i] = EscalationLevel{Trigger: 1, AfterM: int32(i + 1), Targets: users}
	}
	many := make([]string, 21)
	for i := range many {
		many[i] = "u" + string(rune('a'+i))
	}
	for _, tc := range []struct {
		name   string
		levels []EscalationLevel
		ok     bool
	}{
		{"one level", level(nil), true},
		{"all target kinds", level(func(l *EscalationLevel) {
			l.Targets = EscalationTargets{
				UserIDs: []string{"u"}, Schedules: []EscalationSchedule{{ID: "s", BackupOnly: true}}, OncallGroupIDs: []string{"g"},
				Depts: []EscalationDept{{ID: "d"}, {ID: "d", Up: 1}},
			}
		}), true},
		{"two levels increasing", []EscalationLevel{
			{Trigger: 1, AfterM: 10, Targets: users}, {Trigger: 2, AfterM: 30, Targets: users, RepeatIntervalM: 60, MaxNotify: 2},
		}, true},
		{"ten levels", eleven[:10], true},
		{"limits", level(func(l *EscalationLevel) { l.AfterM, l.RepeatIntervalM, l.MaxNotify = 10080, 10080, 100 }), true},
		{"no repeat no cap", level(func(l *EscalationLevel) { l.RepeatIntervalM, l.MaxNotify = 0, 0 }), true},
		{"no levels", nil, false},
		{"eleven levels", eleven, false},
		{"bad trigger", level(func(l *EscalationLevel) { l.Trigger = 3 }), false},
		{"zero trigger", level(func(l *EscalationLevel) { l.Trigger = 0 }), false},
		{"after zero", level(func(l *EscalationLevel) { l.AfterM = 0 }), false},
		{"after too large", level(func(l *EscalationLevel) { l.AfterM = 10081 }), false},
		{"not increasing", []EscalationLevel{{Trigger: 1, AfterM: 10, Targets: users}, {Trigger: 1, AfterM: 10, Targets: users}}, false},
		{"decreasing", []EscalationLevel{{Trigger: 1, AfterM: 10, Targets: users}, {Trigger: 1, AfterM: 5, Targets: users}}, false},
		{"repeat below 5", level(func(l *EscalationLevel) { l.RepeatIntervalM = 4 }), false},
		{"repeat too large", level(func(l *EscalationLevel) { l.RepeatIntervalM = 10081 }), false},
		{"negative max", level(func(l *EscalationLevel) { l.MaxNotify = -1 }), false},
		{"max too large", level(func(l *EscalationLevel) { l.MaxNotify = 101 }), false},
		{"no targets", level(func(l *EscalationLevel) { l.Targets = EscalationTargets{} }), false},
		{"too many users", level(func(l *EscalationLevel) { l.Targets = EscalationTargets{UserIDs: many} }), false},
		{"duplicate user", level(func(l *EscalationLevel) { l.Targets = EscalationTargets{UserIDs: []string{"a", "a"}} }), false},
		{"empty user", level(func(l *EscalationLevel) { l.Targets = EscalationTargets{UserIDs: []string{""}} }), false},
		{"duplicate group", level(func(l *EscalationLevel) { l.Targets = EscalationTargets{OncallGroupIDs: []string{"g", "g"}} }), false},
		{"duplicate schedule", level(func(l *EscalationLevel) {
			l.Targets = EscalationTargets{Schedules: []EscalationSchedule{{ID: "s"}, {ID: "s", BackupOnly: true}}}
		}), false},
		{"empty schedule", level(func(l *EscalationLevel) { l.Targets = EscalationTargets{Schedules: []EscalationSchedule{{}}} }), false},
		{"duplicate dept level", level(func(l *EscalationLevel) {
			l.Targets = EscalationTargets{Depts: []EscalationDept{{ID: "d", Up: 1}, {ID: "d", Up: 1}}}
		}), false},
		{"dept up too large", level(func(l *EscalationLevel) { l.Targets = EscalationTargets{Depts: []EscalationDept{{ID: "d", Up: 6}}} }), false},
		{"dept up negative", level(func(l *EscalationLevel) { l.Targets = EscalationTargets{Depts: []EscalationDept{{ID: "d", Up: -1}}} }), false},
	} {
		err := ValidateEscalationLevels(tc.levels)
		if (err == nil) != tc.ok {
			t.Errorf("%s: ValidateEscalationLevels() = %v, want ok=%v", tc.name, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error does not wrap ErrInvalid", tc.name)
		}
	}
	if !(EscalationTargets{}).Empty() || users.Empty() {
		t.Error("EscalationTargets.Empty")
	}
}

func TestValidMetricName(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]bool{
		"job:http_requests:rate5m": true, "_x": true, ":a": true, "a1": true,
		"": false, "1a": false, "a-b": false, "a b": false, "指标": false, strings.Repeat("a", 129): false,
	} {
		if ValidMetricName(name) != want {
			t.Errorf("ValidMetricName(%q) != %v", name, want)
		}
	}
}

func BenchmarkCalendarWorking(b *testing.B) {
	cal, err := CalendarSnapshot{
		Timezone: "Asia/Shanghai", Weekdays: []int32{1, 2, 3, 4, 5},
		Segments: []CalendarSegment{{Start: "09:00", End: "12:00"}, {Start: "13:30", End: "18:00"}},
		Holidays: []string{"2026-10-01", "2026-10-02"}, Workdays: []string{"2026-10-10"},
	}.Compile()
	if err != nil {
		b.Fatal(err)
	}
	e := Effective{Mode: EffectiveModeWorkTime}
	now := time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	for b.Loop() {
		if !e.ContainsWithCalendar(now, cal) {
			b.Fatal("expected working time")
		}
	}
}
