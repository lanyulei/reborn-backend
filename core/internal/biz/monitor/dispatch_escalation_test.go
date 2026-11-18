package biz_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

const (
	escID      = "018f3e1a-eeee-7000-8000-00000000e001"
	ogOps      = "og-ops"
	ciWeb      = "018f3e1a-cccc-7000-8000-00000000c001"
	uOwnerUUID = "018f3e1a-dddd-7000-8000-00000000d001"
)

// fakeRoster / fakeOncallGroups / fakeCIIndexB 升级派发用到的跨域桩与资源索引。
type fakeRoster struct {
	primary, backup map[string][]string
	err             error
}

func (f *fakeRoster) OnDutyByRole(_ context.Context, id string) ([]string, []string, error) {
	return f.primary[id], f.backup[id], f.err
}

type fakeOncallGroups struct {
	groups []biz.OncallGroup
	admins map[string][]string
}

func (f *fakeOncallGroups) Groups(context.Context) ([]biz.OncallGroup, error) { return f.groups, nil }

func (f *fakeOncallGroups) AdminIDs(_ context.Context, id string) ([]string, error) {
	return f.admins[id], nil
}

type fakeCIIndexB struct {
	biz.CIIndexReader
	entries map[string]*biz.CIEntry
	reads   int
}

func (f *fakeCIIndexB) Briefs(_ context.Context, ids []string) (map[string]*biz.CIEntry, error) {
	f.reads++
	out := map[string]*biz.CIEntry{}
	for _, id := range ids {
		if e, ok := f.entries[id]; ok {
			out[id] = e
		}
	}
	return out, nil
}

// escEnv 在 dispatchEnv 之上：规则引用一条两级升级策略。第 1 级通知运维值班的备班与值班分组管理员，
// 第 2 级通知运维部负责人与 dave。
type escEnv struct {
	*dispatchEnv
	policies *testfake.MonitorEscalations
	roster   *fakeRoster
	cis      *fakeCIIndexB
}

func newEscEnv(t *testing.T) *escEnv {
	t.Helper()
	d := newDispatchEnv(t)
	e := &escEnv{
		dispatchEnv: d, policies: testfake.NewMonitorEscalations(),
		roster: &fakeRoster{primary: map[string][]string{schedOps: {uBob}}, backup: map[string][]string{schedOps: {uCarol}}},
		cis:    &fakeCIIndexB{entries: map[string]*biz.CIEntry{}},
	}
	e.policies.Seed(&biz.Escalation{ID: escID, Spec: biz.EscalationSpec{
		GroupID: gInfra, Name: "基础设施升级", Mode: mc.EscalationModeStack, MinSeverity: 1, Enabled: true,
		Levels: []mc.EscalationLevel{
			{Trigger: mc.EscalationTriggerUnacked, AfterM: 15, RepeatIntervalM: 30, MaxNotify: 3, Targets: mc.EscalationTargets{
				Schedules: []mc.EscalationSchedule{{ID: schedOps, BackupOnly: true}}, OncallGroupIDs: []string{ogOps},
			}},
			{Trigger: mc.EscalationTriggerUnresolved, AfterM: 60, Targets: mc.EscalationTargets{
				UserIDs: []string{uDave, uGone}, Depts: []mc.EscalationDept{{ID: deptOps, Up: 1}},
			}},
		},
	}})
	d.store.SetRule(d.rule.ID, func(r *biz.Rule) { r.Spec.EscalationID = escID })
	d.dispatch.WithP2(biz.NewDispatchExt(e.policies, e.policies, e.cis, e.roster,
		&fakeOncallGroups{admins: map[string][]string{ogOps: {uAlice, uCarol}}}))
	return e
}

func (e *escEnv) seedEsc(id string, level, n int32) {
	e.store.SeedOutboxLevel(id, e.alert, mc.NotifyKindEscalation, mc.EscalationOutboxSeq(level, n), level)
}

func TestDispatchEscalation_LevelRecipientsAndPayload(t *testing.T) {
	t.Parallel()
	e := newEscEnv(t)
	e.seedEsc("ob-e1", 1, 1)
	if n := e.run(t); n != 1 {
		t.Fatalf("processed = %d", n)
	}
	events := e.notifyEvents(t)
	if len(events) != 1 {
		t.Fatalf("events = %d", len(events))
	}
	ev := events[0]
	// 仅备班（carol）∪ 值班分组管理员（alice、carol），去重；主班 bob 不通知。
	if ev.EventType != mc.EventAlertEscalated || !slices.Equal(ev.Recipients, []string{uCarol, uAlice}) || ev.Severity != mc.SeverityCritical {
		t.Fatalf("event = %+v", ev)
	}
	// 去重键用发件箱的编码序号：不同级、不同次自然不同。
	wantKey := "monitor:" + alertFP + ":" + strings.ReplaceAll(e.alert, "-", "") + ":4:100001"
	if ev.DedupKey != wantKey {
		t.Fatalf("dedup = %q, want %q", ev.DedupKey, wantKey)
	}
	p := payloadOf(t, ev)
	if p.EscalationLevel != 1 || p.EscalationSeq != 1 || p.EscalationName != "基础设施升级" || p.NotifySeq != 1 ||
		p.Status != mc.PayloadStatusFiring || p.RuleName != "cpu-high" || p.GroupName != "基础设施 / 主机" || p.Value != "95.5" ||
		p.FirstFireTime == "" || p.Duration == "" || p.AlertURL == "" {
		t.Fatalf("payload = %+v", p)
	}
	// 内置模板（0076）读取的键都在载荷里。
	var raw map[string]any
	if err := json.Unmarshal(ev.Payload, &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{
		"escalation_level", "escalation_seq", "escalation_name", "rule_name", "group_name", "datasource_name",
		"value", "first_fire_time", "duration", "labels", "annotations", "expr", "alert_url",
	} {
		if _, ok := raw[k]; !ok {
			t.Errorf("payload misses key %q", k)
		}
	}
	logs := e.store.NotifyLogs()
	if len(logs) != 1 || logs[0].Kind != mc.NotifyKindEscalation || logs[0].Source != mc.NotifySourceEscalation || logs[0].Level != 1 ||
		logs[0].Seq != 1 || logs[0].SubscriptionID != "" || !slices.Equal(logs[0].Recipients, []string{uCarol, uAlice}) {
		t.Fatalf("log = %+v", logs[0])
	}
	if p, ok := e.policies.Progress(e.alert, 1); !ok || p.NotifyCount != 1 {
		t.Fatalf("progress = %+v %v", p, ok)
	}
	// 升级通知不计入事件的通知次数。
	if a := e.store.Alert(e.alert); a.NotifyCount != 0 || a.LastNotifyTime != nil {
		t.Fatalf("alert notify_count = %d (must stay untouched)", a.NotifyCount)
	}
	if row := e.store.OutboxRow("ob-e1"); row.Status != mc.OutboxDone {
		t.Fatalf("outbox = %+v", row)
	}
}

func TestDispatchEscalation_RepeatsLevelsAndMonotonicProgress(t *testing.T) {
	t.Parallel()
	e := newEscEnv(t)
	e.seedEsc("ob-1", 1, 1)
	e.seedEsc("ob-2", 1, 2)
	e.seedEsc("ob-3", 2, 1)
	if n := e.run(t); n != 3 {
		t.Fatalf("processed = %d", n)
	}
	events := e.notifyEvents(t)
	if len(events) != 3 {
		t.Fatalf("events = %d", len(events))
	}
	keys := map[string]bool{}
	for _, ev := range events {
		keys[ev.DedupKey] = true
	}
	if len(keys) != 3 {
		t.Fatalf("dedup keys collide: %v", keys)
	}
	// 第 2 级：部门负责人（carol，上溯一级退回本部门）∪ dave；停用的用户被滤掉。
	last := payloadOf(t, events[2])
	if last.EscalationLevel != 2 || last.EscalationSeq != 1 || !slices.Equal(events[2].Recipients, []string{uDave, uCarol}) {
		t.Fatalf("level 2 = %+v recipients %v", last, events[2].Recipients)
	}
	if second := payloadOf(t, events[1]); second.EscalationLevel != 1 || second.EscalationSeq != 2 || second.NotifySeq != 2 {
		t.Fatalf("repeat = %+v", second)
	}
	if p, _ := e.policies.Progress(e.alert, 1); p.NotifyCount != 2 {
		t.Fatalf("level 1 progress = %d", p.NotifyCount)
	}
	if p, _ := e.policies.Progress(e.alert, 2); p.NotifyCount != 1 {
		t.Fatalf("level 2 progress = %d", p.NotifyCount)
	}
	// 迟到的更早序号不会把进度拉回去。
	if err := e.policies.TouchLevelTx(context.Background(), nil, e.alert, 1, 1); err != nil {
		t.Fatal(err)
	}
	if p, _ := e.policies.Progress(e.alert, 1); p.NotifyCount != 2 {
		t.Fatalf("progress went backwards: %d", p.NotifyCount)
	}
	if a := e.store.Alert(e.alert); a.NotifyCount != 0 {
		t.Fatalf("alert notify_count = %d", a.NotifyCount)
	}
	logs := e.store.NotifyLogs()
	if len(logs) != 3 || logs[2].Level != 2 || logs[2].Seq != 1 || logs[1].Seq != 2 {
		t.Fatalf("logs = %d", len(logs))
	}
}

func TestDispatchEscalation_NoRecipientsEmitsWithoutThem(t *testing.T) {
	t.Parallel()
	e := newEscEnv(t)
	e.roster.backup = nil
	e.dispatch.WithP2(biz.NewDispatchExt(e.policies, e.policies, e.cis, e.roster, &fakeOncallGroups{}))
	e.seedEsc("ob-1", 1, 1)
	e.run(t)
	events := e.notifyEvents(t)
	logs := e.store.NotifyLogs()
	if len(events) != 1 || len(events[0].Recipients) != 0 || len(logs) != 1 || logs[0].Note == "" || len(logs[0].Recipients) != 0 {
		t.Fatalf("events = %+v logs = %+v", events, logs)
	}
	if p, ok := e.policies.Progress(e.alert, 1); !ok || p.NotifyCount != 1 {
		t.Fatal("progress must still be recorded: the notification was emitted")
	}
}

// 不该派发的升级通知：行不自洽、事件不在告警中 / 被屏蔽 / 被抑制、规则或策略不可用。
func TestDispatchEscalation_Drops(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		seed   func(e *escEnv)
		reason string
	}{
		{"seq is not an escalation seq", func(e *escEnv) { e.store.SeedOutboxLevel("ob", e.alert, mc.NotifyKindEscalation, 3, 1) }, "序号不合法"},
		{"level column disagrees with seq", func(e *escEnv) {
			e.store.SeedOutboxLevel("ob", e.alert, mc.NotifyKindEscalation, mc.EscalationOutboxSeq(2, 1), 1)
		}, "序号不合法"},
		{"level column missing", func(e *escEnv) {
			e.store.SeedOutboxLevel("ob", e.alert, mc.NotifyKindEscalation, mc.EscalationOutboxSeq(1, 1), 0)
		}, "序号不合法"},
		{"firing row carrying a level", func(e *escEnv) {
			e.store.SeedOutboxLevel("ob", e.alert, mc.NotifyKindFiring, mc.EscalationOutboxSeq(1, 1), 1)
		}, "不一致"},
		{"unknown kind", func(e *escEnv) { e.store.SeedOutboxLevel("ob", e.alert, 9, 1, 0) }, "未知的通知类型"},
		{"resolved alert", func(e *escEnv) {
			e.store.SetAlert(e.alert, func(a *biz.Alert) { a.Status = mc.AlertResolved })
			e.seedEsc("ob", 1, 1)
		}, "不在告警中"},
		{"closed alert", func(e *escEnv) {
			e.store.SetAlert(e.alert, func(a *biz.Alert) { a.Status = mc.AlertClosed })
			e.seedEsc("ob", 1, 1)
		}, "已关闭"},
		{"muted alert", func(e *escEnv) {
			e.store.SetAlert(e.alert, func(a *biz.Alert) { a.Muted = true })
			e.seedEsc("ob", 1, 1)
		}, "屏蔽"},
		{"inhibited alert", func(e *escEnv) {
			e.store.SetAlert(e.alert, func(a *biz.Alert) { a.Inhibited, a.InhibitKind = true, mc.InhibitRule })
			e.seedEsc("ob", 1, 1)
		}, "抑制"},
		{"rule no longer references a policy", func(e *escEnv) {
			e.store.SetRule(e.rule.ID, func(r *biz.Rule) { r.Spec.EscalationID = "" })
			e.seedEsc("ob", 1, 1)
		}, "未引用"},
		{"policy deleted", func(e *escEnv) {
			e.policies.Remove(escID)
			e.seedEsc("ob", 1, 1)
		}, "已删除"},
		{"policy disabled", func(e *escEnv) {
			p, _ := e.policies.Get(context.Background(), escID)
			p.Spec.Enabled = false
			e.policies.Seed(p)
			e.seedEsc("ob", 1, 1)
		}, "已停用"},
		{"level beyond the policy", func(e *escEnv) { e.seedEsc("ob", 3, 1) }, "没有这一级"},
		{"alert gone", func(e *escEnv) {
			e.store.SeedOutboxLevel("ob", "018f3e1a-aaaa-7000-8000-0000000000ff", mc.NotifyKindEscalation, mc.EscalationOutboxSeq(1, 1), 1)
		}, "不存在"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			e := newEscEnv(t)
			c.seed(e)
			if n := e.run(t); n != 1 {
				t.Fatalf("processed = %d", n)
			}
			row := e.store.OutboxRow("ob")
			if row.Status != mc.OutboxDropped || !strings.Contains(row.LastError, c.reason) {
				t.Fatalf("outbox = %+v, want dropped with %q", row, c.reason)
			}
			if len(e.notifyEvents(t)) != 0 || len(e.store.NotifyLogs()) != 0 {
				t.Fatal("a dropped row must not notify anyone")
			}
			if _, ok := e.policies.Progress(e.alert, 1); ok {
				t.Fatal("a dropped row must not advance the level progress")
			}
			if a := e.store.Alert(e.alert); a != nil && a.NotifyCount != 0 {
				t.Fatalf("alert notify_count = %d", a.NotifyCount)
			}
		})
	}
}

// 第一期的 core 会把 kind = 4 的行当成触发通知派发并把 notify_count 抬到 100001：这里确认两种通知并存
// 时互不干扰——触发通知照常计数，升级通知只动自己的进度；同一事件仍按入箱顺序派发。
func TestDispatchEscalation_DoesNotDisturbFiringPath(t *testing.T) {
	t.Parallel()
	e := newEscEnv(t)
	sub := e.subscribe(t, uDave, nil)
	e.store.SeedOutbox("ob-fire", e.alert, mc.NotifyKindFiring, 1)
	e.seedEsc("ob-esc", 1, 1)
	e.store.SeedOutbox("ob-repeat", e.alert, mc.NotifyKindRepeat, 2)
	if n := e.run(t); n != 3 {
		t.Fatalf("processed = %d", n)
	}
	if a := e.store.Alert(e.alert); a.NotifyCount != 2 {
		t.Fatalf("notify_count = %d, want 2 (firing + repeat only)", a.NotifyCount)
	}
	var types []string
	for _, l := range e.store.NotifyLogs() {
		types = append(types, string(rune('0'+l.Kind))+"/"+string(rune('0'+l.Source)))
		// 升级通知不走订阅匹配。
		if l.Kind == mc.NotifyKindEscalation && l.SubscriptionID != "" {
			t.Fatal("escalation must not be matched against subscriptions")
		}
	}
	// 触发：规则 + 订阅；升级：一条；重复：规则（订阅不跟随重复）。
	if !slices.Equal(types, []string{"1/1", "1/2", "4/3", "2/1"}) {
		t.Fatalf("logs (kind/source) = %v (subscription %s)", types, sub.ID)
	}
	if p, _ := e.policies.Progress(e.alert, 1); p.NotifyCount != 1 {
		t.Fatalf("progress = %d", p.NotifyCount)
	}
}

func TestDispatchEscalation_ResolverErrorIsRetriedNotDropped(t *testing.T) {
	t.Parallel()
	e := newEscEnv(t)
	e.roster.err = errors.New("oncall down")
	e.seedEsc("ob", 1, 1)
	if n := e.run(t); n != 0 {
		t.Fatalf("processed = %d", n)
	}
	if row := e.store.OutboxRow("ob"); row.Status != mc.OutboxPending || row.Attempts != 1 {
		t.Fatalf("outbox = %+v, want pending with one failure", row)
	}
	e.roster.err = nil
	if n := e.run(t); n != 1 || len(e.notifyEvents(t)) != 1 {
		t.Fatal("the row must be dispatched once the dependency recovers")
	}
}

// 没有注入第二期协作者的派发用例（第一期的装配）遇到升级通知：不丢弃、不误发，按失败留待重试。
func TestDispatchEscalation_UnwiredIsNotMisdispatched(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	d.store.SetRule(d.rule.ID, func(r *biz.Rule) { r.Spec.EscalationID = escID })
	d.store.SeedOutboxLevel("ob", d.alert, mc.NotifyKindEscalation, mc.EscalationOutboxSeq(1, 1), 1)
	if n := d.run(t); n != 0 {
		t.Fatalf("processed = %d", n)
	}
	if row := d.store.OutboxRow("ob"); row.Status != mc.OutboxPending {
		t.Fatalf("outbox = %+v", row)
	}
	if a := d.store.Alert(d.alert); a.NotifyCount != 0 || len(d.notifyEvents(t)) != 0 {
		t.Fatal("an escalation row must never be dispatched as a firing notification")
	}
}

func TestDispatch_InhibitedAlertIsDroppedAndCanBeRearmed(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	d.store.SetAlert(d.alert, func(a *biz.Alert) { a.Inhibited, a.InhibitKind = true, mc.InhibitTopology })
	d.store.SeedOutbox("ob", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	if row := d.store.OutboxRow("ob"); row.Status != mc.OutboxDropped || len(d.notifyEvents(t)) != 0 {
		t.Fatalf("outbox = %+v", row)
	}
	// 抑制解除后引擎把行重新置为待派发。
	d.store.SetAlert(d.alert, func(a *biz.Alert) { a.Inhibited, a.InhibitKind = false, mc.InhibitNone })
	d.store.RearmOutbox("ob")
	d.run(t)
	if row := d.store.OutboxRow("ob"); row.Status != mc.OutboxDone || len(d.notifyEvents(t)) != 1 {
		t.Fatalf("outbox after rearm = %+v", row)
	}
}

// 事件关联了资源：通知内容体带 ci_id / ci_name，规则勾选「资源负责人」时负责人（可用的系统用户）收到通知。
func TestDispatch_CIOwnerAndPayload(t *testing.T) {
	t.Parallel()
	e := newEscEnv(t)
	e.org.Users[uOwnerUUID] = "Owner"
	e.cis.entries[ciWeb] = &biz.CIEntry{ID: ciWeb, Name: "web-01", Owner: strings.ToUpper(uOwnerUUID), Valid: true}
	e.store.SetRule(e.rule.ID, func(r *biz.Rule) { r.Spec.Notify.CIOwner = true })
	e.store.SetAlert(e.alert, func(a *biz.Alert) { a.CIID = ciWeb })
	e.store.SeedOutbox("ob-fire", e.alert, mc.NotifyKindFiring, 1)
	e.seedEsc("ob-esc", 1, 1)
	if n := e.run(t); n != 2 {
		t.Fatalf("processed = %d", n)
	}
	events := e.notifyEvents(t)
	if !slices.Equal(events[0].Recipients, []string{uAlice, uBob, uCarol, uOwnerUUID}) {
		t.Fatalf("recipients = %v", events[0].Recipients)
	}
	for _, ev := range events {
		if p := payloadOf(t, ev); p.CIID != ciWeb || p.CIName != "web-01" {
			t.Fatalf("%s payload ci = %q / %q", ev.EventType, p.CIID, p.CIName)
		}
	}
	// 升级通知的对象只取策略那一级的：资源负责人不在其中。
	if slices.Contains(events[1].Recipients, uOwnerUUID) {
		t.Fatal("ci owner is a rule target, not an escalation target")
	}
	if e.cis.reads != 2 {
		t.Fatalf("ci index reads = %d, want one per row", e.cis.reads)
	}
}

func TestDispatch_CIOwnerEdgeCases(t *testing.T) {
	t.Parallel()
	for name, entry := range map[string]*biz.CIEntry{
		"index row missing":           nil,
		"owner is free text":          {ID: ciWeb, Name: "web-01", Owner: "运维一组"},
		"owner is not an active user": {ID: ciWeb, Name: "web-01", Owner: uOwnerUUID},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEscEnv(t)
			if entry != nil {
				e.cis.entries[ciWeb] = entry
			}
			e.store.SetRule(e.rule.ID, func(r *biz.Rule) { r.Spec.Notify.CIOwner = true })
			e.store.SetAlert(e.alert, func(a *biz.Alert) { a.CIID = ciWeb })
			e.store.SeedOutbox("ob", e.alert, mc.NotifyKindFiring, 1)
			if n := e.run(t); n != 1 {
				t.Fatalf("processed = %d", n)
			}
			ev := e.notifyEvents(t)[0]
			if !slices.Equal(ev.Recipients, []string{uAlice, uBob, uCarol}) {
				t.Fatalf("recipients = %v", ev.Recipients)
			}
			if p := payloadOf(t, ev); (entry == nil) != (p.CIID == "") {
				t.Fatalf("payload ci = %q", p.CIID)
			}
		})
	}
}

// 没有关联资源的事件不读资源索引；第一期的载荷逐字节不变（不出现 ci_* / escalation_* 键）。
func TestDispatch_NoCIKeepsPhase1Payload(t *testing.T) {
	t.Parallel()
	e := newEscEnv(t)
	e.store.SeedOutbox("ob", e.alert, mc.NotifyKindFiring, 1)
	e.run(t)
	raw := string(e.notifyEvents(t)[0].Payload)
	if e.cis.reads != 0 || strings.Contains(raw, "ci_id") || strings.Contains(raw, "escalation_") || strings.Contains(raw, `"test"`) {
		t.Fatalf("reads = %d payload = %s", e.cis.reads, raw)
	}
}

// testLogger 写进 env 日志缓冲的 logger。
func testLogger(e *env) *slog.Logger {
	return slog.New(slog.NewTextHandler(&syncWriter{buf: e.logs}, nil))
}
