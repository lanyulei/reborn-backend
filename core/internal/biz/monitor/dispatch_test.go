package biz_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// dispatchEnv 在 env 之上预置一条规则与一条活跃告警。规则的通知对象：alice（直接指定）、
// 运维值班（当前值班 bob 与一个已停用的用户）、运维部负责人（carol）。
type dispatchEnv struct {
	*env
	rule  *biz.RuleView
	alert string
}

const alertFP = "0123456789abcdef0123456789abcdef"

func newDispatchEnv(t *testing.T) *dispatchEnv {
	t.Helper()
	e := newEnv(t)
	s := spec(gHost, "cpu-high")
	s.Notify = mc.NotifyTargets{UserIDs: []string{uAlice}, ScheduleIDs: []string{schedOps}, DeptIDs: []string{deptOps}}
	s.Effective.Timezone = "Asia/Shanghai"
	d := &dispatchEnv{env: e, rule: e.mustRule(t, uAlice, s), alert: "018f3e1a-aaaa-7000-8000-00000000a001"}
	d.seedAlert(d.alert, mc.SeverityCritical, map[string]string{"instance": "10.0.0.1", "env": "prod"})
	return d
}

func (d *dispatchEnv) seedAlert(id string, severity int32, labels map[string]string) {
	value := 95.5
	fired := time.Now().Add(-5 * time.Minute)
	d.store.SeedAlert(&biz.Alert{
		ID: id, Fingerprint: alertFP, RuleID: d.rule.ID, RuleName: "cpu-high", GroupID: gHost, DatasourceID: dsVM, DatasourceName: "vm",
		Severity: severity, Status: mc.AlertFiring, Labels: labels, Annotations: map[string]string{"summary": "CPU 95%"},
		Expr: "cpu > 90", Value: &value, FirstFireTime: fired, LastFireTime: time.Now(), GroupKey: "monitor:rule:" + d.rule.ID,
	})
}

func (d *dispatchEnv) run(t *testing.T) int {
	t.Helper()
	n, err := d.dispatch.Run(context.Background())
	if err != nil {
		t.Fatalf("dispatch.Run() error = %v", err)
	}
	return n
}

// events 已落库的通知事件，按写入顺序。
func (d *dispatchEnv) notifyEvents(t *testing.T) []*biznotify.Event {
	t.Helper()
	items, _, err := d.events.ListEvents(context.Background(), biznotify.EventFilter{}, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(items, func(a, b *biznotify.Event) int { return strings.Compare(a.ID, b.ID) })
	return items
}

func (d *dispatchEnv) subscribe(t *testing.T, owner string, mutate func(s *biz.SubscriptionSpec)) *biz.SubscriptionView {
	t.Helper()
	s := biz.SubscriptionSpec{
		Name: "关注主机", GroupIDs: []string{gHost}, Severities: []int32{mc.SeverityCritical, mc.SeverityFatal},
		OnFiring: true, OnResolved: true, Receivers: mc.Receivers{Self: true}, Enabled: true,
	}
	if mutate != nil {
		mutate(&s)
	}
	v, err := d.subs.Create(context.Background(), owner, s)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	return v
}

func payloadOf(t *testing.T, ev *biznotify.Event) mc.NotifyPayload {
	t.Helper()
	var p mc.NotifyPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return p
}

func TestDispatch_RuleRecipients(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	if n := d.run(t); n != 1 {
		t.Fatalf("processed = %d, want 1", n)
	}
	events := d.notifyEvents(t)
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	ev := events[0]
	// 用户 ∪ 值班人 ∪ 部门负责人，去重，停用的值班人被滤掉。
	if !slices.Equal(ev.Recipients, []string{uAlice, uBob, uCarol}) {
		t.Fatalf("recipients = %v", ev.Recipients)
	}
	if ev.EventType != mc.EventAlertFiring || ev.BizID != mc.NotifyBizID || ev.Severity != mc.SeverityCritical ||
		ev.GroupKey != "monitor:rule:"+d.rule.ID || !strings.HasPrefix(ev.Title, "cpu-high [env=prod, instance=10.0.0.1]") {
		t.Fatalf("event = %+v", ev)
	}
	wantKey := "monitor:" + alertFP + ":" + strings.ReplaceAll(d.alert, "-", "") + ":1:1"
	if ev.DedupKey != wantKey || len(ev.DedupKey) > 128 {
		t.Fatalf("dedup key = %q, want %q", ev.DedupKey, wantKey)
	}
	p := payloadOf(t, ev)
	if p.AlertID != d.alert || p.AlertURL != "/monitor/alert/"+d.alert || p.RuleName != "cpu-high" || p.GroupName != "基础设施 / 主机" ||
		p.DatasourceName != "vm" || p.Status != mc.PayloadStatusFiring || p.Value != "95.5" || p.Expr != "cpu > 90" ||
		p.NotifySeq != 1 || p.Labels["instance"] != "10.0.0.1" || p.Annotations["summary"] != "CPU 95%" || p.SubscriptionName != "" {
		t.Fatalf("payload = %+v", p)
	}
	// 时间按规则的时区格式化，时长是预先格式化好的文本。
	if !strings.HasSuffix(p.FirstFireTime, "+08:00") || !strings.Contains(p.Duration, "分钟") || p.RecoveryValue != "" {
		t.Fatalf("formatted fields = %q / %q / %q", p.FirstFireTime, p.Duration, p.RecoveryValue)
	}
	logs := d.store.NotifyLogs()
	if len(logs) != 1 || logs[0].Source != mc.NotifySourceRule || logs[0].Kind != mc.NotifyKindFiring || logs[0].Seq != 1 ||
		!slices.Equal(logs[0].Recipients, ev.Recipients) || !slices.Equal(logs[0].NotifyEventIDs, []string{ev.ID}) || logs[0].Note != "" {
		t.Fatalf("notify log = %+v", logs[0])
	}
	if row := d.store.OutboxRow("ob-1"); row.Status != mc.OutboxDone {
		t.Fatalf("outbox row = %+v", row)
	}
	if a := d.store.Alert(d.alert); a.NotifyCount != 1 || a.LastNotifyTime == nil {
		t.Fatalf("alert notify progress = %d / %v", a.NotifyCount, a.LastNotifyTime)
	}
	// 再跑一轮：没有待派发行，不产生任何新东西。
	if n := d.run(t); n != 0 || len(d.notifyEvents(t)) != 1 {
		t.Fatalf("second run must be a no-op: %d", n)
	}
}

func TestDispatch_NoRecipientsFallsBackToRoute(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	// 值班表空档、部门没有负责人、指定的用户已停用：解析结果为空。
	d.org.OnDutyUsers = map[string][]string{}
	d.org.Leaders = map[string][]string{}
	delete(d.org.Users, uAlice)
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	events := d.notifyEvents(t)
	if len(events) != 1 || len(events[0].Recipients) != 0 {
		t.Fatalf("an event without recipients must still be written: %+v", events)
	}
	logs := d.store.NotifyLogs()
	if len(logs) != 1 || logs[0].Note != "未解析到通知对象" || len(logs[0].Recipients) != 0 || len(logs[0].NotifyEventIDs) != 1 {
		t.Fatalf("notify log = %+v", logs[0])
	}
	if a := d.store.Alert(d.alert); a.NotifyCount != 1 {
		t.Fatalf("notify_count = %d, want 1", a.NotifyCount)
	}
}

func TestDispatch_ChunksRecipients(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	// 45 个值班人：每 20 人一条事件，去重键带分片序号。
	var crowd []string
	for i := range 45 {
		id := fmt.Sprintf("u-crowd-%02d", i)
		d.org.Users[id] = id
		crowd = append(crowd, id)
	}
	d.org.OnDutyUsers[schedOps] = crowd
	d.org.Leaders = map[string][]string{}
	delete(d.org.Users, uAlice)
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	events := d.notifyEvents(t)
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3", len(events))
	}
	seen := map[string]bool{}
	keys := map[string]bool{}
	for _, ev := range events {
		if len(ev.Recipients) > 20 {
			t.Fatalf("chunk too large: %d", len(ev.Recipients))
		}
		for _, r := range ev.Recipients {
			seen[r] = true
		}
		keys[ev.DedupKey] = true
	}
	if len(seen) != 45 || len(keys) != 3 {
		t.Fatalf("recipients covered = %d, distinct dedup keys = %d", len(seen), len(keys))
	}
	if logs := d.store.NotifyLogs(); len(logs) != 1 || len(logs[0].Recipients) != 45 || len(logs[0].NotifyEventIDs) != 3 {
		t.Fatalf("one log row must record all chunks: %+v", logs)
	}
}

func TestDispatch_Subscriptions(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	// 命中：dave 订阅 host（本人）。
	hit := d.subscribe(t, uDave, nil)
	// 命中但接收方全是规则已通知到的人（bob 是值班人）：不再发。
	d.subscribe(t, uBob, func(s *biz.SubscriptionSpec) { s.Name = "bob 自己" })
	// 命中：订阅上级分组（含下级），接收方为本人（已被规则通知）+ dave（已被上面那条订阅通知）+ 群：
	// 只剩群。
	mixed := d.subscribe(t, uAlice, func(s *biz.SubscriptionSpec) {
		s.Name, s.GroupIDs = "infra 全部", []string{gInfra}
		s.Receivers = mc.Receivers{Self: true, UserIDs: []string{uDave}, Groups: []string{"$group:ops"}}
	})
	// 不命中的各种原因。
	d.subscribe(t, uDave, func(s *biz.SubscriptionSpec) { s.Name, s.Enabled = "停用", false })
	d.subscribe(t, uDave, func(s *biz.SubscriptionSpec) { s.Name, s.Severities = "级别不符", []int32{mc.SeverityInfo} })
	d.subscribe(t, uDave, func(s *biz.SubscriptionSpec) { s.Name, s.OnFiring = "只订恢复", false })
	d.subscribe(t, uDave, func(s *biz.SubscriptionSpec) { s.Name, s.DatasourceIDs = "数据源不符", []string{dsOff} })
	d.subscribe(t, uDave, func(s *biz.SubscriptionSpec) {
		s.Name, s.Matchers = "标签不符", []mc.Matcher{{Label: "env", Op: mc.OpEqual, Value: "test"}}
	})
	d.subscribe(t, uCarol, func(s *biz.SubscriptionSpec) { s.Name, s.GroupIDs = "别的分组", []string{gApp} })
	// 标签命中（正则 + in）；接收方是群，需要对分组可管理，所以由 alice 建。
	labelHit := d.subscribe(t, uAlice, func(s *biz.SubscriptionSpec) {
		s.Name = "标签命中"
		s.Receivers = mc.Receivers{Groups: []string{"$group:db"}}
		s.Matchers = []mc.Matcher{{Label: "instance", Op: mc.OpRegex, Value: `10\..*`}, {Label: "env", Op: mc.OpIn, Values: []string{"prod", "pre"}}}
	})
	// 所有者后来失去范围：carol 的订阅建在 app 上，再直接改库把分组换成 host（她对 host 没有范围）。
	lost := d.subscribe(t, uCarol, func(s *biz.SubscriptionSpec) { s.Name, s.GroupIDs = "失去范围", []string{gApp} })
	moved := lost.Spec
	moved.GroupIDs = []string{gHost}
	if err := d.store.Subscriptions().Update(context.Background(), &biz.Subscription{ID: lost.ID, Spec: moved}, 1, uAdmin); err != nil {
		t.Fatal(err)
	}

	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	byType := map[string][]*biznotify.Event{}
	for _, ev := range d.notifyEvents(t) {
		byType[ev.EventType] = append(byType[ev.EventType], ev)
	}
	if len(byType[mc.EventAlertFiring]) != 1 || len(byType[mc.EventSubscriptionFiring]) != 3 {
		t.Fatalf("events: rule=%d subscription=%d, want 1 / 3", len(byType[mc.EventAlertFiring]), len(byType[mc.EventSubscriptionFiring]))
	}
	got := map[string][]string{}
	for _, l := range d.store.NotifyLogs() {
		if l.Source == mc.NotifySourceSubscription {
			got[l.SubscriptionID] = l.Recipients
		}
	}
	if len(got) != 3 || !slices.Equal(got[hit.ID], []string{uDave}) || !slices.Equal(got[mixed.ID], []string{"$group:ops"}) ||
		!slices.Equal(got[labelHit.ID], []string{"$group:db"}) {
		t.Fatalf("subscription logs = %v", got)
	}
	if _, bad := got[lost.ID]; bad {
		t.Fatalf("owner without read scope on the alert's group must be skipped")
	}
	for _, ev := range byType[mc.EventSubscriptionFiring] {
		p := payloadOf(t, ev)
		if p.SubscriptionName == "" || !strings.Contains(ev.DedupKey, ":s:") || len(ev.DedupKey) > 128 {
			t.Fatalf("subscription event = %q key=%q", p.SubscriptionName, ev.DedupKey)
		}
	}

	// 所有者失去范围后订阅自动失效：把 dave 的角色拿掉。
	d.org.UserRoles[uDave] = nil
	d.store.SeedOutbox("ob-2", d.alert, mc.NotifyKindResolved, 2)
	d.store.SetAlert(d.alert, func(a *biz.Alert) {
		now := time.Now()
		recovered := 41.7
		a.Status, a.ResolveTime, a.RecoveryValue = mc.AlertResolved, &now, &recovered
	})
	d.run(t)
	var resolved []string
	for _, l := range d.store.NotifyLogs() {
		if l.Kind == mc.NotifyKindResolved && l.Source == mc.NotifySourceSubscription {
			resolved = append(resolved, l.SubscriptionID)
		}
	}
	// 只剩 alice 名下的两条；dave 的订阅不再命中，alice 那条里的 dave 仍是合法的指定接收人
	// （这回没有别的订阅先通知到他，所以由这条发给他）。
	slices.Sort(resolved)
	want := []string{mixed.ID, labelHit.ID}
	slices.Sort(want)
	if !slices.Equal(resolved, want) {
		t.Fatalf("resolved subscription logs = %v, want %v (dave's own subscription must stop firing)", resolved, want)
	}
	for _, l := range d.store.NotifyLogs() {
		if l.Kind == mc.NotifyKindResolved && l.SubscriptionID == mixed.ID && !slices.Equal(l.Recipients, []string{uDave, "$group:ops"}) {
			t.Fatalf("resolved recipients of the mixed subscription = %v", l.Recipients)
		}
	}
	for _, ev := range d.notifyEvents(t) {
		if ev.EventType == mc.EventAlertResolved {
			if p := payloadOf(t, ev); p.Status != mc.PayloadStatusResolved || p.RecoveryValue != "41.7" || p.NotifySeq != 2 {
				t.Fatalf("resolved payload = %+v", p)
			}
		}
	}
}

func TestDispatch_RepeatOnlyFollowers(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	d.subscribe(t, uDave, func(s *biz.SubscriptionSpec) { s.Name = "不跟随" })
	follow := d.subscribe(t, uDave, func(s *biz.SubscriptionSpec) { s.Name, s.FollowRepeat = "跟随", true })
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.store.SeedOutbox("ob-2", d.alert, mc.NotifyKindRepeat, 2)
	if n := d.run(t); n != 2 {
		t.Fatalf("processed = %d, want 2", n)
	}
	var firing, repeat []string
	for _, l := range d.store.NotifyLogs() {
		if l.Source != mc.NotifySourceSubscription {
			continue
		}
		if l.Kind == mc.NotifyKindFiring {
			firing = append(firing, l.SubscriptionID)
		} else {
			repeat = append(repeat, l.SubscriptionID)
		}
	}
	// 首次触发：两条订阅的接收方都是 dave，只由排在前面的那条发一份；重复通知只有跟随的那条。
	if len(firing) != 1 || !slices.Equal(repeat, []string{follow.ID}) {
		t.Fatalf("firing = %v, repeat = %v", firing, repeat)
	}
	// 重复通知仍是 monitor.alert.firing，通知序号不同、去重键不同。
	var keys []string
	for _, ev := range d.notifyEvents(t) {
		if ev.EventType == mc.EventAlertFiring {
			keys = append(keys, ev.DedupKey)
		}
	}
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("rule events for firing + repeat must have distinct dedup keys: %v", keys)
	}
	if a := d.store.Alert(d.alert); a.NotifyCount != 2 {
		t.Fatalf("notify_count = %d, want 2", a.NotifyCount)
	}
}

// 引擎从不写 notify_count / last_notify_time：只有触发 / 重复通知确实派发时才回写；
// 恢复通知与被丢弃的行不动这两列。
func TestDispatch_NotifyCountOnlyOnFiringOrRepeat(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	first := d.store.Alert(d.alert)
	if first.NotifyCount != 1 || first.LastNotifyTime == nil {
		t.Fatalf("after firing: %d / %v", first.NotifyCount, first.LastNotifyTime)
	}
	time.Sleep(2 * time.Millisecond)
	d.store.SeedOutbox("ob-2", d.alert, mc.NotifyKindRepeat, 2)
	d.run(t)
	second := d.store.Alert(d.alert)
	if second.NotifyCount != 2 || !second.LastNotifyTime.After(*first.LastNotifyTime) {
		t.Fatalf("after repeat: %d / %v", second.NotifyCount, second.LastNotifyTime)
	}
	// 恢复通知：派发了，但不计数、不改最近通知时间。
	time.Sleep(2 * time.Millisecond)
	d.store.SetAlert(d.alert, func(a *biz.Alert) {
		now := time.Now()
		a.Status, a.ResolveTime = mc.AlertResolved, &now
	})
	d.store.SeedOutbox("ob-3", d.alert, mc.NotifyKindResolved, 3)
	d.run(t)
	third := d.store.Alert(d.alert)
	if d.store.OutboxRow("ob-3").Status != mc.OutboxDone || third.NotifyCount != 2 || !third.LastNotifyTime.Equal(*second.LastNotifyTime) {
		t.Fatalf("after resolved: status=%d count=%d last=%v (was %v)", d.store.OutboxRow("ob-3").Status, third.NotifyCount, third.LastNotifyTime, second.LastNotifyTime)
	}
	// 被丢弃的行：不计数。
	other := "018f3e1a-aaaa-7000-8000-00000000a002"
	d.seedAlert(other, mc.SeverityCritical, map[string]string{"instance": "10.0.0.2"})
	d.store.SetAlert(other, func(a *biz.Alert) { a.Muted = true })
	d.store.SeedOutbox("ob-4", other, mc.NotifyKindFiring, 1)
	d.run(t)
	if a := d.store.Alert(other); d.store.OutboxRow("ob-4").Status != mc.OutboxDropped || a.NotifyCount != 0 || a.LastNotifyTime != nil {
		t.Fatalf("dropped row must not touch the alert: %d / %v", a.NotifyCount, a.LastNotifyTime)
	}
}

func TestDispatch_DropPaths(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	ctx := context.Background()
	mk := func(n int, mutate func(a *biz.Alert)) string {
		id := fmt.Sprintf("018f3e1a-aaaa-7000-8000-00000000b%03d", n)
		d.seedAlert(id, mc.SeverityCritical, map[string]string{"n": fmt.Sprint(n)})
		if mutate != nil {
			d.store.SetAlert(id, mutate)
		}
		return id
	}
	muted := mk(1, func(a *biz.Alert) { a.Muted = true })
	closed := mk(2, func(a *biz.Alert) {
		now := time.Now()
		a.Status, a.ResolveTime = mc.AlertClosed, &now
	})
	// 另一条规则，随后删除。
	gone := d.mustRule(t, uAlice, spec(gHost, "to-delete"))
	orphan := mk(3, func(a *biz.Alert) { a.RuleID = gone.ID })
	if err := d.rules.Delete(ctx, uAlice, gone.ID); err != nil {
		t.Fatal(err)
	}
	// 从未发出过触发通知的事件收到恢复通知行。
	neverFired := mk(4, func(a *biz.Alert) {
		now := time.Now()
		a.Status, a.ResolveTime = mc.AlertResolved, &now
	})
	d.store.SeedOutbox("ob-muted", muted, mc.NotifyKindFiring, 1)
	d.store.SeedOutbox("ob-closed", closed, mc.NotifyKindRepeat, 2)
	d.store.SeedOutbox("ob-orphan", orphan, mc.NotifyKindFiring, 1)
	d.store.SeedOutbox("ob-missing", "018f3e1a-aaaa-7000-8000-0000000000ff", mc.NotifyKindFiring, 1)
	d.store.SeedOutbox("ob-never", neverFired, mc.NotifyKindResolved, 1)
	d.store.SeedOutbox("ob-ok", d.alert, mc.NotifyKindFiring, 1)
	if n := d.run(t); n != 6 {
		t.Fatalf("processed = %d, want 6", n)
	}
	for id, reason := range map[string]string{
		"ob-muted": "屏蔽", "ob-closed": "关闭", "ob-orphan": "规则已删除", "ob-missing": "不存在", "ob-never": "恢复通知",
	} {
		row := d.store.OutboxRow(id)
		if row.Status != mc.OutboxDropped || !strings.Contains(row.LastError, reason) {
			t.Errorf("%s = status %d reason %q, want dropped with %q", id, row.Status, row.LastError, reason)
		}
	}
	// 丢弃的行不写事件、不写通知记录；同批里正常的那行照常派发。
	if events := d.notifyEvents(t); len(events) != 1 || payloadOf(t, events[0]).AlertID != d.alert {
		t.Fatalf("events = %d, want only the healthy row's", len(events))
	}
	if logs := d.store.NotifyLogs(); len(logs) != 1 {
		t.Fatalf("notify logs = %d, want 1", len(logs))
	}
	if d.store.OutboxRow("ob-ok").Status != mc.OutboxDone {
		t.Fatalf("healthy row must be dispatched")
	}
	// 规则停用不是丢弃的理由：停用后引擎会关闭事件；在那之前到达的行照常派发。
	disabledAlert := mk(5, nil)
	if _, err := d.rules.Toggle(ctx, uAlice, d.rule.ID, false); err != nil {
		t.Fatal(err)
	}
	d.store.SeedOutbox("ob-disabled", disabledAlert, mc.NotifyKindFiring, 1)
	d.run(t)
	if d.store.OutboxRow("ob-disabled").Status != mc.OutboxDone {
		t.Fatalf("a disabled rule must not cause a permanent drop")
	}
}

// 瞬时失败：整行回滚、记一次失败、保持待派发，不影响同批其它行；下一轮重试成功，且不重复发通知。
func TestDispatch_TransientFailureRetries(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	other := "018f3e1a-aaaa-7000-8000-00000000a002"
	d.seedAlert(other, mc.SeverityCritical, map[string]string{"instance": "10.0.0.2"})
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.store.SeedOutbox("ob-2", other, mc.NotifyKindFiring, 1)
	d.store.FailNextLogInsert(errBoom)
	// 单独一行失败只记日志，不作为 error 返回（否则后台循环会退避，拖慢其他告警的通知）。
	n, err := d.dispatch.Run(context.Background())
	if n != 1 || err != nil {
		t.Fatalf("Run() = %d, %v; want 1 processed and no error for a single failed row", n, err)
	}
	if logged := d.logs.String(); !strings.Contains(logged, "ob-1") || !strings.Contains(logged, "boom") || !strings.Contains(logged, "level=WARN") {
		t.Fatalf("the failed row must be logged with its cause: %q", logged)
	}
	row := d.store.OutboxRow("ob-1")
	if row.Status != mc.OutboxPending || row.Attempts != 1 || !strings.Contains(row.LastError, "boom") {
		t.Fatalf("failed row = %+v, want pending with attempts=1", row)
	}
	if d.store.OutboxRow("ob-2").Status != mc.OutboxDone {
		t.Fatalf("a poisoned row must not block the others")
	}
	if a := d.store.Alert(d.alert); a.NotifyCount != 0 {
		t.Fatalf("failed dispatch must not count as notified: %d", a.NotifyCount)
	}
	// 重试：成功。内存桩没有回滚，第一次已写出的事件靠去重键不再重复。
	if n := d.run(t); n != 1 {
		t.Fatalf("retry processed = %d, want 1", n)
	}
	if d.store.OutboxRow("ob-1").Status != mc.OutboxDone || d.store.Alert(d.alert).NotifyCount != 1 {
		t.Fatalf("retry did not complete the row")
	}
	perAlert := map[string]int{}
	for _, ev := range d.notifyEvents(t) {
		perAlert[payloadOf(t, ev).AlertID]++
	}
	if perAlert[d.alert] != 1 || perAlert[other] != 1 {
		t.Fatalf("each alert must be notified exactly once: %v", perAlert)
	}
}

// 持续失败：失败次数达到上限后才丢弃；在那之前一直是待派发。
func TestDispatch_DropsAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	for i := 1; i <= mc.OutboxMaxAttempts; i++ {
		d.store.FailNextLogInsert(errBoom)
		if n, err := d.dispatch.Run(context.Background()); n != 0 || err != nil {
			t.Fatalf("attempt %d: Run() = %d, %v; a failed row is not a loop error", i, n, err)
		}
		row := d.store.OutboxRow("ob-1")
		wantStatus := mc.OutboxPending
		if i == mc.OutboxMaxAttempts {
			wantStatus = mc.OutboxDropped
		}
		if row.Attempts != int32(i) || row.Status != wantStatus {
			t.Fatalf("after attempt %d: %+v", i, row)
		}
	}
	if n := d.run(t); n != 0 {
		t.Fatalf("a dropped row must not be claimed again: %d", n)
	}
	if d.store.Alert(d.alert).NotifyCount != 0 {
		t.Fatalf("a row that never dispatched must not count")
	}
	if logged := d.logs.String(); !strings.Contains(logged, "level=ERROR") || !strings.Contains(logged, "dropped after too many failures") {
		t.Fatalf("dropping a row must be logged at ERROR: %q", logged)
	}
}

// 屏蔽解除后引擎把已丢弃的行重新置为待派发：按普通待派发行处理——能派发，之前那一世的失败次数不会
// 让它被立刻丢弃，去重键也不会与先前撞车（丢弃的行从未写出过事件）。
func TestDispatch_RearmedRow(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	d.store.SetAlert(d.alert, func(a *biz.Alert) { a.Muted = true })
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	// 第一世：先失败几次（依赖方故障），再因屏蔽被丢弃。
	for range 3 {
		d.store.SetAlert(d.alert, func(a *biz.Alert) { a.Muted = false })
		d.store.FailNextLogInsert(errBoom)
		before := d.store.OutboxRow("ob-1").Attempts
		if _, err := d.dispatch.Run(context.Background()); err != nil || d.store.OutboxRow("ob-1").Attempts != before+1 {
			t.Fatalf("expected the row to fail once: err=%v row=%+v", err, d.store.OutboxRow("ob-1"))
		}
	}
	d.store.SetAlert(d.alert, func(a *biz.Alert) { a.Muted = true })
	d.run(t)
	if row := d.store.OutboxRow("ob-1"); row.Status != mc.OutboxDropped || row.Attempts != 3 {
		t.Fatalf("first life = %+v", row)
	}
	before := len(d.notifyEvents(t))

	// 屏蔽解除：引擎重新置为待派发（清零失败次数）。
	d.store.SetAlert(d.alert, func(a *biz.Alert) { a.Muted = false })
	d.store.RearmOutbox("ob-1")
	if row := d.store.OutboxRow("ob-1"); row.Status != mc.OutboxPending || row.Attempts != 0 || row.LastError != "" {
		t.Fatalf("re-armed row = %+v", row)
	}
	if n := d.run(t); n != 1 {
		t.Fatalf("re-armed row must dispatch: processed = %d", n)
	}
	if row := d.store.OutboxRow("ob-1"); row.Status != mc.OutboxDone {
		t.Fatalf("re-armed row = %+v, want done", row)
	}
	if a := d.store.Alert(d.alert); a.NotifyCount != 1 || a.LastNotifyTime == nil {
		t.Fatalf("re-armed dispatch must count: %d", a.NotifyCount)
	}
	logs := d.store.NotifyLogs()
	if len(logs) != 1 || len(logs[0].NotifyEventIDs) != 1 || len(logs[0].Recipients) != 3 {
		t.Fatalf("notify log after re-arm = %+v", logs)
	}
	// 内存桩不回滚：第一世失败的那几次已各写过事件（真实库里它们随事务回滚，不存在）。这里要确认的是
	// 重新派发得到了一个确实存在的事件，而不是新造一堆。
	after := d.notifyEvents(t)
	if len(after) != before || !slices.ContainsFunc(after, func(ev *biznotify.Event) bool { return ev.ID == logs[0].NotifyEventIDs[0] }) {
		t.Fatalf("events before/after re-arm = %d/%d; log points at %v", before, len(after), logs[0].NotifyEventIDs)
	}

	// 重新置为待派发的重复通知行同理；重新置位后再失败，次数从头计。
	d.store.SetAlert(d.alert, func(a *biz.Alert) { a.Muted = true })
	d.store.SeedOutbox("ob-2", d.alert, mc.NotifyKindRepeat, 2)
	d.run(t)
	d.store.SetAlert(d.alert, func(a *biz.Alert) { a.Muted = false })
	d.store.RearmOutbox("ob-2")
	d.store.FailNextLogInsert(errBoom)
	if n, err := d.dispatch.Run(context.Background()); n != 0 || err != nil {
		t.Fatalf("Run() = %d, %v; want the row to fail without a loop error", n, err)
	}
	if row := d.store.OutboxRow("ob-2"); row.Status != mc.OutboxPending || row.Attempts != 1 {
		t.Fatalf("failure after re-arm = %+v, want pending with attempts=1", row)
	}
	d.run(t)
	if d.store.OutboxRow("ob-2").Status != mc.OutboxDone || d.store.Alert(d.alert).NotifyCount != 2 {
		t.Fatalf("re-armed repeat row did not complete")
	}
}

// 同一事件的行按入箱顺序处理：前一条未终结时后一条不会被领取（恢复通知不会抢在触发通知之前）。
func TestDispatch_OrderWithinAlert(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.store.SeedOutbox("ob-2", d.alert, mc.NotifyKindResolved, 2)
	d.store.FailNextLogInsert(errBoom)
	n, err := d.dispatch.Run(context.Background())
	if n != 0 || err != nil || d.store.OutboxRow("ob-1").Attempts != 1 {
		t.Fatalf("Run() = %d, %v", n, err)
	}
	if d.store.OutboxRow("ob-2").Status != mc.OutboxPending || len(d.store.NotifyLogs()) != 0 {
		t.Fatalf("the later row must wait for the earlier one")
	}
	if n := d.run(t); n != 2 {
		t.Fatalf("processed = %d, want 2", n)
	}
	logs := d.store.NotifyLogs()
	if len(logs) != 2 || logs[0].Kind != mc.NotifyKindFiring || logs[1].Kind != mc.NotifyKindResolved {
		t.Fatalf("order = %+v", logs)
	}
}

// 附加信息很大时内容体被截短到 notify 的载荷上限以内，而不是让这一行永远写不进去。
func TestDispatch_OversizedPayloadIsTrimmed(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	big := map[string]string{}
	for i := range 10 {
		big[fmt.Sprintf("note%d", i)] = strings.Repeat("长", 4000)
	}
	d.store.SetAlert(d.alert, func(a *biz.Alert) { a.Annotations = big })
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	events := d.notifyEvents(t)
	if len(events) != 1 || len(events[0].Payload) > 32*1024 {
		t.Fatalf("payload = %d bytes", len(events[0].Payload))
	}
	p := payloadOf(t, events[0])
	if len(p.Annotations) != 10 || p.Labels["instance"] != "10.0.0.1" || p.RuleName != "cpu-high" {
		t.Fatalf("trimmed payload lost structure: %d annotations", len(p.Annotations))
	}
}

func TestDispatch_BatchBound(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	for i := range 60 {
		id := fmt.Sprintf("018f3e1a-aaaa-7000-8000-00000000c%03d", i)
		d.seedAlert(id, mc.SeverityCritical, map[string]string{"i": fmt.Sprint(i)})
		d.store.SeedOutbox(fmt.Sprintf("ob-%02d", i), id, mc.NotifyKindFiring, 1)
	}
	if n := d.run(t); n != 50 {
		t.Fatalf("one round processes at most 50 rows: %d", n)
	}
	if n := d.run(t); n != 10 {
		t.Fatalf("the rest in the next round: %d", n)
	}
}

// 迟到的重复通知：事件已恢复就不再发「仍在告警」；紧随其后的恢复通知照常发。触发通知不受此限。
func TestDispatch_StaleRepeatIsDropped(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	resolve := func(id string) {
		d.store.SetAlert(id, func(a *biz.Alert) {
			now := time.Now()
			a.Status, a.ResolveTime = mc.AlertResolved, &now
		})
	}
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	resolve(d.alert)
	d.store.SeedOutbox("ob-2", d.alert, mc.NotifyKindRepeat, 2)
	d.store.SeedOutbox("ob-3", d.alert, mc.NotifyKindResolved, 2)
	if n := d.run(t); n != 2 {
		t.Fatalf("processed = %d, want 2", n)
	}
	if row := d.store.OutboxRow("ob-2"); row.Status != mc.OutboxDropped || !strings.Contains(row.LastError, "不再重复通知") {
		t.Fatalf("stale repeat = %+v", row)
	}
	if d.store.OutboxRow("ob-3").Status != mc.OutboxDone || d.store.Alert(d.alert).NotifyCount != 1 {
		t.Fatalf("the recovery notification must still go out and the dropped repeat must not count")
	}
	// 触发通知迟到（事件已恢复）仍然要发：否则恢复通知也会被一并压掉。
	late := "018f3e1a-aaaa-7000-8000-00000000a003"
	d.seedAlert(late, mc.SeverityCritical, map[string]string{"instance": "late"})
	resolve(late)
	d.store.SeedOutbox("ob-4", late, mc.NotifyKindFiring, 1)
	d.store.SeedOutbox("ob-5", late, mc.NotifyKindResolved, 2)
	if n := d.run(t); n != 2 || d.store.OutboxRow("ob-4").Status != mc.OutboxDone || d.store.OutboxRow("ob-5").Status != mc.OutboxDone {
		t.Fatalf("late firing + recovery: processed %d, rows %d / %d", n, d.store.OutboxRow("ob-4").Status, d.store.OutboxRow("ob-5").Status)
	}
}

// 依赖整体不可用时一轮里连续失败几行就收手：积压里其余的行不被白白记一次失败。
func TestDispatch_StopsAfterConsecutiveFailures(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	const rows = 20
	for i := range rows {
		id := fmt.Sprintf("018f3e1a-aaaa-7000-8000-00000000d%03d", i)
		d.seedAlert(id, mc.SeverityCritical, map[string]string{"i": fmt.Sprint(i)})
		d.store.SeedOutbox(fmt.Sprintf("ob-%02d", i), id, mc.NotifyKindFiring, 1)
	}
	d.store.FailLogInserts(1000, errBoom)
	// 连续多行失败才是系统性失败：这时返回 error，让后台循环退避。
	n, err := d.dispatch.Run(context.Background())
	if n != 0 || err == nil || !strings.Contains(err.Error(), "consecutive") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Run() = %d, %v; want a systemic failure", n, err)
	}
	touched := 0
	for i := range rows {
		row := d.store.OutboxRow(fmt.Sprintf("ob-%02d", i))
		if row.Status != mc.OutboxPending {
			t.Fatalf("row %d = %+v, want still pending", i, row)
		}
		if row.Attempts > 0 {
			touched++
		}
	}
	if touched != 5 {
		t.Fatalf("rows charged with a failure = %d, want 5 (the rest must be left alone)", touched)
	}
	// 依赖恢复后照常处理；先前失败过的行也在其中（内存桩不模拟退避）。
	d.store.FailLogInserts(0, nil)
	if n := d.run(t); n != rows {
		t.Fatalf("after recovery processed = %d, want %d", n, rows)
	}
}
