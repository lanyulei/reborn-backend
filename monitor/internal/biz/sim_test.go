package biz

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/kit/promclient"
)

// 测试用固定值。
const (
	testRuleID = "018f3e1a-0000-7000-8000-00000000000a"
	testDSID   = "018f3e1a-0000-7000-8000-00000000000b"
	testGroup  = "018f3e1a-0000-7000-8000-00000000000c"
)

var t0 = time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)

// at 返回 t0 之后 sec 秒的时刻。
func at(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

// baseRule 一条查询（警告）、持续 60s、开启级别抑制、恢复通知开启的规则。
func baseRule(mod ...func(*Rule)) *Rule {
	r := &Rule{
		ID: testRuleID, Name: "cpu high", GroupID: testGroup, GroupPath: "/" + testGroup + "/", Active: true,
		Queries:      []monitorcontract.Query{{Expr: "cpu > 85", Severity: monitorcontract.SeverityWarning}},
		LevelInhibit: true, Interval: 60 * time.Second, For: 60 * time.Second, NotifyRecovery: true,
	}
	for _, m := range mod {
		m(r)
	}
	return r
}

// ser 构造一条 instant 查询返回的序列：kv 为标签的键值对。
func ser(value float64, kv ...string) promclient.Series {
	labels := map[string]string{"__name__": "cpu"}
	for i := 0; i+1 < len(kv); i += 2 {
		labels[kv[i]] = kv[i+1]
	}
	return promclient.Series{Labels: labels, Points: []promclient.Point{{Time: t0, Value: value}}}
}

// q 一条查询的结果。
func q(series ...promclient.Series) []promclient.Series { return series }

type simAlert struct {
	AlertInsert
	Status         int32
	FirstFire      time.Time
	LastFire       time.Time
	ResolveTime    time.Time
	HasRecovery    bool
	RecoveryValue  float64
	NotifyCount    int32
	LastNotifyTime time.Time
	// 以下各列：EscalationLevel 由引擎写；AckTime / TicketID 由 core 写（测试直接改）。
	EscalationLevel int32
	AckTime         time.Time
	TicketID        string
	// Levels 各级升级通知的进度（monitor_alert_escalation，core 派发时回写）。
	Levels []monitorpipeline.EscalationLevelState
}

// merge 没有流水线配置、没有资源索引时的 mergeSamples（第一期的调用形态）。
func merge(rule *Rule, datasourceID string, results [][]promclient.Series) *SampleSet {
	return mergeSamples(rule, datasourceID, results, nil, nil)
}

type simOutbox struct {
	OutboxInsert
	Status int32
	// Done 行被丢弃的时刻（core 写入的 done_time）。
	Done time.Time
}

// sim 是数据层 + core 通知派发的内存等价物：把 Advance 的输出落到内存表里，再据此拼出下一轮的输入。
// 状态机的场景测试经它串起多轮，不需要数据库。
type sim struct {
	t      *testing.T
	rule   *Rule
	mutes  []*Mute
	states map[string]SeriesState
	alerts map[string]*simAlert
	outbox []*simOutbox
	nextID int
	// recovery 下一轮传给 Advance 的恢复取值。
	recovery map[string]float64
	// incomplete 下一轮的结果是否不完整（截断 / 部分结果）。
	incomplete bool
	// lastStatus / lastEval 任务行记录的上一轮结果与时刻。
	lastStatus int32
	lastEval   time.Time
	// renders 附加信息渲染的批次数（渲染在 Advance 之外）。
	renders int
	// rearmed 已丢弃的发件箱行被重新置为待派发的次数。
	rearmed int
	// postponed 上一轮完成的评估之后任务被顺延过（任务行上的标记）；完成一轮评估即清除。
	postponed bool
	// 第二期的输入：资源索引、抑制结论、升级策略（nil = 没有）。
	cis        *monitorpipeline.CIIndex
	inhibits   *InhibitView
	escalation *monitorpipeline.EscalationPolicy
	// events / actions 累计写入的时间线行与事件动作（动作以 (alert, kind) 去重）。
	events  []AlertEventInsert
	actions []ActionInsert
}

func newSim(t *testing.T, rule *Rule) *sim {
	t.Helper()
	return &sim{t: t, rule: rule, states: map[string]SeriesState{}, alerts: map[string]*simAlert{}}
}

func (s *sim) newID() (string, error) {
	s.nextID++
	return fmt.Sprintf("id-%04d", s.nextID), nil
}

func (s *sim) prev() []SeriesState {
	out := make([]SeriesState, 0, len(s.states))
	for _, st := range s.states {
		if a := s.alerts[st.AlertID]; a != nil && a.Status == monitorcontract.AlertFiring {
			st.Muted, st.MuteID, st.NotifyCount, st.LastNotifyTime = a.Muted, a.MuteID, a.NotifyCount, a.LastNotifyTime
			st.AlertSeverity, st.AlertValue, st.LastFireTime = a.Severity, a.Value, a.LastFire
			st.FirstFire, st.AckTime, st.TicketID, st.EscalationLevel = a.FirstFire, a.AckTime, a.TicketID, a.EscalationLevel
			st.Inhibited, st.Inhibit, st.CIID = a.Inhibited, a.Inhibit, a.CIID
			if s.escalation != nil {
				st.Escalation = make([]monitorpipeline.EscalationLevelState, monitorcontract.MaxEscalationLevels)
				copy(st.Escalation, a.Levels)
			}
			for _, o := range s.outbox {
				if o.AlertID != a.ID {
					continue
				}
				rule := o.Kind == monitorcontract.NotifyKindFiring || o.Kind == monitorcontract.NotifyKindRepeat
				if rule && o.Status == monitorcontract.OutboxPending {
					st.PendingNotify = true
				}
				if a.NotifyCount == 0 && o.Kind == monitorcontract.NotifyKindFiring && o.Seq == 1 && o.Status == monitorcontract.OutboxDropped {
					st.FirstNotifyDropped = o.Done
				}
				if o.Kind != monitorcontract.NotifyKindEscalation || st.Escalation == nil {
					continue
				}
				level, n, _ := monitorcontract.SplitEscalationSeq(o.Seq)
				ls := &st.Escalation[level-1]
				switch {
				case o.Status == monitorcontract.OutboxPending:
					ls.Pending = true
				case o.Status == monitorcontract.OutboxDropped && n == ls.NotifyCount+1:
					ls.DroppedAt = o.Done
				}
			}
		} else {
			st.AlertID = ""
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Fingerprint < out[j].Fingerprint })
	return out
}

// input 拼出在 now 评估一轮的状态机输入。
func (s *sim) input(now time.Time, results ...[]promclient.Series) *RoundInput {
	pipe, err := monitorpipeline.Compile(s.rule.Pipeline)
	if err != nil {
		s.t.Fatalf("compile rule pipeline: %v", err)
	}
	return &RoundInput{
		Rule: s.rule, DatasourceID: testDSID, Prev: s.prev(), Samples: mergeSamples(s.rule, testDSID, results, pipe, s.cis),
		Incomplete: s.incomplete, LastStatus: s.lastStatus, LastEval: s.lastEval, Postponed: s.postponed,
		Mutes: newMuteSet(compileMutes(s.mutes), s.rule, testDSID, now), Now: now, Recovery: s.recovery, NewID: s.newID,
		Inhibits: s.inhibits, CIs: s.cis, Escalation: s.escalation,
	}
}

// advance 与评估器同样的调用方式：Advance 报告缺附加信息时在外面渲染后重试。
func (s *sim) advance(in *RoundInput) (*Outcome, error) {
	out, err := Advance(in)
	var missing *MissingAnnotationsError
	if errors.As(err, &missing) {
		s.renders++
		in.Annotations = renderAnnotations(in.Rule.Annotations, missing.Samples)
		out, err = Advance(in)
	}
	return out, err
}

// step 以 results（每条查询一个切片）在 now 评估一轮并落到内存表。
func (s *sim) step(now time.Time, results ...[]promclient.Series) *Outcome {
	s.t.Helper()
	in := s.input(now, results...)
	out, err := s.advance(in)
	if err != nil {
		s.t.Fatalf("Advance() error = %v", err)
	}
	s.apply(now, &out.Transition)
	s.lastStatus, s.lastEval, s.postponed = roundStatus(out.Transition.FiringCount, in.Samples.Series), now, false
	return out
}

// skip 记一轮没有走到状态机的评估（查询错误 / 不在生效时间）：只有任务行的上一轮结果变了。
func (s *sim) skip(now time.Time, status int32) {
	s.lastStatus, s.lastEval, s.postponed = status, now, false
}

// postpone 记一轮被顺延的评估（排不上数据源并发槽）：任务行的上一轮结果与时刻都不变，只留下标记。
func (s *sim) postpone() { s.postponed = true }

func (s *sim) apply(now time.Time, tr *Transition) {
	s.t.Helper()
	for _, fp := range tr.StateDeletes {
		if _, ok := s.states[fp]; !ok {
			s.t.Fatalf("delete of unknown state %s", fp)
		}
		delete(s.states, fp)
	}
	for _, a := range tr.AlertInserts {
		for _, other := range s.alerts {
			if other.Fingerprint == a.Fingerprint && other.Status == monitorcontract.AlertFiring {
				s.t.Fatalf("second active alert for fingerprint %s", a.Fingerprint)
			}
		}
		s.alerts[a.ID] = &simAlert{AlertInsert: a, Status: monitorcontract.AlertFiring, FirstFire: now, LastFire: now}
	}
	for _, in := range tr.StateInserts {
		if _, ok := s.states[in.Fingerprint]; ok {
			s.t.Fatalf("insert of existing state %s", in.Fingerprint)
		}
		labels, _ := json.Marshal(in.Labels)
		s.states[in.Fingerprint] = SeriesState{
			Fingerprint: in.Fingerprint, State: in.State, Severity: in.Severity, QueryIndex: in.QueryIndex, Value: in.Value,
			Labels: labels, ActiveSince: now, LastSeen: now, AlertID: in.AlertID,
		}
	}
	for _, up := range tr.StateUpdates {
		st, ok := s.states[up.Fingerprint]
		if !ok {
			s.t.Fatalf("update of unknown state %s", up.Fingerprint)
		}
		st.State, st.Severity, st.QueryIndex, st.Value, st.AlertID = up.State, up.Severity, up.QueryIndex, up.Value, up.AlertID
		if up.Restart {
			st.ActiveSince = now
		}
		if up.State == monitorcontract.StateRecovering {
			st.RecoverSince = now
		} else {
			st.LastSeen, st.RecoverSince = now, time.Time{}
		}
		s.states[up.Fingerprint] = st
	}
	for _, up := range tr.AlertUpdates {
		a := s.alerts[up.ID]
		if a == nil || a.Status != monitorcontract.AlertFiring {
			s.t.Fatalf("update of non-active alert %s", up.ID)
		}
		a.Severity, a.QueryIndex, a.Value, a.Muted, a.MuteID, a.LastFire = up.Severity, up.QueryIndex, up.Value, up.Muted, up.MuteID, now
		a.Inhibited, a.Inhibit, a.CIID, a.EscalationLevel = up.Inhibited, up.Inhibit, up.CIID, up.EscalationLevel
		if up.Inhibited != (up.Inhibit.Kind != monitorcontract.InhibitNone) {
			s.t.Fatalf("alert %s: inhibited = %v with kind %d violates the table constraint", up.ID, up.Inhibited, up.Inhibit.Kind)
		}
	}
	for _, res := range tr.AlertResolves {
		a := s.alerts[res.ID]
		if a == nil || a.Status != monitorcontract.AlertFiring {
			s.t.Fatalf("resolve of non-active alert %s", res.ID)
		}
		a.Status, a.ResolveTime, a.HasRecovery, a.RecoveryValue = monitorcontract.AlertResolved, now, res.HasRecovery, res.RecoveryValue
		a.Muted, a.MuteID = res.Muted, res.MuteID
	}
	for _, ev := range tr.Events {
		if !monitorcontract.ValidAlertEventType(ev.Type) || s.alerts[ev.AlertID] == nil {
			s.t.Fatalf("bad timeline row %+v", ev)
		}
		s.events = append(s.events, ev)
	}
	for _, act := range tr.Actions {
		if !monitorcontract.ValidActionKind(act.Kind) || s.alerts[act.AlertID] == nil {
			s.t.Fatalf("bad action row %+v", act)
		}
		if !slices.ContainsFunc(s.actions, func(o ActionInsert) bool { return o.AlertID == act.AlertID && o.Kind == act.Kind }) {
			s.actions = append(s.actions, act)
		}
	}
	for _, o := range tr.Outbox {
		// 表约束：升级通知的 level 与 seq 的编码一致，其余通知 level 为 0。
		if level, _, ok := monitorcontract.SplitEscalationSeq(o.Seq); (o.Kind == monitorcontract.NotifyKindEscalation) != (o.Level != 0) ||
			(o.Kind == monitorcontract.NotifyKindEscalation && (!ok || level != o.Level)) {
			s.t.Fatalf("outbox row %+v violates the level check", o)
		}
		var existing *simOutbox
		for _, e := range s.outbox {
			if e.AlertID == o.AlertID && e.Kind == o.Kind && e.Seq == o.Seq {
				existing = e
			}
		}
		switch {
		case existing == nil:
			s.outbox = append(s.outbox, &simOutbox{OutboxInsert: o, Status: monitorcontract.OutboxPending})
		case o.Rearm && existing.Status == monitorcontract.OutboxDropped:
			existing.Status, existing.Done = monitorcontract.OutboxPending, time.Time{}
			s.rearmed++
		}
	}
}

// dispatch 模拟 core 的 monitor-notify：派发全部待派发行（被屏蔽 / 被抑制的事件丢弃），回写通知计数——
// 升级通知记在该级的进度上（monitor_alert_escalation），不动事件的 notify_count。
func (s *sim) dispatch(now time.Time) {
	for _, o := range s.outbox {
		if o.Status != monitorcontract.OutboxPending {
			continue
		}
		a := s.alerts[o.AlertID]
		if a.Muted || a.Inhibited {
			o.Status, o.Done = monitorcontract.OutboxDropped, now
			continue
		}
		o.Status = monitorcontract.OutboxDone
		switch o.Kind {
		case monitorcontract.NotifyKindResolved:
		case monitorcontract.NotifyKindEscalation:
			level, n, _ := monitorcontract.SplitEscalationSeq(o.Seq)
			for len(a.Levels) < int(level) {
				a.Levels = append(a.Levels, monitorpipeline.EscalationLevelState{})
			}
			ls := &a.Levels[level-1]
			ls.NotifyCount, ls.LastNotifyTime = max(ls.NotifyCount, n), now
		default:
			a.NotifyCount++
			a.LastNotifyTime = now
		}
	}
}

// timeline 列出某类时间线行的条数。
func (s *sim) timeline(typ string) int {
	n := 0
	for _, ev := range s.events {
		if ev.Type == typ {
			n++
		}
	}
	return n
}

// actionKinds 累计入队的事件动作类型（按入队顺序）。
func (s *sim) actionKinds() []int32 {
	out := make([]int32, 0, len(s.actions))
	for _, a := range s.actions {
		out = append(out, a.Kind)
	}
	return out
}

// drop 模拟 core 派发失败次数用尽：把全部待派发行丢弃，通知计数不前进。
func (s *sim) drop(now time.Time) {
	for _, o := range s.outbox {
		if o.Status == monitorcontract.OutboxPending {
			o.Status, o.Done = monitorcontract.OutboxDropped, now
		}
	}
}

// kinds 列出发件箱的 "kind/seq"（按写入顺序）。
func (s *sim) kinds() []string {
	out := make([]string, 0, len(s.outbox))
	for _, o := range s.outbox {
		out = append(out, fmt.Sprintf("%d/%d", o.Kind, o.Seq))
	}
	return out
}

// only 返回唯一的状态行；数量不是 1 时失败。
func (s *sim) only() SeriesState {
	s.t.Helper()
	if len(s.states) != 1 {
		s.t.Fatalf("want exactly 1 state row, got %d", len(s.states))
	}
	for _, st := range s.states {
		return st
	}
	return SeriesState{}
}

// alert 返回唯一的告警；数量不是 1 时失败。
func (s *sim) alert() *simAlert {
	s.t.Helper()
	if len(s.alerts) != 1 {
		s.t.Fatalf("want exactly 1 alert, got %d", len(s.alerts))
	}
	for _, a := range s.alerts {
		return a
	}
	return nil
}

func steps(out *Outcome) []string {
	got := make([]string, 0, len(out.Points))
	for _, p := range out.Points {
		got = append(got, p.Step)
	}
	sort.Strings(got)
	return got
}

func equalStrings(a, b []string) bool { return slices.Equal(a, b) }
