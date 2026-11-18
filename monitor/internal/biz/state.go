package biz

import (
	"encoding/json"
	"math"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
)

// RoundInput 状态机一步的全部输入。Advance 只读取这些值，不做任何 I/O。
type RoundInput struct {
	Rule         *Rule
	DatasourceID string
	// Prev 上一轮结束时该任务的全部判定状态。
	Prev []SeriesState
	// Samples 本轮合并后的异常点（查询全部成功才会走到状态机）。
	Samples *SampleSet
	// Incomplete 本轮结果不完整（某条查询被 max_series 截断，或上游声明是部分结果）：没出现的序列
	// 是「未知」而不是「消失」——不进入留观、不判恢复、不删除待触发状态。
	Incomplete bool
	// LastStatus / LastEval 该任务上一轮的结果与时刻（零值 = 从未评估）。上一轮没有完成评估
	// （查询错误 / 不在生效时间）或某个序列上一轮没被观察到时，它的持续 / 留观计时从本轮重新起算：
	// 没观察的时间既不算「持续异常」也不算「持续正常」。
	LastStatus int32
	LastEval   time.Time
	// Postponed 上一轮完成的评估之后，这个任务被领取过但没有评估成（排不上数据源并发槽而顺延）：
	// 那一轮同样什么都没观察到，与上一轮查询错误 / 不在生效时间同等对待。
	Postponed bool
	// Mutes 对该任务此刻生效的屏蔽规则；nil = 没有。
	Mutes *MuteSet
	// Inhibits 规则间抑制 / 拓扑抑制的结论（活跃告警快照上算好的，见 inhibit.go）；nil = 不抑制。
	Inhibits *InhibitView
	// CIs 资源索引：只在判恢复时用来看事件关联的资源是否处于维护态（本轮出现的样本已在合并阶段关联过）。
	CIs *monitorpipeline.CIIndex
	// Escalation 规则引用的升级策略；nil = 没有（未引用、已停用或已删除）。
	Escalation *monitorpipeline.EscalationPolicy
	// Now 本轮时刻（数据库时钟）。
	Now time.Time
	// Recovery 指纹 → 恢复取值；缺失 = 未配置或取值失败（不阻塞恢复）。
	Recovery map[string]float64
	// Annotations 指纹 → 已渲染的附加信息。规则配置了附加信息而本轮要新建的事件在这里找不到时，
	// Advance 返回 *MissingAnnotationsError：模板渲染不属于状态机，由调用方在事务之外完成后重试。
	Annotations map[string]map[string]string
	// NewID 生成告警事件与发件箱行的主键。
	NewID func() (string, error)
}

// Outcome 状态机一步的输出：落库变更 + 判定记录的统计与明细。
type Outcome struct {
	Transition Transition
	// Anomalies 本轮判为异常的序列数；Recovered 本轮判为恢复的序列数。
	Anomalies int32
	Recovered int32
	// Points 各异常点停在哪一步（至多 MaxEvalPoints 条，状态变化的点优先）；Truncated = 有点被省略。
	Points    []monitorcontract.EvalPoint
	Truncated bool
}

// MissingAnnotationsError 本轮要新建事件的样本里有尚未渲染附加信息的。不是故障：调用方渲染
// Samples 的附加信息、填进 RoundInput.Annotations 后重新调用 Advance。
type MissingAnnotationsError struct {
	Samples []*Sample
}

func (e *MissingAnnotationsError) Error() string {
	return "annotations of new alerts are not rendered yet"
}

// stalePending 结果持续不完整时，一个待触发状态多久没被见到就清理掉。
const stalePending = time.Hour

// minAlertHeartbeat 活跃告警行内容没变时刷新 last_fire_time 的间隔下限。
const minAlertHeartbeat = time.Minute

// alertHeartbeat 活跃告警行内容没变时最长多久刷新一次 last_fire_time：max(60s, 评估周期)。
func alertHeartbeat(rule *Rule) time.Duration { return max(minAlertHeartbeat, rule.Interval) }

// sameValue 两个样本值是否相同（NaN 与 NaN 视为相同：否则取值为 NaN 的告警每轮都要写）。
func sameValue(a, b float64) bool { return a == b || (math.IsNaN(a) && math.IsNaN(b)) }

// skipped 上一个该评估的轮次什么都没观察到：没有完成评估（查询错误 / 不在生效时间），或者被顺延了。
func (in *RoundInput) skipped() bool {
	return in.LastStatus == monitorcontract.EvalError || in.LastStatus == monitorcontract.EvalInactive || in.Postponed
}

// unobserved 报告一个待触发状态是否在上一个该评估的轮次没被观察到（查询错误 / 不在生效时间 / 被顺延，
// 或上一轮结果不完整且没见到它）：正常的一轮要么刷新它的 last_seen，要么删掉它。此时持续时长从本轮
// 重新起算。
func (in *RoundInput) unobserved(st *SeriesState) bool {
	return st.State == monitorcontract.StatePending && (st.LastSeen.Before(in.LastEval) || in.Postponed)
}

// fireDue 报告一个本轮出现、尚无活跃告警的序列是否已满持续时长；st 为 nil = 本轮新出现。
func (in *RoundInput) fireDue(st *SeriesState) bool {
	if st == nil || in.unobserved(st) {
		return in.Rule.For <= 0
	}
	return in.Now.Sub(st.ActiveSince) >= in.Rule.For
}

// hasAlert 状态行持有活跃告警（告警中 / 留观中，且告警行仍然活跃）。
func hasAlert(st *SeriesState) bool {
	return st.State != monitorcontract.StatePending && st.AlertID != ""
}

// resolveDue 报告一个本轮未出现的序列是否到了判恢复的时候：必须持有活跃告警；刚从告警中掉出来的、
// 以及上一轮没有完成评估或被顺延的，从本轮起算留观；其余已在留观的从 recover_since 起算。
func (in *RoundInput) resolveDue(st *SeriesState) bool {
	if !hasAlert(st) || in.Incomplete {
		return false
	}
	since := st.RecoverSince
	if st.State == monitorcontract.StateFiring || in.skipped() {
		since = in.Now
	}
	return in.Now.Sub(since) >= in.Rule.keep()
}

// repeatDue 报告是否到了发重复通知的时候：已通知过、到达重复间隔、未达总次数上限，
// 且上一条通知已经派发（还在发件箱里排队时不再追加）。
func repeatDue(rule *Rule, st *SeriesState, now time.Time) bool {
	return rule.RepeatInterval > 0 && !st.PendingNotify && st.NotifyCount >= 1 && !st.LastNotifyTime.IsZero() &&
		now.Sub(st.LastNotifyTime) >= rule.RepeatInterval && underMaxNotify(rule, st)
}

// firstNotifyRetry 规则没有配置重复通知时，被丢弃的首次通知多久重试一次。
const firstNotifyRetry = 10 * time.Minute

// inhibitLiftQuiet 抑制解除时，距该事件上一次通知不足这么久就不再补发「解除后的新通知」：源告警反复
// 触发 / 恢复时，它抑制的每个事件至多按这个节奏通知一次，而不是源每抖动一次就全体通知一遍。
// 屏蔽解除不受它约束（屏蔽规则的起止是人定的，不会抖动）。
const inhibitLiftQuiet = 5 * time.Minute

// firstNotifyRetryDue 报告是否到了重试首次通知的时候：该事件还没有任何一次通知发出去，首次通知行
// 处于已丢弃状态（core 派发失败次数用尽），且距丢弃已满一个重复通知间隔（未配置重复通知时为
// firstNotifyRetry）。否则这个事件整个生命周期都不会再有通知——重复通知与恢复通知都以「通知过」为前提。
// 节奏有上限：重试是把同一行重新置为待派发（不新增行），待派发期间不再重试（调用方先判 PendingNotify），
// core 再次丢弃后要再等一个间隔。
func firstNotifyRetryDue(rule *Rule, st *SeriesState, now time.Time) bool {
	if st.NotifyCount != 0 || st.FirstNotifyDropped.IsZero() {
		return false
	}
	every := rule.RepeatInterval
	if every <= 0 {
		every = firstNotifyRetry
	}
	return now.Sub(st.FirstNotifyDropped) >= every
}

func underMaxNotify(rule *Rule, st *SeriesState) bool {
	return rule.MaxNotify == 0 || st.NotifyCount < rule.MaxNotify
}

// roundStatus 一轮成功评估的结果：有活跃告警 = 告警中；否则查询没有返回任何序列 = 无数据；
// 否则 = 正常（有异常点但都还没触发：等待持续时长，或被「屏蔽事件」命中）。
func roundStatus(firing, series int32) int32 {
	switch {
	case firing > 0:
		return monitorcontract.EvalFiring
	case series == 0:
		return monitorcontract.EvalNoData
	}
	return monitorcontract.EvalOK
}

// pointSink 收集判定记录的明细点：状态发生变化的点（major）优先于维持原状的点（minor）。
type pointSink struct {
	major, minor []monitorcontract.EvalPoint
	dropped      bool
}

// want 报告是否还收这一类点；不收时记下「有点被省略」，调用方据此跳过构造成本。
func (p *pointSink) want(major bool) bool {
	n := len(p.minor)
	if major {
		n = len(p.major)
	}
	if n < monitorcontract.MaxEvalPoints {
		return true
	}
	p.dropped = true
	return false
}

func (p *pointSink) add(major bool, pt monitorcontract.EvalPoint) {
	if major {
		p.major = append(p.major, pt)
	} else {
		p.minor = append(p.minor, pt)
	}
}

func (p *pointSink) result() ([]monitorcontract.EvalPoint, bool) {
	room := monitorcontract.MaxEvalPoints - len(p.major)
	if len(p.minor) > room {
		p.minor, p.dropped = p.minor[:room], true
	}
	return append(p.major, p.minor...), p.dropped
}

func samplePoint(s *Sample, step, alertID, muteID string) monitorcontract.EvalPoint {
	return monitorcontract.EvalPoint{
		Fingerprint: s.Fingerprint, Labels: s.Labels, Severity: s.Severity,
		Value: monitorcontract.FormatValue(s.Value), Step: step, AlertID: alertID, MuteID: muteID,
	}
}

func statePoint(st *SeriesState, labels map[string]string, step, muteID string) monitorcontract.EvalPoint {
	return monitorcontract.EvalPoint{
		Fingerprint: st.Fingerprint, Labels: labels, Severity: st.Severity,
		Value: monitorcontract.FormatValue(st.Value), Step: step, AlertID: st.AlertID, MuteID: muteID,
	}
}

// decodeLabels 解码状态行里的事件标签；损坏的 JSON 按空标签处理（不阻塞恢复）。
func decodeLabels(raw []byte) map[string]string {
	labels := map[string]string{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &labels)
	}
	return labels
}

// advancer Advance 的一次执行上下文。
type advancer struct {
	in     *RoundInput
	out    *Outcome
	tr     *Transition
	points pointSink
	// missing 要新建事件但附加信息尚未渲染的样本。
	missing []*Sample
	// updatesHint / eventsHint 告警更新行、时间线行第一次用到时分配的容量（本轮的上界 / 触发 + 恢复的上界）。
	updatesHint, eventsHint int
}

// Advance 是评估状态机的纯函数：给定规则、上一轮状态、本轮样本（已过改写 / 富化 / 丢弃 / 范围过滤 /
// 级别抑制，见 mergeSamples）、抑制结论、生效的屏蔽规则、升级策略与本轮时刻，算出要落库的全部变更
// （状态行增 / 改 / 删、告警事件增 / 改 / 恢复、发件箱行、时间线行、事件动作）与判定记录明细。
// 不做 I/O、不读时钟、不改写输入、不执行模板；同样的输入（含 NewID 的输出序列）必得同样的结果。
//
// 每个样本在这里走完流水线的后半段，顺序固定：规则间抑制 / 拓扑抑制（Inhibits）→ 屏蔽（Mutes，
// 以及关联资源的维护态——恒为只屏蔽通知）→ 落库 → 通知。被抑制与被「只屏蔽通知」命中的事件照常
// 建立并留档（inhibited / muted），不通知、不升级、不入队事件动作；解除后仍在告警的按一次新通知处理
// （抑制解除另受 inhibitLiftQuiet 约束）。仍在告警的事件每轮经 monitorpipeline.Escalate 计算到期的
// 升级通知（发件箱 kind 4）；已认领的事件停止规则的重复通知与「未认领」类升级。
//
// 转移（设计 §4.2 第 6 步）：
//
//	本轮出现：
//	  无状态            → 待触发；for = 0 时直接走「触发」
//	  待触发，未满 for   → 待触发（刷新 value / severity / last_seen）
//	  待触发，已满 for   → 触发：命中「屏蔽事件」→ 保持待触发、不建事件；
//	                      否则建事件并进入告警中，未被「只屏蔽通知」命中则写发件箱（触发，seq 1）
//	  告警中 / 留观中    → 告警中：事件的 value / severity / 屏蔽标记有变化或到了心跳间隔才写事件行；
//	                      屏蔽刚解除 → 按一次新通知（恒重新激活同键的已丢弃行）；
//	                      否则首次通知行已被丢弃且至今没有通知出去 → 每个重复间隔（未配置时 10 分钟）
//	                      把它重新置为待派发一次；
//	                      否则到达重复间隔且未达上限 → 发件箱（重复，同样重新激活已丢弃行）
//	本轮未出现（结果完整）：
//	  待触发            → 删除状态行
//	  告警中            → 留观中（recover_since = 本轮）；留观时长为 0 时直接走「恢复」
//	  留观中，未满留观   → 不变
//	  留观中，已满留观   → 恢复：事件置已恢复并写恢复值；notify_recovery 且未屏蔽且此前通知过 →
//	                      发件箱（恢复）；删除状态行
//	本轮未出现（结果不完整，Incomplete）：
//	  一律视为未知      → 状态不变；已在留观的把留观起点推到本轮（没观察到不算「持续正常」）
//
// 计时只累计观察到的时间：待触发状态上一轮没被观察到（查询错误 / 不在生效时间 / 结果不完整且没见到它）
// 时持续时长从本轮重新起算；留观中的状态跨过查询错误 / 不在生效时间的轮次后留观从本轮重新起算。
// 被顺延的轮次（排不上数据源并发槽，Postponed）与查询错误 / 不在生效时间的轮次同等对待。
//
// 规则参数中途变更不重置已有状态：下一轮直接按新参数继续判定。
func Advance(in *RoundInput) (*Outcome, error) {
	samples := in.Samples.Kept
	prevIdx := make(map[string]int32, len(in.Prev))
	for i := range in.Prev {
		prevIdx[in.Prev[i].Fingerprint] = int32(i) // #nosec G115 -- 状态行数受 max_series × 查询数约束
	}
	// 预扫一遍：记下每个样本对应的状态行，并数出各输出切片的确切容量。
	match := make([]int32, len(samples))
	seen := make([]bool, len(in.Prev))
	var fresh, matched, withAlert, fireDue int
	for i := range samples {
		pi, ok := prevIdx[samples[i].Fingerprint]
		if !ok {
			match[i] = -1
			fresh++
			continue
		}
		match[i], seen[pi] = pi, true
		matched++
		st := &in.Prev[pi]
		switch {
		case hasAlert(st):
			withAlert++
		case in.fireDue(st):
			fireDue++
		}
	}
	if in.fireDue(nil) {
		fireDue += fresh
	}
	out := &Outcome{Anomalies: int32(len(samples))} // #nosec G115 -- 同上
	a := &advancer{in: in, out: out, tr: &out.Transition}
	a.tr.StateInserts = make([]StateInsert, 0, fresh)
	a.tr.StateUpdates = make([]StateUpdate, 0, len(in.Prev))
	gone := len(in.Prev) - matched
	a.updatesHint, a.eventsHint = withAlert, fireDue+gone
	if fireDue > 0 {
		a.tr.AlertInserts = make([]AlertInsert, 0, fireDue)
		a.tr.Outbox = make([]OutboxInsert, 0, fireDue)
	}
	if gone > 0 {
		a.tr.StateDeletes = make([]string, 0, gone)
	}

	for i := range samples {
		s := &samples[i]
		if match[i] < 0 {
			if in.fireDue(nil) {
				if err := a.fire(s, nil); err != nil {
					return nil, err
				}
				continue
			}
			a.tr.StateInserts = append(a.tr.StateInserts, StateInsert{
				Fingerprint: s.Fingerprint, State: monitorcontract.StatePending, Severity: s.Severity,
				QueryIndex: s.QueryIndex, Value: s.Value, Labels: s.Labels,
			})
			a.pending(s)
			continue
		}
		st := &in.Prev[match[i]]
		switch {
		case hasAlert(st):
			if err := a.refresh(s, st); err != nil {
				return nil, err
			}
		case in.fireDue(st):
			// 含「状态已是告警中但活跃告警不在了」的修复路径：按重新触发处理。
			if err := a.fire(s, st); err != nil {
				return nil, err
			}
		default:
			a.putState(s, st, monitorcontract.StatePending, "")
			a.pending(s)
		}
	}
	for i := range in.Prev {
		if seen[i] {
			continue
		}
		if err := a.absent(&in.Prev[i]); err != nil {
			return nil, err
		}
	}
	for _, extra := range []struct {
		samples []Sample
		step    string
	}{
		{in.Samples.Inhibited, monitorcontract.StepInhibited},
		{in.Samples.Dropped, monitorcontract.StepDropped},
		{in.Samples.OutOfScope, monitorcontract.StepOutOfScope},
	} {
		for i := range extra.samples {
			if a.points.want(false) {
				a.points.add(false, samplePoint(&extra.samples[i], extra.step, "", ""))
			}
		}
	}
	if len(a.missing) > 0 {
		return nil, &MissingAnnotationsError{Samples: a.missing}
	}
	out.Points, out.Truncated = a.points.result()
	return out, nil
}

func (a *advancer) pending(s *Sample) {
	if a.points.want(false) {
		a.points.add(false, samplePoint(s, monitorcontract.StepPending, "", ""))
	}
}

// putState 写样本对应的状态行：st 为 nil 表示新增。
func (a *advancer) putState(s *Sample, st *SeriesState, state int32, alertID string) {
	if st == nil {
		a.tr.StateInserts = append(a.tr.StateInserts, StateInsert{
			Fingerprint: s.Fingerprint, State: state, Severity: s.Severity, QueryIndex: s.QueryIndex,
			Value: s.Value, Labels: s.Labels, AlertID: alertID,
		})
		return
	}
	a.tr.StateUpdates = append(a.tr.StateUpdates, StateUpdate{
		Fingerprint: s.Fingerprint, State: state, Severity: s.Severity, QueryIndex: s.QueryIndex,
		Value: s.Value, AlertID: alertID, Restart: a.in.unobserved(st),
	})
}

func (a *advancer) notify(alertID string, kind, seq, level int32, rearm bool) error {
	id, err := a.in.NewID()
	if err != nil {
		return err
	}
	a.tr.Outbox = append(a.tr.Outbox, OutboxInsert{ID: id, AlertID: alertID, Kind: kind, Seq: seq, Level: level, Rearm: rearm})
	return nil
}

// ref 样本在抑制判定里的样子；alertID 是它的活跃告警（尚未建事件为空）。
func (a *advancer) ref(s *Sample, alertID string) monitorpipeline.AlertRef {
	return monitorpipeline.AlertRef{
		ID: alertID, RuleID: a.in.Rule.ID, GroupPath: a.in.Rule.GroupPath, Severity: s.Severity, Labels: s.Labels, CIID: s.ciID(),
	}
}

// suppression 一个样本此刻的抑制与屏蔽结论（流水线顺序：先抑制，后屏蔽）。
type suppression struct {
	hit       monitorpipeline.InhibitHit
	inhibited bool
	muteID    string
	// mode 命中的屏蔽规则的方式（0 = 没有命中屏蔽规则）；maintenance = 关联资源处于维护态。
	mode        int32
	maintenance bool
}

// muted 事件的通知是否被屏蔽（屏蔽规则，或关联资源处于维护态——后者恒为「只屏蔽通知」）。
func (p *suppression) muted() bool { return p.mode != 0 || p.maintenance }

func (p *suppression) any() bool { return p.inhibited || p.muted() }

// step 判定记录里的步骤：被抑制优先于被屏蔽（与流水线顺序一致），都不是时为 plain。
func (p *suppression) step(plain string) string {
	switch {
	case p.inhibited && p.hit.Kind == monitorcontract.InhibitTopology:
		return monitorcontract.StepInhibitedTopology
	case p.inhibited:
		return monitorcontract.StepInhibitedRule
	case p.mode != 0:
		return monitorcontract.StepMutedNotify
	case p.maintenance:
		return monitorcontract.StepMutedMaintenance
	}
	return plain
}

func (a *advancer) suppress(s *Sample, alertID string) suppression {
	var p suppression
	ref := a.ref(s, alertID)
	p.hit, p.inhibited = a.in.Inhibits.Match(&ref, s.upstream(), a.in.Rule.TopologyInhibit)
	p.muteID, p.mode = a.in.Mutes.Match(s.Labels, s.Severity)
	p.maintenance = s.CI.Maintenance()
	return p
}

// event 记一条时间线行（ev 的 ID 由这里填）。切片在第一次用到时才分配：绝大多数轮次没有任何状态变化。
func (a *advancer) event(ev AlertEventInsert) error {
	id, err := a.in.NewID()
	if err != nil {
		return err
	}
	if a.tr.Events == nil {
		a.tr.Events = make([]AlertEventInsert, 0, a.eventsHint)
	}
	ev.ID = id
	a.tr.Events = append(a.tr.Events, ev)
	return nil
}

// muteEvent 屏蔽 / 解除屏蔽的时间线行：记屏蔽规则；没有规则（因资源维护态而屏蔽）时记资源。
func muteEvent(alertID, typ, muteID, ciID string) AlertEventInsert {
	if muteID != "" {
		ciID = ""
	}
	return AlertEventInsert{AlertID: alertID, Type: typ, MuteID: muteID, CIID: ciID}
}

// fireActions 事件第一次可以对外通知时（触发时未被屏蔽 / 抑制，或屏蔽 / 抑制解除时）按规则配置入队
// 自动建单 / 触发流程 / AI 摘要。(alert_id, kind) 唯一：同一事件每类动作只会有一行，重复入队是空操作。
// 被屏蔽、被抑制期间不入队：没人该被打扰的事件不自动建单。
func (a *advancer) fireActions(alertID string) error {
	act := &a.in.Rule.Actions
	queue := func(kind int32, payload monitorcontract.ActionPayload) error {
		id, err := a.in.NewID()
		if err != nil {
			return err
		}
		a.tr.Actions = append(a.tr.Actions, ActionInsert{ID: id, AlertID: alertID, Kind: kind, Payload: payload})
		return nil
	}
	if act.Ticket.Enabled {
		if err := queue(monitorcontract.ActionTicket, monitorcontract.ActionPayload{
			TemplateID: act.Ticket.TemplateID, AgentUserID: act.Ticket.AgentUserID,
		}); err != nil {
			return err
		}
	}
	if act.Workflow.Enabled {
		if err := queue(monitorcontract.ActionWorkflow, monitorcontract.ActionPayload{
			DefinitionKey: act.Workflow.DefinitionKey, AgentUserID: act.Workflow.AgentUserID,
		}); err != nil {
			return err
		}
	}
	if act.AISummary {
		return queue(monitorcontract.ActionAISummary, monitorcontract.ActionPayload{})
	}
	return nil
}

// fire 异常已满持续时长：抑制与屏蔽判定 → 建事件 → 首次通知与事件动作。
func (a *advancer) fire(s *Sample, st *SeriesState) error {
	p := a.suppress(s, "")
	if p.mode == monitorcontract.MuteModeEvent {
		a.putState(s, st, monitorcontract.StatePending, "")
		if a.points.want(true) {
			a.points.add(true, samplePoint(s, monitorcontract.StepMutedEvent, "", p.muteID))
		}
		return nil
	}
	annotations, rendered := a.in.Annotations[s.Fingerprint]
	if !rendered && len(a.in.Rule.Annotations) > 0 {
		a.missing = append(a.missing, s)
		return nil
	}
	alertID, err := a.in.NewID()
	if err != nil {
		return err
	}
	a.tr.AlertInserts = append(a.tr.AlertInserts, AlertInsert{
		ID: alertID, Fingerprint: s.Fingerprint, Severity: s.Severity, QueryIndex: s.QueryIndex, Value: s.Value,
		Labels: s.Labels, Annotations: annotations,
		GroupKey: monitorcontract.GroupKey(a.in.Rule.ID, s.Labels, a.in.Rule.GroupKeyLabels),
		Muted:    p.muted(), MuteID: p.muteID, Inhibited: p.inhibited, Inhibit: p.hit, CIID: s.ciID(),
	})
	a.putState(s, st, monitorcontract.StateFiring, alertID)
	a.tr.FiringCount++
	if err := a.event(AlertEventInsert{AlertID: alertID, Type: monitorcontract.AlertEventFire, Severity: s.Severity}); err != nil {
		return err
	}
	if p.inhibited {
		if err := a.event(AlertEventInsert{AlertID: alertID, Type: monitorcontract.AlertEventInhibit, Inhibit: p.hit}); err != nil {
			return err
		}
	}
	if p.muted() {
		if err := a.event(muteEvent(alertID, monitorcontract.AlertEventMute, p.muteID, s.ciID())); err != nil {
			return err
		}
	}
	if !p.any() {
		if err := a.notify(alertID, monitorcontract.NotifyKindFiring, 1, 0, false); err != nil {
			return err
		}
		if err := a.fireActions(alertID); err != nil {
			return err
		}
	}
	if a.points.want(true) {
		a.points.add(true, a.point(s, p.step(monitorcontract.StepFired), alertID, &p))
	}
	return nil
}

// point 一个已建事件的样本的判定记录明细（带上屏蔽规则、抑制它的源告警与关联资源）。
func (a *advancer) point(s *Sample, step, alertID string, p *suppression) monitorcontract.EvalPoint {
	pt := samplePoint(s, step, alertID, p.muteID)
	pt.CIID = s.ciID()
	if p.inhibited {
		pt.InhibitAlertID = p.hit.AlertID
	}
	return pt
}

// escalate 一个仍在告警的事件此刻的升级结论，并把到期的升级通知写进发件箱。没有策略时只看认领。
func (a *advancer) escalate(s *Sample, st *SeriesState, suppressed bool) (monitorpipeline.EscalationResult, error) {
	acked := !st.AckTime.IsZero()
	if a.in.Escalation == nil {
		return monitorpipeline.EscalationResult{RuleRepeat: !acked, Level: st.EscalationLevel}, nil
	}
	res := monitorpipeline.Escalate(monitorpipeline.EscalationInput{
		Policy: a.in.Escalation, Now: a.in.Now, FirstFire: st.FirstFire, Severity: s.Severity,
		Acked: acked, Suppressed: suppressed, Levels: st.Escalation,
	})
	// 事件行上的级数是「到达过的最高级」：只增不减（策略被停用、被屏蔽期间在途的行被丢弃都不让它回退）。
	res.Level = max(res.Level, st.EscalationLevel)
	for _, due := range res.Due {
		seq := monitorcontract.EscalationOutboxSeq(due.Level, due.Seq)
		if seq == 0 {
			continue
		}
		if err := a.notify(st.AlertID, monitorcontract.NotifyKindEscalation, seq, due.Level, due.Rearm); err != nil {
			return res, err
		}
		// 时间线只记「升级到第几级」；该级之后的重复提醒在通知记录里。
		if due.Seq == 1 && !due.Rearm {
			if err := a.event(AlertEventInsert{AlertID: st.AlertID, Type: monitorcontract.AlertEventEscalate, Level: due.Level}); err != nil {
				return res, err
			}
		}
	}
	return res, nil
}

// refresh 持有活跃告警的序列本轮仍出现（告警中，或从留观中回到告警中）。
// 事件已存在，屏蔽规则无论哪种方式都只能屏蔽它的通知。
func (a *advancer) refresh(s *Sample, st *SeriesState) error {
	p := a.suppress(s, st.AlertID)
	muted, suppressed := p.muted(), p.any()
	esc, err := a.escalate(s, st, suppressed)
	if err != nil {
		return err
	}
	// 告警行只在内容有变化或到了心跳间隔时写：每条活跃告警每轮都 UPDATE 会命中该表的全部索引。
	if st.AlertSeverity != s.Severity || st.Muted != muted || st.MuteID != p.muteID || !sameValue(st.AlertValue, s.Value) ||
		st.Inhibited != p.inhibited || st.Inhibit != p.hit || st.CIID != s.ciID() || st.EscalationLevel != esc.Level ||
		a.in.Now.Sub(st.LastFireTime) >= alertHeartbeat(a.in.Rule) {
		if a.tr.AlertUpdates == nil {
			// 第一次用到才分配：内容没变的轮次（常态）一行都不写。
			a.tr.AlertUpdates = make([]AlertUpdate, 0, a.updatesHint)
		}
		a.tr.AlertUpdates = append(a.tr.AlertUpdates, AlertUpdate{
			ID: st.AlertID, Severity: s.Severity, QueryIndex: s.QueryIndex, Value: s.Value, Muted: muted, MuteID: p.muteID,
			Inhibited: p.inhibited, Inhibit: p.hit, CIID: s.ciID(), EscalationLevel: esc.Level,
		})
	}
	a.putState(s, st, monitorcontract.StateFiring, st.AlertID)
	a.tr.FiringCount++
	if st.Inhibited != p.inhibited {
		ev := AlertEventInsert{AlertID: st.AlertID, Type: monitorcontract.AlertEventInhibit, Inhibit: p.hit}
		if !p.inhibited {
			ev.Type, ev.Inhibit = monitorcontract.AlertEventUninhibit, st.Inhibit
		}
		if err := a.event(ev); err != nil {
			return err
		}
	}
	if st.Muted != muted {
		ev := muteEvent(st.AlertID, monitorcontract.AlertEventMute, p.muteID, s.ciID())
		if !muted {
			ev = muteEvent(st.AlertID, monitorcontract.AlertEventUnmute, st.MuteID, st.CIID)
		}
		if err := a.event(ev); err != nil {
			return err
		}
	}
	lifted := !suppressed && (st.Muted || st.Inhibited)
	if lifted {
		// 屏蔽 / 抑制期间没有入队的事件动作，在解除的这一轮补上（已入队过的是空操作）。
		if err := a.fireActions(st.AlertID); err != nil {
			return err
		}
	}
	switch {
	case suppressed:
		// 被屏蔽或被抑制：不通知。
	case !st.AckTime.IsZero():
		// 已认领：规则的通知（重复、解除后的新通知、首次通知的重试）一律停止；取消认领后按下面的分支接续——
		// 至多立刻补一条，之后回到重复间隔的节奏，不会把认领期间攒下的次数一次发完。
	case lifted:
		// 屏蔽 / 抑制解除后仍在告警：按一次新通知处理（功能设计 §7.3），不等重复间隔。这次写入恒执行，
		// 即使发件箱里还有一条待派发：core 可能正在按「事件被屏蔽」丢弃它（它读到的是本轮提交前的
		// 屏蔽标记）。写入与 core 的派发事务争用同一发件箱行的锁——core 先拿到，这里等它提交后把
		// 已丢弃的行重新置为待派发；这里先拿到，core 跳过该行，下一轮读到的已是解除屏蔽后的事件。
		switch {
		case st.NotifyCount == 0:
			err = a.notify(st.AlertID, monitorcontract.NotifyKindFiring, 1, 0, true)
		case !esc.RuleRepeat || !underMaxNotify(a.in.Rule, st):
			// 「接替」策略已升级（只提醒当前这一级），或已达通知次数上限。
		case st.Muted || a.in.Now.Sub(st.LastNotifyTime) >= inhibitLiftQuiet:
			err = a.notify(st.AlertID, monitorcontract.NotifyKindRepeat, st.NotifyCount+1, 0, true)
		}
	case st.PendingNotify:
		// 已有一条触发 / 重复通知在发件箱里等待派发：本轮不再追加。
	case firstNotifyRetryDue(a.in.Rule, st, a.in.Now):
		// 首次通知被 core 丢弃（派发失败次数用尽）且事件至今没有通知出去：按有上限的节奏把同一行重新置为待派发。
		err = a.notify(st.AlertID, monitorcontract.NotifyKindFiring, 1, 0, true)
	case st.NotifyCount == 0 && st.FirstNotifyDropped.IsZero():
		// 事件从未通知过、发件箱里也没有它的首次通知（触发时被屏蔽 / 抑制，解除时又正被认领）：补上首次通知。
		err = a.notify(st.AlertID, monitorcontract.NotifyKindFiring, 1, 0, false)
	case esc.RuleRepeat && repeatDue(a.in.Rule, st, a.in.Now):
		// 同键的行若被 core 丢弃过（派发失败次数用尽、派发时事件被屏蔽），通知计数不会前进，
		// 这里写出的仍是同一个键：必须重新激活，否则该事件的重复通知从此永久停止。
		err = a.notify(st.AlertID, monitorcontract.NotifyKindRepeat, st.NotifyCount+1, 0, true)
	}
	if err != nil {
		return err
	}
	if a.points.want(false) {
		a.points.add(false, a.point(s, p.step(monitorcontract.StepFiring), st.AlertID, &p))
	}
	return nil
}

// absent 有状态的序列本轮未出现。
func (a *advancer) absent(st *SeriesState) error {
	if !hasAlert(st) {
		// 结果不完整时没出现的序列是未知：待触发状态留着（下次见到它时持续时长重新起算）。只清理很久没
		// 见到的：持续截断 + 序列更替时这些行否则只增不减，而它们的计时反正已经作废。
		if !a.in.Incomplete || a.in.Now.Sub(st.LastSeen) >= stalePending {
			a.tr.StateDeletes = append(a.tr.StateDeletes, st.Fingerprint)
		}
		return nil
	}
	if !a.in.resolveDue(st) {
		a.tr.FiringCount++
		recovering := monitorcontract.StateRecovering
		// 进入留观；或者已在留观但留观要从本轮重新起算（上一轮没有完成评估 / 本轮结果不完整）。
		enter := st.State == monitorcontract.StateFiring && !a.in.Incomplete
		restart := st.State == recovering && (a.in.Incomplete || a.in.skipped())
		if enter || restart {
			a.tr.StateUpdates = append(a.tr.StateUpdates, StateUpdate{
				Fingerprint: st.Fingerprint, State: recovering, Severity: st.Severity,
				QueryIndex: st.QueryIndex, Value: st.Value, AlertID: st.AlertID,
			})
		}
		if (enter || st.State == recovering) && a.points.want(false) {
			a.points.add(false, statePoint(st, decodeLabels(st.Labels), monitorcontract.StepRecovering, ""))
		}
		return nil
	}
	// 恢复：标签只在需要匹配屏蔽规则或写明细时才解码。
	wantPoint := a.points.want(true)
	var (
		labels map[string]string
		muteID string
		mode   int32
	)
	if a.in.Mutes != nil || wantPoint {
		labels = decodeLabels(st.Labels)
		muteID, mode = a.in.Mutes.Match(labels, st.Severity)
	}
	// 屏蔽与抑制按恢复这一刻算：关联资源仍在维护态算被屏蔽；抑制取快照的结论，快照里没有它时沿用事件行上的标记。
	muted := mode != 0 || a.in.CIs.Get(st.CIID).Maintenance()
	inhibited, known := a.in.Inhibits.Known(st.AlertID)
	if !known {
		inhibited = st.Inhibited
	}
	value, has := a.in.Recovery[st.Fingerprint]
	a.tr.AlertResolves = append(a.tr.AlertResolves, AlertResolve{
		ID: st.AlertID, HasRecovery: has, RecoveryValue: value, Muted: muted, MuteID: muteID,
	})
	a.tr.StateDeletes = append(a.tr.StateDeletes, st.Fingerprint)
	a.out.Recovered++
	if err := a.event(AlertEventInsert{AlertID: st.AlertID, Type: monitorcontract.AlertEventResolve, Severity: st.Severity}); err != nil {
		return err
	}
	if sent := st.notified(); a.in.Rule.NotifyRecovery && !muted && !inhibited && sent >= 1 {
		if err := a.notify(st.AlertID, monitorcontract.NotifyKindResolved, sent+1, 0, false); err != nil {
			return err
		}
	}
	// 已关联工单，或规则开了自动建单（工单可能还在 core 的队列里没建出来）：请 core 在工单上追加恢复记录。
	// 没有工单时 core 把这条动作记为「无需执行」。
	if st.TicketID != "" || a.in.Rule.Actions.Ticket.Enabled {
		id, err := a.in.NewID()
		if err != nil {
			return err
		}
		a.tr.Actions = append(a.tr.Actions, ActionInsert{ID: id, AlertID: st.AlertID, Kind: monitorcontract.ActionTicketNote})
	}
	if wantPoint {
		pt := statePoint(st, labels, monitorcontract.StepResolved, muteID)
		pt.CIID = st.CIID
		a.points.add(true, pt)
	}
	return nil
}

// resolving 预判本轮会判恢复的序列（恢复取值查询要在事务之前发出）。只读；prev 是无锁快照。
func resolving(in *RoundInput, prev []SeriesState) []*SeriesState {
	var due []*SeriesState
	for i := range prev {
		if st := &prev[i]; !in.Samples.has(st.Fingerprint) && in.resolveDue(st) {
			due = append(due, st)
		}
	}
	return due
}
