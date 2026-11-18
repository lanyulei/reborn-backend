package monitorpipeline

import (
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// EscalationPolicy 一条启用的升级策略里参与计算的部分（monitor_escalation 的 mode / min_severity / levels）。
// Levels 须已通过 monitorcontract.ValidateEscalationLevels。
type EscalationPolicy struct {
	Mode        int32
	MinSeverity int32
	Levels      []monitorcontract.EscalationLevel
}

// EscalationLevelState 一个事件在某一级上的通知进度。
type EscalationLevelState struct {
	// NotifyCount / LastNotifyTime 该级已派发的通知次数与最近一次的时间（monitor_alert_escalation，core 回写）。
	NotifyCount    int32
	LastNotifyTime time.Time
	// Pending 发件箱里还有该级待派发的通知（已入箱、core 尚未回写）：期间不再追加。
	Pending bool
	// DroppedAt 该级下一次通知（序号 NotifyCount + 1）的发件箱行被 core 丢弃的时刻；零值 = 没有这样的行。
	DroppedAt time.Time
}

// EscalationInput Escalate 的全部输入。
type EscalationInput struct {
	// Policy 规则引用的升级策略；nil = 没有（未引用、已停用或已删除）。
	Policy *EscalationPolicy
	// Now 本轮时刻；FirstFire 事件的首次触发时间（各级的 after_m 自它起算）。
	Now       time.Time
	FirstFire time.Time
	// Severity 事件当前级别。
	Severity int32
	// Acked 事件已被认领（monitor_alert.ack_time 非空）。
	Acked bool
	// Suppressed 事件被屏蔽或被抑制：期间不升级、不提醒。
	Suppressed bool
	// Levels 各级的通知进度，下标 i 对应第 i+1 级；比策略的级数短时缺的按零值。
	Levels []EscalationLevelState
}

// EscalationNotify 一条到期的升级通知：第 Level 级的第 Seq 次（1 = 升级到该级时的首次通知）。
// 发件箱行 kind = monitorcontract.NotifyKindEscalation、seq = monitorcontract.EscalationOutboxSeq(Level, Seq)。
// Rearm 为真表示同键的行此前被丢弃，应把它重新置为待派发（而不是当作已存在跳过）。
type EscalationNotify struct {
	Level int32
	Seq   int32
	Rearm bool
}

// EscalationResult Escalate 的结论。
type EscalationResult struct {
	// Due 本轮应入箱的升级通知，按级升序；Due 是新切片。
	Due []EscalationNotify
	// Level 事件已到达的最高级（monitor_alert.escalation_level），0 = 尚未升级。
	Level int32
	// RuleRepeat 规则自身的重复通知（发给规则的通知对象）是否还应继续：事件已被认领，或策略为「接替」
	// 且已升级时为 false。没有策略时只看认领。
	RuleRepeat bool
}

// droppedRetry 被丢弃的升级通知在该级没有配置重复间隔时多久重试一次（与规则首次通知的重试同一节奏）。
const droppedRetry = 10 * time.Minute

// Escalate 计算一个告警中的事件此刻到期的升级通知（纯函数：同样的输入恒得同样的结果，重复评估幂等——
// 序号由 NotifyCount 决定，发件箱的唯一键挡住重复行）。规则：
//   - 策略为 nil 或事件级别低于 MinSeverity：不升级
//   - 第 i 级「生效」= 距首次触发已满 after_m，且（触发条件是「未恢复」，或事件未被认领）；
//     「到达」= 生效，或该级已经发过 / 正在发通知。Level = 到达的最高级
//   - 被屏蔽 / 抑制期间（Suppressed）不产生任何通知，Level 只反映已经通知过的级
//   - 叠加：每个生效的级各自计算；接替：只有到达的最高级可以通知（且它须仍然生效），更低的级不再通知
//   - 一级之内：从未通知过 → 第 1 次；否则须配置了重复间隔、距上次通知已满间隔、未达次数上限
//   - 该级有通知在途（Pending）时不追加；下一次通知的行被丢弃（DroppedAt）时，过一个重复间隔
//     （未配置为 10 分钟）后以 Rearm 重新入箱
func Escalate(in EscalationInput) EscalationResult {
	res := EscalationResult{RuleRepeat: !in.Acked}
	p := in.Policy
	if p == nil || len(p.Levels) == 0 || in.Severity < p.MinSeverity {
		return res
	}
	elapsed := in.Now.Sub(in.FirstFire)
	n := min(len(p.Levels), int(monitorcontract.MaxEscalationLevels))
	state := func(i int) EscalationLevelState {
		if i < len(in.Levels) {
			return in.Levels[i]
		}
		return EscalationLevelState{}
	}
	active := func(i int) bool {
		l := &p.Levels[i]
		if in.Suppressed || elapsed < time.Duration(l.AfterM)*time.Minute {
			return false
		}
		return l.Trigger == monitorcontract.EscalationTriggerUnresolved || !in.Acked
	}
	top := -1
	for i := n - 1; i >= 0; i-- {
		if st := state(i); active(i) || st.NotifyCount > 0 || st.Pending {
			top = i
			break
		}
	}
	if top < 0 {
		return res
	}
	res.Level = int32(top + 1) // #nosec G115 -- 至多 MaxEscalationLevels
	takeover := p.Mode == monitorcontract.EscalationModeTakeover
	if takeover {
		res.RuleRepeat = false
	}
	for i := 0; i <= top; i++ {
		if (takeover && i != top) || !active(i) {
			continue
		}
		if due, ok := levelDue(&p.Levels[i], state(i), in.Now); ok {
			due.Level = int32(i + 1) // #nosec G115 -- 至多 MaxEscalationLevels
			res.Due = append(res.Due, due)
		}
	}
	return res
}

// levelDue 一个生效的级此刻是否该发通知。
func levelDue(l *monitorcontract.EscalationLevel, st EscalationLevelState, now time.Time) (EscalationNotify, bool) {
	if st.Pending {
		return EscalationNotify{}, false
	}
	next := st.NotifyCount + 1
	if next >= monitorcontract.EscalationSeqBase || (l.MaxNotify > 0 && st.NotifyCount >= l.MaxNotify) {
		return EscalationNotify{}, false
	}
	interval := time.Duration(l.RepeatIntervalM) * time.Minute
	if !st.DroppedAt.IsZero() {
		wait := interval
		if wait <= 0 {
			wait = droppedRetry
		}
		if now.Sub(st.DroppedAt) < wait {
			return EscalationNotify{}, false
		}
		return EscalationNotify{Seq: next, Rearm: true}, true
	}
	if st.NotifyCount == 0 {
		return EscalationNotify{Seq: 1}, true
	}
	if interval <= 0 || st.LastNotifyTime.IsZero() || now.Sub(st.LastNotifyTime) < interval {
		return EscalationNotify{}, false
	}
	return EscalationNotify{Seq: next}, true
}

// VolumeEstimate 一条始终无人认领、始终不恢复的告警在 24 小时内的通知次数估算（每次通知算一次，
// 不乘以收件人数）。
type VolumeEstimate struct {
	// Rule 规则自身的通知次数（首次 + 重复）。
	Rule int32
	// Levels 各级的通知次数，下标 i 对应第 i+1 级。
	Levels []int32
	// Total 合计。
	Total int32
}

// estimateHorizonM 估算的时间跨度：24 小时。
const estimateHorizonM = 24 * 60

// EstimateVolume 估算 24 小时通知量：以一分钟为步长，用 Escalate 本身推演一条在 0 时刻触发、
// 始终未认领、未恢复、未被屏蔽的最高级别告警（与真实行为同一份逻辑，不会与之漂移）。
// ruleRepeatM / ruleMaxNotify 是规则的重复通知间隔与次数上限（0 = 不重复 / 不限）；只估算策略本身时
// 两者传 0，Rule 恒为 1（首次通知）。policy 为 nil 时只有规则自身的通知。
func EstimateVolume(policy *EscalationPolicy, ruleRepeatM, ruleMaxNotify int32) VolumeEstimate {
	var est VolumeEstimate
	levels := 0
	if policy != nil {
		levels = min(len(policy.Levels), int(monitorcontract.MaxEscalationLevels))
	}
	est.Levels = make([]int32, levels)
	states := make([]EscalationLevelState, levels)
	start := time.Unix(0, 0).UTC()
	in := EscalationInput{Policy: policy, FirstFire: start, Severity: monitorcontract.SeverityFatal, Levels: states}
	var lastRule time.Time
	for m := range estimateHorizonM {
		now := start.Add(time.Duration(m) * time.Minute)
		in.Now = now
		res := Escalate(in)
		switch {
		case m == 0:
			est.Rule, lastRule = 1, now
		case res.RuleRepeat && ruleRepeatM > 0 && (ruleMaxNotify == 0 || est.Rule < ruleMaxNotify) &&
			now.Sub(lastRule) >= time.Duration(ruleRepeatM)*time.Minute:
			est.Rule++
			lastRule = now
		}
		for _, due := range res.Due {
			i := due.Level - 1
			est.Levels[i]++
			states[i].NotifyCount = due.Seq
			states[i].LastNotifyTime = now
		}
	}
	est.Total = est.Rule
	for _, c := range est.Levels {
		est.Total += c
	}
	return est
}
