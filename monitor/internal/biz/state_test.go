package biz

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/promclient"
)

// 常用输入。
var one = [][]promclient.Series{q(ser(90, "instance", "a"))}

func onceMute(id string, mode int32, fromSec, toSec int, mod ...func(*Mute)) *Mute {
	m := &Mute{ID: id, Mode: mode, TimeType: monitorcontract.MuteTimeOnce, Start: at(fromSec), End: at(toSec)}
	for _, f := range mod {
		f(m)
	}
	return m
}

// round 场景里的一轮评估及其期望。
type round struct {
	at      int
	results [][]promclient.Series // nil = 查询成功但没有任何序列
	// before 在本轮之前改动场景（规则参数、屏蔽规则、外部对告警的改动）。
	before func(s *sim)
	// dispatch 本轮之后模拟 core 派发发件箱。
	dispatch bool
	// incomplete 本轮结果不完整（截断 / 部分结果）。
	incomplete bool
	// skip 非 0 = 本轮没有走到状态机，任务记为该结果（查询错误 / 不在生效时间）。
	skip int32

	wantState  int32    // 唯一状态行的 state；0 = 没有状态行
	wantAlerts string   // 全部告警的 status，升序逗号分隔；"" = 没有告警
	wantOutbox []string // 发件箱累计内容 "kind/seq"
	wantSteps  []string // 本轮判定记录明细的步骤（升序）
	wantFiring int32
}

func TestAdvanceScenarios(t *testing.T) {
	const (
		pending    = monitorcontract.StatePending
		firing     = monitorcontract.StateFiring
		recovering = monitorcontract.StateRecovering
	)
	withFor := func(sec int) func(*Rule) { return func(r *Rule) { r.For = time.Duration(sec) * time.Second } }
	tests := []struct {
		name   string
		rule   *Rule
		mutes  []*Mute
		rounds []round
	}{
		{
			name: "appears → pending → firing after for_s → resolved when it disappears",
			rule: baseRule(),
			rounds: []round{
				{at: 0, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 30, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 60, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 120, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"firing"}, wantFiring: 1},
				{at: 180, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
				// 恢复后再次出现是一条新事件。
				{at: 240, results: one, wantState: pending, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"pending"}},
				{at: 300, results: one, wantState: firing, wantAlerts: "1,2", wantOutbox: []string{"1/1", "3/2", "1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
			},
		},
		{
			name: "for_s = 0 fires on first sight",
			rule: baseRule(withFor(0)),
			rounds: []round{
				{at: 0, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
			},
		},
		{
			name: "pending series vanishing leaves no trace",
			rule: baseRule(),
			rounds: []round{
				{at: 0, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 60, wantSteps: []string{}},
				// 重新出现从头计时。
				{at: 120, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 150, results: one, wantState: pending, wantSteps: []string{"pending"}},
			},
		},
		{
			name: "recover_s keeps the alert under observation",
			rule: baseRule(withFor(0), func(r *Rule) { r.Recover = 120 * time.Second }),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 120, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 180, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "nodata_keep_s larger than recover_s wins",
			rule: baseRule(withFor(0), func(r *Rule) { r.Recover, r.NodataKeep = 60*time.Second, 300*time.Second }),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 300, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 360, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "flapping: recovering → firing again without a new notification, observation restarts",
			rule: baseRule(withFor(0), func(r *Rule) { r.Recover = 120 * time.Second }),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 120, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"firing"}, wantFiring: 1},
				{at: 180, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 240, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 300, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "notify_recovery off: no resolved notification",
			rule: baseRule(withFor(0), func(r *Rule) { r.NotifyRecovery = false }),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, wantAlerts: "2", wantOutbox: []string{"1/1"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name:  "resolved without any prior notification: nothing is sent",
			rule:  baseRule(withFor(0)),
			mutes: []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 0, 30)},
			rounds: []round{
				{at: 0, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{}, wantSteps: []string{"muted_notify"}, wantFiring: 1},
				{at: 60, wantAlerts: "2", wantOutbox: []string{}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "firing notification still queued when the alert resolves: recovery is queued behind it",
			rule: baseRule(withFor(0)),
			rounds: []round{
				{at: 0, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "repeat notifications honour repeat_interval_m and max_notify",
			rule: baseRule(withFor(0), func(r *Rule) { r.RepeatInterval, r.MaxNotify = 5*time.Minute, 3 }),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 240, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"firing"}, wantFiring: 1},
				{at: 300, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1", "2/2"}, wantSteps: []string{"firing"}, wantFiring: 1},
				// core 尚未派发：同一键再次写入是空操作。
				{at: 360, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1", "2/2"}, wantSteps: []string{"firing"}, wantFiring: 1},
				{at: 600, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1", "2/2"}, wantSteps: []string{"firing"}, wantFiring: 1},
				{at: 660, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1", "2/2", "2/3"}, wantSteps: []string{"firing"}, wantFiring: 1},
				// 已达上限（首次 + 两次重复 = 3）。
				{at: 1200, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1", "2/2", "2/3"}, wantSteps: []string{"firing"}, wantFiring: 1},
				// 恢复通知不占上限，序号 = 已发送数 + 1。
				{at: 1260, wantAlerts: "2", wantOutbox: []string{"1/1", "2/2", "2/3", "3/4"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "repeat_interval_m = 0 never repeats; unsent first notification never triggers a repeat",
			rule: baseRule(withFor(0)),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 86400, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"firing"}, wantFiring: 1},
			},
		},
		{
			name: "repeat needs a dispatched first notification",
			rule: baseRule(withFor(0), func(r *Rule) { r.RepeatInterval = 5 * time.Minute }),
			rounds: []round{
				{at: 0, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 600, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"firing"}, wantFiring: 1},
			},
		},
		{
			name:  "mute mode 2 (mute event): no alert, state stays pending, fires once the mute ends",
			rule:  baseRule(withFor(0)),
			mutes: []*Mute{onceMute("m1", monitorcontract.MuteModeEvent, 0, 100)},
			rounds: []round{
				{at: 0, results: one, wantState: pending, wantOutbox: []string{}, wantSteps: []string{"muted_event"}},
				{at: 60, results: one, wantState: pending, wantOutbox: []string{}, wantSteps: []string{"muted_event"}},
				{at: 120, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
			},
		},
		{
			name:  "mute mode 2 with for_s: pending first, then muted, vanishing leaves nothing",
			rule:  baseRule(),
			mutes: []*Mute{onceMute("m1", monitorcontract.MuteModeEvent, 0, 1000)},
			rounds: []round{
				{at: 0, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 60, results: one, wantState: pending, wantSteps: []string{"muted_event"}},
				{at: 120, wantSteps: []string{}},
			},
		},
		{
			name:  "mute mode 1 (mute notify): alert created muted without outbox; mute ending while firing sends a fresh first notification",
			rule:  baseRule(withFor(0)),
			mutes: []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 0, 100)},
			rounds: []round{
				{at: 0, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{}, wantSteps: []string{"muted_notify"}, wantFiring: 1},
				{at: 60, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{}, wantSteps: []string{"muted_notify"}, wantFiring: 1},
				{at: 120, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"firing"}, wantFiring: 1},
				// 只发一次：下一轮不再重复写。
				{at: 180, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"firing"}, wantFiring: 1},
				{at: 240, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name:  "mute starting after the first notification, then ending: next round notifies again at once (as a repeat)",
			rule:  baseRule(withFor(0)),
			mutes: []*Mute{onceMute("m1", monitorcontract.MuteModeEvent, 100, 200)},
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				// 事件已存在：屏蔽事件的规则对它只能屏蔽通知。
				{at: 120, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"muted_notify"}, wantFiring: 1},
				{at: 240, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1", "2/2"}, wantSteps: []string{"firing"}, wantFiring: 1},
				{at: 300, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1", "2/2"}, wantSteps: []string{"firing"}, wantFiring: 1},
			},
		},
		{
			name:  "mute ending does not exceed max_notify",
			rule:  baseRule(withFor(0), func(r *Rule) { r.MaxNotify = 1 }),
			mutes: []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 100, 200)},
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 120, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"muted_notify"}, wantFiring: 1},
				{at: 240, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"firing"}, wantFiring: 1},
			},
		},
		{
			name:  "resolving while muted: no recovery notification",
			rule:  baseRule(withFor(0)),
			mutes: []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 100, 200)},
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 120, wantAlerts: "2", wantOutbox: []string{"1/1"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "rule change mid-flight: shorter for_s applies to a pending series without resetting it",
			rule: baseRule(withFor(3600)),
			rounds: []round{
				{at: 0, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 60, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 120, results: one, before: func(s *sim) { s.rule.For = 90 * time.Second }, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
			},
		},
		{
			name: "rule change mid-flight: longer for_s does not reset an existing alert",
			rule: baseRule(withFor(0)),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, results: one, before: func(s *sim) { s.rule.For = time.Hour }, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"firing"}, wantFiring: 1},
			},
		},
		{
			name: "rule change mid-flight: recover_s shortened while recovering resolves on the next round",
			rule: baseRule(withFor(0), func(r *Rule) { r.Recover = time.Hour }),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 120, before: func(s *sim) { s.rule.Recover = 30 * time.Second }, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "alert closed behind the engine's back while the series is still abnormal: fires a new alert",
			rule: baseRule(withFor(0)),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, results: one, before: func(s *sim) { s.alert().Status = monitorcontract.AlertClosed }, wantState: firing, wantAlerts: "1,3", wantOutbox: []string{"1/1", "1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
			},
		},
		// ---- E2 / E3：结果不完整（截断 / 部分结果）时没出现的序列是未知，不是消失 ----
		{
			name: "incomplete result: a firing series that was not returned neither recovers nor resolves",
			rule: baseRule(withFor(0)),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, incomplete: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{}, wantFiring: 1},
				{at: 120, incomplete: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{}, wantFiring: 1},
				// 结果完整后才按「消失」处理。
				{at: 180, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "incomplete result: a recovering series does not resolve and its observation restarts",
			rule: baseRule(withFor(0), func(r *Rule) { r.Recover = 120 * time.Second }),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				// 已满留观（距 60 已 120s），但本轮结果不完整：不恢复。
				{at: 180, incomplete: true, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				// 留观从 180 重新起算：240 时只观察了 60s。
				{at: 240, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 300, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "incomplete result: a pending series that was not returned keeps its state row, its timer restarts",
			rule: baseRule(),
			rounds: []round{
				{at: 0, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 30, incomplete: true, wantState: pending, wantSteps: []string{}},
				// 30 那一轮没观察到它：持续时长从 60 重新起算，而不是从 0 算满 60s 直接触发。
				{at: 60, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 90, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 120, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
			},
		},
		{
			name: "incomplete result: series that are returned are judged as usual",
			rule: baseRule(),
			rounds: []round{
				{at: 0, results: one, incomplete: true, wantState: pending, wantSteps: []string{"pending"}},
				{at: 60, results: one, incomplete: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
			},
		},
		{
			name: "incomplete result: a pending row unseen for an hour is cleaned up",
			rule: baseRule(),
			rounds: []round{
				{at: 0, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 3540, incomplete: true, wantState: pending, wantSteps: []string{}},
				{at: 3600, incomplete: true, wantSteps: []string{}},
			},
		},
		// ---- E9：计时只累计观察到的时间 ----
		{
			name: "query error between rounds: for_s restarts instead of counting the outage",
			rule: baseRule(),
			rounds: []round{
				{at: 0, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 60, skip: monitorcontract.EvalError},
				{at: 120, skip: monitorcontract.EvalError},
				// 距首次出现已 180s ≥ for_s，但其间两轮没有观察：不触发，从本轮重新计时。
				{at: 180, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 210, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 240, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
			},
		},
		{
			name: "outside the effective window between rounds: for_s restarts",
			rule: baseRule(),
			rounds: []round{
				{at: 0, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 60, skip: monitorcontract.EvalInactive},
				{at: 3600, results: one, wantState: pending, wantSteps: []string{"pending"}},
				{at: 3660, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
			},
		},
		{
			name: "query error while recovering: the outage does not count as observed-healthy time",
			rule: baseRule(withFor(0), func(r *Rule) { r.Recover = 120 * time.Second }),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 120, skip: monitorcontract.EvalError},
				{at: 180, skip: monitorcontract.EvalError},
				// 距进入留观已 180s ≥ recover_s，但其间没有观察：留观从本轮重新起算。
				{at: 240, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 300, wantState: recovering, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"recovering"}, wantFiring: 1},
				{at: 360, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "query error with for_s = 0 and no observation period: nothing to restart",
			rule: baseRule(withFor(0)),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, skip: monitorcontract.EvalError},
				{at: 120, results: one, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"firing"}, wantFiring: 1},
				{at: 180, skip: monitorcontract.EvalError},
				{at: 240, wantAlerts: "2", wantOutbox: []string{"1/1", "3/2"}, wantSteps: []string{"resolved"}},
			},
		},
		{
			name: "alert closed behind the engine's back and the series is gone: state row is dropped silently",
			rule: baseRule(withFor(0)),
			rounds: []round{
				{at: 0, results: one, dispatch: true, wantState: firing, wantAlerts: "1", wantOutbox: []string{"1/1"}, wantSteps: []string{"fired"}, wantFiring: 1},
				{at: 60, before: func(s *sim) { s.alert().Status = monitorcontract.AlertClosed }, wantAlerts: "3", wantOutbox: []string{"1/1"}, wantSteps: []string{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSim(t, tt.rule)
			s.mutes = tt.mutes
			for i, r := range tt.rounds {
				if r.before != nil {
					r.before(s)
				}
				results := r.results
				if results == nil {
					results = [][]promclient.Series{nil}
				}
				// 每轮用全新的序列副本：mergeSamples 会接管并改写标签。
				fresh := make([][]promclient.Series, len(results))
				for qi := range results {
					for _, sr := range results[qi] {
						labels := make(map[string]string, len(sr.Labels))
						for k, v := range sr.Labels {
							labels[k] = v
						}
						fresh[qi] = append(fresh[qi], promclient.Series{Labels: labels, Points: sr.Points})
					}
				}
				tag := fmt.Sprintf("round %d (t+%ds)", i, r.at)
				if r.skip != 0 {
					s.skip(at(r.at), r.skip)
					continue
				}
				s.incomplete = r.incomplete
				out := s.step(at(r.at), fresh...)
				if r.wantState == 0 {
					if len(s.states) != 0 {
						t.Fatalf("%s: want no state row, got %+v", tag, s.states)
					}
				} else if got := s.only().State; got != r.wantState {
					t.Fatalf("%s: state = %d, want %d", tag, got, r.wantState)
				}
				var statuses []string
				for _, a := range s.alerts {
					statuses = append(statuses, fmt.Sprint(a.Status))
				}
				sort.Strings(statuses)
				if got := strings.Join(statuses, ","); got != r.wantAlerts {
					t.Fatalf("%s: alert statuses = %q, want %q", tag, got, r.wantAlerts)
				}
				if r.wantOutbox != nil && !equalStrings(s.kinds(), r.wantOutbox) {
					t.Fatalf("%s: outbox = %v, want %v", tag, s.kinds(), r.wantOutbox)
				}
				if r.wantOutbox == nil && len(s.outbox) != 0 {
					t.Fatalf("%s: outbox = %v, want empty", tag, s.kinds())
				}
				if got := steps(out); !equalStrings(got, r.wantSteps) {
					t.Fatalf("%s: steps = %v, want %v", tag, got, r.wantSteps)
				}
				if out.Transition.FiringCount != r.wantFiring {
					t.Fatalf("%s: firing count = %d, want %d", tag, out.Transition.FiringCount, r.wantFiring)
				}
				if r.dispatch {
					s.dispatch(at(r.at))
				}
			}
		})
	}
}

// 首次通知入箱后、派发前被屏蔽：core 把它丢弃；屏蔽解除后同一键重新置为待派发，而不是丢失。
func TestAdvanceRearmsNotificationDroppedWhileMuted(t *testing.T) {
	s := newSim(t, baseRule(func(r *Rule) { r.For = 0 }))
	s.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 30, 100)}
	s.step(at(0), q(ser(90, "instance", "a")))
	s.step(at(60), q(ser(90, "instance", "a")))
	s.dispatch(at(60))
	if len(s.outbox) != 1 || s.outbox[0].Status != monitorcontract.OutboxDropped || s.alert().NotifyCount != 0 {
		t.Fatalf("precondition: first notification dropped by the dispatcher, outbox = %+v", s.outbox[0])
	}
	out := s.step(at(120), q(ser(90, "instance", "a")))
	if len(out.Transition.Outbox) != 1 || !out.Transition.Outbox[0].Rearm || out.Transition.Outbox[0].Kind != monitorcontract.NotifyKindFiring {
		t.Fatalf("outbox = %+v", out.Transition.Outbox)
	}
	if len(s.outbox) != 1 || s.outbox[0].Status != monitorcontract.OutboxPending {
		t.Fatalf("dropped row must be re-armed, got %+v", s.outbox[0])
	}
	s.dispatch(at(120))
	if s.alert().NotifyCount != 1 {
		t.Fatalf("notify count = %d", s.alert().NotifyCount)
	}
	// 之后不再重复写。
	if out = s.step(at(180), q(ser(90, "instance", "a"))); len(out.Transition.Outbox) != 0 {
		t.Fatalf("outbox = %+v", out.Transition.Outbox)
	}
}

// E5：屏蔽解除的那一轮恒执行重新激活 upsert，即使发件箱里还有一条待派发——core 可能正在按
// 「事件被屏蔽」丢弃它。
func TestAdvanceRearmsOnUnmuteEvenWhileANotificationIsPending(t *testing.T) {
	series := func() []promclient.Series { return q(ser(90, "instance", "a")) }
	s := newSim(t, baseRule(func(r *Rule) { r.For = 0 }))
	s.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 30, 100)}
	s.step(at(0), series())  // 触发：首次通知入箱，core 尚未派发
	s.step(at(60), series()) // 屏蔽生效：事件标记为已屏蔽，那条通知仍待派发
	if !s.alert().Muted || s.outbox[0].Status != monitorcontract.OutboxPending {
		t.Fatalf("precondition: muted alert with a pending row, got %+v / %+v", s.alert(), s.outbox[0])
	}
	// 屏蔽解除的这一轮：引擎读状态时那一行还是待派发。
	in := s.input(at(120), series())
	if st := in.Prev[0]; !st.Muted || !st.PendingNotify {
		t.Fatalf("precondition: state = %+v", st)
	}
	out, err := s.advance(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Transition.Outbox) != 1 {
		t.Fatalf("the unmute round must always upsert the notification, outbox = %+v", out.Transition.Outbox)
	}
	if o := out.Transition.Outbox[0]; !o.Rearm || o.Kind != monitorcontract.NotifyKindFiring || o.Seq != 1 {
		t.Fatalf("outbox = %+v", o)
	}
	// 与此同时 core 读到的还是屏蔽中的事件，把那一行丢弃；引擎的 upsert 排在它之后生效。
	s.dispatch(at(120))
	if s.outbox[0].Status != monitorcontract.OutboxDropped {
		t.Fatalf("precondition: core dropped the row, got %+v", s.outbox[0])
	}
	s.apply(at(120), &out.Transition)
	if len(s.outbox) != 1 || s.outbox[0].Status != monitorcontract.OutboxPending {
		t.Fatalf("the notification was lost: %+v", s.outbox[0])
	}
	s.dispatch(at(121))
	if s.alert().NotifyCount != 1 {
		t.Fatalf("notify count = %d", s.alert().NotifyCount)
	}

	// 已通知过的事件同理：屏蔽解除按一次重复通知，同样带重新激活。
	s.mutes = []*Mute{onceMute("m2", monitorcontract.MuteModeNotify, 150, 250)}
	s.step(at(180), series())
	out = s.step(at(300), series())
	if len(out.Transition.Outbox) != 1 || !out.Transition.Outbox[0].Rearm || out.Transition.Outbox[0].Kind != monitorcontract.NotifyKindRepeat ||
		out.Transition.Outbox[0].Seq != 2 {
		t.Fatalf("outbox = %+v", out.Transition.Outbox)
	}
}

// E6：重复通知行被 core 丢弃（派发失败次数用尽）后通知计数不前进，下一次到期写出的是同一个键——
// 必须重新激活，否则该事件的重复通知永久停止。
func TestAdvanceRearmsADroppedRepeatNotification(t *testing.T) {
	series := func() []promclient.Series { return q(ser(90, "instance", "a")) }
	s := newSim(t, baseRule(func(r *Rule) { r.For, r.RepeatInterval = 0, 5*time.Minute }))
	s.step(at(0), series())
	s.dispatch(at(0))
	out := s.step(at(300), series())
	if got := s.kinds(); !equalStrings(got, []string{"1/1", "2/2"}) || !out.Transition.Outbox[0].Rearm {
		t.Fatalf("outbox = %v, transition = %+v", got, out.Transition.Outbox)
	}
	// core 派发失败次数用尽：丢弃，通知计数停在 1。
	s.outbox[1].Status = monitorcontract.OutboxDropped
	for i, sec := range []int{360, 420} {
		out = s.step(at(sec), series())
		if i == 0 {
			if len(out.Transition.Outbox) != 1 {
				t.Fatalf("a dropped repeat must be written again on the next due round, outbox = %+v", out.Transition.Outbox)
			}
			if o := out.Transition.Outbox[0]; !o.Rearm || o.Kind != monitorcontract.NotifyKindRepeat || o.Seq != 2 {
				t.Fatalf("outbox = %+v", o)
			}
			if len(s.outbox) != 2 || s.outbox[1].Status != monitorcontract.OutboxPending {
				t.Fatalf("the dropped row must be pending again: %+v", s.outbox[1])
			}
			continue
		}
		// 重新激活后是待派发：不再重复写。
		if len(out.Transition.Outbox) != 0 {
			t.Fatalf("outbox = %+v", out.Transition.Outbox)
		}
	}
	s.dispatch(at(420))
	if s.alert().NotifyCount != 2 {
		t.Fatalf("notify count = %d", s.alert().NotifyCount)
	}
	// 之后的重复通知照常继续。
	s.step(at(720), series())
	if got := s.kinds(); !equalStrings(got, []string{"1/1", "2/2", "2/3"}) {
		t.Fatalf("outbox = %v", got)
	}
}

// E4：活跃告警行只在内容变化或到了心跳间隔时写，而不是每轮都写。
func TestAdvanceWritesAlertRowOnlyOnChange(t *testing.T) {
	twoLevels := func(r *Rule) {
		r.For, r.Interval = 0, 15*time.Second
		r.Queries = []monitorcontract.Query{
			{Expr: "cpu > 85", Severity: monitorcontract.SeverityWarning},
			{Expr: "cpu > 95", Severity: monitorcontract.SeverityCritical},
		}
	}
	s := newSim(t, baseRule(twoLevels))
	warn := func(v float64) [][]promclient.Series { return [][]promclient.Series{q(ser(v, "instance", "a")), q()} }
	updates := func(sec int, results [][]promclient.Series) int {
		t.Helper()
		out := s.step(at(sec), results...)
		if out.Transition.FiringCount != 1 || len(out.Transition.StateUpdates) != 1 {
			t.Fatalf("t+%d: the state row is refreshed every round, transition = %+v", sec, out.Transition)
		}
		return len(out.Transition.AlertUpdates)
	}
	s.step(at(0), warn(90)...)
	// 取值、级别、屏蔽都没变，且未到心跳间隔（max(60s, 评估周期)）：不写告警行。
	for _, sec := range []int{15, 30, 45} {
		if n := updates(sec, warn(90)); n != 0 {
			t.Fatalf("t+%d: unchanged round wrote the alert row (%d updates)", sec, n)
		}
	}
	if !s.alert().LastFire.Equal(at(0)) {
		t.Fatalf("last fire = %v", s.alert().LastFire)
	}
	// 心跳：距上次写入满 60s。
	if n := updates(60, warn(90)); n != 1 || !s.alert().LastFire.Equal(at(60)) {
		t.Fatalf("heartbeat: updates = %d, last fire = %v", n, s.alert().LastFire)
	}
	if n := updates(75, warn(90)); n != 0 {
		t.Fatalf("round after the heartbeat wrote again")
	}
	// 取值变化。
	if n := updates(90, warn(91)); n != 1 || s.alert().Value != 91 {
		t.Fatalf("value change: updates = %d, alert = %+v", n, s.alert())
	}
	// 级别变化（取值不变）。
	if n := updates(105, [][]promclient.Series{q(ser(91, "instance", "a")), q(ser(91, "instance", "a"))}); n != 1 || s.alert().Severity != monitorcontract.SeverityCritical {
		t.Fatalf("severity change: updates = %d, alert = %+v", n, s.alert())
	}
	if n := updates(110, [][]promclient.Series{q(ser(91, "instance", "a")), q(ser(91, "instance", "a"))}); n != 0 {
		t.Fatalf("unchanged round wrote the alert row")
	}
	// 屏蔽标记变化，然后稳定，然后解除。
	s.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 115, 135)}
	crit := [][]promclient.Series{q(ser(91, "instance", "a")), q(ser(91, "instance", "a"))}
	if n := updates(120, crit); n != 1 || !s.alert().Muted || s.alert().MuteID != "m1" {
		t.Fatalf("mute start: updates = %d, alert = %+v", n, s.alert())
	}
	if n := updates(125, crit); n != 0 {
		t.Fatalf("still muted by the same rule: wrote the alert row")
	}
	if n := updates(140, crit); n != 1 || s.alert().Muted {
		t.Fatalf("mute end: updates = %d, alert = %+v", n, s.alert())
	}
	// NaN 不等于自身：取值恒为 NaN 的告警不得因此每轮都写。
	nan := [][]promclient.Series{q(ser(math.NaN(), "instance", "a")), q(ser(math.NaN(), "instance", "a"))}
	if n := updates(145, nan); n != 1 {
		t.Fatalf("value → NaN: updates = %d", n)
	}
	if n := updates(150, nan); n != 0 {
		t.Fatalf("NaN → NaN wrote the alert row")
	}
	// 长周期规则：心跳间隔 = 评估周期，每轮都到期（last_fire_time 不落后于评估）。
	slow := newSim(t, baseRule(func(r *Rule) { r.For, r.Interval = 0, 5*time.Minute }))
	slow.step(at(0), q(ser(90, "instance", "a")))
	if out := slow.step(at(300), q(ser(90, "instance", "a"))); len(out.Transition.AlertUpdates) != 1 {
		t.Fatalf("slow rule: updates = %d", len(out.Transition.AlertUpdates))
	}
}

// E1：Advance 不执行模板。附加信息没渲染时它报告缺哪些样本、不产生任何变更；渲染结果由调用方传入。
func TestAdvanceNeverRendersTemplates(t *testing.T) {
	rule := baseRule(func(r *Rule) {
		r.For = 0
		r.Annotations = map[string]string{"summary": "{{ $labels.instance }}"}
	})
	results := func() [][]promclient.Series {
		return [][]promclient.Series{q(ser(90, "instance", "a"), ser(91, "instance", "b"))}
	}
	ids := 0
	in := &RoundInput{
		Rule: rule, DatasourceID: testDSID, Samples: merge(rule, testDSID, results()), Now: at(0),
		NewID: func() (string, error) { ids++; return fmt.Sprintf("x-%d", ids), nil },
	}
	out, err := Advance(in)
	var missing *MissingAnnotationsError
	if out != nil || !errors.As(err, &missing) || len(missing.Samples) != 2 {
		t.Fatalf("out = %+v, err = %v", out, err)
	}
	// 调用方传什么就用什么：Advance 不看模板源文本。
	in.Annotations = map[string]map[string]string{}
	for _, s := range missing.Samples {
		in.Annotations[s.Fingerprint] = map[string]string{"summary": "rendered by the caller for " + s.Labels["instance"]}
	}
	if out, err = Advance(in); err != nil || len(out.Transition.AlertInserts) != 2 {
		t.Fatalf("out = %+v, err = %v", out, err)
	}
	for _, a := range out.Transition.AlertInserts {
		if want := "rendered by the caller for " + a.Labels["instance"]; a.Annotations["summary"] != want {
			t.Fatalf("annotations = %v, want %q", a.Annotations, want)
		}
	}
	// 没有新事件的轮次不需要任何渲染结果。
	s := newSim(t, rule)
	s.step(at(0), results()...)
	s.step(at(60), results()...)
	s.step(at(120), nil)
	if s.renders != 1 {
		t.Fatalf("annotations were rendered %d times, want once (only the round that created alerts)", s.renders)
	}
	// 被「屏蔽事件」命中的样本不建事件：同样不需要渲染。
	muted := newSim(t, rule)
	muted.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeEvent, 0, 100)}
	if muted.step(at(0), results()...); muted.renders != 0 {
		t.Fatalf("a muted event must not be rendered")
	}
}

func TestAdvanceTimestampsAndAlertFields(t *testing.T) {
	rule := baseRule(func(r *Rule) {
		r.Recover = 120 * time.Second
		r.Labels = map[string]string{"team": "ops"}
		r.Annotations = map[string]string{"summary": "{{ $labels.instance }} is at {{ $value }}", "bad": "{{ .Nope }}"}
		r.GroupKeyLabels = []string{"instance"}
	})
	s := newSim(t, rule)
	s.step(at(0), q(ser(90, "instance", "a")))
	s.step(at(60), q(ser(91, "instance", "a")))
	st, a := s.only(), s.alert()
	if !st.ActiveSince.Equal(at(0)) || !st.LastSeen.Equal(at(60)) || !st.RecoverSince.IsZero() || st.AlertID != a.ID {
		t.Fatalf("state after firing = %+v", st)
	}
	wantLabels := map[string]string{"instance": "a", "team": "ops"}
	if !reflect.DeepEqual(a.Labels, wantLabels) {
		t.Fatalf("alert labels = %v, want %v", a.Labels, wantLabels)
	}
	if a.Fingerprint != monitorcontract.Fingerprint(testRuleID, testDSID, wantLabels, 0) {
		t.Fatalf("alert fingerprint mismatch")
	}
	if a.Annotations["summary"] != "a is at 91" || a.Annotations["bad"] != templateError {
		t.Fatalf("annotations = %v", a.Annotations)
	}
	if a.GroupKey != monitorcontract.GroupKey(testRuleID, wantLabels, []string{"instance"}) {
		t.Fatalf("group key = %q", a.GroupKey)
	}
	if a.Value != 91 || a.Severity != monitorcontract.SeverityWarning || !a.FirstFire.Equal(at(60)) {
		t.Fatalf("alert = %+v", a)
	}
	s.dispatch(at(60))

	s.step(at(120), q(ser(95, "instance", "a")))
	if a.Value != 95 || !a.LastFire.Equal(at(120)) || !a.FirstFire.Equal(at(60)) {
		t.Fatalf("alert after refresh = %+v", a)
	}
	s.step(at(180), nil)
	if st = s.only(); st.State != monitorcontract.StateRecovering || !st.RecoverSince.Equal(at(180)) || !st.LastSeen.Equal(at(120)) || st.Value != 95 {
		t.Fatalf("state while recovering = %+v", st)
	}
	fp := st.Fingerprint
	s.recovery = map[string]float64{fp: 12.5}
	s.step(at(300), nil)
	if a.Status != monitorcontract.AlertResolved || !a.ResolveTime.Equal(at(300)) || !a.HasRecovery || a.RecoveryValue != 12.5 {
		t.Fatalf("resolved alert = %+v", a)
	}
}

func TestAdvanceLevelInhibition(t *testing.T) {
	twoLevels := func(r *Rule) {
		r.For = 0
		r.Queries = []monitorcontract.Query{
			{Expr: "cpu > 85", Severity: monitorcontract.SeverityWarning},
			{Expr: "cpu > 95", Severity: monitorcontract.SeverityCritical},
		}
	}
	t.Run("on: one event per series, highest severity wins and follows the data", func(t *testing.T) {
		s := newSim(t, baseRule(twoLevels))
		out := s.step(at(0), q(ser(96, "instance", "a"), ser(90, "instance", "b")), q(ser(96, "instance", "a")))
		if len(s.alerts) != 2 || out.Anomalies != 2 {
			t.Fatalf("alerts = %d, anomalies = %d", len(s.alerts), out.Anomalies)
		}
		if got := steps(out); !equalStrings(got, []string{"fired", "fired", "inhibited"}) {
			t.Fatalf("steps = %v", got)
		}
		var a *simAlert
		for _, x := range s.alerts {
			if x.Labels["instance"] == "a" {
				a = x
			}
		}
		if a.Severity != monitorcontract.SeverityCritical || a.QueryIndex != 1 {
			t.Fatalf("alert a = %+v", a)
		}
		// 降到只命中警告档：同一事件，级别随之更新，不新建事件、不新发通知。
		s.step(at(60), q(ser(90, "instance", "a"), ser(90, "instance", "b")), q())
		if len(s.alerts) != 2 || a.Severity != monitorcontract.SeverityWarning || a.QueryIndex != 0 || a.Value != 90 {
			t.Fatalf("after downgrade: alerts = %d, a = %+v", len(s.alerts), a)
		}
		if got := s.kinds(); !equalStrings(got, []string{"1/1", "1/1"}) {
			t.Fatalf("outbox = %v", got)
		}
	})
	t.Run("on: order of queries does not matter", func(t *testing.T) {
		s := newSim(t, baseRule(twoLevels, func(r *Rule) { r.Queries[0], r.Queries[1] = r.Queries[1], r.Queries[0] }))
		s.step(at(0), q(ser(96, "instance", "a")), q(ser(96, "instance", "a")))
		if a := s.alert(); a.Severity != monitorcontract.SeverityCritical || a.QueryIndex != 0 {
			t.Fatalf("alert = %+v", a)
		}
	})
	t.Run("off: each severity is its own event", func(t *testing.T) {
		s := newSim(t, baseRule(twoLevels, func(r *Rule) { r.LevelInhibit = false }))
		out := s.step(at(0), q(ser(96, "instance", "a")), q(ser(96, "instance", "a")))
		if len(s.alerts) != 2 || out.Anomalies != 2 || out.Transition.FiringCount != 2 {
			t.Fatalf("alerts = %d, outcome = %+v", len(s.alerts), out)
		}
		seen := map[int32]string{}
		for _, a := range s.alerts {
			seen[a.Severity] = a.Fingerprint
			if a.Fingerprint != monitorcontract.Fingerprint(testRuleID, testDSID, a.Labels, a.Severity) {
				t.Fatalf("fingerprint must include the severity")
			}
		}
		if len(seen) != 2 || seen[2] == seen[3] {
			t.Fatalf("severities = %v", seen)
		}
		// 严重档消失：只恢复严重档的事件。
		s.step(at(60), q(ser(90, "instance", "a")), q())
		var firing, resolved int
		for _, a := range s.alerts {
			switch {
			case a.Status == monitorcontract.AlertFiring && a.Severity == monitorcontract.SeverityWarning:
				firing++
			case a.Status == monitorcontract.AlertResolved && a.Severity == monitorcontract.SeverityCritical:
				resolved++
			}
		}
		if firing != 1 || resolved != 1 {
			t.Fatalf("firing = %d, resolved = %d", firing, resolved)
		}
	})
}

func TestAdvanceMutePrecedence(t *testing.T) {
	rule := baseRule(func(r *Rule) { r.For = 0 })
	tests := []struct {
		name     string
		mutes    []*Mute
		wantMode int32
		wantID   string
	}{
		{"no mute", nil, 0, ""},
		{"notify only", []*Mute{onceMute("m2", monitorcontract.MuteModeNotify, 0, 100)}, monitorcontract.MuteModeNotify, "m2"},
		{
			"event mute beats notify mute regardless of order",
			[]*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 0, 100), onceMute("m9", monitorcontract.MuteModeEvent, 0, 100)},
			monitorcontract.MuteModeEvent, "m9",
		},
		{
			"same mode: lowest id wins",
			[]*Mute{onceMute("m5", monitorcontract.MuteModeNotify, 0, 100), onceMute("m3", monitorcontract.MuteModeNotify, 0, 100)},
			monitorcontract.MuteModeNotify, "m3",
		},
		{
			"non-matching stronger mute is ignored",
			[]*Mute{
				onceMute("m1", monitorcontract.MuteModeEvent, 0, 100, func(m *Mute) {
					m.Matchers = []monitorcontract.Matcher{{Label: "instance", Op: monitorcontract.OpEqual, Value: "zzz"}}
				}),
				onceMute("m2", monitorcontract.MuteModeNotify, 0, 100),
			},
			monitorcontract.MuteModeNotify, "m2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSim(t, rule)
			s.mutes = tt.mutes
			out := s.step(at(0), q(ser(90, "instance", "a")))
			switch tt.wantMode {
			case 0:
				if a := s.alert(); a.Muted || a.MuteID != "" || len(s.outbox) != 1 {
					t.Fatalf("alert = %+v, outbox = %v", a, s.kinds())
				}
			case monitorcontract.MuteModeNotify:
				if a := s.alert(); !a.Muted || a.MuteID != tt.wantID || len(s.outbox) != 0 {
					t.Fatalf("alert = %+v, outbox = %v", a, s.kinds())
				}
			case monitorcontract.MuteModeEvent:
				if len(s.alerts) != 0 || s.only().State != monitorcontract.StatePending || out.Points[0].MuteID != tt.wantID {
					t.Fatalf("alerts = %d, points = %+v", len(s.alerts), out.Points)
				}
			}
		})
	}
}

func TestAdvanceMixedSeries(t *testing.T) {
	s := newSim(t, baseRule())
	s.step(at(0), q(ser(90, "instance", "a"), ser(90, "instance", "b")))
	s.step(at(60), q(ser(90, "instance", "a"), ser(90, "instance", "b")))
	s.dispatch(at(60))
	// a 继续、b 消失（恢复）、c 新出现。
	out := s.step(at(120), q(ser(92, "instance", "a"), ser(88, "instance", "c")))
	if out.Anomalies != 2 || out.Recovered != 1 || out.Transition.FiringCount != 1 {
		t.Fatalf("outcome = anomalies %d recovered %d firing %d", out.Anomalies, out.Recovered, out.Transition.FiringCount)
	}
	if got := steps(out); !equalStrings(got, []string{"firing", "pending", "resolved"}) {
		t.Fatalf("steps = %v", got)
	}
	if len(s.states) != 2 || len(s.alerts) != 2 {
		t.Fatalf("states = %d, alerts = %d", len(s.states), len(s.alerts))
	}
	tr := out.Transition
	if len(tr.StateInserts) != 1 || len(tr.StateUpdates) != 1 || len(tr.StateDeletes) != 1 ||
		len(tr.AlertUpdates) != 1 || len(tr.AlertResolves) != 1 || len(tr.AlertInserts) != 0 || len(tr.Outbox) != 1 {
		t.Fatalf("transition = %+v", tr)
	}
	if tr.Outbox[0].Kind != monitorcontract.NotifyKindResolved || tr.Outbox[0].Seq != 2 {
		t.Fatalf("outbox = %+v", tr.Outbox)
	}
}

func TestAdvanceIsPure(t *testing.T) {
	rule := baseRule(func(r *Rule) { r.For = 0 })
	s := newSim(t, rule)
	s.step(at(0), q(ser(90, "instance", "a"), ser(90, "instance", "b")))
	prev := s.prev()
	prevCopy := append([]SeriesState(nil), prev...)
	ruleCopy := *rule
	run := func() *Outcome {
		n := 0
		samples := merge(rule, testDSID, [][]promclient.Series{q(ser(93, "instance", "a"), ser(70, "instance", "c"))})
		out, err := Advance(&RoundInput{
			Rule: rule, DatasourceID: testDSID, Prev: prev, Samples: samples, Now: at(60),
			NewID: func() (string, error) { n++; return fmt.Sprintf("x-%d", n), nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	first, second := run(), run()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same input must give the same output:\n%+v\n%+v", first, second)
	}
	if !reflect.DeepEqual(prev, prevCopy) || !reflect.DeepEqual(*rule, ruleCopy) {
		t.Fatalf("Advance must not modify its input")
	}
}

func TestAdvancePropagatesIDError(t *testing.T) {
	boom := errors.New("no entropy")
	rule := baseRule(func(r *Rule) { r.For = 0 })
	_, err := Advance(&RoundInput{
		Rule: rule, DatasourceID: testDSID, Samples: merge(rule, testDSID, [][]promclient.Series{q(ser(1, "instance", "a"))}),
		Now: at(0), NewID: func() (string, error) { return "", boom },
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestAdvanceDetailIsCappedAndPrefersTransitions(t *testing.T) {
	rule := baseRule()
	s := newSim(t, rule)
	series := func(from, to int) []promclient.Series {
		var out []promclient.Series
		for i := from; i < to; i++ {
			out = append(out, ser(90, "instance", fmt.Sprintf("host-%03d", i)))
		}
		return out
	}
	s.step(at(0), series(0, 10))
	// 10 个满持续时长触发（状态变化），另有 200 个新出现（维持待触发）。
	out := s.step(at(60), series(0, 210))
	if len(out.Points) != monitorcontract.MaxEvalPoints || !out.Truncated || out.Anomalies != 210 {
		t.Fatalf("points = %d, truncated = %v, anomalies = %d", len(out.Points), out.Truncated, out.Anomalies)
	}
	fired := 0
	for _, p := range out.Points {
		if p.Step == monitorcontract.StepFired {
			fired++
		}
	}
	if fired != 10 {
		t.Fatalf("all 10 transitions must be kept in the detail, got %d", fired)
	}
	small := newSim(t, rule)
	if out := small.step(at(0), series(0, 3)); out.Truncated || len(out.Points) != 3 {
		t.Fatalf("small round: points = %d, truncated = %v", len(out.Points), out.Truncated)
	}
}

func TestMergeSamples(t *testing.T) {
	rule := baseRule(func(r *Rule) { r.Labels = map[string]string{"env": "prod", "team": "ops"} })
	empty := promclient.Series{Labels: map[string]string{"instance": "nopoints"}}
	set := merge(rule, testDSID, [][]promclient.Series{q(
		ser(1, "instance", "a", "env", "dev"),
		ser(2, "instance", "a", "env", "staging"), // 合并附加标签后与上一条同指纹：保留先出现的
		ser(3, "instance", "b\x00x"),
		empty,
		promclient.Series{Points: []promclient.Point{{Value: 7}}}, // 无标签（scalar 折算的序列）
	)})
	if set.Series != 5 || len(set.Kept) != 3 || len(set.Inhibited) != 0 {
		t.Fatalf("series = %d, kept = %d, inhibited = %d", set.Series, len(set.Kept), len(set.Inhibited))
	}
	a := set.Kept[0]
	if !reflect.DeepEqual(a.Labels, map[string]string{"instance": "a", "env": "prod", "team": "ops"}) || a.Value != 1 {
		t.Fatalf("first sample = %+v", a)
	}
	if got := set.Kept[1].Labels["instance"]; got != "bx" {
		t.Fatalf("NUL must be stripped from label values, got %q", got)
	}
	if !reflect.DeepEqual(set.Kept[2].Labels, map[string]string{"env": "prod", "team": "ops"}) || set.Kept[2].Value != 7 {
		t.Fatalf("label-less sample = %+v", set.Kept[2])
	}
	if !set.has(a.Fingerprint) || set.has("nope") {
		t.Fatalf("has() mismatch")
	}
}

func TestRoundStatus(t *testing.T) {
	for _, tt := range []struct {
		firing, series, want int32
	}{
		{0, 0, monitorcontract.EvalNoData},
		{0, 3, monitorcontract.EvalOK},
		{2, 3, monitorcontract.EvalFiring},
		{1, 0, monitorcontract.EvalFiring}, // 留观中的告警仍算告警中
	} {
		if got := roundStatus(tt.firing, tt.series); got != tt.want {
			t.Errorf("roundStatus(%d, %d) = %d, want %d", tt.firing, tt.series, got, tt.want)
		}
	}
}

func TestResolving(t *testing.T) {
	rule := baseRule(func(r *Rule) { r.Recover = 60 * time.Second })
	prev := []SeriesState{
		{Fingerprint: "pending", State: monitorcontract.StatePending},
		{Fingerprint: "firing", State: monitorcontract.StateFiring, AlertID: "a1"},
		{Fingerprint: "recovering-young", State: monitorcontract.StateRecovering, AlertID: "a2", RecoverSince: at(90)},
		{Fingerprint: "recovering-due", State: monitorcontract.StateRecovering, AlertID: "a3", RecoverSince: at(30)},
		{Fingerprint: "orphan", State: monitorcontract.StateRecovering, RecoverSince: at(0)},
		{Fingerprint: "present", State: monitorcontract.StateRecovering, AlertID: "a4", RecoverSince: at(0)},
	}
	in := &RoundInput{Rule: rule, Samples: &SampleSet{index: map[string]int32{"present": 0}}, Now: at(100), LastStatus: monitorcontract.EvalFiring}
	due := resolving(in, prev)
	if len(due) != 1 || due[0].Fingerprint != "recovering-due" {
		t.Fatalf("due = %+v", due)
	}
	// 结果不完整、或上一轮没有完成评估：本轮不会有任何序列判恢复（不发恢复取值查询）。
	in.Incomplete = true
	if due = resolving(in, prev); len(due) != 0 {
		t.Fatalf("an incomplete round resolves nothing, got %+v", due)
	}
	in.Incomplete, in.LastStatus = false, monitorcontract.EvalError
	if due = resolving(in, prev); len(due) != 0 {
		t.Fatalf("the observation period restarts after a query error, got %+v", due)
	}
	in.LastStatus = monitorcontract.EvalFiring
	rule.Recover = 0
	if due = resolving(in, prev); len(due) != 3 {
		t.Fatalf("with no observation period every absent alert resolves, got %d", len(due))
	}
}

// benchmarkAdvance 状态机单步的基准：n 条序列，mode 决定它们处于哪种转移。
func benchmarkAdvance(b *testing.B, n int, mode string) {
	rule := baseRule(func(r *Rule) {
		r.Labels = map[string]string{"team": "ops"}
		r.Annotations = map[string]string{"summary": "{{ $labels.instance }} cpu is {{ $value | printf \"%.1f\" }}"}
		if mode == "fire" {
			r.For = 0
		}
	})
	// delta 加在每个样本值上："change" 模式下每条告警的取值都变了（每条都要写告警行），
	// "steady" 模式下取值不变（都不写）。
	delta := 0.0
	build := func() [][]promclient.Series {
		out := make([]promclient.Series, n)
		for i := range out {
			out[i] = promclient.Series{
				Labels: map[string]string{"__name__": "cpu", "instance": fmt.Sprintf("10.0.%d.%d:9100", i/250, i%250), "job": "node", "dc": "bj"},
				Points: []promclient.Point{{Value: float64(i) + delta}},
			}
		}
		return [][]promclient.Series{out}
	}
	id := 0
	newID := func() (string, error) { id++; return "018f3e1a-0000-7000-8000-000000000000", nil }
	var prev []SeriesState
	now := at(0)
	if mode == "steady" || mode == "change" || mode == "resolve" {
		// 先让全部序列进入告警中，作为上一轮状态。
		s := &sim{rule: baseRule(func(r *Rule) { r.For = 0 }), states: map[string]SeriesState{}, alerts: map[string]*simAlert{}}
		samples := merge(rule, testDSID, build())
		out, err := Advance(&RoundInput{Rule: s.rule, DatasourceID: testDSID, Samples: samples, Now: now, NewID: s.newID})
		if err != nil {
			b.Fatal(err)
		}
		s.t = nil
		for _, a := range out.Transition.AlertInserts {
			s.alerts[a.ID] = &simAlert{AlertInsert: a, Status: monitorcontract.AlertFiring, NotifyCount: 1, LastNotifyTime: now, LastFire: now}
		}
		for _, in := range out.Transition.StateInserts {
			s.states[in.Fingerprint] = SeriesState{
				Fingerprint: in.Fingerprint, State: in.State, Severity: in.Severity, Value: in.Value,
				Labels: []byte(`{"instance":"10.0.0.1:9100","job":"node","dc":"bj","team":"ops"}`), ActiveSince: now, LastSeen: now, AlertID: in.AlertID,
			}
		}
		prev = s.prev()
	}
	if mode == "change" {
		delta = 1
	}
	// 样本集预先合并好：基准只量状态机这一步（合并另有基准）。
	sets := make([]*SampleSet, 0, 64)
	for range 64 {
		if mode == "resolve" {
			sets = append(sets, merge(rule, testDSID, [][]promclient.Series{nil}))
		} else {
			sets = append(sets, merge(rule, testDSID, build()))
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// 与评估器同样的调用方式：有新事件时在 Advance 之外渲染附加信息后重试（"fire" 模式量的是两者之和）。
		in := &RoundInput{
			Rule: rule, DatasourceID: testDSID, Prev: prev, Samples: sets[i%len(sets)], Now: now.Add(time.Minute), NewID: newID,
			LastStatus: monitorcontract.EvalFiring, LastEval: now,
		}
		out, err := Advance(in)
		var missing *MissingAnnotationsError
		if errors.As(err, &missing) {
			in.Annotations = renderAnnotations(rule.Annotations, missing.Samples)
			out, err = Advance(in)
		}
		if err != nil || int(out.Anomalies+out.Recovered) != n {
			b.Fatalf("unexpected outcome: %v %+v", err, out)
		}
	}
}

func BenchmarkAdvance2000NewPending(b *testing.B)   { benchmarkAdvance(b, 2000, "pending") }
func BenchmarkAdvance2000FireWithTpl(b *testing.B)  { benchmarkAdvance(b, 2000, "fire") }
func BenchmarkAdvance2000SteadyFiring(b *testing.B) { benchmarkAdvance(b, 2000, "steady") }
func BenchmarkAdvance2000SteadyChange(b *testing.B) { benchmarkAdvance(b, 2000, "change") }
func BenchmarkAdvance2000ResolveAll(b *testing.B)   { benchmarkAdvance(b, 2000, "resolve") }

func BenchmarkMergeSamples2000(b *testing.B) {
	rule := baseRule(func(r *Rule) { r.Labels = map[string]string{"team": "ops"} })
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		series := make([]promclient.Series, 2000)
		for i := range series {
			series[i] = promclient.Series{
				Labels: map[string]string{"__name__": "cpu", "instance": fmt.Sprintf("10.0.%d.%d:9100", i/250, i%250), "job": "node", "dc": "bj"},
				Points: []promclient.Point{{Value: float64(i)}},
			}
		}
		b.StartTimer()
		if set := merge(rule, testDSID, [][]promclient.Series{series}); len(set.Kept) != 2000 {
			b.Fatal("unexpected sample count")
		}
	}
}
