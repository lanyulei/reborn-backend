package monitorpipeline

import (
	"slices"
	"testing"
	"time"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

var t0 = time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)

func at(minutes int) time.Time { return t0.Add(time.Duration(minutes) * time.Minute) }

// policy 三级：10 分钟未认领（每 15 分钟、至多 3 次）→ 30 分钟未恢复（每 60 分钟、至多 2 次）→
// 60 分钟未认领（只通知一次）。
func policy(mode int32) *EscalationPolicy {
	return &EscalationPolicy{Mode: mode, Levels: []mc.EscalationLevel{
		{Trigger: mc.EscalationTriggerUnacked, AfterM: 10, RepeatIntervalM: 15, MaxNotify: 3},
		{Trigger: mc.EscalationTriggerUnresolved, AfterM: 30, RepeatIntervalM: 60, MaxNotify: 2},
		{Trigger: mc.EscalationTriggerUnacked, AfterM: 60},
	}}
}

func TestEscalate(t *testing.T) {
	t.Parallel()
	sent := func(n int32, last int) EscalationLevelState {
		return EscalationLevelState{NotifyCount: n, LastNotifyTime: at(last)}
	}
	for _, tc := range []struct {
		name       string
		in         EscalationInput
		want       []EscalationNotify
		level      int32
		ruleRepeat bool
	}{
		{"no policy", EscalationInput{Now: at(500)}, nil, 0, true},
		{"no policy, acked: rule repeats stop", EscalationInput{Now: at(500), Acked: true}, nil, 0, false},
		{"empty policy", EscalationInput{Policy: &EscalationPolicy{Mode: 1}, Now: at(500)}, nil, 0, true},
		{"before the first level", EscalationInput{Policy: policy(1), Now: at(9)}, nil, 0, true},
		{"first level becomes due exactly at after_m", EscalationInput{Policy: policy(1), Now: at(10)}, []EscalationNotify{{Level: 1, Seq: 1}}, 1, true},
		{
			"first notification in flight: nothing is added",
			EscalationInput{Policy: policy(1), Now: at(12), Levels: []EscalationLevelState{{Pending: true}}},
			nil, 1, true,
		},
		{
			"repeat interval not reached",
			EscalationInput{Policy: policy(1), Now: at(24), Levels: []EscalationLevelState{sent(1, 10)}},
			nil, 1, true,
		},
		{
			"repeat due",
			EscalationInput{Policy: policy(1), Now: at(25), Levels: []EscalationLevelState{sent(1, 10)}},
			[]EscalationNotify{{Level: 1, Seq: 2}},
			1, true,
		},
		{
			"stack: a new level and the repeat of an earlier one in the same round",
			EscalationInput{Policy: policy(1), Now: at(40), Levels: []EscalationLevelState{sent(2, 25)}},
			[]EscalationNotify{{Level: 1, Seq: 3}, {Level: 2, Seq: 1}},
			2, true,
		},
		{
			"level cap reached",
			EscalationInput{Policy: policy(1), Now: at(200), Levels: []EscalationLevelState{sent(3, 40), sent(2, 90), sent(1, 60)}},
			nil, 3, true,
		},
		{
			"a level without repeat interval notifies once",
			EscalationInput{Policy: policy(1), Now: at(600), Levels: []EscalationLevelState{sent(3, 40), sent(2, 90), sent(1, 60)}},
			nil, 3, true,
		},
		{
			"takeover: only the highest level notifies, rule repeats stop",
			EscalationInput{Policy: policy(2), Now: at(40), Levels: []EscalationLevelState{sent(2, 25)}},
			[]EscalationNotify{{Level: 2, Seq: 1}},
			2, false,
		},
		{
			"takeover: before any level the rule still repeats",
			EscalationInput{Policy: policy(2), Now: at(5)},
			nil, 0, true,
		},
		{
			"takeover: first level",
			EscalationInput{Policy: policy(2), Now: at(10)},
			[]EscalationNotify{{Level: 1, Seq: 1}},
			1, false,
		},
		{
			"takeover: lower level repeats are not resumed while the top one waits",
			EscalationInput{Policy: policy(2), Now: at(45), Levels: []EscalationLevelState{sent(1, 10), sent(1, 30)}},
			nil, 2, false,
		},
		{
			"acked: unacked levels stop, unresolved level still escalates",
			EscalationInput{Policy: policy(1), Now: at(40), Acked: true, Levels: []EscalationLevelState{sent(2, 25)}},
			[]EscalationNotify{{Level: 2, Seq: 1}},
			2, false,
		},
		{
			"acked before any level: only the unresolved level ever fires",
			EscalationInput{Policy: policy(1), Now: at(500), Acked: true},
			[]EscalationNotify{{Level: 2, Seq: 1}},
			2, false,
		},
		{
			"acked: unresolved level keeps repeating",
			EscalationInput{Policy: policy(1), Now: at(90), Acked: true, Levels: []EscalationLevelState{sent(3, 40), sent(1, 30)}},
			[]EscalationNotify{{Level: 2, Seq: 2}},
			2, false,
		},
		{
			"acked, takeover: the reached unacked top level goes quiet and nothing below resumes",
			EscalationInput{Policy: policy(2), Now: at(200), Acked: true, Levels: []EscalationLevelState{sent(1, 10), sent(1, 30), sent(1, 60)}},
			nil, 3, false,
		},
		{
			"suppressed: nothing is due, level reflects what was already sent",
			EscalationInput{Policy: policy(1), Now: at(500), Suppressed: true, Levels: []EscalationLevelState{sent(1, 10)}},
			nil, 1, true,
		},
		{"suppressed from the start", EscalationInput{Policy: policy(1), Now: at(500), Suppressed: true}, nil, 0, true},
		{
			"below the minimum severity",
			EscalationInput{Policy: &EscalationPolicy{Mode: 1, MinSeverity: 3, Levels: policy(1).Levels}, Now: at(500), Severity: 2},
			nil, 0, true,
		},
		{
			"at the minimum severity",
			EscalationInput{Policy: &EscalationPolicy{Mode: 1, MinSeverity: 3, Levels: policy(1).Levels}, Now: at(10), Severity: 3},
			[]EscalationNotify{{Level: 1, Seq: 1}},
			1, true,
		},
		{
			"dropped first notification is re-armed after the level's interval",
			EscalationInput{Policy: policy(1), Now: at(27), Levels: []EscalationLevelState{{DroppedAt: at(12)}}},
			[]EscalationNotify{{Level: 1, Seq: 1, Rearm: true}},
			1, true,
		},
		{
			"dropped notification waits for the interval",
			EscalationInput{Policy: policy(1), Now: at(26), Levels: []EscalationLevelState{{DroppedAt: at(12)}}},
			nil, 1, true,
		},
		{
			"dropped repeat is re-armed with the same sequence",
			EscalationInput{Policy: policy(1), Now: at(45), Levels: []EscalationLevelState{{NotifyCount: 1, LastNotifyTime: at(10), DroppedAt: at(26)}}},
			[]EscalationNotify{{Level: 1, Seq: 2, Rearm: true}, {Level: 2, Seq: 1}},
			2, true,
		},
		{
			"dropped notification of a level without interval retries after ten minutes",
			EscalationInput{Policy: policy(1), Now: at(75), Levels: []EscalationLevelState{sent(3, 40), sent(1, 30), {DroppedAt: at(65)}}},
			[]EscalationNotify{{Level: 3, Seq: 1, Rearm: true}},
			3, true,
		},
		{
			"dropped notification is not retried beyond the cap",
			EscalationInput{Policy: policy(1), Now: at(500), Levels: []EscalationLevelState{{NotifyCount: 3, LastNotifyTime: at(40), DroppedAt: at(41)}, sent(2, 90), sent(1, 60)}},
			nil, 3, true,
		},
		{
			"progress rows beyond the policy are ignored",
			EscalationInput{
				Policy: &EscalationPolicy{Mode: 1, Levels: policy(1).Levels[:1]}, Now: at(500),
				Levels: []EscalationLevelState{sent(3, 40), sent(5, 90), {Pending: true}},
			},
			nil, 1, true,
		},
	} {
		tc.in.FirstFire = t0
		if tc.in.Severity == 0 {
			tc.in.Severity = mc.SeverityCritical
		}
		got := Escalate(tc.in)
		if !slices.Equal(got.Due, tc.want) || got.Level != tc.level || got.RuleRepeat != tc.ruleRepeat {
			t.Errorf("%s: Escalate() = %+v; want due %+v, level %d, rule repeat %v", tc.name, got, tc.want, tc.level, tc.ruleRepeat)
		}
		// 幂等：同样的输入再算一次结果相同。
		if again := Escalate(tc.in); !slices.Equal(again.Due, got.Due) || again.Level != got.Level {
			t.Errorf("%s: Escalate is not deterministic", tc.name)
		}
		for _, d := range got.Due {
			if mc.EscalationOutboxSeq(d.Level, d.Seq) == 0 {
				t.Errorf("%s: due notification %+v cannot be encoded as an outbox seq", tc.name, d)
			}
		}
	}
}

// 一条始终无人处理的告警按分钟推演：各级的通知时刻与次数符合配置。
func TestEscalateTimeline(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		mode int32
		want map[int32][]int // 级 → 通知时刻（分钟）
	}{
		{mc.EscalationModeStack, map[int32][]int{1: {10, 25, 40}, 2: {30, 90}, 3: {60}}},
		// 接替：升到第 2 级后第 1 级不再提醒；升到第 3 级后第 2 级的重复提醒也停止。
		{mc.EscalationModeTakeover, map[int32][]int{1: {10, 25}, 2: {30}, 3: {60}}},
	} {
		p := policy(tc.mode)
		states := make([]EscalationLevelState, len(p.Levels))
		got := map[int32][]int{}
		for m := range 24 * 60 {
			res := Escalate(EscalationInput{Policy: p, Now: at(m), FirstFire: t0, Severity: 4, Levels: states})
			for _, d := range res.Due {
				got[d.Level] = append(got[d.Level], m)
				states[d.Level-1] = EscalationLevelState{NotifyCount: d.Seq, LastNotifyTime: at(m)}
			}
		}
		for level, want := range tc.want {
			if !slices.Equal(got[level], want) {
				t.Errorf("mode %d level %d notified at %v, want %v", tc.mode, level, got[level], want)
			}
		}
	}
}

func TestEstimateVolume(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		policy        *EscalationPolicy
		repeatM, maxN int32
		wantRule      int32
		wantLevels    []int32
	}{
		{"no policy, no repeat", nil, 0, 0, 1, []int32{}},
		{"no policy, hourly repeat", nil, 60, 0, 24, []int32{}},
		{"no policy, repeat capped", nil, 30, 5, 5, []int32{}},
		{"no policy, repeat longer than a day", nil, 1440, 0, 1, []int32{}},
		{"stack", policy(1), 30, 5, 5, []int32{3, 2, 1}},
		{"stack, policy only", policy(1), 0, 0, 1, []int32{3, 2, 1}},
		// 接替：规则在第 10 分钟升级前只发了首次通知（30 分钟的重复还没到）。
		{"takeover stops the rule and lower levels", policy(2), 30, 5, 1, []int32{2, 1, 1}},
		{"takeover, rule repeats before the first level", policy(2), 5, 0, 2, []int32{2, 1, 1}},
		{
			"uncapped level repeating every 5 minutes",
			&EscalationPolicy{Mode: 1, Levels: []mc.EscalationLevel{{Trigger: 1, AfterM: 5, RepeatIntervalM: 5}}},
			0, 0, 1,
			[]int32{287}, // 第 5 分钟到第 1435 分钟
		},
		{
			"level beyond the day never fires",
			&EscalationPolicy{Mode: 1, Levels: []mc.EscalationLevel{{Trigger: 2, AfterM: 60}, {Trigger: 2, AfterM: 2000}}},
			0, 0, 1,
			[]int32{1, 0},
		},
		{
			"minimum severity does not hide the estimate",
			&EscalationPolicy{Mode: 1, MinSeverity: 4, Levels: []mc.EscalationLevel{{Trigger: 1, AfterM: 60}}},
			0, 0, 1,
			[]int32{1},
		},
	} {
		got := EstimateVolume(tc.policy, tc.repeatM, tc.maxN)
		total := tc.wantRule
		for _, c := range tc.wantLevels {
			total += c
		}
		if got.Rule != tc.wantRule || !slices.Equal(got.Levels, tc.wantLevels) || got.Total != total {
			t.Errorf("%s: EstimateVolume() = %+v; want rule %d, levels %v, total %d", tc.name, got, tc.wantRule, tc.wantLevels, total)
		}
	}
}

func BenchmarkEscalate(b *testing.B) {
	in := EscalationInput{
		Policy: policy(mc.EscalationModeStack), Now: at(95), FirstFire: t0, Severity: 4,
		Levels: []EscalationLevelState{{NotifyCount: 3, LastNotifyTime: at(40)}, {NotifyCount: 1, LastNotifyTime: at(30)}, {NotifyCount: 1, LastNotifyTime: at(60)}},
	}
	b.ReportAllocs()
	for b.Loop() {
		if res := Escalate(in); len(res.Due) != 1 {
			b.Fatalf("unexpected result: %+v", res)
		}
	}
}

func BenchmarkEstimateVolume(b *testing.B) {
	p := policy(mc.EscalationModeStack)
	b.ReportAllocs()
	for b.Loop() {
		EstimateVolume(p, 30, 0)
	}
}
