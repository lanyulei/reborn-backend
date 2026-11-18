package biz

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/kit/promclient"
	"github.com/lanyulei/reborn/backend/monitor/internal/promtest"
)

// 第二轮评审（docs/monitor-review-round2.md，R 项）的回归测试。

// R1：首次通知行被 core 丢弃（派发失败次数用尽）后，事件不再整个生命周期沉默：按重复通知间隔把同一行
// 重新置为待派发。节奏有上限——待派发期间不重试、刚被丢弃不重试、不新增行。
func TestAdvanceRetriesADroppedFirstNotification(t *testing.T) {
	series := func() []promclient.Series { return q(ser(90, "instance", "a")) }
	s := newSim(t, baseRule(func(r *Rule) { r.For, r.RepeatInterval = 0, 5*time.Minute }))
	s.step(at(0), series())
	if got := s.kinds(); !equalStrings(got, []string{"1/1"}) {
		t.Fatalf("outbox = %v", got)
	}
	// 首次通知还在排队（core 派发得慢，或正在失败重试）：过多久都不重新激活、不追加。
	for _, sec := range []int{60, 600, 3600} {
		if out := s.step(at(sec), series()); len(out.Transition.Outbox) != 0 {
			t.Fatalf("t=%d: a pending first notification must be left alone, outbox = %+v", sec, out.Transition.Outbox)
		}
	}
	// core 派发失败次数用尽：丢弃，通知计数停在 0。
	s.drop(at(3600))
	retried := 0
	for sec := 3660; sec <= 3600+3600; sec += 60 {
		out := s.step(at(sec), series())
		if len(out.Transition.Outbox) == 0 {
			continue
		}
		o := out.Transition.Outbox[0]
		if len(out.Transition.Outbox) != 1 || !o.Rearm || o.Kind != monitorcontract.NotifyKindFiring || o.Seq != 1 {
			t.Fatalf("t=%d: outbox = %+v", sec, out.Transition.Outbox)
		}
		// 距丢弃满一个重复间隔才重试；重试后又是待派发，core 再次用尽次数丢弃。
		if want := 3600 + 300*(retried+1); sec != want {
			t.Fatalf("retry #%d at t=%d, want t=%d (one per repeat interval after the drop)", retried+1, sec, want)
		}
		retried++
		s.drop(at(sec))
	}
	// 一小时 60 轮评估、core 每次都立刻丢弃：恰好每 5 分钟一次，而不是每轮一次；发件箱始终只有那一行。
	if retried != 12 || s.rearmed != 12 || len(s.outbox) != 1 || s.alert().NotifyCount != 0 {
		t.Fatalf("retries = %d, rearmed = %d, outbox rows = %d, notify count = %d", retried, s.rearmed, len(s.outbox), s.alert().NotifyCount)
	}

	// 下一次重试派发成功：之后与从未丢弃过的事件完全一样——重复通知、恢复通知照常。
	out := s.step(at(7500), series())
	if len(out.Transition.Outbox) != 1 || s.outbox[0].Status != monitorcontract.OutboxPending {
		t.Fatalf("outbox = %+v", out.Transition.Outbox)
	}
	s.dispatch(at(7500))
	if s.alert().NotifyCount != 1 {
		t.Fatalf("notify count = %d", s.alert().NotifyCount)
	}
	for _, sec := range []int{7560, 7740} {
		if out = s.step(at(sec), series()); len(out.Transition.Outbox) != 0 {
			t.Fatalf("t=%d: outbox = %+v", sec, out.Transition.Outbox)
		}
	}
	s.step(at(7800), series())
	if got := s.kinds(); !equalStrings(got, []string{"1/1", "2/2"}) {
		t.Fatalf("outbox = %v", got)
	}
	s.dispatch(at(7800))
	s.step(at(7860))
	if got := s.kinds(); !equalStrings(got, []string{"1/1", "2/2", "3/3"}) || s.alert().Status != monitorcontract.AlertResolved {
		t.Fatalf("outbox = %v, alert status = %d", got, s.alert().Status)
	}
}

// R1 的边界：没有配置重复通知时用固定间隔；被屏蔽的事件不重试；总次数上限为 1 的规则同样重试
// （还一次都没通知出去）；丢弃后在重试之前就恢复的事件没有恢复通知（从未通知过）。
func TestAdvanceFirstNotificationRetryEdges(t *testing.T) {
	series := func() []promclient.Series { return q(ser(90, "instance", "a")) }
	dropped := func(t *testing.T, mod func(*Rule)) *sim {
		t.Helper()
		s := newSim(t, baseRule(func(r *Rule) { r.For = 0 }, mod))
		s.step(at(0), series())
		s.drop(at(0))
		return s
	}
	t.Run("no repeat interval: fixed cadence", func(t *testing.T) {
		s := dropped(t, func(*Rule) {})
		for _, sec := range []int{60, 540, 599} {
			if out := s.step(at(sec), series()); len(out.Transition.Outbox) != 0 {
				t.Fatalf("t=%d: retried before %v, outbox = %+v", sec, firstNotifyRetry, out.Transition.Outbox)
			}
		}
		out := s.step(at(600), series())
		if len(out.Transition.Outbox) != 1 || !out.Transition.Outbox[0].Rearm || s.rearmed != 1 {
			t.Fatalf("outbox = %+v", out.Transition.Outbox)
		}
		// 没有配置重复通知：首次通知送达后不再有任何通知。
		s.dispatch(at(600))
		for _, sec := range []int{660, 1200, 7200} {
			if out = s.step(at(sec), series()); len(out.Transition.Outbox) != 0 {
				t.Fatalf("t=%d: outbox = %+v", sec, out.Transition.Outbox)
			}
		}
	})
	t.Run("muted alert is not retried", func(t *testing.T) {
		s := dropped(t, func(r *Rule) { r.RepeatInterval = 5 * time.Minute })
		s.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 30, 100000)}
		for _, sec := range []int{60, 360, 3600} {
			if out := s.step(at(sec), series()); len(out.Transition.Outbox) != 0 {
				t.Fatalf("t=%d: outbox = %+v", sec, out.Transition.Outbox)
			}
		}
	})
	t.Run("max_notify 1 still retries the first notification", func(t *testing.T) {
		s := dropped(t, func(r *Rule) { r.RepeatInterval, r.MaxNotify = 5*time.Minute, 1 })
		if out := s.step(at(300), series()); len(out.Transition.Outbox) != 1 {
			t.Fatalf("outbox = %+v", out.Transition.Outbox)
		}
		s.dispatch(at(300))
		if out := s.step(at(900), series()); len(out.Transition.Outbox) != 0 || s.alert().NotifyCount != 1 {
			t.Fatalf("outbox = %+v", out.Transition.Outbox)
		}
	})
	t.Run("resolved before the retry: no recovery notification", func(t *testing.T) {
		s := dropped(t, func(r *Rule) { r.RepeatInterval = 5 * time.Minute })
		s.step(at(60))
		if got := s.kinds(); !equalStrings(got, []string{"1/1"}) || s.alert().Status != monitorcontract.AlertResolved || s.rearmed != 0 {
			t.Fatalf("outbox = %v, status = %d", got, s.alert().Status)
		}
	})
}

// R3：被顺延的轮次（排不上数据源并发槽）什么都没观察到，与查询错误 / 不在生效时间同一口径——
// 待触发的持续时长、留观的时长都从下一次真正观察到的那一轮重新起算。
func TestAdvancePostponedRoundsDoNotAdvanceTimers(t *testing.T) {
	series := func() []promclient.Series { return q(ser(90, "instance", "a")) }
	t.Run("pending", func(t *testing.T) {
		s := newSim(t, baseRule())
		s.step(at(0), series())
		s.step(at(15), series())
		s.postpone() // t=30 的那一轮被顺延
		s.step(at(35), series())
		if st := s.only(); st.State != monitorcontract.StatePending || !st.ActiveSince.Equal(at(35)) {
			t.Fatalf("for must restart at the first observed round after a postponed one: %+v", st)
		}
		// 距最初出现已满 60s，但距重新起算只有 45s：不触发。
		if s.step(at(80), series()); len(s.alerts) != 0 {
			t.Fatalf("fired across an unobserved round")
		}
		if s.step(at(95), series()); len(s.alerts) != 1 {
			t.Fatalf("must fire 60s after the restart")
		}
	})
	t.Run("pending series gone after the postponed round", func(t *testing.T) {
		s := newSim(t, baseRule())
		s.step(at(0), series())
		s.postpone()
		if s.step(at(20)); len(s.states) != 0 {
			t.Fatalf("states = %+v", s.states)
		}
	})
	t.Run("recovering", func(t *testing.T) {
		s := newSim(t, baseRule(func(r *Rule) { r.For, r.Recover = 0, 60*time.Second }))
		s.step(at(0), series())
		s.step(at(15))
		if st := s.only(); st.State != monitorcontract.StateRecovering || !st.RecoverSince.Equal(at(15)) {
			t.Fatalf("state = %+v", st)
		}
		s.postpone()
		s.step(at(50))
		if st := s.only(); st.State != monitorcontract.StateRecovering || !st.RecoverSince.Equal(at(50)) {
			t.Fatalf("the keep timer must restart at the first observed round after a postponed one: %+v", st)
		}
		// 距进入留观已满 60s，但距重新起算只有 45s：不恢复。
		if s.step(at(95)); s.alert().Status != monitorcontract.AlertFiring {
			t.Fatalf("resolved across an unobserved round")
		}
		if s.step(at(110)); s.alert().Status != monitorcontract.AlertResolved {
			t.Fatalf("must resolve 60s after the restart")
		}
	})
	t.Run("the mark is consumed by one completed round", func(t *testing.T) {
		s := newSim(t, baseRule())
		s.postpone()
		s.step(at(0), series())
		if s.step(at(60), series()); len(s.alerts) != 1 {
			t.Fatalf("an ordinary round after the restart must count normally")
		}
	})
}

// R3（评估器）：排不上槽的一轮把标记留在任务行上；下一轮带回，计时从那一轮重新起算；评估完成后标记清除。
func TestEvaluatorPostponedRoundRestartsTimers(t *testing.T) {
	h := newHarness(t, baseRule(), nil, func(d *Datasource) { d.Timeout = time.Second })
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.run(at(0))
	if st := h.store.sim(testDSID).only(); st.State != monitorcontract.StatePending {
		t.Fatalf("state = %+v", st)
	}
	handle := h.cache.Datasource(testDSID)
	for range cap(handle.slots) {
		handle.slots <- struct{}{}
	}
	h.run(at(60))
	if h.obs.lastResult() != ResultBusy || len(h.store.released) != 1 || !h.store.released[0].task.Postponed || len(h.store.rounds) != 1 {
		t.Fatalf("result = %q, released = %+v, rounds = %d", h.obs.lastResult(), h.store.released, len(h.store.rounds))
	}
	for range cap(handle.slots) {
		<-handle.slots
	}
	// 重试的这一轮：距首次出现已 65s ≥ for，但 t=60 的那一轮没有观察——不触发，持续时长从本轮起算。
	h.run(at(65))
	st := h.store.sim(testDSID).only()
	if r := h.store.last(); r.Status != monitorcontract.EvalOK || st.State != monitorcontract.StatePending || !st.ActiveSince.Equal(at(65)) {
		t.Fatalf("round = status %d, state = %+v", r.Status, st)
	}
	if h.store.postponed[testDSID] {
		t.Fatalf("a completed round must clear the mark")
	}
	h.run(at(110))
	if r := h.store.last(); r.Status != monitorcontract.EvalOK {
		t.Fatalf("round = %+v", r)
	}
	h.run(at(125))
	if r := h.store.last(); r.Status != monitorcontract.EvalFiring {
		t.Fatalf("round = %+v", r)
	}
}

// R2：等数据源并发槽的时间不计入查询超时。数据源健康但慢（每次查询 1.6s，超时 2s），24 个任务同时到期、
// 8 个并发槽：第二批任务等了 1.6s 才拿到槽，查询本身照样有完整的 2s——不得记成上游超时（那会落一轮
// 查询错误并重置待触发 / 留观计时）；等槽超出预算（一个查询超时）的第三批走 busy 顺延。
// 每个任务占用租约的时间有上限：等槽预算 + 查询超时。
func TestEvaluatorSlotWaitIsNotChargedToTheQueryTimeout(t *testing.T) {
	const (
		tasks   = 24
		latency = 1600 * time.Millisecond
		timeout = 2 * time.Second
	)
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil, func(d *Datasource) { d.Timeout = timeout })
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.prom.SetDelay(latency)
	ids := make([]string, tasks)
	for i := range ids {
		ids[i] = testDSID
	}
	start := time.Now()
	h.runTasks(context.Background(), at(0), ids...)
	elapsed := time.Since(start)

	var done, failed int
	for _, r := range h.store.rounds {
		if r.Status == monitorcontract.EvalError {
			failed++
			t.Errorf("query error round: %q", r.Error)
		}
		if r.Status == monitorcontract.EvalFiring {
			done++
		}
	}
	if failed != 0 {
		t.Fatalf("%d of %d rounds on a healthy datasource were recorded as query errors", failed, len(h.store.rounds))
	}
	// 两批（16 个）拿到了槽并完成；第三批（8 个）等满预算后顺延，带着标记归还。
	if done != 2*datasourceConcurrency || len(h.store.released) != tasks-done {
		t.Fatalf("completed = %d, postponed = %d, want %d / %d", done, len(h.store.released), 2*datasourceConcurrency, tasks-2*datasourceConcurrency)
	}
	for _, rel := range h.store.released {
		if rel.delay != retryDelay || !rel.task.Postponed {
			t.Fatalf("released = %+v (task %+v)", rel, rel.task)
		}
	}
	busy := 0
	for _, result := range h.obs.results {
		switch result {
		case ResultBusy:
			busy++
		case ResultFiring:
		default:
			t.Errorf("unexpected evaluation result %q", result)
		}
	}
	if busy != tasks-done {
		t.Fatalf("busy = %d, want %d", busy, tasks-done)
	}
	if got := h.prom.MaxInFlight(); got != datasourceConcurrency {
		t.Fatalf("max in-flight queries = %d, want %d", got, datasourceConcurrency)
	}
	if hits := h.prom.TotalHits(); hits != done {
		t.Fatalf("queries sent = %d, want %d (postponed tasks must not query)", hits, done)
	}
	// 租约占用有上限：任何一个任务至多「等槽预算 + 查询超时」（另加调度余量），远在租约
	// roundQueryBudgets × 超时 + leaseMargin 之内。
	bound := 2*timeout + time.Second
	if h.obs.longest > bound || elapsed > bound {
		t.Fatalf("longest evaluation = %v, batch = %v, want <= %v", h.obs.longest, elapsed, bound)
	}
	if lease := roundQueryBudgets*timeout + leaseMargin; bound >= lease {
		t.Fatalf("bound %v must stay inside the lease %v", bound, lease)
	}
	if n := h.logs.count("level=WARN", "datasource queries are failing"); n != 0 {
		t.Fatalf("a healthy datasource was reported as failing:\n%s", h.logs)
	}
}

func TestAcquireSlot(t *testing.T) {
	slots := make(chan struct{}, 1)
	// 有空槽：立即拿到，即使预算已经用完 / ctx 已取消。
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := acquireSlot(cancelled, slots, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("a free slot must be taken immediately: %v", err)
	}
	start := time.Now()
	if err := acquireSlot(context.Background(), slots, time.Now().Add(80*time.Millisecond)); !errors.Is(err, errSlotBusy) {
		t.Fatalf("err = %v", err)
	}
	if waited := time.Since(start); waited < 70*time.Millisecond || waited > time.Second {
		t.Fatalf("waited %v, want about 80ms", waited)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := acquireSlot(ctx, slots, time.Time{}); !errors.Is(err, errSlotBusy) {
		t.Fatalf("err = %v", err)
	}
	// 等待期间槽被释放：拿到。
	time.AfterFunc(30*time.Millisecond, func() { <-slots })
	if err := acquireSlot(context.Background(), slots, time.Now().Add(5*time.Second)); err != nil {
		t.Fatalf("err = %v", err)
	}
}

// R4：上游序列的标签数与长度有上限；超限的序列删减 / 截断后照常判定，并计数。
func TestMergeSamplesLimitsUpstreamLabels(t *testing.T) {
	rule := baseRule(func(r *Rule) { r.Labels = map[string]string{"team": "ops"} })
	many := func(n int) promclient.Series {
		kv := make([]string, 0, 2*n)
		for i := range n {
			kv = append(kv, fmt.Sprintf("l%03d", i), "v")
		}
		return ser(1, kv...)
	}
	long := strings.Repeat("错", 1000) // 3000 字节，上限不在字符边界上

	set := merge(rule, testDSID, [][]promclient.Series{q(
		many(maxSeriesLabels),   // 恰好在上限：不动
		many(maxSeriesLabels+6), // 超出 6 个
		ser(2, "instance", "a", "msg", long),
		ser(3, "instance", "b", strings.Repeat("n", maxLabelNameBytes+1), "x", strings.Repeat("m", maxLabelNameBytes), "y"),
		ser(4, "instance", "c", "exact", strings.Repeat("v", maxLabelValueBytes), "nul", "a\x00b"),
	)})
	// 前两条删减后标签集相同：同指纹只留先出现的一条。
	if set.Limited != 3 || len(set.Kept) != 4 || set.Series != 5 {
		t.Fatalf("limited = %d, kept = %d, series = %d", set.Limited, len(set.Kept), set.Series)
	}
	first := set.Kept[0].Labels
	if len(first) != maxSeriesLabels+1 || first["team"] != "ops" || first["l063"] != "v" {
		t.Fatalf("labels = %d", len(first))
	}
	if _, ok := first["l064"]; ok {
		t.Fatalf("labels beyond the limit must be dropped by name order")
	}
	msg := set.Kept[1].Labels["msg"]
	if len(msg) > maxLabelValueBytes || len(msg) < maxLabelValueBytes-utf8.UTFMax || !utf8.ValidString(msg) || !strings.HasSuffix(msg, truncatedMark) ||
		!strings.HasPrefix(msg, "错错") {
		t.Fatalf("truncated value: len = %d, valid = %v, tail = %q", len(msg), utf8.ValidString(msg), msg[max(0, len(msg)-20):])
	}
	named := set.Kept[2].Labels
	if len(named) != 3 || named[strings.Repeat("m", maxLabelNameBytes)] != "y" {
		t.Fatalf("labels with over-long names must be dropped: %d labels", len(named))
	}
	if exact := set.Kept[3].Labels; len(exact["exact"]) != maxLabelValueBytes || exact["nul"] != "ab" {
		t.Fatalf("values at the limit must be kept as they are: %d, %q", len(exact["exact"]), exact["nul"])
	}

	// 确定性：保留哪些标签与上游返回的键顺序无关——同一条序列两次合并得到同一个指纹。
	again := merge(rule, testDSID, [][]promclient.Series{q(many(maxSeriesLabels + 6))})
	if again.Limited != 1 || again.Kept[0].Fingerprint != set.Kept[0].Fingerprint {
		t.Fatalf("label limiting must be deterministic")
	}
}

// R4（评估器）：超限序列计入判定记录与任务的提示，但这一轮仍是完整结果（没出现的序列照常恢复）。
func TestEvaluatorNotesLabelLimitedSeries(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil)
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.run(at(0))
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "b", "msg", strings.Repeat("x", 5000)))
	h.run(at(60))
	r := h.store.last()
	want := "label limit: 1 series exceeded the label limits (at most 64 labels per series, 128 bytes per name, 1024 bytes per value); " +
		"extra labels were dropped and long values truncated"
	if r.Status != monitorcontract.EvalFiring || r.Error != want || r.Recovered != 1 {
		t.Fatalf("round = status %d error %q recovered %d", r.Status, r.Error, r.Recovered)
	}
	if n := h.logs.count("incomplete result"); n != 0 {
		t.Fatalf("a label-limited round is not an incomplete result:\n%s", h.logs)
	}
	// 同时被截断：不完整的说明在前（固定前缀不变），标签超限的说明接在后面。
	h.cache.opts.MaxSeries = 1
	h.store.versions.Datasource++
	h.store.datasources[0].Version++
	h.cache.lastPoll.Store(0)
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "b", "msg", strings.Repeat("x", 5000)), promtest.S(91, "instance", "c"))
	h.run(at(120))
	r = h.store.last()
	if !strings.HasPrefix(r.Error, incompletePrefix+"query 0 truncated to 1 series; series not returned were left unchanged; "+labelLimitPrefix+"1 series") {
		t.Fatalf("error = %q", r.Error)
	}
	if len(r.Error) > maxErrorBytes {
		t.Fatalf("error is %d bytes", len(r.Error))
	}
}

// R4：printf 的输出预算把非字符串实参也算进去（实现与逐实参的测试在 kit/monitorpipeline）；这里经引擎的
// 调用路径走一遍：$labels 整体交给 printf 时同样受限。
func TestPrintfBudgetHoldsThroughTheEngine(t *testing.T) {
	big := map[string]string{}
	for i := range 40 {
		big[fmt.Sprintf("label_%02d", i)] = strings.Repeat("v", 200)
	}
	small := map[string]string{"instance": "a"}
	tpl, err := monitorpipeline.CompileTemplate(`{{ printf "%v" $labels }}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tpl.Render(big, 0); !errors.Is(err, monitorpipeline.ErrRenderTooLarge) {
		t.Fatalf("err = %v", err)
	}
	if out, err := tpl.Render(small, 0); err != nil || out != "map[instance:a]" {
		t.Fatalf("out = %q, %v", out, err)
	}
	got := renderAnnotations(map[string]string{"all": `{{ printf "%v" $labels }}`}, []*Sample{{Fingerprint: "f", Labels: big}})
	if got["f"]["all"] != templateError {
		t.Fatalf("annotation = %q, want the template error marker", got["f"]["all"])
	}
}
