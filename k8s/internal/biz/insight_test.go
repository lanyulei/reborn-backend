package biz

import (
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

func TestDiffEvents(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1_800_000_000, 0)
	ev := func(uid, name, reason string, count int64, last time.Time) WarningEvent {
		return WarningEvent{UID: uid, Namespace: "shop", Kind: "Pod", Name: name, Reason: reason, Count: count, FirstTime: t0, LastTime: last, Message: "m" + uid}
	}
	// 第一轮：全部算变化；同一对象同一原因的两个事件对象归并成一行。
	round1 := []WarningEvent{ev("a", "web", "BackOff", 3, t0), ev("b", "web", "BackOff", 2, t0.Add(time.Minute)), ev("c", "db", "Failed", 1, t0)}
	changed, seen := DiffEvents(nil, round1)
	if len(changed) != 2 || changed[0].Absolute != 5 || changed[0].Delta != 5 || changed[0].Message != "mb" || !changed[0].LastTime.Equal(t0.Add(time.Minute)) {
		t.Fatalf("round 1 = %+v", changed)
	}
	// 第二轮没有变化：不写库。
	if changed, _ := DiffEvents(seen, round1); len(changed) != 0 {
		t.Fatalf("unchanged round must produce nothing: %+v", changed)
	}
	// 第三轮：a 的次数 3 → 7；新出现 d。只有这两组变化，增量分别是 4 与 1。
	round3 := []WarningEvent{ev("a", "web", "BackOff", 7, t0.Add(2*time.Minute)), ev("b", "web", "BackOff", 2, t0.Add(time.Minute)), ev("c", "db", "Failed", 1, t0), ev("d", "db", "FailedMount", 0, t0)}
	changed, seen3 := DiffEvents(seen, round3)
	if len(changed) != 2 || changed[0].Reason != "BackOff" || changed[0].Delta != 4 || changed[0].Absolute != 9 || changed[1].Reason != "FailedMount" || changed[1].Delta != 1 {
		t.Fatalf("round 3 = %+v", changed)
	}
	if len(seen3) != 4 || seen3["a"].count != 7 {
		t.Fatalf("seen = %+v", seen3)
	}
	// 事件过期后被集群删除又重新出现（新的 UID）：按新事件累加，不丢。
	changed, _ = DiffEvents(seen3, []WarningEvent{ev("e", "web", "BackOff", 1, t0.Add(time.Hour))})
	if len(changed) != 1 || changed[0].Delta != 1 {
		t.Fatalf("re-created event = %+v", changed)
	}
}

func TestShouldNotify(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fresh, stale := now.Add(-time.Minute), now.Add(-k8scontract.EventNotifyFreshness-time.Second)
	if !ShouldNotify(nil, "FailedScheduling", fresh, now) || !ShouldNotify(nil, "backoff", fresh, now) {
		t.Error("default reasons must notify (case-insensitive)")
	}
	if ShouldNotify(nil, "Unhealthy", fresh, now) {
		t.Error("Unhealthy is not a default reason")
	}
	if ShouldNotify(nil, "FailedScheduling", stale, now) {
		t.Error("stale events (replayed after a restart) must not notify")
	}
	if !ShouldNotify([]string{"Unhealthy"}, "Unhealthy", fresh, now) || ShouldNotify([]string{"Unhealthy"}, "BackOff", fresh, now) {
		t.Error("a configured list replaces the default list")
	}
}

func TestSummaryBuilder(t *testing.T) {
	t.Parallel()
	cell := func(st, text string) map[string]any {
		return map[string]any{"status": k8scontract.StateCell{State: st, Text: text}}
	}
	b := NewSummaryBuilder()
	b.AddNode(&Entry{Row: k8scontract.Row{Name: "n1", State: k8scontract.StateOK, Cells: map[string]any{"pods": int64(110)}}, CPU: 4000, Memory: 8 << 30})
	b.AddNode(&Entry{Row: k8scontract.Row{Name: "n2", State: k8scontract.StatePending, Cells: map[string]any{"pods": int64(110)}}, CPU: 4000, Memory: 8 << 30})
	b.AddNode(&Entry{Row: k8scontract.Row{Name: "n3", State: k8scontract.StateAbnormal, Cells: cell(k8scontract.StateAbnormal, "NotReady")}, CPU: 4000, Memory: 8 << 30})
	for i := range 40 {
		c := cell(k8scontract.StateOK, "Running")
		c["restarts"] = int64(i)
		b.AddPod(&Entry{Row: k8scontract.Row{Namespace: "shop", Name: "p", State: k8scontract.StateOK, Cells: c}, CPU: 100, Memory: 1 << 20})
	}
	b.AddPod(&Entry{Row: k8scontract.Row{Name: "bad", State: k8scontract.StateAbnormal, Cells: cell(k8scontract.StateAbnormal, "CrashLoopBackOff")}})
	b.AddPod(&Entry{Row: k8scontract.Row{Name: "odd", Cells: map[string]any{}}})
	wc := cell(k8scontract.StateAbnormal, "Unavailable")
	wc["ready"] = "0/2"
	b.AddWorkload("Deployment", &Entry{Row: k8scontract.Row{Namespace: "shop", Name: "web", State: k8scontract.StateAbnormal, Cells: wc}})
	b.AddWorkload("StatefulSet", &Entry{Row: k8scontract.Row{Name: "db", State: k8scontract.StateOK, Cells: map[string]any{}}})
	s := b.Result()
	if s.NodeTotal != 3 || s.NodeReady != 2 || s.NodeCordoned != 1 || len(s.AbnormalNodes) != 1 || s.AbnormalNodes[0].Text != "NotReady" {
		t.Errorf("nodes = %+v", s)
	}
	if s.CPUAllocatable != 12000 || s.MemoryAllocatable != 24<<30 || s.PodCapacity != 220 || s.CPURequested != 4000 || s.MemoryRequested != 40<<20 {
		t.Errorf("capacity = %+v", s)
	}
	if s.PodTotal != 42 || s.Pods[k8scontract.StateOK] != 40 || s.Pods[k8scontract.StateAbnormal] != 1 || s.Pods[k8scontract.StateUnknown] != 1 {
		t.Errorf("pods = %+v", s.Pods)
	}
	if len(s.RestartPods) != k8scontract.SummaryTopN || s.RestartPods[0].Value != 39 || s.RestartPods[k8scontract.SummaryTopN-1].Value != 30 {
		t.Errorf("restart pods = %+v", s.RestartPods)
	}
	if s.WorkloadTotal != 2 || s.WorkloadAbnormal != 1 || s.AbnormalWorkloads[0].Text != "0/2 Unavailable" || s.AbnormalWorkloads[0].Kind != "Deployment" {
		t.Errorf("workloads = %+v", s.AbnormalWorkloads)
	}
}
