package biz_test

import (
	"context"
	"slices"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 订阅预览不计被抑制的事件（monitor 模块评审 dispatch P3-2）：引擎不会为它们入箱通知、派发也会丢弃，
// 预览若把它们算作「会命中」会虚高（一条源告警抑制几十条目标告警时尤其明显）。
func TestSubscription_PreviewExcludesInhibitedAlerts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	ids := seedAlerts(e)
	now := time.Now()
	inhibited := func(id string, status int32, age time.Duration) {
		a := &biz.Alert{
			ID: id, GroupID: gHost, Severity: mc.SeverityCritical, Status: status, Labels: map[string]string{"instance": id, "env": "prod"},
			FirstFireTime: now.Add(-age), LastFireTime: now.Add(-age), Inhibited: true, InhibitID: "inh-1", InhibitAlertID: ids["host"],
		}
		if status != mc.AlertFiring {
			end := now.Add(-age + time.Minute)
			a.ResolveTime = &end
		}
		e.store.SeedAlert(a)
	}
	inhibited("al-inhibited", mc.AlertFiring, time.Minute)
	inhibited("al-inhibited-old", mc.AlertResolved, 2*time.Hour)

	preview := func(s biz.SubscriptionSpec) []string {
		t.Helper()
		views, total, err := e.subs.Preview(ctx, uAlice, s)
		if err != nil {
			t.Fatalf("Preview: %v", err)
		}
		if total != len(views) {
			t.Fatalf("total %d != %d", total, len(views))
		}
		return alertIDs(views)
	}
	// 活跃的与近 24 小时历史里被抑制的都不计入；未受影响的照常计入。
	if got := preview(subSpec(gInfra)); !slices.Equal(got, []string{ids["host"], ids["host-old"]}) {
		t.Fatalf("infra subscription preview = %v, want the unaffected alerts only", got)
	}
	resolvedOnly := subSpec(gInfra)
	resolvedOnly.OnFiring, resolvedOnly.OnResolved = false, true
	if got := preview(resolvedOnly); !slices.Equal(got, []string{ids["host-old"]}) {
		t.Fatalf("resolved-only preview = %v, want the unaffected resolved alert only", got)
	}
	// 抑制解除后照常计入。
	e.store.SetAlert("al-inhibited", func(a *biz.Alert) { a.Inhibited = false })
	if got := preview(subSpec(gInfra)); !slices.Equal(got, []string{ids["host"], ids["host-old"], "al-inhibited"}) {
		t.Fatalf("preview after the inhibition lifted = %v", got)
	}
}
