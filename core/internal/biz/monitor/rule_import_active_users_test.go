package biz_test

import (
	"context"
	"slices"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// JSON 导入时「通知用户是否仍可用」改用集合判定（core-monitor-rules 优化项 3）后结果不变：仍可用的用户保留、
// 顺序不变；导出之后被停用的用户从规则里去掉并给出提示。
func TestRuleP2_ImportJSONKeepsOnlyActiveNotifyUsers(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	keep1 := "018f3e1a-0d00-7000-8000-0000000000e1"
	gone := "018f3e1a-0d00-7000-8000-0000000000e2"
	keep2 := "018f3e1a-0d00-7000-8000-0000000000e3"
	e.org.Users[keep1], e.org.Users[gone], e.org.Users[keep2] = "张三", "李四", "王五"

	s := spec(gHost, "active-users")
	s.Notify = mc.NotifyTargets{UserIDs: []string{keep2, gone, keep1}}
	src := e.mustRule(t, uAdmin, s)
	content, n, err := e.rules.Export(ctx, uAdmin, []string{src.ID}, "", biz.ExportJSON)
	if err != nil || n != 1 {
		t.Fatalf("Export: %v, n = %d", err, n)
	}
	delete(e.org.Users, gone) // 导出之后停用

	items, err := e.rules.PreviewImport(ctx, uAdmin, biz.ImportOptions{
		Content: content, GroupID: gInfra, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: 2,
	})
	if err != nil || len(items) != 1 || items[0].Spec == nil {
		t.Fatalf("PreviewImport: %v %+v", err, items)
	}
	if got := items[0].Spec.Notify.UserIDs; !slices.Equal(got, []string{keep2, keep1}) {
		t.Fatalf("notify users = %v, want the two active ones in file order", got)
	}
	if len(items[0].Warnings) != 1 {
		t.Fatalf("the dropped user must be reported once, warnings = %v", items[0].Warnings)
	}
}
