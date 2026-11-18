package service

import (
	"testing"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
)

// 聚合视图的过滤条件可省略：转换不能因为 filters 为空而崩溃（曾在真实请求上触发过 panic）。
func TestToSavedViewSpec_NilFilters(t *testing.T) {
	spec := toSavedViewSpec(&v1.AlertViewSpec{Name: "按规则", By: "rule"})
	if spec.Name != "按规则" || spec.By != "rule" || spec.Filters.Muted != nil || spec.Filters.GroupID != "" {
		t.Fatalf("spec = %+v", spec)
	}
	if got := toSavedViewSpec(nil); got.Name != "" {
		t.Fatalf("nil spec = %+v", got)
	}
}
