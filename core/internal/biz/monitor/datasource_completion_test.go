package biz_test

import (
	"context"
	"errors"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
)

// 补全元数据接口（指标名 / 标签名 / 标签值）：与查询同一条可查性链路（启用校验、
// 数据源存在性、错误透传），limit 在用例层夹紧（≤0 缺省、超上限封顶）。
func TestDatasource_CompletionMetadata(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()

	names, err := e.sources.ListMetricNames(ctx, dsVM, 0)
	if err != nil {
		t.Fatalf("ListMetricNames: %v", err)
	}
	if len(names) == 0 || names[0] != "up" {
		t.Fatalf("names = %v", names)
	}
	labels, err := e.sources.ListLabelNames(ctx, dsVM, 0)
	if err != nil {
		t.Fatalf("ListLabelNames: %v", err)
	}
	if labels[0] != "job" {
		t.Fatalf("labels = %v", labels)
	}
	values, err := e.sources.ListLabelValues(ctx, dsVM, "cluster", 0)
	if err != nil {
		t.Fatalf("ListLabelValues: %v", err)
	}
	if values[0] != "value-1" {
		t.Fatalf("values = %v", values)
	}

	// limit 夹紧：≤0 取缺省 200，超上限封顶 1000（桩记录收到的 limit）
	if _, err = e.sources.ListMetricNames(ctx, dsVM, 0); err != nil {
		t.Fatalf("ListMetricNames(0): %v", err)
	}
	if _, err = e.sources.ListMetricNames(ctx, dsVM, 99999); err != nil {
		t.Fatalf("ListMetricNames(99999): %v", err)
	}
	limits := e.query.Limits()
	if n := len(limits); n < 2 {
		t.Fatalf("limits recorded = %v", limits)
	}
	if got := limits[len(limits)-2]; got != 200 {
		t.Fatalf("limit for 0 = %d, want default 200", got)
	}
	if got := limits[len(limits)-1]; got != 1000 {
		t.Fatalf("limit for 99999 = %d, want clamped 1000", got)
	}

	// 停用的数据源：与即时查询同一拒绝语义
	_, err = e.sources.ListMetricNames(ctx, dsOff, 0)
	wantErr(t, "disabled datasource", err, biz.ErrDatasourceDisabled)

	// 不存在的数据源
	if _, err = e.sources.ListMetricNames(ctx, "ds-not-exist", 0); err == nil {
		t.Fatal("want error for missing datasource")
	}

	// 上游失败透传（桩的 Err）
	e.query.Err = errors.New("upstream down")
	if _, err = e.sources.ListLabelValues(ctx, dsVM, "job", 0); err == nil {
		t.Fatal("want error when upstream fails")
	}
	e.query.Err = nil
}
