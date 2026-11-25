package server

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// 表增长观测指标：保留期清理落地的第 (1) 步——只观测，不删任何数据。
// 为什么是估算值而不是精确的「可清理行数」见 internal/data/table_stats.go。
const (
	// tableRowsMetric 指标名（标签 table = 表名）。
	tableRowsMetric = "cmdb_table_rows_estimate"
	// tableRowsTimeout 抓取时现查的单次查询时限（抓取不应被数据库拖住；与 core 的同名指标一致）。
	tableRowsTimeout = 2 * time.Second
)

// registerTableRows 注册表增长观测指标与它的抓取回调。
// 估值取 pg_class.reltuples（autovacuum / analyze 更新）：是估算值，用于观察增长趋势，不是精确计数。
// 在抓取回调里现查：查询失败则本次不上报、不让抓取失败（下次抓取再试）。
// 多副本各自现查同一份系统目录，读到的估值一致，没有共享状态。
func registerTableRows(meter metric.Meter, tables biz.TableStats) error {
	gauge, err := meter.Int64ObservableGauge(tableRowsMetric,
		metric.WithDescription("estimated rows (pg_class.reltuples) of tables that only grow; an estimate for watching growth trends, not an exact count"))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, tableRowsTimeout)
		defer cancel()
		estimates, err := tables.TableRowEstimates(ctx)
		if err != nil {
			return nil
		}
		for table, rows := range estimates {
			o.ObserveInt64(gauge, rows, metric.WithAttributes(attribute.String("table", table)))
		}
		return nil
	}, gauge)
	return err
}
