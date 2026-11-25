package server

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// fakeTableStats 表增长观测指标的假数据源：记下调用次数，返回预置的估算值或错误。
type fakeTableStats struct {
	estimates map[string]int64
	err       error
	calls     int
}

func (f *fakeTableStats) TableRowEstimates(context.Context) (map[string]int64, error) {
	f.calls++
	return f.estimates, f.err
}

// collectTableRows 按生产装配的方式注册表增长观测指标，经真实 OTel 管线采集一轮，
// 返回 core_table_rows_estimate 的数据点（表名 → 值）、指标说明与采集错误。
func collectTableRows(t *testing.T, stats TableStats) (map[string]int64, string, error) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	if err := registerTableRows(provider.Meter("reborn/core"), stats); err != nil {
		t.Fatalf("registerTableRows() error = %v", err)
	}
	var rm metricdata.ResourceMetrics
	err := reader.Collect(context.Background(), &rm)
	points := map[string]int64{}
	description := ""
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != tableRowsMetric {
				t.Fatalf("unexpected metric %q registered by registerTableRows", m.Name)
			}
			description = m.Description
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("%s is a %T, want an int64 gauge", m.Name, m.Data)
			}
			for _, p := range gauge.DataPoints {
				table, _ := p.Attributes.Value(attribute.Key("table"))
				if p.Attributes.Len() != 1 || table.Type() != attribute.STRING {
					t.Fatalf("data point attributes = %v, want exactly {table=<name>}", p.Attributes)
				}
				points[table.AsString()] = p.Value
			}
		}
	}
	return points, description, err
}

// 查询出错：本次不上报（没有数据点），也不让抓取失败（采集不返回错误）。
func TestRegisterTableRows_LookupFailureIsNeitherReportedNorFatal(t *testing.T) {
	t.Parallel()
	stats := &fakeTableStats{err: errors.New("database is unreachable")}
	points, _, err := collectTableRows(t, stats)
	if err != nil {
		t.Fatalf("Collect() error = %v, want nil: a failed lookup must not fail the scrape", err)
	}
	if len(points) != 0 {
		t.Fatalf("points = %v, want none when the lookup fails", points)
	}
	if stats.calls != 1 {
		t.Fatalf("TableRowEstimates() calls = %d, want 1", stats.calls)
	}
}

// 返回值逐表观测：每张表一个数据点，标签 table = 表名；0 也照常上报（从未分析 / 空表）。
func TestRegisterTableRows_ObservesEachTable(t *testing.T) {
	t.Parallel()
	want := map[string]int64{"monitor_rule_version": 12, "incident_outbox": 0, "job_trigger_fire": 1234567}
	stats := &fakeTableStats{estimates: want}
	points, _, err := collectTableRows(t, stats)
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if !reflect.DeepEqual(points, want) {
		t.Fatalf("points = %v, want %v", points, want)
	}
	if stats.calls != 1 {
		t.Fatalf("TableRowEstimates() calls = %d, want one lookup per scrape", stats.calls)
	}
}

// 没有可观测的表（数据源返回空结果）：不产生数据点，采集照常成功。
func TestRegisterTableRows_NothingToObserve(t *testing.T) {
	t.Parallel()
	points, _, err := collectTableRows(t, &fakeTableStats{estimates: map[string]int64{}})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if len(points) != 0 {
		t.Fatalf("points = %v, want none", points)
	}
}

// 指标说明必须讲清这是估算值：观察增长趋势用，不是精确计数（免得有人拿它对账）。
// 没有任何观测值时 OTel 不输出这条指标流，所以这里要带上一个数据点才读得到说明。
func TestRegisterTableRows_DescriptionSaysEstimate(t *testing.T) {
	t.Parallel()
	_, description, err := collectTableRows(t, &fakeTableStats{estimates: map[string]int64{"monitor_rule_version": 1}})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	lower := strings.ToLower(description)
	for _, fragment := range []string{"estimate", "growth", "not an exact count"} {
		if !strings.Contains(lower, fragment) {
			t.Fatalf("description %q must mention %q", description, fragment)
		}
	}
}
