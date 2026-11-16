package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// backlogTimeout 采集作业积压指标时单次查询的时限（抓取不应被数据库拖住）。
const backlogTimeout = 2 * time.Second

// Pinger 存活探测依赖（由 data 层的连接池实现）。
type Pinger interface {
	Ping(ctx context.Context) error
}

// Metrics 可观测指标（OTel → Prometheus /metrics）：
//   - core_loop_runs_total{loop,result} / core_loop_items_total{loop}：各后台循环的迭代次数与处理条数；
//   - core_workflow_jobs_overdue：到期超过一分钟仍没人处理的流程作业数（worker 角色没起来或处理不过来时上涨）；
//   - core_workflow_jobs_overdue_seconds：其中最早一条已经拖了多久；
//   - core_workflow_jobs_dead：处于死信、等待人工处理的流程作业数。
type Metrics struct {
	provider *sdkmetric.MeterProvider
	loopRuns metric.Int64Counter
	items    metric.Int64Counter
}

// NewMetrics 构造指标与导出器。积压指标在每次抓取时现查（一条走部分索引的聚合），
// 查询失败则本次不上报该指标（不让抓取整体失败）。
func NewMetrics(stats bizworkflow.JobStats) (*Metrics, func(), error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, nil, fmt.Errorf("init prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("reborn/core")
	m := &Metrics{provider: provider}
	if m.loopRuns, err = meter.Int64Counter("core_loop_runs_total", metric.WithDescription("worker loop iterations by loop and result")); err != nil {
		return nil, nil, err
	}
	if m.items, err = meter.Int64Counter("core_loop_items_total", metric.WithDescription("items processed by worker loop")); err != nil {
		return nil, nil, err
	}
	overdue, err := meter.Int64ObservableGauge("core_workflow_jobs_overdue",
		metric.WithDescription("workflow jobs pending for more than a minute past their due time"))
	if err != nil {
		return nil, nil, err
	}
	oldest, err := meter.Float64ObservableGauge("core_workflow_jobs_overdue_seconds",
		metric.WithDescription("age in seconds of the oldest overdue workflow job"), metric.WithUnit("s"))
	if err != nil {
		return nil, nil, err
	}
	dead, err := meter.Int64ObservableGauge("core_workflow_jobs_dead",
		metric.WithDescription("workflow jobs in the dead-letter state awaiting manual handling"))
	if err != nil {
		return nil, nil, err
	}
	if _, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, backlogTimeout)
		defer cancel()
		b, err := stats.Backlog(ctx)
		if err != nil {
			return nil
		}
		o.ObserveInt64(overdue, b.Overdue)
		o.ObserveFloat64(oldest, b.OldestSeconds)
		o.ObserveInt64(dead, b.Dead)
		return nil
	}, overdue, oldest, dead); err != nil {
		return nil, nil, err
	}
	return m, func() { _ = provider.Shutdown(context.Background()) }, nil
}

// Record implements loop.Recorder；nil 接收者安全（测试装配可不带指标）。
func (m *Metrics) Record(ctx context.Context, name string, items int, err error) {
	if m == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.loopRuns.Add(ctx, 1, metric.WithAttributes(attribute.String("loop", name), attribute.String("result", result)))
	if items > 0 {
		m.items.Add(ctx, int64(items), metric.WithAttributes(attribute.String("loop", name)))
	}
}

// probes 注册探活与指标端点（任何角色都有；不经鉴权中间件，不带业务数据）。
// 这两个端点只应对集群内的探针 / 采集器开放，不要经网关暴露到公网。
func probes(srv interface {
	HandleFunc(path string, h http.HandlerFunc)
	Handle(path string, h http.Handler)
}, pinger Pinger, metrics *Metrics,
) {
	srv.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if pinger != nil {
			ctx, cancel := context.WithTimeout(r.Context(), backlogTimeout)
			defer cancel()
			if err := pinger.Ping(ctx); err != nil {
				// 不回显底层错误（可能含连接串细节）；原因看服务日志。
				http.Error(w, "database unreachable", http.StatusServiceUnavailable)
				return
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	if metrics != nil {
		srv.Handle("/metrics", promhttp.Handler())
	}
}
