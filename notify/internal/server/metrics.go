package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics 管道可观测指标（OTel → Prometheus /metrics，设计 §10.5）。
type Metrics struct {
	provider *sdkmetric.MeterProvider
	// LoopRuns 各 worker 循环次数（loop, result=ok|error）。
	LoopRuns metric.Int64Counter
	// LoopItems 各 worker 处理条数（loop）。
	LoopItems metric.Int64Counter
	// DispatchResults 发送结果（channel_type, result=sent|retry|dead|released）。
	DispatchResults metric.Int64Counter
}

// ObserveDispatch implements biz.SendObserver.
func (m *Metrics) ObserveDispatch(channelType, result string) {
	if m.DispatchResults == nil {
		return
	}
	m.DispatchResults.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("channel_type", channelType), attribute.String("result", result)))
}

// NewMetrics 构造指标与导出器；backlog 为 nil 时不注册积压观测。
func NewMetrics(backlog biz.BacklogRepo) (*Metrics, func(), error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, nil, fmt.Errorf("init prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("reborn/notify")
	m := &Metrics{provider: provider}
	if m.LoopRuns, err = meter.Int64Counter("notify_loop_runs_total", metric.WithDescription("worker loop iterations by loop and result")); err != nil {
		return nil, nil, err
	}
	if m.LoopItems, err = meter.Int64Counter("notify_loop_items_total", metric.WithDescription("items processed by worker loop")); err != nil {
		return nil, nil, err
	}
	if m.DispatchResults, err = meter.Int64Counter("notify_dispatch_total", metric.WithDescription("dispatch outcomes by channel type and result")); err != nil {
		return nil, nil, err
	}
	if backlog != nil {
		pending, err := meter.Int64ObservableGauge("notify_events_pending", metric.WithDescription("events awaiting routing (publish_state=0)"))
		if err != nil {
			return nil, nil, err
		}
		due, err := meter.Int64ObservableGauge("notify_dispatch_due", metric.WithDescription("dispatches due for sending (status in 1,4 and due)"))
		if err != nil {
			return nil, nil, err
		}
		if _, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
			b, err := backlog.Backlog(ctx)
			if err != nil {
				return err
			}
			o.ObserveInt64(pending, b.PendingEvents)
			o.ObserveInt64(due, b.DueDispatches)
			return nil
		}, pending, due); err != nil {
			return nil, nil, err
		}
	}
	cleanup := func() { _ = provider.Shutdown(context.Background()) }
	return m, cleanup, nil
}

// Handler 返回 /metrics 处理器。
func (m *Metrics) Handler() http.Handler { return promhttp.Handler() }
