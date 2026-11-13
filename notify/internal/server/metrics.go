package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics 管道可观测指标（OTel → Prometheus /metrics，设计 §10.5）：
//   - notify_loop_runs_total{loop,result} / notify_loop_items_total{loop}
//   - notify_dispatch_total{channel_type,result} / notify_send_latency_ms{channel_type}
//   - notify_ingress_requests_total{result} / notify_ingress_events_total{result}
//   - notify_events_pending / notify_dispatch_due（有界计数）/ notify_queue_oldest_age_seconds{queue}
//   - notify_breaker_open{channel_id} / notify_config_age_seconds / notify_config_refresh_failures_total
type Metrics struct {
	provider *sdkmetric.MeterProvider
	// LoopRuns 各 worker 循环次数（loop, result=ok|error）。
	LoopRuns metric.Int64Counter
	// LoopItems 各 worker 处理条数（loop）。
	LoopItems metric.Int64Counter
	// DispatchResults 发送结果（channel_type, result=sent|retry|dead|released）。
	DispatchResults metric.Int64Counter
	// SendLatency 真实出站耗时（毫秒，channel_type）。
	SendLatency metric.Float64Histogram
	// IngressRequests 接入请求结果（result=ok|unauthorized|replayed|invalid|quota_exceeded|error）。
	IngressRequests metric.Int64Counter
	// IngressEvents 接入事件结果（result=accepted|deduplicated）。
	IngressEvents metric.Int64Counter

	mu      sync.Mutex
	breaker biz.BreakerView
}

var (
	_ biz.SendObserver    = (*Metrics)(nil)
	_ biz.IngressObserver = (*Metrics)(nil)
)

// ObserveDispatch implements biz.SendObserver.
func (m *Metrics) ObserveDispatch(channelType, result string) {
	if m.DispatchResults == nil {
		return
	}
	m.DispatchResults.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("channel_type", channelType), attribute.String("result", result)))
}

// ObserveSendLatency implements biz.SendObserver.
func (m *Metrics) ObserveSendLatency(channelType string, d time.Duration) {
	if m.SendLatency == nil {
		return
	}
	m.SendLatency.Record(context.Background(), float64(d.Microseconds())/1000, metric.WithAttributes(attribute.String("channel_type", channelType)))
}

// ObserveIngress implements biz.IngressObserver.
func (m *Metrics) ObserveIngress(result string, n int) {
	if m.IngressEvents == nil || n <= 0 {
		return
	}
	m.IngressEvents.Add(context.Background(), int64(n), metric.WithAttributes(attribute.String("result", result)))
}

// ObserveIngressRequest 接入请求结果计数（server 中间件调用）。
func (m *Metrics) ObserveIngressRequest(result string) {
	if m.IngressRequests == nil {
		return
	}
	m.IngressRequests.Add(context.Background(), 1, metric.WithAttributes(attribute.String("result", result)))
}

// RegisterBreaker 注入熔断视图（sender 装配后调用；未注入则熔断 gauge 恒空）。
func (m *Metrics) RegisterBreaker(view biz.BreakerView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.breaker = view
}

// NewMetrics 构造指标与导出器；backlog 为 nil 时不注册积压观测。
func NewMetrics(backlog biz.BacklogRepo, cfg *biz.ConfigCache) (*Metrics, func(), error) {
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
	if m.SendLatency, err = meter.Float64Histogram("notify_send_latency_ms", metric.WithDescription("outbound send latency in milliseconds by channel type"),
		metric.WithExplicitBucketBoundaries(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000)); err != nil {
		return nil, nil, err
	}
	if m.IngressRequests, err = meter.Int64Counter("notify_ingress_requests_total", metric.WithDescription("ingress requests by result")); err != nil {
		return nil, nil, err
	}
	if m.IngressEvents, err = meter.Int64Counter("notify_ingress_events_total", metric.WithDescription("ingress events by result (accepted|deduplicated)")); err != nil {
		return nil, nil, err
	}
	if err := m.registerGauges(meter, backlog, cfg); err != nil {
		return nil, nil, err
	}
	cleanup := func() { _ = provider.Shutdown(context.Background()) }
	return m, cleanup, nil
}

// registerGauges 积压/队列年龄/熔断/配置年龄的观测回调（抓取时执行）。
func (m *Metrics) registerGauges(meter metric.Meter, backlog biz.BacklogRepo, cfg *biz.ConfigCache) error {
	breakerOpen, err := meter.Int64ObservableGauge("notify_breaker_open", metric.WithDescription("1 for each channel whose breaker is open"))
	if err != nil {
		return err
	}
	configAge, err := meter.Float64ObservableGauge("notify_config_age_seconds", metric.WithDescription("seconds since the config snapshot was last confirmed fresh"))
	if err != nil {
		return err
	}
	configFailures, err := meter.Int64ObservableCounter("notify_config_refresh_failures_total", metric.WithDescription("config refresh failures"))
	if err != nil {
		return err
	}
	instruments := []metric.Observable{breakerOpen, configAge, configFailures}
	var pending, due metric.Int64ObservableGauge
	var oldest metric.Float64ObservableGauge
	if backlog != nil {
		if pending, err = meter.Int64ObservableGauge("notify_events_pending", metric.WithDescription("events awaiting routing (publish_state=0); capped at 10001 = saturated")); err != nil {
			return err
		}
		if due, err = meter.Int64ObservableGauge("notify_dispatch_due", metric.WithDescription("dispatches due for sending (status in 1,4 and due); capped at 10001 = saturated")); err != nil {
			return err
		}
		if oldest, err = meter.Float64ObservableGauge("notify_queue_oldest_age_seconds", metric.WithDescription("age of the oldest due item by queue (events|dispatches)")); err != nil {
			return err
		}
		instruments = append(instruments, pending, due, oldest)
	}
	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		m.mu.Lock()
		view := m.breaker
		m.mu.Unlock()
		if view != nil {
			for id := range view.OpenChannels() {
				o.ObserveInt64(breakerOpen, 1, metric.WithAttributes(attribute.String("channel_id", id)))
			}
		}
		if cfg != nil {
			st := cfg.Status()
			if !st.LastOK.IsZero() {
				o.ObserveFloat64(configAge, time.Since(st.LastOK).Seconds())
			}
			o.ObserveInt64(configFailures, st.Failures)
		}
		if backlog == nil {
			return nil
		}
		b, err := backlog.Backlog(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(pending, b.PendingEvents)
		o.ObserveInt64(due, b.DueDispatches)
		o.ObserveFloat64(oldest, ageSeconds(b.OldestPending), metric.WithAttributes(attribute.String("queue", "events")))
		o.ObserveFloat64(oldest, ageSeconds(b.OldestDue), metric.WithAttributes(attribute.String("queue", "dispatches")))
		return nil
	}, instruments...)
	return err
}

func ageSeconds(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return max(0, time.Since(t).Seconds())
}

// Handler 返回 /metrics 处理器。
func (m *Metrics) Handler() http.Handler { return promhttp.Handler() }
