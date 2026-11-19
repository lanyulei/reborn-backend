// Package server 构造传输层与后台循环的装配：角色门控（SERVER_ROLES）、HTTP
// （仅 /healthz /metrics）、指标。评估引擎不暴露业务接口。
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/loop"
	kitroles "github.com/lanyulei/reborn/backend/kit/roles"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
	"github.com/lanyulei/reborn/backend/monitor/internal/conf"
	"github.com/lanyulei/reborn/backend/monitor/internal/worker"

	"github.com/go-kratos/kratos/v3/middleware/logging"
	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/transport"
	khttp "github.com/go-kratos/kratos/v3/transport/http"
	"github.com/google/wire"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// ProviderSet is server providers.
var ProviderSet = wire.NewSet(
	NewRoles, NewMetrics, wire.Bind(new(loop.Recorder), new(*Metrics)), wire.Bind(new(biz.Observer), new(*Metrics)),
	NewHTTPServer, NewServers, NewInstanceInfo,
)

// AppVersion 程序版本（main 的 -ldflags 注入值）。
type AppVersion string

// maxInstanceHost 实例 ID 里主机名的最大长度（lease_owner / instance 均为 varchar(64)）。
const maxInstanceHost = 40

// NewInstanceInfo 本实例的身份：ID = 主机名 + 启动时生成的随机串（进程重启后必不相同），用作心跳行的主键
// 与任务属主令牌的前缀。
func NewInstanceInfo(roles Roles, version AppVersion) (biz.InstanceInfo, error) {
	nonce := make([]byte, 6)
	if _, err := rand.Read(nonce); err != nil {
		return biz.InstanceInfo{}, err
	}
	host, _ := os.Hostname()
	if len(host) > maxInstanceHost {
		host = host[:maxInstanceHost]
	}
	return biz.InstanceInfo{
		ID: host + "-" + hex.EncodeToString(nonce), Roles: strings.Join(roles.List(), ","), Version: string(version),
	}, nil
}

// 角色名。
const (
	// RolePlanner 规划器：任务物化、数据源探活、维护（选主单实例执行）。
	RolePlanner = "planner"
	// RoleEvaluator 评估器：领取评估任务并推进状态机（多实例并发）。
	RoleEvaluator = "evaluator"
)

// Roles 启用的角色集合。
type Roles map[string]struct{}

// NewRoles 解析角色：环境变量 SERVER_ROLES 优先，其次 conf.engine.roles，两者皆空 = 全部角色；
// 未知角色名直接装配失败。
func NewRoles(c *conf.Engine) (Roles, error) {
	set, _, err := kitroles.Resolve(c.GetRoles(), RolePlanner, RoleEvaluator)
	if err != nil {
		return nil, err
	}
	return Roles(set), nil
}

// Has 报告角色是否启用。
func (r Roles) Has(role string) bool {
	_, ok := r[role]
	return ok
}

// List 按固定顺序列出已启用角色（日志用）。
func (r Roles) List() []string {
	out := make([]string, 0, 2)
	for _, role := range []string{RolePlanner, RoleEvaluator} {
		if r.Has(role) {
			out = append(out, role)
		}
	}
	return out
}

// Metrics 评估引擎可观测指标（OTel → Prometheus /metrics），指标名前缀 monitor_。
type Metrics struct {
	provider *sdkmetric.MeterProvider
	loopRuns metric.Int64Counter
	items    metric.Int64Counter
	evals    metric.Int64Counter
	evalTime metric.Float64Histogram
	evalLag  metric.Float64Histogram
	outbox   metric.Int64Counter
	mutesCut metric.Int64Counter
	cacheCut metric.Int64Counter
	cycles   metric.Int64Counter
	records  metric.Int64Counter
	recTime  metric.Float64Histogram
	recOut   metric.Int64Counter
	commands metric.Int64Counter
	engineEv metric.Int64Counter
}

var _ biz.Observer = (*Metrics)(nil)

// durationBuckets 评估耗时与评估延迟的直方图桶（秒）。
var durationBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

// outboxKinds 发件箱通知类型的指标标签（下标 = monitor_notify_outbox.kind）。
var outboxKinds = [...]string{"unknown", "firing", "repeat", "resolved", "escalation"}

// NewMetrics 构造指标与导出器。
func NewMetrics() (*Metrics, func(), error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, nil, fmt.Errorf("init prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("reborn/monitor")
	m := &Metrics{provider: provider}
	if m.loopRuns, err = meter.Int64Counter("monitor_loop_runs_total", metric.WithDescription("worker loop iterations by loop and result")); err != nil {
		return nil, nil, err
	}
	if m.items, err = meter.Int64Counter("monitor_loop_items_total", metric.WithDescription("items processed by worker loop")); err != nil {
		return nil, nil, err
	}
	if m.evals, err = meter.Int64Counter("monitor_evaluations_total", metric.WithDescription("rule evaluations by result")); err != nil {
		return nil, nil, err
	}
	if m.evalTime, err = meter.Float64Histogram("monitor_evaluation_duration_seconds", metric.WithUnit("s"),
		metric.WithDescription("duration of one rule evaluation (queries + state transition)"),
		metric.WithExplicitBucketBoundaries(durationBuckets...)); err != nil {
		return nil, nil, err
	}
	if m.evalLag, err = meter.Float64Histogram("monitor_evaluation_lag_seconds", metric.WithUnit("s"),
		metric.WithDescription("delay between the scheduled evaluation time and the moment the task was claimed"),
		metric.WithExplicitBucketBoundaries(durationBuckets...)); err != nil {
		return nil, nil, err
	}
	if m.outbox, err = meter.Int64Counter("monitor_outbox_rows_total", metric.WithDescription("notification outbox rows written by kind")); err != nil {
		return nil, nil, err
	}
	if m.mutesCut, err = meter.Int64Counter("monitor_mutes_truncated_total",
		metric.WithDescription("mute rule loads that hit the in-memory limit; rules beyond the limit are not in effect")); err != nil {
		return nil, nil, err
	}
	if m.cacheCut, err = meter.Int64Counter("monitor_cache_truncated_total",
		metric.WithDescription("cache loads that hit an in-memory limit, by cache; entries beyond the limit are not in effect")); err != nil {
		return nil, nil, err
	}
	if m.cycles, err = meter.Int64Counter("monitor_inhibit_cycle_alerts_total",
		metric.WithDescription("alerts treated as not inhibited because inhibit rules form a cycle, summed over snapshot refreshes")); err != nil {
		return nil, nil, err
	}
	if m.records, err = meter.Int64Counter("monitor_record_runs_total", metric.WithDescription("record rule runs by result")); err != nil {
		return nil, nil, err
	}
	if m.recTime, err = meter.Float64Histogram("monitor_record_duration_seconds", metric.WithUnit("s"),
		metric.WithDescription("duration of one record rule run (query + remote write)"),
		metric.WithExplicitBucketBoundaries(durationBuckets...)); err != nil {
		return nil, nil, err
	}
	if m.recOut, err = meter.Int64Counter("monitor_record_series_total", metric.WithDescription("series written back by record rules")); err != nil {
		return nil, nil, err
	}
	if m.commands, err = meter.Int64Counter("monitor_alert_commands_total", metric.WithDescription("alert commands handled by result")); err != nil {
		return nil, nil, err
	}
	if m.engineEv, err = meter.Int64Counter("monitor_engine_events_total", metric.WithDescription("self-monitoring events queued by type")); err != nil {
		return nil, nil, err
	}
	return m, func() { _ = provider.Shutdown(context.Background()) }, nil
}

// Meter 返回引擎的 OTel Meter，供后续用例登记自己的指标（名称须带 monitor_ 前缀）。
func (m *Metrics) Meter() metric.Meter { return m.provider.Meter("reborn/monitor") }

// Record implements loop.Recorder.
func (m *Metrics) Record(ctx context.Context, name string, items int, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.loopRuns.Add(ctx, 1, metric.WithAttributes(attribute.String("loop", name), attribute.String("result", result)))
	if items > 0 {
		m.items.Add(ctx, int64(items), metric.WithAttributes(attribute.String("loop", name)))
	}
}

// ObserveEval implements biz.Observer.
func (m *Metrics) ObserveEval(ctx context.Context, result string, duration, lag time.Duration) {
	m.evals.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
	m.evalTime.Record(ctx, duration.Seconds())
	m.evalLag.Record(ctx, max(lag, 0).Seconds())
}

// AddOutbox implements biz.Observer.
func (m *Metrics) AddOutbox(ctx context.Context, kind int32, n int) {
	label := outboxKinds[0]
	if kind > 0 && int(kind) < len(outboxKinds) {
		label = outboxKinds[kind]
	}
	m.outbox.Add(ctx, int64(n), metric.WithAttributes(attribute.String("kind", label)))
}

// AddMutesTruncated implements biz.Observer.
func (m *Metrics) AddMutesTruncated(ctx context.Context) { m.mutesCut.Add(ctx, 1) }

// AddTruncated implements biz.Observer.
func (m *Metrics) AddTruncated(ctx context.Context, what string) {
	m.cacheCut.Add(ctx, 1, metric.WithAttributes(attribute.String("cache", what)))
}

// AddInhibitCycles implements biz.Observer.
func (m *Metrics) AddInhibitCycles(ctx context.Context, n int) { m.cycles.Add(ctx, int64(n)) }

// ObserveRecord implements biz.Observer.
func (m *Metrics) ObserveRecord(ctx context.Context, result string, duration time.Duration, series int) {
	m.records.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
	m.recTime.Record(ctx, duration.Seconds())
	if series > 0 {
		m.recOut.Add(ctx, int64(series))
	}
}

// AddCommand implements biz.Observer.
func (m *Metrics) AddCommand(ctx context.Context, result string) {
	m.commands.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

// AddEngineEvents implements biz.Observer.
func (m *Metrics) AddEngineEvents(ctx context.Context, typ string, n int) {
	m.engineEv.Add(ctx, int64(n), metric.WithAttributes(attribute.String("type", typ)))
}

// NewHTTPServer HTTP 服务器：只注册 /healthz 与 /metrics，没有业务路由。
// 中间件：recovery → logging。
func NewHTTPServer(c *conf.Server, logger *slog.Logger, pinger biz.Pinger) *khttp.Server {
	opts := []khttp.ServerOption{
		khttp.Middleware(
			recovery.Recovery(),
			logging.Server(logger),
		),
		khttp.Filter(httpx.LimitRequestBody),
	}
	if c.GetHttp().GetNetwork() != "" {
		opts = append(opts, khttp.Network(c.GetHttp().GetNetwork()))
	}
	if c.GetHttp().GetAddr() != "" {
		opts = append(opts, khttp.Address(c.GetHttp().GetAddr()))
	}
	if c.GetHttp().GetTimeout() != nil {
		opts = append(opts, khttp.Timeout(c.GetHttp().GetTimeout().AsDuration()))
	}
	srv := khttp.NewServer(opts...)
	httpx.HardenServer(srv)
	srv.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if err := pinger.Ping(); err != nil {
			http.Error(w, "database unreachable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	srv.Handle("/metrics", promhttp.Handler())
	return srv
}

// NewServers 按角色装配 kratos 托管的全部 Server：HTTP 恒有；规划器与评估器循环按角色门控。
func NewServers(roles Roles, logger *slog.Logger, hs *khttp.Server, workers *worker.Set) []transport.Server {
	servers := []transport.Server{hs}
	for _, l := range workers.Always {
		servers = append(servers, l)
	}
	planner, evaluators := 0, 0
	if roles.Has(RolePlanner) {
		for _, l := range workers.Planner {
			servers = append(servers, l)
		}
		planner = len(workers.Planner)
	}
	if roles.Has(RoleEvaluator) {
		for _, l := range workers.Evaluators {
			servers = append(servers, l)
		}
		evaluators = len(workers.Evaluators)
	}
	logger.Info("monitor engine roles enabled", "roles", roles.List(), "planner_loops", planner, "evaluator_loops", evaluators)
	return servers
}
