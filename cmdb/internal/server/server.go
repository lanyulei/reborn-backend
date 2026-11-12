// Package server 构造传输层与后台 worker 的装配：角色门控（SERVER_ROLES）、HTTP
// （仅 /healthz /metrics）、指标。数据面不暴露业务接口。
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	webhookv1 "github.com/lanyulei/reborn/backend/cmdb/api/webhook/v1"
	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/cmdb/internal/conf"
	"github.com/lanyulei/reborn/backend/cmdb/internal/service"
	"github.com/lanyulei/reborn/backend/cmdb/internal/worker"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/middleware/logging"
	"github.com/go-kratos/kratos/v3/middleware/ratelimit"
	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/middleware/validate"
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
var ProviderSet = wire.NewSet(NewRoles, NewMetrics, wire.Bind(new(loop.Recorder), new(*Metrics)), NewHTTPServer, NewServers)

// 角色名。
const (
	RoleScheduler = "scheduler"
	RoleWorker    = "worker"
	RoleWebhook   = "webhook"
)

// Roles 启用的角色集合。
type Roles map[string]struct{}

// NewRoles 解析角色：环境变量 SERVER_ROLES 优先，其次 conf.sync.roles，两者皆空 = 全部角色。
func NewRoles(c *conf.Sync) Roles {
	list := c.GetRoles()
	if env := strings.TrimSpace(os.Getenv("SERVER_ROLES")); env != "" {
		list = strings.Split(env, ",")
	}
	roles := Roles{}
	for _, r := range list {
		if r = strings.TrimSpace(strings.ToLower(r)); r != "" {
			roles[r] = struct{}{}
		}
	}
	if len(roles) == 0 {
		roles[RoleScheduler], roles[RoleWorker], roles[RoleWebhook] = struct{}{}, struct{}{}, struct{}{}
	}
	return roles
}

// Has 报告角色是否启用。
func (r Roles) Has(role string) bool {
	_, ok := r[role]
	return ok
}

// List 按固定顺序列出已启用角色（日志用）。
func (r Roles) List() []string {
	out := make([]string, 0, 2)
	for _, role := range []string{RoleScheduler, RoleWorker, RoleWebhook} {
		if r.Has(role) {
			out = append(out, role)
		}
	}
	return out
}

// Metrics 数据面可观测指标（OTel → Prometheus /metrics）。
type Metrics struct {
	provider *sdkmetric.MeterProvider
	loopRuns metric.Int64Counter
	items    metric.Int64Counter
}

// NewMetrics 构造指标与导出器。
func NewMetrics() (*Metrics, func(), error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, nil, fmt.Errorf("init prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("reborn/cmdb")
	m := &Metrics{provider: provider}
	if m.loopRuns, err = meter.Int64Counter("cmdb_loop_runs_total", metric.WithDescription("worker loop iterations by loop and result")); err != nil {
		return nil, nil, err
	}
	if m.items, err = meter.Int64Counter("cmdb_loop_items_total", metric.WithDescription("items processed by worker loop")); err != nil {
		return nil, nil, err
	}
	return m, func() { _ = provider.Shutdown(context.Background()) }, nil
}

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

// NewHTTPServer HTTP 服务器：恒注册 /healthz 与 /metrics；回调接入仅 webhook 角色。
// 中间件：recovery → logging → validate → rate-limit。
func NewHTTPServer(c *conf.Server, roles Roles, logger *slog.Logger, pinger biz.Pinger, webhook *service.WebhookService) *khttp.Server {
	opts := []khttp.ServerOption{
		khttp.Middleware(
			recovery.Recovery(),
			logging.Server(logger),
			validate.Validator(httpx.ProtoValidate),
			ratelimit.Server(),
		),
		khttp.Filter(httpx.LimitRequestBody),
		khttp.RequestDecoder(httpx.DecodeRequest),
		khttp.ResponseEncoder(httpx.EncodeResponse),
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
	if roles.Has(RoleWebhook) {
		webhookv1.RegisterCmdbWebhookServiceHTTPServer(srv, webhook)
	}
	return srv
}

// NewServers 按角色装配 kratos 托管的全部 Server：HTTP 恒有；调度器与执行器按角色门控。
func NewServers(roles Roles, logger *slog.Logger, hs *khttp.Server, workers *worker.Set) []transport.Server {
	servers := []transport.Server{hs}
	if roles.Has(RoleScheduler) {
		servers = append(servers, workers.Scheduler, workers.Maintenance)
	}
	if roles.Has(RoleWorker) {
		for _, w := range workers.Workers {
			servers = append(servers, w)
		}
	}
	logger.Info("cmdb sync roles enabled", "roles", roles.List(), "workers", len(workers.Workers))
	return servers
}
