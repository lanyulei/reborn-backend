// Package server 构造传输层与后台组件的装配：角色门控（SERVER_ROLES）、健康检查 / 指标监听、
// 引擎面向执行节点的协议监听、指标。
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/conf"
	"github.com/lanyulei/reborn/backend/job/internal/runner"
	"github.com/lanyulei/reborn/backend/job/internal/service"
	"github.com/lanyulei/reborn/backend/job/internal/worker"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/loop"

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
	conf.NewRoles, NewMetrics, wire.Bind(new(loop.Recorder), new(*Metrics)), wire.Bind(new(biz.Observer), new(*Metrics)),
	NewHTTPServer, NewRunnerAPIServer, NewRunner, NewServers,
)

const (
	// defaultRunnerAPIAddr 执行节点协议监听的缺省地址。
	defaultRunnerAPIAddr = "0.0.0.0:8051"
	// defaultRunnerAPITimeout 执行节点协议单个请求的处理超时（上报可带数百 KB 日志并写库）。
	defaultRunnerAPITimeout = 15 * time.Second
	// 连接级读写超时：请求体至多 8MiB，领取应答至多约 6MiB（带着待分发的文件），慢连接不得长期占住处理协程。
	// 写超时与执行节点的领取超时一致（60s）。
	runnerAPIReadTimeout  = 30 * time.Second
	runnerAPIWriteTimeout = 60 * time.Second
)

// Metrics 引擎可观测指标（OTel → Prometheus /metrics），指标名前缀 job_。标签只有固定枚举值，
// 不含执行 ID、节点 ID 等高基数或敏感内容。
type Metrics struct {
	provider *sdkmetric.MeterProvider
	loopRuns metric.Int64Counter
	items    metric.Int64Counter
	calls    metric.Int64Counter
	execs    metric.Int64Counter
}

var _ biz.Observer = (*Metrics)(nil)

// NewMetrics 构造指标与导出器。
func NewMetrics() (*Metrics, func(), error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, nil, fmt.Errorf("init prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("reborn/job")
	m := &Metrics{provider: provider}
	if m.loopRuns, err = meter.Int64Counter("job_loop_runs_total", metric.WithDescription("worker loop iterations by loop and result")); err != nil {
		return nil, nil, err
	}
	if m.items, err = meter.Int64Counter("job_loop_items_total", metric.WithDescription("items processed by worker loop")); err != nil {
		return nil, nil, err
	}
	if m.calls, err = meter.Int64Counter("job_runner_api_requests_total", metric.WithDescription("runner protocol requests by operation and result")); err != nil {
		return nil, nil, err
	}
	if m.execs, err = meter.Int64Counter("job_executions_total", metric.WithDescription("execution state events by event")); err != nil {
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

// ObserveCall implements biz.Observer.
func (m *Metrics) ObserveCall(ctx context.Context, op, result string) {
	m.calls.Add(ctx, 1, metric.WithAttributes(attribute.String("op", op), attribute.String("result", result)))
}

// AddExecutions implements biz.Observer.
func (m *Metrics) AddExecutions(ctx context.Context, event string, n int) {
	m.execs.Add(ctx, int64(n), metric.WithAttributes(attribute.String("event", event)))
}

// NewHTTPServer 健康检查 / 指标监听：只注册 /healthz（就绪）、/livez（存活）与 /metrics，没有业务路由；两种角色都启动。
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
	srv.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pinger.Ping(); err != nil {
			// 该接口不鉴权：驱动的错误原文（主机名、账号、库名）只进日志，不进应答。
			logger.WarnContext(r.Context(), "health check failed: database unreachable", "error", err)
			http.Error(w, "database unreachable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	// /livez 只说明进程活着，不查数据库：存活探针用它，/healthz 只作就绪探针——否则数据库一抖动，
	// 所有实例都会被重启，在途作业跟着被杀。
	srv.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	srv.Handle("/metrics", promhttp.Handler())
	return srv
}

// RunnerAPIServer 引擎面向执行节点的协议监听（独立端口，可单独对执行节点所在网络开放）。
type RunnerAPIServer struct {
	*khttp.Server
}

// NewRunnerAPIServer 构造执行节点协议监听：只注册 kit/jobcontract 的四个接口；请求体上限 8MiB；
// 连接级超时加固；配置了证书则启用 TLS（1.2+），否则明文 HTTP（仅限开发或已有外层 TLS 终结）。
// 鉴权在 service 层逐请求完成（Bearer 接入令牌）；不挂请求日志中间件——请求与应答里有凭据。
// 未启用 engine 角色时返回 nil（不监听、不读证书）。
func NewRunnerAPIServer(roles conf.Roles, c *conf.Server, svc *service.RunnerService, logger *slog.Logger) (*RunnerAPIServer, error) {
	if !roles.Has(conf.RoleEngine) {
		return nil, nil
	}
	rc := c.GetRunner()
	addr := rc.GetAddr()
	if addr == "" {
		addr = defaultRunnerAPIAddr
	}
	timeout := defaultRunnerAPITimeout
	if d := rc.GetTimeout().AsDuration(); d > 0 {
		timeout = d
	}
	opts := []khttp.ServerOption{
		khttp.Address(addr),
		khttp.Timeout(timeout),
		khttp.Filter(httpx.LimitRequestBody),
	}
	if rc.GetNetwork() != "" {
		opts = append(opts, khttp.Network(rc.GetNetwork()))
	}
	cert, key := rc.GetCertFile(), rc.GetKeyFile()
	switch {
	case cert != "" && key != "":
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("load runner api tls certificate: %w", err)
		}
		opts = append(opts, khttp.TLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}))
	case cert != "" || key != "":
		return nil, errors.New("server.runner.cert_file and server.runner.key_file must be set together")
	default:
		logger.Warn("runner api serves plain HTTP: runner tokens and credentials travel unencrypted; configure server.runner.cert_file/key_file or terminate TLS in front of it")
	}
	srv := khttp.NewServer(opts...)
	httpx.HardenServer(srv)
	srv.ReadTimeout = runnerAPIReadTimeout
	srv.WriteTimeout = runnerAPIWriteTimeout
	for path, h := range svc.Routes() {
		srv.HandleFunc(path, h)
	}
	return &RunnerAPIServer{Server: srv}, nil
}

// afterRunner 两种角色同在一个进程时包住执行节点协议监听：停机时先等本进程的执行节点停完
// （它的在途作业收尾时还要向这个监听上报），再关闭监听。
type afterRunner struct {
	transport.Server
	runner *runner.Runner
}

// Stop implements transport.Server.
func (s *afterRunner) Stop(ctx context.Context) error {
	s.runner.Wait(ctx)
	return s.Server.Stop(ctx)
}

// NewRunner 构造执行节点；未启用 runner 角色时返回 nil（不校验 runner 配置：纯引擎部署不需要它）。
func NewRunner(roles conf.Roles, c *conf.Runner, version runner.AgentVersion, logger *slog.Logger) (*runner.Runner, error) {
	if !roles.Has(conf.RoleRunner) {
		return nil, nil
	}
	cfg, err := runner.NewConfig(c, version)
	if err != nil {
		return nil, err
	}
	return runner.New(cfg, logger)
}

// NewServers 按角色装配 kratos 托管的全部 Server：健康检查 / 指标监听恒有；engine 角色加执行节点
// 协议监听与后台循环；runner 角色加执行节点。
func NewServers(roles conf.Roles, logger *slog.Logger, hs *khttp.Server, api *RunnerAPIServer, workers *worker.Set, rn *runner.Runner) []transport.Server {
	servers := []transport.Server{hs}
	loops := 0
	if roles.Has(conf.RoleEngine) && api != nil {
		if rn != nil {
			servers = append(servers, &afterRunner{Server: api, runner: rn})
		} else {
			servers = append(servers, api)
		}
		for _, l := range workers.Engine {
			servers = append(servers, l)
		}
		loops = len(workers.Engine)
	}
	if roles.Has(conf.RoleRunner) && rn != nil {
		servers = append(servers, rn)
	}
	logger.Info("job roles enabled", "roles", roles.List(), "engine_loops", loops)
	return servers
}
