// Package server 构造传输层与后台组件的装配：内部接口监听（core → 数据面）、Agent 隧道入口监听、
// 健康检查与指标。
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/k8s/internal/conf"
	"github.com/lanyulei/reborn/backend/k8s/internal/data/kube"
	"github.com/lanyulei/reborn/backend/k8s/internal/service"
	"github.com/lanyulei/reborn/backend/k8s/internal/worker"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/loop"

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
	NewMetrics, wire.Bind(new(loop.Recorder), new(*Metrics)), wire.Bind(new(biz.Observer), new(*Metrics)),
	NewInternalKey, NewRegistryOptions, NewHTTPServer, NewServers,
	NewTunnelServer, NewTunnelBackend,
	NewGovSetup,
)

// defaultHTTPAddr 内部接口监听的缺省地址。
const defaultHTTPAddr = "0.0.0.0:8060"

// defaultHTTPTimeout 普通请求的处理超时；流式接口不受它限制（见 service 层）。
const defaultHTTPTimeout = 30 * time.Second

// Metrics 数据面可观测指标（OTel → Prometheus /metrics），指标名前缀 k8s_。标签只有固定枚举值，
// 不含用户、对象名等高基数或敏感内容。
type Metrics struct {
	provider  *sdkmetric.MeterProvider
	meter     metric.Meter
	loopRuns  metric.Int64Counter
	items     metric.Int64Counter
	auditFail metric.Int64Counter
	streams   metric.Int64UpDownCounter
}

var _ biz.Observer = (*Metrics)(nil)

// observe 登记按需采集的指标：各资源类型的缓存对象数、进程堆内存。
func (m *Metrics) observe(kubes *kube.Manager) error {
	cached, err := m.meter.Int64ObservableGauge("k8s_cache_objects", metric.WithDescription("cached objects by resource type, all owned clusters"))
	if err != nil {
		return err
	}
	heap, err := m.meter.Int64ObservableGauge("k8s_heap_bytes", metric.WithDescription("live heap bytes of the process"))
	if err != nil {
		return err
	}
	_, err = m.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for res, n := range kubes.CacheStats() {
			o.ObserveInt64(cached, int64(n), metric.WithAttributes(attribute.String("resource", res)))
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		o.ObserveInt64(heap, int64(ms.HeapAlloc)) //nolint:gosec // 堆大小不会溢出 int64
		return nil
	}, cached, heap)
	return err
}

// NewMetrics 构造指标与导出器。
func NewMetrics() (*Metrics, func(), error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, nil, fmt.Errorf("init prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("reborn/k8s")
	m := &Metrics{provider: provider, meter: meter}
	if m.loopRuns, err = meter.Int64Counter("k8s_loop_runs_total", metric.WithDescription("worker loop iterations by loop and result")); err != nil {
		return nil, nil, err
	}
	if m.items, err = meter.Int64Counter("k8s_loop_items_total", metric.WithDescription("items processed by worker loop")); err != nil {
		return nil, nil, err
	}
	if m.auditFail, err = meter.Int64Counter("k8s_audit_write_failures_total", metric.WithDescription("audit rows that could not be written")); err != nil {
		return nil, nil, err
	}
	if m.streams, err = meter.Int64UpDownCounter("k8s_open_streams", metric.WithDescription("open streams and terminal sessions by kind")); err != nil {
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

// AuditFailed implements biz.Observer.
func (m *Metrics) AuditFailed() { m.auditFail.Add(context.Background(), 1) }

// StreamDelta implements biz.Observer.
func (m *Metrics) StreamDelta(kind string, delta int) {
	m.streams.Add(context.Background(), int64(delta), metric.WithAttributes(attribute.String("kind", kind)))
}

// NewInternalKey 内部密钥（plane.internal_key）。
func NewInternalKey(c *conf.Plane) service.InternalKey {
	return service.InternalKey(c.GetInternalKey())
}

// NewHTTPServer 内部接口监听：/healthz、/metrics 与 /internal/v1/*。
// 不挂请求日志中间件——请求体里有 YAML 与 Secret 内容；审计由业务层写库。
func NewHTTPServer(c *conf.Server, pinger biz.Pinger, internal *service.InternalService, gov *service.GovService, _ GovSetup) *khttp.Server {
	addr := c.GetHttp().GetAddr()
	if addr == "" {
		addr = defaultHTTPAddr
	}
	timeout := defaultHTTPTimeout
	if d := c.GetHttp().GetTimeout().AsDuration(); d > 0 {
		timeout = d
	}
	opts := []khttp.ServerOption{
		khttp.Address(addr),
		khttp.Timeout(timeout),
		khttp.Middleware(recovery.Recovery()),
		khttp.Filter(service.KeepConnContext, limitBody),
	}
	if c.GetHttp().GetNetwork() != "" {
		opts = append(opts, khttp.Network(c.GetHttp().GetNetwork()))
	}
	srv := khttp.NewServer(opts...)
	httpx.HardenServer(srv)
	srv.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if err := pinger.Ping(); err != nil {
			http.Error(w, "database unreachable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	srv.Handle("/metrics", promhttp.Handler())
	registerPprof(srv)
	internal.Register(srv.HandlePrefix, srv.Handle)
	gov.Register(srv.HandlePrefix, srv.Handle)
	return srv
}

// NewRegistryOptions 实例 ID（主机名 + 启动随机串）与 core 访问本实例的地址。
func NewRegistryOptions(c *conf.Server, p *conf.Plane) (biz.RegistryOptions, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "k8s"
	}
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return biz.RegistryOptions{}, err
	}
	if len(host) > 48 {
		host = host[:48]
	}
	advertise := strings.TrimRight(c.GetHttp().GetAdvertiseAddr(), "/")
	if advertise == "" {
		addr := c.GetHttp().GetAddr()
		if addr == "" {
			addr = defaultHTTPAddr
		}
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return biz.RegistryOptions{}, fmt.Errorf("server.http.addr: %w", err)
		}
		advertise = "http://" + net.JoinHostPort(host, port)
	}
	return biz.RegistryOptions{
		InstanceID: host + "-" + hex.EncodeToString(buf), Addr: advertise,
		Lease: p.GetLease().AsDuration(), MaxClusters: int(p.GetMaxClusters()),
	}, nil
}

// lifecycle 进程级的启动与收尾：启动时注入归属参数并登记回调；停止时结束终端会话（会话记录落库）、
// 放弃集群归属、删除实例登记。
type lifecycle struct {
	registry *biz.Registry
	exec     *biz.ExecManager
	done     chan struct{}
}

// Start implements transport.Server.
func (l *lifecycle) Start(context.Context) error {
	<-l.done
	return nil
}

// Stop implements transport.Server.
func (l *lifecycle) Stop(ctx context.Context) error {
	// 两步各用自己的时限：终端会话收尾慢（会话记录、录制落库）不应挤掉「放弃归属」的时间——
	// 归属放不掉，其他实例要等租约过期才能接手。
	ectx, ecancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	l.exec.Shutdown(ectx)
	ecancel()
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	l.registry.Shutdown(sctx)
	close(l.done)
	return nil
}

// NewServers 装配 kratos 托管的全部 Server：内部接口监听、后台循环与收尾钩子。
// kratos 逆序停止并不保证，故收尾钩子自身不依赖其他 Server 仍在运行。
func NewServers(hs *khttp.Server, workers *worker.Set, registry *biz.Registry, scopes *biz.ScopeSyncer, exec *biz.ExecManager, kubes *kube.Manager,
	opts biz.RegistryOptions, metrics *Metrics, logger *slog.Logger,
	tun *TunnelServer, agents *biz.AgentKeeper, agentLoops *worker.AgentLoops, govLoops *worker.GovLoops,
) ([]transport.Server, error) {
	registry.WithOptions(opts)
	registry.WithAgents(agents)
	registry.OnScopeChange(kubes.ClearAccess)
	scopes.OnApplied(kubes.ClearClusterAccess)
	if err := metrics.observe(kubes); err != nil {
		return nil, err
	}
	servers := []transport.Server{hs, &lifecycle{registry: registry, exec: exec, done: make(chan struct{})}}
	for _, l := range workers.Loops {
		servers = append(servers, l)
	}
	servers = append(servers, tun)
	for _, l := range agentLoops.Loops {
		servers = append(servers, l)
	}
	for _, l := range govLoops.Loops {
		servers = append(servers, l)
	}
	logger.Info("k8s data plane starting", "instance", opts.InstanceID, "advertise_addr", opts.Addr, "loops", len(workers.Loops))
	return servers, nil
}
