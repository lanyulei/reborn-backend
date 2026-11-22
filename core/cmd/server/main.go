// Package main 组装 monitor-backend 服务：仅做配置加载、日志装配与 wire 注入，
// 不包含任何业务分支（见 AGENTS.md §1 分层契约）。
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"slices"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/server"
	"github.com/lanyulei/reborn/backend/core/internal/worker"
	"github.com/lanyulei/reborn/backend/kit/bootstrap"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/keycheck"

	"github.com/go-kratos/kratos/v3"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-kratos/kratos/v3/transport"
	"github.com/go-kratos/kratos/v3/transport/http"

	_ "go.uber.org/automaxprocs"
)

// go build -ldflags "-X main.Name=... -X main.Version=..."
var (
	// Name is the name of the compiled software.
	Name string
	// Version is the version of the compiled software.
	Version string
	// flagconf is the config path flag.
	flagconf string

	id, _ = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
}

// newApp 组装 kratos.App；只做装配（HTTP 服务器 + 值班 / 文件清理 / 流程作业 / 工单 SLA 后台循环）。
// 后台循环只在 worker 角色下启动；HTTP 服务器恒有（非 api 角色时只剩探活与指标端点）。
func newApp(logger *slog.Logger, roles server.Roles, hs *http.Server, oncall *worker.OncallLoops, files *worker.FileLoops,
	workflows *worker.WorkflowLoops, tickets *worker.TicketLoops, monitors *worker.MonitorLoops, k8s *worker.K8sLoops, jobs *worker.JobLoops,
	knowledge *worker.KnowledgeLoops, incidents *worker.IncidentLoops, agents *worker.AgentLoops,
) *kratos.App {
	servers := []transport.Server{hs}
	if roles.Has(server.RoleWorker) {
		servers = slices.Concat(servers, oncall.Servers(), files.Servers(), workflows.Servers(), tickets.Servers(), monitors.Servers(), k8s.Servers(), jobs.Servers(),
			knowledge.Servers(), incidents.Servers(), agents.Servers())
	}
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(servers...),
	)
}

func main() {
	flag.Parse()

	// 日志：slog JSON handler 经 Kratos 包装；敏感字段在 handler 构建处统一脱敏
	//（FilterKey 按属性键过滤 + ReplaceAttr 对 args 内嵌凭据按内容打码），
	// 并附加 trace_id/span_id（由 tracing 中间件写入 ctx 后提取）。
	logger := httpx.NewLogger(httpx.JSONHandler(os.Stdout), id, Name, Version)
	log.SetDefault(logger)

	// 配置：kit 内嵌的共享基线 → 本服务 configs → KRATOS_ 环境变量（后者覆盖前者）。
	var bc conf.Bootstrap
	if err := bootstrap.Load(flagconf, &bc); err != nil {
		panic(err)
	}
	// 共享密钥核对：core 负责登记指纹（首次写入为准），其余服务启动时比对；不一致直接中止启动。
	if err := keycheck.Startup(context.Background(), logger, bc.GetData().GetDatabase().GetSource(), true,
		keycheck.Key{Name: keycheck.Master, Secret: bc.GetNotify().GetMaster().GetKey()},
		keycheck.Key{Name: keycheck.K8sInternal, Secret: bc.GetK8S().GetInternalKey()},
	); err != nil {
		panic(err)
	}

	app, cleanup, err := wireApp(bc.Server, bc.Data, bc.Security, bc.Notify, bc.Assistant, bc.Monitor, bc.K8S, bc.Knowledge, bc.Agent, logger)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	// start and wait for stop signal
	if err := app.Run(); err != nil {
		panic(err)
	}
}
