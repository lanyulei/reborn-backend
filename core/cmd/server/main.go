// Package main 组装 monitor-backend 服务：仅做配置加载、日志装配与 wire 注入，
// 不包含任何业务分支（见 AGENTS.md §1 分层契约）。
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/httpx"

	"github.com/go-kratos/kratos/v3"
	"github.com/go-kratos/kratos/v3/config"
	"github.com/go-kratos/kratos/v3/config/env"
	"github.com/go-kratos/kratos/v3/config/file"
	"github.com/go-kratos/kratos/v3/log"
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

// newApp 组装 kratos.App；只做装配（仅 HTTP 服务器）。
func newApp(logger *slog.Logger, hs *http.Server) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(hs),
	)
}

func main() {
	flag.Parse()

	// 日志：slog JSON handler 经 Kratos 包装；敏感字段在 handler 构建处统一脱敏
	//（FilterKey 按属性键过滤 + ReplaceAttr 对 args 内嵌凭据按内容打码），
	// 并附加 trace_id/span_id（由 tracing 中间件写入 ctx 后提取）。
	logger := httpx.NewLogger(httpx.JSONHandler(os.Stdout), id, Name, Version)
	log.SetDefault(logger)

	c := config.New(
		config.WithSource(
			file.NewSource(flagconf),
			env.NewSource("KRATOS"),
		),
	)
	defer func() {
		if err := c.Close(); err != nil {
			logger.Error("close config source failed", "err", err)
		}
	}()

	if err := c.Load(); err != nil {
		panic(err)
	}

	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		panic(err)
	}

	app, cleanup, err := wireApp(bc.Server, bc.Data, bc.Security, bc.Notify, bc.Assistant, logger)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	// start and wait for stop signal
	if err := app.Run(); err != nil {
		panic(err)
	}
}
