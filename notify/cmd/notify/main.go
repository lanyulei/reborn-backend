// Package main 组装通知管道服务：配置加载、日志装配与 wire 注入，不含业务分支。
package main

import (
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"

	"github.com/go-kratos/kratos/v3"
	"github.com/go-kratos/kratos/v3/config"
	"github.com/go-kratos/kratos/v3/config/env"
	"github.com/go-kratos/kratos/v3/config/file"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-kratos/kratos/v3/transport"

	_ "go.uber.org/automaxprocs"
)

// go build -ldflags "-X main.Name=... -X main.Version=..."
var (
	// Name is the name of the compiled software.
	Name = "notify"
	// Version is the version of the compiled software.
	Version string
	// flagconf is the config path flag.
	flagconf string

	id, _ = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
}

// stopTimeout 停机总时限：worker 循环在此窗口内跑完在途迭代（sender 一批最坏
// ≈ batch/concurrency × max_send_timeout），超时才硬中止；kratos 缺省无界。
const stopTimeout = 30 * time.Second

// newApp 组装 kratos.App：servers 已按角色门控（server.NewServers）。
func newApp(logger *slog.Logger, servers []transport.Server) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(servers...),
		kratos.StopTimeout(stopTimeout),
	)
}

func main() {
	flag.Parse()

	logger := httpx.NewLogger(httpx.JSONHandler(os.Stdout), id, Name, Version)
	log.SetDefault(logger)

	c := config.New(config.WithSource(file.NewSource(flagconf), env.NewSource("KRATOS")))
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
	app, cleanup, err := wireApp(bc.Server, bc.Data, bc.Pipeline, bc.Notify, logger)
	if err != nil {
		panic(err)
	}
	defer cleanup()
	if err := app.Run(); err != nil {
		panic(err)
	}
}
