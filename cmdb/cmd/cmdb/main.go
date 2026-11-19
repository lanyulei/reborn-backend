// Package main 组装 CMDB 数据面服务：配置加载、日志装配与 wire 注入，不含业务分支。
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/lanyulei/reborn/backend/cmdb/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/bootstrap"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/keycheck"

	"github.com/go-kratos/kratos/v3"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-kratos/kratos/v3/transport"

	_ "go.uber.org/automaxprocs"
)

// go build -ldflags "-X main.Name=... -X main.Version=..."
var (
	// Name is the name of the compiled software.
	Name = "cmdb"
	// Version is the version of the compiled software.
	Version string
	// flagconf is the config path flag.
	flagconf string

	id, _ = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
}

// newApp 组装 kratos.App：servers 已按角色门控（server.NewServers）。
func newApp(logger *slog.Logger, servers []transport.Server) *kratos.App {
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

	logger := httpx.NewLogger(httpx.JSONHandler(os.Stdout), id, Name, Version)
	log.SetDefault(logger)

	// 配置：kit 内嵌的共享基线 → 本服务 configs → KRATOS_ 环境变量（后者覆盖前者）。
	var bc conf.Bootstrap
	if err := bootstrap.Load(flagconf, &bc); err != nil {
		panic(err)
	}
	// 共享密钥核对：与 core 登记的指纹比对，不一致直接中止启动。
	if err := keycheck.Startup(context.Background(), logger, bc.GetData().GetDatabase().GetSource(), false, keycheck.Key{Name: keycheck.Master, Secret: bc.GetNotify().GetMaster().GetKey()}); err != nil {
		panic(err)
	}
	app, cleanup, err := wireApp(bc.Server, bc.Data, bc.Sync, bc.Notify, logger)
	if err != nil {
		panic(err)
	}
	defer cleanup()
	if err := app.Run(); err != nil {
		panic(err)
	}
}
