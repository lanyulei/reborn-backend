//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"log/slog"

	"github.com/lanyulei/reborn/backend/core/internal/biz"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/data"
	datanotify "github.com/lanyulei/reborn/backend/core/internal/data/notify"
	datasystem "github.com/lanyulei/reborn/backend/core/internal/data/system"
	"github.com/lanyulei/reborn/backend/core/internal/server"
	"github.com/lanyulei/reborn/backend/core/internal/service"

	"github.com/go-kratos/kratos/v3"
	"github.com/google/wire"
)

// wireApp init kratos application.
// data 层因共享 Data 类型不能在顶层聚合各域（避免 import 环），
// 各域 data ProviderSet 在此显式列出；biz/service 顶层已聚合。
func wireApp(*conf.Server, *conf.Data, *conf.Security, *conf.Notify, *slog.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, datasystem.ProviderSet, datanotify.ProviderSet, biz.ProviderSet, service.ProviderSet, newApp))
}
