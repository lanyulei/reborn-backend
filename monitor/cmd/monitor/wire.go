//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"log/slog"

	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
	"github.com/lanyulei/reborn/backend/monitor/internal/conf"
	"github.com/lanyulei/reborn/backend/monitor/internal/data"
	"github.com/lanyulei/reborn/backend/monitor/internal/server"
	"github.com/lanyulei/reborn/backend/monitor/internal/worker"

	"github.com/go-kratos/kratos/v3"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, *conf.Engine, *conf.Notify, server.AppVersion, *slog.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, biz.ProviderSet, worker.ProviderSet, newApp))
}
