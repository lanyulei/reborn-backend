//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"log/slog"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/k8s/internal/conf"
	"github.com/lanyulei/reborn/backend/k8s/internal/data"
	"github.com/lanyulei/reborn/backend/k8s/internal/server"
	"github.com/lanyulei/reborn/backend/k8s/internal/service"
	"github.com/lanyulei/reborn/backend/k8s/internal/worker"

	"github.com/go-kratos/kratos/v3"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, *conf.Plane, *conf.Notify, *slog.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, biz.ProviderSet, service.ProviderSet, worker.ProviderSet, newApp))
}
