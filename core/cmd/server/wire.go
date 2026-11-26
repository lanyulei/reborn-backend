//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"log/slog"

	"github.com/lanyulei/reborn/backend/core/internal/biz"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/data"
	dataagent "github.com/lanyulei/reborn/backend/core/internal/data/agent"
	dataassistant "github.com/lanyulei/reborn/backend/core/internal/data/assistant"
	datacmdb "github.com/lanyulei/reborn/backend/core/internal/data/cmdb"
	datafile "github.com/lanyulei/reborn/backend/core/internal/data/file"
	dataincident "github.com/lanyulei/reborn/backend/core/internal/data/incident"
	datajob "github.com/lanyulei/reborn/backend/core/internal/data/job"
	datak8s "github.com/lanyulei/reborn/backend/core/internal/data/k8s"
	dataknowledge "github.com/lanyulei/reborn/backend/core/internal/data/knowledge"
	datamonitor "github.com/lanyulei/reborn/backend/core/internal/data/monitor"
	datanotify "github.com/lanyulei/reborn/backend/core/internal/data/notify"
	dataoncall "github.com/lanyulei/reborn/backend/core/internal/data/oncall"
	datasystem "github.com/lanyulei/reborn/backend/core/internal/data/system"
	dataticket "github.com/lanyulei/reborn/backend/core/internal/data/ticket"
	dataworkflow "github.com/lanyulei/reborn/backend/core/internal/data/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/server"
	"github.com/lanyulei/reborn/backend/core/internal/service"
	serviceoncall "github.com/lanyulei/reborn/backend/core/internal/service/oncall"
	servicesystem "github.com/lanyulei/reborn/backend/core/internal/service/system"
	serviceworkflow "github.com/lanyulei/reborn/backend/core/internal/service/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/worker"

	"github.com/go-kratos/kratos/v3"
	"github.com/google/wire"
)

// wireApp init kratos application.
// data 层因共享 Data 类型不能在顶层聚合各域（避免 import 环），
// 各域 data ProviderSet 在此显式列出；biz/service 顶层已聚合。
// service 层消费的 biz 窄接口（AdminResolver ← UserUsecase）在组合根绑定，避免 biz 反向依赖 service。
// server 消费的 data 层能力（存活探测 Pinger、表增长观测 TableStats）同在此绑定：两层互不 import。
func wireApp(*conf.Server, *conf.Data, *conf.Security, *conf.Notify, *conf.Assistant, *conf.Monitor, *conf.K8S, *conf.Knowledge, *conf.Agent, *conf.Oncall, *slog.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, datasystem.ProviderSet, datanotify.ProviderSet, dataassistant.ProviderSet, datacmdb.ProviderSet, dataoncall.ProviderSet, datafile.ProviderSet, dataworkflow.ProviderSet, dataticket.ProviderSet, datamonitor.ProviderSet, datajob.ProviderSet, datak8s.ProviderSet, dataknowledge.ProviderSet, dataincident.ProviderSet, dataagent.ProviderSet, biz.ProviderSet, service.ProviderSet, worker.ProviderSet,
		wire.Bind(new(servicesystem.AdminResolver), new(*bizsystem.UserUsecase)),
		wire.Bind(new(serviceoncall.AdminResolver), new(*bizsystem.UserUsecase)),
		wire.Bind(new(serviceworkflow.PermissionChecker), new(*bizsystem.AuthzUsecase)),
		wire.Bind(new(server.Pinger), new(*data.Data)),
		data.NewTableStatsRepo, wire.Bind(new(server.TableStats), new(*data.TableStatsRepo)),
		newApp))
}
