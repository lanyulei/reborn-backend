package server

import (
	"log/slog"

	"github.com/lanyulei/reborn/backend/notify/internal/worker"

	"github.com/go-kratos/kratos/v3/transport"
	khttp "github.com/go-kratos/kratos/v3/transport/http"
	"github.com/google/wire"
)

// ProviderSet is server providers.
var ProviderSet = wire.NewSet(NewRoles, NewMetrics, NewHooks, NewProducerResolver, NewHealth, NewHTTPServer, NewServers)

// NewServers 按角色装配 kratos 托管的全部 Server：HTTP 恒有（ingress 仅 api 角色
// 注册；数据面只承诺 HTTP——HMAC 签名绑定请求体，gRPC 无法等价绑定故不暴露）；
// worker 按角色门控（配置刷新循环随任一角色启用）。
func NewServers(roles Roles, logger *slog.Logger, hs *khttp.Server, workers *worker.Set) []transport.Server {
	servers := []transport.Server{hs, workers.ConfigRefresher}
	if roles.Has(RoleRouter) {
		servers = append(servers, workers.Router, workers.Sweeper)
	}
	if roles.Has(RoleSender) {
		servers = append(servers, workers.Sender)
	}
	if roles.Has(RoleDlqWorker) {
		servers = append(servers, workers.Dlq)
	}
	if roles.Has(RoleMaintenance) {
		servers = append(servers, workers.Maintenance)
	}
	if roles.Has(RoleRelay) {
		logger.Warn("role relay is reserved for the NATS backbone and is a no-op under the pg backbone")
	}
	logger.Info("pipeline roles enabled", "roles", roles.List())
	return servers
}
