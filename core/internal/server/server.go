// Package server 构造传输层服务器：路由注册、中间件挂载与编解码配置。
// 仅 HTTP 一种协议（gRPC 已停用：无调用方；proto 保留，需要时加回生成插件
// 与 grpc.go 即可恢复）。
package server

import (
	bizagent "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/google/wire"
)

// ProviderSet is server providers.
var ProviderSet = wire.NewSet(NewRoles, NewMetrics, wire.Bind(new(loop.Recorder), new(*Metrics)), wire.Bind(new(bizagent.Metrics), new(*Metrics)), NewHTTPServer)
