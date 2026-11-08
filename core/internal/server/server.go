// Package server 构造传输层服务器：路由注册、中间件挂载与编解码配置。
// HTTP 与 gRPC 的中间件链相互独立，需分别挂载（AGENTS.md §5）。
package server

import "github.com/google/wire"

// ProviderSet is server providers.
var ProviderSet = wire.NewSet(NewHTTPServer, NewGRPCServer)
