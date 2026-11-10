// Package biz 承载业务核心：领域模型、仓储接口（依赖倒置）与用例。
// 本包禁止 import 传输层（transport/http、transport/grpc）与任何存储驱动。
//
// 分域结构：core 为单服务多域（模块化单体），各业务域在 internal/biz/<domain>/
// 子包实现（子包包名保持 biz，引用前缀不变）；本包只保留跨域聚合的
// ProviderSet，不放任何业务代码。新增域（如 workflow）时：建子包、
// 子包内定义自己的 ProviderSet，并在下方聚合处追加一行。
package biz

import (
	"github.com/google/wire"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
)

// ProviderSet 聚合各业务域的 biz providers；跨域窄接口在聚合处绑定
// （组合根性质：notify 不横向 import system，绑定只出现在此）。
var ProviderSet = wire.NewSet(
	bizsystem.ProviderSet,
	biznotify.ProviderSet,
	// 订阅代管校验：notify 定义的 AdminResolver 由 system 用户用例实现。
	wire.Bind(new(biznotify.SubscriptionAdminResolver), new(*bizsystem.UserUsecase)),
	// notify 控制面按 biz 隔离：角色 biz_scope 并集由 system 域 AuthzUsecase 解析。
	wire.Bind(new(biznotify.BizScopeResolver), new(*bizsystem.AuthzUsecase)),
	// 业务事件同事务直插（设计 §11）：system 消费 notify 的
	// NotificationWriter——实现固定为 NotificationUsecase（校验/补全收敛
	// 在 notify，system 仅提供业务语义字段；单向依赖 system → notify）。
	wire.Bind(new(biznotify.NotificationWriter), new(*biznotify.NotificationUsecase)),
)
