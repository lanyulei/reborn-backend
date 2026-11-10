// Package service 实现 proto 生成的服务接口：只做参数编排、模型转换与错误透传，
// 业务规则全部收敛在 biz（禁止 import internal/data 与任何驱动）。
//
// 分域结构：各业务域在 internal/service/<domain>/ 子包实现（子包包名保持
// service）；本包只保留跨域聚合的 ProviderSet。新增域时在此追加。
package service

import (
	"github.com/google/wire"

	servicenotify "github.com/lanyulei/reborn/backend/core/internal/service/notify"
	servicesystem "github.com/lanyulei/reborn/backend/core/internal/service/system"
)

// ProviderSet 聚合各业务域的 service providers。
var ProviderSet = wire.NewSet(servicesystem.ProviderSet, servicenotify.ProviderSet)
