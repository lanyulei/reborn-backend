// system 域 service providers：各 proto Server 的实现装配。
// 域内代码同包（package service）互引，跨域经顶层 internal/service 聚合装配。
package service

import "github.com/google/wire"

// ProviderSet is system 域 service providers.
var ProviderSet = wire.NewSet(NewUserService, NewRoleService, NewAuthService, NewSystemConfigService, NewLoginLogService)
