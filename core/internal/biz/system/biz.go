// system 域 biz providers：用户/角色/认证/授权/系统配置/登录日志。
// 域内代码同包（package biz）互引，跨域经顶层 internal/biz 聚合装配。
package biz

import "github.com/google/wire"

// ProviderSet is system 域 biz providers.
var ProviderSet = wire.NewSet(NewUserUsecase, NewRoleUsecase, NewSystemConfigUsecase, NewPermissionUsecase, NewAuthzUsecase, NewPasswordHasher, NewTokenSigner, NewAuthUsecase, NewAuthorizer, NewLoginLogUsecase)
