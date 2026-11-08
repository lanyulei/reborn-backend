// Package biz 承载业务核心：领域模型、仓储接口（依赖倒置）与用例。
// 本包禁止 import 传输层（transport/http、transport/grpc）与任何存储驱动。
package biz

import "github.com/google/wire"

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(NewUserUsecase, NewRoleUsecase, NewSystemConfigUsecase, NewPermissionUsecase, NewAuthzUsecase, NewPasswordHasher, NewTokenSigner, NewAuthUsecase, NewAuthorizer)
