// system 域 data providers：仓储实现与接口绑定。
// 域内代码同包（package data）互引，跨域经顶层 internal/data 聚合装配；
// 连接池（Data）由顶层共享，本域只注册仓储。
package data

import (
	"github.com/google/wire"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
)

// ProviderSet is system 域 data providers.
var ProviderSet = wire.NewSet(
	NewUserRepo,
	NewRoleRepo,
	NewUserRoleRepo,
	NewRefreshTokenRepo,
	NewPermissionRepo,
	NewSystemConfigRepo,
	NewAvatarStore,
	NewLoginLogRepo,
	// 接口在消费侧（biz）定义，实现在本包绑定。
	wire.Bind(new(biz.UserReader), new(*userRepo)),
	wire.Bind(new(biz.UserWriter), new(*userRepo)),
	wire.Bind(new(biz.RoleReader), new(*roleRepo)),
	wire.Bind(new(biz.RoleWriter), new(*roleRepo)),
	wire.Bind(new(biz.UserRoleRepo), new(*userRoleRepo)),
	wire.Bind(new(biz.RefreshTokenReader), new(*refreshTokenRepo)),
	wire.Bind(new(biz.RefreshTokenWriter), new(*refreshTokenRepo)),
	wire.Bind(new(biz.PermissionPolicySource), new(*permissionRepo)),
	wire.Bind(new(biz.PermissionGrantReader), new(*permissionRepo)),
	wire.Bind(new(biz.PermissionAdminRepo), new(*permissionRepo)),
	wire.Bind(new(biz.SystemConfigRepo), new(*systemConfigRepo)),
	wire.Bind(new(biz.AvatarStore), new(*avatarStore)),
	wire.Bind(new(biz.LoginLogWriter), new(*loginLogRepo)),
	wire.Bind(new(biz.LoginLogReader), new(*loginLogRepo)),
)
