package data

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
)

// presetAdminCheckTimeout 是启动时预置口令检查的总时限。
const presetAdminCheckTimeout = 10 * time.Second

// presetAdminCheckOnce 一个进程只查一次：userRepo 在装配时构造一次，但测试会反复构造。
var presetAdminCheckOnce sync.Once

// PresetPasswordAdmins 返回口令仍是迁移预置初始口令（biz.PresetAdminPasswordHash）且启用的超级管理员的用户名。
// 只读、只在启动时调用一次；password 列只参与相等比较，不出库。
func (r *userRepo) PresetPasswordAdmins(ctx context.Context) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT username FROM "system_user"
		 WHERE is_admin AND status = $1::smallint AND delete_time IS NULL AND password = $2::text
		 ORDER BY username`, biz.StatusEnabled, biz.PresetAdminPasswordHash)
	if err != nil {
		return nil, fmt.Errorf("query preset password admins: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan preset password admin: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate preset password admins: %w", err)
	}
	return names, nil
}

// warnPresetPasswordAdmins 启动时的一次性检查：仍有启用的超级管理员在用迁移预置的初始口令（任何读得到仓库的人
// 都知道它）时打一条 WARN，让运维从启动日志里看得见。只留痕、不拦截；日志只带用户名，不带口令与哈希。
// 查询失败同样只记日志——这项检查不是启动的前置条件。
func warnPresetPasswordAdmins(ctx context.Context, logger *slog.Logger, list func(context.Context) ([]string, error)) {
	names, err := list(ctx)
	switch {
	case err != nil:
		logger.WarnContext(ctx, "system: checking for super administrators still on the preset initial password failed", "error", err)
	case len(names) > 0:
		logger.WarnContext(ctx, "system: enabled super administrator still uses the preset initial password from the migration; change it or disable the account",
			"usernames", names)
	}
}

// startPresetPasswordCheck 在后台做启动检查（不拖慢装配；多副本各打各的日志，无共享状态）。
// 与通知域启动补封存（data/notify.NewProducerRepo）同一做法：装配阶段没有请求 ctx，带总时限的独立 ctx，
// 跑完即退出。
func (r *userRepo) startPresetPasswordCheck() {
	if r.data == nil || r.data.Pool == nil {
		return
	}
	presetAdminCheckOnce.Do(func() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), presetAdminCheckTimeout)
			defer cancel()
			warnPresetPasswordAdmins(ctx, slog.Default(), r.PresetPasswordAdmins)
		}()
	})
}
