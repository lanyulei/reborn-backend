package biz

import (
	"context"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 本文件收拢 monitor 域对其他域的全部依赖：只读、单向，一律是在此定义的窄接口，由组合根
// （internal/biz/biz.go）绑定到 system / oncall / notify 域的用例。其他域不依赖 monitor。

// RoleSource 用户的启用角色（system 域 AuthzUsecase 实现）。
type RoleSource interface {
	// EnabledRoleIDs 超管返回 (true, nil)；非可用用户返回 (false, 空)。
	EnabledRoleIDs(ctx context.Context, userID string) (isAdmin bool, roleIDs []string, err error)
}

// RoleRef 角色的展示信息。
type RoleRef struct {
	ID      string
	Name    string
	Enabled bool
}

// RoleDirectory 全部未删除角色（组合根适配 system 域 RoleUsecase；角色为小集合）。
type RoleDirectory interface {
	ListRoles(ctx context.Context) ([]RoleRef, error)
}

// Directory 组织目录的只读查询（system 域 DirectoryUsecase 实现）。
type Directory interface {
	// ActiveUserIDs 过滤出存在且可用的用户，保持入参顺序并去重。
	ActiveUserIDs(ctx context.Context, ids []string) ([]string, error)
	// UserNames 批量取用户显示名；不存在或非可用的用户不在结果中。
	UserNames(ctx context.Context, ids []string) (map[string]string, error)
	// DeptNames 批量取部门名称；不存在的部门不在结果中。
	DeptNames(ctx context.Context, ids []string) (map[string]string, error)
}

// Permissions 接口权限判定（组合根以 system 域 AuthzUsecase 适配，操作码与授权中间件同一口径）。
// 超级管理员恒为 true，非可用用户恒为 false。只用于两处接口授权中间件覆盖不到的判定：免接口授权的
// 告警详情，以及「测试连接时沿用已保存凭据」这条需要编辑权限的支路。
type Permissions interface {
	// CanReadAlerts 是否持有「查询告警事件」接口权限。
	CanReadAlerts(ctx context.Context, userID string) (bool, error)
	// CanEditDatasource 是否持有「编辑监控数据源」接口权限。
	CanEditDatasource(ctx context.Context, userID string) (bool, error)
}

// DeptLeaders 部门负责人查询（system 域 DirectoryUsecase 实现）。
type DeptLeaders interface {
	// DeptLeaderIDs 从 deptID 向上第 level 级部门（1 = 自身）取可用的负责人；该级为空则继续向上。
	DeptLeaderIDs(ctx context.Context, deptID string, level int, exclude string) ([]string, error)
}

// Duty 值班表当前值班人（oncall 域 DutyUsecase 实现）。
type Duty interface {
	// OnDuty 主值班在前、备值班在后；值班表不存在、已停用或无人值班返回空。
	OnDuty(ctx context.Context, scheduleID string, at time.Time) ([]string, error)
}

// Schedules 值班表名称（组合根适配 oncall 域 ScheduleUsecase）。
type Schedules interface {
	// ScheduleNames 批量取值班表名称；不存在或已停用的不在结果中。
	ScheduleNames(ctx context.Context, ids []string) (map[string]string, error)
}

// TargetSchedule 可选的值班表。
type TargetSchedule struct {
	ID        string
	Name      string
	GroupName string
}

// TargetDept 可选的部门。
type TargetDept struct {
	ID       string
	Name     string
	ParentID string
}

// TargetUser 可选的用户（最小披露）。
type TargetUser struct {
	ID       string
	Username string
	Nickname string
}

// TargetSource 通知对象候选项的来源（组合根适配 oncall 域的值班表、system 域的部门与用户）。
type TargetSource interface {
	// Schedules 启用的值班表，按值班组、名称排序。
	Schedules(ctx context.Context) ([]TargetSchedule, error)
	// Depts 全部未删除的部门，上级在前。
	Depts(ctx context.Context) ([]TargetDept, error)
	// Users 可用用户按关键字（登录名 / 昵称）匹配，至多 limit 个。
	Users(ctx context.Context, keyword string, limit int64) ([]TargetUser, error)
}

// EventWriter 在调用方事务内直插通知事件（notify 域 NotificationWriter）。
type EventWriter = biznotify.NotificationWriter

// TxManager 开启事务（internal/data.TxManager）：派发用例把「领取发件箱 + 写通知事件 + 写通知
// 记录 + 回写」放进同一事务。
type TxManager = txrun.Manager
