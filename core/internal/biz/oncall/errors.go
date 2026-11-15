// Package biz 值班域业务核心：值班分组、值班表轮转规则、班次生成与替班、排班异常检测、
// 公共日历地址与后台任务；定义仓储接口（依赖倒置），零驱动、零传输层 import。
package biz

import (
	"fmt"

	v1 "github.com/lanyulei/reborn/backend/core/api/oncall/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// 哨兵错误：reason 来自 api/oncall/v1 错误码枚举，HTTP 状态随枚举映射。
var (
	ErrGroupNotFound      = errors.NotFound(v1.ErrorReason_ONCALL_GROUP_NOT_FOUND.String(), "oncall group not found")
	ErrGroupNameExists    = errors.Conflict(v1.ErrorReason_ONCALL_GROUP_NAME_EXISTS.String(), "oncall group name already exists")
	ErrGroupInvalid       = errors.BadRequest(v1.ErrorReason_ONCALL_GROUP_INVALID.String(), "invalid oncall group")
	ErrGroupInUse         = errors.Conflict(v1.ErrorReason_ONCALL_GROUP_IN_USE.String(), "oncall group still has schedules")
	ErrForbidden          = errors.Forbidden(v1.ErrorReason_ONCALL_FORBIDDEN.String(), "group admin required")
	ErrMemberInvalid      = errors.BadRequest(v1.ErrorReason_ONCALL_MEMBER_INVALID.String(), "invalid group member")
	ErrMemberNotFound     = errors.NotFound(v1.ErrorReason_ONCALL_MEMBER_NOT_FOUND.String(), "group member not found")
	ErrMemberInUse        = errors.Conflict(v1.ErrorReason_ONCALL_MEMBER_IN_USE.String(), "member is still in a schedule rotation")
	ErrLastAdmin          = errors.Conflict(v1.ErrorReason_ONCALL_LAST_ADMIN.String(), "group must keep at least one admin")
	ErrScheduleNotFound   = errors.NotFound(v1.ErrorReason_ONCALL_SCHEDULE_NOT_FOUND.String(), "schedule not found")
	ErrScheduleNameExists = errors.Conflict(v1.ErrorReason_ONCALL_SCHEDULE_NAME_EXISTS.String(), "schedule name already exists in this group")
	ErrScheduleInvalid    = errors.BadRequest(v1.ErrorReason_ONCALL_SCHEDULE_INVALID.String(), "invalid schedule config")
	ErrShiftNotFound      = errors.NotFound(v1.ErrorReason_ONCALL_SHIFT_NOT_FOUND.String(), "shift not found")
	ErrOverrideInvalid    = errors.BadRequest(v1.ErrorReason_ONCALL_OVERRIDE_INVALID.String(), "invalid override")
	ErrShiftConflict      = errors.Conflict(v1.ErrorReason_ONCALL_SHIFT_CONFLICT.String(), "shifts were modified concurrently, please retry")
	ErrShareLinkNotFound  = errors.NotFound(v1.ErrorReason_ONCALL_SHARE_LINK_NOT_FOUND.String(), "share link not found")
	ErrShareLinkInvalid   = errors.BadRequest(v1.ErrorReason_ONCALL_SHARE_LINK_INVALID.String(), "invalid share link")
	ErrSecretUnavailable  = errors.ServiceUnavailable(v1.ErrorReason_ONCALL_SECRET_UNAVAILABLE.String(), "credential master key is not configured")
	ErrTimeRangeInvalid   = errors.BadRequest(v1.ErrorReason_ONCALL_TIME_RANGE_INVALID.String(), "invalid time range")
	// ErrPublicThrottled 公共地址匿名访问过于频繁（429）；reason 与全局限流中间件同口径。
	ErrPublicThrottled = errors.New(429, "RATELIMIT", "too many requests, please try again later")
)

// Actor 操作者：IsAdmin 为超级管理员（越过分组管理员校验）。
type Actor struct {
	UserID  string
	IsAdmin bool
}

// detail 以哨兵错误的 code/reason 构造带具体说明的错误（errors.Is 按 code+reason 匹配，
// 前端按 message 展示原因）。
func detail(base *errors.Error, format string, args ...any) error {
	return errors.New(int(base.Code), base.Reason, fmt.Sprintf(format, args...))
}
