package biz

import (
	"context"
	"time"
)

// 登录结果常量，与库表 system_login_log.result 契约对齐；0 非法（proto3 默认
// 值，防「未设置」歧义，口径同 User.Status）。对外登录接口用户不存在与密码
// 错误统一 INVALID_CREDENTIALS 防枚举，细分值仅存在于审计日志与管理端检索。
const (
	// LoginResultSuccess 登录成功（双令牌已签发）。
	LoginResultSuccess int32 = 1
	// LoginResultUserNotFound 用户不存在（审计细分值，对外不区分）。
	LoginResultUserNotFound int32 = 2
	// LoginResultWrongPassword 密码错误（审计细分值，对外不区分）。
	LoginResultWrongPassword int32 = 3
	// LoginResultAccountDisabled 凭据正确但账号非可用状态。
	LoginResultAccountDisabled int32 = 4
	// LoginResultTokenIssueFailed 凭据全部验证通过但令牌签发/落库失败。
	LoginResultTokenIssueFailed int32 = 5
)

// LoginLog 是登录日志领域模型（DO）：append-only 审计，仅由 AuthUsecase.Login
// 写入。UserID/IP/UserAgent 空串表示未取到（用户不存在 / 非 HTTP 传输），
// data 层落库为 NULL。
type LoginLog struct {
	ID         string
	UserID     string
	Username   string
	Result     int32
	IP         string
	UserAgent  string
	CreateTime time.Time
}

// LoginLogFilter 是登录日志列表过滤条件；零值字段表示不过滤。
type LoginLogFilter struct {
	// Username 登录名模糊匹配，空串不过滤。
	Username string
	// Result 登录结果等值过滤，nil 不过滤。
	Result *int32
	// StartTime 起始时间（含），nil 不过滤。
	StartTime *time.Time
	// EndTime 截止时间（含），nil 不过滤。
	EndTime *time.Time
}

// LoginLogWriter 是登录日志写仓储接口，由 data 层实现（依赖倒置）。
// 写入由登录链路内联调用（尽力而为，失败不阻塞登录），不设删除/更新方法。
type LoginLogWriter interface {
	// Create 插入一条登录日志。
	Create(ctx context.Context, log *LoginLog) error
}

// LoginLogReader 是登录日志只读仓储接口，由 data 层实现（依赖倒置）。
type LoginLogReader interface {
	// ListLoginLogs 条件过滤 + 分页查询；返回页数据与总数。
	ListLoginLogs(ctx context.Context, filter LoginLogFilter, page, pageSize int64) ([]*LoginLog, int64, error)
}

// LoginLogUsecase 是登录日志查询用例；写入由 AuthUsecase.Login 内联完成，
// 本用例只承载管理端检索。
type LoginLogUsecase struct {
	reader LoginLogReader
}

// NewLoginLogUsecase new a LoginLogUsecase.
func NewLoginLogUsecase(reader LoginLogReader) *LoginLogUsecase {
	return &LoginLogUsecase{reader: reader}
}

// List 分页查询登录日志；分页参数先经 normalizePage 规整再委托仓储。
func (uc *LoginLogUsecase) List(ctx context.Context, filter LoginLogFilter, page, pageSize int64) ([]*LoginLog, int64, error) {
	page, pageSize = normalizePage(page, pageSize)
	return uc.reader.ListLoginLogs(ctx, filter, page, pageSize)
}
