package biz

import (
	"context"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// 用户状态常量，与库表契约对齐；0 非法（proto3 默认值，防「未设置」歧义）。
const (
	// StatusEnabled 可用：正常使用。
	StatusEnabled int32 = 1
	// StatusDisabled 停用：管理员停用账号。
	StatusDisabled int32 = 2
	// StatusPaused 暂停：临时暂停（如长假）。
	StatusPaused int32 = 3
	// StatusResigned 离职：账号归档。
	StatusResigned int32 = 4
)

// 角色状态常量（角色仅启用/停用两态）。
const (
	// RoleStatusDisabled 停用。
	RoleStatusDisabled int32 = 0
	// RoleStatusEnabled 启用。
	RoleStatusEnabled int32 = 1
)

// 性别取值（gender 为字符串契约，集合在 proto validate 层强制）。
const (
	// GenderMale 男性。
	GenderMale = "male"
	// GenderFemale 女性。
	GenderFemale = "female"
	// GenderUnknown 未知。
	GenderUnknown = "unknown"
)

// User 是用户领域模型（DO）；与 proto 实体、data 层 PO 显式转换，禁止跨层直传。
// Password 仅在创建/重置链路携带：入参为明文，由用例替换为 Argon2id PHC 串后落库；
// 除认证链路经 UserReader.GetUserByUsername 取哈希校验外，任何读路径不得回填该字段。
// Email/Phone 空串表示未填写，data 层落库为 NULL。
type User struct {
	ID         string
	Username   string
	Password   string
	Nickname   string
	Email      string
	Phone      string
	Gender     string
	IsAdmin    bool
	Status     int32
	CreateTime time.Time
	UpdateTime time.Time
}

// UserFilter 是用户列表过滤条件；Status 为 nil 表示不过滤。
type UserFilter struct {
	Keyword string
	Status  *int32
}

// UserUpdate 是用户部分更新；nil 字段不更新。
// PasswordHash 仅由 ResetPassword 用例填充（Argon2id PHC 串），不接收明文。
type UserUpdate struct {
	Nickname     *string
	Email        *string
	Phone        *string
	Gender       *string
	IsAdmin      *bool
	Status       *int32
	PasswordHash *string
}

// 用户域哨兵错误：跨层业务错误统一携带 Kratos code/reason，
// reason 与 api/system/v1 的 ErrorReason 枚举对齐（单一事实来源）。
var (
	// ErrUserNotFound 目标用户不存在。
	ErrUserNotFound = errors.NotFound(v1.ErrorReason_USER_NOT_FOUND.String(), "user not found")
	// ErrUserExists 用户名已被占用。
	ErrUserExists = errors.Conflict(v1.ErrorReason_USER_EXISTS.String(), "username already exists")
	// ErrUserEmailExists 邮箱已被占用。
	ErrUserEmailExists = errors.Conflict(v1.ErrorReason_USER_EMAIL_EXISTS.String(), "email already exists")
	// ErrUserPhoneExists 手机号已被占用。
	ErrUserPhoneExists = errors.Conflict(v1.ErrorReason_USER_PHONE_EXISTS.String(), "phone already exists")
	// ErrUserSelfOperation 不允许对当前登录账号自身执行停用/删除/变更超管
	// 标记（防止操作者把自己锁在系统之外）；判定在 service 层（需传输层
	// 身份），哨兵在此定义以统一 reason 事实源。
	ErrUserSelfOperation = errors.BadRequest(v1.ErrorReason_USER_SELF_OPERATION.String(), "operation on current user is not allowed")
)

// UserReader 是用户只读仓储接口，由 data 层实现（依赖倒置）。
type UserReader interface {
	// GetUser 按 ID 查询未删除用户，不返回密码；不存在返回 ErrUserNotFound。
	GetUser(ctx context.Context, id string) (*User, error)
	// GetUserByUsername 按用户名查询未删除用户，出参包含 password 哈希 ——
	// 全部读路径中的唯一例外，仅供认证链路（AuthUsecase.Login）消费；
	// 调用方禁止透传、落日志或回填到任何对外出参。
	GetUserByUsername(ctx context.Context, username string) (*User, error)
	// ListUsers 分页查询未删除用户；page 从 1 起，pageSize 上限 100，按创建时间倒序。
	ListUsers(ctx context.Context, filter UserFilter, page, pageSize int64) ([]*User, int64, error)
}

// UserWriter 是用户写仓储接口，由 data 层实现（依赖倒置）。
type UserWriter interface {
	// CreateUser 创建用户；username/email/phone 冲突分别返回对应哨兵错误。
	CreateUser(ctx context.Context, user *User) error
	// UpdateUser 按 ID 部分更新未删除用户；不存在返回 ErrUserNotFound。
	UpdateUser(ctx context.Context, id string, upd UserUpdate) error
	// DeleteUser 软删用户并同事务物理删除其全部角色绑定与刷新令牌；不存在返回 ErrUserNotFound。
	DeleteUser(ctx context.Context, id string) error
}

// UserUsecase 是用户用例：承载业务规则与事务边界。tokens 用于账号安全态
// 变更（重置密码/停用/撤销超管）时吊销该用户全部刷新令牌——否则持有旧
// refresh token 的会话可在 refresh_ttl 内无限续期，管理动作形同虚设。
type UserUsecase struct {
	reader     UserReader
	writer     UserWriter
	roles      UserRoleRepo
	roleReader RoleReader
	hasher     *PasswordHasher
	tokens     RefreshTokenWriter
}

// NewUserUsecase new a UserUsecase.
func NewUserUsecase(reader UserReader, writer UserWriter, roles UserRoleRepo, roleReader RoleReader, hasher *PasswordHasher, tokens RefreshTokenWriter) *UserUsecase {
	return &UserUsecase{reader: reader, writer: writer, roles: roles, roleReader: roleReader, hasher: hasher, tokens: tokens}
}

// Create 创建用户：生成 ID、解码密码（b64$/明文）、哈希、填充审计时间后落库；
// 返回出参不含密码。
func (uc *UserUsecase) Create(ctx context.Context, user *User) (*User, error) {
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	plain, err := decodePassword(user.Password)
	if err != nil {
		return nil, err
	}
	hash, err := uc.hasher.Hash(plain)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}
	now := time.Now().UTC()
	user.ID = id
	user.Password = hash
	user.CreateTime, user.UpdateTime = now, now
	if err := uc.writer.CreateUser(ctx, user); err != nil {
		return nil, err
	}
	user.Password = ""
	return user, nil
}

// Update 部分更新用户资料；不存在返回 ErrUserNotFound。撤销超管标记
// （is_admin true→false）属权限收缩，落库后吊销其全部刷新令牌强制重新
// 登录（access token 剩余 ≤access_ttl 的残留窗口由 authz 实时读 is_admin
// 覆盖）。
func (uc *UserUsecase) Update(ctx context.Context, id string, upd UserUpdate) error {
	if err := uc.writer.UpdateUser(ctx, id, upd); err != nil {
		return err
	}
	if upd.IsAdmin != nil && !*upd.IsAdmin {
		return uc.revokeSessions(ctx, id)
	}
	return nil
}

// UpdateStatus 启用/禁用用户；不存在返回 ErrUserNotFound。置为非可用状态
// 时吊销其全部刷新令牌：令牌失效与账号停用同步生效（authz 同时按实时
// status 拒绝其尚未过期的 access token）。
func (uc *UserUsecase) UpdateStatus(ctx context.Context, id string, status int32) error {
	if err := uc.writer.UpdateUser(ctx, id, UserUpdate{Status: &status}); err != nil {
		return err
	}
	if status != StatusEnabled {
		return uc.revokeSessions(ctx, id)
	}
	return nil
}

// ResetPassword 重置用户密码：解码（b64$/明文）后哈希落库，并吊销该用户
// 全部刷新令牌（账号失陷场景下重置密码必须逐出在线会话）；不存在返回
// ErrUserNotFound。
func (uc *UserUsecase) ResetPassword(ctx context.Context, id, password string) error {
	plain, err := decodePassword(password)
	if err != nil {
		return err
	}
	hash, err := uc.hasher.Hash(plain)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	if err := uc.writer.UpdateUser(ctx, id, UserUpdate{PasswordHash: &hash}); err != nil {
		return err
	}
	return uc.revokeSessions(ctx, id)
}

// revokeSessions 吊销用户全部刷新令牌。失败返回错误：主写已提交，但令牌
// 未吊销意味着安全动作未完成，向调用方暴露以便重试（两步均幂等）。
func (uc *UserUsecase) revokeSessions(ctx context.Context, id string) error {
	if err := uc.tokens.DeleteByUser(ctx, id); err != nil {
		return fmt.Errorf("revoke refresh tokens: %w", err)
	}
	return nil
}

// Delete 软删用户并级联物理删除其角色绑定与刷新令牌；不存在返回 ErrUserNotFound。
func (uc *UserUsecase) Delete(ctx context.Context, id string) error {
	return uc.writer.DeleteUser(ctx, id)
}

// Get 查询用户详情，出参不含密码。
func (uc *UserUsecase) Get(ctx context.Context, id string) (*User, error) {
	return uc.reader.GetUser(ctx, id)
}

// IsAdminUser 报告用户是否超级管理员（is_admin）；用户不存在返回错误
// （fail closed）。跨域窄接口实现：notify 域订阅代管校验经
// biznotify.SubscriptionAdminResolver 消费（组合根 wire.Bind 绑定本类型）。
func (uc *UserUsecase) IsAdminUser(ctx context.Context, id string) (bool, error) {
	user, err := uc.reader.GetUser(ctx, id)
	if err != nil {
		return false, err
	}
	return user.IsAdmin, nil
}

// DescribeActor 返回用户展示信息（跨域窄接口 assistant.ActorDescriber 的实现）：
// 用户不存在返回错误（fail closed）。
func (uc *UserUsecase) DescribeActor(ctx context.Context, id string) (username, nickname string, isAdmin bool, err error) {
	user, err := uc.reader.GetUser(ctx, id)
	if err != nil {
		return "", "", false, err
	}
	return user.Username, user.Nickname, user.IsAdmin, nil
}

// List 分页查询用户；分页参数先规整再委托仓储。
func (uc *UserUsecase) List(ctx context.Context, filter UserFilter, page, pageSize int64) ([]*User, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.reader.ListUsers(ctx, filter, page, pageSize)
}

// BindRoles 全量覆盖用户角色绑定：用户或任一角色不存在返回对应哨兵错误；
// roleIDs 为空表示清空绑定。
func (uc *UserUsecase) BindRoles(ctx context.Context, userID string, roleIDs []string) error {
	if _, err := uc.reader.GetUser(ctx, userID); err != nil {
		return err
	}
	if len(roleIDs) > 0 {
		found, err := uc.roleReader.ListRolesByIDs(ctx, roleIDs)
		if err != nil {
			return err
		}
		if len(found) != len(roleIDs) {
			return ErrRoleNotFound
		}
	}
	return uc.roles.BindUserRoles(ctx, userID, roleIDs)
}

// ListRoles 查询用户已绑定的角色；用户不存在返回 ErrUserNotFound。
func (uc *UserUsecase) ListRoles(ctx context.Context, userID string) ([]*Role, error) {
	if _, err := uc.reader.GetUser(ctx, userID); err != nil {
		return nil, err
	}
	return uc.roles.ListUserRoles(ctx, userID)
}
