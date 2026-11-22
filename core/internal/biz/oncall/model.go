package biz

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 字面量口径（与 DDL CHECK 约束、proto 注释一致）。
const (
	// MemberRoleAdmin 分组管理员；MemberRoleMember 普通成员。
	MemberRoleAdmin  int32 = 1
	MemberRoleMember int32 = 2

	// UnitDay / UnitWeek 轮转周期单位。
	UnitDay  int32 = 1
	UnitWeek int32 = 2

	// RolePrimary 主值班；RoleBackup 备值班。
	RolePrimary int32 = 1
	RoleBackup  int32 = 2

	// SourceAuto 自动排班；SourceOverride 替班（重排不覆盖）。
	SourceAuto     int32 = 1
	SourceOverride int32 = 2

	// StatusEnabled / StatusDisabled 值班表状态。
	StatusEnabled  int32 = 1
	StatusDisabled int32 = 2

	// ScopeGroup / ScopeSchedule / ScopeUser 公共地址范围类型。
	ScopeGroup    int32 = 1
	ScopeSchedule int32 = 2
	ScopeUser     int32 = 3
)

// 容量上限：防止单个分组/名单无限膨胀拖垮排班与通知（通知单事件收件上限 20）。
const (
	maxGroupMembers    = 200
	maxScheduleMembers = 50
	maxGroupSchedules  = 50
	maxShareLinks      = 20
	maxCandidates      = 50
)

// Group 值班分组。MemberCount/ScheduleCount/MyRole 仅读路径填充。
type Group struct {
	ID            string
	Name          string
	Description   string
	MemberCount   int32
	ScheduleCount int32
	// MyRole 查询者在组内的身份：0=非成员。
	MyRole     int32
	CreateTime time.Time
	UpdateTime time.Time
}

// GroupFilter 分组过滤条件。
type GroupFilter struct {
	Keyword string
	// ViewerID 查询者（计算 MyRole）；Mine 为真时只返回其所在分组。
	ViewerID string
	Mine     bool
}

// Member 分组成员（带用户展示信息）。
type Member struct {
	UserID     string
	Username   string
	Nickname   string
	Role       int32
	Available  bool
	InRotation bool
	CreateTime time.Time
}

// User 用户目录条目（值班域对 system 用户的只读视图）。
type User struct {
	ID       string
	Username string
	Nickname string
	Phone    string
	// Available 未删除且状态为可用。
	Available bool
}

// DisplayName 显示名：昵称优先，其次登录名。
func (u *User) DisplayName() string {
	if u == nil {
		return ""
	}
	if u.Nickname != "" {
		return u.Nickname
	}
	return u.Username
}

// Schedule 值班表。Members 与 MemberIDs 同序；GroupName、Members 仅读路径填充。
type Schedule struct {
	ID               string
	GroupID          string
	GroupName        string
	Name             string
	Description      string
	Timezone         string
	RotationUnit     int32
	RotationInterval int32
	AnchorTime       time.Time
	AnchorOffset     int32
	BackupEnabled    bool
	HorizonDays      int32
	RemindMinutes    []int32
	// DigestTime 每日播报时刻 "HH:MM"；空 = 不播报。
	DigestTime     string
	Status         int32
	GeneratedUntil *time.Time
	MemberIDs      []string
	Members        []*User
	CreateTime     time.Time
	UpdateTime     time.Time
}

// Shift 班次。ScheduleName / UserName / OriginUserName / UserAvailable / UserPhone 仅读路径填充。
type Shift struct {
	ID             string
	ScheduleID     string
	ScheduleName   string
	GroupID        string
	Role           int32
	UserID         string
	UserName       string
	UserAvailable  bool
	UserPhone      string
	Start          time.Time
	End            time.Time
	Source         int32
	OriginUserID   string
	OriginUserName string
	Remark         string
	RemindedMask   int32
}

// ShareLink 公共地址。TokenHash / TokenSealed 仅写路径填充（读路径不出库）。
type ShareLink struct {
	ID             string
	ScopeType      int32
	ScopeID        string
	Name           string
	TokenHash      []byte
	TokenSealed    []byte
	ShowPhone      bool
	ExpireTime     *time.Time
	LastAccessTime *time.Time
	CreatorName    string
	CreateTime     time.Time
}

// ChangeLog 变更记录（append-only）。
type ChangeLog struct {
	ID           string
	GroupID      string
	ScheduleID   string
	Action       string
	Summary      string
	Detail       map[string]any
	OperatorID   string
	OperatorName string
	CreateTime   time.Time
}

// DueReminder 待判定提醒的班次（后台任务用）。
type DueReminder struct {
	Shift         *Shift
	Timezone      string
	RemindMinutes []int32
	// Continued 同一人同一角色上一班次与本班次首尾相接（连续值班，不重复提醒）。
	Continued bool
}

// GroupRepo 分组读仓储。
type GroupRepo interface {
	List(ctx context.Context, filter GroupFilter, page, pageSize int64) ([]*Group, int64, error)
	// Get 查询分组（含计数与 viewerID 的身份）；不存在返回 ErrGroupNotFound。
	Get(ctx context.Context, id, viewerID string) (*Group, error)
	ListMembers(ctx context.Context, groupID string) ([]*Member, error)
	// ListUserRoles 用户在各分组内的身份（分组 ID → 身份）。
	ListUserRoles(ctx context.Context, userID string) (map[string]int32, error)
	// ListAdminIDs 分组内可用的管理员用户 ID。
	ListAdminIDs(ctx context.Context, groupID string) ([]string, error)
	ListChangeLogs(ctx context.Context, groupID, scheduleID string, page, pageSize int64) ([]*ChangeLog, int64, error)
}

// UserDirectory 用户目录（system 用户的只读投影）。
type UserDirectory interface {
	// Candidates 可用用户，按登录名/昵称模糊匹配。
	Candidates(ctx context.Context, keyword string, limit int) ([]*User, error)
	// Lookup 按 ID 批量查询（含不可用用户；已删除的不返回）。
	Lookup(ctx context.Context, ids []string) (map[string]*User, error)
}

// ScheduleRepo 值班表读仓储。
type ScheduleRepo interface {
	// List 未删除的值班表（groupID 为空 = 全部），含轮转名单，按分组、名称排序。
	List(ctx context.Context, groupID string) ([]*Schedule, error)
	// Get 不存在返回 ErrScheduleNotFound。
	Get(ctx context.Context, id string) (*Schedule, error)
	// ListDueExtend 已排班范围不足提前天数的启用值班表 ID。
	ListDueExtend(ctx context.Context, limit int) ([]string, error)
	// ListNotifyResults 属于该值班表的值班通知事件（近一天内）及其投递情况；不属于该
	// 值班表、非值班事件或不存在的 ID 不返回。
	ListNotifyResults(ctx context.Context, scheduleID string, eventIDs []string) ([]*NotifyResult, error)
	// MatchNotifyRoute 按通知管道的选路口径（启用、级别下限、事件模式；优先级小者先、
	// 同级取更具体的模式）找出值班事件会命中的规则，返回规则名与其上已启用的渠道数；
	// 没有命中返回空名。带标签条件的规则取决于事件内容，这里不参与试算。
	MatchNotifyRoute(ctx context.Context, eventType string, severity int32) (name string, channels int32, err error)
}

// ShiftScope 班次查询的范围限定：非空字段取交集（公共日历按分组 / 值班表 / 人读取，谓词下推到 SQL，
// 不再全组织扫描后在内存里过滤）。
type ShiftScope struct {
	GroupID    string
	ScheduleID string
	UserID     string
}

// ShiftRepo 班次读仓储。
type ShiftRepo interface {
	// ListRange 未删除值班表中与 [from, to) 相交的班次，按开始时间排序。
	ListRange(ctx context.Context, from, to time.Time) ([]*Shift, error)
	// ListRangeScoped 同 ListRange，但只取 scope 限定范围内的班次。
	ListRangeScoped(ctx context.Context, from, to time.Time, scope ShiftScope) ([]*Shift, error)
	// Get 不存在返回 ErrShiftNotFound。
	Get(ctx context.Context, id string) (*Shift, error)
}

// ShareRepo 公共地址读仓储。
type ShareRepo interface {
	List(ctx context.Context, scopeType int32, scopeID string) ([]*ShareLink, error)
	// Get 未作废的地址；不存在返回 ErrShareLinkNotFound。
	Get(ctx context.Context, id string) (*ShareLink, error)
	// GetSealedToken 未作废地址的令牌密文（密文列引入前创建的地址为空）；不存在返回 ErrShareLinkNotFound。
	GetSealedToken(ctx context.Context, id string) ([]byte, error)
	// FindByHash 按令牌哈希查找未作废的地址；不存在返回 ErrShareLinkNotFound。
	FindByHash(ctx context.Context, hash []byte) (*ShareLink, error)
	// Touch 记录访问时间（至多每小时写一次）。
	Touch(ctx context.Context, id string) error
}

// TokenCipher 公共地址令牌的加解密（data 层以 kit/secretbox 实现）；主密钥未配置返回
// ErrSecretUnavailable——绝不静默落明文。
type TokenCipher interface {
	Seal(plaintext string) ([]byte, error)
	Open(sealed []byte) (string, error)
}

// Store 写路径入口：全部写操作在一个事务内完成（fn 出错回滚并透传）。
type Store interface {
	WithinTx(ctx context.Context, fn func(tx Tx) error) error
}

// Tx 值班域事务视图。嵌入 txrun.Tx 以便在同一事务内直插通知事件（业务提交 = 事件
// 必然落库）。加锁顺序固定为 分组 → 值班表，任何路径不得反向，避免死锁。
type Tx interface {
	txrun.Tx

	// LockGroup 行锁读取分组；不存在返回 ErrGroupNotFound。
	LockGroup(ctx context.Context, id string) (*Group, error)
	// MemberRole 用户在组内的身份；非成员返回 0。
	MemberRole(ctx context.Context, groupID, userID string) (int32, error)
	// InsertGroup 名称冲突返回 ErrGroupNameExists。
	InsertGroup(ctx context.Context, g *Group, actor string) error
	UpdateGroup(ctx context.Context, id, name, description, actor string) error
	// DeleteGroup 软删分组并清除成员。
	DeleteGroup(ctx context.Context, id, actor string) error
	CountSchedules(ctx context.Context, groupID string) (int, error)
	ListMembers(ctx context.Context, groupID string) ([]*Member, error)
	// InsertMembers 已在组内的跳过，返回实际新增数。
	InsertMembers(ctx context.Context, groupID string, userIDs []string, role int32, actor string) (int, error)
	UpdateMemberRole(ctx context.Context, groupID, userID string, role int32) error
	DeleteMember(ctx context.Context, groupID, userID string) error

	// LockSchedule 行锁读取值班表（含轮转名单）；不存在返回 ErrScheduleNotFound。
	LockSchedule(ctx context.Context, id string) (*Schedule, error)
	// InsertSchedule / UpdateSchedule 名称冲突返回 ErrScheduleNameExists；轮转名单整体替换。
	InsertSchedule(ctx context.Context, s *Schedule, actor string) error
	UpdateSchedule(ctx context.Context, s *Schedule, actor string) error
	SetScheduleStatus(ctx context.Context, id string, status int32, actor string) error
	SetGeneratedUntil(ctx context.Context, id string, until *time.Time) error
	// DeleteSchedule 软删值班表并清除轮转名单。
	DeleteSchedule(ctx context.Context, id, actor string) error

	// ListShiftsFrom 值班表中结束时间晚于 from 的班次，按开始时间排序。
	ListShiftsFrom(ctx context.Context, scheduleID string, from time.Time) ([]*Shift, error)
	// GetShift 不存在返回 ErrShiftNotFound。
	GetShift(ctx context.Context, id string) (*Shift, error)
	// DeleteFutureAutoShifts 删除开始时间晚于 now 的自动班次（恰在 now 开始的班次视为进行中，保留）。
	DeleteFutureAutoShifts(ctx context.Context, scheduleID string, now time.Time) error
	// DeleteShiftsFrom 删除开始时间不早于 from 的全部班次（含替班）。
	DeleteShiftsFrom(ctx context.Context, scheduleID string, from time.Time) error
	// TruncateShiftsAt 把跨越 at 的班次在 at 处截断。
	TruncateShiftsAt(ctx context.Context, scheduleID string, at time.Time) error
	// InsertShifts 时间重叠（排他约束）返回 ErrShiftConflict。
	InsertShifts(ctx context.Context, shifts []*Shift, actor string) error
	UpdateShiftRange(ctx context.Context, id string, start, end time.Time, actor string) error
	DeleteShift(ctx context.Context, id string) error

	InsertShareLink(ctx context.Context, link *ShareLink, actor string) error
	UpdateShareLinkToken(ctx context.Context, id string, hash, sealed []byte, actor string) error
	DeleteShareLink(ctx context.Context, id, actor string) error
	// DeleteShareLinksByScope 范围对象被删除时一并作废其公共地址。
	DeleteShareLinksByScope(ctx context.Context, scopeType int32, scopeID, actor string) error
	CountShareLinks(ctx context.Context, scopeType int32, scopeID string) (int, error)

	InsertChangeLog(ctx context.Context, log *ChangeLog) error

	// ListDueReminders 行锁（SKIP LOCKED）取出进入提醒窗口且尚有提醒未发的班次。
	ListDueReminders(ctx context.Context, now time.Time, limit int) ([]*DueReminder, error)
	SetRemindedMask(ctx context.Context, shiftID string, mask int32) error
	// ListDueDigests 行锁（SKIP LOCKED）取出到达播报时刻且当日未播报的启用值班表。
	ListDueDigests(ctx context.Context, now time.Time, limit int) ([]*Schedule, error)
	// HasRecentManualNotify since 之后是否已手动发送过该值班表的该类通知。
	HasRecentManualNotify(ctx context.Context, scheduleID, eventType string, since time.Time) (bool, error)
	// MarkDigested 记录当日（值班表时区）已播报。
	MarkDigested(ctx context.Context, scheduleID string, now time.Time) error
}
