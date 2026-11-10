package biz

import (
	"context"
	"fmt"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/idgen"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

// subscribableChannelTypes 是用户可订阅的渠道类型集合：email（地址取用户
// 资料）与三种群机器人（可指定目标群）；webhook 实例是系统目标 URL、非
// 用户可达地址，不开放订阅。
var subscribableChannelTypes = map[string]struct{}{
	ChannelTypeEmail:    {},
	ChannelTypeDingtalk: {},
	ChannelTypeWecom:    {},
	ChannelTypeFeishu:   {},
}

// SubscriptionPref 单渠道订阅偏好：恰其一为「正取值」——Subscribed=true
// （订阅开关，收件地址按类型解析：email→用户邮箱、机器人→默认绑定）或
// Target 非空（机器人目标群，'$group:ops' 约定）；email 无目标概念，仅开关。
type SubscriptionPref struct {
	Subscribed bool
	Target     string
}

// ChannelPrefs 渠道偏好集合：key 为可订阅渠道类型名；jsonb 落库为设计 §5.1
// 紧凑形态（{"email":true,"dingtalk":"$group:ops"}），由 data 层编解码。
type ChannelPrefs map[string]SubscriptionPref

// Subscription 订阅领域模型（DO）：一行 = 某用户在某 biz 对某事件模式的
// 渠道偏好；自然键 (UserID, BizID, EventPattern) 未删除行内唯一，保存即
// Upsert（同键替换偏好与启停）。
type Subscription struct {
	ID           string
	UserID       string
	BizID        string
	EventPattern string
	ChannelPrefs ChannelPrefs
	Enabled      bool
	CreateTime   time.Time
	UpdateTime   time.Time
}

// SubscriptionFilter 订阅列表过滤条件；UserID 恒为目标用户（本人或 admin
// 代管解析结果，服务层保证非空），指针为 nil 表示不过滤。
type SubscriptionFilter struct {
	UserID  string
	Keyword string
	Enabled *bool
}

// 订阅哨兵错误：reason 与 api/notify/v1 的 ErrorReason 枚举对齐。
var (
	// ErrSubscriptionNotFound 目标订阅不存在。
	ErrSubscriptionNotFound = errors.NotFound(notifyv1.ErrorReason_NOTIFY_SUBSCRIPTION_NOT_FOUND.String(), "subscription not found")
	// ErrSubscriptionInvalid 订阅不合法：event_pattern 越界、渠道类型不可
	// 订阅、偏好取值非正（subscribed=false 且无 target）或 email 携带 target。
	ErrSubscriptionInvalid = errors.BadRequest(notifyv1.ErrorReason_NOTIFY_SUBSCRIPTION_INVALID.String(), "invalid subscription")
	// ErrSubscriptionForbidden 非管理员代管他人订阅（订阅仅本人或 admin 代管）。
	ErrSubscriptionForbidden = errors.Forbidden(notifyv1.ErrorReason_NOTIFY_SUBSCRIPTION_FORBIDDEN.String(), "cannot manage subscriptions of another user")
)

// SubscriptionAdminResolver 判定操作者是否超级管理员（订阅代管校验）；
// 依赖倒置：接口在 notify 域定义，由 system 域 UserUsecase 实现、组合根绑定，
// 域间不横向 import。
type SubscriptionAdminResolver interface {
	// IsAdminUser 报告用户是否 is_admin；用户不存在返回错误（fail closed）。
	IsAdminUser(ctx context.Context, userID string) (bool, error)
}

// SubscriptionRepo 订阅仓储接口，由 data 层实现（依赖倒置）。订阅不参与
// 管道配置轮询（§4.1 缓存集不含订阅），写路径无 cfg_version bump。
type SubscriptionRepo interface {
	// UpsertSubscription 保存订阅：同自然键（未删除行）整组替换偏好与启停，
	// 否则新建；返回落库权威行（含 id 与时间戳）。
	UpsertSubscription(ctx context.Context, sub *Subscription) (*Subscription, error)
	// DeleteSubscription 软删订阅；不存在返回 ErrSubscriptionNotFound。
	DeleteSubscription(ctx context.Context, id string) error
	// GetSubscription 按 ID 查询未删除订阅；不存在返回 ErrSubscriptionNotFound。
	GetSubscription(ctx context.Context, id string) (*Subscription, error)
	// ListSubscriptions 按目标用户分页查询未删除订阅；page 从 1 起，
	// pageSize 上限 100，按创建时间倒序。
	ListSubscriptions(ctx context.Context, filter SubscriptionFilter, page, pageSize int64) ([]*Subscription, int64, error)
}

// SubscriptionUsecase 订阅用例：承载订阅校验（模式/渠道偏好）与 Upsert 编排。
type SubscriptionUsecase struct {
	repo SubscriptionRepo
}

// NewSubscriptionUsecase new a SubscriptionUsecase.
func NewSubscriptionUsecase(repo SubscriptionRepo) *SubscriptionUsecase {
	return &SubscriptionUsecase{repo: repo}
}

// validateChannelPrefs 校验渠道偏好集合：非空、≤可订阅类型数、key 可订阅、
// 每项恰一正取值、email 不携带 target。
func validateChannelPrefs(prefs ChannelPrefs) error {
	if len(prefs) == 0 {
		return fmt.Errorf("%w: empty channel_prefs", ErrSubscriptionInvalid)
	}
	if len(prefs) > len(subscribableChannelTypes) {
		return fmt.Errorf("%w: channel_prefs has %d pairs, want <= %d",
			ErrSubscriptionInvalid, len(prefs), len(subscribableChannelTypes))
	}
	for channelType, pref := range prefs {
		if _, ok := subscribableChannelTypes[channelType]; !ok {
			return fmt.Errorf("%w: channel type %q is not subscribable", ErrSubscriptionInvalid, channelType)
		}
		// 恰一正取值：subscribed=true 或 target 非空（false 且无 target = 无意义项）。
		if pref.Subscribed == (pref.Target != "") {
			return fmt.Errorf("%w: pref for %q must set exactly one of subscribed/target",
				ErrSubscriptionInvalid, channelType)
		}
		// email 地址取用户资料，无目标群概念。
		if channelType == ChannelTypeEmail && pref.Target != "" {
			return fmt.Errorf("%w: email pref does not take a target", ErrSubscriptionInvalid)
		}
	}
	return nil
}

// validateSubscription 校验一条订阅的全部字段。
func validateSubscription(sub *Subscription) error {
	if !eventPatternPattern.MatchString(sub.EventPattern) {
		return fmt.Errorf("%w: bad event_pattern %q", ErrSubscriptionInvalid, sub.EventPattern)
	}
	return validateChannelPrefs(sub.ChannelPrefs)
}

// Upsert 保存订阅：校验、BizID 缺省 default、生成候选 ID 与审计时间后落库；
// 同自然键（未删除行）由仓储整组替换偏好与启停，返回权威行。
func (uc *SubscriptionUsecase) Upsert(ctx context.Context, sub *Subscription) (*Subscription, error) {
	if sub.UserID == "" {
		return nil, fmt.Errorf("%w: missing user_id", ErrSubscriptionInvalid)
	}
	if sub.BizID == "" {
		sub.BizID = DefaultBizID
	}
	if err := validateSubscription(sub); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	sub.ID = id
	sub.CreateTime, sub.UpdateTime = now, now
	return uc.repo.UpsertSubscription(ctx, sub)
}

// Delete 软删订阅；不存在返回 ErrSubscriptionNotFound。
func (uc *SubscriptionUsecase) Delete(ctx context.Context, id string) error {
	return uc.repo.DeleteSubscription(ctx, id)
}

// Get 查询订阅详情（删除前的属主校验等场景）。
func (uc *SubscriptionUsecase) Get(ctx context.Context, id string) (*Subscription, error) {
	return uc.repo.GetSubscription(ctx, id)
}

// List 分页查询订阅；分页参数先规整再委托仓储。
func (uc *SubscriptionUsecase) List(ctx context.Context, filter SubscriptionFilter, page, pageSize int64) ([]*Subscription, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListSubscriptions(ctx, filter, page, pageSize)
}
