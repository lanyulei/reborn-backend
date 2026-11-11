package biz

import (
	"context"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

// eventPatternPattern 事件匹配模式：'*' 全匹配 / 精确点分串 / 前缀通配
// （'monitor.alert.*' 等价前缀 'monitor.alert.'，仅允许尾部通配；与
// protovalidate 同式，入口双保险）。路由与静默规则共用同一模式语义。
var eventPatternPattern = notifycontract.EventPatternPattern

// 路由数值边界（severity 对齐事件分级 1 info/2 warning/3 critical/4 fatal；
// 聚合窗口 0=不聚合或 30s-5m，见设计文档 §4.5）。
const (
	routeSeverityMin    = 1
	routeSeverityMax    = 4
	routePriorityMin    = 1
	routePriorityMax    = 9
	routeWindowMin      = 30
	routeWindowMax      = 300
	routeThresholdMax   = 100000
	routeDurationMaxMin = 1440
	routeChannelsMax    = 50
	routeTagsMax        = 16
)

// RouteRecipient 收件策略：fixed 与 subscription 可并存——fixed 先行、订阅者
// 按渠道偏好追加，同 (channel, recipient) 去重（管道侧 §5.1.1 解析）。
// json 标签即 notify_route.recipient_policy 列的落库结构。
type RouteRecipient = notifycontract.RouteRecipient

// RouteRule 路由规则：匹配（pattern/severity/标签）与分发（渠道/收件/聚合）
// 决策互为整体，创建与整组替换更新共用同一校验。
type RouteRule struct {
	EventPattern      string
	SeverityMin       int32
	TagMatch          map[string]string // nil = 无条件
	Priority          int32
	ChannelIDs        []string
	Recipient         RouteRecipient
	AggregateWindowS  int32
	SuppressThreshold int32
	SuppressDurationM int32
}

// Route 路由领域模型（DO）；内嵌 RouteRule（规则字段经提升按扁平访问），
// 与 proto 实体、data 层 PO 显式转换，禁止跨层直传。
type Route struct {
	ID    string
	BizID string
	RouteRule
	Enabled    bool
	CreateTime time.Time
	UpdateTime time.Time
}

// RouteFilter 路由列表过滤条件；指针为 nil 表示不过滤。
type RouteFilter struct {
	BizID   string // 空 = 不过滤（受限用户由 service 层强制携带）
	Keyword string
	Enabled *bool
}

// RouteUpdate 路由更新：Rule 非 nil 时整组替换规则字段（路由规则是一个整体
// 决策，不做字段级拼装）；Enabled 独立启停；两者至少其一。
type RouteUpdate struct {
	Rule    *RouteRule
	Enabled *bool
}

// 路由哨兵错误：reason 与 api/notify/v1 的 ErrorReason 枚举对齐。
var (
	// ErrRouteNotFound 目标路由不存在。
	ErrRouteNotFound = errors.NotFound(notifyv1.ErrorReason_NOTIFY_ROUTE_NOT_FOUND.String(), "route not found")
	// ErrRouteInvalid 路由不合法：event_pattern/数值边界越界、收件策略为空、
	// 引用了不存在/已删除/跨 biz 的渠道。
	ErrRouteInvalid = errors.BadRequest(notifyv1.ErrorReason_NOTIFY_ROUTE_INVALID.String(), "invalid route")
)

// RouteRepo 路由仓储接口，由 data 层实现（依赖倒置）。
type RouteRepo interface {
	// CreateRoute 创建路由；设计即无唯一约束（多路由可重叠，priority 决胜）。
	CreateRoute(ctx context.Context, route *Route) error
	// UpdateRoute 按 ID 更新未删除路由（Rule 整组替换 / Enabled 启停）；
	// 不存在返回 ErrRouteNotFound。
	UpdateRoute(ctx context.Context, id string, upd RouteUpdate) error
	// DeleteRoute 软删路由；不存在返回 ErrRouteNotFound。
	DeleteRoute(ctx context.Context, id string) error
	// GetRoute 按 ID 查询未删除路由；不存在返回 ErrRouteNotFound。
	GetRoute(ctx context.Context, id string) (*Route, error)
	// ListRoutes 分页查询未删除路由；page 从 1 起，pageSize 上限 100，按创建时间倒序。
	ListRoutes(ctx context.Context, filter RouteFilter, page, pageSize int64) ([]*Route, int64, error)
}

// RouteUsecase 路由用例：承载规则校验（模式/边界/收件策略）与渠道绑定校验
// （存在、未删除、同 biz——router 匹配按 biz 隔离，跨 biz 绑定不可能命中）。
type RouteUsecase struct {
	repo     RouteRepo
	channels ChannelRepo
}

// NewRouteUsecase new a RouteUsecase.
func NewRouteUsecase(repo RouteRepo, channels ChannelRepo) *RouteUsecase {
	return &RouteUsecase{repo: repo, channels: channels}
}

// validateRouteRule 校验一条路由规则的全部字段；channel_ids 需逐个确认
// 同 biz 渠道存在（未删除）。
func (uc *RouteUsecase) validateRouteRule(ctx context.Context, bizID string, rule *RouteRule) error {
	if !eventPatternPattern.MatchString(rule.EventPattern) {
		return fmt.Errorf("%w: bad event_pattern %q", ErrRouteInvalid, rule.EventPattern)
	}
	if rule.SeverityMin < routeSeverityMin || rule.SeverityMin > routeSeverityMax {
		return fmt.Errorf("%w: severity_min %d out of range [%d,%d]",
			ErrRouteInvalid, rule.SeverityMin, routeSeverityMin, routeSeverityMax)
	}
	if rule.Priority < routePriorityMin || rule.Priority > routePriorityMax {
		return fmt.Errorf("%w: priority %d out of range [%d,%d]",
			ErrRouteInvalid, rule.Priority, routePriorityMin, routePriorityMax)
	}
	if w := rule.AggregateWindowS; w != 0 && (w < routeWindowMin || w > routeWindowMax) {
		return fmt.Errorf("%w: aggregate_window_s %d must be 0 or in [%d,%d]",
			ErrRouteInvalid, w, routeWindowMin, routeWindowMax)
	}
	if rule.SuppressThreshold < 1 || rule.SuppressThreshold > routeThresholdMax {
		return fmt.Errorf("%w: suppress_threshold %d out of range [1,%d]",
			ErrRouteInvalid, rule.SuppressThreshold, routeThresholdMax)
	}
	if rule.SuppressDurationM < 1 || rule.SuppressDurationM > routeDurationMaxMin {
		return fmt.Errorf("%w: suppress_duration_m %d out of range [1,%d]",
			ErrRouteInvalid, rule.SuppressDurationM, routeDurationMaxMin)
	}
	if len(rule.TagMatch) > routeTagsMax {
		return fmt.Errorf("%w: tag_match exceeds %d pairs", ErrRouteInvalid, routeTagsMax)
	}
	for k, v := range rule.TagMatch {
		if k == "" || len(k) > 64 || v == "" || len(v) > 255 {
			return fmt.Errorf("%w: bad tag_match pair %q", ErrRouteInvalid, k)
		}
	}
	if err := uc.validateRouteChannels(ctx, bizID, rule.ChannelIDs, rule.Recipient); err != nil {
		return err
	}
	return nil
}

// validateRouteChannels 校验渠道绑定：非空、无重复、全部为同 biz 未删除
// 渠道；并按渠道类型校验收件策略——仅地址类渠道（email）依赖 fixed/subscription
// 解析收件地址，webhook 以实例 URL、群机器人以实例绑定群为隐含收件，可整组
// 留空（与管道 recipient 解析一致）。单条批量查询（最多 50 个 ID 逐个
// GetChannel 是 N+1）。
func (uc *RouteUsecase) validateRouteChannels(ctx context.Context, bizID string, channelIDs []string, recipient RouteRecipient) error {
	if len(channelIDs) == 0 {
		return fmt.Errorf("%w: channel_ids must not be empty", ErrRouteInvalid)
	}
	if len(channelIDs) > routeChannelsMax {
		return fmt.Errorf("%w: channel_ids exceeds %d", ErrRouteInvalid, routeChannelsMax)
	}
	seen := make(map[string]struct{}, len(channelIDs))
	for _, id := range channelIDs {
		if _, dup := seen[id]; dup {
			return fmt.Errorf("%w: duplicate channel %s", ErrRouteInvalid, id)
		}
		seen[id] = struct{}{}
	}
	channels, err := uc.channels.ListChannelsByIDs(ctx, channelIDs)
	if err != nil {
		return err
	}
	found := make(map[string]*Channel, len(channels))
	for _, ch := range channels {
		found[ch.ID] = ch
	}
	for _, id := range channelIDs {
		ch, ok := found[id]
		if !ok {
			return fmt.Errorf("%w: channel %s not found", ErrRouteInvalid, id)
		}
		if ch.BizID != bizID {
			return fmt.Errorf("%w: channel %s belongs to biz %q, not %q", ErrRouteInvalid, id, ch.BizID, bizID)
		}
		if ch.Type == ChannelTypeEmail && len(recipient.Fixed) == 0 && !recipient.Subscription {
			return fmt.Errorf("%w: email channel %s requires fixed recipients or subscription", ErrRouteInvalid, id)
		}
	}
	return nil
}

// Create 创建路由：校验规则与渠道绑定，生成 ID 与审计时间后落库；
// BizID 缺省 default。
func (uc *RouteUsecase) Create(ctx context.Context, route *Route) (*Route, error) {
	if route.BizID == "" {
		route.BizID = DefaultBizID
	}
	if err := uc.validateRouteRule(ctx, route.BizID, &route.RouteRule); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	route.ID = id
	route.CreateTime, route.UpdateTime = now, now
	if err := uc.repo.CreateRoute(ctx, route); err != nil {
		return nil, err
	}
	return route, nil
}

// Update 更新路由：Rule 整组替换（先取现路由拿 biz 再整组校验），
// Enabled 独立启停；不存在返回 ErrRouteNotFound。
func (uc *RouteUsecase) Update(ctx context.Context, id string, upd RouteUpdate) error {
	if upd.Rule != nil {
		current, err := uc.repo.GetRoute(ctx, id)
		if err != nil {
			return err
		}
		if err := uc.validateRouteRule(ctx, current.BizID, upd.Rule); err != nil {
			return err
		}
	}
	return uc.repo.UpdateRoute(ctx, id, upd)
}

// Delete 软删路由；不存在返回 ErrRouteNotFound。
func (uc *RouteUsecase) Delete(ctx context.Context, id string) error {
	return uc.repo.DeleteRoute(ctx, id)
}

// Get 查询路由详情。
func (uc *RouteUsecase) Get(ctx context.Context, id string) (*Route, error) {
	return uc.repo.GetRoute(ctx, id)
}

// List 分页查询路由；分页参数先规整再委托仓储。
func (uc *RouteUsecase) List(ctx context.Context, filter RouteFilter, page, pageSize int64) ([]*Route, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListRoutes(ctx, filter, page, pageSize)
}
