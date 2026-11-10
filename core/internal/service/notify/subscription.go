package service

import (
	"context"
	"fmt"
	"strings"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifySubscriptionService 实现 api/notify/v1 生成的 NotifySubscriptionServiceServer
// （订阅中心）：目标用户缺省=操作者本人（claims），指定他人须 admin（代管），
// 属主校验在本层强制；proto 与领域模型显式转换。
type NotifySubscriptionService struct {
	notifyv1.UnimplementedNotifySubscriptionServiceServer

	uc    *biz.SubscriptionUsecase
	admin biz.SubscriptionAdminResolver
}

// NewNotifySubscriptionService new a NotifySubscriptionService.
func NewNotifySubscriptionService(uc *biz.SubscriptionUsecase, admin biz.SubscriptionAdminResolver) *NotifySubscriptionService {
	return &NotifySubscriptionService{uc: uc, admin: admin}
}

// resolveTargetUser 解析订阅目标用户并校验代管权限：requested 缺省或等于
// 操作者 → 本人；指定他人 → 操作者须 is_admin，否则 ErrSubscriptionForbidden。
func (s *NotifySubscriptionService) resolveTargetUser(ctx context.Context, requested string) (string, error) {
	operator, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return "", err
	}
	// UUID 比较大小写不敏感（string.uuid 校验接受大写，库内与 JWT subject
	// 为规范小写）；命中本人时返回操作者（规范形态）。
	if requested == "" || strings.EqualFold(requested, operator) {
		return operator, nil
	}
	admin, err := s.admin.IsAdminUser(ctx, operator)
	if err != nil {
		return "", err
	}
	if !admin {
		return "", biz.ErrSubscriptionForbidden
	}
	return requested, nil
}

// prefsFromProto 转换渠道偏好载荷（oneof → DO 恰一正取值）。oneof 未设置
// 的条目（如 `"email":{}`）protovalidate 的 map 规则拦不住，此处显式拒绝
// 而非静默丢弃——否则请求 200 但该渠道偏好实际未保存。
func prefsFromProto(proto map[string]*notifyv1.SubscriptionPref) (biz.ChannelPrefs, error) {
	prefs := make(biz.ChannelPrefs, len(proto))
	for channelType, pref := range proto {
		switch pref.GetKind().(type) {
		case *notifyv1.SubscriptionPref_Subscribed:
			prefs[channelType] = biz.SubscriptionPref{Subscribed: pref.GetSubscribed()}
		case *notifyv1.SubscriptionPref_Target:
			prefs[channelType] = biz.SubscriptionPref{Target: pref.GetTarget()}
		default:
			return nil, fmt.Errorf("%w: channel_prefs[%q] must set subscribed or target", biz.ErrSubscriptionInvalid, channelType)
		}
	}
	return prefs, nil
}

// toProtoPrefs 偏好 DO 转 proto（target 非空落 target、否则落开关）。
func toProtoPrefs(prefs biz.ChannelPrefs) map[string]*notifyv1.SubscriptionPref {
	out := make(map[string]*notifyv1.SubscriptionPref, len(prefs))
	for channelType, pref := range prefs {
		if pref.Target != "" {
			out[channelType] = &notifyv1.SubscriptionPref{
				Kind: &notifyv1.SubscriptionPref_Target{Target: pref.Target},
			}
		} else {
			out[channelType] = &notifyv1.SubscriptionPref{
				Kind: &notifyv1.SubscriptionPref_Subscribed{Subscribed: pref.Subscribed},
			}
		}
	}
	return out
}

// toProtoSubscription 领域模型转 proto 实体。
func toProtoSubscription(sub *biz.Subscription) *notifyv1.NotifySubscription {
	return &notifyv1.NotifySubscription{
		Id:           sub.ID,
		UserId:       sub.UserID,
		BizId:        sub.BizID,
		EventPattern: sub.EventPattern,
		ChannelPrefs: toProtoPrefs(sub.ChannelPrefs),
		Enabled:      sub.Enabled,
		CreateTime:   timestamppb.New(sub.CreateTime),
		UpdateTime:   timestamppb.New(sub.UpdateTime),
	}
}

// UpsertSubscription implements proto NotifySubscriptionService/UpsertSubscription：
// 目标用户缺省=本人、指定他人须 admin；同自然键整组替换偏好与启停。
func (s *NotifySubscriptionService) UpsertSubscription(ctx context.Context, req *notifyv1.UpsertSubscriptionRequest) (*notifyv1.UpsertSubscriptionResponse, error) {
	target, err := s.resolveTargetUser(ctx, req.GetUserId())
	if err != nil {
		return nil, err
	}
	enabled := true // 与其它资源一致：未指定默认启用
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	prefs, err := prefsFromProto(req.GetChannelPrefs())
	if err != nil {
		return nil, err
	}
	sub, err := s.uc.Upsert(ctx, &biz.Subscription{
		UserID:       target,
		BizID:        req.GetBizId(),
		EventPattern: req.GetEventPattern(),
		ChannelPrefs: prefs,
		Enabled:      enabled,
	})
	if err != nil {
		return nil, err
	}
	return &notifyv1.UpsertSubscriptionResponse{Subscription: toProtoSubscription(sub)}, nil
}

// DeleteSubscription implements proto NotifySubscriptionService/DeleteSubscription：
// 先取订阅做属主校验（本人或 admin），再软删。
func (s *NotifySubscriptionService) DeleteSubscription(ctx context.Context, req *notifyv1.DeleteSubscriptionRequest) (*notifyv1.DeleteSubscriptionResponse, error) {
	operator, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sub, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(sub.UserID, operator) {
		admin, err := s.admin.IsAdminUser(ctx, operator)
		if err != nil {
			return nil, err
		}
		if !admin {
			return nil, biz.ErrSubscriptionForbidden
		}
	}
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &notifyv1.DeleteSubscriptionResponse{}, nil
}

// ListSubscriptions implements proto NotifySubscriptionService/ListSubscriptions：
// 目标用户缺省=本人、指定他人须 admin；恒按单一目标用户过滤（无全量模式）。
func (s *NotifySubscriptionService) ListSubscriptions(ctx context.Context, req *notifyv1.ListSubscriptionsRequest) (*notifyv1.ListSubscriptionsResponse, error) {
	target, err := s.resolveTargetUser(ctx, req.GetUserId())
	if err != nil {
		return nil, err
	}
	filter := biz.SubscriptionFilter{
		UserID:  target,
		Keyword: req.GetKeyword(),
		Enabled: req.Enabled,
	}
	subs, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ListSubscriptionsResponse{
		Items: make([]*notifyv1.NotifySubscription, 0, len(subs)),
		Total: total,
	}
	for _, sub := range subs {
		resp.Items = append(resp.Items, toProtoSubscription(sub))
	}
	return resp, nil
}
