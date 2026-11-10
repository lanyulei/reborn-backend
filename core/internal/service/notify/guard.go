package service

import (
	"context"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
)

// bizGuard 通知控制面按 biz 隔离的 service 层守卫（身份来自传输层 claims，
// 故在 service 判定；RBAC 中间件只回答「有无接口权限」，回答不了「对哪个 biz」）：
//   - 不限用户（超管 / 任一启用角色 biz_scope 为空——现有角色缺省）一律放行，
//     行为与引入前完全一致；
//   - 受限用户：写/读单行须目标 biz ∈ 范围（403 NOTIFY_BIZ_FORBIDDEN）；列表
//     须携带 biz（或 channel_id）过滤（400 NOTIFY_BIZ_REQUIRED），越权 403。
type bizGuard struct {
	resolver biz.BizScopeResolver
}

func newBizGuard(resolver biz.BizScopeResolver) bizGuard { return bizGuard{resolver: resolver} }

// scope 解析当前操作者范围；unrestricted=true 时 set 为 nil。
func (g bizGuard) scope(ctx context.Context) (bool, map[string]struct{}, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return false, nil, err
	}
	unrestricted, list, err := g.resolver.BizScope(ctx, userID)
	if err != nil {
		return false, nil, err
	}
	if unrestricted {
		return true, nil, nil
	}
	set := make(map[string]struct{}, len(list))
	for _, b := range list {
		set[b] = struct{}{}
	}
	return false, set, nil
}

// requireBiz 目标 biz（空 = default）须在范围内。
func (g bizGuard) requireBiz(ctx context.Context, bizID string) error {
	unrestricted, set, err := g.scope(ctx)
	if err != nil || unrestricted {
		return err
	}
	if bizID == "" {
		bizID = biz.DefaultBizID
	}
	if _, ok := set[bizID]; !ok {
		return fmt.Errorf("%w: biz %q", biz.ErrBizForbidden, bizID)
	}
	return nil
}

// requireRow 受限用户先读目标行取其 biz 再校验（不限用户不读，零额外开销）。
func (g bizGuard) requireRow(ctx context.Context, bizOf func() (string, error)) error {
	unrestricted, set, err := g.scope(ctx)
	if err != nil || unrestricted {
		return err
	}
	bizID, err := bizOf()
	if err != nil {
		return err
	}
	if _, ok := set[bizID]; !ok {
		return fmt.Errorf("%w: biz %q", biz.ErrBizForbidden, bizID)
	}
	return nil
}

// listBiz 列表过滤：不限用户原样透传请求 biz（可空）；受限用户必须携带且在范围内。
func (g bizGuard) listBiz(ctx context.Context, requested *string) (string, error) {
	unrestricted, set, err := g.scope(ctx)
	if err != nil {
		return "", err
	}
	if unrestricted {
		if requested == nil {
			return "", nil
		}
		return *requested, nil
	}
	if requested == nil || *requested == "" {
		return "", biz.ErrBizRequired
	}
	if _, ok := set[*requested]; !ok {
		return "", fmt.Errorf("%w: biz %q", biz.ErrBizForbidden, *requested)
	}
	return *requested, nil
}

// channelLookup 取渠道（运行表控制台按渠道归属 biz 隔离）。
type channelLookup interface {
	Get(ctx context.Context, id string) (*biz.Channel, error)
}

// requireChannelFilter 运行表列表（任务/明细/死信无 biz 列）：受限用户必须携带
// channel_id 且该渠道 biz 在范围内。
func (g bizGuard) requireChannelFilter(ctx context.Context, channelID string, channels channelLookup) error {
	unrestricted, _, err := g.scope(ctx)
	if err != nil || unrestricted {
		return err
	}
	if channelID == "" {
		return fmt.Errorf("%w: channel_id", biz.ErrBizRequired)
	}
	return g.requireRow(ctx, func() (string, error) {
		ch, err := channels.Get(ctx, channelID)
		if err != nil {
			return "", err
		}
		return ch.BizID, nil
	})
}
