package biz

import (
	"context"
	"time"

	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// monitorRouteProbe 以 notify 域的路由试算回答「这类监控事件有没有通知路由能匹配」（模拟触发用；
// 单向依赖 monitor → notify 的只读用例）。
type monitorRouteProbe struct {
	preview *biznotify.PreviewUsecase
}

// NewMonitorRouteProbe 构造监控域的通知路由试算适配器。
func NewMonitorRouteProbe(preview *biznotify.PreviewUsecase) bizmonitor.RouteProbe {
	return monitorRouteProbe{preview: preview}
}

// RouteMatched implements bizmonitor.RouteProbe：按管道同一语义试算（事件类型、级别下限、标签匹配——
// 监控事件的标签在内容体的 labels 下）。只回答有没有路由命中，不涉及路由内容与渠道。
func (p monitorRouteProbe) RouteMatched(ctx context.Context, eventType string, severity int32, labels map[string]string) (bool, error) {
	tags := make(map[string]any, len(labels))
	for k, v := range labels {
		tags[k] = v
	}
	out, err := p.preview.Preview(ctx, biznotify.PreviewInput{
		BizID: mc.NotifyBizID, EventType: eventType, Severity: severity, Payload: map[string]any{"labels": tags},
	}, time.Now())
	if err != nil {
		return false, err
	}
	return out.Matched != nil, nil
}
