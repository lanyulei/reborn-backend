package biz

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestPreviewUsecase_Preview 试算语义与管道一致：静默命中、priority → 精确度决胜、
// tag_match 点分路径、停用路由/渠道处理、模板解析链。
func TestPreviewUsecase_Preview(t *testing.T) {
	t.Parallel()
	routes, silences, channels, templates := newRouteRepoStub(), newSilenceRepoStub(), newChannelRepoStub(), newTemplateRepoStub()
	ctx := context.Background()
	chUC := NewChannelUsecase(channels, new(eventWriterStub), channels)
	hook, err := chUC.Create(ctx, &Channel{BizID: "monitor", Type: ChannelTypeWebhook, Name: "hook", Config: &ChannelConfig{Webhook: &WebhookConfig{URL: "https://x/h", Method: "POST"}}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	off, err := chUC.Create(ctx, &Channel{BizID: "monitor", Type: ChannelTypeDingtalk, Name: "ding", Secret: "s", Config: &ChannelConfig{Robot: &RobotConfig{}}, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	mk := func(id, pattern string, prio int32, tags map[string]string, enabled bool) {
		_ = routes.CreateRoute(ctx, &Route{ID: id, BizID: "monitor", Enabled: enabled, RouteRule: RouteRule{
			EventPattern: pattern, SeverityMin: 2, Priority: prio, TagMatch: tags, ChannelIDs: []string{hook.ID, off.ID, "018f3e1a-dead-7000-8000-000000000000"},
			Recipient: RouteRecipient{Fixed: []string{"ops@example.com"}}, AggregateWindowS: 60,
		}})
	}
	mk("018f3e1a-0000-7000-8000-00000000000a", "monitor.*", 5, nil, true)
	mk("018f3e1a-0000-7000-8000-00000000000b", "monitor.alert.firing", 5, nil, true)                  // 同优先级：精确优先
	mk("018f3e1a-0000-7000-8000-00000000000c", "*", 1, map[string]string{"labels.env": "prod"}, true) // 最高优先级但标签不符
	mk("018f3e1a-0000-7000-8000-00000000000d", "monitor.alert.firing", 1, nil, false)                 // 停用
	_ = templates.CreateTemplate(ctx, &Template{ID: "018f3e1a-0000-7000-8000-0000000000t1", BizID: "monitor", EventType: "*", ChannelType: ChannelTypeWebhook, Enabled: true})
	uc := NewPreviewUsecase(routes, silences, channels, templates)
	out, err := uc.Preview(ctx, PreviewInput{BizID: "monitor", EventType: "monitor.alert.firing", Severity: 3, GroupKey: "g", Payload: map[string]any{"labels": map[string]any{"env": "dev"}}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if out.Silenced || out.Matched == nil || out.Matched.ID != "018f3e1a-0000-7000-8000-00000000000b" || len(out.Candidates) != 2 || !out.Aggregate {
		t.Fatalf("preview = %+v", out)
	}
	if len(out.Channels) != 2 || out.Channels[0].Channel.ID != hook.ID || out.Channels[0].TemplateID != "018f3e1a-0000-7000-8000-0000000000t1" ||
		out.Channels[1].Channel.Enabled || out.Channels[1].TemplateID != "" {
		t.Fatalf("channels = %+v", out.Channels)
	}
	// 标签命中 → 优先级 1 的通配路由生效。
	out, _ = uc.Preview(ctx, PreviewInput{BizID: "monitor", EventType: "monitor.alert.firing", Severity: 3, Payload: map[string]any{"labels": map[string]any{"env": "prod"}}}, time.Now())
	if out.Matched == nil || out.Matched.ID != "018f3e1a-0000-7000-8000-00000000000c" || len(out.Candidates) != 3 || out.Aggregate {
		t.Fatalf("tag match preview = %+v", out)
	}
	// 静默：全程规则命中。
	_ = silences.CreateSilence(ctx, &Silence{ID: "018f3e1a-0000-7000-8000-0000000000s1", BizID: "monitor", Enabled: true, SilenceRule: SilenceRule{EventPattern: "monitor.*", SeverityMax: 4}})
	out, _ = uc.Preview(ctx, PreviewInput{BizID: "monitor", EventType: "monitor.alert.firing", Severity: 3}, time.Now())
	if !out.Silenced || len(out.SilenceIDs) != 1 || out.Matched == nil {
		t.Fatalf("silenced preview = %+v", out)
	}
	if _, err := uc.Preview(ctx, PreviewInput{EventType: "Bad Type", Severity: 3}, time.Now()); !errors.Is(err, ErrRouteInvalid) {
		t.Fatalf("invalid event type error = %v", err)
	}
	if out, _ := uc.Preview(ctx, PreviewInput{BizID: "other", EventType: "monitor.alert.firing", Severity: 3}, time.Now()); out.Matched != nil || len(out.Channels) != 0 {
		t.Fatalf("no route biz preview = %+v", out)
	}
}
