package biz

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

// PreviewUsecase 路由试算：对一条假想事件按管道同一语义（kit/notifycontract 的
// 模式/标签/时间窗匹配函数，决胜序 priority → 精确度 → id）回答"会不会被静默、
// 被哪条路由接住、发到哪些渠道实例、各用哪个模板、是否进聚合"。只读：不落库、
// 不 bump 版本；配置表规模，不分页。
type PreviewUsecase struct {
	routes    RouteRepo
	silences  SilenceRepo
	channels  ChannelRepo
	templates TemplateRepo
}

// NewPreviewUsecase new a PreviewUsecase.
func NewPreviewUsecase(routes RouteRepo, silences SilenceRepo, channels ChannelRepo, templates TemplateRepo) *PreviewUsecase {
	return &PreviewUsecase{routes: routes, silences: silences, channels: channels, templates: templates}
}

// PreviewInput 假想事件。
type PreviewInput struct {
	BizID     string
	EventType string
	Severity  int32
	Payload   map[string]any // tag_match 比对用；nil = 无载荷
	GroupKey  string
}

// PreviewChannel 生效路由绑定的渠道实例及模板解析结果（TemplateID 空 = 内置默认）。
type PreviewChannel struct {
	Channel    *Channel
	TemplateID string
}

// RoutePreview 试算结果。
type RoutePreview struct {
	Silenced   bool
	SilenceIDs []string
	Matched    *Route   // Candidates[0]；无命中为 nil
	Candidates []*Route // 全部命中路由，按决胜序
	Channels   []PreviewChannel
	Aggregate  bool // 生效路由配置聚合窗口且事件带 group_key
}

// Preview 执行试算。
func (uc *PreviewUsecase) Preview(ctx context.Context, in PreviewInput, now time.Time) (*RoutePreview, error) {
	if in.BizID == "" {
		in.BizID = DefaultBizID
	}
	if !eventTypePattern.MatchString(in.EventType) {
		return nil, fmt.Errorf("%w: event_type %q 须为小写点分事件名", ErrRouteInvalid, in.EventType)
	}
	if in.Severity < 1 || in.Severity > 4 {
		return nil, fmt.Errorf("%w: severity %d out of range", ErrRouteInvalid, in.Severity)
	}
	out := &RoutePreview{}
	silences, err := uc.silences.ListSilencesByBiz(ctx, in.BizID)
	if err != nil {
		return nil, err
	}
	for _, sl := range silences {
		if !sl.Enabled || in.Severity > sl.SeverityMax || !notifycontract.MatchPattern(sl.EventPattern, in.EventType) {
			continue
		}
		if sl.Window == nil || notifycontract.WindowContains(*sl.Window, now) {
			out.Silenced = true
			out.SilenceIDs = append(out.SilenceIDs, sl.ID)
		}
	}
	routes, err := uc.routes.ListRoutesByBiz(ctx, in.BizID)
	if err != nil {
		return nil, err
	}
	for _, r := range routes {
		if !r.Enabled || in.Severity < r.SeverityMin || !notifycontract.MatchPattern(r.EventPattern, in.EventType) {
			continue
		}
		if !notifycontract.TagsMatch(r.TagMatch, in.Payload) {
			continue
		}
		out.Candidates = append(out.Candidates, r)
	}
	// 与管道 NewSnapshot 的预排序完全一致。
	sort.SliceStable(out.Candidates, func(i, j int) bool {
		a, b := out.Candidates[i], out.Candidates[j]
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		if sa, sb := notifycontract.PatternSpecificity(a.EventPattern), notifycontract.PatternSpecificity(b.EventPattern); sa != sb {
			return sa < sb
		}
		return a.ID < b.ID
	})
	if len(out.Candidates) == 0 {
		return out, nil
	}
	out.Matched = out.Candidates[0]
	out.Aggregate = out.Matched.AggregateWindowS > 0 && in.GroupKey != ""
	channels, err := uc.channels.ListChannelsByIDs(ctx, out.Matched.ChannelIDs)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*Channel, len(channels))
	for _, ch := range channels {
		byID[ch.ID] = ch
	}
	templates, err := uc.templates.ListTemplatesByBiz(ctx, in.BizID)
	if err != nil {
		return nil, err
	}
	type key struct{ event, channel string }
	index := make(map[key]*Template, len(templates))
	for _, t := range templates {
		if t.Enabled {
			index[key{t.EventType, t.ChannelType}] = t
		}
	}
	for _, id := range out.Matched.ChannelIDs {
		ch, ok := byID[id]
		if !ok {
			continue // 已删除的悬挂绑定：管道同样跳过
		}
		pc := PreviewChannel{Channel: ch}
		// 解析链与管道 Snapshot.Template 一致：(event,channel) > (event,*) > (*,channel) > (*,*)。
		for _, k := range []key{{in.EventType, ch.Type}, {in.EventType, TemplateEventTypeAny}, {TemplateEventTypeAny, ch.Type}, {TemplateEventTypeAny, TemplateChannelAny}} {
			if t, ok := index[k]; ok {
				pc.TemplateID = t.ID
				break
			}
		}
		out.Channels = append(out.Channels, pc)
	}
	return out, nil
}
