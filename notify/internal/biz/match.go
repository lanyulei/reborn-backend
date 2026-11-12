package biz

import (
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

// MatchPattern 事件模式匹配（路由/静默/订阅/生产者白名单同一语义，kit/notifycontract 实现）：
// '*' 全匹配；'x.y.*' 等价前缀 'x.y.'；其余精确相等。
func MatchPattern(pattern, eventType string) bool {
	return notifycontract.MatchPattern(pattern, eventType)
}

// patternSpecificity 精确 0 < 前缀 1 < 全匹配 2（同优先级路由的决胜次序）。
func patternSpecificity(pattern string) int { return notifycontract.PatternSpecificity(pattern) }

// MatchRoute 选路：biz 内启用路由中 pattern 命中 ∧ severity ≥ 下限 ∧ tag_match
// 全等（AND）者取优先级最高（priority 最小）；无命中返回 nil。
func (s *Snapshot) MatchRoute(bizID, eventType string, severity int32, payload map[string]any) *Route {
	for _, r := range s.routes[bizID] {
		if !r.Enabled || severity < r.SeverityMin || !MatchPattern(r.EventPattern, eventType) {
			continue
		}
		if !tagsMatch(r.TagMatch, payload) {
			continue
		}
		return r
	}
	return nil
}

// tagsMatch tag_match 判定：kit/notifycontract.TagsMatch（控制面试算同一实现）。
func tagsMatch(tags map[string]string, payload map[string]any) bool {
	return notifycontract.TagsMatch(tags, payload)
}

// Silenced 静默判定：biz 内任一启用规则 pattern 命中 ∧ severity ≤ severity_max
// ∧ 时间窗命中（nil=全程）即抑制。
func (s *Snapshot) Silenced(bizID, eventType string, severity int32, now time.Time) bool {
	for _, sl := range s.silences[bizID] {
		if !sl.Enabled || severity > sl.SeverityMax || !MatchPattern(sl.EventPattern, eventType) {
			continue
		}
		if sl.Window == nil || sl.Window.Contains(now) {
			return true
		}
	}
	return false
}

// Contains 报告 now 是否落在时间窗内（kit/notifycontract.WindowContains：按 tz 本地
// 时间；start>end 跨午夜；tz 非法按 UTC；时刻解析失败视为不命中）。
func (w *SilenceWindow) Contains(now time.Time) bool {
	return notifycontract.WindowContains(notifycontract.SilenceWindow(*w), now)
}
