package biz

import (
	"fmt"
	"strings"
	"time"
)

// MatchPattern 事件模式匹配（路由/静默/订阅/生产者白名单同一语义）：
// '*' 全匹配；'x.y.*' 等价前缀 'x.y.'；其余精确相等。
func MatchPattern(pattern, eventType string) bool {
	switch {
	case pattern == "*":
		return true
	case strings.HasSuffix(pattern, ".*"):
		return strings.HasPrefix(eventType, pattern[:len(pattern)-1])
	default:
		return pattern == eventType
	}
}

// patternSpecificity 精确 0 < 前缀 1 < 全匹配 2（同优先级路由的决胜次序）。
func patternSpecificity(pattern string) int {
	switch {
	case pattern == "*":
		return 2
	case strings.HasSuffix(pattern, ".*"):
		return 1
	default:
		return 0
	}
}

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

// tagsMatch tag_match 为空无条件；否则 payload 须为对象且每个键的字面值相等
// （数字/布尔按 JSON 文本比较）。键先按顶层字面键查找，未命中再按点分路径
// 下钻嵌套对象（如 "labels.env" → payload.labels.env），适配监控事件把标签
// 放在嵌套字段的常见形态。
func tagsMatch(tags map[string]string, payload map[string]any) bool {
	if len(tags) == 0 {
		return true
	}
	if payload == nil {
		return false
	}
	for k, want := range tags {
		v, ok := lookupPath(payload, k)
		if !ok {
			return false
		}
		if scalarString(v) != want {
			return false
		}
	}
	return true
}

// lookupPath 顶层字面键优先；否则按 "." 逐级进入嵌套 map。
func lookupPath(payload map[string]any, key string) (any, bool) {
	if v, ok := payload[key]; ok {
		return v, true
	}
	if !strings.Contains(key, ".") {
		return nil, false
	}
	var cur any = payload
	for _, part := range strings.Split(key, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[part]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func scalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%g", x)
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%v", x)
	}
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

// Contains 报告 now 是否落在时间窗内（按 tz 本地时间；start>end 跨午夜）。
// tz 非法按 UTC；时刻解析失败视为不命中（宁放行不误静默）。
func (w *SilenceWindow) Contains(now time.Time) bool {
	loc := time.UTC
	if w.Tz != "" {
		if l, err := time.LoadLocation(w.Tz); err == nil {
			loc = l
		}
	}
	start, ok1 := parseClock(w.Start)
	end, ok2 := parseClock(w.End)
	if !ok1 || !ok2 {
		return false
	}
	t := now.In(loc)
	cur := t.Hour()*60 + t.Minute()
	if start <= end {
		return cur >= start && cur < end
	}
	return cur >= start || cur < end // 跨午夜
}

func parseClock(s string) (int, bool) {
	var h, m int
	if _, err := fmt.Sscanf(s, "%d:%d", &h, &m); err != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}
