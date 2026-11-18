package biz

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// compiledMute 一条屏蔽规则的可匹配形态：匹配器已编译，创建后只读、可并发使用。
type compiledMute struct {
	Mute
	// matchers 为 nil 且 invalid 为假 = 没有标签条件（匹配任意标签）。
	matchers *monitorcontract.Matchers
	// invalid 匹配条件非法：宁可不命中，不误屏蔽（与 monitorcontract.MatchAll 同一口径）。
	invalid bool
	// severities 级别位图（bit n = 级别 n）；0 = 不限。
	severities uint8
}

func compileMute(m *Mute) *compiledMute {
	c := &compiledMute{Mute: *m}
	ms, err := monitorcontract.CompileMatchers(m.Matchers)
	if err != nil {
		c.invalid = true
	} else {
		c.matchers = ms
	}
	for _, s := range m.Severities {
		if monitorcontract.ValidSeverity(s) {
			c.severities |= 1 << uint(s) // #nosec G115 -- ValidSeverity 保证 1-4
		}
	}
	return c
}

// compileMutes 编译并按「强者在前」排序：屏蔽事件（mode 2）先于只屏蔽通知（mode 1），
// 同一方式按 ID 升序（UUIDv7 = 先创建的在前），使多条命中时的结果确定。
func compileMutes(mutes []*Mute) []*compiledMute {
	out := make([]*compiledMute, 0, len(mutes))
	for _, m := range mutes {
		out = append(out, compileMute(m))
	}
	slices.SortFunc(out, func(a, b *compiledMute) int {
		if a.Mode != b.Mode {
			return cmp.Compare(b.Mode, a.Mode)
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return out
}

// activeAt 报告屏蔽规则在 now 是否处于屏蔽时间内。一次性：[Start, End)；
// 周期：落在周期时间窗内，且在可选的 [Start, End) 之内。
func (c *compiledMute) activeAt(now time.Time) bool {
	if !c.Start.IsZero() && now.Before(c.Start) {
		return false
	}
	if !c.End.IsZero() && !now.Before(c.End) {
		return false
	}
	switch c.TimeType {
	case monitorcontract.MuteTimeOnce:
		return !c.Start.IsZero() && !c.End.IsZero()
	case monitorcontract.MuteTimePeriodic:
		// 周期规则至少要有一个时间窗（Effective 的空 windows 表示全时段，对屏蔽规则不适用）。
		return len(c.Periodic.Windows) > 0 && c.Periodic.Contains(now)
	}
	return false
}

// appliesTo 报告屏蔽规则的范围条件（分组含下级 / 规则 / 数据源）是否覆盖该任务。
func (c *compiledMute) appliesTo(rule *Rule, datasourceID string) bool {
	if c.invalid {
		return false
	}
	if c.GroupID != "" && (c.GroupPath == "" || !strings.HasPrefix(rule.GroupPath, c.GroupPath)) {
		return false
	}
	if len(c.RuleIDs) > 0 && !slices.Contains(c.RuleIDs, rule.ID) {
		return false
	}
	return len(c.DatasourceIDs) == 0 || slices.Contains(c.DatasourceIDs, datasourceID)
}

// MuteSet 对某个任务在某一时刻生效的屏蔽规则（已按范围与时间过滤、强者在前）。
// 零值与 nil 均不命中任何事件。只读，可并发使用。
type MuteSet struct {
	items []*compiledMute
}

// newMuteSet 从全量屏蔽规则里筛出对该任务此刻生效的。
func newMuteSet(all []*compiledMute, rule *Rule, datasourceID string, now time.Time) *MuteSet {
	var items []*compiledMute
	for _, c := range all {
		if c.appliesTo(rule, datasourceID) && c.activeAt(now) {
			items = append(items, c)
		}
	}
	if len(items) == 0 {
		return nil
	}
	return &MuteSet{items: items}
}

// Match 返回命中该事件的最强屏蔽规则：mode 为 0 表示未命中。多条命中时屏蔽事件优先于
// 只屏蔽通知，同一方式取 ID 最小者。
func (s *MuteSet) Match(labels map[string]string, severity int32) (id string, mode int32) {
	if s == nil {
		return "", 0
	}
	for _, c := range s.items {
		if c.severities != 0 && c.severities&(1<<uint(severity&7)) == 0 { // #nosec G115 -- 掩码后 0-7
			continue
		}
		if c.matchers.Match(labels) {
			return c.ID, c.Mode
		}
	}
	return "", 0
}
