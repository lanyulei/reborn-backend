package monitorpipeline

import (
	"cmp"
	"slices"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// AlertRef 抑制判定看到的一个告警：既用作源（活跃告警快照里的一项），也用作目标（待判定的事件）。
// GroupPath 是规则所属分组的物化路径；CIID 为关联的资源（没有为空）；Labels 只读。
type AlertRef struct {
	ID        string
	RuleID    string
	GroupPath string
	Severity  int32
	Labels    map[string]string
	CIID      string
}

// InhibitRule 一条启用的抑制规则（monitor_inhibit 一行 + 作用分组的物化路径）。GroupPath 为空 = 作用于
// 全部分组；作用分组已删除的规则不要传进来。
type InhibitRule struct {
	ID        string
	GroupPath string
	Source    monitorcontract.InhibitSelector
	Target    monitorcontract.InhibitSelector
	Equal     []string
}

// InhibitHit 一次抑制命中：Kind 为 monitorcontract.InhibitRule 时 InhibitID 是抑制规则，
// 为 InhibitTopology 时 InhibitID 为空；AlertID 是造成抑制的源告警。
type InhibitHit struct {
	Kind      int32
	InhibitID string
	AlertID   string
}

type compiledSelector struct {
	ruleIDs []string
	// severities 级别位图（bit n = 级别 n）；0 = 不限。
	severities uint8
	matchers   *monitorcontract.Matchers
}

func (s *compiledSelector) match(a *AlertRef) bool {
	if len(s.ruleIDs) > 0 && !slices.Contains(s.ruleIDs, a.RuleID) {
		return false
	}
	if s.severities != 0 && s.severities&(1<<uint(a.Severity&7)) == 0 { // #nosec G115 -- 掩码后 0-7
		return false
	}
	return s.matchers.Match(a.Labels)
}

type compiledInhibit struct {
	id, groupPath  string
	source, target compiledSelector
	equal          []string
}

// covers 报告告警是否在抑制规则的作用分组（含下级）内。
func (c *compiledInhibit) covers(a *AlertRef) bool {
	return c.groupPath == "" || strings.HasPrefix(a.GroupPath, c.groupPath)
}

// key 相等标签取值拼成的键；事件上不存在的标签按空串（与匹配器同一口径：缺失 = 空）。
func (c *compiledInhibit) key(buf []byte, labels map[string]string) []byte {
	buf = buf[:0]
	for _, name := range c.equal {
		buf = append(buf, labels[name]...)
		buf = append(buf, 0xff)
	}
	return buf
}

// Inhibitor 全部启用的抑制规则（已校验、已编译）。创建后只读；nil 没有任何规则。
type Inhibitor struct {
	rules []compiledInhibit
}

// CompileInhibits 编译抑制规则。不合法的规则（过不了 monitorcontract.ValidateInhibit）被跳过——宁可
// 不抑制，不误抑制，与屏蔽规则同一口径；规则按 ID 排序，多条命中时结果确定。
func CompileInhibits(rules []InhibitRule) *Inhibitor {
	out := &Inhibitor{rules: make([]compiledInhibit, 0, len(rules))}
	for i := range rules {
		r := &rules[i]
		if monitorcontract.ValidateInhibit(r.Source, r.Target, r.Equal) != nil {
			continue
		}
		c := compiledInhibit{id: r.ID, groupPath: r.GroupPath, equal: slices.Clone(r.Equal)}
		var ok1, ok2 bool
		c.source, ok1 = compileSelector(r.Source)
		c.target, ok2 = compileSelector(r.Target)
		if ok1 && ok2 {
			out.rules = append(out.rules, c)
		}
	}
	slices.SortFunc(out.rules, func(a, b compiledInhibit) int { return cmp.Compare(a.id, b.id) })
	return out
}

func compileSelector(s monitorcontract.InhibitSelector) (compiledSelector, bool) {
	c := compiledSelector{ruleIDs: slices.Clone(s.RuleIDs)}
	ms, err := monitorcontract.CompileMatchers(s.Matchers)
	if err != nil {
		return c, false
	}
	c.matchers = ms
	for _, sev := range s.Severities {
		if monitorcontract.ValidSeverity(sev) {
			c.severities |= 1 << uint(sev) // #nosec G115 -- ValidSeverity 保证 1-4
		}
	}
	return c, true
}

// Len 生效的抑制规则数。
func (in *Inhibitor) Len() int {
	if in == nil {
		return 0
	}
	return len(in.rules)
}

// sourceEntry 某条规则、某组相等标签取值下的源告警：pure 只匹配源条件，both 同时匹配目标条件。
// 各取 ID 最小者（UUIDv7 = 最早触发），结果确定。
type sourceEntry struct {
	pure, both string
}

// InhibitSnapshot 一份活跃告警快照上的抑制索引。创建后只读，可并发使用；nil 不抑制任何事件。
type InhibitSnapshot struct {
	in *Inhibitor
	// sources[i] 对应 in.rules[i]：相等标签键 → 源告警。
	sources []map[string]sourceEntry
	// byCI 资源 ID → 该资源上的活跃告警（ID 最小者），拓扑抑制用。
	byCI map[string]string
}

// Snapshot 以「当前活跃且未被抑制」的告警建立抑制索引（被抑制的告警不能再抑制别人，避免链式误伤）。
// active 只在调用期间读取。接收者为 nil 时仍返回可用于拓扑抑制的快照。
func (in *Inhibitor) Snapshot(active []AlertRef) *InhibitSnapshot {
	s := &InhibitSnapshot{in: in, byCI: map[string]string{}}
	if in != nil {
		s.sources = make([]map[string]sourceEntry, len(in.rules))
	}
	var buf []byte
	for i := range active {
		a := &active[i]
		if a.CIID != "" {
			if cur, ok := s.byCI[a.CIID]; !ok || a.ID < cur {
				s.byCI[a.CIID] = a.ID
			}
		}
		if in == nil {
			continue
		}
		for ri := range in.rules {
			r := &in.rules[ri]
			if !r.covers(a) || !r.source.match(a) {
				continue
			}
			if s.sources[ri] == nil {
				s.sources[ri] = map[string]sourceEntry{}
			}
			buf = r.key(buf, a.Labels)
			e := s.sources[ri][string(buf)]
			if r.target.match(a) {
				if e.both == "" || a.ID < e.both {
					e.both = a.ID
				}
			} else if e.pure == "" || a.ID < e.pure {
				e.pure = a.ID
			}
			s.sources[ri][string(buf)] = e
		}
	}
	return s
}

// Match 报告事件是否被某条抑制规则抑制：事件在规则的作用分组内、满足目标条件，且快照里有同样在作用
// 分组内、满足源条件、相等标签取值全部相同的告警。同时满足源与目标两侧条件的告警之间互不抑制
// （与 Alertmanager 一致，避免一批同类告警互相抑制后全部静默）；告警不会抑制自己。
// 多条规则命中时取规则 ID 最小者。
func (s *InhibitSnapshot) Match(target *AlertRef) (InhibitHit, bool) {
	if s == nil || s.in == nil {
		return InhibitHit{}, false
	}
	var arr [128]byte
	buf := arr[:0]
	for ri := range s.in.rules {
		sources := s.sources[ri]
		if len(sources) == 0 {
			continue
		}
		r := &s.in.rules[ri]
		if !r.covers(target) || !r.target.match(target) {
			continue
		}
		buf = r.key(buf, target.Labels)
		e, ok := sources[string(buf)]
		if !ok {
			continue
		}
		if e.pure != "" {
			return InhibitHit{Kind: monitorcontract.InhibitRule, InhibitID: r.id, AlertID: e.pure}, true
		}
		if e.both != "" && !r.source.match(target) {
			return InhibitHit{Kind: monitorcontract.InhibitRule, InhibitID: r.id, AlertID: e.both}, true
		}
	}
	return InhibitHit{}, false
}

// Upstream 拓扑抑制：upstreamIDs（事件关联资源的上游资源，CI.UpstreamIDs）中任一资源在快照里有活跃
// 告警即抑制，返回按 upstreamIDs 顺序遇到的第一个。只对开启了 topology_inhibit 的规则调用。
func (s *InhibitSnapshot) Upstream(upstreamIDs []string) (InhibitHit, bool) {
	if s == nil {
		return InhibitHit{}, false
	}
	for _, id := range upstreamIDs {
		if alertID, ok := s.byCI[id]; ok {
			return InhibitHit{Kind: monitorcontract.InhibitTopology, AlertID: alertID}, true
		}
	}
	return InhibitHit{}, false
}
