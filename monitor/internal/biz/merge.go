package biz

import (
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/kit/promclient"
)

// 上游序列的标签上限。标签集进指纹、落进判定状态 / 告警事件 / 判定记录的 jsonb，并交给模板渲染：
// 不设上限时，一个异常（或恶意）的数据源能让每轮写入与渲染的成本随它任意增长。
const (
	// maxSeriesLabels 单条序列的标签数上限（去掉 __name__ 之后、合并规则附加标签之前）；超出时保留
	// 名称排序最靠前的这么多个（确定：与上游的返回顺序无关）。
	maxSeriesLabels = 64
	// maxLabelNameBytes 标签名的字节上限（与 monitorcontract.MaxLabelNameLen 一致）；超出的标签整个丢弃。
	maxLabelNameBytes = monitorcontract.MaxLabelNameLen
	// maxLabelValueBytes 标签值的字节上限；超出的截断（不切断字符）并以 truncatedMark 结尾。
	maxLabelValueBytes = 1024
	// truncatedMark 被截断的标签值的结尾标记（计入 maxLabelValueBytes）。
	truncatedMark = "...[truncated]"
)

// limitLabels 就地把一条序列的标签收进上限，并去掉标签值里的 U+0000（jsonb 不接受：异常的上游标签值
// 不得让整轮落库失败）。返回是否有标签被丢弃或截断。
func limitLabels(labels map[string]string) bool {
	limited := false
	for name, value := range labels {
		if len(name) > maxLabelNameBytes {
			delete(labels, name)
			limited = true
			continue
		}
		if strings.IndexByte(value, 0) >= 0 {
			value = strings.ReplaceAll(value, "\x00", "")
			labels[name] = value
		}
		if len(value) > maxLabelValueBytes {
			cut := maxLabelValueBytes - len(truncatedMark)
			for cut > 0 && !utf8.RuneStart(value[cut]) {
				cut--
			}
			labels[name] = value[:cut] + truncatedMark
			limited = true
		}
	}
	if len(labels) > maxSeriesLabels {
		names := make([]string, 0, len(labels))
		for name := range labels {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names[maxSeriesLabels:] {
			delete(labels, name)
		}
		limited = true
	}
	return limited
}

// mergeSamples 把各查询的结果合并为本轮样本，即事件流水线在状态机之前的各阶段（P2/P3 设计 §3.5，
// 各阶段的实现在 kit/monitorpipeline，与 core 的模拟触发共用）：
//
//	去掉 __name__、把上游标签收进上限（limitLabels，条数记在 Limited）、合并规则附加标签（附加标签优先）
//	→ 标签改写（pipe.Relabel）
//	→ 算指纹：开启级别抑制时不含级别，未开启时含级别（各级别是各自独立的事件）
//	→ 关联资源并写入 ci_ 富化标签（指纹之后：富化标签不进指纹）→ 条件丢弃 → 资源范围过滤（pipe.Resolve）
//	→ 级别抑制：同指纹只留最高级别；同一指纹的重复序列只留先出现的一条
//
// 被丢弃 / 不在范围内的点不进 Kept：对已有的状态而言等同于本轮没有出现。规则间抑制、拓扑抑制、屏蔽
// （含维护态）在状态机里按样本判定（它们取决于该序列此前的状态）。
//
// results[i] 对应 rule.Queries[i]；序列的 Labels 被就地改写并由返回值接管（promclient 的结果
// 归调用方所有），调用后不得再使用 results。pipe 为 nil = 规则没有流水线配置；cis 为 nil = 资源索引
// 尚未加载（不关联；启用了资源范围的规则不产生事件）。纯函数：不做 I/O。
func mergeSamples(rule *Rule, datasourceID string, results [][]promclient.Series, pipe *monitorpipeline.Pipeline,
	cis *monitorpipeline.CIIndex,
) *SampleSet {
	total := 0
	for _, r := range results {
		total += len(r)
	}
	set := &SampleSet{
		Kept:   make([]Sample, 0, total),
		Series: int32(min(total, 1<<30)), // #nosec G115 -- 已夹紧
		index:  make(map[string]int32, total),
	}
	for qi, series := range results {
		severity := rule.Queries[qi].Severity
		fpSeverity := severity
		if rule.LevelInhibit {
			fpSeverity = 0
		}
		for i := range series {
			if len(series[i].Points) == 0 {
				continue
			}
			labels := series[i].Labels
			if labels == nil {
				labels = make(map[string]string, len(rule.Labels))
			}
			delete(labels, "__name__")
			if limitLabels(labels) {
				set.Limited++
			}
			for name, value := range rule.Labels {
				labels[name] = value
			}
			pipe.Relabel(labels)
			sample := Sample{
				Fingerprint: monitorcontract.Fingerprint(rule.ID, datasourceID, labels, fpSeverity),
				Labels:      labels,
				Value:       series[i].Points[len(series[i].Points)-1].Value,
				Severity:    severity,
				QueryIndex:  int32(qi), // #nosec G115 -- 查询至多 MaxRuleQueries 条
			}
			res := pipe.Resolve(labels, sample.Value, cis)
			if res.DropErr != nil {
				set.DropErrors++
			}
			switch res.Verdict {
			case monitorpipeline.Dropped:
				set.DroppedCount++
				if len(set.Dropped) < monitorcontract.MaxEvalPoints {
					set.Dropped = append(set.Dropped, sample)
				}
				continue
			case monitorpipeline.OutOfScope:
				set.OutOfScopeCount++
				if len(set.OutOfScope) < monitorcontract.MaxEvalPoints {
					set.OutOfScope = append(set.OutOfScope, sample)
				}
				continue
			case monitorpipeline.Keep:
			}
			sample.CI = res.CI
			at, dup := set.index[sample.Fingerprint]
			if !dup {
				set.index[sample.Fingerprint] = int32(len(set.Kept)) // #nosec G115 -- 受 max_series × 查询数约束
				set.Kept = append(set.Kept, sample)
				continue
			}
			// 同指纹不同级别只会出现在级别抑制开启时（否则级别已进指纹）。
			switch kept := &set.Kept[at]; {
			case sample.Severity > kept.Severity:
				set.inhibit(*kept)
				*kept = sample
			case sample.Severity < kept.Severity:
				set.inhibit(sample)
			}
		}
	}
	return set
}

func (s *SampleSet) inhibit(sample Sample) {
	if len(s.Inhibited) < monitorcontract.MaxEvalPoints {
		s.Inhibited = append(s.Inhibited, sample)
	}
}
