package monitorpipeline

import (
	"slices"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// Spec 一条规则里与事件流水线有关的配置（monitor_rule 的 relabel / drop_expr / ci_match /
// resource_scope 四列）。零值 = 不改写、不丢弃、按全局缺省关联资源、不限资源范围。
type Spec struct {
	Relabel  []monitorcontract.RelabelStep
	DropExpr string
	CIMatch  monitorcontract.CIMatch
	Scope    monitorcontract.ResourceScope
}

// Pipeline 一条规则编译后的事件流水线。创建后只读，可并发使用。
type Pipeline struct {
	relabel *Relabeler
	drop    *DropExpr
	match   monitorcontract.CIMatch
	scope   monitorcontract.ResourceScope
}

// Compile 校验并编译规则的流水线配置。core 保存规则时与引擎加载规则时调用同一个函数：
// 能保存的配置引擎一定能执行。除各部分自身的校验外，还要求每个富化标签（CIMatch.EnrichTags）转义后
// 仍是合法的标签名（TagLabel）。失败返回包装了 monitorcontract.ErrInvalid 的错误。
func Compile(spec Spec) (*Pipeline, error) {
	if err := spec.CIMatch.Validate(); err != nil {
		return nil, err
	}
	if err := spec.Scope.Validate(); err != nil {
		return nil, err
	}
	for _, tag := range spec.CIMatch.EnrichTags {
		// 非 ASCII 的字符在标签名里要转义，过长的资源标签转义后会超出标签名的长度上限。
		if !monitorcontract.ValidLabelName(TagLabel(tag)) {
			return nil, invalidf("enrich tag %q is too long to be written as an event label", tag)
		}
	}
	relabel, err := CompileRelabel(spec.Relabel)
	if err != nil {
		return nil, err
	}
	drop, err := CompileDropExpr(spec.DropExpr)
	if err != nil {
		return nil, err
	}
	p := &Pipeline{relabel: relabel, drop: drop, match: spec.CIMatch, scope: spec.Scope}
	// 配置来自调用方的切片：克隆后流水线不再受其后续修改影响。
	p.match.ModelCodes = slices.Clone(spec.CIMatch.ModelCodes)
	p.match.EnrichTags = slices.Clone(spec.CIMatch.EnrichTags)
	p.scope.ModelCodes = slices.Clone(spec.Scope.ModelCodes)
	p.scope.Envs = slices.Clone(spec.Scope.Envs)
	p.scope.Tags = slices.Clone(spec.Scope.Tags)
	return p, nil
}

// Relabel 流水线第一步：就地改写事件标签（序列标签去掉 __name__ 并合并规则附加标签之后）。
// 调用方随后用改写结果计算指纹。nil 流水线不改写。
func (p *Pipeline) Relabel(labels map[string]string) {
	if p != nil {
		p.relabel.Apply(labels)
	}
}

// Verdict 事件经过关联 / 丢弃 / 范围过滤后的去留。
type Verdict int8

// 去留结论。
const (
	Keep       Verdict = iota // 继续后续阶段
	Dropped                   // 被条件丢弃表达式命中（monitorcontract.StepDropped）
	OutOfScope                // 不在规则限定的资源范围内（monitorcontract.StepOutOfScope）
)

// Resolution Pipeline.Resolve 的结论。
type Resolution struct {
	// CI 关联到的资源（只读）；未关联为 nil。CIStatus 说明原因。
	CI       *CI
	CIStatus CIMatchStatus
	// Verdict 去留；为 Dropped / OutOfScope 时调用方不为该样本产生事件。
	Verdict Verdict
	// Maintenance 关联的资源处于维护态：事件照常产生，按「只屏蔽通知」处理。
	Maintenance bool
	// DropErr 条件丢弃表达式渲染失败的原因（此时不丢弃）；供判定记录 / 模拟触发说明。
	DropErr error
}

// Resolve 流水线在指纹之后的三步，顺序固定：
//  1. 按规则的关联配置在 cis 里找资源，并把富化标签（ci_env / ci_owner / ci_model / ci_tag_*）就地写入 labels；
//  2. 条件丢弃：表达式能读到富化标签；
//  3. 资源范围过滤。
//
// cis 为 nil（索引尚未加载）时不关联；此时启用了资源范围的规则不产生事件。nil 流水线按零值配置处理。
func (p *Pipeline) Resolve(labels map[string]string, value float64, cis *CIIndex) Resolution {
	var zero Pipeline
	if p == nil {
		p = &zero
	}
	var res Resolution
	res.CI, res.CIStatus = cis.Lookup(p.match, labels)
	Enrich(labels, res.CI, p.match.EnrichTags)
	drop, err := p.drop.Drop(labels, value)
	res.DropErr = err
	switch {
	case drop:
		res.Verdict = Dropped
	case !InScope(p.scope, res.CI):
		res.Verdict = OutOfScope
	}
	res.Maintenance = res.CI.Maintenance()
	return res
}
