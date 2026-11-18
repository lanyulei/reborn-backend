package monitorpipeline

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 改写产物的上限：改写在指纹之前，产物会进指纹与各张表的 jsonb。
const (
	// maxRelabelValueBytes 改写写入的标签值上限，超出截断（不切断字符）。
	maxRelabelValueBytes = 1024
	// maxRelabelLabels labelmap 最多把标签集撑到这么多个，之后不再新增。
	maxRelabelLabels = 128
	// defaultRelabelSeparator / defaultRelabelRegex / defaultRelabelReplacement 同 Prometheus 的缺省值。
	defaultRelabelSeparator   = ";"
	defaultRelabelRegex       = "(.*)"
	defaultRelabelReplacement = "$1"
)

type relabelStep struct {
	action      string
	sources     []string
	separator   string
	re          *regexp.Regexp
	target      string
	replacement string
}

// Relabeler 一条规则的标签改写步骤（已校验、正则已编译）。nil 不改写。
type Relabeler struct {
	steps []relabelStep
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{monitorcontract.ErrInvalid}, args...)...)
}

// CompileRelabel 校验并编译改写步骤；没有步骤返回 nil, nil。规则：至多 MaxRelabelSteps 步；正则不超过
// MaxRelabelRegexLen 字节、可编译（按 Prometheus 方式整体锚定）；replace 须有合法且不以 "__" 开头的
// target_label，source_labels 至多 MaxRelabelSourceLabels 个合法标签名；labelmap / labeldrop / labelkeep
// 须有 regex，且不得带 source_labels / target_label（labeldrop / labelkeep 也不得带 replacement）。
// 失败返回包装了 monitorcontract.ErrInvalid 的错误。
func CompileRelabel(steps []monitorcontract.RelabelStep) (*Relabeler, error) {
	if len(steps) == 0 {
		return nil, nil
	}
	if len(steps) > monitorcontract.MaxRelabelSteps {
		return nil, invalidf("at most %d relabel steps", monitorcontract.MaxRelabelSteps)
	}
	out := &Relabeler{steps: make([]relabelStep, 0, len(steps))}
	for i, s := range steps {
		c, err := compileRelabelStep(s)
		if err != nil {
			return nil, fmt.Errorf("relabel step %d: %w", i, err)
		}
		out.steps = append(out.steps, c)
	}
	return out, nil
}

func compileRelabelStep(s monitorcontract.RelabelStep) (relabelStep, error) {
	c := relabelStep{action: s.Action, separator: s.Separator, target: s.TargetLabel, replacement: s.Replacement}
	if len(s.Regex) > monitorcontract.MaxRelabelRegexLen {
		return c, invalidf("regex longer than %d bytes", monitorcontract.MaxRelabelRegexLen)
	}
	if len(s.Replacement) > monitorcontract.MaxRelabelTextLen || len(s.Separator) > monitorcontract.MaxRelabelTextLen ||
		!utf8.ValidString(s.Replacement) || !utf8.ValidString(s.Separator) {
		return c, invalidf("replacement / separator too long or not UTF-8")
	}
	pattern := s.Regex
	switch s.Action {
	case monitorcontract.RelabelReplace:
		if !monitorcontract.ValidLabelName(s.TargetLabel) || strings.HasPrefix(s.TargetLabel, "__") {
			return c, invalidf("target_label %q", s.TargetLabel)
		}
		if len(s.SourceLabels) > monitorcontract.MaxRelabelSourceLabels {
			return c, invalidf("at most %d source_labels", monitorcontract.MaxRelabelSourceLabels)
		}
		for _, name := range s.SourceLabels {
			if !monitorcontract.ValidLabelName(name) {
				return c, invalidf("source label %q", name)
			}
		}
		c.sources = slices.Clone(s.SourceLabels)
		if pattern == "" {
			pattern = defaultRelabelRegex
		}
		if s.Separator == "" {
			c.separator = defaultRelabelSeparator
		}
		if s.Replacement == "" {
			c.replacement = defaultRelabelReplacement
		}
	case monitorcontract.RelabelLabelMap, monitorcontract.RelabelLabelDrop, monitorcontract.RelabelLabelKeep:
		if pattern == "" {
			return c, invalidf("%s requires regex", s.Action)
		}
		if len(s.SourceLabels) != 0 || s.TargetLabel != "" || s.Separator != "" {
			return c, invalidf("%s takes only regex", s.Action)
		}
		if s.Action == monitorcontract.RelabelLabelMap {
			if s.Replacement == "" {
				c.replacement = defaultRelabelReplacement
			}
		} else if s.Replacement != "" {
			return c, invalidf("%s takes only regex", s.Action)
		}
	default:
		return c, invalidf("action %q", s.Action)
	}
	// 先单独编译用户正则：不配对的写法包装后可能恰好合法，会破坏锚定（同 monitorcontract 的匹配器）。
	if _, err := regexp.Compile(pattern); err != nil {
		return c, invalidf("regex: %v", err)
	}
	re, err := regexp.Compile("^(?s:" + pattern + ")$")
	if err != nil {
		return c, invalidf("regex: %v", err)
	}
	c.re = re
	return c, nil
}

// Apply 就地改写标签集，按步骤顺序执行；结果与 map 的遍历顺序无关（同一输入恒得同一输出，指纹才稳定）。
// 改写不会产生不合法或以 "__" 开头的标签名（这样的结果被丢弃），写入的值截断到 1024 字节。
func (r *Relabeler) Apply(labels map[string]string) {
	if r == nil {
		return
	}
	for i := range r.steps {
		s := &r.steps[i]
		switch s.action {
		case monitorcontract.RelabelReplace:
			s.replace(labels)
		case monitorcontract.RelabelLabelMap:
			s.labelMap(labels)
		case monitorcontract.RelabelLabelDrop:
			for name := range labels {
				if s.re.MatchString(name) {
					delete(labels, name)
				}
			}
		case monitorcontract.RelabelLabelKeep:
			for name := range labels {
				if !s.re.MatchString(name) {
					delete(labels, name)
				}
			}
		}
	}
}

func (s *relabelStep) replace(labels map[string]string) {
	var src string
	switch len(s.sources) {
	case 0:
	case 1:
		src = labels[s.sources[0]]
	default:
		var b strings.Builder
		for i, name := range s.sources {
			if i > 0 {
				b.WriteString(s.separator)
			}
			b.WriteString(labels[name])
		}
		src = b.String()
	}
	m := s.re.FindStringSubmatchIndex(src)
	if m == nil {
		return
	}
	value := clipValue(string(s.re.ExpandString(nil, s.replacement, src, m)))
	if value == "" {
		delete(labels, s.target)
		return
	}
	labels[s.target] = value
}

func (s *relabelStep) labelMap(labels map[string]string) {
	// 按名称排序后处理：两个标签映射到同一个新名称时，取值由排序靠后者决定，而不是 map 的遍历顺序。
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		m := s.re.FindStringSubmatchIndex(name)
		if m == nil {
			continue
		}
		target := string(s.re.ExpandString(nil, s.replacement, name, m))
		if !monitorcontract.ValidLabelName(target) || strings.HasPrefix(target, "__") {
			continue
		}
		if _, exists := labels[target]; !exists && len(labels) >= maxRelabelLabels {
			continue
		}
		labels[target] = labels[name]
	}
}

// clipValue 去掉 NUL（jsonb 不接受）、修正非法 UTF-8，并截断到 maxRelabelValueBytes（不切断字符）。
func clipValue(v string) string {
	if strings.IndexByte(v, 0) >= 0 {
		v = strings.ReplaceAll(v, "\x00", "")
	}
	v = strings.ToValidUTF8(v, "?")
	if len(v) <= maxRelabelValueBytes {
		return v
	}
	cut := maxRelabelValueBytes
	for cut > 0 && !utf8.RuneStart(v[cut]) {
		cut--
	}
	return v[:cut]
}
