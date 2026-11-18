package monitorpipeline

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"text/template"
	"time"
	"unicode"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 模板限额（monitor 技术设计 §4.3）。源文本上限、前置声明与全部沙箱规则在 kit/monitorcontract
// （ValidateTemplate）；这里是通过校验的模板的执行。
const (
	// RenderTimeout 单次渲染的时间上限。
	RenderTimeout = 100 * time.Millisecond
	// MaxRenderBytes 单次渲染的输出上限。
	MaxRenderBytes = 4 << 10
	// printfArgCost printf 的一个非字符串实参（数值、布尔等）在输出预算里的估算开销。
	printfArgCost = 32
)

// 渲染中止的原因（Template.Render 返回的错误可用 errors.Is 判断）。
var (
	ErrRenderTimeout  = errors.New("monitorpipeline: template render timed out")
	ErrRenderTooLarge = errors.New("monitorpipeline: template output too large")
)

// templateFuncs monitorcontract.TemplateFuncs 列出的自定义函数的实现；其余可用函数是
// text/template 的内建，哪些内建可用由 ValidateTemplate 把关。创建后只读。
var templateFuncs = template.FuncMap{
	"printf":             safePrintf,
	"humanize":           func(v any) (string, error) { return withFloat(v, humanize) },
	"humanizePercentage": func(v any) (string, error) { return withFloat(v, humanizePercentage) },
	"humanizeDuration":   func(v any) (string, error) { return withFloat(v, humanizeDuration) },
	"toUpper":            strings.ToUpper,
	"toLower":            strings.ToLower,
	"title":              title,
}

// safePrintf 即 fmt.Sprintf，但格式串须通过 monitorcontract.CheckPrintfFormat（校验时已查过字面量，
// 这里是纵深防御），且输出不得超过单次渲染的输出上限：格式化之前先按实参估算（字符串与标签 map 按
// 实际长度，其余实参按 printfArgCost），上游返回的超长标签值不会先被成倍放大再丢弃；格式化之后再按
// 实际长度核对一次。
func safePrintf(format string, args ...any) (string, error) {
	if err := monitorcontract.CheckPrintfFormat(format); err != nil {
		return "", err
	}
	total := len(format)
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			total += len(v)
		case map[string]string:
			for name, value := range v {
				total += len(name) + len(value) + 2
			}
		default:
			total += printfArgCost
		}
		if total > MaxRenderBytes {
			return "", ErrRenderTooLarge
		}
	}
	out := fmt.Sprintf(format, args...)
	if len(out) > MaxRenderBytes {
		return "", ErrRenderTooLarge
	}
	return out, nil
}

// templateData 模板的执行上下文（字段名是 monitorcontract.TemplatePrefix 的约定）。
type templateData struct {
	Labels map[string]string
	Value  float64
}

// Template 一段通过校验、已解析的模板（附加信息、恢复取值 PromQL、条件丢弃表达式共用）。
// 创建后只读，可并发渲染。
type Template struct {
	t *template.Template
}

// CompileTemplate 校验（monitorcontract.ValidateTemplate）并解析一段模板源文本。校验保证执行成本有界
// （与源文本长度 × 标签数成正比）：未通过校验的模板一律不执行。失败返回包装了
// monitorcontract.ErrInvalid 的错误。
func CompileTemplate(src string) (*Template, error) {
	if err := monitorcontract.ValidateTemplate(src); err != nil {
		return nil, err
	}
	// missingkey=zero：不存在的标签渲染为空串（与 Prometheus 一致），而不是 "<no value>"。
	t, err := template.New("t").Funcs(templateFuncs).Option("missingkey=zero").Parse(monitorcontract.TemplatePrefix + src)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", monitorcontract.ErrInvalid, err)
	}
	return &Template{t: t}, nil
}

// limitedWriter 超过输出上限或截止时刻即报错中止执行（模板执行没有 ctx 入口，只能经 Writer 中止）。
type limitedWriter struct {
	buf      bytes.Buffer
	deadline time.Time
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.buf.Len()+len(p) > MaxRenderBytes {
		return 0, ErrRenderTooLarge
	}
	if time.Now().After(w.deadline) {
		return 0, ErrRenderTimeout
	}
	return w.buf.Write(p)
}

// Render 以事件标签与取值渲染模板：至多 RenderTimeout、输出至多 MaxRenderBytes，超出返回
// ErrRenderTimeout / ErrRenderTooLarge。labels 只读。
func (t *Template) Render(labels map[string]string, value float64) (string, error) {
	w := &limitedWriter{deadline: time.Now().Add(RenderTimeout)}
	if err := t.t.Execute(w, templateData{Labels: labels, Value: value}); err != nil {
		return "", err
	}
	return w.buf.String(), nil
}

func withFloat(v any, f func(float64) string) (string, error) {
	x, err := toFloat(v)
	if err != nil {
		return "", err
	}
	return f(x), nil
}

func toFloat(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case string:
		return strconv.ParseFloat(x, 64)
	}
	return 0, fmt.Errorf("cannot convert %T to a number", v)
}

// siUp / siDown SI 词头：humanize 与 humanizeDuration 共用。
var (
	siUp   = [...]string{"k", "M", "G", "T", "P", "E", "Z", "Y"}
	siDown = [...]string{"m", "u", "n", "p", "f", "a", "z", "y"}
)

// scaleDown 把绝对值小于 1 的数逐级放大 1000 倍，返回放大后的数与对应的词头。
func scaleDown(v float64) (float64, string) {
	prefix := ""
	for _, p := range siDown {
		if math.Abs(v) >= 1 {
			break
		}
		prefix = p
		v *= 1000
	}
	return v, prefix
}

// humanize 按 SI 词头缩写数值（与 Prometheus 模板同名函数一致）：1234567 → 1.235M。
func humanize(v float64) string {
	if v == 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Sprintf("%.4g", v)
	}
	prefix := ""
	if math.Abs(v) >= 1 {
		for _, p := range siUp {
			if math.Abs(v) < 1000 {
				break
			}
			prefix = p
			v /= 1000
		}
	} else {
		v, prefix = scaleDown(v)
	}
	return fmt.Sprintf("%.4g%s", v, prefix)
}

// humanizePercentage 把比率格式化为百分数：0.1234 → 12.34%。
func humanizePercentage(v float64) string { return fmt.Sprintf("%.4g%%", v*100) }

// humanizeDuration 把秒数格式化为时长（与 Prometheus 模板同名函数一致）：3725 → 1h 2m 5s。
func humanizeDuration(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Sprintf("%.4g", v)
	}
	if v == 0 {
		return "0s"
	}
	if math.Abs(v) >= 1 {
		sign := ""
		if v < 0 {
			sign = "-"
			v = -v
		}
		whole := math.Floor(v)
		seconds := int64(math.Mod(whole, 60))
		minutes := int64(math.Mod(whole/60, 60))
		hours := int64(math.Mod(whole/3600, 24))
		days := int64(math.Min(whole/86400, math.MaxInt32))
		switch {
		case days != 0:
			return fmt.Sprintf("%s%dd %dh %dm %ds", sign, days, hours, minutes, seconds)
		case hours != 0:
			return fmt.Sprintf("%s%dh %dm %ds", sign, hours, minutes, seconds)
		case minutes != 0:
			return fmt.Sprintf("%s%dm %ds", sign, minutes, seconds)
		}
		return fmt.Sprintf("%s%.4gs", sign, v)
	}
	v, prefix := scaleDown(v)
	return fmt.Sprintf("%.4g%ss", v, prefix)
}

// title 把每个词的首字母大写（strings.Title 已废弃；这里只按空白分词，足够附加信息使用）。
func title(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	start := true
	for _, r := range s {
		if start {
			r = unicode.ToTitle(r)
		}
		start = unicode.IsSpace(r)
		b.WriteRune(r)
	}
	return b.String()
}

// DropExpr 规则的条件丢弃表达式：一段与附加信息同一沙箱的模板，渲染结果去掉首尾空白后恰为 "true"
// 即丢弃该事件。例：{{ if eq $labels.env "dev" }}true{{ end }}。创建后只读，可并发使用；nil 不丢弃任何事件。
type DropExpr struct {
	t *Template
}

// CompileDropExpr 校验并解析条件丢弃表达式；空串（或全是空白）返回 nil, nil（未配置）。
// 源文本不超过 monitorcontract.MaxDropExprBytes，其余规则同 CompileTemplate。
func CompileDropExpr(src string) (*DropExpr, error) {
	if strings.TrimSpace(src) == "" {
		return nil, nil
	}
	if len(src) > monitorcontract.MaxDropExprBytes {
		return nil, fmt.Errorf("%w: drop expression longer than %d bytes", monitorcontract.ErrInvalid, monitorcontract.MaxDropExprBytes)
	}
	t, err := CompileTemplate(src)
	if err != nil {
		return nil, err
	}
	return &DropExpr{t: t}, nil
}

// Drop 报告事件是否应被丢弃。渲染失败（超时、输出过大、执行出错）时不丢弃并返回错误：
// 表达式出问题不得让告警无声消失。
func (d *DropExpr) Drop(labels map[string]string, value float64) (bool, error) {
	if d == nil {
		return false, nil
	}
	out, err := d.t.Render(labels, value)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) == "true", nil
}
