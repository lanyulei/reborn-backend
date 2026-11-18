package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// plain 平台自己写的字符串（ssh 选项、提权方式、已按契约校验的主机地址）：写入变量文件时不加标记。
// 其余字符串一律来自任务（参数取值、CMDB 名称、用户名、口令），写入时标为 !unsafe。
type plain string

// unsafeTag Ansible 的「不可信」标记：带它的字符串不会被当作 Jinja2 模板求值。来自任务的字符串必须带上，
// 否则参数取值或 CMDB 名称里的 {{ lookup('pipe', …) }} 会在节点上执行——只有「执行」权限、改不了
// Playbook 的用户也能借此在节点上运行任意命令。
const unsafeTag = "!unsafe "

// jsonNumberPattern JSON 数字字面量。
var jsonNumberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// encodeVars 把变量树编码成 Ansible 能读的变量文件 / 清单：YAML 的流式（flow）写法，形同 JSON，
// 只是来自任务的字符串前面带 !unsafe 标记。Ansible 读 `-e @文件` 与 YAML 清单时，先按 JSON 解析
// （因标记而失败），再按 YAML 解析——文件名沿用 .json 不影响。
//
// 不用 JSON 的 {"__ansible_unsafe": "…"} 写法：它依赖 Ansible 的 JSON 解码钩子，在不支持的版本上
// 会悄悄变成一个字典而不是报错；!unsafe 标记不被支持时是解析错误，作业明确失败。
//
// 支持的值：nil、bool、string、plain、json.Number、各种整数、float64、[]any、map[string]any；
// 其它类型返回错误。映射的键按字典序输出（结果确定，便于比对）。
func encodeVars(v any) ([]byte, error) {
	var b bytes.Buffer
	if err := writeValue(&b, v); err != nil {
		return nil, err
	}
	b.WriteByte('\n')
	return b.Bytes(), nil
}

func writeValue(b *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case plain:
		writeString(b, string(x))
	case string:
		b.WriteString(unsafeTag)
		writeString(b, x)
	case json.Number:
		return writeNumber(b, x.String())
	case float64:
		return writeNumber(b, strconv.FormatFloat(x, 'f', -1, 64))
	case int:
		b.WriteString(strconv.Itoa(x))
	case int32:
		b.WriteString(strconv.FormatInt(int64(x), 10))
	case int64:
		b.WriteString(strconv.FormatInt(x, 10))
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteString(", ")
			}
			if err := writeValue(b, e); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteString(", ")
			}
			writeString(b, k)
			b.WriteString(": ")
			if err := writeValue(b, x[k]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	default:
		return fmt.Errorf("unsupported variable type %T", v)
	}
	return nil
}

// writeNumber 写数字。YAML 1.1（PyYAML）只把带小数点的写法认作浮点数：`1e+21` 会被读成字符串，
// 所以带指数的一律展开成普通十进制。
func writeNumber(b *bytes.Buffer, lit string) error {
	if !jsonNumberPattern.MatchString(lit) {
		return fmt.Errorf("invalid number literal %q", lit)
	}
	if strings.ContainsAny(lit, "eE") {
		f, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return fmt.Errorf("invalid number literal %q", lit)
		}
		lit = strconv.FormatFloat(f, 'f', -1, 64)
	}
	b.WriteString(lit)
	return nil
}

// writeString 写双引号字符串。JSON 的字符串转义是 YAML 双引号写法的子集，只差几个 JSON 允许原样
// 出现、而 YAML 解析器拒绝或另作解释的字符：DEL 与 C1 控制符（其中 U+0085 在 YAML 里是换行）、
// U+FFFE / U+FFFF——把它们也写成 \uXXXX。非法 UTF-8 由 json.Marshal 换成 U+FFFD。
func writeString(b *bytes.Buffer, s string) {
	enc, _ := json.Marshal(s) // 字符串编码不会失败
	for _, r := range string(enc) {
		if (r >= 0x7f && r <= 0x9f) || r == 0xfffe || r == 0xffff {
			fmt.Fprintf(b, `\u%04x`, r)
			continue
		}
		b.WriteRune(r)
	}
}
