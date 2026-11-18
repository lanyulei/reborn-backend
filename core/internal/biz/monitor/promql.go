package biz

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/VictoriaMetrics/metricsql"
)

// 表达式的硬性上限（解析前判定）。
const (
	// maxExprBytes 表达式长度上限（与接口校验同一口径）。
	maxExprBytes = 8192
	// maxExprDepth 括号（圆 / 方 / 花）嵌套深度上限：解析器递归下降，深度即栈深。
	maxExprDepth = 64
	// maxTemplateCalls 一条表达式里 ru() / ttf() 的调用次数上限。这两个 MetricsQL 函数在解析期按内置
	// 模板展开，参数各被复制两份，嵌套 n 层即 2^n 倍（18 层、127 字节的表达式就要 1.3GB）；
	// 限制在 4 次以内，展开至多 16 倍。
	maxTemplateCalls = 4
	// maxExprOps 一条表达式里运算符记号的个数上限（见 guardExpr 的计数口径）。解析成本随运算符个数
	// 线性增长，这里只是给它一个与长度上限无关的界；实际的长表达式（2-4KiB）在 200 个以内。
	maxExprOps = 512
	// maxExprPow 一条表达式里 ^ 的个数上限。^ 是唯一右结合的运算符，解析器每接上一个都要沿右侧
	// 整条链走一遍：n 个连写就是 n² 的时间，且不可取消（8KiB 的 a^a^… 约 190ms）。32 个以内
	// 这部分成本可以忽略。
	maxExprPow = 32
)

// 解析前的拒绝原因。
var (
	errEmptyExpr     = detail(ErrRuleInvalid, "expression must not be empty")
	errExprTooLong   = detail(ErrRuleInvalid, "expression is longer than %d bytes", maxExprBytes)
	errExprTooDeep   = detail(ErrRuleInvalid, "expression is nested deeper than %d levels", maxExprDepth)
	errExprWith      = detail(ErrRuleInvalid, "WITH templates are not supported: expand the template and write the full expression")
	errExprTemplates = detail(ErrRuleInvalid, "ru() / ttf() may be called at most %d times in one expression", maxTemplateCalls)
	errExprOps       = detail(ErrRuleInvalid, "expression has more than %d operators", maxExprOps)
	errExprPow       = detail(ErrRuleInvalid, "the ^ operator may be used at most %d times in one expression", maxExprPow)
	errExprString    = detail(ErrRuleInvalid, "unterminated string literal")
	errExprEscape    = detail(ErrRuleInvalid, "invalid escape sequence in identifier")
)

// checkExpr 校验 PromQL 语法。解析器是本文件唯一的出口：换实现只改这一个函数。当前用 MetricsQL
// 解析器——MetricsQL 是 PromQL 的超集，与实际使用的 VictoriaMetrics 一致；标准 Prometheus 不认识的
// MetricsQL 函数在这里也会通过，执行时由数据源自身报错（判定记录里可见）。
//
// metricsql.Parse 会在解析期展开 WITH 模板，展开结果随模板层数指数增长且不可取消，所以必须先过
// guardExpr：它拒绝的表达式绝不能交给解析器。
func checkExpr(expr string) error {
	if strings.TrimSpace(expr) == "" {
		return errEmptyExpr
	}
	if err := guardExpr(expr); err != nil {
		return err
	}
	_, err := metricsql.Parse(expr)
	return err
}

// guardExpr 解析前的硬性保护，单趟线性扫描：
//   - 长度不超过 maxExprBytes，括号嵌套不超过 maxExprDepth；
//   - 拒绝 WITH 模板（`WITH (...) expr`）；
//   - ru() / ttf() 合计至多 maxTemplateCalls 次；
//   - 运算符记号至多 maxExprOps 个，其中 ^ 至多 maxExprPow 个。
//
// 运算符按记号计数，不区分一元 / 二元（从严）：+ - * / % ^、比较（== != > < >= <=）与关键字
// and / or / unless / atan2 / if / ifnot / default 各算一个；花括号里的内容是标签过滤条件
// （= != =~ !~ 与过滤条件之间的 or），不计。
//
// 扫描与 metricsql 的词法器对齐：字符串（" ' ` 三种引号、反斜杠转义）与注释（# 到行尾）里的内容不
// 参与判定；标识符可含反斜杠转义（\xNN、\uNNNN、\<可打印字符>）。拿不准时一律从严：
//   - WITH 按「标识符以 with 结尾（不区分大小写）、其后（隔着空白 / 注释）紧跟左括号」判定，不看
//     with 前面还有什么——标识符后面跟左括号只能是函数调用，而没有任何函数名以 with 结尾
//     （without 不受影响）；
//   - 内置模板按转义还原后的名字匹配（r\x75(…) 就是 ru(…)），与解析器一致。
func guardExpr(expr string) error {
	if len(expr) > maxExprBytes {
		return errExprTooLong
	}
	depth, braces, calls, ops, pow := 0, 0, 0, 0, 0
	// countOp 记一个运算符记号（花括号内不计）；超限返回对应的错误。
	countOp := func(isPow bool) error {
		if braces > 0 {
			return nil
		}
		if ops++; ops > maxExprOps {
			return errExprOps
		}
		if isPow {
			if pow++; pow > maxExprPow {
				return errExprPow
			}
		}
		return nil
	}
	for i := 0; i < len(expr); {
		switch c := expr[i]; c {
		case '#':
			n := strings.IndexByte(expr[i:], '\n')
			if n < 0 {
				return nil
			}
			i += n + 1
			continue
		case '"', '\'', '`':
			n := stringEnd(expr[i:])
			if n < 0 {
				return errExprString
			}
			i += n
			continue
		case '(', '[', '{':
			if depth++; depth > maxExprDepth {
				return errExprTooDeep
			}
			if c == '{' {
				braces++
			}
			i++
			continue
		case ')', ']', '}':
			depth--
			if c == '}' {
				braces--
			}
			i++
			continue
		case '^', '+', '-', '*', '/', '%', '=', '!', '<', '>':
			if err := countOp(c == '^'); err != nil {
				return err
			}
			// == != >= <= =~ !~ 是一个记号。
			if i++; c != '^' && i < len(expr) && (expr[i] == '=' || expr[i] == '~') && strings.IndexByte("=!<>", c) >= 0 {
				i++
			}
			continue
		}
		n, name, ok := scanExprIdent(expr[i:])
		if !ok {
			return errExprEscape
		}
		if n == 0 {
			i++
			continue
		}
		if isWordOp(name) {
			if err := countOp(false); err != nil {
				return err
			}
		}
		if nextIsParen(expr[i+n:]) {
			if n >= 4 && strings.EqualFold(expr[i+n-4:i+n], "with") {
				return errExprWith
			}
			if name == "ru" || name == "ttf" {
				if calls++; calls > maxTemplateCalls {
					return errExprTemplates
				}
			}
		}
		i += n
	}
	return nil
}

// isWordOp 报告标识符是否关键字形式的二元运算符（不区分大小写）。
func isWordOp(name string) bool {
	if len(name) < 2 || len(name) > 7 {
		return false
	}
	for _, op := range [...]string{"and", "or", "unless", "atan2", "if", "ifnot", "default"} {
		if strings.EqualFold(name, op) {
			return true
		}
	}
	return false
}

// stringEnd 返回 s 开头的字符串字面量的长度（含两端引号），未闭合返回 -1。与 metricsql 的
// scanString 同一算法：引号前有奇数个反斜杠时该引号被转义。
func stringEnd(s string) int {
	quote := s[0]
	for i := 1; i < len(s); i++ {
		n := strings.IndexByte(s[i:], quote)
		if n < 0 {
			return -1
		}
		i += n
		bs := 0
		for bs < i && s[i-bs-1] == '\\' {
			bs++
		}
		if bs%2 == 0 {
			return i + 1
		}
	}
	return -1
}

// skipBlank 跳过空白与注释，返回剩余部分。
func skipBlank(s string) string {
	for len(s) > 0 {
		switch s[0] {
		case ' ', '\t', '\n', '\v', '\f', '\r':
			s = s[1:]
		case '#':
			n := strings.IndexByte(s, '\n')
			if n < 0 {
				return ""
			}
			s = s[n+1:]
		default:
			return s
		}
	}
	return s
}

// nextIsParen 报告跳过空白与注释后是否紧跟左括号。
func nextIsParen(s string) bool {
	s = skipBlank(s)
	return len(s) > 0 && s[0] == '('
}

// scanExprIdent 扫描 s 开头的标识符，返回其字节长度与转义还原后的名字；s 不以标识符开头返回 0。
// 与 metricsql 的 scanIdent / unescapeIdent 对齐。ok 为 false 表示开头是无法识别的反斜杠转义
// （词法器同样会报错）。
func scanExprIdent(s string) (n int, name string, ok bool) {
	var b strings.Builder
	for n < len(s) {
		r, size := utf8.DecodeRuneInString(s[n:])
		if r == '\\' {
			er, esize := decodeExprEscape(s[n+size:])
			if er == utf8.RuneError {
				break
			}
			b.WriteRune(er)
			n += size + esize
			continue
		}
		// 首字符：字母、下划线、冒号；其后还可以是数字与点。
		letter := unicode.IsLetter(r) || r == '_' || r == ':'
		digit := r >= '0' && r <= '9' || r == '.'
		if !letter && (n == 0 || !digit) {
			break
		}
		b.WriteRune(r)
		n += size
	}
	if n == 0 && len(s) > 0 && s[0] == '\\' {
		return 0, "", false
	}
	return n, b.String(), true
}

// decodeExprEscape 解码反斜杠之后的转义序列（与 metricsql 的 decodeEscapeSequence 相同）：
// xNN、uNNNN 或一个可打印字符；不合法返回 utf8.RuneError。
func decodeExprEscape(s string) (rune, int) {
	if len(s) == 0 {
		return utf8.RuneError, 0
	}
	digits := 0
	switch s[0] {
	case 'x', 'X':
		digits = 2
	case 'u', 'U':
		digits = 4
	}
	if digits > 0 {
		if len(s) <= digits {
			return utf8.RuneError, 0
		}
		v, err := strconv.ParseUint(s[1:1+digits], 16, 32)
		if err != nil {
			return utf8.RuneError, 0
		}
		return rune(v), 1 + digits // #nosec G115 -- 至多 4 位十六进制
	}
	r, size := utf8.DecodeRuneInString(s)
	if !unicode.IsPrint(r) {
		return utf8.RuneError, 0
	}
	return r, size
}

// checkTemplate 校验附加信息 / 恢复取值模板：解析与全部沙箱规则都在 monitorcontract.ValidateTemplate
// 里——评估引擎渲染前调用的是同一个函数，这里通过的模板引擎一定会执行，这里拒绝的引擎一定不执行。
func checkTemplate(text string) error {
	if err := mc.ValidateTemplate(text); err != nil {
		// ErrInvalid 的前缀对用户没有信息量，只留具体原因。
		return errors.New(strings.TrimPrefix(err.Error(), mc.ErrInvalid.Error()+": "))
	}
	return nil
}

// checkRecoveryExpr 校验恢复取值表达式：须是合法模板；不含模板动作时再按 PromQL 校验
// （带 {{ }} 的表达式渲染后才是完整的 PromQL，这里无法预判）。
func checkRecoveryExpr(expr string) error {
	if err := checkTemplate(expr); err != nil {
		return err
	}
	if strings.Contains(expr, "{{") {
		return nil
	}
	return checkExpr(expr)
}

// promDurationRE Prometheus 时长写法：各单位按 y w d h m s ms 的顺序至多出现一次，如 5m、1h30m、1d。
var promDurationRE = regexp.MustCompile(`^(?:(\d{1,9})y)?(?:(\d{1,9})w)?(?:(\d{1,9})d)?(?:(\d{1,9})h)?(?:(\d{1,9})m)?(?:(\d{1,9})s)?(?:(\d{1,9})ms)?$`)

// promDurationUnits 与 promDurationRE 的捕获组一一对应（年按 365 天，同 Prometheus）。
var promDurationUnits = [...]time.Duration{
	365 * 24 * time.Hour, 7 * 24 * time.Hour, 24 * time.Hour, time.Hour, time.Minute, time.Second, time.Millisecond,
}

// maxPromDuration 可解析的时长上限（远大于任何合法的规则参数，只为防溢出）。
const maxPromDuration = 100 * 365 * 24 * time.Hour

// parsePromDuration 解析 Prometheus 时长；"0" 合法，空串与未知写法返回 false。
func parsePromDuration(s string) (time.Duration, bool) {
	if s == "0" {
		return 0, true
	}
	m := promDurationRE.FindStringSubmatch(s)
	if s == "" || m == nil {
		return 0, false
	}
	var total time.Duration
	for i, unit := range promDurationUnits {
		if m[i+1] == "" {
			continue
		}
		n, err := strconv.ParseInt(m[i+1], 10, 64)
		if err != nil || n > int64(maxPromDuration/unit) {
			return 0, false
		}
		if total += time.Duration(n) * unit; total > maxPromDuration {
			return 0, false
		}
	}
	return total, true
}

// formatPromDuration 把秒数写成 Prometheus 时长（导出用）：能整除的用最大单位，否则用秒。
func formatPromDuration(seconds int32) string {
	switch {
	case seconds == 0:
		return "0s"
	case seconds%86400 == 0:
		return strconv.Itoa(int(seconds/86400)) + "d"
	case seconds%3600 == 0:
		return strconv.Itoa(int(seconds/3600)) + "h"
	case seconds%60 == 0:
		return strconv.Itoa(int(seconds/60)) + "m"
	}
	return strconv.Itoa(int(seconds)) + "s"
}
