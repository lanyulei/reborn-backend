package biz

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metricsql"
)

// withBomb 第 n 层把上一层复制两份的 WITH 模板：展开后有 2^n 个叶子。n = 20 时表达式 328 字节，
// 未加保护的解析要 1.3GB 内存。
func withBomb(n int) string {
	var b strings.Builder
	b.WriteString("WITH (a0 = x")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, ", a%d = a%d + a%d", i, i-1, i-1)
	}
	fmt.Fprintf(&b, ") a%d", n)
	return b.String()
}

// nestCall 把 fn 嵌套 n 层：ru / ttf 是解析期按内置模板展开的函数，参数各被复制两份。
func nestCall(fn string, n int) string {
	s := "x"
	for range n {
		if fn == "ttf" {
			s = fn + "(" + s + ")"
		} else {
			s = fn + "(x, " + s + ")"
		}
	}
	return s
}

// rejectFast 断言 checkExpr 立即拒绝：这些表达式一旦进了解析器就是指数级的时间与内存。
func rejectFast(t *testing.T, expr string, want error) {
	t.Helper()
	start := time.Now()
	err := checkExpr(expr)
	if took := time.Since(start); took > 50*time.Millisecond {
		t.Fatalf("checkExpr(%d bytes) took %v, want an immediate rejection", len(expr), took)
	}
	if !errors.Is(err, want) || errorMessage(err) != errorMessage(want) {
		t.Fatalf("checkExpr(%.60q) = %v, want %v", expr, err, want)
	}
}

func TestCheckExpr_RejectsWithTemplates(t *testing.T) {
	t.Parallel()
	// 由小到大：保护一旦失效，第一条就失败，不会真去解析后面的炸弹。
	for _, expr := range []string{
		"WITH (a = up) a",
		"with(a = up) a",
		"WiTh\t(a = up) a",
		"with # 注释\n (a = up) a",
		"sum(WITH (a = up) a) > 0",
		"up + with (f(q) = q * 2) f(up)",
		"x1with (a = up) a",
		withBomb(14),
		withBomb(20), // 评审报告里的 328 字节表达式
		withBomb(40),
		withBomb(400), // 接近长度上限
		"WITH (f(q) = q + q) " + nestCall("f", 60),
	} {
		rejectFast(t, expr, errExprWith)
	}
	if n := len(withBomb(20)); n != 328 {
		t.Fatalf("len(withBomb(20)) = %d, want the 328-byte expression from the review", n)
	}
}

func TestCheckExpr_BoundsBuiltinTemplates(t *testing.T) {
	t.Parallel()
	for _, expr := range []string{
		nestCall("ru", 5),
		nestCall("ru", 18), // 127 字节，未加保护时 1.3GB
		nestCall("ttf", 5),
		nestCall("ttf", 40),
		`r\x75(x, r\x75(x, r\x75(x, r\x75(x, r\x75(x, y)))))`, // 转义写法同样是 ru
		"ru(a, b) + ru(a, b) + ttf(a) + ttf(b) + ru (c, d)",
	} {
		rejectFast(t, expr, errExprTemplates)
	}
}

func TestCheckExpr_HardLimits(t *testing.T) {
	t.Parallel()
	deep := func(n int) string { return strings.Repeat("(", n) + "up" + strings.Repeat(")", n) }
	rejectFast(t, deep(maxExprDepth+1), errExprTooDeep)
	rejectFast(t, "sum("+strings.Repeat("abs(", maxExprDepth)+"up"+strings.Repeat(")", maxExprDepth)+")", errExprTooDeep)
	rejectFast(t, "up > "+strings.Repeat("1", maxExprBytes), errExprTooLong)
	rejectFast(t, `up{job="api} > 0`, errExprString)
	rejectFast(t, `up + \x4`, errExprEscape)
	if err := checkExpr(deep(maxExprDepth)); err != nil {
		t.Errorf("checkExpr(depth %d) = %v, want nil", maxExprDepth, err)
	}
	if err := checkExpr("up > " + strings.Repeat("1", 300)); err != nil {
		t.Errorf("checkExpr(long number) = %v, want nil", err)
	}
}

// TestCheckExpr_KeepsMetricsQL 保护只拒绝解析期的模板展开：MetricsQL 的函数、修饰符与写法照常可用，
// 名字里带 with 的指标、字符串与注释里的 with ( 也不受影响。
func TestCheckExpr_KeepsMetricsQL(t *testing.T) {
	t.Parallel()
	for _, expr := range []string{
		`with{job="api"} > 1`,
		"with > 1",
		"with_total + requests_with_errors > 1",
		`up{msg=~"failed with (.*)"} == 0`,
		"up # with (\n == 0",
		"sum without (instance) (up) > 0",
		"sum(up) without(instance) > 0",
		`ru(node_memory_MemAvailable_bytes, node_memory_MemTotal_bytes) > 90`,
		`ttf(node_filesystem_avail_bytes{mountpoint="/"}) < 3600`,
		"ru(a, b) > 90 or ru(c, d) > 90 or ttf(e) < 60 or ttf(f) < 60",
		"range_median(up[5m]) > 0",
		`alias(up, "x") > 0`,
		"median_over_time(up[5m]) > 0",
		"timestamp_with_name(up) > 0", // 名字里带 with 的函数（228 个 MetricsQL 函数里没有以 with 结尾的）
		"rollup_rate(http_requests_total[5m]) > 100",
		`quantiles_over_time("phi", 0.5, 0.9, latency[5m]) > 1`,
		"topk_max(3, rate(http_requests_total[5m]))",
		`label_set(up, "team", "ops") == 0`,
		"rate(http_requests_total[5m]) keep_metric_names > 10",
		"(up default 0) == 0",
		"up[5m] offset 1h",
		"rate(http_requests_total) > 1",
		`{__name__=~"node_.+", job="node"} > 1Ki`,
		`\x75p == 0`,
		`'quoted \' with (' == "x"`,
	} {
		if err := checkExpr(expr); err != nil {
			t.Errorf("checkExpr(%q) = %v, want nil", expr, err)
		}
	}
}

// FuzzCheckExpr 保护与解析器的词法必须一致：凡是通过保护的表达式，解析结果的规模都与输入成线性
// 关系（ru / ttf 至多放大 2^maxTemplateCalls 倍）。种子语料随普通测试一起跑。
func FuzzCheckExpr(f *testing.F) {
	for _, seed := range []string{
		"up == 0", withBomb(3), nestCall("ru", 4), nestCall("ttf", 4), `r\x75(x, y)`, `w\x69th (a = 1) a`,
		"with # c\n (a = up) a", `up{a="with ("} > 0`, "1with(a=1) a", "5mwith(a=1) a", "x + 0x1with(a=1) a",
		"a\\'with(b = 1) b", "`with (` + with(a = 1) a", "sum(up) without(instance)", "ru(ru(x, ttf(y)), ttf(ttf(z)))",
		"# with (\nwith (a = up) a", `"\" with (a = 1) a`, "with\\ (a = 1) a",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		if len(expr) > 512 || guardExpr(expr) != nil {
			return
		}
		e, err := metricsql.Parse(expr)
		if err != nil {
			return
		}
		if out := len(e.AppendString(nil)); out > 256*len(expr)+4096 {
			t.Fatalf("expression of %d bytes expanded to %d bytes: %q", len(expr), out, expr)
		}
	})
}

func BenchmarkCheckExpr(b *testing.B) {
	const expr = `sum by (instance) (rate(node_cpu_seconds_total{mode!="idle"}[5m])) / count by (instance) (node_cpu_seconds_total{mode="idle"}) > 0.9`
	b.ReportAllocs()
	for b.Loop() {
		if err := checkExpr(expr); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGuardExpr_Typical 保护本身的开销（每次保存、每次校验都要付）。
func BenchmarkGuardExpr_Typical(b *testing.B) {
	const expr = `sum by (instance) (rate(node_cpu_seconds_total{mode!="idle"}[5m])) / count by (instance) (node_cpu_seconds_total{mode="idle"}) > 0.9`
	b.ReportAllocs()
	for b.Loop() {
		if err := guardExpr(expr); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCheckExpr_WithBomb 拒绝一条接近长度上限的 WITH 炸弹的开销。
func BenchmarkCheckExpr_WithBomb(b *testing.B) {
	expr := withBomb(400)
	b.ReportAllocs()
	for b.Loop() {
		if err := checkExpr(expr); err == nil {
			b.Fatal("bomb was accepted")
		}
	}
}
