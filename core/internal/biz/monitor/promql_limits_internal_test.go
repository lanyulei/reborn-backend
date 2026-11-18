package biz

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

// 评审第二轮 K3：^ 是右结合的，解析器每接上一个都要沿右侧整条链走一遍——n 个连写是 n² 的时间，
// 且不可取消（8KiB 的 a^a^… 约 190ms，1MiB 的导入约 25 秒）。

// opChain 以 first 开头、不断接上 unit，凑到不超过 size 字节。
func opChain(first, unit string, size int) string {
	return first + strings.Repeat(unit, (size-len(first))/len(unit))
}

func TestCheckExpr_BoundsOperators(t *testing.T) {
	t.Parallel()
	// 右结合链：操作数是什么都一样（指标、数字、字符串、时长、带一元负号、带修饰符）。
	for _, expr := range []string{
		opChain("a", "^a", maxExprBytes),
		opChain("1", "^1", maxExprBytes),
		opChain(`"a"`, `^"a"`, maxExprBytes),
		opChain("1m", "^1m", maxExprBytes),
		opChain("a", "^(a)", maxExprBytes),
		opChain("a", " ^ on(x) a", maxExprBytes),
		opChain("a", "^a", 2*(maxExprPow+1)+1), // 刚好多一个
	} {
		rejectFast(t, expr, errExprPow)
	}
	// 运算符总数：符号与关键字、一元与二元都计。
	for _, expr := range []string{
		opChain("a", "+a", maxExprBytes),
		opChain("a", " or a", maxExprBytes),
		opChain("a", " UNLESS a", maxExprBytes),
		opChain("a", ">=bool a", maxExprBytes),
		strings.Repeat("-", maxExprBytes-1) + "a",
		opChain("a", "+a", 2*(maxExprOps+1)+1),
	} {
		rejectFast(t, expr, errExprOps)
	}
	// 上限之内照常通过。
	for name, expr := range map[string]string{
		"pow at the limit": opChain("a", "^a", 2*maxExprPow+1),
		"ops at the limit": opChain("a", "+a", 2*maxExprOps+1),
		// 花括号里是标签过滤条件，不是运算：600 个条件、600 个 or 都不计。
		"many matchers": "up{" + opChain(`a!="b"`, `,a=~"b"`, 6000) + "} == 0",
		"filter or":     "up{" + opChain(`a="b"`, ` or a="b"`, 6000) + "} == 0",
		// 字符串与注释里的运算符不计。
		"operators in a string": `up{msg="` + strings.Repeat("^+-", 2000) + `"} == 0`,
		"operators in comments": "up # " + strings.Repeat("^", 2000) + "\n == 0",
	} {
		if err := checkExpr(expr); err != nil {
			t.Errorf("%s: checkExpr(%d bytes) = %v, want nil", name, len(expr), err)
		}
	}
	if n := strings.Count(opChain("a", "^a", 2*maxExprPow+1), "^"); n != maxExprPow {
		t.Fatalf("boundary expression has %d ^ operators, want %d", n, maxExprPow)
	}
}

// longExprs 贴近实际的长表达式（2-4KiB）：上限不能误伤它们。
func longExprs() map[string]string {
	// 多窗口多燃烧率的 SLO 告警（Google SRE Workbook 的写法），六个窗口。
	burn := func(window string, factor float64) string {
		sel := `{job="api-gateway",namespace="production",cluster=~"prod-(us|eu|ap)-[0-9]+",handler!~"/healthz|/readyz|/metrics"`
		return fmt.Sprintf(`(sum by (cluster, namespace, service) (rate(http_server_requests_seconds_count%s,status=~"5.."}[%s]))`+
			` / sum by (cluster, namespace, service) (rate(http_server_requests_seconds_count%s}[%s])) > (%g * 0.001))`, sel, window, sel, window, factor)
	}
	var slo []string
	for _, w := range []struct{ long, short string }{{"1h", "5m"}, {"6h", "30m"}, {"3d", "6h"}} {
		slo = append(slo, "("+burn(w.long, 14.4)+"\n  and\n"+burn(w.short, 14.4)+")")
	}
	// 逐个服务各自阈值的并集：一条规则盖住一批服务。
	var union []string
	for i := range 12 {
		svc := "payment-service-" + strconv.Itoa(i)
		union = append(union, fmt.Sprintf(`(histogram_quantile(0.99, sum by (le, service, instance) (rate(http_request_duration_seconds_bucket{service=%q,env="prod",route!~"/internal/.*"}[5m])))`+
			` > %d.5 and on (service) sum by (service) (rate(http_requests_total{service=%q,env="prod"}[5m])) > 1)`, svc, i+1, svc))
	}
	// 磁盘预测：一长串排除条件，加上按实例关联的元数据。
	var mounts []string
	for i := range 40 {
		mounts = append(mounts, fmt.Sprintf("/var/lib/kubelet/pods/volume-%02d/.*", i))
	}
	disk := `(predict_linear(node_filesystem_avail_bytes{fstype=~"ext4|xfs|btrfs",mountpoint!~"` + strings.Join(mounts, "|") + `"}[6h], 4 * 3600) < 0
  and node_filesystem_avail_bytes{fstype=~"ext4|xfs|btrfs"} / node_filesystem_size_bytes{fstype=~"ext4|xfs|btrfs"} < 0.2
  and node_filesystem_readonly{fstype=~"ext4|xfs|btrfs"} == 0)
  * on (instance) group_left (nodename, cluster, datacenter, rack) node_uname_info{job="node-exporter"}
  unless on (instance) (node_maintenance_window_active == 1 or absent_over_time(node_boot_time_seconds[15m]))
  unless on (instance, mountpoint) (` + strings.Join(func() []string {
		var parts []string
		for i := range 12 {
			parts = append(parts, fmt.Sprintf(`disk_alert_exclusion{instance=~"db-%02d-.+",mountpoint=~"/data/archive-%02d(/.*)?"} == 1`, i, i))
		}
		return parts
	}(), "\n    or ") + ")"
	return map[string]string{
		"slo burn rate":    strings.Join(slo, "\nor\n"),
		"per-service p99":  strings.Join(union, "\nor\n"),
		"disk fill window": disk,
		// 方差 / 范数一类的写法会连用几次 ^。
		"powers": "sqrt(" + strings.Join(func() []string {
			var parts []string
			for i := range 24 {
				parts = append(parts, fmt.Sprintf(`(avg_over_time(sensor_axis_reading{axis="a%02d",site="plant-7",line=~"line-[0-9]+"}[10m]) - %d.25) ^ 2`, i, i))
			}
			return parts
		}(), "\n + ") + ") > 3.5",
	}
}

func TestCheckExpr_AcceptsLongRealisticExpressions(t *testing.T) {
	t.Parallel()
	for name, expr := range longExprs() {
		if len(expr) < 2<<10 || len(expr) > 4<<10 {
			t.Errorf("%s: %d bytes, want a 2-4KiB expression", name, len(expr))
		}
		if err := checkExpr(expr); err != nil {
			t.Errorf("%s (%d bytes): checkExpr() = %v, want nil", name, len(expr), err)
		}
		s := validSpec()
		s.Queries[0].Expr = expr
		if err := validateStatic(&s); err != nil {
			t.Errorf("%s: validateStatic() = %v, want nil", name, err)
		}
	}
}

// 1MiB 以内、每条都是 8KiB 右结合链的规则文件：修复前逐条 190ms、整批十几秒且不可取消；
// 现在每条立即判为非法。
func TestImport_PowChainsAreRejectedFast(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	b.WriteString("groups:\n- name: g\n  rules:\n")
	const rules = 60
	for i := range rules {
		fmt.Fprintf(&b, "  - alert: a%d\n    expr: %s\n", i, opChain("a", "^a", maxExprBytes))
	}
	start := time.Now()
	items := convertAll(t, b.String())
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("converting %d rules took %v", rules, took)
	}
	if len(items) != rules {
		t.Fatalf("items = %d, want %d", len(items), rules)
	}
	for _, item := range items {
		if item.Status != ImportInvalid || !strings.Contains(item.Error, errorMessage(errExprPow)) {
			t.Fatalf("item %d = %s %q, want invalid with the ^ limit", item.Index, item.Status, item.Error)
		}
	}
}

// 单次导入 / 预览的表达式总量有上限（YAML 与本系统 JSON 同一口径）。
func TestImport_TotalExpressionBytes(t *testing.T) {
	t.Parallel()
	expr := "up{pad=\"" + strings.Repeat("x", 4000) + "\"} == 0"
	yamlFile := func(n int) string {
		return "groups:\n- name: g\n  rules:\n" + strings.Repeat("  - alert: a\n    expr: '"+expr+"'\n", n)
	}
	jsonFile := func(n int) string {
		rule := `{"name":"a","queries":[{"expr":` + strconv.Quote(expr) + `,"severity":3}],"recovery_expr":` + strconv.Quote(expr) + `}`
		return `{"format":"` + RuleExportFormat + `","rules":[` + strings.TrimSuffix(strings.Repeat(rule+",", n), ",") + `]}`
	}
	fits := maxImportExprBytes / len(expr)
	if _, err := parseRuleFile(yamlFile(fits)); err != nil {
		t.Fatalf("yaml within the limit: %v", err)
	}
	if _, err := parseRuleExport(jsonFile(fits / 2)); err != nil {
		t.Fatalf("json within the limit: %v", err)
	}
	for name, err := range map[string]error{
		"yaml": func() error { _, err := parseRuleFile(yamlFile(fits + 1)); return err }(),
		"json": func() error { _, err := parseRuleExport(jsonFile(fits/2 + 1)); return err }(),
	} {
		if !errors.Is(err, ErrRuleImportInvalid) || !strings.Contains(errorMessage(err), "expressions add up to more than") {
			t.Errorf("%s over the limit: err = %v, want ErrRuleImportInvalid about the expression total", name, err)
		}
	}
	if len(yamlFile(fits+1)) > maxImportBytes {
		t.Fatalf("the rejected file must be within the %d-byte content limit to prove the expression limit is what rejects it", maxImportBytes)
	}
}

// BenchmarkCheckExpr_PowChain 拒绝一条 8KiB 右结合链的开销（修复前解析它约 190ms）。
func BenchmarkCheckExpr_PowChain(b *testing.B) {
	expr := opChain("a", "^a", maxExprBytes)
	b.ReportAllocs()
	for b.Loop() {
		if err := checkExpr(expr); err == nil {
			b.Fatal("chain was accepted")
		}
	}
}

// worstAccepted 能通过保护的、解析最慢的几种 8KiB 表达式：^ 用满上限且操作数尽量大、运算符用满
// 上限、超长的参数表与过滤条件。
func worstAccepted() map[string]string {
	pad := func(n int) string { return "a" + strings.Repeat("_", n-1) }
	operand := "sum(" + opChain("a", ",a", (maxExprBytes-64)/(maxExprPow+1)-8) + ")"
	return map[string]string{
		"pow at the limit":      opChain(operand, "^"+operand, maxExprBytes),
		"pow tail after ops":    opChain("a", "+a", 2*(maxExprOps-maxExprPow)) + strings.Repeat("^a", maxExprPow),
		"ops at the limit":      opChain(pad(15), "+"+pad(15), maxExprBytes),
		"comparisons":           opChain(pad(9), "==bool "+pad(9), maxExprBytes),
		"arguments":             "sum(" + opChain("a", ",a", maxExprBytes-8) + ")",
		"nested calls":          strings.Repeat("abs(", maxExprDepth-1) + "sum(" + opChain("a", ",a", maxExprBytes-600) + ")" + strings.Repeat(")", maxExprDepth-1),
		"label filters":         "a{" + opChain(`a="b"`, `,a="b"`, maxExprBytes-8) + "}",
		"label filters with or": "a{" + opChain(`a="b"`, ` or a="b"`, maxExprBytes-8) + "}",
	}
}

// BenchmarkCheckExpr_WorstAccepted 通过保护的最坏情况：单条 8KiB 表达式的校验成本上限。
func BenchmarkCheckExpr_WorstAccepted(b *testing.B) {
	for name, expr := range worstAccepted() {
		b.Run(name, func(b *testing.B) {
			if err := guardExpr(expr); err != nil {
				b.Fatalf("guardExpr(%d bytes) = %v, want an accepted expression", len(expr), err)
			}
			b.SetBytes(int64(len(expr)))
			for b.Loop() {
				_ = checkExpr(expr)
			}
		})
	}
}

// BenchmarkCheckExpr_LongRealistic 实际的 2-4KiB 表达式的校验成本。
func BenchmarkCheckExpr_LongRealistic(b *testing.B) {
	for name, expr := range longExprs() {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(expr)))
			for b.Loop() {
				if err := checkExpr(expr); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkImportPreview_WorstCase 一次导入 / 预览的语法校验成本上限：表达式总量用满
// maxImportExprBytes，每条都是通过保护的最慢的 8KiB 表达式。
func BenchmarkImportPreview_WorstCase(b *testing.B) {
	for name, expr := range worstAccepted() {
		var doc strings.Builder
		doc.WriteString("groups:\n- name: g\n  rules:\n")
		for i := range min(maxImportExprBytes/len(expr), MaxImportRules) {
			fmt.Fprintf(&doc, "  - alert: a%d\n    expr: '%s'\n", i, expr)
		}
		content := doc.String()
		b.Run(name, func(b *testing.B) {
			if len(content) > maxImportBytes {
				b.Fatalf("content is %d bytes, over the import limit", len(content))
			}
			for b.Loop() {
				parsed, err := parseRuleFile(content)
				if err != nil {
					b.Fatal(err)
				}
				for i, p := range parsed {
					convertRule(i, p, importOpts())
				}
			}
		})
	}
}

// 通过保护的最坏情况必须有界：单条表达式的校验在几毫秒内（见 BenchmarkCheckExpr_WorstAccepted），
// 这里留足余量只断言量级，防止保护口径回退后重新出现百毫秒级的解析。
// 用相对耗时判定，不用绝对的毫秒数：整套测试并行跑（-race、CPU 被占满）时绝对耗时可以差出几十倍，而
// 「最坏的可接受表达式」与一条普通长表达式的比值是稳定的——被保护拦下的那类表达式（n² 的 ^ 链）比值在
// 千倍以上，能通过保护的最坏情形在几十倍以内。
func TestCheckExpr_WorstAcceptedIsBounded(t *testing.T) {
	bestOf := func(expr string) time.Duration {
		best := time.Hour
		for range 7 {
			start := time.Now()
			_ = checkExpr(expr)
			best = min(best, time.Since(start))
		}
		return best
	}
	// 基准：约 2 KiB 的普通表达式（40 个阈值条件的并集）。
	baseline := bestOf(strings.TrimSuffix(strings.Repeat(`sum(rate(http_requests_total{job="api"}[5m])) by (instance) > 100 or `, 40), " or "))
	limit := max(300*baseline, 60*time.Millisecond)
	for name, expr := range worstAccepted() {
		if err := guardExpr(expr); err != nil {
			t.Errorf("%s: guardExpr(%d bytes) = %v, want an accepted expression", name, len(expr), err)
			continue
		}
		if best := bestOf(expr); best > limit {
			t.Errorf("%s: checkExpr(%d bytes) took %v (baseline %v, limit %v), want a few milliseconds", name, len(expr), best, baseline, limit)
		}
	}
}
