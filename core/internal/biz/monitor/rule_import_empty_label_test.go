package biz

import (
	"strings"
	"testing"
)

// Prometheus 规则文件里取值为空的标签等价于没有这个标签（monitor 模块评审 core-monitor-rules P3-5）：
// 此前原样收下后过不了标签校验，整条规则被判为 invalid，报的还是「labels: label "team" value」这种看不懂的错。
func TestConvertRule_EmptyLabelValueIsSkippedWithAWarning(t *testing.T) {
	t.Parallel()
	items := convertAll(t, "groups:\n- name: g\n  rules:\n  - alert: a\n    expr: up\n    labels: {severity: warning, team: \"\", owner: ops}\n")
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	it := items[0]
	if it.Status != ImportNew || it.Error != "" || it.Spec == nil {
		t.Fatalf("item = {%s %q %s}, want a convertible rule", it.Name, it.Status, it.Error)
	}
	if _, has := it.Spec.Labels["team"]; has || it.Spec.Labels["owner"] != "ops" {
		t.Fatalf("labels = %v, want the empty one dropped and the others kept", it.Spec.Labels)
	}
	warnings := strings.Join(it.Warnings, "|")
	if strings.Count(warnings, "team") != 1 || !strings.Contains(warnings, "值为空") {
		t.Fatalf("warnings = %v, want one about the skipped empty label", it.Warnings)
	}
	// 级别标签取值为空同样采用默认级别，而不是让规则不合法。
	if items = convertAll(t, "groups:\n- name: g\n  rules:\n  - alert: b\n    expr: up\n    labels: {severity: \"\"}\n"); items[0].Status != ImportNew {
		t.Fatalf("an empty severity label must fall back to the default severity: %+v", items[0])
	}
	// 不合法的标签名照旧拒绝。
	if items = convertAll(t, "groups:\n- name: g\n  rules:\n  - alert: c\n    expr: up\n    labels: {\"bad-name\": x}\n"); items[0].Status != ImportInvalid {
		t.Fatalf("an invalid label name must still be rejected: %+v", items[0])
	}
}
