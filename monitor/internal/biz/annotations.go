package biz

import (
	"strings"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
)

// 模板的校验、沙箱与执行限额（单次渲染 ≤ 100ms、输出 ≤ 4KB）都在 kit：校验是
// monitorcontract.ValidateTemplate，执行是 monitorpipeline.CompileTemplate / Template.Render，
// core 保存规则、模拟触发与引擎渲染用的是同一份。这里只有引擎自己的两个用法。

// templateError 渲染失败时该项的取值；不影响触发。
const templateError = "<template error>"

// renderAnnotations 为一批要新建事件的样本渲染规则的附加信息（指纹 → 名称 → 文本）。模板每批只解析一次；
// 单项失败（未通过校验、执行出错、超时、输出过大）取 templateError，不影响其余项与触发。
// 在数据库事务之外调用：状态机（Advance）只接收渲染结果，自身不执行任何模板。
func renderAnnotations(source map[string]string, samples []*Sample) map[string]map[string]string {
	parsed := make(map[string]*monitorpipeline.Template, len(source)) // 值为 nil = 校验或解析失败
	for name, text := range source {
		t, err := monitorpipeline.CompileTemplate(text)
		if err != nil {
			t = nil
		}
		parsed[name] = t
	}
	out := make(map[string]map[string]string, len(samples))
	for _, s := range samples {
		rendered := make(map[string]string, len(parsed))
		for name, t := range parsed {
			rendered[name] = templateError
			if t == nil {
				continue
			}
			if text, err := t.Render(s.Labels, s.Value); err == nil {
				rendered[name] = text
			}
		}
		out[s.Fingerprint] = rendered
	}
	return out
}

// renderRecoveryExpr 渲染恢复取值 PromQL；失败或结果为空返回 false（该事件不取恢复值）。
func renderRecoveryExpr(t *monitorpipeline.Template, labels map[string]string, value float64) (string, bool) {
	s, err := t.Render(labels, value)
	if err != nil || strings.TrimSpace(s) == "" || !utf8.ValidString(s) {
		return "", false
	}
	return s, true
}
