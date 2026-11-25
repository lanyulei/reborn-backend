package biz

import (
	"errors"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

// 嵌套 range 是指数级迭代，试渲染与预览没有超时也不响应取消：必须在解析期拒绝。此前只靠试渲染把关，
// 而样例载荷只有 1 个键，30 层嵌套、781 字节的模板瞬间通过校验并能落库——真实载荷一多，管道每次
// 渲染都超时且 goroutine 持续占满 CPU。
func TestValidateTemplate_RejectsNestedRange(t *testing.T) {
	t.Parallel()
	nested := "x" + strings.Repeat("{{range $.Payload}}", 30) + strings.Repeat("{{end}}", 30)
	for _, format := range []string{"", notifycontract.TemplateFormatText, notifycontract.TemplateFormatHTML} {
		err := validateTemplateTextFormat("body", nested, format)
		if !errors.Is(err, ErrTemplateInvalid) || !strings.Contains(err.Error(), notifycontract.ErrNestedRange.Error()) {
			t.Errorf("format %q: validateTemplateTextFormat() = %v, want ErrTemplateInvalid caused by a nested range", format, err)
		}
	}
	// 预览（RenderTemplate）走同一个编译入口。
	if _, err := renderText("body", nested, notifycontract.TemplateFormatText, trialRenderContext()); !errors.Is(err, ErrTemplateInvalid) {
		t.Fatalf("renderText(nested range) error = %v, want ErrTemplateInvalid", err)
	}
	// 单层 range、以及包在 if / with 里的 range 都是合法的。
	for _, text := range []string{
		"{{range .Samples}}- {{.title}}{{end}}",
		"{{if .Count}}{{range .Samples}}{{.title}}{{end}}{{end}}",
		"{{with .Payload}}{{range .}}{{.}}{{end}}{{end}}",
	} {
		if err := validateTemplateTextFormat("body", text, notifycontract.TemplateFormatText); err != nil {
			t.Errorf("validateTemplateTextFormat(%q) error = %v, want nil", text, err)
		}
		if err := validateTemplateTextFormat("body", text, notifycontract.TemplateFormatHTML); err != nil {
			t.Errorf("validateTemplateTextFormat(html %q) error = %v, want nil", text, err)
		}
	}
}
