package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

// 模板可以引用嵌套的载荷路径（tag_match 支持 labels.env 这类点分路径）：此前写入校验用只有一个键的样例载荷
// 试渲染，`{{.Payload.labels.env}}` 因下钻缺失键报错而被拒——「预览通过、保存失败」。
func TestTemplateValidation_AcceptsNestedPayloadPaths(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"", notifycontract.TemplateFormatText, notifycontract.TemplateFormatHTML} {
		for _, body := range []string{
			`环境：{{.Payload.labels.env}}`,
			`{{.Payload.a.b.c.d}} / {{$.Payload.labels.env}}`,
			`{{with .Payload.labels}}{{.env}}{{end}}`,
			`{{if .Payload.labels.env}}prod{{else}}other{{end}}`,
			`{{.Payload.labels.env | upper}}`,
			`{{range .Samples}}{{.metric}}{{$.Payload.labels.env}}{{end}}`,
		} {
			if err := validateTemplateTextFormat("body", body, format); err != nil {
				t.Errorf("format %q body %q: validateTemplateTextFormat() error = %v, want nil", format, body, err)
			}
		}
	}
	// 宽松放行的只有载荷下钻：顶层字段拼错仍然拒绝——包括出现在嵌套载荷路径之后的（第一遍在此中断，
	// 靠第二遍兜住）。
	// （条件为假时不执行的分支体、空循环的循环体不在试渲染的覆盖范围内，是既有的已知宽松点。）
	for _, body := range []string{
		`{{.Payload.labels.env}}{{.Nope}}`,
		`{{.Payload.labels.env}}{{$.Nope}}{{.Payload.a.b}}`,
	} {
		if err := validateTemplateTextFormat("body", body, notifycontract.TemplateFormatText); !errors.Is(err, ErrTemplateInvalid) {
			t.Errorf("body %q: validateTemplateTextFormat() error = %v, want ErrTemplateInvalid", body, err)
		}
	}
	// 顶层字段类型用错也照旧拒绝（不是载荷下钻）。
	if err := validateTemplateTextFormat("body", `{{severity .Title}}`, notifycontract.TemplateFormatText); !errors.Is(err, ErrTemplateInvalid) {
		t.Errorf("severity on a string: validateTemplateTextFormat() error = %v, want ErrTemplateInvalid", err)
	}

	// 经用例入口（Create）同样可保存。
	stub := newTemplateRepoStub()
	uc := NewTemplateUsecase(stub)
	tpl := newAlertTemplate("monitor.alert.firing", ChannelTypeEmail)
	tpl.Subject, tpl.Body = "s", `环境：{{.Payload.labels.env}}`
	if _, err := uc.Create(context.Background(), tpl); err != nil {
		t.Fatalf("Create(template with a nested payload path) error = %v", err)
	}
}
