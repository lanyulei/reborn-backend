package biz

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

// 嵌套 range 是指数级迭代，且渲染 goroutine 只在写出时才响应取消（超时返回后仍在空转）：与 core 的
// 写入校验同一条规则，在解析期按确定性错误拒绝。
func TestRenderer_RejectsNestedRange(t *testing.T) {
	t.Parallel()
	r := NewRenderer()
	ev := testEvent("e1", "monitor.alert.firing", 3)
	nested := "x" + strings.Repeat("{{range .Payload}}", 30) + strings.Repeat("{{end}}", 30)
	for _, tpl := range []*Template{
		{ID: "nested-body", Version: 1, Subject: "s", Body: nested},
		{ID: "nested-subject", Version: 1, Subject: nested, Body: "b"},
		{ID: "nested-html", Version: 1, Subject: "s", Body: nested, Format: notifycontract.TemplateFormatHTML},
	} {
		_, _, _, err := r.Render(context.Background(), tpl, ChannelTypeEmail, RenderContextFor(ev, nil))
		if !errors.Is(err, notifycontract.ErrNestedRange) {
			t.Errorf("Render(%s) error = %v, want a nested-range rejection (not a timeout)", tpl.ID, err)
		}
	}
	// 单层 range 照常渲染。
	ok := &Template{ID: "single", Version: 1, Subject: "s", Body: "{{range .Samples}}[{{.title}}]{{end}}"}
	if _, body, _, err := r.Render(context.Background(), ok, ChannelTypeEmail, RenderContextFor(ev, &AggContext{Count: 2, Samples: []AggSample{{Title: "a"}, {Title: "b"}}})); err != nil || body != "[a][b]" {
		t.Fatalf("Render(single range) = %q, %v", body, err)
	}
}
