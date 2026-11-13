package biz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"text/template"
	"time"
)

func TestRendererDefaultAndCustom(t *testing.T) {
	t.Parallel()
	r := NewRenderer()
	ev := testEvent("e1", "monitor.alert.firing", 3)
	ev.Payload = []byte(`{"host":"web-1","cpu":92.5}`)
	subject, body, _, err := r.Render(context.Background(), nil, ChannelTypeEmail, RenderContextFor(ev, nil))
	if err != nil {
		t.Fatalf("Render(default) error = %v", err)
	}
	if subject != "[default] t-e1" || !strings.Contains(body, "web-1") || strings.Contains(body, "聚合") {
		t.Fatalf("default render = %q / %q", subject, body)
	}
	tpl := &Template{
		ID: "t", Version: 3, Subject: `{{.BizID}}:{{.Title}}`,
		Body: `主机 {{.Payload.host}} CPU {{.Payload.cpu}}{{if gt .Count 1}} 共 {{.Count}} 条{{range .Samples}}|{{.title}}{{.nope}}{{end}}{{end}}`,
	}
	subject, body, _, err = r.Render(context.Background(), tpl, ChannelTypeEmail, RenderContextFor(ev, nil))
	if err != nil || subject != "default:t-e1" || body != "主机 web-1 CPU 92.5" {
		t.Fatalf("custom render = %q / %q, %v", subject, body, err)
	}
	agg := &AggContext{Count: 3, Samples: []AggSample{{Title: "a"}, {Title: "b"}}}
	_, body, _, err = r.Render(context.Background(), tpl, ChannelTypeEmail, RenderContextFor(ev, agg))
	// 样本/载荷为 map：缺失键渲染为可见的 "<no value>"（不失败，与 core 写入试渲染口径一致）。
	if err != nil || body != "主机 web-1 CPU 92.5 共 3 条|a<no value>|b<no value>" {
		t.Fatalf("aggregate render = %q, %v", body, err)
	}
	// 摘要模式：样本不注入。
	agg.Digest = true
	_, body, _, _ = r.Render(context.Background(), tpl, ChannelTypeEmail, RenderContextFor(ev, agg))
	if body != "主机 web-1 CPU 92.5 共 3 条" {
		t.Fatalf("digest render = %q", body)
	}
}

func TestRendererLimits(t *testing.T) {
	t.Parallel()
	r := NewRenderer()
	ev := testEvent("e1", "a.b", 1)
	// 输出超 64KB。
	big := &Template{Subject: "s", Body: `{{range .Samples}}` + strings.Repeat("x", 1024) + `{{end}}`}
	rc := RenderContextFor(ev, &AggContext{Count: 100, Samples: make([]AggSample, 100)})
	if _, _, _, err := r.Render(context.Background(), big, ChannelTypeEmail, rc); !errors.Is(err, ErrRenderTooLarge) {
		t.Fatalf("oversize render error = %v, want ErrRenderTooLarge", err)
	}
	// 语法错误：确定性失败。
	if _, _, _, err := r.Render(context.Background(), &Template{Subject: "{{.Title", Body: "x"}, ChannelTypeEmail, rc); err == nil {
		t.Fatal("bad template must fail")
	}
	// 超时。
	slow := r.WithTimeout(time.Nanosecond)
	if _, _, _, err := slow.Render(context.Background(), big, ChannelTypeEmail, rc); !errors.Is(err, ErrRenderTimeout) && !errors.Is(err, ErrRenderTooLarge) {
		t.Fatalf("timeout render error = %v", err)
	}
}

func TestContentCodec(t *testing.T) {
	t.Parallel()
	raw, err := EncodeContent(&DispatchContent{Agg: &AggContext{Count: 2}})
	if err != nil {
		t.Fatal(err)
	}
	c, err := DecodeContent(raw)
	if err != nil || c.Rendered() || c.Agg == nil || c.Agg.Count != 2 {
		t.Fatalf("DecodeContent = %+v, %v", c, err)
	}
	if c, err := DecodeContent(nil); err != nil || c.Rendered() {
		t.Fatalf("DecodeContent(nil) = %+v, %v", c, err)
	}
	if _, err := DecodeContent([]byte("{oops")); err == nil {
		t.Fatal("bad content must error")
	}
}

// TestRendererDefaultsAndFuncs 群机器人用紧凑缺省正文、邮件多行；载荷以 JSON 打印；
// 白名单函数可用；非对象载荷被包成 {"_raw": …} 不会让 .Payload.host 执行失败。
func TestRendererDefaultsAndFuncs(t *testing.T) {
	t.Parallel()
	r := NewRenderer()
	ev := testEvent("e1", "monitor.alert.firing", 3)
	ev.Payload = []byte(`{"host":"web-1"}`)
	_, email, _, err := r.Render(context.Background(), nil, ChannelTypeEmail, RenderContextFor(ev, nil))
	if err != nil || !strings.Contains(email, "级别：error") || !strings.Contains(email, `载荷：{"host":"web-1"}`) {
		t.Fatalf("email default = %q, %v", email, err)
	}
	_, robot, _, err := r.Render(context.Background(), nil, ChannelTypeDingtalk, RenderContextFor(ev, nil))
	if err != nil || strings.Contains(robot, "级别：") || !strings.Contains(robot, "error ·") || !strings.Contains(robot, `{"host":"web-1"}`) {
		t.Fatalf("robot default = %q, %v", robot, err)
	}
	tpl := &Template{ID: "f", Version: 1, Subject: "{{severity .Severity}}/{{upper .Title}}", Body: "{{trunc (json .Payload) 8}}|{{fmtTime .EventTime \"2006\"}}"}
	subject, body, _, err := r.Render(context.Background(), tpl, ChannelTypeEmail, RenderContextFor(ev, nil))
	if err != nil || subject != "error/T-E1" || !strings.HasPrefix(body, `{"host":…|`) {
		t.Fatalf("funcs render = %q / %q, %v", subject, body, err)
	}
	ev.Payload = []byte(`[1,2]`)
	tpl2 := &Template{ID: "g", Version: 1, Subject: "s", Body: "h={{.Payload.host}} raw={{json .Payload._raw}}"}
	if _, body, _, err = r.Render(context.Background(), tpl2, ChannelTypeEmail, RenderContextFor(ev, nil)); err != nil || body != "h=<no value> raw=[1,2]" {
		t.Fatalf("non-object payload = %q, %v", body, err)
	}
}

// TestRendererFormat html 正文走 html/template（载荷转义）、format 按渠道降级并随结果返回。
func TestRendererFormat(t *testing.T) {
	t.Parallel()
	r := NewRenderer()
	ev := testEvent("e1", "a.b", 3)
	ev.Payload = []byte(`{"host":"<b>x</b>"}`)
	htmlTpl := &Template{ID: "h", Version: 1, Subject: "{{.Title}}", Body: "<p>{{.Payload.host}}</p>", Format: "html"}
	_, body, format, err := r.Render(context.Background(), htmlTpl, ChannelTypeEmail, RenderContextFor(ev, nil))
	if err != nil || format != "html" || body != "<p>&lt;b&gt;x&lt;/b&gt;</p>" {
		t.Fatalf("html email = %q %q %v", body, format, err)
	}
	_, body, format, err = r.Render(context.Background(), htmlTpl, ChannelTypeDingtalk, RenderContextFor(ev, nil))
	if err != nil || format != "markdown" {
		t.Fatalf("html on robot must degrade to markdown: %q %q %v", body, format, err)
	}
	cardTpl := &Template{ID: "c", Version: 1, Subject: "s", Body: "b", Format: "card"}
	if _, _, format, _ = r.Render(context.Background(), cardTpl, ChannelTypeFeishu, RenderContextFor(ev, nil)); format != "card" {
		t.Fatalf("card on feishu = %q", format)
	}
	if _, _, format, _ = r.Render(context.Background(), cardTpl, ChannelTypeEmail, RenderContextFor(ev, nil)); format != "text" {
		t.Fatalf("card on email must degrade to text, got %q", format)
	}
	if _, _, format, _ = r.Render(context.Background(), nil, ChannelTypeWebhook, RenderContextFor(ev, nil)); format != "text" {
		t.Fatalf("default template format = %q", format)
	}
	textTpl := &Template{ID: "t", Version: 1, Subject: "s", Body: "<p>{{.Payload.host}}</p>"}
	if _, body, _, _ = r.Render(context.Background(), textTpl, ChannelTypeEmail, RenderContextFor(ev, nil)); body != "<p><b>x</b></p>" {
		t.Fatalf("text template must not escape: %q", body)
	}
}

// TestRendererStopsAfterCancel ctx 结束后模板执行在下一次输出写入处中止（渲染协程不在
// 超时后继续跑完）。
func TestRendererStopsAfterCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tpl, err := template.New("t").Parse("{{range .Samples}}x{{end}}")
	if err != nil {
		t.Fatal(err)
	}
	rc := RenderContext{Samples: make([]map[string]any, 100)}
	if _, err := execLimited(ctx, tpl, rc); !errors.Is(err, context.Canceled) {
		t.Fatalf("execLimited(cancelled) error = %v, want context.Canceled", err)
	}
	if out, err := execLimited(context.Background(), tpl, rc); err != nil || len(out) != 100 {
		t.Fatalf("execLimited() = %d bytes, %v", len(out), err)
	}
}
