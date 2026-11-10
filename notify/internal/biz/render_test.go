package biz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRendererDefaultAndCustom(t *testing.T) {
	t.Parallel()
	r := NewRenderer()
	ev := testEvent("e1", "monitor.alert.firing", 3)
	ev.Payload = []byte(`{"host":"web-1","cpu":92.5}`)
	subject, body, err := r.Render(context.Background(), nil, RenderContextFor(ev, nil))
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
	subject, body, err = r.Render(context.Background(), tpl, RenderContextFor(ev, nil))
	if err != nil || subject != "default:t-e1" || body != "主机 web-1 CPU 92.5" {
		t.Fatalf("custom render = %q / %q, %v", subject, body, err)
	}
	agg := &AggContext{Count: 3, Samples: []AggSample{{Title: "a"}, {Title: "b"}}}
	_, body, err = r.Render(context.Background(), tpl, RenderContextFor(ev, agg))
	// 样本/载荷为 map：缺失键渲染为可见的 "<no value>"（不失败，与 core 写入试渲染口径一致）。
	if err != nil || body != "主机 web-1 CPU 92.5 共 3 条|a<no value>|b<no value>" {
		t.Fatalf("aggregate render = %q, %v", body, err)
	}
	// 摘要模式：样本不注入。
	agg.Digest = true
	_, body, _ = r.Render(context.Background(), tpl, RenderContextFor(ev, agg))
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
	if _, _, err := r.Render(context.Background(), big, rc); !errors.Is(err, ErrRenderTooLarge) {
		t.Fatalf("oversize render error = %v, want ErrRenderTooLarge", err)
	}
	// 语法错误：确定性失败。
	if _, _, err := r.Render(context.Background(), &Template{Subject: "{{.Title", Body: "x"}, rc); err == nil {
		t.Fatal("bad template must fail")
	}
	// 超时。
	slow := r.WithTimeout(time.Nanosecond)
	if _, _, err := slow.Render(context.Background(), big, rc); !errors.Is(err, ErrRenderTimeout) && !errors.Is(err, ErrRenderTooLarge) {
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
