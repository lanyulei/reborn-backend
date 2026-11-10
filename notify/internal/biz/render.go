package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"text/template"
	"time"
)

// 渲染约束（设计 §6.3）：零函数、100ms 超时、输出 64KB 上限。
const (
	defaultRenderTimeout = 100 * time.Millisecond
	maxRenderBytes       = 64 * 1024
)

// ErrRenderTooLarge 渲染输出超 64KB。
var ErrRenderTooLarge = errors.New("render output exceeds 64KB")

// ErrRenderTimeout 渲染超时。
var ErrRenderTimeout = errors.New("render timed out")

// RenderContext 模板注入变量（白名单与 core 写入校验一致：Title/Payload/
// Severity/Count/Samples/BizID/EventTime）。顶层字段是结构体——拼错即渲染
// 失败（确定性，进死信可观测）；Payload / Samples 元素是 map——字段缺失按
// missingkey=zero 宽松，渲染为可见的 "<no value>"（载荷字段是否存在属运行期
// 语义；与 core 写入时的试渲染口径一致，拼错能在通知正文里被看见而非静默）。
type RenderContext struct {
	Title     string
	Payload   any
	Severity  int32
	Count     int
	Samples   []map[string]any
	BizID     string
	EventTime time.Time
}

// 内置默认模板（解析链全部缺失时使用）。
var (
	defaultSubject = template.Must(template.New("subject").Parse(`[{{.BizID}}] {{.Title}}`))
	defaultBody    = template.Must(template.New("body").Parse(
		"{{.Title}}\n级别：{{.Severity}}\n时间：{{.EventTime}}\n" +
			"{{if gt .Count 1}}聚合 {{.Count}} 条事件\n{{range .Samples}}- {{.title}}（{{.create_time}}）\n{{end}}{{end}}" +
			"{{if .Payload}}载荷：{{.Payload}}\n{{end}}"))
)

// Renderer 模板渲染器：text/template 零函数沙箱，超时与体积受限；解析结果缓存
// 由调用方（按 template id+version）持有——P1 每次解析（模板文本 ≤ 数 KB，
// 解析成本微秒级，先不缓存）。
type Renderer struct {
	timeout time.Duration
}

// NewRenderer new a Renderer（timeout ≤0 取缺省 100ms）。
func NewRenderer() *Renderer { return &Renderer{timeout: defaultRenderTimeout} }

// WithTimeout 返回指定超时的渲染器副本。
func (r *Renderer) WithTimeout(d time.Duration) *Renderer {
	if d <= 0 {
		d = defaultRenderTimeout
	}
	return &Renderer{timeout: d}
}

// Render 渲染 subject/body：tpl 为 nil 用内置默认。模板语法错误/未知字段为
// 确定性失败（调用方按不可重试处理）；超时视为瞬时失败。
func (r *Renderer) Render(ctx context.Context, tpl *Template, rc RenderContext) (subject, body string, err error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	subjT, bodyT := defaultSubject, defaultBody
	if tpl != nil {
		if subjT, err = template.New("subject").Option("missingkey=zero").Parse(tpl.Subject); err != nil {
			return "", "", fmt.Errorf("parse subject template %s: %w", tpl.ID, err)
		}
		if bodyT, err = template.New("body").Option("missingkey=zero").Parse(tpl.Body); err != nil {
			return "", "", fmt.Errorf("parse body template %s: %w", tpl.ID, err)
		}
	}
	type result struct {
		subject, body string
		err           error
	}
	done := make(chan result, 1)
	go func() {
		s, err := execLimited(subjT, rc)
		if err != nil {
			done <- result{err: fmt.Errorf("render subject: %w", err)}
			return
		}
		b, err := execLimited(bodyT, rc)
		if err != nil {
			done <- result{err: fmt.Errorf("render body: %w", err)}
			return
		}
		done <- result{subject: s, body: b}
	}()
	select {
	case res := <-done:
		return res.subject, res.body, res.err
	case <-ctx.Done():
		return "", "", ErrRenderTimeout
	}
}

// limitedWriter 超上限即报错中止执行。
type limitedWriter struct {
	buf bytes.Buffer
	max int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.buf.Len()+len(p) > w.max {
		return 0, ErrRenderTooLarge
	}
	return w.buf.Write(p)
}

func execLimited(t *template.Template, rc RenderContext) (string, error) {
	w := &limitedWriter{max: maxRenderBytes}
	if err := t.Execute(w, rc); err != nil {
		return "", err
	}
	return w.buf.String(), nil
}

// RenderContextFor 由事件与聚合上下文构造渲染变量；Count 单事件为 1。
func RenderContextFor(event *Event, agg *AggContext) RenderContext {
	rc := RenderContext{
		Title:     event.Title,
		Payload:   event.PayloadValue(),
		Severity:  event.Severity,
		Count:     1,
		BizID:     event.BizID,
		EventTime: event.CreateTime,
	}
	if agg != nil {
		rc.Count = agg.Count
		if !agg.Digest {
			rc.Samples = make([]map[string]any, 0, len(agg.Samples))
			for _, s := range agg.Samples {
				rc.Samples = append(rc.Samples, map[string]any{
					"event_id": s.EventID, "title": s.Title, "severity": s.Severity, "create_time": s.CreateTime,
				})
			}
		}
	}
	return rc
}

// EncodeContent / DecodeContent：dispatch.content 快照编解码。
func EncodeContent(c *DispatchContent) ([]byte, error) { return json.Marshal(c) }

// DecodeContent nil/空 → 空结构。
func DecodeContent(raw []byte) (*DispatchContent, error) {
	c := &DispatchContent{}
	if len(raw) == 0 {
		return c, nil
	}
	if err := json.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("decode dispatch content: %w", err)
	}
	return c, nil
}
