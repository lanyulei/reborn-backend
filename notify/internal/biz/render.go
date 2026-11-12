package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	htmltemplate "html/template"
	"io"
	"sync"
	"text/template"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

// 渲染约束（设计 §6.3）：仅白名单纯函数、100ms 超时、输出 64KB 上限。
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

// 内置默认模板（解析链全部缺失时使用）：邮件/webhook 多行完整版，群机器人紧凑版
// （正文受平台字节上限约束）。载荷以 JSON 打印而非 Go map 字面量。
var (
	defaultSubject = template.Must(template.New("subject").Funcs(notifycontract.TemplateFuncs()).Parse(`[{{.BizID}}] {{.Title}}`))
	defaultBody    = template.Must(template.New("body").Funcs(notifycontract.TemplateFuncs()).Parse(
		"{{.Title}}\n级别：{{severity .Severity}}\n时间：{{fmtTime .EventTime}}\n" +
			"{{if gt .Count 1}}聚合 {{.Count}} 条事件\n{{range .Samples}}- {{.title}}（{{fmtTime .create_time}}）\n{{end}}{{end}}" +
			"{{if .Payload}}载荷：{{json .Payload}}\n{{end}}"))
	defaultBodyCompact = template.Must(template.New("body").Funcs(notifycontract.TemplateFuncs()).Parse(
		"{{.Title}}\n{{severity .Severity}} · {{fmtTime .EventTime}}{{if gt .Count 1}} · 聚合 {{.Count}} 条{{end}}" +
			"{{if .Payload}}\n{{trunc (json .Payload) 500}}{{end}}"))
)

// defaultBodyFor 按渠道类型取缺省正文模板。
func defaultBodyFor(channelType string) *template.Template {
	switch channelType {
	case ChannelTypeDingtalk, ChannelTypeWecom, ChannelTypeFeishu:
		return defaultBodyCompact
	}
	return defaultBody
}

// Renderer 模板渲染器：text/template 零函数沙箱，超时与体积受限；解析结果按
// (template id, version) 缓存（模板版本化：同 id 改文本必 +1，缓存天然失效）。
type Renderer struct {
	timeout time.Duration
	cache   *templateCache
}

// templateCache 解析缓存：条目数超上限整体重置（配置表级规模，极少触发）。
type templateCache struct {
	mu     sync.RWMutex
	parsed map[templateKeyV]parsedTemplate
}

type templateKeyV struct {
	id      string
	version int32
}

// parsedTemplate 解析结果：subject 恒 text 引擎；body 按 format 选 text/html 引擎
// （html/template 上下文感知转义，载荷不会注入标签）。
type parsedTemplate struct {
	subject *template.Template
	body    executor
	format  string
}

// executor 统一 text/template 与 html/template 的执行入口。
type executor interface {
	Execute(w io.Writer, data any) error
}

const templateCacheMax = 1024

// NewRenderer new a Renderer（timeout ≤0 取缺省 100ms）。
func NewRenderer() *Renderer {
	return &Renderer{timeout: defaultRenderTimeout, cache: &templateCache{parsed: map[templateKeyV]parsedTemplate{}}}
}

// WithTimeout 返回指定超时的渲染器副本（共享解析缓存）。
func (r *Renderer) WithTimeout(d time.Duration) *Renderer {
	if d <= 0 {
		d = defaultRenderTimeout
	}
	return &Renderer{timeout: d, cache: r.cache}
}

// parse 取解析结果：命中缓存直接返回；解析失败不缓存（确定性错误，调用方死信）。
func (r *Renderer) parse(tpl *Template) (parsedTemplate, error) {
	key := templateKeyV{id: tpl.ID, version: tpl.Version}
	r.cache.mu.RLock()
	pt, ok := r.cache.parsed[key]
	r.cache.mu.RUnlock()
	if ok {
		return pt, nil
	}
	subjT, err := template.New("subject").Funcs(notifycontract.TemplateFuncs()).Option("missingkey=zero").Parse(tpl.Subject)
	if err != nil {
		return parsedTemplate{}, fmt.Errorf("parse subject template %s: %w", tpl.ID, err)
	}
	var bodyT executor
	if tpl.Format == notifycontract.TemplateFormatHTML {
		bodyT, err = htmltemplate.New("body").Funcs(notifycontract.TemplateFuncs()).Option("missingkey=zero").Parse(tpl.Body)
	} else {
		bodyT, err = template.New("body").Funcs(notifycontract.TemplateFuncs()).Option("missingkey=zero").Parse(tpl.Body)
	}
	if err != nil {
		return parsedTemplate{}, fmt.Errorf("parse body template %s: %w", tpl.ID, err)
	}
	pt = parsedTemplate{subject: subjT, body: bodyT, format: tpl.Format}
	r.cache.mu.Lock()
	if len(r.cache.parsed) >= templateCacheMax {
		r.cache.parsed = map[templateKeyV]parsedTemplate{}
	}
	r.cache.parsed[key] = pt
	r.cache.mu.Unlock()
	return pt, nil
}

// Render 渲染 subject/body：tpl 为 nil 用内置默认（按 channelType 选多行/紧凑版，
// 格式 text）。返回的 format 已按渠道能力降级（notifycontract.EffectiveFormat）。
// 模板语法错误/未知字段为确定性失败（调用方按不可重试处理）；超时视为瞬时失败。
// 白名单函数（kit/notifycontract.TemplateFuncs）与 core 写入校验同一集合。
func (r *Renderer) Render(ctx context.Context, tpl *Template, channelType string, rc RenderContext) (subject, body, format string, err error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	var (
		subjT executor = defaultSubject
		bodyT executor = defaultBodyFor(channelType)
	)
	format = notifycontract.TemplateFormatText
	if tpl != nil {
		pt, err := r.parse(tpl)
		if err != nil {
			return "", "", "", err
		}
		subjT, bodyT, format = pt.subject, pt.body, pt.format
	}
	format = notifycontract.EffectiveFormat(channelType, format)
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
		return res.subject, res.body, format, res.err
	case <-ctx.Done():
		return "", "", "", ErrRenderTimeout
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

func execLimited(t executor, rc RenderContext) (string, error) {
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
