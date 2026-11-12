package biz

import (
	"bytes"
	"context"
	"fmt"
	htmltemplate "html/template"
	"io"
	"text/template"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

// TemplateChannelAny 是模板渠道类型维度的通配值（'*'：适用全部渠道类型）。
const TemplateChannelAny = notifycontract.TemplateChannelAny

// TemplateEventTypeAny 是模板事件类型维度的通配值（'*'：适用全部事件）。
const TemplateEventTypeAny = notifycontract.TemplateEventTypeAny

// templateEventTypePattern 事件类型取值：'*' 或小写字母/数字开头的点分串
// （如 monitor.alert.firing；与 api protovalidate 同式，入口双保险）。
var templateEventTypePattern = notifycontract.TemplateEventTypePattern

// templateChannelTypes 渠道类型维度合法取值：通配 + 五种渠道类型。
var templateChannelTypes = map[string]struct{}{
	TemplateChannelAny:  {},
	ChannelTypeEmail:    {},
	ChannelTypeWebhook:  {},
	ChannelTypeDingtalk: {},
	ChannelTypeWecom:    {},
	ChannelTypeFeishu:   {},
}

// templateVars 是渲染注入变量白名单（设计文档 §6.3；管道侧按同一集合注入，
// 本层在写入时校验顶层引用，落库前拦截拼写错误而非渲染时静默输出空值）。
// 可用函数为 kit/notifycontract.TemplateFuncs（fmtTime/severity/json/trunc/upper/lower），
// 写入校验与管道渲染注册同一集合。
var templateVars = map[string]struct{}{
	"Title":     {},
	"Payload":   {},
	"Severity":  {},
	"Count":     {},
	"Samples":   {},
	"BizID":     {},
	"EventTime": {},
}

// Template 模板领域模型（DO）：biz × event_type × channel_type 三维覆盖，
// 渲染解析链精确 > 通配（管道侧消费）；version 每次更新自增，dispatch 记录
// (template_id, version) 可复现历史通知。
type Template struct {
	ID          string
	BizID       string
	EventType   string
	ChannelType string
	Subject     string
	Body        string
	Format      string // text/markdown/html/card（空 = text）
	Version     int32
	Enabled     bool
	CreateTime  time.Time
	UpdateTime  time.Time
}

// TemplateFilter 模板列表过滤条件；指针为 nil 表示不过滤。
type TemplateFilter struct {
	BizID       string // 空 = 不过滤（受限用户由 service 层强制携带）
	Keyword     string
	EventType   *string
	ChannelType *string
	Enabled     *bool
}

// TemplateUpdate 模板部分更新；nil 字段不更新（spec 可改，组合仍须唯一；
// Version 恒由仓储自增，不经此结构）。
type TemplateUpdate struct {
	EventType   *string
	ChannelType *string
	Subject     *string
	Body        *string
	Format      *string
	Enabled     *bool
}

// templateFormats 合法输出格式。
var templateFormats = map[string]struct{}{
	notifycontract.TemplateFormatText: {}, notifycontract.TemplateFormatMarkdown: {},
	notifycontract.TemplateFormatHTML: {}, notifycontract.TemplateFormatCard: {},
}

// normalizeFormat 空 → text；非法 → ErrTemplateInvalid。
func normalizeFormat(format string) (string, error) {
	if format == "" {
		return notifycontract.TemplateFormatText, nil
	}
	if _, ok := templateFormats[format]; !ok {
		return "", fmt.Errorf("%w: bad format %q", ErrTemplateInvalid, format)
	}
	return format, nil
}

// 模板哨兵错误：reason 与 api/notify/v1 的 ErrorReason 枚举对齐。
var (
	// ErrTemplateNotFound 目标模板不存在。
	ErrTemplateNotFound = errors.NotFound(notifyv1.ErrorReason_NOTIFY_TEMPLATE_NOT_FOUND.String(), "template not found")
	// ErrTemplateSpecExists 同 biz 内 (event_type, channel_type) 组合已被占用（未删除行）。
	ErrTemplateSpecExists = errors.Conflict(notifyv1.ErrorReason_NOTIFY_TEMPLATE_SPEC_EXISTS.String(), "template spec already exists")
	// ErrTemplateInvalid 模板不合法：Go template 语法错误 / 白名单外变量 / spec 值非法。
	ErrTemplateInvalid = errors.BadRequest(notifyv1.ErrorReason_NOTIFY_TEMPLATE_INVALID.String(), "invalid template")
)

// TemplateRepo 模板仓储接口，由 data 层实现（依赖倒置）。
type TemplateRepo interface {
	// CreateTemplate 创建模板；同 biz 未删除行内 (event_type, channel_type)
	// 组合重复返回 ErrTemplateSpecExists。
	CreateTemplate(ctx context.Context, tpl *Template) error
	// UpdateTemplate 按 ID 部分更新未删除模板并 version+1；不存在返回
	// ErrTemplateNotFound。
	UpdateTemplate(ctx context.Context, id string, upd TemplateUpdate) error
	// DeleteTemplate 软删模板（三维组合可复用）；不存在返回 ErrTemplateNotFound。
	DeleteTemplate(ctx context.Context, id string) error
	// GetTemplate 按 ID 查询未删除模板；不存在返回 ErrTemplateNotFound。
	GetTemplate(ctx context.Context, id string) (*Template, error)
	// ListTemplates 分页查询未删除模板；page 从 1 起，pageSize 上限 100，按创建时间倒序。
	ListTemplates(ctx context.Context, filter TemplateFilter, page, pageSize int64) ([]*Template, int64, error)
	// ListTemplatesByBiz 取 biz 内全部未删除模板（不分页；路由试算的模板解析用）。
	ListTemplatesByBiz(ctx context.Context, bizID string) ([]*Template, error)
}

// TemplateUsecase 模板用例：承载 spec 合法性与模板内容校验（语法 + 变量白名单）。
type TemplateUsecase struct {
	repo TemplateRepo
}

// NewTemplateUsecase new a TemplateUsecase.
func NewTemplateUsecase(repo TemplateRepo) *TemplateUsecase {
	return &TemplateUsecase{repo: repo}
}

// validateTemplateSpec 校验事件类型/渠道类型维度取值（'*' 或合法具体值）。
func validateTemplateSpec(eventType, channelType string) error {
	if !templateEventTypePattern.MatchString(eventType) {
		return fmt.Errorf("%w: bad event_type %q", ErrTemplateInvalid, eventType)
	}
	if _, ok := templateChannelTypes[channelType]; !ok {
		return fmt.Errorf("%w: bad channel_type %q", ErrTemplateInvalid, channelType)
	}
	return nil
}

// validateTemplateText 校验一段模板文本：语法可解析（零函数注册——text/template
// 在解析期即拒绝未登记函数）后，用样例上下文**试渲染**：顶层变量是结构体字段，
// 拼错即 "can't evaluate field" 失败；`{{template "x"}}` 在执行期因无子模板失败；
// Payload/Samples 为 map 形态按 missingkey=zero 宽松（载荷字段是否存在属运行期
// 语义，与管道渲染口径一致：顶层严格、载荷宽松）。试渲染替代了此前的解析树
// 白名单遍历——遍历器在 with/range 重绑定域内不校验，严格性名不副实。
func validateTemplateText(name, text string) error {
	return validateTemplateTextFormat(name, text, notifycontract.TemplateFormatText)
}

// validateTemplateTextFormat 按输出格式选引擎：html 走 html/template（上下文感知
// 自动转义，与管道渲染同引擎），其余 text/template。
func validateTemplateTextFormat(name, text, format string) error {
	exec, err := compileTemplate(name, text, format)
	if err != nil {
		return err
	}
	if err := exec(io.Discard, trialRenderContext()); err != nil {
		return fmt.Errorf("%w: %s render: %v", ErrTemplateInvalid, name, err)
	}
	return nil
}

// compileTemplate 解析模板为执行函数（text/html 引擎由 format 决定）。
func compileTemplate(name, text, format string) (func(w io.Writer, data any) error, error) {
	if format == notifycontract.TemplateFormatHTML {
		t, err := htmltemplate.New(name).Funcs(notifycontract.TemplateFuncs()).Option("missingkey=zero").Parse(text)
		if err != nil {
			return nil, fmt.Errorf("%w: %s parse: %v", ErrTemplateInvalid, name, err)
		}
		return t.Execute, nil
	}
	t, err := template.New(name).Funcs(notifycontract.TemplateFuncs()).Option("missingkey=zero").Parse(text)
	if err != nil {
		return nil, fmt.Errorf("%w: %s parse: %v", ErrTemplateInvalid, name, err)
	}
	return t.Execute, nil
}

// maxRenderBytes 预览渲染输出上限（与管道渲染同口径 64KB）。
const maxRenderBytes = 64 * 1024

// limitedWriter 超上限即报错中止执行。
type limitedWriter struct {
	buf bytes.Buffer
	max int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.buf.Len()+len(p) > w.max {
		return 0, fmt.Errorf("output exceeds %d bytes", w.max)
	}
	return w.buf.Write(p)
}

// Render 用样例上下文渲染 subject/body（控制台预览，不落库）：与写入校验同一
// 解析/执行口径（零函数、missingkey=zero、顶层字段严格），输出各限 64KB；
// 错误以 ErrTemplateInvalid 呈现。
func (uc *TemplateUsecase) Render(subject, body, format string, sample TrialRenderContext) (string, string, error) {
	format, err := normalizeFormat(format)
	if err != nil {
		return "", "", err
	}
	// 标题恒按纯文本渲染（邮件 Subject / 卡片标题都不是 HTML 上下文）。
	s, err := renderText("subject", subject, notifycontract.TemplateFormatText, sample)
	if err != nil {
		return "", "", err
	}
	b, err := renderText("body", body, format, sample)
	if err != nil {
		return "", "", err
	}
	return s, b, nil
}

func renderText(name, text, format string, sample TrialRenderContext) (string, error) {
	exec, err := compileTemplate(name, text, format)
	if err != nil {
		return "", err
	}
	w := &limitedWriter{max: maxRenderBytes}
	if err := exec(w, sample); err != nil {
		return "", fmt.Errorf("%w: %s render: %v", ErrTemplateInvalid, name, err)
	}
	return w.buf.String(), nil
}

// TrialRenderContext 试渲染样例上下文：字段集 = 渲染白名单 templateVars（管道
// biz.RenderContext 同形）；Title/Payload/Samples 非空以驱动 if/with/range 主体执行。
type TrialRenderContext struct {
	Title     string
	Payload   map[string]any
	Severity  int32
	Count     int
	Samples   []map[string]any
	BizID     string
	EventTime time.Time
}

func trialRenderContext() TrialRenderContext {
	return TrialRenderContext{
		Title:     "sample",
		Payload:   map[string]any{"host": "sample-host"},
		Severity:  2,
		Count:     2,
		Samples:   []map[string]any{{"event_id": "sample", "title": "sample", "severity": int32(2), "create_time": time.Unix(0, 0).UTC()}},
		BizID:     DefaultBizID,
		EventTime: time.Unix(0, 0).UTC(),
	}
}

// Create 创建模板：校验 spec 与模板内容，生成 ID 与审计时间后落库；
// BizID 缺省 default，Version 起始为 1。
func (uc *TemplateUsecase) Create(ctx context.Context, tpl *Template) (*Template, error) {
	if err := validateTemplateSpec(tpl.EventType, tpl.ChannelType); err != nil {
		return nil, err
	}
	format, err := normalizeFormat(tpl.Format)
	if err != nil {
		return nil, err
	}
	tpl.Format = format
	if err := validateTemplateText("subject", tpl.Subject); err != nil {
		return nil, err
	}
	if err := validateTemplateTextFormat("body", tpl.Body, format); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	tpl.ID = id
	if tpl.BizID == "" {
		tpl.BizID = DefaultBizID
	}
	tpl.Version = 1
	tpl.CreateTime, tpl.UpdateTime = now, now
	if err := uc.repo.CreateTemplate(ctx, tpl); err != nil {
		return nil, err
	}
	return tpl, nil
}

// Update 部分更新模板：提供的 spec 字段与模板文本均先校验（正文按生效 format 的
// 引擎校验：改 format 或改正文都要拿现有模板补齐另一半）；version 由仓储自增。
func (uc *TemplateUsecase) Update(ctx context.Context, id string, upd TemplateUpdate) error {
	var current *Template
	load := func() (*Template, error) {
		if current == nil {
			tpl, err := uc.repo.GetTemplate(ctx, id)
			if err != nil {
				return nil, err
			}
			current = tpl
		}
		return current, nil
	}
	if upd.EventType != nil || upd.ChannelType != nil {
		tpl, err := load()
		if err != nil {
			return err
		}
		eventType, channelType := tpl.EventType, tpl.ChannelType
		if upd.EventType != nil {
			eventType = *upd.EventType
		}
		if upd.ChannelType != nil {
			channelType = *upd.ChannelType
		}
		if err := validateTemplateSpec(eventType, channelType); err != nil {
			return err
		}
	}
	if upd.Format != nil {
		format, err := normalizeFormat(*upd.Format)
		if err != nil {
			return err
		}
		upd.Format = &format
	}
	if upd.Subject != nil {
		if err := validateTemplateText("subject", *upd.Subject); err != nil {
			return err
		}
	}
	if upd.Body != nil || upd.Format != nil {
		tpl, err := load()
		if err != nil {
			return err
		}
		body, format := tpl.Body, tpl.Format
		if upd.Body != nil {
			body = *upd.Body
		}
		if upd.Format != nil {
			format = *upd.Format
		}
		if format == "" {
			format = notifycontract.TemplateFormatText
		}
		if err := validateTemplateTextFormat("body", body, format); err != nil {
			return err
		}
	}
	return uc.repo.UpdateTemplate(ctx, id, upd)
}

// Delete 软删模板；不存在返回 ErrTemplateNotFound。
func (uc *TemplateUsecase) Delete(ctx context.Context, id string) error {
	return uc.repo.DeleteTemplate(ctx, id)
}

// Get 查询模板详情。
func (uc *TemplateUsecase) Get(ctx context.Context, id string) (*Template, error) {
	return uc.repo.GetTemplate(ctx, id)
}

// List 分页查询模板；分页参数先规整再委托仓储。
func (uc *TemplateUsecase) List(ctx context.Context, filter TemplateFilter, page, pageSize int64) ([]*Template, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListTemplates(ctx, filter, page, pageSize)
}
