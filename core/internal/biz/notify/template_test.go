package biz

import (
	"context"
	"reflect"
	"testing"

	"github.com/go-kratos/kratos/v3/errors"
)

// templateRepoStub 模板仓储内存桩：承载用例断言所需的最小语义
// （spec 组合冲突、软删不可见、nil 字段不更新、version 自增）。
type templateRepoStub struct {
	templates map[string]*Template
}

func newTemplateRepoStub() *templateRepoStub {
	return &templateRepoStub{templates: make(map[string]*Template)}
}

func (s *templateRepoStub) CreateTemplate(_ context.Context, tpl *Template) error {
	for _, existing := range s.templates {
		if existing.BizID == tpl.BizID && existing.EventType == tpl.EventType && existing.ChannelType == tpl.ChannelType {
			return ErrTemplateSpecExists
		}
	}
	cp := *tpl
	s.templates[cp.ID] = &cp
	return nil
}

func (s *templateRepoStub) UpdateTemplate(_ context.Context, id string, upd TemplateUpdate) error {
	tpl, ok := s.templates[id]
	if !ok {
		return ErrTemplateNotFound
	}
	if upd.EventType != nil {
		tpl.EventType = *upd.EventType
	}
	if upd.ChannelType != nil {
		tpl.ChannelType = *upd.ChannelType
	}
	if upd.Subject != nil {
		tpl.Subject = *upd.Subject
	}
	if upd.Body != nil {
		tpl.Body = *upd.Body
	}
	if upd.Enabled != nil {
		tpl.Enabled = *upd.Enabled
	}
	tpl.Version++
	return nil
}

func (s *templateRepoStub) DeleteTemplate(_ context.Context, id string) error {
	if _, ok := s.templates[id]; !ok {
		return ErrTemplateNotFound
	}
	delete(s.templates, id)
	return nil
}

func (s *templateRepoStub) GetTemplate(_ context.Context, id string) (*Template, error) {
	tpl, ok := s.templates[id]
	if !ok {
		return nil, ErrTemplateNotFound
	}
	cp := *tpl
	return &cp, nil
}

func (s *templateRepoStub) ListTemplates(_ context.Context, filter TemplateFilter, page, pageSize int64) ([]*Template, int64, error) {
	var matched []*Template
	for _, tpl := range s.templates {
		if filter.EventType != nil && tpl.EventType != *filter.EventType {
			continue
		}
		if filter.ChannelType != nil && tpl.ChannelType != *filter.ChannelType {
			continue
		}
		if filter.Enabled != nil && tpl.Enabled != *filter.Enabled {
			continue
		}
		cp := *tpl
		matched = append(matched, &cp)
	}
	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

// newAlertTemplate 返回合法告警模板入参。
func newAlertTemplate(eventType, channelType string) *Template {
	return &Template{
		EventType:   eventType,
		ChannelType: channelType,
		Subject:     `[{{.BizID}}] {{.Title}}`,
		Body:        `级别 {{.Severity}}：{{.Title}}（{{.Count}} 条），主机 {{.Payload.host}}。`,
		Enabled:     true,
	}
}

func TestTemplateUsecase_Create(t *testing.T) {
	t.Parallel()
	stub := newTemplateRepoStub()
	uc := NewTemplateUsecase(stub)

	tpl, err := uc.Create(context.Background(), newAlertTemplate("monitor.alert.firing", ChannelTypeDingtalk))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !uuidV7Pattern.MatchString(tpl.ID) {
		t.Fatalf("Create() id = %q, want UUIDv7", tpl.ID)
	}
	if tpl.BizID != DefaultBizID {
		t.Fatalf("Create() bizID = %q, want default %q", tpl.BizID, DefaultBizID)
	}
	if tpl.Version != 1 {
		t.Fatalf("Create() version = %d, want 1", tpl.Version)
	}
	if tpl.CreateTime.IsZero() || tpl.UpdateTime.IsZero() {
		t.Fatal("Create() timestamps not set")
	}
	if !tpl.Enabled {
		t.Fatal("Create() enabled passthrough = false, want true")
	}
}

func TestTemplateUsecase_CreateDuplicateSpec(t *testing.T) {
	t.Parallel()
	stub := newTemplateRepoStub()
	uc := NewTemplateUsecase(stub)
	ctx := context.Background()

	if _, err := uc.Create(ctx, newAlertTemplate("monitor.alert.firing", ChannelTypeDingtalk)); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// 同 (biz,event,channel) 组合重复拒绝。
	_, err := uc.Create(ctx, newAlertTemplate("monitor.alert.firing", ChannelTypeDingtalk))
	if !errors.Is(err, ErrTemplateSpecExists) {
		t.Fatalf("Create(dup spec) error = %v, want ErrTemplateSpecExists", err)
	}
	// 任一维度不同即放行。
	if _, err := uc.Create(ctx, newAlertTemplate("monitor.alert.firing", ChannelTypeEmail)); err != nil {
		t.Fatalf("Create(different channel) error = %v", err)
	}
	if _, err := uc.Create(ctx, newAlertTemplate(TemplateEventTypeAny, ChannelTypeDingtalk)); err != nil {
		t.Fatalf("Create(wildcard event) error = %v", err)
	}
}

func TestTemplateUsecase_CreateSpecInvalid(t *testing.T) {
	t.Parallel()
	stub := newTemplateRepoStub()
	uc := NewTemplateUsecase(stub)

	cases := []struct {
		name, event, channel string
	}{
		{"uppercase event", "Monitor.Alert", ChannelTypeDingtalk},
		{"partial wildcard event", "monitor.*", ChannelTypeDingtalk},
		{"leading dot event", ".alert", ChannelTypeDingtalk},
		{"empty event", "", ChannelTypeDingtalk},
		{"unknown channel", "monitor.alert.firing", "sms"},
		{"empty channel", "monitor.alert.firing", ""},
	}
	for _, tc := range cases {
		tpl := newAlertTemplate(tc.event, tc.channel)
		_, err := uc.Create(context.Background(), tpl)
		if !errors.Is(err, ErrTemplateInvalid) {
			t.Errorf("Create(%s) error = %v, want ErrTemplateInvalid", tc.name, err)
		}
	}
	// 通配双 '*' 合法（兜底模板）。
	if _, err := uc.Create(context.Background(), newAlertTemplate(TemplateEventTypeAny, TemplateChannelAny)); err != nil {
		t.Fatalf("Create(both wildcard) error = %v", err)
	}
}

func TestTemplateUsecase_CreateTemplateTextInvalid(t *testing.T) {
	t.Parallel()
	stub := newTemplateRepoStub()
	uc := NewTemplateUsecase(stub)

	cases := []struct {
		name, subject, body string
	}{
		{"bad syntax subject", `[{{.Title`, ``},
		{"bad syntax body", `ok`, `{{if .Title}`},
		{"unknown variable", `{{.Titel}}`, ``},
		{"unknown variable in body", `ok`, `{{.Payload}} {{.Nope}}`},
		{"unknown function", `{{frobnicate .Title}}`, ``},
		{"sub-template invocation", `{{template "inner"}}`, ``},
		{"unknown var inside if body", `{{if .Title}}{{.Nope}}{{end}}`, ``},
		{"root variable unknown", `{{$.Nope}}`, ``},
		{"root variable unknown inside range", `ok`, `{{range .Samples}}{{$.Nope}}{{end}}`},
	}
	for _, tc := range cases {
		tpl := newAlertTemplate("monitor.alert.firing", ChannelTypeEmail)
		tpl.Subject, tpl.Body = tc.subject, tc.body
		if _, err := uc.Create(context.Background(), tpl); !errors.Is(err, ErrTemplateInvalid) {
			t.Errorf("Create(%s) error = %v, want ErrTemplateInvalid", tc.name, err)
		}
	}
}

func TestTemplateUsecase_TemplateTextValid(t *testing.T) {
	t.Parallel()
	stub := newTemplateRepoStub()
	uc := NewTemplateUsecase(stub)

	// 覆盖全部白名单变量 + 分支/循环/重绑定结构；重绑定域内相对引用放行。
	tpl := newAlertTemplate("monitor.alert.firing", ChannelTypeEmail)
	tpl.Subject = `{{.BizID}} {{.Title}} {{.Severity}} {{.Count}} {{.EventTime}}`
	tpl.Body = `{{.Title}} 详情见 {{.Payload.url}}。` +
		`{{if gt .Severity 2}}P1 紧急{{else}}常规{{end}}` +
		`{{range .Samples}}- {{.metric}} = {{.value}}{{end}}` +
		`{{with .Payload}}主机 {{.host}}{{end}}` +
		`{{range .Samples}}{{.metric}} / {{$.Title}}{{end}}` +
		`{{range .Samples}}{{.metric}}{{else}}{{.Title}}{{end}}`
	out, err := uc.Create(context.Background(), tpl)
	if err != nil {
		t.Fatalf("Create(valid template) error = %v", err)
	}
	if out.ID == "" {
		t.Fatal("Create(valid template) not persisted")
	}
}

func TestTemplateUsecase_Update(t *testing.T) {
	t.Parallel()
	stub := newTemplateRepoStub()
	uc := NewTemplateUsecase(stub)
	ctx := context.Background()

	tpl, err := uc.Create(ctx, newAlertTemplate("monitor.alert.firing", ChannelTypeDingtalk))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// 改 spec + 停用：组合唯一校验通过、version 由仓储自增。
	event := "monitor.alert.resolved"
	enabled := false
	if err := uc.Update(ctx, tpl.ID, TemplateUpdate{EventType: &event, Enabled: &enabled}); err != nil {
		t.Fatalf("Update(spec) error = %v", err)
	}
	got, err := uc.Get(ctx, tpl.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.EventType != event || got.Enabled || got.Version != 2 {
		t.Fatalf("Update(spec) result = %+v", got)
	}

	// 非法新 spec 拒绝。
	bad := "Monitor"
	if err := uc.Update(ctx, tpl.ID, TemplateUpdate{EventType: &bad}); !errors.Is(err, ErrTemplateInvalid) {
		t.Fatalf("Update(bad spec) error = %v, want ErrTemplateInvalid", err)
	}
	// 非法新模板文本拒绝。
	badText := `{{.Titel}}`
	if err := uc.Update(ctx, tpl.ID, TemplateUpdate{Subject: &badText}); !errors.Is(err, ErrTemplateInvalid) {
		t.Fatalf("Update(bad subject) error = %v, want ErrTemplateInvalid", err)
	}
	// 改 body 合法。
	body := `恢复：{{.Title}}`
	if err := uc.Update(ctx, tpl.ID, TemplateUpdate{Body: &body}); err != nil {
		t.Fatalf("Update(body) error = %v", err)
	}
	got, err = uc.Get(ctx, tpl.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Body != body || got.Version != 3 {
		t.Fatalf("Update(body) result = %+v", got)
	}

	// 不存在模板。
	if err := uc.Update(ctx, "missing-id", TemplateUpdate{Body: &body}); !errors.Is(err, ErrTemplateNotFound) {
		t.Fatalf("Update(missing) error = %v, want ErrTemplateNotFound", err)
	}
}

func TestTemplateUsecase_Delete(t *testing.T) {
	t.Parallel()
	stub := newTemplateRepoStub()
	uc := NewTemplateUsecase(stub)
	ctx := context.Background()

	tpl, err := uc.Create(ctx, newAlertTemplate("monitor.alert.firing", ChannelTypeDingtalk))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := uc.Delete(ctx, tpl.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := uc.Delete(ctx, tpl.ID); !errors.Is(err, ErrTemplateNotFound) {
		t.Fatalf("Delete(twice) error = %v, want ErrTemplateNotFound", err)
	}
}

func TestTemplateUsecase_ListPagingNormalize(t *testing.T) {
	t.Parallel()
	stub := newTemplateRepoStub()
	uc := NewTemplateUsecase(stub)
	ctx := context.Background()

	for _, spec := range [][2]string{
		{"monitor.alert.firing", ChannelTypeDingtalk},
		{"monitor.alert.firing", ChannelTypeEmail},
		{TemplateEventTypeAny, TemplateChannelAny},
	} {
		if _, err := uc.Create(ctx, newAlertTemplate(spec[0], spec[1])); err != nil {
			t.Fatalf("Create(%v) error = %v", spec, err)
		}
	}

	// 非法分页参数规整为默认值（page=1/pageSize=20）。
	items, total, err := uc.List(ctx, TemplateFilter{}, 0, 0)
	if err != nil {
		t.Fatalf("List(zero paging) error = %v", err)
	}
	if total != 3 || len(items) != 3 {
		t.Fatalf("List(zero paging) = %d items, total %d, want 3/3", len(items), total)
	}

	// 过滤透传仓储。
	event := "monitor.alert.firing"
	items, total, err = uc.List(ctx, TemplateFilter{EventType: &event}, 1, 20)
	if err != nil {
		t.Fatalf("List(event filter) error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("List(event filter) = %d items, total %d, want 2/2", len(items), total)
	}
}

// TestTrialRenderContextMatchesWhitelist 试渲染样例上下文的字段集 = 渲染白名单
// templateVars（管道注入集合），防止两者漂移。
func TestTrialRenderContextMatchesWhitelist(t *testing.T) {
	t.Parallel()
	rt := reflect.TypeOf(trialRenderContext())
	if rt.NumField() != len(templateVars) {
		t.Fatalf("TrialRenderContext has %d fields, whitelist has %d", rt.NumField(), len(templateVars))
	}
	for i := 0; i < rt.NumField(); i++ {
		if _, ok := templateVars[rt.Field(i).Name]; !ok {
			t.Errorf("field %s not in templateVars", rt.Field(i).Name)
		}
	}
	// 载荷/样本字段宽松（运行期语义），顶层字段严格。
	if err := validateTemplateText("body", `{{.Payload.anything}} {{range .Samples}}{{.whatever}}{{end}}`); err != nil {
		t.Fatalf("payload/sample fields must be lenient: %v", err)
	}
	if err := validateTemplateText("body", `{{.Payload}}{{.Count}}{{.Nope}}`); err == nil {
		t.Fatal("unknown top-level field must be rejected")
	}
}
