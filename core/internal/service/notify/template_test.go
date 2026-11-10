package service

import (
	"testing"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

func newTemplateService(t *testing.T) (*NotifyTemplateService, *testfake.Store) {
	t.Helper()
	store := testfake.NewStore()
	return NewNotifyTemplateService(biz.NewTemplateUsecase(store), testfake.Unrestricted()), store
}

// newTemplateCreate 返回合法告警模板创建请求。
func newTemplateCreate(eventType, channelType string) *notifyv1.CreateTemplateRequest {
	return &notifyv1.CreateTemplateRequest{
		EventType:   eventType,
		ChannelType: channelType,
		Subject:     `[{{.BizID}}] {{.Title}}`,
		Body:        `级别 {{.Severity}}：{{.Title}}（{{.Count}} 条）`,
	}
}

func TestNotifyTemplateService_CreateRoundtrip(t *testing.T) {
	t.Parallel()
	svc, _ := newTemplateService(t)
	ctx := testCtx()

	resp, err := svc.CreateTemplate(ctx, newTemplateCreate("monitor.alert.firing", "dingtalk"))
	if err != nil {
		t.Fatalf("CreateTemplate() error = %v", err)
	}
	tpl := resp.GetTemplate()
	if tpl.GetId() == "" || tpl.GetBizId() != biz.DefaultBizID {
		t.Fatalf("CreateTemplate() = %+v, want id and default biz", tpl)
	}
	if !tpl.GetEnabled() || tpl.GetVersion() != 1 {
		t.Fatalf("CreateTemplate() enabled/version = %t/%d, want true/1", tpl.GetEnabled(), tpl.GetVersion())
	}
	if tpl.GetSubject() != `[{{.BizID}}] {{.Title}}` {
		t.Fatalf("CreateTemplate() subject = %q", tpl.GetSubject())
	}

	// 通配模板（兜底）：双 '*'。
	anyResp, err := svc.CreateTemplate(ctx, newTemplateCreate("*", "*"))
	if err != nil {
		t.Fatalf("CreateTemplate(wildcard) error = %v", err)
	}
	if anyResp.GetTemplate().GetEventType() != "*" || anyResp.GetTemplate().GetChannelType() != "*" {
		t.Fatalf("CreateTemplate(wildcard) = %+v", anyResp.GetTemplate())
	}
}

func TestNotifyTemplateService_CreateErrors(t *testing.T) {
	t.Parallel()
	svc, _ := newTemplateService(t)
	ctx := testCtx()

	// 组合冲突。
	if _, err := svc.CreateTemplate(ctx, newTemplateCreate("monitor.alert.firing", "dingtalk")); err != nil {
		t.Fatalf("CreateTemplate() error = %v", err)
	}
	_, err := svc.CreateTemplate(ctx, newTemplateCreate("monitor.alert.firing", "dingtalk"))
	if !errors.Is(err, biz.ErrTemplateSpecExists) {
		t.Fatalf("CreateTemplate(dup spec) error = %v, want ErrTemplateSpecExists", err)
	}

	// 白名单外变量。
	bad := newTemplateCreate("monitor.alert.resolved", "dingtalk")
	bad.Body = "{{.Titel}}"
	if _, err := svc.CreateTemplate(ctx, bad); !errors.Is(err, biz.ErrTemplateInvalid) {
		t.Fatalf("CreateTemplate(bad var) error = %v, want ErrTemplateInvalid", err)
	}

	// 非法渠道类型值。
	worse := newTemplateCreate("monitor.alert.resolved", "sms")
	if _, err := svc.CreateTemplate(ctx, worse); !errors.Is(err, biz.ErrTemplateInvalid) {
		t.Fatalf("CreateTemplate(bad channel) error = %v, want ErrTemplateInvalid", err)
	}
}

func TestNotifyTemplateService_UpdateTemplate(t *testing.T) {
	t.Parallel()
	svc, _ := newTemplateService(t)
	ctx := testCtx()

	created, err := svc.CreateTemplate(ctx, newTemplateCreate("monitor.alert.firing", "dingtalk"))
	if err != nil {
		t.Fatalf("CreateTemplate() error = %v", err)
	}
	id := created.GetTemplate().GetId()

	// 改 spec + 换正文 + 停用；version 自增对读路径可见。
	event, enabled := "monitor.alert.resolved", false
	body := `恢复：{{.Title}}`
	if _, err := svc.UpdateTemplate(ctx, &notifyv1.UpdateTemplateRequest{
		Id:        id,
		EventType: &event,
		Enabled:   &enabled,
		Body:      &body,
	}); err != nil {
		t.Fatalf("UpdateTemplate() error = %v", err)
	}
	got, err := svc.GetTemplate(ctx, &notifyv1.GetTemplateRequest{Id: id})
	if err != nil {
		t.Fatalf("GetTemplate() error = %v", err)
	}
	if got.GetTemplate().GetEventType() != event || got.GetTemplate().GetEnabled() {
		t.Fatalf("UpdateTemplate() result = %+v", got.GetTemplate())
	}
	if got.GetTemplate().GetVersion() != 2 || got.GetTemplate().GetBody() != body {
		t.Fatalf("UpdateTemplate() version/body = %d/%q, want 2/%q", got.GetTemplate().GetVersion(), got.GetTemplate().GetBody(), body)
	}

	// 非法模板文本拒绝且不落库。
	badText := `{{.Nope}}`
	if _, err := svc.UpdateTemplate(ctx, &notifyv1.UpdateTemplateRequest{Id: id, Subject: &badText}); !errors.Is(err, biz.ErrTemplateInvalid) {
		t.Fatalf("UpdateTemplate(bad text) error = %v, want ErrTemplateInvalid", err)
	}

	// 不存在模板。
	if _, err := svc.UpdateTemplate(ctx, &notifyv1.UpdateTemplateRequest{Id: missingChannelID, Body: &body}); !errors.Is(err, biz.ErrTemplateNotFound) {
		t.Fatalf("UpdateTemplate(missing) error = %v, want ErrTemplateNotFound", err)
	}
}

func TestNotifyTemplateService_DeleteTemplate(t *testing.T) {
	t.Parallel()
	svc, _ := newTemplateService(t)
	ctx := testCtx()

	created, err := svc.CreateTemplate(ctx, newTemplateCreate("monitor.alert.firing", "dingtalk"))
	if err != nil {
		t.Fatalf("CreateTemplate() error = %v", err)
	}
	id := created.GetTemplate().GetId()

	if _, err := svc.DeleteTemplate(ctx, &notifyv1.DeleteTemplateRequest{Id: id}); err != nil {
		t.Fatalf("DeleteTemplate() error = %v", err)
	}
	if _, err := svc.GetTemplate(ctx, &notifyv1.GetTemplateRequest{Id: id}); !errors.Is(err, biz.ErrTemplateNotFound) {
		t.Fatalf("GetTemplate(deleted) error = %v, want ErrTemplateNotFound", err)
	}
	// 软删后组合可复用。
	if _, err := svc.CreateTemplate(ctx, newTemplateCreate("monitor.alert.firing", "dingtalk")); err != nil {
		t.Fatalf("CreateTemplate(reuse spec) error = %v", err)
	}
}

func TestNotifyTemplateService_ListTemplates(t *testing.T) {
	t.Parallel()
	svc, _ := newTemplateService(t)
	ctx := testCtx()

	seed := [][2]string{
		{"monitor.alert.firing", "dingtalk"},
		{"monitor.alert.firing", "email"},
		{"*", "*"},
	}
	for _, spec := range seed {
		if _, err := svc.CreateTemplate(ctx, newTemplateCreate(spec[0], spec[1])); err != nil {
			t.Fatalf("CreateTemplate(%v) error = %v", spec, err)
		}
	}

	list, err := svc.ListTemplates(ctx, &notifyv1.ListTemplatesRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListTemplates() error = %v", err)
	}
	if list.GetTotal() != 3 || len(list.GetItems()) != 3 {
		t.Fatalf("ListTemplates() = %d items, total %d, want 3/3", len(list.GetItems()), list.GetTotal())
	}

	// 事件类型精确过滤（含 '*'）。
	event := "monitor.alert.firing"
	list, err = svc.ListTemplates(ctx, &notifyv1.ListTemplatesRequest{Page: 1, PageSize: 10, EventType: &event})
	if err != nil {
		t.Fatalf("ListTemplates(event) error = %v", err)
	}
	if list.GetTotal() != 2 {
		t.Fatalf("ListTemplates(event) total = %d, want 2", list.GetTotal())
	}
	wildcard := "*"
	list, err = svc.ListTemplates(ctx, &notifyv1.ListTemplatesRequest{Page: 1, PageSize: 10, EventType: &wildcard})
	if err != nil {
		t.Fatalf("ListTemplates(wildcard) error = %v", err)
	}
	if list.GetTotal() != 1 || list.GetItems()[0].GetChannelType() != "*" {
		t.Fatalf("ListTemplates(wildcard) = %+v", list.GetItems())
	}

	// 关键字模糊匹配 event_type/subject（不含 body）。
	kw := "monitor"
	list, err = svc.ListTemplates(ctx, &notifyv1.ListTemplatesRequest{Page: 1, PageSize: 10, Keyword: &kw})
	if err != nil {
		t.Fatalf("ListTemplates(keyword) error = %v", err)
	}
	if list.GetTotal() != 2 {
		t.Fatalf("ListTemplates(keyword event) total = %d, want 2", list.GetTotal())
	}
	kw = "BizID"
	list, err = svc.ListTemplates(ctx, &notifyv1.ListTemplatesRequest{Page: 1, PageSize: 10, Keyword: &kw})
	if err != nil {
		t.Fatalf("ListTemplates(keyword subject) error = %v", err)
	}
	if list.GetTotal() != 3 {
		t.Fatalf("ListTemplates(keyword subject) total = %d, want 3", list.GetTotal())
	}
}
