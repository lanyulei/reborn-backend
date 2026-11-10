//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/go-kratos/kratos/v3/errors"
)

// newTemplateDO 返回带固定 ID 与审计时间的模板 DO（Version 起始 1，由用例维护）。
func newTemplateDO(id, eventType, channelType string) *biz.Template {
	return &biz.Template{
		ID:          id,
		BizID:       biz.DefaultBizID,
		EventType:   eventType,
		ChannelType: channelType,
		Subject:     `[{{.BizID}}] {{.Title}}`,
		Body:        `级别 {{.Severity}}：{{.Title}}（{{.Count}} 条）`,
		Version:     1,
		Enabled:     true,
		CreateTime:  time.Now().UTC().Truncate(time.Microsecond),
		UpdateTime:  time.Now().UTC().Truncate(time.Microsecond),
	}
}

// templateCfgVersion 读回 notify_template 的配置版本号。
func templateCfgVersion(t *testing.T, d *basedata.Data) int64 {
	t.Helper()
	var version int64
	if err := d.Pool.QueryRow(context.Background(),
		"SELECT version FROM notify_cfg_version WHERE table_name = 'notify_template'").Scan(&version); err != nil {
		t.Fatalf("read template cfg version: %v", err)
	}
	return version
}

func TestIntegration_TemplateCRUD(t *testing.T) {
	d := newIntegrationNotifyData(t)
	repo := NewTemplateRepo(d)
	ctx := context.Background()

	firing := newTemplateDO("018f3e1a-0201-7000-8000-000000000001", "monitor.alert.firing", biz.ChannelTypeDingtalk)
	if err := repo.CreateTemplate(ctx, firing); err != nil {
		t.Fatalf("CreateTemplate() error = %v", err)
	}
	if v := templateCfgVersion(t, d); v < 1 {
		t.Fatalf("cfg version after create = %d, want >= 1", v)
	}

	// 组合冲突（部分唯一索引 uk_notify_template_spec）映射哨兵错误。
	dup := newTemplateDO("018f3e1a-0201-7000-8000-000000000002", "monitor.alert.firing", biz.ChannelTypeDingtalk)
	if err := repo.CreateTemplate(ctx, dup); !errors.Is(err, biz.ErrTemplateSpecExists) {
		t.Fatalf("CreateTemplate(dup spec) error = %v, want ErrTemplateSpecExists", err)
	}
	// 任一维度不同放行；不同 biz 同组合放行（biz 隔离）。
	if err := repo.CreateTemplate(ctx, newTemplateDO("018f3e1a-0201-7000-8000-000000000003", "monitor.alert.firing", biz.ChannelTypeEmail)); err != nil {
		t.Fatalf("CreateTemplate(other channel) error = %v", err)
	}
	wildcard := newTemplateDO("018f3e1a-0201-7000-8000-000000000004", biz.TemplateEventTypeAny, biz.TemplateChannelAny)
	wildcard.BizID = "monitor"
	if err := repo.CreateTemplate(ctx, wildcard); err != nil {
		t.Fatalf("CreateTemplate(other biz wildcard) error = %v", err)
	}

	// 读路径回填全字段。
	got, err := repo.GetTemplate(ctx, firing.ID)
	if err != nil {
		t.Fatalf("GetTemplate() error = %v", err)
	}
	if got.BizID != biz.DefaultBizID || got.EventType != "monitor.alert.firing" ||
		got.ChannelType != biz.ChannelTypeDingtalk || got.Version != 1 || !got.Enabled {
		t.Fatalf("GetTemplate() = %+v", got)
	}
	if got.Subject != firing.Subject || got.Body != firing.Body {
		t.Fatalf("GetTemplate() subject/body = %q/%q", got.Subject, got.Body)
	}
	if got.CreateTime.IsZero() || got.UpdateTime.IsZero() {
		t.Fatal("GetTemplate() timestamps zero")
	}
	if _, err := repo.GetTemplate(ctx, missingChannelID); !errors.Is(err, biz.ErrTemplateNotFound) {
		t.Fatalf("GetTemplate(missing) error = %v, want ErrTemplateNotFound", err)
	}

	// 更新：部分字段 + version 自增；每次写同事务 bump cfg 版本。
	vBefore := templateCfgVersion(t, d)
	body, enabled := `恢复：{{.Title}}`, false
	if err := repo.UpdateTemplate(ctx, firing.ID, biz.TemplateUpdate{Body: &body, Enabled: &enabled}); err != nil {
		t.Fatalf("UpdateTemplate() error = %v", err)
	}
	if vAfter := templateCfgVersion(t, d); vAfter != vBefore+1 {
		t.Fatalf("cfg version after update = %d, want %d (same-tx bump)", vAfter, vBefore+1)
	}
	got, err = repo.GetTemplate(ctx, firing.ID)
	if err != nil {
		t.Fatalf("GetTemplate() error = %v", err)
	}
	if got.Body != body || got.Enabled || got.Version != 2 {
		t.Fatalf("UpdateTemplate() result = %+v", got)
	}
	if got.Subject != firing.Subject {
		t.Fatalf("UpdateTemplate() must keep nil fields, subject = %q", got.Subject)
	}

	// spec 更新撞组合 → 23505 映射。
	event := "monitor.alert.firing"
	if err := repo.UpdateTemplate(ctx, firing.ID, biz.TemplateUpdate{EventType: &event}); err != nil {
		t.Fatalf("UpdateTemplate(same event) error = %v", err)
	}
	channel := biz.ChannelTypeEmail
	if err := repo.UpdateTemplate(ctx, firing.ID, biz.TemplateUpdate{ChannelType: &channel}); !errors.Is(err, biz.ErrTemplateSpecExists) {
		t.Fatalf("UpdateTemplate(conflict spec) error = %v, want ErrTemplateSpecExists", err)
	}
	if err := repo.UpdateTemplate(ctx, missingChannelID, biz.TemplateUpdate{Body: &body}); !errors.Is(err, biz.ErrTemplateNotFound) {
		t.Fatalf("UpdateTemplate(missing) error = %v, want ErrTemplateNotFound", err)
	}

	// 列表：event_type 精确 / keyword（event|channel|subject 模糊）/ enabled 过滤与分页。
	items, total, err := repo.ListTemplates(ctx, biz.TemplateFilter{}, 1, 20)
	if err != nil {
		t.Fatalf("ListTemplates() error = %v", err)
	}
	if total != 3 || len(items) != 3 {
		t.Fatalf("ListTemplates() = %d items, total %d, want 3/3", len(items), total)
	}
	items, total, err = repo.ListTemplates(ctx, biz.TemplateFilter{EventType: &event}, 1, 20)
	if err != nil {
		t.Fatalf("ListTemplates(event) error = %v", err)
	}
	// firing-钉钉（已改回 firing）与 firing-邮件。
	if total != 2 || len(items) != 2 {
		t.Fatalf("ListTemplates(event) = %d items, total %d, want 2/2", len(items), total)
	}
	anyEvent := biz.TemplateEventTypeAny
	items, total, err = repo.ListTemplates(ctx, biz.TemplateFilter{EventType: &anyEvent}, 1, 20)
	if err != nil {
		t.Fatalf("ListTemplates(wildcard) error = %v", err)
	}
	if total != 1 || items[0].BizID != "monitor" {
		t.Fatalf("ListTemplates(wildcard) = %d %+v", total, items)
	}
	items, total, err = repo.ListTemplates(ctx, biz.TemplateFilter{Keyword: "恢复"}, 1, 20)
	if err != nil {
		t.Fatalf("ListTemplates(keyword body, not searchable) error = %v", err)
	}
	if total != 0 {
		t.Fatalf("ListTemplates(keyword body) total = %d, want 0 (body 不在检索列)", total)
	}
	items, total, err = repo.ListTemplates(ctx, biz.TemplateFilter{Keyword: "BizID"}, 1, 20)
	if err != nil {
		t.Fatalf("ListTemplates(keyword subject) error = %v", err)
	}
	if total != 3 {
		t.Fatalf("ListTemplates(keyword subject) total = %d, want 3", total)
	}
	onlyEnabled := true
	items, total, err = repo.ListTemplates(ctx, biz.TemplateFilter{Enabled: &onlyEnabled}, 1, 20)
	if err != nil {
		t.Fatalf("ListTemplates(enabled) error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("ListTemplates(enabled) = %d items, total %d, want 2/2", len(items), total)
	}
	items, total, err = repo.ListTemplates(ctx, biz.TemplateFilter{}, 2, 2)
	if err != nil {
		t.Fatalf("ListTemplates(paged) error = %v", err)
	}
	if total != 3 || len(items) != 1 {
		t.Fatalf("ListTemplates(paged) = %d items, total %d, want 1/3", len(items), total)
	}

	// 软删：读不可见、组合可复用、版本继续 bump。
	vBefore = templateCfgVersion(t, d)
	if err := repo.DeleteTemplate(ctx, firing.ID); err != nil {
		t.Fatalf("DeleteTemplate() error = %v", err)
	}
	if vAfter := templateCfgVersion(t, d); vAfter != vBefore+1 {
		t.Fatalf("cfg version after delete = %d, want %d", vAfter, vBefore+1)
	}
	if _, err := repo.GetTemplate(ctx, firing.ID); !errors.Is(err, biz.ErrTemplateNotFound) {
		t.Fatalf("GetTemplate(deleted) error = %v, want ErrTemplateNotFound", err)
	}
	if err := repo.DeleteTemplate(ctx, firing.ID); !errors.Is(err, biz.ErrTemplateNotFound) {
		t.Fatalf("DeleteTemplate(twice) error = %v, want ErrTemplateNotFound", err)
	}
	// 复用被软删行的组合；新行 version 重新从入参起算（用例层为 1）。
	reuse := newTemplateDO("018f3e1a-0201-7000-8000-000000000005", "monitor.alert.firing", biz.ChannelTypeDingtalk)
	if err := repo.CreateTemplate(ctx, reuse); err != nil {
		t.Fatalf("CreateTemplate(reuse spec) error = %v", err)
	}
	got, err = repo.GetTemplate(ctx, reuse.ID)
	if err != nil {
		t.Fatalf("GetTemplate(reuse) error = %v", err)
	}
	if got.Version != 1 {
		t.Fatalf("GetTemplate(reuse) version = %d, want 1", got.Version)
	}
}
