//go:build integration

package data

import (
	"context"
	"testing"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	datasystem "github.com/lanyulei/reborn/backend/core/internal/data/system"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/go-kratos/kratos/v3/errors"
)

// TestIntegration_NotificationWithinTx 跨域同事务直插（设计 §11 核心保证）：
// 业务写（ReplaceForRole）与通知事件写（NotifyEvent）经同一 txrun 事务
// 原子提交——任一失败整体回滚，授权变更与通知永不脱节。全链路真实 PG
// （TxManager + system/notify 两域仓储 + NotificationUsecase）。
func TestIntegration_NotificationWithinTx(t *testing.T) {
	d := newIntegrationNotifyData(t)
	manager := basedata.NewTxManager(d)
	permRepo := datasystem.NewPermissionRepo(d)
	eventRepo := NewEventRepo(d)
	notifyUC := biznotify.NewNotificationUsecase(eventRepo, nil)
	ctx := context.Background()

	// 种子角色（本测试同时写 system 域表，先清场；外键依赖须单条语句
	// 连带截断，清单对齐 system 域集成测试）。
	if _, err := d.Pool.Exec(ctx,
		`TRUNCATE TABLE system_permission, system_grant, system_user_role, system_role`); err != nil {
		t.Fatalf("truncate system tables: %v", err)
	}
	const roleID = "018f3e1a-0500-7000-8000-0000000000e1"
	if _, err := d.Pool.Exec(ctx,
		`INSERT INTO system_role (id, name, code) VALUES ($1, '运维', 'ops')`, roleID); err != nil {
		t.Fatalf("seed role: %v", err)
	}

	// 成功路径：业务写 + 事件写同事务提交。
	grants := bizsystem.GrantSet{
		Menus: []string{"system.user"},
		Apis:  []string{"ffff0000000000000000000000000001"},
	}
	err := manager.WithinTx(ctx, func(tx txrun.Tx) error {
		if err := permRepo.ReplaceForRole(ctx, tx, roleID, grants, grants.Apis); err != nil {
			return err
		}
		_, err := notifyUC.NotifyEvent(ctx, tx, biznotify.Event{
			BizID:     "core",
			EventType: "permission.change",
			Severity:  2,
			Title:     "角色权限变更",
			Payload:   []byte(`{"role_id":"` + roleID + `","operator":"u-9"}`),
			GroupKey:  "permission.change." + roleID,
		})
		return err
	})
	if err != nil {
		t.Fatalf("WithinTx(commit path) error = %v", err)
	}

	// 两域表均可见：授权物化 + 事件落库（internal / received / pending）。
	var grantRows, permRows int
	if err := d.Pool.QueryRow(ctx,
		"SELECT count(*) FROM system_grant WHERE role_id = $1", roleID).Scan(&grantRows); err != nil {
		t.Fatalf("count grants: %v", err)
	}
	if err := d.Pool.QueryRow(ctx,
		"SELECT count(*) FROM system_permission WHERE role_id = $1", roleID).Scan(&permRows); err != nil {
		t.Fatalf("count permissions: %v", err)
	}
	if grantRows != 2 || permRows != 1 {
		t.Fatalf("grants/permissions = %d/%d, want 2/1", grantRows, permRows)
	}
	events, total, err := eventRepo.ListEvents(ctx, biznotify.EventFilter{EventType: "permission.change"}, 1, 10)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if total != 1 || len(events) != 1 {
		t.Fatalf("ListEvents() = %d items, want 1", total)
	}
	if events[0].Source != biznotify.EventSourceInternal || events[0].Status != biznotify.EventStatusReceived ||
		events[0].PublishState != 0 {
		t.Fatalf("event pipeline fields = %+v", events[0])
	}
	if events[0].GroupKey != "permission.change."+roleID || events[0].BizID != "core" {
		t.Fatalf("event biz/group = %q/%q", events[0].BizID, events[0].GroupKey)
	}

	// 失败路径：事件校验失败（NotifyEvent 拒绝）→ 整个事务回滚 → 授权
	// 变更同样不落库（原子性——业务与通知永不脱节的正向证明）。
	next := bizsystem.GrantSet{Menus: []string{"system.role"}}
	err = manager.WithinTx(ctx, func(tx txrun.Tx) error {
		if err := permRepo.ReplaceForRole(ctx, tx, roleID, next, nil); err != nil {
			return err
		}
		// 非法事件（通配符类型）触发校验失败。
		_, err := notifyUC.NotifyEvent(ctx, tx, biznotify.Event{EventType: "monitor.*", Title: "t"})
		return err
	})
	if !errors.Is(err, biznotify.ErrNotificationInvalid) {
		t.Fatalf("WithinTx(rollback path) error = %v, want ErrNotificationInvalid", err)
	}
	if err := d.Pool.QueryRow(ctx,
		"SELECT count(*) FROM system_grant WHERE role_id = $1", roleID).Scan(&grantRows); err != nil {
		t.Fatalf("count grants after rollback: %v", err)
	}
	if grantRows != 2 {
		t.Fatalf("grants after rollback = %d, want 2（业务写随事件失败整体回滚）", grantRows)
	}
	if _, total, err = eventRepo.ListEvents(ctx, biznotify.EventFilter{EventType: "monitor.*"}, 1, 10); err != nil || total != 0 {
		t.Fatalf("events after rollback = %d (%v), want 0（失败事件不落库）", total, err)
	}
}
