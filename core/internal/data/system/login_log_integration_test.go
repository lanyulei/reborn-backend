//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
)

// newLoginLog 构造固定时间的登录日志 DO；userID 传空串表示 NULL 语义
// （用户不存在的失败尝试）。
func newLoginLog(id, userID, username string, result int32, at time.Time) *biz.LoginLog {
	return &biz.LoginLog{
		ID: id, UserID: userID, Username: username, Result: result,
		IP: "10.0.0.8", UserAgent: "integration-agent/1.0", CreateTime: at,
	}
}

// TestIntegration_LoginLogRepo 登录日志仓储契约：Create 落库（空 user_id 归一
// NULL、回读归一空串）与 ListLoginLogs 的过滤（用户名模糊/结果/时间范围含端点）、
// 排序（create_time DESC, id DESC）与分页 total。
func TestIntegration_LoginLogRepo(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewLoginLogRepo(d)
	ctx := context.Background()

	base := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	seeds := []*biz.LoginLog{
		newLoginLog("018f3e1a-0009-7000-8000-000000000001", "018f3e1a-0002-7000-8000-000000000001", "alice", biz.LoginResultSuccess, base),
		newLoginLog("018f3e1a-0009-7000-8000-000000000002", "018f3e1a-0002-7000-8000-000000000001", "alice", biz.LoginResultWrongPassword, base.Add(time.Minute)),
		// 用户不存在：UserID 空串落 NULL。
		newLoginLog("018f3e1a-0009-7000-8000-000000000003", "", "nobody", biz.LoginResultUserNotFound, base.Add(2*time.Minute)),
		newLoginLog("018f3e1a-0009-7000-8000-000000000004", "018f3e1a-0002-7000-8000-000000000002", "bob", biz.LoginResultAccountDisabled, base.Add(3*time.Minute)),
	}
	for _, l := range seeds {
		if err := repo.Create(ctx, l); err != nil {
			t.Fatalf("Create(%s) error = %v", l.ID, err)
		}
	}

	t.Run("create null user id round-trip", func(t *testing.T) {
		logs, total, err := repo.ListLoginLogs(ctx, biz.LoginLogFilter{Username: "nobody"}, 1, 10)
		if err != nil {
			t.Fatalf("ListLoginLogs() error = %v", err)
		}
		if total != 1 || len(logs) != 1 {
			t.Fatalf("ListLoginLogs(nobody) = (%d, %d), want (1, 1)", total, len(logs))
		}
		got := logs[0]
		if got.UserID != "" {
			t.Errorf("null user id read back as %q, want empty", got.UserID)
		}
		if got.Result != biz.LoginResultUserNotFound || got.IP != "10.0.0.8" ||
			got.UserAgent != "integration-agent/1.0" || got.CreateTime.IsZero() {
			t.Errorf("round-trip field mismatch: %+v", got)
		}
	})

	t.Run("list sorted desc with pagination", func(t *testing.T) {
		logs, total, err := repo.ListLoginLogs(ctx, biz.LoginLogFilter{}, 1, 2)
		if err != nil {
			t.Fatalf("ListLoginLogs() error = %v", err)
		}
		if total != 4 || len(logs) != 2 {
			t.Fatalf("page 1 = (%d total, %d items), want (4, 2)", total, len(logs))
		}
		if logs[0].ID != seeds[3].ID || logs[1].ID != seeds[2].ID {
			t.Fatalf("page 1 order = [%s, %s], want newest first", logs[0].ID, logs[1].ID)
		}
		page2, _, err := repo.ListLoginLogs(ctx, biz.LoginLogFilter{}, 2, 2)
		if err != nil {
			t.Fatalf("ListLoginLogs(page 2) error = %v", err)
		}
		if len(page2) != 2 || page2[0].ID != seeds[1].ID || page2[1].ID != seeds[0].ID {
			t.Fatalf("page 2 order = %v", page2)
		}
	})

	t.Run("filters", func(t *testing.T) {
		result := int32(biz.LoginResultWrongPassword)
		_, total, err := repo.ListLoginLogs(ctx, biz.LoginLogFilter{Result: &result}, 1, 10)
		if err != nil {
			t.Fatalf("ListLoginLogs(result) error = %v", err)
		}
		if total != 1 {
			t.Errorf("result filter total = %d, want 1", total)
		}

		_, total, err = repo.ListLoginLogs(ctx, biz.LoginLogFilter{Username: "ali"}, 1, 10)
		if err != nil {
			t.Fatalf("ListLoginLogs(username) error = %v", err)
		}
		if total != 2 {
			t.Errorf("username filter total = %d, want 2", total)
		}

		start, end := base.Add(time.Minute), base.Add(2*time.Minute)
		_, total, err = repo.ListLoginLogs(ctx, biz.LoginLogFilter{StartTime: &start, EndTime: &end}, 1, 10)
		if err != nil {
			t.Fatalf("ListLoginLogs(time range) error = %v", err)
		}
		if total != 2 {
			t.Errorf("time range (inclusive) total = %d, want 2", total)
		}
	})
}
