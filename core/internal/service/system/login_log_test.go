package service

import (
	"context"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// newLoginLogService 构造内存仓储链的 LoginLogService 并预置种子日志。
func newLoginLogService(t *testing.T, seeds ...*biz.LoginLog) (*LoginLogService, *testfake.Store) {
	t.Helper()
	store := testfake.NewStore()
	for _, l := range seeds {
		if err := store.Create(context.Background(), l); err != nil {
			t.Fatalf("seed login log %+v: %v", l, err)
		}
	}
	return NewLoginLogService(biz.NewLoginLogUsecase(store)), store
}

// seedLoginLog 构造一条种子日志（显式时间保证排序稳定）。
func seedLoginLog(id, username string, result int32, at time.Time) *biz.LoginLog {
	return &biz.LoginLog{
		ID: id, UserID: "user-" + username, Username: username, Result: result,
		IP: "10.0.0.8", UserAgent: "agent/1.0", CreateTime: at,
	}
}

// TestLoginLogService_ListLoginLogs 出参契约：过滤（用户名模糊/结果/时间范围，
// 含端点）、排序（create_time DESC, id DESC）与分页 total。
func TestLoginLogService_ListLoginLogs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	svc, _ := newLoginLogService(t,
		seedLoginLog("l1", "alice", biz.LoginResultSuccess, base),
		seedLoginLog("l2", "alice", biz.LoginResultWrongPassword, base.Add(time.Minute)),
		seedLoginLog("l3", "bob", biz.LoginResultUserNotFound, base.Add(2*time.Minute)),
		seedLoginLog("l4", "bob", biz.LoginResultSuccess, base.Add(3*time.Minute)),
	)

	t.Run("no filter, sorted desc", func(t *testing.T) {
		t.Parallel()
		resp, err := svc.ListLoginLogs(ctx, &v1.ListLoginLogsRequest{Page: 1, PageSize: 10})
		if err != nil {
			t.Fatalf("ListLoginLogs() error = %v", err)
		}
		if resp.GetTotal() != 4 || len(resp.GetItems()) != 4 {
			t.Fatalf("ListLoginLogs() = (%d total, %d items), want (4, 4)", resp.GetTotal(), len(resp.GetItems()))
		}
		if resp.GetItems()[0].GetId() != "l4" || resp.GetItems()[3].GetId() != "l1" {
			t.Fatalf("items order = %v, want l4..l1 desc", resp.GetItems())
		}
		item := resp.GetItems()[0]
		if item.GetUserId() == "" || item.GetIp() != "10.0.0.8" ||
			item.GetUserAgent() != "agent/1.0" || item.GetCreateTime() == nil {
			t.Fatalf("item field mapping incomplete: %+v", item)
		}
	})

	t.Run("username filter", func(t *testing.T) {
		t.Parallel()
		resp, err := svc.ListLoginLogs(ctx, &v1.ListLoginLogsRequest{
			Page: 1, PageSize: 10,
			Username: ptrOf("ALI"),
		})
		if err != nil {
			t.Fatalf("ListLoginLogs() error = %v", err)
		}
		if resp.GetTotal() != 2 {
			t.Fatalf("username filter total = %d, want 2", resp.GetTotal())
		}
	})

	t.Run("result filter", func(t *testing.T) {
		t.Parallel()
		resp, err := svc.ListLoginLogs(ctx, &v1.ListLoginLogsRequest{
			Page: 1, PageSize: 10,
			Result: ptrOf(biz.LoginResultSuccess),
		})
		if err != nil {
			t.Fatalf("ListLoginLogs() error = %v", err)
		}
		if resp.GetTotal() != 2 {
			t.Fatalf("result filter total = %d, want 2", resp.GetTotal())
		}
	})

	t.Run("time range inclusive", func(t *testing.T) {
		t.Parallel()
		resp, err := svc.ListLoginLogs(ctx, &v1.ListLoginLogsRequest{
			Page: 1, PageSize: 10,
			StartTime: timestamppb.New(base.Add(time.Minute)),
			EndTime:   timestamppb.New(base.Add(2 * time.Minute)),
		})
		if err != nil {
			t.Fatalf("ListLoginLogs() error = %v", err)
		}
		if resp.GetTotal() != 2 || resp.GetItems()[0].GetId() != "l3" || resp.GetItems()[1].GetId() != "l2" {
			t.Fatalf("time range items = %v, want l3,l2 (inclusive)", resp.GetItems())
		}
	})

	t.Run("pagination", func(t *testing.T) {
		t.Parallel()
		resp, err := svc.ListLoginLogs(ctx, &v1.ListLoginLogsRequest{Page: 2, PageSize: 3})
		if err != nil {
			t.Fatalf("ListLoginLogs() error = %v", err)
		}
		if resp.GetTotal() != 4 || len(resp.GetItems()) != 1 || resp.GetItems()[0].GetId() != "l1" {
			t.Fatalf("page 2 items = %v, want only l1", resp.GetItems())
		}
	})
}

// ptrOf 返回局部值指针（proto optional 字段装配）。
func ptrOf[T any](v T) *T { return &v }
