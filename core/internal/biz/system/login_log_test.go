package biz

import (
	"context"
	"testing"
)

// loginLogReaderStub 记录 List 调用参数的读仓储 stub。
type loginLogReaderStub struct {
	filter   LoginLogFilter
	page     int64
	pageSize int64
	logs     []*LoginLog
	total    int64
}

func (s *loginLogReaderStub) ListLoginLogs(_ context.Context, filter LoginLogFilter, page, pageSize int64) ([]*LoginLog, int64, error) {
	s.filter, s.page, s.pageSize = filter, page, pageSize
	return s.logs, s.total, nil
}

// TestLoginLogUsecase_List 分页规整后委托仓储：越界值收敛到 [1,100]，
// filter 原样透传。
func TestLoginLogUsecase_List(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stub := &loginLogReaderStub{
		logs:  []*LoginLog{{ID: "l1", Username: "alice", Result: LoginResultSuccess}},
		total: 1,
	}
	uc := NewLoginLogUsecase(stub)

	result := int32(LoginResultWrongPassword)
	filter := LoginLogFilter{Username: "ali", Result: &result}

	cases := []struct {
		name             string
		page, pageSize   int64
		wantPage, wantPS int64
	}{
		{"as-is", 2, 20, 2, 20},
		{"page floor", 0, 20, 1, 20},
		{"size floor", 1, 0, 1, defaultPageSize},
		{"size ceiling", 1, 1000, 1, maxPageSize},
	}
	for _, tc := range cases {
		logs, total, err := uc.List(ctx, filter, tc.page, tc.pageSize)
		if err != nil {
			t.Fatalf("%s: List() error = %v", tc.name, err)
		}
		if stub.page != tc.wantPage || stub.pageSize != tc.wantPS {
			t.Errorf("%s: repo got (page=%d, size=%d), want (%d, %d)",
				tc.name, stub.page, stub.pageSize, tc.wantPage, tc.wantPS)
		}
		if stub.filter.Username != "ali" || stub.filter.Result == nil || *stub.filter.Result != result {
			t.Errorf("%s: filter not passed through: %+v", tc.name, stub.filter)
		}
		if total != 1 || len(logs) != 1 || logs[0].ID != "l1" {
			t.Errorf("%s: List() = (%d total, %d items), want (1, 1)", tc.name, total, len(logs))
		}
	}
}
