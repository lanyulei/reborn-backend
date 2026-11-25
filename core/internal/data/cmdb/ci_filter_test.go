package data

import (
	"context"
	"errors"
	"fmt"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// 属性过滤键来自请求：不合法（大写、点号等）是客户端错误 400（ErrCIInvalid），此前返回裸 error → 500。
// 键的校验先于任何查询，这里不需要数据库（Pool 为空）。
func TestCIList_InvalidAttrFilterKey(t *testing.T) {
	t.Parallel()
	repo := NewCIRepo(&basedata.Data{})
	for _, key := range []string{"IP", "a.b", "x'y", ""} {
		_, _, err := repo.List(context.Background(), biz.CIFilter{Attrs: map[string]any{key: "v"}}, 1, 10)
		if !errors.Is(err, biz.ErrCIInvalid) {
			t.Errorf("attr filter key %q: error = %v, want ErrCIInvalid", key, err)
		}
	}
}

// 死锁（40P01）映射为可重试的 ErrUniqueRuleBusy；资源更新 / 删除一侧同样套这层映射（见 Update /
// UpdateBatch / SoftDelete），其它错误原样透传。
func TestMapDeadlock(t *testing.T) {
	t.Parallel()
	deadlock := fmt.Errorf("update cmdb ci: %w", &pgconn.PgError{Code: pgerrcode.DeadlockDetected})
	if err := mapDeadlock(deadlock); !errors.Is(err, biz.ErrUniqueRuleBusy) {
		t.Fatalf("deadlock must map to ErrUniqueRuleBusy, got %v", err)
	}
	other := fmt.Errorf("update cmdb ci: %w", &pgconn.PgError{Code: pgerrcode.UniqueViolation})
	if err := mapDeadlock(other); err != other { //nolint:errorlint // 断言原样透传（同一指针），不是按错误链匹配
		t.Fatalf("other errors must pass through, got %v", err)
	}
	if err := mapDeadlock(biz.ErrCIVersionConflict); err != biz.ErrCIVersionConflict { //nolint:errorlint // 同上
		t.Fatalf("business errors must pass through, got %v", err)
	}
	if mapDeadlock(nil) != nil {
		t.Fatal("nil must stay nil")
	}
}
