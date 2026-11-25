package data

import (
	"context"
	"slices"
	"strings"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// recordingTx 记录执行过的语句与实参（txrun.Tx 的窄桩）。
type recordingTx struct {
	stmts []string
	args  [][]any
}

func (t *recordingTx) Exec(_ context.Context, sql string, args ...any) (int64, error) {
	t.stmts = append(t.stmts, sql)
	t.args = append(t.args, args)
	return 0, nil
}

func (t *recordingTx) QueryRow(context.Context, string, ...any) txrun.Row { return nil }

// 全量替换授权：加锁 → 清两表 → 每类授权一条批量插入（空的那类不发语句）；每行仍各有一个应用侧
// 生成的主键，资源码按入参顺序原样落库——与逐行插入写出的行集合相同。
func TestReplaceForRoleBatchesInserts(t *testing.T) {
	t.Parallel()
	const roleID = "018f3e1a-0100-7000-8000-000000000001"
	grants := biz.GrantSet{Menus: []string{"m1", "m2", "m3"}, Apis: []string{"a1"}}
	ops := []string{"a1", "o2"}
	tx := &recordingTx{}
	if err := NewPermissionRepo(nil).ReplaceForRole(context.Background(), tx, roleID, grants, ops); err != nil {
		t.Fatalf("ReplaceForRole() error = %v", err)
	}
	// 锁、两条 DELETE、menu、api、operations（elements 为空不发语句）。
	if len(tx.stmts) != 6 {
		t.Fatalf("statements = %d, want 6: %v", len(tx.stmts), tx.stmts)
	}
	for i, want := range []string{"pg_advisory_xact_lock", "DELETE FROM system_grant", "DELETE FROM system_permission", "INSERT INTO system_grant", "INSERT INTO system_grant", "INSERT INTO system_permission"} {
		if !strings.Contains(tx.stmts[i], want) {
			t.Errorf("statement %d = %q, want it to contain %q", i, tx.stmts[i], want)
		}
	}
	checkIDs := func(v any, n int) {
		t.Helper()
		ids, _ := v.([]string)
		if len(ids) != n {
			t.Fatalf("ids = %v, want %d", v, n)
		}
		seen := map[string]bool{}
		for _, id := range ids {
			if !idgen.IsUUID(id) || seen[id] {
				t.Errorf("id %q invalid or duplicated", id)
			}
			seen[id] = true
		}
	}
	for _, tc := range []struct {
		stmt  int
		kind  string
		codes []string
	}{{3, grantTypeMenu, grants.Menus}, {4, grantTypeAPI, grants.Apis}} {
		a := tx.args[tc.stmt]
		if len(a) != 4 || a[0] != roleID || a[1] != tc.kind || !slices.Equal(a[3].([]string), tc.codes) {
			t.Errorf("grant insert %d args = %v", tc.stmt, a)
		}
		checkIDs(a[2], len(tc.codes))
	}
	if a := tx.args[5]; len(a) != 3 || a[0] != roleID || !slices.Equal(a[2].([]string), ops) {
		t.Errorf("permission insert args = %v", a)
	} else {
		checkIDs(a[1], len(ops))
	}

	// 清空授权：只有锁与两条 DELETE。
	tx = &recordingTx{}
	if err := NewPermissionRepo(nil).ReplaceForRole(context.Background(), tx, roleID, biz.GrantSet{}, nil); err != nil || len(tx.stmts) != 3 {
		t.Fatalf("clear: err %v, statements %v", err, tx.stmts)
	}
}
