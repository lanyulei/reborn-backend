package data

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/jackc/pgx/v5"
)

// recordingTx 记下 ClaimTx 执行的语句与实参；QueryRow 恒返回空行（没有可领取的行）。
type recordingTx struct {
	txrun.Tx
	sql  string
	args []any
}

func (t *recordingTx) QueryRow(_ context.Context, sql string, args ...any) txrun.Row {
	t.sql, t.args = sql, args
	return noRow{}
}

type noRow struct{}

func (noRow) Scan(...any) error { return pgx.ErrNoRows }

// 领取语句的形状（monitor 模块评审 dispatch P3-3 的回归锁）：恢复通知可以越过同一事件更早的「重复通知」
// 与「升级通知」，例外里的 $6 / $7 必须正好是这两种类型——触发通知不在例外之内，否则「未发出过触发通知
// 不发恢复通知」的判定会被打破。选取语义本身由集成测试在真实库上验证
// （TestDispatch_ResolvedPassesAnEscalationRowInBackoff）。
func TestOutboxClaimTx_ResolvedPassesRepeatAndEscalationOnly(t *testing.T) {
	tx := &recordingTx{}
	item, err := NewOutboxRepo(nil).ClaimTx(context.Background(), tx, nil)
	if err != nil || item != nil {
		t.Fatalf("ClaimTx() = %v, %v; want nothing claimed from an empty result", item, err)
	}
	if !strings.Contains(tx.sql, "AND NOT (o.kind = $5 AND p.kind IN ($6, $7))") {
		t.Fatalf("claim statement lacks the pass-over exception:\n%s", tx.sql)
	}
	want := []any{
		mc.OutboxPending,
		[]string{},
		outboxBackoffBaseSeconds, outboxBackoffMaxSeconds,
		mc.NotifyKindResolved, mc.NotifyKindRepeat, mc.NotifyKindEscalation,
	}
	if !reflect.DeepEqual(tx.args, want) {
		t.Fatalf("claim args = %#v\nwant            %#v", tx.args, want)
	}
}
