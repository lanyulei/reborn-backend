// Package data 故障管理域仓储实现（pgx + PostgreSQL，手写参数化 SQL）。本域所有表（incident_*）只有 core 读写；
// 写路径一律在 biz 用例的事务（txrun）内执行，仓储把句柄经 basedata.PgxTx 还原为 pgx.Tx。
package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/google/wire"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// ProviderSet is 故障管理域 data providers。
var ProviderSet = wire.NewSet(
	NewIncidentRepo, NewMemberRepo, NewEventRepo, NewLinkRepo, NewChangeRefRepo, NewCandidateRepo, NewRuleRepo, NewBusinessRepo,
	NewPlaybookRepo, NewProposalRepo, NewUpdateRepo, NewShareRepo, NewPostmortemRepo, NewActionItemRepo, NewSourceRepo, NewWebhookRepo,
	NewOutboxRepo, NewActionRepo, NewCursorRepo, NewSettingRepo, NewStatsRepo, NewTokenBox, NewWebhookSender, NewLocker,
	wire.Bind(new(biz.IncidentReader), new(*incidentRepo)),
	wire.Bind(new(biz.IncidentWriter), new(*incidentRepo)),
	wire.Bind(new(biz.MemberRepo), new(*memberRepo)),
	wire.Bind(new(biz.EventRepo), new(*eventRepo)),
	wire.Bind(new(biz.AlertLinkRepo), new(*linkRepo)),
	wire.Bind(new(biz.ChangeRefRepo), new(*changeRefRepo)),
	wire.Bind(new(biz.CandidateRepo), new(*candidateRepo)),
	wire.Bind(new(biz.RuleRepo), new(*ruleRepo)),
	wire.Bind(new(biz.BusinessRepo), new(*businessRepo)),
	wire.Bind(new(biz.PlaybookRepo), new(*playbookRepo)),
	wire.Bind(new(biz.ProposalRepo), new(*proposalRepo)),
	wire.Bind(new(biz.UpdateRepo), new(*updateRepo)),
	wire.Bind(new(biz.ShareRepo), new(*shareRepo)),
	wire.Bind(new(biz.PostmortemRepo), new(*postmortemRepo)),
	wire.Bind(new(biz.ActionItemRepo), new(*actionItemRepo)),
	wire.Bind(new(biz.SourceRepo), new(*sourceRepo)),
	wire.Bind(new(biz.WebhookRepo), new(*webhookRepo)),
	wire.Bind(new(biz.OutboxRepo), new(*outboxRepo)),
	wire.Bind(new(biz.ActionRepo), new(*actionRepo)),
	wire.Bind(new(biz.CursorRepo), new(*cursorRepo)),
	wire.Bind(new(biz.SettingRepo), new(*settingRepo)),
	wire.Bind(new(biz.StatsRepo), new(*statsRepo)),
	wire.Bind(new(biz.TokenCipher), new(*TokenBox)),
	wire.Bind(new(biz.WebhookSender), new(*WebhookSender)),
	wire.Bind(new(loop.Locker), new(*Locker)),
)

// sqlArgs 动态条件的实参收集器：add 追加一个实参并返回其占位符（$n）。
type sqlArgs struct {
	args []any
}

func (a *sqlArgs) add(v any) string {
	a.args = append(a.args, v)
	return fmt.Sprintf("$%d", len(a.args))
}

// decodeJSON 解码 jsonb 列（空值跳过）。
func decodeJSON(raw []byte, dst any, what string) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode %s: %w", what, err)
	}
	return nil
}

// encodeJSON 编码 jsonb 列。
func encodeJSON(v any, what string) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", what, err)
	}
	return raw, nil
}

// isUniqueViolation 报告是否唯一约束冲突（constraint 为空不区分约束）。
// dup: 与 data/monitor 等域各有一份相同的实现（各域 data 包不能互相 import）。
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation &&
		(constraint == "" || pgErr.ConstraintName == constraint)
}

// textArray nil 切片落空数组（pgx 会把 nil 编码为 NULL）。
func textArray(items []string) []string {
	if items == nil {
		return []string{}
	}
	return items
}

// nilIfEmpty 空串落 NULL（uuid 列）。
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nilTime nil 落 NULL。
func nilTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

// int32Array nil 切片落空数组。
func int32Array(items []int32) []int32 {
	if items == nil {
		return []int32{}
	}
	return items
}

// Locker 后台循环选主用的 advisory lock（kit/dbx.TryAdvisoryLock）。
type Locker struct {
	data *basedata.Data
}

// NewLocker new a Locker.
func NewLocker(data *basedata.Data) *Locker { return &Locker{data: data} }

// TryLock implements loop.Locker。
func (l *Locker) TryLock(ctx context.Context, key string) (func(), bool, error) {
	return tryAdvisoryLock(ctx, l.data, key)
}
