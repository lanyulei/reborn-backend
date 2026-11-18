package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/promclient"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5"
)

// InhibitProviderSet 抑制规则、记录规则与 Remote Write 地址校验的 data providers（由组合根并入 ProviderSet）。
var InhibitProviderSet = wire.NewSet(
	NewInhibitRepo, NewRecordRuleRepo,
	wire.Bind(new(biz.InhibitReader), new(*inhibitRepo)),
	wire.Bind(new(biz.InhibitNames), new(*inhibitRepo)),
	wire.Bind(new(biz.InhibitWriter), new(*inhibitRepo)),
	wire.Bind(new(biz.InhibitStats), new(*inhibitRepo)),
	wire.Bind(new(biz.RecordRuleReader), new(*recordRuleRepo)),
	wire.Bind(new(biz.RecordRuleWriter), new(*recordRuleRepo)),
)

var (
	_ biz.InhibitReader     = (*inhibitRepo)(nil)
	_ biz.InhibitNames      = (*inhibitRepo)(nil)
	_ biz.InhibitWriter     = (*inhibitRepo)(nil)
	_ biz.InhibitStats      = (*inhibitRepo)(nil)
	_ biz.WriteURLValidator = (*QueryClient)(nil)
)

// 第二期带乐观锁的表（表名为代码内常量，不接受外部输入）。
const (
	tableInhibit    = "monitor_inhibit"
	tableRecordRule = "monitor_record_rule"
)

// ValidateWriteURL implements biz.WriteURLValidator：格式按 promclient.ValidateWriteURL，地址字面量按与查询
// 地址同一份出站策略（链路本地 / 云元数据恒拒绝，回环按 monitor.block_loopback）。不发起网络请求；主机名
// 解析出的真实地址由评估引擎在每次建立连接时检查（promclient 的 DialGuard）。
func (q *QueryClient) ValidateWriteURL(raw string) error {
	if err := promclient.ValidateWriteURL(raw); err != nil {
		// promclient 的配置错误说明是固定文案（不回显地址）。
		return biz.DatasourceInvalid("%s", err.Error())
	}
	return q.checkLiteralHost(raw)
}

// inhibitColumns 抑制规则查询列序（表别名 i）。
const inhibitColumns = `i.id::text, COALESCE(i.group_id::text, ''), i.name, i.description, i.source, i.target, i.equal_labels,
	i.enabled, i.version, i.create_time, i.update_time, COALESCE(i.create_user::text, ''), COALESCE(i.update_user::text, '')`

func scanInhibit(scan func(dest ...any) error) (*biz.Inhibit, error) {
	var in biz.Inhibit
	s := &in.Spec
	var source, target []byte
	if err := scan(&in.ID, &s.GroupID, &s.Name, &s.Description, &source, &target, &s.EqualLabels,
		&s.Enabled, &in.Version, &in.CreateTime, &in.UpdateTime, &in.CreateUser, &in.UpdateUser); err != nil {
		return nil, err
	}
	if err := decodeJSON(source, &s.Source, "inhibit source"); err != nil {
		return nil, err
	}
	if err := decodeJSON(target, &s.Target, "inhibit target"); err != nil {
		return nil, err
	}
	return &in, nil
}

type inhibitRepo struct {
	data *basedata.Data
}

// NewInhibitRepo new an inhibitRepo.
func NewInhibitRepo(data *basedata.Data) *inhibitRepo { return &inhibitRepo{data: data} }

func (r *inhibitRepo) query(ctx context.Context, what, tail string, args ...any) ([]*biz.Inhibit, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+inhibitColumns+" FROM monitor_inhibit i WHERE i.delete_time IS NULL"+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	items := make([]*biz.Inhibit, 0, 16)
	for rows.Next() {
		in, err := scanInhibit(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		items = append(items, in)
	}
	return items, rows.Err()
}

// Get implements biz.InhibitReader.
func (r *inhibitRepo) Get(ctx context.Context, id string) (*biz.Inhibit, error) {
	in, err := scanInhibit(r.data.Pool.QueryRow(ctx,
		"SELECT "+inhibitColumns+" FROM monitor_inhibit i WHERE i.id = $1 AND i.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrInhibitNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor inhibit: %w", err)
	}
	return in, nil
}

// List implements biz.InhibitReader。分组条件走 idx_monitor_inhibit_group（部分索引，未删除的行）。
func (r *inhibitRepo) List(ctx context.Context, filter biz.InhibitFilter, page, pageSize int64) ([]*biz.Inhibit, int64, error) {
	var a sqlArgs
	where := ""
	switch {
	case filter.Groups.All && !filter.IncludeGlobal:
		where += " AND i.group_id IS NOT NULL"
	case filter.Groups.All:
	case filter.IncludeGlobal:
		where += " AND (i.group_id IS NULL OR i.group_id = ANY(" + a.add(dbx.TextArray(filter.Groups.IDs)) + "::uuid[]))"
	default:
		where += " AND i.group_id = ANY(" + a.add(dbx.TextArray(filter.Groups.IDs)) + "::uuid[])"
	}
	if filter.Keyword != "" {
		p := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (i.name ILIKE " + p + " OR i.description ILIKE " + p + ")"
	}
	if filter.Enabled != nil {
		where += " AND i.enabled = " + a.add(*filter.Enabled)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_inhibit i WHERE i.delete_time IS NULL"+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor inhibits: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	items, err := r.query(ctx, "list monitor inhibits", where+" ORDER BY i.create_time DESC, i.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	return items, total, err
}

// ListEnabled implements biz.InhibitReader.
func (r *inhibitRepo) ListEnabled(ctx context.Context, limit int) ([]*biz.Inhibit, error) {
	return r.query(ctx, "list enabled monitor inhibits", " AND i.enabled ORDER BY i.id LIMIT $1", limit)
}

// InhibitNames implements biz.InhibitNames（含已删除的：历史事件仍要展示命中的抑制规则）。
func (r *inhibitRepo) InhibitNames(ctx context.Context, ids []string) (map[string]string, error) {
	if len(ids) == 0 {
		return map[string]string{}, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT id::text, name FROM monitor_inhibit WHERE id = ANY($1::uuid[])", ids)
	if err != nil {
		return nil, fmt.Errorf("list monitor inhibit names: %w", err)
	}
	return collectNames(rows, "monitor inhibit name")
}

// InhibitedCounts implements biz.InhibitStats。只看告警中的事件（(status, last_fire_time) 索引），与
// MutedCounts 同一做法。
func (r *inhibitRepo) InhibitedCounts(ctx context.Context, inhibitIDs []string) (map[string]int32, error) {
	out := make(map[string]int32, len(inhibitIDs))
	if len(inhibitIDs) == 0 {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT inhibit_id::text, count(*) FROM monitor_alert WHERE status = $1 AND inhibited AND inhibit_id = ANY($2::uuid[]) GROUP BY inhibit_id`,
		mc.AlertFiring, inhibitIDs)
	if err != nil {
		return nil, fmt.Errorf("count inhibited monitor alerts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int32
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("scan inhibited monitor alert count: %w", err)
		}
		out[id] = n
	}
	return out, rows.Err()
}

// inhibitArgs 抑制规则写入的列实参（顺序：group_id, name, description, source, target, equal_labels, enabled）。
func inhibitArgs(s *biz.InhibitSpec) ([]any, error) {
	source, err := json.Marshal(s.Source)
	if err != nil {
		return nil, fmt.Errorf("encode inhibit source: %w", err)
	}
	target, err := json.Marshal(s.Target)
	if err != nil {
		return nil, fmt.Errorf("encode inhibit target: %w", err)
	}
	return []any{dbx.NilIfEmpty(s.GroupID), s.Name, s.Description, source, target, dbx.TextArray(s.EqualLabels), s.Enabled}, nil
}

// lockInhibitGroup 作用分组加共享锁并确认它还在（与分组删除互斥）；全局规则没有分组。
func lockInhibitGroup(ctx context.Context, tx pgx.Tx, groupID string) error {
	if groupID == "" {
		return nil
	}
	return lockGroup(ctx, tx, groupID)
}

// Create implements biz.InhibitWriter.
func (r *inhibitRepo) Create(ctx context.Context, in *biz.Inhibit, actor string) error {
	args, err := inhibitArgs(&in.Spec)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockInhibitGroup(ctx, tx, in.Spec.GroupID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO monitor_inhibit (id, create_user, update_user, group_id, name, description, source, target, equal_labels, enabled)
			 VALUES ($1, $2, $2, $3, $4, $5, $6, $7, $8::text[], $9)`,
			append([]any{in.ID, dbx.NilIfEmpty(actor)}, args...)...); err != nil {
			return fmt.Errorf("insert monitor inhibit: %w", err)
		}
		return bumpCfg(ctx, tx, mc.CfgInhibit)
	})
}

// Update implements biz.InhibitWriter.
func (r *inhibitRepo) Update(ctx context.Context, in *biz.Inhibit, expectVersion int32, actor string) error {
	args, err := inhibitArgs(&in.Spec)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockInhibitGroup(ctx, tx, in.Spec.GroupID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx,
			`UPDATE monitor_inhibit SET group_id = $4, name = $5, description = $6, source = $7, target = $8, equal_labels = $9::text[],
			   enabled = $10, version = version + 1, update_time = now(), update_user = $3
			 WHERE id = $1 AND version = $2 AND delete_time IS NULL`,
			append([]any{in.ID, expectVersion, dbx.NilIfEmpty(actor)}, args...)...)
		if err != nil {
			return fmt.Errorf("update monitor inhibit: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return missingOrConflict(ctx, tx, tableInhibit, in.ID, biz.ErrInhibitNotFound)
		}
		return bumpCfg(ctx, tx, mc.CfgInhibit)
	})
}

// Delete implements biz.InhibitWriter.
func (r *inhibitRepo) Delete(ctx context.Context, id string, expectVersion int32, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := softDelete(ctx, tx, tableInhibit, id, expectVersion, actor, biz.ErrInhibitNotFound); err != nil {
			return err
		}
		return bumpCfg(ctx, tx, mc.CfgInhibit)
	})
}
