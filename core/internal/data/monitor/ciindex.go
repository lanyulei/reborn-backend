package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5"
)

// SyncProviderSet 资源索引、工作日历快照、事件动作的仓储（monitor-ci-sync / monitor-calendar / monitor-action
// 三个后台循环用）。
var SyncProviderSet = wire.NewSet(
	NewCIIndexRepo, NewCalendarRepo, NewActionRepo,
	wire.Bind(new(biz.CIIndexReader), new(*ciIndexRepo)),
	wire.Bind(new(biz.CfgVersionReader), new(*ciIndexRepo)),
	wire.Bind(new(biz.CIIndexStore), new(*ciIndexRepo)),
	wire.Bind(new(biz.CalendarReader), new(*calendarRepo)),
	wire.Bind(new(biz.CalendarStore), new(*calendarRepo)),
	wire.Bind(new(biz.ActionRepo), new(*actionRepo)),
)

var (
	_ biz.CIIndexReader     = (*ciIndexRepo)(nil)
	_ biz.CfgVersionReader  = (*ciIndexRepo)(nil)
	_ biz.CIIndexStore      = (*ciIndexRepo)(nil)
	_ biz.CIIndexWriter     = ciIndexTx{}
	_ biz.CIIndexReconciler = ciIndexTx{}
)

// ciIndexColumns monitor_ci_index 的查询列序。
const ciIndexColumns = `ci_id::text, model_code, name, env, status, owner, tags, attrs, upstream_ids::text[], valid, source_update_time`

func scanCIEntry(scan func(dest ...any) error) (*biz.CIEntry, error) {
	var e biz.CIEntry
	var attrs []byte
	if err := scan(&e.ID, &e.ModelCode, &e.Name, &e.Env, &e.Status, &e.Owner, &e.Tags, &attrs, &e.UpstreamIDs, &e.Valid, &e.SourceUpdateTime); err != nil {
		return nil, err
	}
	if err := decodeJSON(attrs, &e.Attrs, "ci index attrs"); err != nil {
		return nil, err
	}
	return &e, nil
}

type ciIndexRepo struct {
	data *basedata.Data
}

// NewCIIndexRepo new a ciIndexRepo.
func NewCIIndexRepo(data *basedata.Data) *ciIndexRepo { return &ciIndexRepo{data: data} }

func (r *ciIndexRepo) query(ctx context.Context, what, tail string, args ...any) ([]*biz.CIEntry, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+ciIndexColumns+" FROM monitor_ci_index "+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	items := make([]*biz.CIEntry, 0, 64)
	for rows.Next() {
		e, err := scanCIEntry(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		items = append(items, e)
	}
	return items, rows.Err()
}

// Briefs implements biz.CIIndexReader（含已失效的行）。
func (r *ciIndexRepo) Briefs(ctx context.Context, ids []string) (map[string]*biz.CIEntry, error) {
	out := make(map[string]*biz.CIEntry, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	items, err := r.query(ctx, "list monitor ci index by ids", "WHERE ci_id = ANY($1::uuid[])", ids)
	if err != nil {
		return nil, err
	}
	for _, e := range items {
		out[e.ID] = e
	}
	return out, nil
}

// ListValid implements biz.CIIndexReader.
func (r *ciIndexRepo) ListValid(ctx context.Context, limit int) ([]*biz.CIEntry, error) {
	return r.query(ctx, "list valid monitor ci index", "WHERE valid ORDER BY ci_id LIMIT $1", limit)
}

// Summary implements biz.CIIndexReader.
func (r *ciIndexRepo) Summary(ctx context.Context) (int32, *time.Time, error) {
	var valid int32
	var last *time.Time
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FILTER (WHERE valid), max(sync_time) FROM monitor_ci_index").Scan(&valid, &last); err != nil {
		return 0, nil, fmt.Errorf("summarize monitor ci index: %w", err)
	}
	return valid, last, nil
}

// CfgVersion implements biz.CfgVersionReader.
func (r *ciIndexRepo) CfgVersion(ctx context.Context, name string) (int64, error) {
	var v int64
	err := r.data.Pool.QueryRow(ctx, "SELECT version FROM monitor_cfg_version WHERE name = $1", name).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get monitor cfg version %s: %w", name, err)
	}
	return v, nil
}

// Sync implements biz.CIIndexStore。一轮同步 = 一个事务：先取事务级 advisory lock（事务结束或连接断开即
// 释放，实例崩溃不会留下死锁），没取到说明别的实例正在同步；fn 里的写入与配置版本 +1 一起提交。
// 事务在 fn 读取 CMDB 期间保持打开（只占一条连接；读 CMDB 走连接池里的其它连接）。
func (r *ciIndexRepo) Sync(ctx context.Context, fn func(ctx context.Context, w biz.CIIndexWriter, rec biz.CIIndexReconciler) (int, error)) (bool, error) {
	ran := false
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var locked bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))", mc.LockCISync).Scan(&locked); err != nil {
			return fmt.Errorf("lock monitor ci sync: %w", err)
		}
		if !locked {
			return nil
		}
		ran = true
		w := ciIndexTx{tx: tx}
		changed, err := fn(ctx, w, w)
		if err != nil || changed == 0 {
			return err
		}
		return bumpCfg(ctx, tx, mc.CfgCIIndex)
	})
	return ran, err
}

// ciIndexTx 一轮同步事务内的写入。
type ciIndexTx struct {
	tx pgx.Tx
}

// HighWater implements biz.CIIndexWriter.
func (t ciIndexTx) HighWater(ctx context.Context) (*time.Time, error) {
	var at *time.Time
	if err := t.tx.QueryRow(ctx, "SELECT max(source_update_time) FROM monitor_ci_index").Scan(&at); err != nil {
		return nil, fmt.Errorf("get monitor ci index high water: %w", err)
	}
	return at, nil
}

// ciIndexRow 批量写入时一行的 JSON 形状（jsonb_to_recordset 按键名取列）。
type ciIndexRow struct {
	ID        string              `json:"ci_id"`
	ModelCode string              `json:"model_code"`
	Name      string              `json:"name"`
	Env       string              `json:"env"`
	Status    string              `json:"status"`
	Owner     string              `json:"owner"`
	Tags      []string            `json:"tags"`
	Attrs     map[string][]string `json:"attrs"`
	Valid     bool                `json:"valid"`
	UpdatedAt *time.Time          `json:"source_update_time"`
}

// Upsert implements biz.CIIndexWriter。单条语句写一批；内容没有变化的行不写（IS DISTINCT FROM），
// upstream_ids 不动。source_update_time 只进不退。
func (t ciIndexTx) Upsert(ctx context.Context, entries []*biz.CIEntry) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	rows := make([]ciIndexRow, 0, len(entries))
	for _, e := range entries {
		attrs := e.Attrs
		if attrs == nil {
			attrs = map[string][]string{}
		}
		rows = append(rows, ciIndexRow{
			ID: e.ID, ModelCode: e.ModelCode, Name: e.Name, Env: e.Env, Status: e.Status, Owner: e.Owner,
			Tags: dbx.TextArray(e.Tags), Attrs: attrs, Valid: e.Valid, UpdatedAt: e.SourceUpdateTime,
		})
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return 0, fmt.Errorf("encode monitor ci index rows: %w", err)
	}
	// 同一批里同一个资源只留更新时间最新的一行（ON CONFLICT 不允许一条语句两次命中同一行）。
	tag, err := t.tx.Exec(ctx,
		`INSERT INTO monitor_ci_index AS i (ci_id, model_code, name, env, status, owner, tags, attrs, valid, source_update_time)
		 SELECT DISTINCT ON (s.ci_id) s.ci_id, s.model_code, s.name, s.env, s.status, s.owner,
		        ARRAY(SELECT jsonb_array_elements_text(s.tags)), s.attrs, s.valid, s.source_update_time
		 FROM jsonb_to_recordset($1::jsonb) AS s(ci_id uuid, model_code text, name text, env text, status text, owner text,
		        tags jsonb, attrs jsonb, valid boolean, source_update_time timestamptz)
		 ORDER BY s.ci_id, s.source_update_time DESC NULLS LAST
		 ON CONFLICT (ci_id) DO UPDATE SET model_code = EXCLUDED.model_code, name = EXCLUDED.name, env = EXCLUDED.env,
		        status = EXCLUDED.status, owner = EXCLUDED.owner, tags = EXCLUDED.tags, attrs = EXCLUDED.attrs, valid = EXCLUDED.valid,
		        source_update_time = GREATEST(i.source_update_time, EXCLUDED.source_update_time), sync_time = now()
		 WHERE (i.model_code, i.name, i.env, i.status, i.owner, i.tags, i.attrs, i.valid)
		        IS DISTINCT FROM (EXCLUDED.model_code, EXCLUDED.name, EXCLUDED.env, EXCLUDED.status, EXCLUDED.owner, EXCLUDED.tags, EXCLUDED.attrs, EXCLUDED.valid)
		    OR i.source_update_time IS DISTINCT FROM GREATEST(i.source_update_time, EXCLUDED.source_update_time)`, raw)
	if err != nil {
		return 0, fmt.Errorf("upsert monitor ci index: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// InvalidateExcept implements biz.CIIndexReconciler.
func (t ciIndexTx) InvalidateExcept(ctx context.Context, keep []string) (int, error) {
	tag, err := t.tx.Exec(ctx,
		"UPDATE monitor_ci_index SET valid = false, sync_time = now() WHERE valid AND ci_id <> ALL($1::uuid[])", dbx.TextArray(keep))
	if err != nil {
		return 0, fmt.Errorf("invalidate monitor ci index: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// SetUpstreams implements biz.CIIndexReconciler。整体替换：不在 upstreams 里的行清空；没有变化的行不写。
// 调用方已把每个资源的上游按 ID 排好序（数组比较与顺序有关）。
func (t ciIndexTx) SetUpstreams(ctx context.Context, upstreams map[string][]string) (int, error) {
	if upstreams == nil {
		upstreams = map[string][]string{}
	}
	raw, err := json.Marshal(upstreams)
	if err != nil {
		return 0, fmt.Errorf("encode monitor ci upstreams: %w", err)
	}
	tag, err := t.tx.Exec(ctx,
		`WITH src AS (
		   SELECT e.key::uuid AS ci_id, ARRAY(SELECT jsonb_array_elements_text(e.value)::uuid) AS ups FROM jsonb_each($1::jsonb) AS e
		 )
		 UPDATE monitor_ci_index i SET upstream_ids = n.ups, sync_time = now()
		 FROM (SELECT x.ci_id, COALESCE(s.ups, '{}'::uuid[]) AS ups FROM monitor_ci_index x LEFT JOIN src s ON s.ci_id = x.ci_id) n
		 WHERE i.ci_id = n.ci_id AND i.upstream_ids IS DISTINCT FROM n.ups`, raw)
	if err != nil {
		return 0, fmt.Errorf("set monitor ci upstreams: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
