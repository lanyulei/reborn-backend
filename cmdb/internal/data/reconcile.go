package data

import (
	"context"
	"fmt"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var _ biz.ReconcileRepo = (*reconcileRepo)(nil)

// reconcileRepo 集合 SQL 对账：全部语句以 (run_id, model_code, source) 为范围，参数绑定，
// 动态字段名（人工字段、唯一字段）一律以 text[] 实参经 jsonb 运算符处理，无 SQL 拼接。
type reconcileRepo struct {
	data *Data
}

// NewReconcileRepo new a reconcileRepo.
func NewReconcileRepo(data *Data) *reconcileRepo { return &reconcileRepo{data: data} }

// 历史 request_id 以运行 ID 标记（core 的 X-Request-Id 位置），便于按运行追溯。

// stmtUniqueKey 为暂存行计算唯一键：唯一字段 code 升序 `code=value|...`（与 kit/cmdbcontract.UniqueKey 一致）。
const stmtUniqueKey = `
UPDATE cmdb_sync_staging s SET unique_key = COALESCE((
    SELECT string_agg(u.code || '=' || (s.attrs ->> u.code), '|' ORDER BY u.code)
    FROM unnest($3::text[]) AS u(code)
    WHERE s.attrs ? u.code AND jsonb_typeof(s.attrs -> u.code) <> 'null'), '')
WHERE s.run_id = $1 AND s.model_code = $2`

// stmtTouch 命中的资源刷新同步时间、清陈旧与消失标记（不改 version：无业务变化）。
const stmtTouch = `
UPDATE cmdb_ci c SET last_synced_at = now(), stale = false, missing_since = NULL
FROM cmdb_sync_staging s
WHERE s.run_id = $1 AND s.model_code = $2 AND c.model_code = $2 AND c.source = $3 AND c.external_id = s.external_id AND c.delete_time IS NULL`

// stmtUpdate 变化的资源：名称 / 属性（人工字段不覆盖）/ 退役→重新激活，version+1，历史只记差异键。
// 名称或唯一键与其它资源冲突的行跳过（计入失败样本）。
const stmtUpdate = `
WITH s AS (
    SELECT external_id, name, attrs, unique_key FROM cmdb_sync_staging WHERE run_id = $1 AND model_code = $2
), changed AS (
    SELECT c.id, c.name AS old_name, s.name AS new_name, c.status AS old_status,
           CASE WHEN c.status = $6 THEN $7 ELSE c.status END AS new_status,
           c.attrs AS old_attrs, (c.attrs || (s.attrs - $4::text[])) AS new_attrs, s.unique_key
    FROM cmdb_ci c JOIN s ON s.external_id = c.external_id
    WHERE c.model_code = $2 AND c.source = $3 AND c.delete_time IS NULL
      AND (c.name <> s.name OR c.status = $6 OR c.attrs <> (c.attrs || (s.attrs - $4::text[])))
      AND NOT EXISTS (
          SELECT 1 FROM cmdb_ci o WHERE o.model_code = $2 AND o.id <> c.id AND o.delete_time IS NULL
            AND (o.name = s.name OR (s.unique_key <> '' AND o.unique_key = s.unique_key)))
), diffs AS (
    SELECT ch.*, d.before_attrs, d.after_attrs
    FROM changed ch
    CROSS JOIN LATERAL (
        SELECT jsonb_object_agg(k, COALESCE(ch.old_attrs -> k, 'null'::jsonb)) AS before_attrs,
               jsonb_object_agg(k, COALESCE(ch.new_attrs -> k, 'null'::jsonb)) AS after_attrs
        FROM jsonb_object_keys(ch.old_attrs || ch.new_attrs) AS k
        WHERE ch.old_attrs -> k IS DISTINCT FROM ch.new_attrs -> k
    ) d
), upd AS (
    UPDATE cmdb_ci c SET name = df.new_name, status = df.new_status, attrs = df.new_attrs, unique_key = df.unique_key,
        version = c.version + 1, update_time = now(), update_user = NULL, last_synced_at = now(), stale = false, missing_since = NULL
    FROM diffs df WHERE c.id = df.id
    RETURNING c.id
), hist AS (
    INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
    SELECT gen_random_uuid(), df.id, CASE WHEN df.old_status <> df.new_status THEN $8 ELSE $9 END,
        jsonb_build_object(
            'before', '{}'::jsonb
                || CASE WHEN df.old_name <> df.new_name THEN jsonb_build_object('name', df.old_name) ELSE '{}'::jsonb END
                || CASE WHEN df.old_status <> df.new_status THEN jsonb_build_object('status', df.old_status) ELSE '{}'::jsonb END
                || CASE WHEN df.before_attrs IS NOT NULL THEN jsonb_build_object('attrs', df.before_attrs) ELSE '{}'::jsonb END,
            'after', '{}'::jsonb
                || CASE WHEN df.old_name <> df.new_name THEN jsonb_build_object('name', df.new_name) ELSE '{}'::jsonb END
                || CASE WHEN df.old_status <> df.new_status THEN jsonb_build_object('status', df.new_status) ELSE '{}'::jsonb END
                || CASE WHEN df.after_attrs IS NOT NULL THEN jsonb_build_object('attrs', df.after_attrs) ELSE '{}'::jsonb END),
        '', $3, $5, now()
    FROM diffs df
)
SELECT count(*) FROM upd`

// stmtInsert 新资源：外部标识未命中、且名称/唯一键不与现有资源或本批其它行冲突的行。
const stmtInsert = `
WITH cand AS (
    SELECT s.external_id, s.name, s.attrs, s.unique_key FROM cmdb_sync_staging s
    WHERE s.run_id = $1 AND s.model_code = $2
      AND NOT EXISTS (SELECT 1 FROM cmdb_ci c WHERE c.model_code = $2 AND c.source = $3 AND c.external_id = s.external_id AND c.delete_time IS NULL)
      AND NOT EXISTS (SELECT 1 FROM cmdb_ci c WHERE c.model_code = $2 AND c.delete_time IS NULL
                        AND (c.name = s.name OR (s.unique_key <> '' AND c.unique_key = s.unique_key)))
), by_name AS (
    SELECT DISTINCT ON (name) * FROM cand ORDER BY name, external_id
), ok AS (
    SELECT DISTINCT ON (COALESCE(NULLIF(unique_key, ''), external_id)) * FROM by_name ORDER BY COALESCE(NULLIF(unique_key, ''), external_id), external_id
), ins AS (
    INSERT INTO cmdb_ci (id, model_code, name, status, env, source, external_id, unique_key, attrs, last_synced_at, stale, version, create_time, update_time)
    SELECT gen_random_uuid(), $2, ok.name, $7, $6, $3, ok.external_id, ok.unique_key, ok.attrs - $4::text[], now(), false, 1, now(), now() FROM ok
    RETURNING id, name, env, external_id, attrs
), hist AS (
    INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
    SELECT gen_random_uuid(), ins.id, $8,
        jsonb_build_object('before', '{}'::jsonb, 'after', jsonb_build_object('name', ins.name, 'status', $7, 'env', ins.env, 'externalId', ins.external_id, 'attrs', ins.attrs)),
        '', $3, $5, now()
    FROM ins
)
SELECT count(*) FROM ins`

// stmtFailedSamples 对账后仍未落库的暂存行（新建冲突）与被跳过的更新（冲突），带原因；
// matched 标记该行是否命中了既有资源（跳过的更新），用于把 Unchanged 从命中数中扣除。
const stmtFailedSamples = `
SELECT s.external_id, s.name,
    EXISTS (SELECT 1 FROM cmdb_ci c WHERE c.model_code = $2 AND c.source = $3 AND c.external_id = s.external_id AND c.delete_time IS NULL) AS matched,
    CASE WHEN EXISTS (SELECT 1 FROM cmdb_ci c WHERE c.model_code = $2 AND c.delete_time IS NULL AND c.name = s.name
                        AND NOT (c.source = $3 AND c.external_id = s.external_id))
             THEN 'name already used by another resource'
         WHEN s.unique_key <> '' AND EXISTS (SELECT 1 FROM cmdb_ci c WHERE c.model_code = $2 AND c.delete_time IS NULL AND c.unique_key = s.unique_key
                        AND NOT (c.source = $3 AND c.external_id = s.external_id))
             THEN 'unique key already used by another resource'
         ELSE 'duplicate name or unique key within this run' END
FROM cmdb_sync_staging s
WHERE s.run_id = $1 AND s.model_code = $2
  AND (NOT EXISTS (SELECT 1 FROM cmdb_ci c WHERE c.model_code = $2 AND c.source = $3 AND c.external_id = s.external_id AND c.delete_time IS NULL)
       OR EXISTS (SELECT 1 FROM cmdb_ci c JOIN cmdb_ci o ON o.model_code = $2 AND o.id <> c.id AND o.delete_time IS NULL
                     WHERE c.model_code = $2 AND c.source = $3 AND c.external_id = s.external_id AND c.delete_time IS NULL
                       AND (o.name = s.name OR (s.unique_key <> '' AND o.unique_key = s.unique_key))))
ORDER BY s.external_id`

// stmtDisappear 本源本类型中本次未出现的资源：首次标记消失时间（已标记的保留原时间）。
const stmtDisappear = `
UPDATE cmdb_ci c SET missing_since = now()
WHERE c.model_code = $2 AND c.source = $3 AND c.delete_time IS NULL AND c.missing_since IS NULL AND c.status <> $4
  AND NOT EXISTS (SELECT 1 FROM cmdb_sync_staging s WHERE s.run_id = $1 AND s.model_code = $2 AND s.external_id = c.external_id)`

// stmtRetire 消失超过宽限期的资源退役并记历史。
const stmtRetire = `
WITH r AS (
    SELECT id, status FROM cmdb_ci
    WHERE model_code = $1 AND source = $2 AND delete_time IS NULL AND status <> $4 AND missing_since IS NOT NULL AND missing_since <= now() - $3::interval
), upd AS (
    UPDATE cmdb_ci c SET status = $4, version = c.version + 1, update_time = now(), update_user = NULL FROM r WHERE c.id = r.id RETURNING c.id
), hist AS (
    INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
    SELECT gen_random_uuid(), r.id, $6, jsonb_build_object('before', jsonb_build_object('status', r.status), 'after', jsonb_build_object('status', $4::text)), '', $2, $5, now()
    FROM r
)
SELECT id FROM upd`

// stmtCascadeRetire 级联退役：刚退役资源作为 src 的 cascade_retire 关系，其 dst 未退役者一并退役
// （与 core 人工退役的级联语义一致；多级由调用方循环，上限 cascadeMaxDepth）。
const stmtCascadeRetire = `
WITH dep AS (
    SELECT DISTINCT d.id, d.status
    FROM cmdb_relation r
    JOIN cmdb_relation_type t ON t.code = r.type_code AND t.on_delete = $4 AND t.delete_time IS NULL
    JOIN cmdb_ci d ON d.id = r.dst_id AND d.delete_time IS NULL AND d.status <> $2
    WHERE r.src_id = ANY($1::uuid[])
), upd AS (
    UPDATE cmdb_ci c SET status = $2, version = c.version + 1, update_time = now(), update_user = NULL FROM dep WHERE c.id = dep.id RETURNING c.id
), hist AS (
    INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
    SELECT gen_random_uuid(), dep.id, $5, jsonb_build_object('before', jsonb_build_object('status', dep.status), 'after', jsonb_build_object('status', $2::text)), '', $3, $6, now()
    FROM dep
)
SELECT id FROM upd`

// cascadeMaxDepth 级联退役最大层数（防环）。
const cascadeMaxDepth = 8

// collectIDs 读取单列 id 结果集。
func collectIDs(rows pgx.Rows, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, 8)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// clampInt32 行数计数落 int32（统计列类型），超界封顶。
func clampInt32(n int64) int32 {
	if n > int64(^uint32(0)>>1) {
		return int32(^uint32(0) >> 1)
	}
	return int32(n) //nolint:gosec // 上界已在前一分支封顶
}

// ReconcileModel implements biz.ReconcileRepo（一类型一事务）。
func (r *reconcileRepo) ReconcileModel(ctx context.Context, p biz.ReconcileParams) (biz.Counts, []cmdbcontract.RunError, error) {
	var counts biz.Counts
	var samples []cmdbcontract.RunError
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		manual, unique := dbx.TextArray(p.ManualKeys), dbx.TextArray(p.UniqueCodes)
		if _, err := tx.Exec(ctx, stmtUniqueKey, p.RunID, p.ModelCode, unique); err != nil {
			return fmt.Errorf("compute unique keys: %w", err)
		}
		touched, err := tx.Exec(ctx, stmtTouch, p.RunID, p.ModelCode, p.Source)
		if err != nil {
			return fmt.Errorf("touch matched: %w", err)
		}
		var updated int64
		if err := tx.QueryRow(ctx, stmtUpdate, p.RunID, p.ModelCode, p.Source, manual, p.RunID,
			cmdbcontract.CIStatusRetired, cmdbcontract.CIStatusActive, cmdbcontract.ActionTransition, cmdbcontract.ActionUpdate).Scan(&updated); err != nil {
			return fmt.Errorf("update changed: %w", err)
		}
		var created int64
		if err := tx.QueryRow(ctx, stmtInsert, p.RunID, p.ModelCode, p.Source, manual, p.RunID, p.Env,
			cmdbcontract.CIStatusActive, cmdbcontract.ActionCreate).Scan(&created); err != nil {
			return fmt.Errorf("insert new: %w", err)
		}
		rows, err := tx.Query(ctx, stmtFailedSamples, p.RunID, p.ModelCode, p.Source)
		if err != nil {
			return fmt.Errorf("failed samples: %w", err)
		}
		var failed, skipped int32
		for rows.Next() {
			var e cmdbcontract.RunError
			var matched bool
			if err := rows.Scan(&e.ExternalID, &e.Name, &matched, &e.Reason); err != nil {
				rows.Close()
				return err
			}
			e.ModelCode = p.ModelCode
			failed++
			if matched {
				skipped++
			}
			if len(samples) < cmdbcontract.RunErrorSampleLimit {
				samples = append(samples, e)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		counts.Updated, counts.Created, counts.Failed = clampInt32(updated), clampInt32(created), failed
		// touched 含被更新与被跳过（冲突）的行；无变化 = 命中 - 更新 - 跳过。
		counts.Unchanged = max(clampInt32(touched.RowsAffected())-counts.Updated-skipped, 0)
		if p.SkipDisappear {
			return nil
		}
		gone, err := tx.Exec(ctx, stmtDisappear, p.RunID, p.ModelCode, p.Source, cmdbcontract.CIStatusRetired)
		if err != nil {
			return fmt.Errorf("mark disappeared: %w", err)
		}
		counts.Disappeared = clampInt32(gone.RowsAffected())
		retiredIDs, err := collectIDs(tx.Query(ctx, stmtRetire, p.ModelCode, p.Source, p.Grace, cmdbcontract.CIStatusRetired, p.RunID, cmdbcontract.ActionTransition))
		if err != nil {
			return fmt.Errorf("retire: %w", err)
		}
		counts.Retired = clampInt32(int64(len(retiredIDs)))
		// 级联退役（cascade_retire 关系的下游），逐层直到无新增。
		frontier := retiredIDs
		for level := 0; level < cascadeMaxDepth && len(frontier) > 0; level++ {
			next, err := collectIDs(tx.Query(ctx, stmtCascadeRetire, dbx.TextArray(frontier), cmdbcontract.CIStatusRetired, p.Source,
				cmdbcontract.OnDeleteCascadeRetire, cmdbcontract.ActionTransition, p.RunID))
			if err != nil {
				return fmt.Errorf("cascade retire: %w", err)
			}
			counts.Retired += clampInt32(int64(len(next)))
			frontier = next
		}
		return nil
	})
	return counts, samples, err
}

// stmtRelationsCTE 解析暂存声明的关系为 (type, src, dst, me, direction)，两端须为同源同步资源。
const stmtRelationsCTE = `
WITH decl AS (
    SELECT s.model_code, s.external_id, rel ->> 'type' AS type_code, rel ->> 'direction' AS direction,
           rel ->> 'peer_model' AS peer_model, rel ->> 'peer_external_id' AS peer_eid
    FROM cmdb_sync_staging s CROSS JOIN LATERAL jsonb_array_elements(s.relations) AS rel
    WHERE s.run_id = $1
), resolved AS (
    SELECT d.type_code, d.direction, me.id AS me_id,
           CASE WHEN d.direction = $3 THEN me.id ELSE peer.id END AS src_id,
           CASE WHEN d.direction = $3 THEN peer.id ELSE me.id END AS dst_id
    FROM decl d
    JOIN cmdb_ci me ON me.model_code = d.model_code AND me.source = $2 AND me.external_id = d.external_id AND me.delete_time IS NULL
    JOIN cmdb_ci peer ON peer.model_code = d.peer_model AND peer.source = $2 AND peer.external_id = d.peer_eid AND peer.delete_time IS NULL
    JOIN cmdb_relation_type t ON t.code = d.type_code AND t.delete_time IS NULL
    WHERE me.id <> peer.id
)`

// stmtRelationsDelete 本次声明过某类型某方向关系的资源：其同源同类型该侧的其它关系删除（如虚拟机迁移主机）。
const stmtRelationsDelete = stmtRelationsCTE + `
, keys AS (SELECT DISTINCT type_code, direction, me_id FROM resolved)
, del AS (
    DELETE FROM cmdb_relation r USING keys k
    WHERE r.source = $2 AND r.type_code = k.type_code
      AND ((k.direction = $3 AND r.src_id = k.me_id) OR (k.direction <> $3 AND r.dst_id = k.me_id))
      AND NOT EXISTS (SELECT 1 FROM resolved x WHERE x.type_code = r.type_code AND x.src_id = r.src_id AND x.dst_id = r.dst_id)
    RETURNING r.type_code, r.src_id, r.dst_id
)
INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
SELECT gen_random_uuid(), h.ci_id, $4, h.diff, '', $2, $1, now()
FROM del d
CROSS JOIN LATERAL (
    SELECT d.src_id AS ci_id, jsonb_build_object('relationType', d.type_code, 'direction', $3::text,
        'target', jsonb_build_object('id', dst.id, 'name', dst.name, 'modelCode', dst.model_code)) AS diff
    FROM cmdb_ci dst WHERE dst.id = d.dst_id
    UNION ALL
    SELECT d.dst_id, jsonb_build_object('relationType', d.type_code, 'direction', $5::text,
        'target', jsonb_build_object('id', src.id, 'name', src.name, 'modelCode', src.model_code))
    FROM cmdb_ci src WHERE src.id = d.src_id
) h`

// stmtRelationsInsert 缺失的关系插入并记双向历史。
const stmtRelationsInsert = stmtRelationsCTE + `
, ins AS (
    INSERT INTO cmdb_relation (id, type_code, src_id, dst_id, source, create_time)
    SELECT DISTINCT ON (type_code, src_id, dst_id) gen_random_uuid(), type_code, src_id, dst_id, $2, now() FROM resolved
    ORDER BY type_code, src_id, dst_id
    ON CONFLICT (type_code, src_id, dst_id) DO NOTHING
    RETURNING type_code, src_id, dst_id
)
INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
SELECT gen_random_uuid(), h.ci_id, $4, h.diff, '', $2, $1, now()
FROM ins i
CROSS JOIN LATERAL (
    SELECT i.src_id AS ci_id, jsonb_build_object('relationType', i.type_code, 'direction', $3::text,
        'target', jsonb_build_object('id', dst.id, 'name', dst.name, 'modelCode', dst.model_code)) AS diff
    FROM cmdb_ci dst WHERE dst.id = i.dst_id
    UNION ALL
    SELECT i.dst_id, jsonb_build_object('relationType', i.type_code, 'direction', $5::text,
        'target', jsonb_build_object('id', src.id, 'name', src.name, 'modelCode', src.model_code))
    FROM cmdb_ci src WHERE src.id = i.src_id
) h`

// ReconcileRelations implements biz.ReconcileRepo（一事务：先删多余，再补缺失）。
func (r *reconcileRepo) ReconcileRelations(ctx context.Context, runID, source string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, stmtRelationsDelete, runID, source, cmdbcontract.DirectionOutgoing, cmdbcontract.ActionUnlink, cmdbcontract.DirectionIncoming); err != nil {
			return fmt.Errorf("delete stale relations: %w", err)
		}
		if _, err := tx.Exec(ctx, stmtRelationsInsert, runID, source, cmdbcontract.DirectionOutgoing, cmdbcontract.ActionLink, cmdbcontract.DirectionIncoming); err != nil {
			return fmt.Errorf("insert relations: %w", err)
		}
		return nil
	})
}
