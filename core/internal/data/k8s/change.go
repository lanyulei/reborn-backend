package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.ChangeStore  = (*changeRepo)(nil)
	_ biz.ChangeReader = (*changeRepo)(nil)
	_ biz.ChangeFlow   = (*changeRepo)(nil)
	_ biz.ChangeQueue  = (*changeRepo)(nil)
	_ biz.SettingRepo  = (*settingRepo)(nil)
)

// changeRepo 变更单（k8s_change）。exec 列（加密的执行计划）本包只写入、从不读出。
type changeRepo struct {
	data *basedata.Data
}

// NewChangeRepo new a changeRepo.
func NewChangeRepo(data *basedata.Data) *changeRepo { return &changeRepo{data: data} }

// changeCols 列表与详情共用的列（不含 display / params）。
const changeCols = `id::text, kind, cluster_id::text, cluster_name, namespace, api_group, api_version, resource, res_kind, name,
	action, op, summary, status, requester_id::text, requester_name, client_ip, COALESCE(ticket_id::text, ''), ticket_number,
	message, result, create_time, approve_time, expire_time, finish_time`

func changeDest(c *biz.Change, result *[]byte) []any {
	return []any{
		&c.ID, &c.Kind, &c.ClusterID, &c.ClusterName, &c.Namespace, &c.APIGroup, &c.APIVersion, &c.Resource, &c.ResKind, &c.Name,
		&c.Action, &c.Op, &c.Summary, &c.Status, &c.RequesterID, &c.RequesterName, &c.ClientIP, &c.TicketID, &c.TicketNumber,
		&c.Message, result, &c.CreateTime, &c.ApproveTime, &c.ExpireTime, &c.FinishTime,
	}
}

// Create implements biz.ChangeStore.
func (r *changeRepo) Create(ctx context.Context, c *biz.Change, exec []byte) error {
	display, err := json.Marshal(c.Display)
	if err != nil {
		return err
	}
	params := []byte(c.Params)
	if len(params) == 0 {
		params = []byte(`{}`)
	}
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO k8s_change (id, kind, cluster_id, cluster_name, namespace, api_group, api_version, resource, res_kind, name,
		   action, op, summary, display, exec, params, status, requester_id, requester_name, client_ip)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`,
		c.ID, c.Kind, c.ClusterID, c.ClusterName, c.Namespace, c.APIGroup, c.APIVersion, c.Resource, c.ResKind, c.Name,
		c.Action, c.Op, c.Summary, display, dbx.NilIfEmptyBytes(exec), params, c.Status, c.RequesterID, c.RequesterName, c.ClientIP); err != nil {
		return fmt.Errorf("insert k8s change: %w", err)
	}
	return nil
}

// AttachTicket implements biz.ChangeStore.
func (r *changeRepo) AttachTicket(ctx context.Context, id, ticketID, number string) error {
	var err error
	if ticketID == "" {
		_, err = r.data.Pool.Exec(ctx, "DELETE FROM k8s_change WHERE id = $1 AND ticket_id IS NULL AND status = $2", id, kc.ChangePending)
	} else {
		_, err = r.data.Pool.Exec(ctx, "UPDATE k8s_change SET ticket_id = $2, ticket_number = $3 WHERE id = $1", id, ticketID, number)
	}
	if err != nil {
		return fmt.Errorf("attach ticket to k8s change: %w", err)
	}
	return nil
}

// Approval implements biz.ChangeStore.
func (r *changeRepo) Approval(ctx context.Context, clusterID string) (*biz.ClusterApproval, error) {
	var a biz.ClusterApproval
	err := r.data.Pool.QueryRow(ctx,
		`SELECT name, display_name, env, enabled, COALESCE(approval_template_id::text, '')
		 FROM k8s_cluster WHERE id = $1 AND delete_time IS NULL`, clusterID).
		Scan(&a.Name, &a.DisplayName, &a.Env, &a.Enabled, &a.TemplateID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrClusterNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get k8s cluster approval: %w", err)
	}
	return &a, nil
}

// List implements biz.ChangeReader.
func (r *changeRepo) List(ctx context.Context, f biz.ChangeFilter, page, pageSize int64) ([]*biz.Change, int64, error) {
	a := sqlArgs{}
	where := " WHERE true"
	switch {
	case f.RequesterID != "":
		where += " AND requester_id = " + a.add(f.RequesterID)
	case f.Access != nil:
		where += " AND (requester_id = " + a.add(f.OrRequesterID) + " OR (true" + objectScopeWhere(&a, *f.Access, "cluster_id", "namespace") + "))"
	case f.ClusterIDs != nil:
		where += " AND cluster_id = ANY(" + a.add(dbx.TextArray(f.ClusterIDs)) + "::uuid[])"
	}
	if f.ClusterID != "" {
		where += " AND cluster_id = " + a.add(f.ClusterID)
	}
	if f.Status != nil {
		where += " AND status = " + a.add(*f.Status)
	}
	if f.Keyword != "" {
		k := a.add(dbx.LikeContains(f.Keyword))
		where += " AND (name ILIKE " + k + " OR summary ILIKE " + k + " OR requester_name ILIKE " + k + " OR ticket_number ILIKE " + k + ")"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM k8s_change"+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count k8s changes: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+changeCols+" FROM k8s_change"+where+" ORDER BY create_time DESC, id DESC LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list k8s changes: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Change, 0, pageSize)
	for rows.Next() {
		var c biz.Change
		var result []byte
		if err := rows.Scan(changeDest(&c, &result)...); err != nil {
			return nil, 0, fmt.Errorf("scan k8s change: %w", err)
		}
		_ = json.Unmarshal(result, &c.Result)
		items = append(items, &c)
	}
	return items, total, rows.Err()
}

// Get implements biz.ChangeReader.
func (r *changeRepo) Get(ctx context.Context, id string) (*biz.Change, error) {
	var c biz.Change
	var result, display, params []byte
	dest := append(changeDest(&c, &result), &display, &params)
	err := r.data.Pool.QueryRow(ctx, "SELECT "+changeCols+", display, params FROM k8s_change WHERE id = $1", id).Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrChangeNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get k8s change: %w", err)
	}
	_ = json.Unmarshal(result, &c.Result)
	c.Display = &kc.ChangeDisplay{}
	_ = json.Unmarshal(display, c.Display)
	c.Params = params
	return &c, nil
}

// PendingByTicketTx implements biz.ChangeFlow.
func (r *changeRepo) PendingByTicketTx(ctx context.Context, tx txrun.Tx, ticketID string) (*biz.Change, error) {
	var c biz.Change
	err := tx.QueryRow(ctx,
		"SELECT id::text, kind, requester_id::text, requester_name FROM k8s_change WHERE ticket_id = $1 AND status = $2 FOR UPDATE",
		ticketID, kc.ChangePending).Scan(&c.ID, &c.Kind, &c.RequesterID, &c.RequesterName)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find k8s change by ticket: %w", err)
	}
	return &c, nil
}

// ResolveTx implements biz.ChangeFlow。批准：写身份、批准时间与失效时间；其余：直接进入终态。
func (r *changeRepo) ResolveTx(ctx context.Context, tx txrun.Tx, id string, status int32, message string, requester *kc.ChangeRequester, ttl time.Duration) error {
	var err error
	if status == kc.ChangeApproved {
		var raw []byte
		if raw, err = json.Marshal(requester); err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`UPDATE k8s_change SET status = $2, requester = $3, approve_time = now(), expire_time = now() + make_interval(secs => $4), message = ''
			 WHERE id = $1 AND status = $5`, id, status, raw, ttl.Seconds(), kc.ChangePending)
	} else {
		_, err = tx.Exec(ctx, "UPDATE k8s_change SET status = $2, message = $3, finish_time = now() WHERE id = $1 AND status = $4",
			id, status, message, kc.ChangePending)
	}
	if err != nil {
		return fmt.Errorf("resolve k8s change: %w", err)
	}
	return nil
}

// Finish implements biz.ChangeFlow.
func (r *changeRepo) Finish(ctx context.Context, id string, status int32, message string) error {
	if _, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_change SET status = $2, message = $3, finish_time = now(), lease_owner = '', lease_until = NULL
		 WHERE id = $1 AND status IN ($4, $5)`, id, status, message, kc.ChangeApproved, kc.ChangeExecuting); err != nil {
		return fmt.Errorf("finish k8s change: %w", err)
	}
	return nil
}

// ClaimPermission implements biz.ChangeQueue。置为执行中并写一段租约：执行器崩溃后租约过期可被重领
// （授权写入是幂等的整组替换）。
func (r *changeRepo) ClaimPermission(ctx context.Context) (*biz.Change, error) {
	var c biz.Change
	var params []byte
	err := r.data.Pool.QueryRow(ctx,
		`UPDATE k8s_change SET status = $1, lease_owner = 'core', lease_until = now() + interval '2 minutes', attempts = attempts + 1
		 WHERE id = (
		     SELECT id FROM k8s_change
		     WHERE kind = $2 AND expire_time > now() AND (status = $3 OR (status = $1 AND lease_until < now()))
		     ORDER BY approve_time, id LIMIT 1 FOR UPDATE SKIP LOCKED)
		 RETURNING id::text, cluster_id::text, cluster_name, requester_id::text, requester_name, client_ip, params`,
		kc.ChangeExecuting, kc.ChangeKindPermission, kc.ChangeApproved).
		Scan(&c.ID, &c.ClusterID, &c.ClusterName, &c.RequesterID, &c.RequesterName, &c.ClientIP, &params)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim k8s permission change: %w", err)
	}
	c.Kind, c.Params = kc.ChangeKindPermission, params
	return &c, nil
}

// Expire implements biz.ChangeQueue。已批准、或执行中但租约已过期（执行器失联）的变更单，过了失效时间即作废。
func (r *changeRepo) Expire(ctx context.Context) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_change SET status = $1, finish_time = now(), lease_owner = '', lease_until = NULL,
		   message = CASE WHEN message = '' THEN '批准后超过有效期，未执行' ELSE message END
		 WHERE expire_time <= now() AND (status = $2 OR (status = $3 AND (lease_until IS NULL OR lease_until < now())))`,
		kc.ChangeExpired, kc.ChangeApproved, kc.ChangeExecuting)
	if err != nil {
		return 0, fmt.Errorf("expire k8s changes: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// NoteTx implements biz.ChangeQueue.
func (r *changeRepo) NoteTx(ctx context.Context, tx txrun.Tx) (*biz.Change, error) {
	var c biz.Change
	var result []byte
	err := tx.QueryRow(ctx,
		`UPDATE k8s_change SET noted = true
		 WHERE id = (SELECT id FROM k8s_change WHERE status >= $1 AND NOT noted ORDER BY finish_time, id LIMIT 1 FOR UPDATE SKIP LOCKED)
		 RETURNING `+changeCols, kc.ChangeSucceeded).Scan(changeDest(&c, &result)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim finished k8s change: %w", err)
	}
	_ = json.Unmarshal(result, &c.Result)
	return &c, nil
}

// settingRepo 平台设置（k8s_setting）。
type settingRepo struct {
	data *basedata.Data
}

// NewSettingRepo new a settingRepo.
func NewSettingRepo(data *basedata.Data) *settingRepo { return &settingRepo{data: data} }

// Get implements biz.SettingRepo.
func (r *settingRepo) Get(ctx context.Context) (map[string]string, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT name, value FROM k8s_setting")
	if err != nil {
		return nil, fmt.Errorf("list k8s settings: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("scan k8s setting: %w", err)
		}
		out[k] = v
	}
	return out, rows.Err()
}

// Set implements biz.SettingRepo.
func (r *settingRepo) Set(ctx context.Context, values map[string]string, userID string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		for k, v := range values {
			if _, err := tx.Exec(ctx,
				`INSERT INTO k8s_setting (name, value, update_user) VALUES ($1, $2, $3)
				 ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, update_user = EXCLUDED.update_user, update_time = now()`,
				k, v, dbx.NilIfEmpty(userID)); err != nil {
				return fmt.Errorf("set k8s setting %s: %w", k, err)
			}
		}
		return nil
	})
}

// Approved implements biz.ChangeRefresher.
func (r *changeRepo) Approved(ctx context.Context, limit int) ([]biz.ApprovedChange, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT id::text, requester_id::text FROM k8s_change WHERE status = $1 AND expire_time > now() ORDER BY approve_time, id LIMIT $2",
		kc.ChangeApproved, limit)
	if err != nil {
		return nil, fmt.Errorf("list approved k8s changes: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.ApprovedChange, error) {
		var c biz.ApprovedChange
		return c, row.Scan(&c.ID, &c.RequesterID)
	})
}

// RefreshRequester implements biz.ChangeRefresher。保留快照里的用户名（展示用），只换角色与超管标记。
func (r *changeRepo) RefreshRequester(ctx context.Context, id string, requester *kc.ChangeRequester) error {
	raw, err := json.Marshal(map[string]any{"roleIds": requester.RoleIDs, "isAdmin": requester.IsAdmin})
	if err != nil {
		return err
	}
	if _, err := r.data.Pool.Exec(ctx,
		"UPDATE k8s_change SET requester = COALESCE(requester, '{}'::jsonb) || $2::jsonb WHERE id = $1 AND status = $3 AND requester IS DISTINCT FROM (COALESCE(requester, '{}'::jsonb) || $2::jsonb)",
		id, raw, kc.ChangeApproved); err != nil {
		return fmt.Errorf("refresh k8s change requester: %w", err)
	}
	return nil
}
