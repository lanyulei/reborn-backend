package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.CalendarStore   = (*configRepo)(nil)
	_ biz.DelegationRepo  = (*configRepo)(nil)
	_ biz.Proxies         = (*configRepo)(nil)
	_ biz.WebhookRepo     = (*configRepo)(nil)
	_ biz.WebhookResolver = (*configRepo)(nil)
	_ biz.WebhookChecker  = (*configRepo)(nil)
)

// configRepo 流程引擎配置类数据（工作日历、代理设置、Webhook 端点）的 PostgreSQL 实现。
type configRepo struct {
	data *basedata.Data
	box  *secretbox.Box
}

// NewConfigRepo new a configRepo。Webhook 签名密钥与通知渠道凭据共用同一把主密钥
// （AES-256-GCM）；未配置主密钥时不带密钥的端点不受影响。
func NewConfigRepo(data *basedata.Data, c *conf.Notify) (*configRepo, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &configRepo{data: data, box: box}, nil
}

// GetCalendar implements biz.CalendarStore：命令事务内调用时复用事务连接。
func (r *configRepo) GetCalendar(ctx context.Context) (biz.CalendarConfig, error) {
	var (
		raw []byte
		c   biz.CalendarConfig
	)
	if err := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx, "SELECT config FROM workflow_calendar WHERE id = 1").Scan(&raw); err != nil {
		return c, fmt.Errorf("get workflow calendar: %w", err)
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, fmt.Errorf("decode workflow calendar: %w", err)
	}
	return c, nil
}

// SaveCalendar implements biz.CalendarStore.
func (r *configRepo) SaveCalendar(ctx context.Context, c biz.CalendarConfig) error {
	raw, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("encode workflow calendar: %w", err)
	}
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO workflow_calendar (id, config, update_user) VALUES (1, $1, $2)
		 ON CONFLICT (id) DO UPDATE SET config = EXCLUDED.config, update_time = now(), update_user = EXCLUDED.update_user`,
		raw, basedata.OperatorArg(ctx)); err != nil {
		return fmt.Errorf("save workflow calendar: %w", err)
	}
	return nil
}

// CreateDelegation implements biz.DelegationRepo：按用户加事务级咨询锁，把「不重叠」「条数上限」
// 的判定与写入串行化（同一用户并发新增不会各自通过检查）。
func (r *configRepo) CreateDelegation(ctx context.Context, d *biz.Delegation, limit int) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('workflow_delegation:' || $1::text, 0))", d.UserID); err != nil {
			return fmt.Errorf("lock delegation owner: %w", err)
		}
		var count int
		var overlap bool
		if err := tx.QueryRow(ctx,
			`SELECT count(*), COALESCE(bool_or(start_time < $3 AND end_time > $2), false)
			 FROM workflow_delegation WHERE user_id = $1 AND delete_time IS NULL`,
			d.UserID, d.StartTime, d.EndTime).Scan(&count, &overlap); err != nil {
			return fmt.Errorf("check delegations: %w", err)
		}
		if overlap || count >= limit {
			return biz.ErrDelegationInvalid
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO workflow_delegation (id, user_id, delegate_id, start_time, end_time, reason, create_time, update_time, create_user, update_user)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, $8)`,
			d.ID, d.UserID, d.DelegateID, d.StartTime, d.EndTime, d.Reason, d.CreateTime, basedata.OperatorArg(ctx)); err != nil {
			return fmt.Errorf("insert workflow delegation: %w", err)
		}
		return nil
	})
}

// DeleteDelegation implements biz.DelegationRepo.
func (r *configRepo) DeleteDelegation(ctx context.Context, userID, id string) error {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE workflow_delegation SET delete_time = now(), delete_user = $3 WHERE id = $1 AND user_id = $2 AND delete_time IS NULL",
		id, userID, basedata.OperatorArg(ctx))
	if err != nil {
		return fmt.Errorf("delete workflow delegation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrDelegationNotFound
	}
	return nil
}

// ListDelegations implements biz.DelegationRepo.
func (r *configRepo) ListDelegations(ctx context.Context, userID string) ([]*biz.Delegation, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, user_id, delegate_id, start_time, end_time, reason, create_time FROM workflow_delegation
		 WHERE user_id = $1 AND delete_time IS NULL ORDER BY start_time DESC, id`, userID)
	if err != nil {
		return nil, fmt.Errorf("list workflow delegations: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Delegation, error) {
		var d biz.Delegation
		return &d, row.Scan(&d.ID, &d.UserID, &d.DelegateID, &d.StartTime, &d.EndTime, &d.Reason, &d.CreateTime)
	})
	if err != nil {
		return nil, fmt.Errorf("scan workflow delegation: %w", err)
	}
	return out, nil
}

// ActiveProxies implements biz.Proxies：at 时刻生效的代理人（同一用户的时间段互不重叠，
// 至多一条）。命令事务内调用时复用事务连接。
func (r *configRepo) ActiveProxies(ctx context.Context, userIDs []string, at time.Time) (map[string]string, error) {
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`SELECT user_id, delegate_id FROM workflow_delegation
		 WHERE user_id IN (SELECT unnest($1::text[])::uuid) AND delete_time IS NULL AND start_time <= $2 AND end_time > $2`,
		userIDs, at)
	if err != nil {
		return nil, fmt.Errorf("list active proxies: %w", err)
	}
	defer rows.Close()
	out := make(map[string]string, len(userIDs))
	for rows.Next() {
		var user, delegate string
		if err := rows.Scan(&user, &delegate); err != nil {
			return nil, fmt.Errorf("scan active proxy: %w", err)
		}
		out[user] = delegate
	}
	return out, rows.Err()
}

// seal 加密签名密钥；空串落 NULL。未配置主密钥返回 biz.ErrSecretUnavailable。
func (r *configRepo) seal(secret string) ([]byte, error) {
	if secret == "" {
		return nil, nil
	}
	sealed, err := r.box.Seal(secret)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return nil, biz.ErrSecretUnavailable
	}
	return sealed, err
}

func webhookNameTaken(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == "uk_workflow_webhook_name"
}

// CreateWebhook implements biz.WebhookRepo：数量上限随写入一并判定（并发创建可能略超，
// 上限只为约束规模，不要求精确）。
func (r *configRepo) CreateWebhook(ctx context.Context, w *biz.Webhook, limit int) error {
	sealed, err := r.seal(w.Secret)
	if err != nil {
		return err
	}
	tag, err := r.data.Pool.Exec(ctx,
		`INSERT INTO workflow_webhook (id, name, url, secret, status, description, update_time, create_user, update_user)
		 SELECT $1, $2, $3, $4, $5, $6, $7, $8, $8
		 WHERE (SELECT count(*) FROM workflow_webhook WHERE delete_time IS NULL) < $9`,
		w.ID, w.Name, w.URL, sealed, w.Status, w.Description, w.UpdateTime, basedata.OperatorArg(ctx), limit)
	if err != nil {
		if webhookNameTaken(err) {
			return biz.ErrWebhookNameExists
		}
		return fmt.Errorf("insert workflow webhook: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrWebhookInvalid
	}
	return nil
}

// UpdateWebhook implements biz.WebhookRepo.
func (r *configRepo) UpdateWebhook(ctx context.Context, w *biz.Webhook, setSecret bool) error {
	sealed, err := r.seal(w.Secret)
	if err != nil {
		return err
	}
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE workflow_webhook SET name = $2, url = $3, status = $4, description = $5,
			secret = CASE WHEN $6 THEN $7 ELSE secret END, update_time = now(), update_user = $8
		 WHERE id = $1 AND delete_time IS NULL`,
		w.ID, w.Name, w.URL, w.Status, w.Description, setSecret, sealed, basedata.OperatorArg(ctx))
	if err != nil {
		if webhookNameTaken(err) {
			return biz.ErrWebhookNameExists
		}
		return fmt.Errorf("update workflow webhook: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrWebhookNotFound
	}
	return nil
}

// DeleteWebhook implements biz.WebhookRepo：仍被某个流程当前发布的版本引用时拒绝（草稿与
// 历史版本不拦：草稿发布时会校验，历史版本只有存量实例在用，届时调用失败进死信由人工处理）。
func (r *configRepo) DeleteWebhook(ctx context.Context, id string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var locked bool
		err := tx.QueryRow(ctx, "SELECT true FROM workflow_webhook WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).Scan(&locked)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return biz.ErrWebhookNotFound
			}
			return fmt.Errorf("lock workflow webhook: %w", err)
		}
		var used bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM workflow_definition d JOIN workflow_definition_version v ON v.id = d.published_version_id
				WHERE d.delete_time IS NULL
				  AND jsonb_path_exists(v.graph, '$.nodes[*] ? (@.service.webhookId == $id)', jsonb_build_object('id', $1::text)))`,
			id).Scan(&used); err != nil {
			return fmt.Errorf("check workflow webhook usage: %w", err)
		}
		if used {
			return biz.ErrWebhookInUse
		}
		if _, err := tx.Exec(ctx, "UPDATE workflow_webhook SET delete_time = now(), delete_user = $2 WHERE id = $1",
			id, basedata.OperatorArg(ctx)); err != nil {
			return fmt.Errorf("soft delete workflow webhook: %w", err)
		}
		return nil
	})
}

// ListWebhooks implements biz.WebhookRepo（不含密钥）。
func (r *configRepo) ListWebhooks(ctx context.Context) ([]*biz.Webhook, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, name, url, description, status, secret IS NOT NULL, update_time FROM workflow_webhook
		 WHERE delete_time IS NULL ORDER BY name, id`)
	if err != nil {
		return nil, fmt.Errorf("list workflow webhooks: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Webhook, error) {
		var (
			w      biz.Webhook
			status int16
		)
		err := row.Scan(&w.ID, &w.Name, &w.URL, &w.Description, &status, &w.HasSecret, &w.UpdateTime)
		w.Status = int32(status)
		return &w, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan workflow webhook: %w", err)
	}
	return out, nil
}

// Resolve implements biz.WebhookResolver：取启用的端点并解密签名密钥。
func (r *configRepo) Resolve(ctx context.Context, id string) (*biz.Webhook, error) {
	var (
		w      = biz.Webhook{ID: id, Status: biz.WebhookEnabled}
		sealed []byte
	)
	err := r.data.Pool.QueryRow(ctx,
		"SELECT name, url, secret FROM workflow_webhook WHERE id = $1 AND delete_time IS NULL AND status = 1", id).
		Scan(&w.Name, &w.URL, &sealed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrWebhookNotFound
		}
		return nil, fmt.Errorf("resolve workflow webhook: %w", err)
	}
	if sealed != nil {
		if w.Secret, err = r.box.Open(sealed); err != nil {
			return nil, fmt.Errorf("open workflow webhook secret: %w", err)
		}
		w.HasSecret = true
	}
	return &w, nil
}

// UsableWebhooks implements biz.WebhookChecker.
func (r *configRepo) UsableWebhooks(ctx context.Context, ids []string) (map[string]bool, error) {
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`SELECT id::text FROM workflow_webhook
		 WHERE id IN (SELECT unnest($1::text[])::uuid) AND delete_time IS NULL AND status = 1`, ids)
	if err != nil {
		return nil, fmt.Errorf("check workflow webhooks: %w", err)
	}
	found, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("scan workflow webhook id: %w", err)
	}
	out := make(map[string]bool, len(found))
	for _, id := range found {
		out[id] = true
	}
	return out, nil
}
