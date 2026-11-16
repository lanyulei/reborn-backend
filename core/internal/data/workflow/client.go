package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.ClientRepo          = (*configRepo)(nil)
	_ biz.ClientAuthenticator = (*configRepo)(nil)
)

const clientColumns = "id, name, token_prefix, acting_user_id, definition_keys, status, description, last_used_time, update_time"

func scanClient(row pgx.CollectableRow) (*biz.Client, error) {
	var (
		c        biz.Client
		keys     []byte
		status   int16
		lastUsed *time.Time
	)
	if err := row.Scan(&c.ID, &c.Name, &c.TokenPrefix, &c.ActingUserID, &keys, &status, &c.Description, &lastUsed, &c.UpdateTime); err != nil {
		return nil, err
	}
	c.Status = int32(status)
	if lastUsed != nil {
		c.LastUsedTime = *lastUsed
	}
	return &c, json.Unmarshal(keys, &c.DefinitionKeys)
}

func clientNameTaken(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == "uk_workflow_client_name"
}

// CreateClient implements biz.ClientRepo：数量上限随写入一并判定（并发创建可能略超，上限只为
// 约束规模，不要求精确）。
func (r *configRepo) CreateClient(ctx context.Context, c *biz.Client, tokenHash []byte, limit int) error {
	keys, err := json.Marshal(c.DefinitionKeys)
	if err != nil {
		return fmt.Errorf("encode workflow client keys: %w", err)
	}
	tag, err := r.data.Pool.Exec(ctx,
		`INSERT INTO workflow_client (id, name, token_hash, token_prefix, acting_user_id, definition_keys, status, description,
			update_time, create_user, update_user)
		 SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10
		 WHERE (SELECT count(*) FROM workflow_client WHERE delete_time IS NULL) < $11`,
		c.ID, c.Name, tokenHash, c.TokenPrefix, c.ActingUserID, keys, c.Status, c.Description, c.UpdateTime,
		basedata.OperatorArg(ctx), limit)
	if err != nil {
		if clientNameTaken(err) {
			return biz.ErrClientNameExists
		}
		return fmt.Errorf("insert workflow client: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrClientInvalid
	}
	return nil
}

// UpdateClient implements biz.ClientRepo.
func (r *configRepo) UpdateClient(ctx context.Context, c *biz.Client) error {
	keys, err := json.Marshal(c.DefinitionKeys)
	if err != nil {
		return fmt.Errorf("encode workflow client keys: %w", err)
	}
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE workflow_client SET name = $2, acting_user_id = $3, definition_keys = $4, status = $5, description = $6,
			update_time = now(), update_user = $7
		 WHERE id = $1 AND delete_time IS NULL`,
		c.ID, c.Name, c.ActingUserID, keys, c.Status, c.Description, basedata.OperatorArg(ctx))
	if err != nil {
		if clientNameTaken(err) {
			return biz.ErrClientNameExists
		}
		return fmt.Errorf("update workflow client: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrClientNotFound
	}
	return nil
}

// SetClientToken implements biz.ClientRepo.
func (r *configRepo) SetClientToken(ctx context.Context, id string, tokenHash []byte, prefix string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE workflow_client SET token_hash = $2, token_prefix = $3, update_time = now(), update_user = $4
		 WHERE id = $1 AND delete_time IS NULL`, id, tokenHash, prefix, basedata.OperatorArg(ctx))
	if err != nil {
		return fmt.Errorf("rotate workflow client token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrClientNotFound
	}
	return nil
}

// DeleteClient implements biz.ClientRepo：软删并把令牌哈希换成随机值（令牌唯一索引不分是否
// 删除；被删凭据的令牌此后不可能再匹配上任何行）。
func (r *configRepo) DeleteClient(ctx context.Context, id string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE workflow_client SET delete_time = now(), delete_user = $2, token_hash = sha256(gen_random_uuid()::text::bytea)
		 WHERE id = $1 AND delete_time IS NULL`, id, basedata.OperatorArg(ctx))
	if err != nil {
		return fmt.Errorf("delete workflow client: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrClientNotFound
	}
	return nil
}

// ListClients implements biz.ClientRepo.
func (r *configRepo) ListClients(ctx context.Context) ([]*biz.Client, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+clientColumns+" FROM workflow_client WHERE delete_time IS NULL ORDER BY name, id")
	if err != nil {
		return nil, fmt.Errorf("list workflow clients: %w", err)
	}
	out, err := pgx.CollectRows(rows, scanClient)
	if err != nil {
		return nil, fmt.Errorf("scan workflow client: %w", err)
	}
	return out, nil
}

// FindClientByToken implements biz.ClientAuthenticator：按令牌哈希走唯一索引；命中后至多每
// touch 更新一次最近调用时间（条件更新，绝大多数请求只读不写）。
func (r *configRepo) FindClientByToken(ctx context.Context, tokenHash []byte, touch time.Duration) (*biz.Client, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+clientColumns+" FROM workflow_client WHERE token_hash = $1 AND delete_time IS NULL AND status = 1", tokenHash)
	if err != nil {
		return nil, fmt.Errorf("find workflow client: %w", err)
	}
	c, err := pgx.CollectExactlyOneRow(rows, scanClient)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrClientUnauthorized
		}
		return nil, fmt.Errorf("scan workflow client: %w", err)
	}
	if c.LastUsedTime.IsZero() || time.Since(c.LastUsedTime) > touch {
		if _, err := r.data.Pool.Exec(ctx,
			`UPDATE workflow_client SET last_used_time = now()
			 WHERE id = $1 AND (last_used_time IS NULL OR last_used_time < now() - make_interval(secs => $2))`,
			c.ID, touch.Seconds()); err != nil && ctx.Err() != nil {
			// 「最近调用时间」只是展示信息：写不进去不影响认证；只有请求已取消才中止。
			return nil, ctx.Err()
		}
	}
	return c, nil
}
