// Package data 实现 assistant 域 biz 仓储与外部能力接口：PostgreSQL 仓储
// （手写参数化 SQL）、OpenAI 兼容模型客户端、HTTP 回环执行器（出站 I/O 仅在本层）。
package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var _ biz.ConversationRepo = (*conversationRepo)(nil)

// conversationSelectColumns 会话查询列序，与 scanConversation 一一对应。
const conversationSelectColumns = "id, user_id, title, kind, create_time, update_time"

func scanConversation(scan func(dest ...any) error) (*biz.Conversation, error) {
	var c biz.Conversation
	if err := scan(&c.ID, &c.UserID, &c.Title, &c.Kind, &c.CreateTime, &c.UpdateTime); err != nil {
		return nil, err
	}
	return &c, nil
}

// conversationRepo 会话仓储的 PostgreSQL 实现。
type conversationRepo struct {
	data *basedata.Data
}

// NewConversationRepo new a conversationRepo.
func NewConversationRepo(data *basedata.Data) *conversationRepo {
	return &conversationRepo{data: data}
}

// Create implements biz.ConversationRepo.
func (r *conversationRepo) Create(ctx context.Context, c *biz.Conversation) error {
	kind := c.Kind
	if kind == 0 {
		kind = biz.ConversationKindUser
	}
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO assistant_conversation (id, user_id, title, kind, create_time, update_time)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		c.ID, c.UserID, c.Title, kind, c.CreateTime, c.UpdateTime)
	if err != nil {
		return fmt.Errorf("insert assistant conversation: %w", err)
	}
	return nil
}

// Get implements biz.ConversationRepo：按用户隔离，他人/已删除同样 404。
func (r *conversationRepo) Get(ctx context.Context, id, userID string) (*biz.Conversation, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+conversationSelectColumns+
			" FROM assistant_conversation WHERE id = $1 AND user_id = $2 AND delete_time IS NULL", id, userID)
	c, err := scanConversation(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrConversationNotFound
		}
		return nil, fmt.Errorf("get assistant conversation: %w", err)
	}
	return c, nil
}

// GetAny implements biz.ConversationRepo：不按用户隔离。
func (r *conversationRepo) GetAny(ctx context.Context, id string) (*biz.Conversation, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+conversationSelectColumns+" FROM assistant_conversation WHERE id = $1 AND delete_time IS NULL", id)
	c, err := scanConversation(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrConversationNotFound
		}
		return nil, fmt.Errorf("get assistant conversation: %w", err)
	}
	return c, nil
}

// List implements biz.ConversationRepo：本人会话按最近活动倒序分页。
func (r *conversationRepo) List(ctx context.Context, userID string, page, pageSize int64) ([]*biz.Conversation, int64, error) {
	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM assistant_conversation WHERE user_id = $1 AND kind = 1 AND delete_time IS NULL", userID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count assistant conversations: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+conversationSelectColumns+
			" FROM assistant_conversation WHERE user_id = $1 AND kind = 1 AND delete_time IS NULL"+
			" ORDER BY update_time DESC, id DESC LIMIT $2 OFFSET $3",
		userID, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("list assistant conversations: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Conversation, 0, pageSize)
	for rows.Next() {
		c, err := scanConversation(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan assistant conversation: %w", err)
		}
		items = append(items, c)
	}
	return items, total, rows.Err()
}

// Touch implements biz.ConversationRepo.
func (r *conversationRepo) Touch(ctx context.Context, id string, at time.Time) error {
	if _, err := r.data.Pool.Exec(ctx,
		"UPDATE assistant_conversation SET update_time = $2 WHERE id = $1", id, at); err != nil {
		return fmt.Errorf("touch assistant conversation: %w", err)
	}
	return nil
}

// SoftDelete implements biz.ConversationRepo.
func (r *conversationRepo) SoftDelete(ctx context.Context, id, userID string, at time.Time) error {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE assistant_conversation SET delete_time = $3 WHERE id = $1 AND user_id = $2 AND delete_time IS NULL",
		id, userID, at)
	if err != nil {
		return fmt.Errorf("soft delete assistant conversation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrConversationNotFound
	}
	return nil
}

// Purge implements biz.ConversationRepo：三张表同事务物理删除。ID 来自 agent_run.conversation_id（uuid 列），
// 直接按 uuid[] 比较以命中索引（把列转成 text 再比较会放弃索引走全表）。
func (r *conversationRepo) Purge(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		for _, stmt := range []string{
			"DELETE FROM assistant_tool_call WHERE conversation_id = ANY($1::uuid[])",
			"DELETE FROM assistant_message WHERE conversation_id = ANY($1::uuid[])",
			"DELETE FROM assistant_conversation WHERE id = ANY($1::uuid[])",
		} {
			if _, err := tx.Exec(ctx, stmt, ids); err != nil {
				return fmt.Errorf("purge assistant conversations: %w", err)
			}
		}
		return nil
	})
}

// AcquireLease implements biz.ConversationRepo：单语句条件更新，多实例互斥；到期时间与过期
// 判定都用数据库时钟。
func (r *conversationRepo) AcquireLease(ctx context.Context, id, userID, token string, ttl time.Duration) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE assistant_conversation SET run_token = $3, run_lease_until = now() + make_interval(secs => $4::float8)
		 WHERE id = $1 AND user_id = $2 AND delete_time IS NULL
		   AND (run_token IS NULL OR run_lease_until < now())`,
		id, userID, token, ttl.Seconds())
	if err != nil {
		return false, fmt.Errorf("acquire assistant lease: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ReleaseLease implements biz.ConversationRepo：属主校验，仅本轮 token 可释放。
func (r *conversationRepo) ReleaseLease(ctx context.Context, id, token string) error {
	if _, err := r.data.Pool.Exec(ctx,
		"UPDATE assistant_conversation SET run_token = NULL, run_lease_until = NULL WHERE id = $1 AND run_token = $2",
		id, token); err != nil {
		return fmt.Errorf("release assistant lease: %w", err)
	}
	return nil
}
