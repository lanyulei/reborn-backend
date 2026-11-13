package data

import (
	"context"
	"encoding/json"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgx/v5"
)

var _ biz.MessageRepo = (*messageRepo)(nil)

// messageContent 是 content jsonb 的持久化结构（与模型协议字段对齐）。
type messageContent struct {
	Content    string            `json:"content,omitempty"`
	Reasoning  string            `json:"reasoning,omitempty"`
	ToolCalls  []biz.ToolCallRef `json:"tool_calls,omitempty"`
	ToolCallID string            `json:"tool_call_id,omitempty"`
}

// messageRepo 消息仓储的 PostgreSQL 实现。
type messageRepo struct {
	data *basedata.Data
}

// NewMessageRepo new a messageRepo.
func NewMessageRepo(data *basedata.Data) *messageRepo {
	return &messageRepo{data: data}
}

// Append implements biz.MessageRepo：seq 由子查询取会话内 max+1（同一会话
// 的写入受会话租约串行化；唯一约束是最后防线）。
func (r *messageRepo) Append(ctx context.Context, m *biz.Message) error {
	raw, err := json.Marshal(messageContent{
		Content: m.Content, Reasoning: m.Reasoning, ToolCalls: m.ToolCalls, ToolCallID: m.ToolCallID,
	})
	if err != nil {
		return fmt.Errorf("marshal assistant message: %w", err)
	}
	err = r.data.Pool.QueryRow(ctx,
		`INSERT INTO assistant_message (id, conversation_id, seq, role, content, create_time)
		 VALUES ($1, $2,
		   (SELECT COALESCE(MAX(seq), 0) + 1 FROM assistant_message WHERE conversation_id = $2),
		   $3, $4, $5)
		 RETURNING seq`,
		m.ID, m.ConversationID, m.Role, raw, m.CreateTime).Scan(&m.Seq)
	if err != nil {
		return fmt.Errorf("insert assistant message: %w", err)
	}
	return nil
}

// List implements biz.MessageRepo：完整历史，按 seq 正序。
func (r *messageRepo) List(ctx context.Context, conversationID string) ([]*biz.Message, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, conversation_id, seq, role, content, create_time
		 FROM assistant_message WHERE conversation_id = $1 ORDER BY seq`, conversationID)
	if err != nil {
		return nil, fmt.Errorf("list assistant messages: %w", err)
	}
	return scanMessages(rows)
}

// ListRecent implements biz.MessageRepo：最近 limit 条，按 seq 正序（子查询
// 倒序取尾部后外层翻回正序）。
func (r *messageRepo) ListRecent(ctx context.Context, conversationID string, limit int) ([]*biz.Message, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, conversation_id, seq, role, content, create_time FROM (
		   SELECT id, conversation_id, seq, role, content, create_time
		   FROM assistant_message WHERE conversation_id = $1 ORDER BY seq DESC LIMIT $2
		 ) recent ORDER BY seq`, conversationID, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent assistant messages: %w", err)
	}
	return scanMessages(rows)
}

// scanMessages 扫描消息结果集并解码 content jsonb；负责关闭 rows。
func scanMessages(rows pgx.Rows) ([]*biz.Message, error) {
	defer rows.Close()
	items := make([]*biz.Message, 0, 32)
	for rows.Next() {
		var m biz.Message
		var raw []byte
		if err := rows.Scan(&m.ID, &m.ConversationID, &m.Seq, &m.Role, &raw, &m.CreateTime); err != nil {
			return nil, fmt.Errorf("scan assistant message: %w", err)
		}
		var c messageContent
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, fmt.Errorf("decode assistant message %s: %w", m.ID, err)
		}
		m.Content, m.Reasoning, m.ToolCalls, m.ToolCallID = c.Content, c.Reasoning, c.ToolCalls, c.ToolCallID
		items = append(items, &m)
	}
	return items, rows.Err()
}
