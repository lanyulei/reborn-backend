package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
)

var _ biz.ToolCallRepo = (*toolCallRepo)(nil)

// toolCallSelectColumns 工具调用查询列序，与 scanToolCall 一一对应。
const toolCallSelectColumns = "id, conversation_id, message_id, user_id, call_id, tool_name, title, operation, operation_code, " +
	"write, input, status, output, error, duration_ms, create_time, resolve_time"

func scanToolCall(scan func(dest ...any) error) (*biz.ToolCall, error) {
	var tc biz.ToolCall
	var output []byte
	var errMsg *string
	var input []byte
	if err := scan(&tc.ID, &tc.ConversationID, &tc.MessageID, &tc.UserID, &tc.CallID, &tc.ToolName, &tc.Title,
		&tc.Operation, &tc.OperationCode, &tc.Write, &input, &tc.Status, &output, &errMsg, &tc.DurationMS,
		&tc.CreateTime, &tc.ResolveTime); err != nil {
		return nil, err
	}
	tc.Input = json.RawMessage(input)
	if output != nil {
		tc.Output = json.RawMessage(output)
	}
	if errMsg != nil {
		tc.Error = *errMsg
	}
	return &tc, nil
}

// toolCallRepo 工具调用审计仓储的 PostgreSQL 实现。
type toolCallRepo struct {
	data *basedata.Data
}

// NewToolCallRepo new a toolCallRepo.
func NewToolCallRepo(data *basedata.Data) *toolCallRepo {
	return &toolCallRepo{data: data}
}

// Create implements biz.ToolCallRepo.
func (r *toolCallRepo) Create(ctx context.Context, tc *biz.ToolCall) error {
	input := tc.Input
	if len(input) == 0 {
		input = json.RawMessage("{}")
	}
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO assistant_tool_call (id, conversation_id, message_id, user_id, call_id, tool_name, title,
		   operation, operation_code, write, input, status, create_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		tc.ID, tc.ConversationID, tc.MessageID, tc.UserID, tc.CallID, tc.ToolName, tc.Title,
		tc.Operation, tc.OperationCode, tc.Write, []byte(input), tc.Status, tc.CreateTime)
	if err != nil {
		return fmt.Errorf("insert assistant tool call: %w", err)
	}
	return nil
}

// ListByConversation implements biz.ToolCallRepo：按发起时间正序。
func (r *toolCallRepo) ListByConversation(ctx context.Context, conversationID string) ([]*biz.ToolCall, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+toolCallSelectColumns+" FROM assistant_tool_call WHERE conversation_id = $1 ORDER BY create_time, id",
		conversationID)
	if err != nil {
		return nil, fmt.Errorf("list assistant tool calls: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.ToolCall, 0, 16)
	for rows.Next() {
		tc, err := scanToolCall(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan assistant tool call: %w", err)
		}
		items = append(items, tc)
	}
	return items, rows.Err()
}

// Transition implements biz.ToolCallRepo：条件状态迁移（幂等）。
func (r *toolCallRepo) Transition(ctx context.Context, id, conversationID, from, to string, at time.Time) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE assistant_tool_call SET status = $4, resolve_time = $5 WHERE id = $1 AND conversation_id = $2 AND status = $3",
		id, conversationID, from, to, at)
	if err != nil {
		return false, fmt.Errorf("transition assistant tool call: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Finish implements biz.ToolCallRepo：写入终态。
func (r *toolCallRepo) Finish(ctx context.Context, tc *biz.ToolCall) error {
	_, err := r.data.Pool.Exec(ctx,
		`UPDATE assistant_tool_call SET status = $2, output = $3, error = $4, duration_ms = $5, resolve_time = $6
		 WHERE id = $1`,
		tc.ID, tc.Status, dbx.NilIfEmptyBytes([]byte(tc.Output)), dbx.NilIfEmpty(tc.Error), tc.DurationMS, tc.ResolveTime)
	if err != nil {
		return fmt.Errorf("finish assistant tool call: %w", err)
	}
	return nil
}
