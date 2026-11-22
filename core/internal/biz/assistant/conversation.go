package biz

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/kit/paging"
)

// ConversationUsecase 会话管理用例：列表 / 历史 / 删除，一律限定本人。
type ConversationUsecase struct {
	convs ConversationRepo
	msgs  MessageRepo
	calls ToolCallRepo
}

// NewConversationUsecase new a ConversationUsecase.
func NewConversationUsecase(convs ConversationRepo, msgs MessageRepo, calls ToolCallRepo) *ConversationUsecase {
	return &ConversationUsecase{convs: convs, msgs: msgs, calls: calls}
}

// List 分页查询本人会话（最近活动倒序）。
func (uc *ConversationUsecase) List(ctx context.Context, userID string, page, pageSize int64) ([]*Conversation, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.convs.List(ctx, userID, page, pageSize)
}

// historyDisplayLimit 历史查询一次返回的消息条数上限（最近的这么多条）：绝大多数会话远小于它，
// 行为与返回全量一致；极长的会话不再一次读出全部消息与工具输出。
const historyDisplayLimit = 1000

// History 返回本人会话的消息（至多最近 historyDisplayLimit 条）与对应的工具调用记录
// （service 层聚合为展示轮次）。
func (uc *ConversationUsecase) History(ctx context.Context, id, userID string) ([]*Message, []*ToolCall, error) {
	if _, err := uc.convs.Get(ctx, id, userID); err != nil {
		return nil, nil, err
	}
	msgs, err := uc.msgs.ListRecent(ctx, id, historyDisplayLimit)
	if err != nil {
		return nil, nil, err
	}
	// 历史被截到最近一段时，审计记录也只取这一段时间内的。
	var since time.Time
	if len(msgs) >= historyDisplayLimit {
		since = msgs[0].CreateTime
	}
	calls, err := uc.calls.ListSince(ctx, id, since)
	if err != nil {
		return nil, nil, err
	}
	return msgs, calls, nil
}

// Delete 软删本人会话（消息与审计记录保留）。
func (uc *ConversationUsecase) Delete(ctx context.Context, id, userID string) error {
	return uc.convs.SoftDelete(ctx, id, userID, time.Now())
}
