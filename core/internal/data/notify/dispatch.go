package data

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

// 编译期断言锁住 biz 契约。
var _ biz.DispatchRepo = (*dispatchRepo)(nil)

// dispatchRepo 发送任务仓储 PostgreSQL 实现：只读查询运行表 notify_dispatch
// （管道写 / core 读，设计 §5.2），无写路径、不 bump cfg_version。content 为
// jsonb（保留原始 JSON 字节由 service 层解码）；template_id/next_retry_time/
// last_error 可空，经二级指针接收 NULL。
type dispatchRepo struct {
	data *basedata.Data
}

// NewDispatchRepo new a dispatchRepo.
func NewDispatchRepo(data *basedata.Data) *dispatchRepo {
	return &dispatchRepo{data: data}
}

// dispatchSelectColumns 是发送任务查询列序，与 scanDispatch 对应。
const dispatchSelectColumns = "id, event_id, channel_id, recipient, template_id, content, " +
	"status, attempts, next_retry_time, last_error, send_key, create_time"

// scanDispatch 按列序扫描一行发送任务数据并解码为 DO（content 保留原始
// JSON 字节；可空列 template_id/next_retry_time/last_error 经二级指针）。
func scanDispatch(scan func(dest ...any) error) (*biz.Dispatch, error) {
	var (
		id, eventID, channelID, recipient string
		templateID, lastError             *string
		content                           []byte
		status, attempts                  int32
		nextRetry                         *time.Time
		sendKey                           string
		createTime                        time.Time
	)
	if err := scan(&id, &eventID, &channelID, &recipient, &templateID, &content,
		&status, &attempts, &nextRetry, &lastError, &sendKey, &createTime); err != nil {
		return nil, err
	}
	dispatch := &biz.Dispatch{
		ID:         id,
		EventID:    eventID,
		ChannelID:  channelID,
		Recipient:  recipient,
		Content:    content,
		Status:     status,
		Attempts:   attempts,
		NextRetry:  nextRetry,
		SendKey:    sendKey,
		CreateTime: createTime,
	}
	if templateID != nil {
		dispatch.TemplateID = *templateID
	}
	if lastError != nil {
		dispatch.LastError = *lastError
	}
	return dispatch, nil
}

// ListDispatches implements biz.DispatchRepo：四维过滤 + 分页，两条参数化
// 查询；按生成时间倒序（分区裁剪 + idx_notify_dispatch_query）。
func (r *dispatchRepo) ListDispatches(ctx context.Context, filter biz.DispatchFilter, page, pageSize int64) ([]*biz.Dispatch, int64, error) {
	where := []string{"true"}
	args := make([]any, 0, 5)
	add := func(condition string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(condition, "$"+strconv.Itoa(len(args))))
	}
	if filter.EventID != "" {
		add("event_id = %s", filter.EventID)
	}
	if filter.ChannelID != "" {
		add("channel_id = %s", filter.ChannelID)
	}
	if filter.Status != nil {
		add("status = %s", *filter.Status)
	}
	if filter.CreateTimeStart != nil {
		add("create_time >= %s", *filter.CreateTimeStart)
	}
	if filter.CreateTimeEnd != nil {
		add("create_time <= %s", *filter.CreateTimeEnd)
	}
	whereClause := strings.Join(where, " AND ")

	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_dispatch WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count dispatches: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + dispatchSelectColumns + " FROM notify_dispatch WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list dispatches: %w", err)
	}
	defer rows.Close()

	dispatches := make([]*biz.Dispatch, 0, pageSize)
	for rows.Next() {
		dispatch, err := scanDispatch(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan dispatch: %w", err)
		}
		dispatches = append(dispatches, dispatch)
	}
	return dispatches, total, rows.Err()
}
