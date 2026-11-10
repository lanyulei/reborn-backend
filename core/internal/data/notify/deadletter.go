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
var _ biz.DeadLetterRepo = (*deadLetterRepo)(nil)

// deadLetterRepo 死信仓储 PostgreSQL 实现：读列表 + 唯一写动作（标记重放）。
// notify_dlq 量小不分区（设计 §5.2）；标记重放是条件 UPDATE
// （WHERE replay_state = 1，§4.7 状态机），并发标记只有一方成功；core 不
// bump cfg_version（运行表非配置表，dlq worker 轮询本表）。last_error 可
// 空，经二级指针接收 NULL。
type deadLetterRepo struct {
	data *basedata.Data
}

// NewDeadLetterRepo new a deadLetterRepo.
func NewDeadLetterRepo(data *basedata.Data) *deadLetterRepo {
	return &deadLetterRepo{data: data}
}

// deadLetterSelectColumns 是死信查询列序，与 scanDeadLetter 对应。
const deadLetterSelectColumns = "id, dispatch_id, event_id, channel_id, recipient, " +
	"last_error, attempts, replay_state, create_time, update_time"

// scanDeadLetter 按列序扫描一行死信数据并解码为 DO（可空列 last_error
// 经二级指针）。
func scanDeadLetter(scan func(dest ...any) error) (*biz.DeadLetter, error) {
	var (
		id, dispatchID, eventID, channelID, recipient string
		lastError                                     *string
		attempts, replayState                         int32
		createTime, updateTime                        time.Time
	)
	if err := scan(&id, &dispatchID, &eventID, &channelID, &recipient,
		&lastError, &attempts, &replayState, &createTime, &updateTime); err != nil {
		return nil, err
	}
	letter := &biz.DeadLetter{
		ID:          id,
		DispatchID:  dispatchID,
		EventID:     eventID,
		ChannelID:   channelID,
		Recipient:   recipient,
		Attempts:    attempts,
		ReplayState: replayState,
		CreateTime:  createTime,
		UpdateTime:  updateTime,
	}
	if lastError != nil {
		letter.LastError = *lastError
	}
	return letter, nil
}

// ListDeadLetters implements biz.DeadLetterRepo：四维过滤 + 分页，两条参数化
// 查询；按转入时间倒序（稳定排序补 id DESC）。
func (r *deadLetterRepo) ListDeadLetters(ctx context.Context, filter biz.DeadLetterFilter, page, pageSize int64) ([]*biz.DeadLetter, int64, error) {
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
	if filter.ReplayState != nil {
		add("replay_state = %s", *filter.ReplayState)
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
		"SELECT count(*) FROM notify_dlq WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count dead letters: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + deadLetterSelectColumns + " FROM notify_dlq WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()

	letters := make([]*biz.DeadLetter, 0, pageSize)
	for rows.Next() {
		letter, err := scanDeadLetter(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan dead letter: %w", err)
		}
		letters = append(letters, letter)
	}
	return letters, total, rows.Err()
}

// MarkReplayPending implements biz.DeadLetterRepo：条件 UPDATE 保证状态机
// （WHERE replay_state = 1，并发标记只有一方成功），单语句 RETURNING 取
// 权威行；0 行命中时区分「不存在」与「状态不满足」。
func (r *deadLetterRepo) MarkReplayPending(ctx context.Context, id string) (*biz.DeadLetter, error) {
	letter, err := r.queryOne(ctx,
		"UPDATE notify_dlq SET replay_state = 2, update_time = now() "+
			"WHERE id = $1 AND replay_state = 1 RETURNING "+deadLetterSelectColumns, id)
	if err != nil {
		return nil, err
	}
	if letter != nil {
		return letter, nil
	}
	// 0 行：行不存在 → 404；存在但状态非 none → 409。
	existing, err := r.queryOne(ctx,
		"SELECT "+deadLetterSelectColumns+" FROM notify_dlq WHERE id = $1", id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, biz.ErrDeadLetterNotFound
	}
	return nil, fmt.Errorf("%w: replay_state=%d（仅 %d 可标记重放）",
		biz.ErrDeadLetterInvalidState, existing.ReplayState, biz.ReplayStateNone)
}

// GetDeadLetter implements biz.DeadLetterRepo。
func (r *deadLetterRepo) GetDeadLetter(ctx context.Context, id string) (*biz.DeadLetter, error) {
	letter, err := r.queryOne(ctx, "SELECT "+deadLetterSelectColumns+" FROM notify_dlq WHERE id = $1", id)
	if err != nil {
		return nil, err
	}
	if letter == nil {
		return nil, biz.ErrDeadLetterNotFound
	}
	return letter, nil
}

// queryOne 执行单行查询并扫描；pgx.ErrNoRows 归一为 nil, nil。
func (r *deadLetterRepo) queryOne(ctx context.Context, query string, args ...any) (*biz.DeadLetter, error) {
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query dead letter: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	letter, err := scanDeadLetter(rows.Scan)
	if err != nil {
		return nil, fmt.Errorf("scan dead letter: %w", err)
	}
	return letter, rows.Err()
}
