package data

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.EventRepo   = (*eventRepo)(nil)
	_ biz.EventWriter = (*eventRepo)(nil)
	_ biz.EventTxRepo = (*eventRepo)(nil)
)

// eventRepo 事件仓储 PostgreSQL 实现：查询运行表 notify_event（管道写 /
// core 读，设计 §5.2），core 侧写路径仅 InsertEvent（§5.3：连通性测试 /
// NotificationWriter 直插），不 bump cfg_version（运行表非配置表）。
// payload/recipients 为 jsonb（字节中转：payload 保原始 JSON，recipients
// 反序列化为字符串数组）。
type eventRepo struct {
	data *basedata.Data
}

// NewEventRepo new an eventRepo.
func NewEventRepo(data *basedata.Data) *eventRepo {
	return &eventRepo{data: data}
}

// eventSelectColumns 是事件查询列序，与 scanEvent 对应。
const eventSelectColumns = "id, biz_id, event_type, severity, title, payload, dedup_key, " +
	"group_key, recipients, source, status, publish_state, publish_attempts, next_publish_time, create_time"

// scanEvent 按列序扫描一行事件数据并解码为 DO（recipients 经字节中转
// 反序列化；payload 保留原始 JSON 字节；dedup_key/group_key 可空经二级
// 指针接收 NULL）。
func scanEvent(scan func(dest ...any) error) (*biz.Event, error) {
	var (
		id, bizID, eventType, title string
		payload, recipientsRaw      []byte
		dedupKey, groupKey          *string
		source                      string
		severity, status, pubState  int32
		pubAttempts                 int32
		nextPubTime, createTime     time.Time
	)
	if err := scan(&id, &bizID, &eventType, &severity, &title, &payload, &dedupKey,
		&groupKey, &recipientsRaw, &source, &status, &pubState, &pubAttempts,
		&nextPubTime, &createTime); err != nil {
		return nil, err
	}
	event := &biz.Event{
		ID:              id,
		BizID:           bizID,
		EventType:       eventType,
		Severity:        severity,
		Title:           title,
		Payload:         payload,
		Source:          source,
		Status:          status,
		PublishState:    pubState,
		PublishAttempts: pubAttempts,
		NextPublishTime: nextPubTime,
		CreateTime:      createTime,
	}
	if dedupKey != nil {
		event.DedupKey = *dedupKey
	}
	if groupKey != nil {
		event.GroupKey = *groupKey
	}
	if len(recipientsRaw) > 0 {
		if err := json.Unmarshal(recipientsRaw, &event.Recipients); err != nil {
			return nil, fmt.Errorf("decode recipients: %w", err)
		}
	}
	return event, nil
}

// ListEvents implements biz.EventRepo：四维过滤 + 分页，两条参数化查询；
// 按落库时间倒序（分区裁剪 + idx_notify_event_query）。
func (r *eventRepo) ListEvents(ctx context.Context, filter biz.EventFilter, page, pageSize int64) ([]*biz.Event, int64, error) {
	where := []string{"true"}
	args := make([]any, 0, 5)
	add := func(condition string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(condition, "$"+strconv.Itoa(len(args))))
	}
	if filter.BizID != "" {
		add("biz_id = %s", filter.BizID)
	}
	if filter.EventType != "" {
		add("event_type = %s", filter.EventType)
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
		"SELECT count(*) FROM notify_event WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count events: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + eventSelectColumns + " FROM notify_event WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	events := make([]*biz.Event, 0, pageSize)
	for rows.Next() {
		event, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, event)
	}
	return events, total, rows.Err()
}

// insertEventPlan 生成事件 INSERT 的语句（kit/notifycontract.InsertEventSQL，与管道
// ingress 同一语句；时间列取 DB now()）与实参：可空列空值落 NULL（dedup_key/group_key
// 空串、payload/recipients 空集）。池连接直插（EventWriter）与
// 调用方事务直插（EventTxRepo）共用本计划。
func insertEventPlan(event *biz.Event) (string, []any, error) {
	var dedupKey, groupKey, payload any
	if event.DedupKey != "" {
		dedupKey = event.DedupKey
	}
	if event.GroupKey != "" {
		groupKey = event.GroupKey
	}
	if len(event.Payload) > 0 {
		payload = event.Payload
	}
	var recipients any
	if len(event.Recipients) > 0 {
		raw, err := json.Marshal(event.Recipients)
		if err != nil {
			return "", nil, fmt.Errorf("encode recipients: %w", err)
		}
		recipients = raw
	}
	return notifycontract.InsertEventSQL, []any{
		event.ID, event.BizID, event.EventType, event.Severity, event.Title, payload,
		dedupKey, groupKey, recipients, event.Source, event.Status, event.PublishState,
		event.PublishAttempts,
	}, nil
}

// InsertEvent implements biz.EventWriter：core 对 notify_event 的唯一写路径
// 仅 INSERT（设计 §5.3），无 UPDATE/DELETE——落库后状态归管道状态机推进。
func (r *eventRepo) InsertEvent(ctx context.Context, event *biz.Event) error {
	sql, args, err := insertEventPlan(event)
	if err != nil {
		return err
	}
	if _, err := r.data.Pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// GateDedupTx implements biz.EventTxRepo：与管道 ingress 同一单语句 upsert——
// 过期行原地替换为本次事件，有效期内命中返回首次 event_id；DO UPDATE 行锁同时
// 挡住并发清理。
func (r *eventRepo) GateDedupTx(ctx context.Context, tx txrun.Tx, bizID, dedupKey, eventID string, ttl time.Duration) (string, error) {
	var winner string
	if err := tx.QueryRow(ctx, notifycontract.DedupGateSQL,
		bizID, dedupKey, eventID, ttl).Scan(&winner); err != nil {
		return "", fmt.Errorf("dedup gate: %w", err)
	}
	return winner, nil
}

// InsertEventTx implements biz.EventTxRepo：与 InsertEvent 同一 INSERT 计划，
// 但经调用方事务句柄执行——业务写与事件写同事务原子提交（设计 §11，
// NotificationWriter 链路）；提交/回滚归事务属主管辖。
func (r *eventRepo) InsertEventTx(ctx context.Context, tx txrun.Tx, event *biz.Event) error {
	sql, args, err := insertEventPlan(event)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("insert event in tx: %w", err)
	}
	return nil
}
