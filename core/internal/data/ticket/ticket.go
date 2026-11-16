package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.TicketStore  = (*ticketRepo)(nil)
	_ biz.TicketReader = (*ticketRepo)(nil)
	_ biz.CommentRepo  = (*commentRepo)(nil)
)

// ticketRepo 工单仓储的 PostgreSQL 实现。
type ticketRepo struct {
	data *basedata.Data
}

// NewTicketRepo new a ticketRepo.
func NewTicketRepo(data *basedata.Data) *ticketRepo {
	return &ticketRepo{data: data}
}

const ticketColumns = `id, number, template_id, template_name, category_name, title, priority, status,
	creator, creator_dept, instance_id, create_time, end_time, response_due, resolve_due, response_time,
	rating, rating_comment, rating_time`

// scanTicket 扫描工单行；extra 追加在标准列之后（如 form、data）。
func scanTicket(scan func(dest ...any) error, extra ...any) (*biz.Ticket, error) {
	var (
		t                                         biz.Ticket
		priority, status                          int16
		rating                                    *int16
		dept, instance                            *string
		endTime, respDue, resDue, respAt, ratedAt *time.Time
	)
	dest := append([]any{
		&t.ID, &t.Number, &t.TemplateID, &t.TemplateName, &t.CategoryName, &t.Title, &priority, &status,
		&t.Creator, &dept, &instance, &t.CreateTime, &endTime, &respDue, &resDue, &respAt,
		&rating, &t.RatingComment, &ratedAt,
	}, extra...)
	if err := scan(dest...); err != nil {
		return nil, err
	}
	t.Priority, t.Status = int32(priority), int32(status)
	if dept != nil {
		t.CreatorDept = *dept
	}
	if instance != nil {
		t.InstanceID = *instance
	}
	if rating != nil {
		t.Rating = int32(*rating)
	}
	for _, pair := range []struct {
		from *time.Time
		to   *time.Time
	}{{endTime, &t.EndTime}, {respDue, &t.ResponseDue}, {resDue, &t.ResolveDue}, {respAt, &t.ResponseTime}, {ratedAt, &t.RatingTime}} {
		if pair.from != nil {
			*pair.to = *pair.from
		}
	}
	return &t, nil
}

// NextNumberTx implements biz.TicketStore：单语句 upsert 自增——同一前缀同一天的并发提单
// 在这一行上串行，各自拿到不同的序号；事务回滚则自增一并回滚，不留空号。
// 「当天」取数据库会话时区的 current_date（连接串里统一配置），不依赖各实例的进程时区。
func (r *ticketRepo) NextNumberTx(ctx context.Context, tx txrun.Tx, prefix string) (string, int, error) {
	var (
		day string
		seq int
	)
	err := tx.QueryRow(ctx,
		`INSERT INTO ticket_sequence (prefix, day, seq) VALUES ($1, current_date, 1)
		 ON CONFLICT (prefix, day) DO UPDATE SET seq = ticket_sequence.seq + 1
		 RETURNING to_char(day, 'YYYYMMDD'), seq`, prefix).Scan(&day, &seq)
	if err != nil {
		return "", 0, fmt.Errorf("next ticket number: %w", err)
	}
	return day, seq, nil
}

// FindByRequestTx implements biz.TicketStore.
func (r *ticketRepo) FindByRequestTx(ctx context.Context, tx txrun.Tx, creator, requestID string) (*biz.Ticket, error) {
	t, err := scanTicket(tx.QueryRow(ctx,
		"SELECT "+ticketColumns+" FROM ticket_order WHERE creator = $1 AND request_id = $2", creator, requestID).Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("find ticket by request: %w", err)
	}
	return t, nil
}

// CreateTx implements biz.TicketStore.
func (r *ticketRepo) CreateTx(ctx context.Context, tx txrun.Tx, t *biz.Ticket, requestID string) error {
	form, err := json.Marshal(nonNil(t.Form))
	if err != nil {
		return fmt.Errorf("encode ticket form: %w", err)
	}
	data := t.Data
	if data == nil {
		data = map[string]any{}
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("encode ticket data: %w", err)
	}
	escalate, err := json.Marshal(nonNilStrings(t.SLAEscalate))
	if err != nil {
		return fmt.Errorf("encode ticket escalation: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO ticket_order (id, number, template_id, template_name, category_name, title, priority, form, data, status,
			creator, request_id, create_time, update_time, response_due, resolve_due, sla_escalate)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13, $14, $15, $16)`,
		t.ID, t.Number, t.TemplateID, t.TemplateName, t.CategoryName, t.Title, t.Priority, form, raw, t.Status,
		t.Creator, dbx.NilIfEmpty(requestID), t.CreateTime, nullTime(t.ResponseDue), nullTime(t.ResolveDue), escalate); err != nil {
		if uniqueViolation(err, "uk_ticket_order_request") {
			return biz.ErrDuplicateRequest // 同一幂等键被并发提交抢先：用例回读首张工单
		}
		return fmt.Errorf("insert ticket: %w", err)
	}
	return nil
}

// SyncTx implements biz.TicketStore：状态恒写；其余列入参为空 / 零值时保持原值。
func (r *ticketRepo) SyncTx(ctx context.Context, tx txrun.Tx, id string, sync biz.FlowSync) error {
	if _, err := tx.Exec(ctx,
		`UPDATE ticket_order SET status = $2, end_time = COALESCE($3, end_time), instance_id = COALESCE($4, instance_id),
			creator_dept = COALESCE($5, creator_dept), update_time = now()
		 WHERE id = $1 AND (instance_id IS NULL OR $4::uuid IS NULL OR instance_id = $4::uuid)`,
		id, sync.Status, nullTime(sync.EndTime), dbx.NilIfEmpty(sync.InstanceID), dbx.NilIfEmpty(sync.CreatorDept)); err != nil {
		return fmt.Errorf("sync ticket: %w", err)
	}
	return nil
}

// Get implements biz.TicketReader.
func (r *ticketRepo) Get(ctx context.Context, id string) (*biz.Ticket, error) {
	var form, data []byte
	t, err := scanTicket(r.data.Pool.QueryRow(ctx,
		"SELECT "+ticketColumns+", form, data FROM ticket_order WHERE id = $1", id).Scan, &form, &data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrTicketNotFound
		}
		return nil, fmt.Errorf("get ticket: %w", err)
	}
	if err := json.Unmarshal(form, &t.Form); err != nil {
		return nil, fmt.Errorf("decode ticket form: %w", err)
	}
	if err := json.Unmarshal(data, &t.Data); err != nil {
		return nil, fmt.Errorf("decode ticket data: %w", err)
	}
	return t, nil
}

// List implements biz.TicketReader：条件过滤 + 分页，两条参数化查询。
func (r *ticketRepo) List(ctx context.Context, filter biz.TicketFilter, page, pageSize int64) ([]*biz.Ticket, int64, error) {
	where := []string{"true"}
	args := make([]any, 0, 8)
	add := func(cond string, value any) {
		args = append(args, value)
		where = append(where, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if filter.Keyword != "" {
		add("(title ILIKE ? OR number ILIKE ?)", dbx.LikeContains(filter.Keyword))
	}
	if filter.Status != nil {
		add("status = ?", *filter.Status)
	}
	if filter.TemplateID != "" {
		add("template_id = ?", filter.TemplateID)
	}
	if filter.Priority != nil {
		add("priority = ?", *filter.Priority)
	}
	if filter.Creator != "" {
		add("creator = ?", filter.Creator)
	}
	if filter.ResourceID != "" {
		add("EXISTS (SELECT 1 FROM ticket_resource r WHERE r.ticket_id = ticket_order.id AND r.ci_id = ?)", filter.ResourceID)
	}
	if filter.Overdue {
		where = append(where, overdueCondition)
	}
	whereClause := strings.Join(where, " AND ")
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM ticket_order WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count tickets: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+ticketColumns+" FROM ticket_order WHERE "+whereClause+
		" ORDER BY create_time DESC, id DESC LIMIT $"+strconv.Itoa(len(args)-1)+" OFFSET $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list tickets: %w", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Ticket, error) { return scanTicket(row.Scan) })
	if err != nil {
		return nil, 0, fmt.Errorf("scan ticket: %w", err)
	}
	return items, total, nil
}

// SLA 口径（列表筛选与统计共用）：
//   - 响应：已响应的看响应时间；没人响应就正常结束（完成 / 拒绝）的看结束时间；仍在处理的看当前时间。
//   - 解决：正常结束的看结束时间；仍在处理的看当前时间。
//   - 被撤回 / 终止的工单不计入（既不算达成，也不算超时）。
const (
	responseAt   = `COALESCE(response_time, CASE WHEN status = 1 THEN now() WHEN status IN (2, 3) THEN end_time END)`
	resolveAt    = `CASE WHEN status = 1 THEN now() WHEN status IN (2, 3) THEN end_time END`
	responseLate = `(response_due IS NOT NULL AND ` + responseAt + ` > response_due)`
	resolveLate  = `(resolve_due IS NOT NULL AND ` + resolveAt + ` > resolve_due)`
	// overdueCondition SLA 已超时（响应或解决任一超时）。
	//
	// 这个条件没有配套索引，是有意为之：把它展开成可走索引的并列分支、再配部分索引的做法实测过
	// （50 万工单、约 6% 超时）——规划器能给出 BitmapOr，但超时的行散落在全表各页，精确计数仍要
	// 回表读遍几乎所有数据页，反而比并行顺序扫描慢（106ms 对 40ms）。「仍在处理且时限已过」
	// 又随当前时间变化，无法物化成一列。在保持精确总数的前提下，顺序扫描就是最优计划。
	overdueCondition = `(` + responseLate + ` OR ` + resolveLate + `)`
)

// commentRepo 工单评论仓储的 PostgreSQL 实现。
type commentRepo struct {
	data *basedata.Data
}

// NewCommentRepo new a commentRepo.
func NewCommentRepo(data *basedata.Data) *commentRepo {
	return &commentRepo{data: data}
}

// Add implements biz.CommentRepo：先按评论人加事务级咨询锁、再锁工单行，然后计数——同一个人
// （跨工单、跨实例）与同一张工单的并发评论都串行，两项上限不会被冲破。加锁顺序固定为
// 「人 → 工单」。工单行用 FOR NO KEY UPDATE：与流程回写工单状态互斥（都很短），不阻塞外键检查。
// 限流按数据库时钟统计，各实例时钟不一致不影响结果。
func (r *commentRepo) Add(ctx context.Context, c *biz.Comment, limit, perMinute int) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var count int
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('ticket_comment:' || $1::text, 0))", c.UserID); err != nil {
			return fmt.Errorf("lock commenter: %w", err)
		}
		if err := tx.QueryRow(ctx,
			// statement_timestamp()（本条语句开始的时刻）而不是 clock_timestamp()：后者每行求值、
			// 不能作索引范围条件，会把此人的全部评论都读出来过滤。本条语句在拿到咨询锁之后才
			// 开始，等锁的时间同样不计入。
			"SELECT count(*) FROM ticket_comment WHERE user_id = $1 AND create_time > statement_timestamp() - interval '1 minute'", c.UserID).Scan(&count); err != nil {
			return fmt.Errorf("count recent comments: %w", err)
		}
		if count >= perMinute {
			return biz.ErrCommentRateLimited
		}
		if err := tx.QueryRow(ctx, "SELECT true FROM ticket_order WHERE id = $1 FOR NO KEY UPDATE", c.TicketID).Scan(new(bool)); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return biz.ErrTicketNotFound
			}
			return fmt.Errorf("lock ticket: %w", err)
		}
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM ticket_comment WHERE ticket_id = $1", c.TicketID).Scan(&count); err != nil {
			return fmt.Errorf("count ticket comments: %w", err)
		}
		if count >= limit {
			return biz.ErrLimitExceeded
		}
		// 回复：被回复的须是本工单的顶层评论（评论只追加不删除，工单行已锁，检查后不会失效）。
		var parent *string
		if c.ParentID != "" {
			var top bool
			err := tx.QueryRow(ctx, "SELECT parent_id IS NULL FROM ticket_comment WHERE id = $1 AND ticket_id = $2", c.ParentID, c.TicketID).Scan(&top)
			if errors.Is(err, pgx.ErrNoRows) || (err == nil && !top) {
				return biz.ErrCommentParentInvalid
			}
			if err != nil {
				return fmt.Errorf("check parent comment: %w", err)
			}
			parent = &c.ParentID
		}
		// 评论时间取数据库的当前时钟（不是事务开始时刻：等锁的时间不应算在内），与限流统计同一口径。
		if err := tx.QueryRow(ctx,
			`INSERT INTO ticket_comment (id, ticket_id, user_id, content, create_time, parent_id)
			 VALUES ($1, $2, $3, $4, clock_timestamp(), $5) RETURNING create_time`,
			c.ID, c.TicketID, c.UserID, c.Content, parent).Scan(&c.CreateTime); err != nil {
			return fmt.Errorf("insert ticket comment: %w", err)
		}
		return nil
	})
}

// List implements biz.CommentRepo.
func (r *commentRepo) List(ctx context.Context, ticketID string, limit int) ([]*biz.Comment, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT id, user_id, content, create_time, coalesce(parent_id::text, '') FROM ticket_comment WHERE ticket_id = $1 ORDER BY id LIMIT $2", ticketID, limit)
	if err != nil {
		return nil, fmt.Errorf("list ticket comments: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Comment, error) {
		c := biz.Comment{TicketID: ticketID}
		return &c, row.Scan(&c.ID, &c.UserID, &c.Content, &c.CreateTime, &c.ParentID)
	})
	if err != nil {
		return nil, fmt.Errorf("scan ticket comment: %w", err)
	}
	return out, nil
}
