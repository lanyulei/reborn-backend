package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.ExecutionReader    = (*executionRepo)(nil)
	_ biz.ExecutionLogReader = (*executionRepo)(nil)
	_ biz.ExecutionWriter    = (*executionRepo)(nil)
)

// logBatch 读日志时每批取的块数（单块至多 256KB）。
const logBatch = 8

// executionColumns 执行记录查询列序。领取令牌（lease_token）不读出：它只在引擎与节点之间使用。
const executionColumns = `e.id::text, e.kind, COALESCE(e.template_id::text, ''), e.template_name, e.template_version, e.trigger,
	e.trigger_ref, e.creator::text, COALESCE(e.request_id, ''), e.status, e.spec, e.runner_zone, e.exclusive, e.timeout_s,
	e.host_total, e.host_ok, e.host_failed, e.host_unreachable, e.commit_id, e.error,
	e.cancel_requested, COALESCE(e.cancel_user::text, ''), COALESCE(e.runner_id::text, ''),
	e.log_seq, e.log_truncated, e.create_time, e.start_time, e.finish_time,
	COALESCE(e.group_id::text, ''), e.slice_no, e.slice_total, e.approval_reason, COALESCE(e.approval_instance_id::text, ''), e.risk_hits`

func scanExecution(scan func(dest ...any) error) (*biz.Execution, error) {
	var e biz.Execution
	var spec, hits []byte
	if err := scan(&e.ID, &e.Kind, &e.TemplateID, &e.TemplateName, &e.TemplateVersion, &e.Trigger,
		&e.TriggerRef, &e.Creator, &e.RequestID, &e.Status, &spec, &e.RunnerZone, &e.Exclusive, &e.TimeoutS,
		&e.HostTotal, &e.HostOK, &e.HostFailed, &e.HostUnreachable, &e.CommitID, &e.Error,
		&e.CancelRequested, &e.CancelUser, &e.RunnerID,
		&e.LogSeq, &e.LogTruncated, &e.CreateTime, &e.StartTime, &e.FinishTime,
		&e.GroupID, &e.SliceNo, &e.SliceTotal, &e.ApprovalReason, &e.ApprovalInstanceID, &hits); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(spec, &e.Spec); err != nil {
		return nil, fmt.Errorf("decode job execution spec: %w", err)
	}
	if err := json.Unmarshal(hits, &e.RiskHits); err != nil {
		return nil, fmt.Errorf("decode job execution risk hits: %w", err)
	}
	return &e, nil
}

type executionRepo struct {
	data *basedata.Data
}

// NewExecutionRepo new an executionRepo.
func NewExecutionRepo(data *basedata.Data) *executionRepo { return &executionRepo{data: data} }

// truncateRunes 按字符截断（落库列为 varchar(n)）。
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// pgxTx 取回事务句柄背后的 pgx.Tx。
func pgxTx(tx biz.Tx) (pgx.Tx, error) {
	ptx, ok := basedata.PgxTx(tx)
	if !ok {
		return nil, errors.New("job data: transaction handle was not issued by the data layer")
	}
	return ptx, nil
}

// CreateTx implements biz.ExecutionWriter。建单、主机快照、文件内容同在调用方事务内。
//
// 幂等：第一片带 request_id 时先查同一执行人的同键执行，命中即返回；并发的重复提交由部分唯一索引
// uk_job_execution_request 兜底（ON CONFLICT DO NOTHING 后再查一次）。
// 互斥：模板禁止并发时，部分唯一索引 uk_job_execution_exclusive 保证同一模板至多一条未结束
// （待审批 / 排队中 / 执行中）的执行，多实例下无需应用层加锁。
func (r *executionRepo) CreateTx(ctx context.Context, btx biz.Tx, items []biz.NewExecution) (string, bool, error) {
	tx, err := pgxTx(btx)
	if err != nil {
		return "", false, err
	}
	if len(items) == 0 {
		return "", false, errors.New("insert job execution: nothing to insert")
	}
	head := items[0].Execution
	findExisting := func() (string, error) {
		var existing string
		err := tx.QueryRow(ctx, "SELECT id::text FROM job_execution WHERE creator = $1 AND request_id = $2", head.Creator, head.RequestID).Scan(&existing)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("find job execution by request id: %w", err)
		}
		return existing, nil
	}
	if head.RequestID != "" {
		if existing, err := findExisting(); err != nil || existing != "" {
			return existing, false, err
		}
	}
	for i, it := range items {
		inserted, err := r.insertOne(ctx, tx, it)
		if err != nil {
			return "", false, err
		}
		if !inserted {
			if i != 0 {
				return "", false, errors.New("insert job execution: unexpected conflict on a later slice")
			}
			// 并发的同键提交先一步落库：返回那一条。
			existing, err := findExisting()
			if err == nil && existing == "" {
				err = errors.New("insert job execution: conflicting row vanished")
			}
			return existing, false, err
		}
	}
	return head.ID, true, nil
}

// insertOne 插入一条执行及其主机快照、文件内容；幂等键冲突返回 inserted = false。
func (r *executionRepo) insertOne(ctx context.Context, tx pgx.Tx, it biz.NewExecution) (bool, error) {
	e, hosts := it.Execution, it.Hosts
	spec, err := json.Marshal(e.Spec)
	if err != nil {
		return false, fmt.Errorf("encode job execution spec: %w", err)
	}
	if e.RiskHits == nil {
		e.RiskHits = []jc.RiskHit{}
	}
	hits, err := json.Marshal(e.RiskHits)
	if err != nil {
		return false, fmt.Errorf("encode job execution risk hits: %w", err)
	}
	// 互斥位冲突（唯一约束）会让调用方的整个事务失败并回滚——此时还没有发起审批流程，没有要保留的东西。
	tag, err := tx.Exec(ctx,
		`INSERT INTO job_execution (id, kind, template_id, template_name, template_version, trigger, trigger_ref, creator,
		   request_id, status, spec, runner_zone, exclusive, timeout_s, host_total,
		   group_id, slice_no, slice_total, approval_reason, risk_hits)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
		 ON CONFLICT (creator, request_id) WHERE request_id IS NOT NULL DO NOTHING`,
		e.ID, e.Kind, dbx.NilIfEmpty(e.TemplateID), truncateRunes(e.TemplateName, 64), e.TemplateVersion, e.Trigger, e.TriggerRef, e.Creator,
		dbx.NilIfEmpty(e.RequestID), e.Status, spec, e.RunnerZone, e.Exclusive, e.TimeoutS, len(hosts),
		dbx.NilIfEmpty(e.GroupID), e.SliceNo, e.SliceTotal, e.ApprovalReason, hits)
	if err != nil {
		if isUniqueViolation(err, "uk_job_execution_exclusive") {
			return false, biz.ErrExecutionConflict
		}
		return false, fmt.Errorf("insert job execution: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	seqs := make([]int32, len(hosts))
	ciIDs, names, envs, addrs := make([]string, len(hosts)), make([]string, len(hosts)), make([]string, len(hosts)), make([]string, len(hosts))
	for i, h := range hosts {
		seqs[i], ciIDs[i], addrs[i] = int32(i+1), h.CIID, h.Address
		names[i], envs[i] = truncateRunes(h.Name, 255), truncateRunes(h.Env, 64)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO job_execution_host (execution_id, seq, ci_id, name, env, address, port)
		 SELECT $1, h.seq, h.ci_id, h.name, h.env, h.address, $7
		 FROM unnest($2::int[], $3::uuid[], $4::text[], $5::text[], $6::text[]) AS h(seq, ci_id, name, env, address)`,
		e.ID, seqs, ciIDs, names, envs, addrs, e.Spec.Options.Port); err != nil {
		return false, fmt.Errorf("insert job execution hosts: %w", err)
	}
	if e.Kind == jc.KindFile {
		if _, err := tx.Exec(ctx, "INSERT INTO job_execution_payload (execution_id, content) VALUES ($1, $2)", e.ID, it.Payload); err != nil {
			return false, fmt.Errorf("insert job execution payload: %w", err)
		}
	}
	return true, nil
}

// approvalScope 审批的业务对象 → 执行行的条件：执行 ID 或分组 ID。
const approvalScope = "(id = $1 OR group_id = $1)"

// SetApprovalTx implements biz.ExecutionWriter.
func (r *executionRepo) SetApprovalTx(ctx context.Context, tx biz.Tx, businessID, instanceID string) error {
	if _, err := tx.Exec(ctx, "UPDATE job_execution SET approval_instance_id = $2 WHERE "+approvalScope, businessID, instanceID); err != nil {
		return fmt.Errorf("set job execution approval: %w", err)
	}
	return nil
}

// ResolveApprovalTx implements biz.ExecutionWriter。只动仍是待审批的行：已被取消的执行不受审批结果影响。
func (r *executionRepo) ResolveApprovalTx(ctx context.Context, tx biz.Tx, businessID string, approved bool, reason string) (int64, error) {
	// 先按 ID 顺序锁住整组：与撤回（Cancel，同样的顺序）并发时不会互相死锁。
	if _, err := tx.Exec(ctx, "SELECT 1 FROM job_execution WHERE "+approvalScope+" ORDER BY id FOR UPDATE", businessID); err != nil {
		return 0, fmt.Errorf("lock job execution group: %w", err)
	}
	var n int64
	var err error
	if approved {
		n, err = tx.Exec(ctx, "UPDATE job_execution SET status = $2 WHERE "+approvalScope+" AND status = $3",
			businessID, jc.StatusQueued, jc.StatusPending)
	} else {
		n, err = tx.Exec(ctx, "UPDATE job_execution SET status = $2, finish_time = now(), error = $4 WHERE "+approvalScope+" AND status = $3",
			businessID, jc.StatusRejected, jc.StatusPending, reason)
	}
	if err != nil {
		return 0, fmt.Errorf("resolve job execution approval: %w", err)
	}
	return n, nil
}

// List implements biz.ExecutionReader.
func (r *executionRepo) List(ctx context.Context, filter biz.ExecutionFilter, page, pageSize int64) ([]*biz.Execution, int64, error) {
	var a sqlArgs
	where := "true"
	if filter.TemplateID != "" {
		where += " AND e.template_id = " + a.add(filter.TemplateID)
	}
	if len(filter.Statuses) > 0 {
		where += " AND e.status = ANY(" + a.add(filter.Statuses) + "::smallint[])"
	}
	if filter.Creator != "" {
		where += " AND e.creator = " + a.add(filter.Creator)
	}
	if filter.Keyword != "" {
		where += " AND e.template_name ILIKE " + a.add(dbx.LikeContains(filter.Keyword))
	}
	if filter.StartTime != nil {
		where += " AND e.create_time >= " + a.add(*filter.StartTime)
	}
	if filter.EndTime != nil {
		where += " AND e.create_time < " + a.add(*filter.EndTime)
	}
	if len(filter.Kinds) > 0 {
		where += " AND e.kind = ANY(" + a.add(filter.Kinds) + "::smallint[])"
	}
	if len(filter.Triggers) > 0 {
		where += " AND e.trigger = ANY(" + a.add(filter.Triggers) + "::smallint[])"
	}
	if filter.GroupID != "" {
		where += " AND e.group_id = " + a.add(filter.GroupID)
	}
	if filter.TriggerRef != "" {
		where += " AND e.trigger_ref = " + a.add(filter.TriggerRef)
	}
	if filter.CIID != "" {
		where += " AND EXISTS (SELECT 1 FROM job_execution_host h WHERE h.ci_id = " + a.add(filter.CIID) + " AND h.execution_id = e.id)"
	}
	if len(filter.CIIDs) > 0 {
		where += " AND EXISTS (SELECT 1 FROM job_execution_host h WHERE h.ci_id = ANY(" + a.add(dbx.TextArray(filter.CIIDs)) + "::uuid[]) AND h.execution_id = e.id)"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM job_execution e WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count job executions: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+executionColumns+" FROM job_execution e WHERE "+where+
		" ORDER BY e.create_time DESC, e.id DESC LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list job executions: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Execution, 0, pageSize)
	for rows.Next() {
		e, err := scanExecution(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan job execution: %w", err)
		}
		items = append(items, e)
	}
	return items, total, rows.Err()
}

// Get implements biz.ExecutionReader.
func (r *executionRepo) Get(ctx context.Context, id string) (*biz.Execution, error) {
	e, err := scanExecution(r.data.Pool.QueryRow(ctx, "SELECT "+executionColumns+" FROM job_execution e WHERE e.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrExecutionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job execution: %w", err)
	}
	return e, nil
}

// ListHosts implements biz.ExecutionReader.
func (r *executionRepo) ListHosts(ctx context.Context, id string, filter biz.ExecutionHostFilter, page, pageSize int64) ([]*biz.ExecutionHost, int64, error) {
	a := sqlArgs{args: []any{id}}
	where := "execution_id = $1"
	if len(filter.Statuses) > 0 {
		where += " AND status = ANY(" + a.add(filter.Statuses) + "::smallint[])"
	}
	if filter.Keyword != "" {
		k := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (name ILIKE " + k + " OR address ILIKE " + k + ")"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM job_execution_host WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count job execution hosts: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx,
		`SELECT seq, ci_id::text, name, env, address, port, status, ok, changed, failed, unreachable, skipped
		 FROM job_execution_host WHERE `+where+" ORDER BY seq LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list job execution hosts: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.ExecutionHost, 0, min(pageSize, 64))
	for rows.Next() {
		var h biz.ExecutionHost
		if err := rows.Scan(&h.Seq, &h.CIID, &h.Name, &h.Env, &h.Address, &h.Port, &h.Status,
			&h.OK, &h.Changed, &h.Failed, &h.Unreachable, &h.Skipped); err != nil {
			return nil, 0, fmt.Errorf("scan job execution host: %w", err)
		}
		items = append(items, &h)
	}
	return items, total, rows.Err()
}

// ReadLog implements biz.ExecutionLogReader。分批读取（每批 logBatch 块），累计内容达到 maxBytes 即止，
// 内存占用有界：至多 maxBytes + 一批。
func (r *executionRepo) ReadLog(ctx context.Context, id string, afterSeq int64, maxBytes int) (string, int64, error) {
	var b strings.Builder
	var lastSeq int64
	cursor := afterSeq
	for b.Len() < maxBytes {
		n, err := r.readLogBatch(ctx, id, cursor, maxBytes, &b, &lastSeq)
		if err != nil {
			return "", 0, err
		}
		if n < logBatch {
			break
		}
		cursor = lastSeq
	}
	return b.String(), lastSeq, nil
}

// readLogBatch 读一批日志块追加到 b，返回读到的块数；累计达到 maxBytes 后本批剩余的块不再追加
// （lastSeq 停在最后追加的那一块，下次从它之后继续）。
func (r *executionRepo) readLogBatch(ctx context.Context, id string, afterSeq int64, maxBytes int, b *strings.Builder, lastSeq *int64) (int, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT seq, content FROM job_execution_log WHERE execution_id = $1 AND seq > $2 ORDER BY seq LIMIT $3", id, afterSeq, logBatch)
	if err != nil {
		return 0, fmt.Errorf("read job execution log: %w", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var seq int64
		var content string
		if err := rows.Scan(&seq, &content); err != nil {
			return 0, fmt.Errorf("scan job execution log: %w", err)
		}
		n++
		if b.Len() >= maxBytes {
			continue // 已达上限：排空本批，不再追加
		}
		b.WriteString(content)
		*lastSeq = seq
	}
	return n, rows.Err()
}

// Cancel implements biz.ExecutionWriter。排队中的直接取消；执行中与结果未知的登记取消请求；其余终态拒绝。
// 行锁后按当前状态处理：与引擎的领取（同样先行锁）互斥，
// 不会出现「已被节点领取却被标成已取消」。
func (r *executionRepo) Cancel(ctx context.Context, id, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var status int32
		var requested bool
		var groupID *string
		// 切片组按 ID 顺序整组加锁（与审批结果落账同一顺序，避免死锁）；锁住之后再读本行的状态。
		if _, err := tx.Exec(ctx,
			`SELECT 1 FROM job_execution WHERE id = $1 OR group_id = (SELECT group_id FROM job_execution WHERE id = $1)
			 ORDER BY id FOR UPDATE`, id); err != nil {
			return fmt.Errorf("lock job execution group: %w", err)
		}
		err := tx.QueryRow(ctx, "SELECT status, cancel_requested, group_id::text FROM job_execution WHERE id = $1 FOR UPDATE", id).Scan(&status, &requested, &groupID)
		if errors.Is(err, pgx.ErrNoRows) {
			return biz.ErrExecutionNotFound
		}
		if err != nil {
			return fmt.Errorf("lock job execution: %w", err)
		}
		switch {
		case status == jc.StatusPending:
			// 待审批：审批是对整次提交（整组切片）的，撤回即整组取消。
			scope := id
			if groupID != nil {
				scope = *groupID
			}
			_, err = tx.Exec(ctx,
				`UPDATE job_execution SET status = $2, cancel_requested = true, cancel_user = $3, finish_time = now()
				 WHERE `+approvalScope+" AND status = $4", scope, jc.StatusCanceled, dbx.NilIfEmpty(actor), jc.StatusPending)
		case status == jc.StatusQueued:
			_, err = tx.Exec(ctx,
				`UPDATE job_execution SET status = $2, cancel_requested = true, cancel_user = $3, finish_time = now()
				 WHERE id = $1`, id, jc.StatusCanceled, dbx.NilIfEmpty(actor))
		case (status == jc.StatusRunning || status == jc.StatusUnknown) && !requested:
			// 结果未知（节点失联）的执行同样登记取消请求、状态不变：作业可能仍在运行，
			// 节点恢复连接后的第一次上报就会得知并终止它。
			_, err = tx.Exec(ctx,
				"UPDATE job_execution SET cancel_requested = true, cancel_user = $2 WHERE id = $1", id, dbx.NilIfEmpty(actor))
		case status == jc.StatusRunning || status == jc.StatusUnknown:
			// 已请求过取消：保持首个请求人，幂等返回。
		default:
			return biz.ErrExecutionFinished
		}
		if err != nil {
			return fmt.Errorf("cancel job execution: %w", err)
		}
		return nil
	})
}
