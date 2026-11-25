package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.InstanceStore      = (*instanceRepo)(nil)
	_ biz.InstanceReader     = (*instanceRepo)(nil)
	_ biz.ParticipantChecker = (*instanceRepo)(nil)
)

// instanceRepo 流程实例执行状态仓储的 PostgreSQL 实现。
type instanceRepo struct {
	data *basedata.Data
}

// NewInstanceRepo new an instanceRepo.
func NewInstanceRepo(data *basedata.Data) *instanceRepo {
	return &instanceRepo{data: data}
}

// errForeignTx 事务句柄不是本进程 TxManager 签发的（装配错误，不应发生）。
var errForeignTx = errors.New("workflow: transaction handle was not issued by the data layer")

// queryer Pool 与 Tx 共有的查询面。
type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const instanceColumns = `id, definition_id, definition_key, definition_name, version_id, version, business_type, business_id,
	title, initiator, initiator_dept, status, start_time, end_time, parent_instance_id, parent_node_id, parent_token_id, depth, client_id,
	root_id, root_business_type, root_business_id, auto_skips, form_key`

// scanInstance 扫描实例行；extra 追加在标准列之后（如 variables）。
func scanInstance(scan func(dest ...any) error, in *biz.Instance, extra ...any) error {
	var (
		status, depth               int16
		dept, parent, token, client *string
		endTime                     *time.Time
	)
	dest := append([]any{
		&in.ID, &in.DefinitionID, &in.DefinitionKey, &in.DefinitionName, &in.VersionID, &in.Version,
		&in.BusinessType, &in.BusinessID, &in.Title, &in.Initiator, &dept, &status, &in.StartTime, &endTime,
		&parent, &in.ParentNodeID, &token, &depth, &client,
		&in.RootID, &in.RootBusinessType, &in.RootBusinessID, &in.AutoSkips, &in.FormKey,
	}, extra...)
	if err := scan(dest...); err != nil {
		return err
	}
	in.Status, in.Depth = int32(status), int32(depth)
	if dept != nil {
		in.InitiatorDept = *dept
	}
	if parent != nil {
		in.ParentInstanceID = *parent
	}
	if token != nil {
		in.ParentTokenID = *token
	}
	if client != nil {
		in.ClientID = *client
	}
	if endTime != nil {
		in.EndTime = *endTime
	}
	return nil
}

// nullTime 零值时间落 NULL。
func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// load 读出实例的完整执行状态；lock 时锁住实例行（NO KEY UPDATE），withHistory 时带全部历史。
func load(ctx context.Context, q queryer, instanceID string, lock, withHistory bool) (*biz.State, error) {
	s := &biz.State{}
	sql := "SELECT " + instanceColumns + ", variables FROM workflow_instance WHERE id = $1"
	if lock {
		// NO KEY UPDATE 足以让同一实例的命令串行，又不与外键检查（KEY SHARE）冲突：别的实例
		// 往本实例名下排作业（子流程唤醒父流程、父流程终止子流程）时不会被本实例的命令挡住，
		// 父子两个实例的事务因此不会互相等待成环。
		sql += " FOR NO KEY UPDATE"
	}
	var vars []byte
	if err := scanInstance(q.QueryRow(ctx, sql, instanceID).Scan, &s.Instance, &vars); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrInstanceNotFound
		}
		return nil, fmt.Errorf("load workflow instance: %w", err)
	}
	if err := json.Unmarshal(vars, &s.Instance.Variables); err != nil {
		return nil, fmt.Errorf("decode workflow variables: %w", err)
	}

	rows, err := q.Query(ctx, "SELECT id, node_id, status, scope FROM workflow_token WHERE instance_id = $1 ORDER BY id", instanceID)
	if err != nil {
		return nil, fmt.Errorf("load workflow tokens: %w", err)
	}
	s.Tokens, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Token, error) {
		var (
			t      biz.Token
			status int16
		)
		err := row.Scan(&t.ID, &t.NodeID, &status, &t.Scope)
		t.Status = int32(status)
		return &t, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan workflow token: %w", err)
	}

	rows, err = q.Query(ctx,
		`SELECT id, token_id, node_id, node_name, kind, mode, status, create_time, end_time, due_time
		 FROM workflow_task WHERE instance_id = $1 ORDER BY create_time, id`, instanceID)
	if err != nil {
		return nil, fmt.Errorf("load workflow tasks: %w", err)
	}
	byID := map[string]*biz.Task{}
	s.Tasks, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Task, error) {
		var (
			t                biz.Task
			status           int16
			endTime, dueTime *time.Time
		)
		err := row.Scan(&t.ID, &t.TokenID, &t.NodeID, &t.NodeName, &t.Kind, &t.Mode, &status, &t.CreateTime, &endTime, &dueTime)
		t.Status = int32(status)
		if endTime != nil {
			t.EndTime = *endTime
		}
		if dueTime != nil {
			t.DueTime = *dueTime
		}
		byID[t.ID] = &t
		return &t, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan workflow task: %w", err)
	}

	rows, err = q.Query(ctx,
		`SELECT id, task_id, user_id, seq, status, comment, create_time, action_time, origin, owner_id, on_behalf
		 FROM workflow_task_assignee WHERE instance_id = $1 ORDER BY seq, id`, instanceID)
	if err != nil {
		return nil, fmt.Errorf("load workflow assignees: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			a             biz.TaskAssignee
			taskID        string
			status        int16
			actionTime    *time.Time
			owner, behalf *string
		)
		if err := rows.Scan(&a.ID, &taskID, &a.UserID, &a.Seq, &status, &a.Comment, &a.CreateTime, &actionTime, &a.Origin, &owner, &behalf); err != nil {
			return nil, fmt.Errorf("scan workflow assignee: %w", err)
		}
		a.Status = int32(status)
		if owner != nil {
			a.OwnerID = *owner
		}
		if behalf != nil {
			a.OnBehalf = *behalf
		}
		if actionTime != nil {
			a.ActionTime = *actionTime
		}
		if t := byID[taskID]; t != nil {
			t.Assignees = append(t.Assignees, &a)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	if !withHistory {
		return s, nil
	}
	// 详情读路径：带上等待中的定时 / 自动任务作业，以及任何类型的死信（页面据此展示
	// 「在等什么」并让管理员重试 / 跳过）。
	rows, err = q.Query(ctx, "SELECT "+jobColumns+` FROM workflow_job
		WHERE instance_id = $1 AND (status = 4 OR (status = 1 AND kind IN ('timer', 'service'))) ORDER BY create_time, id`, instanceID)
	if err != nil {
		return nil, fmt.Errorf("load workflow jobs: %w", err)
	}
	if s.Jobs, err = pgx.CollectRows(rows, scanJob); err != nil {
		return nil, fmt.Errorf("scan workflow job: %w", err)
	}
	rows, err = q.Query(ctx,
		`SELECT id, node_id, node_name, task_id, action, operator, comment, detail, create_time
		 FROM workflow_history WHERE instance_id = $1 ORDER BY id`, instanceID)
	if err != nil {
		return nil, fmt.Errorf("load workflow history: %w", err)
	}
	s.Histories, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.History, error) {
		var (
			h                biz.History
			taskID, operator *string
			detail           []byte
		)
		if err := row.Scan(&h.ID, &h.NodeID, &h.NodeName, &taskID, &h.Action, &operator, &h.Comment, &detail, &h.CreateTime); err != nil {
			return nil, err
		}
		if taskID != nil {
			h.TaskID = *taskID
		}
		if operator != nil {
			h.Operator = *operator
		}
		return &h, json.Unmarshal(detail, &h.Detail)
	})
	if err != nil {
		return nil, fmt.Errorf("scan workflow history: %w", err)
	}
	return s, nil
}

// Load implements biz.InstanceReader.
func (r *instanceRepo) Load(ctx context.Context, instanceID string) (*biz.State, error) {
	return load(ctx, r.data.Pool, instanceID, false, true)
}

// LoadForUpdateTx implements biz.InstanceStore：锁住实例行，同一实例的写命令串行。
func (r *instanceRepo) LoadForUpdateTx(ctx context.Context, tx txrun.Tx, instanceID string) (*biz.State, error) {
	ptx, ok := basedata.PgxTx(tx)
	if !ok {
		return nil, errForeignTx
	}
	return load(ctx, ptx, instanceID, true, false)
}

// CreateTx implements biz.InstanceStore.
func (r *instanceRepo) CreateTx(ctx context.Context, tx txrun.Tx, s *biz.State, requestID string) error {
	ptx, ok := basedata.PgxTx(tx)
	if !ok {
		return errForeignTx
	}
	in := &s.Instance
	vars, err := json.Marshal(nonNilMap(in.Variables))
	if err != nil {
		return fmt.Errorf("encode workflow variables: %w", err)
	}
	if _, err := ptx.Exec(ctx,
		`INSERT INTO workflow_instance (id, definition_id, version_id, definition_key, definition_name, version, business_type, business_id,
			title, initiator, initiator_dept, status, variables, start_time, end_time, update_time, request_id,
			parent_instance_id, parent_node_id, parent_token_id, depth, client_id,
			root_id, root_business_type, root_business_id, auto_skips, form_key)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $14, $16, $17, $18, $19, $20, $21,
			$22, $23, $24, $25, $26)`,
		in.ID, in.DefinitionID, in.VersionID, in.DefinitionKey, in.DefinitionName, in.Version, in.BusinessType, in.BusinessID,
		in.Title, in.Initiator, dbx.NilIfEmpty(in.InitiatorDept), in.Status, vars, in.StartTime, nullTime(in.EndTime),
		dbx.NilIfEmpty(requestID), dbx.NilIfEmpty(in.ParentInstanceID), in.ParentNodeID, dbx.NilIfEmpty(in.ParentTokenID), in.Depth,
		dbx.NilIfEmpty(in.ClientID), in.RootID, in.RootBusinessType, in.RootBusinessID, in.AutoSkips, in.FormKey); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation &&
			(pgErr.ConstraintName == "uk_workflow_instance_business" || pgErr.ConstraintName == "uk_workflow_instance_request") {
			return biz.ErrBusinessDuplicate
		}
		return fmt.Errorf("insert workflow instance: %w", err)
	}
	return writeState(ctx, ptx, s)
}

// FindByRequestTx implements biz.InstanceStore.
func (r *instanceRepo) FindByRequestTx(ctx context.Context, tx txrun.Tx, initiator, requestID string) (*biz.Instance, error) {
	ptx, ok := basedata.PgxTx(tx)
	if !ok {
		return nil, errForeignTx
	}
	var (
		in   biz.Instance
		vars []byte
	)
	// 连同流程变量一起读：幂等重放返回的实例应与首次发起时返回的一致。
	err := scanInstance(ptx.QueryRow(ctx,
		"SELECT "+instanceColumns+", variables FROM workflow_instance WHERE initiator = $1 AND request_id = $2", initiator, requestID).Scan, &in, &vars)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("find workflow instance by request: %w", err)
	}
	if err := json.Unmarshal(vars, &in.Variables); err != nil {
		return nil, fmt.Errorf("decode workflow variables: %w", err)
	}
	return &in, nil
}

// SaveTx implements biz.InstanceStore.
func (r *instanceRepo) SaveTx(ctx context.Context, tx txrun.Tx, s *biz.State) error {
	ptx, ok := basedata.PgxTx(tx)
	if !ok {
		return errForeignTx
	}
	in := &s.Instance
	if _, err := ptx.Exec(ctx,
		"UPDATE workflow_instance SET status = $2, end_time = $3, auto_skips = $4, update_time = now() WHERE id = $1",
		in.ID, in.Status, nullTime(in.EndTime), in.AutoSkips); err != nil {
		return fmt.Errorf("update workflow instance: %w", err)
	}
	if s.VarsChanged {
		vars, err := json.Marshal(nonNilMap(in.Variables))
		if err != nil {
			return fmt.Errorf("encode workflow variables: %w", err)
		}
		if _, err := ptx.Exec(ctx, "UPDATE workflow_instance SET variables = $2 WHERE id = $1", in.ID, vars); err != nil {
			return fmt.Errorf("update workflow variables: %w", err)
		}
	}
	if in.Status != biz.InstanceRunning {
		// 实例已结束：名下仍待执行的作业与死信一并作废；仍在进行的子流程各排一条终止作业（由执行器
		// 逐个只锁子实例去处理——父子实例从不在同一事务里同时加锁）。须在写入本次新排的
		// 作业（如唤醒父流程）之前执行，它们不属于「遗留」。
		if _, err := ptx.Exec(ctx,
			"UPDATE workflow_job SET status = 3, update_time = now() WHERE instance_id = $1 AND status IN (1, 4)", in.ID); err != nil {
			return fmt.Errorf("cancel workflow jobs: %w", err)
		}
		if _, err := ptx.Exec(ctx,
			`INSERT INTO workflow_job (id, instance_id, kind, due_time)
			 SELECT gen_random_uuid(), id, 'stop', now() FROM workflow_instance WHERE parent_instance_id = $1 AND status = 1`, in.ID); err != nil {
			return fmt.Errorf("enqueue child stop jobs: %w", err)
		}
	}
	return writeState(ctx, ptx, s)
}

func nonNilMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// writeState 以 upsert 批量回写本次命令新增或变化的执行位置 / 任务（连同其名下变化的处理人），
// 并追加本次的历史。全部语句经一个 pgx.Batch 单次往返；实例行锁保证同一实例无并发
// 写入，upsert 不会相互覆盖。只写变化的部分：命令的写入量与实例已有的任务数无关。
func writeState(ctx context.Context, tx pgx.Tx, s *biz.State) error {
	id := s.Instance.ID
	batch := &pgx.Batch{}
	for _, t := range s.ChangedTokens() {
		batch.Queue(`INSERT INTO workflow_token (id, instance_id, node_id, status, scope) VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (id) DO UPDATE SET node_id = EXCLUDED.node_id, status = EXCLUDED.status`,
			t.ID, id, t.NodeID, t.Status, t.Scope)
	}
	for _, t := range s.ChangedTasks() {
		batch.Queue(`INSERT INTO workflow_task (id, instance_id, token_id, node_id, node_name, kind, mode, status, create_time, end_time, due_time)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, end_time = EXCLUDED.end_time, due_time = EXCLUDED.due_time`,
			t.ID, id, t.TokenID, t.NodeID, t.NodeName, t.Kind, t.Mode, t.Status, t.CreateTime, nullTime(t.EndTime), nullTime(t.DueTime))
		if t.Status != biz.TaskPending {
			// 任务已了结：它名下还没到期的超时 / 提醒作业不必留到到期再作废。
			batch.Queue("UPDATE workflow_job SET status = 3, update_time = now() WHERE task_id = $1 AND status = 1", t.ID)
		}
		// 只写变化的席位：upsert 的 DO UPDATE 列与 biz.State 的席位基线字段一一对应，未变化的席位
		// 重写一遍也是原值。
		for _, a := range s.ChangedAssignees(t) {
			batch.Queue(`INSERT INTO workflow_task_assignee (id, task_id, instance_id, user_id, seq, status, comment, create_time, action_time,
					origin, owner_id, on_behalf)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, comment = EXCLUDED.comment, action_time = EXCLUDED.action_time,
					owner_id = EXCLUDED.owner_id`,
				a.ID, t.ID, id, a.UserID, a.Seq, a.Status, a.Comment, a.CreateTime, nullTime(a.ActionTime),
				a.Origin, dbx.NilIfEmpty(a.OwnerID), dbx.NilIfEmpty(a.OnBehalf))
		}
	}
	for _, h := range s.Histories {
		detail, err := json.Marshal(nonNilMap(h.Detail))
		if err != nil {
			return fmt.Errorf("encode workflow history detail: %w", err)
		}
		batch.Queue(`INSERT INTO workflow_history (id, instance_id, node_id, node_name, task_id, action, operator, comment, detail, create_time)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			h.ID, id, h.NodeID, h.NodeName, dbx.NilIfEmpty(h.TaskID), h.Action, dbx.NilIfEmpty(h.Operator), h.Comment, detail, h.CreateTime)
	}
	for _, j := range s.Jobs {
		payload, err := json.Marshal(nonNilMap(j.Payload))
		if err != nil {
			return fmt.Errorf("encode workflow job payload: %w", err)
		}
		owner := id
		if j.InstanceID != "" {
			owner = j.InstanceID
		}
		batch.Queue(`INSERT INTO workflow_job (id, instance_id, kind, node_id, token_id, task_id, payload, due_time)
			VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8, now()))`,
			j.ID, owner, j.Kind, j.NodeID, dbx.NilIfEmpty(j.TokenID), dbx.NilIfEmpty(j.TaskID), payload, nullTime(j.DueTime))
	}
	if batch.Len() == 0 {
		return nil
	}
	results := tx.SendBatch(ctx, batch)
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("write workflow state: %w", err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close workflow state batch: %w", err)
	}
	return nil
}

// IsParticipant implements biz.ParticipantChecker：发起人、持有未被取消的处理 / 抄送席位，
// 或是这类席位的被代理人。席位按「该实例 + 以它为根的各级子流程」找（instanceID 为顶层流程
// 时即整棵流程树；idx_workflow_instance_root 取出树上的实例，再按 (instance_id, user_id) 探查）。
func (r *instanceRepo) IsParticipant(ctx context.Context, instanceID, userID string) (bool, error) {
	var ok bool
	if err := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM workflow_instance WHERE id = $1 AND initiator = $2)
		     OR EXISTS (SELECT 1 FROM workflow_task_assignee WHERE instance_id = $1 AND user_id = $2 AND status <> $3)
		     OR EXISTS (SELECT 1 FROM workflow_task_assignee WHERE instance_id = $1 AND on_behalf = $2 AND status <> $3)
		     OR EXISTS (SELECT 1 FROM workflow_instance i JOIN workflow_task_assignee a ON a.instance_id = i.id
		                WHERE i.root_id = $1 AND i.id <> $1 AND (a.user_id = $2 OR a.on_behalf = $2) AND a.status <> $3)`,
		instanceID, userID, biz.AssigneeCancelled).Scan(&ok); err != nil {
		return false, fmt.Errorf("check workflow participant: %w", err)
	}
	return ok, nil
}

// InstanceIDOfTask implements biz.InstanceReader.
func (r *instanceRepo) InstanceIDOfTask(ctx context.Context, taskID string) (string, error) {
	var id string
	if err := r.data.Pool.QueryRow(ctx, "SELECT instance_id FROM workflow_task WHERE id = $1", taskID).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", biz.ErrTaskNotFound
		}
		return "", fmt.Errorf("get workflow task: %w", err)
	}
	return id, nil
}

// ListInstances implements biz.InstanceReader：条件过滤 + 分页；当前节点名经一次聚合查询回填（无逐行 N+1）。
func (r *instanceRepo) ListInstances(ctx context.Context, filter biz.InstanceFilter, page, pageSize int64) ([]*biz.InstanceItem, int64, error) {
	where := []string{"true"}
	args := make([]any, 0, 6)
	add := func(cond string, value any) {
		args = append(args, value)
		where = append(where, cond+strconv.Itoa(len(args)))
	}
	if filter.Keyword != "" {
		add("title ILIKE $", dbx.LikeContains(filter.Keyword))
	}
	if filter.Status != nil {
		add("status = $", *filter.Status)
	}
	if filter.DefinitionID != "" {
		add("definition_id = $", filter.DefinitionID)
	}
	if filter.Initiator != "" {
		add("initiator = $", filter.Initiator)
	}
	switch filter.BusinessType {
	case "":
	case biz.InstanceFilterNoBusiness:
		add("business_type = $", "")
	default:
		add("business_type = $", filter.BusinessType)
	}
	if len(filter.BusinessIDs) > 0 {
		add("business_id = ANY($", dbx.TextArray(filter.BusinessIDs))
		where[len(where)-1] += ")"
	}
	if filter.StartAfter != nil {
		add("start_time >= $", *filter.StartAfter)
	}
	if filter.StartBefore != nil {
		add("start_time < $", *filter.StartBefore)
	}
	whereClause := strings.Join(where, " AND ")
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM workflow_instance WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count workflow instances: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+instanceColumns+" FROM workflow_instance WHERE "+whereClause+
		" ORDER BY start_time DESC, id DESC LIMIT $"+strconv.Itoa(len(args)-1)+" OFFSET $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list workflow instances: %w", err)
	}
	byID := make(map[string]*biz.InstanceItem, pageSize)
	ids := make([]string, 0, pageSize)
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.InstanceItem, error) {
		var it biz.InstanceItem
		if err := scanInstance(row.Scan, &it.Instance); err != nil {
			return nil, err
		}
		byID[it.ID] = &it
		ids = append(ids, it.ID)
		return &it, nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("scan workflow instance: %w", err)
	}
	nodes, err := r.data.Pool.Query(ctx,
		`SELECT instance_id, node_name FROM workflow_task
		 WHERE instance_id IN (SELECT unnest($1::text[])::uuid) AND status = $2 ORDER BY create_time, id`, ids, biz.TaskPending)
	if err != nil {
		return nil, 0, fmt.Errorf("list current nodes: %w", err)
	}
	defer nodes.Close()
	for nodes.Next() {
		var instanceID, name string
		if err := nodes.Scan(&instanceID, &name); err != nil {
			return nil, 0, fmt.Errorf("scan current node: %w", err)
		}
		if it := byID[instanceID]; it != nil {
			it.CurrentNodes = append(it.CurrentNodes, name)
		}
	}
	return items, total, nodes.Err()
}

// taskViewStatuses 各任务视图对应的处理人状态集合。
var taskViewStatuses = map[string][]int32{
	biz.TaskViewTodo: {biz.AssigneePending},
	biz.TaskViewDone: {biz.AssigneeApproved, biz.AssigneeRejected, biz.AssigneeTransferred, biz.AssigneeReturned},
	biz.TaskViewCC:   {biz.AssigneeNotified},
}

// ListTasks implements biz.InstanceReader：以处理人表为驱动，待办 / 抄送按到达时间
// （idx_workflow_task_assignee_user）、已办按处理时间（idx_workflow_task_assignee_done）倒序。
// 业务类型按根流程所挂的业务对象过滤与返回：子流程里的任务归到它最终所属的业务对象上。
func (r *instanceRepo) ListTasks(ctx context.Context, userID, view, keyword, businessType string, page, pageSize int64) ([]*biz.TaskItem, int64, error) {
	statuses, ok := taskViewStatuses[view]
	if !ok {
		return nil, 0, fmt.Errorf("workflow: unknown task view %q", view)
	}
	args := []any{userID}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	// 单一状态用等值条件：规划器才能沿 (user_id, status, create_time DESC) 有序扫描，不必排序。
	where := " WHERE a.user_id = $1"
	if len(statuses) == 1 {
		where += " AND a.status = " + arg(statuses[0])
	} else {
		where += " AND a.status = ANY(" + arg(statuses) + ")"
	}
	order := " ORDER BY a.create_time DESC, a.id DESC"
	if view == biz.TaskViewDone {
		// 已办的席位必有处理时间；写明这个条件，规划器才用得上按处理时间排好序的部分索引。
		where += " AND a.action_time IS NOT NULL"
		order = " ORDER BY a.action_time DESC, a.id DESC"
	}
	// 计数只在条件用到实例列时才关联实例表（外键保证席位必有对应的任务与实例）。
	needInstance := false
	if view == biz.TaskViewTodo {
		// 待办只列进行中的实例：实例结束时其待处理席位已一并取消，此条件是纵深防御。
		where += " AND i.status = " + arg(biz.InstanceRunning)
		needInstance = true
	}
	if keyword != "" {
		where += " AND i.title ILIKE " + arg(dbx.LikeContains(keyword))
		needInstance = true
	}
	if businessType != "" {
		where += " AND i.root_business_type = " + arg(businessType)
		needInstance = true
	}
	const joinInstance = " JOIN workflow_instance i ON i.id = a.instance_id"
	countFrom := " FROM workflow_task_assignee a"
	if needInstance {
		countFrom += joinInstance
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*)"+countFrom+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count workflow tasks: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	from := " FROM workflow_task_assignee a JOIN workflow_task t ON t.id = a.task_id" + joinInstance + where
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx,
		`SELECT t.id, i.id, i.title, i.definition_name, t.node_name, i.initiator, i.status, a.status,
			a.create_time, a.action_time, i.root_business_type, i.root_business_id`+from+order+
			" LIMIT $"+strconv.Itoa(len(args)-1)+" OFFSET $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list workflow tasks: %w", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.TaskItem, error) {
		var (
			it                biz.TaskItem
			instStatus, state int16
			actionTime        *time.Time
		)
		if err := row.Scan(&it.TaskID, &it.InstanceID, &it.Title, &it.DefinitionName, &it.NodeName, &it.Initiator,
			&instStatus, &state, &it.CreateTime, &actionTime, &it.BusinessType, &it.BusinessID); err != nil {
			return nil, err
		}
		it.InstanceStatus, it.Status = int32(instStatus), int32(state)
		if actionTime != nil {
			it.ActionTime = *actionTime
		}
		return &it, nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("scan workflow task: %w", err)
	}
	return items, total, nil
}
