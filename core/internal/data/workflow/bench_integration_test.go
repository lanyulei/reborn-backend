//go:build integration

package data

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	datanotify "github.com/lanyulei/reborn/backend/core/internal/data/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 本文件为「只回写变化的席位」改写补基准对比（AGENTS §6：性能敏感改动必须附 go test -bench 对比
// 结果）：同一份夹具上，N 人会签任务被第一个处理人同意时，比较 HEAD（回写整张任务的处理人）与
// 当前（只写变化的席位）一次 Approve 命令的耗时与被重写的席位行数。
//
//   - 旧实现逐字取自 git HEAD（只读）：SaveTx / writeState 只改了签名（方法接收者换成装饰器入参）与
//     末尾 writeState 的调用名；经 legacySaveStore 装进与 newEnv 同一组依赖的运行时，走真实的 Approve。
//   - 不含性能阈值断言（机器差异会变成不稳定测试），只在显式 -bench 时运行。
//   - 与集成测试同一份夹具：TEST_DATABASE_SOURCE 指向一次性库，会 TRUNCATE 工作流域的表。
//     go test -tags=integration -run '^$' -bench 'BenchmarkIntegration_' -benchtime 10x ./internal/data/workflow/

// legacySaveStore 实例仓储的装饰器：只有回写（SaveTx）换成 HEAD 的写法，其余方法原样委托。
type legacySaveStore struct {
	biz.InstanceStore
}

func (l legacySaveStore) SaveTx(ctx context.Context, tx txrun.Tx, s *biz.State) error {
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
	return writeStateLegacy(ctx, ptx, s)
}

func writeStateLegacy(ctx context.Context, tx pgx.Tx, s *biz.State) error {
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
		for _, a := range t.Assignees {
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

// legacyRuntime 与 newEnv 同一组依赖的运行时，只把写回仓储换成 HEAD 的写法。
// dup: 依赖装配与 newEnv 里的那一行相同（测试文件；newEnv 的 env 没有暴露运行时的各依赖）。
func legacyRuntime(e *env) *biz.RuntimeUsecase {
	instRepo := NewInstanceRepo(e.d)
	return biz.NewRuntimeUsecase(e.defs, legacySaveStore{instRepo}, instRepo, instRepo, e.dir, usersAdapter{e.dir}, e.cfg,
		biz.NewCalendarUsecase(e.cfg), basedata.NewTxManager(e.d), biznotify.NewNotificationUsecase(datanotify.NewEventRepo(e.d), nil),
		biz.Listeners{"ticket": e.listener}, nil, nil)
}

// seedBenchUsers 插入 n 个启用用户（会签席位），返回其 ID。
func seedBenchUsers(tb testing.TB, e *env, n int) []string {
	tb.Helper()
	rows, err := e.d.Pool.Query(context.Background(),
		`INSERT INTO "system_user" (id, username, password, nickname, status)
		 SELECT gen_random_uuid(), 'bench-' || g, 'x', '基准' || g, 1 FROM generate_series(1, $1::int) g RETURNING id::text`, n)
	if err != nil {
		tb.Fatalf("seed bench users: %v", err)
	}
	defer rows.Close()
	ids := make([]string, 0, n)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			tb.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil || len(ids) != n {
		tb.Fatalf("bench users = %d, %v; want %d", len(ids), err, n)
	}
	return ids
}

// 夹具用到的 biz/workflow 校验上限（那边未导出）：一条用户规则至多 50 人、一个任务至多 100 个席位
// （graph.go 的 maxRuleTargets、engine.go 的 maxAssigneesPerTask）。
const (
	benchRuleTargets      = 50
	benchAssigneesPerTask = 100
)

// benchCountersignGraph 发起 → N 人会签（全部同意才通过）→ 结束。超出单条规则人数上限的拆到下一条规则。
func benchCountersignGraph(users []string) string {
	rules := make([]string, 0, 2)
	for start := 0; start < len(users); start += benchRuleTargets {
		end := min(start+benchRuleTargets, len(users))
		quoted := make([]string, 0, end-start)
		for _, u := range users[start:end] {
			quoted = append(quoted, `"`+u+`"`)
		}
		rules = append(rules, `{"kind":"user","userIds":[`+strings.Join(quoted, ",")+`]}`)
	}
	return `{"nodes":[{"id":"start","type":"start"},
		{"id":"ops","type":"userTask","name":"会签","assignee":{"mode":"all","rules":[` + strings.Join(rules, ",") + `]}},
		{"id":"end","type":"end"}],
		"edges":[{"id":"e1","source":"start","target":"ops"},{"id":"e2","source":"ops","target":"end"}],"settings":{}}`
}

// seatXmins 实例名下每个席位的行版本（xmin）：命令前后对比即知哪些席位行被重写。
func seatXmins(tb testing.TB, e *env, instanceID string) map[string]string {
	tb.Helper()
	rows, err := e.d.Pool.Query(context.Background(),
		"SELECT user_id::text, xmin::text FROM workflow_task_assignee WHERE instance_id = $1::uuid", instanceID)
	if err != nil {
		tb.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var user, xmin string
		if err := rows.Scan(&user, &xmin); err != nil {
			tb.Fatal(err)
		}
		out[user] = xmin
	}
	if err := rows.Err(); err != nil {
		tb.Fatal(err)
	}
	return out
}

// BenchmarkIntegration_SeatWriteback 席位回写：HEAD 回写整张任务的处理人，当前只写变化的席位。
// 一个 op = N 人会签任务上一次 Approve 命令（真实运行时：加锁读状态 → 引擎推进 → 回写 → 提交），
// 每个 op 之前新发起一个实例（计时区间外）。上报命令前后行版本变了的席位行数。
func BenchmarkIntegration_SeatWriteback(b *testing.B) {
	ctx := context.Background()
	// 12 人 = 审查报告里的场景；另一档取单任务席位上限（100）。
	for _, seats := range []int{12, benchAssigneesPerTask} {
		e := newEnv(b)
		users := seedBenchUsers(b, e, seats)
		e.publish(fmt.Sprintf("bench-countersign-%d", seats), benchCountersignGraph(users))
		// 新发起一个实例，返回实例与其会签任务的 ID。tb 必须是当前（子）基准：失败时不能让父基准 FailNow。
		start := func(tb testing.TB) (instanceID, taskID string) {
			in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: fmt.Sprintf("bench-countersign-%d", seats), Title: "会签", Initiator: uInit})
			if err != nil {
				tb.Fatalf("Start() error = %v", err)
			}
			if err := e.d.Pool.QueryRow(ctx, "SELECT id::text FROM workflow_task WHERE instance_id = $1::uuid", in.ID).Scan(&taskID); err != nil {
				tb.Fatal(err)
			}
			return in.ID, taskID
		}
		key := fmt.Sprintf("seats=%d", seats)
		b.Run(key+"/legacy=all-seats", func(b *testing.B) {
			runSeatWriteback(b, e, legacyRuntime(e), start, seats, users[0])
		})
		b.Run(key+"/current=changed-seats", func(b *testing.B) {
			runSeatWriteback(b, e, e.rt, start, seats, users[0])
		})
	}
}

// runSeatWriteback 在给定的运行时上跑一次预热 + b.N 次 Approve；rewritten 返回一次命令重写的席位行数。
func runSeatWriteback(b *testing.B, e *env, rt *biz.RuntimeUsecase, start func(testing.TB) (string, string), seats int, approver string) {
	b.Helper()
	ctx := context.Background()
	approve := func() int {
		instanceID, taskID := start(b)
		before := seatXmins(b, e, instanceID)
		if len(before) != seats {
			b.Fatalf("seats after start = %d, want %d", len(before), seats)
		}
		b.StartTimer()
		if _, err := rt.Approve(ctx, taskID, approver, "同意"); err != nil {
			b.Fatalf("Approve() error = %v", err)
		}
		b.StopTimer()
		after := seatXmins(b, e, instanceID)
		n := 0
		for user, xmin := range after {
			if before[user] != xmin {
				n++
			}
		}
		return n
	}
	// 预热：pgx 的语句描述缓存与服务端预备语句就位后，首条语句多出的描述往返不计入。
	b.StopTimer()
	approve()
	rewritten := 0
	b.ResetTimer()
	for range b.N {
		rewritten += approve()
	}
	b.ReportMetric(float64(rewritten)/float64(b.N), "seat-rows-rewritten/op")
}
