//go:build integration

package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
)

// —— 事件指令：连续失败的上限。仓储以引擎的受限登录角色运行（newEnv）：记失败与放弃写到的
// first_failed_at / last_failed_at 没有 0091 的列级授权就会直接报 42501。时刻一律是数据库时钟，
// 「过了多久」由 admin 把这两个列整体前移来模拟。 ——

// 记失败的两个阈值，与 biz 的常量（未导出）取值一致；SQL 的语义按这两个值验证。
const (
	itFailureGap  = time.Minute
	itGiveUpAfter = 5 * time.Minute
)

// commandObserver 记事件指令各结果的次数（并发用的两个实例共用）。
type commandObserver struct {
	countObserver
	mu       sync.Mutex
	commands map[string]int
}

func (o *commandObserver) AddCommand(_ context.Context, result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.commands == nil {
		o.commands = map[string]int{}
	}
	o.commands[result]++
}

func (o *commandObserver) results() map[string]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return maps.Clone(o.commands)
}

// commandRow monitor_alert_cmd 里本组测试关心的列。
type commandRow struct {
	Status                      int32
	Result                      string
	FirstFailedAt, LastFailedAt *time.Time
	DoneTime                    *time.Time
}

func (e *env) command(id string) commandRow {
	e.t.Helper()
	var r commandRow
	if err := e.admin.QueryRow(context.Background(),
		"SELECT status, result, first_failed_at, last_failed_at, done_time FROM monitor_alert_cmd WHERE id = $1", id).
		Scan(&r.Status, &r.Result, &r.FirstFailedAt, &r.LastFailedAt, &r.DoneTime); err != nil {
		e.t.Fatal(err)
	}
	return r
}

// 记失败：同一段连续失败保持起点，两次失败隔得太久就重新计时，满 5 分钟才报告「已满上限」。
func TestCommandFailureRecordingTracksTheStreak(t *testing.T) {
	e := newEnv(t)
	// 0091 的列级授权：引擎角色能写两个失败时刻列。
	if e.count(`SELECT (has_column_privilege($1::name, 'monitor_alert_cmd', 'first_failed_at', 'UPDATE')
		AND has_column_privilege($1::name, 'monitor_alert_cmd', 'last_failed_at', 'UPDATE'))::int`, loginRole) != 1 {
		t.Fatal("the engine role must be allowed to update the failure time columns")
	}
	ds := e.datasource("http://127.0.0.1:1")
	ruleID := e.rule(ruleSpec{})
	alert := e.seedAlert(ruleID, ds, "fp-streak", monitorcontract.AlertFiring, 0)
	cmd := e.closeCmd(alert, e.id(), "r")
	repo, ctx := NewCommandRepo(e.d), context.Background()
	record := func(want bool) {
		t.Helper()
		exhausted, err := repo.RecordCommandFailure(ctx, cmd, itFailureGap, itGiveUpAfter)
		if err != nil || exhausted != want {
			t.Fatalf("RecordCommandFailure() = %v, %v; want exhausted = %v", exhausted, err, want)
		}
	}
	// 首次失败：这一段从现在开始，远不满 5 分钟；指令仍是待处理。
	record(false)
	if r := e.command(cmd); r.FirstFailedAt == nil || r.LastFailedAt == nil || r.Status != monitorcontract.CmdPending || r.DoneTime != nil {
		t.Fatalf("after the first failure: %+v", r)
	}
	// 这一段已持续 2 分钟（起点前移 2 分钟；此后每 2s 左右失败一次，最近一次失败就是刚才）：又失败 → 同一段，起点不变。
	e.exec("UPDATE monitor_alert_cmd SET first_failed_at = first_failed_at - interval '2 minutes' WHERE id = $1", cmd)
	start := e.command(cmd).FirstFailedAt
	record(false)
	if r := e.command(cmd); r.FirstFailedAt == nil || !r.FirstFailedAt.Equal(*start) || r.LastFailedAt.Before(start.Add(2*time.Minute)) {
		t.Fatalf("a failure right after the previous one must keep the streak start: %+v (start %v)", r, start)
	}
	// 中断：上一次失败在 3 分钟前（超过 1 分钟的间隔阈值）才又失败——不算同一段，从这一次重新计时。
	e.exec("UPDATE monitor_alert_cmd SET last_failed_at = last_failed_at - interval '3 minutes' WHERE id = $1", cmd)
	record(false)
	if e.count("SELECT count(*) FROM monitor_alert_cmd WHERE id = $1 AND first_failed_at > $2 AND first_failed_at > now() - interval '30 seconds' AND status = 1",
		cmd, start) != 1 {
		t.Fatalf("a failure long after the previous one must restart the streak: %+v (old start %v)", e.command(cmd), start)
	}
	// 宕机：这一段本已持续 16 分钟（早已超过上限），但引擎停了 10 分钟、重启后数据库又抖了一下——两次失败隔了
	// 10 分钟，不算连续失败：重新计时，不会因为宕机前的那一段已满 5 分钟而被立刻放弃。
	e.exec("UPDATE monitor_alert_cmd SET first_failed_at = first_failed_at - interval '16 minutes', last_failed_at = last_failed_at - interval '10 minutes' WHERE id = $1", cmd)
	record(false)
	if e.count("SELECT count(*) FROM monitor_alert_cmd WHERE id = $1 AND first_failed_at > now() - interval '30 seconds' AND status = 1", cmd) != 1 {
		t.Fatalf("a failure after an engine downtime must restart the streak instead of giving up: %+v", e.command(cmd))
	}
	// 连续失败满 5 分钟：起点在 5 分钟前、最近一次失败仍是刚才（同一段）→ 报告已满上限，起点保持不变。
	e.exec("UPDATE monitor_alert_cmd SET first_failed_at = first_failed_at - interval '5 minutes' WHERE id = $1", cmd)
	record(true)
	if e.count("SELECT count(*) FROM monitor_alert_cmd WHERE id = $1 AND first_failed_at <= now() - $2::interval AND last_failed_at > now() - interval '30 seconds' AND status = 1",
		cmd, itGiveUpAfter) != 1 {
		t.Fatalf("an exhausted streak must keep its start: %+v", e.command(cmd))
	}
	// 指令已不是待处理（被处理掉或被放弃）：什么都不记。
	e.exec("UPDATE monitor_alert_cmd SET status = 3, result = 'x', done_time = now() WHERE id = $1", cmd)
	before := e.command(cmd)
	if _, err := repo.RecordCommandFailure(ctx, cmd, itFailureGap, itGiveUpAfter); !errors.Is(err, biz.ErrCommandGone) {
		t.Fatalf("recording a failure of a settled command must report ErrCommandGone, got %v", err)
	}
	if after := e.command(cmd); !after.FirstFailedAt.Equal(*before.FirstFailedAt) || !after.LastFailedAt.Equal(*before.LastFailedAt) {
		t.Fatal("a settled command must not be touched")
	}
}

// 放弃：不满时限、指令行被别的实例锁着都不写任何东西；围栏命中后指令置未执行、时间线恰好一条系统产生的
// action_failed（detail 只带 text）；重复放弃不再写。
func TestAbandonCommandIsFencedAndWritesOneEvent(t *testing.T) {
	e := newEnv(t)
	ds := e.datasource("http://127.0.0.1:1")
	ruleID := e.rule(ruleSpec{})
	alert := e.seedAlert(ruleID, ds, "fp-abandon", monitorcontract.AlertFiring, 0)
	cmd := e.closeCmd(alert, e.id(), "r")
	repo, ctx := NewCommandRepo(e.d), context.Background()
	command := biz.AlertCommand{ID: cmd, AlertID: alert, Kind: monitorcontract.AlertCmdClose}
	const why = "手动关闭未执行：引擎连续 5 分钟执行失败：boom"
	newID := func() (string, error) { return e.id(), nil }
	untouched := func() {
		t.Helper()
		if r := e.command(cmd); r.Status != monitorcontract.CmdPending || r.Result != "" || r.DoneTime != nil ||
			e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1", alert) != 0 {
			t.Fatalf("the command must be untouched: %+v, timeline = %s", r, e.timeline(alert))
		}
	}
	abandon := func(c biz.AlertCommand, id func() (string, error)) (bool, error) {
		t.Helper()
		return repo.AbandonCommand(ctx, c, why, itGiveUpAfter, id)
	}
	// 从未失败过：不满时限。
	if abandoned, err := abandon(command, newID); err != nil || abandoned {
		t.Fatalf("AbandonCommand() = %v, %v; want a fence miss", abandoned, err)
	}
	untouched()
	// 失败过一次，但这一段还不满 5 分钟。
	if _, err := repo.RecordCommandFailure(ctx, cmd, itFailureGap, itGiveUpAfter); err != nil {
		t.Fatal(err)
	}
	if abandoned, err := abandon(command, newID); err != nil || abandoned {
		t.Fatalf("AbandonCommand() = %v, %v; want a fence miss", abandoned, err)
	}
	untouched()
	// 连续失败已满 5 分钟（起点前移 5 分钟，最近一次失败仍是刚才）。
	e.exec("UPDATE monitor_alert_cmd SET first_failed_at = first_failed_at - interval '5 minutes' WHERE id = $1", cmd)
	// 另一个事务正持有指令行（相当于别的实例正在处理）：SKIP LOCKED，立即返回而不排队。
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT 1 FROM monitor_alert_cmd WHERE id = $1 FOR UPDATE", cmd); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := repo.AbandonCommand(ctx, command, why, itGiveUpAfter, newID)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AbandonCommand() under a held lock: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a locked command must be skipped at once")
	}
	untouched()
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// 放弃：指令置未执行并写明原因；时间线恰好一条 action_failed，没有操作人，detail 只有说明。
	if abandoned, err := abandon(command, newID); err != nil || !abandoned {
		t.Fatalf("AbandonCommand() = %v, %v; want abandoned", abandoned, err)
	}
	r := e.command(cmd)
	if r.Status != monitorcontract.CmdRejected || r.Result != why || r.DoneTime == nil || e.timeline(alert) != "action_failed" {
		t.Fatalf("abandoned command = %+v, timeline = %s", r, e.timeline(alert))
	}
	var detail []byte
	if err := e.admin.QueryRow(ctx,
		"SELECT detail::text FROM monitor_alert_event WHERE alert_id = $1 AND type = $2 AND operator IS NULL",
		alert, monitorcontract.AlertEventActionFailed).Scan(&detail); err != nil {
		t.Fatalf("the abandoned command must leave exactly one system-made event: %v", err)
	}
	var d monitorcontract.AlertEventDetail
	if err := json.Unmarshal(detail, &d); err != nil || d != (monitorcontract.AlertEventDetail{Text: why}) {
		t.Fatalf("event detail = %s (%v), want only the text", detail, err)
	}
	// 只动指令行与时间线：事件仍在告警中，判定状态还在。
	if e.p2Alert(ruleID).Status != int(monitorcontract.AlertFiring) || e.count("SELECT count(*) FROM monitor_eval_state WHERE alert_id = $1", alert) != 1 {
		t.Fatal("abandoning a command must not touch the alert or its state")
	}
	// 重复放弃：已不是待处理，什么都不写（处理时间不变，仍只有一条事件）。
	if abandoned, err := abandon(command, newID); err != nil || abandoned {
		t.Fatalf("AbandonCommand() again = %v, %v; want a fence miss", abandoned, err)
	}
	if again := e.command(cmd); !again.DoneTime.Equal(*r.DoneTime) || e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1", alert) != 1 {
		t.Fatalf("abandoning twice must not write again: %+v", again)
	}
	// 生成不了事件主键：整个事务回滚，指令仍是待处理、没有事件。
	second := e.closeCmd(alert, e.id(), "again")
	if _, err := repo.RecordCommandFailure(ctx, second, itFailureGap, itGiveUpAfter); err != nil {
		t.Fatal(err)
	}
	e.exec("UPDATE monitor_alert_cmd SET first_failed_at = first_failed_at - interval '5 minutes' WHERE id = $1", second)
	if _, err := abandon(biz.AlertCommand{ID: second, AlertID: alert, Kind: monitorcontract.AlertCmdClose}, func() (string, error) {
		return "", errors.New("no id")
	}); err == nil {
		t.Fatal("want the id error")
	}
	if r := e.command(second); r.Status != monitorcontract.CmdPending || r.DoneTime != nil ||
		e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1", alert) != 1 {
		t.Fatalf("a failed abandon must roll back: %+v", r)
	}
}

// 引擎连续 5 分钟执行失败后放弃手动关闭：两个实例同时消费，每条指令只放弃一次、只写一条事件；放弃后不再有
// 待处理的关闭指令（core 的「待关闭」据此消失），同一事件可以重新下达关闭并被正常执行。
func TestCommandGiveUpEndToEndUnderTwoInstances(t *testing.T) {
	e := newEnv(t)
	ds := e.datasource("http://127.0.0.1:1")
	ruleID := e.rule(ruleSpec{})
	const alerts = 6
	operator := e.id()
	var (
		alertIDs []string
		cmdIDs   []string
	)
	for i := range alerts {
		alertIDs = append(alertIDs, e.seedAlert(ruleID, ds, fmt.Sprintf("fp-giveup-%d", i), monitorcontract.AlertFiring, 0))
		cmdIDs = append(cmdIDs, e.closeCmd(alertIDs[i], operator, "机器已下线"))
	}
	// 让关闭恒失败：时间线暂时不允许再写 close 事件（相当于约束冲突这类每次都会撞上的执行失败）。
	const constraint = "ck_it_no_close_event"
	e.exec("ALTER TABLE monitor_alert_event DROP CONSTRAINT IF EXISTS " + constraint)
	e.exec("ALTER TABLE monitor_alert_event ADD CONSTRAINT " + constraint + " CHECK (type <> 'close')")
	t.Cleanup(func() {
		if _, err := e.admin.Exec(context.Background(), "ALTER TABLE monitor_alert_event DROP CONSTRAINT IF EXISTS "+constraint); err != nil {
			t.Errorf("drop %s: %v", constraint, err)
		}
	})
	obs := &commandObserver{}
	instance := func() *biz.CommandUsecase {
		return biz.NewCommandUsecase(NewCommandRepo(e.d), NewCommandRepo(e.d), obs, testLogger())
	}
	ctx := context.Background()
	// 第一次失败：每条记一笔，不满 5 分钟，仍是待处理；整批失败向循环返回错误（退避后重试）。
	if _, err := instance().RunOnce(ctx); err == nil || !strings.Contains(err.Error(), constraint) {
		t.Fatalf("RunOnce() error = %v, want the failing close", err)
	}
	if e.count("SELECT count(*) FROM monitor_alert_cmd WHERE status = 1 AND first_failed_at IS NOT NULL AND last_failed_at IS NOT NULL") != alerts ||
		e.count("SELECT count(*) FROM monitor_alert_event WHERE type = 'action_failed'") != 0 || obs.results()[biz.CommandAbandoned] != 0 {
		t.Fatal("below the limit every failing command stays pending with its failure recorded")
	}
	// 连续失败满 5 分钟：两个实例同时消费。
	e.exec("UPDATE monitor_alert_cmd SET first_failed_at = first_failed_at - interval '5 minutes' WHERE status = 1")
	a, b := instance(), instance()
	var wg sync.WaitGroup
	for _, uc := range []*biz.CommandUsecase{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5 {
				// 有的轮次里本实例一条都没处理成（都由另一个实例放弃了）：返回错误是正常的。
				_, _ = uc.RunOnce(ctx)
			}
		}()
	}
	wg.Wait()
	// 每条指令恰好放弃一次：置未执行并写明原因（放得进 varchar(255)），时间线恰好一条与原因一致的说明。
	if n := e.count(`SELECT count(*) FROM monitor_alert_cmd WHERE status = 3 AND done_time IS NOT NULL
		AND result LIKE '手动关闭未执行：引擎连续 5 分钟执行失败：%' AND length(result) <= 255`); n != alerts {
		t.Fatalf("abandoned commands = %d, want %d", n, alerts)
	}
	if e.count("SELECT count(*) FROM monitor_alert_cmd WHERE status = 1") != 0 {
		t.Fatal("no close command may stay pending once the streak is exhausted")
	}
	if e.count(`SELECT count(*) FROM monitor_alert_cmd c JOIN monitor_alert_event ev ON ev.alert_id = c.alert_id AND ev.type = 'action_failed'
		WHERE c.id = ANY($1::uuid[]) AND ev.operator IS NULL AND ev.detail = jsonb_build_object('text', c.result)`, cmdIDs) != alerts ||
		e.count("SELECT count(*) FROM monitor_alert_event WHERE type = 'action_failed'") != alerts {
		t.Fatal("every abandoned command must leave exactly one matching action_failed event")
	}
	if got := obs.results(); got[biz.CommandAbandoned] != alerts || got[biz.CommandFailed] < alerts {
		t.Fatalf("metrics = %v", got)
	}
	// 放弃之后执行不再失败：用户重新下达的关闭被正常执行，时间线依次是 action_failed、close。
	e.exec("ALTER TABLE monitor_alert_event DROP CONSTRAINT " + constraint)
	again := e.closeCmd(alertIDs[0], operator, "再试一次")
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if e.command(again).Status != monitorcontract.CmdDone ||
		e.count("SELECT count(*) FROM monitor_alert WHERE id = $1 AND status = $2 AND closed_by = $3 AND close_reason = '再试一次'",
			alertIDs[0], monitorcontract.AlertClosed, operator) != 1 || e.timeline(alertIDs[0]) != "action_failed,close" {
		t.Fatalf("the close requested again must be executed: command = %+v, timeline = %s", e.command(again), e.timeline(alertIDs[0]))
	}
}
