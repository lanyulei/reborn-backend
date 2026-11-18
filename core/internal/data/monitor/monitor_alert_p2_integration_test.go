//go:build integration

package data

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// alertP2Env 告警事件操作的集成环境：一个分组、一条规则，alice 可管理、bob 只读。
type alertP2Env struct {
	*env
	grp  *biz.RuleGroup
	rule *biz.Rule
}

func newAlertP2Env(t *testing.T) *alertP2Env {
	t.Helper()
	e := newEnv(t)
	if _, err := e.pool.Exec(context.Background(), "TRUNCATE TABLE monitor_alert_event, monitor_alert_cmd"); err != nil {
		t.Fatalf("truncate p2 tables: %v", err)
	}
	g := e.group("", "p2-alerts")
	if err := e.scopes.Replace(context.Background(), g.ID, []*biz.RoleScope{
		{RoleID: roleA, GroupID: g.ID, Level: mc.ScopeManage}, {RoleID: roleB, GroupID: g.ID, Level: mc.ScopeRead},
	}, uAdmin); err != nil {
		t.Fatalf("grant scopes: %v", err)
	}
	return &alertP2Env{env: e, grp: g, rule: e.rule(g.ID, "cpu")}
}

func (e *alertP2Env) firing() string {
	return e.alert(alertRow{ruleID: e.rule.ID, ruleName: "cpu", groupID: e.grp.ID})
}

type stubDesk struct{ n int }

func (d *stubDesk) Create(_ context.Context, p biz.TicketCreate) (*biz.TicketInfo, error) {
	d.n++
	return &biz.TicketInfo{ID: fmt.Sprintf("018f3e1a-0d10-7000-8000-%012d", d.n), Number: "INC-1", Title: p.Title, Status: 1, Creator: p.Creator}, nil
}

func (d *stubDesk) View(context.Context, string, string) (*biz.TicketInfo, error) { return nil, nil }
func (d *stubDesk) Comment(context.Context, string, string, string) error         { return nil }

type stubCMDB struct{}

func (stubCMDB) Card(context.Context, string, string) (*biz.CICard, bool, error) {
	return nil, false, nil
}

func (stubCMDB) Impact(context.Context, string, string, int, int) ([]biz.CIImpact, bool, error) {
	return nil, false, nil
}

type stubCIs struct{}

func (stubCIs) Briefs(context.Context, []string) (map[string]*biz.CIEntry, error) { return nil, nil }

func (stubCIs) ListValid(context.Context, int) ([]*biz.CIEntry, error) { return nil, nil }

func (stubCIs) Summary(context.Context) (int32, *time.Time, error) { return 0, nil, nil }

type stubInhibits struct{}

func (stubInhibits) InhibitNames(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

// usecases 在给定连接池上装配告警查询与事件操作用例（跨域依赖用桩）。
func (e *alertP2Env) usecases(d *basedata.Data) (*biz.AlertUsecase, *biz.AlertActionUsecase, *alertOpsRepo) {
	r := reposOf(e.t, d)
	ops := NewAlertOpsRepo(d)
	scopes := biz.NewScopeUsecase(r.org, r.scopes, r.groups)
	detail := biz.NewAlertDetailUsecase(ops, ops, ops, stubCIs{}, stubCMDB{}, &stubDesk{}, r.org, r.mutes, stubInhibits{})
	r.org.AlertReaders = []string{uAlice, uBob}
	alerts := biz.ProvideAlertUsecase(scopes, r.alerts, r.alerts, r.logs, r.rules, r.mutes, r.org, r.org,
		biz.NewAlertDecorator(r.org, stubInhibits{}, stubCIs{}, ops, ops), detail)
	actions := biz.NewAlertActionUsecase(alerts, detail, basedata.NewTxManager(d), ops, ops, ops, ops, ops, &stubDesk{}, stubCMDB{})
	return alerts, actions, ops
}

type ackState struct {
	user    *string
	ackTime *time.Time
	ticket  *string
}

func (e *alertP2Env) state(id string) ackState {
	e.t.Helper()
	var s ackState
	if err := e.pool.QueryRow(context.Background(),
		"SELECT ack_user::text, ack_time, ticket_id::text FROM monitor_alert WHERE id = $1", id).Scan(&s.user, &s.ackTime, &s.ticket); err != nil {
		e.t.Fatal(err)
	}
	return s
}

func TestAlertOpsRepo_AckUnackTransfer(t *testing.T) {
	e := newAlertP2Env(t)
	ctx := context.Background()
	ops := NewAlertOpsRepo(e.d)
	txm := basedata.NewTxManager(e.d)
	id := e.firing()
	resolved := e.alert(alertRow{ruleID: e.rule.ID, ruleName: "cpu", groupID: e.grp.ID, status: mc.AlertResolved})
	in := func(fn func(tx txrun.Tx) (bool, error)) bool {
		t.Helper()
		var ok bool
		if err := txm.WithinTx(ctx, func(tx txrun.Tx) (err error) { ok, err = fn(tx); return err }); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	// 已恢复的事件：三种写入的条件都不成立。
	if in(func(tx txrun.Tx) (bool, error) { return ops.AckTx(ctx, tx, resolved, uAlice) }) ||
		in(func(tx txrun.Tx) (bool, error) { return ops.TransferTx(ctx, tx, resolved, "", uAlice) }) {
		t.Fatal("resolved alert must not be acked / transferred")
	}
	if !in(func(tx txrun.Tx) (bool, error) { return ops.AckTx(ctx, tx, id, uAlice) }) {
		t.Fatal("ack failed")
	}
	first := e.state(id)
	if first.user == nil || *first.user != uAlice || first.ackTime == nil {
		t.Fatalf("after ack: %+v", first)
	}
	// 已认领：再认领落空；认领人不符的取消 / 转交落空。
	if in(func(tx txrun.Tx) (bool, error) { return ops.AckTx(ctx, tx, id, uBob) }) ||
		in(func(tx txrun.Tx) (bool, error) { return ops.UnackTx(ctx, tx, id, uBob) }) ||
		in(func(tx txrun.Tx) (bool, error) { return ops.TransferTx(ctx, tx, id, "", uBob) }) ||
		in(func(tx txrun.Tx) (bool, error) { return ops.TransferTx(ctx, tx, id, uBob, uCarol) }) {
		t.Fatal("conditional updates must not fire on a stale expectation")
	}
	// 转交保留原认领时间。
	if !in(func(tx txrun.Tx) (bool, error) { return ops.TransferTx(ctx, tx, id, uAlice, uBob) }) {
		t.Fatal("transfer failed")
	}
	if s := e.state(id); *s.user != uBob || !s.ackTime.Equal(*first.ackTime) {
		t.Fatalf("after transfer: %+v", s)
	}
	if !in(func(tx txrun.Tx) (bool, error) { return ops.UnackTx(ctx, tx, id, uBob) }) {
		t.Fatal("unack failed")
	}
	if s := e.state(id); s.user != nil || s.ackTime != nil {
		t.Fatalf("after unack: %+v", s)
	}
	// 未认领时转交 = 指派。
	if !in(func(tx txrun.Tx) (bool, error) { return ops.TransferTx(ctx, tx, id, "", uCarol) }) {
		t.Fatal("assign failed")
	}
	if s := e.state(id); *s.user != uCarol || s.ackTime == nil {
		t.Fatalf("after assign: %+v", s)
	}
	// 事务回滚：认领与时间线一起撤销。
	other := e.firing()
	_ = txm.WithinTx(ctx, func(tx txrun.Tx) error {
		if ok, err := ops.AckTx(ctx, tx, other, uAlice); err != nil || !ok {
			t.Fatalf("ack in tx: %v %v", ok, err)
		}
		if err := ops.AppendTx(ctx, tx, &biz.AlertEvent{ID: newID(t), AlertID: other, Type: mc.AlertEventAck, Operator: uAlice}); err != nil {
			t.Fatal(err)
		}
		return context.Canceled
	})
	if s := e.state(other); s.user != nil || e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1", other) != 0 {
		t.Fatalf("rollback left state behind: %+v", s)
	}
}

// 并发认领：只有一个成功，认领人与时间线各一份。
func TestAlertAction_ConcurrentAck(t *testing.T) {
	e := newAlertP2Env(t)
	ctx := context.Background()
	_, actions, _ := e.usecases(e.d)
	id := e.firing()
	users := []string{uAlice, uBob, uAdmin}
	var wg sync.WaitGroup
	errs := make([]error, 12)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = actions.Ack(ctx, users[i%len(users)], id)
		}()
	}
	wg.Wait()
	winner := e.state(id)
	if winner.user == nil {
		t.Fatal("nobody acked")
	}
	for i, err := range errs {
		mine := users[i%len(users)] == *winner.user
		if mine && err != nil || !mine && !kerrors.Is(err, biz.ErrAlertAlreadyAcked) {
			t.Fatalf("ack[%d] by %s: %v (winner %s)", i, users[i%len(users)], err, *winner.user)
		}
	}
	if n := e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = 'ack'", id); n != 1 {
		t.Fatalf("ack timeline entries = %d, want 1", n)
	}
}

func TestAlertOpsRepo_CloseCmd(t *testing.T) {
	e := newAlertP2Env(t)
	ctx := context.Background()
	ops := NewAlertOpsRepo(e.d)
	id := e.firing()
	resolved := e.alert(alertRow{ruleID: e.rule.ID, ruleName: "cpu", groupID: e.grp.ID, status: mc.AlertResolved})
	if firing, err := ops.RequestClose(ctx, resolved, uAlice, "x"); err != nil || firing {
		t.Fatalf("RequestClose(resolved) = %v, %v", firing, err)
	}
	if firing, err := ops.RequestClose(ctx, newID(t), uAlice, "x"); err != nil || firing {
		t.Fatalf("RequestClose(missing) = %v, %v", firing, err)
	}
	// 并发重复下达：只落一条待处理指令。
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if firing, err := ops.RequestClose(ctx, id, uAlice, "机器下线"); err != nil || !firing {
				t.Errorf("RequestClose = %v, %v", firing, err)
			}
		}()
	}
	wg.Wait()
	if n := e.count("SELECT count(*) FROM monitor_alert_cmd WHERE alert_id = $1 AND kind = 1 AND status = 1 AND reason = '机器下线' AND operator = $2", id, uAlice); n != 1 {
		t.Fatalf("pending close cmds = %d, want 1", n)
	}
	if e.count("SELECT count(*) FROM monitor_alert_cmd") != 1 {
		t.Fatal("no cmd may be written for a resolved / missing alert")
	}
	pending, err := ops.PendingClose(ctx, []string{id, resolved})
	if err != nil || !pending[id] || pending[resolved] {
		t.Fatalf("PendingClose = %v, %v", pending, err)
	}
	// 引擎处理完（置为已执行）之后可以再次下达，且不再算「关闭中」。
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert_cmd SET status = 3, done_time = now() WHERE alert_id = $1", id); err != nil {
		t.Fatal(err)
	}
	if pending, _ := ops.PendingClose(ctx, []string{id}); pending[id] {
		t.Fatal("a processed cmd must not count as pending")
	}
	if firing, err := ops.RequestClose(ctx, id, uBob, "again"); err != nil || !firing || e.count("SELECT count(*) FROM monitor_alert_cmd WHERE alert_id = $1", id) != 2 {
		t.Fatalf("second RequestClose = %v, %v", firing, err)
	}
}

func TestAlertOpsRepo_EventsLinksStats(t *testing.T) {
	e := newAlertP2Env(t)
	ctx := context.Background()
	ops := NewAlertOpsRepo(e.d)
	txm := basedata.NewTxManager(e.d)
	a, b := e.firing(), e.firing()
	ticket1, ticket2 := newID(t), newID(t)

	ev := &biz.AlertEvent{ID: newID(t), AlertID: a, Type: mc.AlertEventComment, Operator: uAlice, Detail: mc.AlertEventDetail{Text: "备注"}}
	if err := ops.Append(ctx, ev); err != nil || ev.CreateTime.IsZero() {
		t.Fatalf("Append: %v / %v", err, ev.CreateTime)
	}
	// 系统产生的条目：operator 为空；detail 只落用到的键。
	if err := ops.Append(ctx, &biz.AlertEvent{ID: newID(t), AlertID: a, Type: mc.AlertEventTicket, Detail: mc.AlertEventDetail{TicketID: ticket1, TicketNumber: "INC-1", Auto: true}}); err != nil {
		t.Fatal(err)
	}
	if err := ops.Append(ctx, &biz.AlertEvent{ID: newID(t), AlertID: a, Type: "bogus"}); err == nil {
		t.Fatal("an unknown timeline type must be rejected by the CHECK")
	}
	if n := e.count(`SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = 'comment' AND detail = '{"text":"备注"}'::jsonb`, a); n != 1 {
		t.Fatal("detail must only carry the keys in use")
	}
	events, err := ops.ListByAlert(ctx, a, 10)
	if err != nil || len(events) != 2 || events[0].Type != mc.AlertEventComment || events[0].Operator != uAlice || events[0].Detail.Text != "备注" ||
		events[1].Operator != "" || !events[1].Detail.Auto {
		t.Fatalf("ListByAlert = %+v, %v", events, err)
	}
	if one, _ := ops.ListByAlert(ctx, a, 1); len(one) != 1 || one[0].ID != ev.ID {
		t.Fatalf("limit not applied: %+v", one)
	}

	// 关联工单：只在尚未关联时写；已关联时返回现有的；事件不存在报错。
	link := func(alertID, ticketID string) (linked bool, existing string, err error) {
		err = txm.WithinTx(ctx, func(tx txrun.Tx) (ferr error) {
			linked, existing, ferr = ops.LinkTicketTx(ctx, tx, alertID, ticketID)
			return ferr
		})
		return
	}
	if linked, _, err := link(a, ticket1); err != nil || !linked {
		t.Fatalf("link = %v, %v", linked, err)
	}
	if linked, existing, err := link(a, ticket2); err != nil || linked || existing != ticket1 {
		t.Fatalf("relink = %v, %q, %v", linked, existing, err)
	}
	_, _, err = link(newID(t), ticket1)
	wantErr(t, "link a missing alert", err, biz.ErrAlertNotFound)
	// 较新的 ticket 条目优先。
	time.Sleep(5 * time.Millisecond)
	if err := ops.Append(ctx, &biz.AlertEvent{ID: newID(t), AlertID: a, Type: mc.AlertEventTicket, Operator: uBob, Detail: mc.AlertEventDetail{TicketID: ticket2, TicketNumber: "INC-2"}}); err != nil {
		t.Fatal(err)
	}
	links, err := ops.TicketLinks(ctx, []string{a, b})
	if err != nil || len(links) != 1 || links[a] != (biz.TicketLink{TicketID: ticket2, Number: "INC-2", Creator: uBob}) {
		t.Fatalf("TicketLinks = %+v, %v", links, err)
	}

	// 流程实例只写一次；AI 摘要可覆盖，受长度 CHECK 约束。
	inst1, inst2 := newID(t), newID(t)
	if err := txm.WithinTx(ctx, func(tx txrun.Tx) error {
		if err := ops.SetWorkflowInstanceTx(ctx, tx, a, inst1); err != nil {
			return err
		}
		if err := ops.SetWorkflowInstanceTx(ctx, tx, a, inst2); err != nil {
			return err
		}
		return ops.SetAISummaryTx(ctx, tx, a, "摘要")
	}); err != nil {
		t.Fatal(err)
	}
	if n := e.count("SELECT count(*) FROM monitor_alert WHERE id = $1 AND workflow_instance_id = $2 AND ai_summary = '摘要'", a, inst1); n != 1 {
		t.Fatal("workflow instance / ai summary not written as expected")
	}
	if err := txm.WithinTx(ctx, func(tx txrun.Tx) error { return ops.SetAISummaryTx(ctx, tx, a, strings.Repeat("x", 4097)) }); err == nil {
		t.Fatal("an oversized ai summary must be rejected by the CHECK")
	}

	// 读回新列。
	got, err := e.alerts.Get(ctx, a)
	if err != nil || got.TicketID != ticket1 || got.WorkflowInstanceID != inst1 || got.AISummary != "摘要" {
		t.Fatalf("alert = %+v, %v", got, err)
	}
	// 批量读取与规则名。
	list, err := ops.ListByIDs(ctx, []string{a, b, newID(t)})
	names, err2 := ops.RuleNames(ctx, []string{a, newID(t)})
	if err != nil || err2 != nil || len(list) != 2 || len(names) != 1 || names[a] != "cpu" {
		t.Fatalf("ListByIDs = %d, %v; RuleNames = %v, %v", len(list), err, names, err2)
	}

	// 按资源统计活跃告警：只算范围内、告警中的。
	ci, otherGroup := newID(t), e.group("", "p2-other")
	c := e.alert(alertRow{ruleID: e.rule.ID, ruleName: "cpu", groupID: otherGroup.ID})
	d := e.alert(alertRow{ruleID: e.rule.ID, ruleName: "cpu", groupID: e.grp.ID, status: mc.AlertResolved})
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert SET ci_id = $1 WHERE id = ANY($2::uuid[])", ci, []string{a, b, c, d}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		groups biz.GroupFilter
		want   int32
	}{{biz.GroupFilter{All: true}, 3}, {biz.GroupFilter{IDs: []string{e.grp.ID}}, 2}, {biz.GroupFilter{}, 0}} {
		counts, err := ops.FiringByCI(ctx, []string{ci, newID(t)}, tc.groups)
		if err != nil || counts[ci] != tc.want || len(counts) > 1 {
			t.Fatalf("FiringByCI(%+v) = %v, %v; want %d", tc.groups, counts, err, tc.want)
		}
	}
	plan := e.explain("SELECT a.ci_id::text, count(*) FROM monitor_alert a WHERE a.ci_id = ANY($1::uuid[]) AND a.status = 1 GROUP BY a.ci_id", []string{ci})
	t.Logf("FiringByCI plan:\n%s", plan)

	// 列表的新过滤条件（真实 SQL）。
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert SET ack_user = $2, ack_time = now() WHERE id = $1", b, uBob); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert SET inhibited = true, inhibit_kind = 2 WHERE id = $1", c); err != nil {
		t.Fatal(err)
	}
	yes := true
	for name, tc := range map[string]struct {
		f    biz.AlertFilter
		want []string
	}{
		"ci":               {biz.AlertFilter{CIID: ci, Statuses: []int32{mc.AlertFiring}}, []string{a, b, c}},
		"acked":            {biz.AlertFilter{Acked: &yes}, []string{b}},
		"inhibited":        {biz.AlertFilter{Inhibited: &yes}, []string{c}},
		"ack user":         {biz.AlertFilter{AckUserID: uBob}, []string{b}},
		"handle unacked":   {biz.AlertFilter{HandleState: biz.HandleUnacked, Statuses: []int32{mc.AlertFiring}}, []string{a}},
		"handle acked":     {biz.AlertFilter{HandleState: biz.HandleAcked}, []string{b}},
		"handle inhibited": {biz.AlertFilter{HandleState: biz.HandleInhibited}, []string{c}},
		"mine or acked":    {biz.AlertFilter{RecipientID: uBob, OrAckUser: true}, []string{b}},
	} {
		tc.f.Groups = biz.GroupFilter{All: true}
		items, total, err := e.alerts.List(ctx, tc.f, 1, 50)
		if err != nil || int(total) != len(tc.want) {
			t.Fatalf("%s: total = %d, %v; want %d", name, total, err, len(tc.want))
		}
		for _, it := range items {
			found := false
			for _, id := range tc.want {
				found = found || id == it.ID
			}
			if !found {
				t.Fatalf("%s: unexpected alert %s", name, it.ID)
			}
		}
	}
}

// 以只属于 monitor_core_role 的登录账号跑通事件操作的全部写入：认领 / 取消认领 / 转交（列级 UPDATE）、
// 备注与工单时间线（INSERT）、关闭指令（INSERT）、关联工单 / 流程实例 / AI 摘要（列级 UPDATE）；并确认
// 引擎负责的列与表确实写不了。
func TestAlertActions_AsCoreLoginRole(t *testing.T) {
	admin := newAlertP2Env(t)
	ctx := context.Background()
	d, _ := loginPool(t, admin.pool)
	alerts, actions, ops := admin.usecases(d)
	id := admin.firing()

	if _, err := actions.Ack(ctx, uBob, id); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, err := actions.Transfer(ctx, uAlice, id, uAlice, "接手"); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if _, err := actions.Unack(ctx, uAlice, id); err != nil {
		t.Fatalf("unack: %v", err)
	}
	if _, err := actions.Comment(ctx, uBob, id, "看过了"); err != nil {
		t.Fatalf("comment: %v", err)
	}
	if res, err := actions.BatchAck(ctx, uBob, []string{id}); err != nil || !res[0].OK {
		t.Fatalf("batch ack: %+v, %v", res, err)
	}
	v, err := actions.Close(ctx, uAlice, id, "下线")
	if err != nil || !v.Closing || v.Status != mc.AlertFiring {
		t.Fatalf("close: %+v, %v", v, err)
	}
	if res, err := actions.BatchClose(ctx, uAlice, []string{id}, "下线"); err != nil || !res[0].OK {
		t.Fatalf("batch close: %+v, %v", res, err)
	}
	tk, v, err := actions.CreateTicket(ctx, uAlice, id, biz.AlertTicketInput{TemplateID: newID(t)})
	if err != nil || v.TicketID != tk.ID || v.TicketNumber != "INC-1" {
		t.Fatalf("ticket: %+v / %+v, %v", tk, v, err)
	}
	if err := basedata.NewTxManager(d).WithinTx(ctx, func(tx txrun.Tx) error {
		if err := ops.SetWorkflowInstanceTx(ctx, tx, id, newID(t)); err != nil {
			return err
		}
		return ops.SetAISummaryTx(ctx, tx, id, "摘要")
	}); err != nil {
		t.Fatalf("link columns: %v", err)
	}
	// 详情（时间线合并、展示名、关闭中）与列表同样在授权内。
	det, x, err := alerts.GetDetail(ctx, uBob, id)
	if err != nil || det.Alert.AckUser != uBob || !det.Alert.Closing || det.Alert.AISummary != "摘要" || x.Ticket == nil || x.Ticket.Number != "INC-1" {
		t.Fatalf("detail: %+v / %+v, %v", det, x, err)
	}
	var types []string
	for _, it := range x.Timeline {
		types = append(types, it.Type)
	}
	if got, want := strings.Join(types, ","), "ack,transfer,unack,comment,ack,ticket"; got != want {
		t.Fatalf("timeline = %s, want %s", got, want)
	}
	if _, _, err := alerts.List(ctx, uBob, biz.AlertListQuery{Mine: true}, 1, 20); err != nil {
		t.Fatalf("list: %v", err)
	}
	if n := admin.count("SELECT count(*) FROM monitor_alert_cmd WHERE alert_id = $1 AND status = 1", id); n != 1 {
		t.Fatalf("pending cmds = %d", n)
	}

	// 引擎负责的部分写不了：事件的状态 / 关闭人、指令的处理结果、时间线的改与删。
	for what, sql := range map[string]string{
		"alert status":    "UPDATE monitor_alert SET status = 3, resolve_time = now() WHERE id = $1",
		"alert closed_by": "UPDATE monitor_alert SET closed_by = $1 WHERE id = $1",
		"alert inhibited": "UPDATE monitor_alert SET inhibited = true, inhibit_kind = 1 WHERE id = $1",
		"cmd status":      "UPDATE monitor_alert_cmd SET status = 2 WHERE alert_id = $1",
		"cmd delete":      "DELETE FROM monitor_alert_cmd WHERE alert_id = $1",
		"event update":    "UPDATE monitor_alert_event SET type = 'fire' WHERE alert_id = $1",
		"event delete":    "DELETE FROM monitor_alert_event WHERE alert_id = $1",
	} {
		if _, err := d.Pool.Exec(ctx, sql, id); err == nil || !strings.Contains(err.Error(), "42501") {
			t.Errorf("%s: err = %v, want insufficient privilege (42501)", what, err)
		}
	}
}
