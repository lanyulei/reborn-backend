package biz_test

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

const (
	ciHost  = "018f3e1a-0c10-7000-8000-000000000001"
	ciVM    = "018f3e1a-0c10-7000-8000-000000000002"
	ciDark  = "018f3e1a-0c10-7000-8000-000000000003"
	ownerID = "018f3e1a-0c11-7000-8000-000000000001"
)

// fakeDesk 工单域桩：按幂等键去重；visible 里的用户才能查看工单。
type fakeDesk struct {
	created []biz.TicketCreate
	byReq   map[string]*biz.TicketInfo
	visible map[string][]string
	err     error
}

func (d *fakeDesk) Create(_ context.Context, p biz.TicketCreate) (*biz.TicketInfo, error) {
	if d.err != nil {
		return nil, d.err
	}
	key := p.Creator + "|" + p.RequestID
	if t, ok := d.byReq[key]; ok {
		return t, nil
	}
	d.created = append(d.created, p)
	n := strconv.Itoa(len(d.created))
	t := &biz.TicketInfo{ID: "tk-" + n, Number: "INC-000" + n, Title: p.Title, Status: 1, Creator: p.Creator}
	if d.byReq == nil {
		d.byReq = map[string]*biz.TicketInfo{}
	}
	d.byReq[key] = t
	return t, nil
}

func (d *fakeDesk) View(_ context.Context, ticketID, userID string) (*biz.TicketInfo, error) {
	for _, t := range d.byReq {
		if t.ID == ticketID && (t.Creator == userID || slices.Contains(d.visible[ticketID], userID)) {
			return t, nil
		}
	}
	return nil, nil
}

func (d *fakeDesk) Comment(context.Context, string, string, string) error { return nil }

// fakeCMDB CMDB 桩：viewers[ci] 里的用户才能在资源管理里查看该资源。
type fakeCMDB struct {
	cards   map[string]biz.CICard
	viewers map[string][]string
	impact  map[string][]biz.CIImpact
	trunc   bool
}

func (c *fakeCMDB) Card(_ context.Context, userID, ciID string) (*biz.CICard, bool, error) {
	card, ok := c.cards[ciID]
	if !ok {
		return nil, false, nil
	}
	if !slices.Contains(c.viewers[ciID], userID) {
		return nil, true, nil
	}
	return &card, true, nil
}

func (c *fakeCMDB) Impact(_ context.Context, userID, ciID string, _, limit int) ([]biz.CIImpact, bool, error) {
	nodes := slices.Clone(c.impact[ciID])
	for i := range nodes {
		nodes[i].CanView = slices.Contains(c.viewers[nodes[i].ID], userID)
	}
	return nodes[:min(len(nodes), limit)], c.trunc, nil
}

type fakeCIIndex map[string]*biz.CIEntry

func (f fakeCIIndex) Briefs(_ context.Context, ids []string) (map[string]*biz.CIEntry, error) {
	out := map[string]*biz.CIEntry{}
	for _, id := range ids {
		if e, ok := f[id]; ok {
			out[id] = e
		}
	}
	return out, nil
}
func (f fakeCIIndex) ListValid(context.Context, int) ([]*biz.CIEntry, error) { return nil, nil }
func (f fakeCIIndex) Summary(context.Context) (int32, *time.Time, error)     { return 0, nil, nil }

type fakeInhibitNames map[string]string

func (f fakeInhibitNames) InhibitNames(_ context.Context, ids []string) (map[string]string, error) {
	out := map[string]string{}
	for _, id := range ids {
		if n, ok := f[id]; ok {
			out[id] = n
		}
	}
	return out, nil
}

// p2env 在 P1 的 env 上接上事件操作、时间线与展示名。
type p2env struct {
	*env
	ops     *testfake.MonitorAlertOps
	desk    *fakeDesk
	cmdb    *fakeCMDB
	actions *biz.AlertActionUsecase
	ids     map[string]string
}

func newP2Env(t *testing.T) *p2env {
	t.Helper()
	e := newEnv(t)
	ops := testfake.NewMonitorAlertOps(e.store)
	desk := &fakeDesk{visible: map[string][]string{}}
	cmdb := &fakeCMDB{
		cards: map[string]biz.CICard{
			ciHost: {ID: ciHost, Name: "host-1", ModelCode: "host", ModelName: "主机", Env: "prod", Status: "active", Owner: ownerID, OwnerName: "Owner", Tags: []string{"core"}},
			ciDark: {ID: ciDark, Name: "secret-db", ModelCode: "db", Owner: "外包团队"},
		},
		viewers: map[string][]string{ciHost: {uAlice, uBob}, ciVM: {uAlice}},
		impact:  map[string][]biz.CIImpact{ciHost: {{ID: ciVM, Name: "vm-1", ModelCode: "vm", ModelName: "虚拟机", Depth: 1}, {ID: ciDark, Name: "secret-db", ModelCode: "db", ModelName: "数据库", Depth: 2}}},
	}
	cis := fakeCIIndex{
		ciHost: {ID: ciHost, Name: "host-1", Valid: true}, ciVM: {ID: ciVM, Name: "vm-1", Valid: true}, ciDark: {ID: ciDark, Name: "secret-db", Valid: false},
	}
	inhibits := fakeInhibitNames{"inh-1": "主机失联抑制"}
	decorator := biz.NewAlertDecorator(e.org, inhibits, cis, ops, ops)
	detail := biz.NewAlertDetailUsecase(ops, ops, ops, cis, cmdb, desk, e.org, e.store.Mutes(), inhibits)
	e.alerts.WithP2(decorator, detail)
	p := &p2env{env: e, ops: ops, desk: desk, cmdb: cmdb, ids: seedAlerts(e)}
	p.actions = biz.NewAlertActionUsecase(e.alerts, detail, testfake.NewTxManager(), ops, ops, ops, ops, ops, desk, cmdb)
	return p
}

func (p *p2env) eventTypes(alertID string) []string {
	var out []string
	for _, ev := range p.ops.Events() {
		if ev.AlertID == alertID {
			out = append(out, ev.Type)
		}
	}
	return out
}

// notify 让 user 成为事件的通知接收方。
func (p *p2env) notify(alertID string, users ...string) {
	p.ops.SeedNotifyLog(&biz.NotifyLog{ID: "log-" + alertID + users[0], AlertID: alertID, Kind: mc.NotifyKindFiring, Seq: 1, Source: mc.NotifySourceRule, Recipients: users})
}

// TestAlertAction_Authorization 每个操作的范围矩阵：范围外且非接收方 404；只是接收方 403；只读可以认领 /
// 取消认领 / 转交 / 备注，不能关闭 / 转工单；可管理都可以。
func TestAlertAction_Authorization(t *testing.T) {
	ctx := context.Background()
	type call func(p *p2env, user, id string) error
	actions := map[string]struct {
		do     call
		manage bool
	}{
		"ack":      {func(p *p2env, u, id string) error { _, err := p.actions.Ack(ctx, u, id); return err }, false},
		"unack":    {func(p *p2env, u, id string) error { _, err := p.actions.Unack(ctx, u, id); return err }, false},
		"transfer": {func(p *p2env, u, id string) error { _, err := p.actions.Transfer(ctx, u, id, uCarol, ""); return err }, false},
		"comment":  {func(p *p2env, u, id string) error { _, err := p.actions.Comment(ctx, u, id, "看一下"); return err }, false},
		"close":    {func(p *p2env, u, id string) error { _, err := p.actions.Close(ctx, u, id, "下线"); return err }, true},
		"ticket": {func(p *p2env, u, id string) error {
			_, _, err := p.actions.CreateTicket(ctx, u, id, biz.AlertTicketInput{TemplateID: "tpl"})
			return err
		}, true},
	}
	for name, act := range actions {
		t.Run(name, func(t *testing.T) {
			p := newP2Env(t)
			host := p.ids["host"]
			// carol 只对 app 有范围：host 的事件对她不存在。
			wantErr(t, "out of scope", act.do(p, uCarol, host), biz.ErrAlertNotFound)
			wantErr(t, "missing alert", act.do(p, uAlice, "al-nope"), biz.ErrAlertNotFound)
			// carol 被通知过：知道事件存在，但不能操作。
			p.notify(host, uCarol)
			wantErr(t, "recipient only", act.do(p, uCarol, host), biz.ErrForbidden)
			// 停用的接收方与陌生人一样。
			p.notify(p.ids["app"], uGone)
			wantErr(t, "inactive recipient", act.do(p, uGone, p.ids["app"]), biz.ErrAlertNotFound)
			// bob 只读。
			err := act.do(p, uBob, host)
			if act.manage {
				wantErr(t, "read level on manage action", err, biz.ErrForbidden)
			} else if err != nil {
				t.Fatalf("read level: %v", err)
			}
			// alice 可管理；admin 不限。
			if err := act.do(p, uAlice, p.ids["db"]); err != nil {
				t.Fatalf("manage level: %v", err)
			}
			if err := act.do(p, uAdmin, p.ids["app"]); err != nil {
				t.Fatalf("admin: %v", err)
			}
		})
	}
}

func TestAlertAction_AckUnackTransfer(t *testing.T) {
	ctx := context.Background()
	p := newP2Env(t)
	host, old := p.ids["host"], p.ids["host-old"]

	// 已恢复的事件不能认领 / 取消认领 / 转交 / 关闭。
	_, err := p.actions.Ack(ctx, uBob, old)
	wantErr(t, "ack resolved", err, biz.ErrAlertNotFiring)
	_, err = p.actions.Unack(ctx, uBob, old)
	wantErr(t, "unack resolved", err, biz.ErrAlertNotFiring)
	_, err = p.actions.Transfer(ctx, uBob, old, uAlice, "")
	wantErr(t, "transfer resolved", err, biz.ErrAlertNotFiring)
	_, err = p.actions.Close(ctx, uAlice, old, "")
	wantErr(t, "close resolved", err, biz.ErrAlertNotFiring)

	// 认领：写认领人与时间、留一条时间线；视图带展示名与处理状态。
	v, err := p.actions.Ack(ctx, uBob, host)
	if err != nil || v.AckUser != uBob || v.AckUserName != "Bob" || v.AckTime == nil || v.HandleState() != biz.HandleAcked || !v.CanAck || v.CanManage {
		t.Fatalf("Ack = %+v, %v", v, err)
	}
	// 本人重复认领：原样返回，不多写时间线。
	if _, err := p.actions.Ack(ctx, uBob, host); err != nil {
		t.Fatalf("re-ack: %v", err)
	}
	if got := p.eventTypes(host); !slices.Equal(got, []string{mc.AlertEventAck}) {
		t.Fatalf("timeline = %v", got)
	}
	// 他人认领：409，metadata 带认领人。
	_, err = p.actions.Ack(ctx, uDave, host)
	wantErr(t, "ack by another", err, biz.ErrAlertAlreadyAcked)
	if md := kerrors.FromError(err).Metadata; md["ack_user_id"] != uBob || md["ack_user_name"] != "Bob" {
		t.Fatalf("metadata = %v", md)
	}
	// 取消认领：只读的旁人不行，认领人本人或可管理的人可以。
	_, err = p.actions.Unack(ctx, uDave, host)
	wantErr(t, "unack by a reader who is not the acker", err, biz.ErrForbidden)
	// 转交：已被他人认领的事件，只读的旁人不能转交。
	_, err = p.actions.Transfer(ctx, uDave, host, uAlice, "")
	wantErr(t, "transfer someone else's ack without manage", err, biz.ErrForbidden)
	// 转交对象须可用、不能是当前认领人；说明不能过长。
	_, err = p.actions.Transfer(ctx, uBob, host, uGone, "")
	wantErr(t, "transfer to inactive", err, biz.ErrAlertActionInvalid)
	_, err = p.actions.Transfer(ctx, uBob, host, uBob, "")
	wantErr(t, "transfer to current acker", err, biz.ErrAlertActionInvalid)
	_, err = p.actions.Transfer(ctx, uBob, host, uAlice, strings.Repeat("长", 501))
	wantErr(t, "long note", err, biz.ErrAlertActionInvalid)
	// 认领人转交：保留原认领时间。
	ackTime := *p.store.Alert(host).AckTime
	v, err = p.actions.Transfer(ctx, uBob, host, uAlice, " 夜班交接 ")
	if err != nil || v.AckUser != uAlice || !v.AckTime.Equal(ackTime) {
		t.Fatalf("Transfer = %+v, %v", v, err)
	}
	evs := p.ops.Events()
	if last := evs[len(evs)-1]; last.Type != mc.AlertEventTransfer || last.Operator != uBob || last.Detail.FromUser != uBob || last.Detail.ToUser != uAlice || last.Detail.Text != "夜班交接" {
		t.Fatalf("transfer event = %+v", last)
	}
	// 可管理的人可以取消别人的认领。
	p.store.SetAlert(host, func(a *biz.Alert) { a.AckUser = uBob })
	v, err = p.actions.Unack(ctx, uAlice, host)
	if err != nil || v.AckUser != "" || v.AckTime != nil || v.HandleState() != biz.HandleUnacked {
		t.Fatalf("Unack = %+v, %v", v, err)
	}
	// 本来就未认领：原样返回，不写时间线。
	n := len(p.ops.Events())
	if _, err := p.actions.Unack(ctx, uBob, host); err != nil || len(p.ops.Events()) != n {
		t.Fatalf("unack unacked: %v", err)
	}
	// 未认领时转交 = 指派：认领时间取当前。
	v, err = p.actions.Transfer(ctx, uDave, host, uBob, "")
	if err != nil || v.AckUser != uBob || v.AckTime == nil {
		t.Fatalf("assign = %+v, %v", v, err)
	}
}

// TestAlertAction_Races 条件更新落空（别人抢先）时按最新状态给结论，不覆盖别人的写入。
func TestAlertAction_Races(t *testing.T) {
	ctx := context.Background()
	p := newP2Env(t)
	host := p.ids["host"]
	// 认领前一刻被 dave 抢先。
	p.ops.BeforeWrite = func() {
		p.store.SetAlert(host, func(a *biz.Alert) { now := time.Now(); a.AckUser, a.AckTime = uDave, &now })
	}
	_, err := p.actions.Ack(ctx, uBob, host)
	wantErr(t, "lost ack race", err, biz.ErrAlertAlreadyAcked)
	if a := p.store.Alert(host); a.AckUser != uDave || len(p.ops.Events()) != 0 {
		t.Fatalf("ack overwritten: %+v", a)
	}
	// 取消认领前一刻事件已恢复。
	p.ops.BeforeWrite = func() {
		p.store.SetAlert(host, func(a *biz.Alert) { now := time.Now(); a.Status, a.ResolveTime = mc.AlertResolved, &now })
	}
	_, err = p.actions.Unack(ctx, uDave, host)
	wantErr(t, "unack after resolve", err, biz.ErrAlertNotFiring)
	// 转交前一刻认领人被换成了别人：只读的操作者不再有权转交。
	db := p.ids["db"]
	p.store.SetAlert(db, func(a *biz.Alert) { a.AckUser = uBob })
	p.ops.BeforeWrite = func() { p.store.SetAlert(db, func(a *biz.Alert) { a.AckUser = uAlice }) }
	_, err = p.actions.Transfer(ctx, uBob, db, uDave, "")
	wantErr(t, "transfer after acker changed", err, biz.ErrForbidden)
	if a := p.store.Alert(db); a.AckUser != uAlice {
		t.Fatalf("transfer overwrote a concurrent change: %+v", a)
	}
}

func TestAlertAction_CommentAndClose(t *testing.T) {
	ctx := context.Background()
	p := newP2Env(t)
	host, old := p.ids["host"], p.ids["host-old"]

	for _, bad := range []string{"", "  \n\t ", strings.Repeat("字", 2001)} {
		_, err := p.actions.Comment(ctx, uBob, host, bad)
		wantErr(t, "bad comment", err, biz.ErrAlertActionInvalid)
	}
	// 任何状态的事件都可以备注；返回新增的时间线条目。
	item, err := p.actions.Comment(ctx, uBob, old, "  已确认是误报  ")
	if err != nil || item.Type != mc.AlertEventComment || item.Detail.Text != "已确认是误报" || item.Operator == nil || item.Operator.Name != "Bob" || item.Time.IsZero() {
		t.Fatalf("Comment = %+v, %v", item, err)
	}

	// 关闭：只下达指令；事件仍是告警中、Closing 为真；不写时间线（由引擎写）。
	_, err = p.actions.Close(ctx, uAlice, host, strings.Repeat("长", 256))
	wantErr(t, "long reason", err, biz.ErrAlertActionInvalid)
	v, err := p.actions.Close(ctx, uAlice, host, " 机器下线 ")
	if err != nil || v.Status != mc.AlertFiring || !v.Closing || v.ClosedBy != "" {
		t.Fatalf("Close = %+v, %v", v, err)
	}
	// 重复下达是空操作。
	if _, err := p.actions.Close(ctx, uAdmin, host, "again"); err != nil {
		t.Fatalf("re-close: %v", err)
	}
	cmds := p.ops.Cmds()
	if len(cmds) != 1 || cmds[0].Operator != uAlice || cmds[0].Reason != "机器下线" || cmds[0].Status != mc.CmdPending {
		t.Fatalf("cmds = %+v", cmds)
	}
	if got := p.eventTypes(host); len(got) != 0 {
		t.Fatalf("close must not write the timeline, got %v", got)
	}
	// 列表也带 Closing。
	views, _, err := p.alerts.List(ctx, uAlice, biz.AlertListQuery{AlertFilter: biz.AlertFilter{Statuses: []int32{mc.AlertFiring}}}, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range views {
		if v.Closing != (v.ID == host) {
			t.Fatalf("closing of %s = %v", v.ID, v.Closing)
		}
	}
}

func TestAlertAction_Batch(t *testing.T) {
	ctx := context.Background()
	p := newP2Env(t)
	host, old, db, app := p.ids["host"], p.ids["host-old"], p.ids["db"], p.ids["app"]
	p.store.SetAlert(db, func(a *biz.Alert) { a.AckUser = uAlice })
	p.notify(app, uBob) // 批量里只是接收方的事件按不存在处理

	results, err := p.actions.BatchAck(ctx, uBob, []string{host, old, db, app, "al-nope"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"", "MONITOR_ALERT_NOT_FIRING", "MONITOR_ALERT_ALREADY_ACKED", "MONITOR_ALERT_NOT_FOUND", "MONITOR_ALERT_NOT_FOUND"}
	for i, r := range results {
		if r.Reason != want[i] || r.OK != (want[i] == "") || (r.OK == (r.Message != "")) {
			t.Fatalf("result[%d] = %+v, want reason %q", i, r, want[i])
		}
	}
	if results[0].ID != host || p.store.Alert(host).AckUser != uBob {
		t.Fatal("first item must be acked")
	}
	// 批量关闭要求可管理：bob 只读 → 逐条 403；alice 对 host / db 可以。
	results, err = p.actions.BatchClose(ctx, uBob, []string{host, db}, "x")
	if err != nil || results[0].Reason != "MONITOR_FORBIDDEN" || results[1].Reason != "MONITOR_FORBIDDEN" {
		t.Fatalf("BatchClose(read) = %+v, %v", results, err)
	}
	results, err = p.actions.BatchClose(ctx, uAlice, []string{host, old, app, db}, "批量下线")
	if err != nil || !results[0].OK || results[1].Reason != "MONITOR_ALERT_NOT_FIRING" || results[2].Reason != "MONITOR_ALERT_NOT_FOUND" || !results[3].OK {
		t.Fatalf("BatchClose = %+v, %v", results, err)
	}
	if len(p.ops.Cmds()) != 2 {
		t.Fatalf("cmds = %+v", p.ops.Cmds())
	}
	// 入参：空、重复、超过上限。
	ids := make([]string, 0, 201)
	for i := range 201 {
		ids = append(ids, "al-"+strings.Repeat("x", i+1))
	}
	for _, bad := range [][]string{nil, {host, host}, ids} {
		_, err := p.actions.BatchAck(ctx, uAlice, bad)
		wantErr(t, "bad ids", err, biz.ErrAlertActionInvalid)
	}
	_, err = p.actions.BatchClose(ctx, uAlice, []string{host}, strings.Repeat("长", 256))
	wantErr(t, "long reason", err, biz.ErrAlertActionInvalid)
}

func TestAlertAction_CreateTicket(t *testing.T) {
	ctx := context.Background()
	p := newP2Env(t)
	host, db := p.ids["host"], p.ids["db"]
	p.store.SetAlert(host, func(a *biz.Alert) { a.CIID = ciHost })
	p.store.SetAlert(db, func(a *biz.Alert) { a.CIID = ciDark })

	// 缺省标题 / 优先级 / 幂等键；调用方能查看资源 → 作为关联资源。
	tk, v, err := p.actions.CreateTicket(ctx, uAlice, host, biz.AlertTicketInput{TemplateID: "tpl", Data: map[string]any{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	got := p.desk.created[0]
	if got.Title != "【严重】cpu" || got.Priority != 3 || got.RequestID != host || got.Creator != uAlice || !slices.Equal(got.ResourceIDs, []string{ciHost}) || got.Data["k"] != "v" {
		t.Fatalf("ticket params = %+v", got)
	}
	if !tk.CanView || tk.Number == "" || v.TicketID != tk.ID || v.TicketNumber != tk.Number {
		t.Fatalf("ticket = %+v alert = %+v", tk, v)
	}
	evs := p.ops.Events()
	if len(evs) != 1 || evs[0].Type != mc.AlertEventTicket || evs[0].Operator != uAlice || evs[0].Detail.TicketID != tk.ID || evs[0].Detail.Auto {
		t.Fatalf("events = %+v", evs)
	}
	// 已关联工单：409，metadata 带工单 ID。
	_, _, err = p.actions.CreateTicket(ctx, uAdmin, host, biz.AlertTicketInput{TemplateID: "tpl"})
	wantErr(t, "ticket exists", err, biz.ErrAlertTicketExists)
	if kerrors.FromError(err).Metadata["ticket_id"] != tk.ID {
		t.Fatalf("metadata = %v", kerrors.FromError(err).Metadata)
	}
	// 带幂等键的重复提交（提单人本人）：返回首次创建的工单，不再建单、不再写时间线；换个人带幂等键仍是 409。
	again, _, err := p.actions.CreateTicket(ctx, uAlice, host, biz.AlertTicketInput{TemplateID: "tpl", RequestID: "retry-1"})
	if err != nil || again.ID != tk.ID || again.Number != tk.Number || len(p.desk.created) != 1 || len(p.ops.Events()) != 1 {
		t.Fatalf("replay = %+v, err=%v, created=%d", again, err, len(p.desk.created))
	}
	_, _, err = p.actions.CreateTicket(ctx, uAdmin, host, biz.AlertTicketInput{TemplateID: "tpl", RequestID: "retry-1"})
	wantErr(t, "replay by another user", err, biz.ErrAlertTicketExists)
	// 调用方看不到资源 → 不带关联资源；显式的标题 / 优先级 / 幂等键照用。
	_, _, err = p.actions.CreateTicket(ctx, uAlice, db, biz.AlertTicketInput{TemplateID: "tpl", Title: " 数据库慢 ", Priority: 1, RequestID: "req-1", CreatorDept: "d1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := p.desk.created[1]; got.Title != "数据库慢" || got.Priority != 1 || got.RequestID != "req-1" || got.CreatorDept != "d1" || len(got.ResourceIDs) != 0 {
		t.Fatalf("ticket params = %+v", got)
	}
	// 工单域的错误原样返回，事件不被关联。
	app := p.ids["app"]
	p.desk.err = kerrors.BadRequest("TICKET_TEMPLATE_UNAVAILABLE", "nope")
	_, _, err = p.actions.CreateTicket(ctx, uAdmin, app, biz.AlertTicketInput{TemplateID: "tpl"})
	if kerrors.FromError(err).Reason != "TICKET_TEMPLATE_UNAVAILABLE" || p.store.Alert(app).TicketID != "" {
		t.Fatalf("ticket-domain error = %v", err)
	}
	p.desk.err = nil
	// 重试（工单已建、关联尚未写入时崩溃）：同一幂等键拿回同一张工单并完成关联，只有一条时间线。
	p.desk.byReq[uAdmin+"|"+app] = &biz.TicketInfo{ID: "tk-9", Number: "INC-0009", Creator: uAdmin}
	tk, _, err = p.actions.CreateTicket(ctx, uAdmin, app, biz.AlertTicketInput{TemplateID: "tpl"})
	if err != nil || tk.ID != "tk-9" || p.store.Alert(app).TicketID != "tk-9" {
		t.Fatalf("retry = %+v, %v", tk, err)
	}
	if got := p.eventTypes(app); !slices.Equal(got, []string{mc.AlertEventTicket}) {
		t.Fatalf("timeline = %v", got)
	}
}

// TestAlertAction_TicketLinkRace 工单建好后发现事件已被别人关联了另一张工单：409，不覆盖、不写时间线。
func TestAlertAction_TicketLinkRace(t *testing.T) {
	ctx := context.Background()
	p := newP2Env(t)
	host := p.ids["host"]
	racing := &racingDesk{fakeDesk: p.desk, onCreate: func() { p.store.SetAlert(host, func(a *biz.Alert) { a.TicketID = "tk-other" }) }}
	detail := biz.NewAlertDetailUsecase(p.ops, p.ops, p.ops, fakeCIIndex{}, p.cmdb, racing, p.org, p.store.Mutes(), fakeInhibitNames{})
	actions := biz.NewAlertActionUsecase(p.alerts, detail, testfake.NewTxManager(), p.ops, p.ops, p.ops, p.ops, p.ops, racing, p.cmdb)
	_, _, err := actions.CreateTicket(ctx, uAlice, host, biz.AlertTicketInput{TemplateID: "tpl"})
	wantErr(t, "lost link race", err, biz.ErrAlertTicketExists)
	if kerrors.FromError(err).Metadata["ticket_id"] != "tk-other" || p.store.Alert(host).TicketID != "tk-other" || len(p.ops.Events()) != 0 {
		t.Fatalf("link race: %v / %+v", err, p.ops.Events())
	}
}

type racingDesk struct {
	*fakeDesk
	onCreate func()
}

func (d *racingDesk) Create(ctx context.Context, p biz.TicketCreate) (*biz.TicketInfo, error) {
	t, err := d.fakeDesk.Create(ctx, p)
	d.onCreate()
	return t, err
}

func TestAlertDetail_TimelineAndCards(t *testing.T) {
	ctx := context.Background()
	p := newP2Env(t)
	host := p.ids["host"]
	base := time.Now().Add(-time.Hour)
	at := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }
	p.store.SetAlert(host, func(a *biz.Alert) {
		a.CIID, a.TicketID, a.Inhibited, a.InhibitKind, a.InhibitID, a.InhibitAlertID = ciHost, "tk-1", true, mc.InhibitRule, "inh-1", p.ids["db"]
		a.ClosedBy = uAlice
	})
	// 另一条范围外（app）的活跃告警也关联同一资源：不计入 bob / alice 的活跃告警数。
	p.store.SetAlert(p.ids["app"], func(a *biz.Alert) { a.CIID = ciHost })
	p.store.SetAlert(p.ids["db"], func(a *biz.Alert) { a.CIID = ciVM })
	p.desk.byReq = map[string]*biz.TicketInfo{"x": {ID: "tk-1", Number: "INC-0001", Title: "CPU 高", Status: 2, Creator: uAlice}}
	for i, ev := range []*biz.AlertEvent{
		{Type: mc.AlertEventFire, Detail: mc.AlertEventDetail{Severity: 3}},
		{Type: mc.AlertEventMute, Detail: mc.AlertEventDetail{CIID: ciHost}},
		{Type: mc.AlertEventInhibit, Detail: mc.AlertEventDetail{InhibitKind: mc.InhibitRule, InhibitID: "inh-1", InhibitAlertID: p.ids["db"]}},
		{Type: mc.AlertEventComment, Operator: uBob, Detail: mc.AlertEventDetail{Text: "内部备注"}},
		{Type: mc.AlertEventTransfer, Operator: uBob, Detail: mc.AlertEventDetail{FromUser: uBob, ToUser: "u-deleted", Text: "交接"}},
		{Type: mc.AlertEventTicket, Operator: uAlice, Detail: mc.AlertEventDetail{TicketID: "tk-1", TicketNumber: "INC-0001", Auto: true}},
		{Type: mc.AlertEventClose, Operator: uAlice, Detail: mc.AlertEventDetail{Cause: mc.CloseCauseManual, Text: "下线"}},
	} {
		ev.ID, ev.AlertID, ev.CreateTime = "ev-"+string(rune('a'+i)), host, at(i*2)
		p.ops.SeedEvent(ev)
	}
	p.ops.SeedNotifyLog(&biz.NotifyLog{ID: "log-1", AlertID: host, Kind: mc.NotifyKindEscalation, Seq: 2, Level: 3, Source: mc.NotifySourceEscalation, Recipients: []string{uCarol, uAlice}, CreateTime: at(1)})
	p.ops.SeedNotifyLog(&biz.NotifyLog{ID: "log-2", AlertID: host, Kind: mc.NotifyKindFiring, Seq: 1, Source: mc.NotifySourceRule, Recipients: []string{uAlice}, CreateTime: at(3)})

	// 完整视图（alice：可管理、能看资源与工单）。
	d, x, err := p.alerts.GetDetail(ctx, uAlice, host)
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, it := range x.Timeline {
		order = append(order, it.ID)
	}
	if want := []string{"ev-a", "log-1", "ev-b", "log-2", "ev-c", "ev-d", "ev-e", "ev-f", "ev-g"}; !slices.Equal(order, want) {
		t.Fatalf("timeline order = %v", order)
	}
	byID := map[string]*biz.TimelineItem{}
	for _, it := range x.Timeline {
		byID[it.ID] = it
	}
	if n := byID["log-1"]; n.Type != biz.TimelineNotification || n.Log == nil || n.Log.Level != 3 || n.Operator != nil {
		t.Fatalf("notification item = %+v", n)
	}
	if m := byID["ev-b"]; !m.Maintenance || m.Operator != nil {
		t.Fatalf("maintenance mute = %+v", m)
	}
	if i := byID["ev-c"]; i.InhibitName != "主机失联抑制" {
		t.Fatalf("inhibit item = %+v", i)
	}
	if tr := byID["ev-e"]; tr.Operator.Name != "Bob" || tr.FromUser.Name != "Bob" || tr.ToUser.ID != "u-deleted" || !tr.ToUser.Missing {
		t.Fatalf("transfer item = %+v / %+v", tr.FromUser, tr.ToUser)
	}
	a := d.Alert
	if a.InhibitName != "主机失联抑制" || a.InhibitAlertName != "slow" || a.CIName != "host-1" || a.TicketNumber != "INC-0001" || a.ClosedByName != "Alice" ||
		a.HandleState() != biz.HandleInhibited || !a.CanManage || !a.CanAck {
		t.Fatalf("decorated alert = %+v", a)
	}
	if ci := x.CI; ci == nil || !ci.CanView || !ci.Exists || ci.Owner != "Owner" || ci.OwnerID != ownerID || ci.ModelName != "主机" || ci.FiringCount != 1 {
		t.Fatalf("ci card = %+v", x.CI)
	}
	if len(x.Impact) != 2 || !x.Impact[0].CanView || x.Impact[0].ModelName != "虚拟机" || x.Impact[0].FiringCount != 1 ||
		x.Impact[1].CanView || x.Impact[1].ModelCode != "" || x.Impact[1].ModelName != "" || x.Impact[1].Name != "secret-db" || x.Impact[1].Depth != 2 {
		t.Fatalf("impact = %+v / %+v", x.Impact[0], x.Impact[1])
	}
	if tk := x.Ticket; tk == nil || !tk.CanView || tk.Title != "CPU 高" || tk.Status != 2 || !tk.Auto || tk.Number != "INC-0001" {
		t.Fatalf("ticket card = %+v", x.Ticket)
	}

	// dave：监控范围内（host 只读），但没有 CMDB 权限、看不了工单 → 资源只有 ID 与名称，工单只有编号。
	_, x, err = p.alerts.GetDetail(ctx, uDave, host)
	if err != nil {
		t.Fatal(err)
	}
	if ci := x.CI; ci.CanView || ci.Name != "host-1" || !ci.Exists || ci.Env != "" || ci.Owner != "" || ci.OwnerID != "" || ci.ModelCode != "" || len(ci.Tags) != 0 {
		t.Fatalf("ci card without cmdb access = %+v", ci)
	}
	for _, n := range x.Impact {
		if n.CanView || n.ModelCode != "" {
			t.Fatalf("impact node leaked: %+v", n)
		}
	}
	if tk := x.Ticket; tk.CanView || tk.Title != "" || tk.Status != 0 || tk.Number != "INC-0001" || !tk.Auto {
		t.Fatalf("ticket card without access = %+v", tk)
	}

	// 收件人视图（carol：范围外，被升级通知过）：只有触发 / 关闭与发给她的通知；没有操作人、备注与文字；
	// 资源只有 ID 与名称、没有影响面；不带抑制规则。
	d, x, err = p.alerts.GetDetail(ctx, uCarol, host)
	if err != nil {
		t.Fatal(err)
	}
	order = order[:0]
	for _, it := range x.Timeline {
		order = append(order, it.ID)
		if it.Operator != nil || it.Detail.Text != "" || it.FromUser != nil {
			t.Fatalf("recipient timeline leaked: %+v", it)
		}
	}
	if !slices.Equal(order, []string{"ev-a", "log-1", "ev-g"}) {
		t.Fatalf("recipient timeline = %v", order)
	}
	if byRecipient := x.Timeline[1].Log; !slices.Equal(byRecipient.Recipients, []string{uCarol}) {
		t.Fatalf("recipient log = %+v", byRecipient)
	}
	if x.Timeline[2].Detail.Cause != mc.CloseCauseManual {
		t.Fatalf("close cause dropped: %+v", x.Timeline[2])
	}
	a = d.Alert
	if !a.Recipient || a.CanAck || a.CanManage || a.InhibitID != "" || a.InhibitName != "" || !a.Inhibited || a.CIName != "host-1" {
		t.Fatalf("recipient alert = %+v", a)
	}
	if ci := x.CI; ci.CanView || ci.Name != "host-1" || ci.FiringCount != 0 || len(x.Impact) != 0 {
		t.Fatalf("recipient ci = %+v impact = %d", ci, len(x.Impact))
	}

	// 资源已不在 CMDB：exists 为 false，名称取自索引。
	p.store.SetAlert(host, func(a *biz.Alert) { a.CIID = ciDark })
	_, x, err = p.alerts.GetDetail(ctx, uAlice, host)
	if err != nil || x.CI.CanView || x.CI.Exists || x.CI.Name != "secret-db" {
		t.Fatalf("gone ci = %+v, %v", x.CI, err)
	}
}

func TestAlertList_P2Filters(t *testing.T) {
	ctx := context.Background()
	p := newP2Env(t)
	host, db, app := p.ids["host"], p.ids["db"], p.ids["app"]
	now := time.Now()
	p.store.SetAlert(host, func(a *biz.Alert) { a.AckUser, a.AckTime, a.CIID = uBob, &now, ciHost })
	p.store.SetAlert(app, func(a *biz.Alert) {
		a.AckUser, a.AckTime, a.Inhibited, a.InhibitKind = uBob, &now, true, mc.InhibitTopology
	})
	list := func(user string, q biz.AlertListQuery) []string {
		t.Helper()
		q.Statuses = []int32{mc.AlertFiring}
		views, _, err := p.alerts.List(ctx, user, q, 1, 50)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, len(views))
		for _, v := range views {
			ids = append(ids, v.ID)
		}
		slices.Sort(ids)
		return ids
	}
	yes, no := true, false
	sorted := func(ids ...string) []string { slices.Sort(ids); return ids }
	cases := []struct {
		name string
		f    biz.AlertFilter
		want []string
	}{
		{"ci", biz.AlertFilter{CIID: ciHost}, sorted(host)},
		{"acked", biz.AlertFilter{Acked: &yes}, sorted(host, app)},
		{"unacked", biz.AlertFilter{Acked: &no}, sorted(db)},
		{"inhibited", biz.AlertFilter{Inhibited: &yes}, sorted(app)},
		{"ack user", biz.AlertFilter{AckUserID: uBob}, sorted(host, app)},
		{"handle inhibited", biz.AlertFilter{HandleState: biz.HandleInhibited}, sorted(app)},
		{"handle muted", biz.AlertFilter{HandleState: biz.HandleMuted}, sorted(db)},
		{"handle acked", biz.AlertFilter{HandleState: biz.HandleAcked}, sorted(host)},
		{"handle unacked", biz.AlertFilter{HandleState: biz.HandleUnacked}, []string{}},
	}
	for _, c := range cases {
		if got := list(uAdmin, biz.AlertListQuery{AlertFilter: c.f}); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	// 「我负责的」= 通知过我的或由我认领的；范围外由我认领的事件（app）按收件人视图返回。
	if got := list(uBob, biz.AlertListQuery{Mine: true}); !slices.Equal(got, sorted(host, app)) {
		t.Errorf("mine = %v", got)
	}
	views, _, _ := p.alerts.List(ctx, uBob, biz.AlertListQuery{Mine: true}, 1, 50)
	for _, v := range views {
		if v.Recipient != (v.ID == app) || v.CanAck != (v.ID == host) || v.AckUserName != "Bob" || v.AckDurationS < 0 {
			t.Errorf("mine view %s = recipient %v canAck %v ackName %q", v.ID, v.Recipient, v.CanAck, v.AckUserName)
		}
	}
	// 聚合的新过滤条件。
	items, total, err := p.alerts.Aggregate(ctx, uAdmin, biz.AggregateQuery{By: "rule", Acked: &yes, Inhibited: &no, Labels: map[string]string{"env": "prod"}})
	if err != nil || total != 1 || len(items) != 1 || items[0].Name != "cpu" {
		t.Fatalf("Aggregate = %+v, %d, %v", items, total, err)
	}
}
