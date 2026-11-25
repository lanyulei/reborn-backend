package biz

import (
	"context"
	"slices"
	"testing"
	"time"
)

// 巡检同步工单状态：只把状态置为已完成。改进项是逐条查询工单状态之前读出的，期间用户可能编辑了这条改进项，
// 整体写回会把标题 / 负责人 / 期限覆盖成旧值。
func TestSyncItemTicketsUpdatesStatusOnly(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	inc := &Incident{ID: "i1", Status: StatusResolved}
	row := &ActionItem{ID: "it1", IncidentID: "i1", Title: "修复连接池", Status: ItemOpen, TicketID: "t1", TicketNumber: "T-1"}
	items := &stubItems{row: row, markOK: true, ticketed: []*ActionItem{
		{ID: "it1", IncidentID: "i1", Title: "修复连接池", Status: ItemOpen, TicketID: "t1", TicketNumber: "T-1"},
	}}
	tickets := &stubTickets{view: &TicketInfo{ID: "t1", Finished: true}}
	tickets.onView = func() { row.Title, row.OwnerID = "修复连接池（已改）", "u9" } // 查工单期间用户编辑了这条改进项
	writer := &mapWriter{byID: map[string]*Incident{"i1": inc}}
	events := &recordingEvents{}
	d := &Deps{Txm: fakeTxm{}, Writer: writer, Members: fakeMembers{}, Events: events, Items: items, Tickets: tickets, Logger: testLogger()}
	uc := NewPostmortemUsecase(d, &IncidentUsecase{d: d, now: func() time.Time { return now }}, nil)

	n, err := uc.syncItemTickets(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("syncItemTickets() = %d, %v; want 1, nil", n, err)
	}
	if !slices.Equal(items.marked, []string{"it1"}) || items.saves != 0 {
		t.Fatalf("marked = %v saves = %d, want only a status update and no full write-back", items.marked, items.saves)
	}
	if row.Status != ItemDone || row.Title != "修复连接池（已改）" || row.OwnerID != "u9" {
		t.Fatalf("row after sync = %+v, want done with the concurrent edit kept", row)
	}
	if len(events.appended) != 1 || events.appended[0].Type != EventActionItem || events.appended[0].Detail.TicketID != "t1" {
		t.Fatalf("timeline events = %+v, want one action_item naming the ticket", events.appended)
	}
}

// 工单还没结束：什么都不做。
func TestSyncItemTicketsSkipsUnfinishedTickets(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusResolved}
	row := &ActionItem{ID: "it1", IncidentID: "i1", Status: ItemOpen, TicketID: "t1"}
	items := &stubItems{row: row, markOK: true, ticketed: []*ActionItem{{ID: "it1", IncidentID: "i1", Status: ItemOpen, TicketID: "t1"}}}
	events := &recordingEvents{}
	d := &Deps{
		Txm: fakeTxm{}, Writer: &mapWriter{byID: map[string]*Incident{"i1": inc}}, Members: fakeMembers{}, Events: events,
		Items: items, Tickets: &stubTickets{view: &TicketInfo{ID: "t1"}}, Logger: testLogger(),
	}
	uc := NewPostmortemUsecase(d, &IncidentUsecase{d: d, now: time.Now}, nil)

	n, err := uc.syncItemTickets(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("syncItemTickets() = %d, %v; want 0, nil", n, err)
	}
	if len(items.marked) != 0 || items.saves != 0 || len(events.appended) != 0 || row.Status != ItemOpen {
		t.Fatalf("unfinished ticket must not touch the item: marked %v saves %d events %+v status %d", items.marked, items.saves, events.appended, row.Status)
	}
}

// 查工单期间这条改进项已被手动置为完成：围栏未命中，不记时间线，也不算失败。
func TestSyncItemTicketsSkipsWhenItemNoLongerOpen(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusResolved}
	row := &ActionItem{ID: "it1", IncidentID: "i1", Status: ItemDone, TicketID: "t1"}
	items := &stubItems{row: row, ticketed: []*ActionItem{{ID: "it1", IncidentID: "i1", Status: ItemOpen, TicketID: "t1"}}}
	writer := &mapWriter{byID: map[string]*Incident{"i1": inc}}
	events := &recordingEvents{}
	d := &Deps{
		Txm: fakeTxm{}, Writer: writer, Members: fakeMembers{}, Events: events, Items: items,
		Tickets: &stubTickets{view: &TicketInfo{ID: "t1", Finished: true}}, Logger: testLogger(),
	}
	uc := NewPostmortemUsecase(d, &IncidentUsecase{d: d, now: time.Now}, nil)

	n, err := uc.syncItemTickets(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("syncItemTickets() = %d, %v; want 0, nil (no-op is not an error)", n, err)
	}
	if !slices.Equal(items.marked, []string{"it1"}) || items.saves != 0 || len(events.appended) != 0 || len(writer.saved) != 0 {
		t.Fatalf("fenced-out sync must write nothing: marked %v saves %d events %+v incident saves %d",
			items.marked, items.saves, events.appended, len(writer.saved))
	}
}

// 改进项建单：只写工单号。建单是跨域调用（可达数秒），其间用户对这条改进项的编辑不能被建单前读出的旧对象覆盖。
func TestCreateItemTicketUpdatesTicketOnly(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	due := now.AddDate(0, 0, 7)
	inc := &Incident{ID: "i1", Number: "INC-20260101-001", Status: StatusResolved, Severity: SeverityS3}
	row := &ActionItem{ID: "it1", IncidentID: "i1", Title: "修复连接池", Status: ItemOpen}
	items := &stubItems{row: row, item: &ActionItem{ID: "it1", IncidentID: "i1", Title: "修复连接池", Status: ItemOpen}}
	tickets := &stubTickets{}
	tickets.onCreate = func() { row.Title, row.DueDate = "修复连接池（已改）", &due } // 建单期间用户编辑了这条改进项
	events := &recordingEvents{}
	d := &Deps{
		Txm: fakeTxm{}, Incidents: fakeIncidents{inc: inc}, Writer: &mapWriter{byID: map[string]*Incident{"i1": inc}},
		Members: fakeMembers{}, Admins: fakeAdmins{}, Events: events, Items: items, Tickets: tickets, Dir: fakeDirectory{}, Logger: testLogger(),
	}
	uc := NewPostmortemUsecase(d, &IncidentUsecase{d: d, now: func() time.Time { return now }}, nil)

	v, err := uc.CreateItemTicket(context.Background(), "u1", "i1", "it1", "tpl", 0)
	if err != nil {
		t.Fatalf("CreateItemTicket() error = %v", err)
	}
	if !slices.Equal(items.tickets, []string{"it1:t1:T-1"}) || items.saves != 0 {
		t.Fatalf("ticket links = %v saves = %d, want only the ticket columns written", items.tickets, items.saves)
	}
	if row.TicketID != "t1" || row.TicketNumber != "T-1" || row.Title != "修复连接池（已改）" || row.DueDate == nil || !row.DueDate.Equal(due) {
		t.Fatalf("row after linking = %+v, want the ticket linked and the concurrent edit kept", row)
	}
	if v.TicketID != "t1" || v.TicketNumber != "T-1" {
		t.Fatalf("view ticket = %q %q, want the created ticket", v.TicketID, v.TicketNumber)
	}
	if len(tickets.created) != 1 || len(events.appended) != 1 || events.appended[0].Type != EventTicket {
		t.Fatalf("created tickets = %d, timeline events = %+v; want one ticket and one ticket event", len(tickets.created), events.appended)
	}
}
