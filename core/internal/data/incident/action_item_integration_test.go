//go:build integration

package data

import (
	"context"
	"errors"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 改进项的两个窄更新只改目标列：SetTicketTx 只写工单号，MarkDoneTx 只写状态并围栏「仍是待完成」。
// 它们替代了用建单 / 查工单之前读出的旧对象整体写回（会覆盖期间的并发编辑）。
func TestActionItemRepo_NarrowUpdates(t *testing.T) {
	d := newIntegrationIncidentData(t)
	repo, tm := NewActionItemRepo(d), basedata.NewTxManager(d)
	ctx := context.Background()
	const (
		itemOpen = "018f3e1a-0a40-7000-8000-0000000000a1"
		itemDone = "018f3e1a-0a40-7000-8000-0000000000a2"
		ticketID = "018f3e1a-0a50-7000-8000-0000000000b1"
	)
	seedIncident(t, d, incA, biz.StatusResolved)
	for _, row := range []struct {
		id     string
		status int32
	}{{itemOpen, biz.ItemOpen}, {itemDone, biz.ItemDone}} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO incident_action_item (id, incident_id, title, status) VALUES ($1, $2::uuid, '原标题', $3)",
			row.id, incA, row.status); err != nil {
			t.Fatal(err)
		}
	}
	type itemRow struct {
		title, ticketID, ticketNumber string
		status                        int32
	}
	read := func(id string) itemRow {
		t.Helper()
		var r itemRow
		if err := d.Pool.QueryRow(ctx,
			"SELECT title, COALESCE(ticket_id::text, ''), ticket_number, status FROM incident_action_item WHERE id = $1", id).
			Scan(&r.title, &r.ticketID, &r.ticketNumber, &r.status); err != nil {
			t.Fatalf("read item %s: %v", id, err)
		}
		return r
	}

	// 建单：只写工单号，标题与状态不动。
	if err := tm.WithinTx(ctx, func(tx txrun.Tx) error {
		return repo.SetTicketTx(ctx, tx, itemOpen, ticketID, "T-1")
	}); err != nil {
		t.Fatalf("SetTicketTx() error = %v", err)
	}
	if r := read(itemOpen); r.title != "原标题" || r.ticketID != ticketID || r.ticketNumber != "T-1" || r.status != biz.ItemOpen {
		t.Fatalf("after SetTicketTx: %+v, want the ticket linked and everything else untouched", r)
	}
	// 改进项不存在：ErrActionItemNotFound。
	if err := tm.WithinTx(ctx, func(tx txrun.Tx) error {
		return repo.SetTicketTx(ctx, tx, incB, ticketID, "T-2")
	}); !errors.Is(err, biz.ErrActionItemNotFound) {
		t.Fatalf("SetTicketTx() on a missing item: error = %v, want ErrActionItemNotFound", err)
	}

	// 工单结束：待完成 → 已完成，只改状态。
	var marked bool
	if err := tm.WithinTx(ctx, func(tx txrun.Tx) error {
		var err error
		marked, err = repo.MarkDoneTx(ctx, tx, itemOpen)
		return err
	}); err != nil || !marked {
		t.Fatalf("MarkDoneTx() = %v, %v; want true, nil", marked, err)
	}
	if r := read(itemOpen); r.status != biz.ItemDone || r.title != "原标题" || r.ticketID != ticketID {
		t.Fatalf("after MarkDoneTx: %+v, want done with the title and ticket kept", r)
	}
	// 已完成的（刚被置为完成的、以及原本就完成的）：围栏未命中，不再写。
	for _, id := range []string{itemOpen, itemDone} {
		if err := tm.WithinTx(ctx, func(tx txrun.Tx) error {
			var err error
			marked, err = repo.MarkDoneTx(ctx, tx, id)
			return err
		}); err != nil || marked {
			t.Fatalf("MarkDoneTx(%s) = %v, %v; want false, nil", id, marked, err)
		}
	}
}
