package data

import (
	"context"
	"encoding/json"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.CalendarReader = (*calendarRepo)(nil)
	_ biz.CalendarStore  = (*calendarRepo)(nil)
)

type calendarRepo struct {
	data *basedata.Data
}

// NewCalendarRepo new a calendarRepo.
func NewCalendarRepo(data *basedata.Data) *calendarRepo { return &calendarRepo{data: data} }

// Snapshot implements biz.CalendarReader。空对象（尚未物化）解码为零值。
func (r *calendarRepo) Snapshot(ctx context.Context) (mc.CalendarSnapshot, error) {
	var raw []byte
	var snap mc.CalendarSnapshot
	if err := r.data.Pool.QueryRow(ctx, "SELECT config FROM monitor_calendar WHERE id = 1").Scan(&raw); err != nil {
		return snap, fmt.Errorf("get monitor calendar: %w", err)
	}
	return snap, decodeJSON(raw, &snap, "monitor calendar")
}

// Save implements biz.CalendarStore。jsonb 的比较与键序、空白无关：内容相同即不写、不抬版本号，
// 多实例各自同步也只有真正的变化会被写入。
func (r *calendarRepo) Save(ctx context.Context, snap *mc.CalendarSnapshot) (bool, error) {
	raw := []byte("{}")
	if snap != nil {
		var err error
		if raw, err = json.Marshal(snap); err != nil {
			return false, fmt.Errorf("encode monitor calendar: %w", err)
		}
	}
	changed := false
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			"UPDATE monitor_calendar SET config = $1::jsonb, update_time = now() WHERE id = 1 AND config IS DISTINCT FROM $1::jsonb", raw)
		if err != nil {
			return fmt.Errorf("save monitor calendar: %w", err)
		}
		if changed = tag.RowsAffected() == 1; !changed {
			return nil
		}
		return bumpCfg(ctx, tx, mc.CfgCalendar)
	})
	return changed && err == nil, err
}
