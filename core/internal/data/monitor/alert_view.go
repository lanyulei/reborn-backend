package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.SavedViewReader = (*alertViewRepo)(nil)
	_ biz.SavedViewWriter = (*alertViewRepo)(nil)
)

// alertViewRepo 活跃告警聚合视图（monitor_alert_view，仅 core 读写，物理删除）。
type alertViewRepo struct {
	data *basedata.Data
}

// NewAlertViewRepo new an alertViewRepo.
func NewAlertViewRepo(data *basedata.Data) *alertViewRepo { return &alertViewRepo{data: data} }

// alertViewColumns 聚合视图查询列序（表别名 v）。
const alertViewColumns = `v.id::text, v.owner_id::text, v.name, v.by, v.filters, v.is_public, v.create_time, v.update_time`

func scanAlertView(scan func(dest ...any) error) (*biz.SavedView, error) {
	var v biz.SavedView
	var filters []byte
	if err := scan(&v.ID, &v.OwnerID, &v.Spec.Name, &v.Spec.By, &filters, &v.Spec.IsPublic, &v.CreateTime, &v.UpdateTime); err != nil {
		return nil, err
	}
	if err := decodeJSON(filters, &v.Spec.Filters, "alert view filters"); err != nil {
		return nil, err
	}
	return &v, nil
}

// ListVisible implements biz.SavedViewReader.
func (r *alertViewRepo) ListVisible(ctx context.Context, ownerID string, limit int) ([]*biz.SavedView, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+alertViewColumns+` FROM monitor_alert_view v WHERE v.is_public OR v.owner_id = $1
		 ORDER BY v.is_public DESC, v.name, v.id LIMIT $2`, ownerID, limit)
	if err != nil {
		return nil, fmt.Errorf("list monitor alert views: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.SavedView, 0, 16)
	for rows.Next() {
		v, err := scanAlertView(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan monitor alert view: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// Get implements biz.SavedViewReader.
func (r *alertViewRepo) Get(ctx context.Context, id string) (*biz.SavedView, error) {
	v, err := scanAlertView(r.data.Pool.QueryRow(ctx, "SELECT "+alertViewColumns+" FROM monitor_alert_view v WHERE v.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrAlertViewNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor alert view: %w", err)
	}
	return v, nil
}

func alertViewWriteErr(err error, what string) error {
	if isUniqueViolation(err, "uk_monitor_alert_view_name") {
		return biz.ErrAlertViewNameExists
	}
	return fmt.Errorf("%s monitor alert view: %w", what, err)
}

// Create implements biz.SavedViewWriter。个人视图的数量上限是「先数后写」：同一所有者的新建以事务级
// advisory lock 串行化（跨实例有效，事务结束自动释放）。
func (r *alertViewRepo) Create(ctx context.Context, v *biz.SavedView, maxPersonal int) error {
	filters, err := json.Marshal(v.Spec.Filters)
	if err != nil {
		return fmt.Errorf("encode alert view filters: %w", err)
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if !v.Spec.IsPublic {
			if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('monitor:alert-view:' || $1::text, 0))", v.OwnerID); err != nil {
				return fmt.Errorf("lock monitor alert views of owner: %w", err)
			}
			var n int
			if err := tx.QueryRow(ctx, "SELECT count(*) FROM monitor_alert_view WHERE owner_id = $1 AND NOT is_public", v.OwnerID).Scan(&n); err != nil {
				return fmt.Errorf("count monitor alert views: %w", err)
			}
			if n >= maxPersonal {
				return biz.Detail(biz.ErrAlertViewInvalid, "at most %d personal views per user", maxPersonal)
			}
		}
		err := tx.QueryRow(ctx,
			`INSERT INTO monitor_alert_view (id, owner_id, name, by, filters, is_public) VALUES ($1, $2, $3, $4, $5, $6)
			 RETURNING create_time, update_time`,
			v.ID, v.OwnerID, v.Spec.Name, v.Spec.By, filters, v.Spec.IsPublic).Scan(&v.CreateTime, &v.UpdateTime)
		if err != nil {
			return alertViewWriteErr(err, "insert")
		}
		return nil
	})
}

// Update implements biz.SavedViewWriter。以所有者与原先的公共标记围栏。
func (r *alertViewRepo) Update(ctx context.Context, v *biz.SavedView, wasPublic bool) error {
	filters, err := json.Marshal(v.Spec.Filters)
	if err != nil {
		return fmt.Errorf("encode alert view filters: %w", err)
	}
	err = r.data.Pool.QueryRow(ctx,
		`UPDATE monitor_alert_view SET name = $4, by = $5, filters = $6, is_public = $7, update_time = now()
		 WHERE id = $1 AND owner_id = $2 AND is_public = $3 RETURNING update_time`,
		v.ID, v.OwnerID, wasPublic, v.Spec.Name, v.Spec.By, filters, v.Spec.IsPublic).Scan(&v.UpdateTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return biz.ErrAlertViewNotFound
	}
	if err != nil {
		return alertViewWriteErr(err, "update")
	}
	return nil
}

// Delete implements biz.SavedViewWriter.
func (r *alertViewRepo) Delete(ctx context.Context, id, ownerID string, wasPublic bool) error {
	tag, err := r.data.Pool.Exec(ctx, "DELETE FROM monitor_alert_view WHERE id = $1 AND owner_id = $2 AND is_public = $3", id, ownerID, wasPublic)
	if err != nil {
		return fmt.Errorf("delete monitor alert view: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrAlertViewNotFound
	}
	return nil
}
