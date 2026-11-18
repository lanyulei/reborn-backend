package data

import (
	"context"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.HelmRepoStore   = (*helmRepo)(nil)
	_ biz.HelmRepoDeleter = (*helmRepo)(nil)
)

// helmSelect 凭据只读出「是否已保存」。
const helmSelect = `SELECT id::text, name, type, url, username, ca_cert, insecure_skip_verify, description, enabled,
	secret IS NOT NULL, version, create_time, update_time FROM k8s_helm_repo`

func scanHelmRepo(scan func(dest ...any) error) (*biz.HelmRepo, error) {
	var r biz.HelmRepo
	if err := scan(&r.ID, &r.Name, &r.Type, &r.URL, &r.Username, &r.CACert, &r.Insecure, &r.Description, &r.Enabled,
		&r.HasSecret, &r.Version, &r.CreateTime, &r.UpdateTime); err != nil {
		return nil, err
	}
	return &r, nil
}

type helmRepo struct {
	data *basedata.Data
}

// NewHelmRepo new a helmRepo.
func NewHelmRepo(data *basedata.Data) *helmRepo { return &helmRepo{data: data} }

// List implements biz.HelmRepoStore.
func (r *helmRepo) List(ctx context.Context, keyword string, enabledOnly bool, page, pageSize int64) ([]*biz.HelmRepo, int64, error) {
	a := sqlArgs{}
	where := " WHERE delete_time IS NULL"
	if keyword != "" {
		k := a.add(dbx.LikeContains(keyword))
		where += " AND (name ILIKE " + k + " OR url ILIKE " + k + ")"
	}
	if enabledOnly {
		where += " AND enabled"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM k8s_helm_repo"+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count k8s helm repos: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, helmSelect+where+" ORDER BY name LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list k8s helm repos: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.HelmRepo, 0, 16)
	for rows.Next() {
		item, err := scanHelmRepo(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan k8s helm repo: %w", err)
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

// Get implements biz.HelmRepoStore.
func (r *helmRepo) Get(ctx context.Context, id string) (*biz.HelmRepo, error) {
	item, err := scanHelmRepo(r.data.Pool.QueryRow(ctx, helmSelect+" WHERE id = $1 AND delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrHelmRepoNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get k8s helm repo: %w", err)
	}
	return item, nil
}

// Save implements biz.HelmRepoStore.
func (r *helmRepo) Save(ctx context.Context, h *biz.HelmRepo, secret *[]byte, expectVersion int32, audit *biz.AuditEntry) error {
	actor := ""
	if audit != nil {
		actor = audit.UserID
	}
	// keep：沿用已保存的凭据；否则写入 value（空 = 清除）。
	keep, value := secret == nil, []byte(nil)
	if secret != nil {
		value = *secret
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var err error
		if expectVersion == 0 {
			_, err = tx.Exec(ctx,
				`INSERT INTO k8s_helm_repo (id, name, type, url, username, secret, ca_cert, insecure_skip_verify, description, enabled, create_user, update_user)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)`,
				h.ID, h.Name, h.Type, h.URL, h.Username, dbx.NilIfEmptyBytes(value), h.CACert, h.Insecure, h.Description, h.Enabled, dbx.NilIfEmpty(actor))
		} else {
			var version int32
			err = tx.QueryRow(ctx, "SELECT version FROM k8s_helm_repo WHERE id = $1 AND delete_time IS NULL FOR UPDATE", h.ID).Scan(&version)
			if errors.Is(err, pgx.ErrNoRows) {
				return biz.ErrHelmRepoNotFound
			}
			if err != nil {
				return fmt.Errorf("lock k8s helm repo: %w", err)
			}
			if version != expectVersion {
				return biz.ErrVersionConflict
			}
			_, err = tx.Exec(ctx,
				`UPDATE k8s_helm_repo SET name = $2, url = $3, username = $4, secret = CASE WHEN $5 THEN secret ELSE $6 END,
				   ca_cert = $7, insecure_skip_verify = $8, description = $9, enabled = $10,
				   version = version + 1, update_time = now(), update_user = $11
				 WHERE id = $1`,
				h.ID, h.Name, h.URL, h.Username, keep, dbx.NilIfEmptyBytes(value), h.CACert, h.Insecure, h.Description, h.Enabled, dbx.NilIfEmpty(actor))
		}
		if err != nil {
			if isUniqueViolation(err, "uk_k8s_helm_repo_name") {
				return biz.ErrHelmRepoNameExists
			}
			return fmt.Errorf("save k8s helm repo: %w", err)
		}
		return insertAudit(ctx, tx, audit, "", "", h.Name)
	})
}

// Delete implements biz.HelmRepoDeleter：软删并清除凭据。
func (r *helmRepo) Delete(ctx context.Context, id string, audit *biz.AuditEntry) error {
	actor := ""
	if audit != nil {
		actor = audit.UserID
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var name string
		err := tx.QueryRow(ctx,
			`UPDATE k8s_helm_repo SET delete_time = now(), delete_user = $2, secret = NULL
			 WHERE id = $1 AND delete_time IS NULL RETURNING name`, id, dbx.NilIfEmpty(actor)).Scan(&name)
		if errors.Is(err, pgx.ErrNoRows) {
			return biz.ErrHelmRepoNotFound
		}
		if err != nil {
			return fmt.Errorf("delete k8s helm repo: %w", err)
		}
		return insertAudit(ctx, tx, audit, "", "", name)
	})
}
