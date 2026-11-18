package data

import (
	"context"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.ProjectReader = (*projectRepo)(nil)
	_ biz.ProjectWriter = (*projectRepo)(nil)
)

// projectColumns 项目查询列序。密文不读出，只取是否已设置。
const projectColumns = `p.id::text, p.name, p.repo_url, p.ref, p.auth_type, p.username, p.secret IS NOT NULL, p.description,
	(SELECT count(*) FROM job_template t WHERE t.project_id = p.id AND t.delete_time IS NULL),
	p.version, p.create_time, p.update_time, COALESCE(p.update_user::text, '')`

func scanProject(scan func(dest ...any) error) (*biz.Project, error) {
	var p biz.Project
	if err := scan(&p.ID, &p.Name, &p.RepoURL, &p.Ref, &p.AuthType, &p.Username, &p.HasSecret, &p.Description,
		&p.TemplateCount, &p.Version, &p.CreateTime, &p.UpdateTime, &p.UpdateUser); err != nil {
		return nil, err
	}
	return &p, nil
}

type projectRepo struct {
	data *basedata.Data
}

// NewProjectRepo new a projectRepo.
func NewProjectRepo(data *basedata.Data) *projectRepo { return &projectRepo{data: data} }

// List implements biz.ProjectReader.
func (r *projectRepo) List(ctx context.Context, keyword string, page, pageSize int64) ([]*biz.Project, int64, error) {
	var a sqlArgs
	where := "p.delete_time IS NULL"
	if keyword != "" {
		k := a.add(dbx.LikeContains(keyword))
		where += " AND (p.name ILIKE " + k + " OR p.repo_url ILIKE " + k + " OR p.description ILIKE " + k + ")"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM job_project p WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count job projects: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+projectColumns+" FROM job_project p WHERE "+where+
		" ORDER BY p.create_time DESC, p.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list job projects: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Project, 0, pageSize)
	for rows.Next() {
		p, err := scanProject(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan job project: %w", err)
		}
		items = append(items, p)
	}
	return items, total, rows.Err()
}

// Get implements biz.ProjectReader.
func (r *projectRepo) Get(ctx context.Context, id string) (*biz.Project, error) {
	p, err := scanProject(r.data.Pool.QueryRow(ctx,
		"SELECT "+projectColumns+" FROM job_project p WHERE p.id = $1 AND p.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrProjectNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job project: %w", err)
	}
	return p, nil
}

// ListOptions implements biz.ProjectReader.
func (r *projectRepo) ListOptions(ctx context.Context) ([]*biz.Project, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT id::text, name, ref FROM job_project WHERE delete_time IS NULL ORDER BY name, id LIMIT $1", maxOptions)
	if err != nil {
		return nil, fmt.Errorf("list job project options: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Project, 0, 16)
	for rows.Next() {
		var p biz.Project
		if err := rows.Scan(&p.ID, &p.Name, &p.Ref); err != nil {
			return nil, fmt.Errorf("scan job project option: %w", err)
		}
		items = append(items, &p)
	}
	return items, rows.Err()
}

func projectWriteErr(err error, what string) error {
	if isUniqueViolation(err, "uk_job_project_name") {
		return biz.ErrProjectNameExists
	}
	return fmt.Errorf("%s job project: %w", what, err)
}

// Create implements biz.ProjectWriter.
func (r *projectRepo) Create(ctx context.Context, p *biz.Project, secret []byte, actor string) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO job_project (id, name, repo_url, ref, auth_type, username, secret, description, create_user, update_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
		p.ID, p.Name, p.RepoURL, p.Ref, p.AuthType, p.Username, dbx.NilIfEmptyBytes(secret), p.Description, dbx.NilIfEmpty(actor))
	if err != nil {
		return projectWriteErr(err, "insert")
	}
	return nil
}

// Update implements biz.ProjectWriter.
func (r *projectRepo) Update(ctx context.Context, p *biz.Project, secret biz.SecretChange, expectVersion int32, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockVersion(ctx, tx, "job_project", p.ID, expectVersion, biz.ErrProjectNotFound); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE job_project SET name = $2, repo_url = $3, ref = $4, auth_type = $5, username = $6,
			   secret = CASE WHEN $7 THEN $8::bytea ELSE secret END, description = $9,
			   version = version + 1, update_time = now(), update_user = $10
			 WHERE id = $1`,
			p.ID, p.Name, p.RepoURL, p.Ref, p.AuthType, p.Username, secret.Set, dbx.NilIfEmptyBytes(secret.Sealed),
			p.Description, dbx.NilIfEmpty(actor)); err != nil {
			return projectWriteErr(err, "update")
		}
		return nil
	})
}

// Delete implements biz.ProjectWriter。先行锁项目行再查引用（与模板保存的共享锁互斥）；软删同时清除密文。
func (r *projectRepo) Delete(ctx context.Context, id, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockForDelete(ctx, tx, "job_project", id, biz.ErrProjectNotFound); err != nil {
			return err
		}
		names, total, err := templatesUsing(ctx, tx, "project_id", id)
		if err != nil {
			return err
		}
		if total > 0 {
			return biz.InUse(biz.ErrProjectInUse, names, total)
		}
		if _, err := tx.Exec(ctx,
			"UPDATE job_project SET delete_time = now(), delete_user = $2, secret = NULL, version = version + 1 WHERE id = $1",
			id, dbx.NilIfEmpty(actor)); err != nil {
			return fmt.Errorf("delete job project: %w", err)
		}
		return nil
	})
}
