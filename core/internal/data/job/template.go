package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.TemplateReader = (*templateRepo)(nil)
	_ biz.TemplateWriter = (*templateRepo)(nil)
)

// templateSelect 模板查询：项目 / 凭据的名称随行取出（已删除的为空）。
const templateSelect = `SELECT t.id::text, t.name, t.description, t.project_id::text, t.playbook, t.credential_id::text,
	t.target, t.params, t.options, t.runner_zone, t.exclusive, t.enabled, t.require_approval, t.slice_size,
	COALESCE(p.name, ''), COALESCE(p.ref, ''), COALESCE(c.name, ''),
	t.version, t.create_time, t.update_time, COALESCE(t.update_user::text, '')
	FROM job_template t
	LEFT JOIN job_project p ON p.id = t.project_id AND p.delete_time IS NULL
	LEFT JOIN job_credential c ON c.id = t.credential_id AND c.delete_time IS NULL`

func scanTemplate(scan func(dest ...any) error) (*biz.Template, error) {
	var t biz.Template
	var target, params, options []byte
	if err := scan(&t.ID, &t.Name, &t.Description, &t.ProjectID, &t.Playbook, &t.CredentialID,
		&target, &params, &options, &t.RunnerZone, &t.Exclusive, &t.Enabled, &t.RequireApproval, &t.SliceSize,
		&t.ProjectName, &t.ProjectRef, &t.CredentialName,
		&t.Version, &t.CreateTime, &t.UpdateTime, &t.UpdateUser); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(target, &t.Target); err != nil {
		return nil, fmt.Errorf("decode job template target: %w", err)
	}
	if err := json.Unmarshal(params, &t.Params); err != nil {
		return nil, fmt.Errorf("decode job template params: %w", err)
	}
	if err := json.Unmarshal(options, &t.Options); err != nil {
		return nil, fmt.Errorf("decode job template options: %w", err)
	}
	return &t, nil
}

type templateRepo struct {
	data *basedata.Data
}

// NewTemplateRepo new a templateRepo.
func NewTemplateRepo(data *basedata.Data) *templateRepo { return &templateRepo{data: data} }

// List implements biz.TemplateReader.
func (r *templateRepo) List(ctx context.Context, filter biz.TemplateFilter, page, pageSize int64) ([]*biz.Template, int64, error) {
	var a sqlArgs
	where := "t.delete_time IS NULL"
	if filter.Keyword != "" {
		k := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (t.name ILIKE " + k + " OR t.playbook ILIKE " + k + " OR t.description ILIKE " + k + ")"
	}
	if filter.ProjectID != "" {
		where += " AND t.project_id = " + a.add(filter.ProjectID)
	}
	if filter.Enabled != nil {
		where += " AND t.enabled = " + a.add(*filter.Enabled)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM job_template t WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count job templates: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, templateSelect+" WHERE "+where+
		" ORDER BY t.create_time DESC, t.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list job templates: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Template, 0, pageSize)
	for rows.Next() {
		t, err := scanTemplate(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan job template: %w", err)
		}
		items = append(items, t)
	}
	return items, total, rows.Err()
}

// Get implements biz.TemplateReader.
func (r *templateRepo) Get(ctx context.Context, id string) (*biz.Template, error) {
	t, err := scanTemplate(r.data.Pool.QueryRow(ctx, templateSelect+" WHERE t.id = $1 AND t.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrTemplateNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job template: %w", err)
	}
	return t, nil
}

// templateJSON 编码模板的三个 jsonb 列。
func templateJSON(t *biz.Template) (target, params, options []byte, err error) {
	if target, err = json.Marshal(t.Target); err != nil {
		return nil, nil, nil, fmt.Errorf("encode job template target: %w", err)
	}
	if params, err = json.Marshal(t.Params); err != nil {
		return nil, nil, nil, fmt.Errorf("encode job template params: %w", err)
	}
	if options, err = json.Marshal(t.Options); err != nil {
		return nil, nil, nil, fmt.Errorf("encode job template options: %w", err)
	}
	return target, params, options, nil
}

// lockRefs 在写事务内确认模板引用的项目与凭据都存在并加共享锁（与它们的删除互斥）。
// 加锁顺序固定为「项目 → 凭据」。
func lockRefs(ctx context.Context, tx pgx.Tx, projectID, credentialID string) error {
	var locked string
	err := tx.QueryRow(ctx, "SELECT id::text FROM job_project WHERE id = $1 AND delete_time IS NULL FOR SHARE", projectID).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return biz.TemplateInvalid("the selected project no longer exists")
	}
	if err != nil {
		return fmt.Errorf("lock job project: %w", err)
	}
	err = tx.QueryRow(ctx, "SELECT id::text FROM job_credential WHERE id = $1 AND delete_time IS NULL FOR SHARE", credentialID).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return biz.TemplateInvalid("the selected credential no longer exists")
	}
	if err != nil {
		return fmt.Errorf("lock job credential: %w", err)
	}
	return nil
}

func templateWriteErr(err error, what string) error {
	if isUniqueViolation(err, "uk_job_template_name") {
		return biz.ErrTemplateNameExists
	}
	return fmt.Errorf("%s job template: %w", what, err)
}

// Create implements biz.TemplateWriter.
func (r *templateRepo) Create(ctx context.Context, t *biz.Template, actor string) error {
	target, params, options, err := templateJSON(t)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockRefs(ctx, tx, t.ProjectID, t.CredentialID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO job_template (id, name, description, project_id, playbook, credential_id, target, params, options,
			   runner_zone, exclusive, enabled, require_approval, slice_size, create_user, update_user)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $15)`,
			t.ID, t.Name, t.Description, t.ProjectID, t.Playbook, t.CredentialID, target, params, options,
			t.RunnerZone, t.Exclusive, t.Enabled, t.RequireApproval, t.SliceSize, dbx.NilIfEmpty(actor)); err != nil {
			return templateWriteErr(err, "insert")
		}
		return nil
	})
}

// Update implements biz.TemplateWriter。先锁引用、再锁模板行：与 Create 同一加锁顺序。
func (r *templateRepo) Update(ctx context.Context, t *biz.Template, expectVersion int32, actor string) error {
	target, params, options, err := templateJSON(t)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockRefs(ctx, tx, t.ProjectID, t.CredentialID); err != nil {
			return err
		}
		if err := lockVersion(ctx, tx, "job_template", t.ID, expectVersion, biz.ErrTemplateNotFound); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE job_template SET name = $2, description = $3, project_id = $4, playbook = $5, credential_id = $6,
			   target = $7, params = $8, options = $9, runner_zone = $10, exclusive = $11, enabled = $12,
			   require_approval = $13, slice_size = $14,
			   version = version + 1, update_time = now(), update_user = $15
			 WHERE id = $1`,
			t.ID, t.Name, t.Description, t.ProjectID, t.Playbook, t.CredentialID, target, params, options,
			t.RunnerZone, t.Exclusive, t.Enabled, t.RequireApproval, t.SliceSize, dbx.NilIfEmpty(actor)); err != nil {
			return templateWriteErr(err, "update")
		}
		return nil
	})
}

// Delete implements biz.TemplateWriter。
func (r *templateRepo) Delete(ctx context.Context, id, actor string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE job_template SET delete_time = now(), delete_user = $2, version = version + 1
		 WHERE id = $1 AND delete_time IS NULL`, id, dbx.NilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("delete job template: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrTemplateNotFound
	}
	return nil
}
