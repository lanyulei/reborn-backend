// Package data 工单域的 data 层：服务目录、工单与评论的 PostgreSQL 仓储。
package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.CatalogReader      = (*catalogRepo)(nil)
	_ biz.CategoryWriter     = (*catalogRepo)(nil)
	_ biz.TemplateWriter     = (*catalogRepo)(nil)
	_ biz.WorkflowReferences = (*catalogRepo)(nil)
)

// catalogRepo 服务目录仓储的 PostgreSQL 实现。
type catalogRepo struct {
	data *basedata.Data
}

// NewCatalogRepo new a catalogRepo.
func NewCatalogRepo(data *basedata.Data) *catalogRepo {
	return &catalogRepo{data: data}
}

// uniqueViolation 报告 err 是否为指定约束上的唯一冲突。
func uniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == constraint
}

// ListCategories implements biz.CatalogReader.
func (r *catalogRepo) ListCategories(ctx context.Context) ([]*biz.Category, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT id, name, description, sort FROM ticket_category WHERE delete_time IS NULL ORDER BY sort, create_time, id")
	if err != nil {
		return nil, fmt.Errorf("list ticket categories: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Category, error) {
		var c biz.Category
		return &c, row.Scan(&c.ID, &c.Name, &c.Description, &c.Sort)
	})
	if err != nil {
		return nil, fmt.Errorf("scan ticket category: %w", err)
	}
	return out, nil
}

// ListTemplates implements biz.CatalogReader（不含表单）。
func (r *catalogRepo) ListTemplates(ctx context.Context, enabledOnly bool) ([]*biz.Template, error) {
	sql := `SELECT id, category_id, name, description, workflow_key, number_prefix, status, sort, update_time, ci_link, icon, form_columns
		FROM ticket_template WHERE delete_time IS NULL`
	if enabledOnly {
		sql += " AND status = 1"
	}
	rows, err := r.data.Pool.Query(ctx, sql+" ORDER BY sort, create_time, id")
	if err != nil {
		return nil, fmt.Errorf("list ticket templates: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Template, error) {
		var (
			t                       biz.Template
			status, ciLink, columns int16
		)
		err := row.Scan(&t.ID, &t.CategoryID, &t.Name, &t.Description, &t.WorkflowKey, &t.NumberPrefix, &status, &t.Sort, &t.UpdateTime, &ciLink, &t.Icon, &columns)
		t.Status, t.CILink, t.FormColumns = int32(status), int32(ciLink), int32(columns)
		return &t, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan ticket template: %w", err)
	}
	return out, nil
}

// GetTemplate implements biz.CatalogReader：提单事务内调用时复用事务连接。
func (r *catalogRepo) GetTemplate(ctx context.Context, id string) (*biz.Template, error) {
	var (
		t                       biz.Template
		status, ciLink, columns int16
		form, sla, escalate     []byte
	)
	err := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		`SELECT t.id, t.category_id, c.name, t.name, t.description, t.workflow_key, t.number_prefix, t.status, t.sort, t.form, t.update_time,
			t.sla, t.sla_escalate, t.ci_link, t.icon, t.form_columns
		 FROM ticket_template t JOIN ticket_category c ON c.id = t.category_id
		 WHERE t.id = $1 AND t.delete_time IS NULL`, id).
		Scan(&t.ID, &t.CategoryID, &t.CategoryName, &t.Name, &t.Description, &t.WorkflowKey, &t.NumberPrefix, &status, &t.Sort, &form, &t.UpdateTime,
			&sla, &escalate, &ciLink, &t.Icon, &columns)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrTemplateNotFound
		}
		return nil, fmt.Errorf("get ticket template: %w", err)
	}
	t.Status, t.CILink, t.FormColumns = int32(status), int32(ciLink), int32(columns)
	if err := json.Unmarshal(form, &t.Form); err != nil {
		return nil, fmt.Errorf("decode ticket template form: %w", err)
	}
	if err := json.Unmarshal(sla, &t.SLA); err != nil {
		return nil, fmt.Errorf("decode ticket template sla: %w", err)
	}
	if err := json.Unmarshal(escalate, &t.SLAEscalate); err != nil {
		return nil, fmt.Errorf("decode ticket template escalation: %w", err)
	}
	return &t, nil
}

// DefinitionReferences implements bizworkflow.DefinitionReferrer：仍绑定着流程标识 key 的启用中的
// 模板（已停用的模板提不了单，不妨碍流程停用 / 删除；它再启用时会校验流程可发起）。
func (r *catalogRepo) DefinitionReferences(ctx context.Context, key string) ([]string, error) {
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`SELECT name FROM ticket_template WHERE workflow_key = $1 AND status = 1 AND delete_time IS NULL
		 ORDER BY name LIMIT 5`, key)
	if err != nil {
		return nil, fmt.Errorf("list ticket templates by workflow: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var name string
		err := row.Scan(&name)
		return fmt.Sprintf("ticket template %q", name), err
	})
	if err != nil {
		return nil, fmt.Errorf("scan ticket template name: %w", err)
	}
	return out, nil
}

// CreateCategory implements biz.CategoryWriter：数量上限随写入一并判定（并发创建可能略超上限，
// 上限只为约束目录规模，不要求精确）。
func (r *catalogRepo) CreateCategory(ctx context.Context, c *biz.Category, limit int) error {
	tag, err := r.data.Pool.Exec(ctx,
		`INSERT INTO ticket_category (id, name, description, sort, create_user, update_user)
		 SELECT $1, $2, $3, $4, $5, $5
		 WHERE (SELECT count(*) FROM ticket_category WHERE delete_time IS NULL) < $6`,
		c.ID, c.Name, c.Description, c.Sort, basedata.OperatorArg(ctx), limit)
	if err != nil {
		if uniqueViolation(err, "uk_ticket_category_name") {
			return biz.ErrCategoryNameExists
		}
		return fmt.Errorf("insert ticket category: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrLimitExceeded
	}
	return nil
}

// UpdateCategory implements biz.CategoryWriter.
func (r *catalogRepo) UpdateCategory(ctx context.Context, c *biz.Category) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE ticket_category SET name = $2, description = $3, sort = $4, update_time = now(), update_user = $5
		 WHERE id = $1 AND delete_time IS NULL`, c.ID, c.Name, c.Description, c.Sort, basedata.OperatorArg(ctx))
	if err != nil {
		if uniqueViolation(err, "uk_ticket_category_name") {
			return biz.ErrCategoryNameExists
		}
		return fmt.Errorf("update ticket category: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrCategoryNotFound
	}
	return nil
}

// DeleteCategory implements biz.CategoryWriter：锁分类行后判定无模板再软删；模板创建方
// 对分类行持键共享锁（见 CreateTemplate），二者互斥，不会留下挂在已删分类下的模板。
func (r *catalogRepo) DeleteCategory(ctx context.Context, id string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var locked bool
		err := tx.QueryRow(ctx, "SELECT true FROM ticket_category WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).Scan(&locked)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return biz.ErrCategoryNotFound
			}
			return fmt.Errorf("lock ticket category: %w", err)
		}
		var used bool
		if err := tx.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM ticket_template WHERE category_id = $1 AND delete_time IS NULL)", id).Scan(&used); err != nil {
			return fmt.Errorf("check ticket category usage: %w", err)
		}
		if used {
			return biz.ErrCategoryInUse
		}
		if _, err := tx.Exec(ctx, "UPDATE ticket_category SET delete_time = now(), delete_user = $2 WHERE id = $1",
			id, basedata.OperatorArg(ctx)); err != nil {
			return fmt.Errorf("soft delete ticket category: %w", err)
		}
		return nil
	})
}

// lockCategory 对未删除的分类加键共享锁（与删除分类互斥）；不存在返回 ErrCategoryNotFound。
func lockCategory(ctx context.Context, tx pgx.Tx, id string) error {
	var ok bool
	err := tx.QueryRow(ctx, "SELECT true FROM ticket_category WHERE id = $1 AND delete_time IS NULL FOR KEY SHARE", id).Scan(&ok)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return biz.ErrCategoryNotFound
		}
		return fmt.Errorf("lock ticket category: %w", err)
	}
	return nil
}

// CreateTemplate implements biz.TemplateWriter.
func (r *catalogRepo) CreateTemplate(ctx context.Context, t *biz.Template, limit int) error {
	form, sla, escalate, err := encodeTemplate(t)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockCategory(ctx, tx, t.CategoryID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx,
			`INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort, update_time,
				create_user, update_user, sla, sla_escalate, ci_link, icon, form_columns)
			 SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $13, $14, $15, $16, $17
			 WHERE (SELECT count(*) FROM ticket_template WHERE delete_time IS NULL) < $12`,
			t.ID, t.CategoryID, t.Name, t.Description, t.WorkflowKey, t.NumberPrefix, form, t.Status, t.Sort, t.UpdateTime,
			basedata.OperatorArg(ctx), limit, sla, escalate, t.CILink, t.Icon, t.FormColumns)
		if err != nil {
			if uniqueViolation(err, "uk_ticket_template_name") {
				return biz.ErrTemplateNameExists
			}
			return fmt.Errorf("insert ticket template: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrLimitExceeded
		}
		return nil
	})
}

// UpdateTemplate implements biz.TemplateWriter.
func (r *catalogRepo) UpdateTemplate(ctx context.Context, t *biz.Template) error {
	form, sla, escalate, err := encodeTemplate(t)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockCategory(ctx, tx, t.CategoryID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx,
			`UPDATE ticket_template SET category_id = $2, name = $3, description = $4, workflow_key = $5, number_prefix = $6,
				form = $7, status = $8, sort = $9, update_time = now(), update_user = $10, sla = $11, sla_escalate = $12, ci_link = $13, icon = $14, form_columns = $15
			 WHERE id = $1 AND delete_time IS NULL`,
			t.ID, t.CategoryID, t.Name, t.Description, t.WorkflowKey, t.NumberPrefix, form, t.Status, t.Sort, basedata.OperatorArg(ctx),
			sla, escalate, t.CILink, t.Icon, t.FormColumns)
		if err != nil {
			if uniqueViolation(err, "uk_ticket_template_name") {
				return biz.ErrTemplateNameExists
			}
			return fmt.Errorf("update ticket template: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrTemplateNotFound
		}
		return nil
	})
}

// DeleteTemplate implements biz.TemplateWriter.
func (r *catalogRepo) DeleteTemplate(ctx context.Context, id string) error {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE ticket_template SET delete_time = now(), delete_user = $2 WHERE id = $1 AND delete_time IS NULL",
		id, basedata.OperatorArg(ctx))
	if err != nil {
		return fmt.Errorf("soft delete ticket template: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrTemplateNotFound
	}
	return nil
}

// encodeTemplate 把模板的表单、SLA 规则与升级对象编码为 jsonb 实参（空值落 []）。
func encodeTemplate(t *biz.Template) (form, sla, escalate []byte, err error) {
	if form, err = json.Marshal(nonNil(t.Form)); err != nil {
		return nil, nil, nil, fmt.Errorf("encode ticket template form: %w", err)
	}
	rules := t.SLA
	if rules == nil {
		rules = []biz.SLARule{}
	}
	if sla, err = json.Marshal(rules); err != nil {
		return nil, nil, nil, fmt.Errorf("encode ticket template sla: %w", err)
	}
	if escalate, err = json.Marshal(nonNilStrings(t.SLAEscalate)); err != nil {
		return nil, nil, nil, fmt.Errorf("encode ticket template escalation: %w", err)
	}
	return form, sla, escalate, nil
}

func nonNilStrings(items []string) []string {
	if items == nil {
		return []string{}
	}
	return items
}

// nonNil 空表单落 [] 而不是 null。
func nonNil(form []biz.FormField) []biz.FormField {
	if form == nil {
		return []biz.FormField{}
	}
	return form
}

// nullTime 零值时间落 NULL。
func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
