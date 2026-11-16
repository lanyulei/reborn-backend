// Package data 流程引擎域的 data 层：流程定义 / 版本与实例执行状态的 PostgreSQL 仓储。
package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.DefinitionReader    = (*definitionRepo)(nil)
	_ biz.DefinitionWriter    = (*definitionRepo)(nil)
	_ biz.DefinitionPublisher = (*definitionRepo)(nil)
	_ biz.VersionReader       = (*definitionRepo)(nil)
)

// definitionRepo 流程定义仓储的 PostgreSQL 实现。
type definitionRepo struct {
	data *basedata.Data
}

// NewDefinitionRepo new a definitionRepo.
func NewDefinitionRepo(data *basedata.Data) *definitionRepo {
	return &definitionRepo{data: data}
}

// definitionColumns 定义查询列序（不含草稿），与 scanDefinition 一一对应。
const definitionColumns = "id, key, name, category, description, status, revision, published_version, published_version_id, create_time, update_time"

func scanDefinition(scan func(dest ...any) error, extra ...any) (*biz.Definition, error) {
	var (
		d         biz.Definition
		status    int16
		versionID *string
	)
	dest := append([]any{
		&d.ID, &d.Key, &d.Name, &d.Category, &d.Description, &status, &d.Revision,
		&d.PublishedVersion, &versionID, &d.CreateTime, &d.UpdateTime,
	}, extra...)
	if err := scan(dest...); err != nil {
		return nil, err
	}
	d.Status = int32(status)
	if versionID != nil {
		d.PublishedVersionID = *versionID
	}
	return &d, nil
}

// GetDefinition implements biz.DefinitionReader.
func (r *definitionRepo) GetDefinition(ctx context.Context, id string) (*biz.Definition, error) {
	var draft []byte
	d, err := scanDefinition(basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		"SELECT "+definitionColumns+", draft FROM workflow_definition WHERE id = $1 AND delete_time IS NULL", id).Scan, &draft)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrDefinitionNotFound
		}
		return nil, fmt.Errorf("get workflow definition: %w", err)
	}
	d.Draft = draft
	return d, nil
}

// GetDefinitionByKey implements biz.DefinitionReader：不读草稿（每次发起流程都会走到这里）。
func (r *definitionRepo) GetDefinitionByKey(ctx context.Context, key string) (*biz.Definition, error) {
	d, err := scanDefinition(basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		"SELECT "+definitionColumns+" FROM workflow_definition WHERE key = $1 AND delete_time IS NULL", key).Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrDefinitionNotFound
		}
		return nil, fmt.Errorf("get workflow definition by key: %w", err)
	}
	return d, nil
}

// maxKeyReferences 单类引用方至多列出的条数。
const maxKeyReferences = 5

// KeyReferences implements biz.DefinitionReader：其他流程当前发布版本里的子流程节点（jsonb
// 包含匹配；流程定义的数量有限，顺序扫描即可），以及开放接口接入凭据的允许发起范围。
func (r *definitionRepo) KeyReferences(ctx context.Context, key, excludeID string) ([]string, error) {
	node, err := json.Marshal(map[string]any{"nodes": []any{map[string]any{
		"type": biz.NodeSubProcess, "subProcess": map[string]any{"definitionKey": key},
	}}})
	if err != nil {
		return nil, err
	}
	allow, err := json.Marshal([]string{key})
	if err != nil {
		return nil, err
	}
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`(SELECT 'workflow "' || d.name || '" (sub-process)' FROM workflow_definition d
		  JOIN workflow_definition_version v ON v.id = d.published_version_id
		  WHERE d.delete_time IS NULL AND d.id <> $1 AND v.graph @> $2::jsonb ORDER BY d.name LIMIT $4)
		 UNION ALL
		 (SELECT 'API client "' || c.name || '"' FROM workflow_client c
		  WHERE c.delete_time IS NULL AND c.definition_keys @> $3::jsonb ORDER BY c.name LIMIT $4)`,
		excludeID, string(node), string(allow), maxKeyReferences)
	if err != nil {
		return nil, fmt.Errorf("list workflow definition references: %w", err)
	}
	refs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("scan workflow definition reference: %w", err)
	}
	return refs, nil
}

// ListDefinitions implements biz.DefinitionReader：条件过滤 + 分页，两条参数化查询。
func (r *definitionRepo) ListDefinitions(ctx context.Context, filter biz.DefinitionFilter, page, pageSize int64) ([]*biz.Definition, int64, error) {
	where := []string{"delete_time IS NULL"}
	args := make([]any, 0, 4)
	if filter.Keyword != "" {
		args = append(args, dbx.LikeContains(filter.Keyword))
		n := strconv.Itoa(len(args))
		where = append(where, "(name ILIKE $"+n+" OR key ILIKE $"+n+")")
	}
	if filter.Status != nil {
		args = append(args, *filter.Status)
		where = append(where, "status = $"+strconv.Itoa(len(args)))
	}
	if filter.Published != nil {
		if *filter.Published {
			where = append(where, "published_version > 0")
		} else {
			where = append(where, "published_version = 0")
		}
	}
	whereClause := strings.Join(where, " AND ")
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM workflow_definition WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count workflow definitions: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+definitionColumns+" FROM workflow_definition WHERE "+whereClause+
		" ORDER BY update_time DESC, id DESC LIMIT $"+strconv.Itoa(len(args)-1)+" OFFSET $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list workflow definitions: %w", err)
	}
	defs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Definition, error) { return scanDefinition(row.Scan) })
	if err != nil {
		return nil, 0, fmt.Errorf("scan workflow definition: %w", err)
	}
	return defs, total, nil
}

// CreateDefinition implements biz.DefinitionWriter.
func (r *definitionRepo) CreateDefinition(ctx context.Context, d *biz.Definition) error {
	operator := basedata.OperatorArg(ctx)
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO workflow_definition (id, key, name, category, description, status, draft, revision, create_time, update_time, create_user, update_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)`,
		d.ID, d.Key, d.Name, d.Category, d.Description, d.Status, d.Draft, d.Revision, d.CreateTime, d.UpdateTime, operator)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == "uk_workflow_definition_key" {
			return biz.ErrDefinitionKeyExists
		}
		return fmt.Errorf("insert workflow definition: %w", err)
	}
	return nil
}

// UpdateDefinition implements biz.DefinitionWriter：动态 SET 只允许静态列名白名单，值全部参数化。
func (r *definitionRepo) UpdateDefinition(ctx context.Context, id string, upd biz.DefinitionUpdate) error {
	sets := make([]string, 0, 5)
	args := make([]any, 0, 6)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Name != nil {
		add("name", *upd.Name)
	}
	if upd.Category != nil {
		add("category", *upd.Category)
	}
	if upd.Description != nil {
		add("description", *upd.Description)
	}
	if upd.Status != nil {
		add("status", *upd.Status)
	}
	add("update_user", basedata.OperatorArg(ctx))
	args = append(args, id)
	tag, err := r.data.Pool.Exec(ctx, "UPDATE workflow_definition SET "+strings.Join(sets, ", ")+
		", update_time = now() WHERE id = $"+strconv.Itoa(len(args))+" AND delete_time IS NULL", args...)
	if err != nil {
		return fmt.Errorf("update workflow definition: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrDefinitionNotFound
	}
	return nil
}

// SaveDraft implements biz.DefinitionWriter：单语句条件更新做乐观锁——修订号不符不命中，
// 再区分「不存在」与「已被他人修改」。
func (r *definitionRepo) SaveDraft(ctx context.Context, id string, draft []byte, revision int32) (int32, error) {
	var next int32
	err := r.data.Pool.QueryRow(ctx,
		`UPDATE workflow_definition SET draft = $2, revision = revision + 1, update_time = now(), update_user = $4
		 WHERE id = $1 AND revision = $3 AND delete_time IS NULL RETURNING revision`,
		id, draft, revision, basedata.OperatorArg(ctx)).Scan(&next)
	if err == nil {
		return next, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("save workflow draft: %w", err)
	}
	if _, err := r.GetDefinition(ctx, id); err != nil {
		return 0, err
	}
	return 0, biz.ErrDefinitionConflict
}

// Publish implements biz.DefinitionPublisher：事务内锁定义行，确认草稿修订号未变后
// 写入新版本并推进发布指针。并发发布因行锁串行，版本号不会重复（唯一索引兜底）。
func (r *definitionRepo) Publish(ctx context.Context, id string, revision int32, v *biz.DefinitionVersion) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var current, published int32
		err := tx.QueryRow(ctx,
			"SELECT revision, published_version FROM workflow_definition WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).
			Scan(&current, &published)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return biz.ErrDefinitionNotFound
			}
			return fmt.Errorf("lock workflow definition: %w", err)
		}
		if current != revision {
			return biz.ErrDefinitionConflict
		}
		v.Version = published + 1
		operator := basedata.OperatorArg(ctx)
		if _, err := tx.Exec(ctx,
			`INSERT INTO workflow_definition_version (id, definition_id, version, graph, create_time, create_user)
			 VALUES ($1, $2, $3, $4, $5, $6)`, v.ID, id, v.Version, v.Graph, v.CreateTime, operator); err != nil {
			return fmt.Errorf("insert workflow version: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE workflow_definition SET published_version = $2, published_version_id = $3, update_time = now(), update_user = $4
			 WHERE id = $1`, id, v.Version, v.ID, operator); err != nil {
			return fmt.Errorf("advance published version: %w", err)
		}
		return nil
	})
}

// DeleteDefinition implements biz.DefinitionPublisher：锁定义行后判定无进行中的实例再软删。
// 发起实例前会确认定义未删除，但不持有定义行锁：极端并发下可能在软删提交的同时
// 多出一个实例——该实例照常按其绑定的不可变版本跑完，不影响正确性。
func (r *definitionRepo) DeleteDefinition(ctx context.Context, id string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var locked bool
		err := tx.QueryRow(ctx,
			"SELECT true FROM workflow_definition WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).Scan(&locked)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return biz.ErrDefinitionNotFound
			}
			return fmt.Errorf("lock workflow definition: %w", err)
		}
		var running bool
		if err := tx.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM workflow_instance WHERE definition_id = $1 AND status = $2)",
			id, biz.InstanceRunning).Scan(&running); err != nil {
			return fmt.Errorf("check running instances: %w", err)
		}
		if running {
			return biz.ErrDefinitionInUse
		}
		if _, err := tx.Exec(ctx,
			"UPDATE workflow_definition SET delete_time = now(), delete_user = $2 WHERE id = $1",
			id, basedata.OperatorArg(ctx)); err != nil {
			return fmt.Errorf("soft delete workflow definition: %w", err)
		}
		return nil
	})
}

func scanVersion(scan func(dest ...any) error, withGraph bool) (*biz.DefinitionVersion, error) {
	var (
		v          biz.DefinitionVersion
		createUser *string
	)
	dest := []any{&v.ID, &v.DefinitionID, &v.Version, &createUser, &v.CreateTime}
	if withGraph {
		dest = append(dest, &v.Graph)
	}
	if err := scan(dest...); err != nil {
		return nil, err
	}
	if createUser != nil {
		v.CreateUser = *createUser
	}
	return &v, nil
}

const versionColumns = "id, definition_id, version, create_user, create_time"

func (r *definitionRepo) getVersion(ctx context.Context, where string, args ...any) (*biz.DefinitionVersion, error) {
	v, err := scanVersion(basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		"SELECT "+versionColumns+", graph FROM workflow_definition_version WHERE "+where, args...).Scan, true)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrDefinitionNotFound
		}
		return nil, fmt.Errorf("get workflow version: %w", err)
	}
	return v, nil
}

// GetVersion implements biz.VersionReader.
func (r *definitionRepo) GetVersion(ctx context.Context, versionID string) (*biz.DefinitionVersion, error) {
	return r.getVersion(ctx, "id = $1", versionID)
}

// GetVersionByNumber implements biz.VersionReader.
func (r *definitionRepo) GetVersionByNumber(ctx context.Context, definitionID string, version int32) (*biz.DefinitionVersion, error) {
	return r.getVersion(ctx, "definition_id = $1 AND version = $2", definitionID, version)
}

// ListVersions implements biz.VersionReader.
func (r *definitionRepo) ListVersions(ctx context.Context, definitionID string, limit int) ([]*biz.DefinitionVersion, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+versionColumns+" FROM workflow_definition_version WHERE definition_id = $1 ORDER BY version DESC LIMIT $2",
		definitionID, limit)
	if err != nil {
		return nil, fmt.Errorf("list workflow versions: %w", err)
	}
	versions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.DefinitionVersion, error) {
		return scanVersion(row.Scan, false)
	})
	if err != nil {
		return nil, fmt.Errorf("scan workflow version: %w", err)
	}
	return versions, nil
}
