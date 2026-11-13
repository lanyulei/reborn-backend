package data

import (
	"context"
	"crypto/sha1" //nolint:gosec // 仅用于缩短索引名，非安全用途
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var _ biz.AttrIndexer = (*attrIndexer)(nil)

// identPattern 标识符白名单（类型编码与字段编码在 proto 已限定同一模式；DDL 前再校验一次）。
var identPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// attrIndexer 属性表达式索引：CREATE INDEX CONCURRENTLY ... ON cmdb_ci ((attrs ->> 'code')) WHERE model_code = '...' AND delete_time IS NULL。
// CONCURRENTLY：建 / 删索引期间不阻塞 cmdb_ci 的写入；在事务块外、独占连接上执行，
// 不受请求超时与 statement_timeout 限制（自带 attrIndexDDLTimeout 上界）。标识符经白名单 + Sanitize。
type attrIndexer struct {
	data *basedata.Data
}

// NewAttrIndexer new an attrIndexer.
func NewAttrIndexer(data *basedata.Data) *attrIndexer { return &attrIndexer{data: data} }

// attrIndexName 索引名（≤63 字符）：可读前缀（截断）+ (model/field) 哈希后缀。
// "/" 不在标识符字符集内，哈希输入无歧义——model `a_b`+field `c` 与 model `a`+field `b_c` 不再同名。
func attrIndexName(modelCode, fieldCode string) string {
	sum := sha1.Sum([]byte(modelCode + "/" + fieldCode)) //nolint:gosec // 仅缩短标识符
	readable := modelCode + "_" + fieldCode
	if len(readable) > 33 {
		readable = readable[:33]
	}
	return "idx_cmdb_ci_attr_" + readable + "_" + hex.EncodeToString(sum[:])[:12]
}

// legacyAttrIndexName 旧命名（`_` 直接拼接，存在歧义）；仅用于识别 / 清理存量索引。
func legacyAttrIndexName(modelCode, fieldCode string) string {
	name := "idx_cmdb_ci_attr_" + modelCode + "_" + fieldCode
	if len(name) <= 63 {
		return name
	}
	sum := sha1.Sum([]byte(modelCode + "/" + fieldCode)) //nolint:gosec // 仅缩短标识符
	return "idx_cmdb_ci_attr_" + hex.EncodeToString(sum[:])[:20]
}

// legacyIndexMatches 报告旧命名索引是否存在且确实属于该 (类型, 字段)
// （旧名有歧义：按索引定义中的字段与类型字面量确认，避免误认 / 误删别的字段的索引）。
func (r *attrIndexer) legacyIndexMatches(ctx context.Context, modelCode, fieldCode string) (bool, error) {
	var def string
	err := r.data.Pool.QueryRow(ctx,
		"SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1",
		legacyAttrIndexName(modelCode, fieldCode)).Scan(&def)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("inspect legacy attr index: %w", err)
	}
	return strings.Contains(def, "->> '"+fieldCode+"'") && strings.Contains(def, "= '"+modelCode+"'"), nil
}

func checkIdent(modelCode, fieldCode string) error {
	if !identPattern.MatchString(modelCode) || !identPattern.MatchString(fieldCode) {
		return fmt.Errorf("invalid identifier for attr index: %q / %q", modelCode, fieldCode)
	}
	return nil
}

// EnsureAttrIndex implements biz.AttrIndexer（幂等）。
func (r *attrIndexer) EnsureAttrIndex(ctx context.Context, modelCode, fieldCode string) error {
	if err := checkIdent(modelCode, fieldCode); err != nil {
		return err
	}
	// 存量旧命名索引已覆盖该字段时不重复建。
	if ok, err := r.legacyIndexMatches(ctx, modelCode, fieldCode); err != nil || ok {
		return err
	}
	rawName := attrIndexName(modelCode, fieldCode)
	name := pgx.Identifier{rawName}.Sanitize()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), attrIndexDDLTimeout)
	defer cancel()
	// 并发建索引中途失败会留下 INVALID 的同名索引（IF NOT EXISTS 会把它当成已存在）：先清掉再建。
	var valid bool
	err := r.data.Pool.QueryRow(ctx,
		`SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = current_schema() AND c.relname = $1`, rawName).Scan(&valid)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return fmt.Errorf("inspect attr index: %w", err)
	case valid:
		return nil
	default:
		if err := dbx.ExecNoTimeout(ctx, r.data.Pool, "DROP INDEX CONCURRENTLY IF EXISTS "+name); err != nil {
			return fmt.Errorf("drop invalid attr index: %w", err)
		}
	}
	// 字面量只来自白名单标识符（无引号/反斜杠），可安全内联。
	stmt := fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON cmdb_ci ((attrs ->> '%s')) WHERE model_code = '%s' AND delete_time IS NULL",
		name, fieldCode, modelCode)
	if err := dbx.ExecNoTimeout(ctx, r.data.Pool, stmt); err != nil {
		return fmt.Errorf("create attr index: %w", err)
	}
	return nil
}

// attrIndexDDLTimeout 单次并发建 / 删索引的上界（脱离请求超时：DDL 一旦开始不应被 5s 的请求时限打断）。
const attrIndexDDLTimeout = 10 * time.Minute

// DropAttrIndex implements biz.AttrIndexer（幂等）。
func (r *attrIndexer) DropAttrIndex(ctx context.Context, modelCode, fieldCode string) error {
	if err := checkIdent(modelCode, fieldCode); err != nil {
		return err
	}
	names := []string{attrIndexName(modelCode, fieldCode)}
	if ok, err := r.legacyIndexMatches(ctx, modelCode, fieldCode); err != nil {
		return err
	} else if ok {
		names = append(names, legacyAttrIndexName(modelCode, fieldCode))
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), attrIndexDDLTimeout)
	defer cancel()
	for _, name := range names {
		if err := dbx.ExecNoTimeout(ctx, r.data.Pool, "DROP INDEX CONCURRENTLY IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
			return fmt.Errorf("drop attr index: %w", err)
		}
	}
	return nil
}
