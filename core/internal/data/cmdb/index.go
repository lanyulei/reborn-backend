package data

import (
	"context"
	"crypto/sha1" //nolint:gosec // 仅用于缩短索引名，非安全用途
	"encoding/hex"
	"fmt"
	"regexp"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgx/v5"
)

var _ biz.AttrIndexer = (*attrIndexer)(nil)

// identPattern 标识符白名单（类型编码与字段编码在 proto 已限定同一模式；DDL 前再校验一次）。
var identPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// attrIndexer 属性表达式索引：CREATE INDEX ... ON cmdb_ci ((attrs ->> 'code')) WHERE model_code = '...' AND delete_time IS NULL。
// 非 CONCURRENTLY：索引创建在事务外执行，表规模（十万级）下秒级完成；标识符经白名单 + Sanitize。
type attrIndexer struct {
	data *basedata.Data
}

// NewAttrIndexer new an attrIndexer.
func NewAttrIndexer(data *basedata.Data) *attrIndexer { return &attrIndexer{data: data} }

// attrIndexName 索引名 ≤63 字符：过长时以哈希缩短。
func attrIndexName(modelCode, fieldCode string) string {
	name := "idx_cmdb_ci_attr_" + modelCode + "_" + fieldCode
	if len(name) <= 63 {
		return name
	}
	sum := sha1.Sum([]byte(modelCode + "/" + fieldCode)) //nolint:gosec // 仅缩短标识符
	return "idx_cmdb_ci_attr_" + hex.EncodeToString(sum[:])[:20]
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
	name := pgx.Identifier{attrIndexName(modelCode, fieldCode)}.Sanitize()
	// 字面量只来自白名单标识符（无引号/反斜杠），可安全内联。
	stmt := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON cmdb_ci ((attrs ->> '%s')) WHERE model_code = '%s' AND delete_time IS NULL",
		name, fieldCode, modelCode)
	if _, err := r.data.Pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("create attr index: %w", err)
	}
	return nil
}

// DropAttrIndex implements biz.AttrIndexer（幂等）。
func (r *attrIndexer) DropAttrIndex(ctx context.Context, modelCode, fieldCode string) error {
	if err := checkIdent(modelCode, fieldCode); err != nil {
		return err
	}
	if _, err := r.data.Pool.Exec(ctx, "DROP INDEX IF EXISTS "+pgx.Identifier{attrIndexName(modelCode, fieldCode)}.Sanitize()); err != nil {
		return fmt.Errorf("drop attr index: %w", err)
	}
	return nil
}
