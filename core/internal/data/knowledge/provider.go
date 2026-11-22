// Package data 知识库域仓储的 PostgreSQL 实现与 embedding 客户端。
package data

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/google/wire"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// ProviderSet is knowledge 域 data providers.
var ProviderSet = wire.NewSet(
	NewSpaceRepo,
	NewDocumentRepo,
	NewVersionRepo,
	NewChunkRepo,
	NewLinkRepo,
	NewIngestRepo,
	NewIndexRepo,
	NewSearchLogRepo,
	NewEmbedder,
	wire.Bind(new(biz.SpaceRepo), new(*spaceRepo)),
	wire.Bind(new(biz.DocumentRepo), new(*documentRepo)),
	wire.Bind(new(biz.VersionRepo), new(*versionRepo)),
	wire.Bind(new(biz.ChunkRepo), new(*chunkRepo)),
	wire.Bind(new(biz.LinkRepo), new(*linkRepo)),
	wire.Bind(new(biz.IngestRepo), new(*ingestRepo)),
	wire.Bind(new(biz.IndexRepo), new(*indexRepo)),
	wire.Bind(new(biz.SearchLogRepo), new(*searchLogRepo)),
	wire.Bind(new(biz.Embedder), new(*EmbeddingClient)),
)

// sqlArgs 动态条件的实参收集器：add 追加一个实参并返回其占位符（$n）。
type sqlArgs struct {
	args []any
}

func (a *sqlArgs) add(v any) string {
	a.args = append(a.args, v)
	return "$" + strconv.Itoa(len(a.args))
}

// isUniqueViolation 报告是否为指定唯一约束 / 索引的冲突（constraint 为空不区分）。
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation &&
		(constraint == "" || pgErr.ConstraintName == constraint)
}

// execer Pool 与 Tx 共有的执行面。
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// execFrom 返回本次写入应使用的连接：ctx 携带事务句柄时用事务，否则用连接池。
func execFrom(ctx context.Context, data *basedata.Data) execer {
	if tx, ok := basedata.PgxTx(txrun.FromContext(ctx)); ok {
		return tx
	}
	return data.Pool
}

// nullTime 把 *time.Time 规整为可直接作为实参的值。
func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

// nullStr 把 *string 规整为可直接作为实参的值。
func nullStr(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// probeTimeout 启动期探测超时。
const probeTimeout = 3 * time.Second

// probeVectorColumn 探测向量列是否存在（pgvector 已安装且迁移里的条件 DDL 已执行）。
func probeVectorColumn(data *basedata.Data, logger *slog.Logger) bool {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	var ok bool
	err := data.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'knowledge_chunk' AND column_name = 'embedding')`).Scan(&ok)
	if err != nil {
		logger.Warn("knowledge: probe vector column failed, vector search disabled", "error", err)
		return false
	}
	if !ok {
		logger.Info("knowledge: pgvector column absent, search runs full-text only")
	}
	return ok
}

// vectorLiteral 把向量编码为 pgvector 文本字面量（[v1,v2,…]）。
func vectorLiteral(v []float32) string {
	buf := make([]byte, 0, len(v)*8+2)
	buf = append(buf, '[')
	for i, f := range v {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = strconv.AppendFloat(buf, float64(f), 'f', -1, 32)
	}
	buf = append(buf, ']')
	return string(buf)
}

// wrap 统一包装仓储错误。
func wrap(op string, err error) error { return fmt.Errorf("%s: %w", op, err) }
