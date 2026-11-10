package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约。
var _ biz.ProducerRepo = (*producerRepo)(nil)

// producerRepo 生产者仓储 PostgreSQL 实现；每次写操作（含 token 轮换）同
// 事务 bump notify_cfg_version('notify_producer')——生产者属管道 ingress
// 鉴权缓存集，Disable/Rotate ≤10s 生效。event_whitelist 为 text[]（pgx v5
// 与 []string 双向直传），token_hash 为 bytea（SHA-256，原文不落库）。
type producerRepo struct {
	data *basedata.Data
}

// NewProducerRepo new a producerRepo.
func NewProducerRepo(data *basedata.Data) *producerRepo {
	return &producerRepo{data: data}
}

// mapProducerUniqueViolation 将唯一约束冲突（23505）映射为业务哨兵错误。
func mapProducerUniqueViolation(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
		if pgErr.ConstraintName == "uk_notify_producer_name" {
			return biz.ErrProducerNameExists
		}
	}
	return err
}

// producerSelectColumns 是生产者查询列序，与 scanProducer 对应。
const producerSelectColumns = "id, name, biz_id, event_whitelist, token_hash, expire_time, enabled, create_time, update_time, qps_limit, daily_limit"

// scanProducer 按列序扫描一行生产者数据并解码为 DO；expire_time 为可空
// timestamptz（nil = 永不过期）。
func scanProducer(scan func(dest ...any) error) (*biz.Producer, error) {
	var (
		id, name, bizID string
		whitelist       []string
		tokenHash       []byte
		expireTime      *time.Time
		enabled         bool
		createTime      time.Time
		updateTime      time.Time
		qps, daily      int32
	)
	if err := scan(&id, &name, &bizID, &whitelist, &tokenHash, &expireTime,
		&enabled, &createTime, &updateTime, &qps, &daily); err != nil {
		return nil, err
	}
	return &biz.Producer{
		ID:      id,
		BizID:   bizID,
		Enabled: enabled,
		ProducerSpec: biz.ProducerSpec{
			Name:           name,
			EventWhitelist: whitelist,
			ExpireTime:     expireTime,
			QPSLimit:       qps,
			DailyLimit:     daily,
		},
		TokenHash:  tokenHash,
		CreateTime: createTime,
		UpdateTime: updateTime,
	}, nil
}

// whitelistArg 归一化白名单参数：nil slice 会被 pgx 编码为 NULL（撞
// NOT NULL 约束），归一为空数组 '{}'（空 = 不限）。
func whitelistArg(list []string) []string {
	if list == nil {
		return []string{}
	}
	return list
}

// CreateProducer implements biz.ProducerRepo：落库并同事务 bump 版本。
func (r *producerRepo) CreateProducer(ctx context.Context, producer *biz.Producer) error {
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO notify_producer
			     (id, name, biz_id, event_whitelist, token_hash, expire_time, enabled, create_time, update_time, qps_limit, daily_limit)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			producer.ID, producer.Name, producer.BizID,
			whitelistArg(producer.EventWhitelist), producer.TokenHash, producer.ExpireTime,
			producer.Enabled, producer.CreateTime, producer.UpdateTime, producer.QPSLimit, producer.DailyLimit); err != nil {
			return mapProducerUniqueViolation(fmt.Errorf("insert producer: %w", err))
		}
		return bumpCfgVersion(ctx, tx, "notify_producer")
	})
}

// UpdateProducer implements biz.ProducerRepo：Spec 整组替换配置字段或
// Enabled 启停；update_time 恒刷新。
func (r *producerRepo) UpdateProducer(ctx context.Context, id string, upd biz.ProducerUpdate) error {
	sets := make([]string, 0, 4)
	args := make([]any, 0, 5)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Spec != nil {
		add("name", upd.Spec.Name)
		add("event_whitelist", whitelistArg(upd.Spec.EventWhitelist))
		add("expire_time", upd.Spec.ExpireTime)
		add("qps_limit", upd.Spec.QPSLimit)
		add("daily_limit", upd.Spec.DailyLimit)
	}
	if upd.Enabled != nil {
		add("enabled", *upd.Enabled)
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, id)
	query := "UPDATE notify_producer SET " + strings.Join(sets, ", ") +
		", update_time = now() WHERE id = $" + strconv.Itoa(len(args)) + " AND delete_time IS NULL"
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return mapProducerUniqueViolation(fmt.Errorf("update producer: %w", err))
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrProducerNotFound
		}
		return bumpCfgVersion(ctx, tx, "notify_producer")
	})
}

// UpdateProducerToken implements biz.ProducerRepo：单语句轮换令牌哈希并
// 返回权威行（旧 token 立即失效），同事务 bump 版本。
func (r *producerRepo) UpdateProducerToken(ctx context.Context, id string, tokenHash []byte) (*biz.Producer, error) {
	var producer *biz.Producer
	err := basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx,
			"UPDATE notify_producer SET token_hash = $2, update_time = now()"+
				" WHERE id = $1 AND delete_time IS NULL RETURNING "+producerSelectColumns,
			id, tokenHash)
		p, err := scanProducer(row.Scan)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return biz.ErrProducerNotFound
			}
			return fmt.Errorf("rotate producer token: %w", err)
		}
		producer = p
		return bumpCfgVersion(ctx, tx, "notify_producer")
	})
	if err != nil {
		return nil, err
	}
	return producer, nil
}

// DeleteProducer implements biz.ProducerRepo：软删并同事务 bump 版本。
func (r *producerRepo) DeleteProducer(ctx context.Context, id string) error {
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			"UPDATE notify_producer SET delete_time = now(), update_time = now()"+
				" WHERE id = $1 AND delete_time IS NULL", id)
		if err != nil {
			return fmt.Errorf("soft delete producer: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrProducerNotFound
		}
		return bumpCfgVersion(ctx, tx, "notify_producer")
	})
}

// GetProducer implements biz.ProducerRepo。
func (r *producerRepo) GetProducer(ctx context.Context, id string) (*biz.Producer, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+producerSelectColumns+" FROM notify_producer WHERE id = $1 AND delete_time IS NULL", id)
	producer, err := scanProducer(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrProducerNotFound
		}
		return nil, fmt.Errorf("get producer: %w", err)
	}
	return producer, nil
}

// ListProducers implements biz.ProducerRepo：条件过滤 + 分页，两条参数化查询。
func (r *producerRepo) ListProducers(ctx context.Context, filter biz.ProducerFilter, page, pageSize int64) ([]*biz.Producer, int64, error) {
	where := []string{"delete_time IS NULL"}
	args := make([]any, 0, 2)
	if filter.BizID != "" {
		args = append(args, filter.BizID)
		where = append(where, "biz_id = $"+strconv.Itoa(len(args)))
	}
	if filter.Keyword != "" {
		args = append(args, basedata.LikeContains(filter.Keyword))
		where = append(where, "name ILIKE $"+strconv.Itoa(len(args)))
	}
	if filter.Enabled != nil {
		args = append(args, *filter.Enabled)
		where = append(where, "enabled = $"+strconv.Itoa(len(args)))
	}
	whereClause := strings.Join(where, " AND ")

	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_producer WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count producers: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + producerSelectColumns + " FROM notify_producer WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list producers: %w", err)
	}
	defer rows.Close()

	producers := make([]*biz.Producer, 0, pageSize)
	for rows.Next() {
		producer, err := scanProducer(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan producer: %w", err)
		}
		producers = append(producers, producer)
	}
	return producers, total, rows.Err()
}
