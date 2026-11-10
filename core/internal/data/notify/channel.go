package data

import (
	"bytes"
	"context"
	"encoding/json"
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
var _ biz.ChannelRepo = (*channelRepo)(nil)

// channelRepo 渠道仓储 PostgreSQL 实现；每次写操作同事务 bump notify_cfg_version
// （管道服务 10s 轮询该版本号刷新配置缓存，见 notify-design.md §5.1）。
type channelRepo struct {
	data *basedata.Data
	box  *SecretBox
}

// NewChannelRepo new a channelRepo.
func NewChannelRepo(data *basedata.Data, box *SecretBox) *channelRepo {
	return &channelRepo{data: data, box: box}
}

// mapChannelUniqueViolation 将唯一约束冲突（23505）映射为业务哨兵错误。
func mapChannelUniqueViolation(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
		if pgErr.ConstraintName == "uk_notify_channel_name" {
			return biz.ErrChannelNameExists
		}
	}
	return err
}

// bumpChannelCfgVersion 同事务递增 notify_channel 的配置版本号（通用实现见
// version.go）。
func bumpChannelCfgVersion(ctx context.Context, tx pgx.Tx) error {
	return bumpCfgVersion(ctx, tx, "notify_channel")
}

// sealChannelSecret 加密渠道凭据；空明文返回 nil（落 NULL，如 webhook 未配密钥）。
func (r *channelRepo) sealChannelSecret(plaintext string) ([]byte, error) {
	if plaintext == "" {
		return nil, nil
	}
	return r.box.Seal(plaintext)
}

// encodeChannelConfig 将类型化配置序列化为 config jsonb（schema 见 §5.1.1）。
func encodeChannelConfig(cfg *biz.ChannelConfig) ([]byte, error) {
	var target any
	switch {
	case cfg.Email != nil:
		target = cfg.Email
	case cfg.Webhook != nil:
		target = cfg.Webhook
	case cfg.Robot != nil:
		target = cfg.Robot
	default:
		return nil, fmt.Errorf("encode channel config: no branch set")
	}
	return json.Marshal(target)
}

// decodeChannelConfig 按渠道类型将 config jsonb 严格解码为类型化配置
// （DisallowUnknownFields：落库结构漂移即刻暴露而非静默吞字段）。
func decodeChannelConfig(channelType string, raw []byte) (*biz.ChannelConfig, error) {
	cfg := &biz.ChannelConfig{}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var err error
	switch channelType {
	case biz.ChannelTypeEmail:
		cfg.Email = &biz.EmailConfig{}
		err = dec.Decode(cfg.Email)
	case biz.ChannelTypeWebhook:
		cfg.Webhook = &biz.WebhookConfig{}
		err = dec.Decode(cfg.Webhook)
	case biz.ChannelTypeDingtalk, biz.ChannelTypeWecom, biz.ChannelTypeFeishu:
		cfg.Robot = &biz.RobotConfig{}
		err = dec.Decode(cfg.Robot)
	default:
		return nil, fmt.Errorf("decode channel config: unknown type %q", channelType)
	}
	if err != nil {
		return nil, fmt.Errorf("decode channel config (type %s): %w", channelType, err)
	}
	return cfg, nil
}

// channelSelectColumns 是渠道查询列序（不含 secret——只写不读），与 scanChannel 对应。
const channelSelectColumns = "id, biz_id, type, name, config, rate_limit, enabled, create_time, update_time"

// scanChannel 按列序扫描一行渠道数据并解码为 DO。
func scanChannel(scan func(dest ...any) error) (*biz.Channel, error) {
	var (
		id, bizID, typ, name   string
		cfgRaw, rlRaw          []byte
		enabled                bool
		createTime, updateTime time.Time
	)
	if err := scan(&id, &bizID, &typ, &name, &cfgRaw, &rlRaw, &enabled, &createTime, &updateTime); err != nil {
		return nil, err
	}
	cfg, err := decodeChannelConfig(typ, cfgRaw)
	if err != nil {
		return nil, err
	}
	var rl biz.RateLimit
	if err := json.Unmarshal(rlRaw, &rl); err != nil {
		return nil, fmt.Errorf("decode channel rate_limit: %w", err)
	}
	return &biz.Channel{
		ID:         id,
		BizID:      bizID,
		Type:       typ,
		Name:       name,
		Config:     cfg,
		RateLimit:  rl,
		Enabled:    enabled,
		CreateTime: createTime,
		UpdateTime: updateTime,
	}, nil
}

// CreateChannel implements biz.ChannelRepo：落库（secret 加密）并同事务 bump 版本。
func (r *channelRepo) CreateChannel(ctx context.Context, ch *biz.Channel) error {
	cfgJSON, err := encodeChannelConfig(ch.Config)
	if err != nil {
		return err
	}
	rlJSON, err := json.Marshal(ch.RateLimit)
	if err != nil {
		return fmt.Errorf("encode channel rate_limit: %w", err)
	}
	secret, err := r.sealChannelSecret(ch.Secret)
	if err != nil {
		return err
	}
	signSecret, err := r.sealChannelSecret(ch.SignSecret)
	if err != nil {
		return err
	}
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO notify_channel
			     (id, biz_id, type, name, config, secret, sign_secret, rate_limit, enabled, create_time, update_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			ch.ID, ch.BizID, ch.Type, ch.Name, cfgJSON, secret, signSecret, rlJSON, ch.Enabled, ch.CreateTime, ch.UpdateTime); err != nil {
			return mapChannelUniqueViolation(err)
		}
		return bumpChannelCfgVersion(ctx, tx)
	})
}

// UpdateChannel implements biz.ChannelRepo：动态 SET 只允许静态列名白名单；
// secret 为 nil 不更新（保留旧密文）；无可更新字段时幂等 no-op。
func (r *channelRepo) UpdateChannel(ctx context.Context, id string, upd biz.ChannelUpdate) error {
	sets := make([]string, 0, 5)
	args := make([]any, 0, 6)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Name != nil {
		add("name", *upd.Name)
	}
	if upd.Config != nil {
		cfgJSON, err := encodeChannelConfig(upd.Config)
		if err != nil {
			return err
		}
		add("config", cfgJSON)
	}
	if upd.RateLimit != nil {
		rlJSON, err := json.Marshal(*upd.RateLimit)
		if err != nil {
			return fmt.Errorf("encode channel rate_limit: %w", err)
		}
		add("rate_limit", rlJSON)
	}
	if upd.Enabled != nil {
		add("enabled", *upd.Enabled)
	}
	if upd.Secret != nil {
		secret, err := r.sealChannelSecret(*upd.Secret)
		if err != nil {
			return err
		}
		add("secret", secret)
	}
	if upd.SignSecret != nil {
		// 空串 → sealChannelSecret 返回 nil → 列落 NULL（清除签名）。
		signSecret, err := r.sealChannelSecret(*upd.SignSecret)
		if err != nil {
			return err
		}
		add("sign_secret", signSecret)
	}
	if len(sets) == 0 {
		return nil
	}

	args = append(args, id)
	query := "UPDATE notify_channel SET " + strings.Join(sets, ", ") +
		", update_time = now() WHERE id = $" + strconv.Itoa(len(args)) + " AND delete_time IS NULL"
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return mapChannelUniqueViolation(err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrChannelNotFound
		}
		return bumpChannelCfgVersion(ctx, tx)
	})
}

// DeleteChannel implements biz.ChannelRepo：软删并同事务 bump 版本。
func (r *channelRepo) DeleteChannel(ctx context.Context, id string) error {
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			"UPDATE notify_channel SET delete_time = now(), update_time = now()"+
				" WHERE id = $1 AND delete_time IS NULL", id)
		if err != nil {
			return fmt.Errorf("soft delete channel: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrChannelNotFound
		}
		return bumpChannelCfgVersion(ctx, tx)
	})
}

// GetChannel implements biz.ChannelRepo。
func (r *channelRepo) GetChannel(ctx context.Context, id string) (*biz.Channel, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+channelSelectColumns+" FROM notify_channel WHERE id = $1 AND delete_time IS NULL", id)
	ch, err := scanChannel(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrChannelNotFound
		}
		return nil, fmt.Errorf("get channel: %w", err)
	}
	return ch, nil
}

// ListChannelsByIDs implements biz.ChannelRepo：单条查询 ID 集合内的未删除
// 渠道（缺失 ID 不报错，由调用方比对）；空输入不查库。
func (r *channelRepo) ListChannelsByIDs(ctx context.Context, ids []string) ([]*biz.Channel, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+channelSelectColumns+" FROM notify_channel WHERE id = ANY($1::uuid[]) AND delete_time IS NULL", ids)
	if err != nil {
		return nil, fmt.Errorf("list channels by ids: %w", err)
	}
	defer rows.Close()

	channels := make([]*biz.Channel, 0, len(ids))
	for rows.Next() {
		ch, err := scanChannel(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan channel: %w", err)
		}
		channels = append(channels, ch)
	}
	return channels, rows.Err()
}

// ListChannels implements biz.ChannelRepo：条件过滤 + 分页，两条参数化查询。
func (r *channelRepo) ListChannels(ctx context.Context, filter biz.ChannelFilter, page, pageSize int64) ([]*biz.Channel, int64, error) {
	where := []string{"delete_time IS NULL"}
	args := make([]any, 0, 5)
	if filter.BizID != "" {
		args = append(args, filter.BizID)
		where = append(where, "biz_id = $"+strconv.Itoa(len(args)))
	}
	if filter.Keyword != "" {
		args = append(args, basedata.LikeContains(filter.Keyword))
		n := strconv.Itoa(len(args))
		where = append(where, "(name ILIKE $"+n+" OR type ILIKE $"+n+")")
	}
	if filter.Type != nil {
		args = append(args, *filter.Type)
		where = append(where, "type = $"+strconv.Itoa(len(args)))
	}
	if filter.Enabled != nil {
		args = append(args, *filter.Enabled)
		where = append(where, "enabled = $"+strconv.Itoa(len(args)))
	}
	whereClause := strings.Join(where, " AND ")

	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_channel WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count channels: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + channelSelectColumns + " FROM notify_channel WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list channels: %w", err)
	}
	defer rows.Close()

	channels := make([]*biz.Channel, 0, pageSize)
	for rows.Next() {
		ch, err := scanChannel(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan channel: %w", err)
		}
		channels = append(channels, ch)
	}
	return channels, total, rows.Err()
}
