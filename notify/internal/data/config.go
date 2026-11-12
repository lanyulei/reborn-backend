package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.ConfigRepo = (*configRepo)(nil)

// configRepo 配置表只读仓储：版本号与全量快照。快照在单个 REPEATABLE READ
// 只读事务内读取（版本号与各表同一一致性视图，杜绝撕裂快照，评审 #20）。
// jsonb 解码结构与 core biz 的 json 标签一致（契约）。
type configRepo struct {
	data *Data
	box  *SecretBox
}

// NewConfigRepo new a configRepo.
func NewConfigRepo(data *Data, box *SecretBox) *configRepo {
	return &configRepo{data: data, box: box}
}

// LoadVersions implements biz.ConfigRepo.
func (r *configRepo) LoadVersions(ctx context.Context) (biz.ConfigVersions, error) {
	return loadVersions(ctx, r.data.Pool)
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func loadVersions(ctx context.Context, q querier) (biz.ConfigVersions, error) {
	rows, err := q.Query(ctx, "SELECT table_name, version FROM notify_cfg_version")
	if err != nil {
		return nil, fmt.Errorf("load cfg versions: %w", err)
	}
	defer rows.Close()
	out := biz.ConfigVersions{}
	for rows.Next() {
		var name string
		var version int64
		if err := rows.Scan(&name, &version); err != nil {
			return nil, fmt.Errorf("scan cfg version: %w", err)
		}
		out[name] = version
	}
	return out, rows.Err()
}

// LoadSnapshot implements biz.ConfigRepo.
func (r *configRepo) LoadSnapshot(ctx context.Context) (*biz.Snapshot, error) {
	var snap *biz.Snapshot
	err := dbx.WithTxOptions(ctx, r.data.Pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		versions, err := loadVersions(ctx, tx)
		if err != nil {
			return err
		}
		channels, err := r.loadChannels(ctx, tx)
		if err != nil {
			return err
		}
		routes, err := loadRoutes(ctx, tx)
		if err != nil {
			return err
		}
		silences, err := loadSilences(ctx, tx)
		if err != nil {
			return err
		}
		templates, err := loadTemplates(ctx, tx)
		if err != nil {
			return err
		}
		producers, err := loadProducers(ctx, tx)
		if err != nil {
			return err
		}
		snap = biz.NewSnapshot(versions, channels, routes, silences, templates, producers, time.Now())
		return nil
	})
	return snap, err
}

func (r *configRepo) loadChannels(ctx context.Context, tx pgx.Tx) ([]*biz.Channel, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, biz_id, type, name, config, secret, sign_secret, rate_limit, enabled
		 FROM notify_channel WHERE delete_time IS NULL`)
	if err != nil {
		return nil, fmt.Errorf("load channels: %w", err)
	}
	defer rows.Close()
	var out []*biz.Channel
	for rows.Next() {
		var (
			ch              biz.Channel
			config, rateRaw []byte
			secret, sign    []byte
		)
		if err := rows.Scan(&ch.ID, &ch.BizID, &ch.Type, &ch.Name, &config, &secret, &sign, &rateRaw, &ch.Enabled); err != nil {
			return nil, fmt.Errorf("scan channel: %w", err)
		}
		if err := decodeChannelConfig(&ch, config); err != nil {
			// 配置解码失败视为该实例不健康（不拖垮整体），发送侧延后并告警。
			ch.SecretErr = fmt.Errorf("decode config: %w", err)
		}
		if len(rateRaw) > 0 {
			if err := json.Unmarshal(rateRaw, &ch.RateLimit); err != nil {
				return nil, fmt.Errorf("decode rate_limit of %s: %w", ch.ID, err)
			}
		}
		if len(secret) > 0 {
			plain, err := r.box.Open(secret)
			if err != nil {
				ch.SecretErr = err // 评审 #21：单渠道解密失败不拖垮快照
			} else {
				ch.Secret = plain
			}
		}
		if len(sign) > 0 {
			plain, err := r.box.Open(sign)
			if err != nil {
				ch.SecretErr = err
			} else {
				ch.SignSecret = plain
			}
		}
		out = append(out, &ch)
	}
	return out, rows.Err()
}

// decodeChannelConfig 按 type 解码 config jsonb（结构与 core 一致；未知字段拒绝）。
func decodeChannelConfig(ch *biz.Channel, raw []byte) error {
	dec := func(v any) error {
		d := json.NewDecoder(bytesReader(raw))
		d.DisallowUnknownFields()
		return d.Decode(v)
	}
	switch ch.Type {
	case biz.ChannelTypeEmail:
		ch.Email = &biz.EmailConfig{}
		return dec(ch.Email)
	case biz.ChannelTypeWebhook:
		ch.Webhook = &biz.WebhookConfig{}
		return dec(ch.Webhook)
	case biz.ChannelTypeDingtalk, biz.ChannelTypeWecom, biz.ChannelTypeFeishu:
		ch.Robot = &biz.RobotConfig{}
		return dec(ch.Robot)
	default:
		return fmt.Errorf("unknown channel type %q", ch.Type)
	}
}

func loadRoutes(ctx context.Context, tx pgx.Tx) ([]*biz.Route, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, biz_id, event_pattern, severity_min, tag_match, priority, channel_ids, recipient_policy,
		        aggregate_window_s, suppress_threshold, suppress_duration_m, enabled
		 FROM notify_route WHERE delete_time IS NULL`)
	if err != nil {
		return nil, fmt.Errorf("load routes: %w", err)
	}
	defer rows.Close()
	var out []*biz.Route
	for rows.Next() {
		var (
			r                 biz.Route
			tagRaw, recipRaw  []byte
			severity, prio    int16
			window, thr, durM int32
		)
		if err := rows.Scan(&r.ID, &r.BizID, &r.EventPattern, &severity, &tagRaw, &prio, &r.ChannelIDs, &recipRaw,
			&window, &thr, &durM, &r.Enabled); err != nil {
			return nil, fmt.Errorf("scan route: %w", err)
		}
		r.SeverityMin, r.Priority = int32(severity), int32(prio)
		r.AggregateWindowS, r.SuppressThreshold, r.SuppressDurationM = window, thr, durM
		if len(tagRaw) > 0 {
			if err := json.Unmarshal(tagRaw, &r.TagMatch); err != nil {
				return nil, fmt.Errorf("decode tag_match of %s: %w", r.ID, err)
			}
		}
		if len(recipRaw) > 0 {
			if err := json.Unmarshal(recipRaw, &r.Recipient); err != nil {
				return nil, fmt.Errorf("decode recipient_policy of %s: %w", r.ID, err)
			}
		}
		out = append(out, &r)
	}
	return out, rows.Err()
}

func loadSilences(ctx context.Context, tx pgx.Tx) ([]*biz.Silence, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, biz_id, event_pattern, severity_max, time_window, enabled FROM notify_silence WHERE delete_time IS NULL`)
	if err != nil {
		return nil, fmt.Errorf("load silences: %w", err)
	}
	defer rows.Close()
	var out []*biz.Silence
	for rows.Next() {
		var (
			s      biz.Silence
			sevMax int16
			winRaw []byte
		)
		if err := rows.Scan(&s.ID, &s.BizID, &s.EventPattern, &sevMax, &winRaw, &s.Enabled); err != nil {
			return nil, fmt.Errorf("scan silence: %w", err)
		}
		s.SeverityMax = int32(sevMax)
		if len(winRaw) > 0 {
			s.Window = &biz.SilenceWindow{}
			if err := json.Unmarshal(winRaw, s.Window); err != nil {
				return nil, fmt.Errorf("decode time_window of %s: %w", s.ID, err)
			}
		}
		out = append(out, &s)
	}
	return out, rows.Err()
}

func loadTemplates(ctx context.Context, tx pgx.Tx) ([]*biz.Template, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, biz_id, event_type, channel_type, subject, body, version, enabled, format FROM notify_template WHERE delete_time IS NULL`)
	if err != nil {
		return nil, fmt.Errorf("load templates: %w", err)
	}
	defer rows.Close()
	var out []*biz.Template
	for rows.Next() {
		var t biz.Template
		if err := rows.Scan(&t.ID, &t.BizID, &t.EventType, &t.ChannelType, &t.Subject, &t.Body, &t.Version, &t.Enabled, &t.Format); err != nil {
			return nil, fmt.Errorf("scan template: %w", err)
		}
		out = append(out, &t)
	}
	return out, rows.Err()
}

func loadProducers(ctx context.Context, tx pgx.Tx) ([]*biz.Producer, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, biz_id, name, event_whitelist, token_hash, expire_time, enabled, qps_limit, daily_limit FROM notify_producer WHERE delete_time IS NULL`)
	if err != nil {
		return nil, fmt.Errorf("load producers: %w", err)
	}
	defer rows.Close()
	var out []*biz.Producer
	for rows.Next() {
		var p biz.Producer
		if err := rows.Scan(&p.ID, &p.BizID, &p.Name, &p.EventWhitelist, &p.TokenHash, &p.ExpireTime, &p.Enabled, &p.QPSLimit, &p.DailyLimit); err != nil {
			return nil, fmt.Errorf("scan producer: %w", err)
		}
		out = append(out, &p)
	}
	return out, rows.Err()
}
