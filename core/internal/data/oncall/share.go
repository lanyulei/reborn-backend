package data

import (
	"context"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/jackc/pgx/v5"
)

var _ biz.ShareRepo = (*shareRepo)(nil)

// shareSelectColumns 公共地址查询列序；token_hash / token_sealed 不出库（校验走 FindByHash
// 的等值匹配，密文只经 GetSealedToken 单独读取）。
const shareSelectColumns = `l.id::text, l.scope_type, l.scope_id::text, l.name, l.show_phone, l.expire_time, l.last_access_time,
	COALESCE(NULLIF(u.nickname, ''), u.username, ''), l.create_time`

const shareFrom = ` FROM oncall_share_link l LEFT JOIN "system_user" u ON u.id = l.create_user`

func scanShareLink(scan func(dest ...any) error) (*biz.ShareLink, error) {
	var l biz.ShareLink
	if err := scan(&l.ID, &l.ScopeType, &l.ScopeID, &l.Name, &l.ShowPhone, &l.ExpireTime, &l.LastAccessTime, &l.CreatorName, &l.CreateTime); err != nil {
		return nil, err
	}
	return &l, nil
}

type shareRepo struct {
	data *basedata.Data
}

// NewShareRepo new a shareRepo.
func NewShareRepo(data *basedata.Data) *shareRepo { return &shareRepo{data: data} }

// List implements biz.ShareRepo.
func (r *shareRepo) List(ctx context.Context, scopeType int32, scopeID string) ([]*biz.ShareLink, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+shareSelectColumns+shareFrom+` WHERE l.scope_type = $1 AND l.scope_id = $2 AND l.delete_time IS NULL
		 ORDER BY l.create_time DESC, l.id`, scopeType, scopeID)
	if err != nil {
		return nil, fmt.Errorf("list oncall share links: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.ShareLink, 0, 8)
	for rows.Next() {
		l, err := scanShareLink(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan oncall share link: %w", err)
		}
		items = append(items, l)
	}
	return items, rows.Err()
}

func (r *shareRepo) one(ctx context.Context, where string, arg any) (*biz.ShareLink, error) {
	l, err := scanShareLink(r.data.Pool.QueryRow(ctx,
		"SELECT "+shareSelectColumns+shareFrom+" WHERE "+where+" AND l.delete_time IS NULL", arg).Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrShareLinkNotFound
		}
		return nil, fmt.Errorf("get oncall share link: %w", err)
	}
	return l, nil
}

// Get implements biz.ShareRepo.
func (r *shareRepo) Get(ctx context.Context, id string) (*biz.ShareLink, error) {
	return r.one(ctx, "l.id = $1", id)
}

// GetSealedToken implements biz.ShareRepo.
func (r *shareRepo) GetSealedToken(ctx context.Context, id string) ([]byte, error) {
	var sealed []byte
	err := r.data.Pool.QueryRow(ctx, "SELECT token_sealed FROM oncall_share_link WHERE id = $1 AND delete_time IS NULL", id).Scan(&sealed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrShareLinkNotFound
		}
		return nil, fmt.Errorf("get oncall share link token: %w", err)
	}
	return sealed, nil
}

// FindByHash implements biz.ShareRepo：按哈希等值查找（唯一索引），不做逐字节比较，
// 无时序侧信道可言。
func (r *shareRepo) FindByHash(ctx context.Context, hash []byte) (*biz.ShareLink, error) {
	return r.one(ctx, "l.token_hash = $1", hash)
}

// Touch implements biz.ShareRepo：至多每小时写一次，避免公共页面高频访问变成写放大。
func (r *shareRepo) Touch(ctx context.Context, id string) error {
	_, err := r.data.Pool.Exec(ctx,
		`UPDATE oncall_share_link SET last_access_time = now()
		 WHERE id = $1 AND (last_access_time IS NULL OR last_access_time < now() - interval '1 hour')`, id)
	if err != nil {
		return fmt.Errorf("touch oncall share link: %w", err)
	}
	return nil
}

var _ biz.TokenCipher = (*TokenBox)(nil)

// TokenBox 公共地址令牌加解密：kit/secretbox（AES-256-GCM），主密钥与通知渠道凭据相同
// （KRATOS_NOTIFY_MASTER_KEY）。与渠道凭据的"只写不读"不同，这里 core 自己要解密——
// 令牌本就是要给管理者看的地址，加密只为数据库泄露时不直接暴露可用地址。
type TokenBox struct {
	box *secretbox.Box
}

// NewTokenBox 由启动配置构造。
func NewTokenBox(c *conf.Notify) (*TokenBox, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &TokenBox{box: box}, nil
}

// Seal implements biz.TokenCipher.
func (b *TokenBox) Seal(plaintext string) ([]byte, error) {
	out, err := b.box.Seal(plaintext)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return nil, biz.ErrSecretUnavailable
	}
	return out, err
}

// Open implements biz.TokenCipher.
func (b *TokenBox) Open(sealed []byte) (string, error) {
	out, err := b.box.Open(sealed)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return "", biz.ErrSecretUnavailable
	}
	if err != nil {
		// 主密钥已更换或密文损坏：不向客户端暴露细节。
		return "", fmt.Errorf("open oncall share token: %w", err)
	}
	return out, nil
}
