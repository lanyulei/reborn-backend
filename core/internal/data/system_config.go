package data

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/lanyulei/monitor/backend/core/internal/biz"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var _ biz.SystemConfigRepo = (*systemConfigRepo)(nil)

// systemConfigPO 是 system_config 表的持久化对象；与 biz.SystemConfig
// 显式转换，禁止跨层直传。该表不做软删（全局唯一配置行，覆盖即更新）。
type systemConfigPO struct {
	id         string
	title      string
	subTitle   string
	logo       string
	footer     string
	updateTime time.Time
	updateUser *string
}

// toEntity 将 PO 转换为领域模型（DO）；update_user 为 NULL 时留空。
func (p systemConfigPO) toEntity() *biz.SystemConfig {
	cfg := &biz.SystemConfig{
		ID:         p.id,
		Title:      p.title,
		SubTitle:   p.subTitle,
		Logo:       p.logo,
		Footer:     p.footer,
		UpdateTime: p.updateTime,
	}
	if p.updateUser != nil {
		cfg.UpdateUser = *p.updateUser
	}
	return cfg
}

// systemConfigRepo 是系统配置仓储的 PostgreSQL 实现：手写参数化 SQL；
// logo 文件走本地磁盘（data.logoDir，覆盖式单文件）。
type systemConfigRepo struct {
	data *Data
}

// NewSystemConfigRepo new a systemConfigRepo.
func NewSystemConfigRepo(data *Data) *systemConfigRepo {
	return &systemConfigRepo{data: data}
}

// nullableUUID 空串转 NULL（uuid 列写参数用），非空原样透传。
func nullableUUID(v string) *string {
	if v == "" {
		return nil
	}
	s := v
	return &s
}

// GetConfig implements biz.SystemConfigRepo：表内无行返回零值配置
// （未配置语义），pgx.ErrNoRows 不上抛为错误。
func (r *systemConfigRepo) GetConfig(ctx context.Context) (*biz.SystemConfig, error) {
	var p systemConfigPO
	err := r.data.pool.QueryRow(ctx,
		`SELECT id, title, sub_title, logo, footer, update_time, update_user
		 FROM system_config LIMIT 1`).
		Scan(&p.id, &p.title, &p.subTitle, &p.logo, &p.footer,
			&p.updateTime, &p.updateUser)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &biz.SystemConfig{}, nil
		}
		return nil, fmt.Errorf("get system config: %w", err)
	}
	return p.toEntity(), nil
}

// SaveConfig implements biz.SystemConfigRepo：先 UPDATE 覆盖已有行
//（沿用原行主键），0 行受影响（首次配置）则以 cfg.ID INSERT；单写者场景
// 下竞态可忽略，并发首写撞主键时返回错误由调用方重试。
func (r *systemConfigRepo) SaveConfig(ctx context.Context, cfg *biz.SystemConfig) error {
	tag, err := r.data.pool.Exec(ctx,
		`UPDATE system_config
		 SET title = $1, sub_title = $2, logo = $3, footer = $4,
		     update_time = $5, update_user = $6`,
		cfg.Title, cfg.SubTitle, cfg.Logo, cfg.Footer,
		cfg.UpdateTime, nullableUUID(cfg.UpdateUser))
	if err != nil {
		return fmt.Errorf("update system config: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	if _, err := r.data.pool.Exec(ctx,
		`INSERT INTO system_config (id, title, sub_title, logo, footer, update_time, update_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		cfg.ID, cfg.Title, cfg.SubTitle, cfg.Logo, cfg.Footer,
		cfg.UpdateTime, nullableUUID(cfg.UpdateUser)); err != nil {
		return fmt.Errorf("insert system config: %w", err)
	}
	return nil
}

// logoFileExts 是 logo 落盘文件可能的扩展名集合（读取时探查存在者）。
var logoFileExts = []string{".png", ".jpg", ".svg", ".webp", ".ico"}

// extMime 是扩展名回推 MIME 的映射（读取时构造响应 Content-Type）。
var extMime = map[string]string{
	".png":  "image/png",
	".jpg":  "image/jpeg",
	".svg":  "image/svg+xml",
	".webp": "image/webp",
	".ico":  "image/x-icon",
}

// SaveLogoFile implements biz.SystemConfigRepo：覆盖式写入 logo 文件——
// 先删除全部旧扩展名文件（单文件语义），再写当前扩展名；目录不存在时
// 递归创建。version 由 biz 侧统一算法计算（sha1 前 8 位）。
func (r *systemConfigRepo) SaveLogoFile(ctx context.Context, data []byte, ext string) (string, error) {
	dir := r.data.logoDir
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create logo dir: %w", err)
	}
	for _, e := range logoFileExts {
		if err := os.Remove(filepath.Join(dir, "logo"+e)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("remove old logo: %w", err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "logo"+ext), data, 0o644); err != nil {
		return "", fmt.Errorf("write logo file: %w", err)
	}
	return biz.LogoVersion(data), nil
}

// ReadLogoFile implements biz.SystemConfigRepo：按扩展名集合探查现存文件；
// 一个都不存在视为从未上传，返回 biz.ErrLogoNotFound。
func (r *systemConfigRepo) ReadLogoFile(ctx context.Context) ([]byte, string, error) {
	for _, ext := range logoFileExts {
		data, err := os.ReadFile(filepath.Join(r.data.logoDir, "logo"+ext))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, "", fmt.Errorf("read logo file: %w", err)
		}
		return data, extMime[ext], nil
	}
	return nil, "", biz.ErrLogoNotFound
}
