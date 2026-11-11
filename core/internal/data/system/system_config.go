package data

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

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
// logo 文件走本地磁盘（data.LogoDir，覆盖式单文件）。
type systemConfigRepo struct {
	data *basedata.Data
}

// NewSystemConfigRepo new a systemConfigRepo.
func NewSystemConfigRepo(data *basedata.Data) *systemConfigRepo {
	return &systemConfigRepo{data: data}
}

// GetConfig implements biz.SystemConfigRepo：表内无行返回零值配置
// （未配置语义），pgx.ErrNoRows 不上抛为错误。ORDER BY id 使多行（历史
// 并发首写残留）时取值确定。
func (r *systemConfigRepo) GetConfig(ctx context.Context) (*biz.SystemConfig, error) {
	var p systemConfigPO
	err := r.data.Pool.QueryRow(ctx,
		`SELECT id, title, sub_title, logo, footer, update_time, update_user
		 FROM system_config ORDER BY id LIMIT 1`).
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
// （沿用原行主键），0 行受影响（首次配置）则以 cfg.ID INSERT。INSERT 带
// WHERE NOT EXISTS 守卫：表无唯一约束（每次 Save 的 cfg.ID 都是新 UUID，
// 并发首写不会撞主键），两条并发首写若都通过 UPDATE 的 0 行判定，守卫
// 让后到者的 INSERT 落 0 行而非产生第二行。
func (r *systemConfigRepo) SaveConfig(ctx context.Context, cfg *biz.SystemConfig) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE system_config
		 SET title = $1, sub_title = $2, logo = $3, footer = $4,
		     update_time = $5, update_user = $6`,
		cfg.Title, cfg.SubTitle, cfg.Logo, cfg.Footer,
		cfg.UpdateTime, dbx.NilIfEmpty(cfg.UpdateUser))
	if err != nil {
		return fmt.Errorf("update system config: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO system_config (id, title, sub_title, logo, footer, update_time, update_user)
		 SELECT $1, $2, $3, $4, $5, $6, $7
		 WHERE NOT EXISTS (SELECT 1 FROM system_config)`,
		cfg.ID, cfg.Title, cfg.SubTitle, cfg.Logo, cfg.Footer,
		cfg.UpdateTime, dbx.NilIfEmpty(cfg.UpdateUser)); err != nil {
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

// SaveLogoFile implements biz.SystemConfigRepo：覆盖式写入 logo 文件（单文件
// 语义）。原子替换：先写同目录临时文件并 fsync，再 rename 到目标名（POSIX
// rename 原子，并发读方要么看到完整旧文件要么看到完整新文件，绝不会读到
// 半截或 404），最后清理其他扩展名的旧文件。目录不存在时递归创建。version
// 由 biz 侧统一算法计算（sha256 前 8 位）。
func (r *systemConfigRepo) SaveLogoFile(ctx context.Context, data []byte, ext string) (string, error) {
	dir := r.data.LogoDir
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("create logo dir: %w", err)
	}
	target := filepath.Join(dir, "logo"+ext)
	if err := writeFileAtomic(dir, target, data); err != nil {
		return "", fmt.Errorf("write logo file: %w", err)
	}
	for _, e := range logoFileExts {
		if e == ext {
			continue
		}
		if err := os.Remove(filepath.Join(dir, "logo"+e)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("remove old logo: %w", err)
		}
	}
	return biz.LogoVersion(data), nil
}

// writeFileAtomic 经同目录临时文件 + fsync + rename 原子落盘 target（0600）。
// 任一步失败清理临时文件，目标文件保持原状。
func writeFileAtomic(dir, target string, data []byte) (err error) {
	tmp, err := os.CreateTemp(dir, ".logo-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if err = tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if _, err = tmp.Write(data); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err = os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}

// ReadLogoFile implements biz.SystemConfigRepo：按扩展名集合探查现存文件；
// 一个都不存在视为从未上传，返回 biz.ErrLogoNotFound。
func (r *systemConfigRepo) ReadLogoFile(ctx context.Context) ([]byte, string, error) {
	for _, ext := range logoFileExts {
		// #nosec G304 -- 路径由受控配置目录与固定扩展名白名单拼接，无外部输入
		data, err := os.ReadFile(filepath.Join(r.data.LogoDir, "logo"+ext))
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
