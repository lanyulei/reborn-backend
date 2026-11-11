package biz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// SystemConfig 是系统配置领域模型（DO）：全局展示配置（标题/子标题/logo/
// 页脚文案）的单例值对象；与 proto 实体、data 层 PO 显式转换。
type SystemConfig struct {
	// ID 配置行主键；仅未配置过（表内无行）时用于首次插入，已有行时
	// 仓储沿用原行主键覆盖更新。
	ID         string
	Title      string
	SubTitle   string
	Logo       string
	Footer     string
	UpdateTime time.Time
	UpdateUser string
}

// SystemConfigRepo 是系统配置仓储接口（单例读写 + logo 文件落盘），
// 由 data 层实现（依赖倒置）。
type SystemConfigRepo interface {
	// GetConfig 读取系统配置行；表内无行时返回零值配置（未配置语义），不报错。
	GetConfig(ctx context.Context) (*SystemConfig, error)
	// SaveConfig 全量保存系统配置：已有行则覆盖更新（沿用原行主键），
	// 无行则以 cfg.ID 插入（单行语义由本方法保证）。
	SaveConfig(ctx context.Context, cfg *SystemConfig) error
	// SaveLogoFile 覆盖式写入 logo 图片文件（单文件语义：先清掉旧扩展名
	// 文件），返回内容指纹（sha1 前 8 位，内容变化即变化，作缓存刷新参数）。
	SaveLogoFile(ctx context.Context, data []byte, ext string) (version string, err error)
	// ReadLogoFile 读取当前 logo 文件；从未上传过时返回 ErrLogoNotFound。
	ReadLogoFile(ctx context.Context) (data []byte, mime string, err error)
}

// ErrLogoNotFound 系统 logo 从未上传过（未配置语义，service 层映射 404）。
var ErrLogoNotFound = errors.New("logo not found")

// LogoMimeExts 是允许的 logo MIME 类型到文件扩展名的映射（与 proto 校验
// 白名单一致；扩展名决定落盘文件名）。
var LogoMimeExts = map[string]string{
	"image/png":     ".png",
	"image/jpeg":    ".jpg",
	"image/svg+xml": ".svg",
	"image/webp":    ".webp",
	"image/x-icon":  ".ico",
}

// logoURLPath 是 logo 文件的服务路径（同 GET 接口；?v= 为内容指纹）。
const logoURLPath = "/api/v1/system/config/logo"

// SystemConfigUsecase 是系统配置用例：单例配置的读与全量覆盖写。
type SystemConfigUsecase struct {
	repo SystemConfigRepo
}

// NewSystemConfigUsecase new a SystemConfigUsecase.
func NewSystemConfigUsecase(repo SystemConfigRepo) *SystemConfigUsecase {
	return &SystemConfigUsecase{repo: repo}
}

// Get 读取系统配置；未配置过时返回零值配置（各字段空串，前端兜底默认值）。
func (uc *SystemConfigUsecase) Get(ctx context.Context) (*SystemConfig, error) {
	return uc.repo.GetConfig(ctx)
}

// Save 全量覆盖系统配置：生成新行主键（首次插入用）、填充审计时间与修改人
// 后委托仓储；updateUser 来自 JWT claims（写接口必经鉴权中间件）。
func (uc *SystemConfigUsecase) Save(ctx context.Context, cfg *SystemConfig, updateUser string) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	cfg.ID = id
	cfg.UpdateTime = time.Now().UTC()
	cfg.UpdateUser = updateUser
	return uc.repo.SaveConfig(ctx, cfg)
}

// UploadLogo 保存本地上传的 logo 图片（覆盖旧文件），返回带内容指纹的
// 访问 URL；文件不落配置行，前端拿到 URL 后随配置保存写入 logo 字段。
func (uc *SystemConfigUsecase) UploadLogo(ctx context.Context, data []byte, mime string) (string, error) {
	ext, ok := LogoMimeExts[mime]
	if !ok {
		// proto 校验白名单外的类型（防御双保险，正常到不了这里）
		return "", fmt.Errorf("unsupported logo mime: %s", mime)
	}
	version, err := uc.repo.SaveLogoFile(ctx, data, ext)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s?v=%s", logoURLPath, version), nil
}

// GetLogo 读取当前 logo 图片二进制与 MIME；从未上传过返回 ErrLogoNotFound。
func (uc *SystemConfigUsecase) GetLogo(ctx context.Context) ([]byte, string, error) {
	return uc.repo.ReadLogoFile(ctx)
}

// LogoVersion 计算内容指纹（sha256 前 8 位 hex）；内容变化即变化，
// 作为 logo URL 的缓存刷新参数（data 层落盘后回填）。仅作缓存键，
// 非安全哈希用途，但仍取强原语避免弱算法告警。
func LogoVersion(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:4])
}
