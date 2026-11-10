package service

import (
	"context"
	"errors"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"github.com/go-kratos/kratos/v3/transport"
	"google.golang.org/genproto/googleapis/api/httpbody"
)

// SystemConfigService 实现 api/system/v1 生成的 SystemConfigServiceServer。
type SystemConfigService struct {
	v1.UnimplementedSystemConfigServiceServer

	uc *biz.SystemConfigUsecase
}

// NewSystemConfigService new a SystemConfigService.
func NewSystemConfigService(uc *biz.SystemConfigUsecase) *SystemConfigService {
	return &SystemConfigService{uc: uc}
}

// GetSystemConfig implements proto SystemConfigService/GetSystemConfig：
// 匿名可读（登录页登录前拉取标题/logo），未配置过时各字段为空串。
func (s *SystemConfigService) GetSystemConfig(ctx context.Context, _ *v1.GetSystemConfigRequest) (*v1.GetSystemConfigResponse, error) {
	cfg, err := s.uc.Get(ctx)
	if err != nil {
		return nil, err
	}
	return &v1.GetSystemConfigResponse{Config: toProtoSystemConfig(cfg)}, nil
}

// UpdateSystemConfig implements proto SystemConfigService/UpdateSystemConfig：
// 全量覆盖（四字段整页提交）；修改人取 JWT claims，接口经鉴权/授权中间件
// （仅 admin 或被授权角色可调）。
func (s *SystemConfigService) UpdateSystemConfig(ctx context.Context, req *v1.UpdateSystemConfigRequest) (*v1.UpdateSystemConfigResponse, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	cfg := &biz.SystemConfig{
		Title:    req.GetTitle(),
		SubTitle: req.GetSubTitle(),
		Logo:     req.GetLogo(),
		Footer:   req.GetFooter(),
	}
	if err := s.uc.Save(ctx, cfg, userID); err != nil {
		return nil, err
	}
	return &v1.UpdateSystemConfigResponse{}, nil
}

// UploadSystemLogo implements proto SystemConfigService/UploadSystemLogo：
// 保存本地上传的 logo 文件（覆盖式单文件），返回带内容指纹的 URL；文件
// 不直接落配置行，前端拿到 URL 随 UpdateSystemConfig 写入 logo 字段。
// 接口经鉴权/授权中间件（仅 admin 或被授权角色可调）。
func (s *SystemConfigService) UploadSystemLogo(ctx context.Context, req *v1.UploadSystemLogoRequest) (*v1.UploadSystemLogoResponse, error) {
	url, err := s.uc.UploadLogo(ctx, req.GetData(), req.GetMime())
	if err != nil {
		return nil, err
	}
	return &v1.UploadSystemLogoResponse{Url: url}, nil
}

// GetSystemLogo implements proto SystemConfigService/GetSystemLogo：以
// HttpBody 原始字节流返回当前 logo（供 <img> 直载），匿名可读；从未
// 上传过返回 LOGO_NOT_FOUND（404）。
//
// 响应附加 nosniff + CSP sandbox：白名单允许 SVG（可内嵌脚本），而本接口
// 与前端同源——浏览器直接打开该 URL 时若按文档渲染，脚本将在应用源下执行
// （持 config.edit 权限者可植入存储型 XSS）。sandbox 让其作为文档加载时
// 运行在不透明源且禁脚本；<img> 引用不受影响（图片上下文忽略 CSP）。
func (s *SystemConfigService) GetSystemLogo(ctx context.Context, _ *v1.GetSystemLogoRequest) (*httpbody.HttpBody, error) {
	data, mime, err := s.uc.GetLogo(ctx)
	if err != nil {
		if errors.Is(err, biz.ErrLogoNotFound) {
			return nil, v1.ErrorLogoNotFound("系统 logo 尚未上传")
		}
		return nil, err
	}
	if tr, ok := transport.FromServerContext(ctx); ok {
		tr.ReplyHeader().Set("X-Content-Type-Options", "nosniff")
		tr.ReplyHeader().Set("Content-Security-Policy", "sandbox")
	}
	return &httpbody.HttpBody{ContentType: mime, Data: data}, nil
}

// toProtoSystemConfig 将领域模型（DO）显式转换为 proto 实体。
func toProtoSystemConfig(c *biz.SystemConfig) *v1.SystemConfig {
	if c == nil {
		return &v1.SystemConfig{}
	}
	return &v1.SystemConfig{
		Title:    c.Title,
		SubTitle: c.SubTitle,
		Logo:     c.Logo,
		Footer:   c.Footer,
	}
}
