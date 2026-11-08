package service

import (
	"context"
	"testing"

	v1 "github.com/lanyulei/monitor/backend/core/api/system/v1"
	"github.com/lanyulei/monitor/backend/core/internal/biz"
	"github.com/lanyulei/monitor/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

func newSystemConfigService(t *testing.T) (*SystemConfigService, *testfake.Store) {
	t.Helper()
	store := testfake.NewStore()
	return NewSystemConfigService(biz.NewSystemConfigUsecase(store)), store
}

// 未配置过：Get 返回空配置（各字段空串），不报错——前端按空值兜底默认。
func TestSystemConfigService_GetUnconfigured(t *testing.T) {
	t.Parallel()
	svc, _ := newSystemConfigService(t)

	resp, err := svc.GetSystemConfig(context.Background(), &v1.GetSystemConfigRequest{})
	if err != nil {
		t.Fatalf("GetSystemConfig() error = %v", err)
	}
	cfg := resp.GetConfig()
	if cfg.GetTitle() != "" || cfg.GetSubTitle() != "" || cfg.GetLogo() != "" || cfg.GetFooter() != "" {
		t.Fatalf("unconfigured config = %+v, want all empty", cfg)
	}
}

// 写读闭环：Update 全量覆盖后 Get 回读一致；空串字段覆盖为空（清空语义）。
func TestSystemConfigService_UpdateThenGet(t *testing.T) {
	t.Parallel()
	svc, _ := newSystemConfigService(t)

	if _, err := svc.UpdateSystemConfig(claimsContext("user-1"), &v1.UpdateSystemConfigRequest{
		Title: "监控中心", SubTitle: "个人监控系统", Logo: "https://example.com/logo.svg", Footer: "© 2026",
	}); err != nil {
		t.Fatalf("UpdateSystemConfig() error = %v", err)
	}
	resp, err := svc.GetSystemConfig(context.Background(), &v1.GetSystemConfigRequest{})
	if err != nil {
		t.Fatalf("GetSystemConfig() error = %v", err)
	}
	cfg := resp.GetConfig()
	if cfg.GetTitle() != "监控中心" || cfg.GetSubTitle() != "个人监控系统" ||
		cfg.GetLogo() != "https://example.com/logo.svg" || cfg.GetFooter() != "© 2026" {
		t.Fatalf("config roundtrip = %+v", cfg)
	}

	// 全量覆盖：清空子标题与页脚，其余保留
	if _, err := svc.UpdateSystemConfig(claimsContext("user-1"), &v1.UpdateSystemConfigRequest{
		Title: "监控中心", Logo: "https://example.com/logo.svg",
	}); err != nil {
		t.Fatalf("UpdateSystemConfig() second error = %v", err)
	}
	resp, err = svc.GetSystemConfig(context.Background(), &v1.GetSystemConfigRequest{})
	if err != nil {
		t.Fatalf("GetSystemConfig() second error = %v", err)
	}
	cfg = resp.GetConfig()
	if cfg.GetTitle() != "监控中心" || cfg.GetSubTitle() != "" || cfg.GetFooter() != "" {
		t.Fatalf("config overwrite = %+v, want subTitle/footer cleared", cfg)
	}
}

// 写接口必经鉴权：ctx 无 claims（匿名）时拒绝，返回令牌无效哨兵。
func TestSystemConfigService_UpdateWithoutClaims(t *testing.T) {
	t.Parallel()
	svc, _ := newSystemConfigService(t)

	_, err := svc.UpdateSystemConfig(context.Background(), &v1.UpdateSystemConfigRequest{Title: "x"})
	if !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("UpdateSystemConfig(anonymous) error = %v, want ErrTokenInvalid", err)
	}
}

// logo 上传读回闭环：Upload 返回带指纹的 URL，Get 以 HttpBody 回读原始
// 字节与 MIME；重复上传覆盖旧内容（单文件语义）且指纹随内容变化。
func TestSystemConfigService_UploadThenGetLogo(t *testing.T) {
	t.Parallel()
	svc, _ := newSystemConfigService(t)

	resp, err := svc.UploadSystemLogo(context.Background(), &v1.UploadSystemLogoRequest{
		Data: []byte("\x89PNG-fake-1"), Mime: "image/png",
	})
	if err != nil {
		t.Fatalf("UploadSystemLogo() error = %v", err)
	}
	if url := resp.GetUrl(); url != "/api/v1/system/config/logo?v="+biz.LogoVersion([]byte("\x89PNG-fake-1")) {
		t.Fatalf("UploadSystemLogo() url = %q, want fingerprinted path", url)
	}

	body, err := svc.GetSystemLogo(context.Background(), &v1.GetSystemLogoRequest{})
	if err != nil {
		t.Fatalf("GetSystemLogo() error = %v", err)
	}
	if body.GetContentType() != "image/png" || string(body.GetData()) != "\x89PNG-fake-1" {
		t.Fatalf("GetSystemLogo() = (%q, %q), want roundtrip", body.GetContentType(), body.GetData())
	}

	// 覆盖上传：内容变化指纹随之变化，读回新内容
	resp2, err := svc.UploadSystemLogo(context.Background(), &v1.UploadSystemLogoRequest{
		Data: []byte("<svg>fake-2</svg>"), Mime: "image/svg+xml",
	})
	if err != nil {
		t.Fatalf("UploadSystemLogo() second error = %v", err)
	}
	if resp2.GetUrl() == resp.GetUrl() {
		t.Fatalf("fingerprint unchanged after overwrite: %q", resp2.GetUrl())
	}
	body2, err := svc.GetSystemLogo(context.Background(), &v1.GetSystemLogoRequest{})
	if err != nil {
		t.Fatalf("GetSystemLogo() second error = %v", err)
	}
	if body2.GetContentType() != "image/svg+xml" || string(body2.GetData()) != "<svg>fake-2</svg>" {
		t.Fatalf("GetSystemLogo() after overwrite = (%q, %q), want new content", body2.GetContentType(), body2.GetData())
	}
}

// 从未上传过 logo：Get 返回 LOGO_NOT_FOUND（404，未配置语义）。
func TestSystemConfigService_GetLogoNotFound(t *testing.T) {
	t.Parallel()
	svc, _ := newSystemConfigService(t)

	_, err := svc.GetSystemLogo(context.Background(), &v1.GetSystemLogoRequest{})
	if !errors.Is(err, v1.ErrorLogoNotFound("系统 logo 尚未上传")) {
		t.Fatalf("GetSystemLogo(unuploaded) error = %v, want LOGO_NOT_FOUND", err)
	}
}
