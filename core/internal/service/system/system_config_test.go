package service

import (
	"context"
	"testing"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

func newSystemConfigService(t *testing.T) (*SystemConfigService, *testfake.Store) {
	t.Helper()
	store := testfake.NewStore()
	files, _, _, err := testfake.NewFileUsecase(nil)
	if err != nil {
		t.Fatalf("testfake.NewFileUsecase() error = %v", err)
	}
	return NewSystemConfigService(biz.NewSystemConfigUsecase(store, files)), store
}

// logoUploader 上传 logo 的操作人（上传接口须登录）。
const logoUploader = "018f3e1a-0000-7000-8000-0000000000aa"

func logoCtx() context.Context { return claimsContext(logoUploader) }

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

	resp, err := svc.UploadSystemLogo(logoCtx(), &v1.UploadSystemLogoRequest{
		Data: []byte("\x89PNG\r\n\x1a\nfake-1"), Mime: "image/png",
	})
	if err != nil {
		t.Fatalf("UploadSystemLogo() error = %v", err)
	}
	if url := resp.GetUrl(); url != "/api/v1/system/config/logo?v="+biz.LogoVersion([]byte("\x89PNG\r\n\x1a\nfake-1")) {
		t.Fatalf("UploadSystemLogo() url = %q, want fingerprinted path", url)
	}

	body, err := svc.GetSystemLogo(context.Background(), &v1.GetSystemLogoRequest{})
	if err != nil {
		t.Fatalf("GetSystemLogo() error = %v", err)
	}
	if body.GetContentType() != "image/png" || string(body.GetData()) != "\x89PNG\r\n\x1a\nfake-1" {
		t.Fatalf("GetSystemLogo() = (%q, %q), want roundtrip", body.GetContentType(), body.GetData())
	}

	// 覆盖上传：内容变化指纹随之变化，读回新内容
	resp2, err := svc.UploadSystemLogo(logoCtx(), &v1.UploadSystemLogoRequest{
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

// 上传接口须登录：没有身份时拒绝，不落文件。
func TestSystemConfigService_UploadLogoNeedsUser(t *testing.T) {
	t.Parallel()
	svc, _ := newSystemConfigService(t)
	if _, err := svc.UploadSystemLogo(context.Background(), &v1.UploadSystemLogoRequest{Data: []byte("x"), Mime: "image/png"}); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("UploadSystemLogo(anonymous) error = %v, want ErrTokenInvalid", err)
	}
	if _, err := svc.GetSystemLogo(context.Background(), &v1.GetSystemLogoRequest{}); !errors.Is(err, v1.ErrorLogoNotFound("")) {
		t.Fatalf("GetSystemLogo() error = %v, want LOGO_NOT_FOUND", err)
	}
}

// 类型按内容判定，不看声明：声明成 PNG 的 HTML 被拒；声明错了的真图片按实际类型保存。
func TestSystemConfigService_LogoTypeByContent(t *testing.T) {
	t.Parallel()
	svc, _ := newSystemConfigService(t)
	for name, data := range map[string]string{"html": "<html><script>alert(1)</script></html>", "text": "not an image", "pdf": "%PDF-1.7\n"} {
		if _, err := svc.UploadSystemLogo(logoCtx(), &v1.UploadSystemLogoRequest{Data: []byte(data), Mime: "image/png"}); !errors.Is(err, biz.ErrImageTypeInvalid) {
			t.Errorf("UploadSystemLogo(%s as png) error = %v, want ErrImageTypeInvalid", name, err)
		}
	}
	if _, err := svc.GetSystemLogo(context.Background(), &v1.GetSystemLogoRequest{}); !errors.Is(err, v1.ErrorLogoNotFound("")) {
		t.Fatalf("rejected uploads must not be stored: %v", err)
	}
	if _, err := svc.UploadSystemLogo(logoCtx(), &v1.UploadSystemLogoRequest{Data: []byte("\xff\xd8\xff\xe0jpeg-bytes"), Mime: "image/png"}); err != nil {
		t.Fatalf("UploadSystemLogo(jpeg declared as png) error = %v", err)
	}
	if body, err := svc.GetSystemLogo(context.Background(), &v1.GetSystemLogoRequest{}); err != nil || body.GetContentType() != "image/jpeg" {
		t.Fatalf("GetSystemLogo() type = %q, %v; want the detected image/jpeg", body.GetContentType(), err)
	}
}

// 改用对象存储之前上传的 logo（旧位置上的文件）：照常读到，并在首次读取时导入；之后的上传
// 以新文件为准，旧位置上的文件不再生效。
func TestSystemConfigService_LegacyLogo(t *testing.T) {
	t.Parallel()
	svc, store := newSystemConfigService(t)
	store.SetLegacyLogo([]byte("<svg>old</svg>"), "image/svg+xml")
	for range 2 {
		body, err := svc.GetSystemLogo(context.Background(), &v1.GetSystemLogoRequest{})
		if err != nil || body.GetContentType() != "image/svg+xml" || string(body.GetData()) != "<svg>old</svg>" {
			t.Fatalf("GetSystemLogo(legacy) = (%q, %q), %v", body.GetContentType(), body.GetData(), err)
		}
	}
	// 已导入：旧位置上的文件变了也不影响读取。
	store.SetLegacyLogo([]byte("changed-on-disk"), "image/png")
	if body, err := svc.GetSystemLogo(context.Background(), &v1.GetSystemLogoRequest{}); err != nil || string(body.GetData()) != "<svg>old</svg>" {
		t.Fatalf("GetSystemLogo(after import) = %q, %v; want the imported copy", body.GetData(), err)
	}
	if _, err := svc.UploadSystemLogo(logoCtx(), &v1.UploadSystemLogoRequest{Data: []byte("\x89PNG\r\n\x1a\nnew"), Mime: "image/png"}); err != nil {
		t.Fatalf("UploadSystemLogo() error = %v", err)
	}
	if body, err := svc.GetSystemLogo(context.Background(), &v1.GetSystemLogoRequest{}); err != nil || body.GetContentType() != "image/png" || string(body.GetData()) != "\x89PNG\r\n\x1a\nnew" {
		t.Fatalf("GetSystemLogo(after upload) = (%q, %q), %v", body.GetContentType(), body.GetData(), err)
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
