package biz

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

// Helm 仓库的哨兵错误。
var (
	ErrHelmRepoNotFound   = errors.NotFound(v1.ErrorReason_K8S_HELM_REPO_NOT_FOUND.String(), "helm repository not found")
	ErrHelmRepoNameExists = errors.Conflict(v1.ErrorReason_K8S_HELM_REPO_NAME_EXISTS.String(), "helm repository name already exists")
	ErrHelmRepoInvalid    = errors.BadRequest(v1.ErrorReason_K8S_HELM_REPO_INVALID.String(), "invalid helm repository")
)

// HelmRepo Helm 仓库（k8s_helm_repo）。恒不含凭据密文。
type HelmRepo struct {
	ID          string
	Name        string
	Type        int32
	URL         string
	Username    string
	CACert      string
	Insecure    bool
	Description string
	Enabled     bool
	HasSecret   bool
	Version     int32
	CreateTime  time.Time
	UpdateTime  time.Time
}

// HelmRepoInput Helm 仓库的可编辑内容。
type HelmRepoInput struct {
	Name        string
	Type        int32
	URL         string
	Username    string
	CACert      string
	Insecure    bool
	Description string
	Enabled     bool
}

// HelmRepoStore Helm 仓库读写。每次写入同事务追加一条审计。数据面按仓库的 version 失效索引缓存，
// 无需 bump 配置版本。
type HelmRepoStore interface {
	List(ctx context.Context, keyword string, enabledOnly bool, page, pageSize int64) ([]*HelmRepo, int64, error)
	// Get 不存在返回 ErrHelmRepoNotFound。
	Get(ctx context.Context, id string) (*HelmRepo, error)
	// Save 新建（expectVersion = 0）或乐观锁更新。secret：nil 沿用已保存的，空切片清除，其余为新密文。
	// 名称重复返回 ErrHelmRepoNameExists，版本不一致返回 ErrVersionConflict。
	Save(ctx context.Context, r *HelmRepo, secret *[]byte, expectVersion int32, audit *AuditEntry) error
}

// HelmRepoDeleter Helm 仓库删除（软删并清除凭据）。
type HelmRepoDeleter interface {
	Delete(ctx context.Context, id string, audit *AuditEntry) error
}

// HelmUsecase Helm 仓库管理与目录。访问仓库的动作（测试、目录、Chart 详情）一律转给数据面：
// core 不发起到仓库的出站请求，也不解密仓库凭据。
type HelmUsecase struct {
	store   HelmRepoStore
	deleter HelmRepoDeleter
	sealer  SecretSealer
	dir     Directory
	planes  *PlaneUsecase
}

// NewHelmUsecase new a HelmUsecase.
func NewHelmUsecase(store HelmRepoStore, deleter HelmRepoDeleter, sealer SecretSealer, dir Directory, planes *PlaneUsecase) *HelmUsecase {
	return &HelmUsecase{store: store, deleter: deleter, sealer: sealer, dir: dir, planes: planes}
}

// normalize 校验并规整仓库内容。hasSecret：保存后是否带凭据（决定是否允许明文 http）。
func (in *HelmRepoInput) normalize(hasSecret bool) error {
	in.Name, in.Username, in.Description = strings.TrimSpace(in.Name), strings.TrimSpace(in.Username), strings.TrimSpace(in.Description)
	if in.Name == "" {
		return detail(ErrHelmRepoInvalid, "name is required")
	}
	if hasSecret && in.Username == "" {
		return detail(ErrHelmRepoInvalid, "username is required when a password or token is set")
	}
	u, err := kc.NormalizeHelmRepoURL(in.Type, in.URL, in.Username != "" && hasSecret)
	if err != nil {
		return detail(ErrHelmRepoInvalid, "url: %s", err)
	}
	in.URL = u
	in.CACert = strings.TrimSpace(in.CACert)
	if in.CACert != "" {
		if err := checkCACert(in.CACert); err != nil {
			return detail(ErrHelmRepoInvalid, "ca_cert: %s", err)
		}
	}
	return nil
}

func helmRepoAuditDetail(in HelmRepoInput, hasSecret bool) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{
		"name": in.Name, "type": in.Type, "url": in.URL, "username": in.Username, "has_secret": hasSecret,
		"has_ca_cert": in.CACert != "", "insecure_skip_verify": in.Insecure, "enabled": in.Enabled,
	})
	return raw
}

func (in HelmRepoInput) apply(r *HelmRepo) {
	r.Name, r.Type, r.URL, r.Username, r.CACert = in.Name, in.Type, in.URL, in.Username, in.CACert
	r.Insecure, r.Description, r.Enabled = in.Insecure, in.Description, in.Enabled
}

// List 分页查询。
func (uc *HelmUsecase) List(ctx context.Context, keyword string, page int64, pageSize int32) ([]*HelmRepo, int64, error) {
	p, size := paging.Normalize(page, int64(pageSize))
	return uc.store.List(ctx, strings.TrimSpace(keyword), false, p, size)
}

// Options 已启用的仓库（安装 / 升级时选择）。
func (uc *HelmUsecase) Options(ctx context.Context) ([]*HelmRepo, error) {
	items, _, err := uc.store.List(ctx, "", true, 1, 500)
	return items, err
}

// Create 新建仓库。secret 为 nil 或空 = 匿名访问。
func (uc *HelmUsecase) Create(ctx context.Context, caller Caller, in HelmRepoInput, secret *string) (*HelmRepo, error) {
	has := secret != nil && *secret != ""
	if err := in.normalize(has); err != nil {
		return nil, err
	}
	var sealed *[]byte
	if has {
		b, err := uc.sealer.Seal(*secret)
		if err != nil {
			return nil, err
		}
		sealed = &b
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	r := &HelmRepo{ID: id}
	in.apply(r)
	audit := newAuditEntry(ctx, uc.dir, caller, kc.AuditResourceHelmRepo, kc.ActionCreate, helmRepoAuditDetail(in, has))
	if err := uc.store.Save(ctx, r, sealed, 0, audit); err != nil {
		return nil, err
	}
	return uc.store.Get(ctx, id)
}

// Update 编辑仓库（整体替换）。secret：nil 沿用已保存的，空串清除。类型不可改。
// 沿用已保存的凭据时地址的主机不可改：否则已保存的凭据会被发往新的主机。
func (uc *HelmUsecase) Update(ctx context.Context, caller Caller, id string, in HelmRepoInput, secret *string, version int32) (*HelmRepo, error) {
	cur, err := uc.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if in.Type != cur.Type {
		return nil, detail(ErrHelmRepoInvalid, "repository type cannot be changed")
	}
	has := cur.HasSecret
	if secret != nil {
		has = *secret != ""
	}
	if err := in.normalize(has); err != nil {
		return nil, err
	}
	var sealed *[]byte
	switch {
	case secret == nil:
		if cur.HasSecret && !sameHost(cur.URL, in.URL) {
			return nil, detail(ErrHelmRepoInvalid, "the address host changed: re-enter the password or token")
		}
	case *secret == "":
		empty := []byte{}
		sealed = &empty
	default:
		b, serr := uc.sealer.Seal(*secret)
		if serr != nil {
			return nil, serr
		}
		sealed = &b
	}
	r := &HelmRepo{ID: id}
	in.apply(r)
	audit := newAuditEntry(ctx, uc.dir, caller, kc.AuditResourceHelmRepo, kc.ActionUpdate, helmRepoAuditDetail(in, has))
	if err := uc.store.Save(ctx, r, sealed, version, audit); err != nil {
		return nil, err
	}
	return uc.store.Get(ctx, id)
}

func sameHost(a, b string) bool {
	host := func(s string) string {
		_, rest, _ := strings.Cut(s, "://")
		h, _, _ := strings.Cut(rest, "/")
		return strings.ToLower(h)
	}
	return host(a) == host(b)
}

// Delete 删除仓库；已安装的 Release 不受影响。
func (uc *HelmUsecase) Delete(ctx context.Context, caller Caller, id string) error {
	return uc.deleter.Delete(ctx, id, newAuditEntry(ctx, uc.dir, caller, kc.AuditResourceHelmRepo, kc.ActionDelete, nil))
}

// Test 测试仓库配置：校验后转给数据面实际访问。未带凭据且指定了已保存的仓库时沿用其凭据（数据面读取并解密）。
func (uc *HelmUsecase) Test(ctx context.Context, caller Caller, id string, in HelmRepoInput, secret *string, chart string) ([]byte, error) {
	has := secret != nil && *secret != ""
	req := kc.HelmRepoTestRequest{}
	if !has && id != "" {
		cur, err := uc.store.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if cur.HasSecret && secret == nil {
			has, req.RepoID = true, cur.ID
		}
	}
	if err := in.normalize(has); err != nil {
		return nil, err
	}
	if chart != "" && !kc.ValidHelmChartName(chart) {
		return nil, detail(ErrHelmRepoInvalid, "chart: invalid chart name")
	}
	req.Type, req.URL, req.Username, req.CACert, req.InsecureSkipVerify, req.Chart = in.Type, in.URL, in.Username, in.CACert, in.Insecure, chart
	if secret != nil {
		req.Secret = *secret
	}
	return uc.planes.Call(ctx, caller, "", kc.OpHelmRepoTest, req)
}

// Charts Chart 目录（数据面访问仓库）。
func (uc *HelmUsecase) Charts(ctx context.Context, caller Caller, req kc.HelmChartsRequest) ([]byte, error) {
	if req.Chart != "" && !kc.ValidHelmChartName(req.Chart) {
		return nil, detail(ErrInvalidArgument, "chart: invalid chart name")
	}
	return uc.planes.Call(ctx, caller, "", kc.OpHelmCharts, req)
}

// Chart Chart 详情（数据面访问仓库）。
func (uc *HelmUsecase) Chart(ctx context.Context, caller Caller, req kc.HelmChartRequest) ([]byte, error) {
	if !kc.ValidHelmChartName(req.Chart) || !kc.ValidHelmVersion(req.Version) {
		return nil, detail(ErrInvalidArgument, "invalid chart name or version")
	}
	return uc.planes.Call(ctx, caller, "", kc.OpHelmChart, req)
}

// CheckRelease 校验 Release 引用与 Chart 引用的字面量（数据面会再校验一遍）。
func CheckRelease(name, chart, version string) error {
	if !kc.ValidHelmReleaseName(name) {
		return detail(ErrInvalidArgument, "name: lowercase letters, digits, '-' and '.', at most 53 characters")
	}
	if chart != "" && !kc.ValidHelmChartName(chart) {
		return detail(ErrInvalidArgument, "chart: invalid chart name")
	}
	if version != "" && !kc.ValidHelmVersion(version) {
		return detail(ErrInvalidArgument, "version: invalid chart version")
	}
	return nil
}
