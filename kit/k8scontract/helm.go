package k8scontract

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Helm 应用（功能设计 §11）。仓库配置在 k8s_helm_repo（core 写，数据面只读并解密凭据）；
// Chart 的下载与渲染、Release 的全部操作都在数据面进行，且一律以调用者的身份访问集群。
//
// 与集群无关的操作（OpHelmRepo* / OpHelmChart*）不带 ClusterHeader，任选一个数据面实例执行。
// ---------------------------------------------------------------------------

// Helm 操作名（InternalOpPath 之后的路径段）。
const (
	OpHelmRepoTest   = "helm-repo-test"  // HelmRepoTestRequest → HelmRepoTestResponse（无集群）
	OpHelmCharts     = "helm-charts"     // HelmChartsRequest → HelmChartsResponse（无集群）
	OpHelmChart      = "helm-chart"      // HelmChartRequest → HelmChartResponse（无集群）
	OpHelmList       = "helm-list"       // HelmListRequest → HelmListResponse
	OpHelmGet        = "helm-get"        // HelmReleaseRequest → HelmReleaseDetail
	OpHelmPreview    = "helm-preview"    // HelmApplyRequest → HelmPreviewResponse
	OpHelmInstall    = "helm-install"    // HelmApplyRequest → HelmApplyResponse
	OpHelmUpgrade    = "helm-upgrade"    // HelmApplyRequest → HelmApplyResponse
	OpHelmRollback   = "helm-rollback"   // HelmRollbackRequest → HelmApplyResponse
	OpHelmUninstall  = "helm-uninstall"  // HelmReleaseRequest → HelmApplyResponse
	OpNamespaceQuota = "namespace-quota" // NamespaceQuotaRequest → NamespaceQuotaResponse
)

// HelmClusterless 报告该操作是否与集群无关（不带 ClusterHeader）。
func HelmClusterless(op string) bool {
	return op == OpHelmRepoTest || op == OpHelmCharts || op == OpHelmChart
}

// IsHelmOp 报告是否为 Helm 操作。
func IsHelmOp(op string) bool { return strings.HasPrefix(op, "helm-") }

// IsHelmWrite 报告是否为改动 Release 的 Helm 操作（异步执行并审计）。
func IsHelmWrite(op string) bool {
	return op == OpHelmInstall || op == OpHelmUpgrade || op == OpHelmRollback || op == OpHelmUninstall
}

// Helm 仓库类型（k8s_helm_repo.type）。
const (
	HelmRepoHTTP int32 = 1 // 经典仓库：<url>/index.yaml
	HelmRepoOCI  int32 = 2 // OCI 仓库：oci://<registry>/<项目路径>，Chart 名接在其后
)

// Helm 审计动作与资源（k8s_audit）。
const (
	ActionHelmInstall   = "helm_install"
	ActionHelmUpgrade   = "helm_upgrade"
	ActionHelmRollback  = "helm_rollback"
	ActionHelmUninstall = "helm_uninstall"
	// HelmReleaseResource / HelmReleaseKind Release 在审计里的资源类型与 Kind（api_group 为 helm.sh）。
	HelmReleaseGroup    = "helm.sh"
	HelmReleaseVersion  = "v3"
	HelmReleaseResource = "helmreleases"
	HelmReleaseKind     = "HelmRelease"
	// AuditResourceHelmRepo 仓库配置变更在审计里的资源类型。
	AuditResourceHelmRepo = "helm-repo"
)

// Helm 限制。
const (
	MaxHelmIndexBytes  = 48 << 20 // index.yaml 上限
	MaxHelmChartBytes  = 20 << 20 // Chart 包上限
	MaxHelmValuesBytes = 512 << 10
	MaxHelmCharts      = 2000 // 目录一次返回的 Chart 数上限
	MaxHelmVersions    = 100  // 每个 Chart 返回的版本数上限（新的在前）
	// HelmSyncWait 写操作同步等待的时长：到点未完成则返回 Pending，操作在数据面继续执行。
	HelmSyncWait = 20 * time.Second
	// HelmOpTimeout 单次写操作（含钩子）的总时限。
	HelmOpTimeout = 5 * time.Minute
)

var (
	helmReleaseName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
	helmChartName   = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._/-]{0,253}$`)
	helmVersion     = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._+-]{0,127}$`)
)

// ValidHelmReleaseName Release 名：DNS 子域名，至多 53 字符（Helm 的限制）。
func ValidHelmReleaseName(name string) bool {
	return len(name) > 0 && len(name) <= 53 && helmReleaseName.MatchString(name)
}

// ValidHelmChartName Chart 名（OCI 仓库下可含路径分隔符）；不得含 ".."。
func ValidHelmChartName(name string) bool {
	return helmChartName.MatchString(name) && !strings.Contains(name, "..") && !strings.Contains(name, "//")
}

// ValidHelmVersion Chart 版本 / OCI 标签。
func ValidHelmVersion(v string) bool { return helmVersion.MatchString(v) }

// NormalizeHelmRepoURL 校验并规整仓库地址（core 写入时与数据面使用前都调用，口径一致）：
//   - HTTP 仓库：http / https；OCI 仓库：oci；
//   - 不得内嵌凭据、query、fragment；必须有主机名；
//   - 带凭据（hasCredential）时不允许明文 http（凭据会以明文传输）。
//
// 返回去掉末尾斜杠的地址。不做私网拦截：仓库由持有仓库管理权限的人维护，天然指向内网
// （与监控数据源同一立场）；链路本地 / 云元数据地址在数据面拨号时拒绝。
func NormalizeHelmRepoURL(typ int32, raw string, hasCredential bool) (string, error) {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" {
		return "", errors.New("url is not a valid absolute address")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("url must not contain credentials, query or fragment")
	}
	switch typ {
	case HelmRepoHTTP:
		if u.Scheme != "http" && u.Scheme != "https" {
			return "", errors.New("url of an HTTP repository must start with https:// or http://")
		}
		if u.Scheme == "http" && hasCredential {
			return "", errors.New("credentials require https: they would be sent in clear text over http")
		}
	case HelmRepoOCI:
		if u.Scheme != "oci" {
			return "", errors.New("url of an OCI repository must start with oci://")
		}
	default:
		return "", errors.New("unknown repository type")
	}
	if strings.Contains(u.Path, "..") {
		return "", errors.New("url path must not contain ..")
	}
	return strings.TrimRight(u.String(), "/"), nil
}

// ---- 仓库与 Chart ----

// HelmRepoTestRequest 测试仓库配置。RepoID 非空且 Secret 为空时沿用已保存的凭据。
type HelmRepoTestRequest struct {
	RepoID             string `json:"repoId,omitempty"`
	Type               int32  `json:"type"`
	URL                string `json:"url"`
	Username           string `json:"username,omitempty"`
	Secret             string `json:"secret,omitempty"`
	CACert             string `json:"caCert,omitempty"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"`
	// Chart OCI 仓库测试时用于验证凭据的 Chart 名（可空：只验证地址格式与连通性）。
	Chart string `json:"chart,omitempty"`
}

// HelmRepoTestResponse 测试结果。
type HelmRepoTestResponse struct {
	OK         bool   `json:"ok"`
	Charts     int    `json:"charts"`
	DurationMS int64  `json:"durationMs"`
	Error      string `json:"error,omitempty"`
}

// HelmChartsRequest Chart 目录。OCI 仓库没有目录接口：必须给出 Chart 名，返回它的标签。
type HelmChartsRequest struct {
	RepoID  string `json:"repoId"`
	Keyword string `json:"keyword,omitempty"`
	// Chart 只看这一个 Chart（OCI 仓库必填）。
	Chart string `json:"chart,omitempty"`
	// Refresh 跳过 5 分钟的索引缓存。
	Refresh bool `json:"refresh,omitempty"`
}

// HelmChartVersion Chart 的一个版本。
type HelmChartVersion struct {
	Version    string     `json:"version"`
	AppVersion string     `json:"appVersion,omitempty"`
	Created    *time.Time `json:"created,omitempty"`
	Deprecated bool       `json:"deprecated,omitempty"`
}

// HelmChartSummary 目录中的一个 Chart。
type HelmChartSummary struct {
	Name        string             `json:"name"`
	Description string             `json:"description,omitempty"`
	Icon        string             `json:"icon,omitempty"`
	Home        string             `json:"home,omitempty"`
	Keywords    []string           `json:"keywords,omitempty"`
	Versions    []HelmChartVersion `json:"versions"`
}

// HelmChartsResponse Chart 目录。
type HelmChartsResponse struct {
	Items []HelmChartSummary `json:"items"`
	// Truncated 结果被 MaxHelmCharts 截断，请用关键字缩小范围。
	Truncated bool `json:"truncated,omitempty"`
	// FetchTime 索引的抓取时间。
	FetchTime time.Time `json:"fetchTime"`
}

// HelmChartRequest Chart 详情。
type HelmChartRequest struct {
	RepoID  string `json:"repoId"`
	Chart   string `json:"chart"`
	Version string `json:"version"`
}

// HelmChartResponse Chart 详情。
type HelmChartResponse struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	AppVersion  string `json:"appVersion,omitempty"`
	Description string `json:"description,omitempty"`
	Type        string `json:"type,omitempty"`
	// Values 缺省 values.yaml 原文。
	Values string `json:"values"`
	// Schema values.schema.json 原文（没有为空）。
	Schema string `json:"schema,omitempty"`
	Readme string `json:"readme,omitempty"`
}

// ---- Release ----

// HelmListRequest Release 列表。Namespace 为空 = 全部命名空间（只有部分权限时返回有权限的那些）。
type HelmListRequest struct {
	Namespace string `json:"namespace,omitempty"`
	Keyword   string `json:"keyword,omitempty"`
}

// HelmRelease Release 概要。
type HelmRelease struct {
	Namespace    string    `json:"namespace"`
	Name         string    `json:"name"`
	Revision     int       `json:"revision"`
	Status       string    `json:"status"`
	Chart        string    `json:"chart"`
	ChartVersion string    `json:"chartVersion"`
	AppVersion   string    `json:"appVersion,omitempty"`
	UpdateTime   time.Time `json:"updateTime"`
	Description  string    `json:"description,omitempty"`
}

// HelmListResponse Release 列表，按命名空间、名称排序。
type HelmListResponse struct {
	Items []HelmRelease `json:"items"`
	// PartialNamespaces 请求全部命名空间但只有部分权限时，实际返回了数据的命名空间。
	PartialNamespaces []string `json:"partialNamespaces,omitempty"`
}

// HelmReleaseRequest 针对单个 Release 的请求。
type HelmReleaseRequest struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// HelmResource Release 清单里的一个资源。
type HelmResource struct {
	Kind string `json:"kind"`
	// Ref 可在资源浏览器里打开的引用；集群不认识该类型时 Resource 为空。
	Ref Ref `json:"ref"`
}

// HelmReleaseDetail Release 详情。
type HelmReleaseDetail struct {
	Release HelmRelease `json:"release"`
	// Values 用户提供的 values（YAML）；没有为空串。
	Values    string         `json:"values"`
	Notes     string         `json:"notes,omitempty"`
	Manifest  string         `json:"manifest"`
	Resources []HelmResource `json:"resources"`
	// History 历史版本，新的在前。
	History []HelmRelease `json:"history"`
}

// HelmApplyRequest 安装或升级（也是预检的请求）。
type HelmApplyRequest struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	RepoID    string `json:"repoId"`
	Chart     string `json:"chart"`
	Version   string `json:"version"`
	// Values 用户提供的 values（YAML）。
	Values string `json:"values,omitempty"`
	// Upgrade 仅预检用：true = 按升级预检（与当前 Release 对比）。
	Upgrade bool `json:"upgrade,omitempty"`
}

// HelmPreviewResponse 预检结果：渲染出的清单；升级时带当前 Release 的清单与 values 供两层对比。
type HelmPreviewResponse struct {
	Upgrade         bool   `json:"upgrade"`
	Manifest        string `json:"manifest"`
	CurrentManifest string `json:"currentManifest,omitempty"`
	Values          string `json:"values"`
	CurrentValues   string `json:"currentValues,omitempty"`
	CurrentChart    string `json:"currentChart,omitempty"`
	Notes           string `json:"notes,omitempty"`
}

// HelmRollbackRequest 回滚到指定版本。
type HelmRollbackRequest struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Revision  int    `json:"revision"`
}

// HelmApplyResponse 写操作的结果。Pending = 在 HelmSyncWait 内未完成，操作仍在执行，
// 稍后查看 Release 的状态。
type HelmApplyResponse struct {
	Pending  bool   `json:"pending"`
	Revision int    `json:"revision,omitempty"`
	Status   string `json:"status,omitempty"`
	Notes    string `json:"notes,omitempty"`
}

// ---- 命名空间配额 ----

// NamespaceQuotaRequest 命名空间的配额用量。Namespace 为空 = 调用者可见的全部命名空间。
type NamespaceQuotaRequest struct {
	Namespace string `json:"namespace,omitempty"`
}

// QuotaUsage 一项资源的已用量与上限（K8s 数量文本，如 500m、2Gi）。
type QuotaUsage struct {
	Resource string `json:"resource"`
	Used     string `json:"used"`
	Hard     string `json:"hard"`
	// Ratio 已用 / 上限，0-1；上限为 0 时为 0。
	Ratio float64 `json:"ratio"`
}

// NamespaceQuota 一个命名空间的配额汇总（多个 ResourceQuota 取每项资源最严格的上限）。
type NamespaceQuota struct {
	Namespace string       `json:"namespace"`
	Quotas    []string     `json:"quotas"`
	Items     []QuotaUsage `json:"items"`
}

// NamespaceQuotaResponse 配额用量；没有 ResourceQuota 的命名空间不在结果中。
type NamespaceQuotaResponse struct {
	Items []NamespaceQuota `json:"items"`
}
