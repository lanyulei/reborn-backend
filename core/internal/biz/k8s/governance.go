package biz

import (
	"bytes"
	"compress/gzip"
	"context"
	_ "embed" // 内置的 K8s 告警规则模板
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

// 治理类错误。
var (
	ErrKubeTokenNotFound   = errors.NotFound(v1.ErrorReason_K8S_KUBE_TOKEN_NOT_FOUND.String(), "kubeconfig token not found")
	ErrKubeTokenLimit      = errors.Conflict(v1.ErrorReason_K8S_KUBE_TOKEN_LIMIT.String(), "too many active kubeconfig tokens, revoke unused ones first")
	ErrKubeTokenInvalid    = errors.BadRequest(v1.ErrorReason_K8S_KUBE_TOKEN_INVALID.String(), "invalid kubeconfig request")
	ErrInspectionNotFound  = errors.NotFound(v1.ErrorReason_K8S_INSPECTION_NOT_FOUND.String(), "inspection not found")
	ErrInspectionBusy      = errors.Conflict(v1.ErrorReason_K8S_INSPECTION_BUSY.String(), "an inspection is already pending or running for this cluster")
	ErrExecRecordNotFound  = errors.NotFound(v1.ErrorReason_K8S_EXEC_RECORD_NOT_FOUND.String(), "terminal session not found or has no recording")
	ErrApprovalRequired    = errors.Forbidden(v1.ErrorReason_K8S_APPROVAL_REQUIRED.String(), "changes on this cluster require approval and cannot be made through a kubeconfig; submit the change in the platform")
	errKubeUnauthorized    = errors.Unauthorized(v1.ErrorReason_K8S_TICKET_INVALID.String(), "kubeconfig token is invalid, expired or revoked")
	errKubeTooManyRequests = errors.New(http.StatusTooManyRequests, v1.ErrorReason_K8S_TOO_MANY_STREAMS.String(), "too many requests for this kubeconfig token")
)

// GovOptions 治理类配置（k8s.kubeconfig）。
type GovOptions struct {
	// ServerURL 平台的对外地址（如 https://ops.example.com）；空 = 由前端按当前站点地址拼接。
	ServerURL string
	// MaxTTL 限时 kubeconfig 的最长有效期；≤0 取 7 天。
	MaxTTL time.Duration
}

// KubeToken 限时 kubeconfig 令牌（k8s_kube_token）。
type KubeToken struct {
	ID           string
	UserID       string
	Username     string
	ClusterID    string
	ClusterName  string
	Note         string
	CreateTime   time.Time
	ExpireTime   time.Time
	LastUsedTime *time.Time
	LastUsedIP   string
	RevokeTime   *time.Time
}

// Active 令牌在 now 时刻是否仍有效。
func (t *KubeToken) Active(now time.Time) bool {
	return t.RevokeTime == nil && now.Before(t.ExpireTime)
}

// KubeTokenFilter 令牌列表过滤条件；UserID 为空 = 不按用户过滤。
type KubeTokenFilter struct {
	UserID     string
	ClusterID  string
	ActiveOnly bool
}

// KubeTokenRepo 限时 kubeconfig 令牌的管理。
type KubeTokenRepo interface {
	// Create 写入令牌；该用户未失效的令牌数已达 limit 返回 ErrKubeTokenLimit。同事务写审计。
	Create(ctx context.Context, t *KubeToken, hash []byte, limit int, audit *AuditEntry) error
	List(ctx context.Context, filter KubeTokenFilter, page, pageSize int64) ([]*KubeToken, int64, error)
	// Revoke 吊销未失效的令牌：id 非空 = 该令牌（ownerID 非空时还须属于该用户），否则 = ownerID 的全部令牌。
	// 返回吊销的条数。同事务写审计。
	Revoke(ctx context.Context, id, ownerID, by string, audit *AuditEntry) (int, error)
}

// KubeTokenAuth 校验一个请求所需的令牌与集群状态（每次都读库，短暂缓存见 GovernanceUsecase）。
type KubeTokenAuth struct {
	ID               string
	UserID           string
	ClusterID        string
	Hash             []byte
	ExpireTime       time.Time
	Revoked          bool
	ClusterEnabled   bool
	ApprovalRequired bool
}

// KubeTokenAuthRepo 代理入口校验令牌用。
type KubeTokenAuthRepo interface {
	// Lookup 按令牌 ID 读取；不存在返回 (nil, nil)。
	Lookup(ctx context.Context, id string) (*KubeTokenAuth, error)
	// Touch 回写最近使用时间与来源 IP（尽力而为）。
	Touch(ctx context.Context, id, ip string) error
}

// InspectionRun 一次巡检（k8s_inspection_run）。
type InspectionRun struct {
	ID          string
	ClusterID   string
	ClusterName string
	Trigger     int32
	Status      int32
	Score       int32
	Summary     kc.InspectionSummary
	Error       string
	CreateTime  time.Time
	FinishTime  *time.Time
}

// InspectionFinding 一条发现项。
type InspectionFinding struct {
	Check     string
	Severity  int32
	Namespace string
	Kind      string
	Name      string
	Message   string
}

// InspectionRepo 巡检记录（数据面写入；core 只读，另可插入待执行的手动巡检）。
type InspectionRepo interface {
	List(ctx context.Context, clusterID string, page, pageSize int64) ([]*InspectionRun, int64, error)
	// Get 读取一次巡检及其发现项。namespaces 非 nil 时只返回这些命名空间与集群级（命名空间为空）的发现项。
	Get(ctx context.Context, id string, namespaces []string, limit int) (*InspectionRun, []*InspectionFinding, error)
	// Create 插入一条待执行的手动巡检；该集群已有待执行 / 执行中的返回 ErrInspectionBusy。同事务写审计。
	Create(ctx context.Context, run *InspectionRun, userID string, audit *AuditEntry) error
}

// ExecRecording 一个终端会话的录制原文。
type ExecRecording struct {
	Session   *ExecSession
	Truncated bool
	// Chunks 按序号排列的块（gzip）。
	Chunks [][]byte
}

// ExecRecordRepo 终端录制读取与回放审计。
type ExecRecordRepo interface {
	// Recording 读取会话与录制块；会话不存在或没有录制返回 ErrExecRecordNotFound。
	Recording(ctx context.Context, sessionID string) (*ExecRecording, error)
	// AuditReplay 记一条回放审计。
	AuditReplay(ctx context.Context, audit *AuditEntry, s *ExecSession) error
}

// GrantSource 角色的数据范围（data 层的 insightRepo 实现）。
type GrantSource interface {
	Grants(ctx context.Context, roleIDs []string) ([]*RoleScope, error)
}

// PlaneRouter 数据面的原始通道地址（k8splane.Client 实现）：返回负责该集群的实例地址与已签名的身份头，
// 由 server 层做不缓冲的反向代理（集群代理、文件传输）。
type PlaneRouter interface {
	Route(ctx context.Context, id kc.Identity, clusterID string) (base string, header http.Header, err error)
	// Invalidate 丢弃集群归属缓存（数据面应答「不是我负责」时调用）。
	Invalidate(clusterID string)
}

// ExecRecordEvent 回放事件。
type ExecRecordEvent struct {
	OffsetMS int64
	Type     string
	Data     []byte
	Cols     uint32
	Rows     uint32
}

// maxReplayBytes 单次回放解压后的输出上限（防御：录制上限由数据面保证，这里再兜底）。
const maxReplayBytes = 64 << 20

// DecodeRecording 解压并解析录制块为按时间排序的事件。无法解析的行跳过。
func DecodeRecording(chunks [][]byte) ([]ExecRecordEvent, error) {
	var out []ExecRecordEvent
	var total int64
	for _, chunk := range chunks {
		zr, err := gzip.NewReader(bytes.NewReader(chunk))
		if err != nil {
			return nil, fmt.Errorf("corrupted recording chunk: %w", err)
		}
		raw, err := io.ReadAll(io.LimitReader(zr, maxReplayBytes-total+1))
		_ = zr.Close()
		if err != nil {
			return nil, fmt.Errorf("corrupted recording chunk: %w", err)
		}
		if total += int64(len(raw)); total > maxReplayBytes {
			return nil, detail(ErrInvalidArgument, "recording is too large to replay")
		}
		for _, line := range bytes.Split(raw, []byte{'\n'}) {
			if len(line) == 0 {
				continue
			}
			var row []json.RawMessage
			var ms int64
			var typ, data string
			if json.Unmarshal(line, &row) != nil || len(row) != 3 || json.Unmarshal(row[0], &ms) != nil ||
				json.Unmarshal(row[1], &typ) != nil || json.Unmarshal(row[2], &data) != nil {
				continue
			}
			ev := ExecRecordEvent{OffsetMS: ms, Type: typ}
			switch typ {
			case kc.RecordOutput:
				b, err := base64.StdEncoding.DecodeString(data)
				if err != nil {
					continue
				}
				ev.Data = b
			case kc.RecordResize:
				c, r, ok := strings.Cut(data, "x")
				cols, e1 := strconv.ParseUint(c, 10, 16)
				rows, e2 := strconv.ParseUint(r, 10, 16)
				if !ok || e1 != nil || e2 != nil {
					continue
				}
				ev.Cols, ev.Rows = uint32(cols), uint32(rows)
			default:
				continue
			}
			out = append(out, ev)
		}
	}
	return out, nil
}

//go:embed rules/k8s-alerts.yaml
var alertRuleTemplate string

// AlertRuleTemplate 内置的 K8s 告警规则模板（Prometheus 规则 YAML）与规则条数。
func AlertRuleTemplate() (string, int) {
	return alertRuleTemplate, strings.Count(alertRuleTemplate, "- alert:")
}

// kubeSession 缓存的令牌校验结果。
type kubeSession struct {
	auth     *KubeTokenAuth
	identity kc.Identity
	expire   time.Time
	touched  time.Time

	// 令牌桶限速。
	tokens float64
	last   time.Time
}

const (
	// kubeSessionTTL 令牌状态与用户身份的缓存时长：吊销、停用、角色变化在此时间内生效。
	kubeSessionTTL = 5 * time.Second
	// kubeRate / kubeBurst 每个令牌的请求限速（kubectl 的资源发现会在瞬间发出上百个请求）。
	kubeRate  = 50.0
	kubeBurst = 300.0
	// kubeTouchInterval 最近使用时间的回写间隔。
	kubeTouchInterval = time.Minute
	// maxKubeConnsPerToken / maxKubeConnsPerUser 同时在途的请求数上限（watch、exec 等长连接每条占一个）：
	// 只限请求速率的话，一个令牌可以攒出成千上万条长连接，耗尽 core 的文件描述符。
	maxKubeConnsPerToken = 100
	maxKubeConnsPerUser  = 300
	// maxKubeSessions 缓存条数上限（超过即清空：只影响 5 秒内的重复校验）。
	maxKubeSessions = 10000
)

// GovernanceUsecase 治理用例：限时 kubeconfig、巡检查询、终端录制回放。
type GovernanceUsecase struct {
	tokens   KubeTokenRepo
	auths    KubeTokenAuthRepo
	inspects InspectionRepo
	records  ExecRecordRepo
	grants   GrantSource
	clusters ClusterReader
	roles    RoleSource
	accounts Accounts
	dir      Directory
	planes   *PlaneUsecase
	router   PlaneRouter
	opts     GovOptions
	now      func() time.Time

	mu       sync.Mutex
	sessions map[string]*kubeSession
	// tokenConns / userConns 在途请求数（令牌 ID / 用户 ID → 条数）；与 sessions 分开存，缓存清空不影响计数。
	tokenConns map[string]int
	userConns  map[string]int
}

// NewGovernanceUsecase new a GovernanceUsecase.
func NewGovernanceUsecase(tokens KubeTokenRepo, auths KubeTokenAuthRepo, inspects InspectionRepo, records ExecRecordRepo, grants GrantSource,
	clusters ClusterReader, roles RoleSource, accounts Accounts, dir Directory, planes *PlaneUsecase, router PlaneRouter, opts GovOptions,
) *GovernanceUsecase {
	if opts.MaxTTL <= 0 || opts.MaxTTL > kc.DefaultMaxKubeToken {
		opts.MaxTTL = kc.DefaultMaxKubeToken
	}
	opts.ServerURL = strings.TrimRight(strings.TrimSpace(opts.ServerURL), "/")
	return &GovernanceUsecase{
		tokens: tokens, auths: auths, inspects: inspects, records: records, grants: grants, clusters: clusters, roles: roles,
		accounts: accounts, dir: dir, planes: planes, router: router, opts: opts, now: time.Now, sessions: map[string]*kubeSession{},
	}
}

// access 解析调用者的数据范围。
func (uc *GovernanceUsecase) access(ctx context.Context, userID string) (Access, bool, error) {
	isAdmin, roleIDs, err := uc.roles.EnabledRoleIDs(ctx, userID)
	if err != nil {
		return Access{}, false, err
	}
	if isAdmin {
		return Access{All: true}, true, nil
	}
	if len(roleIDs) == 0 {
		return NewAccess(nil), false, nil
	}
	grants, err := uc.grants.Grants(ctx, roleIDs)
	if err != nil {
		return Access{}, false, err
	}
	return NewAccess(grants), false, nil
}

// cluster 取调用者数据范围内、已启用的集群。范围外与不存在同样返回 404（不泄露集群是否存在）。
func (uc *GovernanceUsecase) cluster(ctx context.Context, caller Caller, clusterID string) (*Cluster, Access, error) {
	access, _, err := uc.access(ctx, caller.UserID)
	if err != nil {
		return nil, access, err
	}
	if !access.AllowsCluster(clusterID) {
		return nil, access, ErrClusterNotFound
	}
	c, err := uc.clusters.Get(ctx, clusterID)
	if err != nil {
		return nil, access, err
	}
	if !c.Enabled {
		return nil, access, ErrClusterNotFound
	}
	return c, access, nil
}

// KubeProxyPath 集群代理入口的路径（其后接集群 API 路径）。
func KubeProxyPath(clusterID string) string { return "/api/v1/k8s/proxy/" + clusterID }

// IssuedKubeconfig 签发结果。Secret 只在此返回一次。
type IssuedKubeconfig struct {
	Token      *KubeToken
	Secret     string
	Server     string
	Kubeconfig string
}

// RenderKubeconfig 生成 kubeconfig 文本。名称只含集群名（已由登记时的正则约束）与固定前缀，无需转义；
// server 与 token 以 JSON 字符串写出（YAML 的双引号标量与之兼容）。
func RenderKubeconfig(clusterName, server, token string) string {
	q := func(s string) string { b, _ := json.Marshal(s); return string(b) }
	name := "reborn-" + clusterName
	return "apiVersion: v1\nkind: Config\ncurrent-context: " + name + "\n" +
		"clusters:\n- name: " + name + "\n  cluster:\n    server: " + q(server) + "\n" +
		"users:\n- name: " + name + "\n  user:\n    token: " + q(token) + "\n" +
		"contexts:\n- name: " + name + "\n  context:\n    cluster: " + name + "\n    user: " + name + "\n"
}

// IssueKubeconfig 为调用者签发限时 kubeconfig。集群须在其数据范围内；令牌不携带任何权限——
// 每个请求都按该用户当时的角色经数据面访问集群。
func (uc *GovernanceUsecase) IssueKubeconfig(ctx context.Context, caller Caller, clusterID string, ttl time.Duration, note string) (*IssuedKubeconfig, error) {
	if ttl < kc.MinKubeTokenTTL || ttl > uc.opts.MaxTTL {
		return nil, detail(ErrKubeTokenInvalid, "validity must be between %s and %s", kc.MinKubeTokenTTL, uc.opts.MaxTTL)
	}
	c, _, err := uc.cluster(ctx, caller, clusterID)
	if err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	secret, hash, err := kc.NewToken(id)
	if err != nil {
		return nil, err
	}
	now := uc.now()
	t := &KubeToken{
		ID: id, UserID: caller.UserID, ClusterID: c.ID, ClusterName: c.Name, Note: strings.TrimSpace(note),
		CreateTime: now, ExpireTime: now.Add(ttl),
	}
	detailJSON, _ := json.Marshal(map[string]any{"tokenId": id, "ttlSeconds": int64(ttl.Seconds()), "note": t.Note})
	audit := newAuditEntry(ctx, uc.dir, caller, kc.AuditResourceKubeToken, kc.ActionKubeconfigIssue, detailJSON)
	t.Username = audit.Username
	if err := uc.tokens.Create(ctx, t, hash, kc.MaxKubeTokensPerUser, audit); err != nil {
		return nil, err
	}
	server := uc.opts.ServerURL + KubeProxyPath(c.ID)
	return &IssuedKubeconfig{Token: t, Secret: secret, Server: server, Kubeconfig: RenderKubeconfig(c.Name, server, secret)}, nil
}

// ListKubeTokens 查询令牌：非超管只能看自己的。
func (uc *GovernanceUsecase) ListKubeTokens(ctx context.Context, caller Caller, all bool, userID string, filter KubeTokenFilter, page, pageSize int64) ([]*KubeToken, int64, error) {
	isAdmin, _, err := uc.roles.EnabledRoleIDs(ctx, caller.UserID)
	if err != nil {
		return nil, 0, err
	}
	filter.UserID = caller.UserID
	if isAdmin {
		switch {
		case userID != "":
			filter.UserID = userID
		case all:
			filter.UserID = ""
		}
	}
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.tokens.List(ctx, filter, page, pageSize)
}

// RevokeKubeToken 吊销一个令牌：自己的，或超管吊销任何人的。
func (uc *GovernanceUsecase) RevokeKubeToken(ctx context.Context, caller Caller, id string) error {
	isAdmin, _, err := uc.roles.EnabledRoleIDs(ctx, caller.UserID)
	if err != nil {
		return err
	}
	owner := caller.UserID
	if isAdmin {
		owner = ""
	}
	detailJSON, _ := json.Marshal(map[string]any{"tokenId": id})
	n, err := uc.tokens.Revoke(ctx, id, owner, caller.UserID,
		newAuditEntry(ctx, uc.dir, caller, kc.AuditResourceKubeToken, kc.ActionKubeconfigRevoke, detailJSON))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrKubeTokenNotFound
	}
	uc.forget(id)
	return nil
}

// RevokeUserKubeTokens 吊销某个用户的全部令牌（仅超管）。
func (uc *GovernanceUsecase) RevokeUserKubeTokens(ctx context.Context, caller Caller, userID string) (int, error) {
	isAdmin, _, err := uc.roles.EnabledRoleIDs(ctx, caller.UserID)
	if err != nil {
		return 0, err
	}
	if !isAdmin {
		return 0, detail(ErrForbidden, "only administrators can revoke another user's kubeconfig tokens")
	}
	detailJSON, _ := json.Marshal(map[string]any{"userId": userID})
	n, err := uc.tokens.Revoke(ctx, "", userID, caller.UserID,
		newAuditEntry(ctx, uc.dir, caller, kc.AuditResourceKubeToken, kc.ActionKubeconfigRevoke, detailJSON))
	if err != nil {
		return 0, err
	}
	uc.mu.Lock()
	uc.sessions = map[string]*kubeSession{}
	uc.mu.Unlock()
	return n, nil
}

func (uc *GovernanceUsecase) forget(tokenID string) {
	uc.mu.Lock()
	delete(uc.sessions, tokenID)
	uc.mu.Unlock()
}

// KubeRoute 一个已通过校验的集群代理请求该去的地方。
type KubeRoute struct {
	TokenID   string
	ClusterID string
	// Base 数据面实例地址；Header 已签名的身份头（含集群头与令牌头）。
	Base   string
	Header http.Header
	// Release 请求结束时调用一次：归还该令牌与该用户的并发连接名额。
	Release func()
}

// AuthorizeKube 校验经限时 kubeconfig 发来的一个请求并给出转发目标。
//
// 每个请求都校验：令牌存在、密钥匹配（常数时间）、未吊销、未过期、属于 URL 里的集群；集群已启用；
// 用户仍可用。令牌状态与用户身份缓存 5 秒（吊销由本实例发起时立即清除），身份按用户当时的启用角色解析，
// 因此角色解绑、账号停用、数据范围收回都会生效。未通过的原因不区分，一律 401。
// 需审批的集群上，非超管的变更请求返回 ErrApprovalRequired。
func (uc *GovernanceUsecase) AuthorizeKube(ctx context.Context, bearer, clusterID, method, kubePath, clientIP string) (*KubeRoute, error) {
	tokenID, secret, ok := kc.ParseToken(bearer)
	if !ok {
		return nil, errKubeUnauthorized
	}
	now := uc.now()
	uc.mu.Lock()
	s := uc.sessions[tokenID]
	if s != nil && now.After(s.expire) {
		s = nil
	}
	uc.mu.Unlock()
	if s == nil {
		auth, err := uc.auths.Lookup(ctx, tokenID)
		if err != nil {
			return nil, err
		}
		if auth == nil || !kc.SecretMatches(secret, auth.Hash) {
			return nil, errKubeUnauthorized
		}
		if auth.Revoked || !now.Before(auth.ExpireTime) || !auth.ClusterEnabled {
			return nil, errKubeUnauthorized
		}
		active, err := uc.accounts.Active(ctx, auth.UserID)
		if err != nil {
			if errors.FromError(err).Code == http.StatusInternalServerError {
				return nil, err
			}
			return nil, errKubeUnauthorized
		}
		if !active {
			return nil, errKubeUnauthorized
		}
		id, err := uc.planes.identity(ctx, Caller{UserID: auth.UserID, ClientIP: clientIP}, auth.ClusterID)
		if err != nil {
			if errors.Is(err, ErrClusterNotFound) { // 用户在该集群上已没有任何授权
				return nil, errKubeUnauthorized
			}
			return nil, err
		}
		s = &kubeSession{auth: auth, identity: id, expire: now.Add(kubeSessionTTL), tokens: kubeBurst, last: now}
		uc.mu.Lock()
		if old := uc.sessions[tokenID]; old != nil { // 保留限速与回写状态
			s.tokens, s.last, s.touched = old.tokens, old.last, old.touched
		}
		if len(uc.sessions) >= maxKubeSessions {
			uc.sessions = map[string]*kubeSession{}
		}
		uc.sessions[tokenID] = s
		uc.mu.Unlock()
	} else if !kc.SecretMatches(secret, s.auth.Hash) || !now.Before(s.auth.ExpireTime) {
		return nil, errKubeUnauthorized
	}
	if s.auth.ClusterID != clusterID {
		return nil, errKubeUnauthorized
	}
	// 限速与最近使用时间。
	uc.mu.Lock()
	s.tokens = min(kubeBurst, s.tokens+now.Sub(s.last).Seconds()*kubeRate)
	s.last = now
	limited := s.tokens < 1
	if !limited {
		s.tokens--
	}
	userID := s.auth.UserID
	if !limited && (uc.tokenConns[tokenID] >= maxKubeConnsPerToken || uc.userConns[userID] >= maxKubeConnsPerUser) {
		limited = true
	}
	if !limited {
		if uc.tokenConns == nil {
			uc.tokenConns, uc.userConns = map[string]int{}, map[string]int{}
		}
		uc.tokenConns[tokenID]++
		uc.userConns[userID]++
	}
	touch := now.Sub(s.touched) >= kubeTouchInterval
	if touch {
		s.touched = now
	}
	id := s.identity
	uc.mu.Unlock()
	if limited {
		return nil, errKubeTooManyRequests
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			uc.mu.Lock()
			if uc.tokenConns[tokenID]--; uc.tokenConns[tokenID] <= 0 {
				delete(uc.tokenConns, tokenID)
			}
			if uc.userConns[userID]--; uc.userConns[userID] <= 0 {
				delete(uc.userConns, userID)
			}
			uc.mu.Unlock()
		})
	}
	if touch {
		_ = uc.auths.Touch(context.WithoutCancel(ctx), tokenID, clientIP)
	}
	if s.auth.ApprovalRequired && kc.KubeIsChange(method, kubePath) {
		if !id.IsAdmin {
			release()
			return nil, ErrApprovalRequired
		}
		// 超管经 kubectl 在需审批的集群上直接变更：与界面上的「紧急变更」同等对待，审计标记为紧急。
		id.Emergency = true
	}
	id.ClientIP = clientIP
	if rid, err := idgen.NewID(); err == nil {
		id.RequestID = rid
	}
	base, header, err := uc.router.Route(ctx, id, clusterID)
	if err != nil {
		release()
		return nil, err
	}
	header.Set(kc.KubeTokenHeader, tokenID)
	return &KubeRoute{TokenID: tokenID, ClusterID: clusterID, Base: base, Header: header, Release: release}, nil
}

// InvalidateRoute 数据面应答「不是我负责」时丢弃归属缓存。
func (uc *GovernanceUsecase) InvalidateRoute(clusterID string) { uc.router.Invalidate(clusterID) }

// PlaneRoute 以调用者的身份取数据面的原始通道（文件传输）。
func (uc *GovernanceUsecase) PlaneRoute(ctx context.Context, caller Caller, clusterID string) (string, http.Header, error) {
	id, err := uc.planes.identity(ctx, caller, clusterID)
	if err != nil {
		return "", nil, err
	}
	return uc.router.Route(ctx, id, clusterID)
}

// ListInspectionRuns 查询某个集群的巡检记录。
func (uc *GovernanceUsecase) ListInspectionRuns(ctx context.Context, caller Caller, clusterID string, page, pageSize int64) ([]*InspectionRun, int64, error) {
	if _, _, err := uc.cluster(ctx, caller, clusterID); err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.inspects.List(ctx, clusterID, page, pageSize)
}

// maxFindings 单次返回的发现项上限。
const maxFindings = 5000

// GetInspectionRun 巡检详情。只有部分命名空间授权的用户只看到这些命名空间的发现项与集群级发现项
// （节点等——他们本来就能在界面上看到节点）；scoped 标明结果被过滤过。
func (uc *GovernanceUsecase) GetInspectionRun(ctx context.Context, caller Caller, id string) (*InspectionRun, []*InspectionFinding, bool, error) {
	access, _, err := uc.access(ctx, caller.UserID)
	if err != nil {
		return nil, nil, false, err
	}
	run, _, err := uc.inspects.Get(ctx, id, []string{}, 0)
	if err != nil {
		return nil, nil, false, err
	}
	if !access.AllowsCluster(run.ClusterID) {
		return nil, nil, false, ErrInspectionNotFound
	}
	var namespaces []string
	scoped := false
	if !access.All {
		if ca := access.Clusters[run.ClusterID]; !ca.AllNamespaces {
			scoped = true
			namespaces = make([]string, 0, len(ca.Namespaces))
			for ns := range ca.Namespaces {
				namespaces = append(namespaces, ns)
			}
		}
	}
	run, findings, err := uc.inspects.Get(ctx, id, namespaces, maxFindings)
	return run, findings, scoped, err
}

// RunInspection 立即执行一次巡检：插入待执行的记录，由负责该集群的数据面实例领取。
func (uc *GovernanceUsecase) RunInspection(ctx context.Context, caller Caller, clusterID string) (*InspectionRun, error) {
	c, _, err := uc.cluster(ctx, caller, clusterID)
	if err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	run := &InspectionRun{ID: id, ClusterID: c.ID, ClusterName: c.Name, Trigger: kc.InspectManual, Status: kc.InspectPending, CreateTime: uc.now()}
	detailJSON, _ := json.Marshal(map[string]any{"runId": id})
	audit := newAuditEntry(ctx, uc.dir, caller, kc.AuditResourceInspection, kc.ActionInspect, detailJSON)
	if err := uc.inspects.Create(ctx, run, caller.UserID, audit); err != nil {
		return nil, err
	}
	return run, nil
}

// ExecRecord 回放终端录制：返回按时间排序的事件；每次读取记一条审计。
func (uc *GovernanceUsecase) ExecRecord(ctx context.Context, caller Caller, sessionID string) ([]ExecRecordEvent, bool, error) {
	access, _, err := uc.access(ctx, caller.UserID)
	if err != nil {
		return nil, false, err
	}
	rec, err := uc.records.Recording(ctx, sessionID)
	if err != nil {
		return nil, false, err
	}
	// 录像里是别人在容器里看到的全部输出：只有数据范围覆盖该命名空间的人能回放（范围外按不存在处理）。
	if !access.AllowsObject(rec.Session.ClusterID, rec.Session.Namespace) {
		return nil, false, ErrExecRecordNotFound
	}
	events, err := DecodeRecording(rec.Chunks)
	if err != nil {
		return nil, false, err
	}
	detailJSON, _ := json.Marshal(map[string]any{
		"sessionId": sessionID, "sessionUser": rec.Session.Username, "namespace": rec.Session.Namespace,
		"pod": rec.Session.Pod, "container": rec.Session.Container,
	})
	audit := newAuditEntry(ctx, uc.dir, caller, kc.AuditResourceExecRecord, kc.ActionExecReplay, detailJSON)
	if err := uc.records.AuditReplay(ctx, audit, rec.Session); err != nil {
		return nil, false, err
	}
	return events, rec.Truncated, nil
}
