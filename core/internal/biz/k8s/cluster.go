package biz

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// registerTokenTTL 注册令牌有效期。
const registerTokenTTL = 24 * time.Hour

var (
	clusterNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	envPattern         = regexp.MustCompile(`^[a-z0-9_-]{0,32}$`)
)

// ClusterUsecase 集群登记用例。凭据只加密写入；测试连接转给数据面。
type ClusterUsecase struct {
	reader ClusterReader
	writer ClusterWriter
	tokens AgentTokenRepo
	sealer SecretSealer
	roles  RoleSource
	dir    Directory
	planes *PlaneUsecase
	agent  agentConfig
	// datasources 监控数据源校验；由 InsightUsecase 在装配期注入（nil = 不校验，仅单元测试）。
	datasources MetricsSource
	// templates 审批用工单模板的校验；由 ChangeUsecase 在装配期注入（nil = 不校验，仅单元测试）。
	templates ChangeTickets
}

// checkApprovalTemplate 审批模板须可用于变更审批；未改动时不重复校验（模板事后被停用不妨碍编辑集群的其他项）。
func (uc *ClusterUsecase) checkApprovalTemplate(ctx context.Context, id, cur string) error {
	if id == "" || id == cur || uc.templates == nil {
		return nil
	}
	return uc.templates.CheckTemplate(ctx, id)
}

// NewClusterUsecase new a ClusterUsecase.
func NewClusterUsecase(reader ClusterReader, writer ClusterWriter, tokens AgentTokenRepo, sealer SecretSealer,
	roles RoleSource, dir Directory, planes *PlaneUsecase, c *conf.K8S,
) *ClusterUsecase {
	return &ClusterUsecase{
		reader: reader, writer: writer, tokens: tokens, sealer: sealer, roles: roles, dir: dir, planes: planes,
		agent: agentConfig{ServerURL: strings.TrimSpace(c.GetAgent().GetServerUrl()), Image: strings.TrimSpace(c.GetAgent().GetImage())},
	}
}

// List 分页查询集群。
func (uc *ClusterUsecase) List(ctx context.Context, filter ClusterFilter, page, pageSize int64) ([]*Cluster, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	return uc.reader.List(ctx, filter, page, pageSize)
}

// Get 集群详情。
func (uc *ClusterUsecase) Get(ctx context.Context, id string) (*Cluster, error) {
	return uc.reader.Get(ctx, id)
}

// Options 调用者数据范围内的已启用集群。
func (uc *ClusterUsecase) Options(ctx context.Context, userID string) ([]*ClusterOption, error) {
	isAdmin, roleIDs, err := uc.roles.EnabledRoleIDs(ctx, userID)
	if err != nil {
		return nil, err
	}
	if !isAdmin && len(roleIDs) == 0 {
		return nil, nil
	}
	return uc.reader.Options(ctx, isAdmin, roleIDs)
}

// normalize 规整并校验集群内容本身（不含凭据）。错误说明不含证书内容。
func (in *ClusterInput) normalize() error {
	in.Name = strings.TrimSpace(in.Name)
	in.DisplayName = strings.TrimSpace(in.DisplayName)
	in.Env = strings.TrimSpace(in.Env)
	in.APIServer = strings.TrimSpace(in.APIServer)
	in.ProxyURL = strings.TrimSpace(in.ProxyURL)
	in.CACert = strings.TrimSpace(strings.ReplaceAll(in.CACert, "\r\n", "\n"))
	in.Description = strings.TrimSpace(in.Description)
	switch {
	case !clusterNamePattern.MatchString(in.Name):
		return detail(ErrClusterInvalid, "name must be 1-63 lowercase letters, digits or hyphens, and start and end with a letter or digit")
	case !envPattern.MatchString(in.Env):
		return detail(ErrClusterInvalid, "invalid env")
	}
	switch in.AccessMode {
	case kc.AccessAgent:
		if in.AgentLocation != kc.AgentLocationInCluster && in.AgentLocation != kc.AgentLocationRelay {
			return detail(ErrClusterInvalid, "agent location must be in-cluster (1) or relay node (2)")
		}
		if in.APIServer != "" || in.ProxyURL != "" || in.CACert != "" {
			return detail(ErrClusterInvalid, "api server, proxy and CA certificate only apply to direct access")
		}
	case kc.AccessDirect:
		if in.AgentLocation != kc.AgentLocationNone {
			return detail(ErrClusterInvalid, "agent location does not apply to direct access")
		}
		if err := checkAPIServer(in.APIServer); err != nil {
			return detail(ErrClusterInvalid, "api server: %s", err)
		}
		if err := checkProxyURL(in.ProxyURL); err != nil {
			return detail(ErrClusterInvalid, "proxy url: %s", err)
		}
		if in.CACert != "" {
			if err := checkCACert(in.CACert); err != nil {
				return detail(ErrClusterInvalid, "ca certificate: %s", err)
			}
			in.CACert += "\n"
		}
	default:
		return detail(ErrClusterInvalid, "access mode must be agent (1) or direct (2)")
	}
	return in.normalizeInsight()
}

// checkAPIServer API Server 地址：仅 https，不得带用户信息、查询串与片段。
func checkAPIServer(raw string) error {
	if raw == "" {
		return errors.New("must not be blank")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" {
		return errors.New("not a valid URL")
	}
	switch {
	case u.Scheme != "https":
		return errors.New("only https is allowed")
	case u.User != nil:
		return errors.New("must not embed credentials")
	case u.RawQuery != "" || u.Fragment != "" || u.ForceQuery:
		return errors.New("must not contain a query or fragment")
	}
	return nil
}

// checkProxyURL 代理地址：可空；http / https，不得带查询串与片段（代理账号走 userinfo 是惯例，允许）。
func checkProxyURL(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return errors.New("not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("only http and https proxies are supported")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return errors.New("must not contain a query or fragment")
	}
	return nil
}

// checkCACert CA 证书：一个或多个 PEM 证书块，每块都能解析。
func checkCACert(pemText string) error {
	rest := []byte(pemText)
	n := 0
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return errors.New("expect PEM blocks of type CERTIFICATE")
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return errors.New("not a valid X.509 certificate")
		}
		n++
	}
	if n == 0 || len(strings.TrimSpace(string(rest))) > 0 {
		return errors.New("not a valid PEM certificate bundle")
	}
	return nil
}

// normalizeCredential 规整并校验直连凭据：令牌，或客户端证书 + 私钥，二选一。错误说明不含凭据内容。
func normalizeCredential(in *CredentialInput) (kc.Credential, error) {
	token := strings.TrimSpace(in.Token)
	cert := strings.TrimSpace(strings.ReplaceAll(in.ClientCert, "\r\n", "\n"))
	key := strings.TrimSpace(strings.ReplaceAll(in.ClientKey, "\r\n", "\n"))
	switch {
	case token != "" && (cert != "" || key != ""):
		return kc.Credential{}, errors.New("provide either a token or a client certificate with its key, not both")
	case token != "":
		if strings.ContainsAny(token, " \t\r\n\x00") {
			return kc.Credential{}, errors.New("token must not contain whitespace")
		}
		return kc.Credential{Token: token}, nil
	case cert != "" && key != "":
		cert, key = cert+"\n", key+"\n"
		if _, err := tls.X509KeyPair([]byte(cert), []byte(key)); err != nil {
			return kc.Credential{}, errors.New("client certificate and key are not a valid PEM key pair")
		}
		return kc.Credential{ClientCert: cert, ClientKey: key}, nil
	case cert != "" || key != "":
		return kc.Credential{}, errors.New("client certificate and client key must be provided together")
	}
	return kc.Credential{}, errors.New("a token or a client certificate with its key is required")
}

// seal 校验并加密凭据。
func (uc *ClusterUsecase) seal(in *CredentialInput) ([]byte, error) {
	cred, err := normalizeCredential(in)
	if err != nil {
		return nil, detail(ErrClusterInvalid, "credential: %s", err)
	}
	// 明文只在这里序列化后立刻加密，不落日志、不出本函数。
	raw, err := json.Marshal(cred) //nolint:gosec // G117：凭据结构本就是要加密落库的内容，序列化结果直接交给 Seal
	if err != nil {
		return nil, err
	}
	return uc.sealer.Seal(string(raw))
}

// clusterAuditDetail 集群配置变更的审计内容：不含凭据与 CA 证书原文。
func clusterAuditDetail(in ClusterInput, credentialChanged bool) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{
		"name": in.Name, "displayName": in.DisplayName, "env": in.Env, "accessMode": in.AccessMode,
		"agentLocation": in.AgentLocation, "apiServer": in.APIServer, "hasProxy": in.ProxyURL != "", "hasCaCert": in.CACert != "",
		"execDisabled": in.ExecDisabled, "enabled": in.Enabled, "description": in.Description, "credentialChanged": credentialChanged,
		"datasourceId": in.DatasourceID, "eventReasons": in.EventReasons, "approvalTemplateId": in.ApprovalTemplateID,
	})
	return raw
}

// audit 构造一条 core 侧的审计；用户名取不到时留空（不阻断写操作）。
func (uc *ClusterUsecase) audit(ctx context.Context, caller Caller, resource, action string, detail json.RawMessage) *AuditEntry {
	return newAuditEntry(ctx, uc.dir, caller, resource, action, detail)
}

func newAuditEntry(ctx context.Context, dir Directory, caller Caller, resource, action string, detail json.RawMessage) *AuditEntry {
	e := &AuditEntry{UserID: caller.UserID, ClientIP: caller.ClientIP, Resource: resource, Action: action, Detail: detail}
	if names, err := dir.UserNames(ctx, []string{caller.UserID}); err == nil {
		e.Username = names[caller.UserID]
	}
	if id, err := idgen.NewID(); err == nil {
		e.RequestID = id
	}
	return e
}

func fromInput(id string, in ClusterInput) *Cluster {
	return &Cluster{
		ID: id, Name: in.Name, DisplayName: in.DisplayName, Env: in.Env, AccessMode: in.AccessMode, AgentLocation: in.AgentLocation,
		APIServer: in.APIServer, ProxyURL: in.ProxyURL, CACert: in.CACert, ExecDisabled: in.ExecDisabled,
		Description: in.Description, Enabled: in.Enabled, DatasourceID: in.DatasourceID, EventReasons: in.EventReasons,
		ApprovalTemplateID: in.ApprovalTemplateID,
	}
}

// Create 登记集群。直连必须带凭据；Agent 方式不接受凭据。
func (uc *ClusterUsecase) Create(ctx context.Context, caller Caller, in ClusterInput, cred *CredentialInput) (*Cluster, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}
	var sealed []byte
	switch {
	case in.AccessMode == kc.AccessAgent && !cred.empty():
		return nil, detail(ErrClusterInvalid, "credential only applies to direct access")
	case in.AccessMode == kc.AccessDirect:
		if cred.empty() {
			return nil, detail(ErrClusterInvalid, "credential: a token or a client certificate with its key is required")
		}
		var err error
		if sealed, err = uc.seal(cred); err != nil {
			return nil, err
		}
	}
	if err := uc.checkDatasource(ctx, in.DatasourceID, ""); err != nil {
		return nil, err
	}
	if err := uc.checkApprovalTemplate(ctx, in.ApprovalTemplateID, ""); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	audit := uc.audit(ctx, caller, kc.AuditResourceCluster, kc.ActionCreate, clusterAuditDetail(in, sealed != nil))
	if err := uc.writer.Create(ctx, fromInput(id, in), sealed, audit); err != nil {
		return nil, err
	}
	return uc.reader.Get(ctx, id)
}

// Update 编辑集群（整体替换）。名称与接入方式不可改；不传凭据沿用已保存的。
func (uc *ClusterUsecase) Update(ctx context.Context, caller Caller, id string, in ClusterInput, cred *CredentialInput, version int32) (*Cluster, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}
	cur, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	switch {
	case in.Name != cur.Name:
		return nil, detail(ErrClusterInvalid, "cluster name cannot be changed")
	case in.AccessMode != cur.AccessMode:
		return nil, detail(ErrClusterInvalid, "access mode cannot be changed")
	}
	in.ProxyURL = keepProxyUserinfo(in.ProxyURL, cur.ProxyURL)
	var sealed []byte
	if !cred.empty() {
		if in.AccessMode == kc.AccessAgent {
			return nil, detail(ErrClusterInvalid, "credential only applies to direct access")
		}
		if sealed, err = uc.seal(cred); err != nil {
			return nil, err
		}
	} else if cur.HasCredential && endpointChanged(cur, in) {
		// 已保存的凭据只属于已保存的地址：改地址 / 代理 / CA 而不重填凭据，等于把凭据发给新地址。
		return nil, detail(ErrClusterInvalid, "credential: re-enter the credential when the API server, proxy or CA changes")
	}
	if err := uc.checkDatasource(ctx, in.DatasourceID, cur.DatasourceID); err != nil {
		return nil, err
	}
	if err := uc.checkApprovalTemplate(ctx, in.ApprovalTemplateID, cur.ApprovalTemplateID); err != nil {
		return nil, err
	}
	audit := uc.audit(ctx, caller, kc.AuditResourceCluster, kc.ActionUpdate, clusterAuditDetail(in, sealed != nil))
	if err := uc.writer.Update(ctx, fromInput(id, in), sealed, version, audit); err != nil {
		return nil, err
	}
	return uc.reader.Get(ctx, id)
}

// Delete 移除集群。
func (uc *ClusterUsecase) Delete(ctx context.Context, caller Caller, id string) error {
	return uc.writer.Delete(ctx, id, uc.audit(ctx, caller, kc.AuditResourceCluster, kc.ActionDelete, nil))
}

// TestInput 测试连接的入参。
type TestInput struct {
	ClusterID  string
	APIServer  string
	ProxyURL   string
	CACert     string
	Credential *CredentialInput
}

// Test 测试直连配置：校验后转给数据面实际连接。未带凭据且指定了已保存的集群时沿用其凭据
// （由数据面读取并解密）。返回数据面的 JSON 响应体（kc.ClusterTestResponse）。
func (uc *ClusterUsecase) Test(ctx context.Context, caller Caller, in TestInput) ([]byte, error) {
	probe := ClusterInput{Name: "probe", AccessMode: kc.AccessDirect, APIServer: in.APIServer, ProxyURL: in.ProxyURL, CACert: in.CACert}
	if err := probe.normalize(); err != nil {
		return nil, err
	}
	var cur *Cluster
	if in.ClusterID != "" {
		var err error
		if cur, err = uc.reader.Get(ctx, in.ClusterID); err != nil {
			return nil, err
		}
		// 响应里的代理账号已打码：表单原样带回时换回已保存的。
		probe.ProxyURL = keepProxyUserinfo(probe.ProxyURL, cur.ProxyURL)
	}
	req := kc.ClusterTestRequest{APIServer: probe.APIServer, ProxyURL: probe.ProxyURL, CACert: probe.CACert}
	if in.Credential.empty() {
		if cur == nil {
			return nil, detail(ErrClusterInvalid, "credential: a token or a client certificate with its key is required")
		}
		if !cur.HasCredential {
			return nil, detail(ErrClusterInvalid, "this cluster has no saved credential")
		}
		if endpointChanged(cur, probe) {
			return nil, detail(ErrClusterInvalid, "credential: re-enter the credential to test a changed API server, proxy or CA")
		}
		req.ClusterID = cur.ID
	} else {
		cred, err := normalizeCredential(in.Credential)
		if err != nil {
			return nil, detail(ErrClusterInvalid, "credential: %s", err)
		}
		req.Credential = cred
	}
	return uc.planes.Call(ctx, caller, "", kc.OpClusterTest, req)
}

// InstallAgent 签发一次性的注册令牌并渲染安装物。之前未使用的注册令牌随即作废。
func (uc *ClusterUsecase) InstallAgent(ctx context.Context, caller Caller, id string) (*AgentInstall, error) {
	if err := uc.agent.check(); err != nil {
		return nil, err
	}
	cur, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.AccessMode != kc.AccessAgent {
		return nil, detail(ErrClusterInvalid, "this cluster uses direct access; agents only apply to agent access")
	}
	tokenID, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	token, hash, err := kc.NewToken(tokenID)
	if err != nil {
		return nil, err
	}
	expire := time.Now().Add(registerTokenTTL)
	audit := uc.audit(ctx, caller, kc.AuditResourceAgentToken, kc.ActionAgentInstall, nil)
	if err := uc.tokens.ReplaceRegister(ctx, id, tokenID, hash, expire, audit); err != nil {
		return nil, err
	}
	out := &AgentInstall{Location: cur.AgentLocation, ExpireTime: expire}
	if cur.AgentLocation == kc.AgentLocationRelay {
		out.Command, err = uc.agent.relayCommand(cur.Name, token)
	} else {
		out.Manifest, err = uc.agent.manifest(cur.Name, token)
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RevokeAgent 吊销该集群的全部 Agent 令牌。
func (uc *ClusterUsecase) RevokeAgent(ctx context.Context, caller Caller, id string) error {
	return uc.tokens.RevokeAll(ctx, id, uc.audit(ctx, caller, kc.AuditResourceAgentToken, kc.ActionAgentRevoke, nil))
}

// endpointChanged 报告连接地址（API Server / 代理 / CA）是否与已保存的不同。
func endpointChanged(cur *Cluster, in ClusterInput) bool {
	return strings.TrimRight(cur.APIServer, "/") != strings.TrimRight(in.APIServer, "/") ||
		cur.ProxyURL != in.ProxyURL || strings.TrimSpace(cur.CACert) != strings.TrimSpace(in.CACert)
}

// proxyUserMask 响应里代替代理账号的占位。
const proxyUserMask = "REDACTED"

// MaskProxyURL 把代理地址里的账号密码换成占位：代理账号是凭据，任何响应都不返回原文。
func MaskProxyURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	u.User = url.User(proxyUserMask)
	return u.String()
}

// keepProxyUserinfo 编辑时沿用已保存的代理账号：输入不带账号或带的是占位，且其余部分与已保存的一致，
// 则返回已保存的原文；否则按输入。
func keepProxyUserinfo(in, stored string) string {
	su, err := url.Parse(stored)
	if err != nil || su.User == nil || in == "" {
		return in
	}
	iu, err := url.Parse(in)
	if err != nil {
		return in
	}
	if iu.User != nil && iu.User.String() != proxyUserMask {
		return in
	}
	bare := *su
	bare.User, iu.User = nil, nil
	if bare.String() != iu.String() {
		return in
	}
	return stored
}
