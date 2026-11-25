package testfake

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// K8sStore 容器平台域的内存仓储（语义对齐 internal/data/k8s）：集群、Agent 令牌、角色数据范围、
// core 侧审计、终端票据、通知发件箱，以及数据面实例定位（k8splane.Resolver）。
type K8sStore struct {
	mu          sync.Mutex
	clusters    map[string]*biz.Cluster
	credentials map[string][]byte
	// tokens 集群 ID → 令牌类型 → 个数。
	tokens  map[string]map[int32]int
	scopes  map[string][]*biz.RoleScope
	audits  []*biz.Audit
	execs   []*biz.ExecSession
	tickets map[string]k8sTicket
	outbox  []*K8sOutboxRow
	// CfgVersions 配置版本（name → version）。
	cfg map[string]int64
	// owners 集群 ID → 数据面地址；anyAddr 任一实例地址。
	owners      map[string]string
	anyAddr     string
	invalidated []string
	now         func() time.Time
}

type k8sTicket struct {
	ticket biz.ExecTicket
	expire time.Time
}

// K8sOutboxRow 发件箱行及其派发状态（测试断言用）。
type K8sOutboxRow struct {
	Item      biz.OutboxItem
	Status    int32
	LastError string
}

// NewK8sStore new a K8sStore.
func NewK8sStore() *K8sStore {
	return &K8sStore{
		clusters: map[string]*biz.Cluster{}, credentials: map[string][]byte{}, tokens: map[string]map[int32]int{},
		scopes: map[string][]*biz.RoleScope{}, tickets: map[string]k8sTicket{}, cfg: map[string]int64{}, owners: map[string]string{},
		now: time.Now,
	}
}

// SetNow 替换时钟（票据过期测试用）。
func (s *K8sStore) SetNow(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

// SetOwner 登记负责某集群的数据面地址（空串 = 没有存活的负责实例）。
func (s *K8sStore) SetOwner(clusterID, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.owners[clusterID] = addr
}

// SetAny 登记任一存活实例的地址。
func (s *K8sStore) SetAny(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.anyAddr = addr
}

// Invalidated 被丢弃过归属缓存的集群 ID（按发生顺序）。
func (s *K8sStore) Invalidated() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.invalidated)
}

// CfgVersion 某项配置被 bump 的次数。
func (s *K8sStore) CfgVersion(name string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg[name]
}

// Credential 某集群落库的凭据密文（断言只写不读用）。
func (s *K8sStore) Credential(clusterID string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.credentials[clusterID])
}

// TokenCount 某集群某类令牌的个数。
func (s *K8sStore) TokenCount(clusterID string, kind int32) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokens[clusterID][kind]
}

// AddLongTermToken 模拟 Agent 注册后换发的长期令牌。
func (s *K8sStore) AddLongTermToken(clusterID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tokens[clusterID] == nil {
		s.tokens[clusterID] = map[int32]int{}
	}
	s.tokens[clusterID][kc.TokenLongTerm]++
}

// Audits 全部审计（拷贝），新的在前。
func (s *K8sStore) Audits() []*biz.Audit {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*biz.Audit, 0, len(s.audits))
	for i := len(s.audits) - 1; i >= 0; i-- {
		cp := *s.audits[i]
		out = append(out, &cp)
	}
	return out
}

// SeedAudit 预置一条数据面写入的审计。
func (s *K8sStore) SeedAudit(a *biz.Audit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *a
	s.audits = append(s.audits, &cp)
}

// SeedExecSession 预置一条终端会话记录。
func (s *K8sStore) SeedExecSession(e *biz.ExecSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *e
	s.execs = append(s.execs, &cp)
}

// SeedOutbox 预置一行通知发件箱。
func (s *K8sStore) SeedOutbox(item biz.OutboxItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outbox = append(s.outbox, &K8sOutboxRow{Item: item, Status: biz.OutboxPending})
}

// Outbox 全部发件箱行（拷贝）。
func (s *K8sStore) Outbox() []K8sOutboxRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]K8sOutboxRow, 0, len(s.outbox))
	for _, r := range s.outbox {
		out = append(out, *r)
	}
	return out
}

func (s *K8sStore) bump(names ...string) {
	for _, n := range names {
		s.cfg[n]++
	}
}

func (s *K8sStore) audit(a *biz.AuditEntry, clusterID, clusterName, name string) {
	if a == nil {
		return
	}
	id, _ := idgen.NewID()
	s.audits = append(s.audits, &biz.Audit{
		ID: id, CreateTime: s.now(), UserID: a.UserID, Username: a.Username, ClientIP: a.ClientIP, ClusterID: clusterID,
		ClusterName: clusterName, Resource: a.Resource, Name: name, Action: a.Action, Result: kc.AuditOK, Patch: a.Detail,
	})
}

func (s *K8sStore) view(c *biz.Cluster) *biz.Cluster {
	cp := *c
	cp.HasCredential = len(s.credentials[c.ID]) > 0
	cp.AgentTokens = int32(s.tokens[c.ID][kc.TokenLongTerm]) //nolint:gosec // 测试桩，小整数
	return &cp
}

// ---- biz.ClusterReader ----

// List implements biz.ClusterReader.
func (s *K8sStore) List(_ context.Context, f biz.ClusterFilter, page, pageSize int64) ([]*biz.Cluster, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*biz.Cluster
	for _, c := range s.clusters {
		if f.Keyword != "" && !strings.Contains(c.Name, f.Keyword) && !strings.Contains(c.DisplayName, f.Keyword) {
			continue
		}
		if f.Env != nil && c.Env != *f.Env {
			continue
		}
		if f.Status != nil && c.Runtime.Status != *f.Status {
			continue
		}
		all = append(all, s.view(c))
	}
	slices.SortFunc(all, func(a, b *biz.Cluster) int { return cmp.Or(cmp.Compare(a.Env, b.Env), cmp.Compare(a.Name, b.Name)) })
	total := int64(len(all))
	start := min((page-1)*pageSize, total)
	return all[start:min(start+pageSize, total)], total, nil
}

// Get implements biz.ClusterReader.
func (s *K8sStore) Get(_ context.Context, id string) (*biz.Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clusters[id]
	if !ok {
		return nil, biz.ErrClusterNotFound
	}
	return s.view(c), nil
}

// Options implements biz.ClusterReader.
func (s *K8sStore) Options(_ context.Context, all bool, roleIDs []string) ([]*biz.ClusterOption, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	levels := map[string]int32{}
	for _, roleID := range roleIDs {
		for _, sc := range s.scopes[roleID] {
			levels[sc.ClusterID] = max(levels[sc.ClusterID], sc.Level)
		}
	}
	var out []*biz.ClusterOption
	for _, c := range s.clusters {
		level := levels[c.ID]
		if all {
			level = kc.LevelClusterAdmin
		}
		if !c.Enabled || level == 0 {
			continue
		}
		out = append(out, &biz.ClusterOption{
			ID: c.ID, Name: c.Name, DisplayName: c.DisplayName, Env: c.Env, Status: c.Runtime.Status,
			LastError: c.Runtime.LastError, ExecDisabled: c.ExecDisabled, K8sVersion: c.Runtime.K8sVersion, Level: level,
		})
	}
	slices.SortFunc(out, func(a, b *biz.ClusterOption) int {
		return cmp.Or(cmp.Compare(a.Env, b.Env), cmp.Compare(a.Name, b.Name))
	})
	return out, nil
}

// ---- biz.ClusterWriter ----

// Create implements biz.ClusterWriter.
func (s *K8sStore) Create(_ context.Context, c *biz.Cluster, credential []byte, audit *biz.AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cur := range s.clusters {
		if cur.Name == c.Name {
			return biz.ErrClusterNameExists
		}
	}
	cp := *c
	cp.Version, cp.CreateTime, cp.UpdateTime = 1, s.now(), s.now()
	s.clusters[c.ID] = &cp
	if len(credential) > 0 {
		s.credentials[c.ID] = slices.Clone(credential)
	}
	s.bump(kc.CfgCluster)
	s.audit(audit, c.ID, c.Name, c.Name)
	return nil
}

// Update implements biz.ClusterWriter.
func (s *K8sStore) Update(_ context.Context, c *biz.Cluster, credential []byte, expectVersion int32, audit *biz.AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.clusters[c.ID]
	if !ok {
		return biz.ErrClusterNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	cur.DisplayName, cur.Env, cur.AgentLocation = c.DisplayName, c.Env, c.AgentLocation
	cur.APIServer, cur.ProxyURL, cur.CACert = c.APIServer, c.ProxyURL, c.CACert
	cur.ExecDisabled, cur.Description, cur.Enabled = c.ExecDisabled, c.Description, c.Enabled
	cur.Version++
	cur.UpdateTime = s.now()
	if len(credential) > 0 {
		s.credentials[c.ID] = slices.Clone(credential)
	}
	s.bump(kc.CfgCluster)
	s.audit(audit, cur.ID, cur.Name, cur.Name)
	return nil
}

// Delete implements biz.ClusterWriter.
func (s *K8sStore) Delete(_ context.Context, id string, audit *biz.AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.clusters[id]
	if !ok {
		return biz.ErrClusterNotFound
	}
	delete(s.clusters, id)
	delete(s.credentials, id)
	delete(s.tokens, id)
	for roleID, items := range s.scopes {
		s.scopes[roleID] = slices.DeleteFunc(items, func(it *biz.RoleScope) bool { return it.ClusterID == id })
	}
	s.bump(kc.CfgCluster, kc.CfgRoleScope, kc.CfgAgentToken)
	s.audit(audit, id, cur.Name, cur.Name)
	return nil
}

// ---- biz.AgentTokenRepo ----

// ReplaceRegister implements biz.AgentTokenRepo.
func (s *K8sStore) ReplaceRegister(_ context.Context, clusterID, _ string, _ []byte, _ time.Time, audit *biz.AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.clusters[clusterID]
	if !ok {
		return biz.ErrClusterNotFound
	}
	if s.tokens[clusterID] == nil {
		s.tokens[clusterID] = map[int32]int{}
	}
	s.tokens[clusterID][kc.TokenRegister] = 1
	s.bump(kc.CfgAgentToken)
	s.audit(audit, clusterID, cur.Name, cur.Name)
	return nil
}

// RevokeAll implements biz.AgentTokenRepo.
func (s *K8sStore) RevokeAll(_ context.Context, clusterID string, audit *biz.AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.clusters[clusterID]
	if !ok {
		return biz.ErrClusterNotFound
	}
	delete(s.tokens, clusterID)
	s.bump(kc.CfgAgentToken)
	s.audit(audit, clusterID, cur.Name, cur.Name)
	return nil
}

// ---- 分视图：方法名与集群读写冲突的仓储各用一个视图类型 ----

// K8sScopes biz.ScopeRepo 视图。
type K8sScopes struct{ s *K8sStore }

// Scopes 返回 biz.ScopeRepo 视图。
func (s *K8sStore) Scopes() K8sScopes { return K8sScopes{s} }

// Get implements biz.ScopeRepo.
func (v K8sScopes) Get(_ context.Context, roleID string) ([]*biz.RoleScope, error) {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*biz.RoleScope, 0, len(s.scopes[roleID]))
	for _, it := range s.scopes[roleID] {
		c, ok := s.clusters[it.ClusterID]
		if !ok {
			continue
		}
		cp := *it
		cp.ClusterName = c.Name
		out = append(out, &cp)
	}
	slices.SortFunc(out, func(a, b *biz.RoleScope) int {
		return cmp.Or(cmp.Compare(a.ClusterName, b.ClusterName), cmp.Compare(a.Namespace, b.Namespace))
	})
	return out, nil
}

// Replace implements biz.ScopeRepo.
func (v K8sScopes) Replace(_ context.Context, roleID string, items []*biz.RoleScope, audit *biz.AuditEntry) error {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	saved := make([]*biz.RoleScope, 0, len(items))
	for _, it := range items {
		if _, ok := s.clusters[it.ClusterID]; !ok {
			return biz.ScopeInvalid("some clusters do not exist or have been removed")
		}
		cp := *it
		saved = append(saved, &cp)
	}
	s.scopes[roleID] = saved
	s.bump(kc.CfgRoleScope)
	s.audit(audit, "", "", roleID)
	return nil
}

// Grant implements biz.ScopeRepo：与仓储一致——在同一把锁内读出、并入一条授权、整组写回。
func (v K8sScopes) Grant(_ context.Context, roleID string, item *biz.RoleScope, audit *biz.AuditEntry) error {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.clusters[item.ClusterID]; !ok {
		return biz.ScopeInvalid("some clusters do not exist or have been removed")
	}
	cur := make([]*biz.RoleScope, 0, len(s.scopes[roleID])+1)
	for _, it := range s.scopes[roleID] {
		cp := *it
		cur = append(cur, &cp)
	}
	s.scopes[roleID] = biz.MergeScope(cur, item.ClusterID, item.Namespace, item.Level)
	s.bump(kc.CfgRoleScope)
	s.audit(audit, "", "", roleID)
	return nil
}

// K8sAudits biz.AuditReader 视图。
type K8sAudits struct{ s *K8sStore }

// AuditReader 返回 biz.AuditReader 视图。
func (s *K8sStore) AuditReader() K8sAudits { return K8sAudits{s} }

// List implements biz.AuditReader.
func (v K8sAudits) List(_ context.Context, f biz.AuditFilter, page, pageSize int64) ([]*biz.Audit, int64, error) {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*biz.Audit
	for i := len(s.audits) - 1; i >= 0; i-- {
		a := s.audits[i]
		switch {
		case a.CreateTime.Before(f.Start) || a.CreateTime.After(f.End),
			f.ClusterID != "" && a.ClusterID != f.ClusterID,
			f.Namespace != "" && a.Namespace != f.Namespace,
			f.Resource != "" && a.Resource != f.Resource,
			f.Name != "" && a.Name != f.Name,
			f.Action != "" && a.Action != f.Action,
			f.UserID != "" && a.UserID != f.UserID,
			f.Result != nil && a.Result != *f.Result,
			f.Keyword != "" && !strings.Contains(a.Name, f.Keyword) && !strings.Contains(a.Username, f.Keyword):
			continue
		}
		cp := *a
		cp.BeforeObj, cp.AfterObj, cp.Patch = nil, nil, nil
		all = append(all, &cp)
	}
	total := int64(len(all))
	start := min((page-1)*pageSize, total)
	return all[start:min(start+pageSize, total)], total, nil
}

// Get implements biz.AuditReader.
func (v K8sAudits) Get(_ context.Context, id string) (*biz.Audit, error) {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.audits {
		if a.ID == id {
			cp := *a
			return &cp, nil
		}
	}
	return nil, biz.ErrAuditNotFound
}

// ListExecSessions implements biz.AuditReader.
func (v K8sAudits) ListExecSessions(_ context.Context, f biz.ExecSessionFilter, page, pageSize int64) ([]*biz.ExecSession, int64, error) {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*biz.ExecSession
	for i := len(s.execs) - 1; i >= 0; i-- {
		e := s.execs[i]
		switch {
		case e.StartTime.Before(f.Start) || e.StartTime.After(f.End),
			f.ClusterID != "" && e.ClusterID != f.ClusterID,
			f.UserID != "" && e.UserID != f.UserID,
			f.Keyword != "" && !strings.Contains(e.Pod, f.Keyword) && !strings.Contains(e.Username, f.Keyword):
			continue
		}
		cp := *e
		all = append(all, &cp)
	}
	total := int64(len(all))
	start := min((page-1)*pageSize, total)
	return all[start:min(start+pageSize, total)], total, nil
}

// K8sTickets biz.TicketRepo 视图。
type K8sTickets struct{ s *K8sStore }

// Tickets 返回 biz.TicketRepo 视图。
func (s *K8sStore) Tickets() K8sTickets { return K8sTickets{s} }

// Create implements biz.TicketRepo.
func (v K8sTickets) Create(_ context.Context, hash []byte, t biz.ExecTicket, ttl time.Duration) error {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tickets[string(hash)] = k8sTicket{ticket: t, expire: s.now().Add(ttl)}
	return nil
}

// Redeem implements biz.TicketRepo.
func (v K8sTickets) Redeem(_ context.Context, hash []byte) (*biz.ExecTicket, error) {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tickets[string(hash)]
	if !ok || !s.now().Before(t.expire) {
		return nil, biz.ErrTicketInvalid
	}
	delete(s.tickets, string(hash))
	return &t.ticket, nil
}

// K8sOutbox biz.OutboxRepo 视图。
type K8sOutbox struct{ s *K8sStore }

// OutboxRepo 返回 biz.OutboxRepo 视图。
func (s *K8sStore) OutboxRepo() K8sOutbox { return K8sOutbox{s} }

// ClaimTx implements biz.OutboxRepo.
func (v K8sOutbox) ClaimTx(_ context.Context, _ txrun.Tx, exclude []string) (*biz.OutboxItem, error) {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.outbox {
		if r.Status == biz.OutboxPending && !slices.Contains(exclude, r.Item.ID) {
			cp := r.Item
			return &cp, nil
		}
	}
	return nil, nil
}

// FinishTx implements biz.OutboxRepo.
func (v K8sOutbox) FinishTx(_ context.Context, _ txrun.Tx, id string, status int32, reason string) error {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.outbox {
		if r.Item.ID == id {
			r.Status, r.LastError = status, reason
		}
	}
	return nil
}

// Fail implements biz.OutboxRepo.
func (v K8sOutbox) Fail(_ context.Context, id, reason string, maxAttempts int32) (bool, error) {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.outbox {
		if r.Item.ID == id && r.Status == biz.OutboxPending {
			r.Item.Attempts++
			r.LastError = reason
			if r.Item.Attempts >= maxAttempts {
				r.Status = biz.OutboxDropped
				return true, nil
			}
		}
	}
	return false, nil
}

// K8sResolver k8splane.Resolver 视图（不带缓存）。
type K8sResolver struct{ s *K8sStore }

// Resolver 返回 k8splane.Resolver 视图。
func (s *K8sStore) Resolver() K8sResolver { return K8sResolver{s} }

// Owner implements k8splane.Resolver.
func (v K8sResolver) Owner(_ context.Context, clusterID string) (string, error) {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clusters[clusterID]
	if !ok || !c.Enabled {
		return "", biz.ErrClusterNotFound
	}
	if s.owners[clusterID] == "" {
		return "", biz.PlaneUnavailable("no live data plane instance owns this cluster yet")
	}
	return s.owners[clusterID], nil
}

// Any implements k8splane.Resolver.
func (v K8sResolver) Any(context.Context) (string, error) {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.anyAddr == "" {
		return "", biz.ErrPlaneUnavailable
	}
	return s.anyAddr, nil
}

// Invalidate implements k8splane.Resolver.
func (v K8sResolver) Invalidate(clusterID string) {
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalidated = append(s.invalidated, clusterID)
}

// K8sSealer 可检查的假加密器：密文 = "sealed:" + 明文。
type K8sSealer struct {
	// Unavailable 模拟主密钥未配置。
	Unavailable bool
}

// Seal implements biz.SecretSealer.
func (s *K8sSealer) Seal(plaintext string) ([]byte, error) {
	if s.Unavailable {
		return nil, biz.ErrSecretUnavailable
	}
	return []byte("sealed:" + plaintext), nil
}
