package testfake

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// JobStore 作业中心域的内存仓储，语义对齐 data 层（名称唯一、乐观锁、引用保护、幂等键、模板互斥位、
// 取消的状态分支）。执行引擎不在 core 内：测试用 Engine* 方法模拟它对执行记录的推进。
type JobStore struct {
	mu          sync.Mutex
	now         func() time.Time
	credentials map[string]*jobCredential
	projects    map[string]*jobProject
	templates   map[string]*biz.Template
	runners     map[string]*jobRunner
	executions  map[string]*biz.Execution
	hosts       map[string][]*biz.ExecutionHost
	logs        map[string][]string
	payloads    map[string][]byte
	scripts     map[string]*biz.Script
	rules       map[string]*biz.RiskRule
	policy      biz.Policy
	seq         int
}

type jobCredential struct {
	biz.Credential
	secret, becomeSecret []byte
}

type jobProject struct {
	biz.Project
	secret []byte
}

type jobRunner struct {
	biz.Runner
	tokenHash []byte
}

// NewJobStore new a JobStore.
func NewJobStore() *JobStore {
	return &JobStore{
		now:         time.Now,
		credentials: map[string]*jobCredential{},
		projects:    map[string]*jobProject{},
		templates:   map[string]*biz.Template{},
		runners:     map[string]*jobRunner{},
		executions:  map[string]*biz.Execution{},
		hosts:       map[string][]*biz.ExecutionHost{},
		logs:        map[string][]string{},
		payloads:    map[string][]byte{},
		scripts:     map[string]*biz.Script{},
		rules:       map[string]*biz.RiskRule{},
		policy:      biz.Policy{Version: 1},
	}
}

// JobSealer 作业凭据加密的测试替身：密文 = "sealed:" + 明文，便于断言「落库的是密文、接口不回显」。
type JobSealer struct {
	// Disabled 模拟主密钥未配置。
	Disabled bool
}

// Seal implements biz.SecretSealer.
func (s *JobSealer) Seal(plaintext string) ([]byte, error) {
	if s.Disabled {
		return nil, biz.ErrSecretUnavailable
	}
	return []byte("sealed:" + plaintext), nil
}

func jobPage[T any](items []T, page, pageSize int64) ([]T, int64) {
	total := int64(len(items))
	start := min((page-1)*pageSize, total)
	return items[start:min(start+pageSize, total)], total
}

func jobContains(keyword string, fields ...string) bool {
	if keyword == "" {
		return true
	}
	k := strings.ToLower(keyword)
	for _, f := range fields {
		if strings.Contains(strings.ToLower(f), k) {
			return true
		}
	}
	return false
}

// CredentialSecret 返回凭据落库的密文（主密文、提权密文），供测试断言。
func (s *JobStore) CredentialSecret(id string) (secret, becomeSecret []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.credentials[id]; c != nil {
		return slices.Clone(c.secret), slices.Clone(c.becomeSecret)
	}
	return nil, nil
}

// RunnerTokenHash 返回执行节点落库的令牌摘要，供测试断言。
func (s *JobStore) RunnerTokenHash(id string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.runners[id]; r != nil {
		return slices.Clone(r.tokenHash)
	}
	return nil
}

// EngineClaim 模拟引擎派发：排队中 → 执行中。
func (s *JobStore) EngineClaim(id, runnerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.executions[id]; e != nil && e.Status == jc.StatusQueued {
		now := s.now()
		e.Status, e.RunnerID, e.StartTime = jc.StatusRunning, runnerID, &now
	}
}

// EngineLog 模拟引擎写入日志块；content 为空表示日志已截断后的块（只推进序号）。
func (s *JobStore) EngineLog(id string, chunks ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.executions[id]
	if e == nil {
		return
	}
	for _, c := range chunks {
		s.logs[id] = append(s.logs[id], c)
		e.LogSeq++
		if c == "" {
			e.LogTruncated = true
		}
	}
}

// EngineLose 模拟引擎的租约清扫：执行中 → 结果未知（节点失联）。
func (s *JobStore) EngineLose(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.executions[id]; e != nil && e.Status == jc.StatusRunning {
		now := s.now()
		e.Status, e.FinishTime, e.Error = jc.StatusUnknown, &now, "runner lost"
	}
}

// EngineFinish 模拟引擎收到结束上报：写主机结果并推出终态。
func (s *JobStore) EngineFinish(id, outcome string, exitCode int32, results []jc.HostResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.executions[id]
	if e == nil {
		return
	}
	byAddr := map[string]jc.HostResult{}
	for _, r := range results {
		byAddr[r.Address] = r
	}
	e.HostOK, e.HostFailed, e.HostUnreachable = 0, 0, 0
	for _, h := range s.hosts[id] {
		r, ok := byAddr[h.Address]
		if !ok {
			continue
		}
		h.OK, h.Changed, h.Failed, h.Unreachable, h.Skipped, h.Status = r.OK, r.Changed, r.Failed, r.Unreachable, r.Skipped, r.Status()
		switch h.Status {
		case jc.HostOK, jc.HostChanged:
			e.HostOK++
		case jc.HostFailed:
			e.HostFailed++
		case jc.HostUnreachable:
			e.HostUnreachable++
		}
	}
	now := s.now()
	e.Status, e.FinishTime = jc.FinalStatus(outcome, exitCode, results), &now
}

// ---------------------------------------------------------------------------
// 凭据
// ---------------------------------------------------------------------------

type jobCredentialView struct{ s *JobStore }

// Credentials 凭据仓储视图。
func (s *JobStore) Credentials() interface {
	biz.CredentialReader
	biz.CredentialWriter
} {
	return jobCredentialView{s}
}

func (v jobCredentialView) view(c *jobCredential) *biz.Credential {
	out := c.Credential
	out.HasBecomeSecret = len(c.becomeSecret) > 0
	for _, t := range v.s.templates {
		if t.CredentialID == c.ID {
			out.TemplateCount++
		}
	}
	return &out
}

func (v jobCredentialView) sorted() []*jobCredential {
	out := make([]*jobCredential, 0, len(v.s.credentials))
	for _, c := range v.s.credentials {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	return out
}

func (v jobCredentialView) List(_ context.Context, f biz.CredentialFilter, page, pageSize int64) ([]*biz.Credential, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.Credential
	for _, c := range v.sorted() {
		if jobContains(f.Keyword, c.Name, c.Username, c.Description) && (f.Type == nil || *f.Type == c.Type) {
			items = append(items, v.view(c))
		}
	}
	out, total := jobPage(items, page, pageSize)
	return out, total, nil
}

func (v jobCredentialView) Get(_ context.Context, id string) (*biz.Credential, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	c := v.s.credentials[id]
	if c == nil {
		return nil, biz.ErrCredentialNotFound
	}
	return v.view(c), nil
}

func (v jobCredentialView) ListOptions(context.Context) ([]*biz.Credential, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.Credential
	for _, c := range v.sorted() {
		items = append(items, v.view(c))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items, nil
}

func (v jobCredentialView) nameTaken(name, exceptID string) bool {
	for _, c := range v.s.credentials {
		if c.Name == name && c.ID != exceptID {
			return true
		}
	}
	return false
}

func (v jobCredentialView) Create(_ context.Context, c *biz.Credential, secret, becomeSecret []byte, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.nameTaken(c.Name, "") {
		return biz.ErrCredentialNameExists
	}
	row := &jobCredential{Credential: *c, secret: secret, becomeSecret: becomeSecret}
	row.Version, row.CreateTime, row.UpdateTime, row.UpdateUser = 1, v.s.now(), v.s.now(), actor
	v.s.credentials[c.ID] = row
	return nil
}

func (v jobCredentialView) Update(_ context.Context, c *biz.Credential, secret, becomeSecret biz.SecretChange, expectVersion int32, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur := v.s.credentials[c.ID]
	switch {
	case cur == nil:
		return biz.ErrCredentialNotFound
	case cur.Version != expectVersion:
		return biz.ErrVersionConflict
	case v.nameTaken(c.Name, c.ID):
		return biz.ErrCredentialNameExists
	}
	created := cur.CreateTime
	cur.Credential = *c
	cur.Version, cur.CreateTime, cur.UpdateTime, cur.UpdateUser = expectVersion+1, created, v.s.now(), actor
	if secret.Set {
		cur.secret = secret.Sealed
	}
	if becomeSecret.Set {
		cur.becomeSecret = becomeSecret.Sealed
	}
	return nil
}

func (v jobCredentialView) Delete(_ context.Context, id, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.s.credentials[id] == nil {
		return biz.ErrCredentialNotFound
	}
	if names := v.s.templatesUsing(func(t *biz.Template) bool { return t.CredentialID == id }); len(names) > 0 {
		return biz.InUse(biz.ErrCredentialInUse, names, len(names))
	}
	delete(v.s.credentials, id)
	return nil
}

func (s *JobStore) templatesUsing(match func(*biz.Template) bool) []string {
	var names []string
	for _, t := range s.templates {
		if match(t) {
			names = append(names, t.Name)
		}
	}
	sort.Strings(names)
	return names
}

// ---------------------------------------------------------------------------
// 项目
// ---------------------------------------------------------------------------

type jobProjectView struct{ s *JobStore }

// Projects 项目仓储视图。
func (s *JobStore) Projects() interface {
	biz.ProjectReader
	biz.ProjectWriter
} {
	return jobProjectView{s}
}

func (v jobProjectView) view(p *jobProject) *biz.Project {
	out := p.Project
	out.HasSecret = len(p.secret) > 0
	for _, t := range v.s.templates {
		if t.ProjectID == p.ID {
			out.TemplateCount++
		}
	}
	return &out
}

func (v jobProjectView) all() []*biz.Project {
	out := make([]*biz.Project, 0, len(v.s.projects))
	for _, p := range v.s.projects {
		out = append(out, v.view(p))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	return out
}

func (v jobProjectView) List(_ context.Context, keyword string, page, pageSize int64) ([]*biz.Project, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.Project
	for _, p := range v.all() {
		if jobContains(keyword, p.Name, p.RepoURL, p.Description) {
			items = append(items, p)
		}
	}
	out, total := jobPage(items, page, pageSize)
	return out, total, nil
}

func (v jobProjectView) Get(_ context.Context, id string) (*biz.Project, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	p := v.s.projects[id]
	if p == nil {
		return nil, biz.ErrProjectNotFound
	}
	return v.view(p), nil
}

func (v jobProjectView) ListOptions(context.Context) ([]*biz.Project, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	items := v.all()
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items, nil
}

func (v jobProjectView) nameTaken(name, exceptID string) bool {
	for _, p := range v.s.projects {
		if p.Name == name && p.ID != exceptID {
			return true
		}
	}
	return false
}

func (v jobProjectView) Create(_ context.Context, p *biz.Project, secret []byte, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.nameTaken(p.Name, "") {
		return biz.ErrProjectNameExists
	}
	row := &jobProject{Project: *p, secret: secret}
	row.Version, row.CreateTime, row.UpdateTime, row.UpdateUser = 1, v.s.now(), v.s.now(), actor
	v.s.projects[p.ID] = row
	return nil
}

func (v jobProjectView) Update(_ context.Context, p *biz.Project, secret biz.SecretChange, expectVersion int32, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur := v.s.projects[p.ID]
	switch {
	case cur == nil:
		return biz.ErrProjectNotFound
	case cur.Version != expectVersion:
		return biz.ErrVersionConflict
	case v.nameTaken(p.Name, p.ID):
		return biz.ErrProjectNameExists
	}
	created := cur.CreateTime
	cur.Project = *p
	cur.Version, cur.CreateTime, cur.UpdateTime, cur.UpdateUser = expectVersion+1, created, v.s.now(), actor
	if secret.Set {
		cur.secret = secret.Sealed
	}
	return nil
}

func (v jobProjectView) Delete(_ context.Context, id, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.s.projects[id] == nil {
		return biz.ErrProjectNotFound
	}
	if names := v.s.templatesUsing(func(t *biz.Template) bool { return t.ProjectID == id }); len(names) > 0 {
		return biz.InUse(biz.ErrProjectInUse, names, len(names))
	}
	delete(v.s.projects, id)
	return nil
}

// ---------------------------------------------------------------------------
// 作业模板
// ---------------------------------------------------------------------------

type jobTemplateView struct{ s *JobStore }

// Templates 作业模板仓储视图。
func (s *JobStore) Templates() interface {
	biz.TemplateReader
	biz.TemplateWriter
} {
	return jobTemplateView{s}
}

func (v jobTemplateView) view(t *biz.Template) *biz.Template {
	out := *t
	out.Params, out.Target.CIIDs = slices.Clone(t.Params), slices.Clone(t.Target.CIIDs)
	if p := v.s.projects[t.ProjectID]; p != nil {
		out.ProjectName, out.ProjectRef = p.Name, p.Ref
	}
	if c := v.s.credentials[t.CredentialID]; c != nil {
		out.CredentialName = c.Name
	}
	return &out
}

func (v jobTemplateView) List(_ context.Context, f biz.TemplateFilter, page, pageSize int64) ([]*biz.Template, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.Template
	for _, t := range v.s.templates {
		if jobContains(f.Keyword, t.Name, t.Playbook, t.Description) && (f.ProjectID == "" || f.ProjectID == t.ProjectID) &&
			(f.Enabled == nil || *f.Enabled == t.Enabled) {
			items = append(items, v.view(t))
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID > items[j].ID })
	out, total := jobPage(items, page, pageSize)
	return out, total, nil
}

func (v jobTemplateView) Get(_ context.Context, id string) (*biz.Template, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	t := v.s.templates[id]
	if t == nil {
		return nil, biz.ErrTemplateNotFound
	}
	return v.view(t), nil
}

func (v jobTemplateView) check(t *biz.Template) error {
	if v.s.projects[t.ProjectID] == nil {
		return biz.TemplateInvalid("the selected project no longer exists")
	}
	if v.s.credentials[t.CredentialID] == nil {
		return biz.TemplateInvalid("the selected credential no longer exists")
	}
	for _, other := range v.s.templates {
		if other.Name == t.Name && other.ID != t.ID {
			return biz.ErrTemplateNameExists
		}
	}
	return nil
}

func (v jobTemplateView) Create(_ context.Context, t *biz.Template, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(t); err != nil {
		return err
	}
	row := *t
	row.Version, row.CreateTime, row.UpdateTime, row.UpdateUser = 1, v.s.now(), v.s.now(), actor
	v.s.templates[t.ID] = &row
	return nil
}

func (v jobTemplateView) Update(_ context.Context, t *biz.Template, expectVersion int32, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur := v.s.templates[t.ID]
	if cur == nil {
		return biz.ErrTemplateNotFound
	}
	if err := v.check(t); err != nil {
		return err
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	row := *t
	row.Version, row.CreateTime, row.UpdateTime, row.UpdateUser = expectVersion+1, cur.CreateTime, v.s.now(), actor
	v.s.templates[t.ID] = &row
	return nil
}

func (v jobTemplateView) Delete(_ context.Context, id, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.s.templates[id] == nil {
		return biz.ErrTemplateNotFound
	}
	delete(v.s.templates, id)
	return nil
}

// ---------------------------------------------------------------------------
// 执行记录
// ---------------------------------------------------------------------------

type jobExecutionView struct{ s *JobStore }

// Executions 执行记录仓储视图。
func (s *JobStore) Executions() interface {
	biz.ExecutionReader
	biz.ExecutionLogReader
	biz.ExecutionWriter
} {
	return jobExecutionView{s}
}

func (v jobExecutionView) CreateTx(_ context.Context, _ biz.Tx, items []biz.NewExecution) (string, bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	head := items[0].Execution
	for _, other := range v.s.executions {
		if head.RequestID != "" && other.Creator == head.Creator && other.RequestID == head.RequestID {
			return other.ID, false, nil
		}
	}
	if head.Exclusive {
		for _, other := range v.s.executions {
			if other.TemplateID == head.TemplateID && other.Exclusive && jc.Active(other.Status) {
				return "", false, biz.ErrExecutionConflict
			}
		}
	}
	for _, it := range items {
		row := *it.Execution
		row.CreateTime, row.HostTotal = v.s.now(), int32(len(it.Hosts)) //nolint:gosec // 测试数据，主机数有界
		v.s.seq++
		row.CreateTime = row.CreateTime.Add(time.Duration(v.s.seq) * time.Microsecond)
		v.s.executions[row.ID] = &row
		v.s.payloads[row.ID] = it.Payload
		for i, h := range it.Hosts {
			v.s.hosts[row.ID] = append(v.s.hosts[row.ID], &biz.ExecutionHost{
				Seq: int32(i + 1), CIID: h.CIID, Name: h.Name, Env: h.Env, Address: h.Address, Port: row.Spec.Options.Port,
			})
		}
	}
	return head.ID, true, nil
}

func (v jobExecutionView) inScope(e *biz.Execution, businessID string) bool {
	return e.ID == businessID || (e.GroupID != "" && e.GroupID == businessID)
}

func (v jobExecutionView) SetApprovalTx(_ context.Context, _ biz.Tx, businessID, instanceID string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	for _, e := range v.s.executions {
		if v.inScope(e, businessID) {
			e.ApprovalInstanceID = instanceID
		}
	}
	return nil
}

func (v jobExecutionView) ResolveApprovalTx(_ context.Context, _ biz.Tx, businessID string, approved bool, reason string) (int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var n int64
	for _, e := range v.s.executions {
		if !v.inScope(e, businessID) || e.Status != jc.StatusPending {
			continue
		}
		n++
		if approved {
			e.Status = jc.StatusQueued
			continue
		}
		now := v.s.now()
		e.Status, e.FinishTime, e.Error = jc.StatusRejected, &now, reason
	}
	return n, nil
}

func (v jobExecutionView) List(_ context.Context, f biz.ExecutionFilter, page, pageSize int64) ([]*biz.Execution, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.Execution
	for _, e := range v.s.executions {
		switch {
		case f.TemplateID != "" && f.TemplateID != e.TemplateID,
			len(f.Statuses) > 0 && !slices.Contains(f.Statuses, e.Status),
			f.Creator != "" && f.Creator != e.Creator,
			!jobContains(f.Keyword, e.TemplateName),
			f.StartTime != nil && e.CreateTime.Before(*f.StartTime),
			f.EndTime != nil && !e.CreateTime.Before(*f.EndTime),
			len(f.Kinds) > 0 && !slices.Contains(f.Kinds, e.Kind),
			len(f.Triggers) > 0 && !slices.Contains(f.Triggers, e.Trigger),
			f.GroupID != "" && f.GroupID != e.GroupID,
			f.TriggerRef != "" && f.TriggerRef != e.TriggerRef,
			f.CIID != "" && !slices.ContainsFunc(v.s.hosts[e.ID], func(h *biz.ExecutionHost) bool { return h.CIID == f.CIID }):
			continue
		}
		c := *e
		items = append(items, &c)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreateTime.After(items[j].CreateTime) })
	out, total := jobPage(items, page, pageSize)
	return out, total, nil
}

func (v jobExecutionView) Get(_ context.Context, id string) (*biz.Execution, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e := v.s.executions[id]
	if e == nil {
		return nil, biz.ErrExecutionNotFound
	}
	c := *e
	return &c, nil
}

func (v jobExecutionView) ListHosts(_ context.Context, id string, f biz.ExecutionHostFilter, page, pageSize int64) ([]*biz.ExecutionHost, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.ExecutionHost
	for _, h := range v.s.hosts[id] {
		if (len(f.Statuses) == 0 || slices.Contains(f.Statuses, h.Status)) && jobContains(f.Keyword, h.Name, h.Address) {
			c := *h
			items = append(items, &c)
		}
	}
	out, total := jobPage(items, page, pageSize)
	return out, total, nil
}

func (v jobExecutionView) ReadLog(_ context.Context, id string, afterSeq int64, maxBytes int) (string, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var b strings.Builder
	var last int64
	for i, chunk := range v.s.logs[id] {
		seq := int64(i + 1)
		if seq <= afterSeq || chunk == "" {
			continue
		}
		if b.Len() >= maxBytes {
			break
		}
		b.WriteString(chunk)
		last = seq
	}
	return b.String(), last, nil
}

func (v jobExecutionView) Cancel(_ context.Context, id, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e := v.s.executions[id]
	switch {
	case e == nil:
		return biz.ErrExecutionNotFound
	case e.Status == jc.StatusPending:
		now := v.s.now()
		for _, other := range v.s.executions {
			if other.Status == jc.StatusPending && (other.ID == e.ID || (e.GroupID != "" && other.GroupID == e.GroupID)) {
				other.Status, other.CancelRequested, other.CancelUser, other.FinishTime = jc.StatusCanceled, true, actor, &now
			}
		}
	case e.Status == jc.StatusQueued:
		now := v.s.now()
		e.Status, e.CancelRequested, e.CancelUser, e.FinishTime = jc.StatusCanceled, true, actor, &now
	case (e.Status == jc.StatusRunning || e.Status == jc.StatusUnknown) && !e.CancelRequested:
		e.CancelRequested, e.CancelUser = true, actor
	case e.Status == jc.StatusRunning || e.Status == jc.StatusUnknown:
	default:
		return biz.ErrExecutionFinished
	}
	return nil
}

// ---------------------------------------------------------------------------
// 执行节点
// ---------------------------------------------------------------------------

type jobRunnerView struct{ s *JobStore }

// Runners 执行节点仓储视图。
func (s *JobStore) Runners() interface {
	biz.RunnerReader
	biz.RunnerWriter
	biz.RunnerTokens
} {
	return jobRunnerView{s}
}

func (v jobRunnerView) List(_ context.Context, keyword string, page, pageSize int64) ([]*biz.Runner, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.Runner
	for _, r := range v.s.runners {
		if jobContains(keyword, r.Name, r.Zone, r.Hostname) {
			c := r.Runner
			items = append(items, &c)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID > items[j].ID })
	out, total := jobPage(items, page, pageSize)
	return out, total, nil
}

func (v jobRunnerView) Get(_ context.Context, id string) (*biz.Runner, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	r := v.s.runners[id]
	if r == nil {
		return nil, biz.ErrRunnerNotFound
	}
	c := r.Runner
	return &c, nil
}

func (v jobRunnerView) Zones(context.Context) ([]string, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	zones := []string{}
	for _, r := range v.s.runners {
		if r.Zone != "" && !slices.Contains(zones, r.Zone) {
			zones = append(zones, r.Zone)
		}
	}
	sort.Strings(zones)
	return zones, nil
}

func (v jobRunnerView) nameTaken(name, exceptID string) bool {
	for _, r := range v.s.runners {
		if r.Name == name && r.ID != exceptID {
			return true
		}
	}
	return false
}

func (v jobRunnerView) Create(_ context.Context, r *biz.Runner, tokenHash []byte, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.nameTaken(r.Name, "") {
		return biz.ErrRunnerNameExists
	}
	row := &jobRunner{Runner: *r, tokenHash: tokenHash}
	row.Version, row.CreateTime, row.UpdateTime = 1, v.s.now(), v.s.now()
	v.s.runners[r.ID] = row
	return nil
}

func (v jobRunnerView) Update(_ context.Context, r *biz.Runner, expectVersion int32, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur := v.s.runners[r.ID]
	switch {
	case cur == nil:
		return biz.ErrRunnerNotFound
	case cur.Version != expectVersion:
		return biz.ErrVersionConflict
	case v.nameTaken(r.Name, r.ID):
		return biz.ErrRunnerNameExists
	}
	cur.Name, cur.Zone, cur.Enabled, cur.Description = r.Name, r.Zone, r.Enabled, r.Description
	cur.Version, cur.UpdateTime = expectVersion+1, v.s.now()
	return nil
}

func (v jobRunnerView) Delete(_ context.Context, id, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.s.runners[id] == nil {
		return biz.ErrRunnerNotFound
	}
	delete(v.s.runners, id)
	return nil
}

func (v jobRunnerView) SetToken(_ context.Context, id string, tokenHash []byte, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	r := v.s.runners[id]
	if r == nil {
		return biz.ErrRunnerNotFound
	}
	r.tokenHash = tokenHash
	r.Version++
	return nil
}

func (v jobRunnerView) Names(_ context.Context, ids []string) (map[string]string, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := map[string]string{}
	for _, id := range ids {
		if r := v.s.runners[id]; r != nil {
			out[id] = r.Name
		}
	}
	return out, nil
}

// Payload 返回执行固化的文件内容，供测试断言。
func (s *JobStore) Payload(executionID string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.payloads[executionID])
}

// ---------------------------------------------------------------------------
// 脚本库 / 高危规则 / 全局策略
// ---------------------------------------------------------------------------

type jobScriptView struct{ s *JobStore }

// Scripts 脚本库仓储视图。
func (s *JobStore) Scripts() biz.ScriptRepo { return jobScriptView{s} }

func (v jobScriptView) List(_ context.Context, f biz.ScriptFilter, page, pageSize int64) ([]*biz.Script, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.Script
	for _, sc := range v.s.scripts {
		if jobContains(f.Keyword, sc.Name, sc.Description) && (f.Interpreter == "" || f.Interpreter == sc.Interpreter) {
			c := *sc
			c.Content = ""
			items = append(items, &c)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID > items[j].ID })
	out, total := jobPage(items, page, pageSize)
	return out, total, nil
}

func (v jobScriptView) Get(_ context.Context, id string) (*biz.Script, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	sc := v.s.scripts[id]
	if sc == nil {
		return nil, biz.ErrScriptNotFound
	}
	c := *sc
	return &c, nil
}

func (v jobScriptView) Write(_ context.Context, op biz.WriteOp, sc *biz.Script, expectVersion int32, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur := v.s.scripts[sc.ID]
	if op != biz.OpCreate && cur == nil {
		return biz.ErrScriptNotFound
	}
	if op == biz.OpDelete {
		delete(v.s.scripts, sc.ID)
		return nil
	}
	for _, other := range v.s.scripts {
		if other.Name == sc.Name && other.ID != sc.ID {
			return biz.ErrScriptNameExists
		}
	}
	row := *sc
	row.Size, row.UpdateTime, row.UpdateUser, row.Version, row.CreateTime = int32(len(sc.Content)), v.s.now(), actor, 1, v.s.now() //nolint:gosec // 测试数据
	if op == biz.OpUpdate {
		if cur.Version != expectVersion {
			return biz.ErrVersionConflict
		}
		row.Version, row.CreateTime = expectVersion+1, cur.CreateTime
	}
	v.s.scripts[sc.ID] = &row
	return nil
}

type jobRiskRuleView struct{ s *JobStore }

// RiskRules 高危命令规则仓储视图。
func (s *JobStore) RiskRules() biz.RiskRuleRepo { return jobRiskRuleView{s} }

func (v jobRiskRuleView) List(context.Context) ([]*biz.RiskRule, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	items := make([]*biz.RiskRule, 0, len(v.s.rules))
	for _, r := range v.s.rules {
		c := *r
		items = append(items, &c)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Action != items[j].Action {
			return items[i].Action < items[j].Action
		}
		return items[i].Name < items[j].Name
	})
	return items, nil
}

func (v jobRiskRuleView) Get(_ context.Context, id string) (*biz.RiskRule, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	r := v.s.rules[id]
	if r == nil {
		return nil, biz.ErrRiskRuleNotFound
	}
	c := *r
	return &c, nil
}

func (v jobRiskRuleView) Write(_ context.Context, op biz.WriteOp, r *biz.RiskRule, expectVersion int32, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur := v.s.rules[r.ID]
	if op != biz.OpCreate && cur == nil {
		return biz.ErrRiskRuleNotFound
	}
	if op == biz.OpDelete {
		delete(v.s.rules, r.ID)
		return nil
	}
	for _, other := range v.s.rules {
		if other.Name == r.Name && other.ID != r.ID {
			return biz.ErrRiskRuleNameExists
		}
	}
	row := *r
	row.UpdateTime, row.UpdateUser, row.Version = v.s.now(), actor, 1
	if op == biz.OpUpdate {
		if cur.Version != expectVersion {
			return biz.ErrVersionConflict
		}
		row.Version = expectVersion + 1
	}
	v.s.rules[r.ID] = &row
	return nil
}

type jobPolicyView struct{ s *JobStore }

// Policies 全局策略仓储视图。
func (s *JobStore) Policies() biz.PolicyRepo { return jobPolicyView{s} }

func (v jobPolicyView) Get(context.Context) (*biz.Policy, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	p := v.s.policy
	p.ProtectedEnvs = slices.Clone(p.ProtectedEnvs)
	return &p, nil
}

func (v jobPolicyView) Update(_ context.Context, p *biz.Policy, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if p.Version != v.s.policy.Version {
		return biz.ErrVersionConflict
	}
	v.s.policy = biz.Policy{
		ProtectedEnvs: slices.Clone(p.ProtectedEnvs), ApprovalWorkflowKey: p.ApprovalWorkflowKey, AdhocApproval: p.AdhocApproval,
		MaxHosts: p.MaxHosts, Version: p.Version + 1, UpdateTime: v.s.now(), UpdateUser: actor,
	}
	return nil
}

// JobRuns 「执行作业」权限的测试替身：Denied 里的用户没有权限。
type JobRuns struct{ Denied, OwnOnly map[string]bool }

// CanRun implements biz.RunPermission.
func (r JobRuns) CanRun(_ context.Context, userID string) (bool, error) {
	return !r.Denied[userID], nil
}

// CanViewAll implements biz.RunPermission（OwnOnly 里的用户只能看自己的执行）。
func (r JobRuns) CanViewAll(_ context.Context, userID string) (bool, error) {
	return !r.OwnOnly[userID], nil
}

// JobSources 触发来源名称的测试替身（恒为空）。
type JobSources struct{}

// SourceNames implements biz.TriggerNames.
func (JobSources) SourceNames(context.Context, []string, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

// JobApprovals 审批流程的测试替身：记录发起的审批，由测试调用 Resolve 模拟流程结束。
type JobApprovals struct {
	mu sync.Mutex
	// Keys 可发起的流程标识 → 名称。
	Keys map[string]string
	// AutoApprove 发起即通过（模拟流程节点全部自动通过）。
	AutoApprove bool
	Started     []biz.ApprovalStart
	Cancelled   []string
	// Approvers 流程实例 ID → 处理人。
	Approvers map[string][]string
}

// Involved implements biz.Approvals.
func (a *JobApprovals) Involved(_ context.Context, instanceID, userID string) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Contains(a.Approvers[instanceID], userID), nil
}

// Startable implements biz.Approvals.
func (a *JobApprovals) Startable(_ context.Context, key string) (string, bool, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	name, ok := a.Keys[key]
	return name, ok, ok, nil
}

// StartTx implements biz.Approvals.
func (a *JobApprovals) StartTx(_ context.Context, _ biz.Tx, p biz.ApprovalStart) (biz.ApprovalOutcome, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.Keys[p.DefinitionKey]; !ok {
		return biz.ApprovalOutcome{}, biz.ErrApprovalUnavailable
	}
	a.Started = append(a.Started, p)
	return biz.ApprovalOutcome{InstanceID: "018f3e1a-0e0e-7000-8000-" + fmt.Sprintf("%012d", len(a.Started)), Finished: a.AutoApprove, Approved: a.AutoApprove}, nil
}

// Cancel implements biz.Approvals.
func (a *JobApprovals) Cancel(_ context.Context, instanceID, _ string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Cancelled = append(a.Cancelled, instanceID)
	return nil
}

// Last 最近一次发起的审批。
func (a *JobApprovals) Last() (biz.ApprovalStart, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.Started) == 0 {
		return biz.ApprovalStart{}, false
	}
	return a.Started[len(a.Started)-1], true
}

// JobFiles 文件服务的测试替身：按「用户 ID/文件 ID」存放内容。
type JobFiles struct {
	mu    sync.Mutex
	files map[string]biz.UploadedFile
}

// Put 预置一个文件。
func (f *JobFiles) Put(userID, fileID, name string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.files == nil {
		f.files = map[string]biz.UploadedFile{}
	}
	f.files[userID+"/"+fileID] = biz.UploadedFile{Name: name, Data: data}
}

// Read implements biz.Files.
func (f *JobFiles) Read(_ context.Context, userID, fileID string) (*biz.UploadedFile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.files[userID+"/"+fileID]
	if !ok {
		return nil, biz.ErrAdhocInvalid
	}
	return &file, nil
}
