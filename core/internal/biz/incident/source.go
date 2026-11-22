package biz

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 接入源与开放接口（功能设计 §3.3）：外部系统凭令牌（Bearer）开 / 查 / 备注 / 恢复故障。
// 令牌模型与流程域开放客户端一致：库内只存 SHA-256，可重置、可停用；external_key 幂等；来源 IP 白名单。

const (
	sourceTokenPrefix = "ink_"
	sourceTokenBytes  = 32
	sourceShownPrefix = 10
	maxSources        = 100
)

// SourceUsecase 接入源维护。
type SourceUsecase struct {
	d *Deps
}

// NewSourceUsecase new a SourceUsecase.
func NewSourceUsecase(d *Deps) *SourceUsecase { return &SourceUsecase{d: d} }

// SourceView 接入源及其展示信息。
type SourceView struct {
	*Source
	BusinessName string
}

// SourceSpec 可编辑内容。
type SourceSpec struct {
	Name            string
	Description     string
	DefaultSeverity int32
	BusinessID      string
	Groups          []string
	AllowedIPs      []string
	Enabled         bool
}

func newSourceToken() (token string, hash []byte, prefix string, err error) {
	raw := make([]byte, sourceTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, "", err
	}
	token = sourceTokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))
	return token, sum[:], token[:sourceShownPrefix], nil
}

func (uc *SourceUsecase) validate(ctx context.Context, s *SourceSpec) error {
	s.Name, s.Description = cleanText(s.Name), cleanText(s.Description)
	if s.Name == "" || len([]rune(s.Name)) > 128 {
		return detail(ErrSourceInvalid, "name must be 1-128 characters")
	}
	if s.DefaultSeverity < SeverityS2 || s.DefaultSeverity > SeverityS4 {
		return detail(ErrSourceInvalid, "default_severity must be 2-4 (sources cannot open S1)")
	}
	for _, g := range s.Groups {
		if !strings.HasPrefix(g, "$") {
			return detail(ErrSourceInvalid, "group identifier must start with $")
		}
	}
	s.Groups = uniq(s.Groups)
	for i, ip := range s.AllowedIPs {
		ip = strings.TrimSpace(ip)
		s.AllowedIPs[i] = ip
		if _, _, err := net.ParseCIDR(ip); err != nil && net.ParseIP(ip) == nil {
			return detail(ErrSourceInvalid, "allowed_ips[%d] %q is not an IP or CIDR", i, ip)
		}
	}
	s.AllowedIPs = uniq(s.AllowedIPs)
	if s.BusinessID != "" {
		if _, err := uc.d.Businesses.Get(ctx, s.BusinessID); err != nil {
			return err
		}
	}
	return nil
}

func (uc *SourceUsecase) views(ctx context.Context, items []*Source) ([]*SourceView, error) {
	out := make([]*SourceView, 0, len(items))
	for _, s := range items {
		v := &SourceView{Source: s}
		if s.BusinessID != "" {
			if b, err := uc.d.Businesses.Get(ctx, s.BusinessID); err == nil {
				v.BusinessName = b.Name
			}
		}
		out = append(out, v)
	}
	return out, nil
}

// List 全部接入源。
func (uc *SourceUsecase) List(ctx context.Context) ([]*SourceView, error) {
	items, err := uc.d.Sources.List(ctx)
	if err != nil {
		return nil, err
	}
	return uc.views(ctx, items)
}

// Create 新建并签发令牌（明文只返回这一次）。
func (uc *SourceUsecase) Create(ctx context.Context, userID string, spec SourceSpec) (*SourceView, string, error) {
	if err := uc.validate(ctx, &spec); err != nil {
		return nil, "", err
	}
	items, err := uc.d.Sources.List(ctx)
	if err != nil {
		return nil, "", err
	}
	if len(items) >= maxSources {
		return nil, "", detail(ErrSourceInvalid, "at most %d sources", maxSources)
	}
	token, hash, prefix, err := newSourceToken()
	if err != nil {
		return nil, "", err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, "", err
	}
	s := &Source{
		ID: id, Name: spec.Name, Description: spec.Description, TokenHash: hash, TokenPrefix: prefix, DefaultSeverity: spec.DefaultSeverity,
		BusinessID: spec.BusinessID, Groups: spec.Groups, AllowedIPs: spec.AllowedIPs, Enabled: spec.Enabled,
	}
	if err := uc.d.Sources.Create(ctx, s, userID); err != nil {
		return nil, "", err
	}
	v, err := uc.get(ctx, id)
	return v, token, err
}

// Update 编辑（不改令牌）。
func (uc *SourceUsecase) Update(ctx context.Context, userID, id string, spec SourceSpec, version int32) (*SourceView, error) {
	if err := uc.validate(ctx, &spec); err != nil {
		return nil, err
	}
	s := &Source{ID: id, Name: spec.Name, Description: spec.Description, DefaultSeverity: spec.DefaultSeverity, BusinessID: spec.BusinessID, Groups: spec.Groups, AllowedIPs: spec.AllowedIPs, Enabled: spec.Enabled}
	if err := uc.d.Sources.Update(ctx, s, version, userID); err != nil {
		return nil, err
	}
	return uc.get(ctx, id)
}

// ResetToken 重置令牌。
func (uc *SourceUsecase) ResetToken(ctx context.Context, userID, id string) (*SourceView, string, error) {
	if _, err := uc.d.Sources.Get(ctx, id); err != nil {
		return nil, "", err
	}
	token, hash, prefix, err := newSourceToken()
	if err != nil {
		return nil, "", err
	}
	if err := uc.d.Sources.SetToken(ctx, id, hash, prefix, userID); err != nil {
		return nil, "", err
	}
	v, err := uc.get(ctx, id)
	return v, token, err
}

// Delete 删除（令牌同时作废）。
func (uc *SourceUsecase) Delete(ctx context.Context, userID, id string) error {
	if _, err := uc.d.Sources.Get(ctx, id); err != nil {
		return err
	}
	return uc.d.Sources.Delete(ctx, id, userID)
}

func (uc *SourceUsecase) get(ctx context.Context, id string) (*SourceView, error) {
	s, err := uc.d.Sources.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, []*Source{s})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// ---------------------------------------------------------------------------
// 开放接口
// ---------------------------------------------------------------------------

// OpenUsecase 开放接口：令牌认证、来源校验、以接入源身份操作故障。
type OpenUsecase struct {
	d        *Deps
	inc      *IncidentUsecase
	settings *SettingsUsecase
	limiter  *publicLimiter
	now      func() time.Time
}

// NewOpenUsecase new an OpenUsecase.
func NewOpenUsecase(d *Deps, inc *IncidentUsecase, settings *SettingsUsecase) *OpenUsecase {
	return &OpenUsecase{d: d, inc: inc, settings: settings, limiter: newPublicLimiter(time.Now), now: time.Now}
}

// authenticate 校验令牌与来源 IP，返回接入源。
func (uc *OpenUsecase) authenticate(ctx context.Context, token, ip string) (*Source, error) {
	if !uc.limiter.allow(ip) {
		return nil, ErrRateLimited
	}
	token = strings.TrimSpace(token)
	if token == "" || !strings.HasPrefix(token, sourceTokenPrefix) {
		uc.limiter.fail(ip)
		return nil, ErrSourceUnauthorized
	}
	sum := sha256.Sum256([]byte(token))
	s, err := uc.d.Sources.FindByHash(ctx, sum[:])
	if err != nil {
		if kratosCodeIs(err, 401) || isNotFound(err) {
			uc.limiter.fail(ip)
			return nil, ErrSourceUnauthorized
		}
		return nil, err
	}
	if !s.Enabled {
		return nil, ErrSourceUnauthorized
	}
	if len(s.AllowedIPs) > 0 && !ipAllowed(ip, s.AllowedIPs) {
		return nil, detail(ErrSourceForbidden, "client address is not allowed")
	}
	if err := uc.d.Sources.Touch(ctx, s.ID); err != nil {
		uc.d.Logger.WarnContext(ctx, "incident source: touch", "source", s.ID, "error", err)
	}
	return s, nil
}

// ipAllowed 来源 IP 是否在白名单（IP 或 CIDR）内。
func ipAllowed(ip string, allowed []string) bool {
	addr := net.ParseIP(ip)
	if addr == nil {
		return false
	}
	for _, a := range allowed {
		if _, cidr, err := net.ParseCIDR(a); err == nil {
			if cidr.Contains(addr) {
				return true
			}
			continue
		}
		if net.ParseIP(a).Equal(addr) {
			return true
		}
	}
	return false
}

// OpenCreateInput 开放接口开故障入参。
type OpenCreateInput struct {
	ExternalKey  string
	Title        string
	Severity     int32
	Summary      string
	BusinessCode string
	OccurTime    *time.Time
	CINames      []string
}

// OpenResult 开放接口的故障视图。
type OpenResult struct {
	Incident *Incident
	Created  bool
}

// Create 开故障（幂等）。
func (uc *OpenUsecase) Create(ctx context.Context, token, ip string, in OpenCreateInput) (*OpenResult, error) {
	s, err := uc.authenticate(ctx, token, ip)
	if err != nil {
		return nil, err
	}
	in.ExternalKey = strings.TrimSpace(in.ExternalKey)
	if in.ExternalKey == "" {
		return nil, invalid("external_key is required")
	}
	if existing, err := uc.d.Incidents.FindByExternalKey(ctx, s.ID, in.ExternalKey); err != nil {
		return nil, err
	} else if existing != nil {
		return &OpenResult{Incident: existing}, nil
	}
	if in.Title = cleanText(in.Title); in.Title == "" {
		return nil, invalid("title is required")
	}
	severity := s.DefaultSeverity
	if in.Severity != 0 {
		if in.Severity < SeverityS2 || in.Severity > SeverityS4 {
			return nil, detail(ErrSourceForbidden, "sources can only open S2-S4 incidents")
		}
		severity = in.Severity
	}
	businessID := s.BusinessID
	if code := strings.TrimSpace(in.BusinessCode); code != "" {
		b, err := uc.d.Businesses.GetByCode(ctx, code)
		if err != nil {
			return nil, err
		}
		businessID = b.ID
	}
	var cis []string
	if names := uniq(in.CINames); len(names) > 0 {
		cards, err := uc.d.CMDB.FindByNames(ctx, names)
		if err != nil {
			return nil, err
		}
		for _, c := range cards {
			cis = append(cis, c.ID)
		}
	}
	inc, err := uc.inc.open(ctx, ActorSystem, "", CreateInput{
		Title: in.Title, Severity: severity, BusinessID: businessID, Summary: in.Summary, AffectedCIIDs: cis, Groups: s.Groups, OccurTime: in.OccurTime,
		Source: SourceOpen, SourceRef: s.ID, ExternalKey: in.ExternalKey, LinkMethod: LinkManual,
	})
	if err != nil {
		if ErrVersionConflict.Is(err) {
			// 并发重复提交：唯一索引挡住后返回已存在的。
			if existing, ferr := uc.d.Incidents.FindByExternalKey(ctx, s.ID, in.ExternalKey); ferr == nil && existing != nil {
				return &OpenResult{Incident: existing}, nil
			}
		}
		return nil, err
	}
	return &OpenResult{Incident: inc, Created: true}, nil
}

// own 校验故障属于本接入源。
func (uc *OpenUsecase) own(ctx context.Context, s *Source, id string) (*Incident, error) {
	inc, err := uc.d.Incidents.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inc.Source != SourceOpen || inc.SourceRef != s.ID {
		return nil, detail(ErrSourceForbidden, "incident was not opened by this source")
	}
	return inc, nil
}

// Get 查询本接入源开出的故障。
func (uc *OpenUsecase) Get(ctx context.Context, token, ip, id string) (*OpenResult, error) {
	s, err := uc.authenticate(ctx, token, ip)
	if err != nil {
		return nil, err
	}
	inc, err := uc.own(ctx, s, id)
	if err != nil {
		return nil, err
	}
	return &OpenResult{Incident: inc}, nil
}

// Note 追加备注。
func (uc *OpenUsecase) Note(ctx context.Context, token, ip, id, text string) (*OpenResult, error) {
	s, err := uc.authenticate(ctx, token, ip)
	if err != nil {
		return nil, err
	}
	if _, err := uc.own(ctx, s, id); err != nil {
		return nil, err
	}
	text = cleanText(text)
	if text == "" || len(text) > maxCommentBytes {
		return nil, invalid("text must be 1-%d bytes", maxCommentBytes)
	}
	inc, err := uc.inc.mutate(ctx, id, ActorSystem, "", func(m *mutation) error {
		if m.inc.Status == StatusMerged || m.inc.Status == StatusClosed {
			return ErrStateInvalid
		}
		m.event(EventComment, EventDetail{Text: "[" + s.Name + "] " + text})
		m.touch()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &OpenResult{Incident: inc}, nil
}

// Resolve 置已恢复（只能恢复本接入源开出的、仍活跃的故障）。
func (uc *OpenUsecase) Resolve(ctx context.Context, token, ip, id, note string) (*OpenResult, error) {
	s, err := uc.authenticate(ctx, token, ip)
	if err != nil {
		return nil, err
	}
	if _, err := uc.own(ctx, s, id); err != nil {
		return nil, err
	}
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	note = cleanText(note)
	if note == "" {
		note = "接入源「" + s.Name + "」报告已恢复"
	}
	inc, err := uc.inc.mutate(ctx, id, ActorSystem, "", func(m *mutation) error {
		if !IsActive(m.inc.Status) {
			return ErrStateInvalid
		}
		return uc.inc.transition(ctx, m, access{admin: true}, TransitionInput{Action: ActionResolve, Note: note}, settings)
	})
	if err != nil {
		return nil, err
	}
	return &OpenResult{Incident: inc}, nil
}
