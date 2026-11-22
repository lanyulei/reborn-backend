package biz

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"
)

// settingsTTL 设置缓存时长：后台循环每轮都要读设置，不必每次查库；界面修改后至多延迟这么久生效。
const settingsTTL = 30 * time.Second

// 缺省根因分类。
var defaultRootCauses = []RootCause{
	{Code: "change", Name: "变更引入"},
	{Code: "capacity", Name: "容量不足"},
	{Code: "code", Name: "代码缺陷"},
	{Code: "config", Name: "配置错误"},
	{Code: "dependency", Name: "第三方依赖"},
	{Code: "infra", Name: "基础设施"},
	{Code: "human", Name: "人为操作"},
	{Code: "unknown", Name: "未知"},
}

// 个人收到通知的缺省动作段。
var defaultPersonalEvents = []string{"opened", "assigned", "escalated", "stale", "proposal", "review_due"}

// knownPersonalEvents 允许配置为个人通知的动作段。
var knownPersonalEvents = []string{"opened", "assigned", "escalated", "stale", "proposal", "review_due", "status_changed", "severity_changed", "update", "merged", "agent"}

// DefaultSettings 内置缺省设置（功能设计 §2.2 / §2.5）。
func DefaultSettings() *Settings {
	return &Settings{
		Severities: map[int32]SeveritySetting{
			SeverityS1: {RespondM: 5, StaleM: 30, FirstUpdateM: 30, ReviewDays: 3},
			SeverityS2: {RespondM: 15, StaleM: 60, FirstUpdateM: 60, ReviewDays: 7},
			SeverityS3: {RespondM: 30, AllowAutoResolve: true, AllowAutoApprove: true},
			SeverityS4: {AllowAutoResolve: true, AllowAutoApprove: true},
		},
		RootCauses:          slices.Clone(defaultRootCauses),
		StormThreshold:      5,
		CandidateTTLMinutes: 60,
		ReopenWindowHours:   72,
		AutoCloseDays:       7,
		PersonalEvents:      slices.Clone(defaultPersonalEvents),
		ProposalTTLMinutes:  30,
		VerifyWindowMinutes: 10,
		ChangeLookbackHours: 24,
		AgentMaxFindings:    200,

		AgentAutoCooldownMinutes:       30,
		AgentAutoMaxRunning:            5,
		AgentAutoMaxRunningPerBusiness: 2,
	}
}

// merge 把库里保存的内容叠在缺省值上（未保存过的字段取缺省）。
func (s *Settings) merge(saved *Settings) {
	if saved == nil {
		return
	}
	for sev, v := range saved.Severities {
		if ValidSeverity(sev) {
			s.Severities[sev] = v
		}
	}
	if len(saved.RootCauses) > 0 {
		s.RootCauses = slices.Clone(saved.RootCauses)
	}
	s.FallbackScheduleID = saved.FallbackScheduleID
	for dst, src := range map[*int32]int32{
		&s.StormThreshold: saved.StormThreshold, &s.CandidateTTLMinutes: saved.CandidateTTLMinutes, &s.ReopenWindowHours: saved.ReopenWindowHours,
		&s.AutoCloseDays: saved.AutoCloseDays, &s.ProposalTTLMinutes: saved.ProposalTTLMinutes, &s.VerifyWindowMinutes: saved.VerifyWindowMinutes,
		&s.ChangeLookbackHours: saved.ChangeLookbackHours, &s.AgentMaxFindings: saved.AgentMaxFindings,
		&s.AgentAutoCooldownMinutes: saved.AgentAutoCooldownMinutes, &s.AgentAutoMaxRunning: saved.AgentAutoMaxRunning,
		&s.AgentAutoMaxRunningPerBusiness: saved.AgentAutoMaxRunningPerBusiness,
	} {
		if src > 0 {
			*dst = src
		}
	}
	if saved.PersonalEvents != nil {
		s.PersonalEvents = slices.Clone(saved.PersonalEvents)
	}
	s.Version = saved.Version
}

// Sev 某等级的响应要求（未知等级按 S4）。
func (s *Settings) Sev(severity int32) SeveritySetting {
	if v, ok := s.Severities[severity]; ok {
		return v
	}
	return s.Severities[SeverityS4]
}

// Personal 该动作段是否通知个人。
func (s *Settings) Personal(action string) bool { return slices.Contains(s.PersonalEvents, action) }

// RootCauseName 根因编码的名称（未知编码原样返回）。
func (s *Settings) RootCauseName(code string) string {
	for _, rc := range s.RootCauses {
		if rc.Code == code {
			return rc.Name
		}
	}
	return code
}

// ValidRootCause 报告根因编码是否在分类里（空串允许）。
func (s *Settings) ValidRootCause(code string) bool {
	if code == "" {
		return true
	}
	return slices.ContainsFunc(s.RootCauses, func(rc RootCause) bool { return rc.Code == code })
}

// validateSettings 校验设置内容。
func validateSettings(s *Settings) error {
	for sev, v := range s.Severities {
		if !ValidSeverity(sev) {
			return detail(ErrSettingInvalid, "unknown severity %d", sev)
		}
		for name, n := range map[string]int32{"respond_m": v.RespondM, "stale_m": v.StaleM, "first_update_m": v.FirstUpdateM} {
			if n < 0 || n > 1440 {
				return detail(ErrSettingInvalid, "%s of S%d must be within 0-1440", name, sev)
			}
		}
		if v.ReviewDays < 0 || v.ReviewDays > 60 {
			return detail(ErrSettingInvalid, "review_days of S%d must be within 0-60", sev)
		}
	}
	if len(s.RootCauses) == 0 || len(s.RootCauses) > 50 {
		return detail(ErrSettingInvalid, "1-50 root causes are required")
	}
	seen := map[string]struct{}{}
	for i := range s.RootCauses {
		rc := &s.RootCauses[i]
		rc.Code, rc.Name = strings.TrimSpace(rc.Code), cleanText(rc.Name)
		if rc.Code == "" || rc.Name == "" {
			return detail(ErrSettingInvalid, "root cause code and name are required")
		}
		if _, dup := seen[rc.Code]; dup {
			return detail(ErrSettingInvalid, "duplicated root cause code %q", rc.Code)
		}
		seen[rc.Code] = struct{}{}
	}
	for _, e := range s.PersonalEvents {
		if !slices.Contains(knownPersonalEvents, e) {
			return detail(ErrSettingInvalid, "unknown personal event %q", e)
		}
	}
	bounds := []struct {
		name      string
		v, lo, hi int32
	}{
		{"storm_threshold", s.StormThreshold, 1, 100},
		{"candidate_ttl_m", s.CandidateTTLMinutes, 5, 1440},
		{"reopen_window_h", s.ReopenWindowHours, 1, 720},
		{"auto_close_days", s.AutoCloseDays, 1, 90},
		{"proposal_ttl_m", s.ProposalTTLMinutes, 5, 1440},
		{"verify_window_m", s.VerifyWindowMinutes, 1, 120},
		{"change_lookback_h", s.ChangeLookbackHours, 1, 168},
		{"agent_max_findings", s.AgentMaxFindings, 10, 1000},
		{"agent_auto_cooldown_m", s.AgentAutoCooldownMinutes, 1, 1440},
		{"agent_auto_max_running", s.AgentAutoMaxRunning, 1, 100},
		{"agent_auto_max_running_per_business", s.AgentAutoMaxRunningPerBusiness, 1, 100},
	}
	for _, b := range bounds {
		if b.v < b.lo || b.v > b.hi {
			return detail(ErrSettingInvalid, "%s must be within %d-%d", b.name, b.lo, b.hi)
		}
	}
	return nil
}

// SettingsUsecase 单例设置：读带短缓存，写乐观锁。
type SettingsUsecase struct {
	repo SettingRepo
	now  func() time.Time

	mu      sync.Mutex
	cached  *Settings
	expires time.Time
}

// NewSettingsUsecase new a SettingsUsecase.
func NewSettingsUsecase(repo SettingRepo) *SettingsUsecase {
	return &SettingsUsecase{repo: repo, now: time.Now}
}

// Get 当前设置（缺省值叠加库里保存的内容）；返回值为副本，可自由读取。
func (uc *SettingsUsecase) Get(ctx context.Context) (*Settings, error) {
	uc.mu.Lock()
	if uc.cached != nil && uc.now().Before(uc.expires) {
		s := *uc.cached
		uc.mu.Unlock()
		return &s, nil
	}
	uc.mu.Unlock()
	saved, err := uc.repo.Get(ctx)
	if err != nil {
		return nil, err
	}
	s := DefaultSettings()
	s.merge(saved)
	uc.mu.Lock()
	uc.cached, uc.expires = s, uc.now().Add(settingsTTL)
	uc.mu.Unlock()
	cp := *s
	return &cp, nil
}

// Update 保存设置（整体替换，version 为读取时的版本）。
func (uc *SettingsUsecase) Update(ctx context.Context, userID string, s *Settings, version int32) (*Settings, error) {
	if s == nil {
		return nil, ErrSettingInvalid
	}
	full := DefaultSettings()
	full.merge(s)
	if err := validateSettings(full); err != nil {
		return nil, err
	}
	if err := uc.repo.Save(ctx, full, version, userID); err != nil {
		return nil, err
	}
	uc.mu.Lock()
	uc.cached = nil
	uc.mu.Unlock()
	return uc.Get(ctx)
}

// Invalidate 清掉缓存（测试与同进程写后读用）。
func (uc *SettingsUsecase) Invalidate() {
	uc.mu.Lock()
	uc.cached = nil
	uc.mu.Unlock()
}
