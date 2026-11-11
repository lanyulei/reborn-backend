package biz

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

// silenceClockHHMM 24 小时制 HH:MM（与 protovalidate 同式，入口双保险）。
var silenceClockHHMM = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)

// 静默数值边界（severity 对齐事件分级 1 info/2 warning/3 critical/4 fatal）。
const (
	silenceSeverityMin = 1
	silenceSeverityMax = 4
)

// SilenceWindow 免打扰时间窗：支持跨午夜（如 22:00→08:00 覆盖夜间）；
// json 标签即 notify_silence.time_window 列的落库结构，管道按窗口与时区
// 判定命中（tz 缺省 UTC）。
type SilenceWindow = notifycontract.SilenceWindow

// SilenceRule 静默规则：匹配决策（pattern/severity 上限/时间窗）互为整体，
// 创建与整组替换更新共用同一校验。
type SilenceRule struct {
	EventPattern string
	SeverityMax  int32
	Window       *SilenceWindow // nil = 全程静默
}

// Silence 静默领域模型（DO）；内嵌 SilenceRule（规则字段经提升按扁平访问），
// 与 proto 实体、data 层 PO 显式转换，禁止跨层直传。
type Silence struct {
	ID    string
	BizID string
	SilenceRule
	Enabled    bool
	CreateTime time.Time
	UpdateTime time.Time
}

// SilenceFilter 静默列表过滤条件；指针为 nil 表示不过滤。
type SilenceFilter struct {
	BizID   string // 空 = 不过滤（受限用户由 service 层强制携带）
	Keyword string
	Enabled *bool
}

// SilenceUpdate 静默更新：Rule 非 nil 时整组替换规则字段；Enabled 独立启停；
// 两者至少其一。
type SilenceUpdate struct {
	Rule    *SilenceRule
	Enabled *bool
}

// 静默哨兵错误：reason 与 api/notify/v1 的 ErrorReason 枚举对齐。
var (
	// ErrSilenceNotFound 目标静默规则不存在。
	ErrSilenceNotFound = errors.NotFound(notifyv1.ErrorReason_NOTIFY_SILENCE_NOT_FOUND.String(), "silence not found")
	// ErrSilenceInvalid 静默规则不合法：event_pattern/severity_max 越界、
	// 时间窗格式错误、起点等于终点（零长窗口）或未知时区。
	ErrSilenceInvalid = errors.BadRequest(notifyv1.ErrorReason_NOTIFY_SILENCE_INVALID.String(), "invalid silence")
)

// SilenceRepo 静默仓储接口，由 data 层实现（依赖倒置）。
type SilenceRepo interface {
	// CreateSilence 创建静默规则；设计即无唯一约束（多条可重叠，任一命中即抑制）。
	CreateSilence(ctx context.Context, silence *Silence) error
	// UpdateSilence 按 ID 更新未删除静默规则（Rule 整组替换 / Enabled 启停）；
	// 不存在返回 ErrSilenceNotFound。
	UpdateSilence(ctx context.Context, id string, upd SilenceUpdate) error
	// DeleteSilence 软删静默规则；不存在返回 ErrSilenceNotFound。
	DeleteSilence(ctx context.Context, id string) error
	// GetSilence 按 ID 查询未删除静默规则；不存在返回 ErrSilenceNotFound。
	GetSilence(ctx context.Context, id string) (*Silence, error)
	// ListSilences 分页查询未删除静默规则；page 从 1 起，pageSize 上限 100，
	// 按创建时间倒序。
	ListSilences(ctx context.Context, filter SilenceFilter, page, pageSize int64) ([]*Silence, int64, error)
}

// SilenceUsecase 静默用例：承载规则校验（模式/严重级/时间窗）。
type SilenceUsecase struct {
	repo SilenceRepo
}

// NewSilenceUsecase new a SilenceUsecase.
func NewSilenceUsecase(repo SilenceRepo) *SilenceUsecase {
	return &SilenceUsecase{repo: repo}
}

// validateSilenceRule 校验一条静默规则的全部字段。
func validateSilenceRule(rule *SilenceRule) error {
	if !eventPatternPattern.MatchString(rule.EventPattern) {
		return fmt.Errorf("%w: bad event_pattern %q", ErrSilenceInvalid, rule.EventPattern)
	}
	if rule.SeverityMax < silenceSeverityMin || rule.SeverityMax > silenceSeverityMax {
		return fmt.Errorf("%w: severity_max %d out of range [%d,%d]",
			ErrSilenceInvalid, rule.SeverityMax, silenceSeverityMin, silenceSeverityMax)
	}
	if rule.Window != nil {
		if err := validateSilenceWindow(rule.Window); err != nil {
			return err
		}
	}
	return nil
}

// validateSilenceWindow 校验时间窗：HH:MM 格式、起点不等于终点（零长窗口
// 无意义）、时区可解析（IANA）。start > end 表示跨午夜（合法）。
func validateSilenceWindow(w *SilenceWindow) error {
	if !silenceClockHHMM.MatchString(w.Start) {
		return fmt.Errorf("%w: bad window start %q", ErrSilenceInvalid, w.Start)
	}
	if !silenceClockHHMM.MatchString(w.End) {
		return fmt.Errorf("%w: bad window end %q", ErrSilenceInvalid, w.End)
	}
	if w.Start == w.End {
		return fmt.Errorf("%w: window %s-%s is zero-length", ErrSilenceInvalid, w.Start, w.End)
	}
	if w.Tz != "" {
		if _, err := time.LoadLocation(w.Tz); err != nil {
			return fmt.Errorf("%w: unknown tz %q", ErrSilenceInvalid, w.Tz)
		}
	}
	return nil
}

// Create 创建静默规则：校验规则，生成 ID 与审计时间后落库；
// BizID 缺省 default。
func (uc *SilenceUsecase) Create(ctx context.Context, silence *Silence) (*Silence, error) {
	if silence.BizID == "" {
		silence.BizID = DefaultBizID
	}
	if err := validateSilenceRule(&silence.SilenceRule); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	silence.ID = id
	silence.CreateTime, silence.UpdateTime = now, now
	if err := uc.repo.CreateSilence(ctx, silence); err != nil {
		return nil, err
	}
	return silence, nil
}

// Update 更新静默规则：Rule 整组替换（先确认存在），Enabled 独立启停；
// 不存在返回 ErrSilenceNotFound。
func (uc *SilenceUsecase) Update(ctx context.Context, id string, upd SilenceUpdate) error {
	if upd.Rule != nil {
		if _, err := uc.repo.GetSilence(ctx, id); err != nil {
			return err
		}
		if err := validateSilenceRule(upd.Rule); err != nil {
			return err
		}
	}
	return uc.repo.UpdateSilence(ctx, id, upd)
}

// Delete 软删静默规则；不存在返回 ErrSilenceNotFound。
func (uc *SilenceUsecase) Delete(ctx context.Context, id string) error {
	return uc.repo.DeleteSilence(ctx, id)
}

// Get 查询静默规则详情。
func (uc *SilenceUsecase) Get(ctx context.Context, id string) (*Silence, error) {
	return uc.repo.GetSilence(ctx, id)
}

// List 分页查询静默规则；分页参数先规整再委托仓储。
func (uc *SilenceUsecase) List(ctx context.Context, filter SilenceFilter, page, pageSize int64) ([]*Silence, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListSilences(ctx, filter, page, pageSize)
}
