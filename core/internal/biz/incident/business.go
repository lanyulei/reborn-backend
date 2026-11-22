package biz

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 业务字典（功能设计 §2.1）：故障归属的业务 / 服务，带缺省值班表（指挥来源）、通知群与匹配标签值。

var businessCodeRE = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// BusinessUsecase 业务字典。
type BusinessUsecase struct {
	d *Deps
}

// NewBusinessUsecase new a BusinessUsecase.
func NewBusinessUsecase(d *Deps) *BusinessUsecase { return &BusinessUsecase{d: d} }

// BusinessView 业务及其展示信息。
type BusinessView struct {
	*Business
	OwnerName    string
	ScheduleName string
	CIName       string
}

func (uc *BusinessUsecase) validate(ctx context.Context, b *Business) error {
	b.Code, b.Name, b.Description = strings.TrimSpace(b.Code), cleanText(b.Name), cleanText(b.Description)
	if !businessCodeRE.MatchString(b.Code) {
		return detail(ErrBusinessInvalid, "code must match ^[a-z][a-z0-9_-]{0,63}$")
	}
	if b.Name == "" || len([]rune(b.Name)) > 128 {
		return detail(ErrBusinessInvalid, "name must be 1-128 characters")
	}
	if len(b.Groups) > 10 || len(b.LabelValues) > 50 {
		return detail(ErrBusinessInvalid, "too many groups or label values")
	}
	for _, g := range b.Groups {
		if !strings.HasPrefix(g, "$") {
			return detail(ErrBusinessInvalid, "group identifier must start with $")
		}
	}
	b.Groups, b.LabelValues = uniq(b.Groups), uniq(b.LabelValues)
	if b.OwnerID != "" {
		active, err := uc.d.Dir.ActiveUserIDs(ctx, []string{b.OwnerID})
		if err != nil {
			return err
		}
		if len(active) != 1 {
			return detail(ErrBusinessInvalid, "owner must be an active user")
		}
	}
	if b.ScheduleID != "" {
		names, err := uc.d.Schedules.ScheduleNames(ctx, []string{b.ScheduleID})
		if err != nil {
			return err
		}
		if len(names) != 1 {
			return detail(ErrBusinessInvalid, "schedule does not exist or is disabled")
		}
	}
	if b.CIID != "" {
		cards, err := uc.d.CMDB.Cards(ctx, []string{b.CIID})
		if err != nil {
			return err
		}
		if len(cards) != 1 {
			return detail(ErrBusinessInvalid, "ci does not exist")
		}
	}
	return nil
}

func (uc *BusinessUsecase) views(ctx context.Context, items []*Business) ([]*BusinessView, error) {
	var users, schedules, cis []string
	for _, b := range items {
		users = append(users, b.OwnerID)
		schedules = append(schedules, b.ScheduleID)
		cis = append(cis, b.CIID)
	}
	userNames, err := uc.d.Dir.UserNames(ctx, uniq(users))
	if err != nil {
		return nil, err
	}
	scheduleNames, err := uc.d.Schedules.ScheduleNames(ctx, uniq(schedules))
	if err != nil {
		return nil, err
	}
	ciNames := map[string]string{}
	if ids := uniq(cis); len(ids) > 0 {
		cards, err := uc.d.CMDB.Cards(ctx, ids)
		if err != nil {
			return nil, err
		}
		for _, c := range cards {
			ciNames[c.ID] = c.Name
		}
	}
	out := make([]*BusinessView, 0, len(items))
	for _, b := range items {
		out = append(out, &BusinessView{Business: b, OwnerName: userNames[b.OwnerID], ScheduleName: scheduleNames[b.ScheduleID], CIName: ciNames[b.CIID]})
	}
	return out, nil
}

// List 分页。
func (uc *BusinessUsecase) List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*BusinessView, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.d.Businesses.List(ctx, cleanText(keyword), enabled, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, items)
	return views, total, err
}

// Options 启用的业务（候选项）。
func (uc *BusinessUsecase) Options(ctx context.Context) ([]Named, error) {
	items, err := uc.d.Businesses.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Named, 0, len(items))
	for _, b := range items {
		if b.Enabled {
			out = append(out, Named{ID: b.ID, Name: b.Name})
		}
	}
	return out, nil
}

// Create 新建。
func (uc *BusinessUsecase) Create(ctx context.Context, userID string, b Business) (*BusinessView, error) {
	if err := uc.validate(ctx, &b); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	b.ID = id
	if err := uc.d.Businesses.Create(ctx, &b, userID); err != nil {
		return nil, err
	}
	return uc.get(ctx, id)
}

// Update 编辑（编码不可改）。
func (uc *BusinessUsecase) Update(ctx context.Context, userID, id string, b Business, version int32) (*BusinessView, error) {
	cur, err := uc.d.Businesses.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	b.Code = cur.Code
	if err := uc.validate(ctx, &b); err != nil {
		return nil, err
	}
	b.ID = id
	if err := uc.d.Businesses.Update(ctx, &b, version, userID); err != nil {
		return nil, err
	}
	return uc.get(ctx, id)
}

// Delete 删除；仍被故障 / 规则 / 接入源引用时拒绝。
func (uc *BusinessUsecase) Delete(ctx context.Context, userID, id string) error {
	cur, err := uc.d.Businesses.Get(ctx, id)
	if err != nil {
		return err
	}
	n, err := uc.d.Businesses.InUse(ctx, id)
	if err != nil {
		return err
	}
	rules, err := uc.d.Rules.CountBusinessRefs(ctx, id)
	if err != nil {
		return err
	}
	sources, err := uc.d.Sources.CountBusinessRefs(ctx, id)
	if err != nil {
		return err
	}
	if n+rules+sources > 0 {
		return detail(ErrBusinessInUse, "business is referenced by %d incident(s)/source(s) and %d rule(s)", n+sources, rules)
	}
	return uc.d.Businesses.Delete(ctx, id, cur.Version, userID)
}

func (uc *BusinessUsecase) get(ctx context.Context, id string) (*BusinessView, error) {
	b, err := uc.d.Businesses.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, []*Business{b})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// matchBusiness 按标签值匹配业务字典（启用的）；多条命中取 sort 最小者。
func matchBusiness(all []*Business, value string) *Business {
	if value == "" {
		return nil
	}
	for _, b := range all {
		if b.Enabled && slices.Contains(b.LabelValues, value) {
			return b
		}
	}
	return nil
}
