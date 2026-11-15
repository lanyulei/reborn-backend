package biz

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 查询与预览的范围上限。
const (
	// maxShiftRange 班次查询跨度上限（月视图 6 周 + 列表视图一个季度足够）。
	maxShiftRange = 100 * 24 * time.Hour
	// defaultPreviewDays 预览缺省天数。
	defaultPreviewDays = 30
)

// ScheduleConfig 值班表配置入参（新建、编辑、预览共用）。
type ScheduleConfig struct {
	Name             string
	Description      string
	Timezone         string
	RotationUnit     int32
	RotationInterval int32
	AnchorTime       time.Time
	MemberIDs        []string
	StartUserID      string
	BackupEnabled    bool
	HorizonDays      int32
	RemindMinutes    []int32
	DigestTime       string
}

// ScheduleUsecase 值班表、班次与替班用例。
type ScheduleUsecase struct {
	store     Store
	groups    GroupRepo
	schedules ScheduleRepo
	shifts    ShiftRepo
	notify    *Notifier
	now       func() time.Time
}

// NewScheduleUsecase new a ScheduleUsecase.
func NewScheduleUsecase(store Store, groups GroupRepo, schedules ScheduleRepo, shifts ShiftRepo, notify *Notifier) *ScheduleUsecase {
	return &ScheduleUsecase{store: store, groups: groups, schedules: schedules, shifts: shifts, notify: notify, now: clock}
}

// clock 当前时间，截断到秒：班次边界落在整秒上，且与 PostgreSQL 的微秒精度无舍入差。
func clock() time.Time { return time.Now().UTC().Truncate(time.Second) }

// rotationOf 值班表对应的轮转规则；时区非法返回 ErrScheduleInvalid。
func rotationOf(s *Schedule) (Rotation, error) {
	loc, err := loadLocation(s.Timezone)
	if err != nil {
		return Rotation{}, err
	}
	return Rotation{
		Loc: loc, StepDays: rotationStepDays(s), Anchor: s.AnchorTime, Offset: int(s.AnchorOffset),
		Members: s.MemberIDs, Backup: s.BackupEnabled,
	}, nil
}

// loadLocation 解析 IANA 时区名；拒绝空串与 "Local"（随部署机器变化，不可作为规则）。
func loadLocation(name string) (*time.Location, error) {
	if name == "" || name == "Local" {
		return nil, detail(ErrScheduleInvalid, "timezone is required")
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, detail(ErrScheduleInvalid, "unknown timezone %q", name)
	}
	return loc, nil
}

// apply 校验配置并写入 s（不含 ID / GroupID / Status）。members 为分组成员集合。
func (c ScheduleConfig) apply(s *Schedule, members map[string]*Member) error {
	name := strings.TrimSpace(c.Name)
	if name == "" {
		return detail(ErrScheduleInvalid, "name is required")
	}
	if _, err := loadLocation(c.Timezone); err != nil {
		return err
	}
	if c.RotationUnit != UnitDay && c.RotationUnit != UnitWeek {
		return detail(ErrScheduleInvalid, "rotation unit must be day or week")
	}
	if limit := int32(30); c.RotationInterval < 1 || c.RotationInterval > limit || (c.RotationUnit == UnitWeek && c.RotationInterval > 4) {
		return detail(ErrScheduleInvalid, "rotation interval must be 1-30 days or 1-4 weeks")
	}
	if c.AnchorTime.IsZero() {
		return detail(ErrScheduleInvalid, "first handover time is required")
	}
	if len(c.MemberIDs) == 0 {
		return detail(ErrScheduleInvalid, "rotation needs at least one member")
	}
	// 主备值班人不能是同一个人：开启备值班时名单至少两人（备值班为主值班的下一位）。
	if c.BackupEnabled && len(c.MemberIDs) < 2 {
		return detail(ErrScheduleInvalid, "backup duty needs at least two members in the rotation")
	}
	if len(c.MemberIDs) > maxScheduleMembers {
		return detail(ErrScheduleInvalid, "rotation can have at most %d members", maxScheduleMembers)
	}
	seen := make(map[string]struct{}, len(c.MemberIDs))
	for _, id := range c.MemberIDs {
		if _, dup := seen[id]; dup {
			return detail(ErrScheduleInvalid, "member %s appears more than once", id)
		}
		seen[id] = struct{}{}
		if _, ok := members[id]; !ok {
			return detail(ErrScheduleInvalid, "user %s is not a member of this group", id)
		}
	}
	offset := 0
	if c.StartUserID != "" {
		if offset = slices.Index(c.MemberIDs, c.StartUserID); offset < 0 {
			return detail(ErrScheduleInvalid, "start user must be in the rotation")
		}
	}
	if c.HorizonDays < 7 || c.HorizonDays > 180 {
		return detail(ErrScheduleInvalid, "horizon must be 7-180 days")
	}
	if len(c.RemindMinutes) > 2 {
		return detail(ErrScheduleInvalid, "at most two reminders")
	}
	for _, m := range c.RemindMinutes {
		if m < 0 || m > 7*24*60 {
			return detail(ErrScheduleInvalid, "reminder lead time must be within 7 days")
		}
	}
	if c.DigestTime != "" {
		if _, err := time.Parse("15:04", c.DigestTime); err != nil {
			return detail(ErrScheduleInvalid, "digest time must be HH:MM")
		}
	}
	s.Name, s.Description, s.Timezone = name, strings.TrimSpace(c.Description), c.Timezone
	s.RotationUnit, s.RotationInterval = c.RotationUnit, c.RotationInterval
	// 交接时刻取整到分钟：秒级的交接点没有业务意义，还会让按分钟选择的替班时间段
	// 在班次边界留下几十秒的碎片。
	s.AnchorTime, s.AnchorOffset = c.AnchorTime.UTC().Truncate(time.Minute), int32(offset)
	s.BackupEnabled, s.HorizonDays = c.BackupEnabled, c.HorizonDays
	s.RemindMinutes = slices.Clone(c.RemindMinutes)
	s.DigestTime = c.DigestTime
	s.MemberIDs = slices.Clone(c.MemberIDs)
	return nil
}

// rotationChanged 报告两份配置的轮转规则是否不同（不同才需要重排未开始的班次）。
func rotationChanged(a, b *Schedule) bool {
	return a.Timezone != b.Timezone || a.RotationUnit != b.RotationUnit || a.RotationInterval != b.RotationInterval ||
		!a.AnchorTime.Equal(b.AnchorTime) || a.AnchorOffset != b.AnchorOffset || a.BackupEnabled != b.BackupEnabled ||
		!slices.Equal(a.MemberIDs, b.MemberIDs)
}

// memberSet 分组成员按用户 ID 索引。
func memberSet(members []*Member) map[string]*Member {
	set := make(map[string]*Member, len(members))
	for _, m := range members {
		set[m.UserID] = m
	}
	return set
}

// generate 生成值班表的班次（调用方须已持有值班表行锁）：
//  1. reset 为真时先删除尚未开始的自动班次（规则变了，按新规则重排；恰在此刻开始的
//     班次视为进行中，保留）；
//  2. 按轮转规则计算 [now, now+提前天数) 内的理想安排；
//  3. 减去已存在班次占用的时间（已过去、进行中、替班，reset 为假时还包括未开始的自动班次）；
//  4. 写入剩余片段并推进 generated_until。
//
// 首次发布、后台补排（reset=false）、改配置后重排、撤销替班后恢复共用本过程。
// 重排前后同一人同一角色同一开始时间的班次沿用已发送的提醒标记，避免重复提醒。
func generate(ctx context.Context, tx Tx, s *Schedule, now time.Time, reset bool, actor string) error {
	if s.Status != StatusEnabled {
		return nil
	}
	rot, err := rotationOf(s)
	if err != nil {
		return err
	}
	existing, err := tx.ListShiftsFrom(ctx, s.ID, now)
	if err != nil {
		return err
	}
	masks := map[string]int32{}
	if reset {
		kept := existing[:0:0]
		for _, sh := range existing {
			if sh.Source == SourceAuto && sh.Start.After(now) {
				masks[shiftKey(sh.Role, sh.UserID, sh.Start)] = sh.RemindedMask
				continue
			}
			kept = append(kept, sh)
		}
		existing = kept
		if err := tx.DeleteFutureAutoShifts(ctx, s.ID, now); err != nil {
			return err
		}
	}
	slots, until := rot.Plan(now, now.AddDate(0, 0, int(s.HorizonDays)))
	fill := avoidSameUser(fillSlots(slots, existing), existing, rot.Members)
	shifts := make([]*Shift, 0, len(fill))
	for _, sl := range fill {
		id, err := idgen.NewID()
		if err != nil {
			return fmt.Errorf("generate shift id: %w", err)
		}
		shifts = append(shifts, &Shift{
			ID: id, ScheduleID: s.ID, GroupID: s.GroupID, Role: sl.Role, UserID: sl.UserID,
			Start: sl.Start, End: sl.End, Source: SourceAuto,
			RemindedMask: masks[shiftKey(sl.Role, sl.UserID, sl.Start)],
		})
	}
	if err := tx.InsertShifts(ctx, shifts, actor); err != nil {
		return err
	}
	s.GeneratedUntil = &until
	return tx.SetGeneratedUntil(ctx, s.ID, &until)
}

func shiftKey(role int32, userID string, start time.Time) string {
	return fmt.Sprintf("%d|%s|%d", role, userID, start.Unix())
}

// List 查询值班表（groupID 为空 = 全部）。
func (uc *ScheduleUsecase) List(ctx context.Context, groupID string) ([]*Schedule, error) {
	return uc.schedules.List(ctx, groupID)
}

// Get 查询值班表详情。
func (uc *ScheduleUsecase) Get(ctx context.Context, id string) (*Schedule, error) {
	return uc.schedules.Get(ctx, id)
}

// Create 新建值班表并生成班次。
func (uc *ScheduleUsecase) Create(ctx context.Context, actor Actor, groupID string, cfg ScheduleConfig) (*Schedule, error) {
	id, err := idgen.NewID()
	if err != nil {
		return nil, fmt.Errorf("generate schedule id: %w", err)
	}
	s := &Schedule{ID: id, GroupID: groupID, Status: StatusEnabled}
	err = uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, groupID); err != nil {
			return err
		}
		n, err := tx.CountSchedules(ctx, groupID)
		if err != nil {
			return err
		}
		if n >= maxGroupSchedules {
			return detail(ErrScheduleInvalid, "a group can have at most %d schedules", maxGroupSchedules)
		}
		members, err := tx.ListMembers(ctx, groupID)
		if err != nil {
			return err
		}
		if err := cfg.apply(s, memberSet(members)); err != nil {
			return err
		}
		if err := tx.InsertSchedule(ctx, s, actor.UserID); err != nil {
			return err
		}
		if err := generate(ctx, tx, s, uc.now(), false, actor.UserID); err != nil {
			return err
		}
		// 新建 / 重排值班表不发通知：成员按上班提醒、每日播报获知排班。
		return writeLog(ctx, tx, actor, groupID, id, ActionScheduleCreate, "创建值班表「"+s.Name+"」", map[string]any{"after": scheduleSnapshot(s)})
	})
	if err != nil {
		return nil, err
	}
	return uc.schedules.Get(ctx, id)
}

// Update 修改值班表配置。轮转规则有变化时重排全部尚未开始的自动班次并通知名单成员；
// 进行中的班次保持到结束，已过去的班次与替班班次不受影响。
func (uc *ScheduleUsecase) Update(ctx context.Context, actor Actor, id string, cfg ScheduleConfig) (*Schedule, error) {
	cur, err := uc.schedules.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	err = uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, cur.GroupID); err != nil {
			return err
		}
		old, err := tx.LockSchedule(ctx, id)
		if err != nil {
			return err
		}
		members, err := tx.ListMembers(ctx, old.GroupID)
		if err != nil {
			return err
		}
		next := *old
		if err := cfg.apply(&next, memberSet(members)); err != nil {
			return err
		}
		if err := tx.UpdateSchedule(ctx, &next, actor.UserID); err != nil {
			return err
		}
		changed := rotationChanged(old, &next)
		if err := generate(ctx, tx, &next, uc.now(), changed, actor.UserID); err != nil {
			return err
		}
		return writeLog(ctx, tx, actor, old.GroupID, id, ActionScheduleUpdate, "修改值班表「"+next.Name+"」", map[string]any{
			"before": scheduleSnapshot(old), "after": scheduleSnapshot(&next), "rescheduled": changed,
		})
	})
	if err != nil {
		return nil, err
	}
	return uc.schedules.Get(ctx, id)
}

// SetStatus 启用/停用值班表。停用：进行中的班次在此刻结束，未开始的班次（含替班）
// 全部清除；启用：按当前配置重新生成。
func (uc *ScheduleUsecase) SetStatus(ctx context.Context, actor Actor, id string, status int32) error {
	cur, err := uc.schedules.Get(ctx, id)
	if err != nil {
		return err
	}
	return uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, cur.GroupID); err != nil {
			return err
		}
		s, err := tx.LockSchedule(ctx, id)
		if err != nil {
			return err
		}
		if s.Status == status {
			return nil
		}
		now := uc.now()
		if err := tx.SetScheduleStatus(ctx, id, status, actor.UserID); err != nil {
			return err
		}
		s.Status = status
		summary := "启用值班表「" + s.Name + "」"
		if status == StatusDisabled {
			summary = "停用值班表「" + s.Name + "」"
			if err := clearShiftsFrom(ctx, tx, id, now); err != nil {
				return err
			}
			if err := tx.SetGeneratedUntil(ctx, id, nil); err != nil {
				return err
			}
		} else if err := generate(ctx, tx, s, now, false, actor.UserID); err != nil {
			return err
		}
		return writeLog(ctx, tx, actor, s.GroupID, id, ActionScheduleStatus, summary, map[string]any{"status": status})
	})
}

// clearShiftsFrom 结束进行中的班次并清除全部未开始的班次（历史班次保留）。
func clearShiftsFrom(ctx context.Context, tx Tx, scheduleID string, now time.Time) error {
	if err := tx.DeleteShiftsFrom(ctx, scheduleID, now); err != nil {
		return err
	}
	return tx.TruncateShiftsAt(ctx, scheduleID, now)
}

// Delete 删除值班表：未开始的班次清除，公共地址随之作废。
func (uc *ScheduleUsecase) Delete(ctx context.Context, actor Actor, id string) error {
	cur, err := uc.schedules.Get(ctx, id)
	if err != nil {
		return err
	}
	return uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, cur.GroupID); err != nil {
			return err
		}
		s, err := tx.LockSchedule(ctx, id)
		if err != nil {
			return err
		}
		if err := clearShiftsFrom(ctx, tx, id, uc.now()); err != nil {
			return err
		}
		if err := tx.DeleteShareLinksByScope(ctx, ScopeSchedule, id, actor.UserID); err != nil {
			return err
		}
		if err := tx.DeleteSchedule(ctx, id, actor.UserID); err != nil {
			return err
		}
		return writeLog(ctx, tx, actor, s.GroupID, id, ActionScheduleDelete, "删除值班表「"+s.Name+"」", map[string]any{"before": scheduleSnapshot(s)})
	})
}

// scheduleSnapshot 变更记录里的配置快照。
func scheduleSnapshot(s *Schedule) map[string]any {
	return map[string]any{
		"name": s.Name, "timezone": s.Timezone, "rotation_unit": s.RotationUnit, "rotation_interval": s.RotationInterval,
		"anchor_time": s.AnchorTime.Format(time.RFC3339), "anchor_offset": s.AnchorOffset, "member_ids": s.MemberIDs,
		"backup_enabled": s.BackupEnabled, "horizon_days": s.HorizonDays, "remind_minutes": s.RemindMinutes,
		"digest_time": s.DigestTime,
	}
}

// Preview 用草稿配置计算未来 days 天的排班与异常，不写库。结果只反映轮转规则本身，
// 不含已有的替班与进行中的班次；跨表撞班与其它值班表的真实班次比对。
func (uc *ScheduleUsecase) Preview(ctx context.Context, groupID, scheduleID string, cfg ScheduleConfig, days int32) ([]*Shift, []*Issue, error) {
	if _, err := uc.groups.Get(ctx, groupID, ""); err != nil {
		return nil, nil, err
	}
	members, err := uc.groups.ListMembers(ctx, groupID)
	if err != nil {
		return nil, nil, err
	}
	set := memberSet(members)
	draft := &Schedule{ID: scheduleID, GroupID: groupID, Status: StatusEnabled}
	if err := cfg.apply(draft, set); err != nil {
		return nil, nil, err
	}
	rot, err := rotationOf(draft)
	if err != nil {
		return nil, nil, err
	}
	if days <= 0 {
		days = defaultPreviewDays
	}
	now := uc.now()
	to := now.AddDate(0, 0, int(days))
	slots, until := rot.Plan(now, to)
	draft.GeneratedUntil = &until
	preview := make([]*Shift, 0, len(slots))
	for _, sl := range slots {
		m := set[sl.UserID]
		preview = append(preview, &Shift{
			ScheduleID: scheduleID, ScheduleName: draft.Name, GroupID: groupID, Role: sl.Role, UserID: sl.UserID,
			UserName: memberName(m), UserAvailable: m.Available, Start: sl.Start, End: sl.End, Source: SourceAuto,
		})
	}

	others, err := uc.schedules.List(ctx, "")
	if err != nil {
		return nil, nil, err
	}
	others = slices.DeleteFunc(others, func(s *Schedule) bool { return scheduleID != "" && s.ID == scheduleID })
	real, err := uc.shifts.ListRange(ctx, now, until)
	if err != nil {
		return nil, nil, err
	}
	real = slices.DeleteFunc(real, func(sh *Shift) bool { return scheduleID != "" && sh.ScheduleID == scheduleID })
	all := append(real, preview...)
	sortShifts(all)
	issues := detectIssues(detectInput{Schedules: append(others, draft), Shifts: all, From: now, To: to})
	issues = slices.DeleteFunc(issues, func(is *Issue) bool { return is.ScheduleID != scheduleID })
	return preview, capIssues(issues), nil
}

// sortShifts 按开始时间排序（稳定）。
func sortShifts(shifts []*Shift) {
	slices.SortStableFunc(shifts, func(a, b *Shift) int { return a.Start.Compare(b.Start) })
}

// ShiftQuery 班次查询条件。
type ShiftQuery struct {
	From       time.Time
	To         time.Time
	GroupID    string
	ScheduleID string
	UserID     string
}

// ListShifts 查询与 [From, To) 相交的班次，并返回范围内尚未过去的排班异常。
func (uc *ScheduleUsecase) ListShifts(ctx context.Context, q ShiftQuery) ([]*Shift, []*Issue, error) {
	if !q.From.Before(q.To) || q.To.Sub(q.From) > maxShiftRange {
		return nil, nil, detail(ErrTimeRangeInvalid, "time range must be positive and within 100 days")
	}
	schedules, err := uc.schedules.List(ctx, "")
	if err != nil {
		return nil, nil, err
	}
	// 异常检测需要全量班次（跨表撞班要看其它分组）并向两侧回看（连续值班判定）。
	look := issueLookback(schedules)
	all, err := uc.shifts.ListRange(ctx, q.From.Add(-look), q.To.Add(look))
	if err != nil {
		return nil, nil, err
	}
	match := func(groupID, scheduleID string) bool {
		return (q.GroupID == "" || groupID == q.GroupID) && (q.ScheduleID == "" || scheduleID == q.ScheduleID)
	}
	items := make([]*Shift, 0, len(all))
	for _, sh := range all {
		if sh.End.After(q.From) && sh.Start.Before(q.To) && match(sh.GroupID, sh.ScheduleID) && (q.UserID == "" || sh.UserID == q.UserID) {
			items = append(items, sh)
		}
	}
	issues := detectIssues(detectInput{Schedules: schedules, Shifts: all, From: maxTime(q.From, uc.now()), To: q.To})
	issues = slices.DeleteFunc(issues, func(is *Issue) bool {
		return !match(is.GroupID, is.ScheduleID) || (q.UserID != "" && is.UserID != q.UserID)
	})
	return items, capIssues(issues), nil
}

// OverrideInput 替班入参。
type OverrideInput struct {
	ScheduleID string
	Role       int32
	UserID     string
	Start      time.Time
	End        time.Time
	Remark     string
}

// onHandover 报告 t 是否落在值班表每天的交接时刻上（值班表时区下与轮转起点同一钟面时刻）。
func onHandover(s *Schedule, loc *time.Location, t time.Time) bool {
	ah, am, as := s.AnchorTime.In(loc).Clock()
	th, tm, ts := t.In(loc).Clock()
	return ah == th && am == tm && as == ts
}

// CreateOverride 替班：把 [Start, End) 内 Role 的值班人换成 UserID。值班以天为最小单位：
// Start / End 须落在值班表每天的交接时刻上（即替整数天）。被覆盖的班次按边界切开，替班
// 班次记录原值班人且不会被重排覆盖。开始时间早于当前（替的是已经开始的这一天）按当前时间处理。
func (uc *ScheduleUsecase) CreateOverride(ctx context.Context, actor Actor, in OverrideInput) ([]*Shift, error) {
	cur, err := uc.schedules.Get(ctx, in.ScheduleID)
	if err != nil {
		return nil, err
	}
	var created []*Shift
	err = uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, cur.GroupID); err != nil {
			return err
		}
		s, err := tx.LockSchedule(ctx, in.ScheduleID)
		if err != nil {
			return err
		}
		now := uc.now()
		loc, err := loadLocation(s.Timezone)
		if err != nil {
			return err
		}
		reqStart, end := in.Start.UTC().Truncate(time.Second), in.End.UTC().Truncate(time.Second)
		start := maxTime(reqStart, now)
		switch {
		case s.Status != StatusEnabled || s.GeneratedUntil == nil:
			return detail(ErrOverrideInvalid, "schedule is disabled")
		case !onHandover(s, loc, reqStart) || !onHandover(s, loc, end):
			return detail(ErrOverrideInvalid, "start and end time must fall on the daily handover time (%s, %s)",
				s.AnchorTime.In(loc).Format("15:04"), s.Timezone)
		case in.Role == RoleBackup && !s.BackupEnabled:
			return detail(ErrOverrideInvalid, "schedule has no backup role")
		case !start.Before(end):
			return detail(ErrOverrideInvalid, "end time must be after start time and in the future")
		case end.After(*s.GeneratedUntil):
			return detail(ErrOverrideInvalid, "time range exceeds the generated schedule (until %s)", s.GeneratedUntil.Format(time.RFC3339))
		}
		members, err := tx.ListMembers(ctx, s.GroupID)
		if err != nil {
			return err
		}
		if m := memberSet(members)[in.UserID]; m == nil || !m.Available {
			return detail(ErrOverrideInvalid, "substitute must be an available member of this group")
		}
		existing, err := tx.ListShiftsFrom(ctx, s.ID, start)
		if err != nil {
			return err
		}
		// 主备值班人不能是同一个人：这段时间内另一角色已是该成员时拒绝。
		for _, e := range existing {
			if e.Role != in.Role && e.UserID == in.UserID && e.Start.Before(end) && e.End.After(start) {
				return detail(ErrOverrideInvalid, "this member is already the %s in this period; primary and backup must be different people",
					map[int32]string{RolePrimary: "primary", RoleBackup: "backup"}[e.Role])
			}
		}
		plan := planOverride(s, existing, in.Role, in.UserID, start, end, strings.TrimSpace(in.Remark))
		if len(plan.Created) == 0 {
			return detail(ErrOverrideInvalid, "this member is already on duty for the whole time range")
		}
		for _, id := range plan.Deletes {
			if err := tx.DeleteShift(ctx, id); err != nil {
				return err
			}
		}
		for _, sh := range plan.Updates {
			if err := tx.UpdateShiftRange(ctx, sh.ID, sh.Start, sh.End, actor.UserID); err != nil {
				return err
			}
		}
		for _, sh := range plan.Inserts {
			if sh.ID, err = idgen.NewID(); err != nil {
				return fmt.Errorf("generate shift id: %w", err)
			}
		}
		if err := tx.InsertShifts(ctx, plan.Inserts, actor.UserID); err != nil {
			return err
		}
		created = plan.Created
		if err := writeLog(ctx, tx, actor, s.GroupID, s.ID, ActionShiftOverride, "替班："+s.Name+" "+RoleLabel(in.Role), map[string]any{
			"role": in.Role, "user_id": in.UserID, "origin_user_ids": plan.Origins,
			"start_time": start.Format(time.RFC3339), "end_time": end.Format(time.RFC3339), "remark": in.Remark,
		}); err != nil {
			return err
		}
		return uc.notify.shiftChanged(ctx, tx, s, shiftChange{
			Role: in.Role, UserID: in.UserID, Origins: plan.Origins, Start: start, End: end, Remark: in.Remark,
		})
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

// RevokeOverride 撤销替班：未开始的替班整段删除，进行中的替班在此刻结束，腾出的时间
// 按轮转规则恢复为自动排班；已结束的替班不可撤销。
func (uc *ScheduleUsecase) RevokeOverride(ctx context.Context, actor Actor, shiftID string) error {
	cur, err := uc.shifts.Get(ctx, shiftID)
	if err != nil {
		return err
	}
	return uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, cur.GroupID); err != nil {
			return err
		}
		s, err := tx.LockSchedule(ctx, cur.ScheduleID)
		if err != nil {
			return err
		}
		sh, err := tx.GetShift(ctx, shiftID)
		if err != nil {
			return err
		}
		now := uc.now()
		switch {
		case sh.Source != SourceOverride:
			return detail(ErrOverrideInvalid, "this shift is not an override")
		case !sh.End.After(now):
			return detail(ErrOverrideInvalid, "this override has already ended")
		}
		from := maxTime(sh.Start, now)
		if sh.Start.Before(now) {
			err = tx.UpdateShiftRange(ctx, sh.ID, sh.Start, now, actor.UserID)
		} else {
			err = tx.DeleteShift(ctx, sh.ID)
		}
		if err != nil {
			return err
		}
		if err := generate(ctx, tx, s, now, false, actor.UserID); err != nil {
			return err
		}
		if err := writeLog(ctx, tx, actor, s.GroupID, s.ID, ActionShiftRevoke, "撤销替班："+s.Name+" "+RoleLabel(sh.Role), map[string]any{
			"role": sh.Role, "user_id": sh.UserID, "origin_user_id": sh.OriginUserID,
			"start_time": from.Format(time.RFC3339), "end_time": sh.End.Format(time.RFC3339),
		}); err != nil {
			return err
		}
		var origins []string
		if sh.OriginUserID != "" {
			origins = []string{sh.OriginUserID}
		}
		return uc.notify.shiftChanged(ctx, tx, s, shiftChange{
			Role: sh.Role, UserID: sh.UserID, Origins: origins, Start: from, End: sh.End, Revoked: true,
		})
	})
}

// Duty 某张值班表此刻的值班情况。
type Duty struct {
	Schedule *Schedule
	Primary  *Shift
	Backup   *Shift
}

// Current 查询此刻各启用值班表的值班人；两个条件都为空时返回全部。
func (uc *ScheduleUsecase) Current(ctx context.Context, groupID, scheduleID string) ([]*Duty, error) {
	schedules, err := uc.schedules.List(ctx, groupID)
	if err != nil {
		return nil, err
	}
	now := uc.now()
	shifts, err := uc.shifts.ListRange(ctx, now, now.Add(time.Second))
	if err != nil {
		return nil, err
	}
	duties := make([]*Duty, 0, len(schedules))
	index := make(map[string]*Duty, len(schedules))
	for _, s := range schedules {
		if s.Status != StatusEnabled || (scheduleID != "" && s.ID != scheduleID) {
			continue
		}
		d := &Duty{Schedule: s}
		duties = append(duties, d)
		index[s.ID] = d
	}
	for _, sh := range shifts {
		d := index[sh.ScheduleID]
		if d == nil || sh.Start.After(now) {
			continue
		}
		if sh.Role == RolePrimary {
			d.Primary = sh
		} else {
			d.Backup = sh
		}
	}
	return duties, nil
}
