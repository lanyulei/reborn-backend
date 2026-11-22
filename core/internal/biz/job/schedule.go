package biz

import (
	"context"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/robfig/cron/v3"
)

const (
	// scheduleBatch 一轮处理的到期定时作业数。
	scheduleBatch = 20
	// previewTimes 预览的触发时间个数。
	previewTimes = 5
)

// cronParser 标准 5 段 Cron（分 时 日 月 周）；不接受秒字段与 @every 之类的描述符——最短间隔 1 分钟。
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// parseCron 解析 Cron 表达式与时区。
func parseCron(expr, timezone string) (cron.Schedule, *time.Location, error) {
	// 表达式里不接受 CRON_TZ= / TZ= 前缀：时区只由 timezone 字段决定。
	if strings.Contains(expr, "=") {
		return nil, nil, detail(ErrScheduleInvalid, "invalid cron expression: timezone prefixes are not allowed")
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil || timezone == "" || timezone == "Local" {
		return nil, nil, detail(ErrScheduleInvalid, "unknown timezone %q", timezone)
	}
	sched, err := cronParser.Parse(expr)
	if err != nil {
		return nil, nil, detail(ErrScheduleInvalid, "invalid cron expression (expect 5 fields: minute hour day month weekday): %s", err.Error())
	}
	return sched, loc, nil
}

// nextAfter 表达式在 after 之后的下一次触发时间；永不触发（如 2 月 30 日）返回错误。
func nextAfter(sched cron.Schedule, loc *time.Location, after time.Time) (time.Time, error) {
	next := sched.Next(after.In(loc))
	if next.IsZero() {
		return time.Time{}, detail(ErrScheduleInvalid, "the cron expression never fires")
	}
	return next, nil
}

// ScheduleUsecase 定时作业用例。
type ScheduleUsecase struct {
	repo      ScheduleRepo
	queue     ScheduleQueue
	templates TemplateReader
	execs     *ExecutionUsecase
	dir       Directory
}

// NewScheduleUsecase new a ScheduleUsecase.
func NewScheduleUsecase(repo ScheduleRepo, queue ScheduleQueue, templates TemplateReader, execs *ExecutionUsecase, dir Directory) *ScheduleUsecase {
	return &ScheduleUsecase{repo: repo, queue: queue, templates: templates, execs: execs, dir: dir}
}

// List 分页查询定时作业。
func (uc *ScheduleUsecase) List(ctx context.Context, filter ScheduleFilter, page, pageSize int64) ([]*Schedule, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	items, total, err := uc.repo.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	return items, total, uc.fillNames(ctx, items)
}

func (uc *ScheduleUsecase) fillNames(ctx context.Context, items []*Schedule) error {
	ids := make([]string, 0, len(items))
	for _, s := range items {
		ids = append(ids, s.Owner)
	}
	names, err := uc.dir.UserNames(ctx, uniq(ids))
	if err != nil {
		return err
	}
	for _, s := range items {
		s.OwnerName = names[s.Owner]
	}
	return nil
}

func (uc *ScheduleUsecase) get(ctx context.Context, id string) (*Schedule, error) {
	s, err := uc.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return s, uc.fillNames(ctx, []*Schedule{s})
}

// Preview 校验 Cron 表达式并返回接下来的几个触发时间。
func (uc *ScheduleUsecase) Preview(_ context.Context, expr, timezone string) ([]time.Time, error) {
	sched, loc, err := parseCron(strings.TrimSpace(expr), strings.TrimSpace(timezone))
	if err != nil {
		return nil, err
	}
	out := make([]time.Time, 0, previewTimes)
	at := time.Now()
	for range previewTimes {
		if at, err = nextAfter(sched, loc, at); err != nil {
			break // 之后不再触发（如指定了已过去的日期组合）
		}
		out = append(out, at)
	}
	if len(out) == 0 {
		return nil, err
	}
	return out, nil
}

// normalize 规整并校验定时作业：Cron 与时区可解析、模板存在、参数取值合乎模板的参数定义；
// 启用时算出下次触发时间。
func (uc *ScheduleUsecase) normalize(ctx context.Context, s *Schedule) error {
	s.Name, s.Description = strings.TrimSpace(s.Name), strings.TrimSpace(s.Description)
	s.Cron, s.Timezone = strings.Join(strings.Fields(s.Cron), " "), strings.TrimSpace(s.Timezone)
	if s.Name == "" {
		return detail(ErrScheduleInvalid, "name must not be blank")
	}
	if s.Misfire != jc.MisfireSkip && s.Misfire != jc.MisfireRunOnce {
		return detail(ErrScheduleInvalid, "unknown misfire policy")
	}
	sched, loc, err := parseCron(s.Cron, s.Timezone)
	if err != nil {
		return err
	}
	s.NextTime = nil
	tpl, err := uc.templates.Get(ctx, s.TemplateID)
	if err != nil {
		// 停用必须总能成功：模板已被删除的定时作业也要停得下来（内容原样保存，反正不会再触发）。
		if !s.Enabled && errorsIs(err, ErrTemplateNotFound) {
			return nil
		}
		return missingScheduleRef(err)
	}
	if s.TemplateVersion, err = confirmTemplate(ErrScheduleInvalid, tpl, s.TemplateVersion, s.Enabled); err != nil {
		return err
	}
	vars, err := resolveParams(tpl.Params, s.Params)
	if err != nil {
		if !s.Enabled {
			return nil // 同上：模板改过后参数对不上，也不妨碍停用
		}
		return detail(ErrScheduleInvalid, "%s", Message(err))
	}
	// 只存调用方给出的键（缺省值在每次触发时按模板当时的定义补全）。
	fixed := make(map[string]any, len(s.Params))
	for k := range s.Params {
		if v, ok := vars[k]; ok {
			fixed[k] = v
		}
	}
	s.Params = fixed
	if s.Enabled {
		next, err := nextAfter(sched, loc, time.Now())
		if err != nil {
			return err
		}
		s.NextTime = &next
	}
	return nil
}

// confirmTemplate 算出定时作业 / 触发器要钉住的模板版本。
//
// seen 是保存人在页面上看到的模板版本：0（未提供）取当前版本；与当前版本一致照常；不一致说明他确认的
// 是旧内容——启用时拒绝（否则列表上的启停开关就能在没人看过新内容的情况下「确认」它），停用时保留
// 旧版本号（停用不构成确认，之后再启用仍须核对）。
func confirmTemplate(invalid *errors.Error, tpl *Template, seen int32, enabled bool) (int32, error) {
	switch {
	case seen == 0 || seen == tpl.Version:
		return tpl.Version, nil
	case enabled:
		return 0, detail(invalid, "the job template changed (v%d → v%d) since this was last saved; open it, review the template and save again", seen, tpl.Version)
	}
	return seen, nil
}

func missingScheduleRef(err error) error {
	if errorsIs(err, ErrTemplateNotFound) {
		return detail(ErrScheduleInvalid, "the selected job template does not exist")
	}
	return err
}

// Create 新建定时作业；创建人即负责人。
func (uc *ScheduleUsecase) Create(ctx context.Context, userID string, s *Schedule) (*Schedule, error) {
	// 保存人即负责人：他自己不能执行作业，就不能让定时 / 触发替他执行。
	if err := uc.execs.RequireRun(ctx, userID); err != nil {
		return nil, err
	}
	if err := uc.normalize(ctx, s); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	s.ID = id
	if err := uc.repo.Write(ctx, OpCreate, s, 0, userID); err != nil {
		return nil, err
	}
	return uc.get(ctx, id)
}

// Update 编辑定时作业（整体替换）；负责人变更为当前操作人——之后的触发以其身份与数据范围执行，
// 不会出现「改了别人的定时作业、却以别人的权限执行」。
func (uc *ScheduleUsecase) Update(ctx context.Context, userID string, s *Schedule, version int32) (*Schedule, error) {
	// 保存人即负责人：他自己不能执行作业，就不能让定时 / 触发替他执行。停用不需要：有编辑权限的人
	// 必须总能把一个失控的定时作业停下来（再次启用时仍会检查启用人）。
	if s.Enabled {
		if err := uc.execs.RequireRun(ctx, userID); err != nil {
			return nil, err
		}
	}
	if err := uc.normalize(ctx, s); err != nil {
		return nil, err
	}
	if err := uc.repo.Write(ctx, OpUpdate, s, version, userID); err != nil {
		return nil, err
	}
	return uc.get(ctx, s.ID)
}

// Delete 删除定时作业。
func (uc *ScheduleUsecase) Delete(ctx context.Context, userID, id string) error {
	return uc.repo.Write(ctx, OpDelete, &Schedule{ID: id}, 0, userID)
}

// Run 触发到期的定时作业（后台循环；多实例并发安全）。返回本轮由本实例触发的条数。
//
// 每条到期的定时作业先由仓储认领（行锁 + SKIP LOCKED，并复核 next_time 仍是读到的计划时间）：多实例下
// 只有一个实例拿到它，其余跳过，不再各自都去提交一遍。持锁期间算出下一次触发时间、以「定时作业 ID +
// 计划时间」为幂等键提交执行，再随锁一起落账推进 next_time。提交被业务规则拒绝（模板停用、负责人无
// 权限等）记为本次触发失败，照常推进到下一次；数据库等故障整体回滚，下一轮重做（幂等键保证即使提交
// 已成功也不会再建一条）。
func (uc *ScheduleUsecase) Run(ctx context.Context) (int, error) {
	items, now, err := uc.queue.Due(ctx, scheduleBatch)
	if err != nil {
		return 0, err
	}
	// 一条出故障不挡住后面的：逐条处理，最后返回遇到的第一个错误（循环据此退避）。
	done := 0
	var first error
	for _, s := range items {
		if s.NextTime == nil {
			continue
		}
		ok, err := uc.queue.Fire(ctx, s.ID, *s.NextTime, func(ctx context.Context, cur *Schedule) (ScheduleRun, error) {
			return uc.fire(ctx, cur, now)
		})
		if err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		if ok {
			done++
		}
	}
	return done, first
}

// fire 算出一次触发的结果（持锁期间调用）：下一次触发时间、本次提交的执行或失败原因。
func (uc *ScheduleUsecase) fire(ctx context.Context, s *Schedule, now time.Time) (ScheduleRun, error) {
	planned := *s.NextTime
	run := ScheduleRun{Fired: planned}
	sched, loc, err := parseCron(s.Cron, s.Timezone)
	if err == nil {
		run.Next, err = nextAfter(sched, loc, now)
	}
	if err != nil {
		// 存量的表达式不再可解析 / 不再触发：记下原因并停在一天之后再看，不让它每轮都被取出来。
		run.Next, run.Error = now.Add(24*time.Hour), truncateRunes(Message(err), 512)
		return run, nil
	}
	// 错过的触发：计划时间之后已经过了两个以上周期（服务停机等；晚一个周期以内照常触发——循环间隔、锁等待
	// 与短暂停机不该让作业被判为错过）。按策略跳过或补跑一次——无论错过多少次都只补一次。
	missed := sched.Next(sched.Next(planned.In(loc))).Before(now)
	if missed && s.Misfire == jc.MisfireSkip {
		run.Error = "错过触发时间，已跳过"
		return run, nil
	}
	exec, err := uc.execs.Submit(ctx, s.Owner, SubmitInput{
		TemplateID: s.TemplateID, TemplateVersion: s.TemplateVersion, Params: s.Params, Trigger: jc.TriggerSchedule, TriggerRef: s.ID,
		RequestID: "sch-" + strings.ReplaceAll(s.ID, "-", "") + "-" + planned.UTC().Format("20060102T150405"),
	})
	switch {
	case err == nil:
		run.ExecutionID = exec.ID
	case Rejected(err):
		run.Error = truncateRunes(Message(err), 512)
	default:
		return run, err
	}
	return run, nil
}
