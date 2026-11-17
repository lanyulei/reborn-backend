package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
	"unicode/utf8"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 字段权限取值（流程节点的 formPermissions）；hidden 的判定在流程引擎一侧（FormAccess.Hidden）。
const (
	permEditable = "editable"
	permRequired = "required"
)

const (
	// EventSLABreached SLA 超时通知的事件类型（通知域按它匹配路由与模板）。
	EventSLABreached = "ticket.sla.breached"
	// slaBatch 单轮巡检处理的超时条数。
	slaBatch = 50
	// slaSeverity SLA 超时通知的事件级别（warning）。
	slaSeverity int32 = 2
	// notifyChunk 单个通知事件的显式收件上限（与 notify 入参约束一致）。
	notifyChunk = 20
	// maxNoticeTitle 通知标题的字节上限（通知域限 255 字节，留出余量）。
	maxNoticeTitle = 240
	// defaultStatsDays / maxStatsDays 统计窗口的缺省与上限（天）。
	defaultStatsDays = 30
	maxStatsDays     = 366
	// statsTTL / statsCacheSize 统计结果的进程内缓存时长与条数上限。
	statsTTL       = 30 * time.Second
	statsCacheSize = 64
	// slaMaxSkips 单轮巡检至多新发现几张处理失败的工单；到数即整轮报错等下一轮（已发现的
	// 记在 slaParked 里，下一轮直接绕开，所以失败的工单再多也只是多花几轮，不会卡住其余的）。
	slaMaxSkips = 5
	// slaParkFor 处理失败的工单暂缓多久再试；slaParkedMax 暂缓名单的容量上限。
	slaParkFor   = 10 * time.Minute
	slaParkedMax = 1000
)

// applySLA 按模板里与工单优先级匹配的 SLA 规则算出响应 / 解决时限，并记下升级通知对象。
func (uc *Usecase) applySLA(ctx context.Context, tpl *Template, t *Ticket) error {
	i := slices.IndexFunc(tpl.SLA, func(r SLARule) bool { return r.Priority == t.Priority })
	if i < 0 {
		return nil
	}
	rule := tpl.SLA[i]
	var err error
	if rule.Response != nil {
		if t.ResponseDue, err = uc.ext.Deadlines.Due(ctx, t.CreateTime, *rule.Response); err != nil {
			return err
		}
	}
	if rule.Resolve != nil {
		if t.ResolveDue, err = uc.ext.Deadlines.Due(ctx, t.CreateTime, *rule.Resolve); err != nil {
			return err
		}
	}
	t.SLAEscalate = tpl.SLAEscalate
	return nil
}

// checkResourceLink 按模板的关联资源要求核对提单携带的资源数：不允许关联时不得携带、
// 要求必填时至少一个。
func checkResourceLink(tpl *Template, ids []string) error {
	switch {
	case len(ids) > maxResources || (tpl.CILink == CILinkNone && len(ids) > 0):
		return detail(ErrFormInvalid, "this template does not accept linked resources")
	case tpl.CILink == CILinkRequired && len(ids) == 0:
		return detail(ErrFormInvalid, "at least one resource must be linked")
	}
	return nil
}

// prepareResources 在提单事务之外校验并解析关联的 CMDB 资源：每一个都须是提单人有权查看的
// 资源。必须在事务外调用（见 create 的说明）；模板不存在 / 已停用留给事务内统一报出。
func (uc *Usecase) prepareResources(ctx context.Context, p CreateParams) ([]Resource, error) {
	ids := p.ResourceIDs
	if len(ids) == 0 {
		return nil, nil
	}
	tpl, err := uc.catalog.GetTemplate(ctx, p.TemplateID)
	if err != nil {
		if errors.Is(err, ErrTemplateNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if tpl.Status != TemplateEnabled {
		return nil, nil
	}
	if err := checkResourceLink(tpl, ids); err != nil {
		return nil, err
	}
	resources, err := uc.ext.Resources.Resolve(ctx, p.Creator, ids)
	if err != nil {
		return nil, err
	}
	if len(resources) != len(ids) {
		return nil, detail(ErrFormInvalid, "a linked resource does not exist or is not accessible to you")
	}
	return resources, nil
}

// applyFormAccess 按查看者在流程上的身份处理表单：提单人与管理视图看到全部；只是处理人 /
// 抄送人时，去掉对他隐藏的字段（定义与取值都不返回）。他当前有待处理的任务时带上各字段
// 的权限，供页面决定哪些可改。
func (uc *Usecase) applyFormAccess(ctx context.Context, d *Detail, userID string) error {
	t := d.Ticket
	if t.InstanceID == "" {
		return nil
	}
	fa, err := uc.ext.Flows.FormAccess(ctx, t.InstanceID, userID)
	if err != nil {
		return err
	}
	if fa.Pending {
		d.Permissions = fa.Permissions
		d.CanEditForm = slices.ContainsFunc(t.Form, func(f FormField) bool {
			p := fa.Permissions[f.Code]
			return p == permEditable || p == permRequired
		})
	}
	if t.Creator == userID || !fa.Seated || (len(fa.Hidden) == 0 && !fa.HideAll) {
		return nil
	}
	all, err := uc.access.supervisor.CanViewAll(ctx, userID)
	if err != nil || all {
		return err
	}
	if fa.HideAll {
		// 流程图已无法解析，判断不了哪些字段该对他隐藏：整张表单都不给（宁可少给）。
		t.Form, t.Data = nil, map[string]any{}
		return nil
	}
	t.Form = slices.DeleteFunc(slices.Clone(t.Form), func(f FormField) bool { return slices.Contains(fa.Hidden, f.Code) })
	t.Data = maps.Clone(t.Data)
	for _, code := range fa.Hidden {
		delete(t.Data, code)
	}
	return nil
}

// UpdateForm 当前处理人补充 / 修改表单：只能改流程当前环节标为可编辑 / 必填的字段。改动在
// 锁住流程实例的同一事务里写入工单并同步到流程变量。
func (uc *Usecase) UpdateForm(ctx context.Context, id, userID string, data map[string]any) error {
	t, err := uc.access.visible(ctx, id, userID)
	if err != nil {
		return err
	}
	err = uc.ext.Flows.Amend(ctx, t.InstanceID, userID, func(ctx context.Context, tx txrun.Tx, perms map[string]string) (map[string]any, error) {
		form, current, err := uc.ext.Ops.LockFormTx(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		// 按改动后的取值判断显示条件：不显示的字段不能改。
		preview := maps.Clone(current)
		if preview == nil {
			preview = map[string]any{}
		}
		maps.Copy(preview, data)
		hidden := hiddenFields(form, preview)
		editable := make([]FormField, 0, len(form))
		for _, f := range form {
			if p := perms[f.Code]; (p == permEditable || p == permRequired) && !hidden[f.Code] {
				editable = append(editable, f)
			}
		}
		if len(editable) == 0 {
			return nil, ErrFormLocked
		}
		// 只按可编辑的字段校验：传了别的字段即「未定义的键」被拒绝。
		if err := checkRaw(editable, data); err != nil {
			return nil, err
		}
		changed, err := formschema.Validate(schemaFields(editable), data, true)
		if err != nil {
			var fe *formschema.Error
			if errors.As(err, &fe) {
				return nil, detail(ErrFormInvalid, fe.Error())
			}
			return nil, err
		}
		// 模板里必填的字段可以改，但不能清空（提单时就必须有值）。
		for _, f := range editable {
			if _, sent := data[f.Code]; sent && f.Required && empty(changed[f.Code]) {
				return nil, detail(ErrFormInvalid, fmt.Sprintf("field %q is required and cannot be cleared", f.Name))
			}
		}
		merged := maps.Clone(current)
		if merged == nil {
			merged = map[string]any{}
		}
		vars := make(map[string]any, len(data))
		for code := range data {
			if v, ok := changed[code]; ok {
				merged[code], vars[code] = v, v
			} else { // 传 null：清空
				delete(merged, code)
				vars[code] = nil
			}
		}
		// 改动让某些字段不再显示：它们的值一并清掉（流程变量同步），不留下看不见的旧值。
		for code := range hiddenFields(form, merged) {
			if _, has := merged[code]; has {
				delete(merged, code)
				vars[code] = nil
			}
		}
		if _, err := validateSize(merged); err != nil {
			return nil, err
		}
		if err := uc.ext.Ops.SetDataTx(ctx, tx, id, merged); err != nil {
			return nil, err
		}
		return vars, nil
	})
	if errors.Is(err, bizworkflow.ErrTaskNotAssignee) {
		return ErrFormLocked
	}
	return err
}

// validateSize 表单数据的大小与字符约束（与提单时一致）。
func validateSize(data map[string]any) ([]byte, error) {
	raw, err := json.Marshal(data)
	if err != nil || len(raw) > maxDataBytes {
		return nil, detail(ErrFormInvalid, "form data is too large")
	}
	return raw, nil
}

// Rate 提单人评价已结束的工单（每张一次）。
func (uc *Usecase) Rate(ctx context.Context, id, userID string, rating int32, comment string) error {
	ok, err := uc.ext.Insight.Rate(ctx, id, userID, rating, clean(comment))
	if err != nil {
		return err
	}
	if !ok {
		return ErrRatingUnavailable
	}
	return nil
}

// statsKey 统计缓存的键：窗口天数 + 分日时区。
type statsKey struct {
	days int32
	zone string
}

// statsEntry 一份缓存的统计结果（只读，过期后整体替换）。
type statsEntry struct {
	stats *Stats
	at    time.Time
}

// Stats 最近 days 天提交的工单的统计。zone 为趋势分日所用的时区（IANA 名称，如 Asia/Shanghai，
// 由页面传浏览器所在时区，这样「哪一天」与页面按本地日期补齐的日历一致）；窗口从该时区
// days-1 天前的零点开始，即恰好 days 个完整的自然日。zone 为空时沿用旧口径：按数据库会话
// 时区分日，窗口从 days-1 天前的此刻开始。
//
// 结果在进程内缓存 statsTTL：统计看板的每次打开都是对窗口内全部工单的一次聚合，短暂的
// 缓存把反复刷新 / 多人同时查看的开销摊成一次，代价是数字至多滞后 statsTTL。
func (uc *Usecase) Stats(ctx context.Context, days int32, zone string) (*Stats, error) {
	if days <= 0 {
		days = defaultStatsDays
	}
	days = min(days, maxStatsDays)
	now := time.Now()
	since := now.AddDate(0, 0, -int(days)+1)
	if zone != "" {
		loc, err := time.LoadLocation(zone)
		// "Local" 是 Go 进程自己的时区别名，数据库不认识。
		if err != nil || zone == "Local" {
			return nil, ErrTimezoneUnknown
		}
		// 规范成该时区的名称（LoadLocation 接受的名称即 IANA 名称，PostgreSQL 同样认识）。
		zone = loc.String()
		local := now.In(loc)
		since = time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, -int(days)+1)
	}
	key := statsKey{days: days, zone: zone}
	uc.statsMu.Lock()
	entry, ok := uc.statsCache[key]
	uc.statsMu.Unlock()
	if ok && now.Sub(entry.at) < statsTTL {
		return entry.stats, nil
	}
	st, err := uc.ext.Insight.Stats(ctx, since, zone)
	if err != nil {
		return nil, err
	}
	st.Timezone = zone
	uc.statsMu.Lock()
	if uc.statsCache == nil || len(uc.statsCache) >= statsCacheSize {
		// 键空间有界（天数 × 时区），放满了整体清空即可，不值得做逐项淘汰。
		uc.statsCache = make(map[statsKey]statsEntry)
	}
	uc.statsCache[key] = statsEntry{stats: st, at: now}
	uc.statsMu.Unlock()
	return st, nil
}

// breachError 处理某一条 SLA 超时失败：带上工单 ID，巡检据此跳过它重试其余的。
type breachError struct {
	ticketID string
	err      error
}

func (e *breachError) Error() string { return e.err.Error() }
func (e *breachError) Unwrap() error { return e.err }

// NotifySLA 巡检 SLA：为已超时且尚未通知的工单各发一条通知（当前处理人 + 升级对象；都没有
// 时发给流程管理员）。返回处理的条数（后台循环据此决定是否立即再跑一轮）。领取、打标记与
// 通知事件的写入在同一事务里：要么都生效，要么都不生效，多实例并发也不会重复通知。
//
// 某一条处理失败（如它的流程数据读不出来）会让整批回滚：这时记日志、把它暂缓（本进程内
// 记住，slaParkFor 之后再试）并排除在外再领一批。暂缓名单跨轮次保留，所以有问题的工单
// 无论多少张，都不会把其余工单的通知卡住——每轮至多新暂缓 slaMaxSkips 张，几轮之后即绕开。
func (uc *Usecase) NotifySLA(ctx context.Context) (int, error) {
	skip := uc.slaSkips(time.Now())
	for found := 0; ; found++ {
		n, err := uc.notifySLABatch(ctx, skip)
		if err == nil {
			return n, nil
		}
		var be *breachError
		if ctx.Err() != nil || !errors.As(err, &be) {
			return 0, err
		}
		uc.ext.Log.ErrorContext(ctx, "ticket SLA notification failed; parking the ticket",
			"ticket", be.ticketID, "retry_after", slaParkFor.String(), "error", be.err)
		uc.slaPark(be.ticketID, time.Now())
		if found+1 >= slaMaxSkips {
			return 0, err
		}
		skip = append(skip, be.ticketID)
	}
}

// slaSkips 当前仍在暂缓期内的工单（到期的顺带清掉）。
func (uc *Usecase) slaSkips(now time.Time) []string {
	uc.slaMu.Lock()
	defer uc.slaMu.Unlock()
	skip := make([]string, 0, len(uc.slaParked))
	for id, until := range uc.slaParked {
		if now.Before(until) {
			skip = append(skip, id)
		} else {
			delete(uc.slaParked, id)
		}
	}
	slices.Sort(skip)
	return skip
}

// slaPark 暂缓一张处理失败的工单。名单已满时清空重来（宁可多试几次，不无限占内存）。
func (uc *Usecase) slaPark(id string, now time.Time) {
	uc.slaMu.Lock()
	defer uc.slaMu.Unlock()
	if uc.slaParked == nil || len(uc.slaParked) >= slaParkedMax {
		uc.slaParked = map[string]time.Time{}
	}
	uc.slaParked[id] = now.Add(slaParkFor)
}

// notifySLABatch 在一个事务里领取并通知一批 SLA 超时（skip 中的工单不领取）。
func (uc *Usecase) notifySLABatch(ctx context.Context, skip []string) (int, error) {
	n := 0
	err := uc.txs.WithinTx(ctx, func(tx txrun.Tx) error {
		ctx := txrun.NewContext(ctx, tx)
		breaches, err := uc.ext.Insight.ClaimBreachesTx(ctx, tx, slaBatch, skip)
		if err != nil {
			return err
		}
		n = len(breaches)
		for _, b := range breaches {
			if err := uc.notifyBreach(ctx, tx, b); err != nil {
				return &breachError{ticketID: b.TicketID, err: err}
			}
		}
		return nil
	})
	return n, err
}

// notifyBreach 为一条 SLA 超时写入通知事件。
func (uc *Usecase) notifyBreach(ctx context.Context, tx txrun.Tx, b *SLABreach) error {
	recipients := slices.Clone(b.Escalate)
	if b.InstanceID != "" {
		pending, err := uc.ext.Flows.PendingUsers(ctx, b.InstanceID)
		if err != nil {
			return err
		}
		recipients = append(recipients, pending...)
		if len(recipients) == 0 {
			// 没有升级对象、流程上此刻也没有待处理的人（停在定时 / 自动任务 / 等待信号上）：
			// 交给流程管理员，超时不能无声无息地过去。
			if recipients, err = uc.ext.Flows.Admins(ctx, b.InstanceID); err != nil {
				return err
			}
		}
	}
	slices.Sort(recipients)
	recipients = slices.Compact(recipients)
	if len(recipients) == 0 {
		// 确实无人可通知：标记照常保留（否则每一轮都会再领到它），留一条日志供排查。
		uc.ext.Log.WarnContext(ctx, "ticket SLA breached but nobody to notify", "ticket", b.TicketID, "number", b.Number, "kind", b.Kind)
		return nil
	}
	payload, err := json.Marshal(map[string]any{
		"ticketId": b.TicketID, "number": b.Number, "title": b.Title, "kind": b.Kind, "creator": b.Creator,
	})
	if err != nil {
		return err
	}
	// 不设 GroupKey：事件带着明确的收件人，被通知域按分组聚合的话，摘要只发给路由自己的对象，
	// 这些收件人就收不到了。
	for chunk := range slices.Chunk(recipients, notifyChunk) {
		if _, err := uc.ext.Notify.NotifyEvent(ctx, tx, biznotify.Event{
			BizID: biznotify.DefaultBizID, EventType: EventSLABreached, Severity: slaSeverity,
			Title: truncate(fmt.Sprintf("%s %s", b.Number, b.Title), maxNoticeTitle), Payload: payload,
			Recipients: chunk,
		}); err != nil {
			// 通知域以业务错误拒收这一条（内容不合其约束）：记日志后跳过，标记照常保留——
			// 重试也不会成功。其它错误（数据库故障等）交给巡检按「这一条失败」处理。
			var ke *kerrors.Error
			if errors.As(err, &ke) && ke.Code >= 400 && ke.Code < 500 {
				uc.ext.Log.WarnContext(ctx, "ticket SLA notification rejected", "ticket", b.TicketID, "kind", b.Kind, "error", err)
				return nil
			}
			return err
		}
	}
	return nil
}

// truncate 截到至多 n 字节且不切断字符（通知标题按字节限长：按字符数截会让多字节文字超限）。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
