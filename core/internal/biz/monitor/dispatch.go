package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 通知派发参数。
const (
	// dispatchBatch 一轮至多处理的发件箱行数；处理满一批时循环会立刻再跑一轮。
	dispatchBatch = 50
	// maxEventRecipients 单个通知事件的显式收件上限（与 notify 入参约束一致），超出则拆成多个事件。
	maxEventRecipients = 20
	// maxConsecutiveFailures 一轮里连续失败这么多行就收手：多半是某个依赖整体不可用，继续往下处理只会
	// 让积压里的每一行都白白记一次失败（失败次数用尽即永久丢弃）。剩下的行原样留到下一轮。
	maxConsecutiveFailures = 5
)

// 丢弃原因（记入发件箱 last_error）。丢弃是终态：评估引擎不会为同一个通知序号重写发件箱行，丢弃触发通知
// 意味着该事件此后的重复通知与恢复通知都不会再有。所以只在条件确实不可逆时丢弃；其余错误一律按失败
// 重试（带退避），重试次数用尽才丢弃。「事件已被屏蔽」是例外中的可逆情形——屏蔽解除后由引擎把这一行
// 重新置为待派发（同时清零失败次数），届时按普通待派发行处理。
const (
	dropAlertGone    = "告警事件不存在"
	dropAlertMuted   = "事件已被屏蔽"
	dropAlertClosed  = "事件已关闭"
	dropRuleDeleted  = "规则已删除"
	dropNeverFired   = "未发出过触发通知，不发恢复通知"
	dropStaleRepeat  = "事件已不在告警中，不再重复通知"
	noteNoRecipients = "未解析到通知对象"
	// dropAlertInhibited 与「已被屏蔽」同样可逆：抑制解除后由引擎把行重新置为待派发。
	dropAlertInhibited = "事件已被抑制"
	// 以下三项是发件箱行本身不自洽（通知类型不认识、级数与序号对不上）：这样的行绝不能按触发 / 重复
	// 通知派发——升级通知的序号是编码值，当成通知次数回写会把事件的通知计数抬到十万以上。
	dropUnknownKind   = "未知的通知类型"
	dropLevelMismatch = "通知类型与升级级数不一致"
	dropBadEscalation = "升级通知的序号不合法"
)

// checkItem 核对发件箱行自身是否自洽，返回丢弃原因（空 = 可以派发）与升级通知的级内序号。
// 只有三种写法是合法的：触发 / 重复 / 恢复且级数为 0；升级且序号能解出与级数列一致的 (级, 级内序号)。
func checkItem(item *OutboxItem) (reason string, levelSeq int32) {
	switch item.Kind {
	case mc.NotifyKindFiring, mc.NotifyKindRepeat, mc.NotifyKindResolved:
		if item.Level != 0 {
			return dropLevelMismatch, 0
		}
		return "", 0
	case mc.NotifyKindEscalation:
		level, n, ok := mc.SplitEscalationSeq(item.Seq)
		if !ok || level != item.Level {
			return dropBadEscalation, 0
		}
		return "", n
	}
	return dropUnknownKind, 0
}

// DispatchUsecase 把评估引擎写入的通知发件箱派发为通知事件（后台循环 monitor-notify）。
//
// 多实例安全：每行在自己的事务里处理——FOR UPDATE SKIP LOCKED 领取、解析收件人、经
// NotificationWriter 同事务写通知事件、写通知记录、回写事件通知计数、把发件箱行置为已派发，一起提交。
// 任一步失败整行回滚，另起一条语句记一次失败（attempts+1，指数退避后重试，次数用尽才丢弃并记日志）。
// 逐行事务而不是整批一个事务：一行有问题只回滚它自己，不拖累同批其余行，事务也短。行锁保证同一行
// 不会被两个实例同时处理；notify 的去重键是第二道保险。进程内只有「已编译订阅匹配器」缓存
// （SubscriptionMatcher），按 (订阅 ID, 版本) 取用，不承载正确性。
//
// 分组树与订阅所有者的数据范围在一轮（至多 dispatchBatch 行，通常不到一秒）内只读一次、各行共用
// （matchState）：授权变更最迟在下一轮生效。
type DispatchUsecase struct {
	txm    TxManager
	outbox OutboxRepo
	writer DispatchWriter
	alerts AlertReader
	rules  RuleReader
	// matcher 订阅匹配；resolver 通知对象解析。两者与模拟触发共用同一份实现。
	matcher  *SubscriptionMatcher
	resolver *RecipientResolver
	events   EventWriter
	log      *slog.Logger
	now      func() time.Time
	// siteURL 前端站点地址（无结尾斜杠）；空 = 通知里的链接用相对路径。
	siteURL string
	// 第二、三期的协作者（WithP2 注入；未注入时升级通知按失败重试，事件不带关联资源）。
	escalations EscalationReader
	progress    EscalationProgress
	cis         CIIndexReader
}

// NewDispatchUsecase new a DispatchUsecase.
func NewDispatchUsecase(txm TxManager, outbox OutboxRepo, writer DispatchWriter, alerts AlertReader, rules RuleReader,
	subs SubscriptionReader, scopes *ScopeUsecase, dir Directory, leaders DeptLeaders, duty Duty, events EventWriter, logger *slog.Logger,
	c *conf.Monitor,
) (*DispatchUsecase, error) {
	siteURL, err := normalizeSiteURL(c.GetSiteUrl())
	if err != nil {
		return nil, err
	}
	return &DispatchUsecase{
		txm: txm, outbox: outbox, writer: writer, alerts: alerts, rules: rules,
		matcher: NewSubscriptionMatcher(subs, scopes, logger), resolver: NewRecipientResolver(dir, leaders, duty, nil, nil),
		events: events, log: logger, now: time.Now, siteURL: siteURL,
	}, nil
}

// normalizeSiteURL 校验并规整 monitor.site_url：空串原样返回；否则须是不带 query / fragment / 凭据的
// http(s) 地址，去掉结尾的斜杠。配置写错在启动时就暴露，而不是把坏链接发进每一条通知。
func normalizeSiteURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("monitor.site_url must be an http(s) address without credentials, query or fragment")
	}
	return strings.TrimRight(raw, "/"), nil
}

// errOutboxEmpty 没有可领取的发件箱行（内部控制流）。
var errOutboxEmpty = errors.New("monitor: notify outbox is empty")

// Run 处理一批发件箱行，返回处理（派发或丢弃）的行数。单行失败不影响其余行；失败的行本轮不再重试。
//
// 返回的 error 只代表系统性失败——领取不了、失败记不了账、或连续 maxConsecutiveFailures 行都失败
// （多半是某个依赖整体不可用）。后台循环收到 error 会退避，所以单独一行的失败（它自己有按行的退避）
// 只记日志、不作为 error 返回：否则一条坏行会让整个循环变慢，拖累其他告警的通知。
func (uc *DispatchUsecase) Run(ctx context.Context) (int, error) {
	rs := &matchState{}
	return drain(ctx, uc.txm, "monitor notify dispatch", rowHandler[OutboxItem]{
		claim:   uc.outbox.ClaimTx,
		id:      func(item *OutboxItem) string { return item.ID },
		process: func(ctx context.Context, tx txrun.Tx, item *OutboxItem) error { return uc.process(ctx, tx, item, rs) },
		fail:    uc.fail,
	})
}

// rowHandler 逐行事务处理一张发件箱表所需的四个动作。
type rowHandler[T any] struct {
	// claim 在事务内行锁领取一行（exclude 为本轮已失败的行）；没有可领取的返回 nil。
	claim func(ctx context.Context, tx txrun.Tx, exclude []string) (*T, error)
	id    func(row *T) string
	// process 在同一事务内处理这一行；返回 nil 表示该行已终结。
	process func(ctx context.Context, tx txrun.Tx, row *T) error
	// fail 事务回滚后独立记一次失败；只有记账本身失败才返回 error。
	fail func(ctx context.Context, row *T, cause error) error
}

// drain 逐行事务处理一批（至多 dispatchBatch 行）：通知发件箱与自监控事件发件箱共用。每行一个事务
// ——领取与处理一起提交，处理失败整行回滚后另记一次失败、本轮不再碰它；连续 maxConsecutiveFailures 行
// 失败即收手并返回 error。
func drain[T any](ctx context.Context, txm TxManager, what string, h rowHandler[T]) (int, error) {
	var failed []string
	var last error
	done, streak := 0, 0
	for done+len(failed) < dispatchBatch && streak < maxConsecutiveFailures {
		var row *T
		err := txm.WithinTx(ctx, func(tx txrun.Tx) error {
			var err error
			if row, err = h.claim(ctx, tx, failed); err != nil {
				return err
			}
			if row == nil {
				return errOutboxEmpty
			}
			return h.process(ctx, tx, row)
		})
		switch {
		case errors.Is(err, errOutboxEmpty):
			return done, nil
		case err == nil:
			done++
			streak = 0
		case row == nil || ctx.Err() != nil:
			// 领取本身失败（数据库不可用）或正在停机：不记失败次数，留给下一轮。
			return done, err
		default:
			failed = append(failed, h.id(row))
			streak++
			last = err
			if err := h.fail(ctx, row, err); err != nil {
				return done, err
			}
		}
	}
	if streak >= maxConsecutiveFailures {
		return done, fmt.Errorf("%s: %d consecutive rows failed, last: %w", what, streak, last)
	}
	return done, nil
}

// fail 事务已回滚后，独立记一次失败并留日志。记账用不受取消影响的 ctx：停机中途失败的行也要留下次数。
// 只有记账本身失败才返回 error。
func (uc *DispatchUsecase) fail(ctx context.Context, item *OutboxItem, cause error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	dropped, err := uc.outbox.Fail(ctx, item.ID, truncate(cause.Error(), 500))
	if err != nil {
		return fmt.Errorf("dispatch outbox %s: %w (record failure: %w)", item.ID, cause, err)
	}
	if dropped {
		uc.log.ErrorContext(ctx, "monitor notify outbox row dropped after too many failures",
			"outbox_id", item.ID, "alert_id", item.AlertID, "kind", item.Kind, "seq", item.Seq, "attempts", mc.OutboxMaxAttempts, "error", cause)
		return nil
	}
	uc.log.WarnContext(ctx, "monitor notify outbox row failed, will retry after backoff",
		"outbox_id", item.ID, "alert_id", item.AlertID, "kind", item.Kind, "seq", item.Seq, "attempts", item.Attempts+1, "error", cause)
	return nil
}

// process 在事务内派发一行。返回 nil 表示该行已终结（已派发或已丢弃）。
func (uc *DispatchUsecase) process(ctx context.Context, tx txrun.Tx, item *OutboxItem, rs *matchState) error {
	drop := func(reason string) error {
		// 丢弃不可恢复（见丢弃原因处的说明），逐条留痕：事件、通知类型与序号、原因。
		uc.log.WarnContext(ctx, "monitor notify outbox row dropped",
			"outbox_id", item.ID, "alert_id", item.AlertID, "kind", item.Kind, "seq", item.Seq, "reason", reason)
		return uc.outbox.FinishTx(ctx, tx, item.ID, mc.OutboxDropped, reason)
	}
	// 先看行本身：通知类型按白名单分流，不认识或不自洽的行直接丢弃，绝不落入触发 / 重复通知的路径。
	reason, levelSeq := checkItem(item)
	if reason != "" {
		return drop(reason)
	}
	alert, err := uc.alerts.Get(ctx, item.AlertID)
	if isNotFound(err) {
		return drop(dropAlertGone)
	}
	if err != nil {
		return err
	}
	if alert.Muted {
		return drop(dropAlertMuted)
	}
	if alert.Inhibited {
		return drop(dropAlertInhibited)
	}
	if alert.Status == mc.AlertClosed {
		return drop(dropAlertClosed)
	}
	if item.Kind == mc.NotifyKindEscalation {
		return uc.processEscalation(ctx, tx, item, alert, levelSeq, rs, drop)
	}
	// 恢复通知只在确实发出过触发通知之后才发（引擎入箱时触发通知可能还在排队；它若最终被丢弃，
	// 这里不能单独发一条「已恢复」）。同一事件更早的行未终结时本行不会被领取，所以此刻计数已是定论。
	if item.Kind == mc.NotifyKindResolved && alert.NotifyCount == 0 {
		return drop(dropNeverFired)
	}
	// 迟到的重复通知：事件已恢复，再发一条「仍在告警」只会误导（紧随其后的恢复通知照常发）。事件不会
	// 从恢复回到告警中，所以这是不可逆的；触发通知不在此列——哪怕事件已恢复也要发，否则恢复通知也没了。
	if item.Kind == mc.NotifyKindRepeat && alert.Status != mc.AlertFiring {
		return drop(dropStaleRepeat)
	}
	rule, err := uc.rules.Get(ctx, alert.RuleID)
	if isNotFound(err) {
		return drop(dropRuleDeleted)
	}
	if err != nil {
		return err
	}

	d, err := uc.newDispatch(ctx, tx, item, alert, rule, item.Seq)
	if err != nil {
		return err
	}
	candidates, err := uc.matcher.match(ctx, rs, SubscriptionEvent{
		AlertID: alert.ID, GroupID: alert.GroupID, RuleID: alert.RuleID, DatasourceID: alert.DatasourceID, Severity: alert.Severity,
		Labels: alert.Labels, Resolved: item.Kind == mc.NotifyKindResolved, RepeatOnly: item.Kind == mc.NotifyKindRepeat,
	})
	if err != nil {
		return err
	}
	d.tree = rs.tree
	// notified 本行已经通知到的接收方（用户与群标识）：规则的通知对象在先，随后每条订阅通知到的都
	// 记进去——同一个人命中多条订阅也只收一份。
	notified, err := d.notifyRule(ctx)
	if err != nil {
		return err
	}
	for _, sub := range candidates {
		if notified, err = d.notifySubscription(ctx, sub, notified); err != nil {
			return err
		}
	}
	// 引擎不写通知计数：重复通知的节奏与次数上限完全取决于这里的回写——只在触发 / 重复通知确实
	// 派发时回写（与通知事件同事务），恢复通知与被丢弃的行不计。
	if item.Kind != mc.NotifyKindResolved {
		if err := uc.writer.TouchAlertTx(ctx, tx, alert.ID, item.Seq); err != nil {
			return err
		}
	}
	return uc.outbox.FinishTx(ctx, tx, item.ID, mc.OutboxDone, "")
}

// dispatch 一行发件箱的派发上下文。
type dispatch struct {
	uc    *DispatchUsecase
	tx    txrun.Tx
	item  *OutboxItem
	alert *Alert
	rule  *Rule
	tree  *groupTree
	// sess 通知对象解析会话（一行内规则与多条订阅可能引用同一张值班表，只查一次）。
	sess *ResolveSession
	now  time.Time
	// seq 本次通知的序号（写进通知记录与内容体）：触发 / 重复 / 恢复为发件箱的 seq，升级通知为级内序号。
	seq int32
	// ci 事件关联的资源；未关联、或资源索引里没有这一行时为 nil。
	ci *CIEntry
}

// newDispatch 组装一行的派发上下文：解析会话，以及事件关联的资源（有的话读一次，规则的「资源负责人」
// 与通知内容体都用它）。
func (uc *DispatchUsecase) newDispatch(ctx context.Context, tx txrun.Tx, item *OutboxItem, alert *Alert, rule *Rule, seq int32) (*dispatch, error) {
	now := uc.now()
	d := &dispatch{uc: uc, tx: tx, item: item, alert: alert, rule: rule, sess: uc.resolver.Session(now), now: now, seq: seq}
	if alert.CIID == "" || uc.cis == nil {
		return d, nil
	}
	entries, err := uc.cis.Briefs(ctx, []string{alert.CIID})
	if err != nil {
		return nil, fmt.Errorf("load ci %s of alert %s: %w", alert.CIID, alert.ID, err)
	}
	d.ci = entries[alert.CIID]
	return d, nil
}

// notifyRule 把规则的通知对象解析为用户并写通知事件与通知记录；返回已通知的用户。
func (d *dispatch) notifyRule(ctx context.Context) ([]string, error) {
	// 去重后的可用用户。
	users, _, err := d.sess.Rule(ctx, d.rule.Spec.Notify, d.ci)
	if err != nil {
		return nil, err
	}
	eventType := mc.EventAlertFiring
	if d.item.Kind == mc.NotifyKindResolved {
		eventType = mc.EventAlertResolved
	}
	// 没有解析到通知对象：不带收件人写一条事件，退回 notify 路由的固定收件与订阅者。
	var recipients []string
	note := noteNoRecipients
	if len(users) > 0 {
		recipients, note = users, ""
	}
	ids, err := d.emit(ctx, eventType, "", d.payload(""), recipients)
	if err != nil {
		return nil, err
	}
	return users, d.writeLog(ctx, mc.NotifySourceRule, "", users, ids, note)
}

// notifySubscription 向一条订阅的接收方发订阅通知，返回加上本次接收方之后的已通知集合。已经通知到的
// 接收方（规则的通知对象、排在前面的订阅通知到的用户与群）不再重复。
func (d *dispatch) notifySubscription(ctx context.Context, sub *Subscription, notified []string) ([]string, error) {
	users, err := d.sess.Subscription(ctx, sub)
	if err != nil {
		return nil, err
	}
	recipients := slices.DeleteFunc(slices.Concat(users, sub.Spec.Receivers.Groups), func(id string) bool { return slices.Contains(notified, id) })
	if len(recipients) == 0 {
		return notified, nil
	}
	eventType := mc.EventSubscriptionFiring
	if d.item.Kind == mc.NotifyKindResolved {
		eventType = mc.EventSubscriptionResolved
	}
	ids, err := d.emit(ctx, eventType, sub.ID, d.payload(sub.Spec.Name), recipients)
	if err != nil {
		return nil, err
	}
	return append(notified, recipients...), d.writeLog(ctx, mc.NotifySourceSubscription, sub.ID, recipients, ids, "")
}

func (d *dispatch) writeLog(ctx context.Context, source int32, subscriptionID string, recipients, eventIDs []string, note string) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	if recipients == nil {
		recipients = []string{}
	}
	return d.uc.writer.InsertLogTx(ctx, d.tx, &NotifyLog{
		ID: id, AlertID: d.alert.ID, Kind: d.item.Kind, Seq: d.seq, Level: d.item.Level, Source: source, SubscriptionID: subscriptionID,
		Recipients: recipients, NotifyEventIDs: eventIDs, Note: note,
	})
}

// dedupKey monitor:<指纹>:<事件 ID>:<kind>:<seq>[:s:<订阅 ID>][:<分片>]（ID 去掉连字符，整体不超过
// notify 的 128 字节上限）。同一行重试得到同样的键，不同通知序号的键不同（重复通知不被去重吞掉）。
// 与设计的差别：设计写的是「事件 ID 前 8 位」，但事件 ID 是 UUIDv7，前 8 位是毫秒时间戳的高位——
// 同一对象在约一分钟内恢复又再次触发时两个事件的前 8 位相同，第二次的触发通知会被 notify 当作重复
// 丢掉。所以用完整的事件 ID。
// 被丢弃后又由引擎重新置为待派发的行沿用同一个键也不会撞车：丢弃的行从未写出过通知事件（丢弃要么
// 发生在写事件之前，要么整笔事务已回滚，去重门闩随之回滚）。
func (d *dispatch) dedupKey(subscriptionID string, chunk int) string {
	var b strings.Builder
	b.WriteString("monitor:" + d.alert.Fingerprint + ":" + strings.ReplaceAll(d.alert.ID, "-", "") + ":")
	b.WriteString(strconv.Itoa(int(d.item.Kind)) + ":" + strconv.Itoa(int(d.item.Seq)))
	if subscriptionID != "" {
		b.WriteString(":s:" + strings.ReplaceAll(subscriptionID, "-", ""))
	}
	if chunk > 0 {
		b.WriteString(":" + strconv.Itoa(chunk))
	}
	return b.String()
}

// emit 写通知事件：recipients 为 nil 时不带收件人写一条；否则每 maxEventRecipients 人一条。
// 返回落库事件的 ID（去重命中时为首次事件的 ID）。
func (d *dispatch) emit(ctx context.Context, eventType, subscriptionID string, payload mc.NotifyPayload, recipients []string) ([]string, error) {
	raw, err := encodePayload(payload)
	if err != nil {
		return nil, fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	chunks := [][]string{nil}
	if len(recipients) > 0 {
		chunks = slices.Collect(slices.Chunk(recipients, maxEventRecipients))
	}
	ids := make([]string, 0, len(chunks))
	for i, chunk := range chunks {
		ev, err := d.uc.events.NotifyEvent(ctx, d.tx, biznotify.Event{
			BizID: mc.NotifyBizID, EventType: eventType, Severity: d.alert.Severity, Title: d.title(),
			Payload: raw, GroupKey: d.alert.GroupKey, DedupKey: d.dedupKey(subscriptionID, i), Recipients: chunk,
		})
		if err != nil {
			return nil, err
		}
		ids = append(ids, ev.ID)
	}
	return ids, nil
}

// title 通知标题（规则名 + 关键标签）。
func (d *dispatch) title() string { return alertTitle(d.alert.RuleName, d.alert.Labels) }

// payload 通知内容体；subscriptionName 仅订阅通知有值。
func (d *dispatch) payload(subscriptionName string) mc.NotifyPayload {
	return buildPayload(payloadInput{
		Alert: d.alert, Timezone: d.rule.Spec.Effective.Timezone, GroupName: d.tree.fullName(d.alert.GroupID), SiteURL: d.uc.siteURL,
		Resolved: d.item.Kind == mc.NotifyKindResolved, Seq: d.seq, SubscriptionName: subscriptionName, Now: d.now, CI: d.ci,
	})
}
