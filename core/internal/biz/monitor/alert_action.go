package biz

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// MaxBatchAlerts 一次批量事件操作的条数上限。
const MaxBatchAlerts = 200

// maxTransferNoteLen 转交说明的最大字符数。
const maxTransferNoteLen = 500

// AlertAckWriter 认领状态的写入（core 对 monitor_alert 只有 ack_user / ack_time 等列的写权限）。每个方法
// 是一条带条件的 UPDATE：条件不成立（事件已不在告警中、认领人已被别人改掉）时不写并返回 false，
// 由用例重新读取后给出确切的结论——不做先查后写。
type AlertAckWriter interface {
	// AckTx 事件告警中且无人认领时，把认领人置为 userID、认领时间置为当前。
	AckTx(ctx context.Context, tx txrun.Tx, alertID, userID string) (bool, error)
	// UnackTx 事件告警中且认领人是 expect 时，清空认领人与认领时间。
	UnackTx(ctx context.Context, tx txrun.Tx, alertID, expect string) (bool, error)
	// TransferTx 事件告警中且认领人是 expect（空 = 无人认领）时，把认领人改为 to；此前已认领的保留原认领
	// 时间，未认领的置为当前。
	TransferTx(ctx context.Context, tx txrun.Tx, alertID, expect, to string) (bool, error)
}

// AlertCmdWriter 事件指令（monitor_alert_cmd：core 只能 SELECT、INSERT，由评估引擎消费）。
type AlertCmdWriter interface {
	// RequestClose 事件告警中时下达一条手动关闭指令；同一事件已有待处理的关闭指令时是空操作。返回事件
	// 此刻是否在告警中（false = 没有下达）。
	RequestClose(ctx context.Context, alertID, operator, reason string) (firing bool, err error)
}

// AlertActionUsecase 告警事件操作：认领 / 取消认领 / 转交 / 备注 / 手动关闭 / 转工单，及批量认领、关闭。
//
// 范围规则：认领、取消认领、转交、备注要求对事件所在分组只读以上；手动关闭、转工单要求可管理。只因是
// 通知接收方才看得到事件的人不能做任何操作（403）；既不在范围内又不是接收方的，与事件不存在同样 404。
//
// 多实例 / 并发：状态变更是单条带条件的 UPDATE，与时间线条目同事务提交；关闭只写一条指令（部分唯一
// 索引保证同一事件至多一条待处理），由评估引擎执行。重复提交得到确定的结果。
type AlertActionUsecase struct {
	alerts  *AlertUsecase
	detail  *AlertDetailUsecase
	txm     TxManager
	refs    AlertRefReader
	acks    AlertAckWriter
	cmds    AlertCmdWriter
	events  AlertEventRepo
	links   AlertLinkWriter
	tickets TicketDesk
	cmdb    CMDBViewer
}

// NewAlertActionUsecase new an AlertActionUsecase.
func NewAlertActionUsecase(alerts *AlertUsecase, detail *AlertDetailUsecase, txm TxManager, refs AlertRefReader, acks AlertAckWriter,
	cmds AlertCmdWriter, events AlertEventRepo, links AlertLinkWriter, tickets TicketDesk, cmdb CMDBViewer,
) *AlertActionUsecase {
	return &AlertActionUsecase{
		alerts: alerts, detail: detail, txm: txm, refs: refs, acks: acks, cmds: cmds, events: events, links: links, tickets: tickets, cmdb: cmdb,
	}
}

func actionInvalid(format string, args ...any) error {
	return detail(ErrAlertActionInvalid, format, args...)
}

// authorize 事件操作的范围判定。
func (uc *AlertActionUsecase) authorize(ctx context.Context, scope Scope, a *Alert, userID string, manage bool) error {
	if !scope.CanRead(a.GroupID) {
		// 不泄露事件是否存在：只有通知接收方知道它存在，对他明确说「不能操作」。
		ok, err := uc.alerts.isActiveRecipient(ctx, a.ID, userID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrAlertNotFound
		}
		return ErrForbidden
	}
	if manage {
		return scope.requireManage(a.GroupID)
	}
	return nil
}

// load 解析范围、读取事件并做范围判定。
func (uc *AlertActionUsecase) load(ctx context.Context, userID, id string, manage bool) (Scope, *Alert, error) {
	scope, err := uc.alerts.scopes.Resolve(ctx, userID)
	if err != nil {
		return Scope{}, nil, err
	}
	a, err := uc.alerts.alerts.Get(ctx, id)
	if err != nil {
		return Scope{}, nil, err
	}
	return scope, a, uc.authorize(ctx, scope, a, userID, manage)
}

// view 重新读取事件并组装带展示名的视图（操作的返回值）。
func (uc *AlertActionUsecase) view(ctx context.Context, scope Scope, id string) (*AlertView, error) {
	a, err := uc.alerts.alerts.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	views, err := uc.alerts.views(ctx, scope, []*Alert{a})
	if err != nil {
		return nil, err
	}
	return views[0], uc.alerts.decorate(ctx, views)
}

// write 在一个事务里执行条件更新，成功时追加一条时间线；返回条件是否成立。
func (uc *AlertActionUsecase) write(ctx context.Context, ev *AlertEvent, update func(tx txrun.Tx) (bool, error)) (bool, error) {
	id, err := idgen.NewID()
	if err != nil {
		return false, err
	}
	ev.ID = id
	var ok bool
	err = uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		var err error
		if ok, err = update(tx); err != nil || !ok {
			return err
		}
		return uc.events.AppendTx(ctx, tx, ev)
	})
	return ok, err
}

// alreadyAcked 409：事件已被他人认领（metadata 带认领人）。
func (uc *AlertActionUsecase) alreadyAcked(ctx context.Context, ackUser string) error {
	names, err := uc.alerts.dir.UserNames(ctx, []string{ackUser})
	if err != nil {
		return err
	}
	return WithMetadata(ErrAlertAlreadyAcked, map[string]string{"ack_user_id": ackUser, "ack_user_name": names[ackUser]})
}

// ack 认领一条已通过范围判定的事件。条件更新落空（并发下状态变了）时按最新状态给出结论。
func (uc *AlertActionUsecase) ack(ctx context.Context, a *Alert, userID string) error {
	for range 2 {
		switch {
		case a.Status != mc.AlertFiring:
			return ErrAlertNotFiring
		case a.AckUser == userID:
			return nil
		case a.AckUser != "":
			return uc.alreadyAcked(ctx, a.AckUser)
		}
		ok, err := uc.write(ctx, &AlertEvent{AlertID: a.ID, Type: mc.AlertEventAck, Operator: userID},
			func(tx txrun.Tx) (bool, error) { return uc.acks.AckTx(ctx, tx, a.ID, userID) })
		if err != nil || ok {
			return err
		}
		if a, err = uc.alerts.alerts.Get(ctx, a.ID); err != nil {
			return err
		}
	}
	return ErrVersionConflict
}

// Ack 认领告警事件。
func (uc *AlertActionUsecase) Ack(ctx context.Context, userID, id string) (*AlertView, error) {
	scope, a, err := uc.load(ctx, userID, id, false)
	if err != nil {
		return nil, err
	}
	if err := uc.ack(ctx, a, userID); err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, id)
}

// Unack 取消认领：认领人本人，或对事件所在分组可管理的人。
func (uc *AlertActionUsecase) Unack(ctx context.Context, userID, id string) (*AlertView, error) {
	scope, a, err := uc.load(ctx, userID, id, false)
	if err != nil {
		return nil, err
	}
	for range 2 {
		if a.Status != mc.AlertFiring {
			return nil, ErrAlertNotFiring
		}
		if a.AckUser == "" {
			return uc.view(ctx, scope, id)
		}
		if a.AckUser != userID && !scope.CanManage(a.GroupID) {
			return nil, ErrForbidden
		}
		expect := a.AckUser
		ok, err := uc.write(ctx, &AlertEvent{AlertID: id, Type: mc.AlertEventUnack, Operator: userID, Detail: mc.AlertEventDetail{FromUser: expect}},
			func(tx txrun.Tx) (bool, error) { return uc.acks.UnackTx(ctx, tx, id, expect) })
		if err != nil {
			return nil, err
		}
		if ok {
			return uc.view(ctx, scope, id)
		}
		if a, err = uc.alerts.alerts.Get(ctx, id); err != nil {
			return nil, err
		}
	}
	return nil, ErrVersionConflict
}

// Transfer 把事件转交给另一位可用用户（此前未认领时等同于指派）。已被他人认领的事件须可管理才能转交。
func (uc *AlertActionUsecase) Transfer(ctx context.Context, userID, id, toUser, note string) (*AlertView, error) {
	scope, a, err := uc.load(ctx, userID, id, false)
	if err != nil {
		return nil, err
	}
	note = cleanText(note)
	if utf8.RuneCountInString(note) > maxTransferNoteLen {
		return nil, actionInvalid("note must be at most %d characters", maxTransferNoteLen)
	}
	toUser = strings.ToLower(toUser)
	active, err := uc.alerts.dir.ActiveUserIDs(ctx, []string{toUser})
	if err != nil {
		return nil, err
	}
	if len(active) != 1 {
		return nil, actionInvalid("the target user does not exist or is not active")
	}
	for range 2 {
		switch {
		case a.Status != mc.AlertFiring:
			return nil, ErrAlertNotFiring
		case a.AckUser == toUser:
			return nil, actionInvalid("the alert is already acknowledged by the target user")
		case a.AckUser != "" && a.AckUser != userID && !scope.CanManage(a.GroupID):
			return nil, ErrForbidden
		}
		expect := a.AckUser
		ev := &AlertEvent{
			AlertID: id, Type: mc.AlertEventTransfer, Operator: userID,
			Detail: mc.AlertEventDetail{FromUser: expect, ToUser: toUser, Text: note},
		}
		ok, err := uc.write(ctx, ev, func(tx txrun.Tx) (bool, error) { return uc.acks.TransferTx(ctx, tx, id, expect, toUser) })
		if err != nil {
			return nil, err
		}
		if ok {
			return uc.view(ctx, scope, id)
		}
		if a, err = uc.alerts.alerts.Get(ctx, id); err != nil {
			return nil, err
		}
	}
	return nil, ErrVersionConflict
}

// cleanText 去掉首尾空白与 NUL 字符（jsonb 不接受 \u0000）。
func cleanText(s string) string { return strings.TrimSpace(strings.ReplaceAll(s, "\x00", "")) }

// Comment 给事件追加一条备注（任何状态的事件都可以），返回新增的时间线条目。
func (uc *AlertActionUsecase) Comment(ctx context.Context, userID, id, text string) (*TimelineItem, error) {
	if _, _, err := uc.load(ctx, userID, id, false); err != nil {
		return nil, err
	}
	if text = cleanText(text); text == "" || utf8.RuneCountInString(text) > mc.MaxCommentLen {
		return nil, actionInvalid("comment must be 1-%d characters and not blank", mc.MaxCommentLen)
	}
	evID, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	ev := &AlertEvent{ID: evID, AlertID: id, Type: mc.AlertEventComment, Operator: userID, Detail: mc.AlertEventDetail{Text: text}}
	if err := uc.events.Append(ctx, ev); err != nil {
		return nil, err
	}
	return uc.detail.eventItem(ctx, ev)
}

// requestClose 给一条已通过范围判定的事件下达手动关闭指令。
func (uc *AlertActionUsecase) requestClose(ctx context.Context, a *Alert, userID, reason string) error {
	if a.Status != mc.AlertFiring {
		return ErrAlertNotFiring
	}
	firing, err := uc.cmds.RequestClose(ctx, a.ID, userID, reason)
	if err != nil {
		return err
	}
	if !firing {
		return ErrAlertNotFiring
	}
	return nil
}

// closeReason 规整关闭原因；过长返回错误。
func closeReason(reason string) (string, error) {
	if reason = cleanText(reason); utf8.RuneCountInString(reason) > mc.MaxCloseReasonLen {
		return "", actionInvalid("reason must be at most %d characters", mc.MaxCloseReasonLen)
	}
	return reason, nil
}

// Close 手动关闭告警中的事件：只下达指令，由评估引擎在下一轮执行（返回的事件 Closing 为 true）。
func (uc *AlertActionUsecase) Close(ctx context.Context, userID, id, reason string) (*AlertView, error) {
	scope, a, err := uc.load(ctx, userID, id, true)
	if err != nil {
		return nil, err
	}
	if reason, err = closeReason(reason); err != nil {
		return nil, err
	}
	if err := uc.requestClose(ctx, a, userID, reason); err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, id)
}

// AlertActionResult 批量操作中一个事件的结果；失败时 Reason 为错误码、Message 为说明。
type AlertActionResult struct {
	ID      string
	OK      bool
	Reason  string
	Message string
}

// batch 批量操作的公共骨架：事件一次读出、范围解析一次，随后逐条独立执行（各自的事务），互不影响；
// 结果顺序与 ids 一致。批量里不在范围内的事件一律按不存在处理（不逐条查询是否通知接收方）。
func (uc *AlertActionUsecase) batch(ctx context.Context, userID string, ids []string, manage bool, do func(a *Alert) error) ([]AlertActionResult, error) {
	if len(ids) == 0 || len(ids) > MaxBatchAlerts || hasDuplicate(ids) {
		return nil, actionInvalid("ids must be 1-%d distinct alert ids", MaxBatchAlerts)
	}
	scope, err := uc.alerts.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	alerts, err := uc.refs.ListByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*Alert, len(alerts))
	for _, a := range alerts {
		byID[a.ID] = a
	}
	out := make([]AlertActionResult, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var err error
		switch a := byID[id]; {
		case a == nil || !scope.CanRead(a.GroupID):
			err = ErrAlertNotFound
		case manage && !scope.CanManage(a.GroupID):
			err = ErrForbidden
		default:
			err = do(a)
		}
		res := AlertActionResult{ID: id, OK: err == nil}
		if err != nil {
			ke := kerrors.FromError(err)
			if ke.Code >= 500 {
				return nil, err // 系统性失败：不把内部错误伪装成逐条的业务结果
			}
			res.Reason, res.Message = ke.Reason, ke.Message
		}
		out = append(out, res)
	}
	return out, nil
}

// BatchAck 批量认领。
func (uc *AlertActionUsecase) BatchAck(ctx context.Context, userID string, ids []string) ([]AlertActionResult, error) {
	return uc.batch(ctx, userID, ids, false, func(a *Alert) error { return uc.ack(ctx, a, userID) })
}

// BatchClose 批量手动关闭（成功 = 关闭已受理）。
func (uc *AlertActionUsecase) BatchClose(ctx context.Context, userID string, ids []string, reason string) ([]AlertActionResult, error) {
	reason, err := closeReason(reason)
	if err != nil {
		return nil, err
	}
	return uc.batch(ctx, userID, ids, true, func(a *Alert) error { return uc.requestClose(ctx, a, userID, reason) })
}

// AlertTicketInput 转工单入参。Priority 为 0 时按事件级别对应；Title 为空时为「【级别】规则名称」。
type AlertTicketInput struct {
	TemplateID  string
	Title       string
	Priority    int32
	Data        map[string]any
	CreatorDept string
	RequestID   string
}

// ticketExists 409：事件已关联工单（metadata 带工单 ID）。
func ticketExists(ticketID string) error {
	return WithMetadata(ErrAlertTicketExists, map[string]string{"ticket_id": ticketID})
}

// replayTicket 事件已关联工单时的应答。带幂等键的重复提交（客户端超时重试）返回首次创建的那张工单：
// 判定口径是「请求带了幂等键，且已关联的工单正是调用方本人提的」——工单域只在创建时按幂等键查重，没有
// 不创建就能按键查找的入口，所以这里不逐字比对幂等键。其余情形（没带幂等键，或工单是别人 / 自动建单提的）
// 一律 409，metadata 带已关联的工单 ID。
func (uc *AlertActionUsecase) replayTicket(ctx context.Context, scope Scope, userID string, a *Alert, requestID string) (*AlertTicketView, *AlertView, error) {
	if requestID == "" {
		return nil, nil, ticketExists(a.TicketID)
	}
	info, err := uc.tickets.View(ctx, a.TicketID, userID)
	if err != nil {
		return nil, nil, err
	}
	if info == nil || info.Creator != userID {
		return nil, nil, ticketExists(a.TicketID)
	}
	view, err := uc.view(ctx, scope, a.ID)
	if err != nil {
		return nil, nil, err
	}
	return &AlertTicketView{ID: info.ID, Number: info.Number, Title: info.Title, Status: info.Status, CanView: true}, view, nil
}

// CreateTicket 把事件转为工单：提单人为当前用户；事件关联的资源只有当前用户能在资源管理里查看时才作为
// 工单的关联资源。工单域的业务错误原样返回。工单创建与关联不在同一事务（工单域自管事务）：以幂等键
// （缺省为事件 ID）保证同一提单人重试得到同一张工单。
func (uc *AlertActionUsecase) CreateTicket(ctx context.Context, userID, id string, in AlertTicketInput) (*AlertTicketView, *AlertView, error) {
	scope, a, err := uc.load(ctx, userID, id, true)
	if err != nil {
		return nil, nil, err
	}
	if a.TicketID != "" {
		return uc.replayTicket(ctx, scope, userID, a, in.RequestID)
	}
	p := TicketCreate{
		TemplateID: in.TemplateID, Title: cleanText(in.Title), Priority: in.Priority, Data: in.Data, Creator: userID,
		CreatorDept: in.CreatorDept, RequestID: in.RequestID,
	}
	if p.Title == "" {
		p.Title = truncate("【"+SeverityLabel(a.Severity)+"】"+a.RuleName, 255)
	}
	if p.Priority == 0 {
		p.Priority = min(max(a.Severity, mc.SeverityInfo), mc.SeverityFatal)
	}
	if p.RequestID == "" {
		p.RequestID = a.ID
	}
	if a.CIID != "" {
		card, _, err := uc.cmdb.Card(ctx, userID, a.CIID)
		if err != nil {
			return nil, nil, err
		}
		if card != nil {
			p.ResourceIDs = []string{a.CIID}
		}
	}
	info, err := uc.tickets.Create(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	evID, err := idgen.NewID()
	if err != nil {
		return nil, nil, err
	}
	existing := ""
	err = uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		linked, cur, err := uc.links.LinkTicketTx(ctx, tx, id, info.ID)
		if err != nil || !linked {
			existing = cur
			return err
		}
		return uc.events.AppendTx(ctx, tx, &AlertEvent{
			ID: evID, AlertID: id, Type: mc.AlertEventTicket, Operator: userID,
			Detail: mc.AlertEventDetail{TicketID: info.ID, TicketNumber: info.Number},
		})
	})
	if err != nil {
		return nil, nil, err
	}
	// 并发下别人先关联了另一张工单；关联的就是这一张（同一幂等键的重试）则照常成功。
	if existing != "" && existing != info.ID {
		return nil, nil, ticketExists(existing)
	}
	view, err := uc.view(ctx, scope, id)
	if err != nil {
		return nil, nil, err
	}
	return &AlertTicketView{ID: info.ID, Number: info.Number, Title: info.Title, Status: info.Status, CanView: true}, view, nil
}
