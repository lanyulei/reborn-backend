package testfake

import (
	"cmp"
	"context"
	"slices"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// MonitorAlertOps 告警事件操作相关仓储的内存实现（时间线、关闭指令、认领 / 关联列的条件更新、按资源
// 统计），建立在 MonitorStore 的事件表之上，语义与 data 层对齐。无事务回滚语义。
type MonitorAlertOps struct {
	s      *MonitorStore
	events []*biz.AlertEvent
	cmds   []MonitorAlertCmd
	// BeforeWrite 非空时在每次认领类条件更新之前调用一次（测试用它模拟并发修改）。
	BeforeWrite func()
}

// MonitorAlertCmd 一条事件指令。
type MonitorAlertCmd struct {
	AlertID  string
	Operator string
	Reason   string
	Status   int32
}

// NewMonitorAlertOps new a MonitorAlertOps.
func NewMonitorAlertOps(s *MonitorStore) *MonitorAlertOps { return &MonitorAlertOps{s: s} }

// Events 全部时间线条目的副本（按写入顺序）。
func (o *MonitorAlertOps) Events() []*biz.AlertEvent {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	return slices.Clone(o.events)
}

// Cmds 全部事件指令的副本。
func (o *MonitorAlertOps) Cmds() []MonitorAlertCmd {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	return slices.Clone(o.cmds)
}

// SeedEvent 预置一条时间线条目（引擎写的类型，或自动建单的 ticket 条目）。
func (o *MonitorAlertOps) SeedEvent(ev *biz.AlertEvent) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	cp := *ev
	if cp.CreateTime.IsZero() {
		cp.CreateTime = o.s.now()
	}
	o.events = append(o.events, &cp)
}

func (o *MonitorAlertOps) hook() {
	if f := o.BeforeWrite; f != nil {
		o.BeforeWrite = nil
		f()
	}
}

// Append implements biz.AlertEventRepo.
func (o *MonitorAlertOps) Append(_ context.Context, ev *biz.AlertEvent) error {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	ev.CreateTime = o.s.now()
	cp := *ev
	o.events = append(o.events, &cp)
	return nil
}

// AppendTx implements biz.AlertEventRepo.
func (o *MonitorAlertOps) AppendTx(ctx context.Context, _ txrun.Tx, ev *biz.AlertEvent) error {
	return o.Append(ctx, ev)
}

// ListByAlert implements biz.AlertEventRepo.
func (o *MonitorAlertOps) ListByAlert(_ context.Context, alertID string, limit int) ([]*biz.AlertEvent, error) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	var out []*biz.AlertEvent
	for _, ev := range o.events {
		if ev.AlertID == alertID {
			cp := *ev
			out = append(out, &cp)
		}
	}
	slices.SortStableFunc(out, func(a, b *biz.AlertEvent) int {
		return cmp.Or(a.CreateTime.Compare(b.CreateTime), cmp.Compare(a.ID, b.ID))
	})
	return out[:min(len(out), limit)], nil
}

// TicketLinks implements biz.AlertTicketLinks.
func (o *MonitorAlertOps) TicketLinks(_ context.Context, alertIDs []string) (map[string]biz.TicketLink, error) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	out := map[string]biz.TicketLink{}
	latest := map[string]time.Time{}
	for _, ev := range o.events {
		if ev.Type != mc.AlertEventTicket || !slices.Contains(alertIDs, ev.AlertID) || ev.CreateTime.Before(latest[ev.AlertID]) {
			continue
		}
		latest[ev.AlertID] = ev.CreateTime
		out[ev.AlertID] = biz.TicketLink{TicketID: ev.Detail.TicketID, Number: ev.Detail.TicketNumber, Creator: ev.Operator, Auto: ev.Detail.Auto}
	}
	return out, nil
}

// LinkTicketTx implements biz.AlertLinkWriter.
func (o *MonitorAlertOps) LinkTicketTx(_ context.Context, _ txrun.Tx, alertID, ticketID string) (bool, string, error) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	a, ok := o.s.alerts[alertID]
	if !ok {
		return false, "", biz.ErrAlertNotFound
	}
	if a.TicketID != "" {
		return false, a.TicketID, nil
	}
	a.TicketID = ticketID
	return true, "", nil
}

// SetWorkflowInstanceTx implements biz.AlertLinkWriter.
func (o *MonitorAlertOps) SetWorkflowInstanceTx(_ context.Context, _ txrun.Tx, alertID, instanceID string) error {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	if a, ok := o.s.alerts[alertID]; ok && a.WorkflowInstanceID == "" {
		a.WorkflowInstanceID = instanceID
	}
	return nil
}

// SetAISummaryTx implements biz.AlertLinkWriter.
func (o *MonitorAlertOps) SetAISummaryTx(_ context.Context, _ txrun.Tx, alertID, summary string) error {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	if a, ok := o.s.alerts[alertID]; ok {
		a.AISummary = summary
	}
	return nil
}

// FiringByCI implements biz.AlertCIStats.
func (o *MonitorAlertOps) FiringByCI(_ context.Context, ciIDs []string, groups biz.GroupFilter) (map[string]int32, error) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	out := map[string]int32{}
	for _, a := range o.s.alerts {
		if a.Status == mc.AlertFiring && a.CIID != "" && slices.Contains(ciIDs, a.CIID) && inGroups(groups, a.GroupID) {
			out[a.CIID]++
		}
	}
	return out, nil
}

// ListByIDs implements biz.AlertRefReader.
func (o *MonitorAlertOps) ListByIDs(_ context.Context, ids []string) ([]*biz.Alert, error) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	var out []*biz.Alert
	for _, id := range ids {
		if a, ok := o.s.alerts[id]; ok {
			cp := *a
			out = append(out, &cp)
		}
	}
	return out, nil
}

// RuleNames implements biz.AlertRefReader.
func (o *MonitorAlertOps) RuleNames(_ context.Context, alertIDs []string) (map[string]string, error) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	out := map[string]string{}
	for _, id := range alertIDs {
		if a, ok := o.s.alerts[id]; ok {
			out[id] = a.RuleName
		}
	}
	return out, nil
}

// PendingClose implements biz.AlertRefReader.
func (o *MonitorAlertOps) PendingClose(_ context.Context, alertIDs []string) (map[string]bool, error) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	out := map[string]bool{}
	for _, c := range o.cmds {
		if c.Status == mc.CmdPending && slices.Contains(alertIDs, c.AlertID) {
			out[c.AlertID] = true
		}
	}
	return out, nil
}

// update 对告警中的事件做一次条件更新。
func (o *MonitorAlertOps) update(alertID string, cond func(a *biz.Alert) bool, apply func(a *biz.Alert, now time.Time)) (bool, error) {
	o.hook()
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	a, ok := o.s.alerts[alertID]
	if !ok || a.Status != mc.AlertFiring || !cond(a) {
		return false, nil
	}
	apply(a, o.s.now())
	return true, nil
}

// AckTx implements biz.AlertAckWriter.
func (o *MonitorAlertOps) AckTx(_ context.Context, _ txrun.Tx, alertID, userID string) (bool, error) {
	return o.update(alertID, func(a *biz.Alert) bool { return a.AckUser == "" },
		func(a *biz.Alert, now time.Time) { a.AckUser, a.AckTime = userID, &now })
}

// UnackTx implements biz.AlertAckWriter.
func (o *MonitorAlertOps) UnackTx(_ context.Context, _ txrun.Tx, alertID, expect string) (bool, error) {
	return o.update(alertID, func(a *biz.Alert) bool { return a.AckUser == expect },
		func(a *biz.Alert, _ time.Time) { a.AckUser, a.AckTime = "", nil })
}

// TransferTx implements biz.AlertAckWriter.
func (o *MonitorAlertOps) TransferTx(_ context.Context, _ txrun.Tx, alertID, expect, to string) (bool, error) {
	return o.update(alertID, func(a *biz.Alert) bool { return a.AckUser == expect },
		func(a *biz.Alert, now time.Time) {
			a.AckUser = to
			if a.AckTime == nil {
				a.AckTime = &now
			}
		})
}

// RequestClose implements biz.AlertCmdWriter.
func (o *MonitorAlertOps) RequestClose(_ context.Context, alertID, operator, reason string) (bool, error) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	a, ok := o.s.alerts[alertID]
	if !ok || a.Status != mc.AlertFiring {
		return false, nil
	}
	if !slices.ContainsFunc(o.cmds, func(c MonitorAlertCmd) bool { return c.AlertID == alertID && c.Status == mc.CmdPending }) {
		o.cmds = append(o.cmds, MonitorAlertCmd{AlertID: alertID, Operator: operator, Reason: reason, Status: mc.CmdPending})
	}
	return true, nil
}

// SeedNotifyLog 预置一条通知记录（让某个用户成为事件的通知接收方）。
func (o *MonitorAlertOps) SeedNotifyLog(l *biz.NotifyLog) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	cp := *l
	if cp.CreateTime.IsZero() {
		cp.CreateTime = o.s.now()
	}
	o.s.logs = append(o.s.logs, &cp)
}
