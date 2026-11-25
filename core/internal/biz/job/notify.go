package biz

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// executionPath 通知里执行详情链接的路径（前端路由）。
const executionPath = "/job/execution/"

// noticeTitleMaxBytes 通知标题的字节上限：通知域按字节限长（标题列 varchar(255)，超限的事件被拒绝后
// 这条通知就丢了）。标题先按字符截到 80 个——名称里有 4 字节字符（emoji、扩展区汉字）时仍可达 320 字节，
// 所以再按字节截一次。
const noticeTitleMaxBytes = 255

// 状态与来源的中文名（通知正文用）。
var (
	statusText = map[int32]string{
		jc.StatusSuccess: "执行成功", jc.StatusFailed: "执行失败", jc.StatusPartial: "部分失败", jc.StatusCanceled: "已取消",
		jc.StatusTimeout: "执行超时", jc.StatusUnknown: "结果未知（执行节点失联）", jc.StatusRejected: "审批未通过",
	}
	triggerText = map[int32]string{
		jc.TriggerManual: "手动执行", jc.TriggerSchedule: "定时作业", jc.TriggerWorkflow: "开放接口", jc.TriggerAlert: "告警自愈",
	}
)

// NotifyUsecase 执行结束通知：把已结束、尚未通知的执行写成通知事件（收件人是执行人）。
type NotifyUsecase struct {
	txs      TxManager
	queue    NotifyQueue
	notifier Notifier
	siteURL  string
}

// SiteURL 前端站点地址（无结尾斜杠；空 = 通知里的链接用相对路径）。
type SiteURL string

// NewNotifyUsecase new a NotifyUsecase.
func NewNotifyUsecase(txs TxManager, queue NotifyQueue, notifier Notifier, site SiteURL) *NotifyUsecase {
	return &NotifyUsecase{txs: txs, queue: queue, notifier: notifier, siteURL: strings.TrimRight(string(site), "/")}
}

// noticeSeverity 通知级别：成功为提示，取消为提示，其余（失败、超时、失联、审批未通过）为警告。
// 缺省路由的最低级别是警告——成功不打扰，想收成功通知把路由调到「提示」即可。
func noticeSeverity(status int32) int32 {
	switch status {
	case jc.StatusSuccess, jc.StatusCanceled:
		return 1
	case jc.StatusUnknown:
		return 3
	}
	return 2
}

// Run 发出执行结束通知（后台循环；多实例并发安全）。每条执行一个事务：行锁领取并标记已通知、
// 写通知事件，同事务提交——不会漏发，也不会重复发。返回本轮处理的条数。
func (uc *NotifyUsecase) Run(ctx context.Context) (int, error) {
	done := 0
	for done < 50 {
		var found bool
		err := uc.txs.WithinTx(ctx, func(tx Tx) error {
			e, err := uc.queue.ClaimTx(ctx, tx)
			if err != nil || e == nil {
				return err
			}
			found = true
			// 被执行人自己取消的执行不通知：他已经知道了。
			if e.Status == jc.StatusCanceled && e.Trigger == jc.TriggerManual {
				return nil
			}
			return uc.notifier.NotifyTx(ctx, tx, uc.notice(e))
		})
		if err != nil {
			return done, err
		}
		if !found {
			break
		}
		done++
	}
	return done, nil
}

func (uc *NotifyUsecase) notice(e *FinishedExecution) Notice {
	duration := "—"
	if e.StartTime != nil {
		duration = e.FinishTime.Sub(*e.StartTime).Round(time.Second).String()
	}
	text := statusText[e.Status]
	if text == "" {
		text = fmt.Sprintf("状态 %d", e.Status)
	}
	return Notice{
		Severity: noticeSeverity(e.Status),
		Title:    truncateBytes(truncateRunes("作业"+text+"："+e.Name, 80), noticeTitleMaxBytes),
		Payload: map[string]any{
			"execution_id": e.ID, "name": e.Name, "kind": e.Kind, "status": e.Status, "status_text": text,
			"error": e.Error, "trigger": e.Trigger, "trigger_text": triggerText[e.Trigger],
			"host_total": e.HostTotal, "host_ok": e.HostOK, "host_failed": e.HostFailed, "host_unreachable": e.HostUnreachable,
			"duration": duration, "url": uc.siteURL + executionPath + e.ID,
		},
		// 去重键带状态与结束时间：「结果未知」之后节点恢复、作业跑完，真实结果还要再通知一次；
		// 恢复后再次失联也是一次新的「结果未知」。
		DedupKey:   fmt.Sprintf("job:exec:%s:%d:%d", strings.ReplaceAll(e.ID, "-", ""), e.Status, e.FinishTime.Unix()),
		Recipients: []string{e.Creator},
	}
}

// truncateBytes 按字节截断到至多 max 字节，且不切开字符（对方按字节限长时用）。
// dup: biz/monitor、biz/incident、biz/oncall 各有一份同名同义的私有函数；各域的 biz 包不能互相 import，
// 合并需要新建共享包，不在本域的改动范围内。
func truncateBytes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	for max > 0 && !utf8.RuneStart(s[max]) {
		max--
	}
	return s[:max]
}
