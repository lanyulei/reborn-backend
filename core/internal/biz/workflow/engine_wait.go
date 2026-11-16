package biz

import (
	"context"
	stderrors "errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/go-kratos/kratos/v3/errors"
)

// pushScope 进入一次包容分叉：在激活栈顶压入「激活 ID:被激活的分支数」。
func pushScope(scope, id string, branches int) string {
	entry := id + ":" + strconv.Itoa(branches)
	if scope == "" {
		return entry
	}
	return scope + "|" + entry
}

// popScope 弹出激活栈顶：返回激活 ID、被激活的分支数与余下的栈。栈为空或格式不符时
// 返回分支数 1（等于不等待，流程不会卡死）。
func popScope(scope string) (id string, branches int, rest string) {
	top := scope
	if i := strings.LastIndexByte(scope, '|'); i >= 0 {
		rest, top = scope[:i], scope[i+1:]
	}
	id, count, ok := strings.Cut(top, ":")
	if n, err := strconv.Atoi(count); ok && err == nil && n >= 1 {
		return id, n, rest
	}
	return "", 1, rest
}

// fork 分叉：消耗当前执行位置，沿每条给定的出边各放一个新的执行位置并推进。
func (x *Execution) fork(ctx context.Context, token *Token, out []*Edge, scope string) error {
	token.Status = TokenDone
	for _, e := range out {
		branch, err := x.newToken(e.Target, scope)
		if err != nil {
			return err
		}
		if err := x.advance(ctx, branch); err != nil {
			return err
		}
	}
	return nil
}

// chooseAll 包容网关选路：全部条件为真的出边；都不成立时走默认分支。求值出错视为不命中
// （并记入历史，见 choose）。
func (x *Execution) chooseAll(n *Node, out []*Edge) ([]*Edge, error) {
	vars := x.vars()
	var (
		def    *Edge
		chosen []*Edge
		failed []any
	)
	for _, e := range out {
		if e.IsDefault {
			def = e
			continue
		}
		if prog := x.g.programs[e.ID]; prog != nil {
			ok, err := prog.eval(vars)
			if err == nil && ok {
				chosen = append(chosen, e)
			}
			if err != nil {
				failed = append(failed, map[string]any{"edge": e.ID, "error": truncate(err.Error(), maxConditionError)})
			}
		}
	}
	if len(chosen) == 0 && def != nil {
		chosen = []*Edge{def}
	}
	return chosen, x.conditionErrors(n, failed)
}

// vars 条件表达式可见的变量。
func (x *Execution) vars() map[string]any {
	in := &x.s.Instance
	form := in.Variables
	if form == nil {
		form = map[string]any{}
	}
	return map[string]any{"form": form, "initiator": in.Initiator, "initiatorDept": in.InitiatorDept}
}

// joinInclusive 包容汇聚：等齐与当前执行位置同属一次激活的分支。未等齐返回 (nil, nil)；
// 等齐后消耗它们并返回合并出的执行位置（激活栈弹出一层）。
func (x *Execution) joinInclusive(token *Token, n *Node) (*Token, error) {
	token.Status = TokenWaiting
	group, want, rest := popScope(token.Scope)
	var waiting []*Token
	for _, t := range x.s.Tokens {
		if t.NodeID != n.ID || t.Status != TokenWaiting {
			continue
		}
		if g, _, _ := popScope(t.Scope); g == group {
			waiting = append(waiting, t)
		}
	}
	if len(waiting) < want {
		return nil, nil
	}
	for _, t := range waiting {
		t.Status = TokenDone
	}
	return x.newToken(n.ID, rest)
}

// addJob 排一条作业。
func (x *Execution) addJob(j *Job) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	j.ID, j.Status = id, JobPending
	x.s.Jobs = append(x.s.Jobs, j)
	return nil
}

// openTimer 到达定时节点：需要等待返回 true（延时 / 等到某时刻排作业，等信号则只是停下）。
func (x *Execution) openTimer(token *Token, n *Node) (bool, error) {
	var due time.Time
	switch n.Timer.Mode {
	case TimerSignal:
		return true, nil
	case TimerDelay:
		due = x.cal.Add(x.now, *n.Timer.Duration)
	case TimerUntil:
		raw, _ := x.s.Instance.Variables[n.Timer.Field].(string)
		at, err := time.Parse(time.RFC3339, raw)
		if err != nil || !at.After(x.now) {
			// 取不到时刻或已经过了：不等待（流程不因表单数据异常而卡住）。
			if err := x.autoSkip(); err != nil {
				return false, err
			}
			return false, x.history(n, "", ActionTimer, "", "", map[string]any{"skipped": true})
		}
		due = at.UTC()
	}
	return true, x.addJob(&Job{Kind: JobTimer, NodeID: n.ID, TokenID: token.ID, DueTime: due})
}

// scheduleTimeout 人工任务配置了处理时限：记下到期时间并排超时 / 提醒作业。
func (x *Execution) scheduleTimeout(token *Token, n *Node, t *Task) error {
	to := n.Timeout
	if to == nil {
		return nil
	}
	// 截到微秒（数据库 timestamptz 的精度）。作业记下自己属于哪一轮时限（payload.due）：
	// 时限被重新起算（改派）后，旧一轮的超时 / 提醒作业据此作废。
	t.DueTime = x.cal.Add(x.now, to.Duration).Truncate(time.Microsecond)
	round := map[string]any{"due": float64(t.DueTime.UnixMicro())}
	if err := x.addJob(&Job{Kind: JobTimeout, NodeID: n.ID, TokenID: token.ID, TaskID: t.ID, DueTime: t.DueTime, Payload: round}); err != nil {
		return err
	}
	if to.Remind == nil {
		return nil
	}
	// 提前量与时限同单位、同日历（发布校验保证）：提醒时刻 = 起点 + (时限 - 提前量)。
	lead := to.Duration
	lead.Value -= to.Remind.Value
	return x.addJob(&Job{Kind: JobRemind, NodeID: n.ID, TokenID: token.ID, TaskID: t.ID, DueTime: x.cal.Add(x.now, lead), Payload: round})
}

// waitingToken 作业所指的执行位置：仍停在作业创建时的那个节点上才算数（流程已被别的
// 动作带走时作业作废）。
func (x *Execution) waitingToken(j *Job) *Token {
	if x.s.Instance.Status != InstanceRunning {
		return nil
	}
	for _, t := range x.s.Tokens {
		if t.ID == j.TokenID && t.Status == TokenActive && t.NodeID == j.NodeID {
			return t
		}
	}
	return nil
}

// pendingTaskOf 作业所指的任务：仍在待处理才算数。
func (x *Execution) pendingTaskOf(j *Job) *Task {
	if x.s.Instance.Status != InstanceRunning {
		return nil
	}
	for _, t := range x.s.Tasks {
		if t.ID == j.TaskID && t.Status == TaskPending {
			return t
		}
	}
	return nil
}

// pendingUsers 任务上当前待处理的用户。
func pendingUsers(t *Task) []string {
	var out []string
	for _, a := range t.Assignees {
		if a.Status == AssigneePending {
			out = append(out, a.UserID)
		}
	}
	return out
}

// leave 离开等待节点并继续推进。
func (x *Execution) leave(ctx context.Context, token *Token) error {
	token.NodeID = x.g.out[token.NodeID][0].Target
	return x.advance(ctx, token)
}

// Fire 执行一条到期的作业（自动任务除外，见 ServiceDone / ServiceDead）。作业所指的
// 位置 / 任务已不在原处时什么也不做——作业天然幂等，重复领取不会重复生效。
func (x *Execution) Fire(ctx context.Context, j *Job) error {
	switch j.Kind {
	case JobTimer:
		token := x.waitingToken(j)
		if token == nil {
			return nil
		}
		if err := x.history(x.g.Node(j.NodeID), "", ActionTimer, "", "", nil); err != nil {
			return err
		}
		return x.leave(ctx, token)
	case JobRemind:
		// 时限被重新起算后旧一轮的提醒作废（新的时限有自己的提醒）。
		if t := x.pendingTaskOf(j); t != nil && roundOf(j) == t.DueTime.UnixMicro() {
			x.notify(NoticeTaskRemind, t, pendingUsers(t))
		}
		return nil
	case JobTimeout:
		return x.fireTimeout(ctx, j)
	case JobResume:
		return x.childFinished(ctx, j)
	case JobStop:
		if x.s.Instance.Status != InstanceRunning {
			return nil
		}
		return x.finish(InstanceTerminated, nil, ActionTerminate, "", "parent workflow ended")
	}
	return nil
}

// fireTimeout 任务超时：按节点配置自动同意 / 自动拒绝 / 转交管理员 / 仅提醒。
func (x *Execution) fireTimeout(ctx context.Context, j *Job) error {
	t := x.pendingTaskOf(j)
	// 任务的时限已被重新起算（改派）：这条是旧一轮的作业，作废。
	if t == nil || roundOf(j) != t.DueTime.UnixMicro() {
		return nil
	}
	n := x.g.Node(t.NodeID)
	i := slices.IndexFunc(x.s.Tokens, func(tk *Token) bool { return tk.ID == t.TokenID })
	if n == nil || n.Timeout == nil || i < 0 {
		return nil
	}
	token, action := x.s.Tokens[i], n.Timeout.Action
	var admins []string
	if action == TimeoutAdmin {
		var err error
		if admins, err = x.dir.ActiveUserIDs(ctx, x.g.Settings.Admins); err != nil {
			return err
		}
		if len(admins) == 0 {
			action = TimeoutRemind // 没有可用的管理员：至少提醒当前处理人
		}
	}
	detail := map[string]any{"action": action}
	if action == TimeoutApprove && x.approveGuard != nil {
		// 业务域不放行（如本环节必填的字段还空着）：不替人同意，改为提醒，流程停在原处。
		// 只认业务错误；其它错误（数据库故障等）照常返回，由作业重试。
		var ke *errors.Error
		if err := x.approveGuard(ctx, n); stderrors.As(err, &ke) && ke.Code >= 400 && ke.Code < 500 {
			action = TimeoutRemind
			detail = map[string]any{"action": action, "blocked": true}
		} else if err != nil {
			return err
		}
	}
	switch action {
	case TimeoutApprove:
		x.closeTask(t, TaskApproved)
		if err := x.history(n, t.ID, ActionTimeout, "", "", detail); err != nil {
			return err
		}
		return x.leave(ctx, token)
	case TimeoutReject:
		return x.rejectTask(ctx, t, token, ActionTimeout, "", "", detail)
	case TimeoutAdmin:
		x.cancelSeats(t)
		if err := x.assign(ctx, t, admins); err != nil {
			return err
		}
		detail["users"] = admins
		return x.history(n, t.ID, ActionTimeout, "", "", detail)
	}
	x.notify(NoticeTaskRemind, t, pendingUsers(t))
	return x.history(n, t.ID, ActionTimeout, "", "", detail)
}

// childFinished 子流程结束：正常通过则父流程继续；被拒绝 → 父流程以「已拒绝」结束；
// 被撤回 / 终止 → 父流程以「已终止」结束。
func (x *Execution) childFinished(ctx context.Context, j *Job) error {
	token := x.waitingToken(j)
	if token == nil {
		return nil
	}
	n := x.g.Node(j.NodeID)
	status := InstanceTerminated
	if v, ok := j.Payload["status"].(float64); ok {
		status = int32(v)
	}
	if err := x.history(n, "", ActionSubProcess, "", "", map[string]any{"child": j.Payload["child"], "status": status}); err != nil {
		return err
	}
	switch status {
	case InstanceCompleted:
		return x.leave(ctx, token)
	case InstanceRejected:
		return x.finish(InstanceRejected, n, ActionComplete, "", "")
	}
	return x.finish(InstanceTerminated, n, ActionComplete, "", "")
}

// mergeVars 把一组键值并入流程变量（同名覆盖）；并入后超出大小上限返回 ErrVariablesInvalid
// （变量不会因为反复并入而无限增长）。
func (x *Execution) mergeVars(vars map[string]any) error {
	if len(vars) == 0 {
		return nil
	}
	in := &x.s.Instance
	merged := make(map[string]any, len(in.Variables)+len(vars))
	maps.Copy(merged, in.Variables)
	maps.Copy(merged, vars)
	// 规整为 JSON 取值：与下一条命令从库里读回的类型一致（见 normalizeVariables）。
	merged, err := normalizeVariables(merged)
	if err != nil {
		return err
	}
	in.Variables, x.s.VarsChanged = merged, true
	return nil
}

// ServiceDone 自动任务执行成功：返回的变量并入流程变量，流程继续。
func (x *Execution) ServiceDone(ctx context.Context, j *Job, vars map[string]any) error {
	token := x.waitingToken(j)
	if token == nil {
		return nil
	}
	if err := x.mergeVars(vars); err != nil {
		return err
	}
	if err := x.history(x.g.Node(j.NodeID), "", ActionService, "", "", nil); err != nil {
		return err
	}
	return x.leave(ctx, token)
}

// roundOf 作业所属的那一轮时限（任务到期时刻的微秒时间戳，排作业时写入 payload）。
func roundOf(j *Job) int64 {
	v, _ := j.Payload["due"].(float64)
	return int64(v)
}

// JobDead 作业无法完成（自动任务重试耗尽，或引擎内部作业遇到不可恢复的错误）：记入历史并
// 通知流程管理员，流程停在原处等人工处理（重试，或对定时 / 自动任务跳过）。
func (x *Execution) JobDead(ctx context.Context, j *Job, reason string) error {
	if x.s.Instance.Status != InstanceRunning {
		return nil
	}
	if x.g == nil {
		// 流程图已无法解析（见 ErrVersionInvalid）：找不到流程管理员与节点名，只记历史。
		return x.history(nil, "", ActionServiceFailed, "", truncate(reason, 500), map[string]any{"kind": j.Kind})
	}
	admins, err := x.dir.ActiveUserIDs(ctx, x.g.Settings.Admins)
	if err != nil {
		return err
	}
	n := x.g.Node(j.NodeID)
	notice := &Notice{Kind: NoticeServiceFailed, Recipients: admins}
	if n != nil {
		notice.NodeName = n.Name
	}
	if len(admins) > 0 {
		x.s.Notices = append(x.s.Notices, notice)
	}
	return x.history(n, "", ActionServiceFailed, "", truncate(reason, 500), map[string]any{"kind": j.Kind})
}

// SkipWait 管理员跳过一个由作业驱动的等待节点（死信的自动任务、未到期的定时节点）：流程从它
// 之后继续。等信号的定时节点没有作业——让它继续的办法是投递那个信号；子流程节点
// 不能跳过（终止子流程即可让父流程按规则结束）。
func (x *Execution) SkipWait(ctx context.Context, j *Job, operator, reason string) error {
	token := x.waitingToken(j)
	if token == nil {
		return ErrTaskNotPending
	}
	if err := x.history(x.g.Node(j.NodeID), "", ActionSkip, operator, reason, map[string]any{"reason": "skipped by admin"}); err != nil {
		return err
	}
	return x.leave(ctx, token)
}

// Signal 投递一个外部信号：全部正在等它的定时节点继续；vars 并入流程变量。
// 没有节点在等这个信号返回 ErrSignalNotAwaited。
func (x *Execution) Signal(ctx context.Context, name string, vars map[string]any) error {
	if x.s.Instance.Status != InstanceRunning {
		return ErrInstanceNotRunning
	}
	var waiting []*Token
	for _, t := range x.s.Tokens {
		if n := x.g.Node(t.NodeID); t.Status == TokenActive && n != nil && n.Type == NodeTimer &&
			n.Timer.Mode == TimerSignal && n.Timer.Signal == name {
			waiting = append(waiting, t)
		}
	}
	if len(waiting) == 0 {
		return ErrSignalNotAwaited
	}
	if err := x.mergeVars(vars); err != nil {
		return err
	}
	for _, t := range waiting {
		if err := x.history(x.g.Node(t.NodeID), "", ActionTimer, "", "", map[string]any{"signal": name}); err != nil {
			return err
		}
		if err := x.leave(ctx, t); err != nil {
			return err
		}
	}
	return nil
}
