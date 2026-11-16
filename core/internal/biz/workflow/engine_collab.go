package biz

import (
	"context"
	"slices"

	"github.com/go-kratos/kratos/v3/errors"
)

// seatTarget 一个将要落座的处理人：user 实际处理人，onBehalf 被代理人（没有代理为空）。
type seatTarget struct {
	user     string
	onBehalf string
}

// proxied 把一组用户换成各自当前生效的代理人（只转一层），并去掉任务上已有未了结席位的人与
// 重复项。下列情况不走代理、仍由本人处理：代理人不可用；代理人是发起人或 avoid 里的人
// （不能借代理绕回自己审批自己）；代理人自己也在这批人里或已在任务上（否则两个人的
// 席位会并成一个，会签少一个人）；代理人已替这批里的另一个人代理。
func (x *Execution) proxied(ctx context.Context, t *Task, users []string, avoid ...string) ([]seatTarget, error) {
	seated := make(map[string]struct{}, len(t.Assignees)+len(users))
	for _, a := range t.Assignees {
		if live(a) {
			seated[a.UserID] = struct{}{}
		}
	}
	proxy := map[string]string{}
	if x.proxies != nil && len(users) > 0 {
		found, err := x.proxies.ActiveProxies(ctx, users, x.now)
		if err != nil {
			return nil, err
		}
		delegates := make([]string, 0, len(found))
		for _, d := range found {
			delegates = append(delegates, d)
		}
		active, err := x.dir.ActiveUserIDs(ctx, delegates)
		if err != nil {
			return nil, err
		}
		taken := map[string]struct{}{}
		for _, user := range users { // 按入参顺序判定，结果确定
			d, ok := found[user]
			_, onTask := seated[d]
			_, used := taken[d]
			if !ok || !slices.Contains(active, d) || d == x.s.Instance.Initiator || slices.Contains(avoid, d) ||
				slices.Contains(users, d) || onTask || used {
				continue
			}
			proxy[user], taken[d] = d, struct{}{}
		}
	}
	out := make([]seatTarget, 0, len(users))
	for _, user := range users {
		st := seatTarget{user: user}
		if d, ok := proxy[user]; ok {
			st = seatTarget{user: d, onBehalf: user}
		}
		if _, dup := seated[st.user]; dup {
			continue
		}
		seated[st.user] = struct{}{}
		out = append(out, st)
	}
	return out, nil
}

// targets 校验并解析「把任务交给这些人」类动作（转办 / 加签 / 委派）的目标：须为可用用户、
// 不含操作人自己、不重复、尚不在任务上；人数有上限。设了代理的换成代理人。
func (x *Execution) targets(ctx context.Context, t *Task, operator string, userIDs []string) ([]seatTarget, error) {
	if len(userIDs) == 0 || slices.Contains(userIDs, operator) {
		return nil, ErrTaskTargetInvalid
	}
	if len(t.Assignees)+len(userIDs) > 2*maxAssigneesPerTask {
		return nil, ErrInstanceLimit // 反复转办 / 加签不得让处理人记录无限增长
	}
	active, err := x.dir.ActiveUserIDs(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	if len(active) != len(userIDs) {
		return nil, ErrTaskTargetInvalid
	}
	seats, err := x.proxied(ctx, t, active, operator)
	if err != nil {
		return nil, err
	}
	if len(seats) != len(active) {
		return nil, ErrTaskTargetInvalid // 有人已在任务上
	}
	return seats, nil
}

// replaceSeat 把席位 old 换给另一个人：old 记为已转办，新席位继承它的顺序、状态与加签 /
// 委派关系；由 old 加签 / 委派出去的席位改挂到新席位上（它们处理完后回到新的人手上）。
func (x *Execution) replaceSeat(t *Task, old *TaskAssignee, to seatTarget) error {
	status := old.Status
	old.Status, old.ActionTime = AssigneeTransferred, x.now
	if err := x.addAssignee(t, to.user, old.Seq, status); err != nil {
		return err
	}
	seat := t.Assignees[len(t.Assignees)-1]
	seat.Origin, seat.OwnerID, seat.OnBehalf = old.Origin, old.OwnerID, to.onBehalf
	for _, a := range t.Assignees {
		if a.OwnerID == old.ID {
			a.OwnerID = seat.ID
		}
	}
	return nil
}

// derive 由席位 owner 加签 / 委派出一批新席位（待处理）并通知。
func (x *Execution) derive(t *Task, owner *TaskAssignee, origin string, seats []seatTarget) error {
	notify := make([]string, 0, len(seats))
	for _, st := range seats {
		if err := x.addAssignee(t, st.user, owner.Seq, AssigneePending); err != nil {
			return err
		}
		seat := t.Assignees[len(t.Assignees)-1]
		seat.Origin, seat.OwnerID, seat.OnBehalf = origin, owner.ID, st.onBehalf
		notify = append(notify, st.user)
	}
	x.notify(NoticeTaskAssigned, t, notify)
	return nil
}

// AddSign 加签：请另外几个人也来处理。position = before（前加签）：他们全部同意之后任务才回到
// 自己手上；after（后加签）：自己现在同意，他们全部同意之后这份同意才生效。被加签的人
// 拒绝即任务被拒绝。
func (x *Execution) AddSign(ctx context.Context, taskID, userID string, userIDs []string, position, comment string) error {
	t, seat, _, err := x.pendingTask(taskID, userID)
	if err != nil {
		return err
	}
	if position != OriginBefore && position != OriginAfter {
		return errors.BadRequest(ErrTaskTargetInvalid.Reason, "add-sign position must be before or after")
	}
	if seat.Origin == OriginDelegate {
		return ErrTaskTargetInvalid // 被委派人只需给出意见，不能再拉人
	}
	seats, err := x.targets(ctx, t, userID, userIDs)
	if err != nil {
		return err
	}
	if position == OriginBefore {
		seat.Status = AssigneeSuspended
	} else {
		seat.Status, seat.Comment, seat.ActionTime = AssigneeApproved, comment, x.now
	}
	if err := x.derive(t, seat, position, seats); err != nil {
		return err
	}
	// 历史里记实际落座的人（设了代理的是代理人）。
	seated := make([]string, 0, len(seats))
	for _, st := range seats {
		seated = append(seated, st.user)
	}
	return x.history(x.g.Node(t.NodeID), t.ID, ActionAddSign, userID, comment, map[string]any{"position": position, "users": seated})
}

// Delegate 委派：请另一个人先给出意见（同意 / 拒绝都只是意见），之后任务回到自己手上由自己决定。
func (x *Execution) Delegate(ctx context.Context, taskID, userID, toUserID, comment string) error {
	t, seat, _, err := x.pendingTask(taskID, userID)
	if err != nil {
		return err
	}
	if seat.Origin == OriginDelegate {
		return ErrTaskTargetInvalid // 不能把别人委派给自己的再委派出去
	}
	seats, err := x.targets(ctx, t, userID, []string{toUserID})
	if err != nil {
		return err
	}
	seat.Status = AssigneeSuspended
	if err := x.derive(t, seat, OriginDelegate, seats); err != nil {
		return err
	}
	return x.history(x.g.Node(t.NodeID), t.ID, ActionDelegate, userID, comment, map[string]any{"to": toUserID})
}

// Handover 管理员交接：把 from 在本实例全部未了结的席位换给 to（离职 / 调岗）。每个席位原样
// 换手（顺序、状态、加签 / 委派关系都保留）——即便 to 已在同一任务上也是如此：他此后
// 要以两个席位各处理一次（会签里这本来就是两个人的两份意见），不做合并，任何席位都
// 不会因此悬空或被凭空算作同意。返回换手的席位数。
func (x *Execution) Handover(_ context.Context, from, to, operator, reason string) (int, error) {
	if x.s.Instance.Status != InstanceRunning {
		return 0, nil
	}
	moved := 0
	for _, t := range x.s.Tasks {
		if t.Status != TaskPending {
			continue
		}
		before := moved
		for _, seat := range slices.Clone(t.Assignees) {
			if seat.UserID != from || !live(seat) {
				continue
			}
			if len(t.Assignees) >= 2*maxAssigneesPerTask {
				return 0, ErrInstanceLimit
			}
			pending := seat.Status == AssigneePending
			if err := x.replaceSeat(t, seat, seatTarget{user: to}); err != nil {
				return 0, err
			}
			moved++
			if pending {
				x.notify(NoticeTaskAssigned, t, []string{to})
			}
		}
		if moved == before {
			continue
		}
		if err := x.history(x.g.Node(t.NodeID), t.ID, ActionHandover, operator, reason, map[string]any{"from": from, "to": to}); err != nil {
			return 0, err
		}
	}
	return moved, nil
}

// Instance 当前实例（只读）。
func (x *Execution) Instance() *Instance { return &x.s.Instance }

// TaskNode 任务所在的节点；任务不存在返回 nil。
func (x *Execution) TaskNode(taskID string) *Node {
	for _, t := range x.s.Tasks {
		if t.ID == taskID {
			return x.g.Node(t.NodeID)
		}
	}
	return nil
}

// permRank 字段权限的宽松程度（同一字段在多个节点上取最宽松的）。
var permRank = map[string]int{"hidden": 0, "readonly": 1, "editable": 2, "required": 3}

// SeatPermissions 用户待处理任务所在节点的字段权限并集；ok=false 表示他当前没有待处理的任务。
// 被委派 / 加签来的人与节点原处理人同权。
func (x *Execution) SeatPermissions(userID string) (perms map[string]string, ok bool) {
	if x.s.Instance.Status != InstanceRunning {
		return nil, false
	}
	return x.permissions(func(t *Task, a *TaskAssignee) bool {
		return t.Status == TaskPending && a.UserID == userID && a.Status == AssigneePending
	})
}

// PendingNodes 用户当前有待处理任务的节点（实例不在进行中时为空）。跨实例汇总字段权限时用：
// 把一棵流程树上各实例的结果拼起来交给 PermissionsOf。
func (x *Execution) PendingNodes(userID string) []*Node {
	if x.s.Instance.Status != InstanceRunning {
		return nil
	}
	nodes, _ := x.seatNodes(func(t *Task, a *TaskAssignee) bool {
		return t.Status == TaskPending && a.UserID == userID && a.Status == AssigneePending
	})
	return nodes
}

// PermissionsOf 一组节点对表单 form 的字段权限并集（同一字段取最宽松的）。
func PermissionsOf(nodes []*Node, form string) map[string]string {
	return mergePermissions(nodes, form)
}

// HiddenFields 对该用户应当隐藏的字段：在他有过席位（处理 / 抄送，未被取消）的每一个节点上
// 都被配置为 hidden 的字段。只要有一个节点允许他看，就不再隐藏——他已经合法地看到过。
// seated=false 表示他在流程上没有任何席位（发起人、管理视图等，由调用方另行判定）。
func (x *Execution) HiddenFields(userID string) (hidden []string, seated bool) {
	nodes := x.SeatedNodes(userID)
	return HiddenOf(nodes, x.s.Instance.FormKey), x.seated(userID)
}

// seatedMatch 用户持有（或被代理持有）的、未被取消的席位。
func seatedMatch(userID string) func(*Task, *TaskAssignee) bool {
	return func(_ *Task, a *TaskAssignee) bool {
		return (a.UserID == userID || a.OnBehalf == userID) && a.Status != AssigneeCancelled
	}
}

func (x *Execution) seated(userID string) bool {
	_, ok := x.seatNodes(seatedMatch(userID))
	return ok
}

// SeatedNodes 用户有过席位（处理 / 抄送，未被取消；被代理人也算）的节点。跨实例汇总字段
// 权限时用：把一棵流程树上各实例的结果拼起来交给 HiddenOf。
func (x *Execution) SeatedNodes(userID string) []*Node {
	nodes, _ := x.seatNodes(seatedMatch(userID))
	return nodes
}

// HiddenOf 在给定的每一个节点上都对表单 form 配置为 hidden 的字段（升序）。
func HiddenOf(nodes []*Node, form string) []string {
	var hidden []string
	for field, perm := range mergePermissions(nodes, form) {
		if perm == "hidden" {
			hidden = append(hidden, field)
		}
	}
	slices.Sort(hidden)
	return hidden
}

// seatNodes 满足 match 的席位所在的节点（去重）；ok=false 表示没有这样的席位。
func (x *Execution) seatNodes(match func(t *Task, a *TaskAssignee) bool) (nodes []*Node, ok bool) {
	for _, t := range x.s.Tasks {
		if !slices.ContainsFunc(t.Assignees, func(a *TaskAssignee) bool { return match(t, a) }) {
			continue
		}
		ok = true
		if n := x.g.Node(t.NodeID); n != nil && !slices.Contains(nodes, n) {
			nodes = append(nodes, n)
		}
	}
	return nodes, ok
}

// permissions 汇总满足 match 的席位所在节点的字段权限（见 mergePermissions）。
func (x *Execution) permissions(match func(t *Task, a *TaskAssignee) bool) (perms map[string]string, ok bool) {
	nodes, ok := x.seatNodes(match)
	return mergePermissions(nodes, x.s.Instance.FormKey), ok
}

// mergePermissions 汇总一组节点对表单 form 的字段权限（每个节点先取 PermissionsFor）：同一
// 字段取最宽松的；某个节点没有为该字段配置权限等同于可见（readonly），因此它在这样的节点上
// 出现一次就不再是 hidden。
func mergePermissions(nodes []*Node, form string) map[string]string {
	perms := map[string]string{}
	effective := make([]map[string]string, len(nodes))
	for i, n := range nodes {
		effective[i] = n.PermissionsFor(form)
		for field, perm := range effective[i] {
			if cur, has := perms[field]; !has || permRank[perm] > permRank[cur] {
				perms[field] = perm
			}
		}
	}
	// 某节点未配置该字段 = 该节点上只读可见。
	for field, perm := range perms {
		if perm != "hidden" {
			continue
		}
		for _, own := range effective {
			if _, configured := own[field]; !configured {
				perms[field] = "readonly"
				break
			}
		}
	}
	return perms
}
