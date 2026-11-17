package biz

import (
	"context"
	"slices"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// SimStep 模拟运行经过的一个节点。
type SimStep struct {
	NodeID   string
	NodeName string
	Type     string
	// Mode / Assignees 人工任务与抄送节点解析出的多人模式与处理人；Skipped 表示处理人为空被跳过。
	Mode      string
	Assignees []string
	Skipped   bool
}

// SimResult 模拟运行的结果：依次经过的节点、流程最终状态与涉及用户的显示名。
type SimResult struct {
	Steps  []SimStep
	Status int32
	Names  map[string]string
}

// maxSimRounds 模拟运行的推进轮数上限；maxSimSteps 整次模拟经过的节点总数上限（带回路的
// 流程图在模拟里会一圈圈走下去，到上限即停，结果里流程仍是「进行中」）。
const (
	maxSimRounds = 300
	maxSimSteps  = 2000
)

// Simulate 在内存里把一份流程图走一遍（不落库、不发通知、不调用外部系统）：人工任务一律
// 按「同意」处理，定时 / 自动任务 / 子流程节点视为顺利通过，据此给出将会经过的节点
// 与各节点解析出的处理人。用于设计器的「模拟运行」。
func Simulate(ctx context.Context, g *Graph, in Instance, dir Directory, duty Duty) (*SimResult, error) {
	in.ID, in.Status = "simulation", InstanceRunning
	s := &State{Instance: in}
	res := &SimResult{}
	// 值班人按此刻的排班解析，与真实发起一致。
	x := NewExecution(g, s, dir, time.Now()).WithDuty(duty)
	x.memo = map[string][]string{}
	x.trace = func(n *Node) {
		// 汇聚网关每到达一条分支就经过一次：只记第一次。
		if paired(n.Type) && slices.ContainsFunc(res.Steps, func(st SimStep) bool { return st.NodeID == n.ID }) {
			return
		}
		res.Steps = append(res.Steps, SimStep{NodeID: n.ID, NodeName: n.Name, Type: n.Type})
	}
	if err := x.Start(ctx); err != nil {
		return nil, err
	}
	total := x.steps
	for round := 0; round < maxSimRounds && total < maxSimSteps && s.Instance.Status == InstanceRunning; round++ {
		x.steps = 0
		token := simNext(g, s)
		if token == nil {
			break
		}
		for _, t := range s.Tasks {
			if t.TokenID == token.ID && t.Status == TaskPending {
				x.closeTask(t, TaskApproved)
			}
		}
		if err := x.leave(ctx, token); err != nil {
			return nil, err
		}
		total += x.steps
	}
	// 把任务上的处理人补到对应的步骤上（同一节点多次经过时按先后一一对应）。
	used := map[string]int{}
	for i := range res.Steps {
		st := &res.Steps[i]
		if st.Type != NodeUserTask && st.Type != NodeCC {
			continue
		}
		seen := 0
		st.Skipped = true
		for _, t := range s.Tasks {
			if t.NodeID != st.NodeID {
				continue
			}
			if seen == used[st.NodeID] {
				st.Skipped, st.Mode = false, t.Mode
				for _, a := range t.Assignees {
					st.Assignees = append(st.Assignees, a.UserID)
				}
				break
			}
			seen++
		}
		if !st.Skipped {
			used[st.NodeID]++
		}
	}
	res.Status = s.Instance.Status
	return res, nil
}

// simNext 取下一个要「放行」的执行位置：停在人工任务或等待节点上的任意一个。
func simNext(g *Graph, s *State) *Token {
	for _, t := range s.Tokens {
		if t.Status != TokenActive {
			continue
		}
		n := g.Node(t.NodeID)
		if n == nil {
			continue
		}
		if n.Type == NodeUserTask || isWait(n.Type) {
			return t
		}
	}
	return nil
}

// Simulate 模拟运行一份流程图（设计器当前的内容，不必已保存）。流程图须先通过校验。
func (uc *DefinitionUsecase) Simulate(ctx context.Context, id string, raw []byte, in Instance, dir Directory, users Users, duty Duty) (*SimResult, error) {
	d, err := uc.reader.GetDefinition(ctx, id)
	if err != nil {
		return nil, err
	}
	// 与真实发起同一口径：变量规整为 JSON 取值，条件表达式的结果才与线上一致。
	if in.Variables, err = normalizeVariables(in.Variables); err != nil {
		return nil, err
	}
	g, issues, err := uc.validate(ctx, d.Key, raw)
	if err != nil {
		return nil, err
	}
	if len(issues) > 0 {
		return nil, errors.BadRequest(ErrDefinitionInvalid.Reason, "workflow definition is invalid: "+issues[0].Message)
	}
	if in.InitiatorDept == "" {
		if in.InitiatorDept, _, err = users.PrimaryDept(ctx, in.Initiator); err != nil {
			return nil, err
		}
	}
	res, err := Simulate(ctx, g, in, dir, duty)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, st := range res.Steps {
		ids = append(ids, st.Assignees...)
	}
	slices.Sort(ids)
	res.Names, err = users.UserNames(ctx, slices.Compact(ids))
	return res, err
}
