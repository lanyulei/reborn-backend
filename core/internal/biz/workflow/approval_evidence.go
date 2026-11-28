package biz

import (
	"context"
	stderrors "errors"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// SkipReasonNoAssignee 人工任务节点因解析不出处理人被引擎自动跳过时，写进流转历史 detail 的原因
// （见 Execution.openTask）。上层业务域据此区分「流程里安排了审批环节，但没有人有机会处理」与
// 「有人审批过」——只有明确选了跳过（onEmpty = skip）的节点，引擎才会跳过它并让流程继续，是否接受
// 这种「通过」由业务域自己判断；其余节点解析不出处理人时流程被拦住（见 blockIfEmpty），不留下这种记录。
const SkipReasonNoAssignee = "no assignee"

// ApprovalEvidence 一棵流程树（根流程及其名下各级子流程）上的审批证据。
type ApprovalEvidence struct {
	// SkippedApprovals 人工任务节点因解析不出处理人被引擎自动跳过的次数。
	SkippedApprovals int
	// HumanApprovals 由人（except 以外）点「同意」的次数。系统的超时自动同意没有操作人，不计入；
	// 被委派人给出的意见（reply）不推动任务通过，也不计入。
	HumanApprovals int
}

// ApprovalEvidenceReader InstanceLookup 的可选能力（data 层实现）：在给定事务内统计一个业务对象的
// 流程树上的审批证据。
type ApprovalEvidenceReader interface {
	ApprovalEvidenceTx(ctx context.Context, tx txrun.Tx, businessType, businessID, except string) (ApprovalEvidence, error)
}

// errNoApprovalEvidence 仓储没有提供审批证据（装配缺陷，不应发生）。
var errNoApprovalEvidence = stderrors.New("workflow: approval evidence is not available")

// ApprovalEvidenceOf 统计业务对象所挂流程树（根流程及其名下各级子流程）上的审批证据；except 为不计入
// 「由人同意」的用户（通常是发起人），空 = 不排除。tx 须是调用方当前的事务：实例结束的那条命令里新产生
// 的历史（如刚被跳过的审批环节）此时尚未提交，另开连接读不到，调用方会据此得出错误的结论。
// 只读，不加锁；业务对象没有流程实例时各项为 0。
func (uc *RuntimeUsecase) ApprovalEvidenceOf(ctx context.Context, tx txrun.Tx, businessType, businessID, except string) (ApprovalEvidence, error) {
	r, ok := uc.lookup.(ApprovalEvidenceReader)
	if !ok {
		return ApprovalEvidence{}, errNoApprovalEvidence
	}
	return r.ApprovalEvidenceTx(ctx, tx, businessType, businessID, except)
}
