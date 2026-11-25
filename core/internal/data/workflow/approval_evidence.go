package data

import (
	"context"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"
)

var _ biz.ApprovalEvidenceReader = (*instanceRepo)(nil)

// ApprovalEvidenceTx implements biz.ApprovalEvidenceReader。
//
// 业务对象只挂在流程树的根流程上（子流程继承 root_business_*，自己不挂业务对象）：先取到根流程，再取它
// 名下各级子流程（root_id，顶层流程的 root_id 是它自己；走 idx_workflow_instance_root），统计这些实例的
// 流转历史。同一业务对象可以先后发起多次流程（唯一约束只管进行中的那一个）：只看最近发起的那一次——
// 调用方问的是「刚结束的这次流程」，更早的流程里有人同意过不能算数。
// 必须走调用方的事务连接：实例结束的那条命令里新写的历史还未提交，另开连接读不到。
// 「被跳过的审批环节」只认处理人为空的自动跳过（skip / no assignee）——定时时刻已过、管理员跳过等待节点
// 写的是别的动作 / 原因；「由人同意」只认带操作人的 approve，并排除 except。
//
// 按业务对象找根流程没有可用的索引（uk_workflow_instance_business 是只含进行中实例的部分索引，流程结束后
// 用不上），是对 workflow_instance 的一次顺序扫描：调用频率是「一张挂着变更单的工单结束一次」，可以接受。
func (r *instanceRepo) ApprovalEvidenceTx(ctx context.Context, tx txrun.Tx, businessType, businessID, except string) (biz.ApprovalEvidence, error) {
	ptx, ok := basedata.PgxTx(tx)
	if !ok {
		return biz.ApprovalEvidence{}, errForeignTx
	}
	var skipped, approved int64
	err := ptx.QueryRow(ctx,
		`WITH root AS (
			SELECT id FROM workflow_instance
			WHERE business_type = $1::text AND business_id = $2::text
			ORDER BY start_time DESC, id DESC LIMIT 1)
		SELECT count(*) FILTER (WHERE h.action = $3::text AND h.detail->>'reason' = $4::text),
		       count(*) FILTER (WHERE h.action = $5::text AND h.operator IS NOT NULL AND h.operator IS DISTINCT FROM $6::uuid)
		FROM workflow_history h
		WHERE h.instance_id IN (SELECT i.id FROM workflow_instance i WHERE i.root_id = (SELECT id FROM root))`,
		businessType, businessID, biz.ActionSkip, biz.SkipReasonNoAssignee, biz.ActionApprove, dbx.NilIfEmpty(except)).
		Scan(&skipped, &approved)
	if err != nil {
		return biz.ApprovalEvidence{}, fmt.Errorf("read workflow approval evidence: %w", err)
	}
	return biz.ApprovalEvidence{SkippedApprovals: int(skipped), HumanApprovals: int(approved)}, nil
}
