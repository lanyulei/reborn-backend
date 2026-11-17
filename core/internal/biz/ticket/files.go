package biz

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"

	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// fileOwners 表单数据里各附件字段引用的文件：文件 ID → 所在字段的字段码。同一个文件出现在
// 两个字段里时 dup 为该文件 ID（owners 里记的是排在前面的字段）——写入前据此拒绝：一个
// 字段移除它时文件即作废，另一个字段就指向了不存在的文件。
func fileOwners(form []FormField, data map[string]any) (owners map[string]string, dup string) {
	owners = map[string]string{}
	for _, f := range form {
		if f.Type != formschema.TypeFile {
			continue
		}
		items, _ := data[f.Code].([]any)
		for _, item := range items {
			id, ok := item.(string)
			if !ok || id == "" {
				continue
			}
			id = strings.ToLower(id)
			if code, taken := owners[id]; taken {
				if code != f.Code && dup == "" {
					dup = id
				}
				continue
			}
			owners[id] = f.Code
		}
	}
	return owners, dup
}

// concealedFields 这张工单的表单里应当对 userID 隐藏的字段（流程节点上配置的字段权限）：
// 提单人与管理视图的持有者看到全部；只是处理人 / 抄送人时，他有席位的节点上标为隐藏的字段
// 不给看。all 为真表示流程图已无法解析、判断不了该隐藏哪些——整张表单都不给（宁可少给）。
// 表单详情与附件下载共用这一个判定，两处口径不会走样。
func (a *Access) concealedFields(ctx context.Context, t *Ticket, userID string, fa *bizworkflow.FormAccess) (hidden []string, all bool, err error) {
	if t.Creator == userID || !fa.Seated || (len(fa.Hidden) == 0 && !fa.HideAll) {
		return nil, false, nil
	}
	viewAll, err := a.supervisor.CanViewAll(ctx, userID)
	if err != nil || viewAll {
		return nil, false, err
	}
	return fa.Hidden, fa.HideAll, nil
}

// FileAccess 工单附件的读取权限。独立于 Access：它要问流程引擎「哪些字段对这个人隐藏」，
// 而流程引擎的查看权限判定又依赖 Access——放在一个类型里就成环了。
type FileAccess struct {
	access *Access
	flows  WorkflowForms
}

// NewFileAccess new a FileAccess.
func NewFileAccess(access *Access, flows WorkflowForms) *FileAccess {
	return &FileAccess{access: access, flows: flows}
}

// CanReadFile implements bizfile.AccessChecker：能查看工单，且文件所在的表单字段没有对他隐藏。
// 不挂在任何字段上的文件是工单级的附件（附件成为字段类型之前提交的工单），权限 = 工单的查看权限。
func (a *FileAccess) CanReadFile(ctx context.Context, userID, bizID, fileID string) (bool, error) {
	t, err := a.access.reader.Get(ctx, bizID)
	if err != nil {
		if errors.Is(err, ErrTicketNotFound) {
			return false, nil
		}
		return false, err
	}
	if ok, err := a.access.canView(ctx, t, userID); err != nil || !ok {
		return false, err
	}
	owners, _ := fileOwners(t.Form, t.Data)
	code, inField := owners[strings.ToLower(fileID)]
	if !inField || t.InstanceID == "" || t.Creator == userID {
		return true, nil
	}
	fa, err := a.flows.FormAccess(ctx, t.InstanceID, userID)
	if err != nil {
		return false, err
	}
	hidden, all, err := a.access.concealedFields(ctx, t, userID, fa)
	if err != nil {
		return false, err
	}
	return !all && !slices.Contains(hidden, code), nil
}

// bindFieldFiles 把表单数据里附件字段引用的文件绑定到工单：只接受 uploader 本人上传、尚未
// 绑定的临时文件（别人的文件、别的工单的文件、已失效的文件都绑不上）。
func (uc *Usecase) bindFieldFiles(ctx context.Context, tx txrun.Tx, uploader, ticketID string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := uc.files.Bind(ctx, tx, uploader, BusinessType, ticketID, ids); err != nil {
		if errors.Is(err, bizfile.ErrFileNotFound) {
			return detail(ErrFormInvalid, "an attachment does not exist or was not uploaded by you")
		}
		return err
	}
	return nil
}

// syncFieldFiles 表单数据由 before 改为 after 之后，让工单绑定的文件跟上：新出现的绑定到工单
// （须是 editor 本人刚上传的），不再被引用的作废。在改表单的同一事务内调用。
func (uc *Usecase) syncFieldFiles(ctx context.Context, tx txrun.Tx, editor, ticketID string, form []FormField, before, after map[string]any) error {
	old, _ := fileOwners(form, before)
	now, dup := fileOwners(form, after)
	if dup != "" {
		return detail(ErrFormInvalid, "a file cannot be attached to more than one field")
	}
	var added, removed []string
	for id := range now {
		if _, kept := old[id]; !kept {
			added = append(added, id)
		}
	}
	for id := range old {
		if _, kept := now[id]; !kept {
			removed = append(removed, id)
		}
	}
	slices.Sort(added)
	slices.Sort(removed)
	if err := uc.bindFieldFiles(ctx, tx, editor, ticketID, added); err != nil {
		return err
	}
	if len(removed) == 0 {
		return nil
	}
	return uc.files.ReleaseFiles(ctx, tx, BusinessType, ticketID, removed)
}

// visibleFiles 详情里返回的附件：挂在表单字段上的文件，字段对查看者不可见（已从 form 里
// 去掉）时一并去掉——否则文件名照样会漏出去。owners 须按隐藏之前的完整表单算出。
func visibleFiles(files []*bizfile.File, owners map[string]string, form []FormField) []*bizfile.File {
	shown := make(map[string]struct{}, len(form))
	for _, f := range form {
		shown[f.Code] = struct{}{}
	}
	return slices.DeleteFunc(slices.Clone(files), func(f *bizfile.File) bool {
		code, inField := owners[strings.ToLower(f.ID)]
		if !inField {
			return false
		}
		_, ok := shown[code]
		return !ok
	})
}

// sortedKeys 返回 map 的键（升序），供需要确定顺序的调用方使用。
func sortedKeys(m map[string]string) []string {
	return slices.Sorted(maps.Keys(m))
}
