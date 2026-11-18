package biz

import (
	"context"
	"errors"
	"slices"
	"strings"

	k8sv1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	bizk8s "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	bizticket "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/paging"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 本文件是容器平台变更审批（k8s）对工单、流程、system 三个域的适配器：都是「调用对方已有用例」，
// 单向依赖 k8s → 其他域。唯一的登记点是流程引擎的监听器注册表（NewWorkflowListeners）：变更的审批
// 就是一张普通工单，工单流程结束时除了同步工单状态，再把结果落到对应的变更单上（同一事务）。

// k8sTickets 以工单域的用例实现变更审批所需的工单操作。
type k8sTickets struct {
	catalog bizticket.CatalogReader
	tickets *bizticket.Usecase
	runtime *bizworkflow.RuntimeUsecase
}

// NewK8sChangeTickets 构造变更审批的工单适配器。
func NewK8sChangeTickets(catalog bizticket.CatalogReader, tickets *bizticket.Usecase, runtime *bizworkflow.RuntimeUsecase) bizk8s.ChangeTickets {
	return k8sTickets{catalog: catalog, tickets: tickets, runtime: runtime}
}

// k8sTicketViewers 工单的查看权限与流程状态（工单域的 Access / Usecase）。
type k8sTicketViewers struct {
	access  *bizticket.Access
	tickets *bizticket.Usecase
}

// NewK8sTicketViewers 构造变更单详情所用的工单查看权限适配器。
func NewK8sTicketViewers(access *bizticket.Access, tickets *bizticket.Usecase) bizk8s.TicketViewers {
	return k8sTicketViewers{access: access, tickets: tickets}
}

// CanViewTicket implements bizk8s.TicketViewers：提单人、流程参与者，或持有工单管理视图的人。
func (v k8sTicketViewers) CanViewTicket(ctx context.Context, userID, ticketID string) (bool, error) {
	ok, err := v.access.CanViewInstance(ctx, userID, ticketID)
	if errors.Is(err, bizticket.ErrTicketNotFound) {
		return false, nil
	}
	return ok, err
}

// TicketFinished implements bizk8s.TicketViewers.
func (v k8sTicketViewers) TicketFinished(ctx context.Context, ticketID, creator string) (int32, bool, error) {
	d, err := v.tickets.Get(ctx, ticketID, creator)
	if err != nil {
		return 0, false, err
	}
	return k8sTicketStatus(d.Ticket.Status), d.Ticket.Status != bizticket.StatusProcessing, nil
}

// NewK8sTicketNotes 构造把执行结果写回工单的适配器。
func NewK8sTicketNotes(tickets *bizticket.Usecase) bizk8s.TicketNotes {
	return k8sTicketNotes{tickets: tickets}
}

func templateUnavailable(msg string) error {
	return kerrors.New(int(bizk8s.ErrChangeTemplate.Code), bizk8s.ErrChangeTemplate.Reason, msg)
}

// k8sFillable 本系统能填写的字段：字段码在约定清单内，且是单行或多行文本。
func k8sFillable(f bizticket.FormField) bool {
	return slices.Contains(bizk8s.TicketFieldCodes, f.Code) && (f.Type == formschema.TypeString || f.Type == formschema.TypeText)
}

// CheckTemplate implements bizk8s.ChangeTickets.
func (t k8sTickets) CheckTemplate(ctx context.Context, templateID string) error {
	tpl, err := t.catalog.GetTemplate(ctx, templateID)
	if err != nil {
		if errors.Is(err, bizticket.ErrTemplateNotFound) {
			return templateUnavailable("the ticket template does not exist")
		}
		return err
	}
	if tpl.Status != bizticket.TemplateEnabled {
		return templateUnavailable("the ticket template is disabled")
	}
	if tpl.CILink == bizticket.CILinkRequired {
		return templateUnavailable("the ticket template requires linked CMDB resources, which a change request cannot provide")
	}
	for _, f := range tpl.Form {
		if f.Required && !k8sFillable(f) {
			return templateUnavailable("the ticket template has a required field (" + f.Name + ") that a change request cannot fill; " +
				"only text fields with codes " + strings.Join(bizk8s.TicketFieldCodes, " / ") + " are filled")
		}
	}
	return nil
}

// Create implements bizk8s.ChangeTickets。只填模板里有的、本系统认识的文本字段；过长的内容截到
// 字段的长度上限以内（详情以变更单页面为准）。
func (t k8sTickets) Create(ctx context.Context, in bizk8s.ChangeTicket) (id, number string, finished bool, status int32, err error) {
	tpl, err := t.catalog.GetTemplate(ctx, in.TemplateID)
	if err != nil {
		if errors.Is(err, bizticket.ErrTemplateNotFound) {
			return "", "", false, 0, templateUnavailable("the ticket template used for approval no longer exists")
		}
		return "", "", false, 0, err
	}
	data := map[string]any{}
	for _, f := range tpl.Form {
		v := in.Fields[f.Code]
		if !k8sFillable(f) || v == "" {
			continue
		}
		limit := 255
		if f.Type == formschema.TypeText {
			limit = 4000
		}
		if f.Rule.MaxLen != nil && *f.Rule.MaxLen > 1 && int(*f.Rule.MaxLen) < limit {
			limit = int(*f.Rule.MaxLen)
		}
		if f.Type == formschema.TypeString {
			v = strings.Join(strings.Fields(v), " ")
		}
		if r := []rune(v); len(r) > limit {
			v = string(r[:limit-1]) + "…"
		}
		data[f.Code] = v
	}
	ticket, err := t.tickets.Create(ctx, bizticket.CreateParams{
		TemplateID: in.TemplateID, Title: in.Title, Priority: 2, Data: data, Creator: in.Creator, RequestID: in.RequestID,
	})
	if err != nil {
		if e := kerrors.FromError(err); e != nil && e.Code >= 400 && e.Code < 500 {
			return "", "", false, 0, templateUnavailable("the approval ticket could not be created: " + e.Message)
		}
		return "", "", false, 0, err
	}
	return ticket.ID, ticket.Number, ticket.Status != bizticket.StatusProcessing, ticket.Status, nil
}

// Cancel implements bizk8s.ChangeTickets：撤销工单的流程实例（工单与变更单由监听器同事务同步）。
func (t k8sTickets) Cancel(ctx context.Context, ticketID, userID string) error {
	d, err := t.tickets.Get(ctx, ticketID, userID)
	if err != nil {
		return err
	}
	if d.Ticket.InstanceID == "" {
		return bizk8s.ErrChangeState
	}
	if err := t.runtime.Cancel(ctx, d.Ticket.InstanceID, userID, "变更单已撤回"); err != nil {
		if e := kerrors.FromError(err); e != nil && e.Code >= 400 && e.Code < 500 {
			return kerrors.New(int(bizk8s.ErrChangeState.Code), bizk8s.ErrChangeState.Reason, "the approval can no longer be withdrawn: "+e.Message)
		}
		return err
	}
	return nil
}

type k8sTicketNotes struct {
	tickets *bizticket.Usecase
}

// Comment implements bizk8s.TicketNotes.
func (n k8sTicketNotes) Comment(ctx context.Context, ticketID, userID, content string) error {
	_, _, err := n.tickets.AddComment(ctx, ticketID, userID, content, "")
	return err
}

// k8sBypass 免审批权限 = 持有「免审批执行变更」接口权限（与授权中间件同一口径；超管恒为 true）。
type k8sBypass struct {
	authz *bizsystem.AuthzUsecase
}

// NewK8sBypassChecker 构造免审批权限判定器。
func NewK8sBypassChecker(authz *bizsystem.AuthzUsecase) bizk8s.BypassChecker {
	return k8sBypass{authz: authz}
}

// CanBypass implements bizk8s.BypassChecker.
func (b k8sBypass) CanBypass(ctx context.Context, userID string) (bool, error) {
	return b.authz.Allowed(ctx, userID, bizsystem.OperationCode(k8sv1.OperationK8SChangeServiceCheckChangeBypass))
}

// CanDebug implements bizk8s.BypassChecker.
func (b k8sBypass) CanDebug(ctx context.Context, userID string) (bool, error) {
	return b.authz.Allowed(ctx, userID, bizsystem.OperationCode(k8sv1.OperationK8SGovernanceServiceCheckDebugContainer))
}

// NewK8sChangeSiteURL 工单与通知里变更单链接的站点地址：与监控告警共用 monitor.site_url。
func NewK8sChangeSiteURL(c *conf.Monitor) bizk8s.ChangeSiteURL {
	return bizk8s.ChangeSiteURL(strings.TrimRight(strings.TrimSpace(c.GetSiteUrl()), "/"))
}

// k8sSelfRoles 自助授权的专属角色：每个用户一个，由平台创建并绑定给他；申请到的数据范围记在这个角色上。
// 角色本身不带任何菜单 / 接口权限——能进哪些页面仍由用户的其他角色决定，这里只承载集群数据范围。
type k8sSelfRoles struct {
	roles *bizsystem.RoleUsecase
	users *bizsystem.UserUsecase
}

// NewK8sSelfRoles 构造自助授权角色的适配器。
func NewK8sSelfRoles(roles *bizsystem.RoleUsecase, users *bizsystem.UserUsecase) bizk8s.SelfRoles {
	return k8sSelfRoles{roles: roles, users: users}
}

// K8sSelfRoleCode 用户专属角色的标识：k8s_self_<用户 ID 去掉连字符>。
func K8sSelfRoleCode(userID string) string {
	return "k8s_self_" + strings.ReplaceAll(strings.ToLower(userID), "-", "")
}

func (s k8sSelfRoles) find(ctx context.Context, code string) (*bizsystem.Role, error) {
	items, _, err := s.roles.List(ctx, bizsystem.RoleFilter{Keyword: code}, 1, paging.MaxPageSize)
	if err != nil {
		return nil, err
	}
	for _, r := range items {
		if r.Code == code {
			return r, nil
		}
	}
	return nil, nil
}

// Ensure implements bizk8s.SelfRoles。幂等：角色已存在即复用，已绑定即不动；并发创建以标识唯一约束兜底。
func (s k8sSelfRoles) Ensure(ctx context.Context, userID, username string) (string, error) {
	code := K8sSelfRoleCode(userID)
	role, err := s.find(ctx, code)
	if err != nil {
		return "", err
	}
	if role == nil {
		name := "k8s 自助授权: " + username
		if r := []rune(name); len(r) > 64 {
			name = string(r[:64])
		}
		role, err = s.roles.Create(ctx, &bizsystem.Role{Name: name, Code: code, Status: bizsystem.RoleStatusEnabled})
		if errors.Is(err, bizsystem.ErrRoleCodeExists) {
			role, err = s.find(ctx, code)
		}
		if err != nil {
			return "", err
		}
		if role == nil {
			return "", errors.New("k8s self-service role vanished after creation")
		}
	}
	// 复用已有的专属角色前确认它仍启用：停用的角色授权不生效，照常「成功」只会让申请人以为已有权限。
	if role.Status != bizsystem.RoleStatusEnabled {
		return "", errors.New("the self-service role of this user is disabled; ask an administrator to enable or remove role " + code)
	}
	bound, err := s.users.ListRoles(ctx, userID)
	if err != nil {
		return "", err
	}
	ids := make([]string, 0, len(bound)+1)
	for _, r := range bound {
		if r.ID == role.ID {
			return role.ID, nil
		}
		ids = append(ids, r.ID)
	}
	return role.ID, s.users.BindRoles(ctx, userID, append(ids, role.ID))
}

// ticketWithK8sChange 工单的流程监听器，外加容器平台的变更单同步。内嵌工单的 StatusSyncer：它的可选能力
// （Guard / ProgressListener 等，引擎按类型断言调用）原样保留，只在实例结束时多做一步。
type ticketWithK8sChange struct {
	*bizticket.StatusSyncer
	changes *bizk8s.ChangeSyncer
}

// OnInstanceFinished implements bizworkflow.Listener：先同步工单状态，再把审批结果落到变更单（同一事务）。
func (l ticketWithK8sChange) OnInstanceFinished(ctx context.Context, tx txrun.Tx, in *bizworkflow.Instance) error {
	if err := l.StatusSyncer.OnInstanceFinished(ctx, tx, in); err != nil {
		return err
	}
	return l.changes.Resolve(ctx, tx, in.BusinessID, k8sTicketStatus(in.Status))
}

// BeforeApprove implements bizworkflow.Guard：工单自己的检查之外，挂着容器平台变更单的工单不允许发起人
// 本人同意，也不接受超时自动同意（变更审批要求由他人审批）。in 可能是子流程：取根流程所挂的工单。
func (l ticketWithK8sChange) BeforeApprove(ctx context.Context, tx txrun.Tx, in *bizworkflow.Instance, n *bizworkflow.Node, userID string) error {
	if err := l.StatusSyncer.BeforeApprove(ctx, tx, in, n, userID); err != nil {
		return err
	}
	return l.changes.CheckApprover(ctx, tx, in.RootBusinessID, userID)
}

// k8sTicketStatus 流程实例终态 → 变更审批的工单结果。
func k8sTicketStatus(status int32) int32 {
	switch status {
	case bizworkflow.InstanceCompleted:
		return bizk8s.TicketApproved
	case bizworkflow.InstanceRejected:
		return bizk8s.TicketRejected
	case bizworkflow.InstanceCancelled:
		return bizk8s.TicketCancelled
	}
	return bizk8s.TicketTerminated
}
