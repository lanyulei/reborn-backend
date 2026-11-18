package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/go-kratos/kratos/v3/errors"
)

// 变更审批（功能设计 §7.3、§12.4）。
//
// 集群配置了审批用的工单模板后，PlaneUsecase 把每个写操作先交给 ChangeUsecase.Intercept：
//   - 调用者请求免审批（紧急变更）且持有免审批权限 → 放行，身份标记 Emergency（落审计）；
//   - 否则向数据面要一份变更计划（以调用者身份读现状并预检），存为变更单并提一张工单，
//     接口返回 428 K8S_CHANGE_SUBMITTED。集群此时没有被改动。
// 工单的流程结束时（ChangeSyncer，流程引擎的事务内）把变更单推进到已批准 / 审批未通过 / 已撤回；
// 已批准的写操作由数据面领取并以发起人身份执行（backend/k8s），申请权限由 core 自己执行（ChangeWorker）。

// 变更审批的哨兵错误。
var (
	ErrChangeNotFound    = errors.NotFound(v1.ErrorReason_K8S_CHANGE_NOT_FOUND.String(), "change not found")
	ErrChangeInvalid     = errors.BadRequest(v1.ErrorReason_K8S_CHANGE_INVALID.String(), "invalid change request")
	ErrChangeState       = errors.Conflict(v1.ErrorReason_K8S_CHANGE_STATE.String(), "the change is not pending approval")
	ErrBypassForbidden   = errors.Forbidden(v1.ErrorReason_K8S_CHANGE_BYPASS_FORBIDDEN.String(), "changes on this cluster require approval and you are not allowed to bypass it")
	ErrChangeTemplate    = errors.Conflict(v1.ErrorReason_K8S_CHANGE_TEMPLATE_UNAVAILABLE.String(), "the ticket template used for approval is unavailable")
	ErrRequestDisabled   = errors.Conflict(v1.ErrorReason_K8S_CHANGE_REQUEST_DISABLED.String(), "self-service requests are not enabled")
	errChangeSubmittedTo = errors.New(428, v1.ErrorReason_K8S_CHANGE_SUBMITTED.String(), "changes on this cluster require approval: a ticket has been created and the change will be executed once it is approved")
)

// 自助申请在审计 / 变更单里的动作名。
const (
	ActionNamespaceRequest  = "namespace_request"
	ActionPermissionRequest = "permission_request"
)

// 平台设置名（k8s_setting.name）。
const (
	SettingNamespaceTemplate  = "namespace_request_template_id"
	SettingPermissionTemplate = "permission_request_template_id"
)

// Change 一张变更单（k8s_change）。Display / Params 只在详情里读出。
type Change struct {
	ID            string
	Kind          int32
	ClusterID     string
	ClusterName   string
	Namespace     string
	APIGroup      string
	APIVersion    string
	Resource      string
	ResKind       string
	Name          string
	Action        string
	Op            string
	Summary       string
	Display       *kc.ChangeDisplay
	Params        json.RawMessage
	Status        int32
	RequesterID   string
	RequesterName string
	ClientIP      string
	TicketID      string
	TicketNumber  string
	Message       string
	Result        kc.ChangeResult
	CreateTime    time.Time
	ApproveTime   *time.Time
	ExpireTime    *time.Time
	FinishTime    *time.Time
}

// ChangeFilter 变更单列表过滤条件。RequesterID 非空只看该用户提交的；ClusterIDs 非 nil 限定在这些集群内
// （空切片 = 一个都看不到）。
type ChangeFilter struct {
	RequesterID string
	ClusterIDs  []string
	// Access 非 nil 时只返回该数据范围内的变更单（按集群与命名空间），外加 OrRequesterID 提交的。
	Access        *Access
	OrRequesterID string
	ClusterID     string
	Status        *int32
	Keyword       string
}

// ClusterApproval 集群的审批配置。
type ClusterApproval struct {
	Name        string
	DisplayName string
	Env         string
	Enabled     bool
	// TemplateID 审批所用的工单模板；空 = 变更不需审批。
	TemplateID string
}

// ChangeStore 变更单的建单与读取。
type ChangeStore interface {
	// Create 写入待审批的变更单；exec 为加密的执行计划（申请权限没有）。
	Create(ctx context.Context, c *Change, exec []byte) error
	// AttachTicket 记下工单；ticketID 为空表示建单失败，删除这张变更单。
	AttachTicket(ctx context.Context, id, ticketID, number string) error
	// Approval 集群的审批配置；不存在返回 ErrClusterNotFound。
	Approval(ctx context.Context, clusterID string) (*ClusterApproval, error)
}

// ChangeReader 变更单查询。
type ChangeReader interface {
	List(ctx context.Context, filter ChangeFilter, page, pageSize int64) ([]*Change, int64, error)
	// Get 含展示内容；不存在返回 ErrChangeNotFound。
	Get(ctx context.Context, id string) (*Change, error)
}

// ChangeFlow 变更单的状态推进。
type ChangeFlow interface {
	// PendingByTicketTx 事务内锁定工单对应的待审批变更单；没有返回 nil。
	PendingByTicketTx(ctx context.Context, tx txrun.Tx, ticketID string) (*Change, error)
	// ResolveTx 落审批结果。批准时写入发起人此刻的身份与失效时间（now + ttl，数据库时钟）。
	ResolveTx(ctx context.Context, tx txrun.Tx, id string, status int32, message string, requester *kc.ChangeRequester, ttl time.Duration) error
	// Finish 写入终态（仅对已批准 / 执行中的变更单生效）。
	Finish(ctx context.Context, id string, status int32, message string) error
}

// ChangeQueue core 侧的后台处理队列。
type ChangeQueue interface {
	// ClaimPermission 取一张已批准、未过期的「申请权限」变更单并置为执行中；没有返回 nil。
	ClaimPermission(ctx context.Context) (*Change, error)
	// Expire 把已批准但过了失效时间的变更单置为已过期，返回行数。
	Expire(ctx context.Context) (int, error)
	// NoteTx 事务内取一张已结束、结果尚未写回的变更单并标记为已写回（FOR UPDATE SKIP LOCKED）；没有返回 nil。
	NoteTx(ctx context.Context, tx txrun.Tx) (*Change, error)
}

// ApprovedChange 一张已批准、尚未开始执行的变更单。
type ApprovedChange struct {
	ID          string
	RequesterID string
}

// ChangeRefresher 已批准变更单的发起人身份刷新（data 层的 changeRepo 实现）。
type ChangeRefresher interface {
	// Approved 已批准、尚未被领取执行的变更单（至多 limit 张）。
	Approved(ctx context.Context, limit int) ([]ApprovedChange, error)
	// RefreshRequester 重写发起人的身份快照；只对仍处于已批准状态的变更单生效。
	RefreshRequester(ctx context.Context, id string, requester *kc.ChangeRequester) error
}

// SettingRepo 平台设置（k8s_setting）。
type SettingRepo interface {
	Get(ctx context.Context) (map[string]string, error)
	Set(ctx context.Context, values map[string]string, userID string) error
}

// ChangeTicket 一张要提的审批工单。
type ChangeTicket struct {
	TemplateID string
	Creator    string
	Title      string
	// RequestID 幂等键（变更单 ID）。
	RequestID string
	// Fields 可填入工单表单的内容（字段码 → 文本）；模板里没有的字段码忽略。
	Fields map[string]string
}

// 工单表单里本系统会填写的字段码：模板按需定义其中任意几个（单行或多行文本）。
// 模板的其他必填字段本系统填不了，这样的模板不能用于变更审批。
const (
	TicketFieldSummary   = "summary"   // 一句话说明
	TicketFieldCluster   = "cluster"   // 集群名称
	TicketFieldNamespace = "namespace" // 命名空间
	TicketFieldTarget    = "target"    // 目标对象
	TicketFieldDetail    = "detail"    // 涉及的字段 / 申请内容
	TicketFieldLink      = "link"      // 变更单链接
	TicketFieldReason    = "reason"    // 申请理由
)

// TicketFieldCodes 本系统会填写的全部字段码。
var TicketFieldCodes = []string{
	TicketFieldSummary, TicketFieldCluster, TicketFieldNamespace, TicketFieldTarget, TicketFieldDetail, TicketFieldLink, TicketFieldReason,
}

// ChangeTickets 工单域的窄接口（组合根以 ticket / workflow 域的用例实现）。
type ChangeTickets interface {
	// CheckTemplate 模板可用于变更审批：存在、已启用，且必填字段都在 TicketFieldCodes 内。
	// 不满足返回 ErrChangeTemplate 的具体说明。
	CheckTemplate(ctx context.Context, templateID string) error
	// Create 以发起人的名义提单，返回工单 ID、编号，以及流程是否已在发起时结束（finished）及其结果。
	Create(ctx context.Context, t ChangeTicket) (id, number string, finished bool, status int32, err error)
	// Cancel 以 userID 的名义撤销工单的审批流程（变更单由监听器同事务置为已撤回）。
	Cancel(ctx context.Context, ticketID, userID string) error
}

// TicketNotes 把执行结果写回工单（评论）。独立于 ChangeTickets：接口不超过三个方法。
type TicketNotes interface {
	Comment(ctx context.Context, ticketID, userID, content string) error
}

// TicketViewers 工单的查看权限（组合根以工单域的 Access 实现）：能看审批工单的人（提单人、流程的
// 审批人与参与者、持有工单管理视图的人）也要能看它所审批的变更单，否则只能凭工单里的摘要做决定。
type TicketViewers interface {
	CanViewTicket(ctx context.Context, userID, ticketID string) (bool, error)
	// TicketFinished 以提单人的名义读工单的流程状态：done 为流程是否已结束，status 为其结果（Ticket*）。
	TicketFinished(ctx context.Context, ticketID, creator string) (status int32, done bool, err error)
}

// BypassChecker 免审批（紧急变更）权限：持有 CheckChangeBypass 接口权限（组合根以 system 域的授权判定实现）。
type BypassChecker interface {
	CanBypass(ctx context.Context, userID string) (bool, error)
	// CanDebug 调试容器权限：持有 CheckDebugContainer 接口权限。打开终端的接口与普通终端是同一个，
	// 只按接口授权的话「终端」权限会连带得到调试容器；这里按 kind 再判一次。
	CanDebug(ctx context.Context, userID string) (bool, error)
}

// SelfRoles 自助授权所用的专属角色（组合根以 system 域的角色 / 用户用例实现）。
type SelfRoles interface {
	// Ensure 取得（没有则创建并绑定）该用户的专属角色，返回角色 ID。
	Ensure(ctx context.Context, userID, username string) (string, error)
}

// ChangeOptions 变更审批参数。
type ChangeOptions struct {
	// TTL 批准后的有效期。
	TTL time.Duration
	// SiteURL 前端站点地址（工单与通知里的变更单链接）；空 = 站内相对路径。
	SiteURL string
}

// ChangeSiteURL 前端站点地址（组合根取自 monitor.site_url）。
type ChangeSiteURL string

// ChangeUsecase 变更单的提交、查询、撤回，以及自助申请。
type ChangeUsecase struct {
	store    ChangeStore
	reader   ChangeReader
	flow     ChangeFlow
	settings SettingRepo
	clusters ClusterReader
	plane    Plane
	planes   *PlaneUsecase
	sealer   SecretSealer
	tickets  ChangeTickets
	bypass   BypassChecker
	roles    RoleSource
	viewers  TicketViewers
	syncer   *ChangeSyncer
	txm      txrun.Manager
	opts     ChangeOptions
	log      *slog.Logger
}

// ChangeDeps ChangeUsecase 的仓储与跨域依赖（聚合成一个参数，构造函数不至于过长）。
type ChangeDeps struct {
	Store    ChangeStore
	Reader   ChangeReader
	Flow     ChangeFlow
	Settings SettingRepo
	Clusters ClusterReader
	Plane    Plane
	Sealer   SecretSealer
	Tickets  ChangeTickets
	Bypass   BypassChecker
	Roles    RoleSource
	Viewers  TicketViewers
	Txm      txrun.Manager
}

// NewChangeDeps new a ChangeDeps.
func NewChangeDeps(store ChangeStore, reader ChangeReader, flow ChangeFlow, settings SettingRepo, clusters ClusterReader,
	plane Plane, sealer SecretSealer, tickets ChangeTickets, bypass BypassChecker, roles RoleSource, viewers TicketViewers, txm txrun.Manager,
) ChangeDeps {
	return ChangeDeps{
		Store: store, Reader: reader, Flow: flow, Settings: settings, Clusters: clusters, Plane: plane,
		Sealer: sealer, Tickets: tickets, Bypass: bypass, Roles: roles, Viewers: viewers, Txm: txm,
	}
}

// NewChangeUsecase new a ChangeUsecase，并把自己登记为数据面写操作的审批关口、集群表单里模板的校验器。
func NewChangeUsecase(d ChangeDeps, planes *PlaneUsecase, clusterUC *ClusterUsecase, syncer *ChangeSyncer, site ChangeSiteURL, logger *slog.Logger) *ChangeUsecase {
	uc := &ChangeUsecase{
		store: d.Store, reader: d.Reader, flow: d.Flow, settings: d.Settings, clusters: d.Clusters, plane: d.Plane, planes: planes,
		sealer: d.Sealer, tickets: d.Tickets, bypass: d.Bypass, roles: d.Roles, viewers: d.Viewers, syncer: syncer, txm: d.Txm,
		opts: ChangeOptions{TTL: kc.DefaultChangeTTL, SiteURL: strings.TrimRight(string(site), "/")}, log: logger,
	}
	planes.gate = uc
	planes.debug = d.Bypass
	clusterUC.templates = d.Tickets
	return uc
}

// WithTTL 覆盖批准后的有效期（仅测试）。
func (uc *ChangeUsecase) WithTTL(ttl time.Duration) { uc.syncer.ttl = ttl }

// writeGate 数据面写操作的审批关口（PlaneUsecase 在转发写操作前调用）。
type writeGate interface {
	// Intercept 返回 nil 表示放行（可能已把 id 标记为紧急变更）；返回错误表示不执行——
	// 其中 K8S_CHANGE_SUBMITTED 表示已转为变更单。
	Intercept(ctx context.Context, caller Caller, id *kc.Identity, clusterID, op string, req any) error
}

// Intercept implements writeGate.
func (uc *ChangeUsecase) Intercept(ctx context.Context, caller Caller, id *kc.Identity, clusterID, op string, req any) error {
	appr, err := uc.store.Approval(ctx, clusterID)
	if err != nil {
		return err
	}
	if appr.TemplateID == "" {
		return nil
	}
	if caller.Emergency {
		ok, err := uc.bypass.CanBypass(ctx, caller.UserID)
		if err != nil {
			return err
		}
		if !ok && !id.IsAdmin {
			return ErrBypassForbidden
		}
		id.Emergency = true
		return nil
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	raw, err := uc.plane.Call(ctx, *id, clusterID, kc.OpChangePlan, kc.ChangePlanRequest{Op: op, Body: body})
	if err != nil {
		return err
	}
	var plan kc.ChangePlanResponse
	if err := json.Unmarshal(raw, &plan); err != nil {
		return fmt.Errorf("decode change plan: %w", err)
	}
	execRaw, err := json.Marshal(plan.Exec)
	if err != nil {
		return err
	}
	d := plan.Display
	c := &Change{
		Kind: kc.ChangeKindOp, ClusterID: clusterID, ClusterName: appr.Name, Namespace: d.Target.Namespace,
		APIGroup: d.Target.Group, APIVersion: d.Target.Version, Resource: d.Target.Resource, ResKind: d.Kind, Name: d.Target.Name,
		Action: d.Action, Op: op, Summary: d.Summary, Display: &d,
		RequesterID: caller.UserID, RequesterName: id.Username, ClientIP: caller.ClientIP,
	}
	return uc.submit(ctx, c, string(execRaw), appr.TemplateID, changeDetailText(&d), "")
}

// changeDetailText 工单里的变更内容：每个对象一段，列出涉及的字段（值已由数据面掩码与截断）。
func changeDetailText(d *kc.ChangeDisplay) string {
	var b strings.Builder
	for i, it := range d.Items {
		if i == 20 {
			fmt.Fprintf(&b, "… 共 %d 个对象\n", len(d.Items))
			break
		}
		target := it.Kind + " " + it.Ref.Name
		if it.Ref.Namespace != "" {
			target = it.Kind + " " + it.Ref.Namespace + "/" + it.Ref.Name
		}
		fmt.Fprintf(&b, "[%s] %s\n", it.Action, target)
		for j, f := range it.Fields {
			if j == 15 {
				fmt.Fprintf(&b, "  … 共 %d 个字段\n", len(it.Fields))
				break
			}
			switch {
			case f.Before == "" && f.After == "":
				fmt.Fprintf(&b, "  %s\n", f.Path)
			case f.Before == "":
				fmt.Fprintf(&b, "  %s: %s\n", f.Path, f.After)
			case f.After == "":
				fmt.Fprintf(&b, "  %s: %s → (删除)\n", f.Path, f.Before)
			default:
				fmt.Fprintf(&b, "  %s: %s → %s\n", f.Path, f.Before, f.After)
			}
		}
	}
	return truncateRunes(strings.TrimRight(b.String(), "\n"), 4000)
}

func truncateRunes(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}

// changeLink 变更单页面的链接。
func (uc *ChangeUsecase) changeLink(id string) string {
	return uc.opts.SiteURL + "/k8s/change?id=" + id
}

// submit 落变更单并提工单，成功后返回 K8S_CHANGE_SUBMITTED（带变更单与工单的标识）。
// 变更单与工单不在一个事务里（工单用例自带事务）：先落变更单，提单失败即删除它。
func (uc *ChangeUsecase) submit(ctx context.Context, c *Change, exec, templateID, detailText, reason string) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	c.ID, c.Status = id, kc.ChangePending
	var sealed []byte
	if exec != "" {
		// 把变更单与集群的标识封进执行计划：数据面打开时核对，防止计划被挪到另一张变更单或另一个集群上执行。
		var plan kc.ChangeExec
		if err := json.Unmarshal([]byte(exec), &plan); err != nil {
			return fmt.Errorf("decode change plan: %w", err)
		}
		plan.ChangeID, plan.ClusterID = c.ID, c.ClusterID
		bound, err := json.Marshal(plan)
		if err != nil {
			return err
		}
		if sealed, err = uc.sealer.Seal(string(bound)); err != nil {
			return err
		}
	}
	if err := uc.store.Create(ctx, c, sealed); err != nil {
		return err
	}
	target := strings.TrimSpace(c.ResKind + " " + c.Name)
	title := truncateRunes(fmt.Sprintf("[K8s 变更] %s：%s", c.ClusterName, c.Summary), 200)
	ticketID, number, finished, status, err := uc.tickets.Create(ctx, ChangeTicket{
		TemplateID: templateID, Creator: c.RequesterID, Title: title, RequestID: id,
		Fields: map[string]string{
			TicketFieldSummary: truncateRunes(c.Summary, 250), TicketFieldCluster: c.ClusterName, TicketFieldNamespace: c.Namespace,
			TicketFieldTarget: target, TicketFieldDetail: detailText, TicketFieldLink: uc.changeLink(id), TicketFieldReason: reason,
		},
	})
	if err != nil {
		// 清理用脱离取消的 ctx：请求被取消时也不能留下没有工单的变更单。
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if derr := uc.store.AttachTicket(cctx, id, "", ""); derr != nil {
			uc.log.ErrorContext(ctx, "remove orphan k8s change failed", "change_id", id, "error", derr)
		}
		return err
	}
	if err := uc.store.AttachTicket(ctx, id, ticketID, number); err != nil {
		return err
	}
	if !finished && uc.viewers != nil {
		// 提单与挂接之间流程可能已经走完（审批人手快，或流程里只有自动环节）：监听器触发时按工单找不到
		// 变更单，结果就落空，变更单会永远停在待审批。挂接后再看一眼工单的流程状态，已结束则补落结果。
		if st, done, err := uc.viewers.TicketFinished(ctx, ticketID, c.RequesterID); err != nil {
			uc.log.WarnContext(ctx, "recheck k8s change ticket failed", "change_id", id, "ticket_id", ticketID, "error", err)
		} else if done {
			if err := uc.txm.WithinTx(ctx, func(tx txrun.Tx) error { return uc.syncer.Resolve(ctx, tx, ticketID, st) }); err != nil {
				return err
			}
		}
	}
	if finished {
		// 流程在发起时即已结束（全部节点被跳过）：监听器触发时变更单还没有挂上工单，这里补落结果。
		// 这种「通过」没有经过任何人：变更审批要求至少有一位他人同意，按未通过处理。
		if status == TicketApproved {
			status = TicketUnreviewed
		}
		if err := uc.txm.WithinTx(ctx, func(tx txrun.Tx) error { return uc.syncer.Resolve(ctx, tx, ticketID, status) }); err != nil {
			return err
		}
	}
	return errChangeSubmittedTo.WithMetadata(map[string]string{"change_id": id, "ticket_id": ticketID, "ticket_number": number})
}

// Submitted 从 K8S_CHANGE_SUBMITTED 错误里取出变更单与工单标识；不是该错误返回 ok = false。
func Submitted(err error) (changeID, ticketID, number string, ok bool) {
	e := errors.FromError(err)
	if e == nil || e.Reason != v1.ErrorReason_K8S_CHANGE_SUBMITTED.String() {
		return "", "", "", false
	}
	return e.Metadata["change_id"], e.Metadata["ticket_id"], e.Metadata["ticket_number"], true
}

// callerAccess 调用者的数据范围。
func (uc *ChangeUsecase) callerAccess(ctx context.Context, userID string) (Access, error) {
	if uc.planes.access == nil { // 仅单元测试：未装配范围解析时按超管与否二分
		isAdmin, _, err := uc.roles.EnabledRoleIDs(ctx, userID)
		if err != nil {
			return Access{}, err
		}
		if isAdmin {
			return Access{All: true}, nil
		}
		return NewAccess(nil), nil
	}
	return uc.planes.access.Resolve(ctx, userID)
}

// changeVisible 变更单的内容（变更前后的对象）是否对该数据范围可见：涉及的每个对象所在的命名空间都在
// 范围内才可见；集群级对象要求「全部命名空间」的授权。只按集群判断会让 A 命名空间的只读用户读到
// B 命名空间里 ConfigMap、环境变量的内容。
func changeVisible(access Access, c *Change) bool {
	if !access.AllowsObject(c.ClusterID, c.Namespace) {
		return false
	}
	if c.Display != nil {
		for _, it := range c.Display.Items {
			if !access.AllowsObject(c.ClusterID, it.Ref.Namespace) {
				return false
			}
		}
	}
	return true
}

// List 分页查询变更单。mine 只看自己提交的；否则看数据范围内（按集群与命名空间）的全部，外加自己提交的。
func (uc *ChangeUsecase) List(ctx context.Context, caller Caller, mine bool, filter ChangeFilter, page, pageSize int64) ([]*Change, int64, error) {
	if mine {
		filter.RequesterID = caller.UserID
		return uc.reader.List(ctx, filter, page, pageSize)
	}
	access, err := uc.callerAccess(ctx, caller.UserID)
	if err != nil {
		return nil, 0, err
	}
	if !access.All {
		filter.Access, filter.OrRequesterID = &access, caller.UserID
	}
	return uc.reader.List(ctx, filter, page, pageSize)
}

// Get 变更单详情：发起人本人、超管，或数据范围覆盖该变更涉及的全部命名空间的用户可见；否则按不存在处理。
func (uc *ChangeUsecase) Get(ctx context.Context, caller Caller, id string) (*Change, error) {
	c, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if c.RequesterID == caller.UserID {
		return c, nil
	}
	access, err := uc.callerAccess(ctx, caller.UserID)
	if err != nil {
		return nil, err
	}
	if changeVisible(access, c) {
		return c, nil
	}
	// 审批这张变更的人不一定在该集群上有数据范围：能看审批工单的人可以看它所审批的变更单。
	if c.TicketID != "" && uc.viewers != nil {
		if ok, err := uc.viewers.CanViewTicket(ctx, caller.UserID, c.TicketID); err != nil || ok {
			return c, err
		}
	}
	return nil, ErrChangeNotFound
}

// Cancel 撤回待审批的变更单（发起人本人或超管）：撤销工单的审批流程，变更单由监听器同事务置为已撤回。
func (uc *ChangeUsecase) Cancel(ctx context.Context, caller Caller, id string) error {
	c, err := uc.reader.Get(ctx, id)
	if err != nil {
		return err
	}
	if c.RequesterID != caller.UserID {
		isAdmin, _, err := uc.roles.EnabledRoleIDs(ctx, caller.UserID)
		if err != nil {
			return err
		}
		if !isAdmin {
			return ErrChangeNotFound
		}
	}
	if c.Status != kc.ChangePending || c.TicketID == "" {
		return ErrChangeState
	}
	// 以发起人的名义撤销：流程引擎只允许发起人撤回自己的流程。
	return uc.tickets.Cancel(ctx, c.TicketID, c.RequesterID)
}

// Settings 平台设置。
func (uc *ChangeUsecase) Settings(ctx context.Context) (nsTemplate, permTemplate string, err error) {
	m, err := uc.settings.Get(ctx)
	if err != nil {
		return "", "", err
	}
	return m[SettingNamespaceTemplate], m[SettingPermissionTemplate], nil
}

// UpdateSettings 设置自助申请所用的工单模板（空 = 关闭）；非空的模板须可用于变更审批。
func (uc *ChangeUsecase) UpdateSettings(ctx context.Context, caller Caller, nsTemplate, permTemplate string) error {
	for _, t := range []string{nsTemplate, permTemplate} {
		if t != "" {
			if err := uc.tickets.CheckTemplate(ctx, t); err != nil {
				return err
			}
		}
	}
	return uc.settings.Set(ctx, map[string]string{SettingNamespaceTemplate: nsTemplate, SettingPermissionTemplate: permTemplate}, caller.UserID)
}

// RequestClusters 可申请权限的集群（全部已启用的集群，只含名称与环境）；未开放权限申请时为空。
func (uc *ChangeUsecase) RequestClusters(ctx context.Context) ([]*ClusterOption, error) {
	_, perm, err := uc.Settings(ctx)
	if err != nil || perm == "" {
		return nil, err
	}
	return uc.clusters.Options(ctx, true, nil)
}

var (
	quantityPattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(m|k|Ki|M|Mi|G|Gi|T|Ti)?$`)
	nsNamePattern   = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
)

// ValidateNamespaceRequest 校验申请命名空间的内容：名称合法且不占用系统前缀；配额项与缺省限制只允许
// 白名单内的键，值为 K8s 数量。执行器（数据面）只按这份结构化内容生成 Namespace / ResourceQuota / LimitRange。
func ValidateNamespaceRequest(r *kc.NamespaceRequest) error {
	if !nsNamePattern.MatchString(r.Name) {
		return detail(ErrChangeInvalid, "namespace name must be lowercase letters, digits and hyphens (1-63 characters)")
	}
	if strings.HasPrefix(r.Name, "kube-") || r.Name == "default" || r.Name == "reborn-system" {
		return detail(ErrChangeInvalid, "namespace name %q is reserved", r.Name)
	}
	if len(r.Labels) > 0 {
		return detail(ErrChangeInvalid, "labels cannot be requested")
	}
	check := func(m map[string]string, allowed map[string]struct{}, what string) error {
		for k, v := range m {
			if _, ok := allowed[k]; !ok {
				return detail(ErrChangeInvalid, "%s %q is not allowed", what, k)
			}
			if !quantityPattern.MatchString(v) {
				return detail(ErrChangeInvalid, "%s %s: %q is not a valid quantity", what, k, v)
			}
		}
		return nil
	}
	if err := check(r.Quota, kc.NamespaceQuotaKeys, "quota item"); err != nil {
		return err
	}
	if err := check(r.LimitDefault, kc.NamespaceLimitKeys, "default limit"); err != nil {
		return err
	}
	return check(r.LimitDefaultRequest, kc.NamespaceLimitKeys, "default request")
}

func sortedPairs(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	// 小集合：插入排序即可，免引入 sort 只为一处。
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// requestCluster 自助申请的前置：对应入口已开放、集群存在且已启用。返回集群与所用模板。
func (uc *ChangeUsecase) requestCluster(ctx context.Context, clusterID, setting string) (*ClusterApproval, string, error) {
	m, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, "", err
	}
	tpl := m[setting]
	if tpl == "" {
		return nil, "", ErrRequestDisabled
	}
	appr, err := uc.store.Approval(ctx, clusterID)
	if err != nil {
		return nil, "", err
	}
	if !appr.Enabled {
		return nil, "", ErrClusterNotFound
	}
	return appr, tpl, nil
}

// RequestNamespace 申请命名空间与配额。成功时返回 K8S_CHANGE_SUBMITTED（用 Submitted 取标识）。
func (uc *ChangeUsecase) RequestNamespace(ctx context.Context, caller Caller, clusterID string, req kc.NamespaceRequest, reason string) error {
	if err := ValidateNamespaceRequest(&req); err != nil {
		return err
	}
	appr, tpl, err := uc.requestCluster(ctx, clusterID, SettingNamespaceTemplate)
	if err != nil {
		return err
	}
	id, err := uc.planes.identity(ctx, caller, "")
	if err != nil {
		return err
	}
	nsRef := kc.Ref{Version: "v1", Resource: "namespaces", Name: req.Name}
	d := kc.ChangeDisplay{
		Action: ActionNamespaceRequest, Target: nsRef, Kind: "Namespace", Summary: "申请命名空间 " + req.Name,
		Items: []kc.ChangeItemDisplay{{Ref: nsRef, Kind: "Namespace", Action: kc.ActionCreate, Fields: []kc.ChangeField{}}},
	}
	var lines []string
	if len(req.Quota) > 0 {
		fields := []kc.ChangeField{}
		for _, p := range sortedPairs(req.Quota) {
			k, v, _ := strings.Cut(p, "=")
			fields = append(fields, kc.ChangeField{Path: "spec.hard." + k, After: v})
		}
		d.Items = append(d.Items, kc.ChangeItemDisplay{
			Ref:  kc.Ref{Version: "v1", Resource: "resourcequotas", Namespace: req.Name, Name: "default"},
			Kind: "ResourceQuota", Action: kc.ActionCreate, Fields: fields,
		})
		lines = append(lines, "配额："+strings.Join(sortedPairs(req.Quota), "，"))
	}
	if len(req.LimitDefault)+len(req.LimitDefaultRequest) > 0 {
		fields := []kc.ChangeField{}
		for _, p := range sortedPairs(req.LimitDefault) {
			k, v, _ := strings.Cut(p, "=")
			fields = append(fields, kc.ChangeField{Path: "default." + k, After: v})
		}
		for _, p := range sortedPairs(req.LimitDefaultRequest) {
			k, v, _ := strings.Cut(p, "=")
			fields = append(fields, kc.ChangeField{Path: "defaultRequest." + k, After: v})
		}
		d.Items = append(d.Items, kc.ChangeItemDisplay{
			Ref:  kc.Ref{Version: "v1", Resource: "limitranges", Namespace: req.Name, Name: "default"},
			Kind: "LimitRange", Action: kc.ActionCreate, Fields: fields,
		})
		lines = append(lines, "容器缺省 limits："+strings.Join(sortedPairs(req.LimitDefault), "，"), "容器缺省 requests："+strings.Join(sortedPairs(req.LimitDefaultRequest), "，"))
	}
	execRaw, err := json.Marshal(kc.ChangeExec{Namespace: &req})
	if err != nil {
		return err
	}
	params, _ := json.Marshal(req)
	c := &Change{
		Kind: kc.ChangeKindNamespace, ClusterID: clusterID, ClusterName: appr.Name, APIVersion: "v1", Resource: "namespaces",
		ResKind: "Namespace", Name: req.Name, Action: ActionNamespaceRequest, Summary: d.Summary, Display: &d, Params: params,
		RequesterID: caller.UserID, RequesterName: id.Username, ClientIP: caller.ClientIP,
	}
	return uc.submit(ctx, c, string(execRaw), tpl, strings.Join(lines, "\n"), reason)
}

// LevelName 档位的中文名。
func LevelName(level int32) string {
	switch level {
	case kc.LevelView:
		return "只读"
	case kc.LevelEdit:
		return "开发"
	case kc.LevelAdmin:
		return "命名空间管理员"
	case kc.LevelClusterAdmin:
		return "集群管理员"
	}
	return ""
}

// RequestPermission 申请集群权限（档位 1–3）。成功时返回 K8S_CHANGE_SUBMITTED。
func (uc *ChangeUsecase) RequestPermission(ctx context.Context, caller Caller, clusterID string, req kc.PermissionRequest) error {
	if req.Level < kc.LevelView || req.Level > kc.LevelAdmin {
		return detail(ErrChangeInvalid, "only the view, edit and namespace-admin levels can be requested")
	}
	if err := checkSelfServiceScope(req); err != nil {
		return err
	}
	appr, tpl, err := uc.requestCluster(ctx, clusterID, SettingPermissionTemplate)
	if err != nil {
		return err
	}
	id, err := uc.planes.identity(ctx, caller, "")
	if err != nil {
		return err
	}
	ns := req.Namespace
	nsText := "命名空间 " + ns
	if ns == kc.AllNamespaces {
		ns, nsText = "", "全部命名空间"
	}
	summary := fmt.Sprintf("申请权限：%s（%s）", LevelName(req.Level), nsText)
	d := kc.ChangeDisplay{Action: ActionPermissionRequest, Summary: summary, Items: []kc.ChangeItemDisplay{}}
	params, _ := json.Marshal(req)
	c := &Change{
		Kind: kc.ChangeKindPermission, ClusterID: clusterID, ClusterName: appr.Name, Namespace: ns, Action: ActionPermissionRequest,
		Summary: summary, Display: &d, Params: params, RequesterID: caller.UserID, RequesterName: id.Username, ClientIP: caller.ClientIP,
	}
	return uc.submit(ctx, c, "", tpl, fmt.Sprintf("申请人：%s\n档位：%s\n范围：%s", id.Username, LevelName(req.Level), nsText), req.Reason)
}

// ---------------------------------------------------------------------------
// 审批结果：流程引擎的监听器（同事务）
// ---------------------------------------------------------------------------

// 工单流程的终态（与 workflow 域的实例状态同值；组合根的适配器负责对应）。
const (
	TicketApproved   int32 = 2
	TicketRejected   int32 = 3
	TicketCancelled  int32 = 4
	TicketTerminated int32 = 5
	// TicketUnreviewed 流程在发起时即告完成（全部环节被跳过）：没有人审批过，按未通过处理。
	TicketUnreviewed int32 = -1
)

// ChangeSyncer 工单流程结束时把结果落到变更单上。独立于 ChangeUsecase：它被登记到流程引擎的监听器，
// 而 ChangeUsecase 经 ChangeTickets 依赖工单 / 流程用例——合在一起会成环（与作业审批同一做法）。
type ChangeSyncer struct {
	flow  ChangeFlow
	roles RoleSource
	ttl   time.Duration
}

// NewChangeSyncer new a ChangeSyncer.
func NewChangeSyncer(flow ChangeFlow, roles RoleSource) *ChangeSyncer {
	return &ChangeSyncer{flow: flow, roles: roles, ttl: kc.DefaultChangeTTL}
}

// ErrSelfApproval 变更的发起人不能审批自己的变更；也不接受超时自动同意。
var ErrSelfApproval = errors.Forbidden(v1.ErrorReason_K8S_CHANGE_STATE.String(), "a change must be approved by someone other than its requester")

// CheckApprover 在「同意」动作生效之前询问（流程引擎的 Guard，同事务）：这张工单挂着待审批的变更单时，
// 发起人本人不能同意，系统的超时自动同意（userID 为空）也不算数——变更审批要求由他人审批。
// 这样流程一旦以「通过」结束，其中的每一次同意都出自发起人以外的人。
func (s *ChangeSyncer) CheckApprover(ctx context.Context, tx txrun.Tx, ticketID, userID string) error {
	c, err := s.flow.PendingByTicketTx(ctx, tx, ticketID)
	if err != nil || c == nil {
		return err
	}
	if userID == "" || userID == c.RequesterID {
		return ErrSelfApproval
	}
	return nil
}

// Resolve 在流程引擎的事务内落审批结果：通过 → 已批准（记下发起人此刻的启用角色与是否超管，数据面据此
// 以发起人身份执行）；驳回 / 终止 → 审批未通过；撤回 → 已撤回。工单没有对应的待审批变更单时什么都不做。
func (s *ChangeSyncer) Resolve(ctx context.Context, tx txrun.Tx, ticketID string, ticketStatus int32) error {
	c, err := s.flow.PendingByTicketTx(ctx, tx, ticketID)
	if err != nil || c == nil {
		return err
	}
	switch ticketStatus {
	case TicketUnreviewed:
		return s.flow.ResolveTx(ctx, tx, c.ID, kc.ChangeRejected, "审批流程未经任何人审批即结束（流程没有可用的审批环节），变更未获批准", nil, 0)
	case TicketApproved:
		// 角色查询复用事务连接（事务里再占一条连接，在连接池打满时会互相等死）。
		isAdmin, roleIDs, err := s.roles.EnabledRoleIDs(txrun.NewContext(ctx, tx), c.RequesterID)
		if err != nil {
			return fmt.Errorf("resolve k8s change requester: %w", err)
		}
		if roleIDs == nil {
			roleIDs = []string{}
		}
		req := &kc.ChangeRequester{UserID: c.RequesterID, Username: c.RequesterName, RoleIDs: roleIDs, IsAdmin: isAdmin}
		return s.flow.ResolveTx(ctx, tx, c.ID, kc.ChangeApproved, "", req, s.ttl)
	case TicketCancelled:
		return s.flow.ResolveTx(ctx, tx, c.ID, kc.ChangeCancelled, "审批已撤回", nil, 0)
	case TicketRejected:
		return s.flow.ResolveTx(ctx, tx, c.ID, kc.ChangeRejected, "审批被驳回", nil, 0)
	default:
		return s.flow.ResolveTx(ctx, tx, c.ID, kc.ChangeRejected, "审批流程被终止", nil, 0)
	}
}

// ---------------------------------------------------------------------------
// 后台：执行权限申请、过期、把结果写回工单并通知发起人
// ---------------------------------------------------------------------------

// ChangeWorker core 的 k8s-change 循环。多实例并发安全：领取都是行锁 SKIP LOCKED 或条件更新。
type ChangeWorker struct {
	queue  ChangeQueue
	flow   ChangeFlow
	scopes ScopeRepo
	roles  SelfRoles
	notes  TicketNotes
	events biznotify.NotificationWriter
	// refresh / identities / accounts 已批准变更单的发起人身份刷新（nil = 不刷新，仅单元测试）。
	refresh    ChangeRefresher
	identities RoleSource
	accounts   Accounts
	txm        txrun.Manager
	site       string
	log        *slog.Logger
	now        func() time.Time
}

// NewChangeWorker new a ChangeWorker.
func NewChangeWorker(queue ChangeQueue, flow ChangeFlow, scopes ScopeRepo, roles SelfRoles, notes TicketNotes,
	events biznotify.NotificationWriter, refresh ChangeRefresher, identities RoleSource, accounts Accounts,
	txm txrun.Manager, site ChangeSiteURL, logger *slog.Logger,
) *ChangeWorker {
	return &ChangeWorker{
		queue: queue, flow: flow, scopes: scopes, roles: roles, notes: notes, events: events, txm: txm,
		refresh: refresh, identities: identities, accounts: accounts,
		site: strings.TrimRight(string(site), "/"), log: logger, now: time.Now,
	}
}

const changeWorkerBatch = 20

// Run 处理一轮，返回处理的行数（有积压时立即再跑一轮）。
func (w *ChangeWorker) Run(ctx context.Context) (int, error) {
	done := 0
	if err := w.refreshRequesters(ctx); err != nil {
		return done, err
	}
	for range changeWorkerBatch {
		c, err := w.queue.ClaimPermission(ctx)
		if err != nil {
			return done, err
		}
		if c == nil {
			break
		}
		status, msg := kc.ChangeSucceeded, ""
		if err := w.grant(ctx, c); err != nil {
			status, msg = kc.ChangeFailed, truncateRunes(errors.FromError(err).Message, kc.MaxChangeMessage)
			w.log.WarnContext(ctx, "grant requested k8s permission failed", "change_id", c.ID, "error", err)
		}
		if err := w.flow.Finish(ctx, c.ID, status, msg); err != nil {
			return done, err
		}
		done++
	}
	if _, err := w.queue.Expire(ctx); err != nil {
		return done, err
	}
	for range changeWorkerBatch {
		var c *Change
		err := w.txm.WithinTx(ctx, func(tx txrun.Tx) error {
			var err error
			if c, err = w.queue.NoteTx(ctx, tx); err != nil || c == nil {
				return err
			}
			return w.notify(ctx, tx, c)
		})
		if err != nil {
			return done, err
		}
		if c == nil {
			break
		}
		w.comment(ctx, c)
		done++
	}
	if done < changeWorkerBatch {
		return 0, nil
	}
	return done, nil
}

// MergeScope 把一条授权并入角色已有的数据范围：同一集群同一命名空间取较高档位。
func MergeScope(cur []*RoleScope, clusterID, namespace string, level int32) []*RoleScope {
	out := make([]*RoleScope, 0, len(cur)+1)
	found := false
	for _, s := range cur {
		n := *s
		if n.ClusterID == clusterID && n.Namespace == namespace {
			found = true
			n.Level = max(n.Level, level)
		}
		out = append(out, &n)
	}
	if !found {
		out = append(out, &RoleScope{ClusterID: clusterID, Namespace: namespace, Level: level})
	}
	return out
}

// grant 执行一张「申请权限」变更单：把授权记到申请人的专属角色上（没有则创建并绑定给他）。
// 审批即授权：这里以系统身份写入，审计记申请人与变更单。
func (w *ChangeWorker) grant(ctx context.Context, c *Change) error {
	var req kc.PermissionRequest
	if err := json.Unmarshal(c.Params, &req); err != nil {
		return detail(ErrChangeInvalid, "malformed permission request")
	}
	if req.Level < kc.LevelView || req.Level > kc.LevelAdmin {
		return detail(ErrChangeInvalid, "level %d cannot be granted by a request", req.Level)
	}
	if err := checkSelfServiceScope(req); err != nil { // 执行前再校验一次：规则收紧前提交的申请同样受限
		return err
	}
	roleID, err := w.roles.Ensure(ctx, c.RequesterID, c.RequesterName)
	if err != nil {
		return err
	}
	cur, err := w.scopes.Get(ctx, roleID)
	if err != nil {
		return err
	}
	det, _ := json.Marshal(map[string]any{"changeId": c.ID, "clusterId": c.ClusterID, "namespace": req.Namespace, "level": req.Level})
	return w.scopes.Replace(ctx, roleID, MergeScope(cur, c.ClusterID, req.Namespace, req.Level), &AuditEntry{
		UserID: c.RequesterID, Username: c.RequesterName, ClientIP: c.ClientIP, RequestID: c.ID,
		Resource: kc.AuditResourceRoleScope, Action: kc.ActionScopeReplace, Detail: det,
	})
}

func changeStatusText(status int32) string {
	switch status {
	case kc.ChangeSucceeded:
		return "已执行成功"
	case kc.ChangeFailed:
		return "执行失败"
	case kc.ChangeExpired:
		return "已批准但超过有效期未能执行，已作废"
	case kc.ChangeRejected:
		return "审批未通过"
	case kc.ChangeCancelled:
		return "已撤回"
	}
	return ""
}

// notify 事务内给发起人写一条结果通知。审批未通过 / 已撤回不另发（工单流程自己会通知）。
func (w *ChangeWorker) notify(ctx context.Context, tx txrun.Tx, c *Change) error {
	if c.Status != kc.ChangeSucceeded && c.Status != kc.ChangeFailed && c.Status != kc.ChangeExpired {
		return nil
	}
	eventType, severity, title := kc.NotifyChangeFailed, int32(3), "变更未执行："+c.Summary
	if c.Status == kc.ChangeSucceeded {
		eventType, severity, title = kc.NotifyChangeSucceeded, 1, "变更已执行："+c.Summary
	}
	msg := c.Message
	if c.Status == kc.ChangeExpired && msg == "" {
		msg = "批准后超过有效期，未执行"
	}
	payload, err := json.Marshal(map[string]any{
		"title": truncateRunes(title, 200), "change_id": c.ID, "cluster_id": c.ClusterID, "cluster_name": c.ClusterName,
		"namespace": c.Namespace, "target": strings.TrimSpace(c.ResKind + " " + c.Name), "summary": c.Summary,
		"status_text": changeStatusText(c.Status), "message": msg, "ticket_id": c.TicketID, "ticket_number": c.TicketNumber,
		"change_url": w.site + "/k8s/change?id=" + c.ID, "time": w.now().Format("2006-01-02 15:04:05"),
	})
	if err != nil {
		return err
	}
	_, err = w.events.NotifyEvent(ctx, tx, biznotify.Event{
		EventType: eventType, Severity: severity, Title: truncateRunes(title, 200), Payload: payload,
		DedupKey: "k8s:change:" + c.ID, GroupKey: "k8s:change:" + c.ClusterID, Recipients: []string{c.RequesterID},
	})
	if e := errors.FromError(err); err != nil && e != nil && e.Code >= 400 && e.Code < 500 {
		// 通知内容被通知域拒绝只影响这一条通知，不让它卡住结果回写。
		w.log.WarnContext(ctx, "k8s change notification rejected", "change_id", c.ID, "error", err)
		return nil
	}
	return err
}

// comment 把执行结果以评论写回工单（尽力而为：失败只记日志，结果仍可在变更单页面看到）。
func (w *ChangeWorker) comment(ctx context.Context, c *Change) {
	if c.TicketID == "" || (c.Status != kc.ChangeSucceeded && c.Status != kc.ChangeFailed && c.Status != kc.ChangeExpired) {
		return
	}
	text := "【系统】变更" + changeStatusText(c.Status)
	if c.Message != "" {
		text += "：" + truncateRunes(c.Message, 1500)
	}
	if err := w.notes.Comment(ctx, c.TicketID, c.RequesterID, text); err != nil {
		w.log.WarnContext(ctx, "write k8s change result to ticket failed", "change_id", c.ID, "ticket_id", c.TicketID, "error", err)
	}
}

// maxRefreshChanges 每轮刷新的已批准变更单上限。
const maxRefreshChanges = 500

// refreshRequesters 已批准、尚未执行的变更单按发起人此刻的状态刷新身份快照：批准时记下的启用角色与
// 是否超管最长可以存活到失效时间（集群离线时变更单一直停在已批准），其间发起人可能被停用、删除或
// 解绑角色。账号已不可用 → 变更单直接置为失败；否则把快照换成当前的角色（数据面据此以发起人身份执行，
// 权限仍由集群判定）。每轮循环执行一次，生效延迟为一个循环周期。
func (w *ChangeWorker) refreshRequesters(ctx context.Context) error {
	if w.refresh == nil {
		return nil
	}
	items, err := w.refresh.Approved(ctx, maxRefreshChanges)
	if err != nil {
		return err
	}
	type state struct {
		active bool
		req    *kc.ChangeRequester
	}
	users := map[string]*state{}
	for _, it := range items {
		st := users[it.RequesterID]
		if st == nil {
			st = &state{}
			users[it.RequesterID] = st
			active, err := w.accounts.Active(ctx, it.RequesterID)
			if err != nil && errors.FromError(err).Code == http.StatusInternalServerError {
				return err
			}
			if err == nil && active {
				isAdmin, roleIDs, err := w.identities.EnabledRoleIDs(ctx, it.RequesterID)
				if err != nil {
					return err
				}
				if roleIDs == nil {
					roleIDs = []string{}
				}
				st.active, st.req = true, &kc.ChangeRequester{UserID: it.RequesterID, RoleIDs: roleIDs, IsAdmin: isAdmin}
			}
		}
		if !st.active {
			if err := w.flow.Finish(ctx, it.ID, kc.ChangeFailed, "发起人的账号已停用或已删除，变更未执行"); err != nil {
				return err
			}
			continue
		}
		if err := w.refresh.RefreshRequester(ctx, it.ID, st.req); err != nil {
			return err
		}
	}
	return nil
}

// selfServiceSystemNamespaces 不接受自助申请的系统命名空间。
var selfServiceSystemNamespaces = map[string]struct{}{
	"kube-system": {}, "kube-public": {}, "kube-node-lease": {}, "reborn-system": {},
}

// checkSelfServiceScope 自助申请权限的范围限制：「全部命名空间」只能申请只读——开发 / 命名空间管理员档位
// 绑到全部命名空间等于能读写 kube-system 里的 Secret 与工作负载，实际上就是集群管理员，而集群管理员
// 不允许自助申请；系统命名空间一律不接受申请。
func checkSelfServiceScope(req kc.PermissionRequest) error {
	if req.Namespace == kc.AllNamespaces {
		if req.Level > kc.LevelView {
			return detail(ErrChangeInvalid, "only the view level can be requested for all namespaces; request a specific namespace instead")
		}
		return nil
	}
	if !nsNamePattern.MatchString(req.Namespace) {
		return detail(ErrChangeInvalid, "invalid namespace")
	}
	if _, system := selfServiceSystemNamespaces[req.Namespace]; system {
		return detail(ErrChangeInvalid, "permissions on system namespaces cannot be requested")
	}
	return nil
}
