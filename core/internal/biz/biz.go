// Package biz 承载业务核心：领域模型、仓储接口（依赖倒置）与用例。
// 本包禁止 import 传输层（transport/http、transport/grpc）与任何存储驱动。
//
// 分域结构：core 为单服务多域（模块化单体），各业务域在 internal/biz/<domain>/
// 子包实现（子包包名保持 biz，引用前缀不变）；本包只保留跨域聚合的
// ProviderSet，不放任何业务代码。新增域（如 workflow）时：建子包、
// 子包内定义自己的 ProviderSet，并在下方聚合处追加一行。
package biz

import (
	"context"
	"time"

	cmdbv1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"

	ticketv1 "github.com/lanyulei/reborn/backend/core/api/ticket/v1"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/google/wire"

	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizoncall "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	bizticket "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
)

// ProviderSet 聚合各业务域的 biz providers；跨域窄接口在聚合处绑定
// （组合根性质：notify 不横向 import system，绑定只出现在此）。
var ProviderSet = wire.NewSet(
	bizsystem.ProviderSet,
	biznotify.ProviderSet,
	bizassistant.ProviderSet,
	bizcmdb.ProviderSet,
	bizoncall.ProviderSet,
	bizfile.ProviderSet,
	NewFileAccessCheckers,
	bizworkflow.ProviderSet,
	NewWorkflowListeners,
	NewWorkflowViewers,
	NewWorkflowDefinitionReferrers,
	// 流程引擎解析处理人 / 用户信息：由 system 域的组织目录实现（单向依赖 workflow → system）。
	wire.Bind(new(bizworkflow.Directory), new(*bizsystem.DirectoryUsecase)),
	NewWorkflowUsers,
	// 工单域：流程 / 附件 / 组织目录 / 管理视图判定由其他域实现（单向依赖 ticket → workflow / file / system）。
	bizticket.ProviderSet,
	wire.Bind(new(bizticket.Workflow), new(*bizworkflow.RuntimeUsecase)),
	wire.Bind(new(bizticket.WorkflowCatalog), new(*bizworkflow.DefinitionUsecase)),
	wire.Bind(new(bizticket.Attachments), new(*bizfile.Usecase)),
	wire.Bind(new(bizticket.Directory), new(*bizsystem.DirectoryUsecase)),
	NewTicketSupervisor,
	NewTicketCatalogManager,
	NewTicketDeadlines,
	NewTicketResources,
	wire.Bind(new(bizticket.WorkflowForms), new(*bizworkflow.RuntimeUsecase)),
	// 智能助手工具按用户授权过滤：有效操作集由 system 域 AuthzUsecase 解析；
	// 操作者展示信息由 system 域 UserUsecase 提供（单向依赖 assistant → system）。
	wire.Bind(new(bizassistant.PermissionSource), new(*bizsystem.AuthzUsecase)),
	wire.Bind(new(bizassistant.ActorDescriber), new(*bizsystem.UserUsecase)),
	// CMDB 数据范围：启用角色由 system 域 AuthzUsecase 解析，角色目录由 RoleUsecase 提供。
	wire.Bind(new(bizcmdb.RoleSource), new(*bizsystem.AuthzUsecase)),
	wire.Bind(new(bizcmdb.RoleDirectory), new(*bizsystem.RoleUsecase)),
	// 订阅代管校验：notify 定义的 AdminResolver 由 system 用户用例实现。
	wire.Bind(new(biznotify.SubscriptionAdminResolver), new(*bizsystem.UserUsecase)),
	// notify 控制面按 biz 隔离：角色 biz_scope 并集由 system 域 AuthzUsecase 解析。
	wire.Bind(new(biznotify.BizScopeResolver), new(*bizsystem.AuthzUsecase)),
	// 业务事件同事务直插（设计 §11）：system 消费 notify 的
	// NotificationWriter——实现固定为 NotificationUsecase（校验/补全收敛
	// 在 notify，system 仅提供业务语义字段；单向依赖 system → notify）。
	wire.Bind(new(biznotify.NotificationWriter), new(*biznotify.NotificationUsecase)),
)

// NewFileAccessCheckers 装配附件访问检查器（业务类型 → 检查器）：持有附件的业务域
// 在此登记自己的检查器；未登记的业务类型其附件一律不可读（deny-by-default）。
func NewFileAccessCheckers(tickets *bizticket.Access) bizfile.AccessCheckers {
	return bizfile.AccessCheckers{bizticket.BusinessType: tickets}
}

// NewWorkflowListeners 装配流程事件监听器（业务类型 → 监听器）：基于流程引擎的业务域
// 在此登记，实例结束时在引擎的同一事务内收到回调。
func NewWorkflowListeners(tickets *bizticket.StatusSyncer) bizworkflow.Listeners {
	return bizworkflow.Listeners{bizticket.BusinessType: tickets}
}

// NewWorkflowViewers 装配流程实例的业务侧查看权限（业务类型 → 判定）。
func NewWorkflowViewers(tickets *bizticket.Access) bizworkflow.Viewers {
	return bizworkflow.Viewers{bizticket.BusinessType: tickets}
}

// NewWorkflowDefinitionReferrers 装配「谁还在引用某个流程定义」的业务域：流程被停用 / 删除
// 之前引擎逐个询问，仍被引用即拒绝。基于流程引擎的业务域在此登记自己的实现——须是不依赖
// 流程用例的类型（如只读仓储），否则与 DefinitionUsecase 互相依赖成环。
func NewWorkflowDefinitionReferrers(tickets bizticket.WorkflowReferences) bizworkflow.DefinitionReferrers {
	return bizworkflow.DefinitionReferrers{tickets}
}

// ticketSupervisor 以接口授权判定「能否查看全部工单」：持有「查询全部工单」接口权限
// （超管恒为 true）。操作码与授权中间件同一口径。
type ticketSupervisor struct {
	authz *bizsystem.AuthzUsecase
}

// NewTicketSupervisor 构造工单域的管理视图判定器。
func NewTicketSupervisor(authz *bizsystem.AuthzUsecase) bizticket.Supervisor {
	return ticketSupervisor{authz: authz}
}

// CanViewAll implements bizticket.Supervisor.
func (s ticketSupervisor) CanViewAll(ctx context.Context, userID string) (bool, error) {
	return s.authz.Allowed(ctx, userID, bizsystem.OperationCode(ticketv1.OperationTicketServiceListTickets))
}

// NewTicketCatalogManager 构造工单域的完整目录查看判定器：持有「查询完整服务目录」接口权限
// （超管恒为 true）者可读已停用的模板。
func NewTicketCatalogManager(authz *bizsystem.AuthzUsecase) bizticket.CatalogManager {
	return ticketSupervisor{authz: authz}
}

// CanManageCatalog implements bizticket.CatalogManager.
func (s ticketSupervisor) CanManageCatalog(ctx context.Context, userID string) (bool, error) {
	return s.authz.Allowed(ctx, userID, bizsystem.OperationCode(ticketv1.OperationTicketCatalogServiceListManagedCatalog))
}

// ticketDeadlines 把流程引擎的工作日历适配为工单的时限推算（SLA 与流程时限用同一份日历）。
type ticketDeadlines struct {
	calendar *bizworkflow.CalendarUsecase
}

// NewTicketDeadlines 构造工单域的时限推算器。
func NewTicketDeadlines(calendar *bizworkflow.CalendarUsecase) bizticket.Deadlines {
	return ticketDeadlines{calendar: calendar}
}

// Due implements bizticket.Deadlines.
func (d ticketDeadlines) Due(ctx context.Context, from time.Time, dur bizticket.Duration) (time.Time, error) {
	return d.calendar.Due(ctx, from, bizworkflow.Duration{Value: int(dur.Value), Unit: dur.Unit, WorkTime: dur.WorkTime})
}

// ticketResources 把 CMDB 的资源查询适配为工单的资源解析：只返回用户有权查看的资源
// （持有资源列表接口权限，且资源在他的数据范围内）。
type ticketResources struct {
	cis    *bizcmdb.CIUsecase
	scopes *bizcmdb.ScopeUsecase
	authz  *bizsystem.AuthzUsecase
}

// NewTicketResources 构造工单域的资源解析器。
func NewTicketResources(cis *bizcmdb.CIUsecase, scopes *bizcmdb.ScopeUsecase, authz *bizsystem.AuthzUsecase) bizticket.Resources {
	return ticketResources{cis: cis, scopes: scopes, authz: authz}
}

// Resolve implements bizticket.Resources.
func (r ticketResources) Resolve(ctx context.Context, userID string, ids []string) ([]bizticket.Resource, error) {
	ok, err := r.authz.Allowed(ctx, userID, bizsystem.OperationCode(cmdbv1.OperationCmdbCIServiceListCIs))
	if err != nil || !ok {
		return nil, err
	}
	scope, err := r.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := make([]bizticket.Resource, 0, len(ids))
	for _, id := range ids {
		ci, err := r.cis.GetByID(ctx, id)
		if errors.Is(err, bizcmdb.ErrCINotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if scope.Allows(ci.ModelCode, ci.Env) {
			out = append(out, bizticket.Resource{ID: ci.ID, Name: ci.Name, ModelName: ci.ModelName})
		}
	}
	return out, nil
}

// workflowUsers 把 system 域的组织目录适配为流程引擎的用户目录（两域各自的展示
// 模型不互相引用，转换只出现在组合根）。
type workflowUsers struct {
	dir *bizsystem.DirectoryUsecase
}

// NewWorkflowUsers 构造流程引擎的用户目录适配器。
func NewWorkflowUsers(dir *bizsystem.DirectoryUsecase) bizworkflow.Users {
	return workflowUsers{dir: dir}
}

// UserNames implements bizworkflow.Users.
func (w workflowUsers) UserNames(ctx context.Context, ids []string) (map[string]string, error) {
	return w.dir.UserNames(ctx, ids)
}

// Candidates implements bizworkflow.Users.
func (w workflowUsers) Candidates(ctx context.Context, keyword string, limit int64) ([]bizworkflow.UserInfo, error) {
	users, err := w.dir.CandidateUsers(ctx, keyword, limit)
	if err != nil {
		return nil, err
	}
	out := make([]bizworkflow.UserInfo, 0, len(users))
	for _, u := range users {
		out = append(out, bizworkflow.UserInfo{ID: u.ID, Username: u.Username, Nickname: u.Nickname})
	}
	return out, nil
}

// PrimaryDept implements bizworkflow.Users.
func (w workflowUsers) PrimaryDept(ctx context.Context, userID string) (string, []string, error) {
	return w.dir.PrimaryDept(ctx, userID)
}
