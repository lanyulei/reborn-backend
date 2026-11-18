package biz

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	cmdbv1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	oncallv1 "github.com/lanyulei/reborn/backend/core/api/oncall/v1"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/google/wire"

	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	bizoncall "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	bizticket "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 监控告警域第二、三期的跨域适配器（组合根）：monitor 域定义的窄接口（biz/monitor/deps_p2.go 与同步来源
// CISource）在这里用 ticket / workflow / cmdb / oncall / assistant / system 各域**已有的公开用例方法**实现。
// 依赖单向（monitor → 各域）：各域不感知监控域，也没有为它新增任何方法。

// MonitorP2ProviderSet 上述适配器的构造函数，由 ProviderSet 聚合。
var MonitorP2ProviderSet = wire.NewSet(
	NewMonitorTicketCatalog, NewMonitorTicketDesk, NewMonitorWorkflows, NewMonitorCMDBViewer, NewMonitorCMDBCatalog,
	NewMonitorOncallGroups, NewMonitorDutyRoster, NewMonitorShifts, NewMonitorSummarizer, NewMonitorEngineAudience,
	NewMonitorCalendarSource, NewMonitorOncallAccess, NewMonitorCISource,
)

// monitorListCap 候选项类列表的条数上限（各域的列表接口一页至多 paging.MaxPageSize 条，按页读到这个数为止）。
const monitorListCap = 500

// monitorPages 按页读取一个「页码 + 页大小」的列表接口，至多 monitorListCap 条。
func monitorPages[T any](list func(page, pageSize int64) ([]T, int64, error)) ([]T, error) {
	out := make([]T, 0, paging.MaxPageSize)
	for page := int64(1); len(out) < monitorListCap; page++ {
		items, _, err := list(page, paging.MaxPageSize)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
		if int64(len(items)) < paging.MaxPageSize {
			break
		}
	}
	return out[:min(len(out), monitorListCap)], nil
}

// ---------------------------------------------------------------------------
// 工单
// ---------------------------------------------------------------------------

type monitorTicketCatalog struct {
	catalog *bizticket.CatalogUsecase
}

// NewMonitorTicketCatalog 构造监控域的工单模板目录适配器。
func NewMonitorTicketCatalog(catalog *bizticket.CatalogUsecase) bizmonitor.TicketCatalog {
	return monitorTicketCatalog{catalog: catalog}
}

// Templates implements bizmonitor.TicketCatalog：启用的模板，按服务目录的顺序。
func (m monitorTicketCatalog) Templates(ctx context.Context) ([]bizmonitor.TicketTemplate, error) {
	categories, err := m.catalog.List(ctx, true)
	if err != nil {
		return nil, err
	}
	out := make([]bizmonitor.TicketTemplate, 0, 32)
	for _, c := range categories {
		for _, t := range c.Templates {
			if len(out) >= monitorListCap {
				return out, nil
			}
			out = append(out, bizmonitor.TicketTemplate{ID: t.ID, Name: t.Name, CategoryName: c.Name})
		}
	}
	return out, nil
}

// Template implements bizmonitor.TicketCatalog：按 userID 的身份读模板（工单域对无权读完整目录的人把已停用
// 的模板当作不存在；有权的人读得到已停用的，这里再按状态挡一次）。
func (m monitorTicketCatalog) Template(ctx context.Context, templateID, userID string) (*bizmonitor.TicketTemplate, error) {
	t, _, err := m.catalog.GetTemplate(ctx, templateID, userID)
	if errors.Is(err, bizticket.ErrTemplateNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if t.Status != bizticket.TemplateEnabled {
		return nil, nil
	}
	return &bizmonitor.TicketTemplate{ID: t.ID, Name: t.Name, CategoryName: t.CategoryName}, nil
}

type monitorTicketDesk struct {
	tickets *bizticket.Usecase
}

// NewMonitorTicketDesk 构造监控域的工单适配器。
func NewMonitorTicketDesk(tickets *bizticket.Usecase) bizmonitor.TicketDesk {
	return monitorTicketDesk{tickets: tickets}
}

func monitorTicketInfo(t *bizticket.Ticket) *bizmonitor.TicketInfo {
	return &bizmonitor.TicketInfo{ID: t.ID, Number: t.Number, Title: t.Title, Status: t.Status, Creator: t.Creator}
}

// Create implements bizmonitor.TicketDesk.
func (m monitorTicketDesk) Create(ctx context.Context, p bizmonitor.TicketCreate) (*bizmonitor.TicketInfo, error) {
	t, err := m.tickets.Create(ctx, bizticket.CreateParams{
		TemplateID: p.TemplateID, Title: p.Title, Priority: p.Priority, Data: p.Data, Creator: p.Creator, CreatorDept: p.CreatorDept,
		RequestID: p.RequestID, ResourceIDs: p.ResourceIDs,
	})
	if err != nil {
		return nil, err
	}
	return monitorTicketInfo(t), nil
}

// View implements bizmonitor.TicketDesk：工单域自己判定查看权限（提单人、流程参与者或有管理视图权限）。
func (m monitorTicketDesk) View(ctx context.Context, ticketID, userID string) (*bizmonitor.TicketInfo, error) {
	d, err := m.tickets.Get(ctx, ticketID, userID)
	if errors.Is(err, bizticket.ErrTicketNotFound) || errors.Is(err, bizticket.ErrTicketForbidden) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return monitorTicketInfo(d.Ticket), nil
}

// Comment implements bizmonitor.TicketDesk。工单评论没有幂等键：先看该用户是否已经发过一条内容相同的
// 评论，有就不再追加——监控域在「评论已发出、落账前进程退出」之后重做时不会多出一条。
func (m monitorTicketDesk) Comment(ctx context.Context, ticketID, userID, content string) error {
	existing, _, err := m.tickets.Comments(ctx, ticketID, userID)
	if err != nil {
		return err
	}
	if slices.ContainsFunc(existing, func(c *bizticket.Comment) bool { return c.UserID == userID && c.Content == content }) {
		return nil
	}
	_, _, err = m.tickets.AddComment(ctx, ticketID, userID, content, "")
	return err
}

// ---------------------------------------------------------------------------
// 流程
// ---------------------------------------------------------------------------

type monitorWorkflows struct {
	defs    *bizworkflow.DefinitionUsecase
	runtime *bizworkflow.RuntimeUsecase
}

// NewMonitorWorkflows 构造监控域的流程适配器。
func NewMonitorWorkflows(defs *bizworkflow.DefinitionUsecase, runtime *bizworkflow.RuntimeUsecase) bizmonitor.Workflows {
	return monitorWorkflows{defs: defs, runtime: runtime}
}

// startable 启用且已发布的流程定义（名称 / 标识含 keyword 的）。
func (m monitorWorkflows) startable(ctx context.Context, keyword string) ([]*bizworkflow.Definition, error) {
	enabled, published := bizworkflow.DefinitionEnabled, true
	return monitorPages(func(page, pageSize int64) ([]*bizworkflow.Definition, int64, error) {
		return m.defs.List(ctx, bizworkflow.DefinitionFilter{Keyword: keyword, Status: &enabled, Published: &published}, page, pageSize)
	})
}

// Definitions implements bizmonitor.Workflows.
func (m monitorWorkflows) Definitions(ctx context.Context) ([]bizmonitor.WorkflowDef, error) {
	defs, err := m.startable(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make([]bizmonitor.WorkflowDef, 0, len(defs))
	for _, d := range defs {
		out = append(out, bizmonitor.WorkflowDef{Key: d.Key, Name: d.Name})
	}
	slices.SortStableFunc(out, func(a, b bizmonitor.WorkflowDef) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// Definition implements bizmonitor.Workflows。流程域没有公开的「按标识取定义」：先用 KeyStartable 判定能否
// 发起，再按标识检索取名称（取不到名称时用标识代替）。
func (m monitorWorkflows) Definition(ctx context.Context, key string) (*bizmonitor.WorkflowDef, error) {
	if key == "" {
		return nil, nil
	}
	_, startable, err := m.defs.KeyStartable(ctx, key)
	if err != nil || !startable {
		return nil, err
	}
	defs, err := m.startable(ctx, key)
	if err != nil {
		return nil, err
	}
	def := &bizmonitor.WorkflowDef{Key: key, Name: key}
	for _, d := range defs {
		if d.Key == key {
			def.Name = d.Name
		}
	}
	return def, nil
}

// Start implements bizmonitor.Workflows：业务对象 = 告警事件，幂等键 = 事件 ID（同一发起人重复发起返回
// 首次的实例）。
func (m monitorWorkflows) Start(ctx context.Context, p bizmonitor.WorkflowStart) (string, error) {
	in, err := m.runtime.Start(ctx, bizworkflow.StartParams{
		DefinitionKey: p.DefinitionKey, BusinessType: mc.WorkflowBusinessType, BusinessID: p.AlertID, Title: p.Title,
		Initiator: p.Initiator, Variables: p.Variables, RequestID: p.AlertID,
	})
	if err != nil {
		return "", err
	}
	return in.ID, nil
}

// ---------------------------------------------------------------------------
// CMDB
// ---------------------------------------------------------------------------

// monitorImpactScan 展开影响面时向 CMDB 读取的拓扑节点数上限（cmdb 域自身的上限）。
const monitorImpactScan = bizcmdb.TopologyMaxNodes

type monitorCMDBViewer struct {
	scopes    *bizcmdb.ScopeUsecase
	cis       *bizcmdb.CIUsecase
	relations *bizcmdb.RelationUsecase
	topology  []bizmonitor.TopologyRelation
}

// NewMonitorCMDBViewer 构造监控域的 CMDB 资源查看适配器。
func NewMonitorCMDBViewer(scopes *bizcmdb.ScopeUsecase, cis *bizcmdb.CIUsecase, relations *bizcmdb.RelationUsecase, c *conf.Monitor) (bizmonitor.CMDBViewer, error) {
	topology, err := bizmonitor.TopologyRelations(c)
	if err != nil {
		return nil, err
	}
	return monitorCMDBViewer{scopes: scopes, cis: cis, relations: relations, topology: topology}, nil
}

// access 调用方在资源管理里的可见性判定：须持有「查询资源详情」接口权限，再按他的 CMDB 数据范围逐个判断。
// 没有接口权限时返回恒假的判定。
func (m monitorCMDBViewer) access(ctx context.Context, userID string) (func(modelCode, env string) bool, error) {
	ok, err := m.scopes.Allowed(ctx, userID, bizsystem.OperationCode(cmdbv1.OperationCmdbCIServiceGetCI))
	if err != nil {
		return nil, err
	}
	if !ok {
		return func(string, string) bool { return false }, nil
	}
	scope, err := m.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	return scope.Allows, nil
}

// Card implements bizmonitor.CMDBViewer.
func (m monitorCMDBViewer) Card(ctx context.Context, userID, ciID string) (*bizmonitor.CICard, bool, error) {
	ci, err := m.cis.GetByID(ctx, ciID)
	if kerrors.Is(err, bizcmdb.ErrCINotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	allows, err := m.access(ctx, userID)
	if err != nil {
		return nil, false, err
	}
	if !allows(ci.ModelCode, ci.Env) {
		return nil, true, nil
	}
	return &bizmonitor.CICard{
		ID: ci.ID, Name: ci.Name, ModelCode: ci.ModelCode, ModelName: ci.ModelName, Env: ci.Env, Status: ci.Status,
		Owner: ci.Owner, OwnerName: ci.OwnerName, Tags: slices.Clone(ci.Tags),
	}, true, nil
}

// Impact implements bizmonitor.CMDBViewer。先向 CMDB 读出资源周围的拓扑（不分方向、至多三层），再只沿
// 配置的拓扑关系、按「上游 → 下游」的方向在内存里展开——CMDB 的拓扑接口只能按 src → dst 一个方向过滤，
// 而拓扑关系里上游可能在任一侧（如 uplink 的上游是 dst）。
func (m monitorCMDBViewer) Impact(ctx context.Context, userID, ciID string, depth, limit int) ([]bizmonitor.CIImpact, bool, error) {
	nodes, edges, truncated, err := m.relations.Neighbors(ctx, ciID, "", bizcmdb.TopologyMaxDepth, monitorImpactScan)
	if kerrors.Is(err, bizcmdb.ErrCINotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	allows, err := m.access(ctx, userID)
	if err != nil {
		return nil, false, err
	}
	byID := make(map[string]*bizcmdb.TopologyNode, len(nodes))
	for _, n := range nodes {
		byID[n.ID] = n
	}
	down := make(map[string][]string, len(edges))
	for _, e := range edges {
		i := slices.IndexFunc(m.topology, func(r bizmonitor.TopologyRelation) bool { return r.TypeCode == e.TypeCode })
		if i < 0 {
			continue
		}
		up, lower := e.SrcID, e.DstID
		if !m.topology[i].UpstreamSrc {
			up, lower = e.DstID, e.SrcID
		}
		down[up] = append(down[up], lower)
	}
	seen := map[string]struct{}{ciID: {}}
	frontier := []string{ciID}
	out := make([]bizmonitor.CIImpact, 0, 16)
	for level := 1; level <= depth && len(frontier) > 0; level++ {
		var layer []bizmonitor.CIImpact
		for _, cur := range frontier {
			for _, id := range down[cur] {
				n := byID[id]
				if _, dup := seen[id]; dup || n == nil {
					continue
				}
				seen[id] = struct{}{}
				item := bizmonitor.CIImpact{ID: n.ID, Name: n.Name, Depth: int32(level)} // #nosec G115 -- 层数至多 3
				if allows(n.ModelCode, n.Env) {
					item.ModelCode, item.ModelName, item.CanView = n.ModelCode, n.ModelName, true
				}
				layer = append(layer, item)
			}
		}
		slices.SortStableFunc(layer, func(a, b bizmonitor.CIImpact) int { return strings.Compare(a.Name, b.Name) })
		frontier = frontier[:0]
		for _, item := range layer {
			frontier = append(frontier, item.ID)
		}
		out = append(out, layer...)
	}
	if len(out) > limit {
		return out[:limit], true, nil
	}
	return out, truncated, nil
}

type monitorCMDBCatalog struct {
	models *bizcmdb.ModelUsecase
}

// NewMonitorCMDBCatalog 构造监控域的 CMDB 模型候选项适配器。
func NewMonitorCMDBCatalog(models *bizcmdb.ModelUsecase) bizmonitor.CMDBCatalog {
	return monitorCMDBCatalog{models: models}
}

// Models implements bizmonitor.CMDBCatalog：启用的模型，按名称排序。
func (m monitorCMDBCatalog) Models(ctx context.Context) ([]bizmonitor.CIModel, error) {
	enabled := bizcmdb.ModelStatusEnabled
	models, err := monitorPages(func(page, pageSize int64) ([]*bizcmdb.Model, int64, error) {
		return m.models.ListModels(ctx, bizcmdb.ModelFilter{Status: &enabled}, page, pageSize)
	})
	if err != nil {
		return nil, err
	}
	out := make([]bizmonitor.CIModel, 0, len(models))
	for _, md := range models {
		out = append(out, bizmonitor.CIModel{Code: md.Code, Name: md.Name})
	}
	slices.SortStableFunc(out, func(a, b bizmonitor.CIModel) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// 资源索引同步读取拓扑关系时的上限：有这类关系的上游资源数、读到的关系条数。超出即报告「不完整」，
// 同步方保留上一次的结果。
const (
	monitorTopologyUpstreams = 5000
	monitorTopologyEdges     = 200000
)

type monitorCISource struct {
	cis       *bizcmdb.CIUsecase
	relations *bizcmdb.RelationUsecase
	topology  []bizmonitor.TopologyRelation
}

// NewMonitorCISource 构造监控域资源索引的同步来源。
func NewMonitorCISource(cis *bizcmdb.CIUsecase, relations *bizcmdb.RelationUsecase, c *conf.Monitor) (bizmonitor.CISource, error) {
	topology, err := bizmonitor.TopologyRelations(c)
	if err != nil {
		return nil, err
	}
	return monitorCISource{cis: cis, relations: relations, topology: topology}, nil
}

// Page implements bizmonitor.CISource：CMDB 资源列表的键集分页（更新时间倒序），不统计总数。敏感属性在
// CMDB 的读路径上已是掩码（同步方不收掩码）。
func (m monitorCISource) Page(ctx context.Context, deleted bool, cursor string, limit int) ([]bizmonitor.SourceCI, string, error) {
	items, _, err := m.cis.List(ctx, bizcmdb.CIFilter{Cursor: cursor, NoCount: true, Deleted: deleted}, 1, int64(limit))
	if err != nil {
		return nil, "", err
	}
	out := make([]bizmonitor.SourceCI, 0, len(items))
	for _, ci := range items {
		out = append(out, bizmonitor.SourceCI{
			ID: ci.ID, ModelCode: ci.ModelCode, Name: ci.Name, Env: ci.Env, Status: ci.Status, Owner: ci.Owner,
			Tags: ci.Tags, Attrs: ci.Attrs, UpdateTime: ci.UpdateTime,
		})
	}
	next := ""
	if len(items) > 0 && len(items) >= min(limit, int(paging.MaxPageSize)) {
		next = bizcmdb.EncodeCursor(items[len(items)-1])
	}
	return out, next, nil
}

// Upstreams implements bizmonitor.CISource。CMDB 没有「按关系类型列出全部关系」的公开方法：对每种拓扑关系，
// 先列出处在上游一侧的资源（资源列表的关联条件），再逐个读它的对端（下游）。调用次数 = 上游资源数 ×
// 每个上游的下游页数——上游（宿主机、交换机）通常远少于下游。
func (m monitorCISource) Upstreams(ctx context.Context) (map[string][]string, bool, error) {
	direct := map[string][]string{}
	upstreams, edges := 0, 0
	for _, rel := range m.topology {
		// 上游在 src 一侧：上游资源是关系的发出方（outgoing），它的对端即下游。
		direction := bizcmdb.DirectionIncoming
		if rel.UpstreamSrc {
			direction = bizcmdb.DirectionOutgoing
		}
		filter := bizcmdb.CIFilter{RelType: rel.TypeCode, RelDirection: direction, NoCount: true}
		for {
			ups, _, err := m.cis.List(ctx, filter, 1, paging.MaxPageSize)
			if err != nil {
				return nil, false, err
			}
			for _, up := range ups {
				if upstreams++; upstreams > monitorTopologyUpstreams {
					return direct, false, nil
				}
				for page := int64(1); ; page++ {
					related, _, err := m.relations.ListRelated(ctx, up.ID, rel.TypeCode, direction, "", page, paging.MaxPageSize)
					if kerrors.Is(err, bizcmdb.ErrCINotFound) || kerrors.Is(err, bizcmdb.ErrRelationTypeNotFound) {
						break // 读的过程中资源或关系类型被删除
					}
					if err != nil {
						return nil, false, err
					}
					for _, r := range related {
						if edges++; edges > monitorTopologyEdges {
							return direct, false, nil
						}
						if !slices.Contains(direct[r.CI.ID], up.ID) {
							direct[r.CI.ID] = append(direct[r.CI.ID], up.ID)
						}
					}
					if int64(len(related)) < paging.MaxPageSize {
						break
					}
				}
			}
			if int64(len(ups)) < paging.MaxPageSize {
				break
			}
			filter.Cursor = bizcmdb.EncodeCursor(ups[len(ups)-1])
		}
	}
	return direct, true, nil
}

// ---------------------------------------------------------------------------
// 值班
// ---------------------------------------------------------------------------

// monitorOncallActor 以系统身份读值班域的只读列表（分组名称、成员）：这些是监控域自己的候选项 / 通知对象
// 解析，对外只披露最小信息，访问控制在监控域的接口上。
var monitorOncallActor = bizoncall.Actor{IsAdmin: true}

type monitorOncallGroups struct {
	groups *bizoncall.GroupUsecase
}

// NewMonitorOncallGroups 构造监控域的值班分组适配器。
func NewMonitorOncallGroups(groups *bizoncall.GroupUsecase) bizmonitor.OncallGroups {
	return monitorOncallGroups{groups: groups}
}

// Groups implements bizmonitor.OncallGroups.
func (m monitorOncallGroups) Groups(ctx context.Context) ([]bizmonitor.OncallGroup, error) {
	groups, err := monitorPages(func(page, pageSize int64) ([]*bizoncall.Group, int64, error) {
		return m.groups.List(ctx, monitorOncallActor, "", false, page, pageSize)
	})
	if err != nil {
		return nil, err
	}
	out := make([]bizmonitor.OncallGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, bizmonitor.OncallGroup{ID: g.ID, Name: g.Name})
	}
	slices.SortStableFunc(out, func(a, b bizmonitor.OncallGroup) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// AdminIDs implements bizmonitor.OncallGroups.
func (m monitorOncallGroups) AdminIDs(ctx context.Context, groupID string) ([]string, error) {
	members, err := m.groups.ListMembers(ctx, monitorOncallActor, groupID)
	if kerrors.Is(err, bizoncall.ErrGroupNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, mb := range members {
		if mb.Role == bizoncall.MemberRoleAdmin && mb.Available {
			out = append(out, mb.UserID)
		}
	}
	return out, nil
}

type monitorDutyRoster struct {
	schedules *bizoncall.ScheduleUsecase
}

// NewMonitorDutyRoster 构造监控域的主 / 备值班人适配器。
func NewMonitorDutyRoster(schedules *bizoncall.ScheduleUsecase) bizmonitor.DutyRoster {
	return monitorDutyRoster{schedules: schedules}
}

// OnDutyByRole implements bizmonitor.DutyRoster：值班域的「此刻值班人」只含启用的值班表。
func (m monitorDutyRoster) OnDutyByRole(ctx context.Context, scheduleID string) (primary, backup []string, err error) {
	duties, err := m.schedules.Current(ctx, "", scheduleID)
	if err != nil {
		return nil, nil, err
	}
	for _, d := range duties {
		if d.Primary != nil && d.Primary.UserID != "" {
			primary = append(primary, d.Primary.UserID)
		}
		if d.Backup != nil && d.Backup.UserID != "" {
			backup = append(backup, d.Backup.UserID)
		}
	}
	return primary, backup, nil
}

type monitorShifts struct {
	schedules *bizoncall.ScheduleUsecase
}

// NewMonitorShifts 构造监控域的班次适配器。
func NewMonitorShifts(schedules *bizoncall.ScheduleUsecase) bizmonitor.Shifts {
	return monitorShifts{schedules: schedules}
}

// Shifts implements bizmonitor.Shifts.
func (m monitorShifts) Shifts(ctx context.Context, scheduleID string, from, to time.Time, limit int) ([]bizmonitor.ShiftSlot, bool, error) {
	if _, err := m.schedules.Get(ctx, scheduleID); err != nil {
		if kerrors.Is(err, bizoncall.ErrScheduleNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	shifts, _, err := m.schedules.ListShifts(ctx, bizoncall.ShiftQuery{From: from, To: to, ScheduleID: scheduleID})
	if err != nil {
		return nil, false, err
	}
	out := make([]bizmonitor.ShiftSlot, 0, min(len(shifts), limit))
	for _, sh := range shifts[:min(len(shifts), limit)] {
		out = append(out, bizmonitor.ShiftSlot{UserID: sh.UserID, UserName: sh.UserName, Role: sh.Role, Start: sh.Start, End: sh.End})
	}
	return out, true, nil
}

type monitorOncallAccess struct {
	authz *bizsystem.AuthzUsecase
}

// NewMonitorOncallAccess 构造监控域对值班日历的接口权限判定器。
func NewMonitorOncallAccess(authz *bizsystem.AuthzUsecase) bizmonitor.OncallAccess {
	return monitorOncallAccess{authz: authz}
}

// CanViewOncallCalendar implements bizmonitor.OncallAccess：持有「查询值班班次」接口权限。
func (m monitorOncallAccess) CanViewOncallCalendar(ctx context.Context, userID string) (bool, error) {
	return m.authz.Allowed(ctx, userID, bizsystem.OperationCode(oncallv1.OperationOncallScheduleServiceListShifts))
}

// ---------------------------------------------------------------------------
// 智能助手、组织目录、工作日历
// ---------------------------------------------------------------------------

// monitorSummaryBytes 摘要输出的读取上限：读到这么多就不再等模型继续生成（监控域落库时还会再截断）。
const monitorSummaryBytes = 4 * mc.MaxAISummaryBytes

// errMonitorSummaryFull 输出已达读取上限，中止流式读取（内部控制流）。
var errMonitorSummaryFull = errors.New("monitor: summary output limit reached")

type monitorSummarizer struct {
	agent *bizassistant.AgentUsecase
	model bizassistant.ChatModel
}

// NewMonitorSummarizer 构造监控域的文本摘要适配器：直接调助手域的模型客户端，不带任何工具、不落会话。
func NewMonitorSummarizer(agent *bizassistant.AgentUsecase, model bizassistant.ChatModel) bizmonitor.Summarizer {
	return monitorSummarizer{agent: agent, model: model}
}

// Available implements bizmonitor.Summarizer.
func (m monitorSummarizer) Available() bool { return m.agent.Enabled() }

// Summarize implements bizmonitor.Summarizer.
func (m monitorSummarizer) Summarize(ctx context.Context, system, prompt string) (string, error) {
	var b strings.Builder
	out, err := m.model.Stream(ctx, bizassistant.ChatRequest{
		System: system, Messages: []*bizassistant.Message{{Role: bizassistant.RoleUser, Content: prompt}},
	}, func(d bizassistant.Delta) error {
		if b.WriteString(d.Text); b.Len() >= monitorSummaryBytes {
			return errMonitorSummaryFull
		}
		return nil
	})
	if errors.Is(err, errMonitorSummaryFull) {
		return b.String(), nil
	}
	if err != nil {
		return "", err
	}
	if out != nil && out.Content != "" {
		return out.Content, nil
	}
	return b.String(), nil
}

type monitorEngineAudience struct {
	dir   *bizsystem.DirectoryUsecase
	roles []string
}

// NewMonitorEngineAudience 构造平台自监控通知的收件人解析器：部署配置 monitor.engine_notify_roles 里各角色的
// 可用成员；没有配置时不指定收件人。
func NewMonitorEngineAudience(dir *bizsystem.DirectoryUsecase, c *conf.Monitor) bizmonitor.EngineAudience {
	var roles []string
	for _, code := range c.GetEngineNotifyRoles() {
		if code = strings.TrimSpace(code); code != "" && !slices.Contains(roles, code) {
			roles = append(roles, code)
		}
	}
	return monitorEngineAudience{dir: dir, roles: roles}
}

// UserIDs implements bizmonitor.EngineAudience.
func (m monitorEngineAudience) UserIDs(ctx context.Context) ([]string, error) {
	if len(m.roles) == 0 {
		return nil, nil
	}
	return m.dir.UserIDsByRoles(ctx, m.roles)
}

type monitorCalendarSource struct {
	calendar *bizworkflow.CalendarUsecase
}

// NewMonitorCalendarSource 构造监控域的工作日历来源。
func NewMonitorCalendarSource(calendar *bizworkflow.CalendarUsecase) bizmonitor.CalendarSource {
	return monitorCalendarSource{calendar: calendar}
}

// WorkCalendar implements bizmonitor.CalendarSource：流程域的日历配置逐字段搬到契约结构（两边的 json 键
// 相同）。配置通不过流程域自己的校验（尚未配置）时 ok 为 false。
func (m monitorCalendarSource) WorkCalendar(ctx context.Context) (mc.CalendarSnapshot, bool, error) {
	c, err := m.calendar.Get(ctx)
	if err != nil {
		return mc.CalendarSnapshot{}, false, err
	}
	if _, err := bizworkflow.NewCalendar(c); err != nil {
		return mc.CalendarSnapshot{}, false, nil
	}
	// 空列表写成 []（不是 null）：快照的内容只在日历真的变了才不同。
	snap := mc.CalendarSnapshot{
		Timezone: c.Timezone, Weekdays: append([]int32{}, c.Weekdays...), Holidays: append([]string{}, c.Holidays...),
		Workdays: append([]string{}, c.Workdays...), Segments: make([]mc.CalendarSegment, 0, len(c.Segments)),
	}
	for _, seg := range c.Segments {
		snap.Segments = append(snap.Segments, mc.CalendarSegment{Start: seg.Start, End: seg.End})
	}
	return snap, true, nil
}
