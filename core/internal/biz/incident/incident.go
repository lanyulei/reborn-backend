package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// Deps 故障域全部用例共用的依赖（wire.Struct 装配）。仓储由 data 层实现；跨域窄接口由组合根适配。
type Deps struct {
	Txm         TxManager
	Incidents   IncidentReader
	Writer      IncidentWriter
	Members     MemberRepo
	Events      EventRepo
	Links       AlertLinkRepo
	Changes     ChangeRefRepo
	Candidates  CandidateRepo
	Rules       RuleRepo
	Businesses  BusinessRepo
	Playbooks   PlaybookRepo
	Proposals   ProposalRepo
	Updates     UpdateRepo
	Shares      ShareRepo
	Postmortems PostmortemRepo
	Items       ActionItemRepo
	Sources     SourceRepo
	Webhooks    WebhookRepo
	Outbox      OutboxRepo
	Actions     ActionRepo
	Cursor      CursorRepo
	Stats       StatsRepo
	Notify      EventWriter
	Dir         Directory
	Leaders     DeptLeaders
	Admins      Admins
	Perms       Permissions
	Duty        Duty
	Groups      OncallGroups
	Schedules   Schedules
	Alerts      Alerts
	Matchers    MatcherCompiler
	CMDB        CMDB
	Sources2    ChangeSources
	Tickets     Tickets
	Flows       Workflows
	Jobs        Jobs
	AI          Summarizer
	Files       Files
	Workdays    Workdays
	Cipher      TokenCipher
	Sender      WebhookSender
	Site        SiteURL
	Accounts    Accounts
	Agent       AgentDispatcher
	Logger      *slog.Logger
}

// ChangeSources 各域变更来源（组合根装配）。
type ChangeSources []ChangeSource

// 上限。
const (
	maxAlertsPerIncident = 5000
	maxMembers           = 200
	maxCommentBytes      = 8000
	agentLeaseDefault    = 30 * time.Minute
	agentLeaseMax        = 2 * time.Hour
	timelineMaxPageSize  = 200
	webhookCacheTTL      = 30 * time.Second
)

// IncidentUsecase 故障的生命周期、角色、时间线与关联。
type IncidentUsecase struct {
	d        *Deps
	settings *SettingsUsecase
	hooks    *webhookCache
	now      func() time.Time
}

// NewIncidentUsecase new an IncidentUsecase.
func NewIncidentUsecase(d *Deps, settings *SettingsUsecase) *IncidentUsecase {
	return &IncidentUsecase{d: d, settings: settings, hooks: newWebhookCache(d.Webhooks), now: time.Now}
}

// ---------------------------------------------------------------------------
// 权限与视图
// ---------------------------------------------------------------------------

// access 当前用户对一条故障的角色。
type access struct {
	admin     bool
	commander bool
	responder bool
	watcher   bool
}

func (a access) canManage() bool { return a.admin || a.commander }
func (a access) canHandle() bool { return a.canManage() || a.responder }

func (uc *IncidentUsecase) accessOf(ctx context.Context, userID string, inc *Incident, members []*Member) (access, error) {
	admin, err := uc.d.Admins.IsAdmin(ctx, userID)
	if err != nil {
		return access{}, err
	}
	a := access{admin: admin, commander: inc.CommanderID != "" && inc.CommanderID == userID}
	for _, m := range members {
		if m.UserID != userID {
			continue
		}
		switch m.Role {
		case RoleResponder:
			a.responder = true
		case RoleWatcher:
			a.watcher = true
		}
	}
	return a, nil
}

// View 故障及其展示信息。
type View struct {
	*Incident
	BusinessName     string
	CommanderName    string
	RuleName         string
	SourceRefName    string
	MergedIntoNumber string
	Responders       []Named
	Watchers         []Named
	AffectedCIs      []Named
	ActiveAlerts     int32
	CanManage        bool
	CanHandle        bool
}

// nameSet 批量名称解析（用户 / 业务 / 资源 / 规则 / 故障编号），一次视图组装只各查一次。
type nameSet struct {
	users, businesses, cis, rules, incidents []string
	names                                    map[string]string
}

func (n *nameSet) user(ids ...string)     { n.users = append(n.users, ids...) }
func (n *nameSet) business(ids ...string) { n.businesses = append(n.businesses, ids...) }
func (n *nameSet) ci(ids ...string)       { n.cis = append(n.cis, ids...) }
func (n *nameSet) rule(ids ...string)     { n.rules = append(n.rules, ids...) }
func (n *nameSet) incident(ids ...string) { n.incidents = append(n.incidents, ids...) }
func (n *nameSet) get(kind, id string) string {
	if id == "" {
		return ""
	}
	return n.names[kind+":"+id]
}

// Named 的便捷构造。
func (n *nameSet) named(kind string, ids []string) []Named {
	out := make([]Named, 0, len(ids))
	for _, id := range ids {
		out = append(out, Named{ID: id, Name: n.get(kind, id)})
	}
	return out
}

// resolve 把收集到的 ID 解析成名称。
func (uc *IncidentUsecase) resolve(ctx context.Context, n *nameSet) error {
	n.names = map[string]string{}
	if ids := uniq(n.users); len(ids) > 0 {
		names, err := uc.d.Dir.UserNames(ctx, ids)
		if err != nil {
			return err
		}
		for id, name := range names {
			n.names["user:"+id] = name
		}
	}
	if ids := uniq(n.businesses); len(ids) > 0 {
		items, err := uc.d.Businesses.ListByIDs(ctx, ids)
		if err != nil {
			return err
		}
		for _, b := range items {
			n.names["business:"+b.ID] = b.Name
		}
	}
	if ids := uniq(n.cis); len(ids) > 0 {
		cards, err := uc.d.CMDB.Cards(ctx, ids)
		if err != nil {
			return err
		}
		for _, c := range cards {
			n.names["ci:"+c.ID] = c.Name
		}
	}
	if ids := uniq(n.rules); len(ids) > 0 {
		rules, err := uc.d.Rules.ListByIDs(ctx, ids)
		if err != nil {
			return err
		}
		for _, r := range rules {
			n.names["rule:"+r.ID] = r.Spec.Name
		}
	}
	if ids := uniq(n.incidents); len(ids) > 0 {
		items, err := uc.d.Incidents.ListByIDs(ctx, ids)
		if err != nil {
			return err
		}
		for _, it := range items {
			n.names["incident:"+it.ID] = it.Number
		}
	}
	return nil
}

// views 组装多条故障的视图（含成员、名称、当前用户的角色）。
func (uc *IncidentUsecase) views(ctx context.Context, userID string, items []*Incident, withActive bool) ([]*View, error) {
	if len(items) == 0 {
		return []*View{}, nil
	}
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.ID)
	}
	members, err := uc.d.Members.ListMany(ctx, ids)
	if err != nil {
		return nil, err
	}
	admin, err := uc.d.Admins.IsAdmin(ctx, userID)
	if err != nil {
		return nil, err
	}
	ns := &nameSet{}
	for _, it := range items {
		ns.user(it.CommanderID)
		ns.business(it.BusinessID)
		ns.ci(it.AffectedCIIDs...)
		ns.rule(it.RuleID)
		ns.incident(it.MergedInto)
		for _, m := range members[it.ID] {
			ns.user(m.UserID)
		}
	}
	if err := uc.resolve(ctx, ns); err != nil {
		return nil, err
	}
	out := make([]*View, 0, len(items))
	for _, it := range items {
		v := &View{
			Incident: it, BusinessName: ns.get("business", it.BusinessID), CommanderName: ns.get("user", it.CommanderID),
			RuleName: ns.get("rule", it.RuleID), MergedIntoNumber: ns.get("incident", it.MergedInto),
			AffectedCIs: ns.named("ci", it.AffectedCIIDs), CanManage: admin || (it.CommanderID != "" && it.CommanderID == userID),
		}
		if it.Source == SourceRule {
			v.SourceRefName = ns.get("rule", it.SourceRef)
		}
		for _, m := range members[it.ID] {
			named := Named{ID: m.UserID, Name: ns.get("user", m.UserID)}
			switch m.Role {
			case RoleResponder:
				v.Responders = append(v.Responders, named)
				if m.UserID == userID {
					v.CanHandle = true
				}
			case RoleWatcher:
				v.Watchers = append(v.Watchers, named)
			}
		}
		v.CanHandle = v.CanHandle || v.CanManage
		out = append(out, v)
	}
	if withActive {
		for _, v := range out {
			n, err := uc.activeAlerts(ctx, v.ID)
			if err != nil {
				return nil, err
			}
			v.ActiveAlerts = n
		}
	}
	return out, nil
}

// activeAlerts 关联告警中仍在告警中的数量（经监控域读当前状态；关联数很大时只看前 500 条）。
func (uc *IncidentUsecase) activeAlerts(ctx context.Context, incidentID string) (int32, error) {
	links, err := uc.d.Links.ListAll(ctx, incidentID, 500)
	if err != nil || len(links) == 0 {
		return 0, err
	}
	ids := make([]string, 0, len(links))
	for _, l := range links {
		ids = append(ids, l.AlertID)
	}
	n, err := uc.d.Alerts.CountFiring(ctx, ids)
	if err != nil {
		return 0, err
	}
	return i32(n), nil
}

// Get 故障详情。
func (uc *IncidentUsecase) Get(ctx context.Context, userID, id string) (*View, error) {
	inc, err := uc.d.Incidents.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, userID, []*Incident{inc}, true)
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// List 分页查询。
func (uc *IncidentUsecase) List(ctx context.Context, userID string, filter IncidentFilter, page, pageSize int64) ([]*View, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	if filter.OpenStart != nil && filter.OpenEnd != nil && !filter.OpenStart.Before(*filter.OpenEnd) {
		return nil, 0, ErrTimeRangeInvalid
	}
	filter.Keyword = cleanText(filter.Keyword)
	items, total, err := uc.d.Incidents.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, userID, items, false)
	return views, total, err
}

// ---------------------------------------------------------------------------
// 写路径：事务内 行锁 → 修改 → 写回 + 时间线 + 通知 + 出站箱
// ---------------------------------------------------------------------------

// mutation 一次写事务里累积的副作用。
type mutation struct {
	uc      *IncidentUsecase
	tx      Tx
	inc     *Incident
	members []*Member
	now     time.Time
	// actor 操作者（系统动作为空）。
	actorType int32
	actorID   string
	events    []*Event
	notices   []notice
	hooks     []hookEvent
	actions   []*Action
	names     map[string]string
	business  string
	alerts    int
	saved     bool
	// agentRef 本次写入要交给运维 Agent 域入队的任务引用（nil = 不入队）。
	agentRef *AgentTaskRef
}

// hookEvent 待入出站箱的领域事件。
type hookEvent struct {
	eventType string
	detail    any
}

func (m *mutation) event(typ string, detail EventDetail) *Event {
	ev := &Event{IncidentID: m.inc.ID, Type: typ, ActorType: m.actorType, ActorID: m.actorID, Detail: detail}
	if m.actorType == ActorSystem {
		ev.ActorID = ""
	}
	m.events = append(m.events, ev)
	return ev
}

func (m *mutation) notify(n notice) { m.notices = append(m.notices, n) }

func (m *mutation) hook(eventType string, detail any) {
	m.hooks = append(m.hooks, hookEvent{eventType: eventType, detail: detail})
}

// agent 标记本次写入要为该故障入队一次运维 Agent 诊断任务（提交时经 Deps.Agent 同事务写入；未装配 Agent 域即忽略）。
func (m *mutation) agent(reason string) {
	inc := m.inc
	m.agentRef = &AgentTaskRef{IncidentID: inc.ID, Number: inc.Number, Title: inc.Title, Severity: inc.Severity, BusinessID: inc.BusinessID, RuleID: inc.RuleID, Reason: reason, Kind: "diagnose"}
}

// agentPostmortem 标记本次写入要入队一次「经验沉淀」任务（复盘完成，每个版本一次）；是否有可用的 Profile 由 Agent 域决定。
func (m *mutation) agentPostmortem(version int32) {
	inc := m.inc
	m.agentRef = &AgentTaskRef{IncidentID: inc.ID, Number: inc.Number, Title: inc.Title, Severity: inc.Severity, BusinessID: inc.BusinessID, RuleID: inc.RuleID, Reason: "postmortem", Kind: "postmortem", Version: version}
}

func (m *mutation) action(kind int32, refID string, payload any) {
	raw, _ := json.Marshal(payload)
	if payload == nil {
		raw = []byte("{}")
	}
	m.actions = append(m.actions, &Action{IncidentID: m.inc.ID, Kind: kind, RefID: refID, Payload: raw})
}

// touch 记一次人工活动（公告 / 备注），无进展提醒据此判定。
func (m *mutation) touch() { m.inc.LastActivity = m.now }

// commit 写回全部副作用。
func (m *mutation) commit(ctx context.Context) error {
	m.inc.UpdateUser = m.actorID
	if !m.saved {
		if err := m.uc.d.Writer.SaveTx(ctx, m.tx, m.inc); err != nil {
			return err
		}
	}
	for _, ev := range m.events {
		id, err := idgen.NewID()
		if err != nil {
			return err
		}
		ev.ID = id
		if err := m.uc.d.Events.AppendTx(ctx, m.tx, ev); err != nil {
			return err
		}
	}
	if len(m.notices) > 0 {
		if m.names == nil {
			ns := &nameSet{}
			ns.user(m.inc.CommanderID, m.actorID)
			ns.ci(m.inc.AffectedCIIDs...)
			ns.business(m.inc.BusinessID)
			for _, n := range m.notices {
				ns.user(n.users...)
			}
			if err := m.uc.resolve(ctx, ns); err != nil {
				return err
			}
			m.names = ns.names
			m.business = ns.get("business", m.inc.BusinessID)
		}
		if m.alerts == 0 {
			n, err := m.uc.d.Links.Count(ctx, m.inc.ID)
			if err != nil {
				return err
			}
			m.alerts = n
		}
		for _, n := range m.notices {
			payload := m.uc.basePayload(m.inc, m.names, m.business, m.alerts)
			payload["actor_name"] = m.names["user:"+m.actorID]
			switch m.actorType {
			case ActorSystem:
				payload["actor_name"] = "系统"
			case ActorAgent:
				payload["actor_name"] = "Agent · " + m.names["user:"+m.actorID]
			}
			if err := m.uc.emitTx(ctx, m.tx, m.inc, n, payload); err != nil {
				return err
			}
		}
	}
	if len(m.hooks) > 0 {
		if err := m.uc.enqueueHooks(ctx, m.tx, m.inc, m.hooks); err != nil {
			return err
		}
	}
	for _, a := range m.actions {
		id, err := idgen.NewID()
		if err != nil {
			return err
		}
		a.ID = id
		if err := m.uc.d.Actions.EnqueueTx(ctx, m.tx, a); err != nil {
			return err
		}
	}
	if m.agentRef != nil && m.uc.d.Agent != nil && !m.inc.IsDrill {
		if err := m.uc.d.Agent.Enqueue(ctx, m.tx, *m.agentRef); err != nil {
			return err
		}
	}
	return nil
}

// mutate 对一条故障执行一次写事务：行锁读取 → fn → 写回副作用。fn 返回错误即整体回滚。
func (uc *IncidentUsecase) mutate(ctx context.Context, id string, actorType int32, actorID string, fn func(m *mutation) error) (*Incident, error) {
	var out *Incident
	err := uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		inc, err := uc.d.Writer.LockTx(ctx, tx, id)
		if err != nil {
			return err
		}
		// 成员在本事务的连接上读（MemberRepo.List 认 ctx 里的事务句柄）：每次写都要读它，走连接池的话每个
		// 写事务固定再占一条连接，并发写多时互相等待。此刻本事务还没有写入，读到的与走连接池相同。
		members, err := uc.d.Members.List(txrun.NewContext(ctx, tx), inc.ID)
		if err != nil {
			return err
		}
		m := &mutation{uc: uc, tx: tx, inc: inc, members: members, now: uc.now(), actorType: actorType, actorID: actorID}
		if err := fn(m); err != nil {
			return err
		}
		if err := m.commit(ctx); err != nil {
			return err
		}
		out = inc
		return nil
	})
	return out, err
}

// userMutate 以用户身份修改并返回视图。
func (uc *IncidentUsecase) userMutate(ctx context.Context, userID, id string, fn func(m *mutation, a access) error) (*View, error) {
	inc, err := uc.mutate(ctx, id, ActorUser, userID, func(m *mutation) error {
		a, err := uc.accessOf(ctx, userID, m.inc, m.members)
		if err != nil {
			return err
		}
		return fn(m, a)
	})
	if err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, userID, []*Incident{inc}, false)
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// ---------------------------------------------------------------------------
// 开故障
// ---------------------------------------------------------------------------

// CreateInput 开故障入参（手动 / 规则 / 开放接口 / 候选转正共用）。
type CreateInput struct {
	Title         string
	Severity      int32
	BusinessID    string
	Summary       string
	AlertIDs      []string
	AffectedCIIDs []string
	CommanderID   string
	ResponderIDs  []string
	Groups        []string
	OccurTime     *time.Time
	IsDrill       bool
	// 来源。
	Source      int32
	SourceRef   string
	ExternalKey string
	RuleID      string
	Correlation string
	// LinkMethod 关联告警的方式；AlertRefs 已读取的告警（规则路径给出，避免重读）。
	LinkMethod int32
	AlertRefs  []*AlertRef
	// response 规则 / 业务的响应配置（指挥与处理人解析）。
	response responseSpec
	// 自动动作。
	AISummary bool
	Playbook  *playbookTrigger
	// Agent 开故障时入队一次运维 Agent 诊断任务（触发规则 action.agent）。
	Agent bool
}

// playbookTrigger 规则配置的自动处置。
type playbookTrigger struct {
	PlaybookID string
	Mode       int32
}

// number 生成编号 INC-YYYYMMDD-NNN。
func (uc *IncidentUsecase) number(ctx context.Context, tx Tx, now time.Time) (string, error) {
	n, err := uc.d.Writer.NextNumberTx(ctx, tx, now)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("INC-%s-%03d", now.Format("20060102"), n), nil
}

// Create 手动开故障（用户身份）。
func (uc *IncidentUsecase) Create(ctx context.Context, userID string, in CreateInput) (*View, error) {
	in.Source, in.LinkMethod = SourceManual, LinkManual
	if in.Title = cleanText(in.Title); in.Title == "" {
		return nil, invalid("title is required")
	}
	if !ValidSeverity(in.Severity) {
		return nil, invalid("severity must be 1-4")
	}
	in.response = responseSpec{commanderUserID: in.CommanderID, responderUserIDs: in.ResponderIDs}
	// 手动选的告警须在调用者的监控数据范围内。
	if len(in.AlertIDs) > 0 {
		refs, err := uc.readableAlerts(ctx, userID, in.AlertIDs)
		if err != nil {
			return nil, err
		}
		in.AlertRefs = refs
	}
	inc, err := uc.open(ctx, ActorUser, userID, in)
	if err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, userID, []*Incident{inc}, false)
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// readableAlerts 读取并校验调用者可见的告警。
func (uc *IncidentUsecase) readableAlerts(ctx context.Context, userID string, ids []string) ([]*AlertRef, error) {
	refs, err := uc.d.Alerts.ListByIDs(ctx, uniq(ids))
	if err != nil {
		return nil, err
	}
	if len(refs) != len(uniq(ids)) {
		return nil, ErrAlertNotFound
	}
	reader := uc.alertReader(userID)
	for _, a := range refs {
		ok, err := reader.canRead(ctx, a)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrAlertNotFound
		}
	}
	return refs, nil
}

// open 开故障的公共路径：解析角色 → 编号 → 落库 → 关联告警 → 时间线 → 通知 → 自动动作。
func (uc *IncidentUsecase) open(ctx context.Context, actorType int32, actorID string, in CreateInput) (*Incident, error) {
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	now := uc.now()
	var business *Business
	if in.BusinessID != "" {
		if business, err = uc.d.Businesses.Get(ctx, in.BusinessID); err != nil {
			return nil, err
		}
		in.response.businessSchedule = business.ScheduleID
		if len(in.Groups) == 0 {
			in.Groups = business.Groups
		}
	}
	in.response.fallbackSchedule = settings.FallbackScheduleID
	// 受影响资源：显式给出的 + 告警关联的。
	cis := slices.Clone(in.AffectedCIIDs)
	occur := now
	if in.OccurTime != nil {
		occur = *in.OccurTime
	}
	for _, a := range in.AlertRefs {
		if a.CIID != "" {
			cis = append(cis, a.CIID)
		}
		if in.OccurTime == nil && a.FirstFireTime.Before(occur) {
			occur = a.FirstFireTime
		}
	}
	cis = uniq(cis)
	if len(cis) > 500 {
		cis = cis[:500]
	}
	if in.response.responderCIOwner && len(cis) > 0 {
		cards, err := uc.d.CMDB.Cards(ctx, cis)
		if err != nil {
			return nil, err
		}
		for _, c := range cards {
			if idgen.IsUUID(c.OwnerID) {
				in.response.affectedCIOwnerIDs = append(in.response.affectedCIOwnerIDs, c.OwnerID)
			}
		}
	}
	roles, err := uc.resolveRoles(ctx, in.response, now)
	if err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	inc := &Incident{
		ID: id, Title: truncate(in.Title, 255), Severity: in.Severity, Status: StatusOpen, Source: in.Source, SourceRef: in.SourceRef,
		ExternalKey: in.ExternalKey, BusinessID: in.BusinessID, RuleID: in.RuleID, CorrelationKey: in.Correlation, Summary: cleanText(in.Summary),
		CommanderID: roles.commanderID, AffectedCIIDs: cis, Groups: uniq(in.Groups), IsDrill: in.IsDrill, OccurTime: occur, OpenTime: now,
		LastActivity: now, AutoApprove: true, CreateUser: actorID, Version: 1,
	}
	if actorType == ActorSystem {
		inc.CreateUser = ""
	}
	err = uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		number, err := uc.number(ctx, tx, now)
		if err != nil {
			return err
		}
		inc.Number = number
		if err := uc.d.Writer.CreateTx(ctx, tx, inc); err != nil {
			return err
		}
		m := &mutation{uc: uc, tx: tx, inc: inc, now: now, actorType: actorType, actorID: actorID, saved: true}
		for _, r := range roles.responders {
			if err := uc.d.Members.SetTx(ctx, tx, &Member{IncidentID: inc.ID, UserID: r, Role: RoleResponder}, actorID); err != nil {
				return err
			}
		}
		m.event(EventOpened, EventDetail{Text: strings.Join(roles.notes, "；"), Severity: inc.Severity, AlertCount: i32(len(in.AlertRefs))})
		if inc.CommanderID != "" {
			m.event(EventCommander, EventDetail{UserID: inc.CommanderID, Text: "开启时解析"})
		}
		for _, r := range roles.responders {
			m.event(EventMemberAdd, EventDetail{UserID: r, To: RoleResponder})
		}
		attached := 0
		for _, a := range in.AlertRefs {
			ok, err := uc.attachOne(ctx, m, a, in.LinkMethod, actorID)
			if err != nil {
				return err
			}
			if ok {
				attached++
			}
		}
		if attached > 0 {
			ids := make([]string, 0, attached)
			for _, a := range in.AlertRefs {
				ids = append(ids, a.ID)
			}
			m.event(EventAlertAttach, EventDetail{AlertIDs: ids[:min(len(ids), 50)], AlertCount: i32(attached), To: in.LinkMethod})
		}
		m.alerts = attached
		// 通知：群 + 指挥与处理人；无指挥时额外提醒兜底升级（第一次巡检接管）。
		users := slices.Concat([]string{inc.CommanderID}, roles.responders)
		n := notice{action: NotifyOpened, dedup: "1"}
		if settings.Personal(NotifyOpened) {
			n.users = uniq(users)
		}
		m.notify(n)
		m.hook("opened", map[string]any{"alert_count": attached})
		if in.AISummary && uc.d.AI != nil && uc.d.AI.Available() {
			m.action(ActionAISummary, "", nil)
		}
		if in.Agent {
			m.agent("opened")
		}
		if in.Playbook != nil && in.Playbook.PlaybookID != "" && in.Playbook.Mode >= PlaybookPropose {
			if err := uc.autoPropose(ctx, m, in.Playbook, settings); err != nil {
				return err
			}
		}
		return m.commit(ctx)
	})
	if err != nil {
		return nil, err
	}
	// 告警侧时间线（独立语句，失败不影响故障）。
	for _, a := range in.AlertRefs {
		uc.noteAlert(ctx, a.ID, actorID, inc)
	}
	return inc, nil
}

// noteAlert 在告警时间线记一条「已关联故障」；失败只记日志。
func (uc *IncidentUsecase) noteAlert(ctx context.Context, alertID, operator string, inc *Incident) {
	if err := uc.d.Alerts.Note(ctx, alertID, operator, "已关联故障 "+inc.Number+" "+inc.Title); err != nil {
		uc.d.Logger.WarnContext(ctx, "incident: note alert timeline", "alert_id", alertID, "incident", inc.ID, "error", err)
	}
}

// attachOne 在事务内关联一条告警（已在本故障内返回 false；属于别的活跃故障返回 ErrAlertLinked）。
func (uc *IncidentUsecase) attachOne(ctx context.Context, m *mutation, a *AlertRef, method int32, actor string) (bool, error) {
	link := &AlertLink{IncidentID: m.inc.ID, AlertID: a.ID, Method: method, RuleName: a.RuleName, Severity: a.Severity, CIID: a.CIID, AttachUser: actor}
	if !a.FirstFireTime.IsZero() {
		t := a.FirstFireTime
		link.FirstFireTime = &t
	}
	ok, err := uc.d.Links.AttachTx(ctx, m.tx, link)
	if err != nil || !ok {
		return false, err
	}
	if a.CIID != "" && !slices.Contains(m.inc.AffectedCIIDs, a.CIID) && len(m.inc.AffectedCIIDs) < 500 {
		m.inc.AffectedCIIDs = append(m.inc.AffectedCIIDs, a.CIID)
	}
	if !a.FirstFireTime.IsZero() && a.FirstFireTime.Before(m.inc.OccurTime) {
		m.inc.OccurTime = a.FirstFireTime
	}
	return true, nil
}

// ---------------------------------------------------------------------------
// 编辑、等级、状态
// ---------------------------------------------------------------------------

// UpdateInput 编辑入参。
type UpdateInput struct {
	Title         string
	BusinessID    string
	Summary       string
	AffectedCIIDs []string
	Groups        []string
	OccurTime     *time.Time
	IsDrill       bool
	Version       int32
}

// Update 编辑基本信息（处理人以上）。
func (uc *IncidentUsecase) Update(ctx context.Context, userID, id string, in UpdateInput) (*View, error) {
	in.Title = cleanText(in.Title)
	if in.Title == "" {
		return nil, invalid("title is required")
	}
	if in.BusinessID != "" {
		if _, err := uc.d.Businesses.Get(ctx, in.BusinessID); err != nil {
			return nil, err
		}
	}
	return uc.userMutate(ctx, userID, id, func(m *mutation, a access) error {
		if !a.canHandle() {
			return ErrForbidden
		}
		if m.inc.Version != in.Version {
			return ErrVersionConflict
		}
		if m.inc.Status == StatusMerged {
			return ErrStateInvalid
		}
		inc := m.inc
		var changes []string
		if inc.Title != in.Title {
			changes = append(changes, "标题")
			inc.Title = truncate(in.Title, 255)
		}
		if inc.BusinessID != in.BusinessID {
			changes = append(changes, "业务")
			inc.BusinessID = in.BusinessID
		}
		if summary := cleanText(in.Summary); inc.Summary != summary {
			changes = append(changes, "摘要")
			inc.Summary = summary
		}
		if cis := uniq(in.AffectedCIIDs); !slices.Equal(cis, inc.AffectedCIIDs) {
			changes = append(changes, "受影响资源")
			inc.AffectedCIIDs = cis
		}
		if groups := uniq(in.Groups); !slices.Equal(groups, inc.Groups) {
			changes = append(changes, "通知群")
			inc.Groups = groups
		}
		if in.OccurTime != nil && !in.OccurTime.Equal(inc.OccurTime) {
			changes = append(changes, "发生时间")
			inc.OccurTime = *in.OccurTime
		}
		if inc.IsDrill != in.IsDrill {
			changes = append(changes, "演练标记")
			inc.IsDrill = in.IsDrill
		}
		if len(changes) > 0 {
			m.event(EventEdit, EventDetail{Text: "修改了 " + strings.Join(changes, "、")})
		}
		return nil
	})
}

// ChangeSeverity 调整等级（指挥 / 管理员）。
func (uc *IncidentUsecase) ChangeSeverity(ctx context.Context, userID, id string, severity int32, note string) (*View, error) {
	if !ValidSeverity(severity) {
		return nil, invalid("severity must be 1-4")
	}
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	return uc.userMutate(ctx, userID, id, func(m *mutation, a access) error {
		if !a.canManage() {
			return ErrForbidden
		}
		inc := m.inc
		if !IsActive(inc.Status) && inc.Status != StatusResolved {
			return ErrStateInvalid
		}
		if inc.Severity == severity {
			return nil
		}
		from := inc.Severity
		inc.Severity = severity
		// 复盘要求随等级重算（已完成 / 复盘中的不动）。
		if inc.Status == StatusResolved && inc.ReviewStatus <= ReviewPending {
			if err := uc.applyReview(ctx, inc, settings); err != nil {
				return err
			}
		}
		m.event(EventSeverity, EventDetail{From: from, To: severity, Text: cleanText(note)})
		n := notice{action: NotifySeverityChanged, dedup: fmt.Sprint(inc.Version + 1), extra: map[string]any{"from_label": SeverityLabel(from), "text": cleanText(note)}}
		if settings.Personal(NotifySeverityChanged) {
			n.users = uc.roleUsers(m)
		}
		m.notify(n)
		m.hook("severity", map[string]any{"from": from, "to": severity})
		// 等级上调（数字变小）且触发规则开了 Agent：再入队一次诊断（Agent 域按故障唯一去重，只提优先级）。
		if severity < from && inc.RuleID != "" && IsActive(inc.Status) && uc.d.Agent != nil {
			if rule, err := uc.d.Rules.Get(ctx, inc.RuleID); err == nil && rule.Spec.Action.Agent {
				m.agent("escalated")
			}
		}
		return nil
	})
}

// roleUsers 指挥 + 处理人（个人通知对象）。
func (uc *IncidentUsecase) roleUsers(m *mutation) []string {
	users := []string{m.inc.CommanderID}
	for _, mb := range m.members {
		if mb.Role == RoleResponder {
			users = append(users, mb.UserID)
		}
	}
	return uniq(users)
}

// applyReview 恢复时按等级设定复盘要求与到期日。
//
// 复盘允许在恢复之前就写、就完成（Save / Complete 不看故障状态），所以这里不能无条件置回「待复盘」：
// 已完成的原样保留——此前恢复时被重置，用户得把同一份复盘再完成一次（或强制关闭）；已在起草的保持起草，
// 只补上到期日（等级不要求复盘时仍按原口径清掉要求，否则一份没写完的草稿会挡住关闭）。
func (uc *IncidentUsecase) applyReview(ctx context.Context, inc *Incident, settings *Settings) error {
	if inc.ReviewStatus == ReviewDone {
		return nil
	}
	days := settings.Sev(inc.Severity).ReviewDays
	if days <= 0 || inc.IsDrill {
		inc.ReviewStatus, inc.ReviewDue = ReviewNone, nil
		return nil
	}
	base := uc.now()
	if inc.ResolveTime != nil {
		base = *inc.ResolveTime
	}
	due, err := uc.d.Workdays.AddWorkdays(ctx, base, int(days))
	if err != nil {
		return err
	}
	inc.ReviewDue, inc.ReviewRemind = &due, 0
	if inc.ReviewStatus != ReviewDrafting {
		inc.ReviewStatus = ReviewPending
	}
	return nil
}

// 状态流转动作。
const (
	ActionRespond  = "respond"
	ActionMitigate = "mitigate"
	ActionResolve  = "resolve"
	ActionClose    = "close"
	ActionReopen   = "reopen"
)

// TransitionInput 状态流转入参。
type TransitionInput struct {
	Action        string
	Note          string
	RootCauseCode string
	Force         bool
}

// Transition 状态流转。
func (uc *IncidentUsecase) Transition(ctx context.Context, userID, id string, in TransitionInput) (*View, error) {
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	in.Note = cleanText(in.Note)
	return uc.userMutate(ctx, userID, id, func(m *mutation, a access) error {
		return uc.transition(ctx, m, a, in, settings)
	})
}

// transition 在事务内执行一次流转（用户 / 系统共用）。
func (uc *IncidentUsecase) transition(ctx context.Context, m *mutation, a access, in TransitionInput, settings *Settings) error {
	inc := m.inc
	from := inc.Status
	if inc.Status == StatusMerged {
		return ErrStateInvalid
	}
	switch in.Action {
	case ActionRespond:
		if inc.Status != StatusOpen {
			return ErrStateInvalid
		}
		// 任何有读权限的人都能认领：非成员认领即成为处理人。
		if !a.canHandle() && m.actorType == ActorUser && m.actorID != "" {
			if len(m.members) >= maxMembers {
				return invalid("too many members")
			}
			if err := uc.d.Members.SetTx(ctx, m.tx, &Member{IncidentID: inc.ID, UserID: m.actorID, Role: RoleResponder}, m.actorID); err != nil {
				return err
			}
			m.members = append(m.members, &Member{IncidentID: inc.ID, UserID: m.actorID, Role: RoleResponder})
			m.event(EventMemberAdd, EventDetail{UserID: m.actorID, To: RoleResponder, Text: "认领"})
		}
		inc.Status = StatusResponding
		inc.RespondTime = ptr(m.now)
		m.touch()
	case ActionMitigate:
		if !a.canHandle() {
			return ErrForbidden
		}
		if inc.Status != StatusOpen && inc.Status != StatusResponding {
			return ErrStateInvalid
		}
		if inc.RespondTime == nil {
			inc.RespondTime = ptr(m.now)
		}
		inc.Status = StatusMitigated
		inc.MitigateTime = ptr(m.now)
		m.touch()
	case ActionResolve:
		if !a.canHandle() {
			return ErrForbidden
		}
		if !IsActive(inc.Status) {
			return ErrStateInvalid
		}
		if in.Note == "" && m.actorType == ActorUser {
			return invalid("resolve note is required")
		}
		if !settings.ValidRootCause(in.RootCauseCode) {
			return invalid("unknown root cause code")
		}
		if inc.RespondTime == nil {
			inc.RespondTime = ptr(m.now)
		}
		inc.Status = StatusResolved
		inc.ResolveTime = ptr(m.now)
		if inc.FirstResolve == nil {
			inc.FirstResolve = ptr(m.now)
		}
		inc.ResolveNote = in.Note
		if in.RootCauseCode != "" {
			inc.RootCauseCode = in.RootCauseCode
		}
		if err := uc.applyReview(ctx, inc, settings); err != nil {
			return err
		}
		inc.AgentOwner, inc.AgentLeaseUntil = "", nil
		if err := uc.cancelPendingProposals(ctx, m, "故障已恢复"); err != nil {
			return err
		}
		m.action(ActionTicketNote, "", map[string]any{"status": StatusResolved})
		m.touch()
	case ActionClose:
		// 关闭（含 force 跳过复盘）的权限是同一条：故障指挥或超级管理员（canManage）。force 不另设更高的
		// 权限，只多要求填写原因并记入时间线。
		if !a.canManage() {
			return ErrForbidden
		}
		if inc.Status != StatusResolved {
			return ErrStateInvalid
		}
		if inc.ReviewStatus == ReviewPending || inc.ReviewStatus == ReviewDrafting {
			if !in.Force {
				return ErrReviewRequired
			}
			if in.Note == "" {
				return invalid("a reason is required to close without a postmortem")
			}
		}
		inc.Status = StatusClosed
		inc.CloseTime = ptr(m.now)
		if err := uc.d.Links.ReleaseAllTx(ctx, m.tx, inc.ID); err != nil {
			return err
		}
		if err := uc.cancelPendingProposals(ctx, m, "故障已关闭"); err != nil {
			return err
		}
	case ActionReopen:
		if !a.canManage() {
			return ErrForbidden
		}
		if inc.Status != StatusResolved {
			return ErrStateInvalid
		}
		if inc.ResolveTime != nil && m.now.Sub(*inc.ResolveTime) > time.Duration(settings.ReopenWindowHours)*time.Hour {
			return detail(ErrStateInvalid, "reopen window (%d hours) has passed", settings.ReopenWindowHours)
		}
		// 同规则、同归并键已有活跃故障时，重开会撞唯一索引 uk_incident_correlation：此前被报成「已被他人修改，
		// 请刷新重试」，刷新重试仍然失败。先查出来，提示改用合并。
		other, err := uc.activeByCorrelation(ctx, inc)
		if err != nil {
			return err
		}
		if other != nil {
			return detail(ErrStateInvalid, "incident %s with the same correlation key is already active, merge into it instead", other.Number)
		}
		inc.Status = StatusResponding
		inc.ResolveTime, inc.MitigateTime = nil, nil
		inc.ReopenCount++
		// 重开后复盘要求作废（再次恢复时按等级重算）：留着旧的到期日，处理中的故障也会收到「复盘即将到期 / 已超期」。
		inc.ReviewStatus, inc.ReviewDue, inc.ReviewRemind = ReviewNone, nil, 0
		m.touch()
		m.event(EventReopen, EventDetail{Text: in.Note})
	default:
		return invalid("unknown action %q", in.Action)
	}
	m.event(EventStatus, EventDetail{From: from, To: inc.Status, Text: in.Note})
	n := notice{action: NotifyStatusChanged, dedup: fmt.Sprintf("%d-%d", inc.Status, inc.Version+1), extra: map[string]any{"text": in.Note}}
	if settings.Personal(NotifyStatusChanged) {
		n.users = uc.roleUsers(m)
	}
	m.notify(n)
	m.hook("status", map[string]any{"from": from, "to": inc.Status, "action": in.Action})
	return nil
}

// activeByCorrelation 同触发规则、同归并键的活跃故障（没有返回 nil）。uk_incident_correlation 保证至多一条。
// 规则开出的故障才有归并键，手动 / 开放接口开出的（RuleID 为空）不存在这种冲突。
func (uc *IncidentUsecase) activeByCorrelation(ctx context.Context, inc *Incident) (*Incident, error) {
	if inc.RuleID == "" || inc.CorrelationKey == "" {
		return nil, nil
	}
	items, _, err := uc.d.Incidents.List(ctx, IncidentFilter{
		RuleID: inc.RuleID, CorrelationKey: inc.CorrelationKey, Statuses: []int32{StatusOpen, StatusResponding, StatusMitigated},
	}, 1, 1)
	if err != nil || len(items) == 0 {
		return nil, err
	}
	return items[0], nil
}

func ptr(t time.Time) *time.Time { return &t }

// ---------------------------------------------------------------------------
// 合并、角色、成员
// ---------------------------------------------------------------------------

// Merge 把 id 合并到 targetID。
func (uc *IncidentUsecase) Merge(ctx context.Context, userID, id, targetID, note string) (*View, error) {
	if id == targetID {
		return nil, invalid("cannot merge an incident into itself")
	}
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	note = cleanText(note)
	// 两条故障按 ID 顺序加锁，避免互相合并时死锁。
	var merged *Incident
	err = uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		first, second := id, targetID
		if second < first {
			first, second = second, first
		}
		a, err := uc.d.Writer.LockTx(ctx, tx, first)
		if err != nil {
			return err
		}
		b, err := uc.d.Writer.LockTx(ctx, tx, second)
		if err != nil {
			return err
		}
		src, dst := a, b
		if src.ID != id {
			src, dst = b, a
		}
		// 同 mutate：两次成员读取都在本事务的连接上（此刻尚无写入，结果与走连接池相同）。
		txCtx := txrun.NewContext(ctx, tx)
		srcMembers, err := uc.d.Members.List(txCtx, src.ID)
		if err != nil {
			return err
		}
		dstMembers, err := uc.d.Members.List(txCtx, dst.ID)
		if err != nil {
			return err
		}
		acc, err := uc.accessOf(ctx, userID, src, srcMembers)
		if err != nil {
			return err
		}
		if !acc.canManage() {
			return ErrForbidden
		}
		if !IsActive(src.Status) && src.Status != StatusResolved {
			return ErrStateInvalid
		}
		if !IsActive(dst.Status) {
			return detail(ErrStateInvalid, "target incident is not active")
		}
		now := uc.now()
		ms := &mutation{uc: uc, tx: tx, inc: src, members: srcMembers, now: now, actorType: ActorUser, actorID: userID}
		md := &mutation{uc: uc, tx: tx, inc: dst, members: dstMembers, now: now, actorType: ActorUser, actorID: userID}
		moved, err := uc.d.Links.MoveAllTx(ctx, tx, src.ID, dst.ID)
		if err != nil {
			return err
		}
		if err := uc.d.Links.ReleaseAllTx(ctx, tx, src.ID); err != nil {
			return err
		}
		// 角色并入目标：源指挥与处理人成为目标处理人，关注人成为关注人。
		has := map[string]int32{dst.CommanderID: RoleCommander}
		for _, mb := range dstMembers {
			has[mb.UserID] = mb.Role
		}
		join := func(userID string, role int32) error {
			if userID == "" || has[userID] != 0 || len(dstMembers)+len(has) > maxMembers {
				return nil
			}
			has[userID] = role
			md.event(EventMemberAdd, EventDetail{UserID: userID, To: role, Text: "合并自 " + src.Number})
			return uc.d.Members.SetTx(ctx, tx, &Member{IncidentID: dst.ID, UserID: userID, Role: role}, userID)
		}
		if err := join(src.CommanderID, RoleResponder); err != nil {
			return err
		}
		for _, mb := range srcMembers {
			if err := join(mb.UserID, mb.Role); err != nil {
				return err
			}
		}
		for _, ci := range src.AffectedCIIDs {
			if !slices.Contains(dst.AffectedCIIDs, ci) && len(dst.AffectedCIIDs) < 500 {
				dst.AffectedCIIDs = append(dst.AffectedCIIDs, ci)
			}
		}
		if src.OccurTime.Before(dst.OccurTime) {
			dst.OccurTime = src.OccurTime
		}
		src.Status, src.MergedInto = StatusMerged, dst.ID
		src.AgentOwner, src.AgentLeaseUntil = "", nil
		// 已合并的故障不再有复盘：不清掉的话巡检会按旧的到期日照样提醒，而复盘接口对它已返回状态错误。
		src.ReviewStatus, src.ReviewDue, src.ReviewRemind = ReviewNone, nil, 0
		if err := uc.cancelPendingProposals(ctx, ms, "故障已合并到 "+dst.Number); err != nil {
			return err
		}
		ms.event(EventMerged, EventDetail{IncidentID: dst.ID, IncidentNumber: dst.Number, AlertCount: i32(moved), Text: note})
		md.event(EventMergedFrom, EventDetail{IncidentID: src.ID, IncidentNumber: src.Number, AlertCount: i32(moved), Text: note})
		n := notice{action: NotifyMerged, dedup: dst.ID, extra: map[string]any{"target_number": dst.Number, "text": note}}
		if settings.Personal(NotifyMerged) {
			n.users = uc.roleUsers(ms)
		}
		ms.notify(n)
		ms.hook("merged", map[string]any{"target_id": dst.ID, "target_number": dst.Number})
		if err := ms.commit(ctx); err != nil {
			return err
		}
		if err := md.commit(ctx); err != nil {
			return err
		}
		merged = src
		return nil
	})
	if err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, userID, []*Incident{merged}, false)
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// AssignCommander 指派 / 转交指挥（指挥、管理员；无指挥时处理人也可指派）。
func (uc *IncidentUsecase) AssignCommander(ctx context.Context, userID, id, toUser, note string) (*View, error) {
	active, err := uc.d.Dir.ActiveUserIDs(ctx, []string{toUser})
	if err != nil {
		return nil, err
	}
	if len(active) != 1 {
		return nil, invalid("the user is not active")
	}
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	return uc.userMutate(ctx, userID, id, func(m *mutation, a access) error {
		if !a.canManage() && (m.inc.CommanderID != "" || !a.canHandle()) {
			return ErrForbidden
		}
		if !IsActive(m.inc.Status) && m.inc.Status != StatusResolved {
			return ErrStateInvalid
		}
		if m.inc.CommanderID == toUser {
			return nil
		}
		from := m.inc.CommanderID
		m.inc.CommanderID = toUser
		// 新指挥若是成员则移出成员列表（角色唯一）。
		for _, mb := range m.members {
			if mb.UserID == toUser {
				if err := uc.d.Members.RemoveTx(ctx, m.tx, m.inc.ID, toUser); err != nil {
					return err
				}
			}
		}
		m.event(EventCommander, EventDetail{UserID: toUser, Text: cleanText(note)})
		// Webhook 订阅里的 commander 事件此前只在可订阅列表中，从未发出。
		m.hook("commander", map[string]any{"commander_id": toUser, "from": from})
		if from != "" {
			m.event(EventMemberRemove, EventDetail{UserID: from, From: RoleCommander, Text: "转交指挥"})
		}
		if settings.Personal(NotifyAssigned) {
			m.notify(notice{
				action: NotifyAssigned, dedup: toUser + fmt.Sprint(m.inc.Version+1), users: []string{toUser}, noGroups: true,
				extra: map[string]any{"role_label": "指挥"},
			})
		}
		return nil
	})
}

// UpdateMembers 添加 / 移除处理人或关注人。有读权限的人可把自己加为关注人、移除自己；其余需处理人以上。
func (uc *IncidentUsecase) UpdateMembers(ctx context.Context, userID, id string, role int32, add, remove []string) (*View, error) {
	if role != RoleResponder && role != RoleWatcher {
		return nil, invalid("role must be 2 or 3")
	}
	add, remove = uniq(add), uniq(remove)
	if len(add) > 0 {
		active, err := uc.d.Dir.ActiveUserIDs(ctx, add)
		if err != nil {
			return nil, err
		}
		add = active
	}
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	return uc.userMutate(ctx, userID, id, func(m *mutation, a access) error {
		selfOnly := len(add)+len(remove) == 1 && (len(add) == 1 && add[0] == userID && role == RoleWatcher || len(remove) == 1 && remove[0] == userID)
		if !a.canHandle() && !selfOnly {
			return ErrForbidden
		}
		if m.inc.Status == StatusMerged || m.inc.Status == StatusClosed {
			return ErrStateInvalid
		}
		current := map[string]int32{}
		for _, mb := range m.members {
			current[mb.UserID] = mb.Role
		}
		for _, u := range remove {
			if _, ok := current[u]; !ok {
				continue
			}
			if err := uc.d.Members.RemoveTx(ctx, m.tx, m.inc.ID, u); err != nil {
				return err
			}
			m.event(EventMemberRemove, EventDetail{UserID: u, From: current[u]})
			delete(current, u)
		}
		var assigned []string
		for _, u := range add {
			if u == m.inc.CommanderID || current[u] == role {
				continue
			}
			if len(current) >= maxMembers {
				return invalid("too many members")
			}
			if err := uc.d.Members.SetTx(ctx, m.tx, &Member{IncidentID: m.inc.ID, UserID: u, Role: role}, userID); err != nil {
				return err
			}
			current[u] = role
			m.event(EventMemberAdd, EventDetail{UserID: u, To: role})
			if role == RoleResponder && u != userID {
				assigned = append(assigned, u)
			}
		}
		if len(assigned) > 0 && settings.Personal(NotifyAssigned) {
			m.notify(notice{action: NotifyAssigned, dedup: fmt.Sprint(m.inc.Version + 1), users: assigned, noGroups: true, extra: map[string]any{"role_label": "处理人"}})
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// 备注、钉住、时间线
// ---------------------------------------------------------------------------

// AddComment 备注（有读权限即可）。
func (uc *IncidentUsecase) AddComment(ctx context.Context, userID, id, text string, fileIDs []string) (*Event, error) {
	text = cleanText(text)
	if text == "" || len(text) > maxCommentBytes {
		return nil, invalid("comment must be 1-%d bytes", maxCommentBytes)
	}
	var out *Event
	_, err := uc.mutate(ctx, id, ActorUser, userID, func(m *mutation) error {
		if m.inc.Status == StatusMerged {
			return ErrStateInvalid
		}
		fileIDs = uniq(fileIDs)
		if len(fileIDs) > 0 {
			if err := uc.d.Files.BindTx(ctx, m.tx, userID, m.inc.ID, fileIDs); err != nil {
				return err
			}
		}
		out = m.event(EventComment, EventDetail{Text: text, FileIDs: fileIDs})
		m.touch()
		m.hook("comment", map[string]any{"text": truncate(text, 2000)})
		return nil
	})
	return out, err
}

// PinEvent 钉住 / 取消钉住（处理人以上）。
func (uc *IncidentUsecase) PinEvent(ctx context.Context, userID, id, eventID string, pinned bool) (*Event, error) {
	ev, err := uc.d.Events.Get(ctx, id, eventID)
	if err != nil {
		return nil, err
	}
	_, err = uc.userMutate(ctx, userID, id, func(m *mutation, a access) error {
		if !a.canHandle() {
			return ErrForbidden
		}
		return uc.d.Events.SetPinnedTx(ctx, m.tx, id, eventID, pinned)
	})
	if err != nil {
		return nil, err
	}
	ev.Pinned = pinned
	return ev, nil
}

// TimelineItem 带操作者显示名的时间线记录。
type TimelineItem struct {
	*Event
	ActorName string
	// Names 细节里引用的用户 / 故障 / 建议的显示名。
	Names map[string]string
}

// ListTimeline 时间线（按时间升序）。
func (uc *IncidentUsecase) ListTimeline(ctx context.Context, id string, filter TimelineFilter, page, pageSize int64) ([]*TimelineItem, int64, error) {
	if _, err := uc.d.Incidents.Get(ctx, id); err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, min(pageSize, timelineMaxPageSize))
	if pageSize < 1 {
		pageSize = paging.DefaultPageSize
	}
	events, total, err := uc.d.Events.List(ctx, id, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	items, err := uc.timelineItems(ctx, events)
	return items, total, err
}

// timelineItems 补齐显示名。
func (uc *IncidentUsecase) timelineItems(ctx context.Context, events []*Event) ([]*TimelineItem, error) {
	ns := &nameSet{}
	for _, ev := range events {
		ns.user(ev.ActorID, ev.Detail.UserID)
		ns.incident(ev.Detail.IncidentID)
	}
	if err := uc.resolve(ctx, ns); err != nil {
		return nil, err
	}
	out := make([]*TimelineItem, 0, len(events))
	for _, ev := range events {
		out = append(out, &TimelineItem{Event: ev, ActorName: ns.get("user", ev.ActorID), Names: ns.names})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Agent 租约与发现
// ---------------------------------------------------------------------------

// AcquireAgentLease 取得 Agent 租约。
func (uc *IncidentUsecase) AcquireAgentLease(ctx context.Context, userID, id string, ttl time.Duration) (*View, error) {
	if ttl <= 0 {
		ttl = agentLeaseDefault
	}
	ttl = min(ttl, agentLeaseMax)
	return uc.userMutate(ctx, userID, id, func(m *mutation, _ access) error {
		inc := m.inc
		if !IsActive(inc.Status) {
			return ErrStateInvalid
		}
		if inc.AgentOwner != "" && inc.AgentOwner != userID && inc.AgentLeaseUntil != nil && inc.AgentLeaseUntil.After(m.now) {
			return ErrAgentBusy
		}
		renew := inc.AgentOwner == userID
		inc.AgentOwner = userID
		inc.AgentLeaseUntil = ptr(m.now.Add(ttl))
		if !renew {
			m.event(EventAgentLease, EventDetail{UserID: userID, Text: "开始 Agent 任务"})
			m.hook("agent_lease", map[string]any{"owner": userID, "until": inc.AgentLeaseUntil})
		}
		return nil
	})
}

// MemberAccess 调用者在该故障里的身份（跨域窄接口：运维 Agent 域据此判定运行详情的可见范围与评价权）。
// 故障不存在返回 ErrNotFound。
func (uc *IncidentUsecase) MemberAccess(ctx context.Context, userID, id string) (admin, commander, responder bool, err error) {
	inc, err := uc.d.Incidents.Get(ctx, id)
	if err != nil {
		return false, false, false, err
	}
	members, err := uc.d.Members.List(ctx, inc.ID)
	if err != nil {
		return false, false, false, err
	}
	a, err := uc.accessOf(ctx, userID, inc, members)
	if err != nil {
		return false, false, false, err
	}
	return a.admin, a.commander, a.responder, nil
}

// Snapshot 不经权限判定地读一条故障（跨域窄接口：运维 Agent 域领取任务前做状态检查）；不存在返回 ErrNotFound。
func (uc *IncidentUsecase) Snapshot(ctx context.Context, id string) (*Incident, error) {
	return uc.d.Incidents.Get(ctx, id)
}

// ReleaseAgentLease 释放租约（持有者或指挥）。
func (uc *IncidentUsecase) ReleaseAgentLease(ctx context.Context, userID, id string) (*View, error) {
	return uc.userMutate(ctx, userID, id, func(m *mutation, a access) error {
		inc := m.inc
		if inc.AgentOwner == "" {
			return nil
		}
		if inc.AgentOwner != userID && !a.canManage() {
			return ErrForbidden
		}
		text := "结束 Agent 任务"
		if inc.AgentOwner != userID {
			text = "指挥停止了 Agent 任务"
		}
		m.event(EventAgentLease, EventDetail{UserID: inc.AgentOwner, Text: text})
		inc.AgentOwner, inc.AgentLeaseUntil = "", nil
		return nil
	})
}

// AddFinding Agent 写入一条发现（须持有有效租约；条数受设置上限）。
func (uc *IncidentUsecase) AddFinding(ctx context.Context, userID, id string, f Finding) (*Event, error) {
	f.Title = cleanText(f.Title)
	if f.Title == "" {
		return nil, invalid("finding title is required")
	}
	f.Conclusion, f.NextStep = cleanText(f.Conclusion), cleanText(f.NextStep)
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	var out *Event
	_, err = uc.mutate(ctx, id, ActorAgent, userID, func(m *mutation) error {
		inc := m.inc
		if inc.AgentOwner != userID || inc.AgentLeaseUntil == nil || !inc.AgentLeaseUntil.After(m.now) {
			return detail(ErrForbidden, "a valid agent lease is required")
		}
		if inc.AgentFindings >= settings.AgentMaxFindings {
			return detail(ErrRateLimited, "agent findings exceed %d", settings.AgentMaxFindings)
		}
		inc.AgentFindings++
		out = m.event(EventFinding, EventDetail{Finding: &f})
		m.hook("finding", f)
		// 运维 Agent 的发现：同一次运行只发一条通知（去重键 = 运行 ID），不按工具调用逐条发。
		if f.RunID != "" {
			n := notice{action: NotifyAgent, dedup: f.RunID, extra: map[string]any{
				"finding_title": f.Title, "confidence": f.Confidence, "text": truncate(f.Conclusion, 2000), "next_step": truncate(f.NextStep, 1000),
			}}
			if settings.Personal(NotifyAgent) {
				n.users = uc.roleUsers(m)
			}
			m.notify(n)
		}
		return nil
	})
	return out, err
}

// ---------------------------------------------------------------------------
// Webhook 出站
// ---------------------------------------------------------------------------

// enqueueHooks 把领域事件按订阅写入出站箱（与业务写同事务）。
func (uc *IncidentUsecase) enqueueHooks(ctx context.Context, tx Tx, inc *Incident, hooks []hookEvent) error {
	webhooks, err := uc.hooks.enabled(ctx)
	if err != nil {
		return err
	}
	if len(webhooks) == 0 {
		return nil
	}
	var items []*OutboxItem
	for _, h := range hooks {
		body, err := json.Marshal(map[string]any{
			"event": h.eventType, "time": uc.now().UTC().Format(time.RFC3339), "incident": map[string]any{
				"id": inc.ID, "number": inc.Number, "title": inc.Title, "severity": inc.Severity, "status": inc.Status,
				"business_id": inc.BusinessID, "commander_id": inc.CommanderID, "affected_ci_ids": inc.AffectedCIIDs,
				"occur_time": inc.OccurTime, "is_drill": inc.IsDrill, "url": uc.d.Site.IncidentURL(inc.ID),
			}, "detail": h.detail,
		})
		if err != nil {
			return fmt.Errorf("encode webhook payload: %w", err)
		}
		for _, w := range webhooks {
			if len(w.Events) > 0 && !slices.Contains(w.Events, h.eventType) {
				continue
			}
			id, err := idgen.NewID()
			if err != nil {
				return err
			}
			items = append(items, &OutboxItem{ID: id, WebhookID: w.ID, IncidentID: inc.ID, EventType: h.eventType, Payload: body})
		}
	}
	if len(items) == 0 {
		return nil
	}
	return uc.d.Outbox.EnqueueTx(ctx, tx, items)
}

// errNoOp 内部控制流：无需写回。
var errNoOp = errors.New("incident: no-op")

// FileAccess 故障附件的访问判定（file 域 AccessChecker）：持有「查询故障详情」接口权限即可读（故障全局可见）。
type FileAccess struct {
	incidents IncidentReader
	perms     Permissions
}

// NewFileAccess new a FileAccess.
// 只依赖仓储与权限判定（不经 Deps）：Deps 持有 file 域用例，而 file 域的检查器表又装配本类型，经 Deps 会成环。
func NewFileAccess(incidents IncidentReader, perms Permissions) *FileAccess {
	return &FileAccess{incidents: incidents, perms: perms}
}

// CanReadFile implements file 域的 AccessChecker。
func (a *FileAccess) CanReadFile(ctx context.Context, userID, bizID, _ string) (bool, error) {
	if _, err := a.incidents.Get(ctx, bizID); err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return a.perms.CanReadIncidents(ctx, userID)
}
