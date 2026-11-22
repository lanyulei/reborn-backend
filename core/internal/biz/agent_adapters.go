package biz

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"time"

	incidentv1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	bizagent "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	bizincident "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/google/wire"
)

// 运维 Agent 域的跨域窄接口适配器：Agent → assistant（无人值守运行、会话与转录、工具目录、回环执行器）、
// Agent → system（机器账号令牌与类型、显示名、菜单权限）、Agent → incident（故障快照、成员判定、读权限）、
// Agent → notify（平台自监控事件）；以及 incident → agent 的入队窄接口（只依赖任务仓储，不构成依赖环）。

// AgentProviderSet 运维 Agent 域的组合根 providers。
var AgentProviderSet = wire.NewSet(
	NewAgentHeadless, NewAgentConversations, NewAgentTools, NewAgentInvoker, NewAgentAccounts, NewAgentIncidents, NewAgentNotifier, NewIncidentAgentDispatcher,
)

// ---------------------------------------------------------------------------
// assistant
// ---------------------------------------------------------------------------

// NewAgentHeadless 无人值守运行入口 = assistant 的 AgentUsecase。
func NewAgentHeadless(uc *bizassistant.AgentUsecase) bizagent.Headless { return uc }

type agentConversations struct {
	convs bizassistant.ConversationRepo
	msgs  bizassistant.MessageRepo
	calls bizassistant.ToolCallRepo
}

// NewAgentConversations 构造会话适配器（kind=agent 的会话建立与转录读取）。
func NewAgentConversations(convs bizassistant.ConversationRepo, msgs bizassistant.MessageRepo, calls bizassistant.ToolCallRepo) bizagent.Conversations {
	return agentConversations{convs: convs, msgs: msgs, calls: calls}
}

// Create implements bizagent.Conversations.
func (a agentConversations) Create(ctx context.Context, userID, title string) (string, error) {
	id, err := idgen.NewID()
	if err != nil {
		return "", err
	}
	now := time.Now()
	if err := a.convs.Create(ctx, &bizassistant.Conversation{ID: id, UserID: userID, Title: title, Kind: bizassistant.ConversationKindAgent, CreateTime: now, UpdateTime: now}); err != nil {
		return "", err
	}
	return id, nil
}

// Transcript implements bizagent.Conversations.
func (a agentConversations) Transcript(ctx context.Context, conversationID string) ([]*bizassistant.Message, []*bizassistant.ToolCall, error) {
	msgs, err := a.msgs.List(ctx, conversationID)
	if err != nil {
		return nil, nil, err
	}
	calls, err := a.calls.ListSince(ctx, conversationID, time.Time{})
	if err != nil {
		return nil, nil, err
	}
	return msgs, calls, nil
}

// Purge implements bizagent.Conversations：物理删除会话及其消息与工具调用记录。
func (a agentConversations) Purge(ctx context.Context, conversationIDs []string) error {
	return a.convs.Purge(ctx, conversationIDs)
}

type agentTools struct {
	catalog bizassistant.ToolCatalog
}

// NewAgentTools 构造工具目录适配器。
func NewAgentTools(catalog bizassistant.ToolCatalog) bizagent.Tools {
	return agentTools{catalog: catalog}
}

// Find implements bizagent.Tools.
func (t agentTools) Find(name string) *bizassistant.ToolSpec { return t.catalog.Find(name) }

// All implements bizagent.Tools.
func (t agentTools) All() []bizassistant.ToolSpec { return slices.Clone(t.catalog) }

// NewAgentInvoker 回环执行器 = assistant 的 ToolInvoker。
func NewAgentInvoker(inv bizassistant.ToolInvoker) bizagent.Invoker { return inv }

// ---------------------------------------------------------------------------
// system
// ---------------------------------------------------------------------------

type agentAccounts struct {
	auth  *bizsystem.AuthUsecase
	users *bizsystem.UserUsecase
	dir   *bizsystem.DirectoryUsecase
	authz *bizsystem.AuthzUsecase
}

// NewAgentAccounts 构造机器账号适配器。
func NewAgentAccounts(auth *bizsystem.AuthUsecase, users *bizsystem.UserUsecase, dir *bizsystem.DirectoryUsecase, authz *bizsystem.AuthzUsecase) bizagent.Accounts {
	return agentAccounts{auth: auth, users: users, dir: dir, authz: authz}
}

// IssueToken implements bizagent.Accounts.
func (a agentAccounts) IssueToken(ctx context.Context, userID string, ttl time.Duration) (string, time.Time, error) {
	return a.auth.IssueMachineToken(ctx, userID, ttl)
}

// IsMachine implements bizagent.Accounts：不存在 / 不是机器 / 已停用 → false。
func (a agentAccounts) IsMachine(ctx context.Context, userID string) (bool, error) {
	u, err := a.users.Get(ctx, userID)
	if err != nil {
		if bizsystem.ErrUserNotFound.Is(err) {
			return false, nil
		}
		return false, err
	}
	return u.IsMachine() && u.Status == bizsystem.StatusEnabled, nil
}

// Names implements bizagent.Accounts.
func (a agentAccounts) Names(ctx context.Context, ids []string) (map[string]string, error) {
	if len(ids) == 0 {
		return map[string]string{}, nil
	}
	return a.dir.UserNames(ctx, ids)
}

// HasMenu implements bizagent.Accounts：超管恒 true。
func (a agentAccounts) HasMenu(ctx context.Context, userID, menu string) (bool, error) {
	admin, err := a.users.IsAdminUser(ctx, userID)
	if err != nil {
		return false, err
	}
	if admin {
		return true, nil
	}
	menus, _, err := a.authz.EffectiveGrants(ctx, userID)
	if err != nil {
		return false, err
	}
	return slices.Contains(menus, menu), nil
}

// IsAdmin implements bizagent.Accounts.
func (a agentAccounts) IsAdmin(ctx context.Context, userID string) (bool, error) {
	return a.users.IsAdminUser(ctx, userID)
}

// ---------------------------------------------------------------------------
// incident
// ---------------------------------------------------------------------------

type agentIncidents struct {
	incidents *bizincident.IncidentUsecase
	authz     *bizsystem.AuthzUsecase
}

// NewAgentIncidents 构造故障只读适配器。
func NewAgentIncidents(incidents *bizincident.IncidentUsecase, authz *bizsystem.AuthzUsecase) bizagent.Incidents {
	return agentIncidents{incidents: incidents, authz: authz}
}

// Snapshot implements bizagent.Incidents.
func (a agentIncidents) Snapshot(ctx context.Context, id string) (*bizagent.IncidentSnapshot, error) {
	inc, err := a.incidents.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	return &bizagent.IncidentSnapshot{
		ID: inc.ID, Number: inc.Number, Title: inc.Title, Severity: inc.Severity, Status: inc.Status, Active: bizincident.IsActive(inc.Status),
		BusinessID: inc.BusinessID, CommanderID: inc.CommanderID, RespondTime: inc.RespondTime, OccurTime: inc.OccurTime, ResolveTime: inc.ResolveTime,
		AgentOwner: inc.AgentOwner, LeaseValid: inc.AgentOwner != "" && inc.AgentLeaseUntil != nil && inc.AgentLeaseUntil.After(now), IsDrill: inc.IsDrill,
	}, nil
}

// MemberAccess implements bizagent.Incidents.
func (a agentIncidents) MemberAccess(ctx context.Context, userID, incidentID string) (bool, bool, bool, error) {
	return a.incidents.MemberAccess(ctx, userID, incidentID)
}

// CanRead implements bizagent.Incidents：持有「查询故障详情」接口权限。
func (a agentIncidents) CanRead(ctx context.Context, userID string) (bool, error) {
	return a.authz.Allowed(ctx, userID, bizsystem.OperationCode(incidentv1.OperationIncidentServiceGetIncident))
}

// incidentAgentDispatcher incident → agent 的入队窄接口：只依赖任务仓储（不依赖 Agent 用例），避免与 Agent → incident 成环。
type incidentAgentDispatcher struct {
	tasks bizagent.TaskRepo
}

// NewIncidentAgentDispatcher 构造入队适配器。
func NewIncidentAgentDispatcher(tasks bizagent.TaskRepo) bizincident.AgentDispatcher {
	return incidentAgentDispatcher{tasks: tasks}
}

// Enqueue implements bizincident.AgentDispatcher。
func (d incidentAgentDispatcher) Enqueue(ctx context.Context, tx bizincident.Tx, ref bizincident.AgentTaskRef) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	now := time.Now()
	title := ref.Title
	if len([]rune(title)) > 255 {
		title = string([]rune(title)[:255])
	}
	kind, source, period := bizagent.KindDiagnose, bizagent.SourceIncident, ""
	if ref.Kind == bizagent.KindPostmortem {
		kind, source, period = bizagent.KindPostmortem, bizagent.SourcePostmortem, "pm:v"+strconv.Itoa(int(ref.Version))
	}
	return d.tasks.EnqueueTx(ctx, tx, &bizagent.Task{
		ID: id, Kind: kind, Source: source, IncidentID: ref.IncidentID, IncidentNumber: ref.Number, IncidentTitle: title,
		Severity: ref.Severity, BusinessID: ref.BusinessID, RuleID: ref.RuleID, Reason: ref.Reason, PeriodKey: period, Status: bizagent.TaskQueued, NextTime: now, QueuedTime: now, UpdateTime: now,
	})
}

// ---------------------------------------------------------------------------
// notify
// ---------------------------------------------------------------------------

// agentNotifyBizID / agentNotifyEventPrefix 平台自监控事件口径：biz_id=agent，event_type=agent.ops.<kind>；
// 收件人 = 部署配置 monitor.engine_notify_roles 里各角色的可用成员（与 monitor.engine.* 同一配置），
// 没有配置时不指定收件人，交由通知路由的固定收件人与订阅者。
const (
	agentNotifyBizID       = "agent"
	agentNotifyEventPrefix = "agent.ops."
	agentNotifySeverity    = 3
	// agentNotifyChunk 单个通知事件的显式收件上限（notify 入参约束），超出拆成多个事件。
	agentNotifyChunk = 20
)

type agentNotifier struct {
	txm      txrun.Manager
	writer   biznotify.NotificationWriter
	audience bizmonitor.EngineAudience
}

// NewAgentNotifier 构造平台自监控通知适配器（收件人解析复用监控域自监控通知的角色配置）。
func NewAgentNotifier(txm txrun.Manager, writer biznotify.NotificationWriter, audience bizmonitor.EngineAudience) bizagent.Notifier {
	return agentNotifier{txm: txm, writer: writer, audience: audience}
}

// Notify implements bizagent.Notifier.
func (n agentNotifier) Notify(ctx context.Context, kind, title, text, dedup string) error {
	payload, err := json.Marshal(map[string]any{"title": title, "text": text, "kind": kind})
	if err != nil {
		return err
	}
	chunks := [][]string{nil}
	if n.audience != nil {
		users, err := n.audience.UserIDs(ctx)
		if err != nil {
			return err
		}
		if len(users) > 0 {
			chunks = slices.Collect(slices.Chunk(users, agentNotifyChunk))
		}
	}
	return n.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		for i, chunk := range chunks {
			key := "agent:" + dedup
			if i > 0 {
				key += ":" + strconv.Itoa(i)
			}
			if _, err := n.writer.NotifyEvent(ctx, tx, biznotify.Event{
				BizID: agentNotifyBizID, EventType: agentNotifyEventPrefix + kind, Severity: agentNotifySeverity, Title: title, Payload: payload,
				GroupKey: "agent:" + kind, DedupKey: key, Recipients: chunk,
			}); err != nil {
				return err
			}
		}
		return nil
	})
}
