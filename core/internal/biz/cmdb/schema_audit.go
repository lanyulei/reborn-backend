package biz

import (
	"context"
	"log/slog"
	"reflect"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 模型变更审计的对象类型与动作。
const (
	AuditTargetGroup        = "group"
	AuditTargetModel        = "model"
	AuditTargetField        = "field"
	AuditTargetRelationType = "relation_type"

	AuditActionCreate = "create"
	AuditActionUpdate = "update"
	AuditActionDelete = "delete"
)

// SchemaAudit 模型变更审计记录（append-only）：分组 / 模型 / 字段 / 关系类型的增删改。
type SchemaAudit struct {
	ID         string
	TargetType string
	TargetCode string
	TargetName string
	// ModelCode 所属模型（模型与字段有值；关系类型为源端模型，可能为通配）。
	ModelCode  string
	Action     string
	Diff       map[string]any
	Operator   string
	RequestID  string
	CreateTime time.Time
}

// SchemaAuditFilter 模型变更审计条件；零值不过滤。
type SchemaAuditFilter struct {
	TargetType string
	ModelCode  string
	Action     string
	Operator   string
	// Keyword 对象编码 / 名称子串。
	Keyword string
	From    *time.Time
	To      *time.Time
}

// SchemaAuditRepo 模型变更审计仓储接口。
type SchemaAuditRepo interface {
	Insert(ctx context.Context, a *SchemaAudit) error
	// List 时间倒序分页。
	List(ctx context.Context, filter SchemaAuditFilter, page, pageSize int64) ([]*SchemaAudit, int64, error)
}

type actorContextKey struct{}

// WithActor 把操作者放入上下文（service 层在写接口入口调用），供模型变更审计取用；
// 不改变既有用例方法签名。
func WithActor(ctx context.Context, actor Actor) context.Context {
	return context.WithValue(ctx, actorContextKey{}, actor)
}

// ActorFromContext 取上下文中的操作者；未设置返回零值（审计记录的操作者为空）。
func ActorFromContext(ctx context.Context) Actor {
	actor, _ := ctx.Value(actorContextKey{}).(Actor)
	return actor
}

// recordSchemaAudit 追加一条模型变更审计。审计是事后旁路记录：元数据写入已提交，
// 审计写失败只记日志、不回滚也不向调用方报错（避免"已生效却报失败"）。
func recordSchemaAudit(ctx context.Context, audits SchemaAuditRepo, targetType, code, name, modelCode, action string, before, after map[string]any) {
	if audits == nil {
		return
	}
	id, err := idgen.NewID()
	if err != nil {
		slog.WarnContext(ctx, "cmdb schema audit: generate id", "error", err)
		return
	}
	actor := ActorFromContext(ctx)
	diff := map[string]any{}
	if before != nil {
		diff["before"] = before
	}
	if after != nil {
		diff["after"] = after
	}
	entry := &SchemaAudit{
		ID: id, TargetType: targetType, TargetCode: code, TargetName: name, ModelCode: modelCode,
		Action: action, Diff: diff, Operator: actor.UserID, RequestID: actor.RequestID, CreateTime: time.Now(),
	}
	// WithoutCancel：元数据已落库，审计不随请求取消而丢失。
	if err := audits.Insert(context.WithoutCancel(ctx), entry); err != nil {
		slog.WarnContext(ctx, "cmdb schema audit: insert", "target_type", targetType, "target_code", code, "action", action, "error", err)
	}
}

// diffAuditMaps 取两份快照中发生变化的键（before/after 各含变化键的旧值/新值）。
func diffAuditMaps(before, after map[string]any) (b, a map[string]any) {
	b, a = map[string]any{}, map[string]any{}
	for key, old := range before {
		if next, ok := after[key]; !ok || !reflect.DeepEqual(old, next) {
			b[key], a[key] = old, after[key]
		}
	}
	for key, next := range after {
		if _, ok := before[key]; !ok {
			b[key], a[key] = nil, next
		}
	}
	return b, a
}

func groupAuditMap(g *ModelGroup) map[string]any {
	return map[string]any{"name": g.Name, "sort": g.Sort}
}

func modelAuditMap(m *Model) map[string]any {
	return map[string]any{
		"name": m.Name, "category": m.Category, "icon": m.Icon, "description": m.Description, "status": m.Status,
	}
}

func fieldAuditMap(f *Field) map[string]any {
	rule := map[string]any{}
	if f.Rule.Pattern != nil {
		rule["pattern"] = *f.Rule.Pattern
	}
	if f.Rule.Min != nil {
		rule["min"] = *f.Rule.Min
	}
	if f.Rule.Max != nil {
		rule["max"] = *f.Rule.Max
	}
	if f.Rule.MinLen != nil {
		rule["minLen"] = *f.Rule.MinLen
	}
	if f.Rule.MaxLen != nil {
		rule["maxLen"] = *f.Rule.MaxLen
	}
	return map[string]any{
		"name": f.Name, "type": f.Type, "required": f.Required, "unique": f.Unique,
		"enumValues": slices.Clone(f.EnumValues), "refModel": f.RefModel, "rule": rule, "owner": f.Owner,
		"listVisible": f.ListVisible, "sort": f.Sort, "description": f.Description,
		"indexed": f.Indexed, "sensitive": f.Sensitive, "deprecated": f.Deprecated,
	}
}

func relationTypeAuditMap(t *RelationType) map[string]any {
	return map[string]any{
		"name": t.Name, "forwardName": t.ForwardName, "backwardName": t.BackwardName,
		"srcModel": t.SrcModel, "dstModel": t.DstModel, "cardinality": t.Cardinality,
		"onDelete": t.OnDelete, "description": t.Description,
	}
}
