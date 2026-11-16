package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// maxSafeInt float64 能精确表示的最大整数（2^53）。
const maxSafeInt = 1 << 53

// CreateParams 提单入参。
type CreateParams struct {
	TemplateID  string
	Title       string
	Priority    int32
	Data        map[string]any
	Creator     string
	CreatorDept string
	FileIDs     []string
	RequestID   string
	// ResourceIDs 关联的 CMDB 资源。
	ResourceIDs []string
}

// Access 工单的查看权限判定：提单人、流程参与者，或持有管理视图权限。独立成类型供
// 工单用例与附件访问检查共用——文件域依赖访问检查、工单用例依赖文件域，拆开才不成环。
type Access struct {
	reader       TicketReader
	participants bizworkflow.ParticipantChecker
	supervisor   Supervisor
}

// NewAccess new an Access.
func NewAccess(reader TicketReader, participants bizworkflow.ParticipantChecker, supervisor Supervisor) *Access {
	return &Access{reader: reader, participants: participants, supervisor: supervisor}
}

// canView 判定用户能否查看工单。
func (a *Access) canView(ctx context.Context, t *Ticket, userID string) (bool, error) {
	if t.Creator == userID {
		return true, nil
	}
	if t.InstanceID != "" {
		ok, err := a.participants.IsParticipant(ctx, t.InstanceID, userID)
		if err != nil || ok {
			return ok, err
		}
	}
	return a.supervisor.CanViewAll(ctx, userID)
}

// visible 取工单并校验查看权限；不存在返回 ErrTicketNotFound，无权返回 ErrTicketForbidden。
func (a *Access) visible(ctx context.Context, id, userID string) (*Ticket, error) {
	t, err := a.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	ok, err := a.canView(ctx, t, userID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrTicketForbidden
	}
	return t, nil
}

// CanReadFile implements bizfile.AccessChecker：工单附件的读取权限 = 工单的查看权限。
func (a *Access) CanReadFile(ctx context.Context, userID, bizID string) (bool, error) {
	t, err := a.reader.Get(ctx, bizID)
	if err != nil {
		if errors.Is(err, ErrTicketNotFound) {
			return false, nil
		}
		return false, err
	}
	return a.canView(ctx, t, userID)
}

// CanViewInstance implements bizworkflow.Viewer：工单流程的查看权限 = 工单的查看权限
// （管理视图的持有者不是流程参与者，也能看流程进度）。
func (a *Access) CanViewInstance(ctx context.Context, userID, businessID string) (bool, error) {
	return a.CanReadFile(ctx, userID, businessID)
}

// Usecase 工单用例。
type Usecase struct {
	catalog  CatalogReader
	store    TicketStore
	reader   TicketReader
	comments CommentRepo
	workflow Workflow
	files    Attachments
	dir      Directory
	access   *Access
	txs      txrun.Manager
	ext      Extensions

	// statsMu 保护 statsCache（统计结果的短时缓存，见 Stats）。
	statsMu    sync.Mutex
	statsCache map[statsKey]statsEntry

	// slaMu 保护 slaParked（SLA 通知处理失败、暂缓重试的工单 → 可再试的时刻，见 NotifySLA）。
	slaMu     sync.Mutex
	slaParked map[string]time.Time
}

// Extensions 工单增强能力（SLA、字段权限、关联资源、评价与统计）的依赖：合成一组，
// 免得构造函数的参数无限变长。
type Extensions struct {
	Ops       TicketOps
	Insight   TicketInsight
	Flows     WorkflowForms
	Deadlines Deadlines
	Resources Resources
	Notify    biznotify.NotificationWriter
	Log       *slog.Logger
}

// NewExtensions 组装工单增强能力的依赖。
func NewExtensions(ops TicketOps, insight TicketInsight, flows WorkflowForms, deadlines Deadlines, resources Resources,
	notify biznotify.NotificationWriter, log *slog.Logger,
) Extensions {
	return Extensions{Ops: ops, Insight: insight, Flows: flows, Deadlines: deadlines, Resources: resources, Notify: notify, Log: log}
}

// NewUsecase new a Usecase.
func NewUsecase(catalog CatalogReader, store TicketStore, reader TicketReader, comments CommentRepo, workflow Workflow,
	files Attachments, dir Directory, access *Access, txs txrun.Manager, ext Extensions,
) *Usecase {
	return &Usecase{
		catalog: catalog, store: store, reader: reader, comments: comments, workflow: workflow,
		files: files, dir: dir, access: access, txs: txs, ext: ext,
	}
}

// schemaFields 表单字段定义 → 通用字段校验定义。
func schemaFields(form []FormField) []formschema.Field {
	out := make([]formschema.Field, len(form))
	for i, f := range form {
		out[i] = formschema.Field{Code: f.Code, Type: f.Type, Required: f.Required, EnumValues: f.EnumValues, Rule: f.Rule}
	}
	return out
}

// validateData 按表单定义校验并归一化表单数据。
func validateData(form []FormField, data map[string]any) (map[string]any, error) {
	if err := checkRaw(form, data); err != nil {
		return nil, err
	}
	out, err := formschema.Validate(schemaFields(form), data, false)
	if err != nil {
		var fe *formschema.Error
		if errors.As(err, &fe) {
			return nil, detail(ErrFormInvalid, fe.Error())
		}
		return nil, err
	}
	return out, nil
}

// checkRaw 表单数据在按字段校验之前的通用约束：大小、不含 NUL 字符、整数不超出可精确表示
// 的范围。提单与处理人改表单共用。
func checkRaw(form []FormField, data map[string]any) error {
	raw, err := json.Marshal(data)
	if err != nil || len(raw) > maxDataBytes {
		return detail(ErrFormInvalid, "form data is too large")
	}
	// jsonb 不接受 \u0000：在这里拒绝，而不是让它在落库时报内部错误。
	if bytes.Contains(raw, []byte(`\u0000`)) {
		return detail(ErrFormInvalid, "form data must not contain NUL characters")
	}
	// 整数经 JSON 以 float64 传入，超出 2^53 已不精确，转成 int64 的结果也不确定：直接拒绝。
	for _, f := range form {
		if n, ok := data[f.Code].(float64); ok && f.Type == formschema.TypeInt && math.Abs(n) > maxSafeInt {
			return detail(ErrFormInvalid, fmt.Sprintf("field %q is out of range", f.Code))
		}
	}
	return nil
}

// empty 报告表单值是否算「没填」：缺失、null、空串、空列表、空对象。
func empty(v any) bool {
	switch val := v.(type) {
	case nil:
		return true
	case string:
		return val == ""
	case []any:
		return len(val) == 0
	case map[string]any:
		return len(val) == 0
	}
	return false
}

// Create 提交工单：一个事务内完成「校验表单 → 取编号 → 落工单 → 绑定附件 → 发起流程」，
// 任一步失败整体回滚（不会出现有工单没流程、或有流程没工单）。
func (uc *Usecase) Create(ctx context.Context, p CreateParams) (*Ticket, error) {
	if p.Title = clean(p.Title); p.Title == "" {
		return nil, detail(ErrFormInvalid, "title must not be blank")
	}
	ticket, err := uc.create(ctx, p)
	if errors.Is(err, ErrDuplicateRequest) {
		// 同一幂等键的并发提交抢先落库：唯一冲突只在对方提交之后才会报出，此时回读必然读到。
		err = uc.txs.WithinTx(ctx, func(tx txrun.Tx) error {
			var ferr error
			ticket, ferr = uc.store.FindByRequestTx(ctx, tx, p.Creator, p.RequestID)
			return ferr
		})
		if err == nil && ticket == nil {
			err = ErrTicketNotFound
		}
	}
	if err != nil {
		return nil, err
	}
	return ticket, nil
}

// create 在一个事务里完成提单。
//
// 关联资源的解析（CMDB 的权限判定与逐个读取）放在事务之外：它们走连接池里的另一条连接，
// 放在事务里就是「占着一条连接再等一条」——并发提单数达到连接池上限时会互相等死，连带
// 拖垮同进程的其它接口。解析只读、结果是名称快照（不建外键），早几毫秒做没有一致性代价；
// 模板的关联要求在事务里按当时的模板再核对一次。
func (uc *Usecase) create(ctx context.Context, p CreateParams) (*Ticket, error) {
	// 幂等重放先于资源解析判定：首次提单已成功、重试时资源已被删除或提单人已失去其权限，
	// 也应拿回原来那张工单，而不是「资源不可访问」。
	if p.RequestID != "" && len(p.ResourceIDs) > 0 {
		var existing *Ticket
		if err := uc.txs.WithinTx(ctx, func(tx txrun.Tx) (ferr error) {
			existing, ferr = uc.store.FindByRequestTx(ctx, tx, p.Creator, p.RequestID)
			return ferr
		}); err != nil || existing != nil {
			return existing, err
		}
	}
	resources, err := uc.prepareResources(ctx, p)
	if err != nil {
		return nil, err
	}
	var ticket *Ticket
	err = uc.txs.WithinTx(ctx, func(tx txrun.Tx) error {
		// 事务内的只读查询（模板等）复用事务连接。
		ctx := txrun.NewContext(ctx, tx)
		if p.RequestID != "" {
			existing, err := uc.store.FindByRequestTx(ctx, tx, p.Creator, p.RequestID)
			if err != nil || existing != nil {
				ticket = existing
				return err
			}
		}
		tpl, err := uc.catalog.GetTemplate(ctx, p.TemplateID)
		if err != nil {
			return err
		}
		if tpl.Status != TemplateEnabled {
			return ErrTemplateUnavailable
		}
		data, err := validateData(tpl.Form, p.Data)
		if err != nil {
			return err
		}
		if err := checkResourceLink(tpl, p.ResourceIDs); err != nil {
			return err
		}
		id, err := idgen.NewID()
		if err != nil {
			return err
		}
		// SLA 时限先于取号算好：取号之后同一前缀当天的提单都在序号行上排队，锁内只留必须的写入。
		sla := &Ticket{Priority: p.Priority, CreateTime: time.Now().UTC()}
		if err := uc.applySLA(ctx, tpl, sla); err != nil {
			return err
		}
		day, seq, err := uc.store.NextNumberTx(ctx, tx, tpl.NumberPrefix)
		if err != nil {
			return err
		}
		ticket = &Ticket{
			ID:         id,
			Number:     fmt.Sprintf("%s-%s-%04d", tpl.NumberPrefix, day, seq),
			TemplateID: tpl.ID, TemplateName: tpl.Name, CategoryName: tpl.CategoryName,
			Title: p.Title, Priority: p.Priority, Form: tpl.Form, Data: data,
			Status: StatusProcessing, Creator: p.Creator, CreateTime: sla.CreateTime,
			ResponseDue: sla.ResponseDue, ResolveDue: sla.ResolveDue, SLAEscalate: sla.SLAEscalate,
		}
		if err := uc.store.CreateTx(ctx, tx, ticket, p.RequestID); err != nil {
			return err
		}
		if err := uc.ext.Ops.LinkResourcesTx(ctx, tx, id, resources); err != nil {
			return err
		}
		if err := uc.files.Bind(ctx, tx, p.Creator, BusinessType, id, p.FileIDs); err != nil {
			if errors.Is(err, bizfile.ErrFileNotFound) {
				return detail(ErrFormInvalid, "an attachment does not exist or was not uploaded by you")
			}
			return err
		}
		// 流程变量 = 表单数据 + 优先级（条件表达式以 form.<字段码> / form.priority 引用）。
		vars := maps.Clone(data)
		vars[priorityVar] = p.Priority
		in, err := uc.workflow.StartTx(ctx, tx, bizworkflow.StartParams{
			DefinitionKey: tpl.WorkflowKey, BusinessType: BusinessType, BusinessID: id,
			// 流程节点上按模板区分的字段权限据此取用。
			FormKey: tpl.ID,
			Title:   ticket.Title, Initiator: p.Creator, InitiatorDept: p.CreatorDept, Variables: vars,
		})
		if err != nil {
			// 模板绑定的流程此后被停用 / 删除：对提单人而言就是这项服务暂不可用。
			if errors.Is(err, bizworkflow.ErrDefinitionUnavailable) || errors.Is(err, bizworkflow.ErrDefinitionNotFound) {
				return detail(ErrTemplateUnavailable, "the workflow bound to this template is unavailable")
			}
			return err
		}
		// 流程可能在发起时即已结束（全部节点被跳过）：状态已由监听器同步，这里只补实例 ID。
		ticket.InstanceID, ticket.CreatorDept, ticket.Status = in.ID, in.InitiatorDept, in.Status
		ticket.EndTime = in.EndTime
		return uc.store.SyncTx(ctx, tx, id, FlowSync{InstanceID: in.ID, CreatorDept: in.InitiatorDept, Status: in.Status, EndTime: in.EndTime})
	})
	if err != nil {
		return nil, err
	}
	return ticket, nil
}

// Detail 工单详情。
type Detail struct {
	Ticket *Ticket
	Files  []*bizfile.File
	// Labels 表单里人员 / 部门字段取值、以及提单人的显示名（ID → 名称）。
	Labels map[string]string
	// Resources 关联的 CMDB 资源。
	Resources []Resource
	// Permissions 查看者作为当前处理人时各字段的权限；CanEditForm 他此刻能否修改表单；
	// CanRate 他此刻能否评价。
	Permissions map[string]string
	CanEditForm bool
	CanRate     bool
}

// Get 查询工单详情（含附件与显示名）。
func (uc *Usecase) Get(ctx context.Context, id, userID string) (*Detail, error) {
	t, err := uc.access.visible(ctx, id, userID)
	if err != nil {
		return nil, err
	}
	files, err := uc.files.ListByBiz(ctx, BusinessType, t.ID)
	if err != nil {
		return nil, err
	}
	d := &Detail{Ticket: t, Files: files}
	if err := uc.applyFormAccess(ctx, d, userID); err != nil {
		return nil, err
	}
	if d.Resources, err = uc.ext.Insight.ListResources(ctx, t.ID); err != nil {
		return nil, err
	}
	d.CanRate = t.Creator == userID && t.Rating == 0 && (t.Status == StatusCompleted || t.Status == StatusRejected)
	users, depts := []string{t.Creator}, []string(nil)
	for _, f := range t.Form {
		v, _ := t.Data[f.Code].(string)
		switch {
		case v == "":
		case f.Type == formschema.TypeUser:
			users = append(users, v)
		case f.Type == formschema.TypeDept:
			depts = append(depts, v)
		}
	}
	labels, err := uc.dir.UserNames(ctx, users)
	if err != nil {
		return nil, err
	}
	if len(depts) > 0 {
		names, err := uc.dir.DeptNames(ctx, depts)
		if err != nil {
			return nil, err
		}
		maps.Copy(labels, names)
	}
	d.Labels = labels
	return d, nil
}

// List 分页查询工单并取提单人显示名。filter.Creator 非空即「我的工单」。
func (uc *Usecase) List(ctx context.Context, filter TicketFilter, page, pageSize int64) ([]*Ticket, map[string]string, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = clean(filter.Keyword)
	items, total, err := uc.reader.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, nil, 0, err
	}
	ids := make([]string, 0, len(items))
	for _, t := range items {
		ids = append(ids, t.Creator)
	}
	names, err := uc.dir.UserNames(ctx, ids)
	return items, names, total, err
}

// Comments 工单评论（须可查看该工单）。
func (uc *Usecase) Comments(ctx context.Context, id, userID string) ([]*Comment, map[string]string, error) {
	if _, err := uc.access.visible(ctx, id, userID); err != nil {
		return nil, nil, err
	}
	comments, err := uc.comments.List(ctx, id, maxComments)
	if err != nil {
		return nil, nil, err
	}
	ids := make([]string, 0, len(comments))
	for _, c := range comments {
		ids = append(ids, c.UserID)
	}
	names, err := uc.dir.UserNames(ctx, ids)
	return comments, names, err
}

// AddComment 发表评论（可查看工单的人都可以评论）；parentID 非空即回复该条顶层评论。
func (uc *Usecase) AddComment(ctx context.Context, id, userID, content, parentID string) (*Comment, string, error) {
	if _, err := uc.access.visible(ctx, id, userID); err != nil {
		return nil, "", err
	}
	if content = clean(content); content == "" {
		return nil, "", detail(ErrFormInvalid, "comment must not be empty")
	}
	cid, err := idgen.NewID()
	if err != nil {
		return nil, "", err
	}
	c := &Comment{ID: cid, TicketID: id, UserID: userID, Content: content, CreateTime: time.Now().UTC(), ParentID: parentID}
	if err := uc.comments.Add(ctx, c, maxComments, maxCommentsPerMinute); err != nil {
		return nil, "", err
	}
	names, err := uc.dir.UserNames(ctx, []string{userID})
	return c, names[userID], err
}

// StatusSyncer 流程事件监听器：实例结束时把终态同步到工单（在引擎的同一事务内）。
// 独立于 Usecase：流程引擎依赖监听器、工单用例依赖流程引擎，拆开才不成环。
type StatusSyncer struct {
	store TicketStore
	ops   TicketOps
}

// NewStatusSyncer new a StatusSyncer.
func NewStatusSyncer(store TicketStore, ops TicketOps) *StatusSyncer {
	return &StatusSyncer{store: store, ops: ops}
}

// OnInstanceProgress implements bizworkflow.ProgressListener：处理人第一次有实质动作即「已响应」。
// in 可能是工单流程的子流程：工单一律取根流程所挂的业务对象。
func (s *StatusSyncer) OnInstanceProgress(ctx context.Context, tx txrun.Tx, in *bizworkflow.Instance, at time.Time) error {
	return s.ops.MarkResponseTx(ctx, tx, in.RootBusinessID, at)
}

// BeforeApprove implements bizworkflow.Guard：本环节标为必填的字段没填，不允许同意。
func (s *StatusSyncer) BeforeApprove(ctx context.Context, tx txrun.Tx, in *bizworkflow.Instance, n *bizworkflow.Node, _ string) error {
	var required []string
	// 按这张工单的模板取节点的字段权限（in 是子流程时 FormKey 继承自根流程）。
	for field, perm := range n.PermissionsFor(in.FormKey) {
		if perm == permRequired {
			required = append(required, field)
		}
	}
	if len(required) == 0 {
		return nil
	}
	// in 可能是工单流程的子流程：工单一律取根流程所挂的业务对象。
	form, data, err := s.ops.LockFormTx(ctx, tx, in.RootBusinessID)
	if err != nil {
		return err
	}
	slices.Sort(required)
	for _, code := range required {
		// 流程节点上配置的字段码不在这张工单的表单里（同一流程绑了不同的模板、或模板后来
		// 删了字段）：处理人无从填写，不拦——否则这个任务永远同意不了。
		i := slices.IndexFunc(form, func(f FormField) bool { return f.Code == code })
		if i < 0 || !empty(data[code]) {
			continue
		}
		return detail(ErrFormInvalid, fmt.Sprintf("field %q must be filled in before approving", form[i].Name))
	}
	return nil
}

// OwnedVariables implements bizworkflow.VariableOwner：工单的表单字段与优先级只能经工单自己的
// 入口修改（UpdateForm 同事务更新工单数据与流程变量）；信号、自动任务的返回不得改写它们，
// 否则流程按新值流转、工单上显示的却还是旧值。
func (s *StatusSyncer) OwnedVariables(ctx context.Context, tx txrun.Tx, in *bizworkflow.Instance) ([]string, error) {
	form, _, err := s.ops.LockFormTx(ctx, tx, in.RootBusinessID)
	if errors.Is(err, ErrTicketNotFound) {
		return nil, nil // 工单已不在（不应发生）：没有可保护的字段
	}
	if err != nil {
		return nil, err
	}
	owned := make([]string, 0, len(form)+1)
	owned = append(owned, priorityVar)
	for _, f := range form {
		owned = append(owned, f.Code)
	}
	return owned, nil
}

// CurrentVariables implements bizworkflow.VariableOwner：表单字段此刻在工单上的取值（优先级
// 提单后不变，不必校准）。处理人在流程树的某个实例上改了表单，其它实例据此跟上。
func (s *StatusSyncer) CurrentVariables(ctx context.Context, tx txrun.Tx, in *bizworkflow.Instance) (map[string]any, error) {
	form, data, err := s.ops.LockFormTx(ctx, tx, in.RootBusinessID)
	if errors.Is(err, ErrTicketNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	current := make(map[string]any, len(form))
	for _, f := range form {
		current[f.Code] = data[f.Code]
	}
	return current, nil
}

// OnInstanceFinished implements bizworkflow.Listener.
func (s *StatusSyncer) OnInstanceFinished(ctx context.Context, tx txrun.Tx, in *bizworkflow.Instance) error {
	return s.store.SyncTx(ctx, tx, in.BusinessID, FlowSync{InstanceID: in.ID, Status: in.Status, EndTime: in.EndTime})
}
