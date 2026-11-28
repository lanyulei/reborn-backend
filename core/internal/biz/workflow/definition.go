package biz

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

// 流程定义状态。
const (
	DefinitionDisabled int32 = 0
	DefinitionEnabled  int32 = 1
)

const (
	// maxDraftBytes 草稿流程图 JSON 的大小上限。
	maxDraftBytes = 256 << 10
	// maxVersionsListed 版本列表返回上限（版本号倒序）。
	maxVersionsListed = 100
	// graphCacheSize 已发布版本流程图的进程内缓存容量。版本不可变，缓存只为省去
	// 重复解析与表达式编译，正确性不依赖它（各实例各自缓存，无需一致）。
	graphCacheSize = 256
)

// BuiltinKeyPrefix 内置流程定义的标识前缀（随版本预置，如 builtin_approval_admin）。保留给系统：
// 用户自建流程的标识不得以它开头，内置流程按标识幂等预置时才不会撞上用户的数据。
const BuiltinKeyPrefix = "builtin_"

// 哨兵错误：reason 来自 api/workflow/v1 错误码枚举。
var (
	ErrDefinitionNotFound    = errors.NotFound(v1.ErrorReason_WORKFLOW_DEFINITION_NOT_FOUND.String(), "workflow definition not found")
	ErrDefinitionKeyExists   = errors.Conflict(v1.ErrorReason_WORKFLOW_DEFINITION_KEY_EXISTS.String(), "workflow definition key already exists")
	ErrDefinitionInvalid     = errors.BadRequest(v1.ErrorReason_WORKFLOW_DEFINITION_INVALID.String(), "workflow definition is invalid")
	ErrDefinitionUnavailable = errors.Conflict(v1.ErrorReason_WORKFLOW_DEFINITION_UNAVAILABLE.String(), "workflow definition is not published or is disabled")
	ErrDefinitionConflict    = errors.Conflict(v1.ErrorReason_WORKFLOW_DEFINITION_CONFLICT.String(), "workflow definition was modified by someone else")
	ErrDefinitionInUse       = errors.Conflict(v1.ErrorReason_WORKFLOW_DEFINITION_IN_USE.String(), "workflow definition still has running instances")
	// ErrDefinitionBuiltin 内置流程定义不能删除（可以修改、停用）：后续版本按固定 ID 升级内置内容。
	ErrDefinitionBuiltin = errors.Conflict(v1.ErrorReason_WORKFLOW_DEFINITION_BUILTIN.String(), "built-in workflow definitions cannot be deleted; disable it instead")
	// ErrVersionInvalid 已发布版本的流程图通不过当前的校验规则（规则收紧后的存量版本，或数据
	// 被改动）：依赖流程图的动作一律拒绝。是确定性的错误（重试也不会成功），按冲突返回——
	// 作业执行器据此直接转死信，而不是无休止地重试。撤回 / 终止不依赖流程图，不受影响。
	ErrVersionInvalid = errors.Conflict(v1.ErrorReason_WORKFLOW_DEFINITION_INVALID.String(), "the published workflow version no longer passes validation; publish a new version")
)

// emptyDraft 新建流程的骨架草稿：只含开始 / 结束节点。
const emptyDraft = `{"nodes":[{"id":"start","type":"start","name":"发起"},{"id":"end","type":"end","name":"结束"}],` +
	`"edges":[{"id":"e_start_end","source":"start","target":"end"}],"settings":{}}`

// Definition 流程定义。Draft 为草稿流程图 JSON（只读：调用方不得修改返回的切片）。
type Definition struct {
	ID                 string
	Key                string
	Name               string
	Category           string
	Description        string
	Status             int32
	Draft              []byte
	Revision           int32
	PublishedVersion   int32
	PublishedVersionID string
	CreateTime         time.Time
	UpdateTime         time.Time
	// Builtin 随版本预置的内置流程：可以修改、停用，不能删除。
	Builtin bool
}

// DefinitionVersion 已发布版本（不可变）。
type DefinitionVersion struct {
	ID           string
	DefinitionID string
	Version      int32
	Graph        []byte
	CreateUser   string
	CreateTime   time.Time
}

// DefinitionFilter 列表过滤条件；指针为 nil 表示不过滤。
type DefinitionFilter struct {
	Keyword   string
	Status    *int32
	Published *bool
}

// DefinitionUpdate 部分更新；nil 字段不更新。
type DefinitionUpdate struct {
	Name        *string
	Category    *string
	Description *string
	Status      *int32
}

// DefinitionReader 流程定义只读仓储接口。
type DefinitionReader interface {
	// GetDefinition 按 ID 查询未删除的定义；不存在返回 ErrDefinitionNotFound。
	GetDefinition(ctx context.Context, id string) (*Definition, error)
	// GetDefinitionByKey 按标识查询未删除的定义（不含草稿内容：发起流程等运行期路径用，草稿
	// 可达数百 KB）；不存在返回 ErrDefinitionNotFound。
	GetDefinitionByKey(ctx context.Context, key string) (*Definition, error)
	// KeyReferences 引擎内部仍引用着流程标识 key 的对象的简短描述：其他流程（excludeID 之外、
	// 未删除）当前发布版本里的子流程节点、开放接口接入凭据的允许发起范围。没有返回空。
	KeyReferences(ctx context.Context, key, excludeID string) ([]string, error)
	// ListDefinitions 分页查询（不含草稿内容），更新时间倒序。
	ListDefinitions(ctx context.Context, filter DefinitionFilter, page, pageSize int64) ([]*Definition, int64, error)
}

// DefinitionWriter 流程定义写仓储接口。
type DefinitionWriter interface {
	// CreateDefinition 创建定义；标识冲突返回 ErrDefinitionKeyExists。
	CreateDefinition(ctx context.Context, d *Definition) error
	// UpdateDefinition 部分更新；不存在返回 ErrDefinitionNotFound。
	UpdateDefinition(ctx context.Context, id string, upd DefinitionUpdate) error
	// SaveDraft 以 revision 做乐观锁保存草稿，返回新的修订号；不存在返回
	// ErrDefinitionNotFound，修订号不符返回 ErrDefinitionConflict。
	SaveDraft(ctx context.Context, id string, draft []byte, revision int32) (int32, error)
}

// DefinitionPublisher 发布与删除（均需与并发写互斥，实现内加行锁）。
type DefinitionPublisher interface {
	// Publish 把当前草稿发布为新版本（版本号 = 已发布版本号 + 1）：仅当草稿修订号仍为
	// revision 时生效，否则返回 ErrDefinitionConflict。graph 为调用方已校验的流程图。
	Publish(ctx context.Context, id string, revision int32, version *DefinitionVersion) error
	// DeleteDefinition 软删定义；仍有进行中的实例返回 ErrDefinitionInUse。
	DeleteDefinition(ctx context.Context, id string) error
}

// VersionReader 已发布版本只读仓储接口。
type VersionReader interface {
	// GetVersion 按版本 ID 查询；不存在返回 ErrDefinitionNotFound。
	GetVersion(ctx context.Context, versionID string) (*DefinitionVersion, error)
	// GetVersionByNumber 按定义 + 版本号查询；不存在返回 ErrDefinitionNotFound。
	GetVersionByNumber(ctx context.Context, definitionID string, version int32) (*DefinitionVersion, error)
	// ListVersions 版本列表（不含流程图），版本号倒序，至多 limit 个。
	ListVersions(ctx context.Context, definitionID string, limit int) ([]*DefinitionVersion, error)
}

// DefinitionReferrer 业务域对流程定义的引用：流程被停用 / 删除之前，引擎逐个询问登记的
// 业务域「是否还在用这个流程标识」（如工单模板绑定了它）。由上层业务域实现、在装配期登记
// （依赖倒置：引擎不 import 业务域）。
type DefinitionReferrer interface {
	// DefinitionReferences 仍引用着流程标识 key 的业务对象的简短描述（如 `ticket template "故障申报"`），
	// 至多几条即可；没有返回空。
	DefinitionReferences(ctx context.Context, key string) ([]string, error)
}

// DefinitionReferrers 登记的业务域引用方（装配期构造，只读）。
type DefinitionReferrers []DefinitionReferrer

// cachedGraph 一个已发布版本的可执行流程图及其原始 JSON（版本不可变，两者都不会过期）。
type cachedGraph struct {
	graph *Graph
	raw   json.RawMessage
}

// DefinitionUsecase 流程定义用例。
type DefinitionUsecase struct {
	reader    DefinitionReader
	writer    DefinitionWriter
	publisher DefinitionPublisher
	versions  VersionReader
	webhooks  WebhookChecker
	referrers DefinitionReferrers

	mu     sync.Mutex
	graphs map[string]cachedGraph // 版本 ID → 已校验的流程图（不可变）
}

// NewDefinitionUsecase new a DefinitionUsecase.
func NewDefinitionUsecase(reader DefinitionReader, writer DefinitionWriter, publisher DefinitionPublisher, versions VersionReader,
	webhooks WebhookChecker, referrers DefinitionReferrers,
) *DefinitionUsecase {
	return &DefinitionUsecase{
		reader: reader, writer: writer, publisher: publisher, versions: versions, webhooks: webhooks, referrers: referrers,
		graphs: make(map[string]cachedGraph),
	}
}

// maxReferencesShown 「仍被引用」的报错里列出的引用方条数上限。
const maxReferencesShown = 5

// checkUnreferenced 流程标识仍被引用时返回 ErrDefinitionInUse（说明里带上引用方）：停用 / 删除
// 一个还被工单模板、其他流程的子流程节点或开放接口凭据用着的流程，会让它们在用到的那一刻
// 才报错（提单人填完表单才被拒绝、父流程停在子流程节点上）。先解除引用再停用 / 删除。
func (uc *DefinitionUsecase) checkUnreferenced(ctx context.Context, d *Definition) error {
	refs, err := uc.reader.KeyReferences(ctx, d.Key, d.ID)
	if err != nil {
		return err
	}
	for _, r := range uc.referrers {
		more, err := r.DefinitionReferences(ctx, d.Key)
		if err != nil {
			return err
		}
		refs = append(refs, more...)
	}
	if len(refs) == 0 {
		return nil
	}
	shown := refs[:min(len(refs), maxReferencesShown)]
	return errors.Conflict(ErrDefinitionInUse.Reason, "workflow definition is still referenced by: "+strings.Join(shown, ", "))
}

// referenceIssues 校验流程图引用的外部对象：自动任务的 Webhook 端点须存在且启用；子流程须
// 存在、不是本流程自身。只在流程图自身已无问题时才检查（调用方保证）。是否「已发布」
// 不在此校验——被引用的流程可以稍后再发布，届时发起不了会在运行期明确报错。
func (uc *DefinitionUsecase) referenceIssues(ctx context.Context, selfKey string, g *Graph) ([]Issue, error) {
	var (
		issues []Issue
		hooks  []string
	)
	for i := range g.Nodes {
		n := &g.Nodes[i]
		switch n.Type {
		case NodeService:
			hooks = append(hooks, n.Service.WebhookID)
		case NodeSubProcess:
			key := n.SubProcess.DefinitionKey
			if key == selfKey {
				issues = append(issues, Issue{NodeID: n.ID, Code: "SUBPROCESS_SELF", Message: "a workflow cannot call itself as a sub-process"})
				continue
			}
			cyclic, err := uc.reaches(ctx, key, selfKey, maxSubProcessDepth)
			switch {
			case errors.Is(err, ErrDefinitionNotFound):
				issues = append(issues, Issue{NodeID: n.ID, Code: "SUBPROCESS_MISSING", Message: fmt.Sprintf("sub-process workflow %q does not exist", key)})
			case err != nil:
				return nil, err
			case cyclic:
				issues = append(issues, Issue{NodeID: n.ID, Code: "SUBPROCESS_CYCLE", Message: fmt.Sprintf("sub-process workflow %q calls back into this workflow", key)})
			}
		}
	}
	if len(hooks) == 0 || uc.webhooks == nil {
		return issues, nil
	}
	usable, err := uc.webhooks.UsableWebhooks(ctx, hooks)
	if err != nil {
		return nil, err
	}
	for i := range g.Nodes {
		if n := &g.Nodes[i]; n.Type == NodeService && !usable[n.Service.WebhookID] {
			issues = append(issues, Issue{NodeID: n.ID, Code: "SERVICE_WEBHOOK_MISSING", Message: "the webhook endpoint does not exist or is disabled"})
		}
	}
	return issues, nil
}

// reaches 报告流程 key（按其当前发布的版本）是否经子流程调用链回到 target；key 不存在返回
// ErrDefinitionNotFound。只沿已发布的版本走、深度有界——运行期另有嵌套深度上限兜底。
func (uc *DefinitionUsecase) reaches(ctx context.Context, key, target string, depth int) (bool, error) {
	d, err := uc.reader.GetDefinitionByKey(ctx, key)
	if err != nil {
		return false, err
	}
	if depth <= 0 || d.PublishedVersionID == "" {
		return false, nil
	}
	g, err := uc.graph(ctx, d.PublishedVersionID)
	if err != nil {
		return false, nil // 存量版本已不合法：运行期自会报错，这里不拦发布
	}
	for i := range g.Nodes {
		n := &g.Nodes[i]
		if n.Type != NodeSubProcess {
			continue
		}
		if n.SubProcess.DefinitionKey == target {
			return true, nil
		}
		if found, err := uc.reaches(ctx, n.SubProcess.DefinitionKey, target, depth-1); err == nil && found {
			return true, nil
		}
	}
	return false, nil
}

// validate 解析并完整校验一份流程图（保存草稿、发布、模拟运行与详情页的问题清单都走这里）：结构与
// 配置（issuesOf）通过后，再查结构之外的问题——「转交流程管理员」却没有流程管理员，以及引用的外部
// 对象。已发布版本的加载不走这里（见 cached）：那里只跑 issuesOf，规则收紧不让存量版本失效。
func (uc *DefinitionUsecase) validate(ctx context.Context, selfKey string, raw []byte) (*Graph, []Issue, error) {
	g, issues, err := issuesOf(raw)
	if err != nil || len(issues) > 0 {
		return g, issues, err
	}
	refs, err := uc.referenceIssues(ctx, selfKey, g)
	return g, append(adminIssues(g), refs...), err
}

// adminIssues 人工任务选了「转交流程管理员」、流程里却没有流程管理员：处理人为空时没有可转的人，
// 流程会被拦住（见 blockIfEmpty）。这样的草稿不让发布，在问题清单里指出来；补上管理员，或把该节点
// 改成别的处置，再发布。g 须已通过结构校验。
func adminIssues(g *Graph) []Issue {
	var issues []Issue
	for i := range g.Nodes {
		n := &g.Nodes[i]
		if n.Type == NodeUserTask && n.Assignee.OnEmpty == OnEmptyAdmin && len(g.Settings.Admins) == 0 {
			issues = append(issues, Issue{NodeID: n.ID, Code: "ADMINS_REQUIRED", Message: fmt.Sprintf(
				"node %q hands to the workflow admins when no assignee is found, but the workflow has no admins", cmp.Or(n.Name, n.ID))})
		}
	}
	return issues
}

// Create 创建流程定义（骨架草稿、启用、未发布）。
func (uc *DefinitionUsecase) Create(ctx context.Context, d *Definition) (*Definition, error) {
	if strings.HasPrefix(d.Key, BuiltinKeyPrefix) {
		return nil, errors.BadRequest(ErrDefinitionInvalid.Reason,
			"workflow key must not start with the built-in prefix "+BuiltinKeyPrefix)
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	d.Name, d.Category, d.Description = clean(d.Name), clean(d.Category), clean(d.Description)
	if strings.TrimSpace(d.Name) == "" {
		return nil, errors.BadRequest(ErrDefinitionInvalid.Reason, "workflow name must not be empty")
	}
	d.ID, d.Status, d.Revision, d.Draft = id, DefinitionEnabled, 1, []byte(emptyDraft)
	d.CreateTime, d.UpdateTime = now, now
	if err := uc.writer.CreateDefinition(ctx, d); err != nil {
		return nil, err
	}
	return d, nil
}

// Update 部分更新并返回更新后的定义。
func (uc *DefinitionUsecase) Update(ctx context.Context, id string, upd DefinitionUpdate) (*Definition, error) {
	for _, field := range []*string{upd.Name, upd.Category, upd.Description} {
		if field != nil {
			*field = clean(*field)
		}
	}
	if upd.Name != nil && strings.TrimSpace(*upd.Name) == "" {
		return nil, errors.BadRequest(ErrDefinitionInvalid.Reason, "workflow name must not be empty")
	}
	if upd.Status != nil && *upd.Status == DefinitionDisabled {
		d, err := uc.reader.GetDefinition(ctx, id)
		if err != nil {
			return nil, err
		}
		if d.Status != DefinitionDisabled {
			if err := uc.checkUnreferenced(ctx, d); err != nil {
				return nil, err
			}
		}
	}
	if err := uc.writer.UpdateDefinition(ctx, id, upd); err != nil {
		return nil, err
	}
	return uc.reader.GetDefinition(ctx, id)
}

// issuesOf 解析并校验流程图 JSON；JSON 本身不合法返回 ErrDefinitionInvalid。
func issuesOf(raw []byte) (*Graph, []Issue, error) {
	if len(raw) > maxDraftBytes {
		// 独立的原因码：页面据此给出「流程过大」的专门提示（说明文字保持不变）。
		return nil, nil, errors.BadRequest(v1.ErrorReason_WORKFLOW_GRAPH_TOO_LARGE.String(), "workflow graph is too large")
	}
	// PostgreSQL 的 jsonb 不接受 \u0000：放进去会在落库时报错。
	if bytes.Contains(raw, []byte(`\u0000`)) {
		return nil, nil, errors.BadRequest(ErrDefinitionInvalid.Reason, "workflow graph must not contain NUL characters")
	}
	g, err := ParseGraph(raw)
	if err != nil {
		return nil, nil, errors.BadRequest(ErrDefinitionInvalid.Reason, "workflow graph is not valid JSON")
	}
	return g, g.Validate(), nil
}

// SaveDraft 保存草稿（允许未完成）：返回新的修订号与当前校验问题。
func (uc *DefinitionUsecase) SaveDraft(ctx context.Context, id string, draft []byte, revision int32) (int32, []Issue, error) {
	if _, _, err := issuesOf(draft); err != nil {
		return 0, nil, err
	}
	next, err := uc.writer.SaveDraft(ctx, id, draft, revision)
	if err != nil {
		return 0, nil, err
	}
	d, err := uc.reader.GetDefinition(ctx, id)
	if err != nil {
		return 0, nil, err
	}
	_, issues, err := uc.validate(ctx, d.Key, draft)
	if err != nil {
		return 0, nil, err
	}
	return next, issues, nil
}

// Publish 发布当前草稿：草稿须通过校验，且修订号与调用方所见一致。
func (uc *DefinitionUsecase) Publish(ctx context.Context, id string, revision int32) (*Definition, error) {
	d, err := uc.reader.GetDefinition(ctx, id)
	if err != nil {
		return nil, err
	}
	if d.Revision != revision {
		return nil, ErrDefinitionConflict
	}
	_, issues, err := uc.validate(ctx, d.Key, d.Draft)
	if err != nil {
		return nil, err
	}
	if len(issues) > 0 {
		return nil, errors.BadRequest(ErrDefinitionInvalid.Reason, "workflow definition is invalid: "+issues[0].Message)
	}
	versionID, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	// 发布的是「调用方所见、且刚校验过」的那份草稿：仓储以 revision 做条件，期间草稿
	// 被他人改动则整体不生效。
	v := &DefinitionVersion{ID: versionID, DefinitionID: id, Graph: d.Draft, CreateTime: time.Now().UTC()}
	if err := uc.publisher.Publish(ctx, id, revision, v); err != nil {
		return nil, err
	}
	return uc.reader.GetDefinition(ctx, id)
}

// Delete 软删流程定义；仍有进行中的实例、或仍被引用（工单模板、其他流程的子流程节点、
// 开放接口凭据）时拒绝。
func (uc *DefinitionUsecase) Delete(ctx context.Context, id string) error {
	d, err := uc.reader.GetDefinition(ctx, id)
	if err != nil {
		return err
	}
	if err := uc.checkUnreferenced(ctx, d); err != nil {
		return err
	}
	return uc.publisher.DeleteDefinition(ctx, id)
}

// Get 查询定义详情与草稿当前的校验问题。
func (uc *DefinitionUsecase) Get(ctx context.Context, id string) (*Definition, []Issue, error) {
	d, err := uc.reader.GetDefinition(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	_, issues, err := uc.validate(ctx, d.Key, d.Draft)
	if errors.Is(err, ErrDefinitionInvalid) {
		// 库内草稿已不可解析（不应发生）：按一条通用问题呈现，不让详情页打不开。
		issues, err = []Issue{{Code: "GRAPH_INVALID", Message: "stored draft is not a valid workflow graph"}}, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return d, issues, nil
}

// List 分页查询流程定义。
func (uc *DefinitionUsecase) List(ctx context.Context, filter DefinitionFilter, page, pageSize int64) ([]*Definition, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = clean(filter.Keyword)
	return uc.reader.ListDefinitions(ctx, filter, page, pageSize)
}

// ListVersions 已发布版本列表（不含流程图）；定义不存在返回 ErrDefinitionNotFound。
func (uc *DefinitionUsecase) ListVersions(ctx context.Context, id string) ([]*DefinitionVersion, error) {
	if _, err := uc.reader.GetDefinition(ctx, id); err != nil {
		return nil, err
	}
	return uc.versions.ListVersions(ctx, id, maxVersionsListed)
}

// GetVersion 查询某个已发布版本（含流程图）。
func (uc *DefinitionUsecase) GetVersion(ctx context.Context, id string, version int32) (*DefinitionVersion, error) {
	return uc.versions.GetVersionByNumber(ctx, id, version)
}

// KeyStartable 报告给定标识的流程定义是否存在，以及当前能否发起（启用且已发布）。
// 跨域窄接口：上层业务域绑定流程前校验。
func (uc *DefinitionUsecase) KeyStartable(ctx context.Context, key string) (exists, startable bool, err error) {
	d, err := uc.reader.GetDefinitionByKey(ctx, key)
	if err != nil {
		if errors.Is(err, ErrDefinitionNotFound) {
			return false, false, nil
		}
		return false, false, err
	}
	return true, d.Status == DefinitionEnabled && d.PublishedVersionID != "", nil
}

// published 按标识取可发起的定义：须存在、启用且已发布。
func (uc *DefinitionUsecase) published(ctx context.Context, key string) (*Definition, error) {
	d, err := uc.reader.GetDefinitionByKey(ctx, key)
	if err != nil {
		return nil, err
	}
	if d.Status != DefinitionEnabled || d.PublishedVersionID == "" {
		return nil, ErrDefinitionUnavailable
	}
	return d, nil
}

// graph 取某个已发布版本的可执行流程图（带进程内缓存）。返回值只读、可并发使用。流程图通不过
// 当前的校验规则时返回 ErrVersionInvalid。
func (uc *DefinitionUsecase) graph(ctx context.Context, versionID string) (*Graph, error) {
	c, err := uc.cached(ctx, versionID)
	if err != nil {
		return nil, err
	}
	return c.graph, nil
}

// cached 取某个已发布版本的缓存项；没有则读库、校验后放入。
func (uc *DefinitionUsecase) cached(ctx context.Context, versionID string) (cachedGraph, error) {
	uc.mu.Lock()
	c, ok := uc.graphs[versionID]
	uc.mu.Unlock()
	if ok {
		return c, nil
	}
	v, err := uc.versions.GetVersion(ctx, versionID)
	if err != nil {
		return cachedGraph{}, err
	}
	g, issues, err := issuesOf(v.Graph)
	if err != nil || len(issues) > 0 {
		// 已发布版本在发布时通过了校验；到这里说明校验规则收紧或数据被改动，拒绝执行。
		return cachedGraph{}, ErrVersionInvalid
	}
	c = cachedGraph{graph: g, raw: v.Graph}
	uc.mu.Lock()
	if len(uc.graphs) >= graphCacheSize {
		clear(uc.graphs) // 容量到顶整体清空：版本不可变，重建成本只是一次解析
	}
	uc.graphs[versionID] = c
	uc.mu.Unlock()
	return c, nil
}

// GraphJSON 取某个已发布版本的流程图 JSON（实例详情展示用；只读，调用方不得修改）。走同一份
// 缓存；流程图已通不过校验的存量版本直接读库——展示不受影响。
func (uc *DefinitionUsecase) GraphJSON(ctx context.Context, versionID string) (json.RawMessage, error) {
	c, err := uc.cached(ctx, versionID)
	if err == nil {
		return c.raw, nil
	}
	if !errors.Is(err, ErrVersionInvalid) {
		return nil, err
	}
	v, err := uc.versions.GetVersion(ctx, versionID)
	if err != nil {
		return nil, err
	}
	return v.Graph, nil
}
