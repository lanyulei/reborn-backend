package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// 导入批量预取的等价性测试与基准。
//
// 参照实现 referenceImport 是改写前 Import / importRow / importRelations 的原样拷贝（冻结在这里）：
// 逐行 GetByName、每条关系 relTypes.Get + GetByName、创建 / 更新后回读。新实现的结论一律与它比较——
// 逐行的动作 / 消息 / 资源、落库后的资源状态、历史、关系全部一致；只有查询次数不同。

// importCalls 仓储方法调用计数；delay 非零时每次调用睡一小段（基准里模拟一次数据库往返）。
type importCalls struct {
	mu    sync.Mutex
	n     map[string]int
	delay time.Duration
}

func newImportCalls(delay time.Duration) *importCalls {
	return &importCalls{n: map[string]int{}, delay: delay}
}

func (c *importCalls) hit(method string) {
	c.mu.Lock()
	c.n[method]++
	c.mu.Unlock()
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
}

func (c *importCalls) count(methods ...string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, m := range methods {
		total += c.n[m]
	}
	return total
}

// 读查询：预取 / 缓存 / 去掉回读所削减的都是这些；写（Create / Update / CreateBatch）两种实现相同。
var importReadMethods = []string{"models.Get", "cis.Get", "cis.GetByName", "cis.GetByNames", "cis.GetByIDs", "cis.ExistsInModel", "relTypes.Get", "relations.Exists", "relations.CountSide"}

// ---- 内存仓储上的计数包装 ----

func memIDs(r *memCIRepo) []string {
	ids := make([]string, 0, len(r.items))
	for id := range r.items {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func memCopy(ci *CI) *CI {
	cp := *ci
	cp.Attrs = cloneAttrs(ci.Attrs)
	cp.Tags = slices.Clone(ci.Tags)
	return &cp
}

// importCIRepo 计数型资源仓储：在内存仓储上逐方法计数，并实现 CIBatchLookup（新实现靠它预取）。
// failBatch 为真时批量查询报错（验证退回逐行查询）。
type importCIRepo struct {
	*memCIRepo
	calls     *importCalls
	failBatch bool
}

func (r *importCIRepo) Get(ctx context.Context, id string) (*CI, error) {
	r.calls.hit("cis.Get")
	return r.memCIRepo.Get(ctx, id)
}

func (r *importCIRepo) GetByName(ctx context.Context, modelCode, name string) (*CI, error) {
	r.calls.hit("cis.GetByName")
	return r.memCIRepo.GetByName(ctx, modelCode, name)
}

// GetByNames implements biz.CIBatchLookup（口径同 GetByName；顺序不保证）。
func (r *importCIRepo) GetByNames(_ context.Context, modelCode string, names []string) ([]*CI, error) {
	r.calls.hit("cis.GetByNames")
	if r.failBatch {
		return nil, errors.New("batch lookup unavailable")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*CI, 0, len(names))
	for _, id := range memIDs(r.memCIRepo) {
		if ci := r.items[id]; ci.ModelCode == modelCode && slices.Contains(names, ci.Name) {
			out = append(out, memCopy(ci))
		}
	}
	return out, nil
}

func (r *importCIRepo) GetByIDs(ctx context.Context, ids []string) ([]*CI, error) {
	r.calls.hit("cis.GetByIDs")
	return r.memCIRepo.GetByIDs(ctx, ids)
}

func (r *importCIRepo) ExistsInModel(ctx context.Context, modelCode, id string) (bool, error) {
	r.calls.hit("cis.ExistsInModel")
	return r.memCIRepo.ExistsInModel(ctx, modelCode, id)
}

func (r *importCIRepo) Create(ctx context.Context, ci *CI, h *History) error {
	r.calls.hit("cis.Create")
	return r.memCIRepo.Create(ctx, ci, h)
}

func (r *importCIRepo) Update(ctx context.Context, ci *CI, expectedVersion int64, h *History) error {
	r.calls.hit("cis.Update")
	return r.memCIRepo.Update(ctx, ci, expectedVersion, h)
}

// importRelTypeRepo 计数型关系类型仓储。
type importRelTypeRepo struct {
	*memRelTypeRepo
	calls *importCalls
}

func (r *importRelTypeRepo) Get(ctx context.Context, code string) (*RelationType, error) {
	r.calls.hit("relTypes.Get")
	return r.memRelTypeRepo.Get(ctx, code)
}

// importModelRepo 计数型类型仓储（每次创建都会回查类型是否启用：一条真实查询）。
type importModelRepo struct {
	*memModelRepo
	calls *importCalls
}

func (r *importModelRepo) Get(ctx context.Context, code string) (*Model, error) {
	r.calls.hit("models.Get")
	return r.memModelRepo.Get(ctx, code)
}

// importRelationRepo 计数型关系仓储：Exists / CountSide / CreateBatch（正式导入建关系要用）。
type importRelationRepo struct {
	*memRelationRepo
	calls *importCalls
}

func (r *importRelationRepo) Exists(ctx context.Context, typeCode, srcID, dstID string) (bool, error) {
	r.calls.hit("relations.Exists")
	return r.memRelationRepo.Exists(ctx, typeCode, srcID, dstID)
}

func (r *importRelationRepo) CountSide(_ context.Context, typeCode, direction, id string) (int64, error) {
	r.calls.hit("relations.CountSide")
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for key := range r.exists {
		parts := strings.Split(key, "|")
		if parts[0] != typeCode {
			continue
		}
		if (direction == DirectionOutgoing && parts[1] == id) || (direction == DirectionIncoming && parts[2] == id) {
			n++
		}
	}
	return n, nil
}

func (r *importRelationRepo) CreateBatch(_ context.Context, rels []*Relation, _ []*History) error {
	r.calls.hit("relations.CreateBatch")
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rel := range rels {
		r.exists[rel.TypeCode+"|"+rel.SrcID+"|"+rel.DstID] = true
	}
	return nil
}

// ciOnly 只暴露 CIRepo：仓储「不具备」批量按名称读取的能力，导入退回逐行 GetByName。
type ciOnly struct{ CIRepo }

// importHarness 一套导入夹具。类型 host（ip 必填、secret 敏感、owner_note 人工、sn 同步只读、app 引用 app）、
// app、switch；关系 host_runs_app（一对多）、host_uplink（多对多）、host_peer（host ↔ host）。
type importHarness struct {
	calls *importCalls
	cis   *importCIRepo
	rels  *importRelationRepo
	types *importRelTypeRepo
	// ids 固定的种子资源 ID（引用字段要用 UUID）。
	ids map[string]string
}

func newImportHarness(calls *importCalls) *importHarness {
	return &importHarness{
		calls: calls,
		cis:   &importCIRepo{memCIRepo: newMemCIRepo(), calls: calls},
		rels:  &importRelationRepo{memRelationRepo: &memRelationRepo{exists: map[string]bool{}}, calls: calls},
		types: &importRelTypeRepo{memRelTypeRepo: &memRelTypeRepo{types: []*RelationType{
			{Code: "host_runs_app", SrcModel: "host", DstModel: "app", Cardinality: CardinalityOneToMany},
			{Code: "host_uplink", SrcModel: "host", DstModel: "switch", Cardinality: CardinalityManyToMany},
			{Code: "host_peer", SrcModel: "host", DstModel: "host", Cardinality: CardinalityManyToMany},
		}}, calls: calls},
		ids: map[string]string{},
	}
}

// seed 预置一台主机；sync 为真时由采集源托管（带同步只读字段 sn）。资源 ID 由名称决定（importTestID）。
func (h *importHarness) seed(name, env string, sync bool) *CI {
	h.ids[name] = importTestID(name)
	ci := &CI{
		ID: h.ids[name], ModelCode: "host", Name: name, Status: CIStatusActive, Env: env, Source: SourceManual, Version: 2,
		Attrs: map[string]any{"ip": "10.0.0.1", "owner_note": "seed"},
	}
	if sync {
		ci.Source, ci.ExternalID, ci.Attrs["sn"] = "sync:"+testIDRoot, "i-"+name, "SN-SEED"
	}
	return h.cis.seed(ci)
}

func (h *importHarness) seedModel(model, name, env string) *CI {
	h.ids[name] = importTestID(name)
	return h.cis.seed(&CI{ID: h.ids[name], ModelCode: model, Name: name, Status: CIStatusActive, Env: env, Source: SourceManual, Version: 1, Attrs: map[string]any{}})
}

// importTestID 由名称决定的 UUID：每一轮新建夹具时同一个名称拿到同一个 ID，引用字段的取值可以写进用例。
func importTestID(name string) string {
	sum := fnv.New32a()
	_, _ = sum.Write([]byte(name))
	return fmt.Sprintf("018f3e1a-0700-7000-8000-%012x", uint64(sum.Sum32())<<8|uint64(len(name)&0xff)) // 末段不超过 12 位十六进制
}

// usecase 组装用例。batched 为假时资源仓储不暴露批量能力（新实现同样退回逐行查询）。
func (h *importHarness) usecase(batched bool) *CIUsecase {
	models := &importModelRepo{memModelRepo: &memModelRepo{models: map[string]*Model{
		"host": {Code: "host", Status: ModelStatusEnabled}, "app": {Code: "app", Status: ModelStatusEnabled}, "switch": {Code: "switch", Status: ModelStatusEnabled},
	}}, calls: h.calls}
	fields := &memFieldRepo{byModel: map[string][]*Field{
		"host": {
			{Code: "ip", Type: FieldTypeIP, Required: true, Owner: FieldOwnerBoth},
			{Code: "secret", Type: FieldTypeString, Sensitive: true},
			{Code: "owner_note", Type: FieldTypeString, Owner: FieldOwnerManual},
			{Code: "sn", Type: FieldTypeString, Owner: FieldOwnerSync},
			{Code: "app", Type: FieldTypeRef, RefModel: "app"},
		},
	}}
	var cis CIRepo = h.cis
	if !batched {
		cis = ciOnly{CIRepo: h.cis}
	}
	relationUC := NewRelationUsecase(h.types, cis, h.rels, fields)
	return NewCIUsecase(models, fields, h.types, cis, h.rels, &memHistoryRepo{store: h.cis.memCIRepo}, relationUC, nil)
}

// ---- 参照实现：改写前 Import / importRow / importRelations 的原样拷贝（冻结） ----

func (uc *CIUsecase) referenceImport(ctx context.Context, actor Actor, scope Scope, modelCode string, rows []ImportRow, dryRun bool) (*ImportResult, error) {
	if len(rows) > ImportMaxRows {
		return nil, detail(ErrCIInvalid, "at most %d rows per import request (got %d); split the file into smaller batches", ImportMaxRows, len(rows))
	}
	m, err := uc.models.Get(ctx, modelCode)
	if err != nil {
		return nil, err
	}
	if m.Status != ModelStatusEnabled {
		return nil, detail(ErrCIInvalid, "model %s is disabled", m.Code)
	}
	fields, err := uc.fields.ListByModel(ctx, modelCode)
	if err != nil {
		return nil, err
	}
	// 导入逐行独立提交：一旦开始就不应被请求时限（服务端缺省 5s）或客户端断开
	// 拦腰截断——那样已提交的行无从得知。脱离请求取消，自带上界。
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), importTimeout)
	defer cancel()
	out := &ImportResult{DryRun: dryRun, Results: make([]ImportRowResult, 0, len(rows))}
	// 两遍处理：先写入全部资源，再建立关系——同一批内互相引用的资源（A 关联 B，A 排在 B 之前）
	// 不受行序影响。pending 为本批将写入的资源名：试运行时资源并未真正创建，引用它们视为可解析。
	results := make([]ImportRowResult, len(rows))
	rowErr := make([]error, len(rows))
	pending := make(map[string]struct{}, len(rows))
	// created（仅试运行用）为本批已判定 create 的名称 → 该行写入后的资源状态。正式导入逐行提交，
	// 后续同名行走的是更新链路；试运行不落库，不记下来的话这些行会按「库中不存在」再判一次 create，
	// 动作与错误都与正式结果不符。
	created := map[string]*CI{}
	for i, row := range rows {
		res := ImportRowResult{Row: int32(i + 1), Name: row.Name}
		res.Action, res.CIID, rowErr[i] = uc.referenceImportRow(ctx, actor, scope, m, fields, row, dryRun, created)
		if rowErr[i] == nil {
			pending[row.Name] = struct{}{}
		}
		results[i] = res
	}
	for i, row := range rows {
		res, ciID, err := results[i], results[i].CIID, rowErr[i]
		if err != nil {
			res.Action, res.Message = ImportActionError, err.Error()
		} else if len(row.Relations) > 0 {
			linked, err := uc.referenceImportRelations(ctx, actor, scope, m, ciID, row.Relations, dryRun, pending)
			if linked > 0 {
				res.Message = fmt.Sprintf("relations linked: %d", linked)
				if res.Action == ImportActionSkip {
					res.Action = ImportActionUpdate
				}
			}
			if err != nil {
				// 行按资源动作计数（Action / CIID 保留），关系失败经固定前缀的消息如实
				// 带出，不把整行记为失败。试运行与正式导入同一口径：预览即正式结果。
				res.Message = ImportRelationFailedPrefix + err.Error()
				if linked > 0 {
					res.Message += fmt.Sprintf(" (relations linked: %d)", linked)
				}
			}
		}
		switch res.Action {
		case ImportActionCreate:
			out.Created++
		case ImportActionUpdate:
			out.Updated++
		case ImportActionSkip:
			out.Skipped++
		default:
			out.Failed++
		}
		out.Results = append(out.Results, res)
	}
	return out, nil
}

func (uc *CIUsecase) referenceImportRow(ctx context.Context, actor Actor, scope Scope, m *Model, fields []*Field, row ImportRow, dryRun bool, created map[string]*CI) (string, string, error) {
	existing, err := uc.cis.GetByName(ctx, m.Code, row.Name)
	if err != nil && err != ErrCINotFound {
		return ImportActionError, "", err
	}
	virtual := false
	if existing == nil && dryRun {
		if existing = created[row.Name]; existing != nil {
			virtual = true
		}
	}
	// 数据范围（与单条创建 / 更新口径一致）：已存在资源须在范围内，写入的环境也须在范围内。
	if existing != nil {
		if !scope.Allows(existing.ModelCode, existing.Env) || (row.Env != nil && !scope.AllowsEnv(*row.Env)) {
			return ImportActionError, "", ErrScopeForbidden
		}
	} else {
		env := ""
		if row.Env != nil {
			env = *row.Env
		}
		if !scope.Allows(m.Code, env) {
			return ImportActionError, "", ErrScopeForbidden
		}
	}
	if existing == nil {
		attrs, err := ValidateAttrs(fields, row.Attrs, false)
		if err != nil {
			return ImportActionError, "", err
		}
		if err := uc.checkRefs(ctx, scope, fields, attrs); err != nil {
			return ImportActionError, "", err
		}
		if dryRun {
			created[row.Name] = importCI(m, row, attrs, actor)
			return ImportActionCreate, "", nil
		}
		ci, err := uc.Create(ctx, actor, scope, importCI(m, row, attrs, actor))
		if err != nil {
			return ImportActionError, "", err
		}
		return ImportActionCreate, ci.ID, nil
	}
	upd := CIUpdate{Env: row.Env, Owner: row.Owner, ExternalID: row.ExternalID, Attrs: row.Attrs}
	if row.Tags != nil {
		tags := slices.Clone(row.Tags)
		upd.Tags = &tags
	}
	if dryRun {
		// 试运行：复用更新链路的校验与差异判定，但不落库。
		next, changed, err := uc.applyUpdate(ctx, actor, scope, existing, fields, upd)
		if err != nil {
			return ImportActionError, existing.ID, err
		}
		if virtual && changed {
			// 本批将创建的资源：记下这一行改完后的状态，再往后的同名行接着在它之上预演。
			created[row.Name] = next
		}
		if !changed {
			return ImportActionSkip, existing.ID, nil
		}
		return ImportActionUpdate, existing.ID, nil
	}
	updated, err := uc.Update(ctx, actor, scope, existing.ID, existing.Version, upd)
	if err != nil {
		return ImportActionError, existing.ID, err
	}
	if updated.Version == existing.Version {
		return ImportActionSkip, existing.ID, nil
	}
	return ImportActionUpdate, existing.ID, nil
}

func (uc *CIUsecase) referenceImportRelations(ctx context.Context, actor Actor, scope Scope, m *Model, ciID string, rels []ImportRelation, dryRun bool, pending map[string]struct{}) (int32, error) {
	var linked int32
	for _, rel := range rels {
		t, err := uc.relTypes.Get(ctx, rel.Type)
		if err != nil {
			return linked, err
		}
		var myModel, targetModel string
		if rel.Direction == DirectionOutgoing {
			myModel, targetModel = t.SrcModel, t.DstModel
		} else {
			myModel, targetModel = t.DstModel, t.SrcModel
		}
		if !matchModel(myModel, m.Code) {
			return linked, detail(ErrRelationNotAllowed, "%s does not apply to %s (%s)", t.Code, m.Code, rel.Direction)
		}
		if targetModel == ModelAny {
			if rel.TargetModel == "" {
				return linked, detail(ErrCIInvalid, "relation %s: target_model is required (any-model side)", t.Code)
			}
			targetModel = rel.TargetModel
		} else if rel.TargetModel != "" && rel.TargetModel != targetModel {
			return linked, detail(ErrRelationNotAllowed, "relation %s expects %s on the other side, got %s", t.Code, targetModel, rel.TargetModel)
		}
		target, err := uc.cis.GetByName(ctx, targetModel, rel.TargetName)
		if err == nil && !scope.Allows(target.ModelCode, target.Env) {
			err = ErrCINotFound
		}
		if err != nil {
			// 试运行：对端是本批将创建的同类型资源（尚未落库）——正式导入时它已存在。
			if _, ok := pending[rel.TargetName]; ok && dryRun && err == ErrCINotFound && targetModel == m.Code {
				linked++
				continue
			}
			if err == ErrCINotFound {
				return linked, detail(ErrCINotFound, "relation %s: %s %q not found", t.Code, targetModel, rel.TargetName)
			}
			return linked, err
		}
		if dryRun || ciID == "" {
			// 试运行：关系已存在时正式导入不新建（Link 返回 0、行保持 skip），预演同样不计数——
			// 否则试运行报「relations linked: N」并把 skip 改报 update，与正式结果不一致。
			if dryRun && ciID != "" {
				src, dst := orient(ciID, target.ID, rel.Direction)
				exists, err := uc.relations.Exists(ctx, t.Code, src, dst)
				if err != nil {
					return linked, err
				}
				if exists {
					continue
				}
			}
			linked++
			continue
		}
		n, err := uc.relationUC.Link(ctx, actor, ciID, t.Code, rel.Direction, []string{target.ID})
		if err != nil {
			return linked, err
		}
		linked += n
	}
	return linked, nil
}

// ---- 结果与状态的比对 ----

// importState 导入后仓储的完整状态（资源 ID 换成名称；时间列不参与比较）。
type importState struct {
	cis       []string
	histories []string
	relations []string
}

type importOutcome struct {
	summary ImportResult
	rows    []string
	state   importState
}

func runImport(seed func(h *importHarness), batched bool, run func(uc *CIUsecase) (*ImportResult, error)) (importOutcome, *importHarness, error) {
	h := newImportHarness(newImportCalls(0))
	seed(h)
	res, err := run(h.usecase(batched))
	if err != nil {
		return importOutcome{}, h, err
	}
	nameOf := map[string]string{}
	for _, ci := range h.cis.items {
		nameOf[ci.ID] = ci.Name
	}
	out := importOutcome{summary: *res}
	for _, r := range res.Results {
		out.rows = append(out.rows, fmt.Sprintf("%d|%s|%s|%s|%s", r.Row, r.Name, r.Action, r.Message, nameOf[r.CIID]))
	}
	for _, id := range memIDs(h.cis.memCIRepo) {
		ci := h.cis.items[id]
		attrs, _ := json.Marshal(ci.Attrs)
		out.state.cis = append(out.state.cis, fmt.Sprintf("%s|%s|%s|%s|%s|%v|%s|%s|%s|%s|%d|%v|%s|%s",
			ci.ModelCode, ci.Name, ci.Status, ci.Env, ci.Owner, ci.Tags, ci.Source, ci.ExternalID, attrs, ci.SourceName, ci.Version, ci.SourceDeleted, ci.CreateUser, ci.UpdateUser))
	}
	for _, hst := range h.cis.histories {
		diff, _ := json.Marshal(hst.Diff)
		out.state.histories = append(out.state.histories, nameOf[hst.CIID]+":"+hst.Action+":"+hst.Operator+":"+string(diff))
	}
	for key := range h.rels.exists {
		parts := strings.Split(key, "|")
		out.state.relations = append(out.state.relations, parts[0]+"|"+nameOf[parts[1]]+">"+nameOf[parts[2]])
	}
	slices.Sort(out.state.relations)
	return out, h, nil
}

func sameImportOutcome(a, b importOutcome) (bool, string) {
	switch {
	case a.summary.Created != b.summary.Created || a.summary.Updated != b.summary.Updated || a.summary.Skipped != b.summary.Skipped || a.summary.Failed != b.summary.Failed:
		return false, fmt.Sprintf("summary created/updated/skipped/failed = %d/%d/%d/%d, reference %d/%d/%d/%d",
			b.summary.Created, b.summary.Updated, b.summary.Skipped, b.summary.Failed, a.summary.Created, a.summary.Updated, a.summary.Skipped, a.summary.Failed)
	case !slices.Equal(a.rows, b.rows):
		return false, fmt.Sprintf("rows\n reference = %s\n new       = %s", strings.Join(a.rows, "\n             "), strings.Join(b.rows, "\n           "))
	case !slices.Equal(a.state.cis, b.state.cis):
		return false, fmt.Sprintf("stored resources\n reference = %s\n new       = %s", strings.Join(a.state.cis, "\n             "), strings.Join(b.state.cis, "\n           "))
	case !slices.Equal(a.state.histories, b.state.histories):
		return false, fmt.Sprintf("histories\n reference = %v\n new       = %v", a.state.histories, b.state.histories)
	case !slices.Equal(a.state.relations, b.state.relations):
		return false, fmt.Sprintf("relations\n reference = %v\n new       = %v", a.state.relations, b.state.relations)
	}
	return true, ""
}

var (
	importAll = Scope{Unrestricted: true}
	// importProdHostApp 只可见 host / app 且仅 prod（未分环境的资源不可见）。
	importProdHostApp = Scope{Models: map[string]struct{}{"host": {}, "app": {}}, Envs: map[string]struct{}{"prod": {}}}
)

var importActor = Actor{UserID: "u"}

// compareImports 同一份初始状态上分别跑参照实现与新实现（仓储具备 / 不具备批量能力），试运行与正式导入
// 都断言：逐行的动作 / 消息 / 资源、落库后的资源状态、历史、关系完全一致。
func compareImports(t testing.TB, seed func(h *importHarness), scope Scope, rows []ImportRow) {
	t.Helper()
	for _, dryRun := range []bool{true, false} {
		ref, _, err := runImport(seed, true, func(uc *CIUsecase) (*ImportResult, error) {
			return uc.referenceImport(context.Background(), importActor, scope, "host", rows, dryRun)
		})
		if err != nil {
			t.Fatalf("reference import (dryRun=%v): %v", dryRun, err)
		}
		for _, batched := range []bool{true, false} {
			got, _, err := runImport(seed, batched, func(uc *CIUsecase) (*ImportResult, error) {
				return uc.Import(context.Background(), importActor, scope, "host", rows, dryRun)
			})
			if err != nil {
				t.Fatalf("import (dryRun=%v batched=%v): %v", dryRun, batched, err)
			}
			if ok, why := sameImportOutcome(ref, got); !ok {
				t.Fatalf("import (dryRun=%v batched=%v) diverges from the reference:\n%s", dryRun, batched, why)
			}
		}
	}
}

// baseSeed 预置：三台人工主机（prod / dev / 未分环境）、一台同步托管主机、两个应用、一台交换机，
// 以及一条已有关系 e1 → a1。
func baseSeed(h *importHarness) {
	for _, e := range []struct {
		name, env string
		sync      bool
	}{{"e1", "prod", false}, {"e2", "dev", false}, {"e3", "", false}, {"e4", "prod", true}} {
		h.seed(e.name, e.env, e.sync)
	}
	h.seedModel("app", "a1", "prod")
	h.seedModel("app", "a2", "dev")
	h.seedModel("switch", "s1", "prod")
	h.rels.exists["host_runs_app|"+h.ids["e1"]+"|"+h.ids["a1"]] = true
}

// importSeedNoLinks baseSeed 去掉预置的关系：场景里的关联要能真正建立（host_runs_app 是一对多，
// a1 已有上游时再关联会报基数冲突）。
func importSeedNoLinks(h *importHarness) {
	baseSeed(h)
	h.rels.exists = map[string]bool{}
}

func importStrp(s string) *string { return &s }

func importRel(typ, direction, target string) ImportRelation {
	return ImportRelation{Type: typ, Direction: direction, TargetName: target}
}

type importScenario struct {
	name  string
	seed  func(h *importHarness)
	scope Scope
	rows  func(h *importHarness) []ImportRow
	// want 行结果里必须出现的片段（确认这个场景确实覆盖到了它声称的分支）。
	want []string
}

// 人工构造的导入批次：创建 / 更新 / 跳过 / 各类报错、同批重名、关系（已存在的对端、本批将创建的对端、
// 不存在 / 范围外的对端、已存在的关系、反向、未知类型、方向不符）、数据范围、同步只读字段、采集源删除后
// 放开、敏感字段、引用字段、状态列——每种的结论都与参照实现逐字段一致。
func importScenarios() []importScenario {
	host := func(h *importHarness, name, ip string) ImportRow {
		return ImportRow{Name: name, Attrs: map[string]any{"ip": ip}}
	}
	ip := "10.0.0."
	return []importScenario{
		{
			name: "create update skip and errors", seed: baseSeed, scope: importAll,
			rows: func(h *importHarness) []ImportRow {
				return []ImportRow{
					{Name: "e1", Attrs: map[string]any{"owner_note": "changed"}}, // update
					{Name: "e1", Attrs: map[string]any{"owner_note": "changed"}}, // skip
					host(h, "n1", ip+"9"), // create
					host(h, "n2", "bad"),  // error：非法 IP
					{Name: "n1", Owner: importStrp("ops"), Tags: []string{"x"}},                             // update：本批新建的同名行
					{Name: "n1", Owner: importStrp("ops"), Tags: []string{"x"}},                             // skip
					{Name: "n3", Attrs: map[string]any{"owner_note": "only"}},                               // error：新名称缺必填（更新链路下才合法）
					{Name: "e2", Attrs: map[string]any{"owner_note": "x"}},                                  // update
					{Name: "e4", Attrs: map[string]any{"sn": "SN-OTHER"}},                                   // error：同步只读字段
					{Name: "e4", Attrs: map[string]any{"sn": "SN-SEED", "owner_note": "ok"}},                // update：同步字段原值回传
					{Name: "e1", Attrs: map[string]any{"secret": MaskedValue, "owner_note": "masked-kept"}}, // update：脱敏占位不覆盖
					{Name: "e1", Attrs: map[string]any{"secret": nil}},                                      // update：清空敏感字段
					{Name: "n4", Attrs: map[string]any{"ip": ip + "11", "app": h.ids["a1"]}},                // create：引用字段合法
					{Name: "n5", Attrs: map[string]any{"ip": ip + "12", "app": h.ids["ghost"]}},             // error：引用不存在
					{Name: "n6", Attrs: map[string]any{"ip": ip + "13", "app": "not-a-uuid"}},               // error：引用不是资源 ID
					{Name: "e2", ExternalID: importStrp("i-other")},                                         // update：人工资源可改外部标识
					{Name: "e4", ExternalID: importStrp("i-other")},                                         // error：同步托管资源的对账锚点
					{Name: "n7", Status: "maintenance", Attrs: map[string]any{"ip": ip + "14"}},             // create：带状态列
				}
			},
			want: []string{"|create|", "|update|", "|skip|", "|error|", "is maintained by sync", "must be a resource id", "reconciliation anchor"},
		},
		{
			name: "duplicate names in one batch", seed: baseSeed, scope: importAll,
			rows: func(*importHarness) []ImportRow {
				return []ImportRow{
					{Name: "n1", Attrs: map[string]any{"ip": ip + "5"}},
					{Name: "n1", Attrs: map[string]any{"ip": ip + "5"}},
					{Name: "n1", Attrs: map[string]any{"owner_note": "x"}},
					{Name: "n1", Attrs: map[string]any{"owner_note": "x"}},
					{Name: "n1", Attrs: nil, Env: importStrp("prod")},
					{Name: "n1", Attrs: map[string]any{"ip": "bad"}},
					{Name: "n2", Attrs: map[string]any{"owner_note": "x"}},
					{Name: "e3", Attrs: map[string]any{"owner_note": "y"}},
					{Name: "e3", Attrs: map[string]any{"owner_note": "y"}},
				}
			},
			want: []string{"|create|", "|skip|", "|update|", "|error|"},
		},
		{
			name: "relations", seed: baseSeed, scope: importAll,
			rows: func(h *importHarness) []ImportRow {
				return []ImportRow{
					{Name: "e1", Relations: []ImportRelation{importRel("host_runs_app", DirectionOutgoing, "a1")}}, // 关系已存在：不计数
					{Name: "e2", Relations: []ImportRelation{importRel("host_runs_app", DirectionOutgoing, "a2"), importRel("host_uplink", DirectionOutgoing, "s1"), importRel("host_uplink", DirectionOutgoing, "s1")}},
					{Name: "n1", Attrs: map[string]any{"ip": ip + "21"}, Relations: []ImportRelation{
						importRel("host_peer", DirectionOutgoing, "n2"),     // 本批后面才创建的对端
						importRel("host_runs_app", DirectionIncoming, "a1"), // 方向不符
					}},
					{Name: "n2", Attrs: map[string]any{"ip": ip + "22"}, Relations: []ImportRelation{importRel("host_uplink", DirectionOutgoing, "ghost")}},
					{Name: "n3", Attrs: map[string]any{"ip": ip + "23"}, Relations: []ImportRelation{importRel("nope", DirectionOutgoing, "a1")}},
					{Name: "n4", Attrs: map[string]any{"ip": ip + "24"}, Relations: []ImportRelation{importRel("host_peer", DirectionIncoming, "e3"), importRel("host_runs_app", DirectionOutgoing, "n3")}},
					{Name: "n5", Attrs: map[string]any{"ip": "bad"}, Relations: []ImportRelation{importRel("host_uplink", DirectionOutgoing, "s1")}}, // 资源步骤已失败：关系不处理
				}
			},
			want: []string{"relations linked: 2", "relations linked: 2", "relation failed: ", "does not apply to host", "not found", "RELATION_TYPE_NOT_FOUND"},
		},
		{
			name: "scope restricted", seed: importSeedNoLinks, scope: importProdHostApp,
			rows: func(h *importHarness) []ImportRow {
				return []ImportRow{
					{Name: "e1", Owner: importStrp("ops")},                                                            // 在范围内：update
					{Name: "e2", Owner: importStrp("ops")},                                                            // 已存在资源在范围外
					{Name: "e3", Owner: importStrp("ops")},                                                            // 未分环境：环境受限时不可见
					{Name: "n1", Env: importStrp("dev"), Attrs: map[string]any{"ip": ip + "31"}},                      // 新环境在范围外
					{Name: "e1", Env: importStrp("dev")},                                                              // 写入的环境在范围外
					{Name: "n2", Env: importStrp("prod"), Attrs: map[string]any{"ip": ip + "32"}},                     // 在范围内：create
					{Name: "n3", Env: importStrp("prod"), Attrs: map[string]any{"ip": ip + "33", "app": h.ids["a2"]}}, // 引用指向范围外的资源
					{Name: "n4", Env: importStrp("prod"), Attrs: map[string]any{"ip": ip + "34"}, Relations: []ImportRelation{
						importRel("host_runs_app", DirectionOutgoing, "a1"),
						importRel("host_runs_app", DirectionOutgoing, "a2"), // 对端环境在范围外 → 不存在（前一条已建立）
					}},
					{Name: "n5", Env: importStrp("prod"), Attrs: map[string]any{"ip": ip + "35"}, Relations: []ImportRelation{importRel("host_uplink", DirectionOutgoing, "s1")}}, // 对端类型在范围外
				}
			},
			want: []string{"SCOPE_FORBIDDEN", "references a missing app resource", "a2\" not found", "s1\" not found", "relations linked: 1"},
		},
		{
			name: "sync source deleted", seed: func(h *importHarness) {
				baseSeed(h)
				for id, ci := range h.cis.items { // 采集源已删除：同步只读字段放回人工维护
					if ci.Name == "e4" {
						h.cis.items[id].SourceDeleted = true
					}
				}
			},
			scope: importAll,
			rows: func(*importHarness) []ImportRow {
				return []ImportRow{{Name: "e4", Attrs: map[string]any{"sn": "SN-OTHER"}}}
			},
			want: []string{"|update|"},
		},
	}
}

func TestImport_BatchedLookupsMatchReference(t *testing.T) {
	t.Parallel()
	for _, s := range importScenarios() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			h := newImportHarness(newImportCalls(0))
			s.seed(h)
			rows := s.rows(h)
			compareImports(t, s.seed, s.scope, rows)
			// 这个场景确实覆盖到了它声称的分支（按参照实现的结果看）。
			out, _, err := runImport(s.seed, true, func(uc *CIUsecase) (*ImportResult, error) {
				return uc.referenceImport(context.Background(), importActor, s.scope, "host", rows, false)
			})
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(out.rows, "\n")
			for _, want := range s.want {
				if !strings.Contains(joined, want) {
					t.Errorf("scenario must exercise %q; reference rows:\n%s", want, joined)
				}
			}
		})
	}
}

// 固定种子的随机批次（名称有重复、字段随机组合、含各类非法值与关系、随机受限范围）：试运行与正式导入的
// 结论都与参照实现完全一致。
func TestImport_BatchedLookupsMatchReferenceOnRandomRows(t *testing.T) {
	t.Parallel()
	pick := func(rnd *rand.Rand, items ...string) string { return items[rnd.IntN(len(items))] }
	maybe := func(rnd *rand.Rand, p float64) bool { return rnd.Float64() < p }
	for seed := uint64(1); seed <= 600; seed++ {
		rnd := rand.New(rand.NewPCG(seed, 0x51ed270b)) //nolint:gosec // G404：固定种子的测试数据，不需要密码学随机数
		names := []string{"e1", "e2", "e3", "e4", "n1", "n2", "n3", "n4", "n5", "n6"}
		targets := []string{"a1", "a2", "s1", "e1", "e3", "n1", "n3", "ghost"}
		rows := make([]ImportRow, 0, 30)
		for i := 0; i < 4+rnd.IntN(24); i++ {
			row := ImportRow{Name: pick(rnd, names...)}
			if maybe(rnd, 0.7) {
				row.Env = importStrp(pick(rnd, "", "prod", "dev", "test"))
			}
			if maybe(rnd, 0.3) {
				row.Owner = importStrp(pick(rnd, "", "ops"))
			}
			if maybe(rnd, 0.3) {
				row.Tags = []string{pick(rnd, "a", "b")}
			}
			if maybe(rnd, 0.2) {
				row.ExternalID = importStrp("ext-" + pick(rnd, "1", "2"))
			}
			if maybe(rnd, 0.15) {
				row.Status = pick(rnd, "active", "maintenance")
			}
			if maybe(rnd, 0.8) {
				attrs := map[string]any{}
				switch rnd.IntN(4) {
				case 0:
					attrs["ip"] = "10.9.0." + pick(rnd, "1", "2")
				case 1:
					attrs["ip"] = "bad"
				}
				switch rnd.IntN(5) {
				case 0:
					attrs["secret"] = "s"
				case 1:
					attrs["secret"] = MaskedValue
				case 2:
					attrs["secret"] = nil
				}
				switch rnd.IntN(4) {
				case 0:
					attrs["owner_note"] = "note"
				case 1:
					attrs["owner_note"] = nil
				}
				switch rnd.IntN(5) {
				case 0:
					attrs["sn"] = "SN-SEED"
				case 1:
					attrs["sn"] = "SN-OTHER"
				}
				switch rnd.IntN(6) {
				case 0:
					attrs["app"] = testIDApp // baseSeed 之外的 ID：引用不存在
				case 1:
					attrs["app"] = "not-a-uuid"
				}
				row.Attrs = attrs
			}
			for j := 0; j < rnd.IntN(4); j++ {
				row.Relations = append(row.Relations, ImportRelation{
					Type: pick(rnd, "host_runs_app", "host_uplink", "host_peer", "nope"), Direction: pick(rnd, DirectionOutgoing, DirectionIncoming),
					TargetModel: pick(rnd, "", "", "", "app", "host"), TargetName: pick(rnd, targets...),
				})
			}
			rows = append(rows, row)
		}
		scope := Scope{Unrestricted: true}
		if maybe(rnd, 0.5) {
			scope = Scope{Models: map[string]struct{}{}, Envs: map[string]struct{}{}}
			for _, m := range []string{"host", "app", "switch"} {
				if maybe(rnd, 0.7) {
					scope.Models[m] = struct{}{}
				}
			}
			for _, e := range []string{"", "prod", "dev"} {
				if maybe(rnd, 0.7) {
					scope.Envs[e] = struct{}{}
				}
			}
		}
		for _, dryRun := range []bool{true, false} {
			ref, _, err := runImport(baseSeed, true, func(uc *CIUsecase) (*ImportResult, error) {
				return uc.referenceImport(context.Background(), importActor, scope, "host", rows, dryRun)
			})
			if err != nil {
				t.Fatalf("seed %d: reference import: %v", seed, err)
			}
			got, _, err := runImport(baseSeed, true, func(uc *CIUsecase) (*ImportResult, error) {
				return uc.Import(context.Background(), importActor, scope, "host", rows, dryRun)
			})
			if err != nil {
				t.Fatalf("seed %d: import: %v", seed, err)
			}
			if ok, why := sameImportOutcome(ref, got); !ok {
				t.Fatalf("seed %d (dryRun=%v): import diverges from the reference\nrows: %+v\n%s", seed, dryRun, rows, why)
			}
		}
	}
}

// importCountSeed / importCountRows 规模 n 的批次：i%3==0 已存在且有变化（update）、i%3==1 已存在无变化
// （skip）、i%3==2 新名称（create）。
func importCountSeed(n int) func(h *importHarness) {
	return func(h *importHarness) {
		for i := 0; i < n; i++ {
			if i%3 != 2 {
				h.seed(fmt.Sprintf("x%04d", i), "prod", false)
			}
		}
	}
}

func importCountRows(n int) []ImportRow {
	rows := make([]ImportRow, 0, n)
	for i := 0; i < n; i++ {
		if i%3 == 2 {
			rows = append(rows, ImportRow{Name: fmt.Sprintf("new%04d", i), Attrs: map[string]any{"ip": "10.0.0.9"}})
			continue
		}
		row := ImportRow{Name: fmt.Sprintf("x%04d", i)}
		if i%3 == 0 {
			row.Attrs = map[string]any{"owner_note": "changed"}
		}
		rows = append(rows, row)
	}
	return rows
}

// importCounts 一次导入的仓储调用次数。
type importCounts struct {
	reads, writes              int
	getByName, getByNames, get int
	relTypeGet                 int
	relationExists             int
}

func countImport(h *importHarness) importCounts {
	return importCounts{
		reads: h.calls.count(importReadMethods...), writes: h.calls.count("cis.Create", "cis.Update"),
		getByName: h.calls.count("cis.GetByName"), getByNames: h.calls.count("cis.GetByNames"), get: h.calls.count("cis.Get"),
		relTypeGet: h.calls.count("relTypes.Get"), relationExists: h.calls.count("relations.Exists"),
	}
}

// N 行导入的查询次数：改写前每行一次 GetByName（正式导入另加更新链路内的读取与创建 / 更新后的回读），
// 改写后一条批量查询 + 每个已存在的行一次当下读取；写次数相同。结论仍与参照实现逐字段一致。
func TestImport_BatchedLookupsQueryCount(t *testing.T) {
	t.Parallel()
	for _, n := range []int{100, 1000} {
		rows := importCountRows(n)
		updates, skips, creates := (n+2)/3, n/3, (n+1)/3
		existing := updates + skips
		for _, dryRun := range []bool{true, false} {
			compareImports(t, importCountSeed(n), importAll, rows)
			run := func(reference bool) (importCounts, ImportResult) {
				out, h, err := runImport(importCountSeed(n), true, func(uc *CIUsecase) (*ImportResult, error) {
					if reference {
						return uc.referenceImport(context.Background(), importActor, importAll, "host", rows, dryRun)
					}
					return uc.Import(context.Background(), importActor, importAll, "host", rows, dryRun)
				})
				if err != nil {
					t.Fatal(err)
				}
				if int(out.summary.Created) != creates || int(out.summary.Updated) != updates || int(out.summary.Skipped) != skips || out.summary.Failed != 0 {
					t.Fatalf("fixture must yield %d creates / %d updates / %d skips, got created=%d updated=%d skipped=%d failed=%d",
						creates, updates, skips, out.summary.Created, out.summary.Updated, out.summary.Skipped, out.summary.Failed)
				}
				return countImport(h), out.summary
			}
			old, _ := run(true)
			cur, _ := run(false)
			// 写：两种实现相同（试运行不落库）。
			if (!dryRun && old.writes != updates+creates) || (dryRun && old.writes != 0) || cur.writes != old.writes {
				t.Fatalf("rows=%d dryRun=%v: writes reference=%d new=%d, want %d", n, dryRun, old.writes, cur.writes, updates+creates)
			}
			// 名称查询：改写前逐行 GetByName；改写后一条批量查询。
			if old.getByName != n || cur.getByName != 0 || cur.getByNames != 1 {
				t.Fatalf("rows=%d dryRun=%v: name lookups: reference GetByName=%d, new GetByName=%d GetByNames=%d", n, dryRun, old.getByName, cur.getByName, cur.getByNames)
			}
			wantOldReads, wantNewReads := n+1, 2 // +1 / 2：都含导入开始时读取类型那一次
			if !dryRun {
				// 改写前：更新 / 跳过的行在更新链路里再读一次，创建 / 更新后各回读一次。
				wantOldReads = 1 + 3*updates + 2*skips + 3*creates
				// 改写后：已存在的行各读一次当下状态（范围判定与合并基于它）；创建时 Create 回查一次类型是否启用。
				wantNewReads = 2 + existing + creates
				// 改写前 cis.Get：更新 / 跳过的行在更新链路里各一次，创建 / 更新后各回读一次。
				if old.get != 2*updates+skips+creates || cur.get != existing {
					t.Fatalf("rows=%d real: cis.Get reference=%d new=%d, want %d / %d", n, old.get, cur.get, 2*updates+skips+creates, existing)
				}
			}
			if old.reads != wantOldReads || cur.reads != wantNewReads {
				t.Fatalf("rows=%d dryRun=%v: reads reference=%d new=%d, want %d / %d", n, dryRun, old.reads, cur.reads, wantOldReads, wantNewReads)
			}
			t.Logf("rows=%d dryRun=%v: reads reference=%d new=%d (GetByName %d -> 0, GetByNames -> 1, cis.Get %d -> %d); writes %d = %d",
				n, dryRun, old.reads, cur.reads, old.getByName, old.get, cur.get, old.writes, cur.writes)
		}
	}
}

// 批量查询失败：退回逐行 GetByName，结论与参照实现一致（不把整批导入变成一次失败）。
func TestImport_BatchLookupFailureFallsBackToPerRowLookups(t *testing.T) {
	t.Parallel()
	rows := importCountRows(30)
	seed := func(h *importHarness) {
		importCountSeed(30)(h)
		h.cis.failBatch = true
	}
	compareImports(t, seed, importAll, rows)
	for _, dryRun := range []bool{true, false} {
		_, h, err := runImport(seed, true, func(uc *CIUsecase) (*ImportResult, error) {
			return uc.Import(context.Background(), importActor, importAll, "host", rows, dryRun)
		})
		if err != nil {
			t.Fatal(err)
		}
		if c := countImport(h); c.getByNames != 1 || c.getByName != len(rows) {
			t.Fatalf("dryRun=%v: a failed batch lookup must fall back to per-row lookups: GetByNames=%d GetByName=%d", dryRun, c.getByNames, c.getByName)
		}
	}
}

// 关系步骤：关系类型一次导入内只查一次（改写前每条声明查一次），关系目标按类型批量预取（改写前每条查一次）。
func TestImport_RelationLookupsAreCachedAndBatched(t *testing.T) {
	t.Parallel()
	seed := baseSeed
	rows := []ImportRow{
		{Name: "n1", Attrs: map[string]any{"ip": "10.0.0.1"}, Relations: []ImportRelation{importRel("host_runs_app", DirectionOutgoing, "a1"), importRel("host_uplink", DirectionOutgoing, "s1"), importRel("host_peer", DirectionOutgoing, "n2")}},
		{Name: "n2", Attrs: map[string]any{"ip": "10.0.0.2"}, Relations: []ImportRelation{importRel("host_uplink", DirectionOutgoing, "s1"), importRel("host_peer", DirectionOutgoing, "n3"), importRel("host_runs_app", DirectionOutgoing, "a2")}},
		{Name: "n3", Attrs: map[string]any{"ip": "10.0.0.3"}, Relations: []ImportRelation{importRel("host_peer", DirectionIncoming, "n1"), importRel("host_runs_app", DirectionOutgoing, "a1"), importRel("host_runs_app", DirectionOutgoing, "ghost")}},
	}
	const declarations = 9
	for _, dryRun := range []bool{true, false} {
		compareImports(t, seed, importAll, rows)
		run := func(reference bool) importCounts {
			_, h, err := runImport(seed, true, func(uc *CIUsecase) (*ImportResult, error) {
				if reference {
					return uc.referenceImport(context.Background(), importActor, importAll, "host", rows, dryRun)
				}
				return uc.Import(context.Background(), importActor, importAll, "host", rows, dryRun)
			})
			if err != nil {
				t.Fatal(err)
			}
			return countImport(h)
		}
		old, cur := run(true), run(false)
		if cur.getByName != 0 || cur.getByNames != 1+3 { // 资源名称（host）+ 对端类型 host / app / switch
			t.Fatalf("dryRun=%v: name lookups GetByName=%d GetByNames=%d, want 0 / 4", dryRun, cur.getByName, cur.getByNames)
		}
		if dryRun && old.relTypeGet != declarations {
			t.Fatalf("dry run: reference must look the relation type up once per declaration: %d", old.relTypeGet)
		}
		// 新实现：导入自己只按不同的类型各查一次（试运行恰好 3 次）。正式导入里 Link 对每次建关系的尝试
		// 还会各查一次（含随后因基数被拒的；Link 的契约不变）——次数与参照实现相同，从参照实现里量出来。
		wantTypeGet := 3
		if !dryRun {
			wantTypeGet += old.relationExists
		}
		if cur.relTypeGet != wantTypeGet {
			t.Fatalf("dryRun=%v: relation type lookups reference=%d new=%d, want %d", dryRun, old.relTypeGet, cur.relTypeGet, wantTypeGet)
		}
		t.Logf("dryRun=%v: reads reference=%d new=%d; relation type lookups %d -> %d; name lookups GetByName %d -> %d, GetByNames %d -> %d",
			dryRun, old.reads, cur.reads, old.relTypeGet, cur.relTypeGet, old.getByName, cur.getByName, old.getByNames, cur.getByNames)
	}
}

// 基准：仓储每次调用固定延迟（模拟一次数据库往返），比较参照实现（逐行查询 + 回读）与新实现
// （批量预取 / 缓存 / 不回读）在 100 / 1000 行、试运行 / 正式导入下的耗时与调用次数。不断言性能阈值。
func BenchmarkImport(b *testing.B) {
	const delay = 200 * time.Microsecond
	for _, n := range []int{100, 1000} {
		rows := make([]ImportRow, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, ImportRow{Name: fmt.Sprintf("bench%04d", i), Attrs: map[string]any{"ip": "10.0.0.9"}})
		}
		for _, dryRun := range []bool{true, false} {
			for _, path := range []string{"reference", "batched"} {
				b.Run(fmt.Sprintf("rows=%d/dryRun=%v/%s", n, dryRun, path), func(b *testing.B) {
					var reads, writes int
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						h := newImportHarness(newImportCalls(delay)) // 每次迭代都是空库：全部行都是新建
						uc := h.usecase(true)
						b.StartTimer()
						var res *ImportResult
						var err error
						if path == "reference" {
							res, err = uc.referenceImport(context.Background(), importActor, importAll, "host", rows, dryRun)
						} else {
							res, err = uc.Import(context.Background(), importActor, importAll, "host", rows, dryRun)
						}
						b.StopTimer()
						if err != nil || int(res.Created) != n || res.Failed != 0 {
							b.Fatalf("import result = created %d failed %d, want %d / 0 (err=%v)", res.Created, res.Failed, n, err)
						}
						c := countImport(h)
						reads, writes = reads+c.reads, writes+c.writes
					}
					b.ReportMetric(float64(reads)/float64(b.N), "reads/op")
					b.ReportMetric(float64(writes)/float64(b.N), "writes/op")
					b.ReportMetric(float64(reads+writes)/float64(b.N), "calls/op")
				})
			}
		}
	}
}
