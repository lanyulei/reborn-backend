package biz

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 导入行动作。
const (
	ImportActionCreate = "create"
	ImportActionUpdate = "update"
	ImportActionSkip   = "skip"
	ImportActionError  = "error"
)

// ImportRelationFailedPrefix 正式导入中资源已写入但关系步骤失败时行消息的固定
// 前缀：该行仍按资源动作（create/update/skip）计数并保留 CIID，前端据此前缀
// 渲染告警。
const ImportRelationFailedPrefix = "relation failed: "

// ExportMaxRows 导出行数上限（防整表拉取撑爆内存）。
const ExportMaxRows = 10000

// ImportRow 导入行（固定列 + 属性）；nil 指针字段表示未提供（更新时不改）。
type ImportRow struct {
	Name       string
	Status     string
	Env        *string
	Owner      *string
	Tags       []string
	ExternalID *string
	Attrs      map[string]any
	Relations  []ImportRelation
}

// ImportRelation 导入行的关系声明：按对端类型 + 名称解析目标（TargetModel 为空时取关系类型该端的固定类型）。
type ImportRelation struct {
	Type        string
	Direction   string
	TargetModel string
	TargetName  string
}

// ImportMaxRows 单次导入的行数上限（与 proto 的 max_items 一致）：导入脱离请求时限独立运行
// （见 Import），行数再多就该拆成多个请求。
const ImportMaxRows = 1000

// importTimeout 单次导入的总时限（最多 ImportMaxRows 行，逐行校验 + 写入）。
const importTimeout = 3 * time.Minute

// ImportRowResult 单行结果。
type ImportRowResult struct {
	Row     int32
	Name    string
	Action  string
	Message string
	CIID    string
}

// ImportResult 导入汇总。
type ImportResult struct {
	DryRun  bool
	Created int32
	Updated int32
	Skipped int32
	Failed  int32
	Results []ImportRowResult
}

// Import 按名称 upsert：存在则作为部分更新（提供的列/属性合并），否则创建。
// dryRun 只校验并判定动作；正式导入逐行独立提交（单行失败不影响其它行，结果
// 逐行返回），导入前建议先试运行。同一批内重复名称按后者覆盖前者的动作判定。
// 关系步骤失败：行按资源动作计数，消息以 ImportRelationFailedPrefix 开头
// （试运行与正式导入同一口径）。
// scope 为调用者数据范围：已存在资源、新环境与关系对端都须在范围内。
// 逐行的名称 / 关系类型 / 关系目标查询在本批内预取或缓存（importBatch），逐行判定不变。
func (uc *CIUsecase) Import(ctx context.Context, actor Actor, scope Scope, modelCode string, rows []ImportRow, dryRun bool) (*ImportResult, error) {
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
	// 本批名称一次取齐（此前逐行 GetByName）。
	batch := newImportBatch(ctx, uc.cis, uc.relTypes, m.Code, rows)
	// 两遍处理：先写入全部资源，再建立关系——同一批内互相引用的资源（A 关联 B，A 排在 B 之前）
	// 不受行序影响。pending 为本批将写入的资源名：试运行时资源并未真正创建，引用它们视为可解析。
	results := make([]ImportRowResult, len(rows))
	rowErr := make([]error, len(rows))
	pending := make(map[string]struct{}, len(rows))
	for i, row := range rows {
		res := ImportRowResult{Row: int32(i + 1), Name: row.Name}
		res.Action, res.CIID, rowErr[i] = uc.importRow(ctx, actor, scope, m, fields, row, dryRun, batch)
		if rowErr[i] == nil {
			pending[row.Name] = struct{}{}
		}
		results[i] = res
	}
	// 关系目标一次取齐：第一遍的资源此时已全部落库，本批新建的对端查得到。
	batch.prefetchTargets(ctx, m, rows, rowErr)
	for i, row := range rows {
		res, ciID, err := results[i], results[i].CIID, rowErr[i]
		if err != nil {
			res.Action, res.Message = ImportActionError, err.Error()
		} else if len(row.Relations) > 0 {
			linked, err := uc.importRelations(ctx, actor, scope, m, ciID, row.Relations, dryRun, pending, batch)
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

// importLookupChunk 单条语句的名称数组上限（同 ExportRelations 的 ID 数组约定）。
const importLookupChunk = 1000

// importBatch 一次导入内复用的读取结果，把逐行的名称 / 关系类型查询换成常数次：
//
//   - byName：按类型批量预取的已存在资源（类型 → 名称 → 资源，查过但不存在的名称值为 nil）。
//     仓储不具备批量能力、或预取查询失败的类型不在里面，逐行回查 GetByName——与改写前
//     「哪一行查失败哪一行报错」的口径一致，不把整批导入变成一次 500；
//   - types：关系类型缓存（一次导入内同一类型只查一次）；
//   - createdIDs：正式导入中本批新建的资源（名称 → 资源）。预取发生在创建之前，它们不在预取
//     结果里；后续同名行据此找到它；
//   - created：试运行中本批已判定 create 的名称 → 预演状态（见 importRow）。
//
// 预取只回答「名称是否已存在、是哪一条」。正式导入处理到已存在的行时仍读一次当下状态（见 importRow）：
// 逐行判定（范围、版本、合并）与改写前逐行 GetByName 读到的保持同一口径。
type importBatch struct {
	cis        CIRepo
	relTypes   RelationTypeRepo
	byName     map[string]map[string]*CI
	types      map[string]*RelationType
	createdIDs map[string]*CI
	created    map[string]*CI
}

// newImportBatch 预取本批全部名称对应的已存在资源（一条查询）。
func newImportBatch(ctx context.Context, cis CIRepo, relTypes RelationTypeRepo, modelCode string, rows []ImportRow) *importBatch {
	b := &importBatch{
		cis: cis, relTypes: relTypes, byName: map[string]map[string]*CI{},
		types: map[string]*RelationType{}, createdIDs: map[string]*CI{}, created: map[string]*CI{},
	}
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.Name)
	}
	b.prefetch(ctx, modelCode, names)
	return b
}

// prefetch 按类型批量取齐名称对应的已存在资源：一条语句查一个类型，结果以当次查询为准
// （同一名称再次预取时覆盖旧条目——两遍处理之间本批可能已改过它）。查询失败时丢弃该类型的预取
// 结果（之后逐行回查），不向上报错。
func (b *importBatch) prefetch(ctx context.Context, modelCode string, names []string) {
	repo, ok := b.cis.(CIBatchLookup)
	if !ok || len(names) == 0 {
		return
	}
	index := b.byName[modelCode]
	if index == nil {
		index = map[string]*CI{}
	}
	unique := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, dup := seen[name]; !dup {
			seen[name] = struct{}{}
			unique = append(unique, name)
		}
	}
	for start := 0; start < len(unique); start += importLookupChunk {
		chunk := unique[start:min(start+importLookupChunk, len(unique))]
		items, err := repo.GetByNames(ctx, modelCode, chunk)
		if err != nil {
			// 已取到的部分也不能用：本批新建的资源不在里面，按逐行回查为准。
			delete(b.byName, modelCode)
			return
		}
		for _, name := range chunk {
			index[name] = nil // 这次没查到：不存在（或已被删除）
		}
		for _, ci := range items {
			index[ci.Name] = ci
		}
	}
	b.byName[modelCode] = index
}

// prefetchTargets 关系目标按名称批量预取。只为资源步骤成功的行预取；关系类型在这里顺带进缓存
// （查不到的类型不缓存，留给 importRelations 报同样的错）。预取失败不报错，逐条回查。
func (b *importBatch) prefetchTargets(ctx context.Context, m *Model, rows []ImportRow, rowErr []error) {
	byModel := map[string][]string{}
	for i, row := range rows {
		if rowErr[i] != nil {
			continue
		}
		for _, rel := range row.Relations {
			t, err := b.relationType(ctx, rel.Type)
			if err != nil {
				continue
			}
			if _, targetModel := relationSides(t, rel.Direction); targetModel != ModelAny {
				byModel[targetModel] = append(byModel[targetModel], rel.TargetName)
			} else if rel.TargetModel != "" {
				byModel[rel.TargetModel] = append(byModel[rel.TargetModel], rel.TargetName)
			}
		}
	}
	for targetModel, names := range byModel {
		b.prefetch(ctx, targetModel, names)
	}
}

// lookup 某（类型, 名称）对应的已存在资源，口径同 CIRepo.GetByName（不存在返回 ErrCINotFound）：
// 已预取的类型查内存，否则逐行回查。
func (b *importBatch) lookup(ctx context.Context, modelCode, name string) (*CI, error) {
	if index, ok := b.byName[modelCode]; ok {
		if ci := index[name]; ci != nil {
			return ci, nil
		}
		return nil, ErrCINotFound
	}
	return b.cis.GetByName(ctx, modelCode, name)
}

// existing 导入行名称对应的资源：本批新建过的优先（预取发生在创建之前），否则查预取结果 / 逐行回查。
// 口径同 CIRepo.GetByName（不存在返回 ErrCINotFound）。
func (b *importBatch) existing(ctx context.Context, modelCode, name string) (*CI, error) {
	if ci, ok := b.createdIDs[name]; ok {
		return ci, nil
	}
	return b.lookup(ctx, modelCode, name)
}

// relationType 关系类型（一次导入内缓存）。
func (b *importBatch) relationType(ctx context.Context, code string) (*RelationType, error) {
	if t, ok := b.types[code]; ok {
		return t, nil
	}
	t, err := b.relTypes.Get(ctx, code)
	if err != nil {
		return nil, err
	}
	b.types[code] = t
	return t, nil
}

// importRow 单行处理：返回动作与资源 ID。batch.created 仅试运行使用：本批前面的同名行已判定
// create 时，这一行按更新链路对那条「将创建」的资源预演。
func (uc *CIUsecase) importRow(ctx context.Context, actor Actor, scope Scope, m *Model, fields []*Field, row ImportRow, dryRun bool, batch *importBatch) (string, string, error) {
	existing, err := batch.existing(ctx, m.Code, row.Name)
	if err != nil && err != ErrCINotFound {
		return ImportActionError, "", err
	}
	if existing != nil && !dryRun {
		// 正式导入以处理到这一行时的库内状态为准：预取只说明「名称已存在、是哪一条」，范围判定与
		// 合并都基于当下读到的行（改写前逐行 GetByName 读到的就是它）。被并发删除的按不存在处理，
		// 走创建链路——同样与改写前一致。
		cur, err := uc.cis.Get(ctx, existing.ID)
		if err != nil && err != ErrCINotFound {
			return ImportActionError, existing.ID, err
		}
		existing = cur
	}
	virtual := false
	if existing == nil && dryRun {
		if existing = batch.created[row.Name]; existing != nil {
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
			batch.created[row.Name] = importCI(m, row, attrs, actor)
			return ImportActionCreate, "", nil
		}
		ci, err := uc.create(ctx, actor, scope, importCI(m, row, attrs, actor))
		if err != nil {
			return ImportActionError, "", err
		}
		batch.createdIDs[row.Name] = ci
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
			batch.created[row.Name] = next
		}
		if !changed {
			return ImportActionSkip, existing.ID, nil
		}
		return ImportActionUpdate, existing.ID, nil
	}
	// 正式导入：existing 是刚读到的当下状态，直接在其上合并写入（改写前 Update 内部再读一次做版本
	// 比对、写完再回读一次，导入只用得上「是否有变化」）。写入后不回读。
	_, changed, err := uc.updateCurrent(ctx, actor, scope, existing, upd)
	if err != nil {
		return ImportActionError, existing.ID, err
	}
	if !changed {
		return ImportActionSkip, existing.ID, nil
	}
	return ImportActionUpdate, existing.ID, nil
}

// importCI 导入行 → 待创建的资源（取值已校验归一；状态缺省 active、标签缺省空；来源与版本
// 由写入时（create）赋值，试运行的预演在这里直接补上）。
func importCI(m *Model, row ImportRow, attrs map[string]any, actor Actor) *CI {
	ci := &CI{ModelCode: m.Code, Name: row.Name, Status: row.Status, Attrs: attrs, Tags: row.Tags, Source: actor.source(), Version: 1}
	if ci.Status == "" {
		ci.Status = CIStatusActive
	}
	if ci.Tags == nil {
		ci.Tags = []string{}
	}
	if row.Env != nil {
		ci.Env = *row.Env
	}
	if row.Owner != nil {
		ci.Owner = *row.Owner
	}
	if row.ExternalID != nil {
		ci.ExternalID = *row.ExternalID
	}
	return ci
}

// applyUpdate 更新链路的校验与合并（Update 与导入试运行的预演共用同一实现，预览即正式结果）：
// 返回合并后的资源与是否有真实变化；不落库，不改版本与审计列。
func (uc *CIUsecase) applyUpdate(ctx context.Context, actor Actor, scope Scope, cur *CI, fields []*Field, upd CIUpdate) (*CI, bool, error) {
	next := *cur
	next.Attrs = cloneAttrs(cur.Attrs)
	if err := rejectExternalIDChange(actor, cur, upd); err != nil {
		return nil, false, err
	}
	if upd.Name != nil {
		next.Name = *upd.Name
	}
	if upd.Env != nil {
		next.Env = *upd.Env
	}
	if upd.Owner != nil {
		next.Owner = *upd.Owner
	}
	if upd.ExternalID != nil {
		next.ExternalID = *upd.ExternalID
	}
	if upd.Tags != nil {
		next.Tags = slices.Clone(*upd.Tags)
	}
	if upd.Attrs != nil {
		upd.Attrs = dropMaskedValues(fields, cloneAttrs(upd.Attrs))
		// 同步只读字段不接受人工修改（同步来源可写）。只对仍由采集源维护的资源生效：人工资源、
		// 已解除同步托管的资源，以及采集源已被删除的资源，都没有同步来写这些字段，须允许人工维护。
		if actor.source() == SourceManual && syncMaintained(cur) {
			if err := rejectSyncOwned(fields, cur.Attrs, upd.Attrs); err != nil {
				return nil, false, err
			}
		}
		// 增量校验改动的键（partial），再合并；清空的键从结果移除。
		patch, err := ValidateAttrs(fields, upd.Attrs, true)
		if err != nil {
			return nil, false, err
		}
		for key, v := range upd.Attrs {
			if v == nil {
				delete(next.Attrs, key)
			}
		}
		for key, v := range patch {
			next.Attrs[key] = v
		}
		// 合并后只校验必填（防止清空必填字段）；存量值与已废弃字段的遗留键不重校验。
		if err := checkRequired(fields, next.Attrs); err != nil {
			return nil, false, err
		}
		if err := uc.checkRefs(ctx, scope, fields, patch); err != nil {
			return nil, false, err
		}
	}
	_, after := diffCI(cur, &next)
	return &next, len(after) > 0, nil
}

// Export 拉取某类型资源（最多 ExportMaxRows）与其字段定义，供 service 编码为 CSV。
func (uc *CIUsecase) Export(ctx context.Context, filter CIFilter) ([]*CI, []*Field, error) {
	if filter.ModelCode == "" {
		return nil, nil, detail(ErrCIInvalid, "model_code is required")
	}
	if _, err := uc.models.Get(ctx, filter.ModelCode); err != nil {
		return nil, nil, err
	}
	fields, err := uc.fields.ListByModel(ctx, filter.ModelCode)
	if err != nil {
		return nil, nil, err
	}
	sortFields(fields)
	// 敏感字段不可作为过滤条件（与 List 同一口径）
	for _, code := range sensitiveCodes(fields) {
		if _, ok := filter.Attrs[code]; ok {
			return nil, nil, detail(ErrCIInvalid, "field %q is sensitive and cannot be used as a filter", code)
		}
	}
	// 键集游标遍历、不逐页计数：OFFSET 翻页在大表上越翻越慢，且每页各跑一次 count(*)
	filter.NoCount = true
	filter.Cursor = ""
	all := make([]*CI, 0, 256)
	for len(all) < ExportMaxRows {
		items, _, err := uc.cis.List(ctx, filter, 1, paging.MaxPageSize)
		if err != nil {
			return nil, nil, err
		}
		all = append(all, items...)
		if int64(len(items)) < paging.MaxPageSize {
			break
		}
		filter.Cursor = EncodeCursor(items[len(items)-1])
	}
	if len(all) > ExportMaxRows {
		all = all[:ExportMaxRows]
	}
	if err := uc.maskAll(ctx, all); err != nil {
		return nil, nil, err
	}
	return all, fields, nil
}

// relationSides 以 direction 为当前资源的视角，拆出关系类型的两端：当前资源所在端的类型与对端的类型。
func relationSides(t *RelationType, direction string) (myModel, targetModel string) {
	if direction == DirectionOutgoing {
		return t.SrcModel, t.DstModel
	}
	return t.DstModel, t.SrcModel
}

// importRelations 解析并建立导入行声明的关系：目标按 (类型, 名称) 查找（batch 内已预取 / 缓存）；
// 已存在跳过（不计数）；dryRun 只解析目标与校验类型，不写入。返回新建关系数（试运行为「正式导入
// 将新建」的条数）。范围外的对端按不存在处理（不泄露其存在）。
func (uc *CIUsecase) importRelations(ctx context.Context, actor Actor, scope Scope, m *Model, ciID string, rels []ImportRelation, dryRun bool, pending map[string]struct{}, batch *importBatch) (int32, error) {
	var linked int32
	for _, rel := range rels {
		t, err := batch.relationType(ctx, rel.Type)
		if err != nil {
			return linked, err
		}
		myModel, targetModel := relationSides(t, rel.Direction)
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
		target, err := batch.lookup(ctx, targetModel, rel.TargetName)
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

// ExportRelations 被导出资源的关联，转成与导入行同构的声明（ciID → 关联列表）。
// 对端不在调用者数据范围内的关联不导出；关系类型该端为通配时带上对端类型（导入时需要）。
func (uc *CIUsecase) ExportRelations(ctx context.Context, scope Scope, items []*CI) (map[string][]ImportRelation, error) {
	out := make(map[string][]ImportRelation, len(items))
	if len(items) == 0 {
		return out, nil
	}
	types, err := uc.relTypes.List(ctx, "")
	if err != nil {
		return nil, err
	}
	byCode := make(map[string]*RelationType, len(types))
	for _, t := range types {
		byCode[t.Code] = t
	}
	ids := make([]string, 0, len(items))
	for _, ci := range items {
		ids = append(ids, ci.ID)
	}
	// 分块查询：单条语句的 ID 数组不超过 1000。
	for start := 0; start < len(ids); start += 1000 {
		rels, err := uc.relations.ListForExport(ctx, ids[start:min(start+1000, len(ids))])
		if err != nil {
			return nil, err
		}
		for _, r := range rels {
			t, ok := byCode[r.TypeCode]
			if !ok || !scope.Allows(r.PeerModel, r.PeerEnv) {
				continue
			}
			decl := ImportRelation{Type: r.TypeCode, Direction: r.Direction, TargetName: r.PeerName}
			// 对端所在的一端为通配时，导入需要显式的对端类型。
			if end := t.DstModel; (r.Direction == DirectionOutgoing && end == ModelAny) || (r.Direction == DirectionIncoming && t.SrcModel == ModelAny) {
				decl.TargetModel = r.PeerModel
			}
			out[r.CIID] = append(out[r.CIID], decl)
		}
	}
	return out, nil
}
