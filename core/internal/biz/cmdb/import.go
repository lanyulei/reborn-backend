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

// importTimeout 单次导入的总时限（最多 1000 行，逐行校验 + 写入）。
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
func (uc *CIUsecase) Import(ctx context.Context, actor Actor, scope Scope, modelCode string, rows []ImportRow, dryRun bool) (*ImportResult, error) {
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
	for i, row := range rows {
		res := ImportRowResult{Row: int32(i + 1), Name: row.Name}
		action, ciID, err := uc.importRow(ctx, actor, scope, m, fields, row, dryRun)
		res.Action, res.CIID = action, ciID
		if err != nil {
			res.Action, res.Message = ImportActionError, err.Error()
		} else if len(row.Relations) > 0 {
			linked, err := uc.importRelations(ctx, actor, scope, m, ciID, row.Relations, dryRun)
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

// importRow 单行处理：返回动作与资源 ID。
func (uc *CIUsecase) importRow(ctx context.Context, actor Actor, scope Scope, m *Model, fields []*Field, row ImportRow, dryRun bool) (string, string, error) {
	existing, err := uc.cis.GetByName(ctx, m.Code, row.Name)
	if err != nil && err != ErrCINotFound {
		return ImportActionError, "", err
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
		if err := uc.checkRefs(ctx, fields, attrs); err != nil {
			return ImportActionError, "", err
		}
		if dryRun {
			return ImportActionCreate, "", nil
		}
		ci := &CI{ModelCode: m.Code, Name: row.Name, Status: row.Status, Attrs: attrs, Tags: row.Tags}
		if row.Env != nil {
			ci.Env = *row.Env
		}
		if row.Owner != nil {
			ci.Owner = *row.Owner
		}
		if row.ExternalID != nil {
			ci.ExternalID = *row.ExternalID
		}
		created, err := uc.Create(ctx, actor, ci)
		if err != nil {
			return ImportActionError, "", err
		}
		return ImportActionCreate, created.ID, nil
	}
	upd := CIUpdate{Env: row.Env, Owner: row.Owner, ExternalID: row.ExternalID, Attrs: row.Attrs}
	if row.Tags != nil {
		tags := slices.Clone(row.Tags)
		upd.Tags = &tags
	}
	if dryRun {
		// 试运行：复用更新链路的校验与差异判定，但不落库。
		changed, err := uc.previewUpdate(ctx, actor, existing, fields, upd)
		if err != nil {
			return ImportActionError, existing.ID, err
		}
		if !changed {
			return ImportActionSkip, existing.ID, nil
		}
		return ImportActionUpdate, existing.ID, nil
	}
	updated, err := uc.Update(ctx, actor, existing.ID, existing.Version, upd)
	if err != nil {
		return ImportActionError, existing.ID, err
	}
	if updated.Version == existing.Version {
		return ImportActionSkip, existing.ID, nil
	}
	return ImportActionUpdate, existing.ID, nil
}

// previewUpdate 不落库地计算一次更新是否有变化（校验口径与 Update 一致）。
func (uc *CIUsecase) previewUpdate(ctx context.Context, actor Actor, cur *CI, fields []*Field, upd CIUpdate) (bool, error) {
	next := *cur
	next.Attrs = cloneAttrs(cur.Attrs)
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
		if actor.source() == SourceManual {
			if err := rejectSyncOwned(fields, cur.Attrs, upd.Attrs); err != nil {
				return false, err
			}
		}
		patch, err := ValidateAttrs(fields, upd.Attrs, true)
		if err != nil {
			return false, err
		}
		for key, v := range upd.Attrs {
			if v == nil {
				delete(next.Attrs, key)
			}
		}
		for key, v := range patch {
			next.Attrs[key] = v
		}
		if err := checkRequired(fields, next.Attrs); err != nil {
			return false, err
		}
		if err := uc.checkRefs(ctx, fields, patch); err != nil {
			return false, err
		}
	}
	_, after := diffCI(cur, &next)
	return len(after) > 0, nil
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

// importRelations 解析并建立导入行声明的关系：目标按 (类型, 名称) 查找；已存在跳过；
// dryRun 只解析目标与校验类型，不写入。返回新建关系数。
// 范围外的对端按不存在处理（不泄露其存在）。
func (uc *CIUsecase) importRelations(ctx context.Context, actor Actor, scope Scope, m *Model, ciID string, rels []ImportRelation, dryRun bool) (int32, error) {
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
			if err == ErrCINotFound {
				return linked, detail(ErrCINotFound, "relation %s: %s %q not found", t.Code, targetModel, rel.TargetName)
			}
			return linked, err
		}
		if dryRun || ciID == "" {
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
