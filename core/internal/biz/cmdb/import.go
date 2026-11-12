package biz

import (
	"context"
	"fmt"
	"slices"

	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 导入行动作。
const (
	ImportActionCreate = "create"
	ImportActionUpdate = "update"
	ImportActionSkip   = "skip"
	ImportActionError  = "error"
)

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
func (uc *CIUsecase) Import(ctx context.Context, actor Actor, modelCode string, rows []ImportRow, dryRun bool) (*ImportResult, error) {
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
	out := &ImportResult{DryRun: dryRun, Results: make([]ImportRowResult, 0, len(rows))}
	for i, row := range rows {
		res := ImportRowResult{Row: int32(i + 1), Name: row.Name}
		action, ciID, err := uc.importRow(ctx, actor, m, fields, row, dryRun)
		res.Action, res.CIID = action, ciID
		if err != nil {
			res.Action, res.Message = ImportActionError, err.Error()
		} else if len(row.Relations) > 0 {
			linked, err := uc.importRelations(ctx, actor, m, ciID, row.Relations, dryRun)
			if err != nil {
				res.Action, res.Message = ImportActionError, err.Error()
			} else if linked > 0 {
				res.Message = fmt.Sprintf("relations linked: %d", linked)
				if res.Action == ImportActionSkip {
					res.Action = ImportActionUpdate
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
func (uc *CIUsecase) importRow(ctx context.Context, actor Actor, m *Model, fields []*Field, row ImportRow, dryRun bool) (string, string, error) {
	existing, err := uc.cis.GetByName(ctx, m.Code, row.Name)
	if err != nil && err != ErrCINotFound {
		return ImportActionError, "", err
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
		if _, err := ValidateAttrs(fields, next.Attrs, false); err != nil {
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
	all := make([]*CI, 0, 256)
	for page := int64(1); len(all) < ExportMaxRows; page++ {
		items, total, err := uc.cis.List(ctx, filter, page, paging.MaxPageSize)
		if err != nil {
			return nil, nil, err
		}
		all = append(all, items...)
		if int64(len(all)) >= total || len(items) == 0 {
			break
		}
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
func (uc *CIUsecase) importRelations(ctx context.Context, actor Actor, m *Model, ciID string, rels []ImportRelation, dryRun bool) (int32, error) {
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
