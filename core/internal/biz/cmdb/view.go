package biz

import (
	"context"
	"slices"
	"time"
)

// 详情布局区块类型。
const (
	ViewSectionAttrs    = "attrs"
	ViewSectionRelation = "relation"
)

// ViewSection 详情页区块（jsonb 持久化，json 标签即契约）。
type ViewSection struct {
	Type         string   `json:"type"`
	Title        string   `json:"title,omitempty"`
	Fields       []string `json:"fields,omitempty"`
	RelationType string   `json:"relationType,omitempty"`
	Direction    string   `json:"direction,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	Collapsed    bool     `json:"collapsed,omitempty"`
}

// ModelView 类型详情布局。
type ModelView struct {
	ModelCode  string
	Sections   []ViewSection
	UpdateUser string
	UpdateTime time.Time
}

// ViewRepo 布局仓储接口；Get 未配置返回 (nil, nil)。
type ViewRepo interface {
	Get(ctx context.Context, modelCode string) (*ModelView, error)
	Upsert(ctx context.Context, view *ModelView) error
}

// GetView 返回有效布局：已配置则原样返回，否则按字段与适用关系生成缺省布局。
func (uc *ModelUsecase) GetView(ctx context.Context, modelCode string) (sections []ViewSection, configured bool, err error) {
	_, fields, types, err := uc.GetModel(ctx, modelCode)
	if err != nil {
		return nil, false, err
	}
	view, err := uc.views.Get(ctx, modelCode)
	if err != nil {
		return nil, false, err
	}
	if view != nil {
		return view.Sections, true, nil
	}
	return DefaultView(modelCode, fields, types), false, nil
}

// DefaultView 缺省布局：一个"属性"分组（全部未废弃字段）+ 每个适用关系方向一个面板。
func DefaultView(modelCode string, fields []*Field, types []*RelationType) []ViewSection {
	codes := make([]string, 0, len(fields))
	for _, f := range fields {
		if !f.Deprecated {
			codes = append(codes, f.Code)
		}
	}
	sections := []ViewSection{{Type: ViewSectionAttrs, Fields: codes}}
	for _, t := range types {
		if t.SrcModel == ModelAny || t.SrcModel == modelCode {
			sections = append(sections, ViewSection{Type: ViewSectionRelation, RelationType: t.Code, Direction: DirectionOutgoing})
		}
		if t.DstModel == ModelAny || t.DstModel == modelCode {
			sections = append(sections, ViewSection{Type: ViewSectionRelation, RelationType: t.Code, Direction: DirectionIncoming})
		}
	}
	return sections
}

// UpdateView 校验并整体替换布局：属性分组只能引用本类型未废弃字段且不重复；
// 关联面板必须是适用于本类型的关系与方向，columns 只能引用对端类型的字段
// （对端为通配时不校验列）。
func (uc *ModelUsecase) UpdateView(ctx context.Context, actor Actor, modelCode string, sections []ViewSection) ([]ViewSection, error) {
	_, fields, types, err := uc.GetModel(ctx, modelCode)
	if err != nil {
		return nil, err
	}
	// 已废弃字段允许被布局继续引用（前端在原分组内灰显展示）；
	// 若此处排除废弃字段，存量布局一旦引用废弃字段，任何布局保存都会被拒绝。
	fieldSet := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		fieldSet[f.Code] = struct{}{}
	}
	typeByCode := make(map[string]*RelationType, len(types))
	for _, t := range types {
		typeByCode[t.Code] = t
	}
	seenField := map[string]struct{}{}
	seenPanel := map[string]struct{}{}
	for i, s := range sections {
		switch s.Type {
		case ViewSectionAttrs:
			for _, code := range s.Fields {
				if _, ok := fieldSet[code]; !ok {
					return nil, detail(ErrViewInvalid, "section %d references unknown field %q", i+1, code)
				}
				if _, dup := seenField[code]; dup {
					return nil, detail(ErrViewInvalid, "field %q appears in more than one section", code)
				}
				seenField[code] = struct{}{}
			}
		case ViewSectionRelation:
			t, ok := typeByCode[s.RelationType]
			if !ok {
				return nil, detail(ErrViewInvalid, "section %d references unknown relation type %q", i+1, s.RelationType)
			}
			var target string
			switch s.Direction {
			case DirectionOutgoing:
				if t.SrcModel != ModelAny && t.SrcModel != modelCode {
					return nil, detail(ErrViewInvalid, "relation %q is not outgoing for %s", t.Code, modelCode)
				}
				target = t.DstModel
			case DirectionIncoming:
				if t.DstModel != ModelAny && t.DstModel != modelCode {
					return nil, detail(ErrViewInvalid, "relation %q is not incoming for %s", t.Code, modelCode)
				}
				target = t.SrcModel
			default:
				return nil, detail(ErrViewInvalid, "section %d has invalid direction", i+1)
			}
			key := s.RelationType + ":" + s.Direction
			if _, dup := seenPanel[key]; dup {
				return nil, detail(ErrViewInvalid, "relation panel %s duplicated", key)
			}
			seenPanel[key] = struct{}{}
			if len(s.Columns) > 0 && target != ModelAny {
				targetFields, err := uc.fields.ListByModel(ctx, target)
				if err != nil {
					return nil, err
				}
				for _, col := range s.Columns {
					if !slices.ContainsFunc(targetFields, func(f *Field) bool { return f.Code == col && !f.Deprecated }) {
						return nil, detail(ErrViewInvalid, "column %q is not a field of %s", col, target)
					}
				}
			}
		default:
			return nil, detail(ErrViewInvalid, "section %d has unknown type %q", i+1, s.Type)
		}
	}
	view := &ModelView{ModelCode: modelCode, Sections: sections, UpdateUser: actor.UserID, UpdateTime: time.Now()}
	if err := uc.views.Upsert(ctx, view); err != nil {
		return nil, err
	}
	return sections, nil
}
