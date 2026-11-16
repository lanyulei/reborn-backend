package biz

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// CatalogUsecase 服务目录用例：分类与模板的维护。
type CatalogUsecase struct {
	reader     CatalogReader
	categories CategoryWriter
	templates  TemplateWriter
	workflows  WorkflowCatalog
	managers   CatalogManager
	dir        Directory
}

// NewCatalogUsecase new a CatalogUsecase.
func NewCatalogUsecase(reader CatalogReader, categories CategoryWriter, templates TemplateWriter, workflows WorkflowCatalog,
	managers CatalogManager, dir Directory,
) *CatalogUsecase {
	return &CatalogUsecase{reader: reader, categories: categories, templates: templates, workflows: workflows, managers: managers, dir: dir}
}

// List 服务目录：全部分类，各含其下的模板（不含表单）。enabledOnly 时只含启用的模板，
// 且不返回没有可用模板的分类（提单页用）。
func (uc *CatalogUsecase) List(ctx context.Context, enabledOnly bool) ([]*Category, error) {
	categories, err := uc.reader.ListCategories(ctx)
	if err != nil {
		return nil, err
	}
	templates, err := uc.reader.ListTemplates(ctx, enabledOnly)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*Category, len(categories))
	for _, c := range categories {
		byID[c.ID] = c
	}
	for _, t := range templates {
		if c := byID[t.CategoryID]; c != nil {
			c.Templates = append(c.Templates, t)
		}
	}
	if !enabledOnly {
		return categories, nil
	}
	out := make([]*Category, 0, len(categories))
	for _, c := range categories {
		if len(c.Templates) > 0 {
			out = append(out, c)
		}
	}
	return out, nil
}

// cleanCategory 归一化分类的文本字段；名称清理后不得为空。
func cleanCategory(c *Category) error {
	c.Name, c.Description = clean(c.Name), clean(c.Description)
	if c.Name == "" {
		return detail(ErrTemplateInvalid, "category name must not be blank")
	}
	return nil
}

// CreateCategory 创建分类。
func (uc *CatalogUsecase) CreateCategory(ctx context.Context, c *Category) (*Category, error) {
	if err := cleanCategory(c); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	c.ID = id
	if err := uc.categories.CreateCategory(ctx, c, maxCategories); err != nil {
		return nil, err
	}
	return c, nil
}

// UpdateCategory 整组更新分类。
func (uc *CatalogUsecase) UpdateCategory(ctx context.Context, c *Category) error {
	if err := cleanCategory(c); err != nil {
		return err
	}
	return uc.categories.UpdateCategory(ctx, c)
}

// DeleteCategory 删除分类；分类下仍有模板时拒绝。
func (uc *CatalogUsecase) DeleteCategory(ctx context.Context, id string) error {
	return uc.categories.DeleteCategory(ctx, id)
}

// formTypes 模板表单允许的字段类型。
var formTypes = map[string]struct{}{
	formschema.TypeString: {}, formschema.TypeText: {}, formschema.TypeInt: {}, formschema.TypeFloat: {},
	formschema.TypeBool: {}, formschema.TypeEnum: {}, formschema.TypeDatetime: {},
	formschema.TypeUser: {}, formschema.TypeDept: {},
}

// validateForm 校验表单字段定义：数量、字段码唯一、类型合法、枚举有可选值、规则自洽。
// priority 是保留字段码（流程变量里表示工单优先级）。
func validateForm(form []FormField) error {
	if len(form) > maxFormFields {
		return detail(ErrTemplateInvalid, fmt.Sprintf("a form may have at most %d fields", maxFormFields))
	}
	seen := make(map[string]struct{}, len(form))
	for _, f := range form {
		bad := func(msg string) error {
			return detail(ErrTemplateInvalid, fmt.Sprintf("field %q %s", f.Code, msg))
		}
		if f.Code == priorityVar {
			return bad("uses a reserved code")
		}
		if _, dup := seen[f.Code]; dup {
			return bad("is duplicated")
		}
		seen[f.Code] = struct{}{}
		if _, ok := formTypes[f.Type]; !ok {
			return bad("has an unsupported type")
		}
		if f.Name == "" {
			return bad("needs a name")
		}
		if f.Type == formschema.TypeEnum && len(f.EnumValues) == 0 {
			return bad("needs at least one enum value")
		}
		r := f.Rule
		if r.Min != nil && r.Max != nil && *r.Min > *r.Max {
			return bad("has min greater than max")
		}
		if r.MinLen != nil && r.MaxLen != nil && *r.MinLen > *r.MaxLen {
			return bad("has minLen greater than maxLen")
		}
		if r.Pattern != nil && *r.Pattern != "" {
			if _, err := formschema.CompilePattern(*r.Pattern); err != nil {
				return bad("has an invalid pattern")
			}
		}
	}
	return nil
}

// validDuration 时限合法：单位已知、数值为正、折合不超过一年。
func validDuration(d *Duration) bool {
	minutes := int64(d.Value)
	switch d.Unit {
	case "minute":
	case "hour":
		minutes *= 60
	case "day":
		minutes *= 24 * 60
	default:
		return false
	}
	return d.Value >= 1 && minutes <= maxSLAMinutes
}

// validateSLA 校验模板的 SLA 规则与关联资源设置：每个优先级至多一条规则，每条至少配置
// 响应或解决时限之一。
func validateSLA(t *Template) error {
	if t.CILink < CILinkNone || t.CILink > CILinkRequired {
		return detail(ErrTemplateInvalid, "unknown resource link mode")
	}
	seen := map[int32]bool{}
	for _, r := range t.SLA {
		switch {
		case r.Priority < 1 || r.Priority > 4 || seen[r.Priority]:
			return detail(ErrTemplateInvalid, "each SLA rule must target a distinct priority")
		case r.Response == nil && r.Resolve == nil:
			return detail(ErrTemplateInvalid, "an SLA rule needs a response or a resolve time")
		case (r.Response != nil && !validDuration(r.Response)) || (r.Resolve != nil && !validDuration(r.Resolve)):
			return detail(ErrTemplateInvalid, "an SLA time is invalid")
		}
		seen[r.Priority] = true
	}
	return nil
}

// checkTemplate 模板保存前的归一化与校验：名称非空、表单定义合法、绑定的流程存在；
// 启用的模板还要求流程当前可发起（已发布且启用），否则目录里会出现一张提不了单的模板。
func (uc *CatalogUsecase) checkTemplate(ctx context.Context, t *Template) error {
	// 表单布局：未指定按单列；只支持单列 / 双列。
	if t.FormColumns == 0 {
		t.FormColumns = 1
	}
	if t.FormColumns != 1 && t.FormColumns != 2 {
		return detail(ErrTemplateInvalid, "form columns must be 1 or 2")
	}
	t.Name, t.Description = clean(t.Name), clean(t.Description)
	if t.Name == "" {
		return detail(ErrTemplateInvalid, "template name must not be blank")
	}
	for i := range t.Form {
		f := &t.Form[i]
		f.Name, f.Description = clean(f.Name), clean(f.Description)
		// 枚举值与正则同样要去掉 NUL：它们存进 jsonb，带着 \u0000 落库会报内部错误。
		for j := range f.EnumValues {
			f.EnumValues[j] = clean(f.EnumValues[j])
		}
		if f.Rule.Pattern != nil {
			p := strings.ReplaceAll(*f.Rule.Pattern, "\x00", "")
			f.Rule.Pattern = &p
		}
	}
	if err := validateForm(t.Form); err != nil {
		return err
	}
	if err := validateSLA(t); err != nil {
		return err
	}
	exists, startable, err := uc.workflows.KeyStartable(ctx, t.WorkflowKey)
	if err != nil {
		return err
	}
	if !exists {
		return detail(ErrTemplateInvalid, fmt.Sprintf("workflow %q does not exist", t.WorkflowKey))
	}
	if t.Status == TemplateEnabled && !startable {
		return detail(ErrTemplateInvalid, fmt.Sprintf("workflow %q is not published or is disabled", t.WorkflowKey))
	}
	return nil
}

// CreateTemplate 创建模板。
func (uc *CatalogUsecase) CreateTemplate(ctx context.Context, t *Template) (*Template, error) {
	if err := uc.checkTemplate(ctx, t); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	t.ID, t.UpdateTime = id, time.Now().UTC()
	if err := uc.templates.CreateTemplate(ctx, t, maxTemplates); err != nil {
		return nil, err
	}
	return t, nil
}

// UpdateTemplate 整组更新模板；已有工单保存的是提单时的表单快照，不受影响。
func (uc *CatalogUsecase) UpdateTemplate(ctx context.Context, t *Template) error {
	if err := uc.checkTemplate(ctx, t); err != nil {
		return err
	}
	return uc.templates.UpdateTemplate(ctx, t)
}

// DeleteTemplate 软删模板。
func (uc *CatalogUsecase) DeleteTemplate(ctx context.Context, id string) error {
	return uc.templates.DeleteTemplate(ctx, id)
}

// GetTemplate 模板详情（含表单）。已停用的模板只有可查看完整目录的人能读，
// 其余人按「不存在」处理（不泄露它的存在）。
func (uc *CatalogUsecase) GetTemplate(ctx context.Context, id, userID string) (*Template, map[string]string, error) {
	t, err := uc.reader.GetTemplate(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	manager, err := uc.managers.CanManageCatalog(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	if !manager {
		if t.Status != TemplateEnabled {
			return nil, nil, ErrTemplateNotFound
		}
		// 提单人只需要填表所需的内容（名称、说明、表单、关联资源要求）。SLA 规则、升级通知
		// 对象、绑定的流程与编号前缀是目录维护者的配置，不下发给提单人。
		t.SLA, t.SLAEscalate, t.WorkflowKey, t.NumberPrefix = nil, nil, "", ""
		return t, nil, nil
	}
	// SLA 升级对象的显示名（模板编辑页回显用）。
	labels, err := uc.dir.UserNames(ctx, t.SLAEscalate)
	return t, labels, err
}
