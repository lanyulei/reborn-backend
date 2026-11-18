package biz

import (
	"context"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// TemplateUsecase 作业模板用例。
type TemplateUsecase struct {
	reader  TemplateReader
	writer  TemplateWriter
	targets *TargetUsecase
	source  HostSource
	dir     Directory
}

// NewTemplateUsecase new a TemplateUsecase.
func NewTemplateUsecase(reader TemplateReader, writer TemplateWriter, targets *TargetUsecase, source HostSource, dir Directory) *TemplateUsecase {
	return &TemplateUsecase{reader: reader, writer: writer, targets: targets, source: source, dir: dir}
}

// List 分页查询作业模板。
func (uc *TemplateUsecase) List(ctx context.Context, userID string, filter TemplateFilter, page, pageSize int64) ([]*Template, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	items, total, err := uc.reader.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	return items, total, uc.fillNames(ctx, userID, items)
}

// Get 查询作业模板详情。
func (uc *TemplateUsecase) Get(ctx context.Context, userID, id string) (*Template, error) {
	t, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return t, uc.fillNames(ctx, userID, []*Template{t})
}

// fillNames 填充更新人显示名与目标资源类型名称（后者按当前用户的数据范围，范围外留空）。
func (uc *TemplateUsecase) fillNames(ctx context.Context, userID string, items []*Template) error {
	if len(items) == 0 {
		return nil
	}
	users, codes := make([]string, 0, len(items)), make([]string, 0, len(items))
	for _, t := range items {
		users, codes = append(users, t.UpdateUser), append(codes, t.Target.ModelCode)
	}
	userNames, err := uc.dir.UserNames(ctx, uniq(users))
	if err != nil {
		return err
	}
	modelNames, err := uc.source.ModelNames(ctx, userID, uniq(codes))
	if err != nil {
		return err
	}
	for _, t := range items {
		t.UpdateUserName, t.ModelName = userNames[t.UpdateUser], modelNames[t.Target.ModelCode]
	}
	return nil
}

// normalize 规整并校验模板内容。目标主机的 CMDB 侧校验按保存人的数据范围：不能把模板指向自己
// 看不到的资源类型。
func (uc *TemplateUsecase) normalize(ctx context.Context, userID string, t *Template) error {
	t.Name = strings.TrimSpace(t.Name)
	t.Description = strings.TrimSpace(t.Description)
	t.Playbook = strings.TrimSpace(t.Playbook)
	switch {
	case t.Name == "":
		return detail(ErrTemplateInvalid, "name must not be blank")
	case !jc.ValidPlaybook(t.Playbook):
		return detail(ErrTemplateInvalid, "playbook must be a relative path inside the repository ending with .yml or .yaml, without .. segments")
	case !jc.ValidZone(t.RunnerZone):
		return detail(ErrTemplateInvalid, "invalid runner zone")
	case t.SliceSize < 0 || t.SliceSize > jc.MaxHosts:
		return detail(ErrTemplateInvalid, "slice size must be between 0 and %d", jc.MaxHosts)
	case t.SliceSize > 0 && t.Exclusive:
		// 切片把一次提交拆成多条并行的执行，与「同一模板同时只能有一条执行」互相矛盾。
		return detail(ErrTemplateInvalid, "job slicing cannot be combined with forbidding concurrent runs")
	}
	if err := t.Options.Normalize(); err != nil {
		return detail(ErrTemplateInvalid, "%s", err.Error())
	}
	if err := validateParams(t.Params); err != nil {
		return detail(ErrTemplateInvalid, "%s", err.Error())
	}
	if t.Params == nil {
		t.Params = []jc.Param{}
	}
	return uc.targets.check(ctx, userID, &t.Target)
}

// Create 新建作业模板。
func (uc *TemplateUsecase) Create(ctx context.Context, userID string, t *Template) (*Template, error) {
	if err := uc.normalize(ctx, userID, t); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	t.ID = id
	if err := uc.writer.Create(ctx, t, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, userID, id)
}

// Update 编辑作业模板（整体替换）；已提交的执行不受影响。
func (uc *TemplateUsecase) Update(ctx context.Context, userID string, t *Template, version int32) (*Template, error) {
	if err := uc.normalize(ctx, userID, t); err != nil {
		return nil, err
	}
	if err := uc.writer.Update(ctx, t, version, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, userID, t.ID)
}

// Delete 删除作业模板；执行记录保留。
func (uc *TemplateUsecase) Delete(ctx context.Context, userID, id string) error {
	return uc.writer.Delete(ctx, id, userID)
}

// PreviewTarget 预览一份尚未保存的目标主机配置会选中哪些主机。
func (uc *TemplateUsecase) PreviewTarget(ctx context.Context, userID string, target jc.Target) (HostSet, error) {
	return uc.targets.Resolve(ctx, userID, target)
}

// Hosts 模板当前会选中、且在用户数据范围内的主机。
func (uc *TemplateUsecase) Hosts(ctx context.Context, userID, id string) (HostSet, error) {
	t, err := uc.reader.Get(ctx, id)
	if err != nil {
		return HostSet{}, err
	}
	return uc.targets.Resolve(ctx, userID, t.Target)
}
