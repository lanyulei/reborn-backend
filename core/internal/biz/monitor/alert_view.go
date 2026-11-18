package biz

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 聚合视图的限额。
const (
	// MaxPersonalViews 每人的个人聚合视图数上限。
	MaxPersonalViews = 50
	// maxListedViews 一次返回的聚合视图数上限。
	maxListedViews = 200
	maxViewLabels  = 5
)

// SavedViewSpec 聚合视图的可编辑内容。
type SavedViewSpec struct {
	Name     string
	By       string
	Filters  mc.AlertViewFilter
	IsPublic bool
}

// SavedView 活跃告警的聚合视图（monitor_alert_view）：保存的聚合维度与过滤条件。
type SavedView struct {
	ID         string
	Spec       SavedViewSpec
	OwnerID    string
	CreateTime time.Time
	UpdateTime time.Time
}

// SavedViewItem 聚合视图及其展示信息。
type SavedViewItem struct {
	*SavedView
	OwnerName string
	Mine      bool
	CanEdit   bool
}

// SavedViewReader 聚合视图读仓储。
type SavedViewReader interface {
	// ListVisible 全部公共视图 + ownerID 的个人视图：公共在前，各自按名称排序，至多 limit 个。
	ListVisible(ctx context.Context, ownerID string, limit int) ([]*SavedView, error)
	// Get 不存在返回 ErrAlertViewNotFound。
	Get(ctx context.Context, id string) (*SavedView, error)
}

// SavedViewWriter 聚合视图写仓储。Update / Delete 以读取时的所有者与公共标记围栏：判定权限所依据的
// 内容若被并发修改，写入不命中并返回 ErrAlertViewNotFound。
type SavedViewWriter interface {
	// Create 同一所有者下重名返回 ErrAlertViewNameExists；新建的是个人视图且该所有者的个人视图数已达
	// maxPersonal 时返回 ErrAlertViewInvalid（事务内判定，并发下不会超限）。回填时间。
	Create(ctx context.Context, v *SavedView, maxPersonal int) error
	// Update 整体替换内容（所有者不变）；重名返回 ErrAlertViewNameExists。回填更新时间。
	Update(ctx context.Context, v *SavedView, wasPublic bool) error
	Delete(ctx context.Context, id, ownerID string, wasPublic bool) error
}

// AlertViewUsecase 活跃告警聚合视图。个人视图只有本人可见、可改；公共视图所有人可见，只有超级管理员
// 可以创建、修改与删除。视图只保存展示方式：打开视图时聚合接口照常按调用方自己的数据范围取数。
type AlertViewUsecase struct {
	scopes *ScopeUsecase
	reader SavedViewReader
	writer SavedViewWriter
	dir    Directory
}

// NewAlertViewUsecase new an AlertViewUsecase.
func NewAlertViewUsecase(scopes *ScopeUsecase, reader SavedViewReader, writer SavedViewWriter, dir Directory) *AlertViewUsecase {
	return &AlertViewUsecase{scopes: scopes, reader: reader, writer: writer, dir: dir}
}

func viewInvalid(format string, args ...any) error {
	return detail(ErrAlertViewInvalid, format, args...)
}

// viewEditable 能否修改 / 删除：公共视图须是超级管理员，个人视图须是本人。
func viewEditable(scope Scope, userID string, v *SavedView) bool {
	if v.Spec.IsPublic {
		return scope.IsAdmin()
	}
	return v.OwnerID == userID
}

// validateView 校验并规整视图内容。过滤条件里的分组须存在且调用方可读（不可读与不存在是同一句话，
// 不泄露分组是否存在）。
func validateView(scope Scope, spec *SavedViewSpec) error {
	if spec.Name = strings.TrimSpace(spec.Name); spec.Name == "" || len([]rune(spec.Name)) > 64 {
		return viewInvalid("name must be 1-64 characters and not blank")
	}
	if label, ok := strings.CutPrefix(spec.By, AggregateByLabel+":"); ok {
		if !mc.ValidLabelName(label) {
			return viewInvalid("invalid label name in by")
		}
	} else if !slices.Contains([]string{AggregateByRule, AggregateBySeverity, AggregateByGroup}, spec.By) {
		return viewInvalid("by must be rule, severity, group or label:<name>")
	}
	f := &spec.Filters
	if f.GroupID != "" && (!scope.GroupExists(f.GroupID) || !scope.CanRead(f.GroupID)) {
		return viewInvalid("the rule group in filters does not exist")
	}
	if f.DatasourceID != "" && !idgen.IsUUIDFold(f.DatasourceID) {
		return viewInvalid("invalid datasource in filters")
	}
	if len(f.Severities) > 4 || hasDuplicate(f.Severities) || slices.ContainsFunc(f.Severities, func(s int32) bool { return !mc.ValidSeverity(s) }) {
		return viewInvalid("severities in filters must be distinct values between 1 and 4")
	}
	if len(f.Labels) > maxViewLabels {
		return viewInvalid("at most %d label filters", maxViewLabels)
	}
	for _, item := range f.Labels {
		name, value, ok := strings.Cut(item, "=")
		if !ok || !mc.ValidLabelName(name) || value == "" || len(item) > 400 {
			return viewInvalid("label filters must look like name=value")
		}
	}
	return nil
}

// List 我能看到的聚合视图：全部公共视图 + 我的个人视图。
func (uc *AlertViewUsecase) List(ctx context.Context, userID string) ([]*SavedViewItem, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	views, err := uc.reader.ListVisible(ctx, userID, maxListedViews)
	if err != nil {
		return nil, err
	}
	return uc.items(ctx, scope, userID, views)
}

// items 批量补齐所有者显示名与本人相关的标记。
func (uc *AlertViewUsecase) items(ctx context.Context, scope Scope, userID string, views []*SavedView) ([]*SavedViewItem, error) {
	owners := make([]string, 0, len(views))
	for _, v := range views {
		owners = append(owners, v.OwnerID)
	}
	names := map[string]string{}
	if owners = uniq(owners); len(owners) > 0 {
		var err error
		if names, err = uc.dir.UserNames(ctx, owners); err != nil {
			return nil, err
		}
	}
	out := make([]*SavedViewItem, 0, len(views))
	for _, v := range views {
		out = append(out, &SavedViewItem{SavedView: v, OwnerName: names[v.OwnerID], Mine: v.OwnerID == userID, CanEdit: viewEditable(scope, userID, v)})
	}
	return out, nil
}

func (uc *AlertViewUsecase) item(ctx context.Context, scope Scope, userID string, v *SavedView) (*SavedViewItem, error) {
	items, err := uc.items(ctx, scope, userID, []*SavedView{v})
	if err != nil {
		return nil, err
	}
	return items[0], nil
}

// Create 保存一个聚合视图。公共视图仅超级管理员可建。
func (uc *AlertViewUsecase) Create(ctx context.Context, userID string, spec SavedViewSpec) (*SavedViewItem, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if spec.IsPublic && !scope.IsAdmin() {
		return nil, ErrForbidden
	}
	if err := validateView(scope, &spec); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	v := &SavedView{ID: id, Spec: spec, OwnerID: userID}
	if err := uc.writer.Create(ctx, v, MaxPersonalViews); err != nil {
		return nil, err
	}
	return uc.item(ctx, scope, userID, v)
}

// editable 读取视图并要求调用方能修改它；否则与不存在同样返回 ErrAlertViewNotFound。
func (uc *AlertViewUsecase) editable(ctx context.Context, userID, id string) (Scope, *SavedView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return Scope{}, nil, err
	}
	v, err := uc.reader.Get(ctx, id)
	if err != nil {
		return Scope{}, nil, err
	}
	if !viewEditable(scope, userID, v) {
		return Scope{}, nil, ErrAlertViewNotFound
	}
	return scope, v, nil
}

// Update 修改聚合视图（整体替换，所有者不变）。把视图设为公共须是超级管理员。
func (uc *AlertViewUsecase) Update(ctx context.Context, userID, id string, spec SavedViewSpec) (*SavedViewItem, error) {
	scope, cur, err := uc.editable(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if spec.IsPublic && !scope.IsAdmin() {
		return nil, ErrForbidden
	}
	if err := validateView(scope, &spec); err != nil {
		return nil, err
	}
	next := &SavedView{ID: cur.ID, Spec: spec, OwnerID: cur.OwnerID, CreateTime: cur.CreateTime}
	if err := uc.writer.Update(ctx, next, cur.Spec.IsPublic); err != nil {
		return nil, err
	}
	return uc.item(ctx, scope, userID, next)
}

// Delete 删除聚合视图：权限同修改。
func (uc *AlertViewUsecase) Delete(ctx context.Context, userID, id string) error {
	_, cur, err := uc.editable(ctx, userID, id)
	if err != nil {
		return err
	}
	return uc.writer.Delete(ctx, id, cur.OwnerID, cur.Spec.IsPublic)
}
