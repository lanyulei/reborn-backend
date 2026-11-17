// Package service 工单域 proto 接口实现：参数编排、模型转换与身份提取。
package service

import (
	"context"
	"strconv"
	"strings"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/ticket/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProviderSet is ticket 域 service providers.
var ProviderSet = wire.NewSet(NewCatalogService, NewTicketService)

// ts 零值时间转为 nil（proto 出参里「未发生」表达为字段缺失）。
func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func toProtoForm(form []biz.FormField) []*v1.FormField {
	out := make([]*v1.FormField, 0, len(form))
	for _, f := range form {
		pf := &v1.FormField{
			Code: f.Code, Name: f.Name, Type: f.Type, Required: f.Required, EnumValues: f.EnumValues,
			Rule: &v1.FormRule{
				Pattern: f.Rule.Pattern, Min: f.Rule.Min, Max: f.Rule.Max, MinLen: f.Rule.MinLen, MaxLen: f.Rule.MaxLen,
			},
			Description: f.Description,
			EnumLabels:  f.EnumLabels, Multiple: f.Multiple, Placeholder: f.Placeholder, Group: f.Group,
			Format: f.Format, Unit: f.Unit, Precision: f.Precision, Width: f.Width,
		}
		if f.Default != nil {
			// 初始值是保存时校验过的 JSON 取值；转换失败（不应发生）就当没有。
			if v, err := structpb.NewValue(f.Default); err == nil {
				pf.DefaultValue = v
			}
		}
		if f.ShowWhen != nil {
			pf.ShowWhen = &v1.FormShowWhen{Field: f.ShowWhen.Field, Values: f.ShowWhen.Values}
		}
		out = append(out, pf)
	}
	return out
}

func fromProtoForm(form []*v1.FormField) []biz.FormField {
	out := make([]biz.FormField, 0, len(form))
	for _, f := range form {
		r := f.GetRule()
		bf := biz.FormField{
			Code: f.GetCode(), Name: f.GetName(), Type: f.GetType(), Required: f.GetRequired(), EnumValues: f.GetEnumValues(),
			Rule:        formschema.Rule{Pattern: r.Pattern, Min: r.Min, Max: r.Max, MinLen: r.MinLen, MaxLen: r.MaxLen},
			Description: f.GetDescription(),
			EnumLabels:  f.GetEnumLabels(), Multiple: f.GetMultiple(), Placeholder: f.GetPlaceholder(), Group: f.GetGroup(),
			Format: f.GetFormat(), Unit: f.GetUnit(), Precision: f.Precision, Width: f.GetWidth(),
		}
		if f.GetDefaultValue() != nil {
			bf.Default = f.GetDefaultValue().AsInterface()
		}
		if w := f.GetShowWhen(); w != nil {
			bf.ShowWhen = &biz.ShowWhen{Field: w.GetField(), Values: w.GetValues()}
		}
		out = append(out, bf)
	}
	return out
}

func toProtoDuration(d *biz.Duration) *v1.SlaDuration {
	if d == nil {
		return nil
	}
	return &v1.SlaDuration{Value: d.Value, Unit: d.Unit, WorkTime: d.WorkTime}
}

func fromProtoDuration(d *v1.SlaDuration) *biz.Duration {
	if d == nil {
		return nil
	}
	return &biz.Duration{Value: d.GetValue(), Unit: d.GetUnit(), WorkTime: d.GetWorkTime()}
}

// toProtoTemplate 模板出参；detail 时带上表单、SLA 规则与升级对象（及其显示名）。
func toProtoTemplate(t *biz.Template, detail bool, labels map[string]string) *v1.Template {
	out := &v1.Template{
		Id: t.ID, CategoryId: t.CategoryID, Name: t.Name, Description: t.Description, WorkflowKey: t.WorkflowKey,
		NumberPrefix: t.NumberPrefix, Status: t.Status, Sort: t.Sort, UpdateTime: ts(t.UpdateTime), CiLink: t.CILink,
		Icon: t.Icon, FormColumns: t.FormColumns,
	}
	if detail {
		out.Form, out.SlaEscalateTo, out.Labels = toProtoForm(t.Form), t.SLAEscalate, labels
		for _, r := range t.SLA {
			out.Sla = append(out.Sla, &v1.SlaRule{Priority: r.Priority, Response: toProtoDuration(r.Response), Resolve: toProtoDuration(r.Resolve)})
		}
	}
	return out
}

func specToTemplate(id string, spec *v1.TemplateSpec) *biz.Template {
	t := &biz.Template{
		ID: id, CategoryID: spec.GetCategoryId(), Name: spec.GetName(), Description: spec.GetDescription(),
		WorkflowKey: spec.GetWorkflowKey(), NumberPrefix: spec.GetNumberPrefix(), Status: spec.GetStatus(),
		Sort: spec.GetSort(), Form: fromProtoForm(spec.GetForm()), CILink: spec.GetCiLink(),
		Icon: spec.GetIcon(),
		// 不传（0）按单列。
		FormColumns: max(spec.GetFormColumns(), 1),
	}
	for _, id := range spec.GetSlaEscalateTo() {
		t.SLAEscalate = append(t.SLAEscalate, strings.ToLower(id))
	}
	for _, r := range spec.GetSla() {
		t.SLA = append(t.SLA, biz.SLARule{Priority: r.GetPriority(), Response: fromProtoDuration(r.GetResponse()), Resolve: fromProtoDuration(r.GetResolve())})
	}
	return t
}

// CatalogService 实现 api/ticket/v1 生成的 TicketCatalogServiceServer。
type CatalogService struct {
	uc *biz.CatalogUsecase
}

// NewCatalogService new a CatalogService.
func NewCatalogService(uc *biz.CatalogUsecase) *CatalogService {
	return &CatalogService{uc: uc}
}

// ListCatalog implements proto TicketCatalogService/ListCatalog：只含可提单的。
func (s *CatalogService) ListCatalog(ctx context.Context, _ *v1.ListCatalogRequest) (*v1.ListCatalogResponse, error) {
	return s.list(ctx, true)
}

// ListManagedCatalog implements proto TicketCatalogService/ListManagedCatalog（由接口授权把关）。
func (s *CatalogService) ListManagedCatalog(ctx context.Context, _ *v1.ListManagedCatalogRequest) (*v1.ListCatalogResponse, error) {
	return s.list(ctx, false)
}

func (s *CatalogService) list(ctx context.Context, enabledOnly bool) (*v1.ListCatalogResponse, error) {
	categories, err := s.uc.List(ctx, enabledOnly)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListCatalogResponse{Items: make([]*v1.Category, 0, len(categories))}
	for _, c := range categories {
		pc := &v1.Category{Id: c.ID, Name: c.Name, Description: c.Description, Sort: c.Sort, Templates: make([]*v1.Template, 0, len(c.Templates))}
		for _, t := range c.Templates {
			pt := toProtoTemplate(t, false, nil)
			if enabledOnly {
				// 提单人的目录只需要名称与说明；绑定的流程、编号前缀是管理信息（与 GetTemplate 同口径）。
				pt.WorkflowKey, pt.NumberPrefix = "", ""
			}
			pc.Templates = append(pc.Templates, pt)
		}
		resp.Items = append(resp.Items, pc)
	}
	return resp, nil
}

// CreateCategory implements proto TicketCatalogService/CreateCategory.
func (s *CatalogService) CreateCategory(ctx context.Context, req *v1.CreateCategoryRequest) (*v1.CreateCategoryResponse, error) {
	c, err := s.uc.CreateCategory(ctx, &biz.Category{Name: req.GetName(), Description: req.GetDescription(), Sort: req.GetSort()})
	if err != nil {
		return nil, err
	}
	return &v1.CreateCategoryResponse{Category: &v1.Category{Id: c.ID, Name: c.Name, Description: c.Description, Sort: c.Sort}}, nil
}

// UpdateCategory implements proto TicketCatalogService/UpdateCategory.
func (s *CatalogService) UpdateCategory(ctx context.Context, req *v1.UpdateCategoryRequest) (*v1.UpdateCategoryResponse, error) {
	err := s.uc.UpdateCategory(ctx, &biz.Category{ID: req.GetId(), Name: req.GetName(), Description: req.GetDescription(), Sort: req.GetSort()})
	if err != nil {
		return nil, err
	}
	return &v1.UpdateCategoryResponse{}, nil
}

// DeleteCategory implements proto TicketCatalogService/DeleteCategory.
func (s *CatalogService) DeleteCategory(ctx context.Context, req *v1.DeleteCategoryRequest) (*v1.DeleteCategoryResponse, error) {
	if err := s.uc.DeleteCategory(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteCategoryResponse{}, nil
}

// CreateTemplate implements proto TicketCatalogService/CreateTemplate.
func (s *CatalogService) CreateTemplate(ctx context.Context, req *v1.CreateTemplateRequest) (*v1.CreateTemplateResponse, error) {
	t, err := s.uc.CreateTemplate(ctx, specToTemplate("", req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateTemplateResponse{Template: toProtoTemplate(t, true, nil)}, nil
}

// UpdateTemplate implements proto TicketCatalogService/UpdateTemplate.
func (s *CatalogService) UpdateTemplate(ctx context.Context, req *v1.UpdateTemplateRequest) (*v1.UpdateTemplateResponse, error) {
	if err := s.uc.UpdateTemplate(ctx, specToTemplate(req.GetId(), req.GetSpec())); err != nil {
		return nil, err
	}
	return &v1.UpdateTemplateResponse{}, nil
}

// DeleteTemplate implements proto TicketCatalogService/DeleteTemplate.
func (s *CatalogService) DeleteTemplate(ctx context.Context, req *v1.DeleteTemplateRequest) (*v1.DeleteTemplateResponse, error) {
	if err := s.uc.DeleteTemplate(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteTemplateResponse{}, nil
}

// GetTemplate implements proto TicketCatalogService/GetTemplate.
func (s *CatalogService) GetTemplate(ctx context.Context, req *v1.GetTemplateRequest) (*v1.GetTemplateResponse, error) {
	uid, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	t, labels, err := s.uc.GetTemplate(ctx, req.GetId(), uid)
	if err != nil {
		return nil, err
	}
	return &v1.GetTemplateResponse{Template: toProtoTemplate(t, true, labels)}, nil
}

// TicketService 实现 api/ticket/v1 生成的 TicketServiceServer。
type TicketService struct {
	uc *biz.Usecase
}

// NewTicketService new a TicketService.
func NewTicketService(uc *biz.Usecase) *TicketService {
	return &TicketService{uc: uc}
}

func toProtoTicket(t *biz.Ticket, names map[string]string) *v1.Ticket {
	return &v1.Ticket{
		Id: t.ID, Number: t.Number, TemplateId: t.TemplateID, TemplateName: t.TemplateName, CategoryName: t.CategoryName,
		Title: t.Title, Priority: t.Priority, Status: t.Status, Creator: t.Creator, CreatorName: names[t.Creator],
		InstanceId: t.InstanceID, CreateTime: ts(t.CreateTime), EndTime: ts(t.EndTime),
		ResponseDue: ts(t.ResponseDue), ResolveDue: ts(t.ResolveDue), ResponseTime: ts(t.ResponseTime),
		Rating: t.Rating, RatingComment: t.RatingComment, RatingTime: ts(t.RatingTime),
		FormColumns: t.FormColumns,
	}
}

// CreateTicket implements proto TicketService/CreateTicket：提单人取自登录身份。
func (s *TicketService) CreateTicket(ctx context.Context, req *v1.CreateTicketRequest) (*v1.CreateTicketResponse, error) {
	uid, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	t, err := s.uc.Create(ctx, biz.CreateParams{
		TemplateID: req.GetTemplateId(), Title: req.GetTitle(), Priority: req.GetPriority(), Data: req.GetData().AsMap(),
		Creator: uid, CreatorDept: req.GetCreatorDept(), FileIDs: req.GetFileIds(), RequestID: req.GetRequestId(),
		ResourceIDs: lowerAll(req.GetCiIds()),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateTicketResponse{Ticket: toProtoTicket(t, nil)}, nil
}

// GetTicket implements proto TicketService/GetTicket.
func (s *TicketService) GetTicket(ctx context.Context, req *v1.GetTicketRequest) (*v1.GetTicketResponse, error) {
	uid, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.uc.Get(ctx, req.GetId(), uid)
	if err != nil {
		return nil, err
	}
	t := toProtoTicket(d.Ticket, d.Labels)
	t.Form = toProtoForm(d.Ticket.Form)
	// 表单数据来自本域校验归一后的 JSON 对象；个别值无法表达为 Struct 时退化为空对象。
	if t.Data, err = structpb.NewStruct(d.Ticket.Data); err != nil {
		t.Data = &structpb.Struct{}
	}
	resp := &v1.GetTicketResponse{
		Ticket: t, Labels: d.Labels, Files: make([]*v1.TicketFile, 0, len(d.Files)),
		Resources:        make([]*v1.TicketResource, 0, len(d.Resources)),
		FieldPermissions: d.Permissions, CanEditForm: d.CanEditForm, CanRate: d.CanRate,
	}
	for _, f := range d.Files {
		resp.Files = append(resp.Files, &v1.TicketFile{Id: f.ID, Filename: f.Filename, Size: f.Size})
	}
	for _, rs := range d.Resources {
		resp.Resources = append(resp.Resources, &v1.TicketResource{Id: rs.ID, Name: rs.Name, ModelName: rs.ModelName})
	}
	return resp, nil
}

func (s *TicketService) list(ctx context.Context, filter biz.TicketFilter, page int64, pageSize int32) (*v1.ListTicketsResponse, error) {
	items, names, total, err := s.uc.List(ctx, filter, page, int64(pageSize))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListTicketsResponse{Items: make([]*v1.Ticket, 0, len(items)), Total: total}
	for _, t := range items {
		resp.Items = append(resp.Items, toProtoTicket(t, names))
	}
	return resp, nil
}

// ListMyTickets implements proto TicketService/ListMyTickets：恒按登录身份过滤。
func (s *TicketService) ListMyTickets(ctx context.Context, req *v1.ListMyTicketsRequest) (*v1.ListTicketsResponse, error) {
	uid, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return s.list(ctx, biz.TicketFilter{Keyword: req.GetKeyword(), Status: req.Status, Creator: uid}, req.GetPage(), req.GetPageSize())
}

// ListTickets implements proto TicketService/ListTickets（管理视图，由接口授权把关）。
func (s *TicketService) ListTickets(ctx context.Context, req *v1.ListTicketsRequest) (*v1.ListTicketsResponse, error) {
	return s.list(ctx, biz.TicketFilter{
		Keyword: req.GetKeyword(), Status: req.Status, TemplateID: req.GetTemplateId(), Priority: req.Priority,
		ResourceID: strings.ToLower(req.GetCiId()), Overdue: req.GetOverdue(),
	}, req.GetPage(), req.GetPageSize())
}

// ListTicketComments implements proto TicketService/ListTicketComments.
func (s *TicketService) ListTicketComments(ctx context.Context, req *v1.ListTicketCommentsRequest) (*v1.ListTicketCommentsResponse, error) {
	uid, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	comments, names, err := s.uc.Comments(ctx, req.GetId(), uid)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListTicketCommentsResponse{Items: make([]*v1.Comment, 0, len(comments))}
	for _, c := range comments {
		resp.Items = append(resp.Items, &v1.Comment{
			Id: c.ID, UserId: c.UserID, UserName: names[c.UserID], Content: c.Content, CreateTime: ts(c.CreateTime), ParentId: c.ParentID,
		})
	}
	return resp, nil
}

// AddTicketComment implements proto TicketService/AddTicketComment.
func (s *TicketService) AddTicketComment(ctx context.Context, req *v1.AddTicketCommentRequest) (*v1.AddTicketCommentResponse, error) {
	uid, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	c, name, err := s.uc.AddComment(ctx, req.GetId(), uid, req.GetContent(), strings.ToLower(req.GetParentId()))
	if err != nil {
		return nil, err
	}
	return &v1.AddTicketCommentResponse{Comment: &v1.Comment{
		Id: c.ID, UserId: c.UserID, UserName: name, Content: c.Content, CreateTime: ts(c.CreateTime), ParentId: c.ParentID,
	}}, nil
}

func lowerAll(ids []string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = strings.ToLower(id)
	}
	return out
}

// UpdateTicketForm implements proto TicketService/UpdateTicketForm.
func (s *TicketService) UpdateTicketForm(ctx context.Context, req *v1.UpdateTicketFormRequest) (*v1.UpdateTicketFormResponse, error) {
	uid, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.UpdateForm(ctx, req.GetId(), uid, req.GetData().AsMap()); err != nil {
		return nil, err
	}
	return &v1.UpdateTicketFormResponse{}, nil
}

// RateTicket implements proto TicketService/RateTicket.
func (s *TicketService) RateTicket(ctx context.Context, req *v1.RateTicketRequest) (*v1.RateTicketResponse, error) {
	uid, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Rate(ctx, req.GetId(), uid, req.GetRating(), req.GetComment()); err != nil {
		return nil, err
	}
	return &v1.RateTicketResponse{}, nil
}

// counts 把「码 → 数量」按码升序铺成列表（缺的码补 0，前端不用再判空）。
func counts(m map[int32]int64, codes ...int32) []*v1.TicketCount {
	out := make([]*v1.TicketCount, 0, len(codes))
	for _, c := range codes {
		out = append(out, &v1.TicketCount{Key: strconv.Itoa(int(c)), Count: m[c]})
	}
	return out
}

// GetTicketStats implements proto TicketService/GetTicketStats（管理视图，由接口授权把关）。
func (s *TicketService) GetTicketStats(ctx context.Context, req *v1.GetTicketStatsRequest) (*v1.GetTicketStatsResponse, error) {
	st, err := s.uc.Stats(ctx, req.GetDays(), req.GetTimezone())
	if err != nil {
		return nil, err
	}
	resp := &v1.GetTicketStatsResponse{
		Total: st.Total, ByStatus: counts(st.ByStatus, 1, 2, 3, 4, 5), ByPriority: counts(st.ByPriority, 1, 2, 3, 4),
		AvgResolveSeconds: st.AvgResolveSeconds, ResponseSlaTotal: st.ResponseTotal, ResponseSlaMet: st.ResponseMet,
		ResolveSlaTotal: st.ResolveTotal, ResolveSlaMet: st.ResolveMet, Rated: st.Rated, AvgRating: st.AvgRating,
		ByTemplate: make([]*v1.TicketCount, 0, len(st.ByTemplate)), Trend: make([]*v1.TicketTrendPoint, 0, len(st.Trend)),
		Timezone: st.Timezone,
	}
	for _, c := range st.ByTemplate {
		resp.ByTemplate = append(resp.ByTemplate, &v1.TicketCount{Key: c.Name, Count: c.Count})
	}
	for _, p := range st.Trend {
		resp.Trend = append(resp.Trend, &v1.TicketTrendPoint{Day: p.Day, Created: p.Created, Closed: p.Closed})
	}
	return resp, nil
}
