package service

import (
	"context"
	"strings"

	v1 "github.com/lanyulei/reborn/backend/core/api/oncall/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/go-kratos/kratos/v3/transport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// OncallShareService 实现 OncallShareServiceServer。
type OncallShareService struct {
	uc    *biz.ShareUsecase
	admin AdminResolver
}

// NewOncallShareService new an OncallShareService.
func NewOncallShareService(uc *biz.ShareUsecase, admin AdminResolver) *OncallShareService {
	return &OncallShareService{uc: uc, admin: admin}
}

func toProtoShareLink(l *biz.ShareLink) *v1.ShareLink {
	out := &v1.ShareLink{
		Id: l.ID, ScopeType: l.ScopeType, ScopeId: l.ScopeID, Name: l.Name, ShowPhone: l.ShowPhone,
		CreatorName: l.CreatorName, CreateTime: timestamppb.New(l.CreateTime),
	}
	if l.ExpireTime != nil {
		out.ExpireTime = timestamppb.New(*l.ExpireTime)
	}
	if l.LastAccessTime != nil {
		out.LastAccessTime = timestamppb.New(*l.LastAccessTime)
	}
	return out
}

// ListShareLinks implements proto OncallShareService/ListShareLinks.
func (s *OncallShareService) ListShareLinks(ctx context.Context, req *v1.ListShareLinksRequest) (*v1.ListShareLinksResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	links, err := s.uc.List(ctx, actor, req.GetScopeType(), strings.ToLower(req.GetScopeId()))
	if err != nil {
		return nil, err
	}
	items := make([]*v1.ShareLink, 0, len(links))
	for _, l := range links {
		items = append(items, toProtoShareLink(l))
	}
	return &v1.ListShareLinksResponse{Items: items}, nil
}

// CreateShareLink implements proto OncallShareService/CreateShareLink.
func (s *OncallShareService) CreateShareLink(ctx context.Context, req *v1.CreateShareLinkRequest) (*v1.CreateShareLinkResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	in := biz.ShareInput{ScopeType: req.GetScopeType(), ScopeID: strings.ToLower(req.GetScopeId()), Name: req.GetName(), ShowPhone: req.GetShowPhone()}
	if req.ExpireTime != nil {
		t := req.GetExpireTime().AsTime()
		in.ExpireTime = &t
	}
	link, token, err := s.uc.Create(ctx, actor, in)
	if err != nil {
		return nil, err
	}
	return &v1.CreateShareLinkResponse{Link: toProtoShareLink(link), Token: token}, nil
}

// GetShareLinkToken implements proto OncallShareService/GetShareLinkToken.
func (s *OncallShareService) GetShareLinkToken(ctx context.Context, req *v1.GetShareLinkTokenRequest) (*v1.GetShareLinkTokenResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	token, err := s.uc.Token(ctx, actor, strings.ToLower(req.GetId()))
	if err != nil {
		return nil, err
	}
	return &v1.GetShareLinkTokenResponse{Token: token}, nil
}

// ResetShareLink implements proto OncallShareService/ResetShareLink.
func (s *OncallShareService) ResetShareLink(ctx context.Context, req *v1.ResetShareLinkRequest) (*v1.ResetShareLinkResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	token, err := s.uc.Reset(ctx, actor, strings.ToLower(req.GetId()))
	if err != nil {
		return nil, err
	}
	return &v1.ResetShareLinkResponse{Token: token}, nil
}

// DeleteShareLink implements proto OncallShareService/DeleteShareLink.
func (s *OncallShareService) DeleteShareLink(ctx context.Context, req *v1.DeleteShareLinkRequest) (*v1.DeleteShareLinkResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, actor, strings.ToLower(req.GetId())); err != nil {
		return nil, err
	}
	return &v1.DeleteShareLinkResponse{}, nil
}

// publicHeaders 匿名响应头：地址里带令牌，禁止缓存与 Referer 外泄；禁止被搜索引擎收录。
func publicHeaders(ctx context.Context) {
	if tr, ok := transport.FromServerContext(ctx); ok {
		h := tr.ReplyHeader()
		h.Set("Cache-Control", "no-store")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Robots-Tag", "noindex, nofollow")
	}
}

// GetPublicCalendar implements proto OncallShareService/GetPublicCalendar（免登录）。
func (s *OncallShareService) GetPublicCalendar(ctx context.Context, req *v1.GetPublicCalendarRequest) (*v1.GetPublicCalendarResponse, error) {
	publicHeaders(ctx)
	cal, err := s.uc.Public(ctx, req.GetToken(), systemservice.ClientIP(ctx), req.GetStartTime().AsTime(), req.GetEndTime().AsTime())
	if err != nil {
		return nil, err
	}
	items := make([]*v1.PublicShift, 0, len(cal.Shifts))
	for _, sh := range cal.Shifts {
		items = append(items, &v1.PublicShift{
			ScheduleName: sh.ScheduleName, Role: sh.Role, UserName: sh.UserName, Phone: sh.UserPhone,
			StartTime: timestamppb.New(sh.Start), EndTime: timestamppb.New(sh.End), Source: sh.Source,
		})
	}
	return &v1.GetPublicCalendarResponse{Title: cal.Title, Description: cal.Description, ScopeType: cal.ScopeType, Items: items}, nil
}
