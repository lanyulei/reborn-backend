package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/go-kratos/kratos/v3/transport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// UpdateService 实现 IncidentUpdateServiceHTTPServer：公告、AI 草稿、分享链接、匿名访问。
type UpdateService struct {
	uc *biz.UpdateUsecase
}

// NewUpdateService new an UpdateService.
func NewUpdateService(uc *biz.UpdateUsecase) *UpdateService { return &UpdateService{uc: uc} }

func toProtoUpdate(u *biz.UpdateView) *v1.IncidentUpdate {
	return &v1.IncidentUpdate{Id: u.ID, Kind: u.Kind, Content: u.Content, Author: named(u.AuthorID, u.AuthorName), CreateTime: timestamppb.New(u.CreateTime)}
}

// ListUpdates implements proto IncidentUpdateService/ListUpdates.
func (s *UpdateService) ListUpdates(ctx context.Context, req *v1.ListUpdatesRequest) (*v1.ListUpdatesResponse, error) {
	if _, err := userID(ctx); err != nil {
		return nil, err
	}
	items, err := s.uc.List(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	out := &v1.ListUpdatesResponse{Items: make([]*v1.IncidentUpdate, 0, len(items))}
	for _, u := range items {
		out.Items = append(out.Items, toProtoUpdate(u))
	}
	return out, nil
}

// CreateUpdate implements proto IncidentUpdateService/CreateUpdate.
func (s *UpdateService) CreateUpdate(ctx context.Context, req *v1.CreateUpdateRequest) (*v1.UpdateResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	u, err := s.uc.Create(ctx, uid, req.GetId(), req.GetKind(), req.GetContent())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateResponse{Update: toProtoUpdate(u)}, nil
}

// DraftUpdate implements proto IncidentUpdateService/DraftUpdate.
func (s *UpdateService) DraftUpdate(ctx context.Context, req *v1.DraftUpdateRequest) (*v1.DraftResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	text, err := s.uc.Draft(ctx, uid, req.GetId(), req.GetKind())
	if err != nil {
		return nil, err
	}
	return &v1.DraftResponse{Content: text}, nil
}

func toProtoShare(l *biz.ShareLink, creatorName string) *v1.ShareLink {
	return &v1.ShareLink{
		Id: l.ID, Name: l.Name, ExpireTime: protoTime(l.ExpireTime), AccessCount: l.AccessCount, LastAccessTime: protoTime(l.LastAccessTime),
		Creator: named(l.CreateUser, creatorName), CreateTime: timestamppb.New(l.CreateTime),
	}
}

// ListShareLinks implements proto IncidentUpdateService/ListShareLinks.
func (s *UpdateService) ListShareLinks(ctx context.Context, req *v1.ListShareLinksRequest) (*v1.ListShareLinksResponse, error) {
	if _, err := userID(ctx); err != nil {
		return nil, err
	}
	items, err := s.uc.ListShares(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	out := &v1.ListShareLinksResponse{Items: make([]*v1.ShareLink, 0, len(items))}
	for _, l := range items {
		out.Items = append(out.Items, toProtoShare(l.ShareLink, l.CreatorName))
	}
	return out, nil
}

// CreateShareLink implements proto IncidentUpdateService/CreateShareLink.
func (s *UpdateService) CreateShareLink(ctx context.Context, req *v1.CreateShareLinkRequest) (*v1.ShareLinkTokenResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	l, token, err := s.uc.CreateShare(ctx, uid, req.GetId(), req.GetName(), int(req.GetExpireHours()))
	if err != nil {
		return nil, err
	}
	return &v1.ShareLinkTokenResponse{Link: toProtoShare(l, ""), Token: token}, nil
}

// GetShareLinkToken implements proto IncidentUpdateService/GetShareLinkToken.
func (s *UpdateService) GetShareLinkToken(ctx context.Context, req *v1.GetShareLinkTokenRequest) (*v1.ShareLinkTokenResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	l, token, err := s.uc.ShareToken(ctx, uid, req.GetId(), req.GetShareId())
	if err != nil {
		return nil, err
	}
	return &v1.ShareLinkTokenResponse{Link: toProtoShare(l, ""), Token: token}, nil
}

// ResetShareLink implements proto IncidentUpdateService/ResetShareLink.
func (s *UpdateService) ResetShareLink(ctx context.Context, req *v1.ResetShareLinkRequest) (*v1.ShareLinkTokenResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	l, token, err := s.uc.ResetShare(ctx, uid, req.GetId(), req.GetShareId())
	if err != nil {
		return nil, err
	}
	return &v1.ShareLinkTokenResponse{Link: toProtoShare(l, ""), Token: token}, nil
}

// DeleteShareLink implements proto IncidentUpdateService/DeleteShareLink.
func (s *UpdateService) DeleteShareLink(ctx context.Context, req *v1.DeleteShareLinkRequest) (*v1.EmptyUpdateResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.DeleteShare(ctx, uid, req.GetId(), req.GetShareId()); err != nil {
		return nil, err
	}
	return &v1.EmptyUpdateResponse{}, nil
}

// publicHeaders 匿名页面的响应头：不缓存、不带 referrer、不被索引。
func publicHeaders(ctx context.Context) {
	if tr, ok := transport.FromServerContext(ctx); ok {
		h := tr.ReplyHeader()
		h.Set("Cache-Control", "no-store")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Robots-Tag", "noindex, nofollow")
	}
}

// GetPublicIncident implements proto IncidentUpdateService/GetPublicIncident（免登录）。
func (s *UpdateService) GetPublicIncident(ctx context.Context, req *v1.GetPublicIncidentRequest) (*v1.GetPublicIncidentResponse, error) {
	publicHeaders(ctx)
	v, err := s.uc.Public(ctx, req.GetToken(), systemservice.ClientIP(ctx))
	if err != nil {
		return nil, err
	}
	out := &v1.GetPublicIncidentResponse{
		Number: v.Incident.Number, Title: v.Incident.Title, Severity: v.Incident.Severity, Status: v.Incident.Status, BusinessName: v.BusinessName,
		OccurTime: timestamppb.New(v.Incident.OccurTime), ResolveTime: protoTime(v.Incident.ResolveTime), IsDrill: v.Incident.IsDrill,
	}
	for _, u := range v.Updates {
		pu := toProtoUpdate(u)
		// 匿名页面不暴露作者的用户 ID，只保留显示名。
		pu.Author = nil
		if u.AuthorName != "" {
			pu.Author = &v1.NamedRef{Name: u.AuthorName}
		}
		out.Updates = append(out.Updates, pu)
	}
	return out, nil
}
