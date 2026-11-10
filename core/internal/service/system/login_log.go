package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// LoginLogService 实现 api/system/v1 生成的 LoginLogServiceServer。
type LoginLogService struct {
	v1.UnimplementedLoginLogServiceServer

	uc *biz.LoginLogUsecase
}

// NewLoginLogService new a LoginLogService.
func NewLoginLogService(uc *biz.LoginLogUsecase) *LoginLogService {
	return &LoginLogService{uc: uc}
}

// ListLoginLogs implements proto LoginLogService/ListLoginLogs.
func (s *LoginLogService) ListLoginLogs(ctx context.Context, req *v1.ListLoginLogsRequest) (*v1.ListLoginLogsResponse, error) {
	filter := biz.LoginLogFilter{Username: req.GetUsername(), Result: req.Result}
	if req.StartTime != nil {
		t := req.StartTime.AsTime()
		filter.StartTime = &t
	}
	if req.EndTime != nil {
		t := req.EndTime.AsTime()
		filter.EndTime = &t
	}
	logs, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListLoginLogsResponse{
		Items: make([]*v1.LoginLog, 0, len(logs)),
		Total: total,
	}
	for _, l := range logs {
		resp.Items = append(resp.Items, toProtoLoginLog(l))
	}
	return resp, nil
}

// toProtoLoginLog 将领域模型（DO）显式转换为 proto 实体。
func toProtoLoginLog(l *biz.LoginLog) *v1.LoginLog {
	if l == nil {
		return nil
	}
	return &v1.LoginLog{
		Id:         l.ID,
		UserId:     l.UserID,
		Username:   l.Username,
		Result:     l.Result,
		Ip:         l.IP,
		UserAgent:  l.UserAgent,
		CreateTime: timestamppb.New(l.CreateTime),
	}
}
