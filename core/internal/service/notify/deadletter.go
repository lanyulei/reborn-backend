package service

import (
	"context"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifyDeadLetterService 实现 api/notify/v1 生成的
// NotifyDeadLetterServiceServer：死信查询 + 标记重放（唯一写动作，状态机
// none → pending 由仓储条件 UPDATE 保证）。
type NotifyDeadLetterService struct {
	notifyv1.UnimplementedNotifyDeadLetterServiceServer

	uc       *biz.DeadLetterUsecase
	guard    bizGuard
	channels *biz.ChannelUsecase
}

// NewNotifyDeadLetterService new a NotifyDeadLetterService.
func NewNotifyDeadLetterService(uc *biz.DeadLetterUsecase, channels *biz.ChannelUsecase, scope biz.BizScopeResolver) *NotifyDeadLetterService {
	return &NotifyDeadLetterService{uc: uc, channels: channels, guard: newBizGuard(scope)}
}

// toProtoDeadLetter 领域模型转 proto 实体（全列标量，无 jsonb 解码）。
func toProtoDeadLetter(letter *biz.DeadLetter) *notifyv1.NotifyDeadLetter {
	return &notifyv1.NotifyDeadLetter{
		Id:          letter.ID,
		DispatchId:  letter.DispatchID,
		EventId:     letter.EventID,
		ChannelId:   letter.ChannelID,
		Recipient:   letter.Recipient,
		LastError:   letter.LastError,
		Attempts:    letter.Attempts,
		ReplayState: letter.ReplayState,
		CreateTime:  timestamppb.New(letter.CreateTime),
		UpdateTime:  timestamppb.New(letter.UpdateTime),
	}
}

// ListDeadLetters implements proto NotifyDeadLetterService/ListDeadLetters：
// 四维过滤透传。
func (s *NotifyDeadLetterService) ListDeadLetters(ctx context.Context, req *notifyv1.ListDeadLettersRequest) (*notifyv1.ListDeadLettersResponse, error) {
	if err := s.guard.requireChannelFilter(ctx, req.GetChannelId(), s.channels); err != nil {
		return nil, err
	}
	filter := biz.DeadLetterFilter{
		EventID:     req.GetEventId(),
		ChannelID:   req.GetChannelId(),
		ReplayState: req.ReplayState,
	}
	if ts := req.GetCreateTimeStart(); ts != nil {
		t := ts.AsTime()
		filter.CreateTimeStart = &t
	}
	if ts := req.GetCreateTimeEnd(); ts != nil {
		t := ts.AsTime()
		filter.CreateTimeEnd = &t
	}
	letters, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ListDeadLettersResponse{
		Items: make([]*notifyv1.NotifyDeadLetter, 0, len(letters)),
		Total: total,
	}
	for _, letter := range letters {
		resp.Items = append(resp.Items, toProtoDeadLetter(letter))
	}
	return resp, nil
}

// ReplayDeadLetter implements proto NotifyDeadLetterService/ReplayDeadLetter：
// 标记待重放并返回更新后的行；哨兵错误原样上抛（Kratos 映射 404/409）。
func (s *NotifyDeadLetterService) ReplayDeadLetter(ctx context.Context, req *notifyv1.ReplayDeadLetterRequest) (*notifyv1.ReplayDeadLetterResponse, error) {
	if err := s.guard.requireRow(ctx, func() (string, error) {
		dl, err := s.uc.Get(ctx, req.GetId())
		if err != nil {
			return "", err
		}
		ch, err := s.channels.Get(ctx, dl.ChannelID)
		if err != nil {
			return "", err
		}
		return ch.BizID, nil
	}); err != nil {
		return nil, err
	}
	letter, err := s.uc.Replay(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &notifyv1.ReplayDeadLetterResponse{DeadLetter: toProtoDeadLetter(letter)}, nil
}
