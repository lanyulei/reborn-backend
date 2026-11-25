package biz

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// 「一条坏数据不拖垮整批」的回归测试。此前 webhook URL / 聚合键超出列宽、聚合 sweeper 遇错即返回，
// 任一情况都会让一批事件整体回滚并永远排在待处理队列最前（全局路由或聚合汇总停摆）。

const chHookLong = "018f3e1a-0a01-7000-8000-000000000005"

// webhook URL 控制面允许到 2048 字符，notify_dispatch.recipient 只有 255：收件标识按列宽截断。
func TestResolveTargets_WebhookRecipientFitsColumn(t *testing.T) {
	t.Parallel()
	long := "https://hooks.example.com/" + strings.Repeat("a", maxRecipientLen)
	snap := testSnapshot(nil, nil, nil)
	snap.Channels[chHookLong] = &Channel{ID: chHookLong, BizID: "default", Type: ChannelTypeWebhook, Enabled: true, Webhook: &WebhookConfig{URL: long}}

	got := ResolveTargets(snap, &Route{ChannelIDs: []string{chHook, chHookLong}}, nil, nil)
	if len(got) != 2 {
		t.Fatalf("ResolveTargets() = %v, want 2 targets", got)
	}
	if got[0].Recipient != "https://hooks.example.com/x" {
		t.Fatalf("short URL must be kept as is, got %q", got[0].Recipient)
	}
	if r := got[1].Recipient; len(r) != maxRecipientLen || r != long[:maxRecipientLen] {
		t.Fatalf("long URL must be truncated to the column width, got len=%d", len(r))
	}
}

// 聚合键 biz|group_key 最长 193，超出 notify_agg_group.group_key 的列宽（191）：这类事件退化为逐条分发。
func TestRouterUsecase_LongAggregateKeyRoutesDirectly(t *testing.T) {
	t.Parallel()
	bizLong := strings.Repeat("b", 64) // 64 + 1 + 128 = 193 > 191
	routes := []*Route{{
		ID: routeAgg, BizID: bizLong, EventPattern: "host.down", SeverityMin: 1, Priority: 5, Enabled: true,
		ChannelIDs: []string{chHook}, Recipient: RouteRecipient{Fixed: []string{"x"}}, AggregateWindowS: 60,
	}}
	snap := testSnapshot(routes, nil, nil)
	aggregated := testEvent("e-agg", "host.down", 2)
	aggregated.BizID, aggregated.GroupKey = bizLong, "room-a"
	direct := testEvent("e-direct", "host.down", 2)
	direct.BizID, direct.GroupKey = bizLong, strings.Repeat("k", maxEventKeyLen)

	uc, tx := newRouter(t, snap, aggregated, direct)
	if _, err := uc.RouteOnce(context.Background(), 500); err != nil {
		t.Fatalf("RouteOnce() error = %v", err)
	}
	if len(tx.aggs) != 1 || tx.aggs[0].Key != AggKey(bizLong, "room-a") {
		t.Fatalf("only the short key must be aggregated, aggs = %+v", tx.aggs)
	}
	if len(tx.dispatches) != 1 || tx.dispatches[0].EventID != "e-direct" {
		t.Fatalf("long key must produce a direct dispatch, dispatches = %+v", tx.dispatches)
	}
	if tx.marked["e-agg"] != EventStatusRouted || tx.marked["e-direct"] != EventStatusRouted {
		t.Fatalf("event statuses = %v, both must be routed", tx.marked)
	}
}

// failingAggRepo 让指定组的 CloseGroup 返回错误（模拟该组的确定性错误）。
type failingAggRepo struct {
	*aggRepoStub
	fail map[string]error
}

func (s *failingAggRepo) CloseGroup(ctx context.Context, key string, threshold, suppress int32,
	fn func(context.Context, RouterTx, *AggGroup) error,
) (bool, error) {
	if err, ok := s.fail[key]; ok {
		return false, err
	}
	return s.aggRepoStub.CloseGroup(ctx, key, threshold, suppress, fn)
}

// 一个组出错不能阻塞其后的组：其余组照常关闭，错误仍向上返回。
func TestSweeperUsecase_FailingGroupDoesNotBlockOthers(t *testing.T) {
	t.Parallel()
	routes := []*Route{{
		ID: routeAgg, BizID: "default", EventPattern: "host.down", SeverityMin: 1, Priority: 5, Enabled: true,
		ChannelIDs: []string{chHook}, Recipient: RouteRecipient{Fixed: []string{"ops@example.com"}}, AggregateWindowS: 60,
	}}
	snap := testSnapshot(routes, nil, nil)
	boom := errors.New("value too long for type character varying(255)")
	repo := &failingAggRepo{fail: map[string]error{"default|broken": boom}, aggRepoStub: &aggRepoStub{
		due: []string{"default|broken", "default|small", "default|later"},
		groups: map[string]*AggGroup{
			"default|broken": {Key: "default|broken", BizID: "default", EventType: "host.down", RouteID: routeAgg, FirstEventID: "e0", Count: 1},
			"default|small":  {Key: "default|small", BizID: "default", EventType: "host.down", RouteID: routeAgg, FirstEventID: "e1", Count: 2},
			"default|later":  {Key: "default|later", BizID: "default", EventType: "host.down", RouteID: routeAgg, FirstEventID: "e2", Count: 1},
		},
		tx: newRouterTxStub(), closeOK: true,
	}}
	uc := NewSweeperUsecase(repo, newTestCache(t, snap), testLogger())
	n, err := uc.SweepOnce(context.Background(), 10)
	if n != 2 || !errors.Is(err, boom) {
		t.Fatalf("SweepOnce() = %d, %v; want 2 closed and the group error returned", n, err)
	}
	if len(repo.closed) != 2 || repo.closed[0] != "default|small" || repo.closed[1] != "default|later" {
		t.Fatalf("groups after the failing one must still be closed, closed = %v", repo.closed)
	}
}

// PostgreSQL 的 text / varchar 拒绝 NUL（22021）：入口按 400 拒绝，而不是落库时返回 500。
func TestValidateEvent_RejectsNUL(t *testing.T) {
	t.Parallel()
	mutations := map[string]func(*Event){
		"title":      func(e *Event) { e.Title = "a\x00b" },
		"dedup_key":  func(e *Event) { e.DedupKey = "k\x00" },
		"group_key":  func(e *Event) { e.GroupKey = "g\x00" },
		"recipients": func(e *Event) { e.Recipients = []string{"ops@example.com", "x\x00y"} },
	}
	for name, mutate := range mutations {
		ev := testEvent("e-nul", "a.b", 2)
		mutate(ev)
		if err := validateEvent(ev); !errors.Is(err, ErrEventInvalid) {
			t.Errorf("%s: validateEvent() = %v, want ErrEventInvalid", name, err)
		}
	}
	ev := testEvent("e-ok", "a.b", 2)
	ev.GroupKey, ev.Recipients = "g", []string{"ops@example.com"}
	if err := validateEvent(ev); err != nil {
		t.Fatalf("valid event: validateEvent() = %v", err)
	}
}

// longIDSender 模拟对端返回超长的渠道侧消息标识（webhook 的 X-Request-Id 响应头长度不受控）。
type longIDSender struct{ senderStub }

func (s *longIDSender) Send(ctx context.Context, ch *Channel, msg RenderedMessage) (SendResult, error) {
	res, err := s.senderStub.Send(ctx, ch, msg)
	res.ProviderMsgID = strings.Repeat("p", maxProviderMsgIDLen+50)
	return res, err
}

// 渠道侧消息标识在落账前按列宽截断：否则落账语句以 22001 失败，已送达的这条会被当作失败重发。
func TestSenderUsecase_ProviderMsgIDTruncatedToColumn(t *testing.T) {
	t.Parallel()
	repo := &dispatchRepoStub{claims: []*ClaimedDispatch{claimed("d1", chEmail)}}
	adapter := &longIDSender{}
	uc := newSender(t, repo, limiterStub{ok: true}, adapter)
	if _, err := uc.SendOnce(context.Background(), 10); err != nil {
		t.Fatalf("SendOnce() error = %v", err)
	}
	if len(repo.finished) != 1 || !repo.finished[0].OK {
		t.Fatalf("finished = %+v", repo.finished)
	}
	if got := repo.finished[0].ProviderMsgID; len(got) != maxProviderMsgIDLen {
		t.Fatalf("ProviderMsgID length = %d, want %d", len(got), maxProviderMsgIDLen)
	}
}

// 停用渠道的积压任务进入本实例的跳过名单，不再每分钟被整批声明再放回。
func TestSenderUsecase_DisabledChannelIsPaused(t *testing.T) {
	t.Parallel()
	repo := &dispatchRepoStub{claims: []*ClaimedDispatch{claimed("d1", chOff)}}
	adapter := &senderStub{}
	uc := newSender(t, repo, limiterStub{ok: true}, adapter)
	n, err := uc.SendOnce(context.Background(), 10)
	if err != nil || n != 1 {
		t.Fatalf("SendOnce() = %d, %v", n, err)
	}
	if len(repo.released) != 1 || repo.released[0] != releaseDisabledDelay || len(adapter.sent) != 0 {
		t.Fatalf("disabled channel must be released without sending: released=%v sent=%d", repo.released, len(adapter.sent))
	}
	paused := uc.pausedChannels()
	if len(paused) != 1 || paused[0] != chOff {
		t.Fatalf("paused channels = %v, want the disabled channel", paused)
	}
}
