package biz

import (
	"context"
	"errors"
	"testing"
	"time"
)

// dispatchRepoStub 记录 sender 的落账调用。
type dispatchRepoStub struct {
	claims      []*ClaimedDispatch
	events      map[string]*Event
	finished    []FinishResult
	released    []time.Duration
	content     [][]byte
	templateIDs []string
	stale       bool
	eventErr    error // GetEvent 瞬时错误（非 ErrEventNotFound）
}

func (s *dispatchRepoStub) Claim(context.Context, int, time.Duration, []string) ([]*ClaimedDispatch, error) {
	return s.claims, nil
}

func (s *dispatchRepoStub) Finish(_ context.Context, _ *ClaimedDispatch, res FinishResult, _ int32) error {
	if s.stale {
		return ErrStaleClaim
	}
	s.finished = append(s.finished, res)
	if res.Content != nil {
		s.content = append(s.content, res.Content)
		s.templateIDs = append(s.templateIDs, res.TemplateID)
	}
	return nil
}

func (s *dispatchRepoStub) Release(_ context.Context, _ *ClaimedDispatch, delay time.Duration) error {
	s.released = append(s.released, delay)
	return nil
}

func (s *dispatchRepoStub) ReapExpired(context.Context, int, int32, func(int32) time.Duration) (int, error) {
	return 0, nil
}

func (s *dispatchRepoStub) GetEvents(_ context.Context, ids []string, _ time.Time) (map[string]*Event, error) {
	if s.eventErr != nil {
		return nil, s.eventErr
	}
	out := map[string]*Event{}
	for _, id := range ids {
		if ev, ok := s.events[id]; ok {
			out[id] = ev
		}
	}
	return out, nil
}

type limiterStub struct {
	ok   bool
	wait time.Duration
}

func (l limiterStub) Take(context.Context, *Channel) (bool, time.Duration, error) {
	return l.ok, l.wait, nil
}

type senderStub struct {
	sent      []RenderedMessage
	err       error
	retryable bool
}

func (s *senderStub) Supports(channelType string) bool { return channelType != ChannelTypeDingtalk }
func (s *senderStub) Send(_ context.Context, _ *Channel, msg RenderedMessage) (SendResult, error) {
	s.sent = append(s.sent, msg)
	return SendResult{ProviderMsgID: "p-1", Retryable: s.retryable}, s.err
}

func claimed(id, channelID string) *ClaimedDispatch {
	return &ClaimedDispatch{Dispatch: Dispatch{
		ID: id, EventID: "e1", ChannelID: channelID, Recipient: "ops@example.com",
		Status: DispatchSending, Attempts: 1, SendKey: "k", CreateTime: time.Now(),
	}}
}

func newSender(t *testing.T, repo *dispatchRepoStub, limiter RateLimiter, adapter ChannelSender) *SenderUsecase {
	t.Helper()
	tpl := &Template{ID: "t1", BizID: "default", EventType: "*", ChannelType: "*", Version: 2, Subject: "S:{{.Title}}", Body: "B:{{.Title}}", Enabled: true}
	cache := newTestCache(t, testSnapshot(nil, nil, []*Template{tpl}))
	repo.events = map[string]*Event{"e1": testEvent("e1", "a.b", 2)}
	return NewSenderUsecase(repo, cache, limiter, adapter, NewRenderer(), testLogger())
}

func TestSenderUsecase_SentAndSnapshot(t *testing.T) {
	t.Parallel()
	repo := &dispatchRepoStub{claims: []*ClaimedDispatch{claimed("d1", chEmail)}}
	adapter := &senderStub{}
	uc := newSender(t, repo, limiterStub{ok: true}, adapter)
	n, err := uc.SendOnce(context.Background(), 10)
	if err != nil || n != 1 {
		t.Fatalf("SendOnce() = %d, %v", n, err)
	}
	if len(adapter.sent) != 1 || adapter.sent[0].Subject != "S:t-e1" || adapter.sent[0].Body != "B:t-e1" || adapter.sent[0].SendKey != "k" {
		t.Fatalf("sent = %+v", adapter.sent)
	}
	if len(repo.content) != 1 {
		t.Fatalf("render snapshot (via Finish) = %d, want 1", len(repo.content))
	}
	c, _ := DecodeContent(repo.content[0])
	if c.TemplateID != "t1" || c.TemplateVersion != 2 || !c.Rendered() || repo.templateIDs[0] != "t1" {
		t.Fatalf("content snapshot = %+v, template column = %v", c, repo.templateIDs)
	}
	if len(repo.finished) != 1 || !repo.finished[0].OK || repo.finished[0].ProviderMsgID != "p-1" {
		t.Fatalf("finished = %+v", repo.finished)
	}
	// 重试复用快照：已渲染内容不再渲染。
	d2 := claimed("d2", chEmail)
	d2.Content = repo.content[0]
	repo.claims = []*ClaimedDispatch{d2}
	repo.content = nil
	if _, err := uc.SendOnce(context.Background(), 10); err != nil || len(repo.content) != 0 || adapter.sent[1].Subject != "S:t-e1" {
		t.Fatalf("retry must reuse snapshot: content saves=%d err=%v", len(repo.content), err)
	}
	if repo.finished[1].Content != nil {
		t.Fatal("retry must not resend the snapshot column")
	}
}

func TestSenderUsecase_FailureOutcomes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		channel   string
		adapter   *senderStub
		limiter   limiterStub
		wantFin   *FinishResult // nil = 期望 release
		wantDelay time.Duration
	}{
		{
			"retryable failure", chEmail, &senderStub{err: errors.New("timeout"), retryable: true},
			limiterStub{ok: true},
			&FinishResult{OK: false, Retryable: true}, 0,
		},
		{
			"non-retryable failure", chEmail, &senderStub{err: errors.New("bad address")},
			limiterStub{ok: true},
			&FinishResult{OK: false, Retryable: false}, 0,
		},
		{"rate limited", chEmail, &senderStub{}, limiterStub{ok: false, wait: 2 * time.Second}, nil, 2 * time.Second},
		{"rate limited min delay", chEmail, &senderStub{}, limiterStub{ok: false, wait: 10 * time.Millisecond}, nil, releaseMinDelay},
		{"channel disabled", chOff, &senderStub{}, limiterStub{ok: true}, nil, releaseDisabledDelay},
		{"channel deleted", "missing", &senderStub{}, limiterStub{ok: true}, &FinishResult{Retryable: false}, 0},
		{"unsupported type", chRobot, &senderStub{}, limiterStub{ok: true}, &FinishResult{Retryable: false}, 0},
	}
	for _, c := range cases {
		repo := &dispatchRepoStub{claims: []*ClaimedDispatch{claimed("d", c.channel)}}
		uc := newSender(t, repo, c.limiter, c.adapter)
		if _, err := uc.SendOnce(context.Background(), 10); err != nil {
			t.Fatalf("%s: SendOnce error = %v", c.name, err)
		}
		if c.wantFin == nil {
			if len(repo.released) != 1 || repo.released[0] != c.wantDelay || len(repo.finished) != 0 {
				t.Errorf("%s: released=%v finished=%v, want release %v", c.name, repo.released, repo.finished, c.wantDelay)
			}
			continue
		}
		if len(repo.finished) != 1 || repo.finished[0].OK != c.wantFin.OK || repo.finished[0].Retryable != c.wantFin.Retryable {
			t.Errorf("%s: finished=%+v, want %+v", c.name, repo.finished, c.wantFin)
		}
		if c.wantFin.Retryable && repo.finished[0].RetryAfter < 30*time.Second {
			t.Errorf("%s: RetryAfter = %v, want ≥ 30s backoff", c.name, repo.finished[0].RetryAfter)
		}
	}
}

func TestSenderUsecase_BreakerAndPause(t *testing.T) {
	t.Parallel()
	repo := &dispatchRepoStub{}
	adapter := &senderStub{err: errors.New("down"), retryable: true}
	uc := newSender(t, repo, limiterStub{ok: true}, adapter)
	for i := 0; i < 10; i++ {
		repo.claims = []*ClaimedDispatch{claimed("d", chHook)}
		if _, err := uc.SendOnce(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	if !uc.Breaker().Local().Open(chHook) {
		t.Fatal("breaker should be open after 10 failures")
	}
	repo.claims = []*ClaimedDispatch{claimed("d", chHook)}
	if _, err := uc.SendOnce(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if len(repo.released) != 1 || repo.released[0] < releaseBreakerDelay || len(adapter.sent) != 10 {
		t.Fatalf("open breaker must release without sending: released=%v sent=%d", repo.released, len(adapter.sent))
	}
	if paused := uc.pausedChannels(); len(paused) != 1 || paused[0] != chHook {
		t.Fatalf("paused channels = %v", paused)
	}
}

// TestSenderUsecase_StaleClaimIgnored 围栏在落账时判定：旧持有者的结果被丢弃
// （ErrStaleClaim 不算处理失败、不记错误日志），at-least-once 既定口径。
func TestSenderUsecase_StaleClaimIgnored(t *testing.T) {
	t.Parallel()
	repo := &dispatchRepoStub{claims: []*ClaimedDispatch{claimed("d", chEmail)}, stale: true}
	adapter := &senderStub{}
	uc := newSender(t, repo, limiterStub{ok: true}, adapter)
	if n, err := uc.SendOnce(context.Background(), 1); err != nil || n != 1 {
		t.Fatalf("SendOnce = %d, %v", n, err)
	}
	if len(repo.finished) != 0 || len(repo.released) != 0 {
		t.Fatalf("stale finish must not be recorded: finished=%d released=%d", len(repo.finished), len(repo.released))
	}
}

// TestSenderUsecase_BreakerOpenPausesUntilReopen 熔断打开期间放回/暂停到 open_until
// 为止，而非固定 5s 反复声明再放回。
func TestSenderUsecase_BreakerOpenPausesUntilReopen(t *testing.T) {
	t.Parallel()
	repo := &dispatchRepoStub{}
	adapter := &senderStub{err: errors.New("down"), retryable: true}
	uc := newSender(t, repo, limiterStub{ok: true}, adapter)
	for i := 0; i < 10; i++ {
		repo.claims = []*ClaimedDispatch{claimed("d", chHook)}
		if _, err := uc.SendOnce(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	repo.claims = []*ClaimedDispatch{claimed("d", chHook)}
	repo.released = nil
	if _, err := uc.SendOnce(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if len(repo.released) != 1 || repo.released[0] < 50*time.Second || repo.released[0] > breakerOpenFor {
		t.Fatalf("open breaker must release until open_until (~60s), got %v", repo.released)
	}
}

// TestSenderUsecase_TransientErrorReleases 发送前瞬时错误（取事件失败）须把任务放回
// （不计尝试、短延后），而不是让它占着 5 分钟租约等回收器记一次失败。
func TestSenderUsecase_TransientErrorReleases(t *testing.T) {
	t.Parallel()
	repo := &dispatchRepoStub{claims: []*ClaimedDispatch{claimed("d1", chEmail), claimed("d2", chEmail)}, eventErr: errors.New("db hiccup")}
	adapter := &senderStub{}
	uc := newSender(t, repo, limiterStub{ok: true}, adapter)
	// 批量取事件失败：整批放回并把错误上报给循环（退避），不占租约。
	if _, err := uc.SendOnce(context.Background(), 10); err == nil {
		t.Fatal("SendOnce() must surface the transient load error")
	}
	if len(repo.released) != 2 || repo.released[0] != releaseTransientDelay || len(repo.finished) != 0 || len(adapter.sent) != 0 {
		t.Fatalf("transient error must release the batch: released=%v finished=%v sent=%d", repo.released, repo.finished, len(adapter.sent))
	}
}

// TestSenderUsecase_RateLimitReturnsProbe 熔断半开放出的探测名额若因无令牌未真正发送，
// 必须归还；否则 probing 只能由 Record 清除，渠道此后一直被拒。
func TestSenderUsecase_RateLimitReturnsProbe(t *testing.T) {
	t.Parallel()
	repo := &dispatchRepoStub{}
	adapter := &senderStub{err: errors.New("down"), retryable: true}
	uc := newSender(t, repo, limiterStub{ok: true}, adapter)
	now := time.Unix(1_800_000_000, 0)
	uc.Breaker().Local().now = func() time.Time { return now }
	for i := 0; i < 10; i++ {
		repo.claims = []*ClaimedDispatch{claimed("d", chHook)}
		if _, err := uc.SendOnce(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	if !uc.Breaker().Local().Open(chHook) {
		t.Fatal("breaker should be open")
	}
	now = now.Add(61 * time.Second) // 到期 → 半开
	uc.paused = map[string]time.Time{}
	uc.limiter = limiterStub{ok: false, wait: time.Second}
	repo.claims = []*ClaimedDispatch{claimed("d", chHook)}
	repo.released = nil
	if _, err := uc.SendOnce(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if len(repo.released) != 1 || len(adapter.sent) != 10 {
		t.Fatalf("no token: must release without sending, released=%v sent=%d", repo.released, len(adapter.sent))
	}
	// 名额已归还：下一次 Allow 应再放出探测。
	if !uc.Breaker().Local().Allow(chHook) {
		t.Fatal("probe slot must be returned after rate-limit release")
	}
}

func TestSenderUsecase_SendTimeoutPerChannel(t *testing.T) {
	t.Parallel()
	uc := newSender(t, &dispatchRepoStub{}, limiterStub{ok: true}, &senderStub{})
	uc.WithOptions(SenderOptions{SendTimeout: 3 * time.Second, MaxSendTimeout: 10 * time.Second})
	cases := []struct {
		name string
		ch   *Channel
		want time.Duration
	}{
		{"email default", &Channel{Type: ChannelTypeEmail}, 3 * time.Second},
		{"webhook unset", &Channel{Type: ChannelTypeWebhook, Webhook: &WebhookConfig{}}, 3 * time.Second},
		{"webhook 1s honoured below default", &Channel{Type: ChannelTypeWebhook, Webhook: &WebhookConfig{TimeoutS: 1}}, time.Second},
		{"webhook 8s above old 3s cap", &Channel{Type: ChannelTypeWebhook, Webhook: &WebhookConfig{TimeoutS: 8}}, 8 * time.Second},
		{"webhook capped", &Channel{Type: ChannelTypeWebhook, Webhook: &WebhookConfig{TimeoutS: 30}}, 10 * time.Second},
	}
	for _, c := range cases {
		if got := uc.sendTimeout(c.ch); got != c.want {
			t.Errorf("%s: sendTimeout = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestSenderUsecase_WritebackSurvivesCancel 消息发出后调用 ctx 被取消（停机硬中止），
// 落账仍须在未取消的 ctx 上完成——否则行停在 sending 直到租约到期重发（重复投递）。
func TestSenderUsecase_WritebackSurvivesCancel(t *testing.T) {
	t.Parallel()
	repo := &ctxRecordingRepo{dispatchRepoStub: dispatchRepoStub{claims: []*ClaimedDispatch{claimed("d1", chEmail)}}}
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &cancelOnSend{cancel: cancel}
	tpl := &Template{ID: "t1", BizID: "default", EventType: "*", ChannelType: "*", Version: 1, Subject: "S", Body: "B", Enabled: true}
	cache := newTestCache(t, testSnapshot(nil, nil, []*Template{tpl}))
	repo.events = map[string]*Event{"e1": testEvent("e1", "a.b", 2)}
	uc := NewSenderUsecase(repo, cache, limiterStub{ok: true}, adapter, NewRenderer(), testLogger())
	if _, err := uc.SendOnce(ctx, 10); err != nil {
		t.Fatalf("SendOnce() error = %v", err)
	}
	if !adapter.sent {
		t.Fatal("message should have been sent")
	}
	if repo.finishCtxErr != nil {
		t.Fatalf("Finish must run on an uncancelled ctx, got err=%v", repo.finishCtxErr)
	}
	if len(repo.finished) != 1 || !repo.finished[0].OK {
		t.Fatalf("finished = %+v", repo.finished)
	}
}

// cancelOnSend 发送成功的同时取消调用方 ctx（模拟消息已发出、随后停机硬中止）。
type cancelOnSend struct {
	cancel context.CancelFunc
	sent   bool
}

func (s *cancelOnSend) Supports(string) bool { return true }
func (s *cancelOnSend) Send(context.Context, *Channel, RenderedMessage) (SendResult, error) {
	s.sent = true
	s.cancel()
	return SendResult{ProviderMsgID: "p-1"}, nil
}

// ctxRecordingRepo 记录 Finish 收到的 ctx 是否已取消。
type ctxRecordingRepo struct {
	dispatchRepoStub
	finishCtxErr error
}

func (r *ctxRecordingRepo) Finish(ctx context.Context, d *ClaimedDispatch, res FinishResult, max int32) error {
	r.finishCtxErr = ctx.Err()
	return r.dispatchRepoStub.Finish(ctx, d, res, max)
}

// TestSenderUsecase_NonRetryableFailuresDoNotTripBreaker 不可重试失败（4xx / 内容非法）
// 是单条消息的问题，不计入渠道熔断统计；并归还半开探测名额。
func TestSenderUsecase_NonRetryableFailuresDoNotTripBreaker(t *testing.T) {
	t.Parallel()
	repo := &dispatchRepoStub{}
	adapter := &senderStub{err: errors.New("400 bad payload")}
	uc := newSender(t, repo, limiterStub{ok: true}, adapter)
	for i := 0; i < 20; i++ {
		repo.claims = []*ClaimedDispatch{claimed("d", chHook)}
		if _, err := uc.SendOnce(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	if uc.Breaker().Local().Open(chHook) || len(adapter.sent) != 20 || len(repo.finished) != 20 {
		t.Fatalf("non-retryable failures must not open the breaker: open=%v sent=%d finished=%d",
			uc.Breaker().Local().Open(chHook), len(adapter.sent), len(repo.finished))
	}
	// 半开探测遇到不可重试失败：名额归还，下一条仍可探测（而不是永久拒绝）。
	local := uc.Breaker().Local()
	base := time.Now()
	local.now = func() time.Time { return base }
	for i := 0; i < breakerMinSamples; i++ {
		local.Record(chHook, false)
	}
	local.now = func() time.Time { return base.Add(breakerOpenFor + time.Second) }
	for i := 0; i < 2; i++ {
		repo.claims = []*ClaimedDispatch{claimed("d", chHook)}
		if _, err := uc.SendOnce(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	if len(adapter.sent) != 22 {
		t.Fatalf("probe slot must be returned after a non-retryable failure: sent=%d, want 22", len(adapter.sent))
	}
}
