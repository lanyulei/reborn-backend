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
}

func (s *dispatchRepoStub) Claim(context.Context, int, time.Duration, []string) ([]*ClaimedDispatch, error) {
	return s.claims, nil
}

func (s *dispatchRepoStub) SaveContent(_ context.Context, d *ClaimedDispatch, content []byte, templateID string) error {
	if s.stale {
		return ErrStaleClaim
	}
	s.content = append(s.content, content)
	s.templateIDs = append(s.templateIDs, templateID)
	d.Content = content
	return nil
}

func (s *dispatchRepoStub) Finish(_ context.Context, _ *ClaimedDispatch, res FinishResult, _ int32) error {
	s.finished = append(s.finished, res)
	return nil
}

func (s *dispatchRepoStub) Release(_ context.Context, _ *ClaimedDispatch, delay time.Duration) error {
	s.released = append(s.released, delay)
	return nil
}

func (s *dispatchRepoStub) ReapExpired(context.Context, int, int32, func(int32) time.Duration) (int, error) {
	return 0, nil
}

func (s *dispatchRepoStub) GetEvent(_ context.Context, id string) (*Event, error) {
	if ev, ok := s.events[id]; ok {
		return ev, nil
	}
	return nil, ErrEventNotFound
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
		t.Fatalf("render snapshot saves = %d, want 1", len(repo.content))
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
		if c.wantFin.Retryable && repo.finished[0].NextRetry.Before(time.Now().Add(29*time.Second)) {
			t.Errorf("%s: NextRetry = %v, want ≥ 30s backoff", c.name, repo.finished[0].NextRetry)
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
	if len(repo.released) != 1 || repo.released[0] != releaseBreakerDelay || len(adapter.sent) != 10 {
		t.Fatalf("open breaker must release without sending: released=%v sent=%d", repo.released, len(adapter.sent))
	}
	if paused := uc.pausedChannels(); len(paused) != 1 || paused[0] != chHook {
		t.Fatalf("paused channels = %v", paused)
	}
}

func TestSenderUsecase_StaleClaimIgnored(t *testing.T) {
	t.Parallel()
	repo := &dispatchRepoStub{claims: []*ClaimedDispatch{claimed("d", chEmail)}, stale: true}
	adapter := &senderStub{}
	uc := newSender(t, repo, limiterStub{ok: true}, adapter)
	if _, err := uc.SendOnce(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if len(adapter.sent) != 0 || len(repo.finished) != 0 {
		t.Fatalf("stale claim must stop before sending: sent=%d finished=%d", len(adapter.sent), len(repo.finished))
	}
}
