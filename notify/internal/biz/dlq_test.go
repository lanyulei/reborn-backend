package biz

import (
	"context"
	"testing"
)

type dlqRepoStub struct {
	letters []*DeadLetter
	built   []*Dispatch
}

func (s *dlqRepoStub) Replay(_ context.Context, _ int, build func(dl *DeadLetter) (*Dispatch, error)) (int, error) {
	for _, dl := range s.letters {
		d, err := build(dl)
		if err != nil {
			return 0, err
		}
		s.built = append(s.built, d)
	}
	return len(s.built), nil
}

// TestDlqUsecase_ReplayKeepsAggregateContext 汇总型死信重放保留聚合上下文、丢弃已渲染
// 正文（按当前模板重新渲染）；无快照 / 非汇总死信按单事件重放。
func TestDlqUsecase_ReplayKeepsAggregateContext(t *testing.T) {
	t.Parallel()
	agg := &AggContext{Count: 7, Digest: true, Samples: []AggSample{{EventID: "e1", Title: "t", Severity: 2}}}
	rendered, _ := EncodeContent(&DispatchContent{Subject: "old", Body: "old", TemplateID: "t1", TemplateVersion: 3, Agg: agg})
	single, _ := EncodeContent(&DispatchContent{Subject: "old", Body: "old"})
	repo := &dlqRepoStub{letters: []*DeadLetter{
		{EventID: "e1", ChannelID: chHook, Recipient: "r", Content: rendered},
		{EventID: "e2", ChannelID: chHook, Recipient: "r", Content: single},
		{EventID: "e3", ChannelID: chHook, Recipient: "r"},
		{EventID: "e4", ChannelID: chHook, Recipient: "r", Content: []byte("{not json")},
	}}
	uc := NewDlqUsecase(repo, testLogger())
	if n, err := uc.ReplayOnce(context.Background(), 10); err != nil || n != 4 {
		t.Fatalf("ReplayOnce = %d, %v", n, err)
	}
	c, err := DecodeContent(repo.built[0].Content)
	if err != nil || c.Agg == nil || c.Agg.Count != 7 || !c.Agg.Digest || len(c.Agg.Samples) != 1 {
		t.Fatalf("aggregate context lost: %+v err=%v", c, err)
	}
	if c.Rendered() || c.TemplateID != "" {
		t.Fatalf("rendered snapshot must be dropped for re-render: %+v", c)
	}
	for i := 1; i < 4; i++ {
		if repo.built[i].Content != nil {
			t.Errorf("letter %d: non-aggregate replay must carry nil content, got %s", i, repo.built[i].Content)
		}
	}
	for _, d := range repo.built {
		if d.Status != DispatchPending || d.ID == "" || d.SendKey == "" {
			t.Fatalf("replayed dispatch malformed: %+v", d)
		}
	}
}
