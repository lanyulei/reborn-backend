package biz

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"
)

// 候选故障在转正之前不属于任何故障，没有时间线可记：谁忽略了、为什么，至少要进日志（此前操作者与说明被直接丢弃）。
func TestDismissCandidateLogsOperatorAndNote(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	candidates := &stubCandidates{c: &Candidate{ID: "c1", RuleID: "r1", CorrelationKey: "k", Status: CandidateOpen, AlertIDs: []string{"a1", "a2"}}}
	d := &Deps{Txm: fakeTxm{}, Candidates: candidates, Logger: slog.New(slog.NewTextHandler(&buf, nil))}
	uc := &IncidentUsecase{d: d, now: time.Now}

	if err := uc.DismissCandidate(context.Background(), "u1", "c1", "  重复告警，已人工确认  "); err != nil {
		t.Fatalf("DismissCandidate() error = %v", err)
	}
	if !slices.Equal(candidates.statuses, []int32{CandidateDismissed}) {
		t.Fatalf("statuses = %v, want dismissed", candidates.statuses)
	}
	logged := buf.String()
	for _, want := range []string{"candidate=c1", "operator=u1", "rule=r1", "correlation_key=k", "alerts=2", "重复告警，已人工确认"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("log %q does not contain %q", logged, want)
		}
	}
}

// 已经处置过的候选不能再忽略，也不留痕。
func TestDismissCandidateRejectsHandledCandidate(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	candidates := &stubCandidates{c: &Candidate{ID: "c1", Status: CandidatePromoted}}
	d := &Deps{Txm: fakeTxm{}, Candidates: candidates, Logger: slog.New(slog.NewTextHandler(&buf, nil))}
	uc := &IncidentUsecase{d: d, now: time.Now}

	if err := uc.DismissCandidate(context.Background(), "u1", "c1", "note"); !ErrCandidateMissing.Is(err) {
		t.Fatalf("DismissCandidate() error = %v, want ErrCandidateMissing", err)
	}
	if len(candidates.statuses) != 0 || buf.Len() != 0 {
		t.Fatalf("a handled candidate must be left untouched: statuses %v, log %q", candidates.statuses, buf.String())
	}
}
