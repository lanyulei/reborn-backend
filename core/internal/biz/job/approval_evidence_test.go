package biz

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// approvalWriter 记录审批结果的执行仓储桩。
type approvalWriter struct {
	ExecutionWriter
	err error
	got []bool
	why []string
}

func (w *approvalWriter) ResolveApprovalTx(_ context.Context, _ Tx, _ string, approved bool, reason string) (int64, error) {
	w.got, w.why = append(w.got, approved), append(w.why, reason)
	return 1, w.err
}

// evidenceStub 审批证据桩。
type evidenceStub struct {
	ev  ApprovalEvidence
	err error
}

func (s evidenceStub) ApprovalEvidenceTx(context.Context, Tx, string, string) (ApprovalEvidence, error) {
	return s.ev, s.err
}

// 「流程走完」不等于有人审批过：审批环节被自动跳过、又没有人（提交人以外）点过同意时不批准；
// 没有审批环节被跳过、或确有人同意时照常批准。与容器平台的变更审批同一口径。
func TestApprovalReviewed(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		ev         ApprovalEvidence
		wantOK     bool
		wantReason string
	}{
		"no approval step was skipped":  {ApprovalEvidence{}, true, ""},
		"skipped, but someone approved": {ApprovalEvidence{Skipped: 2, Approved: 1}, true, ""},
		"all approvals skipped":         {ApprovalEvidence{Skipped: 2}, false, MsgApprovalUnreviewed},
	}
	for name, tc := range cases {
		if ok := ApprovalReviewed(tc.ev); ok != tc.wantOK {
			t.Errorf("%s: ApprovalReviewed(%+v) = %v, want %v", name, tc.ev, ok, tc.wantOK)
		}
	}

	run := func(evidence ApprovalEvidenceReader) (*approvalWriter, error) {
		w := &approvalWriter{}
		err := NewApprovalSyncer(w, evidence).Resolve(context.Background(), nil, "exec-1", true, "submitter", "")
		return w, err
	}
	for name, tc := range cases {
		w, err := run(evidenceStub{ev: tc.ev})
		if err != nil || !slices.Equal(w.got, []bool{tc.wantOK}) || w.why[0] != tc.wantReason {
			t.Errorf("%s: approved = %v, reason = %q, err = %v; want %v, %q", name, w.got, w.why, err, tc.wantOK, tc.wantReason)
		}
	}
	// 没有证据来源、证据读不出来：都不批准（后者报错，由调用方回滚）。
	if w, err := run(nil); err != nil || !slices.Equal(w.got, []bool{false}) || w.why[0] != MsgApprovalUnreviewed {
		t.Fatalf("without an evidence source: approved = %v, reason = %q, err = %v; want rejected", w.got, w.why, err)
	}
	if w, err := run(evidenceStub{err: errors.New("db is down")}); err == nil || w.got != nil {
		t.Fatalf("with a failing evidence source: approved = %v, err = %v; want an error and nothing written", w.got, err)
	}
	// 本来就不通过（驳回 / 撤回）时不看证据。
	w := &approvalWriter{}
	if err := NewApprovalSyncer(w, nil).Resolve(context.Background(), nil, "exec-1", false, "submitter", "审批被驳回"); err != nil ||
		!slices.Equal(w.got, []bool{false}) || w.why[0] != "审批被驳回" {
		t.Fatalf("a rejected flow: approved = %v, reason = %q, err = %v", w.got, w.why, err)
	}
}
