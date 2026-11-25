//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
)

// 升级巡检的两个跳过条件已下推到 SQL：已升满级的、上一次升级未满一个响应时限的故障不再占用批次名额。
func TestIncidentRepo_InspectExcludesNonEscalatable(t *testing.T) {
	d := newIntegrationIncidentData(t)
	repo, ctx := NewIncidentRepo(d), context.Background()
	const (
		incDone    = "018f3e1a-0a10-7000-8000-000000000011" // 已升满级
		incWaiting = "018f3e1a-0a10-7000-8000-000000000012" // 刚升级过
		incDue     = "018f3e1a-0a10-7000-8000-000000000013" // 待升级
		incNever   = "018f3e1a-0a10-7000-8000-000000000014" // 从未升级
	)
	for _, id := range []string{incDone, incWaiting, incDue, incNever} {
		seedIncident(t, d, id, biz.StatusOpen)
	}
	if _, err := d.Pool.Exec(ctx, `UPDATE incident_record SET open_time = now() - interval '2 hours', severity = 2,
		escalation_level = CASE WHEN id = $1::uuid THEN 3 WHEN id = $2::uuid THEN 1 WHEN id = $3::uuid THEN 2 ELSE 0 END,
		escalate_time = CASE WHEN id = $2::uuid THEN now() WHEN id = $3::uuid THEN now() - interval '2 hours' ELSE NULL END`,
		incDone, incWaiting, incDue); err != nil {
		t.Fatal(err)
	}

	var got []string
	collect := func(q biz.InspectQuery) {
		got = nil
		items, err := repo.Inspect(ctx, q, 200)
		if err != nil {
			t.Fatalf("Inspect() error = %v", err)
		}
		for _, it := range items {
			got = append(got, it.ID)
		}
	}
	contains := func(ids []string, want string) bool {
		for _, id := range ids {
			if id == want {
				return true
			}
		}
		return false
	}
	escalatable, hourAgo := int32(3), time.Now().Add(-time.Hour)
	collect(biz.InspectQuery{Statuses: []int32{biz.StatusOpen}, Severities: []int32{2}, EscalationLevelLT: &escalatable, EscalateTimeBefore: &hourAgo})
	// 四条的 open_time 相同，并列时顺序不定：按集合比较。
	if len(got) != 2 || !(contains(got, incNever) && contains(got, incDue)) {
		t.Fatalf("Inspect(escalatable) = %v, want the never-escalated and the due one", got)
	}
	// 两个新条件不设时行为不变（仍返回全部四条）。
	collect(biz.InspectQuery{Statuses: []int32{biz.StatusOpen}, Severities: []int32{2}})
	if len(got) != 4 {
		t.Fatalf("Inspect(without the new filters) = %v, want all four", got)
	}
}
