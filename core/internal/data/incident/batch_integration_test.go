//go:build integration

package data

import (
	"context"
	"reflect"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

const (
	ruleFixedA  = "018f3e1a-0a30-7000-8000-0000000000a1" // 固定指向业务 A
	ruleFixedB  = "018f3e1a-0a30-7000-8000-0000000000a2" // 固定指向业务 B
	ruleByLabel = "018f3e1a-0a30-7000-8000-0000000000a3" // 按告警标签匹配业务
	businessA   = "018f3e1a-0a40-7000-8000-0000000000b1"
	businessB   = "018f3e1a-0a40-7000-8000-0000000000b2"
	playbookOne = "018f3e1a-0a50-7000-8000-0000000000c1"
	playbookTwo = "018f3e1a-0a50-7000-8000-0000000000c2"
	playbookNew = "018f3e1a-0a50-7000-8000-0000000000c3" // 没有执行过
	memberOne   = "018f3e1a-0a60-7000-8000-0000000000d1"
	memberTwo   = "018f3e1a-0a60-7000-8000-0000000000d2"
)

// 概览的候选故障数：不带业务时是全部待定候选；带业务时只统计「规则固定指向该业务」的待定候选，
// 按标签动态匹配业务的规则的候选不计入任何业务。其余几项不受这条改动影响。
func TestStatsRepo_OverviewCandidatesByBusiness(t *testing.T) {
	d := newIntegrationIncidentData(t)
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, "TRUNCATE TABLE incident_candidate"); err != nil {
		t.Fatalf("truncate candidates: %v", err)
	}
	if _, err := d.Pool.Exec(ctx, "DELETE FROM incident_rule WHERE id = ANY($1::uuid[])", textArray([]string{ruleFixedA, ruleFixedB, ruleByLabel})); err != nil {
		t.Fatalf("clean rules: %v", err)
	}
	t.Cleanup(func() {
		_, _ = d.Pool.Exec(context.Background(), "DELETE FROM incident_rule WHERE id = ANY($1::uuid[])", textArray([]string{ruleFixedA, ruleFixedB, ruleByLabel}))
		_, _ = d.Pool.Exec(context.Background(), "TRUNCATE TABLE incident_candidate")
	})
	for id, spec := range map[string]string{
		ruleFixedA:  `{"name":"a","incident":{"severity":3,"business_id":"` + businessA + `"}}`,
		ruleFixedB:  `{"name":"b","incident":{"severity":3,"business_id":"` + businessB + `"}}`,
		ruleByLabel: `{"name":"c","incident":{"severity":3,"business_label":"app"}}`,
	} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO incident_rule (id, name, spec) VALUES ($1::uuid, $2::text, $3::jsonb)", id, "rule-"+id[30:], spec); err != nil {
			t.Fatalf("seed rule %s: %v", id, err)
		}
	}
	// 业务 A：两个待定 + 一个已忽略；业务 B：一个待定；按标签匹配的规则：一个待定。
	seeds := []struct {
		id, rule, key string
		status        int32
	}{
		{"018f3e1a-0a70-7000-8000-0000000000e1", ruleFixedA, "k1", 1},
		{"018f3e1a-0a70-7000-8000-0000000000e2", ruleFixedA, "k2", 1},
		{"018f3e1a-0a70-7000-8000-0000000000e3", ruleFixedA, "k3", 3},
		{"018f3e1a-0a70-7000-8000-0000000000e4", ruleFixedB, "k1", 1},
		{"018f3e1a-0a70-7000-8000-0000000000e5", ruleByLabel, "k1", 1},
	}
	for _, c := range seeds {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO incident_candidate (id, rule_id, correlation_key, status) VALUES ($1::uuid, $2::uuid, $3::text, $4::smallint)", c.id, c.rule, c.key, c.status); err != nil {
			t.Fatalf("seed candidate %s: %v", c.id, err)
		}
	}
	repo := NewStatsRepo(d)
	for _, tc := range []struct {
		business string
		want     int32
	}{
		{"", 4},
		{businessA, 2},
		{businessB, 1},
		{"018f3e1a-0a40-7000-8000-0000000000bf", 0}, // 没有规则指向的业务
	} {
		o, err := repo.Overview(ctx, tc.business, time.Now())
		if err != nil {
			t.Fatalf("Overview(%q) error = %v", tc.business, err)
		}
		if o.Candidates != tc.want {
			t.Fatalf("Overview(%q).Candidates = %d, want %d", tc.business, o.Candidates, tc.want)
		}
	}
}

// 预案执行次数的批量语句与逐个查询同口径：只计执行中 / 成功 / 失败（4/5/6）且在 since 之后创建的；
// 没有执行过的预案不在结果中。
func TestProposalRepo_CountPlaybookExecsManyMatchesSingle(t *testing.T) {
	d := newIntegrationIncidentData(t)
	ctx := context.Background()
	seedIncident(t, d, incA, biz.StatusResponding)
	const day = 24 * time.Hour
	seeds := []struct {
		id, playbook string
		status       int32
		age          time.Duration
	}{
		{"018f3e1a-0a80-7000-8000-0000000000f1", playbookOne, 4, time.Hour},
		{"018f3e1a-0a80-7000-8000-0000000000f2", playbookOne, 5, 2 * day},
		{"018f3e1a-0a80-7000-8000-0000000000f3", playbookOne, 6, 20 * day},
		{"018f3e1a-0a80-7000-8000-0000000000f4", playbookOne, 5, 40 * day},  // 早于 since
		{"018f3e1a-0a80-7000-8000-0000000000f5", playbookOne, 1, time.Hour}, // 待审批，不算执行
		{"018f3e1a-0a80-7000-8000-0000000000f6", playbookTwo, 6, time.Hour},
		{"018f3e1a-0a80-7000-8000-0000000000f7", playbookTwo, 8, time.Hour}, // 已取消
		{"018f3e1a-0a80-7000-8000-0000000000f8", playbookNew, 2, time.Hour}, // 只有已批准未执行的
	}
	for _, p := range seeds {
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO incident_proposal (id, incident_id, title, action, playbook_id, idem_key, status, expire_time, create_time)
			 VALUES ($1::uuid, $2::uuid, 'step', '{}'::jsonb, $3::uuid, $4::text, $5::smallint, now() + interval '1 hour', $6::timestamptz)`,
			p.id, incA, p.playbook, "idem-"+p.id, p.status, time.Now().Add(-p.age)); err != nil {
			t.Fatalf("seed proposal %s: %v", p.id, err)
		}
	}
	repo := NewProposalRepo(d)
	since := time.Now().Add(-30 * 24 * time.Hour)
	ids := []string{playbookOne, playbookTwo, playbookNew}

	got, err := repo.CountPlaybookExecsMany(ctx, ids, since)
	if err != nil {
		t.Fatalf("CountPlaybookExecsMany() error = %v", err)
	}
	want := map[string]int{}
	for _, id := range ids {
		n, err := repo.CountPlaybookExecs(ctx, id, since)
		if err != nil {
			t.Fatalf("CountPlaybookExecs(%s) error = %v", id, err)
		}
		if n > 0 {
			want[id] = n
		}
	}
	if !reflect.DeepEqual(got, want) || got[playbookOne] != 3 || got[playbookTwo] != 1 {
		t.Fatalf("CountPlaybookExecsMany() = %v, per-playbook counts = %v; want {one: 3, two: 1}", got, want)
	}
	if empty, err := repo.CountPlaybookExecsMany(ctx, nil, since); err != nil || len(empty) != 0 {
		t.Fatalf("CountPlaybookExecsMany(nil) = %v, %v; want empty", empty, err)
	}
}

// 写事务里读成员改走事务连接（ctx 携带事务句柄）：加锁之后、写入之前读到的内容与走连接池相同。
func TestMemberRepo_ListWithinTxMatchesPool(t *testing.T) {
	d := newIntegrationIncidentData(t)
	ctx := context.Background()
	seedIncident(t, d, incA, biz.StatusResponding)
	for user, role := range map[string]int32{memberOne: biz.RoleResponder, memberTwo: biz.RoleWatcher} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO incident_member (incident_id, user_id, role) VALUES ($1::uuid, $2::uuid, $3::smallint)", incA, user, role); err != nil {
			t.Fatalf("seed member %s: %v", user, err)
		}
	}
	members, writer, tm := NewMemberRepo(d), NewIncidentRepo(d), basedata.NewTxManager(d)
	want, err := members.List(ctx, incA)
	if err != nil || len(want) != 2 {
		t.Fatalf("List() via pool = %d members, %v; want 2", len(want), err)
	}
	var got []*biz.Member
	if err := tm.WithinTx(ctx, func(tx txrun.Tx) error {
		if _, err := writer.LockTx(ctx, tx, incA); err != nil {
			return err
		}
		var err error
		got, err = members.List(txrun.NewContext(ctx, tx), incA)
		return err
	}); err != nil {
		t.Fatalf("List() within tx error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("List() within tx = %+v, via pool = %+v", got, want)
	}
}
