//go:build integration

package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 评审第一轮（docs/monitor-review-round1.md）在真实 PostgreSQL 上的回归与补充场景。

// ---------------------------------------------------------------------------
// C3 加锁顺序
// ---------------------------------------------------------------------------

// isDeadlock 报告错误是否 PostgreSQL 的死锁检测（40P01）。
func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

// race 让 a、b 同时起跑并返回各自的错误。
func race(a, b func() error) (errA, errB error) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; errA = a() }()
	go func() { defer wg.Done(); <-start; errB = b() }()
	close(start)
	wg.Wait()
	return errA, errB
}

// 数据源的删除与更新 / 新建缺省数据源并发：修复前 Delete 先锁数据源行再 bump 配置版本，而 Update /
// Create 先锁配置版本行再碰数据源行，两个事务互相等待，被数据库判为死锁（接口返回 500）。
func TestDatasourceRepo_ConcurrentWritesDoNotDeadlock(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	const rounds = 60
	for i := range rounds {
		d := e.datasource(fmt.Sprintf("race-%d", i), true)
		var errDelete, errOther error
		if i%2 == 0 {
			upd := *d
			upd.Description = "x"
			errDelete, errOther = race(
				func() error { return e.sources.Delete(ctx, d.ID, uAdmin) },
				func() error { return e.sources.Update(ctx, &upd, biz.SecretChange{}, 1, uAdmin) },
			)
			// 更新要么先完成，要么发现数据源已被删除。
			if errOther != nil && !kerrors.Is(errOther, biz.ErrDatasourceNotFound) {
				t.Fatalf("round %d: update = %v (deadlock = %v)", i, errOther, isDeadlock(errOther))
			}
		} else {
			// 新建一个缺省数据源要取消 d 的缺省标记（更新 d 那一行）。
			next := &biz.Datasource{
				ID: newID(t), Name: fmt.Sprintf("race-next-%d", i), Type: mc.DatasourceTypePrometheus, URL: "http://x:1",
				AuthType: mc.AuthNone, TimeoutMS: 5000, IsDefault: true, Enabled: true,
			}
			errDelete, errOther = race(
				func() error { return e.sources.Delete(ctx, d.ID, uAdmin) },
				func() error { return e.sources.Create(ctx, next, nil, uAdmin) },
			)
			if errOther != nil {
				t.Fatalf("round %d: create default = %v (deadlock = %v)", i, errOther, isDeadlock(errOther))
			}
		}
		if errDelete != nil {
			t.Fatalf("round %d: delete = %v (deadlock = %v)", i, errDelete, isDeadlock(errDelete))
		}
	}
	if n := e.count("SELECT count(*) FROM monitor_datasource WHERE is_default AND delete_time IS NULL"); n > 1 {
		t.Fatalf("default datasources = %d", n)
	}
}

// 分组的改名与删除并发：修复前 Update 先更新分组行再 bump 配置版本，Delete 先锁配置版本行再锁分组行。
func TestGroupRepo_ConcurrentUpdateAndDeleteDoNotDeadlock(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	parent := e.group("", "race")
	const rounds = 60
	for i := range rounds {
		g := e.group(parent.ID, fmt.Sprintf("g-%d", i))
		errUpdate, errDelete := race(
			func() error {
				return e.groups.Update(ctx, &biz.RuleGroup{ID: g.ID, Name: fmt.Sprintf("g-%d-renamed", i)}, uAdmin)
			},
			func() error { return e.groups.Delete(ctx, g.ID, uAdmin) },
		)
		if errUpdate != nil && !kerrors.Is(errUpdate, biz.ErrGroupNotFound) {
			t.Fatalf("round %d: update = %v (deadlock = %v)", i, errUpdate, isDeadlock(errUpdate))
		}
		if errDelete != nil {
			t.Fatalf("round %d: delete = %v (deadlock = %v)", i, errDelete, isDeadlock(errDelete))
		}
	}
}

// ---------------------------------------------------------------------------
// C15 删除仍被引用的分组
// ---------------------------------------------------------------------------

func TestGroupRepo_DeleteRefusedWhileReferenced(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group("", "referenced")
	keep := e.group("", "keep")
	start, end := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	mute := func(name string) *biz.Mute {
		m := &biz.Mute{ID: newID(t), Spec: biz.MuteSpec{GroupID: g.ID, Name: name, Reason: "r", Mode: 1, TimeType: 1, StartTime: &start, EndTime: &end, Enabled: true}}
		if err := e.mutes.Create(ctx, m, uAlice); err != nil {
			t.Fatalf("create mute: %v", err)
		}
		return m
	}
	sub := func(name string, groups ...string) *biz.Subscription {
		s := &biz.Subscription{ID: newID(t), OwnerID: uAlice, Spec: biz.SubscriptionSpec{
			Name: name, GroupIDs: groups, Severities: []int32{3}, OnFiring: true, Receivers: mc.Receivers{Self: true}, Enabled: true,
		}}
		if err := e.subs.Create(ctx, s); err != nil {
			t.Fatalf("create subscription: %v", err)
		}
		return s
	}
	m1, m2 := mute("m1"), mute("m2")
	s1 := sub("s1", g.ID)
	s2 := sub("s2", keep.ID, g.ID)
	sub("unrelated", keep.ID)
	wantCounts := func(what string, mutes, subs string) {
		t.Helper()
		err := e.groups.Delete(ctx, g.ID, uAdmin)
		wantErr(t, what, err, biz.ErrGroupInUse)
		ke := kerrors.FromError(err)
		if ke.Code != 409 || ke.Metadata["mute_count"] != mutes || ke.Metadata["subscription_count"] != subs {
			t.Fatalf("%s: %+v, want 409 with mute_count=%s subscription_count=%s", what, ke, mutes, subs)
		}
	}
	wantCounts("two mutes, two subscriptions", "2", "2")
	if n := e.count("SELECT count(*) FROM monitor_rule_group WHERE id = $1 AND delete_time IS NULL", g.ID); n != 1 {
		t.Fatal("a refused delete must leave the group in place")
	}
	// 已删除的屏蔽规则 / 订阅不算引用。
	if err := e.mutes.Delete(ctx, m1.ID, 1, uAlice); err != nil {
		t.Fatal(err)
	}
	if err := e.subs.Delete(ctx, s1.ID, 1, uAlice); err != nil {
		t.Fatal(err)
	}
	wantCounts("one of each left", "1", "1")
	if err := e.mutes.Delete(ctx, m2.ID, 1, uAlice); err != nil {
		t.Fatal(err)
	}
	wantCounts("only a subscription left", "0", "1")
	// 订阅改为不再订阅它，引用解除。
	s2.Spec.GroupIDs = []string{keep.ID}
	if err := e.subs.Update(ctx, s2, 1, uAlice); err != nil {
		t.Fatal(err)
	}
	if err := e.groups.Delete(ctx, g.ID, uAdmin); err != nil {
		t.Fatalf("delete once unreferenced: %v", err)
	}
	// 反方向：分组删除之后不能再被订阅引用（新建与编辑都在写事务里确认分组存在）。
	late := &biz.Subscription{ID: newID(t), OwnerID: uAlice, Spec: biz.SubscriptionSpec{
		Name: "late", GroupIDs: []string{keep.ID, g.ID}, Severities: []int32{3}, OnFiring: true, Receivers: mc.Receivers{Self: true}, Enabled: true,
	}}
	wantErr(t, "subscribe to a deleted group", e.subs.Create(ctx, late), biz.ErrGroupNotFound)
	s2.Spec.GroupIDs = []string{g.ID}
	wantErr(t, "re-point a subscription at a deleted group", e.subs.Update(ctx, s2, 2, uAlice), biz.ErrGroupNotFound)
	if n := e.count("SELECT count(*) FROM monitor_subscription WHERE group_ids @> ARRAY[$1::uuid] AND delete_time IS NULL", g.ID); n != 0 {
		t.Fatalf("subscriptions pointing at the deleted group: %d", n)
	}
}

// 订阅的写入与分组删除互斥：并发下不会留下指向已删除分组的订阅。
func TestSubscriptionWrite_RacesWithGroupDelete(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	const rounds = 40
	for i := range rounds {
		g := e.group("", fmt.Sprintf("sub-race-%d", i))
		s := &biz.Subscription{ID: newID(t), OwnerID: uAlice, Spec: biz.SubscriptionSpec{
			Name: "s", GroupIDs: []string{g.ID}, Severities: []int32{3}, OnFiring: true, Receivers: mc.Receivers{Self: true}, Enabled: true,
		}}
		errCreate, errDelete := race(
			func() error { return e.subs.Create(ctx, s) },
			func() error { return e.groups.Delete(ctx, g.ID, uAdmin) },
		)
		created := errCreate == nil
		deleted := errDelete == nil
		switch {
		case created && deleted:
			t.Fatalf("round %d: the group was deleted although a subscription references it", i)
		case !created && !kerrors.Is(errCreate, biz.ErrGroupNotFound):
			t.Fatalf("round %d: create = %v", i, errCreate)
		case !deleted && !kerrors.Is(errDelete, biz.ErrGroupInUse):
			t.Fatalf("round %d: delete = %v", i, errDelete)
		}
	}
}

// ---------------------------------------------------------------------------
// C10 索引
// ---------------------------------------------------------------------------

// explain 返回查询计划的文本。
func (e *env) explain(sql string, args ...any) string {
	e.t.Helper()
	rows, err := e.pool.Query(context.Background(), "EXPLAIN "+sql, args...)
	if err != nil {
		e.t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			e.t.Fatal(err)
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

// 在有一定数据量的表上，按首次触发时间的范围查询与订阅候选预筛都走新加的索引，而不是全表扫描。
// 用的就是仓储实际执行的 WHERE（alertWhere / candidateWhere）。
func TestQueryIndexes_UsedOnPopulatedTables(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	// 12 万条历史告警，首次触发时间均匀铺在 120 天里，分属 40 个分组、200 条规则。
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO monitor_alert (id, fingerprint, rule_id, rule_name, group_id, datasource_id, datasource_name, severity, status,
		   labels, first_fire_time, last_fire_time, resolve_time)
		 SELECT gen_random_uuid(), substr(md5(i::text), 1, 32),
		        ('018f3e1a-1000-7000-8000-' || lpad((i % 200)::text, 12, '0'))::uuid, 'r' || (i % 200),
		        ('018f3e1a-2000-7000-8000-' || lpad((i % 40)::text, 12, '0'))::uuid,
		        '018f3e1a-3000-7000-8000-000000000001', 'vm', 1 + i % 4, 2,
		        jsonb_build_object('instance', 'h' || (i % 500)),
		        now() - (i * interval '86.4 seconds'), now() - (i * interval '86.4 seconds') + interval '5 minutes',
		        now() - (i * interval '86.4 seconds') + interval '10 minutes'
		 FROM generate_series(1, 120000) AS i`); err != nil {
		t.Fatalf("populate alerts: %v", err)
	}
	// 2 万条订阅，各订阅 2000 个分组里的 1–2 个；其中一成已删除。
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO monitor_subscription (id, name, owner_id, group_ids, severities, delete_time)
		 SELECT gen_random_uuid(), 's' || i, '018f3e1a-4000-7000-8000-000000000001',
		        ARRAY[('018f3e1a-5000-7000-8000-' || lpad((i % 2000)::text, 12, '0'))::uuid,
		              ('018f3e1a-5000-7000-8000-' || lpad(((i * 7) % 2000)::text, 12, '0'))::uuid],
		        ARRAY[3, 4]::smallint[], CASE WHEN i % 10 = 0 THEN now() END
		 FROM generate_series(1, 20000) AS i`); err != nil {
		t.Fatalf("populate subscriptions: %v", err)
	}
	if _, err := e.pool.Exec(ctx, "ANALYZE monitor_alert, monitor_subscription"); err != nil {
		t.Fatal(err)
	}

	// 告警列表带时间范围（超级管理员：不限分组）与限定分组两种口径。
	from, to := time.Now().Add(-49*time.Hour), time.Now().Add(-48*time.Hour)
	groups := make([]string, 0, 30)
	for i := range 30 {
		groups = append(groups, fmt.Sprintf("018f3e1a-2000-7000-8000-%012d", i))
	}
	for name, filter := range map[string]biz.AlertFilter{
		"all groups":      {Groups: biz.GroupFilter{All: true}, FireStart: &from, FireEnd: &to},
		"readable groups": {Groups: biz.GroupFilter{IDs: groups}, FireStart: &from, FireEnd: &to},
	} {
		var a sqlArgs
		where, err := alertWhere(&a, filter)
		if err != nil {
			t.Fatal(err)
		}
		for what, sql := range map[string]string{
			"count": "SELECT count(*) FROM monitor_alert a WHERE " + where,
			"page":  "SELECT " + alertColumns + " FROM monitor_alert a WHERE " + where + " ORDER BY a.last_fire_time DESC, a.id DESC LIMIT 20",
		} {
			plan := e.explain(sql, a.args...)
			if !strings.Contains(plan, "idx_monitor_alert_first_fire") || strings.Contains(plan, "Seq Scan on monitor_alert") {
				t.Errorf("%s / %s: the time-range query must use idx_monitor_alert_first_fire:\n%s", name, what, plan)
			}
		}
	}
	// 结果正确：一小时窗口内约 41–42 条。
	items, total, err := e.alerts.List(ctx, biz.AlertFilter{Groups: biz.GroupFilter{All: true}, FireStart: &from, FireEnd: &to}, 1, 100)
	if err != nil || total < 40 || total > 43 || int64(len(items)) != total {
		t.Fatalf("time-range list = %d items / %d total err=%v", len(items), total, err)
	}

	// 订阅候选预筛、订阅列表的范围过滤、删除分组前的引用检查：都走 GIN 索引。
	where, args := candidateWhere(biz.SubscriptionCandidate{
		GroupIDs: []string{"018f3e1a-5000-7000-8000-000000000007", "018f3e1a-5000-7000-8000-000000000008"},
		RuleID:   newID(t), DatasourceID: newID(t), Severity: 3,
	})
	for what, q := range map[string]struct {
		sql  string
		args []any
	}{
		"dispatch candidates": {"SELECT " + subscriptionColumns + " FROM monitor_subscription s WHERE s.delete_time IS NULL" + where + " ORDER BY s.id LIMIT 500", args},
		"candidate count":     {"SELECT count(*) FROM monitor_subscription s WHERE s.delete_time IS NULL" + where, args},
		"list by groups": {
			"SELECT count(*) FROM monitor_subscription s WHERE s.delete_time IS NULL AND (s.owner_id = $1 OR s.group_ids && $2::uuid[])",
			[]any{uAlice, dbx.TextArray([]string{"018f3e1a-5000-7000-8000-000000000007"})},
		},
		"group delete check": {
			"SELECT count(*) FROM monitor_subscription WHERE group_ids @> ARRAY[$1::uuid] AND delete_time IS NULL",
			[]any{"018f3e1a-5000-7000-8000-000000000007"},
		},
	} {
		plan := e.explain(q.sql, q.args...)
		if !strings.Contains(plan, "idx_monitor_subscription_groups") || strings.Contains(plan, "Seq Scan on monitor_subscription") {
			t.Errorf("%s: must use idx_monitor_subscription_groups:\n%s", what, plan)
		}
	}
	subs, n, err := e.subs.ListCandidates(ctx, biz.SubscriptionCandidate{
		GroupIDs: []string{"018f3e1a-5000-7000-8000-000000000007", "018f3e1a-5000-7000-8000-000000000008"},
		RuleID:   newID(t), DatasourceID: newID(t), Severity: 3,
	}, 500)
	if err != nil || len(subs) == 0 || n != len(subs) || len(subs) > 60 {
		t.Fatalf("candidates = %d / %d err=%v", len(subs), n, err)
	}
}

// ---------------------------------------------------------------------------
// 通知派发：联调未覆盖的场景，落到真实的 notify_event
// ---------------------------------------------------------------------------

// eventRow 一条落库的通知事件。
type eventRow struct {
	eventType, title, dedupKey string
	recipients                 []string
	payload                    mc.NotifyPayload
}

func (d *dispatchEnv) events(alertID string) []eventRow {
	d.t.Helper()
	rows, err := d.pool.Query(context.Background(),
		"SELECT event_type, title, COALESCE(dedup_key, ''), recipients, payload FROM notify_event WHERE payload->>'alert_id' = $1 ORDER BY create_time, id", alertID)
	if err != nil {
		d.t.Fatal(err)
	}
	defer rows.Close()
	var out []eventRow
	for rows.Next() {
		var ev eventRow
		var recipients, payload []byte
		if err := rows.Scan(&ev.eventType, &ev.title, &ev.dedupKey, &recipients, &payload); err != nil {
			d.t.Fatal(err)
		}
		if err := json.Unmarshal(recipients, &ev.recipients); err != nil {
			d.t.Fatalf("decode recipients %s: %v", recipients, err)
		}
		if err := json.Unmarshal(payload, &ev.payload); err != nil {
			d.t.Fatal(err)
		}
		out = append(out, ev)
	}
	return out
}

// 规则的通知对象是值班表与部门负责人、人数超过 20；订阅的接收方是另一张值班表与群标识。
// 断言落库的通知事件：分片大小、去重键互不相同、收件人恰好各一次、群标识原样作为收件人。
func TestDispatch_OnCallDeptLeadersGroupsAndChunking(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	schedA, schedB, dept := newID(t), newID(t), newID(t)
	users := func(n int) []string {
		out := make([]string, 0, n)
		for range n {
			id := newID(t)
			d.org.Users[id] = "u-" + id[len(id)-4:]
			out = append(out, id)
		}
		return out
	}
	onDuty, leaders, subDuty := users(31), users(14), users(27)
	disabled := newID(t) // 值班表里已停用的人
	d.org.OnDutyUsers = map[string][]string{schedA: append(slices.Clone(onDuty), disabled), schedB: append(slices.Clone(subDuty), onDuty[0])}
	d.org.Leaders = map[string][]string{dept: append(slices.Clone(leaders), onDuty[1])} // 负责人里有一个也在值班
	d.rule.Spec.Notify = mc.NotifyTargets{ScheduleIDs: []string{schedA}, DeptIDs: []string{dept}}
	if err := d.rules.Update(ctx, d.rule, d.rule.Version, biz.RuleChange{Action: mc.RuleActionUpdate, Operator: uAlice}); err != nil {
		t.Fatal(err)
	}
	groups := []string{"$group:ops", "$dingtalk:room-1"}
	sub := &biz.Subscription{ID: newID(t), OwnerID: uAlice, Spec: biz.SubscriptionSpec{
		Name: "值班表 + 群", GroupIDs: []string{d.group.ID}, Severities: []int32{mc.SeverityCritical}, OnFiring: true, Enabled: true,
		Receivers: mc.Receivers{ScheduleIDs: []string{schedB}, Groups: groups},
	}}
	if err := d.subs.Create(ctx, sub); err != nil {
		t.Fatal(err)
	}
	alertID := d.firingAlert()
	d.outboxRow(alertID, mc.NotifyKindFiring, 1)
	d.mustRun(1)

	events := d.events(alertID)
	byType := map[string][]eventRow{}
	keys := map[string]bool{}
	for _, ev := range events {
		byType[ev.eventType] = append(byType[ev.eventType], ev)
		if len(ev.recipients) == 0 || len(ev.recipients) > 20 {
			t.Fatalf("%s: chunk of %d recipients", ev.eventType, len(ev.recipients))
		}
		if keys[ev.dedupKey] || len(ev.dedupKey) > 128 {
			t.Fatalf("dedup key %q is duplicated or too long", ev.dedupKey)
		}
		keys[ev.dedupKey] = true
	}
	// 规则：31 值班 + 14 负责人 = 45 人 → 3 条；订阅：27 值班 + 2 群 = 29 → 2 条。
	if len(byType[mc.EventAlertFiring]) != 3 || len(byType[mc.EventSubscriptionFiring]) != 2 || len(events) != 5 {
		t.Fatalf("events: rule = %d, subscription = %d, total = %d; want 3 / 2 / 5",
			len(byType[mc.EventAlertFiring]), len(byType[mc.EventSubscriptionFiring]), len(events))
	}
	collect := func(evs []eventRow) []string {
		var out []string
		for _, ev := range evs {
			out = append(out, ev.recipients...)
		}
		return out
	}
	// 值班人在前、负责人在后；重复的人只出现一次，停用的人不出现。
	if got := collect(byType[mc.EventAlertFiring]); !slices.Equal(got, slices.Concat(onDuty, leaders)) {
		t.Fatalf("rule recipients (%d) are not on-duty users followed by department leaders, each once", len(got))
	}
	// 订阅：已由规则通知到的值班人（onDuty[0]）不再重复；群标识原样在收件人里。
	if got := collect(byType[mc.EventSubscriptionFiring]); !slices.Equal(got, slices.Concat(subDuty, groups)) {
		t.Fatalf("subscription recipients (%d) = %v", len(got), got[max(0, len(got)-3):])
	}
	for _, ev := range byType[mc.EventSubscriptionFiring] {
		if ev.payload.SubscriptionName != "值班表 + 群" || !strings.Contains(ev.dedupKey, ":s:") {
			t.Fatalf("subscription event = %+v", ev)
		}
	}
	// 通知记录：规则一条、订阅一条，各自记全部收件人与全部事件；「通知过我的」按它查。
	logs, err := d.logs.ListByAlert(ctx, alertID, 10)
	if err != nil || len(logs) != 2 || len(logs[0].Recipients) != 45 || len(logs[0].NotifyEventIDs) != 3 ||
		logs[1].SubscriptionID != sub.ID || len(logs[1].Recipients) != 29 || len(logs[1].NotifyEventIDs) != 2 {
		t.Fatalf("notify logs = %+v err=%v", logs, err)
	}
	for _, who := range []string{onDuty[30], leaders[13], subDuty[26]} {
		if ok, err := d.logs.IsRecipient(ctx, alertID, who); err != nil || !ok {
			t.Fatalf("recipient %s not found via the recipients index: %v", who, err)
		}
	}
	if ok, _ := d.logs.IsRecipient(ctx, alertID, disabled); ok {
		t.Fatal("a disabled on-duty user must not be recorded as a recipient")
	}
	if p := d.progress(alertID); p.count != 1 {
		t.Fatalf("notify_count = %d, want 1 (chunks do not count separately)", p.count)
	}
}

// 重试耗尽后丢弃：之后同一事件后面入箱的恢复通知不再被它挡住，并因从未发出过触发通知而丢弃。
func TestDispatch_ExhaustedRowUnblocksLaterRows(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	alertID := d.firingAlert()
	first := d.outboxRow(alertID, mc.NotifyKindFiring, 1)
	if _, err := d.pool.Exec(ctx, "UPDATE monitor_alert SET status = 2, resolve_time = now() WHERE id = $1", alertID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	second := d.outboxRow(alertID, mc.NotifyKindResolved, 2)
	for i := 1; i < mc.OutboxMaxAttempts; i++ {
		d.writer.failNext(1)
		d.mustFail(first, 0)
		if s := d.outboxState(second); s.status != mc.OutboxPending || s.attempts != 0 {
			t.Fatalf("attempt %d: the later row must wait untouched: %+v", i, s)
		}
		d.expireBackoff(first)
	}
	// 第十次失败：第一行被丢弃；同一轮里第二行随即被领取。
	d.writer.failNext(1)
	if n, err := d.run(); err != nil || n != 1 {
		t.Fatalf("final attempt: Run() = %d, %v; want the later row handled in the same run", n, err)
	}
	if s := d.outboxState(first); s.status != mc.OutboxDropped || s.attempts != mc.OutboxMaxAttempts || !strings.Contains(s.lastError, "notify pipeline unavailable") {
		t.Fatalf("exhausted row = %+v", s)
	}
	if s := d.outboxState(second); s.status != mc.OutboxDropped || s.lastError != "未发出过触发通知，不发恢复通知" {
		t.Fatalf("later row = %+v", s)
	}
	if c := d.count("SELECT count(*) FROM notify_event") + d.count("SELECT count(*) FROM monitor_notify_log"); c != 0 {
		t.Fatalf("nothing was delivered, yet %d event / log rows exist", c)
	}
	if p := d.progress(alertID); p.count != 0 || p.last != nil {
		t.Fatalf("notify progress = %+v", p)
	}
	d.mustRun(0)
}

// ---------------------------------------------------------------------------
// 以受限的登录账号运行
// ---------------------------------------------------------------------------

// recordingEvents 不落库的通知事件写入方：受限账号对 notify_dedup / notify_event 没有权限，用它隔离出
// 监控域自己的语句。
type recordingEvents struct {
	mu     sync.Mutex
	events []biznotify.Event
}

func (r *recordingEvents) NotifyEvent(_ context.Context, _ txrun.Tx, ev biznotify.Event) (*biznotify.Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ev.ID = fmt.Sprintf("018f3e1a-9000-7000-8000-%012d", len(r.events)+1)
	r.events = append(r.events, ev)
	return &ev, nil
}

// loginPool 新建一个 LOGIN 角色（非超级用户，只是 monitor_core_role 的成员），以它的身份连接测试库。
// 角色是集群级对象：测试结束时删除。口令随机生成，不落日志。
func loginPool(t *testing.T, admin *pgxpool.Pool) (*basedata.Data, string) {
	t.Helper()
	ctx := context.Background()
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	role, password := "monitor_core_login_t_"+hex.EncodeToString(buf[:4]), hex.EncodeToString(buf[4:])+"Aa1"
	// 标识符与口令都是本函数生成的十六进制串，不含需要转义的字符。
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD '%s' IN ROLE monitor_core_role", role, password)); err != nil {
		t.Fatalf("create login role: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role); err != nil {
			t.Errorf("drop login role %s: %v", role, err)
		}
	})
	cfg := admin.Config().ConnConfig
	sslmode := "disable"
	if cfg.TLSConfig != nil {
		sslmode = "require"
	}
	d := openData(t, fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", cfg.Host, cfg.Port, role, password, cfg.Database, sslmode))
	return d, role
}

// 生产环境里 core 不是超级用户：用一个真实的登录账号（只是 monitor_core_role 的成员，没有任何别的
// 权限）把本包的语句跑一遍，并跑通通知派发在监控域内的全部写入。
func TestCoreLoginRole(t *testing.T) {
	admin := newDispatchEnv(t)
	ctx := context.Background()
	d, role := loginPool(t, admin.pool)
	var current, session string
	var super bool
	var memberOf []string
	if err := d.Pool.QueryRow(ctx,
		`SELECT current_user::text, session_user::text, (SELECT rolsuper FROM pg_roles WHERE rolname = current_user),
		        ARRAY(SELECT g.rolname::text FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid
		              JOIN pg_roles u ON u.oid = m.member WHERE u.rolname = current_user)`).
		Scan(&current, &session, &super, &memberOf); err != nil {
		t.Fatal(err)
	}
	if current != role || session != role || super || !slices.Equal(memberOf, []string{"monitor_core_role"}) {
		t.Fatalf("connected as %s / %s super=%v member of %v; want a plain login role in monitor_core_role only", current, session, super, memberOf)
	}

	exerciseAsCoreRole(t, admin, d)

	// 通知派发：领取、读事件与规则、读分组树与授权、订阅预筛、写通知记录、回写通知计数、终结发件箱行
	// ——全部以受限账号执行。通知事件本身属于 notify 域，这里用不落库的写入方代替。
	limited := reposOf(t, d)
	sub := &biz.Subscription{ID: newID(t), OwnerID: uAlice, Spec: biz.SubscriptionSpec{
		Name: "给群", GroupIDs: []string{admin.group.ID}, Severities: []int32{mc.SeverityCritical}, OnFiring: true, Enabled: true,
		Receivers: mc.Receivers{Groups: []string{"$group:ops"}},
	}}
	if err := limited.subs.Create(ctx, sub); err != nil {
		t.Fatalf("create subscription as login role: %v", err)
	}
	alertID := admin.firingAlert()
	ob := admin.outboxRow(alertID, mc.NotifyKindFiring, 1)
	rec := &recordingEvents{}
	uc := admin.newUsecase(limited, rec)
	if n, err := uc.Run(ctx); err != nil || n != 1 {
		t.Fatalf("dispatch as login role: Run() = %d, %v", n, err)
	}
	if s := admin.outboxState(ob); s.status != mc.OutboxDone {
		t.Fatalf("outbox row = %+v", s)
	}
	if p := admin.progress(alertID); p.count != 1 || p.last == nil {
		t.Fatalf("notify progress = %+v", p)
	}
	if len(rec.events) != 2 || !slices.Equal(rec.events[0].Recipients, []string{uAlice}) || !slices.Equal(rec.events[1].Recipients, []string{"$group:ops"}) {
		t.Fatalf("events = %+v", rec.events)
	}
	if n := admin.count("SELECT count(*) FROM monitor_notify_log WHERE alert_id = $1", alertID); n != 2 {
		t.Fatalf("notify logs = %d, want 2", n)
	}
	// 失败记账（独立语句）同样在授权内：第二条告警的派发让写入方报错。
	other := admin.firingAlert()
	ob2 := admin.outboxRow(other, mc.NotifyKindFiring, 1)
	admin.writer.failNext(1)
	if n, err := admin.newUsecase(limited, admin.writer).Run(ctx); err != nil || n != 0 {
		t.Fatalf("failing dispatch as login role: Run() = %d, %v", n, err)
	}
	// 写入方经受限账号的事务写通知事件。告警通知带去重键，第一条语句是 notify_dedup 的去重门闩
	// （之后才是 notify_event 的 INSERT）：门闩被数据库拒绝（42501），整行回滚并记一次失败。
	// 这说明 monitor_core_role 只覆盖监控域的表——core 的登录账号还需要它原有的 notify_* 写权限，
	// 派发才能真正发出通知。
	if s := admin.outboxState(ob2); s.status != mc.OutboxPending || s.attempts != 1 || !strings.Contains(s.lastError, "42501") ||
		!strings.Contains(s.lastError, "notify_dedup") || strings.Contains(s.lastError, "notify_event") {
		t.Fatalf("row after a refused notify write = %+v, want pending / attempts 1 / insufficient privilege on notify_dedup", s)
	}
}
