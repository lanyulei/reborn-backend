//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

// 本文件锁定「逐行往返 → 单批 / 单语句」改写（AGENTS §13.7）前后的库内状态等价：聚合 upsert、
// 租约回收、整批放回、死信重放取 content 快照。

// TestIntegration_AggUpsertBatch 一个批内四种组状态（开放 / 关闭且抑制期内 / 关闭且抑制期过 /
// 新建）各走各的分支，裁决与库内状态和逐键执行时相同。
func TestIntegration_AggUpsertBatch(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	const (
		evOld = "018f3e1a-3001-7000-8000-000000000001"
		evNew = "018f3e1a-3001-7000-8000-000000000002"
	)
	now := time.Now().UTC()
	inc := func(key, firstEvent string, n int) biz.AggIncrement {
		samples := make([]biz.AggSample, 0, n)
		for range n {
			samples = append(samples, biz.AggSample{EventID: firstEvent, Title: "t", Severity: 2, CreateTime: now})
		}
		return biz.AggIncrement{
			Key: key, BizID: "default", EventType: "host.down", RouteID: idRouteAgg,
			FirstEventID: firstEvent, FirstEventTime: now, Count: n, Samples: samples,
			WindowS: 30, MaxWindow: time.Hour,
		}
	}
	upsert := func(incs ...biz.AggIncrement) map[string]biz.AggVerdict {
		t.Helper()
		var out map[string]biz.AggVerdict
		if err := dbx.WithTx(ctx, d.Pool, func(tx pgx.Tx) error {
			var err error
			out, err = upsertAggGroups(ctx, tx, incs)
			return err
		}); err != nil {
			t.Fatalf("upsertAggGroups() error = %v", err)
		}
		return out
	}
	const kOpen, kSuppressed, kExpired, kNew = "default|a-open", "default|b-suppressed", "default|c-expired", "default|d-new"

	// 首批：三个组各 2 条，全部新建（无抑制）。
	first := upsert(inc(kOpen, evOld, 2), inc(kSuppressed, evOld, 2), inc(kExpired, evOld, 2))
	if len(first) != 3 || first[kOpen].Suppressed || first[kSuppressed].Suppressed || first[kExpired].Suppressed {
		t.Fatalf("first batch verdicts = %+v", first)
	}
	if _, err := d.Pool.Exec(ctx,
		`UPDATE notify_agg_group SET status = 2,
		   suppressed_until = CASE group_key WHEN $1 THEN now() + interval '10 minutes' ELSE now() - interval '1 second' END
		 WHERE group_key IN ($1, $2)`, kSuppressed, kExpired); err != nil {
		t.Fatal(err)
	}

	// 次批（按键有序，同 router）：开放组累加、抑制期内仅计数、抑制期过重开、新键建组。
	second := upsert(inc(kOpen, evNew, 19), inc(kSuppressed, evNew, 3), inc(kExpired, evNew, 4), inc(kNew, evNew, 1))
	if len(second) != 4 || second[kOpen].Suppressed || !second[kSuppressed].Suppressed || second[kExpired].Suppressed || second[kNew].Suppressed {
		t.Fatalf("second batch verdicts = %+v", second)
	}
	type group struct {
		count, status, samples int
		first                  string
	}
	load := func(key string) group {
		t.Helper()
		var g group
		if err := d.Pool.QueryRow(ctx,
			"SELECT count, status, jsonb_array_length(samples), first_event_id::text FROM notify_agg_group WHERE group_key = $1", key).
			Scan(&g.count, &g.status, &g.samples, &g.first); err != nil {
			t.Fatalf("load group %s: %v", key, err)
		}
		return g
	}
	for key, want := range map[string]group{
		kOpen:       {count: 21, status: 1, samples: 20, first: evOld}, // 累加，样本封顶 20
		kSuppressed: {count: 5, status: 2, samples: 2, first: evOld},   // 仅计数，不重开、样本不动
		kExpired:    {count: 4, status: 1, samples: 4, first: evNew},   // 重开：计数 / 样本 / 首事件重置
		kNew:        {count: 1, status: 1, samples: 1, first: evNew},
	} {
		if got := load(key); got != want {
			t.Errorf("group %s = %+v, want %+v", key, got, want)
		}
	}
	if count(t, d, "SELECT count(*) FROM notify_agg_group WHERE group_key = $1 AND suppressed_until IS NULL AND window_until > now()", kExpired) != 1 {
		t.Error("reopened group must clear suppression and open a fresh window")
	}
	if got := upsert(); len(got) != 0 {
		t.Errorf("empty batch verdicts = %+v", got)
	}
}

// TestIntegration_DispatchReapExpiredBatch 一批过期租约一次落账：每行按自己的 attempts 判定
// failed / dead 与退避；delivery_log、dlq 逐行齐全；渲染快照不动；未过期的 sending 行不碰。
func TestIntegration_DispatchReapExpiredBatch(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	repo := NewDispatchRepo(d)
	const (
		ev       = "018f3e1a-3101-7000-8000-000000000001"
		dFirst   = "018f3e1a-3102-7000-8000-000000000001" // 声明后 attempts=1 → failed
		dThird   = "018f3e1a-3102-7000-8000-000000000002" // 声明后 attempts=3 → failed，带快照
		dDead    = "018f3e1a-3102-7000-8000-000000000003" // 声明后 attempts=6 → dead + dlq
		dHolding = "018f3e1a-3102-7000-8000-000000000004" // 租约未过期：不回收
	)
	seedEvent(t, d, ev, "monitor.alert.firing", 2, "", nil)
	for _, id := range []string{dFirst, dThird, dDead, dHolding} {
		seedDispatch(t, d, id, ev, idChanHook, 1, time.Now())
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := d.Pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%v\n%s", err, sql)
		}
	}
	exec(`UPDATE notify_dispatch SET attempts = 2, content = '{"subject":"s","body":"b"}', template_id = $2 WHERE id = $1`, dThird, idTpl)
	exec("UPDATE notify_dispatch SET attempts = 5, recipient = 'dead@example.com', channel_id = $2 WHERE id = $1", dDead, idChanEmail)

	claimed, err := repo.Claim(ctx, 10, time.Millisecond, nil) // 极短租约模拟崩溃
	if err != nil || len(claimed) != 4 {
		t.Fatalf("Claim() = %d, %v; want 4", len(claimed), err)
	}
	exec("UPDATE notify_dispatch SET next_retry_time = now() + interval '1 hour' WHERE id = $1", dHolding)
	time.Sleep(20 * time.Millisecond)

	// 退避随 attempts 变化：验证每行用的是自己的间隔（1 → 10 分钟，3 → 30 分钟）。
	backoff := func(attempts int32) time.Duration { return time.Duration(attempts) * 10 * time.Minute }
	n, err := repo.ReapExpired(ctx, 10, 6, backoff)
	if err != nil || n != 3 {
		t.Fatalf("ReapExpired() = %d, %v; want 3", n, err)
	}
	failed := `SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 4 AND attempts = $2 AND last_error = $3
		AND next_retry_time BETWEEN now() + make_interval(mins => $4) - interval '1 minute' AND now() + make_interval(mins => $4) + interval '1 minute'`
	if count(t, d, failed, dFirst, 1, leaseExpiredError, 10) != 1 {
		t.Error("first-attempt dispatch must be failed with its own backoff (10m)")
	}
	if count(t, d, failed+" AND content->>'subject' = 's' AND template_id = $5", dThird, 3, leaseExpiredError, 30, idTpl) != 1 {
		t.Error("third-attempt dispatch must be failed with its own backoff (30m) and keep its snapshot")
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 5 AND attempts = 6 AND next_retry_time IS NULL AND last_error = $2", dDead, leaseExpiredError) != 1 {
		t.Error("dispatch at max attempts must be dead")
	}
	if count(t, d, `SELECT count(*) FROM notify_dlq WHERE id = $1 AND dispatch_id = $1 AND event_id = $2 AND channel_id = $3
		AND recipient = 'dead@example.com' AND last_error = $4 AND attempts = 6 AND replay_state = 1`, dDead, ev, idChanEmail, leaseExpiredError) != 1 ||
		count(t, d, "SELECT count(*) FROM notify_dlq") != 1 {
		t.Error("exactly the dead dispatch must be dead-lettered with its own columns")
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 2 AND attempts = 1 AND last_error IS NULL", dHolding) != 1 {
		t.Error("unexpired lease must be left alone")
	}
	logged := `SELECT count(*) FROM notify_delivery_log WHERE dispatch_id = $1 AND channel_id = $2 AND attempt = $3
		AND NOT ok AND latency_ms = 0 AND provider_msg_id IS NULL AND error = $4`
	if count(t, d, logged, dFirst, idChanHook, 1, leaseExpiredError) != 1 ||
		count(t, d, logged, dThird, idChanHook, 3, leaseExpiredError) != 1 ||
		count(t, d, logged, dDead, idChanEmail, 6, leaseExpiredError) != 1 ||
		count(t, d, "SELECT count(*) FROM notify_delivery_log") != 3 ||
		count(t, d, "SELECT count(DISTINCT id) FROM notify_delivery_log") != 3 {
		t.Error("reaper must log exactly one failed attempt per reaped dispatch")
	}
	// 已回收的行不再被回收；之后迟到的旧持有者仍被围栏拒绝（见 TestIntegration_DispatchReapExpired）。
	if again, err := repo.ReapExpired(ctx, 10, 6, backoff); err != nil || again != 0 {
		t.Fatalf("second ReapExpired() = %d, %v; want 0", again, err)
	}
}

// TestIntegration_DispatchReleaseBatch 整批放回的每行各自过围栏：属主有效的行回 pending、撤销
// 尝试计数、回拨发布状态；已被放回 / 被他人重领的行原样不动且不报错。
func TestIntegration_DispatchReleaseBatch(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	repo := NewDispatchRepo(d)
	const (
		ev        = "018f3e1a-3201-7000-8000-000000000001"
		dA        = "018f3e1a-3202-7000-8000-000000000001"
		dB        = "018f3e1a-3202-7000-8000-000000000002"
		dReleased = "018f3e1a-3202-7000-8000-000000000003" // 已被单行 Release
		dRetaken  = "018f3e1a-3202-7000-8000-000000000004" // 已被回收后他人重领（attempts 变了）
	)
	seedEvent(t, d, ev, "monitor.alert.firing", 2, "", nil)
	for _, id := range []string{dA, dB, dReleased, dRetaken} {
		seedDispatch(t, d, id, ev, idChanHook, 1, time.Now())
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_dispatch SET attempts = 2 WHERE id = $1", dB); err != nil {
		t.Fatal(err)
	}
	claimed, err := repo.Claim(ctx, 10, 5*time.Minute, nil)
	if err != nil || len(claimed) != 4 {
		t.Fatalf("Claim() = %d, %v; want 4", len(claimed), err)
	}
	byID := map[string]*biz.ClaimedDispatch{}
	for _, c := range claimed {
		byID[c.ID] = c
	}
	if err := repo.Release(ctx, byID[dReleased], time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Pool.Exec(ctx,
		"UPDATE notify_dispatch SET publish_state = 1, relay_claim_until = now() + interval '1 minute', attempts = CASE WHEN id = $1 THEN attempts + 1 ELSE attempts END WHERE event_id = $2",
		dRetaken, ev); err != nil {
		t.Fatal(err)
	}

	if err := repo.ReleaseBatch(ctx, claimed, 30*time.Second); err != nil {
		t.Fatalf("ReleaseBatch() error = %v", err)
	}
	released := `SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 1 AND attempts = $2 AND publish_state = 0 AND relay_claim_until IS NULL
		AND next_retry_time > now() AND next_retry_time < now() + interval '1 minute'`
	if count(t, d, released, dA, 0) != 1 || count(t, d, released, dB, 2) != 1 {
		t.Error("owned dispatches must return to pending with the attempt undone")
	}
	// 已被单行放回：不得再减一次 attempts、不得把到期时刻从 1 小时改成 30 秒。
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 1 AND attempts = 0 AND publish_state = 1 AND next_retry_time > now() + interval '50 minutes'", dReleased) != 1 {
		t.Error("already released dispatch must be left untouched")
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 2 AND attempts = 2 AND publish_state = 1 AND relay_claim_until IS NOT NULL", dRetaken) != 1 {
		t.Error("dispatch re-claimed by another holder must be left untouched")
	}
	// 重复放回全部围栏不过：无变化、无错误；空批同样无错误。
	if err := repo.ReleaseBatch(ctx, claimed, time.Hour); err != nil {
		t.Fatalf("second ReleaseBatch() error = %v", err)
	}
	if count(t, d, released, dA, 0) != 1 || count(t, d, released, dB, 2) != 1 {
		t.Error("stale batch release must not move the retry time or attempts again")
	}
	if err := repo.ReleaseBatch(ctx, nil, time.Second); err != nil {
		t.Fatalf("empty ReleaseBatch() error = %v", err)
	}
	if count(t, d, "SELECT count(*) FROM notify_delivery_log") != 0 {
		t.Error("release must not log an attempt")
	}
}

// TestIntegration_DlqReplayBatchContent 一批死信的 content 快照一次取回，逐条判定不变：源
// dispatch 在且不晚于死信时间 → 保留聚合上下文；源 dispatch 已清理 / 创建时间晚于该死信 →
// 按单事件重放（即便它落在本批的剪枝上界之内）。
func TestIntegration_DlqReplayBatchContent(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	const (
		ev      = "018f3e1a-3301-7000-8000-000000000001"
		dAgg    = "018f3e1a-3302-7000-8000-000000000001"
		dGone   = "018f3e1a-3302-7000-8000-000000000002"
		dLater  = "018f3e1a-3302-7000-8000-000000000003"
		dNoSnap = "018f3e1a-3302-7000-8000-000000000004"
	)
	seedEvent(t, d, ev, "monitor.alert.firing", 2, "", nil)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := d.Pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%v\n%s", err, sql)
		}
	}
	const agg = `{"subject":"old","body":"old","agg":{"count":9,"digest":true}}`
	exec(`INSERT INTO notify_dispatch (id, event_id, channel_id, recipient, content, status, attempts, next_retry_time, send_key, create_time) VALUES
		($1, $4, $5, 'agg@example.com', $6, 5, 6, NULL, 'k', now() - interval '10 minutes'),
		($2, $4, $5, 'later@example.com', $6, 5, 6, NULL, 'k', now() - interval '10 minutes'),
		($3, $4, $5, 'nosnap@example.com', NULL, 5, 6, NULL, 'k', now() - interval '10 minutes')`,
		dAgg, dLater, dNoSnap, ev, idChanHook, agg)
	// dLater 的死信时间早于其 dispatch 的创建时间（契约外，仅为锁定逐条的时间条件）。
	exec(`INSERT INTO notify_dlq (id, dispatch_id, event_id, channel_id, recipient, last_error, attempts, replay_state, create_time) VALUES
		($1, $1, $5, $6, 'agg@example.com', 'boom', 6, 2, now() - interval '5 minutes'),
		($2, $2, $5, $6, 'gone@example.com', 'boom', 6, 2, now() - interval '5 minutes'),
		($3, $3, $5, $6, 'later@example.com', 'boom', 6, 2, now() - interval '1 hour'),
		($4, $4, $5, $6, 'nosnap@example.com', 'boom', 6, 2, now() - interval '5 minutes')`,
		dAgg, dGone, dLater, dNoSnap, ev, idChanHook)

	n, err := biz.NewDlqUsecase(NewDlqRepo(d), testLogger()).ReplayOnce(ctx, 10)
	if err != nil || n != 4 {
		t.Fatalf("ReplayOnce() = %d, %v; want 4", n, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dlq WHERE replay_state = 3") != 4 {
		t.Fatal("every claimed dead letter must be marked replayed")
	}
	fresh := "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 1 AND recipient = $2"
	if count(t, d, fresh+" AND content->'agg'->>'count' = '9' AND content->>'subject' IS NULL", ev, "agg@example.com") != 1 {
		t.Error("replay must keep the aggregate context of its own source dispatch")
	}
	for _, recipient := range []string{"gone@example.com", "later@example.com", "nosnap@example.com"} {
		if count(t, d, fresh+" AND content IS NULL", ev, recipient) != 1 {
			t.Errorf("replay for %s must be a single-event dispatch (no content)", recipient)
		}
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 1", ev) != 4 {
		t.Error("exactly one fresh dispatch per replayed dead letter")
	}
}
