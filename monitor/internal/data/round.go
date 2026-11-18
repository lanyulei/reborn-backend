package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.RoundRepo = (*roundRepo)(nil)

// roundRepo 一轮评估的落库：状态 / 告警 / 发件箱 / 时间线 / 事件动作 / 判定记录 / 任务写回同一事务。
// 多行写一律 unnest 单语句，除新建告警（需要读回实际 ID）外全部并入一个 pgx.Batch。
type roundRepo struct {
	data *Data
}

// NewRoundRepo new a roundRepo.
func NewRoundRepo(data *Data) *roundRepo { return &roundRepo{data: data} }

// stateQuery 读取任务的判定状态及其活跃告警的通知进度。告警已不再活跃（被关闭 / 清理）时
// 告警列为 NULL，状态机据此按「没有事件」处理。「待派发」只看规则自身的触发 / 重复通知（升级通知的进度
// 另读，见 loadEscalation）。其后一列是首次通知行被丢弃的时刻（core 在终结一行时把 done_time 写成终结
// 时间）；该行不存在、不是已丢弃状态或事件已经通知过时为 NULL。最后几列是第二期加在告警行上的：认领、
// 抑制、关联资源、升级级数、工单——与前面的告警列来自同一行，不增加任何查找。
const stateQuery = `SELECT s.fingerprint, s.state, s.severity, s.query_index, s.value, s.labels, s.active_since, s.last_seen,
	       s.recover_since, a.id, a.muted, a.mute_id, a.notify_count, a.last_notify_time, a.severity, a.value, a.last_fire_time,
	       a.id IS NOT NULL AND EXISTS (
	         SELECT 1 FROM monitor_notify_outbox o WHERE o.alert_id = a.id AND o.kind IN ($6, $3) AND o.status = $4),
	       CASE WHEN a.notify_count = 0 THEN (
	         SELECT COALESCE(o.done_time, o.create_time) FROM monitor_notify_outbox o
	         WHERE o.alert_id = a.id AND o.kind = $6 AND o.seq = 1 AND o.status = $7) END,
	       a.first_fire_time, a.ack_time, a.inhibited, a.inhibit_kind, a.inhibit_id, a.inhibit_alert_id, a.ci_id,
	       a.escalation_level, a.ticket_id
	FROM monitor_eval_state s
	LEFT JOIN monitor_alert a ON a.id = s.alert_id AND a.status = $5
	WHERE s.rule_id = $1 AND s.datasource_id = $2`

func queryStates(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, task *biz.Task, suffix string,
) ([]biz.SeriesState, error) {
	rows, err := q.Query(ctx, stateQuery+suffix, task.RuleID, task.DatasourceID,
		monitorcontract.NotifyKindRepeat, monitorcontract.OutboxPending, monitorcontract.AlertFiring,
		monitorcontract.NotifyKindFiring, monitorcontract.OutboxDropped)
	if err != nil {
		return nil, fmt.Errorf("read eval states: %w", err)
	}
	defer rows.Close()
	var out []biz.SeriesState
	for rows.Next() {
		var (
			st                                biz.SeriesState
			value, alertValue                 *float64
			recoverSince, lastNotif, lastFire *time.Time
			firstDropped                      *time.Time
			alertID, muteID                   *string
			muted                             *bool
			notifyCount, alertSeverity        *int32
			firstFire, ackTime                *time.Time
			inhibited                         *bool
			inhibitKind, escalationLevel      *int32
			inhibitID, inhibitAlertID         *string
			ciID, ticketID                    *string
		)
		if err := rows.Scan(&st.Fingerprint, &st.State, &st.Severity, &st.QueryIndex, &value, &st.Labels, &st.ActiveSince,
			&st.LastSeen, &recoverSince, &alertID, &muted, &muteID, &notifyCount, &lastNotif, &alertSeverity, &alertValue, &lastFire,
			&st.PendingNotify, &firstDropped, &firstFire, &ackTime, &inhibited, &inhibitKind, &inhibitID, &inhibitAlertID,
			&ciID, &escalationLevel, &ticketID); err != nil {
			return nil, fmt.Errorf("scan eval state: %w", err)
		}
		if value != nil {
			st.Value = *value
		}
		if recoverSince != nil {
			st.RecoverSince = *recoverSince
		}
		if alertID != nil {
			st.AlertID = *alertID
			if muted != nil {
				st.Muted = *muted
			}
			if muteID != nil {
				st.MuteID = *muteID
			}
			if notifyCount != nil {
				st.NotifyCount = *notifyCount
			}
			if lastNotif != nil {
				st.LastNotifyTime = *lastNotif
			}
			if alertSeverity != nil {
				st.AlertSeverity = *alertSeverity
			}
			// 告警行的取值为 NULL 时按 NaN：与任何样本值都不同，下一轮必写。
			st.AlertValue = math.NaN()
			if alertValue != nil {
				st.AlertValue = *alertValue
			}
			if lastFire != nil {
				st.LastFireTime = *lastFire
			}
			if firstDropped != nil {
				st.FirstNotifyDropped = *firstDropped
			}
			if firstFire != nil {
				st.FirstFire = *firstFire
			}
			if ackTime != nil {
				st.AckTime = *ackTime
			}
			if inhibited != nil && *inhibited && inhibitKind != nil {
				st.Inhibited, st.Inhibit.Kind = true, *inhibitKind
				st.Inhibit.InhibitID, st.Inhibit.AlertID = deref(inhibitID), deref(inhibitAlertID)
			}
			st.CIID, st.TicketID = deref(ciID), deref(ticketID)
			if escalationLevel != nil {
				st.EscalationLevel = *escalationLevel
			}
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read eval states: %w", err)
	}
	return out, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// loadEscalation 一条语句读出这些状态行的活跃告警各级升级通知的进度，填进 SeriesState.Escalation：
// core 回写的已通知次数与时间（monitor_alert_escalation），以及发件箱里尚未派发成功的升级通知行
// （待派发 → Pending；已丢弃且正是该级的下一次通知 → DroppedAt）。两部分在同一条语句里读（同一个快照）：
// core 派发一行（行置已派发 + 次数加一）是一个事务，这里要么两者都看到，要么都没看到。
// 只在规则引用了升级策略时调用；语句数固定，不随序列数增长。
func loadEscalation(ctx context.Context, tx pgx.Tx, states []biz.SeriesState) error {
	byAlert := make(map[string]int, len(states))
	ids := make([]string, 0, len(states))
	for i := range states {
		if id := states[i].AlertID; id != "" {
			byAlert[id] = i
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx,
		`SELECT e.alert_id::text, e.level, e.notify_count, e.last_notify_time, 0, 0, NULL::timestamptz
		 FROM monitor_alert_escalation e WHERE e.alert_id = ANY ($1::uuid[])
		 UNION ALL
		 SELECT o.alert_id::text, o.level, 0, NULL, o.seq, o.status, COALESCE(o.done_time, o.create_time)
		 FROM monitor_notify_outbox o WHERE o.alert_id = ANY ($1::uuid[]) AND o.kind = $2 AND o.status <> $3`,
		ids, monitorcontract.NotifyKindEscalation, monitorcontract.OutboxDone)
	if err != nil {
		return fmt.Errorf("read escalation progress: %w", err)
	}
	defer rows.Close()
	type queued struct {
		alert       int
		seq, status int32
		at          time.Time
	}
	var outbox []queued
	level := func(alert int, n int32) *monitorpipeline.EscalationLevelState {
		if n < 1 || n > monitorcontract.MaxEscalationLevels {
			return nil
		}
		st := &states[alert]
		for int32(len(st.Escalation)) < n { // #nosec G115 -- 至多 MaxEscalationLevels
			st.Escalation = append(st.Escalation, monitorpipeline.EscalationLevelState{})
		}
		return &st.Escalation[n-1]
	}
	for rows.Next() {
		var (
			alertID            string
			lvl, count, status int32
			seq                int32
			lastNotify, at     *time.Time
		)
		if err := rows.Scan(&alertID, &lvl, &count, &lastNotify, &seq, &status, &at); err != nil {
			return fmt.Errorf("scan escalation progress: %w", err)
		}
		alert, ok := byAlert[alertID]
		if !ok {
			continue
		}
		if seq != 0 {
			q := queued{alert: alert, seq: seq, status: status}
			if at != nil {
				q.at = *at
			}
			outbox = append(outbox, q)
			continue
		}
		if ls := level(alert, lvl); ls != nil {
			ls.NotifyCount = count
			if lastNotify != nil {
				ls.LastNotifyTime = *lastNotify
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read escalation progress: %w", err)
	}
	// 发件箱行在次数都读完之后再处理：「已丢弃的是不是下一次通知」要对着该级的已通知次数判断。
	for _, q := range outbox {
		lvl, n, ok := monitorcontract.SplitEscalationSeq(q.seq)
		if !ok {
			continue
		}
		ls := level(q.alert, lvl)
		switch {
		case ls == nil:
		case q.status == monitorcontract.OutboxPending:
			ls.Pending = true
		case q.status == monitorcontract.OutboxDropped && n == ls.NotifyCount+1:
			ls.DroppedAt = q.at
		}
	}
	return nil
}

// Snapshot implements biz.RoundRepo.
func (r *roundRepo) Snapshot(ctx context.Context, task *biz.Task) ([]biz.SeriesState, error) {
	return queryStates(ctx, r.data.Pool, task, "")
}

// Commit implements biz.RoundRepo。事务内没有任何网络 I/O，也不执行模板：decide 是纯计算。
// 任务行先加锁并校验属主（围栏）：租约已被他人重领或任务已被规划器删除 → ErrLeaseLost，整轮回滚。
// 锁序固定为「任务行 → 状态行 → 告警行」，与规划器的清理事务一致。
func (r *roundRepo) Commit(ctx context.Context, task *biz.Task, escalation bool, decide func(prev []biz.SeriesState) (*biz.Round, error)) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var locked int
		err := tx.QueryRow(ctx,
			"SELECT 1 FROM monitor_eval_task WHERE rule_id = $1 AND datasource_id = $2 AND lease_owner = $3 FOR UPDATE",
			task.RuleID, task.DatasourceID, task.Token).Scan(&locked)
		if errors.Is(err, pgx.ErrNoRows) {
			return biz.ErrLeaseLost
		}
		if err != nil {
			return fmt.Errorf("lock eval task: %w", err)
		}
		prev, err := queryStates(ctx, tx, task, " FOR UPDATE OF s")
		if err != nil {
			return err
		}
		if escalation {
			if err := loadEscalation(ctx, tx, prev); err != nil {
				return err
			}
		}
		round, err := decide(prev)
		if err != nil {
			return err
		}
		if round.Transition != nil {
			if err := insertAlerts(ctx, tx, task, round); err != nil {
				return err
			}
		}
		return writeRound(ctx, tx, task, round)
	})
}

// Record implements biz.RoundRepo：围栏在任务写回语句上（影响 0 行即回滚）。
func (r *roundRepo) Record(ctx context.Context, task *biz.Task, round *biz.Round) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error { return writeRound(ctx, tx, task, round) })
}

// insertAlerts 新建告警事件。活跃事件的部分唯一索引兜底：同指纹已有活跃事件时改为更新它，
// 并把本轮引用新 ID 的状态行与发件箱行改指向已有事件（其首次通知若已入箱，发件箱唯一键使重复写入成为空操作）。
func insertAlerts(ctx context.Context, tx pgx.Tx, task *biz.Task, round *biz.Round) error {
	tr := round.Transition
	n := len(tr.AlertInserts)
	if n == 0 {
		return nil
	}
	ids, fps, groupKeys := make([]string, n), make([]string, n), make([]string, n)
	sevs, qis := make([]int32, n), make([]int32, n)
	values := make([]float64, n)
	labels, annotations, muteIDs := make([]string, n), make([]string, n), make([]string, n)
	muted := make([]bool, n)
	inh := newInhibitColumns(n)
	buf := make([]byte, 0, 512)
	for i := range tr.AlertInserts {
		a := &tr.AlertInserts[i]
		ids[i], fps[i], sevs[i], qis[i], values[i] = a.ID, a.Fingerprint, a.Severity, a.QueryIndex, a.Value
		groupKeys[i], muted[i], muteIDs[i] = a.GroupKey, a.Muted, a.MuteID
		inh.set(i, a.Inhibited, a.Inhibit, a.CIID)
		buf = appendJSONObject(buf[:0], a.Labels)
		labels[i] = string(buf)
		buf = appendJSONObject(buf[:0], a.Annotations)
		annotations[i] = string(buf)
	}
	rows, err := tx.Query(ctx,
		`INSERT INTO monitor_alert (id, fingerprint, rule_id, rule_name, group_id, datasource_id, datasource_name, severity, status,
		   labels, annotations, expr, value, first_fire_time, last_fire_time, muted, mute_id, group_key,
		   inhibited, inhibit_kind, inhibit_id, inhibit_alert_id, ci_id)
		 SELECT v.id, v.fingerprint, $1, $3, $4, $2, $5, v.severity, $6, v.labels::jsonb, v.annotations::jsonb,
		        COALESCE(($7::text[])[v.query_index + 1], ''), v.value, $8, $8, v.muted, NULLIF(v.mute_id, '')::uuid, v.group_key,
		        v.inhibited, v.inhibit_kind, NULLIF(v.inhibit_id, '')::uuid, NULLIF(v.inhibit_alert_id, '')::uuid, NULLIF(v.ci_id, '')::uuid
		 FROM unnest($9::uuid[], $10::text[], $11::int4[], $12::int4[], $13::float8[], $14::text[], $15::text[], $16::bool[],
		             $17::text[], $18::text[], $19::bool[], $20::int4[], $21::text[], $22::text[], $23::text[])
		      AS v(id, fingerprint, severity, query_index, value, labels, annotations, muted, mute_id, group_key,
		           inhibited, inhibit_kind, inhibit_id, inhibit_alert_id, ci_id)
		 ON CONFLICT (fingerprint) WHERE status = 1 DO UPDATE
		   SET severity = EXCLUDED.severity, value = EXCLUDED.value, expr = EXCLUDED.expr, last_fire_time = EXCLUDED.last_fire_time,
		       muted = EXCLUDED.muted, mute_id = EXCLUDED.mute_id, inhibited = EXCLUDED.inhibited, inhibit_kind = EXCLUDED.inhibit_kind,
		       inhibit_id = EXCLUDED.inhibit_id, inhibit_alert_id = EXCLUDED.inhibit_alert_id, ci_id = EXCLUDED.ci_id
		 RETURNING id, fingerprint`,
		task.RuleID, task.DatasourceID, round.Alert.RuleName, round.Alert.GroupID, round.Alert.DatasourceName,
		monitorcontract.AlertFiring, round.Exprs, round.Now,
		ids, fps, sevs, qis, values, labels, annotations, muted, muteIDs, groupKeys,
		inh.inhibited, inh.kinds, inh.ids, inh.alertIDs, inh.ciIDs)
	if err != nil {
		return fmt.Errorf("insert alerts: %w", err)
	}
	actual := make(map[string]string, n) // 指纹 → 实际的告警 ID
	for rows.Next() {
		var id, fp string
		if err := rows.Scan(&id, &fp); err != nil {
			rows.Close()
			return fmt.Errorf("scan inserted alert: %w", err)
		}
		actual[fp] = id
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("insert alerts: %w", err)
	}
	var remap map[string]string // 本轮生成的 ID → 已有活跃事件的 ID
	for i := range tr.AlertInserts {
		if id, ok := actual[fps[i]]; ok && id != ids[i] {
			if remap == nil {
				remap = make(map[string]string)
			}
			remap[ids[i]] = id
		}
	}
	if remap == nil {
		return nil
	}
	for i := range tr.StateInserts {
		if id, ok := remap[tr.StateInserts[i].AlertID]; ok {
			tr.StateInserts[i].AlertID = id
		}
	}
	for i := range tr.StateUpdates {
		if id, ok := remap[tr.StateUpdates[i].AlertID]; ok {
			tr.StateUpdates[i].AlertID = id
		}
	}
	for i := range tr.Outbox {
		if id, ok := remap[tr.Outbox[i].AlertID]; ok {
			tr.Outbox[i].AlertID = id
		}
	}
	for i := range tr.Events {
		if id, ok := remap[tr.Events[i].AlertID]; ok {
			tr.Events[i].AlertID = id
		}
	}
	for i := range tr.Actions {
		if id, ok := remap[tr.Actions[i].AlertID]; ok {
			tr.Actions[i].AlertID = id
		}
	}
	return nil
}

// inhibitColumns 告警行上抑制与关联资源各列的批量参数。
type inhibitColumns struct {
	inhibited            []bool
	kinds                []int32
	ids, alertIDs, ciIDs []string
}

func newInhibitColumns(n int) *inhibitColumns {
	return &inhibitColumns{
		inhibited: make([]bool, n), kinds: make([]int32, n), ids: make([]string, n), alertIDs: make([]string, n), ciIDs: make([]string, n),
	}
}

// set 写第 i 行。未被抑制时来源各列一律为空（表约束：inhibited = (inhibit_kind <> 0)）。
func (c *inhibitColumns) set(i int, inhibited bool, hit monitorpipeline.InhibitHit, ciID string) {
	c.ciIDs[i] = ciID
	if inhibited {
		c.inhibited[i], c.kinds[i], c.ids[i], c.alertIDs[i] = true, hit.Kind, hit.InhibitID, hit.AlertID
	}
}

// writeRound 把一轮的其余写入并入一个 pgx.Batch：状态删 / 增 / 改、发件箱、告警更新 / 恢复、时间线、
// 事件动作、判定记录、任务写回（围栏）。批内语句按入队顺序执行，任一失败整个事务回滚。
func writeRound(ctx context.Context, tx pgx.Tx, task *biz.Task, round *biz.Round) error {
	batch := &pgx.Batch{}
	if tr := round.Transition; tr != nil {
		// 发件箱先于告警更新：与 core 的派发事务同序加锁（发件箱行 → 告警行），不构成死锁环。
		queueStates(batch, task, round, tr)
		queueOutbox(batch, tr)
		queueAlerts(batch, round, tr)
		if err := queueEvents(batch, round, tr); err != nil {
			return err
		}
		if err := queueActions(batch, tr); err != nil {
			return err
		}
	}
	if ev := round.EngineEvent; ev != nil {
		// 只出现在走过状态机的一轮（Commit）：任务行已在事务开头加锁，与自监控检测写故障事件串行。
		sql, args, err := engineEventInsert(ev)
		if err != nil {
			return err
		}
		batch.Queue(sql, args...)
	}
	recordID, err := idgen.NewID()
	if err != nil {
		return err
	}
	if round.Detail.Queries == nil {
		round.Detail.Queries = []monitorcontract.EvalQueryDetail{}
	}
	if round.Detail.Points == nil {
		round.Detail.Points = []monitorcontract.EvalPoint{}
	}
	detail, err := json.Marshal(&round.Detail)
	if err != nil {
		return fmt.Errorf("encode eval detail: %w", err)
	}
	durationMS := round.Duration.Milliseconds()
	batch.Queue(
		`INSERT INTO monitor_eval_record (id, rule_id, datasource_id, eval_time, duration_ms, status, error, series, anomalies, recovered, detail)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)`,
		recordID, task.RuleID, task.DatasourceID, round.Now, durationMS, round.Status, round.Error,
		round.Series, round.Anomalies, round.Recovered, string(detail))
	var firing *int32
	if round.Transition != nil {
		firing = &round.Transition.FiringCount
	}
	// 写回放在批的最后：它的影响行数就是围栏判定（Record 路径没有前置的加锁检查）。
	// error_since / error_count 记连续查询错误的起点与次数（自监控据此判断「规则持续评估失败」）：查询错误的一轮
	// 累计，不在生效时间的一轮什么都没查、保持原值，其余的一轮清零。
	batch.Queue(
		`UPDATE monitor_eval_task
		 SET next_eval_time = now() + $4::interval, lease_owner = NULL, lease_until = NULL, last_eval_time = $5::timestamptz,
		     last_status = $6::int4, last_error = $7, last_duration_ms = $8, firing_count = COALESCE($9, firing_count),
		     error_since = CASE WHEN $6::int4 = $10::int4 THEN COALESCE(error_since, $5::timestamptz)
		                        WHEN $6::int4 = $11::int4 THEN error_since END,
		     error_count = CASE WHEN $6::int4 = $10::int4 THEN error_count + 1 WHEN $6::int4 = $11::int4 THEN error_count ELSE 0 END
		 WHERE rule_id = $1 AND datasource_id = $2 AND lease_owner = $3`,
		task.RuleID, task.DatasourceID, task.Token, round.Interval, round.Now, round.Status, round.Error, durationMS, firing,
		monitorcontract.EvalError, monitorcontract.EvalInactive)

	res := tx.SendBatch(ctx, batch)
	for i := range batch.Len() - 1 {
		if _, err := res.Exec(); err != nil {
			_ = res.Close()
			return fmt.Errorf("write eval round (statement %d): %w", i, err)
		}
	}
	tag, err := res.Exec()
	if cerr := res.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("write back eval task: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return biz.ErrLeaseLost
	}
	return nil
}

func queueStates(batch *pgx.Batch, task *biz.Task, round *biz.Round, tr *biz.Transition) {
	if len(tr.StateDeletes) > 0 {
		batch.Queue("DELETE FROM monitor_eval_state WHERE rule_id = $1 AND datasource_id = $2 AND fingerprint = ANY ($3::text[])",
			task.RuleID, task.DatasourceID, tr.StateDeletes)
	}
	if n := len(tr.StateInserts); n > 0 {
		fps, alertIDs, labels := make([]string, n), make([]string, n), make([]string, n)
		states, sevs, qis := make([]int32, n), make([]int32, n), make([]int32, n)
		values := make([]float64, n)
		buf := make([]byte, 0, 512)
		for i := range tr.StateInserts {
			s := &tr.StateInserts[i]
			fps[i], states[i], sevs[i], qis[i], values[i], alertIDs[i] = s.Fingerprint, s.State, s.Severity, s.QueryIndex, s.Value, s.AlertID
			buf = appendJSONObject(buf[:0], s.Labels)
			labels[i] = string(buf)
		}
		// 状态行在本事务内已全部加锁，正常不会冲突；冲突时以本轮结果为准。
		batch.Queue(
			`INSERT INTO monitor_eval_state (rule_id, datasource_id, fingerprint, state, severity, query_index, value, labels,
			   active_since, last_seen, alert_id)
			 SELECT $1, $2, v.fingerprint, v.state, v.severity, v.query_index, v.value, v.labels::jsonb, $3, $3, NULLIF(v.alert_id, '')::uuid
			 FROM unnest($4::text[], $5::int4[], $6::int4[], $7::int4[], $8::float8[], $9::text[], $10::text[])
			      AS v(fingerprint, state, severity, query_index, value, labels, alert_id)
			 ON CONFLICT (rule_id, datasource_id, fingerprint) DO UPDATE
			   SET state = EXCLUDED.state, severity = EXCLUDED.severity, query_index = EXCLUDED.query_index, value = EXCLUDED.value,
			       last_seen = EXCLUDED.last_seen, recover_since = NULL, alert_id = EXCLUDED.alert_id`,
			task.RuleID, task.DatasourceID, round.Now, fps, states, sevs, qis, values, labels, alertIDs)
	}
	if n := len(tr.StateUpdates); n > 0 {
		fps, alertIDs := make([]string, n), make([]string, n)
		states, sevs, qis := make([]int32, n), make([]int32, n), make([]int32, n)
		values := make([]float64, n)
		restarts := make([]bool, n)
		for i := range tr.StateUpdates {
			s := &tr.StateUpdates[i]
			fps[i], states[i], sevs[i], qis[i], values[i], alertIDs[i] = s.Fingerprint, s.State, s.Severity, s.QueryIndex, s.Value, s.AlertID
			restarts[i] = s.Restart
		}
		batch.Queue(
			`UPDATE monitor_eval_state s
			 SET state = v.state, severity = v.severity, query_index = v.query_index, value = v.value,
			     alert_id = NULLIF(v.alert_id, '')::uuid,
			     active_since = CASE WHEN v.restart THEN $3 ELSE s.active_since END,
			     last_seen = CASE WHEN v.state = $4 THEN s.last_seen ELSE $3 END,
			     recover_since = CASE WHEN v.state = $4 THEN $3::timestamptz END
			 FROM unnest($5::text[], $6::int4[], $7::int4[], $8::int4[], $9::float8[], $10::text[], $11::bool[])
			      AS v(fingerprint, state, severity, query_index, value, alert_id, restart)
			 WHERE s.rule_id = $1 AND s.datasource_id = $2 AND s.fingerprint = v.fingerprint`,
			task.RuleID, task.DatasourceID, round.Now, monitorcontract.StateRecovering, fps, states, sevs, qis, values, alertIDs, restarts)
	}
}

func queueAlerts(batch *pgx.Batch, round *biz.Round, tr *biz.Transition) {
	if n := len(tr.AlertUpdates); n > 0 {
		ids, muteIDs := make([]string, n), make([]string, n)
		sevs, qis, levels := make([]int32, n), make([]int32, n), make([]int32, n)
		values := make([]float64, n)
		muted := make([]bool, n)
		inh := newInhibitColumns(n)
		for i := range tr.AlertUpdates {
			a := &tr.AlertUpdates[i]
			ids[i], sevs[i], qis[i], values[i], muted[i], muteIDs[i] = a.ID, a.Severity, a.QueryIndex, a.Value, a.Muted, a.MuteID
			levels[i] = a.EscalationLevel
			inh.set(i, a.Inhibited, a.Inhibit, a.CIID)
		}
		batch.Queue(
			`UPDATE monitor_alert a
			 SET severity = v.severity, value = v.value, expr = COALESCE(($1::text[])[v.query_index + 1], a.expr), last_fire_time = $2,
			     muted = v.muted, mute_id = NULLIF(v.mute_id, '')::uuid, inhibited = v.inhibited, inhibit_kind = v.inhibit_kind,
			     inhibit_id = NULLIF(v.inhibit_id, '')::uuid, inhibit_alert_id = NULLIF(v.inhibit_alert_id, '')::uuid,
			     ci_id = NULLIF(v.ci_id, '')::uuid, escalation_level = v.escalation_level
			 FROM unnest($4::uuid[], $5::int4[], $6::int4[], $7::float8[], $8::bool[], $9::text[], $10::bool[], $11::int4[],
			             $12::text[], $13::text[], $14::text[], $15::int4[])
			      AS v(id, severity, query_index, value, muted, mute_id, inhibited, inhibit_kind, inhibit_id, inhibit_alert_id,
			           ci_id, escalation_level)
			 WHERE a.id = v.id AND a.status = $3`,
			round.Exprs, round.Now, monitorcontract.AlertFiring, ids, sevs, qis, values, muted, muteIDs,
			inh.inhibited, inh.kinds, inh.ids, inh.alertIDs, inh.ciIDs, levels)
	}
	if n := len(tr.AlertResolves); n > 0 {
		ids, muteIDs := make([]string, n), make([]string, n)
		has, muted := make([]bool, n), make([]bool, n)
		values := make([]float64, n)
		for i := range tr.AlertResolves {
			a := &tr.AlertResolves[i]
			ids[i], has[i], values[i], muted[i], muteIDs[i] = a.ID, a.HasRecovery, a.RecoveryValue, a.Muted, a.MuteID
		}
		batch.Queue(
			`UPDATE monitor_alert a
			 SET status = $1, resolve_time = $2, recovery_value = CASE WHEN v.has THEN v.value END,
			     muted = v.muted, mute_id = NULLIF(v.mute_id, '')::uuid
			 FROM unnest($4::uuid[], $5::bool[], $6::float8[], $7::bool[], $8::text[]) AS v(id, has, value, muted, mute_id)
			 WHERE a.id = v.id AND a.status = $3`,
			monitorcontract.AlertResolved, round.Now, monitorcontract.AlertFiring, ids, has, values, muted, muteIDs)
	}
}

// queueOutbox 发件箱写入以 (alert_id, kind, seq) 幂等：重复评估（租约过期重领、core 尚未回写通知计数）
// 写出同一键时是空操作。Rearm 的行（屏蔽解除后的新通知、重复通知）额外把同键的已丢弃行重新置为待派发：
// ON CONFLICT DO UPDATE 会锁住冲突行（WHERE 不成立时也锁），与 core 的派发事务在该行上串行——core 正在
// 丢弃它时这里等其提交后再重新激活；这里先到时 core 的 SKIP LOCKED 跳过它，下一轮读到本事务提交后的事件。
func queueOutbox(batch *pgx.Batch, tr *biz.Transition) {
	var plain, rearm outboxRows
	for i := range tr.Outbox {
		if o := &tr.Outbox[i]; o.Rearm {
			rearm.add(o)
		} else {
			plain.add(o)
		}
	}
	const insert = `INSERT INTO monitor_notify_outbox (id, alert_id, kind, seq, level, status)
		 SELECT v.id, v.alert_id, v.kind, v.seq, v.level, $1
		 FROM unnest($2::uuid[], $3::uuid[], $4::int4[], $5::int4[], $6::int4[]) AS v(id, alert_id, kind, seq, level)
		 ON CONFLICT (alert_id, kind, seq) `
	if len(plain.ids) > 0 {
		batch.Queue(insert+"DO NOTHING", monitorcontract.OutboxPending, plain.ids, plain.alertIDs, plain.kinds, plain.seqs, plain.levels)
	}
	if len(rearm.ids) > 0 {
		batch.Queue(insert+`DO UPDATE SET status = $1, attempts = 0, last_error = '', create_time = now(), done_time = NULL
			 WHERE monitor_notify_outbox.status = $7`,
			monitorcontract.OutboxPending, rearm.ids, rearm.alertIDs, rearm.kinds, rearm.seqs, rearm.levels, monitorcontract.OutboxDropped)
	}
}

type outboxRows struct {
	ids, alertIDs       []string
	kinds, seqs, levels []int32
}

func (r *outboxRows) add(o *biz.OutboxInsert) {
	r.ids, r.alertIDs = append(r.ids, o.ID), append(r.alertIDs, o.AlertID)
	r.kinds, r.seqs, r.levels = append(r.kinds, o.Kind), append(r.seqs, o.Seq), append(r.levels, o.Level)
}

// maxFlapEvents 一个事件的时间线里，会随抖动反复产生的几类行（屏蔽 / 解除屏蔽、抑制 / 解除抑制、升级）
// 合计至多保留多少条：达到后这几类不再写入（告警行上的屏蔽 / 抑制 / 升级状态照常更新）。
// 触发、恢复、关闭每个事件各至多一条，不受此限。
const maxFlapEvents = 200

// flapEventTypes 受 maxFlapEvents 约束的时间线类型。
var flapEventTypes = []string{
	monitorcontract.AlertEventMute, monitorcontract.AlertEventUnmute, monitorcontract.AlertEventInhibit,
	monitorcontract.AlertEventUninhibit, monitorcontract.AlertEventEscalate,
}

// queueEvents 写时间线行（单语句）。计数子查询只对「本轮确实要写的那几类行」求值：没有状态变化的轮次
// 根本不走到这里，所以它不是逐序列的开销。
func queueEvents(batch *pgx.Batch, round *biz.Round, tr *biz.Transition) error {
	n := len(tr.Events)
	if n == 0 {
		return nil
	}
	ids, alertIDs, types, details := make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	for i := range tr.Events {
		e := &tr.Events[i]
		detail, err := json.Marshal(e.Detail())
		if err != nil {
			return fmt.Errorf("encode alert event detail: %w", err)
		}
		ids[i], alertIDs[i], types[i], details[i] = e.ID, e.AlertID, e.Type, string(detail)
	}
	batch.Queue(
		`INSERT INTO monitor_alert_event (id, alert_id, type, detail, create_time)
		 SELECT v.id, v.alert_id, v.type, v.detail::jsonb, $1
		 FROM unnest($2::uuid[], $3::uuid[], $4::text[], $5::text[]) AS v(id, alert_id, type, detail)
		 WHERE v.type <> ALL ($6::text[])
		    OR (SELECT count(*) FROM monitor_alert_event e WHERE e.alert_id = v.alert_id AND e.type = ANY ($6::text[])) < $7`,
		round.Now, ids, alertIDs, types, details, flapEventTypes, maxFlapEvents)
	return nil
}

// queueActions 写事件动作（单语句）。(alert_id, kind) 唯一：同一事件的同类动作已入队过即是空操作。
func queueActions(batch *pgx.Batch, tr *biz.Transition) error {
	n := len(tr.Actions)
	if n == 0 {
		return nil
	}
	ids, alertIDs, payloads := make([]string, n), make([]string, n), make([]string, n)
	kinds := make([]int32, n)
	for i := range tr.Actions {
		a := &tr.Actions[i]
		payload, err := json.Marshal(&a.Payload)
		if err != nil {
			return fmt.Errorf("encode alert action payload: %w", err)
		}
		ids[i], alertIDs[i], kinds[i], payloads[i] = a.ID, a.AlertID, a.Kind, string(payload)
	}
	batch.Queue(
		`INSERT INTO monitor_alert_action (id, alert_id, kind, payload)
		 SELECT v.id, v.alert_id, v.kind, v.payload::jsonb
		 FROM unnest($1::uuid[], $2::uuid[], $3::int4[], $4::text[]) AS v(id, alert_id, kind, payload)
		 ON CONFLICT (alert_id, kind) DO NOTHING`, ids, alertIDs, kinds, payloads)
	return nil
}

// engineEventInsert 一条自监控事件的写入语句与参数。dedup_key 唯一：重复写入是空操作；Requires 非空时
// 只在该键的事件存在时才写（恢复事件以故障事件发过为前提）。
func engineEventInsert(ev *biz.EngineEventInsert) (string, []any, error) {
	detail, err := json.Marshal(&ev.Detail)
	if err != nil {
		return "", nil, fmt.Errorf("encode engine event detail: %w", err)
	}
	return `INSERT INTO monitor_engine_event (id, type, object_id, dedup_key, detail)
		SELECT $1::uuid, $2, $3::uuid, $4, $5::jsonb
		WHERE $6 = '' OR EXISTS (SELECT 1 FROM monitor_engine_event WHERE dedup_key = $6)
		ON CONFLICT (dedup_key) DO NOTHING`,
		[]any{ev.ID, ev.Type, ev.ObjectID, ev.DedupKey, string(detail), ev.Requires}, nil
}

// appendJSONObject 把 map[string]string 编码为 JSON 对象（不经反射：大规则每轮要编码上千个标签集）。
// nil 编码为 {}。输出保证能被 jsonb 接受：非法 UTF-8 替换为 U+FFFD，U+0000 丢弃。
func appendJSONObject(buf []byte, m map[string]string) []byte {
	buf = append(buf, '{')
	first := true
	for k, v := range m {
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = appendJSONString(buf, k)
		buf = append(buf, ':')
		buf = appendJSONString(buf, v)
	}
	return append(buf, '}')
}

const hexDigits = "0123456789abcdef"

func appendJSONString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			buf = append(buf, s[start:i]...)
			switch c {
			case '"', '\\':
				buf = append(buf, '\\', c)
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\r':
				buf = append(buf, '\\', 'r')
			case '\t':
				buf = append(buf, '\\', 't')
			case 0:
				// jsonb 不接受 \u0000：丢弃。
			default:
				buf = append(buf, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			buf = append(buf, s[start:i]...)
			buf = append(buf, "�"...)
			i++
			start = i
			continue
		}
		i += size
	}
	buf = append(buf, s[start:]...)
	return append(buf, '"')
}
