package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.ConfigRepo  = (*configRepo)(nil)
	_ biz.RuleRepo    = (*configRepo)(nil)
	_ biz.InhibitRepo = (*configRepo)(nil)
)

// configRepo 配置表只读访问：数据源、屏蔽规则、抑制规则、升级策略、资源索引、工作日历、规则、配置版本，
// 以及抑制快照要读的活跃告警。引擎对这些配置表只有 SELECT。
type configRepo struct {
	data *Data
}

// NewConfigRepo new a configRepo.
func NewConfigRepo(data *Data) *configRepo { return &configRepo{data: data} }

// Versions implements biz.ConfigRepo.
func (r *configRepo) Versions(ctx context.Context) (biz.CfgVersions, error) {
	var v biz.CfgVersions
	fields := map[string]*int64{
		monitorcontract.CfgDatasource: &v.Datasource, monitorcontract.CfgMute: &v.Mute, monitorcontract.CfgRuleGroup: &v.RuleGroup,
		monitorcontract.CfgEscalation: &v.Escalation, monitorcontract.CfgInhibit: &v.Inhibit,
		monitorcontract.CfgCIIndex: &v.CIIndex, monitorcontract.CfgCalendar: &v.Calendar,
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT name, version FROM monitor_cfg_version")
	if err != nil {
		return v, fmt.Errorf("read cfg versions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			name    string
			version int64
		)
		if err := rows.Scan(&name, &version); err != nil {
			return v, fmt.Errorf("scan cfg version: %w", err)
		}
		if dst, ok := fields[name]; ok {
			*dst = version
		}
	}
	if err := rows.Err(); err != nil {
		return v, fmt.Errorf("read cfg versions: %w", err)
	}
	return v, nil
}

// ListDatasources implements biz.ConfigRepo. secret 为密文原样返回，由缓存在构造查询客户端时解密。
func (r *configRepo) ListDatasources(ctx context.Context) ([]*biz.Datasource, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, name, url, auth_type, username, secret, headers, timeout_ms, tls_skip_verify, enabled, version, update_time,
		        remote_write_url
		 FROM monitor_datasource WHERE delete_time IS NULL`)
	if err != nil {
		return nil, fmt.Errorf("list datasources: %w", err)
	}
	defer rows.Close()
	var out []*biz.Datasource
	for rows.Next() {
		var (
			ds        biz.Datasource
			headers   []byte
			timeoutMS int32
		)
		if err := rows.Scan(&ds.ID, &ds.Name, &ds.URL, &ds.AuthType, &ds.Username, &ds.Secret, &headers, &timeoutMS,
			&ds.TLSSkipVerify, &ds.Enabled, &ds.Version, &ds.UpdateTime, &ds.RemoteWriteURL); err != nil {
			return nil, fmt.Errorf("scan datasource: %w", err)
		}
		// 请求头损坏时按无自定义请求头处理（core 写入时已校验），不让单个数据源拖垮整个缓存。
		_ = json.Unmarshal(headers, &ds.Headers)
		ds.Timeout = time.Duration(timeoutMS) * time.Millisecond
		out = append(out, &ds)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list datasources: %w", err)
	}
	return out, nil
}

// ListMutes implements biz.ConfigRepo：启用、未删除、未过期（一次性规则已过 end_time 的不加载）。
// 作用分组已删除时 path 为空串，biz 据此判为不命中。按 ID 升序至多 limit 条（超限由调用方发现并告警）。
func (r *configRepo) ListMutes(ctx context.Context, limit int) ([]*biz.Mute, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT m.id, COALESCE(m.group_id::text, ''), COALESCE(g.path, ''), m.rule_ids::text[], m.datasource_ids::text[],
		        m.severities::int4[], m.matchers, m.mode, m.time_type, m.start_time, m.end_time, m.periodic
		 FROM monitor_mute m
		 LEFT JOIN monitor_rule_group g ON g.id = m.group_id AND g.delete_time IS NULL
		 WHERE m.enabled AND m.delete_time IS NULL AND (m.end_time IS NULL OR m.end_time > now())
		 ORDER BY m.id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list mutes: %w", err)
	}
	defer rows.Close()
	var out []*biz.Mute
	for rows.Next() {
		var (
			m                  biz.Mute
			matchers, periodic []byte
			start, end         *time.Time
		)
		if err := rows.Scan(&m.ID, &m.GroupID, &m.GroupPath, &m.RuleIDs, &m.DatasourceIDs, &m.Severities,
			&matchers, &m.Mode, &m.TimeType, &start, &end, &periodic); err != nil {
			return nil, fmt.Errorf("scan mute: %w", err)
		}
		if err := json.Unmarshal(matchers, &m.Matchers); err != nil {
			// 无法解析的匹配条件：放一条必然非法的条件，使该规则恒不命中（宁可不屏蔽，不误屏蔽）。
			m.Matchers = []monitorcontract.Matcher{{}}
		}
		if len(periodic) > 0 {
			if err := json.Unmarshal(periodic, &m.Periodic); err != nil {
				m.Periodic = monitorcontract.Effective{}
			}
		}
		if start != nil {
			m.Start = *start
		}
		if end != nil {
			m.End = *end
		}
		out = append(out, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list mutes: %w", err)
	}
	return out, nil
}

// LoadRule implements biz.RuleRepo：规则行 + 所属分组路径 + 数据库当前时刻，一次往返。
func (r *configRepo) LoadRule(ctx context.Context, ruleID string) (*biz.Rule, time.Time, error) {
	var (
		rule                                    biz.Rule
		now                                     time.Time
		labels, queries, effective, annotations []byte
		ciMatch, scope, relabel, actions        []byte
		intervalS, forS, recoverS, keepS        int32
		repeatM                                 int32
	)
	err := r.data.Pool.QueryRow(ctx,
		`SELECT r.name, r.group_id, COALESCE(g.path, ''), r.enabled AND r.delete_time IS NULL, r.labels, r.queries, r.level_inhibit,
		        r.eval_interval_s, r.for_s, r.recover_s, r.nodata_keep_s, r.effective, r.annotations, r.recovery_expr,
		        r.notify_recovery, r.repeat_interval_m, r.max_notify, r.group_key_labels, now(),
		        r.ci_match, r.resource_scope, r.relabel, r.drop_expr, r.topology_inhibit, COALESCE(r.escalation_id::text, ''), r.actions
		 FROM monitor_rule r LEFT JOIN monitor_rule_group g ON g.id = r.group_id
		 WHERE r.id = $1`, ruleID).
		Scan(&rule.Name, &rule.GroupID, &rule.GroupPath, &rule.Active, &labels, &queries, &rule.LevelInhibit,
			&intervalS, &forS, &recoverS, &keepS, &effective, &annotations, &rule.RecoveryExpr,
			&rule.NotifyRecovery, &repeatM, &rule.MaxNotify, &rule.GroupKeyLabels, &now,
			&ciMatch, &scope, &relabel, &rule.Pipeline.DropExpr, &rule.TopologyInhibit, &rule.EscalationID, &actions)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, time.Time{}, biz.ErrRuleNotFound
	}
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("load rule: %w", err)
	}
	rule.ID = ruleID
	for _, f := range []struct {
		name string
		raw  []byte
		dst  any
	}{
		{"labels", labels, &rule.Labels},
		{"queries", queries, &rule.Queries},
		{"effective", effective, &rule.Effective},
		{"annotations", annotations, &rule.Annotations},
		{"ci_match", ciMatch, &rule.Pipeline.CIMatch},
		{"resource_scope", scope, &rule.Pipeline.Scope},
		{"relabel", relabel, &rule.Pipeline.Relabel},
		{"actions", actions, &rule.Actions},
	} {
		if err := json.Unmarshal(f.raw, f.dst); err != nil {
			return nil, time.Time{}, fmt.Errorf("decode rule %s: %w", f.name, err)
		}
	}
	rule.Interval = time.Duration(intervalS) * time.Second
	rule.For = time.Duration(forS) * time.Second
	rule.Recover = time.Duration(recoverS) * time.Second
	rule.NodataKeep = time.Duration(keepS) * time.Second
	rule.RepeatInterval = time.Duration(repeatM) * time.Minute
	return &rule, now, nil
}

// ListInhibits implements biz.ConfigRepo：启用、未删除，且作用分组（若有）仍然存在。作用于全部分组的规则
// GroupPath 为空串。无法解析的条件按一侧为空处理——monitorpipeline.CompileInhibits 会跳过这样的规则
// （宁可不抑制，不误抑制）。
func (r *configRepo) ListInhibits(ctx context.Context, limit int) ([]monitorpipeline.InhibitRule, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT i.id::text, COALESCE(g.path, ''), i.source, i.target, i.equal_labels
		 FROM monitor_inhibit i
		 LEFT JOIN monitor_rule_group g ON g.id = i.group_id AND g.delete_time IS NULL
		 WHERE i.enabled AND i.delete_time IS NULL AND (i.group_id IS NULL OR g.id IS NOT NULL)
		 ORDER BY i.id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list inhibits: %w", err)
	}
	defer rows.Close()
	var out []monitorpipeline.InhibitRule
	for rows.Next() {
		var (
			rule           monitorpipeline.InhibitRule
			source, target []byte
		)
		if err := rows.Scan(&rule.ID, &rule.GroupPath, &source, &target, &rule.Equal); err != nil {
			return nil, fmt.Errorf("scan inhibit: %w", err)
		}
		if json.Unmarshal(source, &rule.Source) != nil || json.Unmarshal(target, &rule.Target) != nil {
			// 源条件为空的规则过不了校验：恒不生效。
			rule.Source = monitorcontract.InhibitSelector{}
		}
		out = append(out, rule)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list inhibits: %w", err)
	}
	return out, nil
}

// ListEscalations implements biz.ConfigRepo：启用且未删除的升级策略。各级无法解析的按没有任何一级返回
// （缓存据此判为不合法、按没有策略处理）。
func (r *configRepo) ListEscalations(ctx context.Context, limit int) ([]*biz.Escalation, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, mode, min_severity, levels FROM monitor_escalation
		 WHERE enabled AND delete_time IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list escalations: %w", err)
	}
	defer rows.Close()
	var out []*biz.Escalation
	for rows.Next() {
		var (
			e      biz.Escalation
			levels []byte
		)
		if err := rows.Scan(&e.ID, &e.Policy.Mode, &e.Policy.MinSeverity, &levels); err != nil {
			return nil, fmt.Errorf("scan escalation: %w", err)
		}
		if json.Unmarshal(levels, &e.Policy.Levels) != nil {
			e.Policy.Levels = nil
		}
		out = append(out, &e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list escalations: %w", err)
	}
	return out, nil
}

// ciColumns monitor_ci_index 的读取列。
const ciColumns = `ci_id::text, model_code, name, env, status, owner, tags, attrs, upstream_ids::text[], valid, sync_time`

// ListCIs implements biz.ConfigRepo。
func (r *configRepo) ListCIs(ctx context.Context, since time.Time, limit int) ([]biz.CIRow, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if since.IsZero() {
		rows, err = r.data.Pool.Query(ctx, "SELECT "+ciColumns+" FROM monitor_ci_index WHERE valid ORDER BY ci_id LIMIT $1", limit)
	} else {
		rows, err = r.data.Pool.Query(ctx,
			"SELECT "+ciColumns+" FROM monitor_ci_index WHERE sync_time > $1 ORDER BY sync_time LIMIT $2", since, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("list ci index: %w", err)
	}
	defer rows.Close()
	var out []biz.CIRow
	for rows.Next() {
		var (
			row   biz.CIRow
			attrs []byte
		)
		ci := &row.CI
		if err := rows.Scan(&ci.ID, &ci.ModelCode, &ci.Name, &ci.Env, &ci.Status, &ci.Owner, &ci.Tags, &attrs, &ci.UpstreamIDs,
			&row.Valid, &row.SyncTime); err != nil {
			return nil, fmt.Errorf("scan ci index: %w", err)
		}
		// 属性损坏时按没有可匹配属性处理（仍可按名称关联），不让单个资源拖垮整个索引。
		if json.Unmarshal(attrs, &ci.Attrs) != nil {
			ci.Attrs = nil
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list ci index: %w", err)
	}
	return out, nil
}

// LoadCalendar implements biz.ConfigRepo：尚未物化（空对象）或内容无法解析时返回零值（没有日历）。
func (r *configRepo) LoadCalendar(ctx context.Context) (monitorcontract.CalendarSnapshot, error) {
	var (
		snap monitorcontract.CalendarSnapshot
		raw  []byte
	)
	err := r.data.Pool.QueryRow(ctx, "SELECT config FROM monitor_calendar WHERE id = 1").Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return snap, nil
	}
	if err != nil {
		return snap, fmt.Errorf("load calendar: %w", err)
	}
	if json.Unmarshal(raw, &snap) != nil {
		return monitorcontract.CalendarSnapshot{}, nil
	}
	return snap, nil
}

// ListActiveAlerts implements biz.InhibitRepo：经活跃告警的部分唯一索引（fingerprint WHERE status = 1）顺序
// 读取，代价只与活跃告警数有关，与告警表的历史数据量无关——所以超限时保留的是指纹排序最靠前的那些
// （一个稳定的子集），调用方再按事件 ID 排序。分组路径取规则当前所在的分组（规则已不存在时取事件上记的分组）。
func (r *configRepo) ListActiveAlerts(ctx context.Context, withLabels bool, limit int) ([]biz.ActiveAlert, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT a.id::text, a.rule_id::text, COALESCE(g.path, ''), a.severity, CASE WHEN $1 THEN a.labels END,
		        COALESCE(a.ci_id::text, ''), COALESCE(r.topology_inhibit, false)
		 FROM monitor_alert a
		 LEFT JOIN monitor_rule r ON r.id = a.rule_id
		 LEFT JOIN monitor_rule_group g ON g.id = COALESCE(r.group_id, a.group_id)
		 WHERE a.status = $2 AND ($1 OR a.ci_id IS NOT NULL)
		 ORDER BY a.fingerprint LIMIT $3`, withLabels, monitorcontract.AlertFiring, limit)
	if err != nil {
		return nil, fmt.Errorf("list active alerts: %w", err)
	}
	defer rows.Close()
	var out []biz.ActiveAlert
	for rows.Next() {
		var (
			a      biz.ActiveAlert
			labels []byte
		)
		if err := rows.Scan(&a.Ref.ID, &a.Ref.RuleID, &a.Ref.GroupPath, &a.Ref.Severity, &labels, &a.Ref.CIID, &a.Topology); err != nil {
			return nil, fmt.Errorf("scan active alert: %w", err)
		}
		if len(labels) > 0 && json.Unmarshal(labels, &a.Ref.Labels) != nil {
			a.Ref.Labels = nil
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list active alerts: %w", err)
	}
	return out, nil
}
