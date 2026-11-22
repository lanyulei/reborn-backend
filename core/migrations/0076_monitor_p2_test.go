package migrations

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

const monitorP2Migration = "0076_monitor_p2.up.sql"

// monitorP2TemplateRow 匹配 0076 内置的通知模板行：事件类型（可含下划线）、标题模板、正文模板。
var monitorP2TemplateRow = regexp.MustCompile(
	`\('018f3e1a-0cc2-7000-8000-00000000000\d', 'default', '([a-z._]+)', '\*', 'text',\s*'([^']*)',\s*'([^']*)'\)`)

func readMonitorP2(t *testing.T) string {
	t.Helper()
	raw, err := fs.ReadFile(FS, monitorP2Migration)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	return string(raw)
}

// TestMonitorP2SeedTemplatesRender 锁定 0076 内置的五个通知模板（升级通知 + 四个平台自监控通知），口径同
// TestMonitorSeedTemplatesRender：能通过通知模板的写入校验（白名单函数、missingkey=zero、样例载荷试渲染），
// 且用契约载荷（NotifyPayload / EngineEventPayload）渲染时每个取值都有内容。
func TestMonitorP2SeedTemplatesRender(t *testing.T) {
	t.Parallel()
	rows := monitorP2TemplateRow.FindAllStringSubmatch(readMonitorP2(t), -1)
	alert := monitorcontract.NotifyPayload{
		AlertID: "018f3e1a-0000-7000-8000-00000000000a", AlertURL: "https://reborn.example.com/monitor/alert/x",
		RuleID: "018f3e1a-0000-7000-8000-00000000000b", RuleName: "主机 CPU 使用率过高", GroupName: "基础设施",
		DatasourceName: "prod-vm", Severity: monitorcontract.SeverityCritical, Status: monitorcontract.PayloadStatusFiring,
		Labels: map[string]string{"instance": "10.0.0.1:9100"}, Annotations: map[string]string{"summary": "CPU 95%"},
		Value: "95.2", Expr: "cpu_usage > 90", FirstFireTime: "2026-11-17 10:00:00", Duration: "35m", NotifySeq: 3,
		EscalationLevel: 2, EscalationSeq: 3, EscalationName: "运维升级策略", CIID: "018f3e1a-0000-7000-8000-00000000000c", CIName: "web-01",
	}
	engine := monitorcontract.EngineEventPayload{
		ObjectType: monitorcontract.EngineObjectRule, ObjectID: "018f3e1a-0000-7000-8000-00000000000b", ObjectName: "被监控对象甲",
		DatasourceName: "prod-vm", Error: "connection refused", Since: "2026-11-17 10:00:00", Duration: "12m", Failures: 7,
		URL: "https://reborn.example.com/monitor/engine",
	}
	// want 事件类型 → 正文里必须出现的内容。
	want := map[string][]string{
		monitorcontract.EventAlertEscalated: {
			"第 2 级", "第 3 次通知", "运维升级策略", "基础设施", "严重", "prod-vm", "95.2", "35m", "web-01",
			"instance = 10.0.0.1:9100", "summary：CPU 95%", "cpu_usage > 90", alert.AlertURL,
		},
		monitorcontract.EventEngineDatasourceDown: {"被监控对象甲", "2026-11-17 10:00:00", "12m", "connection refused", engine.URL},
		monitorcontract.EventEngineDatasourceUp:   {"被监控对象甲", "2026-11-17 10:00:00", "12m", engine.URL},
		monitorcontract.EventEngineRuleFailing:    {"被监控对象甲", "prod-vm", "2026-11-17 10:00:00", "12m", "7 次", "connection refused", engine.URL},
		monitorcontract.EventEngineRuleRecovered:  {"被监控对象甲", "prod-vm", "2026-11-17 10:00:00", "12m", engine.URL},
	}
	if len(rows) != len(want) {
		t.Fatalf("expected %d seeded templates, found %d", len(want), len(rows))
	}
	for _, e := range monitorcontract.EngineEventTypes() {
		if _, ok := want[e]; !ok {
			t.Errorf("engine event type %q has no expectation here", e)
		}
	}
	decode := func(v any) map[string]any {
		encoded, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any // 管道渲染时载荷即 JSON 解码后的 map
		if err := json.Unmarshal(encoded, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	trial := monitorRenderContext{
		Title: "sample", Payload: map[string]any{"host": "sample-host"}, Severity: 2, Count: 2, BizID: notifycontract.DefaultBizID,
		EventTime: time.Unix(0, 0).UTC(),
	}
	for _, row := range rows {
		eventType := row[1]
		must, ok := want[eventType]
		if !ok {
			t.Errorf("unexpected seeded template for %q", eventType)
			continue
		}
		delete(want, eventType)
		if !notifycontract.TemplateEventTypePattern.MatchString(eventType) {
			t.Errorf("%s: event type rejected by notify contract", eventType)
		}
		full := monitorRenderContext{Severity: alert.Severity, Count: 1, BizID: monitorcontract.NotifyBizID, EventTime: time.Unix(0, 0).UTC()}
		name := engine.ObjectName
		if eventType == monitorcontract.EventAlertEscalated {
			full.Payload, name = decode(alert), alert.RuleName
		} else {
			full.Payload = decode(engine)
		}
		for part, text := range map[string]string{"subject": row[2], "body": row[3]} {
			tpl, err := template.New(part).Funcs(notifycontract.TemplateFuncs()).Option("missingkey=zero").Parse(text)
			if err != nil {
				t.Errorf("%s %s: parse: %v", eventType, part, err)
				continue
			}
			var buf bytes.Buffer
			if err := tpl.Execute(&buf, trial); err != nil {
				t.Errorf("%s %s: trial render (write-time validation) fails: %v", eventType, part, err)
			}
			buf.Reset()
			if err := tpl.Execute(&buf, full); err != nil {
				t.Errorf("%s %s: render: %v", eventType, part, err)
				continue
			}
			out := buf.String()
			if strings.Contains(out, "<no value>") || strings.Contains(out, "map[") {
				t.Errorf("%s %s: template reads a key the payload contract does not provide:\n%s", eventType, part, out)
			}
			if !strings.Contains(out, name) {
				t.Errorf("%s %s: object name missing:\n%s", eventType, part, out)
			}
			if part != "body" {
				// 标题按字节截断到 255 以内（派发侧）：内置标题本身留足余量。
				if len(out) > 200 {
					t.Errorf("%s subject is %d bytes", eventType, len(out))
				}
				continue
			}
			for _, m := range must {
				if !strings.Contains(out, m) {
					t.Errorf("%s body: %q missing:\n%s", eventType, m, out)
				}
			}
		}
		// 可省略的键缺失时不留下空行标题（如「最近一次失败原因：」后面没有内容）。
		if eventType != monitorcontract.EventAlertEscalated {
			full.Payload = decode(monitorcontract.EngineEventPayload{ObjectType: "datasource", ObjectID: "x", ObjectName: name, Since: "s", Duration: "d"})
			tpl := template.Must(template.New("b").Funcs(notifycontract.TemplateFuncs()).Option("missingkey=zero").Parse(row[3]))
			var buf bytes.Buffer
			if err := tpl.Execute(&buf, full); err != nil {
				t.Errorf("%s body with the optional keys absent: %v", eventType, err)
			}
			for _, line := range strings.Split(buf.String(), "\n") {
				if strings.HasSuffix(strings.TrimSpace(line), "：") {
					t.Errorf("%s body leaves a dangling line %q when optional keys are absent", eventType, line)
				}
			}
		}
	}
	for missing := range want {
		t.Errorf("seeded template for %q not found", missing)
	}
}

// quotedList 取出 SQL 片段里全部单引号字面量。
func quotedList(s string) []string {
	var out []string
	for _, m := range regexp.MustCompile(`'([^']*)'`).FindAllStringSubmatch(s, -1) {
		out = append(out, m[1])
	}
	return out
}

// TestMonitorP2SchemaMatchesContract 迁移里的字面量（CHECK 的取值集合、上限、发件箱序号的编码基数、内置的
// 配置版本行与路由）与 kit/monitorcontract 一致：两处任一方改动都在这里失败。
func TestMonitorP2SchemaMatchesContract(t *testing.T) {
	t.Parallel()
	sql := readMonitorP2(t)
	section := func(start, end string) string {
		i := strings.Index(sql, start)
		if i < 0 {
			t.Fatalf("migration lacks %q", start)
		}
		j := strings.Index(sql[i:], end)
		if j < 0 {
			t.Fatalf("migration lacks %q after %q", end, start)
		}
		return sql[i : i+j]
	}
	// 告警时间线的类型集合可被后续迁移重定义（0080 故障域追加 incident），以最后一次定义为准。
	if got := quotedList(latestCheck(t, "ck_monitor_alert_event_type")); !slices.Equal(got, monitorcontract.AlertEventTypes()) {
		t.Errorf("monitor_alert_event.type CHECK = %v\ncontract = %v", got, monitorcontract.AlertEventTypes())
	}
	if got := quotedList(section("CONSTRAINT ck_monitor_engine_event_type CHECK (type IN (", "CONSTRAINT ck_monitor_engine_event_detail")); !slices.Equal(got, monitorcontract.EngineEventTypes()) {
		t.Errorf("monitor_engine_event.type CHECK = %v\ncontract = %v", got, monitorcontract.EngineEventTypes())
	}
	if got := quotedList(section("INSERT INTO monitor_cfg_version (name, version) VALUES", ";")); !slices.Equal(got, []string{
		monitorcontract.CfgEscalation, monitorcontract.CfgInhibit, monitorcontract.CfgCIIndex, monitorcontract.CfgCalendar,
	}) {
		t.Errorf("seeded monitor_cfg_version rows = %v", got)
	}
	base := strconv.Itoa(int(monitorcontract.EscalationSeqBase))
	levels := strconv.Itoa(monitorcontract.MaxEscalationLevels)
	for _, fragment := range []string{
		// 发件箱：升级通知的 kind、序号编码与级数上限。
		"CHECK (kind IN (1, 2, 3, " + strconv.Itoa(int(monitorcontract.NotifyKindEscalation)) + "))",
		"kind = 4 AND level BETWEEN 1 AND " + levels + " AND seq / " + base + " = level AND seq % " + base + " >= 1",
		"CHECK (source IN (1, 2, " + strconv.Itoa(int(monitorcontract.NotifySourceEscalation)) + "))",
		"jsonb_array_length(levels) BETWEEN 1 AND " + levels,
		"CHECK (level BETWEEN 1 AND " + levels + ")",
		"CHECK (escalation_level BETWEEN 0 AND " + levels + ")",
		"jsonb_array_length(relabel) <= " + strconv.Itoa(monitorcontract.MaxRelabelSteps),
		"octet_length(drop_expr) <= " + strconv.Itoa(monitorcontract.MaxDropExprBytes),
		"octet_length(ai_summary) <= " + strconv.Itoa(monitorcontract.MaxAISummaryBytes),
		"cardinality(equal_labels) <= " + strconv.Itoa(monitorcontract.MaxInhibitEqual),
		"close_reason         varchar(" + strconv.Itoa(monitorcontract.MaxCloseReasonLen) + ")",
		"CHECK (name ~ '" + monitorcontract.MetricNamePattern + "')",
		"inhibit_kind IN (0, 1, 2)",
		"'monitor.engine.*'",
		"'" + monitorcontract.EventAlertEscalated + "', '*', 'text'",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("migration lacks %q (schema drifted from kit/monitorcontract)", fragment)
		}
	}
	if !notifycontract.MatchPattern("monitor.alert.*", monitorcontract.EventAlertEscalated) {
		t.Error("the route seeded by 0069 (monitor.alert.*) must cover the escalation event")
	}
	for _, e := range monitorcontract.EngineEventTypes() {
		if !notifycontract.MatchPattern("monitor.engine.*", e) || !notifycontract.EventTypePattern.MatchString(e) {
			t.Errorf("engine event %q is not covered by the seeded route or rejected by the notify contract", e)
		}
	}
	if !notifycontract.EventPatternPattern.MatchString("monitor.engine.*") {
		t.Error("seeded route pattern rejected by the notify contract")
	}
}

// latestCheck 返回全部迁移里最后一次定义的「CONSTRAINT <name> CHECK (type IN (...))」取值片段：
// 后续迁移用 DROP / ADD CONSTRAINT 扩展集合时，契约比对以最新定义为准。
func latestCheck(t *testing.T, name string) string {
	t.Helper()
	pattern := regexp.MustCompile(`CONSTRAINT ` + regexp.QuoteMeta(name) + `\s+CHECK \(type IN \(([^)]*)\)`)
	var files []string
	if err := fs.WalkDir(FS, ".", func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".up.sql") {
			files = append(files, path)
		}
		return err
	}); err != nil {
		t.Fatalf("walk migrations: %v", err)
	}
	slices.Sort(files)
	latest := ""
	for _, f := range files {
		raw, err := fs.ReadFile(FS, f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if m := pattern.FindAllStringSubmatch(string(raw), -1); len(m) > 0 {
			latest = m[len(m)-1][1]
		}
	}
	if latest == "" {
		t.Fatalf("no migration defines %s", name)
	}
	return latest
}
