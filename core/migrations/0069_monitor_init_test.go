package migrations

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"regexp"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

// monitorTemplateRow 匹配 0069 内置的通知模板行：事件类型、标题模板、正文模板。
var monitorTemplateRow = regexp.MustCompile(
	`\('018f3e1a-0cc2-7000-8000-00000000000\d', 'default', '([a-z.]+)', '\*', 'text',\s*'([^']*)',\s*'([^']*)'\)`)

// monitorRenderContext 与通知管道的渲染上下文同形（notify/internal/biz.RenderContext）。
type monitorRenderContext struct {
	Title     string
	Payload   any
	Severity  int32
	Count     int
	Samples   []map[string]any
	BizID     string
	EventTime time.Time
}

// TestMonitorSeedTemplatesRender 锁定 0069 内置的四个告警通知模板：
//   - 能通过通知模板的写入校验口径（白名单函数、missingkey=zero、样例载荷试渲染）——否则用户在
//     界面上原样保存内置模板都会被拒绝；
//   - 用 kit/monitorcontract.NotifyPayload 渲染时每个取值都有内容（模板引用的键与契约一致）。
func TestMonitorSeedTemplatesRender(t *testing.T) {
	t.Parallel()
	raw, err := fs.ReadFile(FS, "0069_monitor_init.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	rows := monitorTemplateRow.FindAllStringSubmatch(string(raw), -1)
	want := map[string]bool{
		monitorcontract.EventAlertFiring: true, monitorcontract.EventAlertResolved: true,
		monitorcontract.EventSubscriptionFiring: true, monitorcontract.EventSubscriptionResolved: true,
	}
	if len(rows) != len(want) {
		t.Fatalf("expected %d seeded templates, found %d", len(want), len(rows))
	}
	payload := monitorcontract.NotifyPayload{
		AlertID: "018f3e1a-0000-7000-8000-00000000000a", AlertURL: "https://reborn.example.com/monitor/alert/x",
		RuleID: "018f3e1a-0000-7000-8000-00000000000b", RuleName: "主机 CPU 使用率过高", GroupName: "基础设施",
		DatasourceName: "prod-vm", Severity: monitorcontract.SeverityCritical, Status: monitorcontract.PayloadStatusFiring,
		Labels: map[string]string{"instance": "10.0.0.1:9100"}, Annotations: map[string]string{"summary": "CPU 95%"},
		Value: "95.2", RecoveryValue: "41.7", Expr: "cpu_usage > 90", FirstFireTime: "2026-11-17 10:00:00", Duration: "5m",
		NotifySeq: 2, SubscriptionName: "我的订阅",
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any // 管道渲染时载荷即 JSON 解码后的 map
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	trial := monitorRenderContext{
		Title: "sample", Payload: map[string]any{"host": "sample-host"}, Severity: 2, Count: 2, BizID: notifycontract.DefaultBizID,
		EventTime: time.Unix(0, 0).UTC(),
	}
	full := monitorRenderContext{
		Title: payload.RuleName, Payload: decoded, Severity: payload.Severity, Count: 1, BizID: monitorcontract.NotifyBizID,
		EventTime: time.Unix(0, 0).UTC(),
	}
	for _, row := range rows {
		eventType := row[1]
		if !want[eventType] {
			t.Errorf("unexpected seeded template for %q", eventType)
			continue
		}
		delete(want, eventType)
		if !notifycontract.TemplateEventTypePattern.MatchString(eventType) {
			t.Errorf("%s: event type rejected by notify contract", eventType)
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
			if !strings.Contains(out, payload.RuleName) {
				t.Errorf("%s %s: rule name missing:\n%s", eventType, part, out)
			}
			if part != "body" {
				continue
			}
			for _, must := range []string{"基础设施", "严重", "prod-vm", "95.2", "instance = 10.0.0.1:9100", "summary：CPU 95%", payload.AlertURL} {
				if !strings.Contains(out, must) {
					t.Errorf("%s body: %q missing:\n%s", eventType, must, out)
				}
			}
			if strings.HasPrefix(eventType, "monitor.subscription.") && !strings.Contains(out, "我的订阅") {
				t.Errorf("%s body: subscription name missing", eventType)
			}
			if strings.HasSuffix(eventType, ".resolved") && !strings.Contains(out, "41.7") {
				t.Errorf("%s body: recovery value missing", eventType)
			}
		}
	}
	for missing := range want {
		t.Errorf("seeded template for %q not found", missing)
	}
}
