//go:build integration

package data

import (
	"context"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"

	"github.com/go-kratos/kratos/v3/errors"
)

// waitGraph 发起 → 等待外部信号 → 审批 → 结束；流程管理员为主管。停在信号节点上时流程上无人待处理。
const waitGraph = `{"nodes":[
	{"id":"start","type":"start","name":"发起"},
	{"id":"wait","type":"timer","name":"等信号","timer":{"mode":"signal","signal":"go"}},
	{"id":"approve","type":"userTask","name":"审批","assignee":{"rules":[{"kind":"user","userIds":["` + uLead + `"]}]}},
	{"id":"end","type":"end","name":"结束"}],
	"edges":[{"id":"e1","source":"start","target":"wait"},{"id":"e2","source":"wait","target":"approve"},{"id":"e3","source":"approve","target":"end"}],
	"settings":{"admins":["` + uSuper + `"]}}`

// 流程停在没有处理人的节点上、模板也没配升级对象：SLA 超时通知落到流程管理员（不被无声丢弃），
// 事件不带分组键。信号不能改写工单名下的流程变量（表单字段、优先级），其它变量照常并入。
func TestIntegration_SLAFallbackAndOwnedVariables(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("wait_flow", waitGraph)
	c, err := e.catalog.CreateCategory(ctx, &biz.Category{Name: "等待"})
	if err != nil {
		t.Fatal(err)
	}
	tpl, err := e.catalog.CreateTemplate(ctx, &biz.Template{
		CategoryID: c.ID, Name: "等信号", WorkflowKey: "wait_flow", NumberPrefix: "W", Status: biz.TemplateEnabled,
		Form: []biz.FormField{{Code: "reason", Name: "原因", Type: formschema.TypeString}},
		SLA:  []biz.SLARule{{Priority: 3, Resolve: &biz.Duration{Value: 1, Unit: "hour"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tk, err := e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "等一等", Priority: 3, Creator: uInit, Data: map[string]any{"reason": "原值"}})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := e.d.Pool.Exec(ctx, "UPDATE ticket_order SET resolve_due = now() - interval '1 minute' WHERE id = $1", tk.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := e.uc.NotifySLA(ctx); err != nil || n != 1 {
		t.Fatalf("NotifySLA() = %d, %v; want 1", n, err)
	}
	var (
		recipients string
		group      *string
	)
	if err := e.d.Pool.QueryRow(ctx, "SELECT recipients::text, group_key FROM notify_event WHERE event_type = $1", biz.EventSLABreached).
		Scan(&recipients, &group); err != nil {
		t.Fatalf("SLA notification not written: %v", err)
	}
	if recipients != `["`+uSuper+`"]` || (group != nil && *group != "") {
		t.Fatalf("SLA notification recipients = %s, group = %v; want the workflow admin and no group key", recipients, group)
	}

	for name, vars := range map[string]map[string]any{
		"form field": {"reason": "被改写"},
		"priority":   {"priority": float64(1)},
	} {
		if _, err := e.rt.Signal(ctx, tk.InstanceID, "go", vars); !errors.Is(err, bizworkflow.ErrVariablesInvalid) {
			t.Fatalf("Signal(%s) error = %v, want ErrVariablesInvalid", name, err)
		}
	}
	if _, err := e.rt.Signal(ctx, tk.InstanceID, "go", map[string]any{"extra": "ok"}); err != nil {
		t.Fatalf("Signal(other variable) error = %v", err)
	}
	d, err := e.uc.Get(ctx, tk.ID, uInit)
	if err != nil || d.Ticket.Data["reason"] != "原值" {
		t.Fatalf("ticket data after signals = %v, err %v", d.Ticket.Data, err)
	}
	if n := e.count(`SELECT count(*) FROM workflow_instance WHERE id = $1
		AND variables->>'reason' = '原值' AND variables->>'extra' = 'ok' AND (variables->>'priority')::int = 3`, tk.InstanceID); n != 1 {
		t.Fatal("workflow variables diverged from the ticket after signals")
	}
}

// 流程仍被启用中的工单模板绑定时不能停用 / 删除；模板停用后放行。
func TestIntegration_DefinitionReferencedByTemplate(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.template("权限申请", flowKey)
	var defID string
	if err := e.d.Pool.QueryRow(ctx, "SELECT id::text FROM workflow_definition WHERE key = $1", flowKey).Scan(&defID); err != nil {
		t.Fatal(err)
	}
	off := bizworkflow.DefinitionDisabled
	if _, err := e.defs.Update(ctx, defID, bizworkflow.DefinitionUpdate{Status: &off}); !errors.Is(err, bizworkflow.ErrDefinitionInUse) {
		t.Fatalf("Update(disable referenced) error = %v, want ErrDefinitionInUse", err)
	}
	if err := e.defs.Delete(ctx, defID); !errors.Is(err, bizworkflow.ErrDefinitionInUse) {
		t.Fatalf("Delete(referenced) error = %v, want ErrDefinitionInUse", err)
	}
	full, _, err := e.catalog.GetTemplate(ctx, tpl.ID, uSuper)
	if err != nil {
		t.Fatal(err)
	}
	full.Status = biz.TemplateDisabled
	if err := e.catalog.UpdateTemplate(ctx, full); err != nil {
		t.Fatalf("UpdateTemplate(disable) error = %v", err)
	}
	if _, err := e.defs.Update(ctx, defID, bizworkflow.DefinitionUpdate{Status: &off}); err != nil {
		t.Fatalf("Update(disable unreferenced) error = %v", err)
	}
}
