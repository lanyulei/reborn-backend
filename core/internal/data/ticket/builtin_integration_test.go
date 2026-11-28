//go:build integration

package data

import (
	"context"
	"slices"
	"testing"

	topbiz "github.com/lanyulei/reborn/backend/core/internal/biz"
	bizk8s "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

var builtinCategoryNames = []string{"运维申请", "变更管理", "故障管理", "容器平台"}

var builtinTemplateNames = []string{
	"通用审批申请", "账号开通申请", "权限申请", "资源开通申请", "咨询 / 服务请求", "值班调整申请",
	"生产变更申请", "K8s 变更审批", "事件工单", "任务 / 改进项", "申请命名空间与配额", "申请集群权限",
}

// 迁移 0095 预置的工单分类与模板在当前代码下合法：每张模板都过得了保存时的校验；容器平台的三张模板
// 满足它自动填单的要求；用内置模板能提单并走到审批；内置的可以修改、停用，不能删除。
func TestIntegration_BuiltinCatalog(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.d.Pool.Exec(ctx, testfake.BuiltinSeedSQL(t)); err != nil {
		t.Fatalf("seed the built-in catalog: %v", err)
	}
	// uSuper 是超管、uLead 是研发组负责人：内置流程的审批人。
	for _, q := range []string{
		`UPDATE "system_user" SET is_admin = true WHERE id = '` + uSuper + `'`,
		`INSERT INTO system_dept_leader (id, dept_id, user_id, sort) VALUES (gen_random_uuid(), '` + dTeam + `', '` + uLead + `', 1)`,
	} {
		if _, err := e.d.Pool.Exec(ctx, q); err != nil {
			t.Fatalf("seed approvers: %v", err)
		}
	}

	categories, err := e.catalog.List(ctx, false)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	var names []string
	byName := map[string]*biz.Template{}
	for _, c := range categories {
		if !c.Builtin {
			t.Fatalf("category %q is not marked built-in", c.Name)
		}
		names = append(names, c.Name)
		for _, tt := range c.Templates {
			if !tt.Builtin || tt.Status != biz.TemplateEnabled {
				t.Fatalf("template %q: builtin %v, status %d; want built-in and enabled", tt.Name, tt.Builtin, tt.Status)
			}
			byName[tt.Name] = tt
		}
	}
	if !slices.Equal(names, builtinCategoryNames) {
		t.Fatalf("categories = %v, want %v", names, builtinCategoryNames)
	}
	for _, name := range builtinTemplateNames {
		tt := byName[name]
		if tt == nil {
			t.Fatalf("built-in template %q is missing", name)
		}
		tpl, _, err := e.catalog.GetTemplate(ctx, tt.ID, uSuper)
		if err != nil {
			t.Fatalf("GetTemplate(%s) error = %v", name, err)
		}
		// 原样保存一遍：跑的是保存时的全部校验（表单字段、SLA、绑定的流程可发起）。
		if err := e.catalog.UpdateTemplate(ctx, tpl); err != nil {
			t.Fatalf("%s does not pass the template validation: %v", name, err)
		}
	}

	// 容器平台的三张模板：能用于审批，并且能由平台代提（只填它认识的字段）。
	changes := topbiz.NewK8sChangeTickets(NewCatalogRepo(e.d), e.uc, e.rt)
	for _, name := range []string{"K8s 变更审批", "申请命名空间与配额", "申请集群权限"} {
		id := byName[name].ID
		if err := changes.CheckTemplate(ctx, id); err != nil {
			t.Fatalf("%s is not usable by the container platform: %v", name, err)
		}
		ticketID, _, finished, status, err := changes.Create(ctx, bizk8s.ChangeTicket{
			TemplateID: id, Creator: uInit, Title: name + "：扩容 nginx", RequestID: "builtin-" + id,
			Fields: map[string]string{
				"summary": "扩容 nginx", "cluster": "prod", "namespace": "default", "target": "Deployment/default/nginx",
				"detail": "副本数 2 → 4", "link": "https://example.invalid/change/1", "reason": "流量上涨",
			},
		})
		if err != nil || ticketID == "" || finished || status != biz.StatusProcessing {
			t.Fatalf("%s: created by the platform = %q, finished %v, status %d, err %v; want a ticket waiting for approval",
				name, ticketID, finished, status, err)
		}
	}

	// 用内置模板提单：部门负责人审批（他同时是超管之外的审批人）。
	approval := byName["通用审批申请"]
	ticket, err := e.uc.Create(ctx, biz.CreateParams{
		TemplateID: approval.ID, Title: "采购显示器", Priority: 2, Creator: uInit,
		Data: map[string]any{"summary": "采购一台显示器", "detail": "开发用，显示器坏了"},
	})
	if err != nil {
		t.Fatalf("Create(a ticket from the built-in template) error = %v", err)
	}
	todo, _, _, err := e.rt.ListTasks(ctx, uLead, bizworkflow.TaskViewTodo, "", "", 1, 10)
	if err != nil || len(todo) != 1 || todo[0].InstanceID != ticket.InstanceID {
		t.Fatalf("todo(lead) = %+v, err %v; want the approval of ticket %s", todo, err, ticket.InstanceID)
	}
	if _, err := e.rt.Approve(ctx, todo[0].TaskID, uLead, "同意"); err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	if got, err := e.uc.Get(ctx, ticket.ID, uInit); err != nil || got.Ticket.Status != biz.StatusCompleted {
		t.Fatalf("ticket after approval: status %d, err %v; want completed", got.Ticket.Status, err)
	}

	// 内置分类、模板不能删除；可以修改，模板可以停用再启用。
	cat := categories[0]
	if err := e.catalog.DeleteCategory(ctx, cat.ID); !errors.Is(err, biz.ErrCategoryBuiltin) {
		t.Fatalf("DeleteCategory(built-in) error = %v, want ErrCategoryBuiltin", err)
	}
	if err := e.catalog.UpdateCategory(ctx, &biz.Category{ID: cat.ID, Name: cat.Name, Description: "改一下说明", Sort: cat.Sort}); err != nil {
		t.Fatalf("UpdateCategory(built-in) error = %v", err)
	}
	if err := e.catalog.DeleteTemplate(ctx, approval.ID); !errors.Is(err, biz.ErrTemplateBuiltin) {
		t.Fatalf("DeleteTemplate(built-in) error = %v, want ErrTemplateBuiltin", err)
	}
	off, on := biz.TemplateDisabled, biz.TemplateEnabled
	tpl, _, err := e.catalog.GetTemplate(ctx, approval.ID, uSuper)
	if err != nil {
		t.Fatal(err)
	}
	tpl.Status = off
	if err := e.catalog.UpdateTemplate(ctx, tpl); err != nil {
		t.Fatalf("disable a built-in template: %v", err)
	}
	tpl.Status = on
	if err := e.catalog.UpdateTemplate(ctx, tpl); err != nil {
		t.Fatalf("re-enable a built-in template: %v", err)
	}

	// 对照：自建的模板照常能删；必填字段不在容器平台约定里的模板不能用于审批。
	mine, err := e.catalog.CreateTemplate(ctx, &biz.Template{
		CategoryID: cat.ID, Name: "自建的审批模板", WorkflowKey: "builtin_approval_admin", NumberPrefix: "MINE", Status: biz.TemplateEnabled,
		Form: []biz.FormField{{Code: "whatever", Name: "必填项", Type: formschema.TypeString, Required: true, Rule: formschema.Rule{}}},
	})
	if err != nil {
		t.Fatalf("CreateTemplate() error = %v", err)
	}
	if mine.Builtin {
		t.Fatal("a template created through the API is marked built-in")
	}
	if err := changes.CheckTemplate(ctx, mine.ID); !errors.Is(err, bizk8s.ErrChangeTemplate) {
		t.Fatalf("CheckTemplate(a required field the platform cannot fill) error = %v, want ErrChangeTemplate", err)
	}
	if err := e.catalog.DeleteTemplate(ctx, mine.ID); err != nil {
		t.Fatalf("DeleteTemplate(user-created) error = %v", err)
	}
}
