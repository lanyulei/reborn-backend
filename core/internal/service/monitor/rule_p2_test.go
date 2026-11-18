package service

import (
	"context"
	"testing"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 内置模板接口不依赖任何仓储：以最小装配验证 proto 映射与 404。
func TestRuleService_TemplatePacks(t *testing.T) {
	t.Parallel()
	svc := NewRuleService(biz.NewRuleUsecase(nil, nil, nil, nil, nil, nil, nil, nil, nil), nil)
	ctx := context.Background()
	list, err := svc.ListRuleTemplatePacks(ctx, &v1.ListRuleTemplatePacksRequest{})
	if err != nil || len(list.GetItems()) < 6 {
		t.Fatalf("ListRuleTemplatePacks = %d items, %v", len(list.GetItems()), err)
	}
	for _, p := range list.GetItems() {
		d, err := svc.GetRuleTemplatePack(ctx, &v1.GetRuleTemplatePackRequest{Code: p.GetCode()})
		if err != nil {
			t.Fatalf("GetRuleTemplatePack(%s): %v", p.GetCode(), err)
		}
		if d.GetPack().GetCode() != p.GetCode() || d.GetContent() == "" || len(d.GetRules()) != int(p.GetRuleCount()) || p.GetRuleCount() == 0 {
			t.Errorf("pack %s: %d rules, rule_count %d", p.GetCode(), len(d.GetRules()), p.GetRuleCount())
		}
		for _, r := range d.GetRules() {
			if r.GetName() == "" || r.GetExpr() == "" || r.GetSeverity() < 1 || r.GetSeverity() > 4 || r.GetSummary() == "" {
				t.Errorf("pack %s: incomplete rule item %+v", p.GetCode(), r)
			}
		}
	}
	if _, err := svc.GetRuleTemplatePack(ctx, &v1.GetRuleTemplatePackRequest{Code: "nope"}); !kerrors.Is(err, biz.ErrRuleTemplateNotFound) {
		t.Errorf("unknown pack: err = %v", err)
	}
	// 未经 ProvideRuleService 装配：模拟触发与候选项明确返回 501，而不是空指针。
	if _, err := svc.SimulateRule(ctx, &v1.SimulateRuleRequest{}); !v1.IsMonitorNotImplemented(err) {
		t.Errorf("SimulateRule without wiring: err = %v", err)
	}
	if _, err := svc.ListMonitorRuleOptions(ctx, &v1.ListMonitorRuleOptionsRequest{}); !v1.IsMonitorNotImplemented(err) {
		t.Errorf("ListMonitorRuleOptions without wiring: err = %v", err)
	}
}
