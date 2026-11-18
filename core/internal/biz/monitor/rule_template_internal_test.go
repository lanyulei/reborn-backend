package biz

import (
	"strings"
	"testing"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 每个内置模板包的每条规则都要过得了导入时的转换与静态校验（表达式语法、附加信息模板、时长）。
func TestTemplatePacks_EveryRuleConverts(t *testing.T) {
	if len(templatePacks) != len(templateCatalog) || len(templatePacks) < 6 {
		t.Fatalf("packs = %d, catalog = %d", len(templatePacks), len(templateCatalog))
	}
	opts := &ImportOptions{GroupID: mc.RootGroupID, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: mc.SeverityWarning}
	seenCodes := map[string]bool{}
	for _, pack := range templatePacks {
		if seenCodes[pack.Code] || pack.Name == "" || pack.Description == "" || pack.Category == "" {
			t.Errorf("pack %q: duplicated or incomplete catalog entry", pack.Code)
		}
		seenCodes[pack.Code] = true
		parsed, err := parseRuleFile(pack.Content)
		if err != nil {
			t.Fatalf("pack %q: %v", pack.Code, err)
		}
		if len(parsed) < 5 || int(pack.RuleCount) != len(parsed) || len(pack.Rules) != len(parsed) {
			t.Errorf("pack %q: %d parsed, RuleCount %d, %d items", pack.Code, len(parsed), pack.RuleCount, len(pack.Rules))
		}
		names := map[string]bool{}
		for i, p := range parsed {
			item := convertRule(i, p, opts)
			if item.Status != ImportNew || len(item.Warnings) != 0 {
				t.Errorf("pack %q rule %d %q: status %s error %q warnings %v", pack.Code, i, item.Name, item.Status, item.Error, item.Warnings)
				continue
			}
			if names[item.Name] {
				t.Errorf("pack %q: rule name %q is duplicated", pack.Code, item.Name)
			}
			names[item.Name] = true
			if strings.TrimSpace(pack.Rules[i].Summary) == "" || pack.Rules[i].Severity != item.Spec.Queries[0].Severity || pack.Rules[i].ForS != item.Spec.ForS {
				t.Errorf("pack %q rule %q: summary item %+v does not match the converted rule", pack.Code, item.Name, pack.Rules[i])
			}
		}
	}
}

func TestTemplatePacks_SortedAndCopied(t *testing.T) {
	uc := &RuleUsecase{}
	packs := uc.TemplatePacks()
	for i := 1; i < len(packs); i++ {
		a, b := packs[i-1], packs[i]
		if a.Category > b.Category || (a.Category == b.Category && a.Name > b.Name) {
			t.Errorf("packs not sorted by category, name: %q before %q", a.Code, b.Code)
		}
	}
	d, err := uc.TemplatePack("mysql")
	if err != nil || d.Code != "mysql" || d.Content == "" {
		t.Fatalf("TemplatePack(mysql) = %+v, %v", d, err)
	}
	d.Rules[0].Name = "mutated"
	if again, _ := uc.TemplatePack("mysql"); again.Rules[0].Name == "mutated" {
		t.Error("TemplatePack must return a copy")
	}
	if _, err := uc.TemplatePack("nope"); err != ErrRuleTemplateNotFound {
		t.Errorf("TemplatePack(nope) error = %v", err)
	}
}
