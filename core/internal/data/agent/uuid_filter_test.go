package data

import (
	"regexp"
	"strings"
	"testing"
)

const filterTestID = "018f3e1a-0a80-7000-8000-0000000000a1"

// uuid 列的等值过滤（core-agent 优化项 B）：只有规范写法的入参才改用 uuid 比较（能走索引）；其它写法保持
// 原来的文本比较——原行为是「查不到」，换成 ::uuid 会让大写 ID 命中、让非法值报错。
func TestUUIDEqKeepsTextCompareForNonCanonicalInput(t *testing.T) {
	cases := map[string]string{
		filterTestID:                              "t.incident_id = ?::uuid",
		strings.ToUpper(filterTestID):             "t.incident_id::text = ?",
		strings.ReplaceAll(filterTestID, "-", ""): "t.incident_id::text = ?",
		"{" + filterTestID + "}":                  "t.incident_id::text = ?",
		"not-a-uuid":                              "t.incident_id::text = ?",
		" " + filterTestID:                        "t.incident_id::text = ?",
	}
	for id, want := range cases {
		if got := uuidEq("t.incident_id", id); got != want {
			t.Errorf("uuidEq(%q) = %q, want %q", id, got, want)
		}
	}
}

func TestProfileIDsCondFallsBackWhenAnyIDIsNotCanonical(t *testing.T) {
	other := "018f3e1a-0a80-7000-8000-0000000000a2"
	if got := profileIDsCond([]string{filterTestID, other}); got != "p.id = ANY($1::uuid[])" {
		t.Fatalf("canonical ids: cond = %q", got)
	}
	for _, ids := range [][]string{{filterTestID, strings.ToUpper(other)}, {"bogus"}, {filterTestID, ""}} {
		if got := profileIDsCond(ids); got != "p.id::text = ANY($1)" {
			t.Fatalf("ids %v: cond = %q, want the text comparison", ids, got)
		}
	}
}

// 证据条目的格式（core-agent 优化项 C）：只有「agent_run: + 规范写法的 uuid」才会被取出 uuid 去连接运行表，
// 与原来的 e = 'agent_run:' || r.id::text 能命中的字符串集合相同。前缀恰好 10 个字符（SQL 里 substring(e FROM 11)）。
func TestAgentRunEvidencePattern(t *testing.T) {
	re := regexp.MustCompile(agentRunEvidenceRE)
	if len("agent_run:") != 10 {
		t.Fatal("the evidence prefix must be 10 characters: the statement takes the uuid from position 11")
	}
	if !re.MatchString("agent_run:" + filterTestID) {
		t.Fatal("canonical evidence must match")
	}
	for _, e := range []string{
		"agent_run:" + strings.ToUpper(filterTestID), "agent_run:" + filterTestID + " ", " agent_run:" + filterTestID,
		"agent_run:" + strings.ReplaceAll(filterTestID, "-", ""), "agent_run:{" + filterTestID + "}", "agent_run:", "alert:" + filterTestID,
		"agent_run:" + filterTestID + "\n", "x\nagent_run:" + filterTestID, "",
	} {
		// Go 的 $ 与 PostgreSQL 的 $ 一样只匹配串尾（非多行模式）。
		if re.MatchString(e) {
			t.Errorf("evidence %q must not match", e)
		}
	}
}
