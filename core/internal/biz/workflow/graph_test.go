package biz

import (
	"slices"
	"strings"
	"testing"
)

func issueCodes(t *testing.T, raw string) []string {
	t.Helper()
	g, err := ParseGraph([]byte(raw))
	if err != nil {
		t.Fatalf("ParseGraph() error = %v", err)
	}
	var codes []string
	for _, is := range g.Validate() {
		codes = append(codes, is.Code)
	}
	slices.Sort(codes)
	return slices.Compact(codes)
}

func TestParseGraph_Invalid(t *testing.T) {
	t.Parallel()
	if _, err := ParseGraph([]byte(`{"nodes":`)); err == nil {
		t.Fatal("ParseGraph(bad json) must fail")
	}
}

func TestValidate_Valid(t *testing.T) {
	t.Parallel()
	for name, raw := range map[string]string{
		"linear": linearGraph(task("a", "any", "u1")), "branching": branching, "parallel": parallel,
		// 人工任务可以选「拦住」（解析不出处理人时不跳过）。
		"block when empty": linearGraph(blockedTask("a", `,"onEmpty":"block"`)),
	} {
		if codes := issueCodes(t, raw); len(codes) > 0 {
			t.Errorf("%s: unexpected issues %v", name, codes)
		}
	}
}

func TestValidate_Issues(t *testing.T) {
	t.Parallel()
	ut := func(id string) string { return task(id, "any", "u1") }
	cases := map[string]struct {
		raw  string
		want string
	}{
		"empty graph":       {`{"nodes":[],"edges":[]}`, "GRAPH_SIZE"},
		"no start":          {`{"nodes":[{"id":"e","type":"end"}],"edges":[]}`, "START_COUNT"},
		"no end":            {`{"nodes":[{"id":"s","type":"start"}],"edges":[]}`, "END_MISSING"},
		"duplicate node id": {`{"nodes":[{"id":"s","type":"start"},{"id":"s","type":"end"},{"id":"e","type":"end"}],"edges":[]}`, "NODE_ID_DUPLICATE"},
		"unknown node type": {`{"nodes":[{"id":"s","type":"start"},{"id":"x","type":"script"},{"id":"e","type":"end"}],"edges":[]}`, "NODE_TYPE_UNKNOWN"},
		"edge to missing":   {`{"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"id":"1","source":"s","target":"zz"}]}`, "EDGE_ENDPOINT_MISSING"},
		"duplicate edge": {
			`{"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"id":"1","source":"s","target":"e"},{"id":"2","source":"s","target":"e"}]}`,
			"EDGE_DUPLICATE",
		},
		"start with two outgoing": {
			`{"nodes":[{"id":"s","type":"start"},` + ut("a") + `,{"id":"e","type":"end"}],"edges":[{"id":"1","source":"s","target":"a"},{"id":"2","source":"s","target":"e"},{"id":"3","source":"a","target":"e"}]}`,
			"START_DEGREE",
		},
		"missing assignee":          {strings.Replace(linearGraph(ut("a")), `"assignee":{"mode":"any","rules":[{"kind":"user","userIds":["u1"]}]}`, `"assignee":{"rules":[]}`, 1), "ASSIGNEE_MISSING"},
		"bad mode":                  {linearGraph(task("a", "vote", "u1")), "ASSIGNEE_MODE"},
		"incomplete rule":           {linearGraph(`{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"deptLeader","level":0}]}}`), "ASSIGNEE_RULE_INVALID"},
		"unknown rule":              {linearGraph(`{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"oracle"}]}}`), "ASSIGNEE_RULE_UNKNOWN"},
		"unknown on-empty policy":   {linearGraph(`{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"initiator"}],"onEmpty":"halt"}}`), "ASSIGNEE_ON_EMPTY"},
		"block on a cc node":        {linearGraph(`{"id":"a","type":"cc","assignee":{"rules":[{"kind":"initiator"}],"onEmpty":"block"}}`), "ON_EMPTY_BLOCK_NOT_ALLOWED"},
		"reject to self":            {linearGraph(`{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"initiator"}]},"onReject":{"to":"a"}}`), "ON_REJECT_TARGET"},
		"reject to non-task":        {linearGraph(`{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"initiator"}]},"onReject":{"to":"e"}}`), "ON_REJECT_TARGET"},
		"condition outside gateway": {strings.Replace(linearGraph(ut("a")), `"source":"a","target":"e"`, `"source":"a","target":"e","condition":"true"`, 1), "CONDITION_NOT_ALLOWED"},
		"missing condition":         {strings.Replace(branching, `"condition":"form.amount >= 10000",`, ``, 1), "CONDITION_MISSING"},
		"invalid condition":         {strings.Replace(branching, `form.amount >= 10000`, `form.amount >=`, 1), "CONDITION_INVALID"},
		"non-boolean condition":     {strings.Replace(branching, `form.amount >= 10000`, `1 + 1`, 1), "CONDITION_INVALID"},
		"unknown variable":          {strings.Replace(branching, `form.amount >= 10000`, `secret == 1`, 1), "CONDITION_INVALID"},
		"no default branch":         {strings.Replace(branching, `"isDefault":true`, `"condition":"true"`, 1), "DEFAULT_BRANCH"},
		"unreachable node": {
			`{"nodes":[{"id":"s","type":"start"},` + ut("a") + `,` + ut("x") + `,{"id":"e","type":"end"}],"edges":[{"id":"1","source":"s","target":"a"},{"id":"2","source":"a","target":"e"},{"id":"3","source":"x","target":"e"}]}`,
			"UNREACHABLE",
		},
		"fork without join": {strings.Replace(parallel, `,"join":"j"`, ``, 1), "JOIN_MISSING"},
		"join mismatch":     {strings.Replace(parallel, `{"id":"8","source":"c","target":"j"}`, `{"id":"8","source":"c","target":"z"}`, 1), "JOIN_MISMATCH"},
		"parallel branch ends flow": {strings.Replace(strings.Replace(parallel, `{"id":"7","source":"b","target":"j"}`, `{"id":"7","source":"b","target":"e"}`, 1),
			`{"id":"j","type":"parallelGateway"}`, `{"id":"j","type":"parallelGateway"}`, 1), "PARALLEL_ESCAPE"},
		"reject across parallel branch": {strings.Replace(parallel, `{"id":"b","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["u2"]}]}}`,
			`{"id":"b","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["u2"]}]},"onReject":{"to":"a"}}`, 1), "ON_REJECT_REGION"},
		"loop without user task": {`{"nodes":[{"id":"s","type":"start"},{"id":"g1","type":"exclusiveGateway"},{"id":"g2","type":"exclusiveGateway"},{"id":"e","type":"end"}],"edges":[` +
			`{"id":"1","source":"s","target":"g1"},{"id":"2","source":"g1","target":"g2"},` +
			`{"id":"3","source":"g2","target":"g1","condition":"true"},{"id":"4","source":"g2","target":"e","isDefault":true}]}`, "AUTO_LOOP"},
	}
	for name, tt := range cases {
		if codes := issueCodes(t, tt.raw); !slices.Contains(codes, tt.want) {
			t.Errorf("%s: issues = %v, want to contain %s", name, codes, tt.want)
		}
	}
}

func TestCondition_CostLimit(t *testing.T) {
	t.Parallel()
	// 指数级展开的表达式在成本上限内被中止，而不是耗尽 CPU。
	prog, err := compileCondition(`form.items.all(a, form.items.all(b, form.items.all(c, a + b + c > 0)))`)
	if err != nil {
		t.Fatalf("compileCondition() error = %v", err)
	}
	items := make([]any, 200)
	for i := range items {
		items[i] = int64(i + 1)
	}
	if _, err := prog.eval(map[string]any{"form": map[string]any{"items": items}, "initiator": "", "initiatorDept": ""}); err == nil {
		t.Fatal("expensive condition must exceed the cost limit")
	}
}

// 并行网关必须构成严格的块结构：下列图在引擎里会让执行位置永远等不齐或提前合并，
// 必须在发布校验时拒绝。
func TestValidate_ParallelBlockStructure(t *testing.T) {
	t.Parallel()
	ut := func(id string) string { return task(id, "any", "u1") }
	gw := func(id, typ, join string) string {
		if join != "" {
			return `{"id":"` + id + `","type":"` + typ + `","join":"` + join + `"}`
		}
		return `{"id":"` + id + `","type":"` + typ + `"}`
	}
	edge := func(spec string) string { // "a>b" 或 "a>b?cond" 或 "a>b!"（默认分支）
		src, rest, _ := strings.Cut(spec, ">")
		dst, extra := rest, ""
		if i := strings.IndexAny(rest, "?!"); i >= 0 {
			dst = rest[:i]
			if rest[i] == '!' {
				extra = `,"isDefault":true`
			} else {
				extra = `,"condition":"` + rest[i+1:] + `"`
			}
		}
		return `{"id":"` + src + "_" + dst + `","source":"` + src + `","target":"` + dst + `"` + extra + `}`
	}
	graph := func(nodes []string, edges ...string) string {
		es := make([]string, 0, len(edges))
		for _, e := range edges {
			es = append(es, edge(e))
		}
		return `{"nodes":[{"id":"s","type":"start"},` + strings.Join(nodes, ",") + `,{"id":"e","type":"end"}],"edges":[` + strings.Join(es, ",") + `]}`
	}
	x, p := NodeExclusive, NodeParallel
	cases := map[string]struct {
		raw  string
		want string // 空 = 应当合法
	}{
		// 入边 > 1 的并行网关不是任何分叉的汇聚：只会到达一个执行位置，永远等不齐。
		"parallel merge without fork": {graph([]string{gw("X", x, ""), ut("A"), ut("B"), gw("P", p, "")},
			"s>X", "X>A?true", "X>B!", "A>P", "B>P", "P>e"), "JOIN_UNPAIRED"},
		// 从分叉之外直接连进并行分支：汇聚只收到一个执行位置。
		"edge entering a branch from outside": {graph([]string{gw("X", x, ""), gw("F", p, "J"), ut("A"), ut("B"), gw("J", p, "")},
			"s>X", "X>F!", "X>A?true", "F>A", "F>B", "A>J", "B>J", "J>e"), "PARALLEL_ENTRY"},
		// 汇聚之后绕回并行分支内部。
		"loop back into a branch": {graph([]string{gw("F", p, "J"), ut("A"), ut("B"), gw("J", p, ""), ut("T"), gw("G", x, "")},
			"s>F", "F>A", "F>B", "A>J", "B>J", "J>T", "T>G", "G>A?true", "G>e!"), "PARALLEL_ENTRY"},
		// 嵌套分叉借用外层的汇聚：三个执行位置对两条入边，提前合并且泄漏一个。
		"nested fork sharing the outer join": {graph([]string{gw("F1", p, "J"), ut("A"), gw("F2", p, "J"), ut("C"), ut("D"), gw("M", x, ""), gw("J", p, "")},
			"s>F1", "F1>A", "F1>F2", "F2>C", "F2>D", "C>M", "D>M", "M>J", "A>J", "J>e"), "JOIN_SHARED"},
		// 嵌套分叉的汇聚落在外层分支之外。
		"nested join outside the enclosing branch": {graph([]string{gw("F1", p, "J1"), ut("A"), gw("F2", p, "J2"), ut("C"), ut("D"), gw("J1", p, ""), gw("J2", p, "")},
			"s>F1", "F1>A", "F1>F2", "F2>C", "F2>D", "A>J1", "C>J2", "D>J2", "J2>e", "J1>e"), ""},
		// 两条分支在汇聚之前先合到同一个节点。
		"branches sharing a node": {graph([]string{gw("F", p, "J"), ut("A"), ut("B"), gw("M", x, ""), ut("C"), gw("J", p, "")},
			"s>F", "F>A", "F>B", "A>M", "B>M", "M>C", "C>J", "F>J", "J>e"), "PARALLEL_OVERLAP"},
		// 同一分叉下跨分支连线。
		"edge across sibling branches": {graph([]string{gw("F", p, "J"), ut("A"), gw("G", x, ""), ut("B"), gw("J", p, "")},
			"s>F", "F>A", "F>B", "A>G", "G>J!", "G>B?true", "B>J", "J>e"), "PARALLEL_OVERLAP"},
		// 合法：正确嵌套的两层并行（内层汇聚在外层分支内）。
		"properly nested forks": {graph([]string{gw("F1", p, "J1"), ut("A"), gw("F2", p, "J2"), ut("C"), ut("D"), gw("J2", p, ""), ut("E"), gw("J1", p, "")},
			"s>F1", "F1>A", "F1>F2", "F2>C", "F2>D", "C>J2", "D>J2", "J2>E", "E>J1", "A>J1", "J1>e"), ""},
		// 合法：并行分支内部的条件分支与回路。
		"exclusive loop inside a branch": {graph([]string{gw("F", p, "J"), ut("A"), gw("G", x, ""), gw("N", x, ""), ut("B"), gw("J", p, "")},
			"s>F", "F>N", "N>A", "A>G", "G>N?true", "G>J!", "F>B", "B>J", "J>e"), ""},
	}
	for name, tt := range cases {
		codes := issueCodes(t, tt.raw)
		switch {
		case name == "nested join outside the enclosing branch":
			// 该图同时违反多条规则，至少须报出一条并行结构问题。
			if !slices.ContainsFunc(codes, func(c string) bool { return strings.HasPrefix(c, "JOIN_") || strings.HasPrefix(c, "PARALLEL_") }) {
				t.Errorf("%s: issues = %v, want a parallel structure issue", name, codes)
			}
		case tt.want == "" && len(codes) > 0:
			t.Errorf("%s: unexpected issues %v", name, codes)
		case tt.want != "" && !slices.Contains(codes, tt.want):
			t.Errorf("%s: issues = %v, want to contain %s", name, codes, tt.want)
		}
	}
}

func TestValidate_IDFormat(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("n", 65)
	for name, tt := range map[string]struct{ raw, want string }{
		"node id too long":   {linearGraph(task(long, "any", "u1")), "NODE_ID_INVALID"},
		"node id with comma": {strings.ReplaceAll(linearGraph(task("a", "any", "u1")), `"a"`, `"a,b"`), "NODE_ID_INVALID"},
		"node name too long": {strings.Replace(linearGraph(task("a", "any", "u1")), `"name":"a"`, `"name":"`+strings.Repeat("名", 50)+`"`, 1), "NODE_NAME_TOO_LONG"},
	} {
		if codes := issueCodes(t, tt.raw); !slices.Contains(codes, tt.want) {
			t.Errorf("%s: issues = %v, want to contain %s", name, codes, tt.want)
		}
	}
}
