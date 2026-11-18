package server

import (
	"net/http"
	"strings"
	"testing"
	"time"

	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 评审第二轮 K5：「结果不完整」在响应里的写法（前端按这些字面量识别并本地化），经 HTTP 链路逐字核对。
//
//	规则列表 / 详情：status 不是 4（查询错误）而 lastError 以 "incomplete result: " 开头
//	判定记录：error 以 "incomplete result: " 开头；queries[].truncated；
//	          queries[].error == "incomplete result: partial response from the datasource"
func TestMonitor_IncompleteResultContract(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	const base = "/api/v1/monitor"
	group := monitorID(t, ts, http.MethodPost, base+"/rule-group", admin, `{"name":"team-incomplete"}`, "group")
	ruleID := monitorID(t, ts, http.MethodPost, base+"/rule", admin, monitorRuleBody(group, "many-series"), "rule")
	const ds = "018f3e1a-eeee-7000-8000-000000000001"
	const note = "incomplete result: query 0 truncated to 10000 series; query 1 returned a partial result; series not returned were left unchanged"
	now := time.Now()
	svc.monitor.store.SeedEvalTask(&bizmonitor.EvalTask{RuleID: ruleID, DatasourceID: ds, LastStatus: mc.EvalFiring, LastEvalTime: &now, LastError: note, FiringCount: 2})
	svc.monitor.store.SeedEvalRecord(&bizmonitor.EvalRecord{
		ID: "018f3e1a-eeee-7000-8000-000000000002", RuleID: ruleID, DatasourceID: ds, EvalTime: now, Status: mc.EvalFiring, Error: note, Series: 10003,
		Detail: mc.EvalDetail{Queries: []mc.EvalQueryDetail{
			{Index: 0, Severity: 3, Series: 10000, Truncated: true},
			{Index: 1, Severity: 2, Series: 3, Partial: true},
			{Index: 2, Severity: 1, Series: 0},
		}},
	})

	lastError := `"lastError":"` + note + `"`
	expectStatus(t, "rule list", ts, http.MethodGet, base+"/rule?page=1&pageSize=10&groupId="+group, admin, "", http.StatusOK, `"status":2`, lastError)
	expectStatus(t, "rule detail", ts, http.MethodGet, base+"/rule/"+ruleID, admin, "", http.StatusOK, `"status":2`, lastError, `"lastStatus":2`)
	resp := expectStatus(t, "eval records", ts, http.MethodGet, base+"/rule/"+ruleID+"/eval-record?page=1&pageSize=10", admin, "", http.StatusOK,
		`"status":2`, `"error":"`+note+`"`)
	// protojson 在分隔符后随机加空格：去掉空格再比对各查询的完整对象。
	compact := strings.ReplaceAll(resp, " ", "")
	for _, want := range []string{
		`{"index":0,"severity":3,"series":10000,"truncated":true,"error":""}`,
		`{"index":1,"severity":2,"series":3,"truncated":false,"error":"incomplete result: partial response from the datasource"}`,
		`{"index":2,"severity":1,"series":0,"truncated":false,"error":""}`,
	} {
		if !strings.Contains(compact, strings.ReplaceAll(want, " ", "")) {
			t.Fatalf("eval record lacks %s: %s", want, resp)
		}
	}
	if strings.Contains(resp, `"partial"`) {
		t.Fatalf("the response has no partial field this round (no proto change): %s", resp)
	}
}
