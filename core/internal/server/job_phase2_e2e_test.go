package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

const jobTargetAllProd = `{"modelCode":"host","mode":2,"env":"prod","addressField":"ip"}`

func (f jobFixture) adhocBody(kind int, adhoc, extra string) string {
	return `{"kind":` + strconv.Itoa(kind) + `,"adhoc":` + adhoc + `,"target":` + jobTargetAllProd +
		`,"credentialId":"` + f.credentialID + `"` + extra + `}`
}

func (f jobFixture) riskRule(t *testing.T, name, pattern string, action int) {
	t.Helper()
	body := `{"spec":{"name":"` + name + `","pattern":` + jsonString(pattern) + `,"action":` + strconv.Itoa(action) + `,"enabled":true,"description":"d"}}`
	expectStatus(t, "create rule "+name, f.ts, http.MethodPost, "/api/v1/job/risk-rule", f.admin, body, http.StatusOK)
}

// enableApproval 配置一个可发起的审批流程。
func (f jobFixture) enableApproval(t *testing.T, extra string) {
	t.Helper()
	f.svc.job.approvals.Keys["job-approval"] = "作业审批"
	expectStatus(t, "set policy", f.ts, http.MethodPut, "/api/v1/job/policy", f.admin,
		`{"policy":{"approvalWorkflowKey":"job-approval","version":1`+extra+`}}`, http.StatusOK,
		`"approvalWorkflowReady":true`, `"approvalWorkflowName":"作业审批"`)
}

func executionID(t *testing.T, body string) string {
	t.Helper()
	var out struct {
		Execution struct {
			ID string `json:"id"`
		}
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil || out.Execution.ID == "" {
		t.Fatalf("no execution id in %s", body)
	}
	return out.Execution.ID
}

// 快速执行：三种类型的内容校验与固化、脚本库引用、文件内容快照、提交前检查。
func TestJob_AdhocKinds(t *testing.T) {
	t.Parallel()
	f := newJobFixture(t)
	ts, admin := f.ts, f.admin
	const submit, check = "/api/v1/job/adhoc", "/api/v1/job/adhoc:check"

	// 命令。
	body := expectStatus(t, "command", ts, http.MethodPost, submit, admin, f.adhocBody(2, `{"command":" uptime && df -h "}`, `,"options":{"forks":3,"batchSize":1,"maxFailPercent":50}`),
		http.StatusOK, `"kind":2`, `"status":1`, `"hostTotal":2`, `"templateName":"命令：uptime && df -h"`,
		`"command":"uptime && df -h"`, `"batchSize":1`, `"maxFailPercent":50`, `"forks":3`, `"modelCode":"host"`, `"credentialName":"deploy-key"`)
	if strings.Contains(body, `"templateId":"0`) || strings.Contains(body, "OPENSSH") {
		t.Fatalf("adhoc execution must have no template and leak no secret: %s", body)
	}
	bad := map[string]string{
		"blank command":     f.adhocBody(2, `{"command":"   "}`, ""),
		"script w/o interp": f.adhocBody(3, `{"script":"echo hi"}`, ""),
		"file w/o upload":   f.adhocBody(4, `{"dest":"/opt/app"}`, ""),
		"relative dest":     f.adhocBody(4, `{"fileId":"018f3e1a-0000-7000-8000-0000000000f1","dest":"opt/app"}`, ""),
	}
	for name, req := range bad {
		expectStatus(t, name, ts, http.MethodPost, submit, admin, req, http.StatusBadRequest, "JOB_ADHOC_INVALID")
	}
	expectStatus(t, "unknown credential", ts, http.MethodPost, submit, admin,
		strings.Replace(f.adhocBody(2, `{"command":"id"}`, ""), f.credentialID, "018f3e1a-0000-7000-8000-00000000dead", 1), http.StatusNotFound, "JOB_CREDENTIAL_NOT_FOUND")

	// 脚本库：新建、列表不带内容、执行时固化当时的版本。
	scriptID := monitorID(t, ts, http.MethodPost, "/api/v1/job/script", admin,
		`{"spec":{"name":"清理日志","interpreter":"bash","content":"#!/bin/bash\r\nfind /var/log/app -mtime +7 -delete\r\n"}}`, "script")
	if list := expectStatus(t, "list scripts", ts, http.MethodGet, "/api/v1/job/script?page=1&pageSize=10", admin, "", http.StatusOK, `"total":"1"`, `"size":`); strings.Contains(list, "find /var") {
		t.Fatalf("script list must not carry content: %s", list)
	}
	expectStatus(t, "duplicate script", ts, http.MethodPost, "/api/v1/job/script", admin,
		`{"spec":{"name":"清理日志","interpreter":"sh","content":"x"}}`, http.StatusConflict, "JOB_SCRIPT_NAME_EXISTS")
	body = expectStatus(t, "script from library", ts, http.MethodPost, submit, admin,
		f.adhocBody(3, `{"scriptId":"`+scriptID+`","scriptArgs":"--dry-run"}`, ""), http.StatusOK,
		`"kind":3`, `"scriptName":"清理日志"`, `"scriptVersion":1`, `"interpreter":"bash"`, `"scriptArgs":"--dry-run"`,
		`find /var/log/app -mtime +7 -delete\n`)
	if strings.Contains(body, `\r\n`) {
		t.Fatalf("script line endings not normalized: %s", body)
	}
	frozen := executionID(t, body)
	expectStatus(t, "edit script", ts, http.MethodPut, "/api/v1/job/script/"+scriptID, admin,
		`{"spec":{"name":"清理日志","interpreter":"bash","content":"echo changed"},"version":1}`, http.StatusOK, `"version":2`)
	if got := expectStatus(t, "frozen script", ts, http.MethodGet, "/api/v1/job/execution/"+frozen, admin, "", http.StatusOK, `"scriptVersion":1`); strings.Contains(got, "echo changed") {
		t.Fatalf("editing the library script changed a submitted execution: %s", got)
	}

	// 文件：内容固化进执行记录，响应里只有名称、大小与摘要。
	const fileID = "018f3e1a-0000-7000-8000-0000000000f1"
	f.svc.job.files.Put(e2eActorID, fileID, "app.conf", []byte("listen 8080;\n"))
	body = expectStatus(t, "file", ts, http.MethodPost, submit, admin, f.adhocBody(4, `{"fileId":"`+fileID+`","dest":"/etc/app/"}`, ""), http.StatusOK,
		`"kind":4`, `"fileName":"app.conf"`, `"dest":"/etc/app"`, `"mode":"0644"`, `"fileSize":"13"`, `"fileSha256":"`)
	if strings.Contains(body, "listen 8080") || strings.Contains(body, fileID) {
		t.Fatalf("file content or id leaked into the response: %s", body)
	}
	if got := f.svc.job.store.Payload(executionID(t, body)); string(got) != "listen 8080;\n" {
		t.Fatalf("stored payload = %q", got)
	}
	// 分发的文本文件同样过高危规则：被拦截的命令不能先当文件发下去。
	f.riskRule(t, "block-rm", `rm\s+-rf\s+/(\s|$)`, 1)
	const evilID = "018f3e1a-0000-7000-8000-0000000000f3"
	f.svc.job.files.Put(e2eActorID, evilID, "cleanup.sh", []byte("#!/bin/sh\nrm -rf /\n"))
	expectStatus(t, "blocked file content", ts, http.MethodPost, submit, admin, f.adhocBody(4, `{"fileId":"`+evilID+`","dest":"/tmp"}`, ""),
		http.StatusForbidden, "JOB_RISK_BLOCKED")
	expectStatus(t, "someone else's file", ts, http.MethodPost, submit, admin,
		f.adhocBody(4, `{"fileId":"018f3e1a-0000-7000-8000-0000000000f2","dest":"/tmp"}`, ""), http.StatusBadRequest, "JOB_ADHOC_INVALID")

	// 提交前检查：主机、上限；不落库。
	expectStatus(t, "check", ts, http.MethodPost, check, admin, `{"kind":2,"adhoc":{"command":"uptime"},"target":`+jobTargetAllProd+`}`, http.StatusOK,
		`"10.0.0.1"`, `"10.0.0.2"`, `"maxHosts":500`, `"blocked":false`)
	expectStatus(t, "list by kind", ts, http.MethodGet, "/api/v1/job/execution?page=1&pageSize=10&kinds=2&kinds=4", admin, "", http.StatusOK, `"total":"2"`)
	expectStatus(t, "list by kind 3", ts, http.MethodGet, "/api/v1/job/execution?page=1&pageSize=10&kinds=3", admin, "", http.StatusOK, `"total":"1"`)
}

// 高危命令规则：拦截、须审批、提醒；规则校验与试匹配；审批未配置时拒绝提交。
func TestJob_RiskRulesAndApproval(t *testing.T) {
	t.Parallel()
	f := newJobFixture(t)
	ts, admin := f.ts, f.admin
	const submit = "/api/v1/job/adhoc"
	f.riskRule(t, "block-rm", `rm\s+-rf\s+/(\s|$)`, 1)
	f.riskRule(t, "approve-reboot", `\breboot\b`, 2)
	f.riskRule(t, "warn-chmod", `chmod\s+-R`, 3)
	for name, pattern := range map[string]string{"bad regex": `([`, "matches empty": `a*`} {
		expectStatus(t, name, ts, http.MethodPost, "/api/v1/job/risk-rule", admin,
			`{"spec":{"name":"x","pattern":`+jsonString(pattern)+`,"action":1,"enabled":true}}`, http.StatusBadRequest, "JOB_RISK_RULE_INVALID")
	}
	expectStatus(t, "test all", ts, http.MethodPost, "/api/v1/job/risk-rule:test", admin, `{"content":"chmod -R 777 /data; reboot"}`, http.StatusOK,
		`"approve-reboot"`, `"warn-chmod"`, `"match":"reboot"`)
	expectStatus(t, "test draft", ts, http.MethodPost, "/api/v1/job/risk-rule:test", admin,
		`{"content":"systemctl stop nginx","draft":{"name":"d","pattern":"systemctl\\s+stop","action":3}}`, http.StatusOK, `"match":"systemctl stop"`)

	// 拦截：命令与脚本内容都查；不建单。
	expectStatus(t, "blocked command", ts, http.MethodPost, submit, admin, f.adhocBody(2, `{"command":"rm -rf / "}`, ""), http.StatusForbidden, "JOB_RISK_BLOCKED", "block-rm")
	expectStatus(t, "blocked script", ts, http.MethodPost, submit, admin,
		f.adhocBody(3, `{"script":"#!/bin/sh\necho start\nrm -rf /\n","interpreter":"sh"}`, ""), http.StatusForbidden, "JOB_RISK_BLOCKED")
	expectStatus(t, "check shows block", ts, http.MethodPost, "/api/v1/job/adhoc:check", admin,
		`{"kind":2,"adhoc":{"command":"rm -rf /"},"target":`+jobTargetAllProd+`}`, http.StatusOK, `"blocked":true`, `"action":1`)

	// 仅提醒：照常排队，命中记在执行上。
	expectStatus(t, "warn only", ts, http.MethodPost, submit, admin, f.adhocBody(2, `{"command":"chmod -R 755 /srv/app"}`, ""), http.StatusOK,
		`"status":1`, `"warn-chmod"`, `"approvalReason":""`)

	// 须审批而未配置审批流程：拒绝，并在检查里说明。
	expectStatus(t, "approval unavailable", ts, http.MethodPost, submit, admin, f.adhocBody(2, `{"command":"reboot"}`, ""), http.StatusConflict, "JOB_APPROVAL_UNAVAILABLE")
	expectStatus(t, "check says not ready", ts, http.MethodPost, "/api/v1/job/adhoc:check", admin,
		`{"kind":2,"adhoc":{"command":"reboot"},"target":`+jobTargetAllProd+`}`, http.StatusOK, `"approvalReady":false`, `命中高危命令规则（approve-reboot）`)
	expectStatus(t, "unknown workflow", ts, http.MethodPut, "/api/v1/job/policy", admin,
		`{"policy":{"approvalWorkflowKey":"nope","version":1}}`, http.StatusBadRequest, "JOB_POLICY_INVALID")

	// 配置审批流程后：进入待审批，流程变量带上原因与主机数。
	f.enableApproval(t, "")
	pending := executionID(t, expectStatus(t, "pending", ts, http.MethodPost, submit, admin, f.adhocBody(2, `{"command":"reboot"}`, `,"requestId":"r-1"`), http.StatusOK,
		`"status":9`, `"approvalInstanceId":"018f3e1a-0e0e-7000-8000-000000000001"`, `命中高危命令规则（approve-reboot）`))
	start, _ := f.svc.job.approvals.Last()
	if start.BusinessID != pending || start.DefinitionKey != "job-approval" || start.Variables["host_count"] != 2 || start.Variables["kind"] != jc.KindCommand {
		t.Fatalf("approval start = %+v", start)
	}
	// 同一幂等键重复提交：不再发起第二个审批。
	expectStatus(t, "idempotent", ts, http.MethodPost, submit, admin, f.adhocBody(2, `{"command":"reboot"}`, `,"requestId":"r-1"`), http.StatusOK, `"id":"`+pending+`"`)
	if n := len(f.svc.job.approvals.Started); n != 1 {
		t.Fatalf("idempotent submit started %d approvals", n)
	}
	// 审批通过 → 排队。
	if err := f.svc.job.syncer.Resolve(context.Background(), testfake.Tx{}, pending, true, ""); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, "approved", ts, http.MethodGet, "/api/v1/job/execution/"+pending, admin, "", http.StatusOK, `"status":1`)

	// 审批驳回 → 审批未通过，不可再取消。
	rejected := executionID(t, expectStatus(t, "pending 2", ts, http.MethodPost, submit, admin, f.adhocBody(2, `{"command":"reboot now"}`, ""), http.StatusOK, `"status":9`))
	if err := f.svc.job.syncer.Resolve(context.Background(), testfake.Tx{}, rejected, false, "审批被驳回"); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, "rejected", ts, http.MethodGet, "/api/v1/job/execution/"+rejected, admin, "", http.StatusOK, `"status":10`, `"error":"审批被驳回"`, `"finishTime"`)
	expectStatus(t, "cancel rejected", ts, http.MethodPost, "/api/v1/job/execution/"+rejected+":cancel", admin, "{}", http.StatusConflict, "JOB_EXECUTION_FINISHED")

	// 撤回待审批的执行：取消执行并撤回审批流程；之后到达的审批结果不再改变它。
	withdrawn := executionID(t, expectStatus(t, "pending 3", ts, http.MethodPost, submit, admin, f.adhocBody(2, `{"command":"sudo reboot"}`, ""), http.StatusOK, `"status":9`))
	expectStatus(t, "withdraw", ts, http.MethodPost, "/api/v1/job/execution/"+withdrawn+":cancel", admin, "{}", http.StatusOK, `"status":6`)
	if n := len(f.svc.job.approvals.Cancelled); n != 1 {
		t.Fatalf("withdraw cancelled %d approval instances", n)
	}
	if err := f.svc.job.syncer.Resolve(context.Background(), testfake.Tx{}, withdrawn, true, ""); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, "late approval ignored", ts, http.MethodGet, "/api/v1/job/execution/"+withdrawn, admin, "", http.StatusOK, `"status":6`)
}

// 全局策略与模板的审批：受保护环境、模板须审批、预检免审批、快速执行一律审批、主机数上限、互斥位含待审批。
func TestJob_PolicyApprovalAndLimits(t *testing.T) {
	t.Parallel()
	f := newJobFixture(t)
	ts, admin := f.ts, f.admin
	f.enableApproval(t, `,"protectedEnvs":["prod"],"maxHosts":2`)

	tplID := monitorID(t, ts, http.MethodPost, "/api/v1/job/template", admin, f.templateBody("deploy", jobFilterProd, `,"exclusive":true`), "template")
	expectStatus(t, "hosts show approval", ts, http.MethodGet, "/api/v1/job/template/"+tplID+"/host", admin, "", http.StatusOK,
		`目标主机含受保护环境（prod）`, `"approvalReady":true`, `"maxHosts":2`)
	// 预检不审批；实际执行进待审批并占住互斥位。
	check := executionID(t, expectStatus(t, "check mode", ts, http.MethodPost, "/api/v1/job/execution", admin, `{"templateId":"`+tplID+`","check":true}`, http.StatusOK, `"status":1`))
	expectStatus(t, "cancel check", ts, http.MethodPost, "/api/v1/job/execution/"+check+":cancel", admin, "{}", http.StatusOK)
	pending := executionID(t, expectStatus(t, "real run", ts, http.MethodPost, "/api/v1/job/execution", admin, `{"templateId":"`+tplID+`"}`, http.StatusOK,
		`"status":9`, `目标主机含受保护环境（prod）`))
	expectStatus(t, "slot held while pending", ts, http.MethodPost, "/api/v1/job/execution", admin, `{"templateId":"`+tplID+`","check":true}`, http.StatusConflict, "JOB_EXECUTION_CONFLICT")
	expectStatus(t, "pending filter", ts, http.MethodGet, "/api/v1/job/execution?page=1&pageSize=10&statuses=9", admin, "", http.StatusOK, `"total":"1"`, pending)

	// test 环境不受保护：模板须审批开关单独生效。
	testOnly := `{"modelCode":"host","mode":2,"env":"test","addressField":"ip"}`
	plain := monitorID(t, ts, http.MethodPost, "/api/v1/job/template", admin, f.templateBody("plain", testOnly, ""), "template")
	expectStatus(t, "no approval", ts, http.MethodPost, "/api/v1/job/execution", admin, `{"templateId":"`+plain+`"}`, http.StatusOK, `"status":1`, `"approvalReason":""`)
	strict := monitorID(t, ts, http.MethodPost, "/api/v1/job/template", admin, f.templateBody("strict", testOnly, `,"requireApproval":true`), "template")
	expectStatus(t, "template requires", ts, http.MethodPost, "/api/v1/job/execution", admin, `{"templateId":"`+strict+`"}`, http.StatusOK, `"status":9`, `作业模板要求审批`)

	// 主机数上限 2：第三台 prod 主机让目标超限。
	createCI(t, ts, admin, `{"modelCode":"host","name":"web-3","env":"prod","attrs":{"ip":"10.0.0.3"}}`)
	expectStatus(t, "over the limit", ts, http.MethodPost, "/api/v1/job/adhoc", admin, f.adhocBody(2, `{"command":"uptime"}`, ""), http.StatusBadRequest, "JOB_TARGET_INVALID", "more than 2")

	// 快速执行一律审批。
	expectStatus(t, "adhoc approval on", ts, http.MethodPut, "/api/v1/job/policy", admin,
		`{"policy":{"approvalWorkflowKey":"job-approval","adhocApproval":true,"version":2}}`, http.StatusOK, `"adhocApproval":true`, `"version":3`)
	expectStatus(t, "stale policy version", ts, http.MethodPut, "/api/v1/job/policy", admin,
		`{"policy":{"approvalWorkflowKey":"job-approval","version":2}}`, http.StatusConflict, "JOB_VERSION_CONFLICT")
	expectStatus(t, "adhoc needs approval", ts, http.MethodPost, "/api/v1/job/adhoc", admin,
		`{"kind":2,"adhoc":{"command":"uptime"},"target":`+testOnly+`,"credentialId":"`+f.credentialID+`"}`, http.StatusOK, `"status":9`, `快速执行须审批`)
}

// 作业切片：一次提交拆成多条执行，共用分组；审批对整组；切片与禁止并发互斥。
func TestJob_Slicing(t *testing.T) {
	t.Parallel()
	f := newJobFixture(t)
	ts, admin := f.ts, f.admin
	for i := 3; i <= 5; i++ {
		n := string(rune('0' + i))
		createCI(t, ts, admin, `{"modelCode":"host","name":"web-`+n+`","env":"prod","attrs":{"ip":"10.0.0.`+n+`"}}`)
	}
	expectStatus(t, "slice + exclusive", ts, http.MethodPost, "/api/v1/job/template", admin,
		f.templateBody("bad", jobFilterProd, `,"sliceSize":2,"exclusive":true`), http.StatusBadRequest, "JOB_TEMPLATE_INVALID")
	tplID := monitorID(t, ts, http.MethodPost, "/api/v1/job/template", admin, f.templateBody("sliced", jobFilterProd, `,"sliceSize":2,"requireApproval":true`), "template")
	f.enableApproval(t, "")

	body := expectStatus(t, "submit", ts, http.MethodPost, "/api/v1/job/execution", admin, `{"templateId":"`+tplID+`"}`, http.StatusOK,
		`"status":9`, `"sliceNo":1`, `"sliceTotal":3`, `"hostTotal":2`)
	var first struct {
		Execution struct {
			ID      string `json:"id"`
			GroupID string `json:"groupId"`
		}
	}
	if err := json.Unmarshal([]byte(body), &first); err != nil || first.Execution.GroupID == "" {
		t.Fatalf("no group id: %s", body)
	}
	group := first.Execution.GroupID
	list := expectStatus(t, "group", ts, http.MethodGet, "/api/v1/job/execution?page=1&pageSize=10&groupId="+group, admin, "", http.StatusOK, `"total":"3"`, `"sliceNo":3`, `"hostTotal":1`)
	if strings.Count(list, `"status":9`) != 3 {
		t.Fatalf("all slices must be pending: %s", list)
	}
	// 一组只发起一个审批，业务对象是分组；通过后整组排队。
	start, _ := f.svc.job.approvals.Last()
	if len(f.svc.job.approvals.Started) != 1 || start.BusinessID != group || start.Variables["host_count"] != 5 {
		t.Fatalf("approval start = %+v (count %d)", start, len(f.svc.job.approvals.Started))
	}
	if err := f.svc.job.syncer.Resolve(context.Background(), testfake.Tx{}, group, true, ""); err != nil {
		t.Fatal(err)
	}
	list = expectStatus(t, "approved group", ts, http.MethodGet, "/api/v1/job/execution?page=1&pageSize=10&groupId="+group, admin, "", http.StatusOK)
	if strings.Count(list, `"status":1`) != 3 {
		t.Fatalf("all slices must be queued after approval: %s", list)
	}

	// 撤回待审批的一片 = 整组撤回。
	body = expectStatus(t, "submit 2", ts, http.MethodPost, "/api/v1/job/execution", admin, `{"templateId":"`+tplID+`"}`, http.StatusOK, `"status":9`)
	if err := json.Unmarshal([]byte(body), &first); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, "withdraw group", ts, http.MethodPost, "/api/v1/job/execution/"+first.Execution.ID+":cancel", admin, "{}", http.StatusOK, `"status":6`)
	list = expectStatus(t, "withdrawn group", ts, http.MethodGet, "/api/v1/job/execution?page=1&pageSize=10&groupId="+first.Execution.GroupID, admin, "", http.StatusOK)
	if strings.Count(list, `"status":6`) != 3 {
		t.Fatalf("withdrawing one slice must cancel the whole group: %s", list)
	}
}
