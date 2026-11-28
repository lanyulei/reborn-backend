package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	topbiz "github.com/lanyulei/reborn/backend/core/internal/biz"
	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	bizjob "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	jobservice "github.com/lanyulei/reborn/backend/core/internal/service/job"
	service "github.com/lanyulei/reborn/backend/core/internal/service/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"

	"golang.org/x/crypto/ssh"
)

// jobServices 作业中心域的 e2e 装配产物：五个 service 与可模拟执行引擎的内存仓储。
type jobServices struct {
	credential *jobservice.CredentialService
	project    *jobservice.ProjectService
	template   *jobservice.TemplateService
	execution  *jobservice.ExecutionService
	runner     *jobservice.RunnerService
	script     *jobservice.ScriptService
	policy     *jobservice.PolicyService
	store      *testfake.JobStore
	approvals  *testfake.JobApprovals
	files      *testfake.JobFiles
	syncer     *bizjob.ApprovalSyncer
}

// newJobServices 组装作业中心域：内存仓储 + 真实的 CMDB 适配器（topbiz.NewJobHostSource）。
// reviewedEvidence 作业审批证据桩：没有任何审批环节被跳过——流程走完即算审批通过（与真实仓储在「没人被跳过」时的结果一致）。
type reviewedEvidence struct{}

// ApprovalEvidenceTx implements bizjob.ApprovalEvidenceReader。
func (reviewedEvidence) ApprovalEvidenceTx(context.Context, bizjob.Tx, string, string) (bizjob.ApprovalEvidence, error) {
	return bizjob.ApprovalEvidence{}, nil
}

func newJobServices(store *testfake.Store, scopes *bizcmdb.ScopeUsecase, models *bizcmdb.ModelUsecase, cis *bizcmdb.CIUsecase) jobServices {
	js := testfake.NewJobStore()
	users := monitorUsers{store: store}
	sealer := &testfake.JobSealer{}
	source := topbiz.NewJobHostSource(scopes, models, cis)
	targets := bizjob.NewTargetUsecase(source)
	approvals := &testfake.JobApprovals{Keys: map[string]string{}}
	files := &testfake.JobFiles{}
	execUC := bizjob.NewExecutionUsecase(
		js.Executions(), js.Executions(), js.Executions(), js.Templates(), js.Projects(), js.Credentials(), js.Runners(), targets, users,
		bizjob.NewExecutionDeps(testfake.NewTxManager(), js.Policies(), js.RiskRules(), js.Scripts(), files, approvals, testfake.JobSources{}, nil, testfake.JobRuns{}))
	return jobServices{
		script:    jobservice.NewScriptService(bizjob.NewScriptUsecase(js.Scripts(), users)),
		policy:    jobservice.NewPolicyService(bizjob.NewPolicyUsecase(js.RiskRules(), js.Policies(), approvals, users)),
		approvals: approvals, files: files, syncer: bizjob.NewApprovalSyncer(js.Executions(), reviewedEvidence{}),
		credential: jobservice.NewCredentialService(bizjob.NewCredentialUsecase(js.Credentials(), js.Credentials(), sealer, users)),
		project:    jobservice.NewProjectService(bizjob.NewProjectUsecase(js.Projects(), js.Projects(), sealer, users)),
		template:   jobservice.NewTemplateService(bizjob.NewTemplateUsecase(js.Templates(), js.Templates(), targets, source, users), execUC),
		execution:  jobservice.NewExecutionService(execUC),
		runner:     jobservice.NewRunnerService(bizjob.NewRunnerUsecase(js.Runners(), js.Runners(), js.Runners())),
		store:      js,
	}
}

// jobOps 作业中心域全部可授权接口（proto full method）。
func jobOps() []string {
	var ops []string
	for _, m := range service.AuthorizableMethods() {
		if strings.HasPrefix(m.Name, "/job.v1.") {
			ops = append(ops, m.Name)
		}
	}
	return ops
}

// jobTestKey 每次测试进程现生成的 ed25519 私钥（OpenSSH 格式，不带口令；不对应任何真实主机）。
var jobTestKey = func() string {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "job-test")
	if err != nil {
		panic(err)
	}
	return string(pem.EncodeToMemory(block))
}()

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// jobFixture 搭好 CMDB（host 类型，两台 prod、一台 test）与一套凭据 / 项目。
type jobFixture struct {
	ts                      *httptest.Server
	svc                     testServices
	admin                   string
	prod1, prod2, testHost  ciJSON
	credentialID, projectID string
}

func newJobFixture(t *testing.T) jobFixture {
	t.Helper()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, admin)
	f := jobFixture{ts: ts, svc: svc, admin: admin}
	f.prod1 = createCI(t, ts, admin, `{"modelCode":"host","name":"web-1","env":"prod","attrs":{"ip":"10.0.0.1"}}`)
	f.prod2 = createCI(t, ts, admin, `{"modelCode":"host","name":"web-2","env":"prod","attrs":{"ip":"10.0.0.2"}}`)
	f.testHost = createCI(t, ts, admin, `{"modelCode":"host","name":"web-t","env":"test","attrs":{"ip":"10.0.9.1"}}`)
	f.credentialID = monitorID(t, ts, http.MethodPost, "/api/v1/job/credential", admin,
		`{"spec":{"name":"deploy-key","type":1,"username":"deploy","secret":`+jsonString(jobTestKey)+`,"becomeMethod":"sudo","becomeSecret":"sudo-pass-123"}}`, "credential")
	f.projectID = monitorID(t, ts, http.MethodPost, "/api/v1/job/project", admin,
		`{"spec":{"name":"ops-playbooks","repoUrl":"https://git.example.com/ops/playbooks.git","ref":"main","authType":2,"username":"ci","secret":"glpat-token-123456"}}`, "project")
	return f
}

func (f jobFixture) templateBody(name, target, extra string) string {
	return `{"spec":{"name":"` + name + `","projectId":"` + f.projectID + `","playbook":"deploy/site.yml","credentialId":"` + f.credentialID +
		`","target":` + target + `,"enabled":true` + extra + `}}`
}

const jobFilterProd = `{"modelCode":"host","mode":2,"env":"prod","addressField":"ip"}`

// 认证与接口授权：未登录 401；没有作业中心接口权限 403。
func TestJob_AuthAndAuthz(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	plain := testBearer(t, svc.signer, createUser(t, ts, admin, "job_plain").Id)
	for _, path := range []string{"/credential", "/project", "/template", "/execution", "/runner"} {
		url := "/api/v1/job" + path + "?page=1&pageSize=10"
		expectStatus(t, "anonymous "+path, ts, http.MethodGet, url, "", "", http.StatusUnauthorized)
		expectStatus(t, "no permission "+path, ts, http.MethodGet, url, plain, "", http.StatusForbidden)
		expectStatus(t, "admin "+path, ts, http.MethodGet, url, admin, "", http.StatusOK)
	}
	expectStatus(t, "submit without permission", ts, http.MethodPost, "/api/v1/job/execution", plain,
		`{"templateId":"018f3e1a-0000-7000-8000-000000000001"}`, http.StatusForbidden)
}

// 凭据：密文只写不读（任何响应都不含私钥 / 密码）、落库的是密文、校验、保留与清除语义、乐观锁、引用保护。
func TestJob_CredentialSecretsAreWriteOnly(t *testing.T) {
	t.Parallel()
	f := newJobFixture(t)
	ts, admin := f.ts, f.admin
	const base = "/api/v1/job/credential"

	leaks := func(what, body string) {
		t.Helper()
		for _, s := range []string{"OPENSSH", "sudo-pass-123", "sealed:", "hunter2-password", `"secret"`, `"becomeSecret"`} {
			if strings.Contains(body, s) {
				t.Fatalf("%s leaks %q: %s", what, s, body)
			}
		}
	}
	body := expectStatus(t, "get", ts, http.MethodGet, base+"/"+f.credentialID, admin, "", http.StatusOK,
		`"hasBecomeSecret":true`, `"fingerprint":"SHA256:`, `"username":"deploy"`)
	leaks("get", body)
	leaks("list", expectStatus(t, "list", ts, http.MethodGet, base+"?page=1&pageSize=10", admin, "", http.StatusOK, `"total":"1"`))
	leaks("options", expectStatus(t, "options", ts, http.MethodGet, "/api/v1/job/credential-option", admin, "", http.StatusOK, "deploy-key"))

	secret, become := f.svc.job.store.CredentialSecret(f.credentialID)
	if !strings.HasPrefix(string(secret), "sealed:-----BEGIN OPENSSH") || string(become) != "sealed:sudo-pass-123" {
		t.Fatalf("stored secrets are not the sealed inputs: %q / %q", secret, become)
	}

	// 校验：不是私钥、缺密文、重名、用户名以连字符开头。
	expectStatus(t, "bad key", ts, http.MethodPost, base, admin,
		`{"spec":{"name":"bad","type":1,"username":"u","secret":"not a key"}}`, http.StatusBadRequest, "JOB_CREDENTIAL_INVALID")
	expectStatus(t, "missing secret", ts, http.MethodPost, base, admin,
		`{"spec":{"name":"bad","type":2,"username":"u"}}`, http.StatusBadRequest, "JOB_CREDENTIAL_INVALID")
	expectStatus(t, "duplicate name", ts, http.MethodPost, base, admin,
		`{"spec":{"name":"deploy-key","type":2,"username":"u","secret":"hunter2-password"}}`, http.StatusConflict, "JOB_CREDENTIAL_NAME_EXISTS")
	expectStatus(t, "option-like username", ts, http.MethodPost, base, admin,
		`{"spec":{"name":"bad","type":2,"username":"-oProxyCommand","secret":"x"}}`, http.StatusBadRequest)

	// 编辑不传密文：两个密文都保留。
	leaks("update keep", expectStatus(t, "update keep", ts, http.MethodPut, base+"/"+f.credentialID, admin,
		`{"spec":{"name":"deploy-key","type":1,"username":"deploy2","becomeMethod":"sudo"},"version":1}`, http.StatusOK,
		`"username":"deploy2"`, `"hasBecomeSecret":true`, `"version":2`))
	if s, b := f.svc.job.store.CredentialSecret(f.credentialID); string(s) != string(secret) || string(b) != string(become) {
		t.Fatal("omitted secrets must be kept")
	}
	// 旧版本号：冲突。
	expectStatus(t, "stale version", ts, http.MethodPut, base+"/"+f.credentialID, admin,
		`{"spec":{"name":"deploy-key","type":1,"username":"deploy"},"version":1}`, http.StatusConflict, "JOB_VERSION_CONFLICT")
	// 换类型必须重新填写密文。
	expectStatus(t, "type change needs secret", ts, http.MethodPut, base+"/"+f.credentialID, admin,
		`{"spec":{"name":"deploy-key","type":2,"username":"deploy","becomeMethod":"sudo"},"version":2}`, http.StatusBadRequest, "JOB_CREDENTIAL_INVALID")
	// 提权密码传空串 = 清除。
	expectStatus(t, "clear become", ts, http.MethodPut, base+"/"+f.credentialID, admin,
		`{"spec":{"name":"deploy-key","type":1,"username":"deploy","becomeMethod":"sudo","becomeSecret":""},"version":2}`, http.StatusOK,
		`"hasBecomeSecret":false`)
	if _, b := f.svc.job.store.CredentialSecret(f.credentialID); len(b) != 0 {
		t.Fatalf("become secret not cleared: %q", b)
	}

	// 被模板引用时不可删除；删除模板后可删。
	tplID := monitorID(t, ts, http.MethodPost, "/api/v1/job/template", admin, f.templateBody("deploy", jobFilterProd, ""), "template")
	expectStatus(t, "delete in use", ts, http.MethodDelete, base+"/"+f.credentialID, admin, "", http.StatusConflict, "JOB_CREDENTIAL_IN_USE", "deploy")
	expectStatus(t, "project in use", ts, http.MethodDelete, "/api/v1/job/project/"+f.projectID, admin, "", http.StatusConflict, "JOB_PROJECT_IN_USE")
	expectStatus(t, "delete template", ts, http.MethodDelete, "/api/v1/job/template/"+tplID, admin, "", http.StatusOK)
	expectStatus(t, "delete", ts, http.MethodDelete, base+"/"+f.credentialID, admin, "", http.StatusOK)
	expectStatus(t, "gone", ts, http.MethodGet, base+"/"+f.credentialID, admin, "", http.StatusNotFound, "JOB_CREDENTIAL_NOT_FOUND")
}

// 项目：仓库地址只接受 http(s) / ssh，认证方式须与地址匹配，令牌不回显。
func TestJob_ProjectValidation(t *testing.T) {
	t.Parallel()
	f := newJobFixture(t)
	ts, admin := f.ts, f.admin
	const base = "/api/v1/job/project"
	body := expectStatus(t, "get", ts, http.MethodGet, base+"/"+f.projectID, admin, "", http.StatusOK, `"hasSecret":true`, `"username":"ci"`)
	if strings.Contains(body, "glpat") || strings.Contains(body, `"secret"`) {
		t.Fatalf("project response leaks the token: %s", body)
	}
	//nolint:gosec // G101 误报：测试请求体里的示例地址与占位令牌
	bad := map[string]string{
		"local path":           `{"spec":{"name":"p","repoUrl":"/srv/repo","ref":"main","authType":1}}`,
		"file scheme":          `{"spec":{"name":"p","repoUrl":"file:///srv/repo","ref":"main","authType":1}}`,
		"transport helper":     `{"spec":{"name":"p","repoUrl":"ext::sh -c id","ref":"main","authType":1}}`,
		"embedded credentials": `{"spec":{"name":"p","repoUrl":"https://u:p@git.example.com/x.git","ref":"main","authType":1}}`,
		"option-like ref":      `{"spec":{"name":"p","repoUrl":"https://git.example.com/x.git","ref":"--upload-pack=id","authType":1}}`,
		"token on ssh url":     `{"spec":{"name":"p","repoUrl":"git@git.example.com:ops/x.git","ref":"main","authType":2,"secret":"tok"}}`,
		"key on https url":     `{"spec":{"name":"p","repoUrl":"https://git.example.com/x.git","ref":"main","authType":3,"secret":` + jsonString(jobTestKey) + `}}`,
		"token missing":        `{"spec":{"name":"p","repoUrl":"https://git.example.com/x.git","ref":"main","authType":2}}`,
	}
	for name, req := range bad {
		expectStatus(t, name, ts, http.MethodPost, base, admin, req, http.StatusBadRequest, "JOB_PROJECT_INVALID")
	}
	// 只改仓库地址、不重填令牌：拒绝（否则已保存的令牌会被发往新地址）。
	expectStatus(t, "url change keeps token", ts, http.MethodPut, base+"/"+f.projectID, admin,
		`{"spec":{"name":"ops-playbooks","repoUrl":"https://evil.example.com/x.git","ref":"main","authType":2,"username":"ci"},"version":1}`,
		http.StatusBadRequest, "JOB_PROJECT_INVALID", "again")
	// 认证方式改为「无」：清除已保存的令牌。
	expectStatus(t, "auth none", ts, http.MethodPut, base+"/"+f.projectID, admin,
		`{"spec":{"name":"ops-playbooks","repoUrl":"https://git.example.com/ops/playbooks.git","ref":"v1.2.0","authType":1},"version":1}`,
		http.StatusOK, `"hasSecret":false`, `"ref":"v1.2.0"`)
	// 再改回令牌认证而不给令牌：拒绝（没有可沿用的）。
	expectStatus(t, "token again without secret", ts, http.MethodPut, base+"/"+f.projectID, admin,
		`{"spec":{"name":"ops-playbooks","repoUrl":"https://git.example.com/ops/playbooks.git","ref":"main","authType":2},"version":2}`,
		http.StatusBadRequest, "JOB_PROJECT_INVALID")
}

// 模板校验：Playbook 路径、参数定义、目标主机的 CMDB 侧校验、预览。
func TestJob_TemplateValidation(t *testing.T) {
	t.Parallel()
	f := newJobFixture(t)
	ts, admin := f.ts, f.admin
	const base = "/api/v1/job/template"
	bad := map[string]string{
		"playbook traversal": strings.Replace(f.templateBody("t", jobFilterProd, ""), "deploy/site.yml", "../../etc/x.yml", 1),
		"playbook absolute":  strings.Replace(f.templateBody("t", jobFilterProd, ""), "deploy/site.yml", "/etc/x.yml", 1),
		"reserved var":       f.templateBody("t", jobFilterProd, `,"params":[{"code":"ansible_user","name":"u","type":"string"}]`),
		"cmdb var":           f.templateBody("t", jobFilterProd, `,"params":[{"code":"cmdb_id","name":"u","type":"string"}]`),
		"duplicate param":    f.templateBody("t", jobFilterProd, `,"params":[{"code":"v","name":"a","type":"string"},{"code":"v","name":"b","type":"int"}]`),
		"enum no options":    f.templateBody("t", jobFilterProd, `,"params":[{"code":"v","name":"a","type":"enum"}]`),
		"bad default":        f.templateBody("t", jobFilterProd, `,"params":[{"code":"v","name":"a","type":"int","default":"abc"}]`),
		"default not option": f.templateBody("t", jobFilterProd, `,"params":[{"code":"v","name":"a","type":"enum","options":["a","b"],"default":"c"}]`),
		"timeout too small":  f.templateBody("t", jobFilterProd, `,"options":{"timeoutS":5}`),
	}
	for name, req := range bad {
		expectStatus(t, name, ts, http.MethodPost, base, admin, req, http.StatusBadRequest, "JOB_TEMPLATE_INVALID")
	}
	badTarget := map[string]string{
		"unknown model":         `{"modelCode":"nope","mode":2}`,
		"unknown address field": `{"modelCode":"host","mode":2,"addressField":"nope"}`,
		"non-text address":      `{"modelCode":"host","mode":2,"addressField":"cores"}`,
		"no ci selected":        `{"modelCode":"host","mode":1}`,
	}
	for name, target := range badTarget {
		expectStatus(t, name, ts, http.MethodPost, base, admin, f.templateBody("t", target, ""), http.StatusBadRequest, "JOB_TARGET_INVALID")
	}
	missingRef := strings.Replace(f.templateBody("t", jobFilterProd, ""), f.projectID, "018f3e1a-0000-7000-8000-00000000dead", 1)
	expectStatus(t, "missing project", ts, http.MethodPost, base, admin, missingRef, http.StatusBadRequest, "JOB_TEMPLATE_INVALID")

	// 缺省值补全后返回；名称唯一。
	body := expectStatus(t, "create", ts, http.MethodPost, base, admin, f.templateBody("deploy", jobFilterProd, ""), http.StatusOK,
		`"forks":10`, `"timeoutS":3600`, `"port":22`, `"projectName":"ops-playbooks"`, `"credentialName":"deploy-key"`, `"modelName":"主机"`)
	if strings.Contains(body, "glpat") || strings.Contains(body, "OPENSSH") {
		t.Fatalf("template response leaks a secret: %s", body)
	}
	expectStatus(t, "duplicate", ts, http.MethodPost, base, admin, f.templateBody("deploy", jobFilterProd, ""), http.StatusConflict, "JOB_TEMPLATE_NAME_EXISTS")

	// 预览：按 ip 字段取地址；用资源名称作地址时同样可用；按环境筛选。
	expectStatus(t, "preview", ts, http.MethodPost, "/api/v1/job/target:preview", admin, `{"target":`+jobFilterProd+`}`, http.StatusOK,
		`"address":"10.0.0.1"`, `"address":"10.0.0.2"`)
	body = expectStatus(t, "preview by name", ts, http.MethodPost, "/api/v1/job/target:preview", admin,
		`{"target":{"modelCode":"host","mode":2,"env":"test"}}`, http.StatusOK, `"address":"web-t"`)
	if strings.Contains(body, "web-1") {
		t.Fatalf("env filter not applied: %s", body)
	}
}

// 数据范围：受限用户不能把模板指向范围外的类型 / 环境，执行时范围外的主机不会进入执行，
// 点名范围外的主机整体拒绝。
func TestJob_TargetFollowsCmdbScope(t *testing.T) {
	t.Parallel()
	f := newJobFixture(t)
	ts, admin := f.ts, f.admin
	sw := createCI(t, ts, admin, `{"modelCode":"switch","name":"sw-1","env":"prod"}`)

	// 受限用户：有全部作业中心接口权限，CMDB 范围 = host 类型、仅 prod 环境。
	user, _, roleID := monitorUser(t, ts, f.svc, admin, "job_scoped", jobOps())
	expectStatus(t, "set cmdb scope", ts, http.MethodPut, "/api/v1/cmdb/scope/role/"+roleID, admin,
		`{"modelCodes":["host"],"envs":["prod"]}`, http.StatusOK)

	// 范围外的类型 / 环境：保存与预览都是 403。
	expectStatus(t, "template on foreign model", ts, http.MethodPost, "/api/v1/job/template", user,
		f.templateBody("sw", `{"modelCode":"switch","mode":1,"ciIds":["`+sw.ID+`"]}`, ""), http.StatusForbidden, "JOB_TARGET_FORBIDDEN")
	expectStatus(t, "preview foreign model", ts, http.MethodPost, "/api/v1/job/target:preview", user,
		`{"target":{"modelCode":"switch","mode":2}}`, http.StatusForbidden, "JOB_TARGET_FORBIDDEN")
	expectStatus(t, "preview foreign env", ts, http.MethodPost, "/api/v1/job/target:preview", user,
		`{"target":{"modelCode":"host","mode":2,"env":"test","addressField":"ip"}}`, http.StatusForbidden, "JOB_TARGET_FORBIDDEN")

	// 管理员建的模板点名了三台主机（含 test 环境的一台）。
	all := `{"modelCode":"host","mode":1,"ciIds":["` + f.prod1.ID + `","` + f.prod2.ID + `","` + f.testHost.ID + `"],"addressField":"ip"}`
	tplID := monitorID(t, ts, http.MethodPost, "/api/v1/job/template", admin, f.templateBody("all-hosts", all, ""), "template")

	// 受限用户看到的目标主机：只有两台 prod，另一台计入 outOfScope。
	body := expectStatus(t, "hosts for scoped user", ts, http.MethodGet, "/api/v1/job/template/"+tplID+"/host", user, "", http.StatusOK,
		`"outOfScope":1`, `"10.0.0.1"`, `"10.0.0.2"`)
	if strings.Contains(body, "10.0.9.1") || strings.Contains(body, "web-t") {
		t.Fatalf("out-of-scope host leaked: %s", body)
	}
	expectStatus(t, "hosts for admin", ts, http.MethodGet, "/api/v1/job/template/"+tplID+"/host", admin, "", http.StatusOK, `"10.0.9.1"`)

	// 受限用户执行：只在范围内的两台上执行。
	execID := monitorID(t, ts, http.MethodPost, "/api/v1/job/execution", user, `{"templateId":"`+tplID+`"}`, "execution")
	body = expectStatus(t, "execution hosts", ts, http.MethodGet, "/api/v1/job/execution/"+execID+"/host?page=1&pageSize=50", admin, "", http.StatusOK, `"total":"2"`)
	if strings.Contains(body, "10.0.9.1") {
		t.Fatalf("out-of-scope host entered the execution: %s", body)
	}
	// 点名范围外的主机：整体拒绝，不悄悄缩小。
	expectStatus(t, "explicit out-of-scope host", ts, http.MethodPost, "/api/v1/job/execution", user,
		`{"templateId":"`+tplID+`","ciIds":["`+f.prod1.ID+`","`+f.testHost.ID+`"]}`, http.StatusBadRequest, "JOB_TARGET_INVALID")
	// 点名模板之外的主机同样拒绝（即使在数据范围内）。
	extra := createCI(t, ts, admin, `{"modelCode":"host","name":"web-3","env":"prod","attrs":{"ip":"10.0.0.3"}}`)
	expectStatus(t, "host outside template", ts, http.MethodPost, "/api/v1/job/execution", admin,
		`{"templateId":"`+tplID+`","ciIds":["`+extra.ID+`"]}`, http.StatusBadRequest, "JOB_TARGET_INVALID")
}

// 提交执行：固化执行内容、参数校验与缺省值、幂等键、模板互斥、停用模板、取消、日志增量读取、主机结果。
func TestJob_ExecutionLifecycle(t *testing.T) {
	t.Parallel()
	f := newJobFixture(t)
	ts, admin, store := f.ts, f.admin, f.svc.job.store
	const base = "/api/v1/job/execution"
	params := `,"exclusive":true,"params":[` +
		`{"code":"version","name":"版本","type":"string","required":true},` +
		`{"code":"replicas","name":"副本数","type":"int","default":"2"},` +
		`{"code":"mode","name":"模式","type":"enum","options":["rolling","all"],"default":"rolling"},` +
		`{"code":"force","name":"强制","type":"bool"}]`
	tplID := monitorID(t, ts, http.MethodPost, "/api/v1/job/template", admin, f.templateBody("deploy", jobFilterProd, params), "template")

	// 参数校验。
	badParams := map[string]string{
		"missing required": `{}`,
		"wrong type":       `{"version":"1.0","replicas":"many"}`,
		"not an option":    `{"version":"1.0","mode":"yolo"}`,
		"undefined key":    `{"version":"1.0","ansible_user":"root"}`,
		"injection key":    `{"version":"1.0","ansible_ssh_common_args":"-oProxyCommand=id"}`,
	}
	for name, p := range badParams {
		expectStatus(t, name, ts, http.MethodPost, base, admin, `{"templateId":"`+tplID+`","params":`+p+`}`, http.StatusBadRequest, "JOB_PARAMS_INVALID")
	}

	// 提交：缺省值补全，执行内容固化；响应不含任何凭据。
	submit := `{"templateId":"` + tplID + `","params":{"version":"1.4.2","force":true},"requestId":"req-0001"}`
	body := expectStatus(t, "submit", ts, http.MethodPost, base, admin, submit, http.StatusOK,
		`"status":1`, `"hostTotal":2`, `"templateVersion":1`, `"version":"1.4.2"`, `"replicas":2`, `"mode":"rolling"`, `"force":true`,
		`"playbook":"deploy/site.yml"`, `"repoUrl":"https://git.example.com/ops/playbooks.git"`, `"credentialName":"deploy-key"`)
	for _, s := range []string{"glpat", "OPENSSH", "sudo-pass", "sealed:", "leaseToken"} {
		if strings.Contains(body, s) {
			t.Fatalf("execution response leaks %q: %s", s, body)
		}
	}
	var created struct {
		Execution struct {
			ID string `json:"id"`
		}
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil || created.Execution.ID == "" {
		t.Fatalf("submit body: %s", body)
	}
	execID := created.Execution.ID

	// 同一幂等键重复提交：返回同一条，不新建。
	if again := monitorID(t, ts, http.MethodPost, base, admin, submit, "execution"); again != execID {
		t.Fatalf("idempotent submit created a new execution: %s vs %s", again, execID)
	}
	// 模板禁止并发：另一个幂等键被拒。
	expectStatus(t, "exclusive conflict", ts, http.MethodPost, base, admin,
		`{"templateId":"`+tplID+`","params":{"version":"1.4.3"},"requestId":"req-0002"}`, http.StatusConflict, "JOB_EXECUTION_CONFLICT")

	// 提交后改模板：已提交的执行内容不变。
	expectStatus(t, "edit template", ts, http.MethodPut, "/api/v1/job/template/"+tplID, admin,
		strings.Replace(f.templateBody("deploy", jobFilterProd, params), `"enabled":true`, `"enabled":false`, 1)[:len(f.templateBody("deploy", jobFilterProd, params))]+`,"version":1}`,
		http.StatusOK, `"version":2`, `"enabled":false`)
	expectStatus(t, "frozen detail", ts, http.MethodGet, base+"/"+execID, admin, "", http.StatusOK, `"templateVersion":1`, `"playbook":"deploy/site.yml"`)

	// 引擎领取、写日志；增量读取。
	runnerBody := expectStatus(t, "create runner", ts, http.MethodPost, "/api/v1/job/runner", admin,
		`{"spec":{"name":"runner-a","zone":"idc-1","enabled":true}}`, http.StatusOK, `"token":"`)
	var rn struct {
		Runner struct {
			ID string `json:"id"`
		}
		Token string
	}
	if err := json.Unmarshal([]byte(runnerBody), &rn); err != nil {
		t.Fatal(err)
	}
	store.EngineClaim(execID, rn.Runner.ID)
	store.EngineLog(execID, "PLAY [all] ***\n", "TASK [ping] ***\n")
	expectStatus(t, "running", ts, http.MethodGet, base+"/"+execID, admin, "", http.StatusOK, `"status":2`, `"runnerName":"runner-a"`)
	expectStatus(t, "log from start", ts, http.MethodGet, base+"/"+execID+"/log?afterSeq=0", admin, "", http.StatusOK,
		`PLAY [all] ***\nTASK [ping] ***\n`, `"lastSeq":"2"`, `"status":2`)
	expectStatus(t, "log no news", ts, http.MethodGet, base+"/"+execID+"/log?afterSeq=2", admin, "", http.StatusOK, `"lastSeq":"2"`)
	store.EngineLog(execID, "ok: [10.0.0.1]\n", "")
	// 截断后的块只推进序号：游标直接推到最大序号，不在缺失的序号上空转。
	expectStatus(t, "log increment", ts, http.MethodGet, base+"/"+execID+"/log?afterSeq=2", admin, "", http.StatusOK,
		`ok: [10.0.0.1]\n`, `"lastSeq":"4"`, `"truncated":true`)

	// 执行中取消：只标记请求，状态仍是执行中；重复取消幂等。
	expectStatus(t, "cancel running", ts, http.MethodPost, base+"/"+execID+":cancel", admin, "{}", http.StatusOK, `"status":2`, `"cancelRequested":true`)
	expectStatus(t, "cancel again", ts, http.MethodPost, base+"/"+execID+":cancel", admin, "{}", http.StatusOK, `"cancelRequested":true`)

	// 引擎结束：一台成功、一台失败 → 部分失败；主机结果可按状态过滤。
	store.EngineFinish(execID, jc.OutcomeExited, 2, []jc.HostResult{
		{Address: "10.0.0.1", OK: 3, Changed: 1}, {Address: "10.0.0.2", OK: 1, Failed: 1},
	})
	expectStatus(t, "partial", ts, http.MethodGet, base+"/"+execID, admin, "", http.StatusOK, `"status":5`, `"hostOk":1`, `"hostFailed":1`)
	body = expectStatus(t, "failed hosts", ts, http.MethodGet, base+"/"+execID+"/host?page=1&pageSize=50&statuses=3&statuses=4", admin, "", http.StatusOK, `"total":"1"`, `"10.0.0.2"`)
	if strings.Contains(body, "10.0.0.1") {
		t.Fatalf("status filter not applied: %s", body)
	}
	expectStatus(t, "cancel finished", ts, http.MethodPost, base+"/"+execID+":cancel", admin, "{}", http.StatusConflict, "JOB_EXECUTION_FINISHED")

	// 模板已停用：不可执行。
	expectStatus(t, "disabled template", ts, http.MethodPost, base, admin,
		`{"templateId":"`+tplID+`","params":{"version":"2.0"}}`, http.StatusConflict, "JOB_TEMPLATE_DISABLED")

	// 列表过滤。
	expectStatus(t, "list by status", ts, http.MethodGet, base+"?page=1&pageSize=10&statuses=5&mine=true", admin, "", http.StatusOK, `"total":"1"`)
	expectStatus(t, "list by ci", ts, http.MethodGet, base+"?page=1&pageSize=10&ciId="+f.testHost.ID, admin, "", http.StatusOK, `"total":"0"`)
	expectStatus(t, "unknown execution", ts, http.MethodGet, base+"/018f3e1a-0000-7000-8000-00000000dead/host?page=1&pageSize=10", admin, "", http.StatusNotFound)
}

// 排队中的执行可直接取消；地址不可用的主机使提交整体被拒。
func TestJob_CancelQueuedAndBadAddress(t *testing.T) {
	t.Parallel()
	f := newJobFixture(t)
	ts, admin := f.ts, f.admin
	tplID := monitorID(t, ts, http.MethodPost, "/api/v1/job/template", admin, f.templateBody("deploy", jobFilterProd, ""), "template")
	execID := monitorID(t, ts, http.MethodPost, "/api/v1/job/execution", admin, `{"templateId":"`+tplID+`","check":true}`, "execution")
	expectStatus(t, "check mode frozen", ts, http.MethodGet, "/api/v1/job/execution/"+execID, admin, "", http.StatusOK, `"check":true`)
	expectStatus(t, "cancel queued", ts, http.MethodPost, "/api/v1/job/execution/"+execID+":cancel", admin, "{}", http.StatusOK,
		`"status":6`, `"cancelRequested":true`, `"finishTime"`)

	// 节点失联（结果未知）：仍可登记取消请求，状态不变；重复请求幂等。
	lost := monitorID(t, ts, http.MethodPost, "/api/v1/job/execution", admin, `{"templateId":"`+tplID+`"}`, "execution")
	f.svc.job.store.EngineClaim(lost, "")
	f.svc.job.store.EngineLose(lost)
	expectStatus(t, "unknown", ts, http.MethodGet, "/api/v1/job/execution/"+lost, admin, "", http.StatusOK, `"status":8`, `"cancelRequested":false`)
	for range 2 {
		expectStatus(t, "cancel unknown", ts, http.MethodPost, "/api/v1/job/execution/"+lost+":cancel", admin, "{}", http.StatusOK,
			`"status":8`, `"cancelRequested":true`)
	}

	// 用资源名称作地址：名称不是合法主机名的那台让提交整体被拒，并在预览里标出原因。
	createCI(t, ts, admin, `{"modelCode":"host","name":"bad host name","env":"prod","attrs":{"ip":"10.0.0.9"}}`)
	byName := `{"modelCode":"host","mode":2,"env":"prod"}`
	expectStatus(t, "preview flags problem", ts, http.MethodPost, "/api/v1/job/target:preview", admin, `{"target":`+byName+`}`, http.StatusOK,
		`"problem":"address_invalid"`)
	byNameID := monitorID(t, ts, http.MethodPost, "/api/v1/job/template", admin, f.templateBody("by-name", byName, ""), "template")
	expectStatus(t, "submit rejected", ts, http.MethodPost, "/api/v1/job/execution", admin, `{"templateId":"`+byNameID+`"}`,
		http.StatusBadRequest, "JOB_TARGET_INVALID", "bad host name")
}

// 执行节点：令牌只返回一次、库里只存摘要、重置后旧令牌失效、区域列表。
func TestJob_RunnerTokens(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	const base = "/api/v1/job/runner"
	body := expectStatus(t, "create", ts, http.MethodPost, base, admin, `{"spec":{"name":"r1","zone":"dmz","enabled":true}}`, http.StatusOK, `"online":false`)
	var created struct {
		Runner struct {
			ID string `json:"id"`
		}
		Token string
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatal(err)
	}
	id, secret, ok := jc.ParseRunnerToken(created.Token)
	if !ok || id != created.Runner.ID || !jc.RunnerSecretMatches(secret, svc.job.store.RunnerTokenHash(id)) {
		t.Fatalf("token %q does not match the stored hash", created.Token)
	}
	// 列表不含令牌。
	if list := expectStatus(t, "list", ts, http.MethodGet, base+"?page=1&pageSize=10", admin, "", http.StatusOK, `"zone":"dmz"`); strings.Contains(list, secret) || strings.Contains(list, "token") {
		t.Fatalf("runner list leaks the token: %s", list)
	}
	body = expectStatus(t, "reset", ts, http.MethodPost, base+"/"+id+":reset-token", admin, "{}", http.StatusOK, `"token":"`)
	if jc.RunnerSecretMatches(secret, svc.job.store.RunnerTokenHash(id)) {
		t.Fatal("old token still valid after reset")
	}
	if strings.Contains(body, secret) {
		t.Fatal("reset returned the old token")
	}
	expectStatus(t, "bad zone", ts, http.MethodPost, base, admin, `{"spec":{"name":"r2","zone":"a b"}}`, http.StatusBadRequest)
	expectStatus(t, "duplicate", ts, http.MethodPost, base, admin, `{"spec":{"name":"r1"}}`, http.StatusConflict, "JOB_RUNNER_NAME_EXISTS")
	expectStatus(t, "zones", ts, http.MethodGet, "/api/v1/job/runner-zone", admin, "", http.StatusOK, `"dmz"`)
	expectStatus(t, "delete", ts, http.MethodDelete, base+"/"+id, admin, "", http.StatusOK)
	expectStatus(t, "reset deleted", ts, http.MethodPost, base+"/"+id+":reset-token", admin, "{}", http.StatusNotFound, "JOB_RUNNER_NOT_FOUND")
}
