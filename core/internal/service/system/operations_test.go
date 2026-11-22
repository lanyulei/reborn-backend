package service

import (
	"cmp"
	"slices"
	"strings"
	"testing"

	cmdbv1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	filev1 "github.com/lanyulei/reborn/backend/core/api/file/v1"
	incidentv1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	monitorv1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	oncallv1 "github.com/lanyulei/reborn/backend/core/api/oncall/v1"
	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	workflowv1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"github.com/go-kratos/kratos/v3/errors"
)

// TestListOperations 锁死操作枚举契约：来自 operationFiles 登记的全部 proto
// （system 各 proto + notify channel/template/route/silence/subscription）的 RPC 扣除免授权接口
// （authzExemptOperations），格式为 proto full method；每个操作必须标注
// title（新增 RPC 漏标注在此失败，防漂移）。
func TestListOperations(t *testing.T) {
	t.Parallel()
	ops := listOperations()

	// 免授权操作不得出现在可授权枚举里（对它们授权无意义）。
	for _, op := range ops {
		if IsAuthzExempt(op.Name) {
			t.Errorf("listOperations() contains authz-exempt operation %q", op.Name)
		}
	}

	// 格式锁死：/package.Service/Method；code 恒为 md5(name) 的 32 位小写 hex。
	for _, op := range ops {
		if len(op.Name) < 4 || op.Name[0] != '/' {
			t.Fatalf("operation %q is not a full method", op.Name)
		}
		svc, method, ok := strings.Cut(op.Name[1:], "/")
		if !ok || svc == "" || method == "" {
			t.Fatalf("operation %q malformed", op.Name)
		}
		// 可授权操作按域分包：新业务域加入 operationFiles 时在此追加前缀，
		// 防止误挂无关 proto。
		if !strings.HasPrefix(svc, "system.v1.") && !strings.HasPrefix(svc, "notify.v1.") &&
			!strings.HasPrefix(svc, "assistant.v1.") && !strings.HasPrefix(svc, "cmdb.v1.") &&
			!strings.HasPrefix(svc, "oncall.v1.") && !strings.HasPrefix(svc, "workflow.v1.") &&
			!strings.HasPrefix(svc, "ticket.v1.") && !strings.HasPrefix(svc, "monitor.v1.") &&
			!strings.HasPrefix(svc, "job.v1.") && !strings.HasPrefix(svc, "k8s.v1.") && !strings.HasPrefix(svc, "knowledge.v1.") && !strings.HasPrefix(svc, "incident.v1.") && !strings.HasPrefix(svc, "agent.v1.") {
			t.Fatalf("operation %q not in an authorized proto package", op.Name)
		}
		if op.Title == "" {
			t.Errorf("operation %q has no title annotation", op.Name)
		}
		if op.Code != biz.OperationCode(op.Name) || len(op.Code) != 32 {
			t.Errorf("operation %q code = %q, want md5(name) 32-hex", op.Name, op.Code)
		}
		// 当前所有可授权 RPC 均暴露 HTTP 路由；新增 RPC 漏标 google.api.http
		// 注解在此失败（与 title 同为防漂移契约）。
		if !strings.HasPrefix(op.Path, "/api/") {
			t.Errorf("operation %q path = %q, want /api/ route", op.Name, op.Path)
		}
		switch op.Method {
		case "GET", "POST", "PUT", "DELETE", "PATCH":
		default:
			t.Errorf("operation %q method = %q, want standard HTTP verb", op.Name, op.Method)
		}
	}

	// 已知操作必须在内（含新增权限接口），且与生成常量逐字一致。
	mustContain := map[string]operation{
		v1.OperationUserServiceCreateUser:                             {Title: "创建用户", Method: "POST", Path: "/api/v1/system/user"},
		v1.OperationUserServiceListUsers:                              {Title: "查询用户列表", Method: "GET", Path: "/api/v1/system/user"},
		v1.OperationUserServiceUpdateUserStatus:                       {Title: "变更用户状态", Method: "POST", Path: "/api/v1/system/user/{id}/status"},
		v1.OperationRoleServiceCreateRole:                             {Title: "创建角色", Method: "POST", Path: "/api/v1/system/role"},
		v1.OperationRoleServiceGetRolePermissions:                     {Title: "查询角色权限", Method: "GET", Path: "/api/v1/system/role/{id}/permission"},
		v1.OperationRoleServiceSetRolePermissions:                     {Title: "设置角色权限", Method: "PUT", Path: "/api/v1/system/role/{id}/permission"},
		v1.OperationRoleServiceListOperations:                         {Title: "查询可授权操作", Method: "GET", Path: "/api/v1/system/operations"},
		notifyv1.OperationNotifyChannelServiceCreateChannel:           {Title: "创建通知渠道", Method: "POST", Path: "/api/v1/notify/channel"},
		notifyv1.OperationNotifyChannelServiceListChannels:            {Title: "查询通知渠道列表", Method: "GET", Path: "/api/v1/notify/channel"},
		notifyv1.OperationNotifyTemplateServiceCreateTemplate:         {Title: "创建通知模板", Method: "POST", Path: "/api/v1/notify/template"},
		notifyv1.OperationNotifyTemplateServiceListTemplates:          {Title: "查询通知模板列表", Method: "GET", Path: "/api/v1/notify/template"},
		notifyv1.OperationNotifyRouteServiceCreateRoute:               {Title: "创建通知路由", Method: "POST", Path: "/api/v1/notify/route"},
		notifyv1.OperationNotifyRouteServiceListRoutes:                {Title: "查询通知路由列表", Method: "GET", Path: "/api/v1/notify/route"},
		notifyv1.OperationNotifySilenceServiceCreateSilence:           {Title: "创建静默规则", Method: "POST", Path: "/api/v1/notify/silence"},
		notifyv1.OperationNotifySilenceServiceListSilences:            {Title: "查询静默规则列表", Method: "GET", Path: "/api/v1/notify/silence"},
		notifyv1.OperationNotifySubscriptionServiceUpsertSubscription: {Title: "保存订阅", Method: "POST", Path: "/api/v1/notify/subscription"},
		notifyv1.OperationNotifySubscriptionServiceListSubscriptions:  {Title: "查询订阅列表", Method: "GET", Path: "/api/v1/notify/subscription"},
		// cmdb insight：漏登记 insight.proto 时其接口无法授予任何角色。
		cmdbv1.OperationCmdbInsightServiceGetOverview:     {Title: "查询运营概览", Method: "GET", Path: "/api/v1/cmdb/overview"},
		cmdbv1.OperationCmdbInsightServiceListSchemaAudit: {Title: "查询模型变更审计", Method: "GET", Path: "/api/v1/cmdb/schema-audit"},
	}
	for want, wantOp := range mustContain {
		idx := slices.IndexFunc(ops, func(op operation) bool { return op.Name == want })
		if idx < 0 {
			t.Errorf("listOperations() missing %s", want)
			continue
		}
		if ops[idx].Title != wantOp.Title {
			t.Errorf("operation %s title = %q, want %q", want, ops[idx].Title, wantOp.Title)
		}
		if ops[idx].Method != wantOp.Method || ops[idx].Path != wantOp.Path {
			t.Errorf("operation %s http = %s %s, want %s %s", want, ops[idx].Method, ops[idx].Path, wantOp.Method, wantOp.Path)
		}
	}
	// 字典序且无重复。
	if !slices.IsSortedFunc(ops, func(a, b operation) int { return cmp.Compare(a.Name, b.Name) }) {
		t.Error("listOperations() not sorted")
	}
	names := make([]string, len(ops))
	for i, op := range ops {
		names[i] = op.Name
	}
	if len(slices.Compact(slices.Clone(names))) != len(names) {
		t.Error("listOperations() contains duplicates")
	}
}

// TestValidateOperations 未枚举 code 拒绝（400 OPERATION_INVALID），已知放行。
func TestValidateOperations(t *testing.T) {
	t.Parallel()

	if err := validateOperations(nil); err != nil {
		t.Fatalf("validateOperations(nil) = %v, want nil", err)
	}
	known := []string{
		biz.OperationCode(v1.OperationUserServiceListUsers),
		biz.OperationCode(v1.OperationRoleServiceListRoles),
	}
	if err := validateOperations(known); err != nil {
		t.Fatalf("validateOperations(known) = %v, want nil", err)
	}

	// 格式合法但不在目录内 → 拒绝。
	err := validateOperations([]string{known[0], biz.OperationCode("/system.v1.UserService/Nope")})
	if err == nil {
		t.Fatal("validateOperations(unknown) = nil, want error")
	}
	if !errors.Is(err, ErrOperationInvalid) {
		t.Fatalf("validateOperations(unknown) error = %v, want ErrOperationInvalid", err)
	}

	// 旧口径的 full method 不再被接受（统一 code）。
	if err := validateOperations([]string{v1.OperationUserServiceListUsers}); !errors.Is(err, ErrOperationInvalid) {
		t.Fatalf("validateOperations(full method) error = %v, want ErrOperationInvalid", err)
	}

	// 认证四接口已不在可授权集合：对它们授权无意义，直接拒绝。
	if err := validateOperations([]string{biz.OperationCode(v1.OperationAuthServiceLogin)}); !errors.Is(err, ErrOperationInvalid) {
		t.Fatalf("validateOperations(Login) error = %v, want ErrOperationInvalid", err)
	}
}

// TestIsAuthzExempt 钉死免授权集合成员：往集合里加成员等于绕过接口权限
// 门禁，任何变更都应是有意为之并同步本测试。
func TestIsAuthzExempt(t *testing.T) {
	t.Parallel()
	want := []string{
		v1.OperationAuthServiceLogin,
		v1.OperationAuthServiceRefreshToken,
		v1.OperationAuthServiceLogout,
		v1.OperationAuthServiceGetLoginProfile,
		// 查询系统配置：登录页匿名拉取标题/logo，有意免检（写接口不免检）
		v1.OperationSystemConfigServiceGetSystemConfig,
		// logo 文件读取：登录页 <img> 直载，有意免检（上传接口不免检）
		v1.OperationSystemConfigServiceGetSystemLogo,
		// 头像：上传为本人自助接口；读取为 <img> 直载的静态资源语义
		v1.OperationAuthServiceUploadMyAvatar,
		v1.OperationAuthServiceGetUserAvatar,
		// 自助修改密码：本人、须核对现密码（仍须 JWT）
		v1.OperationAuthServiceChangeMyPassword,
		// 公共值班日历：凭不可猜测的地址令牌匿名只读（管理接口不免检）
		oncallv1.OperationOncallShareServiceGetPublicCalendar,
		// 通用附件：已登录用户的自助能力（仍须 JWT）；访问控制在文件域按归属判定
		filev1.OperationFileServiceUploadFile,
		filev1.OperationFileServiceGetFile,
		filev1.OperationFileServiceDownloadFile,
		// 告警事件详情：已登录的通知收件人要能打开通知里的链接；访问控制在监控域按「接收方，或
		// 有告警查询权限且在数据范围内」判定，其余一律 404
		monitorv1.OperationMonitorAlertServiceGetAlert,
		monitorv1.OperationMonitorAlertServiceListShiftAlertCounts,
		// 流程引擎的开放接口：供其他系统接入，凭接入令牌认证（库内只存哈希、可停用可重置），
		// 能发起哪些流程由凭据配置决定、只能操作自己发起的实例；凭据的管理接口不免检
		workflowv1.OperationWorkflowOpenServiceOpenStartInstance,
		workflowv1.OperationWorkflowOpenServiceOpenGetInstance,
		workflowv1.OperationWorkflowOpenServiceOpenSignalInstance,
		workflowv1.OperationWorkflowOpenServiceOpenCancelInstance,
		// 容器平台 Web 终端的 WebSocket 入口：浏览器的 WebSocket 不能带 Authorization 头，凭一次性票据认证
		// （30 秒有效、核销即删）；签发票据的 OpenExecTicket 不免检
		OperationK8sExecConnect,
		// 作业中心的开放触发接口：凭触发器的调用令牌或流程 Webhook 的签名认证；触发器的管理接口不免检
		OperationJobOpenFire,
		// 故障的匿名状态页：凭不可猜测的分享令牌匿名只读（管理接口不免检）
		incidentv1.OperationIncidentUpdateServiceGetPublicIncident,
		// 故障管理的开放接口：凭接入令牌认证，只能操作自己创建的故障（接入源的管理接口不免检）
		incidentv1.OperationIncidentOpenServiceOpenCreateIncident,
		incidentv1.OperationIncidentOpenServiceOpenGetIncident,
		incidentv1.OperationIncidentOpenServiceOpenAppendNote,
		incidentv1.OperationIncidentOpenServiceOpenResolveIncident,
		// 告警所属故障：已登录的告警查看者要能看到告警关联到了哪条故障；访问控制在故障域按成员 / 接口权限过滤
		incidentv1.OperationIncidentServiceListIncidentsByAlert,
	}
	if len(authzExemptOperations) != len(want) {
		t.Fatalf("authzExemptOperations has %d members, want exactly %d", len(authzExemptOperations), len(want))
	}
	for _, op := range want {
		if !IsAuthzExempt(op) {
			t.Errorf("IsAuthzExempt(%s) = false, want true", op)
		}
	}
	for _, op := range []string{v1.OperationUserServiceListUsers, v1.OperationRoleServiceDeleteRole} {
		if IsAuthzExempt(op) {
			t.Errorf("IsAuthzExempt(%s) = true, want false", op)
		}
	}
}

// TestOperationRoutesUnambiguous 同一 HTTP 方法下，任意两条路由不得同时匹配同一个
// 请求路径（如 /ticket/{id} 与 /ticket/mine）：路由按注册顺序匹配，静态段会被路径变量
// 抢先吃掉，表现为另一条接口报参数校验错误。新增接口撞上时在此失败。
func TestOperationRoutesUnambiguous(t *testing.T) {
	t.Parallel()
	// 整段是 {name} 才是路径变量；{id}:verb 这类带动作后缀的段按字面比较。
	isVar := func(seg string) bool { return strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") }
	overlap := func(a, b string) bool {
		as, bs := strings.Split(a, "/"), strings.Split(b, "/")
		if len(as) != len(bs) {
			return false
		}
		for i := range as {
			if !isVar(as[i]) && !isVar(bs[i]) && as[i] != bs[i] {
				return false
			}
		}
		return true
	}
	ops := listOperations()
	for i, a := range ops {
		for _, b := range ops[i+1:] {
			if a.Method == b.Method && overlap(a.Path, b.Path) {
				t.Errorf("routes overlap: %s %s (%s) and %s (%s)", a.Method, a.Path, a.Name, b.Path, b.Name)
			}
		}
	}
}
