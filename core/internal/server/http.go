package server

import (
	"log/slog"
	"time"

	agentv1 "github.com/lanyulei/reborn/backend/core/api/agent/v1"
	assistantv1 "github.com/lanyulei/reborn/backend/core/api/assistant/v1"
	cmdbv1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	filev1 "github.com/lanyulei/reborn/backend/core/api/file/v1"
	incidentv1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	jobv1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	k8sv1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	knowledgev1 "github.com/lanyulei/reborn/backend/core/api/knowledge/v1"
	monitorv1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	oncallv1 "github.com/lanyulei/reborn/backend/core/api/oncall/v1"
	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	ticketv1 "github.com/lanyulei/reborn/backend/core/api/ticket/v1"
	workflowv1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	agentservice "github.com/lanyulei/reborn/backend/core/internal/service/agent"
	assistantservice "github.com/lanyulei/reborn/backend/core/internal/service/assistant"
	cmdbservice "github.com/lanyulei/reborn/backend/core/internal/service/cmdb"
	fileservice "github.com/lanyulei/reborn/backend/core/internal/service/file"
	incidentservice "github.com/lanyulei/reborn/backend/core/internal/service/incident"
	jobservice "github.com/lanyulei/reborn/backend/core/internal/service/job"
	k8sservice "github.com/lanyulei/reborn/backend/core/internal/service/k8s"
	knowledgeservice "github.com/lanyulei/reborn/backend/core/internal/service/knowledge"
	monitorservice "github.com/lanyulei/reborn/backend/core/internal/service/monitor"
	notifyservice "github.com/lanyulei/reborn/backend/core/internal/service/notify"
	oncallservice "github.com/lanyulei/reborn/backend/core/internal/service/oncall"
	service "github.com/lanyulei/reborn/backend/core/internal/service/system"
	ticketservice "github.com/lanyulei/reborn/backend/core/internal/service/ticket"
	workflowservice "github.com/lanyulei/reborn/backend/core/internal/service/workflow"
	"github.com/lanyulei/reborn/backend/kit/httpx"

	"github.com/go-kratos/kratos/contrib/otel/v3/tracing"
	"github.com/go-kratos/kratos/v3/middleware/logging"
	"github.com/go-kratos/kratos/v3/middleware/ratelimit"
	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/middleware/validate"
	"github.com/go-kratos/kratos/v3/transport/http"
)

// readTimeout 单个请求（头 + 体）的读取总时限。
const readTimeout = 60 * time.Second

// NewHTTPServer new an HTTP server.
// 中间件顺序固定：recovery → tracing → logging → validate →
// rate-limit → auth → authz → 业务。auth 在 rate-limit 之后：登录含 Argon2id
// （单次几十毫秒的显著 CPU 成本），限流必须先于认证与密码校验执行以防 DoS；
// authz 在 auth 之后：授权判定依赖鉴权注入的 claims（deny-by-default）。
func NewHTTPServer(c *conf.Server, logger *slog.Logger, user *service.UserService,
	role *service.RoleService, dept *service.DeptService, auth *service.AuthService, syscfg *service.SystemConfigService,
	loginLog *service.LoginLogService, notify *notifyservice.NotifyChannelService,
	tmpl *notifyservice.NotifyTemplateService, route *notifyservice.NotifyRouteService,
	silence *notifyservice.NotifySilenceService,
	subscription *notifyservice.NotifySubscriptionService,
	producer *notifyservice.NotifyProducerService,
	event *notifyservice.NotifyEventService,
	dispatch *notifyservice.NotifyDispatchService,
	deliveryLog *notifyservice.NotifyDeliveryLogService,
	deadLetter *notifyservice.NotifyDeadLetterService,
	assistant *assistantservice.AssistantService, assistantChat *assistantservice.AssistantChatService,
	cmdbModel *cmdbservice.CmdbModelService, cmdbCI *cmdbservice.CmdbCIService, cmdbRelation *cmdbservice.CmdbRelationService,
	cmdbScope *cmdbservice.CmdbScopeService, cmdbSync *cmdbservice.CmdbSyncService, cmdbInsight *cmdbservice.CmdbInsightService,
	oncallGroup *oncallservice.OncallGroupService, oncallSchedule *oncallservice.OncallScheduleService, oncallShare *oncallservice.OncallShareService,
	file *fileservice.FileService,
	wfDefinition *workflowservice.DefinitionService, wfInstance *workflowservice.InstanceService, wfTask *workflowservice.TaskService,
	wfConfig *workflowservice.ConfigService, wfOpen *workflowservice.OpenService,
	ticketCatalog *ticketservice.CatalogService, ticket *ticketservice.TicketService,
	monDatasource *monitorservice.DatasourceService, monGroup *monitorservice.RuleGroupService, monRule *monitorservice.RuleService,
	monAlert *monitorservice.AlertService, monMute *monitorservice.MuteService, monSubscription *monitorservice.SubscriptionService,
	monP2 monitorservice.P2Services,
	jobCredential *jobservice.CredentialService, jobProject *jobservice.ProjectService, jobTemplate *jobservice.TemplateService,
	jobExecution *jobservice.ExecutionService, jobRunner *jobservice.RunnerService,
	jobScript *jobservice.ScriptService, jobPolicy *jobservice.PolicyService, jobSchedule *jobservice.ScheduleService,
	jobTrigger *jobservice.TriggerService,
	k8sCluster *k8sservice.ClusterService, k8sScope *k8sservice.ScopeService, k8sAudit *k8sservice.AuditService,
	k8sResource *k8sservice.ResourceService, k8sWorkload *k8sservice.WorkloadService, k8sStream *k8sservice.StreamService,
	k8sHelmRepo *k8sservice.HelmRepoService, k8sHelmRelease *k8sservice.HelmReleaseService,
	k8sInsight *k8sservice.InsightService,
	k8sGov *k8sservice.GovernanceService,
	k8sChange *k8sservice.ChangeService,
	k8sConf *conf.K8S,
	kbSpace *knowledgeservice.SpaceService, kbDoc *knowledgeservice.KnowledgeService, kbIngest *knowledgeservice.IngestService, kbAdmin *knowledgeservice.AdminService,
	incident incidentservice.Services,
	agent agentservice.Services,
	signer *biz.TokenSigner, authzUC *biz.AuthzUsecase,
	roles Roles, pinger Pinger, metrics *Metrics,
) *http.Server {
	opts := []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
			tracing.Server(),
			logging.Server(logger),
			validate.Validator(httpx.ProtoValidate),
			ratelimit.Server(),
			authMiddleware(signer),
			operatorMiddleware(),
			authzMiddleware(authzUC, logger),
		),
		// body 8MiB 上限在路由前包裹；proto.Message 的 JSON 请求/响应统一走 protojson（kit/httpx）。
		http.Filter(k8sBodyLimit), // 通用 8MiB；容器文件上传除外（见 k8s_gov.go）
		http.RequestDecoder(httpx.DecodeRequest),
		http.ResponseEncoder(httpx.EncodeResponse),
	}
	if c.Http.Network != "" {
		opts = append(opts, http.Network(c.Http.Network))
	}
	if c.Http.Addr != "" {
		opts = append(opts, http.Address(c.Http.Addr))
	}
	if c.Http.Timeout != nil {
		opts = append(opts, http.Timeout(c.Http.Timeout.AsDuration()))
	}
	srv := http.NewServer(opts...)
	httpx.HardenServer(srv) // ReadHeaderTimeout / IdleTimeout（Kratos 默认不设）
	// 请求头 + 请求体读取总时限（体上限 8MiB，头像/logo 上传足够）：防慢速
	// 请求体占住连接。不设 WriteTimeout——助手 SSE 为分钟级长响应（其读截止
	// 时间在进入流式阶段前清除，见 assistant.go）。
	srv.ReadTimeout = readTimeout
	// 探活与指标端点任何角色都有；业务接口只在 api 角色下注册——只跑后台循环的进程
	// 不对外提供任何业务入口。
	probes(srv, pinger, metrics)
	if !roles.Has(RoleAPI) {
		return srv
	}
	v1.RegisterUserServiceHTTPServer(srv, user)
	v1.RegisterRoleServiceHTTPServer(srv, role)
	v1.RegisterDeptServiceHTTPServer(srv, dept)
	v1.RegisterAuthServiceHTTPServer(srv, auth)
	v1.RegisterSystemConfigServiceHTTPServer(srv, syscfg)
	v1.RegisterLoginLogServiceHTTPServer(srv, loginLog)
	notifyv1.RegisterNotifyChannelServiceHTTPServer(srv, notify)
	notifyv1.RegisterNotifyTemplateServiceHTTPServer(srv, tmpl)
	notifyv1.RegisterNotifyRouteServiceHTTPServer(srv, route)
	notifyv1.RegisterNotifySilenceServiceHTTPServer(srv, silence)
	notifyv1.RegisterNotifySubscriptionServiceHTTPServer(srv, subscription)
	notifyv1.RegisterNotifyProducerServiceHTTPServer(srv, producer)
	notifyv1.RegisterNotifyEventServiceHTTPServer(srv, event)
	notifyv1.RegisterNotifyDispatchServiceHTTPServer(srv, dispatch)
	notifyv1.RegisterNotifyDeliveryLogServiceHTTPServer(srv, deliveryLog)
	notifyv1.RegisterNotifyDeadLetterServiceHTTPServer(srv, deadLetter)
	assistantv1.RegisterAssistantServiceHTTPServer(srv, assistant)
	// AssistantChatService 的 Chat/Confirm 为 SSE 流式，不走生成注册（见 assistant.go）。
	registerAssistantStream(srv, assistantChat)
	cmdbv1.RegisterCmdbModelServiceHTTPServer(srv, cmdbModel)
	cmdbv1.RegisterCmdbCIServiceHTTPServer(srv, cmdbCI)
	cmdbv1.RegisterCmdbRelationServiceHTTPServer(srv, cmdbRelation)
	cmdbv1.RegisterCmdbScopeServiceHTTPServer(srv, cmdbScope)
	cmdbv1.RegisterCmdbSyncServiceHTTPServer(srv, cmdbSync)
	cmdbv1.RegisterCmdbInsightServiceHTTPServer(srv, cmdbInsight)
	oncallv1.RegisterOncallGroupServiceHTTPServer(srv, oncallGroup)
	oncallv1.RegisterOncallScheduleServiceHTTPServer(srv, oncallSchedule)
	oncallv1.RegisterOncallShareServiceHTTPServer(srv, oncallShare)
	filev1.RegisterFileServiceHTTPServer(srv, file)
	workflowv1.RegisterWorkflowDefinitionServiceHTTPServer(srv, wfDefinition)
	workflowv1.RegisterWorkflowInstanceServiceHTTPServer(srv, wfInstance)
	workflowv1.RegisterWorkflowTaskServiceHTTPServer(srv, wfTask)
	workflowv1.RegisterWorkflowConfigServiceHTTPServer(srv, wfConfig)
	workflowv1.RegisterWorkflowOpenServiceHTTPServer(srv, wfOpen)
	ticketv1.RegisterTicketCatalogServiceHTTPServer(srv, ticketCatalog)
	ticketv1.RegisterTicketServiceHTTPServer(srv, ticket)
	monitorv1.RegisterMonitorDatasourceServiceHTTPServer(srv, monDatasource)
	monitorv1.RegisterMonitorRuleGroupServiceHTTPServer(srv, monGroup)
	monitorv1.RegisterMonitorRuleServiceHTTPServer(srv, monRule)
	monitorv1.RegisterMonitorAlertServiceHTTPServer(srv, monAlert)
	monitorv1.RegisterMonitorMuteServiceHTTPServer(srv, monMute)
	monitorv1.RegisterMonitorSubscriptionServiceHTTPServer(srv, monSubscription)
	monitorv1.RegisterMonitorEscalationServiceHTTPServer(srv, monP2.Escalation)
	monitorv1.RegisterMonitorInhibitServiceHTTPServer(srv, monP2.Inhibit)
	monitorv1.RegisterMonitorRecordRuleServiceHTTPServer(srv, monP2.RecordRule)
	monitorv1.RegisterMonitorAlertViewServiceHTTPServer(srv, monP2.AlertView)
	monitorv1.RegisterMonitorStatsServiceHTTPServer(srv, monP2.Stats)
	monitorv1.RegisterMonitorEngineServiceHTTPServer(srv, monP2.Engine)
	jobv1.RegisterJobCredentialServiceHTTPServer(srv, jobCredential)
	jobv1.RegisterJobProjectServiceHTTPServer(srv, jobProject)
	jobv1.RegisterJobTemplateServiceHTTPServer(srv, jobTemplate)
	jobv1.RegisterJobExecutionServiceHTTPServer(srv, jobExecution)
	jobv1.RegisterJobRunnerServiceHTTPServer(srv, jobRunner)
	jobv1.RegisterJobScriptServiceHTTPServer(srv, jobScript)
	jobv1.RegisterJobPolicyServiceHTTPServer(srv, jobPolicy)
	jobv1.RegisterJobScheduleServiceHTTPServer(srv, jobSchedule)
	jobv1.RegisterJobTriggerServiceHTTPServer(srv, jobTrigger)
	// 开放触发接口要对原始请求体验签，不走生成注册（见 job.go）。
	registerJobOpen(srv, jobTrigger)
	k8sv1.RegisterK8SClusterServiceHTTPServer(srv, k8sCluster)
	k8sv1.RegisterK8SScopeServiceHTTPServer(srv, k8sScope)
	k8sv1.RegisterK8SAuditServiceHTTPServer(srv, k8sAudit)
	k8sv1.RegisterK8SResourceServiceHTTPServer(srv, k8sResource)
	k8sv1.RegisterK8SWorkloadServiceHTTPServer(srv, k8sWorkload)
	k8sv1.RegisterK8SHelmRepoServiceHTTPServer(srv, k8sHelmRepo)
	k8sv1.RegisterK8SHelmReleaseServiceHTTPServer(srv, k8sHelmRelease)
	k8sv1.RegisterK8SInsightServiceHTTPServer(srv, k8sInsight)
	k8sv1.RegisterK8SGovernanceServiceHTTPServer(srv, k8sGov)
	k8sv1.RegisterK8SChangeServiceHTTPServer(srv, k8sChange)
	// 集群代理入口（限时 kubeconfig）与容器文件传输不经生成代码绑定，见 k8s_gov.go。
	registerK8sGovernance(srv, k8sGov, logger)
	// K8sStreamService（SSE）与 Web 终端（WebSocket）不经生成代码绑定，见 k8s.go。
	registerK8sStreams(srv, k8sStream, k8sConf.GetAllowedOrigins(), logger)
	knowledgev1.RegisterKnowledgeSpaceServiceHTTPServer(srv, kbSpace)
	knowledgev1.RegisterKnowledgeServiceHTTPServer(srv, kbDoc)
	knowledgev1.RegisterKnowledgeIngestServiceHTTPServer(srv, kbIngest)
	knowledgev1.RegisterKnowledgeAdminServiceHTTPServer(srv, kbAdmin)
	incidentv1.RegisterIncidentServiceHTTPServer(srv, incident.Incident)
	incidentv1.RegisterIncidentConfigServiceHTTPServer(srv, incident.Config)
	incidentv1.RegisterIncidentProposalServiceHTTPServer(srv, incident.Proposal)
	incidentv1.RegisterIncidentUpdateServiceHTTPServer(srv, incident.Update)
	incidentv1.RegisterIncidentPostmortemServiceHTTPServer(srv, incident.Postmortem)
	incidentv1.RegisterIncidentStatsServiceHTTPServer(srv, incident.Stats)
	incidentv1.RegisterIncidentOpenServiceHTTPServer(srv, incident.Open)
	agentv1.RegisterAgentProfileServiceHTTPServer(srv, agent.Profile)
	agentv1.RegisterAgentTaskServiceHTTPServer(srv, agent.Task)
	agentv1.RegisterAgentStatsServiceHTTPServer(srv, agent.Stats)
	return srv
}
