-- 0082 运维 Agent 域（agent）初始化：Profile（策略）与变更历史、任务队列、运行记录、预算计数；
-- 以及三处对其他域的只增改动：system_user.kind（机器账号）、assistant_conversation.kind（Agent 运行的会话
-- 不进用户会话列表）、incident_proposal.auto_eligible（Agent 申请自动通过）。
-- 设计：backend/docs/ops-agent-functional-design.md（v0.5）。jsonb 列的结构与状态字面量以 core/internal/biz/agent 为准。
-- Agent 域只在 core 内运行，没有独立服务，不另建 DB 角色；连接账号沿用 core 的。

-- ---------------------------------------------------------------------------
-- system：机器账号
-- ---------------------------------------------------------------------------

ALTER TABLE "system_user" ADD COLUMN kind smallint NOT NULL DEFAULT 1;
ALTER TABLE "system_user" ADD CONSTRAINT ck_system_user_kind CHECK (kind IN (1, 2));
COMMENT ON COLUMN "system_user".kind IS '账号类型：1=人 2=机器（没有可用口令、不能登录、没有刷新令牌，只能被签发短期访问令牌；运维 Agent 用）';

-- 内置角色模板「运维 Agent」：故障域的读接口 + 发现 / 评论 / 建议 / 变更关系 / 关联告警 / 租约，以及监控 / 容器 / 资源 /
-- 作业 / 流程 / 知识库的只读接口。非 builtin（builtin 角色会自动绑给每个新建的人类账号）；管理员可改。
-- 没有置状态、改等级、审批建议、发公告、提交作业、改任何配置的权限。
INSERT INTO system_role (id, name, code, status) VALUES (
    '01a1b987-129c-7186-9a25-92bd159a7661', '运维 Agent', 'ops-agent', 1
) ON CONFLICT (code) WHERE delete_time IS NULL DO NOTHING;

INSERT INTO system_permission (id, role_id, operation)
SELECT gen_random_uuid(), r.id, md5(op.name)
FROM system_role r
CROSS JOIN (VALUES
    ('/incident.v1.IncidentService/ListIncidents'),
    ('/incident.v1.IncidentService/GetIncident'),
    ('/incident.v1.IncidentService/SearchIncidents'),
    ('/incident.v1.IncidentService/ListTimeline'),
    ('/incident.v1.IncidentService/ListIncidentAlerts'),
    ('/incident.v1.IncidentService/GetImpact'),
    ('/incident.v1.IncidentService/ListChanges'),
    ('/incident.v1.IncidentService/GetIncidentContext'),
    ('/incident.v1.IncidentService/AddFinding'),
    ('/incident.v1.IncidentService/AddComment'),
    ('/incident.v1.IncidentService/MarkChange'),
    ('/incident.v1.IncidentService/AttachAlerts'),
    ('/incident.v1.IncidentService/AcquireAgentLease'),
    ('/incident.v1.IncidentService/ReleaseAgentLease'),
    ('/incident.v1.IncidentProposalService/ListProposals'),
    ('/incident.v1.IncidentProposalService/CreateProposal'),
    ('/incident.v1.IncidentPostmortemService/GetPostmortem'),
    ('/incident.v1.IncidentPostmortemService/ListPostmortemVersions'),
    ('/incident.v1.IncidentConfigService/ListPlaybooks'),
    ('/incident.v1.IncidentConfigService/GetPlaybook'),
    ('/monitor.v1.MonitorAlertService/ListAlerts'),
    ('/monitor.v1.MonitorAlertService/GetAlert'),
    ('/monitor.v1.MonitorDatasourceService/ListDatasources'),
    ('/monitor.v1.MonitorDatasourceService/QueryDatasource'),
    ('/monitor.v1.MonitorDatasourceService/QueryDatasourceRange'),
    ('/cmdb.v1.CmdbCIService/ListCIs'),
    ('/cmdb.v1.CmdbCIService/GetCI'),
    ('/cmdb.v1.CmdbCIService/SearchCIs'),
    ('/cmdb.v1.CmdbCIService/ListCIHistory'),
    ('/cmdb.v1.CmdbRelationService/ListRelated'),
    ('/cmdb.v1.CmdbRelationService/Neighbors'),
    ('/k8s.v1.K8sInsightService/GetOverview'),
    ('/k8s.v1.K8sInsightService/ListEvents'),
    ('/k8s.v1.K8sInsightService/QueryMetrics'),
    ('/k8s.v1.K8sInsightService/GetRelations'),
    ('/k8s.v1.K8sInsightService/DiagnosePod'),
    ('/k8s.v1.K8sInsightService/DiagnoseWorkload'),
    ('/k8s.v1.K8sResourceService/ListResources'),
    ('/k8s.v1.K8sResourceService/GetObject'),
    ('/k8s.v1.K8sResourceService/ListObjectEvents'),
    ('/job.v1.JobExecutionService/ListJobExecutions'),
    ('/job.v1.JobExecutionService/GetJobExecution'),
    ('/job.v1.JobExecutionService/GetJobExecutionLog'),
    ('/workflow.v1.WorkflowInstanceService/GetInstance'),
    ('/knowledge.v1.KnowledgeService/Search'),
    ('/knowledge.v1.KnowledgeService/GetDocument'),
    ('/knowledge.v1.KnowledgeService/ListRelated'),
    ('/knowledge.v1.KnowledgeService/ListDocuments'),
    ('/knowledge.v1.KnowledgeSpaceService/ListSpaces'),
    ('/knowledge.v1.KnowledgeSpaceService/GetSpace'),
    ('/knowledge.v1.KnowledgeService/CreateDocument')
) AS op(name)
WHERE r.code = 'ops-agent' AND r.delete_time IS NULL
  AND NOT EXISTS (SELECT 1 FROM system_permission p WHERE p.role_id = r.id AND p.operation = md5(op.name));

-- ---------------------------------------------------------------------------
-- assistant：Agent 运行的会话
-- ---------------------------------------------------------------------------

ALTER TABLE assistant_conversation ADD COLUMN kind smallint NOT NULL DEFAULT 1;
COMMENT ON COLUMN assistant_conversation.kind IS '会话类型：1=用户对话（本人会话列表可见） 2=运维 Agent 运行（归机器账号，只经 Agent 运行详情回放）';
COMMENT ON COLUMN assistant_tool_call.status IS 'pending / approved / rejected / denied / succeeded / failed / cancelled / captured（无人值守干跑：写操作未执行，只记录拟调用参数）';

-- ---------------------------------------------------------------------------
-- incident：Agent 申请自动通过
-- ---------------------------------------------------------------------------

ALTER TABLE incident_proposal ADD COLUMN auto_eligible boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN incident_proposal.auto_eligible IS 'Agent 提出时申请自动通过（Agent 侧条件已满足；只有持租约的机器账号可写）；是否真的自动仍见 auto';

-- ---------------------------------------------------------------------------
-- agent：Profile
-- ---------------------------------------------------------------------------

CREATE TABLE agent_profile (
    id             uuid         PRIMARY KEY,
    name           varchar(64)  NOT NULL,
    kind           varchar(16)  NOT NULL DEFAULT 'diagnose',
    agent_user_id  uuid         NOT NULL,
    priority       integer      NOT NULL DEFAULT 100,
    enabled        boolean      NOT NULL DEFAULT true,
    spec           jsonb        NOT NULL,
    breaker_open   boolean      NOT NULL DEFAULT false,
    breaker_reason varchar(255) NOT NULL DEFAULT '',
    version        integer      NOT NULL DEFAULT 1,
    create_time    timestamptz  NOT NULL DEFAULT now(),
    update_time    timestamptz  NOT NULL DEFAULT now(),
    create_user    uuid         NULL,
    update_user    uuid         NULL,
    delete_time    timestamptz  NULL,
    CONSTRAINT ck_agent_profile_kind CHECK (kind IN ('diagnose', 'postmortem', 'inspection')),
    CONSTRAINT ck_agent_profile_spec CHECK (jsonb_typeof(spec) = 'object')
);
COMMENT ON TABLE agent_profile IS '运维 Agent Profile（策略）：机器账号、适用范围、阶段与工具白名单、预算、自动通过条件；结构见 biz/agent ProfileSpec';
COMMENT ON COLUMN agent_profile.kind IS '类型：diagnose 故障诊断 / postmortem 经验沉淀 / inspection 巡检（本期只实现 diagnose）';
COMMENT ON COLUMN agent_profile.agent_user_id IS '机器账号（system_user，kind=2）';
COMMENT ON COLUMN agent_profile.priority IS '优先级（小者优先）：同类型多个 Profile 命中同一故障时取第一个';
COMMENT ON COLUMN agent_profile.spec IS 'Profile 内容 jsonb（biz/agent.ProfileSpec）';
COMMENT ON COLUMN agent_profile.breaker_open IS 'Profile 级熔断：24 小时内自动执行的建议验证失败率超限后自动通过能力停用，需人工恢复';
COMMENT ON COLUMN agent_profile.version IS '乐观锁版本号；每次变更写一条 agent_profile_history';
CREATE UNIQUE INDEX uk_agent_profile_name ON agent_profile (name) WHERE delete_time IS NULL;
CREATE INDEX idx_agent_profile_pick ON agent_profile (kind, priority, create_time) WHERE delete_time IS NULL AND enabled;

CREATE TABLE agent_profile_history (
    id          uuid         PRIMARY KEY,
    profile_id  uuid         NOT NULL REFERENCES agent_profile (id),
    version     integer      NOT NULL,
    actor_id    uuid         NULL,
    before      jsonb        NULL,
    after       jsonb        NOT NULL,
    note        varchar(255) NOT NULL DEFAULT '',
    create_time timestamptz  NOT NULL DEFAULT now()
);
COMMENT ON TABLE agent_profile_history IS 'Profile 变更历史：每次新建 / 编辑 / 恢复熔断记一条（修改人、变更前后内容）';
CREATE INDEX idx_agent_profile_history_profile ON agent_profile_history (profile_id, create_time DESC);

-- ---------------------------------------------------------------------------
-- agent：任务队列
-- ---------------------------------------------------------------------------

CREATE TABLE agent_task (
    id              uuid         PRIMARY KEY,
    profile_id      uuid         NULL,
    kind            varchar(16)  NOT NULL DEFAULT 'diagnose',
    source          smallint     NOT NULL,
    dry_run         boolean      NOT NULL DEFAULT false,
    incident_id     uuid         NOT NULL,
    incident_number varchar(32)  NOT NULL DEFAULT '',
    incident_title  varchar(255) NOT NULL DEFAULT '',
    severity        smallint     NOT NULL DEFAULT 4,
    business_id     uuid         NULL,
    rule_id         uuid         NULL,
    reason          varchar(64)  NOT NULL DEFAULT '',
    period_key      varchar(64)  NOT NULL DEFAULT '',
    status          varchar(16)  NOT NULL DEFAULT 'queued',
    attempts        integer      NOT NULL DEFAULT 0,
    next_time       timestamptz  NOT NULL DEFAULT now(),
    lock_token      uuid         NULL,
    locked_until    timestamptz  NULL,
    requester_id    uuid         NULL,
    input           text         NOT NULL DEFAULT '',
    result          varchar(255) NOT NULL DEFAULT '',
    run_id          uuid         NULL,
    queued_time     timestamptz  NOT NULL DEFAULT now(),
    start_time      timestamptz  NULL,
    end_time        timestamptz  NULL,
    update_time     timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_agent_task_source CHECK (source BETWEEN 1 AND 5),
    CONSTRAINT ck_agent_task_severity CHECK (severity BETWEEN 1 AND 4),
    CONSTRAINT ck_agent_task_status CHECK (status IN ('queued', 'running', 'succeeded', 'no_finding', 'failed', 'cancelled', 'skipped', 'expired'))
);
COMMENT ON TABLE agent_task IS '运维 Agent 任务队列：故障开启 / 升级（incident 同事务插入最小行）、人工召唤、干跑；运行器按等级与入队时间 SKIP LOCKED 领取';
COMMENT ON COLUMN agent_task.profile_id IS 'Profile；入队时未定（NULL），领取时按优先级选第一个适用的';
COMMENT ON COLUMN agent_task.source IS '来源：1=故障开启 / 升级 2=人工召唤 3=复盘完成 4=巡检计划 5=干跑';
COMMENT ON COLUMN agent_task.reason IS '入队原因：opened / escalated / manual / dry_run';
COMMENT ON COLUMN agent_task.period_key IS '巡检周期键（非巡检为空串）；与 profile_id / source / incident_id 组成活跃任务唯一键';
COMMENT ON COLUMN agent_task.status IS 'queued / running / succeeded / no_finding / failed / cancelled / skipped / expired';
COMMENT ON COLUMN agent_task.lock_token IS '领取令牌：运行中的任务由持有者写回；locked_until 过期后可被重新领取（实例崩溃兜底）';
COMMENT ON COLUMN agent_task.result IS '终态说明（skipped / cancelled / failed / expired 的原因）';
-- 诊断任务按故障唯一（租约是故障级的）：同一故障同时只有一条排队 / 进行中的非干跑诊断任务。
CREATE UNIQUE INDEX uk_agent_task_incident_active ON agent_task (incident_id) WHERE kind = 'diagnose' AND NOT dry_run AND status IN ('queued', 'running');
-- 领取按 (severity, queued_time) 取序，next_time 作过滤（绝大多数排队任务都已到期）。
CREATE INDEX idx_agent_task_queue ON agent_task (severity, queued_time) WHERE status = 'queued';
CREATE INDEX idx_agent_task_running ON agent_task (locked_until) WHERE status = 'running';
CREATE INDEX idx_agent_task_incident ON agent_task (incident_id, queued_time DESC);
CREATE INDEX idx_agent_task_profile ON agent_task (profile_id, queued_time DESC);
CREATE INDEX idx_agent_task_status_time ON agent_task (status, queued_time DESC);

-- ---------------------------------------------------------------------------
-- agent：运行记录
-- ---------------------------------------------------------------------------

CREATE TABLE agent_run (
    id               uuid         PRIMARY KEY,
    task_id          uuid         NOT NULL,
    profile_id       uuid         NOT NULL,
    incident_id      uuid         NOT NULL,
    agent_user_id    uuid         NOT NULL,
    conversation_id  uuid         NOT NULL,
    status           varchar(16)  NOT NULL DEFAULT 'running',
    dry_run          boolean      NOT NULL DEFAULT false,
    stages           jsonb        NOT NULL DEFAULT '[]',
    prompt_tokens    bigint       NOT NULL DEFAULT 0,
    completion_tokens bigint      NOT NULL DEFAULT 0,
    tokens_estimated boolean      NOT NULL DEFAULT false,
    finding          jsonb        NULL,
    finding_event_id uuid         NULL,
    proposals        jsonb        NOT NULL DEFAULT '[]',
    denied           jsonb        NOT NULL DEFAULT '[]',
    captured         jsonb        NOT NULL DEFAULT '[]',
    end_reason       varchar(64)  NOT NULL DEFAULT '',
    feedback         smallint     NOT NULL DEFAULT 0,
    feedback_user    uuid         NULL,
    feedback_time    timestamptz  NULL,
    feedback_note    varchar(500) NOT NULL DEFAULT '',
    start_time       timestamptz  NOT NULL DEFAULT now(),
    end_time         timestamptz  NULL,
    create_time      timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_agent_run_status CHECK (status IN ('running', 'succeeded', 'no_finding', 'failed', 'cancelled')),
    CONSTRAINT ck_agent_run_feedback CHECK (feedback BETWEEN 0 AND 2),
    CONSTRAINT ck_agent_run_stages CHECK (jsonb_typeof(stages) = 'array')
);
COMMENT ON TABLE agent_run IS '运维 Agent 运行：一条任务一次运行 = 一个 kind=agent 的助手会话；阶段轨迹、token 用量、产出引用、被拒调用、干跑捕获、人工评价';
COMMENT ON COLUMN agent_run.stages IS '阶段轨迹 jsonb 数组（biz/agent.StageTrace：key / status / steps / tokens / duration_ms / summary / end_reason / last_message_id）';
COMMENT ON COLUMN agent_run.finding IS '产出的发现（biz/agent.Finding）；无结论为 NULL';
COMMENT ON COLUMN agent_run.proposals IS '提出的建议引用数组 [{id, title}]';
COMMENT ON COLUMN agent_run.denied IS '被拒绝（403）的工具调用数组 [{tool, reason, message}]：Profile 上线前的权限与范围检查';
COMMENT ON COLUMN agent_run.captured IS '干跑捕获的写调用数组 [{tool, input}]';
COMMENT ON COLUMN agent_run.feedback IS '人工评价：0 未评 1 有用 2 无用（只保留最后一次）';
CREATE INDEX idx_agent_run_incident ON agent_run (incident_id, start_time DESC);
CREATE INDEX idx_agent_run_profile ON agent_run (profile_id, start_time DESC);
CREATE INDEX idx_agent_run_time ON agent_run (start_time DESC);
CREATE INDEX idx_agent_run_task ON agent_run (task_id);
-- 保留期清理按 end_time 扫描。
CREATE INDEX idx_agent_run_end ON agent_run (end_time) WHERE end_time IS NOT NULL;

-- ---------------------------------------------------------------------------
-- agent：预算计数（按 Profile 按日 / 按小时 UPSERT）
-- ---------------------------------------------------------------------------

CREATE TABLE agent_budget (
    profile_id        uuid        NOT NULL,
    period            varchar(24) NOT NULL,
    tasks             integer     NOT NULL DEFAULT 0,
    prompt_tokens     bigint      NOT NULL DEFAULT 0,
    completion_tokens bigint      NOT NULL DEFAULT 0,
    update_time       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (profile_id, period)
);
COMMENT ON TABLE agent_budget IS '运维 Agent 预算计数：period 为 d:<日> 或 h:<日 T 小时>（服务端时区）；日预算与小时配额据此封顶';
CREATE INDEX idx_agent_budget_period ON agent_budget (period);
