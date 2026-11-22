-- 0080 故障管理域（incident）初始化：故障、成员、时间线、关联告警、关联变更、候选故障、触发规则、业务字典、
-- 预案、动作建议、公告、分享链接、复盘（含版本）、改进项、接入源、Webhook 订阅与出站箱、动作发件箱、
-- 关联器游标、单例设置；以及对监控域的两处只增改动（告警时间线序号与「关联故障」类型）。
-- 设计：backend/docs/incident-functional-design.md（v0.2）。jsonb 列的结构与状态字面量以 core/internal/biz/incident 为准。
-- 故障域只在 core 内运行，没有独立服务，不另建 DB 角色；连接账号沿用 core 的。

-- ---------------------------------------------------------------------------
-- 监控域：告警时间线加全局序号（关联器按它 tail；UUIDv7 主键在多写入方下不保证提交序），
-- 时间线类型放开 incident（故障域经监控域的时间线仓储写入「已关联故障」）。
-- 两个写入方（评估引擎 / core）都不指定该列，缺省自增；序列须对两边的角色开放。
-- ---------------------------------------------------------------------------

ALTER TABLE monitor_alert_event ADD COLUMN seq bigserial;
COMMENT ON COLUMN monitor_alert_event.seq IS '全局递增序号（写入时自增，不参与业务）：故障域关联器据此顺序消费触发 / 恢复事件';
CREATE INDEX idx_monitor_alert_event_seq ON monitor_alert_event (seq);
GRANT USAGE, SELECT ON SEQUENCE monitor_alert_event_seq_seq TO monitor_engine_role, monitor_core_role;

ALTER TABLE monitor_alert_event DROP CONSTRAINT ck_monitor_alert_event_type;
ALTER TABLE monitor_alert_event ADD CONSTRAINT ck_monitor_alert_event_type CHECK (type IN (
    'fire', 'resolve', 'close', 'mute', 'unmute', 'inhibit', 'uninhibit', 'escalate', 'notify',
    'ack', 'unack', 'transfer', 'comment', 'ticket', 'ticket_note', 'workflow', 'ai_summary', 'action_failed', 'incident'));
COMMENT ON COLUMN monitor_alert_event.type IS '类型：fire 触发 / resolve 恢复 / close 关闭 / mute 被屏蔽 / unmute 屏蔽解除 / inhibit 被抑制 / uninhibit 抑制解除 / escalate 升级 / notify 通知说明 / ack 认领 / unack 取消认领 / transfer 转交 / comment 备注 / ticket 转工单 / ticket_note 工单追加记录 / workflow 触发流程 / ai_summary AI 摘要 / action_failed 事件动作失败 / incident 关联故障';

-- ---------------------------------------------------------------------------
-- 单例设置
-- ---------------------------------------------------------------------------

CREATE TABLE incident_setting (
    id          smallint    PRIMARY KEY,
    spec        jsonb       NOT NULL,
    version     integer     NOT NULL DEFAULT 1,
    update_time timestamptz NOT NULL DEFAULT now(),
    update_user uuid        NULL,
    CONSTRAINT ck_incident_setting_single CHECK (id = 1),
    CONSTRAINT ck_incident_setting_spec CHECK (jsonb_typeof(spec) = 'object')
);
COMMENT ON TABLE incident_setting IS '故障管理设置（单行 id=1）：各等级的响应时限 / 无进展提醒 / 首条公告时限 / 复盘期限、根因分类、兜底值班表、通知策略、风暴阈值等；结构见 biz/incident Settings';
COMMENT ON COLUMN incident_setting.spec IS '设置内容 jsonb（biz/incident.Settings）';
COMMENT ON COLUMN incident_setting.version IS '乐观锁版本号';
INSERT INTO incident_setting (id, spec) VALUES (1, '{}');

-- ---------------------------------------------------------------------------
-- 业务字典
-- ---------------------------------------------------------------------------

CREATE TABLE incident_business (
    id           uuid         PRIMARY KEY,
    code         varchar(64)  NOT NULL,
    name         varchar(128) NOT NULL,
    description  varchar(255) NOT NULL DEFAULT '',
    owner_id     uuid         NULL,
    schedule_id  uuid         NULL,
    groups       text[]       NOT NULL DEFAULT '{}',
    ci_id        uuid         NULL,
    label_values text[]       NOT NULL DEFAULT '{}',
    sort         integer      NOT NULL DEFAULT 100,
    enabled      boolean      NOT NULL DEFAULT true,
    version      integer      NOT NULL DEFAULT 1,
    create_time  timestamptz  NOT NULL DEFAULT now(),
    update_time  timestamptz  NOT NULL DEFAULT now(),
    delete_time  timestamptz  NULL,
    create_user  uuid         NULL,
    update_user  uuid         NULL,
    delete_user  uuid         NULL,
    CONSTRAINT ck_incident_business_labels CHECK (cardinality(label_values) <= 50),
    CONSTRAINT ck_incident_business_groups CHECK (cardinality(groups) <= 10)
);
COMMENT ON TABLE incident_business IS '业务字典：故障归属的业务 / 服务，带缺省值班表（指挥来源）、通知群与匹配标签值；软删';
COMMENT ON COLUMN incident_business.code IS '业务编码，未删除行内唯一，创建后不可改';
COMMENT ON COLUMN incident_business.owner_id IS '业务负责人（用户）';
COMMENT ON COLUMN incident_business.schedule_id IS '缺省值班表：该业务的故障指挥取其当前主班';
COMMENT ON COLUMN incident_business.groups IS '缺省通知群标识（$ 开头，如 $group:ops；对应通知管理里群机器人渠道的群），至多 10 个';
COMMENT ON COLUMN incident_business.ci_id IS '可选关联的 CMDB 资源（cmdb 内置应用模型后改为引用它）';
COMMENT ON COLUMN incident_business.label_values IS '匹配标签值：告警标签（触发规则指定的标签名）取到这些值即归为该业务，至多 50 个';
CREATE UNIQUE INDEX uk_incident_business_code ON incident_business (code) WHERE delete_time IS NULL;

-- ---------------------------------------------------------------------------
-- 故障编号计数（按日）
-- ---------------------------------------------------------------------------

CREATE TABLE incident_counter (
    day date    PRIMARY KEY,
    n   integer NOT NULL DEFAULT 0
);
COMMENT ON TABLE incident_counter IS '故障编号按日自增计数：INC-YYYYMMDD-NNN；行锁自增，回滚不留空号';

-- ---------------------------------------------------------------------------
-- 故障
-- ---------------------------------------------------------------------------

CREATE TABLE incident_record (
    id                 uuid         PRIMARY KEY,
    number             varchar(32)  NOT NULL,
    title              varchar(255) NOT NULL,
    severity           smallint     NOT NULL,
    status             smallint     NOT NULL DEFAULT 1,
    review_status      smallint     NOT NULL DEFAULT 0,
    source             smallint     NOT NULL DEFAULT 1,
    source_ref         varchar(128) NOT NULL DEFAULT '',
    external_key       varchar(128) NOT NULL DEFAULT '',
    business_id        uuid         NULL,
    rule_id            uuid         NULL,
    correlation_key    varchar(255) NOT NULL DEFAULT '',
    summary            text         NOT NULL DEFAULT '',
    ai_summary         text         NOT NULL DEFAULT '',
    root_cause_code    varchar(32)  NOT NULL DEFAULT '',
    root_cause         text         NOT NULL DEFAULT '',
    resolve_note       text         NOT NULL DEFAULT '',
    commander_id       uuid         NULL,
    affected_ci_ids    uuid[]       NOT NULL DEFAULT '{}',
    groups             text[]       NOT NULL DEFAULT '{}',
    is_drill           boolean      NOT NULL DEFAULT false,
    occur_time         timestamptz  NOT NULL,
    open_time          timestamptz  NOT NULL DEFAULT now(),
    respond_time       timestamptz  NULL,
    mitigate_time      timestamptz  NULL,
    first_resolve_time timestamptz  NULL,
    resolve_time       timestamptz  NULL,
    close_time         timestamptz  NULL,
    reopen_count       integer      NOT NULL DEFAULT 0,
    merged_into        uuid         NULL,
    escalation_level   smallint     NOT NULL DEFAULT 0,
    escalate_time      timestamptz  NULL,
    last_activity_time timestamptz  NOT NULL DEFAULT now(),
    stale_notify_time  timestamptz  NULL,
    update_count       integer      NOT NULL DEFAULT 0,
    auto_approve       boolean      NOT NULL DEFAULT true,
    agent_owner        uuid         NULL,
    agent_lease_until  timestamptz  NULL,
    agent_finding_count integer     NOT NULL DEFAULT 0,
    review_due         timestamptz  NULL,
    review_remind      smallint     NOT NULL DEFAULT 0,
    search             tsvector     GENERATED ALWAYS AS (to_tsvector('simple', coalesce(title, '') || ' ' || coalesce(summary, '') || ' ' || coalesce(root_cause, ''))) STORED,
    version            integer      NOT NULL DEFAULT 1,
    create_time        timestamptz  NOT NULL DEFAULT now(),
    update_time        timestamptz  NOT NULL DEFAULT now(),
    create_user        uuid         NULL,
    update_user        uuid         NULL,
    CONSTRAINT ck_incident_severity CHECK (severity BETWEEN 1 AND 4),
    CONSTRAINT ck_incident_status CHECK (status BETWEEN 1 AND 6),
    CONSTRAINT ck_incident_review CHECK (review_status BETWEEN 0 AND 3),
    CONSTRAINT ck_incident_source CHECK (source BETWEEN 1 AND 4),
    CONSTRAINT ck_incident_ai_summary CHECK (octet_length(ai_summary) <= 8192),
    CONSTRAINT ck_incident_cis CHECK (cardinality(affected_ci_ids) <= 500)
);
COMMENT ON TABLE incident_record IS '故障：唯一事实源。status 1=待响应 2=处理中 3=已缓解 4=已恢复 5=已关闭 6=已合并；review_status 0=不需要 1=待复盘 2=复盘中 3=已完成；source 1=手动 2=触发规则 3=开放接口 4=Agent';
COMMENT ON COLUMN incident_record.number IS '编号 INC-YYYYMMDD-NNN，唯一';
COMMENT ON COLUMN incident_record.severity IS '等级 1=S1 致命 2=S2 严重 3=S3 一般 4=S4 轻微（数字越小越严重）';
COMMENT ON COLUMN incident_record.source_ref IS '来源对象：触发规则 ID / 接入源 ID';
COMMENT ON COLUMN incident_record.external_key IS '开放接口的幂等键（与接入源一起唯一）';
COMMENT ON COLUMN incident_record.rule_id IS '自动开出时的触发规则';
COMMENT ON COLUMN incident_record.correlation_key IS '自动开出时的归并键：同规则同键的后续告警归并到本故障（活跃期间唯一）';
COMMENT ON COLUMN incident_record.commander_id IS '指挥（一人）；处理人与关注人在 incident_member';
COMMENT ON COLUMN incident_record.affected_ci_ids IS '受影响的 CMDB 资源（关联告警带入 + 手动添加）';
COMMENT ON COLUMN incident_record.groups IS '通知群标识（$ 开头，经通知路由的群机器人渠道投递）';
COMMENT ON COLUMN incident_record.is_drill IS '演练：不进统计，通知标题带 [演练]';
COMMENT ON COLUMN incident_record.occur_time IS '发生时间：关联告警中最早的首次触发时间；手动可改';
COMMENT ON COLUMN incident_record.first_resolve_time IS '首次恢复时间（重开不改）；resolve_time 为末次恢复，MTTR 按它算';
COMMENT ON COLUMN incident_record.last_activity_time IS '最近一条公告或人工备注的时间（无进展提醒据此判定）';
COMMENT ON COLUMN incident_record.auto_approve IS '自动批准是否仍开启；false=已熔断（自动执行失败或验证窗口内恶化），后续建议全部人工审批';
COMMENT ON COLUMN incident_record.agent_owner IS 'Agent 租约持有者（用户）；同一时刻至多一个 Agent 任务';
COMMENT ON COLUMN incident_record.review_remind IS '复盘到期提醒阶段：0 未提醒 1 已提醒到期前 2 已提醒到期';
CREATE UNIQUE INDEX uk_incident_number ON incident_record (number);
CREATE UNIQUE INDEX uk_incident_correlation ON incident_record (rule_id, correlation_key) WHERE rule_id IS NOT NULL AND status <= 3;
CREATE UNIQUE INDEX uk_incident_external ON incident_record (source_ref, external_key) WHERE source = 3 AND external_key <> '';
CREATE INDEX idx_incident_list ON incident_record (status, severity, open_time DESC);
CREATE INDEX idx_incident_business ON incident_record (business_id, open_time DESC);
CREATE INDEX idx_incident_commander ON incident_record (commander_id) WHERE status <= 4;
CREATE INDEX idx_incident_review ON incident_record (review_status, review_due) WHERE review_status IN (1, 2);
CREATE INDEX idx_incident_open_time ON incident_record (open_time DESC);
CREATE INDEX idx_incident_search ON incident_record USING GIN (search);
CREATE INDEX idx_incident_title_trgm ON incident_record USING GIN (title gin_trgm_ops);

CREATE TABLE incident_member (
    incident_id uuid        NOT NULL REFERENCES incident_record (id),
    user_id     uuid        NOT NULL,
    role        smallint    NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    create_user uuid        NULL,
    PRIMARY KEY (incident_id, user_id),
    CONSTRAINT ck_incident_member_role CHECK (role IN (2, 3))
);
COMMENT ON TABLE incident_member IS '故障成员：role 2=处理人 3=关注人（指挥在 incident.commander_id）；物理删除';
CREATE INDEX idx_incident_member_user ON incident_member (user_id);

CREATE TABLE incident_event (
    id          uuid        PRIMARY KEY,
    incident_id uuid        NOT NULL REFERENCES incident_record (id),
    type        varchar(32) NOT NULL,
    actor_type  smallint    NOT NULL DEFAULT 2,
    actor_id    uuid        NULL,
    detail      jsonb       NOT NULL DEFAULT '{}',
    pinned      boolean     NOT NULL DEFAULT false,
    create_time timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_incident_event_actor CHECK (actor_type BETWEEN 1 AND 3),
    CONSTRAINT ck_incident_event_detail CHECK (jsonb_typeof(detail) = 'object')
);
COMMENT ON TABLE incident_event IS '故障时间线（append-only；只有 pinned 允许更新）：开启 / 等级 / 状态 / 角色 / 备注 / 告警关联 / 变更关联 / 公告 / 动作建议 / 预案执行 / AI 产出 / Agent 发现 / 合并 / 复盘…；actor_type 1=用户 2=系统 3=Agent';
COMMENT ON COLUMN incident_event.type IS '类型字面量见 biz/incident（Event* 常量）';
COMMENT ON COLUMN incident_event.detail IS '细节 jsonb（biz/incident.EventDetail）';
CREATE INDEX idx_incident_event_incident ON incident_event (incident_id, create_time, id);

CREATE TABLE incident_alert (
    incident_id   uuid         NOT NULL REFERENCES incident_record (id),
    alert_id      uuid         NOT NULL,
    method        smallint     NOT NULL,
    released      boolean      NOT NULL DEFAULT false,
    rule_name     varchar(255) NOT NULL DEFAULT '',
    severity      smallint     NOT NULL DEFAULT 1,
    ci_id         uuid         NULL,
    first_fire_time timestamptz NULL,
    attach_time   timestamptz  NOT NULL DEFAULT now(),
    attach_user   uuid         NULL,
    PRIMARY KEY (incident_id, alert_id),
    CONSTRAINT ck_incident_alert_method CHECK (method BETWEEN 1 AND 4)
);
COMMENT ON TABLE incident_alert IS '故障 ↔ 告警事件：method 1=触发规则 2=手动 3=Agent 4=合并迁入；released=true 表示故障已关闭 / 合并、告警可被别的故障关联。rule_name / severity / ci_id / first_fire_time 为关联时的快照，当前状态经监控域读取';
CREATE UNIQUE INDEX uk_incident_alert_active ON incident_alert (alert_id) WHERE NOT released;
CREATE INDEX idx_incident_alert_alert ON incident_alert (alert_id);

CREATE TABLE incident_change_ref (
    id          uuid         PRIMARY KEY,
    incident_id uuid         NOT NULL REFERENCES incident_record (id),
    domain      varchar(16)  NOT NULL,
    kind        varchar(32)  NOT NULL,
    ref_id      varchar(64)  NOT NULL,
    summary     varchar(255) NOT NULL DEFAULT '',
    ref_time    timestamptz  NULL,
    relation    smallint     NOT NULL DEFAULT 0,
    create_user uuid         NULL,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_incident_change_relation CHECK (relation BETWEEN 0 AND 3),
    CONSTRAINT uk_incident_change_ref UNIQUE (incident_id, domain, ref_id)
);
COMMENT ON TABLE incident_change_ref IS '故障 ↔ 其他域对象的弱引用（变更关联）：domain k8s / job / workflow / cmdb / cicd；relation 0=未标 1=疑似原因 2=已确认原因 3=无关；详情经各域接口实时读取';

CREATE TABLE incident_candidate (
    id              uuid         PRIMARY KEY,
    rule_id         uuid         NOT NULL,
    correlation_key varchar(255) NOT NULL,
    alert_ids       uuid[]       NOT NULL DEFAULT '{}',
    ci_ids          uuid[]       NOT NULL DEFAULT '{}',
    status          smallint     NOT NULL DEFAULT 1,
    incident_id     uuid         NULL,
    first_time      timestamptz  NOT NULL DEFAULT now(),
    update_time     timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_incident_candidate_status CHECK (status BETWEEN 1 AND 4),
    CONSTRAINT ck_incident_candidate_alerts CHECK (cardinality(alert_ids) <= 1000)
);
COMMENT ON TABLE incident_candidate IS '候选故障：触发规则命中但未达开启门槛的归并桶；status 1=待定 2=已转正 3=已忽略 4=已过期';
CREATE UNIQUE INDEX uk_incident_candidate_open ON incident_candidate (rule_id, correlation_key) WHERE status = 1;
CREATE INDEX idx_incident_candidate_status ON incident_candidate (status, update_time);

-- ---------------------------------------------------------------------------
-- 触发规则 / 预案
-- ---------------------------------------------------------------------------

CREATE TABLE incident_rule (
    id           uuid         PRIMARY KEY,
    name         varchar(128) NOT NULL,
    description  varchar(255) NOT NULL DEFAULT '',
    sort         integer      NOT NULL DEFAULT 100,
    enabled      boolean      NOT NULL DEFAULT true,
    spec         jsonb        NOT NULL,
    version      integer      NOT NULL DEFAULT 1,
    create_time  timestamptz  NOT NULL DEFAULT now(),
    update_time  timestamptz  NOT NULL DEFAULT now(),
    delete_time  timestamptz  NULL,
    create_user  uuid         NULL,
    update_user  uuid         NULL,
    delete_user  uuid         NULL,
    CONSTRAINT ck_incident_rule_spec CHECK (jsonb_typeof(spec) = 'object')
);
COMMENT ON TABLE incident_rule IS '触发规则：告警满足什么条件、怎样归并成故障（匹配条件 / 归并键 / 窗口 / 开启门槛 / 故障属性 / 响应配置 / 自动处置 / 自动恢复）；多条命中取 sort 最小者；软删';
COMMENT ON COLUMN incident_rule.spec IS '规则内容 jsonb（biz/incident.RuleSpec）';
CREATE UNIQUE INDEX uk_incident_rule_name ON incident_rule (name) WHERE delete_time IS NULL;

CREATE TABLE incident_playbook (
    id            uuid         PRIMARY KEY,
    name          varchar(128) NOT NULL,
    description   varchar(255) NOT NULL DEFAULT '',
    doc           text         NOT NULL DEFAULT '',
    steps         jsonb        NOT NULL DEFAULT '[]',
    match         jsonb        NOT NULL DEFAULT '{}',
    agent_user_id uuid         NULL,
    enabled       boolean      NOT NULL DEFAULT true,
    version       integer      NOT NULL DEFAULT 1,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    delete_time   timestamptz  NULL,
    create_user   uuid         NULL,
    update_user   uuid         NULL,
    delete_user   uuid         NULL,
    CONSTRAINT ck_incident_playbook_doc CHECK (octet_length(doc) <= 65536),
    CONSTRAINT ck_incident_playbook_steps CHECK (jsonb_typeof(steps) = 'array' AND jsonb_array_length(steps) <= 20)
);
COMMENT ON TABLE incident_playbook IS '预案：处置手册（Markdown）+ 可执行步骤（流程定义 / 作业模板 + 参数映射 + 审批策略）+ 适用条件 + 代理执行人；软删';
COMMENT ON COLUMN incident_playbook.steps IS '步骤 [{"name","kind":"job|workflow","ref","params":{},"approval":1|2|3,"risk":1|2|3}]（biz/incident.PlaybookStep）';
COMMENT ON COLUMN incident_playbook.agent_user_id IS '代理执行人：作业 / 流程以该用户身份发起，权限与风险规则按其判定';
CREATE UNIQUE INDEX uk_incident_playbook_name ON incident_playbook (name) WHERE delete_time IS NULL;

-- ---------------------------------------------------------------------------
-- 动作建议
-- ---------------------------------------------------------------------------

CREATE TABLE incident_proposal (
    id            uuid         PRIMARY KEY,
    incident_id   uuid         NOT NULL REFERENCES incident_record (id),
    title         varchar(255) NOT NULL,
    reason        text         NOT NULL DEFAULT '',
    evidence      jsonb        NOT NULL DEFAULT '[]',
    action        jsonb        NOT NULL,
    playbook_id   uuid         NULL,
    step_index    integer      NOT NULL DEFAULT 0,
    risk          smallint     NOT NULL DEFAULT 2,
    idem_key      varchar(128) NOT NULL,
    status        smallint     NOT NULL DEFAULT 1,
    auto          boolean      NOT NULL DEFAULT false,
    proposer_type smallint     NOT NULL DEFAULT 1,
    proposer_id   uuid         NULL,
    decided_by    uuid         NULL,
    decided_time  timestamptz  NULL,
    decision_note varchar(255) NOT NULL DEFAULT '',
    expire_time   timestamptz  NOT NULL,
    exec_kind     varchar(16)  NOT NULL DEFAULT '',
    exec_ref      varchar(64)  NOT NULL DEFAULT '',
    exec_user     uuid         NULL,
    result        text         NOT NULL DEFAULT '',
    verify_until  timestamptz  NULL,
    verify_result jsonb        NULL,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_incident_proposal_status CHECK (status BETWEEN 1 AND 8),
    CONSTRAINT ck_incident_proposal_risk CHECK (risk BETWEEN 1 AND 3),
    CONSTRAINT ck_incident_proposal_action CHECK (jsonb_typeof(action) = 'object'),
    CONSTRAINT ck_incident_proposal_evidence CHECK (jsonb_typeof(evidence) = 'array')
);
COMMENT ON TABLE incident_proposal IS '动作建议：人 / AI / Agent 提出的「建议执行某个预案步骤 / 作业 / 流程」。status 1=待审批 2=已批准 3=已拒绝 4=执行中 5=成功 6=失败 7=已过期 8=已取消；proposer_type 1=用户 2=系统 3=Agent；action {"kind":"job|workflow","ref","params","ci_ids"}';
COMMENT ON COLUMN incident_proposal.idem_key IS '幂等键：同一故障内相同动作与参数只有一条待审 / 进行中的建议';
COMMENT ON COLUMN incident_proposal.exec_ref IS '执行对象：作业执行 ID / 流程实例 ID';
COMMENT ON COLUMN incident_proposal.verify_result IS '验证窗口结果 {"alerts_before","alerts_after","resolved","new_alerts","worse"}';
CREATE UNIQUE INDEX uk_incident_proposal_idem ON incident_proposal (incident_id, idem_key) WHERE status IN (1, 2, 4);
CREATE INDEX idx_incident_proposal_incident ON incident_proposal (incident_id, create_time DESC);
CREATE INDEX idx_incident_proposal_pending ON incident_proposal (expire_time) WHERE status = 1;
CREATE INDEX idx_incident_proposal_running ON incident_proposal (update_time) WHERE status = 4;
CREATE INDEX idx_incident_proposal_verify ON incident_proposal (verify_until) WHERE status = 5 AND verify_result IS NULL;

-- ---------------------------------------------------------------------------
-- 公告 / 分享链接
-- ---------------------------------------------------------------------------

CREATE TABLE incident_update (
    id          uuid        PRIMARY KEY,
    incident_id uuid        NOT NULL REFERENCES incident_record (id),
    kind        smallint    NOT NULL,
    content     text        NOT NULL,
    author_id   uuid        NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_incident_update_kind CHECK (kind BETWEEN 1 AND 3),
    CONSTRAINT ck_incident_update_content CHECK (octet_length(content) <= 16384)
);
COMMENT ON TABLE incident_update IS '公告（面向干系人的阶段性说明）：kind 1=开启 2=进展 3=恢复；发出即写通知事件';
CREATE INDEX idx_incident_update_incident ON incident_update (incident_id, create_time DESC);

CREATE TABLE incident_share (
    id               uuid         PRIMARY KEY,
    incident_id      uuid         NOT NULL REFERENCES incident_record (id),
    name             varchar(128) NOT NULL,
    token_hash       bytea        NOT NULL,
    token_sealed     bytea        NOT NULL,
    expire_time      timestamptz  NULL,
    access_count     integer      NOT NULL DEFAULT 0,
    last_access_time timestamptz  NULL,
    create_user      uuid         NULL,
    create_time      timestamptz  NOT NULL DEFAULT now(),
    delete_time      timestamptz  NULL
);
COMMENT ON TABLE incident_share IS '故障公告的只读分享链接：令牌只存哈希（匿名访问查找）与密文（再次查看）；可过期、可重置、可作废';
CREATE UNIQUE INDEX uk_incident_share_token ON incident_share (token_hash);
CREATE INDEX idx_incident_share_incident ON incident_share (incident_id) WHERE delete_time IS NULL;

-- ---------------------------------------------------------------------------
-- 复盘 / 改进项
-- ---------------------------------------------------------------------------

CREATE TABLE incident_postmortem (
    incident_id        uuid        PRIMARY KEY REFERENCES incident_record (id),
    doc                jsonb       NOT NULL DEFAULT '{}',
    current_version    integer     NOT NULL DEFAULT 0,
    review_instance_id uuid        NULL,
    update_user        uuid        NULL,
    update_time        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_incident_postmortem_doc CHECK (jsonb_typeof(doc) = 'object' AND octet_length(doc::text) <= 262144)
);
COMMENT ON TABLE incident_postmortem IS '复盘文档（当前草稿）：章节结构见 biz/incident.PostmortemDoc；完成 / 修订时生成不可变版本';

CREATE TABLE incident_postmortem_version (
    id          uuid         PRIMARY KEY,
    incident_id uuid         NOT NULL REFERENCES incident_record (id),
    version     integer      NOT NULL,
    doc         jsonb        NOT NULL,
    note        varchar(255) NOT NULL DEFAULT '',
    create_user uuid         NULL,
    create_time timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uk_incident_postmortem_version UNIQUE (incident_id, version)
);
COMMENT ON TABLE incident_postmortem_version IS '复盘文档的不可变版本：v1 为首次完成，之后每次修订 +1';

CREATE TABLE incident_action_item (
    id            uuid         PRIMARY KEY,
    incident_id   uuid         NOT NULL REFERENCES incident_record (id),
    title         varchar(255) NOT NULL,
    kind          smallint     NOT NULL DEFAULT 1,
    owner_id      uuid         NULL,
    due_date      date         NULL,
    status        smallint     NOT NULL DEFAULT 1,
    ticket_id     uuid         NULL,
    ticket_number varchar(64)  NOT NULL DEFAULT '',
    create_user   uuid         NULL,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_incident_action_item_kind CHECK (kind BETWEEN 1 AND 4),
    CONSTRAINT ck_incident_action_item_status CHECK (status IN (1, 2))
);
COMMENT ON TABLE incident_action_item IS '复盘改进项：kind 1=修复 2=预防 3=检测 4=流程；status 1=待完成 2=已完成；建单后记工单号并由巡检同步完成状态';
CREATE INDEX idx_incident_action_item_incident ON incident_action_item (incident_id);
CREATE INDEX idx_incident_action_item_ticket ON incident_action_item (ticket_id) WHERE ticket_id IS NOT NULL AND status = 1;

-- ---------------------------------------------------------------------------
-- 接入源（开放接口） / Webhook 订阅 / 出站箱
-- ---------------------------------------------------------------------------

CREATE TABLE incident_source (
    id               uuid         PRIMARY KEY,
    name             varchar(128) NOT NULL,
    description      varchar(255) NOT NULL DEFAULT '',
    token_hash       bytea        NOT NULL,
    token_prefix     varchar(16)  NOT NULL,
    default_severity smallint     NOT NULL DEFAULT 3,
    business_id      uuid         NULL,
    groups           text[]       NOT NULL DEFAULT '{}',
    allowed_ips      text[]       NOT NULL DEFAULT '{}',
    enabled          boolean      NOT NULL DEFAULT true,
    last_used_time   timestamptz  NULL,
    version          integer      NOT NULL DEFAULT 1,
    create_time      timestamptz  NOT NULL DEFAULT now(),
    update_time      timestamptz  NOT NULL DEFAULT now(),
    delete_time      timestamptz  NULL,
    create_user      uuid         NULL,
    update_user      uuid         NULL,
    delete_user      uuid         NULL,
    CONSTRAINT ck_incident_source_severity CHECK (default_severity BETWEEN 2 AND 4),
    CONSTRAINT ck_incident_source_ips CHECK (cardinality(allowed_ips) <= 50)
);
COMMENT ON TABLE incident_source IS '开放接口接入源：外部系统凭令牌（库内只存 SHA-256）开 / 更新 / 恢复故障；缺省等级不得为 S1；allowed_ips 为空不限来源';
CREATE UNIQUE INDEX uk_incident_source_name ON incident_source (name) WHERE delete_time IS NULL;
CREATE UNIQUE INDEX uk_incident_source_token ON incident_source (token_hash);

CREATE TABLE incident_webhook (
    id           uuid         PRIMARY KEY,
    name         varchar(128) NOT NULL,
    url          varchar(512) NOT NULL,
    secret       bytea        NULL,
    events       text[]       NOT NULL DEFAULT '{}',
    enabled      boolean      NOT NULL DEFAULT true,
    version      integer      NOT NULL DEFAULT 1,
    create_time  timestamptz  NOT NULL DEFAULT now(),
    update_time  timestamptz  NOT NULL DEFAULT now(),
    delete_time  timestamptz  NULL,
    create_user  uuid         NULL,
    update_user  uuid         NULL,
    delete_user  uuid         NULL
);
COMMENT ON TABLE incident_webhook IS 'Webhook 订阅（Agent / 外部系统感知故障）：领域事件经出站箱投递到 url，带 HMAC-SHA256 签名（secret 为 AES-256-GCM 密文）；events 为订阅的事件类型，空=全部';
CREATE UNIQUE INDEX uk_incident_webhook_name ON incident_webhook (name) WHERE delete_time IS NULL;

CREATE TABLE incident_outbox (
    id          uuid         PRIMARY KEY,
    webhook_id  uuid         NOT NULL,
    incident_id uuid         NOT NULL,
    event_type  varchar(64)  NOT NULL,
    payload     jsonb        NOT NULL,
    status      smallint     NOT NULL DEFAULT 1,
    attempts    integer      NOT NULL DEFAULT 0,
    next_time   timestamptz  NOT NULL DEFAULT now(),
    last_error  varchar(512) NOT NULL DEFAULT '',
    done_time   timestamptz  NULL,
    create_time timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_incident_outbox_status CHECK (status BETWEEN 1 AND 3)
);
COMMENT ON TABLE incident_outbox IS 'Webhook 出站箱：与业务写同事务入箱，后台循环领取投递（租约 + 退避）；status 1=待投递 2=已投递 3=已丢弃';
CREATE INDEX idx_incident_outbox_pending ON incident_outbox (next_time) WHERE status = 1;
CREATE INDEX idx_incident_outbox_done ON incident_outbox (done_time) WHERE status <> 1;

-- ---------------------------------------------------------------------------
-- 动作发件箱（AI 摘要 / 建议执行 / 验证 / 建单回写） / 关联器游标
-- ---------------------------------------------------------------------------

CREATE TABLE incident_action (
    id          uuid         PRIMARY KEY,
    incident_id uuid         NOT NULL,
    kind        smallint     NOT NULL,
    ref_id      uuid         NULL,
    payload     jsonb        NOT NULL DEFAULT '{}',
    status      smallint     NOT NULL DEFAULT 1,
    attempts    integer      NOT NULL DEFAULT 0,
    next_time   timestamptz  NOT NULL DEFAULT now(),
    last_error  varchar(512) NOT NULL DEFAULT '',
    done_time   timestamptz  NULL,
    create_time timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_incident_action_kind CHECK (kind BETWEEN 1 AND 4),
    CONSTRAINT ck_incident_action_status CHECK (status BETWEEN 1 AND 3)
);
COMMENT ON TABLE incident_action IS '故障动作发件箱：kind 1=AI 摘要 2=执行已批准的动作建议（ref_id=建议） 3=动作验证窗口检查 4=恢复时回写工单；status 1=待执行 2=已完成 3=已失败（次数用尽）；SKIP LOCKED 领取、租约、退避';
CREATE UNIQUE INDEX uk_incident_action_pending ON incident_action (incident_id, kind, ref_id) NULLS NOT DISTINCT WHERE status = 1;
CREATE INDEX idx_incident_action_pending ON incident_action (next_time) WHERE status = 1;

CREATE TABLE incident_cursor (
    name        varchar(32) PRIMARY KEY,
    seq         bigint      NOT NULL DEFAULT 0,
    update_time timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE incident_cursor IS '后台游标：alert_feed = 关联器已处理到的 monitor_alert_event.seq';
INSERT INTO incident_cursor (name, seq) SELECT 'alert_feed', COALESCE(max(seq), 0) FROM monitor_alert_event;

-- ---------------------------------------------------------------------------
-- 内置通知路由与模板（biz_id=default）：事件类型 incident.<s1|s2|s3|s4>.<动作>，等级编入第二段以便订阅 / 路由按等级匹配
-- ---------------------------------------------------------------------------

INSERT INTO notify_route (id, biz_id, name, description, event_pattern, severity_min, priority, channel_ids, recipient_policy, aggregate_window_s)
VALUES ('018f3e1a-0cc1-7000-8000-000000000004', 'default', '故障管理通知',
        '故障开启 / 升级 / 状态变更 / 公告 / 指派 / 待审批 / 复盘到期：发给故障的指挥、处理人、关注人与通知群', 'incident.*', 1, 4,
        ARRAY(SELECT id FROM notify_channel
              WHERE biz_id = 'default' AND type IN ('email', 'feishu', 'dingtalk', 'wecom') AND enabled AND delete_time IS NULL ORDER BY create_time),
        '{"subscription": true}', 0);

-- 模板：等级 × 动作 的笛卡尔积，正文相同、按 payload 字段展示；ID 固定前缀 018f3e1a-0cc4-7000-8000-，down 据此删除。
INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body)
SELECT ('018f3e1a-0cc4-7000-8000-' || lpad(to_hex(row_number() OVER ()), 12, '0'))::uuid, 'default',
       'incident.' || s.code || '.' || a.action, '*', 'text', a.subject, a.body
FROM (VALUES ('s1'), ('s2'), ('s3'), ('s4')) AS s(code)
CROSS JOIN (VALUES
  ('opened', '【{{.Payload.severity_label}} 故障{{if .Payload.drill}} · 演练{{end}}】{{.Payload.number}} {{.Payload.title}}',
   '故障已开启{{if .Payload.drill}}（演练）{{end}}。
编号：{{.Payload.number}}
标题：{{.Payload.title}}
等级：{{.Payload.severity_label}}
业务：{{.Payload.business_name}}
指挥：{{.Payload.commander_name}}
发生时间：{{.Payload.occur_time}}
受影响资源：{{.Payload.affected_cis}}
关联告警：{{.Payload.alert_count}} 条
详情：{{.Payload.url}}'),
  ('escalated', '【{{.Payload.severity_label}} 故障未响应升级】{{.Payload.number}} {{.Payload.title}}',
   '故障开启 {{.Payload.elapsed}} 仍无人响应，已升级到第 {{.Payload.level}} 级，请尽快处理。
编号：{{.Payload.number}}
等级：{{.Payload.severity_label}}
业务：{{.Payload.business_name}}
指挥：{{.Payload.commander_name}}
详情：{{.Payload.url}}'),
  ('stale', '【{{.Payload.severity_label}} 故障无进展提醒】{{.Payload.number}} {{.Payload.title}}',
   '故障处理中，已有 {{.Payload.elapsed}} 没有新的公告或备注{{if .Payload.need_update}}，且尚未发出首条公告{{end}}。
编号：{{.Payload.number}}
当前状态：{{.Payload.status_label}}
详情：{{.Payload.url}}'),
  ('severity_changed', '【故障等级变更 → {{.Payload.severity_label}}】{{.Payload.number}} {{.Payload.title}}',
   '故障等级由 {{.Payload.from_label}} 调整为 {{.Payload.severity_label}}。
编号：{{.Payload.number}}
操作人：{{.Payload.actor_name}}
说明：{{.Payload.text}}
详情：{{.Payload.url}}'),
  ('status_changed', '【故障{{.Payload.status_label}}】{{.Payload.number}} {{.Payload.title}}',
   '故障状态变为「{{.Payload.status_label}}」。
编号：{{.Payload.number}}
等级：{{.Payload.severity_label}}
操作人：{{.Payload.actor_name}}
说明：{{.Payload.text}}
详情：{{.Payload.url}}'),
  ('update', '【故障公告 · {{.Payload.update_kind_label}}】{{.Payload.number}} {{.Payload.title}}',
   '{{.Payload.text}}

编号：{{.Payload.number}}
等级：{{.Payload.severity_label}}
当前状态：{{.Payload.status_label}}
发布人：{{.Payload.actor_name}}
详情：{{.Payload.url}}'),
  ('assigned', '【故障指派】{{.Payload.number}} {{.Payload.title}}',
   '你被指派为故障的{{.Payload.role_label}}。
编号：{{.Payload.number}}
等级：{{.Payload.severity_label}}
当前状态：{{.Payload.status_label}}
详情：{{.Payload.url}}'),
  ('proposal', '【故障动作待审批】{{.Payload.number}} {{.Payload.proposal_title}}',
   '有一条动作建议等待指挥审批。
故障：{{.Payload.number}} {{.Payload.title}}
动作：{{.Payload.proposal_title}}
提出：{{.Payload.actor_name}}
风险：{{.Payload.risk_label}}
理由：{{.Payload.text}}
详情：{{.Payload.url}}'),
  ('review_due', '【故障复盘到期提醒】{{.Payload.number}} {{.Payload.title}}',
   '故障复盘{{if .Payload.overdue}}已超期{{else}}即将到期{{end}}（期限 {{.Payload.review_due}}），请完成复盘文档。
编号：{{.Payload.number}}
等级：{{.Payload.severity_label}}
详情：{{.Payload.url}}'),
  ('merged', '【故障已合并】{{.Payload.number}} → {{.Payload.target_number}}',
   '故障 {{.Payload.number}} 已合并到 {{.Payload.target_number}}，后续进展请关注目标故障。
操作人：{{.Payload.actor_name}}
详情：{{.Payload.url}}'),
  ('storm', '【故障风暴归并】{{.Payload.rule_name}}',
   '触发规则「{{.Payload.rule_name}}」在 1 分钟内开出的故障超过阈值，已停止开新故障，后续告警归并到 {{.Payload.number}}，请检查规则配置。
详情：{{.Payload.url}}'),
  ('engine', '【故障关联器告警】{{.Payload.text}}',
   '{{.Payload.text}}
详情：{{.Payload.url}}')
) AS a(action, subject, body);

UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
