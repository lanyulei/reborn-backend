-- 0075 作业中心二期 / 三期：快速执行（命令 / 脚本 / 文件分发）、脚本库、高危命令规则与全局策略、
-- 审批（复用流程引擎）、定时作业、触发器（开放接口 / 告警自愈）与触发记录、作业切片、执行结束通知。
-- jsonb 列结构与状态字面量的唯一事实源是 kit/jobcontract。约定同 0070。

-- ---------------------------------------------------------------------------
-- 既有表的扩展
-- ---------------------------------------------------------------------------

ALTER TABLE job_template
    ADD COLUMN require_approval boolean NOT NULL DEFAULT false,
    ADD COLUMN slice_size       integer NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_job_template_slice_size CHECK (slice_size BETWEEN 0 AND 500),
    ADD CONSTRAINT ck_job_template_slice_exclusive CHECK (NOT (exclusive AND slice_size > 0));
COMMENT ON COLUMN job_template.require_approval IS '是否每次执行都须审批（预检模式除外）';
COMMENT ON COLUMN job_template.slice_size IS '作业切片的每片主机数；0=不切片。切片后一次提交拆成多条执行，可由多个节点并行执行；与禁止并发互斥';

ALTER TABLE job_execution
    ALTER COLUMN template_id DROP NOT NULL,
    ADD COLUMN kind                 smallint     NOT NULL DEFAULT 1,
    ADD COLUMN trigger_ref          varchar(128) NOT NULL DEFAULT '',
    ADD COLUMN group_id             uuid         NULL,
    ADD COLUMN slice_no             integer      NOT NULL DEFAULT 0,
    ADD COLUMN slice_total          integer      NOT NULL DEFAULT 0,
    ADD COLUMN approval_reason      varchar(512) NOT NULL DEFAULT '',
    ADD COLUMN approval_instance_id uuid         NULL,
    ADD COLUMN risk_hits            jsonb        NOT NULL DEFAULT '[]',
    ADD COLUMN notify_time          timestamptz  NULL,
    DROP CONSTRAINT ck_job_execution_trigger,
    DROP CONSTRAINT ck_job_execution_status,
    DROP CONSTRAINT ck_job_execution_finish,
    ADD CONSTRAINT ck_job_execution_kind CHECK (kind BETWEEN 1 AND 4),
    ADD CONSTRAINT ck_job_execution_template CHECK ((kind = 1) = (template_id IS NOT NULL)),
    ADD CONSTRAINT ck_job_execution_trigger CHECK (trigger BETWEEN 1 AND 4),
    ADD CONSTRAINT ck_job_execution_status CHECK (status BETWEEN 1 AND 10),
    ADD CONSTRAINT ck_job_execution_finish CHECK ((status IN (1, 2, 9)) = (finish_time IS NULL)),
    ADD CONSTRAINT ck_job_execution_risk_hits CHECK (jsonb_typeof(risk_hits) = 'array');
COMMENT ON COLUMN job_execution.template_id IS '作业模板 job_template.id；快速执行为 NULL';
COMMENT ON COLUMN job_execution.template_name IS '提交时的模板名称；快速执行为其显示名（命令摘要 / 脚本名 / 文件名）';
COMMENT ON COLUMN job_execution.kind IS '作业类型：1=作业模板 2=执行命令 3=执行脚本 4=分发文件';
COMMENT ON COLUMN job_execution.trigger IS '触发来源：1=手动 2=定时作业 3=开放接口（流程节点等） 4=告警自愈';
COMMENT ON COLUMN job_execution.trigger_ref IS '触发来源的对象 ID：定时作业 ID / 触发器 ID；手动为空';
COMMENT ON COLUMN job_execution.status IS '状态：1=排队中 2=执行中 3=成功 4=失败 5=部分失败 6=已取消 7=超时 8=结果未知（节点失联） 9=待审批 10=审批未通过';
COMMENT ON COLUMN job_execution.group_id IS '作业切片的分组 ID：同一次提交拆出的各条执行相同；未切片为 NULL';
COMMENT ON COLUMN job_execution.slice_no IS '切片序号，从 1 开始；未切片为 0';
COMMENT ON COLUMN job_execution.slice_total IS '本次提交的切片总数；未切片为 0';
COMMENT ON COLUMN job_execution.approval_reason IS '需要审批的原因（模板要求 / 受保护环境 / 命中高危规则）；无需审批为空';
COMMENT ON COLUMN job_execution.approval_instance_id IS '审批流程实例 workflow_instance.id；无需审批为 NULL';
COMMENT ON COLUMN job_execution.risk_hits IS '提交时命中的高危规则（kit/jobcontract.RiskHit 数组）';
COMMENT ON COLUMN job_execution.notify_time IS '执行结束通知的发出时间；NULL=尚未通知';
-- 存量的已结束执行不补发通知。
UPDATE job_execution SET notify_time = now() WHERE status NOT IN (1, 2);
-- 互斥位：待审批的互斥执行同样占位（审批通过后不会与别的执行撞车）。
DROP INDEX uk_job_execution_exclusive;
CREATE UNIQUE INDEX uk_job_execution_exclusive ON job_execution (template_id) WHERE exclusive AND status IN (1, 2, 9);
-- 未结束的执行（统计页的「进行中」、信号队列）：0070 的队列索引只覆盖排队中。
CREATE INDEX idx_job_execution_active ON job_execution (status) WHERE status IN (1, 2, 9);
CREATE INDEX idx_job_execution_group ON job_execution (group_id) WHERE group_id IS NOT NULL;
CREATE INDEX idx_job_execution_notify ON job_execution (finish_time) WHERE notify_time IS NULL AND status NOT IN (1, 2, 9);
CREATE INDEX idx_job_execution_trigger_ref ON job_execution (trigger_ref, create_time DESC) WHERE trigger_ref <> '';

CREATE TABLE job_execution_payload (
    execution_id uuid  PRIMARY KEY REFERENCES job_execution (id) ON DELETE CASCADE,
    content      bytea NOT NULL
);
COMMENT ON TABLE job_execution_payload IS '分发文件的内容快照：提交时从文件服务读出并固化，引擎派发时随任务下发给执行节点';
COMMENT ON COLUMN job_execution_payload.execution_id IS '所属执行 job_execution.id';
COMMENT ON COLUMN job_execution_payload.content IS '文件内容，至多 4MB；大小与 SHA-256 记在执行的 spec.adhoc 里';

-- ---------------------------------------------------------------------------
-- 配置表
-- ---------------------------------------------------------------------------

CREATE TABLE job_script (
    id          uuid         PRIMARY KEY,
    name        varchar(64)  NOT NULL,
    interpreter varchar(16)  NOT NULL DEFAULT 'bash',
    content     text         NOT NULL,
    description varchar(255) NOT NULL DEFAULT '',
    version     integer      NOT NULL DEFAULT 1,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL,
    CONSTRAINT ck_job_script_interpreter CHECK (interpreter IN ('bash', 'sh', 'python3'))
);
COMMENT ON TABLE job_script IS '脚本库：可复用的脚本，快速执行时引用；执行时内容固化进执行记录，之后改脚本不影响已提交的执行';
COMMENT ON COLUMN job_script.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN job_script.name IS '名称，未删除行内唯一';
COMMENT ON COLUMN job_script.interpreter IS '解释器：bash sh python3';
COMMENT ON COLUMN job_script.content IS '脚本内容，至多 256KB';
COMMENT ON COLUMN job_script.description IS '说明';
COMMENT ON COLUMN job_script.version IS '版本号，每次更新 +1（兼作乐观锁）';
COMMENT ON COLUMN job_script.create_time IS '创建时间';
COMMENT ON COLUMN job_script.update_time IS '最后更新时间';
COMMENT ON COLUMN job_script.delete_time IS '软删除时间，NULL=未删除';
COMMENT ON COLUMN job_script.create_user IS '创建人用户 ID';
COMMENT ON COLUMN job_script.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN job_script.delete_user IS '删除人用户 ID';
CREATE UNIQUE INDEX uk_job_script_name ON job_script (name) WHERE delete_time IS NULL;

CREATE TABLE job_risk_rule (
    id          uuid         PRIMARY KEY,
    name        varchar(64)  NOT NULL,
    pattern     varchar(512) NOT NULL,
    action      smallint     NOT NULL,
    enabled     boolean      NOT NULL DEFAULT true,
    description varchar(255) NOT NULL DEFAULT '',
    version     integer      NOT NULL DEFAULT 1,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL,
    CONSTRAINT ck_job_risk_rule_action CHECK (action IN (1, 2, 3))
);
COMMENT ON TABLE job_risk_rule IS '高危命令规则：对快速执行的命令与脚本内容做正则匹配，命中后拦截、转审批或提醒';
COMMENT ON COLUMN job_risk_rule.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN job_risk_rule.name IS '名称，未删除行内唯一';
COMMENT ON COLUMN job_risk_rule.pattern IS '正则表达式（RE2 语法），在命令 / 脚本内容里查找';
COMMENT ON COLUMN job_risk_rule.action IS '命中后的动作：1=拦截 2=须审批 3=仅提醒';
COMMENT ON COLUMN job_risk_rule.enabled IS '是否启用';
COMMENT ON COLUMN job_risk_rule.description IS '说明（命中时展示给执行人）';
COMMENT ON COLUMN job_risk_rule.version IS '乐观锁版本号，每次更新 +1';
COMMENT ON COLUMN job_risk_rule.create_time IS '创建时间';
COMMENT ON COLUMN job_risk_rule.update_time IS '最后更新时间';
COMMENT ON COLUMN job_risk_rule.delete_time IS '软删除时间，NULL=未删除';
COMMENT ON COLUMN job_risk_rule.create_user IS '创建人用户 ID';
COMMENT ON COLUMN job_risk_rule.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN job_risk_rule.delete_user IS '删除人用户 ID';
CREATE UNIQUE INDEX uk_job_risk_rule_name ON job_risk_rule (name) WHERE delete_time IS NULL;

CREATE TABLE job_policy (
    id                    smallint    PRIMARY KEY DEFAULT 1,
    protected_envs        text[]      NOT NULL DEFAULT '{}',
    approval_workflow_key varchar(64) NOT NULL DEFAULT '',
    adhoc_approval        boolean     NOT NULL DEFAULT false,
    max_hosts             integer     NOT NULL DEFAULT 0,
    version               integer     NOT NULL DEFAULT 1,
    update_time           timestamptz NOT NULL DEFAULT now(),
    update_user           uuid        NULL,
    CONSTRAINT ck_job_policy_singleton CHECK (id = 1),
    CONSTRAINT ck_job_policy_max_hosts CHECK (max_hosts BETWEEN 0 AND 500)
);
COMMENT ON TABLE job_policy IS '作业中心全局策略（单行）';
COMMENT ON COLUMN job_policy.id IS '恒为 1';
COMMENT ON COLUMN job_policy.protected_envs IS '受保护环境（CMDB 资源的环境取值）：目标主机含这些环境时须审批（预检模式除外）';
COMMENT ON COLUMN job_policy.approval_workflow_key IS '审批所用的流程定义标识；空=未配置（此时需要审批的提交一律被拒绝）';
COMMENT ON COLUMN job_policy.adhoc_approval IS '快速执行是否一律须审批';
COMMENT ON COLUMN job_policy.max_hosts IS '单条执行的主机数上限；0=取系统上限 500';
COMMENT ON COLUMN job_policy.version IS '乐观锁版本号，每次更新 +1';
COMMENT ON COLUMN job_policy.update_time IS '最后更新时间';
COMMENT ON COLUMN job_policy.update_user IS '最后更新人用户 ID';
INSERT INTO job_policy (id) VALUES (1);

CREATE TABLE job_schedule (
    id                uuid         PRIMARY KEY,
    name              varchar(64)  NOT NULL,
    template_id       uuid         NOT NULL,
    template_version  integer      NOT NULL DEFAULT 0,
    cron              varchar(64)  NOT NULL,
    timezone          varchar(64)  NOT NULL DEFAULT 'Asia/Shanghai',
    params            jsonb        NOT NULL DEFAULT '{}',
    misfire           smallint     NOT NULL DEFAULT 1,
    enabled           boolean      NOT NULL DEFAULT true,
    owner             uuid         NOT NULL,
    next_time         timestamptz  NULL,
    last_time         timestamptz  NULL,
    last_execution_id uuid         NULL,
    last_error        varchar(512) NOT NULL DEFAULT '',
    description       varchar(255) NOT NULL DEFAULT '',
    version           integer      NOT NULL DEFAULT 1,
    create_time       timestamptz  NOT NULL DEFAULT now(),
    update_time       timestamptz  NOT NULL DEFAULT now(),
    delete_time       timestamptz  NULL,
    create_user       uuid         NULL,
    update_user       uuid         NULL,
    delete_user       uuid         NULL,
    CONSTRAINT ck_job_schedule_params CHECK (jsonb_typeof(params) = 'object'),
    CONSTRAINT ck_job_schedule_misfire CHECK (misfire IN (1, 2))
);
COMMENT ON TABLE job_schedule IS '定时作业：按 Cron 表达式以固定参数执行作业模板；以负责人的身份与数据范围执行';
COMMENT ON COLUMN job_schedule.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN job_schedule.name IS '名称，未删除行内唯一';
COMMENT ON COLUMN job_schedule.template_id IS '作业模板 job_template.id';
COMMENT ON COLUMN job_schedule.template_version IS '负责人保存时确认过的模板版本：模板此后被改过则触发被拒，须重新保存确认';
COMMENT ON COLUMN job_schedule.cron IS '标准 5 段 Cron 表达式（分 时 日 月 周）';
COMMENT ON COLUMN job_schedule.timezone IS 'Cron 表达式所用的 IANA 时区';
COMMENT ON COLUMN job_schedule.params IS '固定的参数取值（变量名 → 值）';
COMMENT ON COLUMN job_schedule.misfire IS '错过触发时间（服务停机等）后的处理：1=跳过 2=补跑一次';
COMMENT ON COLUMN job_schedule.enabled IS '是否启用';
COMMENT ON COLUMN job_schedule.owner IS '负责人用户 ID：执行以其身份提交，目标主机按其 CMDB 数据范围解析';
COMMENT ON COLUMN job_schedule.next_time IS '下次触发时间；停用时为 NULL';
COMMENT ON COLUMN job_schedule.last_time IS '最近一次触发时间';
COMMENT ON COLUMN job_schedule.last_execution_id IS '最近一次触发提交的执行';
COMMENT ON COLUMN job_schedule.last_error IS '最近一次触发失败的原因；成功后清空';
COMMENT ON COLUMN job_schedule.description IS '说明';
COMMENT ON COLUMN job_schedule.version IS '乐观锁版本号，每次编辑 +1（触发不改）';
COMMENT ON COLUMN job_schedule.create_time IS '创建时间';
COMMENT ON COLUMN job_schedule.update_time IS '最后编辑时间';
COMMENT ON COLUMN job_schedule.delete_time IS '软删除时间，NULL=未删除';
COMMENT ON COLUMN job_schedule.create_user IS '创建人用户 ID';
COMMENT ON COLUMN job_schedule.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN job_schedule.delete_user IS '删除人用户 ID';
CREATE UNIQUE INDEX uk_job_schedule_name ON job_schedule (name) WHERE delete_time IS NULL;
CREATE INDEX idx_job_schedule_due ON job_schedule (next_time) WHERE enabled AND delete_time IS NULL;
CREATE INDEX idx_job_schedule_template ON job_schedule (template_id) WHERE delete_time IS NULL;

CREATE TABLE job_trigger (
    id             uuid         PRIMARY KEY,
    name           varchar(64)  NOT NULL,
    type           smallint     NOT NULL,
    template_id    uuid         NOT NULL,
    template_version integer    NOT NULL DEFAULT 0,
    params         jsonb        NOT NULL DEFAULT '{}',
    pass_params    boolean      NOT NULL DEFAULT false,
    token_hash     bytea        NULL,
    token_sealed   bytea        NULL,
    signal         varchar(64)  NOT NULL DEFAULT '',
    alert          jsonb        NOT NULL DEFAULT '{}',
    cooldown_s     integer      NOT NULL DEFAULT 0,
    enabled        boolean      NOT NULL DEFAULT true,
    owner          uuid         NOT NULL,
    last_fire_time timestamptz  NULL,
    description    varchar(255) NOT NULL DEFAULT '',
    version        integer      NOT NULL DEFAULT 1,
    create_time    timestamptz  NOT NULL DEFAULT now(),
    update_time    timestamptz  NOT NULL DEFAULT now(),
    delete_time    timestamptz  NULL,
    create_user    uuid         NULL,
    update_user    uuid         NULL,
    delete_user    uuid         NULL,
    CONSTRAINT ck_job_trigger_type CHECK (type IN (1, 2)),
    CONSTRAINT ck_job_trigger_params CHECK (jsonb_typeof(params) = 'object'),
    CONSTRAINT ck_job_trigger_alert CHECK (jsonb_typeof(alert) = 'object'),
    CONSTRAINT ck_job_trigger_cooldown CHECK (cooldown_s BETWEEN 0 AND 86400),
    CONSTRAINT ck_job_trigger_token CHECK ((type = 1) = (token_hash IS NOT NULL) OR delete_time IS NOT NULL)
);
COMMENT ON TABLE job_trigger IS '触发器：由外部事件执行作业模板。接口触发器供流程节点 / 外部系统调用；告警触发器在告警触发时执行（告警自愈）';
COMMENT ON COLUMN job_trigger.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN job_trigger.name IS '名称，未删除行内唯一';
COMMENT ON COLUMN job_trigger.type IS '类型：1=开放接口 2=告警';
COMMENT ON COLUMN job_trigger.template_id IS '作业模板 job_template.id';
COMMENT ON COLUMN job_trigger.template_version IS '负责人保存时确认过的模板版本：模板此后被改过则触发被拒，须重新保存确认';
COMMENT ON COLUMN job_trigger.params IS '固定的参数取值（变量名 → 值）';
COMMENT ON COLUMN job_trigger.pass_params IS '接口触发器：是否允许调用方传入参数取值（覆盖固定值，仍按模板的参数定义校验）';
COMMENT ON COLUMN job_trigger.token_hash IS '接口触发器的调用令牌的 SHA-256；明文只在创建 / 重置时返回一次';
COMMENT ON COLUMN job_trigger.token_sealed IS '调用令牌的密文（kit/secretbox）：仅用于校验流程 Webhook 的 HMAC 签名（需要明文密钥）；主密钥未配置时为 NULL，此时只支持 Bearer 认证';
COMMENT ON COLUMN job_trigger.signal IS '接口触发器：作业结束后发给发起调用的流程实例的信号名；空=不发信号';
COMMENT ON COLUMN job_trigger.alert IS '告警触发器的匹配条件（kit/jobcontract.AlertMatch）';
COMMENT ON COLUMN job_trigger.cooldown_s IS '冷却秒数：上次触发后这段时间内不再触发；0=不限';
COMMENT ON COLUMN job_trigger.enabled IS '是否启用';
COMMENT ON COLUMN job_trigger.owner IS '负责人用户 ID：执行以其身份提交，目标主机按其 CMDB 数据范围解析';
COMMENT ON COLUMN job_trigger.last_fire_time IS '最近一次触发（已提交执行）的时间';
COMMENT ON COLUMN job_trigger.description IS '说明';
COMMENT ON COLUMN job_trigger.version IS '乐观锁版本号，每次编辑 +1（触发不改）';
COMMENT ON COLUMN job_trigger.create_time IS '创建时间';
COMMENT ON COLUMN job_trigger.update_time IS '最后编辑时间';
COMMENT ON COLUMN job_trigger.delete_time IS '软删除时间，NULL=未删除';
COMMENT ON COLUMN job_trigger.create_user IS '创建人用户 ID';
COMMENT ON COLUMN job_trigger.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN job_trigger.delete_user IS '删除人用户 ID';
CREATE UNIQUE INDEX uk_job_trigger_name ON job_trigger (name) WHERE delete_time IS NULL;
CREATE INDEX idx_job_trigger_template ON job_trigger (template_id) WHERE delete_time IS NULL;

-- ---------------------------------------------------------------------------
-- 运行表
-- ---------------------------------------------------------------------------

CREATE TABLE job_trigger_fire (
    id                   uuid         PRIMARY KEY,
    trigger_id           uuid         NOT NULL,
    ref                  varchar(128) NOT NULL,
    result               smallint     NOT NULL,
    reason               varchar(512) NOT NULL DEFAULT '',
    execution_id         uuid         NULL,
    summary              varchar(255) NOT NULL DEFAULT '',
    workflow_instance_id uuid         NULL,
    signal               varchar(64)  NOT NULL DEFAULT '',
    signal_state         smallint     NOT NULL DEFAULT 0,
    signal_attempts      integer      NOT NULL DEFAULT 0,
    signal_next_time     timestamptz  NULL,
    create_time          timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uk_job_trigger_fire_ref UNIQUE (trigger_id, ref),
    CONSTRAINT ck_job_trigger_fire_result CHECK (result IN (1, 2, 3)),
    CONSTRAINT ck_job_trigger_fire_signal_state CHECK (signal_state BETWEEN 0 AND 3)
);
COMMENT ON TABLE job_trigger_fire IS '触发记录：触发器的每一次触发（含被跳过与失败的），兼作去重表';
COMMENT ON COLUMN job_trigger_fire.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN job_trigger_fire.trigger_id IS '触发器 job_trigger.id';
COMMENT ON COLUMN job_trigger_fire.ref IS '去重键：告警事件 ID / 调用方的幂等键；同一触发器同一键只触发一次';
COMMENT ON COLUMN job_trigger_fire.result IS '结果：1=已提交执行 2=已跳过（冷却中、没有匹配的主机等） 3=失败（提交被拒）';
COMMENT ON COLUMN job_trigger_fire.reason IS '跳过或失败的原因';
COMMENT ON COLUMN job_trigger_fire.execution_id IS '提交的执行 job_execution.id（切片时为第一片）';
COMMENT ON COLUMN job_trigger_fire.summary IS '触发来源的摘要（告警规则名与主机 / 流程标题）';
COMMENT ON COLUMN job_trigger_fire.workflow_instance_id IS '发起调用的流程实例（接口触发器、调用方带了实例 ID 时）';
COMMENT ON COLUMN job_trigger_fire.signal IS '作业结束后要发给流程实例的信号名；空=不发';
COMMENT ON COLUMN job_trigger_fire.signal_state IS '信号状态：0=无需发送 1=待发送 2=已发送 3=已放弃';
COMMENT ON COLUMN job_trigger_fire.signal_attempts IS '信号已尝试发送的次数';
COMMENT ON COLUMN job_trigger_fire.signal_next_time IS '信号下次尝试的时间';
COMMENT ON COLUMN job_trigger_fire.create_time IS '触发时间';
CREATE INDEX idx_job_trigger_fire_trigger ON job_trigger_fire (trigger_id, create_time DESC);
CREATE INDEX idx_job_trigger_fire_signal ON job_trigger_fire (signal_next_time) WHERE signal_state = 1;
CREATE INDEX idx_job_trigger_fire_create_time ON job_trigger_fire USING brin (create_time);
CREATE INDEX idx_job_trigger_fire_execution ON job_trigger_fire (execution_id) WHERE execution_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 授权
-- ---------------------------------------------------------------------------
-- 引擎：快速执行的文件内容只读。脚本库、规则、策略、定时作业、触发器对引擎不可见。
GRANT SELECT ON job_execution_payload TO job_engine_role;
-- core：新配置表与触发记录全权；文件内容只建只读；执行记录补充审批与通知相关的可改列
-- （审批通过 / 驳回由 core 在流程引擎的事务内改状态）。
GRANT SELECT, INSERT, UPDATE, DELETE ON job_script, job_risk_rule, job_schedule, job_trigger, job_trigger_fire TO job_core_role;
GRANT SELECT, UPDATE ON job_policy TO job_core_role;
GRANT SELECT, INSERT, DELETE ON job_execution_payload TO job_core_role;
GRANT UPDATE (approval_instance_id, notify_time) ON job_execution TO job_core_role;

-- ---------------------------------------------------------------------------
-- 内置数据
-- ---------------------------------------------------------------------------

-- 缺省的高危命令规则（可在「安全策略」里调整或停用）。
INSERT INTO job_risk_rule (id, name, pattern, action, description) VALUES
('018f3e1a-0d10-7000-8000-000000000001', '删除根目录或系统目录', 'rm\s+(-[a-zA-Z]+\s+)*(/|/\*|/(bin|boot|etc|lib|lib64|usr|var|home|root|opt)(/\*)?)(\s|;|&|\||$)', 1, '递归删除根目录或系统目录'),
('018f3e1a-0d10-7000-8000-000000000002', '格式化或覆写磁盘', '\b(mkfs(\.\w+)?|wipefs|fdisk|parted)\b|\bdd\b.*\bof=/dev/', 1, '格式化、分区或直接写磁盘设备'),
('018f3e1a-0d10-7000-8000-000000000003', '关机或重启', '\b(shutdown|reboot|poweroff|halt)\b|\binit\s+[06]\b|systemctl\s+(reboot|poweroff|halt)', 2, '关机 / 重启主机须经审批'),
('018f3e1a-0d10-7000-8000-000000000004', '清空或删除数据库', '(?i)\b(drop\s+(database|table)|truncate\s+table)\b', 2, '删除或清空数据库对象须经审批'),
('018f3e1a-0d10-7000-8000-000000000005', '递归修改权限或属主', '\b(chmod|chown)\s+(-[a-zA-Z]+\s+)*-?[a-zA-Z]*R', 3, '递归修改权限 / 属主，请确认路径'),
('018f3e1a-0d10-7000-8000-000000000006', '停止防火墙或关闭 SELinux', 'systemctl\s+(stop|disable)\s+(firewalld|iptables|ufw)|setenforce\s+0', 3, '关闭安全防护，请确认必要性');

-- 执行结束通知的缺省配置（缺省业务命名空间 default）：一条路由 + 一个模板，做法同 0064。
-- 事件由 core 在后台循环的事务内直插 notify_event，收件人是执行人；没有匹配路由时事件只落库不投递。
INSERT INTO notify_route (id, biz_id, name, description, event_pattern, severity_min, priority, channel_ids, recipient_policy)
VALUES ('018f3e1a-0d11-7000-8000-000000000001', 'default', '作业执行通知',
        '作业执行结束且未成功（失败、部分失败、超时、节点失联、审批未通过）：发给执行人；定时作业与触发器发给其负责人。把最低级别调为「提示」则成功也通知', 'job.execution.*', 2, 5,
        ARRAY(SELECT id FROM notify_channel
              WHERE biz_id = 'default' AND type = 'email' AND enabled AND delete_time IS NULL ORDER BY create_time),
        '{"subscription": true}');

INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body) VALUES
('018f3e1a-0d12-7000-8000-000000000001', 'default', 'job.execution.finished', '*', 'text',
 '作业{{.Payload.status_text}}：{{.Payload.name}}',
 '作业：{{.Payload.name}}
结果：{{.Payload.status_text}}{{if .Payload.error}}（{{.Payload.error}}）{{end}}
主机：共 {{.Payload.host_total}} 台，成功 {{.Payload.host_ok}}，失败 {{.Payload.host_failed}}，不可达 {{.Payload.host_unreachable}}
来源：{{.Payload.trigger_text}}
耗时：{{.Payload.duration}}
详情：{{.Payload.url}}')
ON CONFLICT (biz_id, event_type, channel_type) WHERE delete_time IS NULL DO NOTHING;

-- 配置版本 +1：通知管道据此刷新路由与模板缓存。
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
