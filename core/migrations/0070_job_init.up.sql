-- 0070 作业中心（job）：凭据 / 项目（Git 仓库）/ 作业模板 / 执行节点，以及执行引擎的运行表
-- （执行记录、执行主机、执行日志）。
-- jsonb 列结构与状态字面量的唯一事实源是 kit/jobcontract。
-- 约定同 0001：id 为应用侧生成的 UUIDv7；时间一律 timestamptz；配置表软删 + 审计六列，
-- 唯一约束只约束未删除行。跨域引用（用户、CMDB 资源）不设外键。
-- 写入分工：配置表由 core 写、引擎只读（执行节点的心跳列除外）；执行记录由 core 建单、
-- 引擎（backend/job）推进状态并写日志；凭据密文只有引擎解密（core 只加密写入）。

-- ---------------------------------------------------------------------------
-- 配置表
-- ---------------------------------------------------------------------------

CREATE TABLE job_credential (
    id            uuid         PRIMARY KEY,
    name          varchar(64)  NOT NULL,
    type          smallint     NOT NULL,
    username      varchar(64)  NOT NULL,
    secret        bytea        NOT NULL,
    become_method varchar(16)  NOT NULL DEFAULT '',
    become_user   varchar(64)  NOT NULL DEFAULT '',
    become_secret bytea        NULL,
    fingerprint   varchar(128) NOT NULL DEFAULT '',
    description   varchar(255) NOT NULL DEFAULT '',
    version       integer      NOT NULL DEFAULT 1,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    delete_time   timestamptz  NULL,
    create_user   uuid         NULL,
    update_user   uuid         NULL,
    delete_user   uuid         NULL,
    CONSTRAINT ck_job_credential_type CHECK (type IN (1, 2)),
    CONSTRAINT ck_job_credential_become_method CHECK (become_method IN ('', 'sudo', 'su'))
);
COMMENT ON TABLE job_credential IS '作业凭据：登录目标主机的 SSH 账号，软删；core 只加密写入，引擎派发任务时解密';
COMMENT ON COLUMN job_credential.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN job_credential.name IS '名称，未删除行内唯一';
COMMENT ON COLUMN job_credential.type IS '类型：1=SSH 私钥 2=SSH 密码';
COMMENT ON COLUMN job_credential.username IS '登录用户名';
COMMENT ON COLUMN job_credential.secret IS '私钥或密码的密文（kit/secretbox，AES-256-GCM，主密钥同通知渠道凭据）；只写不读';
COMMENT ON COLUMN job_credential.become_method IS '提权方式：空=不提权 sudo su';
COMMENT ON COLUMN job_credential.become_user IS '提权目标用户，空按 root';
COMMENT ON COLUMN job_credential.become_secret IS '提权密码密文；NULL=免密提权';
COMMENT ON COLUMN job_credential.fingerprint IS '私钥对应公钥的指纹（SHA256:...），写入时计算，仅展示；密码类型为空';
COMMENT ON COLUMN job_credential.description IS '说明';
COMMENT ON COLUMN job_credential.version IS '乐观锁版本号，每次更新 +1';
COMMENT ON COLUMN job_credential.create_time IS '创建时间';
COMMENT ON COLUMN job_credential.update_time IS '最后更新时间';
COMMENT ON COLUMN job_credential.delete_time IS '软删除时间，NULL=未删除';
COMMENT ON COLUMN job_credential.create_user IS '创建人用户 ID';
COMMENT ON COLUMN job_credential.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN job_credential.delete_user IS '删除人用户 ID';
CREATE UNIQUE INDEX uk_job_credential_name ON job_credential (name) WHERE delete_time IS NULL;

CREATE TABLE job_project (
    id          uuid         PRIMARY KEY,
    name        varchar(64)  NOT NULL,
    repo_url    varchar(512) NOT NULL,
    ref         varchar(128) NOT NULL DEFAULT 'main',
    auth_type   smallint     NOT NULL DEFAULT 1,
    username    varchar(128) NOT NULL DEFAULT '',
    secret      bytea        NULL,
    description varchar(255) NOT NULL DEFAULT '',
    version     integer      NOT NULL DEFAULT 1,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL,
    CONSTRAINT ck_job_project_auth_type CHECK (auth_type IN (1, 2, 3))
);
COMMENT ON TABLE job_project IS '作业项目：存放 Playbook 的 Git 仓库，软删；执行节点在执行时按分支 / 标签拉取并记录提交号';
COMMENT ON COLUMN job_project.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN job_project.name IS '名称，未删除行内唯一';
COMMENT ON COLUMN job_project.repo_url IS '仓库地址：https:// 、ssh:// 或 user@host:path 形式';
COMMENT ON COLUMN job_project.ref IS '分支或标签名';
COMMENT ON COLUMN job_project.auth_type IS '认证方式：1=无 2=用户名 + 令牌（HTTPS） 3=SSH 私钥';
COMMENT ON COLUMN job_project.username IS 'HTTPS 认证的用户名';
COMMENT ON COLUMN job_project.secret IS '令牌或私钥密文（kit/secretbox）；只写不读，NULL=未配置';
COMMENT ON COLUMN job_project.description IS '说明';
COMMENT ON COLUMN job_project.version IS '乐观锁版本号，每次更新 +1';
COMMENT ON COLUMN job_project.create_time IS '创建时间';
COMMENT ON COLUMN job_project.update_time IS '最后更新时间';
COMMENT ON COLUMN job_project.delete_time IS '软删除时间，NULL=未删除';
COMMENT ON COLUMN job_project.create_user IS '创建人用户 ID';
COMMENT ON COLUMN job_project.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN job_project.delete_user IS '删除人用户 ID';
CREATE UNIQUE INDEX uk_job_project_name ON job_project (name) WHERE delete_time IS NULL;

CREATE TABLE job_template (
    id            uuid         PRIMARY KEY,
    name          varchar(64)  NOT NULL,
    description   varchar(255) NOT NULL DEFAULT '',
    project_id    uuid         NOT NULL,
    playbook      varchar(255) NOT NULL,
    credential_id uuid         NOT NULL,
    target        jsonb        NOT NULL,
    params        jsonb        NOT NULL DEFAULT '[]',
    options       jsonb        NOT NULL DEFAULT '{}',
    runner_zone   varchar(64)  NOT NULL DEFAULT '',
    exclusive     boolean      NOT NULL DEFAULT false,
    enabled       boolean      NOT NULL DEFAULT true,
    version       integer      NOT NULL DEFAULT 1,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    delete_time   timestamptz  NULL,
    create_user   uuid         NULL,
    update_user   uuid         NULL,
    delete_user   uuid         NULL,
    CONSTRAINT ck_job_template_target CHECK (jsonb_typeof(target) = 'object'),
    CONSTRAINT ck_job_template_params CHECK (jsonb_typeof(params) = 'array'),
    CONSTRAINT ck_job_template_options CHECK (jsonb_typeof(options) = 'object')
);
COMMENT ON TABLE job_template IS '作业模板：项目中的 Playbook + 目标主机 + 凭据 + 执行参数 + 执行选项，软删';
COMMENT ON COLUMN job_template.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN job_template.name IS '名称，未删除行内唯一';
COMMENT ON COLUMN job_template.description IS '说明';
COMMENT ON COLUMN job_template.project_id IS '所属项目 job_project.id';
COMMENT ON COLUMN job_template.playbook IS 'Playbook 在仓库内的相对路径';
COMMENT ON COLUMN job_template.credential_id IS '登录凭据 job_credential.id';
COMMENT ON COLUMN job_template.target IS '目标主机（kit/jobcontract.Target）：CMDB 资源类型 + 指定资源或筛选条件 + 地址字段';
COMMENT ON COLUMN job_template.params IS '执行参数定义（kit/jobcontract.Param 数组）：执行时填写，作为 extra vars 传给 Playbook';
COMMENT ON COLUMN job_template.options IS '执行选项（kit/jobcontract.Options）：并发数、超时、提权、预检等';
COMMENT ON COLUMN job_template.runner_zone IS '执行节点区域；空=任意区域的节点';
COMMENT ON COLUMN job_template.exclusive IS '是否禁止并发：为 true 时同一模板同时只能有一个未结束的执行';
COMMENT ON COLUMN job_template.enabled IS '是否启用；停用后不可执行';
COMMENT ON COLUMN job_template.version IS '乐观锁版本号，每次更新 +1';
COMMENT ON COLUMN job_template.create_time IS '创建时间';
COMMENT ON COLUMN job_template.update_time IS '最后更新时间';
COMMENT ON COLUMN job_template.delete_time IS '软删除时间，NULL=未删除';
COMMENT ON COLUMN job_template.create_user IS '创建人用户 ID';
COMMENT ON COLUMN job_template.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN job_template.delete_user IS '删除人用户 ID';
CREATE UNIQUE INDEX uk_job_template_name ON job_template (name) WHERE delete_time IS NULL;
CREATE INDEX idx_job_template_project ON job_template (project_id) WHERE delete_time IS NULL;
CREATE INDEX idx_job_template_credential ON job_template (credential_id) WHERE delete_time IS NULL;

CREATE TABLE job_runner (
    id            uuid         PRIMARY KEY,
    name          varchar(64)  NOT NULL,
    zone          varchar(64)  NOT NULL DEFAULT '',
    token_hash    bytea        NOT NULL,
    enabled       boolean      NOT NULL DEFAULT true,
    description   varchar(255) NOT NULL DEFAULT '',
    agent_version varchar(32)  NOT NULL DEFAULT '',
    hostname      varchar(255) NOT NULL DEFAULT '',
    capacity      integer      NOT NULL DEFAULT 0,
    running       integer      NOT NULL DEFAULT 0,
    last_seen     timestamptz  NULL,
    version       integer      NOT NULL DEFAULT 1,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    delete_time   timestamptz  NULL,
    create_user   uuid         NULL,
    update_user   uuid         NULL,
    delete_user   uuid         NULL
);
COMMENT ON TABLE job_runner IS '执行节点：部署在各网络区域、实际运行 Ansible 的进程，软删；core 维护登记信息，引擎回写心跳';
COMMENT ON COLUMN job_runner.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN job_runner.name IS '名称，未删除行内唯一';
COMMENT ON COLUMN job_runner.zone IS '所在区域；模板指定区域时只派发给同区域的节点';
COMMENT ON COLUMN job_runner.token_hash IS '接入令牌的 SHA-256；明文只在创建 / 重置时返回一次';
COMMENT ON COLUMN job_runner.enabled IS '是否启用；停用后不再领取新任务，在途任务照常上报';
COMMENT ON COLUMN job_runner.description IS '说明';
COMMENT ON COLUMN job_runner.agent_version IS '节点程序版本，心跳上报';
COMMENT ON COLUMN job_runner.hostname IS '节点主机名，心跳上报';
COMMENT ON COLUMN job_runner.capacity IS '节点可同时执行的作业数，心跳上报';
COMMENT ON COLUMN job_runner.running IS '节点正在执行的作业数，心跳上报';
COMMENT ON COLUMN job_runner.last_seen IS '最近一次心跳时间；NULL=从未接入';
COMMENT ON COLUMN job_runner.version IS '乐观锁版本号，每次编辑 +1（心跳不改）';
COMMENT ON COLUMN job_runner.create_time IS '创建时间';
COMMENT ON COLUMN job_runner.update_time IS '最后编辑时间';
COMMENT ON COLUMN job_runner.delete_time IS '软删除时间，NULL=未删除';
COMMENT ON COLUMN job_runner.create_user IS '创建人用户 ID';
COMMENT ON COLUMN job_runner.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN job_runner.delete_user IS '删除人用户 ID';
CREATE UNIQUE INDEX uk_job_runner_name ON job_runner (name) WHERE delete_time IS NULL;

-- ---------------------------------------------------------------------------
-- 运行表
-- ---------------------------------------------------------------------------

CREATE TABLE job_execution (
    id               uuid          PRIMARY KEY,
    template_id      uuid          NOT NULL,
    template_name    varchar(64)   NOT NULL,
    template_version integer       NOT NULL,
    trigger          smallint      NOT NULL DEFAULT 1,
    creator          uuid          NOT NULL,
    request_id       varchar(64)   NULL,
    status           smallint      NOT NULL DEFAULT 1,
    spec             jsonb         NOT NULL,
    runner_zone      varchar(64)   NOT NULL DEFAULT '',
    exclusive        boolean       NOT NULL DEFAULT false,
    timeout_s        integer       NOT NULL,
    host_total       integer       NOT NULL DEFAULT 0,
    host_ok          integer       NOT NULL DEFAULT 0,
    host_failed      integer       NOT NULL DEFAULT 0,
    host_unreachable integer       NOT NULL DEFAULT 0,
    commit_id        varchar(64)   NOT NULL DEFAULT '',
    error            varchar(1024) NOT NULL DEFAULT '',
    cancel_requested boolean       NOT NULL DEFAULT false,
    cancel_user      uuid          NULL,
    runner_id        uuid          NULL,
    lease_token      varchar(64)   NOT NULL DEFAULT '',
    lease_until      timestamptz   NULL,
    log_seq          bigint        NOT NULL DEFAULT 0,
    log_bytes        bigint        NOT NULL DEFAULT 0,
    log_truncated    boolean       NOT NULL DEFAULT false,
    create_time      timestamptz   NOT NULL DEFAULT now(),
    start_time       timestamptz   NULL,
    finish_time      timestamptz   NULL,
    CONSTRAINT ck_job_execution_trigger CHECK (trigger IN (1)),
    CONSTRAINT ck_job_execution_status CHECK (status BETWEEN 1 AND 8),
    CONSTRAINT ck_job_execution_spec CHECK (jsonb_typeof(spec) = 'object'),
    CONSTRAINT ck_job_execution_finish CHECK ((status IN (1, 2)) = (finish_time IS NULL))
);
COMMENT ON TABLE job_execution IS '执行记录：一次作业执行，只追加不删除；core 建单与请求取消，引擎推进状态';
COMMENT ON COLUMN job_execution.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN job_execution.template_id IS '作业模板 job_template.id';
COMMENT ON COLUMN job_execution.template_name IS '提交时的模板名称';
COMMENT ON COLUMN job_execution.template_version IS '提交时的模板版本号';
COMMENT ON COLUMN job_execution.trigger IS '触发来源：1=手动';
COMMENT ON COLUMN job_execution.creator IS '执行人用户 ID';
COMMENT ON COLUMN job_execution.request_id IS '提交幂等键（客户端生成）；同一执行人同一键只建一条';
COMMENT ON COLUMN job_execution.status IS '状态：1=排队中 2=执行中 3=成功 4=失败 5=部分失败 6=已取消 7=超时 8=结果未知（节点失联）';
COMMENT ON COLUMN job_execution.spec IS '提交时固化的执行内容（kit/jobcontract.Spec）：项目与分支、Playbook、凭据 ID、参数取值、执行选项';
COMMENT ON COLUMN job_execution.runner_zone IS '要求的执行节点区域；空=任意';
COMMENT ON COLUMN job_execution.exclusive IS '是否占用模板的互斥位（模板禁止并发）';
COMMENT ON COLUMN job_execution.timeout_s IS '执行超时秒数';
COMMENT ON COLUMN job_execution.host_total IS '目标主机数';
COMMENT ON COLUMN job_execution.host_ok IS '执行成功的主机数';
COMMENT ON COLUMN job_execution.host_failed IS '执行失败的主机数';
COMMENT ON COLUMN job_execution.host_unreachable IS '不可达的主机数';
COMMENT ON COLUMN job_execution.commit_id IS '实际执行的 Git 提交号；执行节点拉取代码后上报';
COMMENT ON COLUMN job_execution.error IS '失败原因（不含凭据）';
COMMENT ON COLUMN job_execution.cancel_requested IS '是否已请求取消；执行中的作业由节点在下一次上报时得知';
COMMENT ON COLUMN job_execution.cancel_user IS '请求取消的用户 ID';
COMMENT ON COLUMN job_execution.runner_id IS '领取本次执行的节点 job_runner.id';
COMMENT ON COLUMN job_execution.lease_token IS '领取令牌：节点上报与结束时须携带，防止过期的节点改写状态';
COMMENT ON COLUMN job_execution.lease_until IS '租约到期时间；到期未续即判定节点失联';
COMMENT ON COLUMN job_execution.log_seq IS '已收到的最大日志序号';
COMMENT ON COLUMN job_execution.log_bytes IS '已收到的日志字节数';
COMMENT ON COLUMN job_execution.log_truncated IS '日志是否因超出上限被截断';
COMMENT ON COLUMN job_execution.create_time IS '提交时间';
COMMENT ON COLUMN job_execution.start_time IS '开始执行时间（被节点领取）';
COMMENT ON COLUMN job_execution.finish_time IS '结束时间';
CREATE UNIQUE INDEX uk_job_execution_request ON job_execution (creator, request_id) WHERE request_id IS NOT NULL;
-- 模板禁止并发时的互斥位：同一模板至多一条未结束（排队中 / 执行中）的互斥执行。
CREATE UNIQUE INDEX uk_job_execution_exclusive ON job_execution (template_id) WHERE exclusive AND status IN (1, 2);
CREATE INDEX idx_job_execution_queue ON job_execution (create_time, id) WHERE status = 1;
CREATE INDEX idx_job_execution_lease ON job_execution (lease_until) WHERE status = 2;
CREATE INDEX idx_job_execution_template ON job_execution (template_id, create_time DESC);
CREATE INDEX idx_job_execution_creator ON job_execution (creator, create_time DESC);
CREATE INDEX idx_job_execution_create_time ON job_execution (create_time DESC, id DESC);

CREATE TABLE job_execution_host (
    execution_id uuid         NOT NULL REFERENCES job_execution (id) ON DELETE CASCADE,
    seq          integer      NOT NULL,
    ci_id        uuid         NOT NULL,
    name         varchar(255) NOT NULL,
    env          varchar(64)  NOT NULL DEFAULT '',
    address      varchar(255) NOT NULL,
    port         integer      NOT NULL,
    status       smallint     NOT NULL DEFAULT 0,
    ok           integer      NOT NULL DEFAULT 0,
    changed      integer      NOT NULL DEFAULT 0,
    failed       integer      NOT NULL DEFAULT 0,
    unreachable  integer      NOT NULL DEFAULT 0,
    skipped      integer      NOT NULL DEFAULT 0,
    PRIMARY KEY (execution_id, seq),
    CONSTRAINT uk_job_execution_host_address UNIQUE (execution_id, address),
    CONSTRAINT ck_job_execution_host_status CHECK (status BETWEEN 0 AND 5),
    CONSTRAINT ck_job_execution_host_port CHECK (port BETWEEN 1 AND 65535)
);
COMMENT ON TABLE job_execution_host IS '执行主机：提交时固化的目标主机快照及其执行结果';
COMMENT ON COLUMN job_execution_host.execution_id IS '所属执行 job_execution.id';
COMMENT ON COLUMN job_execution_host.seq IS '主机在本次执行中的序号，从 1 开始';
COMMENT ON COLUMN job_execution_host.ci_id IS 'CMDB 资源 ID';
COMMENT ON COLUMN job_execution_host.name IS '提交时的资源名称';
COMMENT ON COLUMN job_execution_host.env IS '提交时的资源环境';
COMMENT ON COLUMN job_execution_host.address IS '连接地址（IP 或主机名），同一次执行内唯一，也是 Ansible 清单中的主机名';
COMMENT ON COLUMN job_execution_host.port IS 'SSH 端口';
COMMENT ON COLUMN job_execution_host.status IS '结果：0=未执行 1=成功 2=成功且有变更 3=失败 4=不可达 5=已跳过';
COMMENT ON COLUMN job_execution_host.ok IS '成功的任务数';
COMMENT ON COLUMN job_execution_host.changed IS '产生变更的任务数';
COMMENT ON COLUMN job_execution_host.failed IS '失败的任务数';
COMMENT ON COLUMN job_execution_host.unreachable IS '不可达次数';
COMMENT ON COLUMN job_execution_host.skipped IS '跳过的任务数';
CREATE INDEX idx_job_execution_host_ci ON job_execution_host (ci_id, execution_id);

CREATE TABLE job_execution_log (
    execution_id uuid        NOT NULL,
    seq          bigint      NOT NULL,
    content      text        NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (execution_id, seq)
);
COMMENT ON TABLE job_execution_log IS '执行日志：执行节点按序号分块上报的输出（凭据已掩码）；由引擎写入并按保留期清理';
COMMENT ON COLUMN job_execution_log.execution_id IS '所属执行 job_execution.id';
COMMENT ON COLUMN job_execution_log.seq IS '块序号，从 1 开始连续递增；重复上报同一序号是空操作';
COMMENT ON COLUMN job_execution_log.content IS '日志内容';
COMMENT ON COLUMN job_execution_log.create_time IS '写入时间';
CREATE INDEX idx_job_execution_log_create_time ON job_execution_log USING brin (create_time);

-- ---------------------------------------------------------------------------
-- DB 角色（NOLOGIN 组角色；部署时各服务登录账号 GRANT 为成员，本地开发沿用 postgres 账号）
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'job_engine_role') THEN
        CREATE ROLE job_engine_role NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'job_core_role') THEN
        CREATE ROLE job_core_role NOLOGIN;
    END IF;
END
$$;
-- 引擎：凭据 / 项目 / 执行节点只读（模板不可见：执行内容已固化在执行记录里），执行节点仅可回写心跳，
-- 执行记录与主机仅可更新（不建单、不删除），日志读写与清理。
GRANT SELECT ON job_credential, job_project, job_runner TO job_engine_role;
GRANT UPDATE (agent_version, hostname, capacity, running, last_seen) ON job_runner TO job_engine_role;
GRANT SELECT, UPDATE ON job_execution, job_execution_host TO job_engine_role;
GRANT SELECT, INSERT, DELETE ON job_execution_log TO job_engine_role;
-- core：配置表全权；执行记录建单、读取，仅可改取消与结束相关的列（排队中的执行由 core 直接取消）；
-- 主机快照只建不改；日志只读。
GRANT SELECT, INSERT, UPDATE, DELETE ON job_credential, job_project, job_template, job_runner TO job_core_role;
GRANT SELECT, INSERT ON job_execution, job_execution_host TO job_core_role;
GRANT UPDATE (status, cancel_requested, cancel_user, finish_time, error) ON job_execution TO job_core_role;
GRANT SELECT ON job_execution_log TO job_core_role;
