-- 0072 容器平台（k8s）：集群登记、Agent 令牌、角色数据范围，以及数据面的运行表
-- （实例、集群运行状态、操作审计、终端会话、终端票据、通知发件箱）。
-- jsonb 列结构与状态字面量的唯一事实源是 kit/k8scontract。
-- 约定同 0001：id 为应用侧生成的 UUIDv7；时间一律 timestamptz；配置表软删 + 审计六列，
-- 唯一约束只约束未删除行。跨域引用（用户、角色）不设外键。
-- 写入分工：配置表由 core 写、数据面（backend/k8s）只读；运行表由数据面写、core 只读；
-- 集群凭据密文只有数据面解密（core 只加密写入）。

-- ---------------------------------------------------------------------------
-- 配置表
-- ---------------------------------------------------------------------------

CREATE TABLE k8s_cluster (
    id                   uuid         PRIMARY KEY,
    name                 varchar(63)  NOT NULL,
    display_name         varchar(64)  NOT NULL DEFAULT '',
    env                  varchar(32)  NOT NULL DEFAULT '',
    access_mode          smallint     NOT NULL,
    agent_location       smallint     NOT NULL DEFAULT 0,
    api_server           varchar(512) NOT NULL DEFAULT '',
    proxy_url            varchar(512) NOT NULL DEFAULT '',
    ca_cert              text         NOT NULL DEFAULT '',
    credential           bytea        NULL,
    exec_disabled        boolean      NOT NULL DEFAULT false,
    datasource_id        uuid         NULL,
    approval_template_id uuid         NULL,
    event_reasons        text[]       NOT NULL DEFAULT '{}',
    description          varchar(255) NOT NULL DEFAULT '',
    enabled              boolean      NOT NULL DEFAULT true,
    version              integer      NOT NULL DEFAULT 1,
    create_time          timestamptz  NOT NULL DEFAULT now(),
    update_time          timestamptz  NOT NULL DEFAULT now(),
    delete_time          timestamptz  NULL,
    create_user          uuid         NULL,
    update_user          uuid         NULL,
    delete_user          uuid         NULL,
    CONSTRAINT ck_k8s_cluster_name CHECK (name ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'),
    CONSTRAINT ck_k8s_cluster_access_mode CHECK (access_mode IN (1, 2)),
    CONSTRAINT ck_k8s_cluster_agent_location CHECK ((access_mode = 1 AND agent_location IN (1, 2)) OR (access_mode = 2 AND agent_location = 0))
);
COMMENT ON TABLE k8s_cluster IS '集群登记，软删；直连方式的凭据由 core 加密写入、数据面解密，Agent 方式不保存凭据';
COMMENT ON COLUMN k8s_cluster.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN k8s_cluster.name IS '名称，未删除行内唯一；小写字母、数字、连字符；创建后不可改（即指标标签 cluster 的取值）';
COMMENT ON COLUMN k8s_cluster.display_name IS '显示名';
COMMENT ON COLUMN k8s_cluster.env IS '环境，取值与 CMDB 的 env 一致；prod 视为生产（危险操作需输入资源名）';
COMMENT ON COLUMN k8s_cluster.access_mode IS '接入方式：1=Agent 反向隧道 2=直连；创建后不可改';
COMMENT ON COLUMN k8s_cluster.agent_location IS 'Agent 部署位置：0=不适用（直连） 1=集群内 2=转发节点';
COMMENT ON COLUMN k8s_cluster.api_server IS '直连：API Server 地址，仅 https';
COMMENT ON COLUMN k8s_cluster.proxy_url IS '直连：可选的 HTTP(S) 代理地址';
COMMENT ON COLUMN k8s_cluster.ca_cert IS '直连：CA 证书（PEM）；空=使用系统根证书';
COMMENT ON COLUMN k8s_cluster.credential IS '直连：凭据密文（kit/secretbox，明文结构见 k8scontract.Credential）；只写不读';
COMMENT ON COLUMN k8s_cluster.exec_disabled IS '禁止 Web 终端';
COMMENT ON COLUMN k8s_cluster.datasource_id IS '关联的 monitor 数据源（指标面板用）；NULL=未配置';
COMMENT ON COLUMN k8s_cluster.approval_template_id IS '变更需审批时使用的工单模板；NULL=变更不需审批';
COMMENT ON COLUMN k8s_cluster.event_reasons IS '上报到通知的 Warning 事件原因；空=使用缺省清单';
COMMENT ON COLUMN k8s_cluster.description IS '说明';
COMMENT ON COLUMN k8s_cluster.enabled IS '是否启用；停用后数据面断开连接';
COMMENT ON COLUMN k8s_cluster.version IS '乐观锁版本号，每次更新 +1';
COMMENT ON COLUMN k8s_cluster.create_time IS '创建时间';
COMMENT ON COLUMN k8s_cluster.update_time IS '最后更新时间';
COMMENT ON COLUMN k8s_cluster.delete_time IS '软删除时间，NULL=未删除';
COMMENT ON COLUMN k8s_cluster.create_user IS '创建人用户 ID';
COMMENT ON COLUMN k8s_cluster.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN k8s_cluster.delete_user IS '删除人用户 ID';
CREATE UNIQUE INDEX uk_k8s_cluster_name ON k8s_cluster (name) WHERE delete_time IS NULL;

CREATE TABLE k8s_agent_token (
    id          uuid        PRIMARY KEY,
    cluster_id  uuid        NOT NULL REFERENCES k8s_cluster (id),
    kind        smallint    NOT NULL,
    token_hash  bytea       NOT NULL,
    expire_time timestamptz NULL,
    create_user uuid        NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_k8s_agent_token_kind CHECK (kind IN (1, 2))
);
COMMENT ON TABLE k8s_agent_token IS 'Agent 接入令牌（物理删除）：令牌格式 <令牌 ID>.<随机密钥>，库里只存密钥的 SHA-256';
COMMENT ON COLUMN k8s_agent_token.id IS '令牌 ID，UUIDv7；即令牌明文的前半段';
COMMENT ON COLUMN k8s_agent_token.cluster_id IS '所属集群';
COMMENT ON COLUMN k8s_agent_token.kind IS '类型：1=注册令牌（一次性，换取长期令牌） 2=长期令牌';
COMMENT ON COLUMN k8s_agent_token.token_hash IS '密钥的 SHA-256';
COMMENT ON COLUMN k8s_agent_token.expire_time IS '过期时间；注册令牌 24 小时，长期令牌为 NULL';
COMMENT ON COLUMN k8s_agent_token.create_user IS '签发人用户 ID；由注册令牌换发的长期令牌为 NULL';
COMMENT ON COLUMN k8s_agent_token.create_time IS '签发时间';
CREATE INDEX idx_k8s_agent_token_cluster ON k8s_agent_token (cluster_id, kind);

CREATE TABLE k8s_role_scope (
    role_id     uuid        NOT NULL,
    cluster_id  uuid        NOT NULL REFERENCES k8s_cluster (id),
    namespace   varchar(63) NOT NULL,
    level       smallint    NOT NULL,
    update_user uuid        NULL,
    update_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (role_id, cluster_id, namespace),
    CONSTRAINT ck_k8s_role_scope_level CHECK (level BETWEEN 1 AND 4),
    CONSTRAINT ck_k8s_role_scope_admin CHECK (level < 4 OR namespace = '*'),
    CONSTRAINT ck_k8s_role_scope_namespace CHECK (namespace = '*' OR namespace ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$')
);
COMMENT ON TABLE k8s_role_scope IS '角色数据范围（物理删除）：角色在某集群某命名空间的档位；用户有效范围为其全部启用角色的并集';
COMMENT ON COLUMN k8s_role_scope.role_id IS '角色 ID（不设外键：角色删除后范围行无害，解析时按启用角色读取）';
COMMENT ON COLUMN k8s_role_scope.cluster_id IS '集群';
COMMENT ON COLUMN k8s_role_scope.namespace IS '命名空间；* 表示全部命名空间';
COMMENT ON COLUMN k8s_role_scope.level IS '档位：1=只读 2=开发 3=命名空间管理员 4=集群管理员';
COMMENT ON COLUMN k8s_role_scope.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN k8s_role_scope.update_time IS '最后更新时间';
CREATE INDEX idx_k8s_role_scope_cluster ON k8s_role_scope (cluster_id);

CREATE TABLE k8s_cfg_version (
    name        varchar(64) PRIMARY KEY,
    version     bigint      NOT NULL DEFAULT 1,
    update_time timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE k8s_cfg_version IS '配置版本：对应配置表写入时 +1，数据面轮询它来刷新连接与同步授权';
COMMENT ON COLUMN k8s_cfg_version.name IS '配置名：k8s_cluster / k8s_role_scope / k8s_agent_token';
COMMENT ON COLUMN k8s_cfg_version.version IS '版本号，单调递增';
COMMENT ON COLUMN k8s_cfg_version.update_time IS '最后一次 +1 的时间';

-- ---------------------------------------------------------------------------
-- 运行表
-- ---------------------------------------------------------------------------

CREATE TABLE k8s_instance (
    id             varchar(64)  PRIMARY KEY,
    addr           varchar(255) NOT NULL,
    clusters       integer      NOT NULL DEFAULT 0,
    start_time     timestamptz  NOT NULL DEFAULT now(),
    heartbeat_time timestamptz  NOT NULL DEFAULT now()
);
COMMENT ON TABLE k8s_instance IS '数据面实例：心跳 5 秒，15 秒无心跳视为下线';
COMMENT ON COLUMN k8s_instance.id IS '实例 ID：主机名 + 启动随机串';
COMMENT ON COLUMN k8s_instance.addr IS 'core 访问该实例内部接口的地址（http://host:port）';
COMMENT ON COLUMN k8s_instance.clusters IS '当前负责的集群数';
COMMENT ON COLUMN k8s_instance.start_time IS '启动时间';
COMMENT ON COLUMN k8s_instance.heartbeat_time IS '最近一次心跳（数据库时钟）';

CREATE TABLE k8s_cluster_runtime (
    cluster_id         uuid         PRIMARY KEY REFERENCES k8s_cluster (id),
    owner_instance     varchar(64)  NOT NULL DEFAULT '',
    lease_until        timestamptz  NULL,
    status             smallint     NOT NULL DEFAULT 0,
    status_since       timestamptz  NOT NULL DEFAULT now(),
    k8s_version        varchar(32)  NOT NULL DEFAULT '',
    node_total         integer      NOT NULL DEFAULT 0,
    node_ready         integer      NOT NULL DEFAULT 0,
    agent_version      varchar(32)  NOT NULL DEFAULT '',
    agent_connect_time timestamptz  NULL,
    agent_last_seen    timestamptz  NULL,
    last_error         varchar(512) NOT NULL DEFAULT '',
    probe_time         timestamptz  NULL,
    scope_version      bigint       NOT NULL DEFAULT 0,
    scope_error        varchar(512) NOT NULL DEFAULT '',
    summary            jsonb        NOT NULL DEFAULT '{}',
    summary_time       timestamptz  NULL,
    CONSTRAINT ck_k8s_cluster_runtime_status CHECK (status IN (0, 1, 2))
);
COMMENT ON TABLE k8s_cluster_runtime IS '集群运行状态：一行一个集群，由负责该集群的数据面实例维护';
COMMENT ON COLUMN k8s_cluster_runtime.cluster_id IS '集群';
COMMENT ON COLUMN k8s_cluster_runtime.owner_instance IS '负责该集群的数据面实例；空=无主';
COMMENT ON COLUMN k8s_cluster_runtime.lease_until IS '归属租约到期时间';
COMMENT ON COLUMN k8s_cluster_runtime.status IS '状态：0=未知 1=在线 2=不可达';
COMMENT ON COLUMN k8s_cluster_runtime.status_since IS '进入当前状态的时间';
COMMENT ON COLUMN k8s_cluster_runtime.k8s_version IS 'K8s 版本';
COMMENT ON COLUMN k8s_cluster_runtime.node_total IS '节点总数';
COMMENT ON COLUMN k8s_cluster_runtime.node_ready IS '就绪节点数';
COMMENT ON COLUMN k8s_cluster_runtime.agent_version IS 'Agent 版本';
COMMENT ON COLUMN k8s_cluster_runtime.agent_connect_time IS '当前隧道的建立时间';
COMMENT ON COLUMN k8s_cluster_runtime.agent_last_seen IS 'Agent 最近一次在线时间';
COMMENT ON COLUMN k8s_cluster_runtime.last_error IS '最近一次失败原因（探活或接入自检）';
COMMENT ON COLUMN k8s_cluster_runtime.probe_time IS '最近一次探活时间';
COMMENT ON COLUMN k8s_cluster_runtime.scope_version IS '已同步到集群的授权版本（k8s_cfg_version.k8s_role_scope）';
COMMENT ON COLUMN k8s_cluster_runtime.scope_error IS '最近一次授权同步的错误';
COMMENT ON COLUMN k8s_cluster_runtime.summary IS '总览摘要（k8scontract.ClusterSummary）：资源水位、异常工作负载数等';
COMMENT ON COLUMN k8s_cluster_runtime.summary_time IS '摘要采集时间';
CREATE INDEX idx_k8s_cluster_runtime_owner ON k8s_cluster_runtime (owner_instance);

-- 操作审计：只增不改；按月 RANGE 分区（create_time），按分区 DROP 清理。分区表唯一约束须含分区键，
-- 故主键为 (id, create_time)。DEFAULT 分区兜底：分区维护失效时写入不被拒绝。
CREATE TABLE k8s_audit (
    id           uuid         NOT NULL,
    create_time  timestamptz  NOT NULL DEFAULT now(),
    user_id      uuid         NULL,
    username     varchar(64)  NOT NULL DEFAULT '',
    client_ip    varchar(64)  NOT NULL DEFAULT '',
    request_id   varchar(64)  NOT NULL DEFAULT '',
    cluster_id   uuid         NULL,
    cluster_name varchar(63)  NOT NULL DEFAULT '',
    namespace    varchar(63)  NOT NULL DEFAULT '',
    api_group    varchar(128) NOT NULL DEFAULT '',
    api_version  varchar(32)  NOT NULL DEFAULT '',
    resource     varchar(128) NOT NULL DEFAULT '',
    kind         varchar(128) NOT NULL DEFAULT '',
    name         varchar(253) NOT NULL DEFAULT '',
    action       varchar(32)  NOT NULL,
    result       smallint     NOT NULL,
    error        varchar(512) NOT NULL DEFAULT '',
    before_obj   jsonb        NULL,
    after_obj    jsonb        NULL,
    patch        jsonb        NULL,
    truncated    boolean      NOT NULL DEFAULT false,
    emergency    boolean      NOT NULL DEFAULT false,
    change_id    uuid         NULL,
    PRIMARY KEY (id, create_time),
    CONSTRAINT ck_k8s_audit_result CHECK (result IN (1, 2))
) PARTITION BY RANGE (create_time);
COMMENT ON TABLE k8s_audit IS '操作审计：全部写操作、Secret 明文查看、集群与授权的配置变更；只增不改，按月分区';
COMMENT ON COLUMN k8s_audit.id IS '记录 ID，UUIDv7';
COMMENT ON COLUMN k8s_audit.create_time IS '操作时间（分区键，数据库时钟）';
COMMENT ON COLUMN k8s_audit.user_id IS '操作人用户 ID；系统执行（已审批变更）时为变更发起人';
COMMENT ON COLUMN k8s_audit.username IS '操作人用户名（冗余，用户删除后仍可读）';
COMMENT ON COLUMN k8s_audit.client_ip IS '来源 IP';
COMMENT ON COLUMN k8s_audit.request_id IS '请求 ID';
COMMENT ON COLUMN k8s_audit.cluster_id IS '集群 ID';
COMMENT ON COLUMN k8s_audit.cluster_name IS '集群名称（冗余）';
COMMENT ON COLUMN k8s_audit.namespace IS '命名空间；集群级资源为空';
COMMENT ON COLUMN k8s_audit.api_group IS '资源所属 API 组';
COMMENT ON COLUMN k8s_audit.api_version IS '资源 API 版本';
COMMENT ON COLUMN k8s_audit.resource IS '资源类型（复数小写）；平台配置变更时为 cluster / role-scope / agent-token';
COMMENT ON COLUMN k8s_audit.kind IS '资源 Kind';
COMMENT ON COLUMN k8s_audit.name IS '资源名称';
COMMENT ON COLUMN k8s_audit.action IS '动作（k8scontract.Action*）';
COMMENT ON COLUMN k8s_audit.result IS '结果：1=成功 2=失败';
COMMENT ON COLUMN k8s_audit.error IS '失败原因';
COMMENT ON COLUMN k8s_audit.before_obj IS '变更前的对象：去掉 managedFields 与 status，Secret 的 data 掩码';
COMMENT ON COLUMN k8s_audit.after_obj IS '变更后的对象（同上）';
COMMENT ON COLUMN k8s_audit.patch IS '本次改动或动作参数';
COMMENT ON COLUMN k8s_audit.truncated IS '对象超过 256KB 被置空';
COMMENT ON COLUMN k8s_audit.emergency IS '紧急变更（免审批直接执行）';
COMMENT ON COLUMN k8s_audit.change_id IS '关联的变更单（走审批时）';
CREATE INDEX idx_k8s_audit_time ON k8s_audit (create_time DESC);
CREATE INDEX idx_k8s_audit_cluster ON k8s_audit (cluster_id, create_time DESC);
CREATE INDEX idx_k8s_audit_user ON k8s_audit (user_id, create_time DESC);
CREATE INDEX idx_k8s_audit_object ON k8s_audit (cluster_id, namespace, resource, name, create_time DESC);

CREATE TABLE k8s_audit_default PARTITION OF k8s_audit DEFAULT;
-- 预建当月与未来 2 个月的分区（UTC 月界；边界字面量带显式 +00）。命名 k8s_audit_YYYY_MM，
-- 之后由数据面的维护任务接管（预建 / 按保留期 DROP）。
DO $$
DECLARE
    m date := date_trunc('month', now() AT TIME ZONE 'UTC')::date;
    i integer;
BEGIN
    FOR i IN 0..2 LOOP
        EXECUTE format(
            'CREATE TABLE k8s_audit_%s PARTITION OF k8s_audit FOR VALUES FROM (%L) TO (%L)',
            to_char(m + make_interval(months => i), 'YYYY_MM'),
            to_char(m + make_interval(months => i), 'YYYY-MM-DD') || ' 00:00:00+00',
            to_char(m + make_interval(months => i + 1), 'YYYY-MM-DD') || ' 00:00:00+00');
    END LOOP;
END
$$;

CREATE TABLE k8s_exec_session (
    id           uuid         PRIMARY KEY,
    user_id      uuid         NOT NULL,
    username     varchar(64)  NOT NULL DEFAULT '',
    client_ip    varchar(64)  NOT NULL DEFAULT '',
    cluster_id   uuid         NOT NULL,
    cluster_name varchar(63)  NOT NULL DEFAULT '',
    namespace    varchar(63)  NOT NULL,
    pod          varchar(253) NOT NULL,
    container    varchar(253) NOT NULL DEFAULT '',
    kind         smallint     NOT NULL DEFAULT 1,
    start_time   timestamptz  NOT NULL DEFAULT now(),
    end_time     timestamptz  NULL,
    end_reason   varchar(32)  NOT NULL DEFAULT '',
    bytes_in     bigint       NOT NULL DEFAULT 0,
    bytes_out    bigint       NOT NULL DEFAULT 0,
    recorded     boolean      NOT NULL DEFAULT false,
    CONSTRAINT ck_k8s_exec_session_kind CHECK (kind IN (1, 2))
);
COMMENT ON TABLE k8s_exec_session IS 'Web 终端会话记录';
COMMENT ON COLUMN k8s_exec_session.id IS '会话 ID，UUIDv7';
COMMENT ON COLUMN k8s_exec_session.user_id IS '用户 ID';
COMMENT ON COLUMN k8s_exec_session.username IS '用户名（冗余）';
COMMENT ON COLUMN k8s_exec_session.client_ip IS '来源 IP';
COMMENT ON COLUMN k8s_exec_session.cluster_id IS '集群 ID';
COMMENT ON COLUMN k8s_exec_session.cluster_name IS '集群名称（冗余）';
COMMENT ON COLUMN k8s_exec_session.namespace IS '命名空间';
COMMENT ON COLUMN k8s_exec_session.pod IS 'Pod 名称';
COMMENT ON COLUMN k8s_exec_session.container IS '容器名称';
COMMENT ON COLUMN k8s_exec_session.kind IS '类型：1=容器终端 2=调试容器';
COMMENT ON COLUMN k8s_exec_session.start_time IS '开始时间';
COMMENT ON COLUMN k8s_exec_session.end_time IS '结束时间；NULL=进行中';
COMMENT ON COLUMN k8s_exec_session.end_reason IS '结束原因（k8scontract.ExecEnd*）';
COMMENT ON COLUMN k8s_exec_session.bytes_in IS '输入字节数';
COMMENT ON COLUMN k8s_exec_session.bytes_out IS '输出字节数';
COMMENT ON COLUMN k8s_exec_session.recorded IS '是否有录制内容（k8s_exec_record）';
CREATE INDEX idx_k8s_exec_session_time ON k8s_exec_session (start_time DESC);
CREATE INDEX idx_k8s_exec_session_user ON k8s_exec_session (user_id, start_time DESC);
CREATE INDEX idx_k8s_exec_session_cluster ON k8s_exec_session (cluster_id, start_time DESC);

CREATE TABLE k8s_exec_record (
    session_id  uuid        NOT NULL,
    seq         integer     NOT NULL,
    data        bytea       NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, seq)
);
COMMENT ON TABLE k8s_exec_record IS '终端录制：会话输出按块追加（asciicast v2 的事件行），回放时按序读取';
COMMENT ON COLUMN k8s_exec_record.session_id IS '会话 ID';
COMMENT ON COLUMN k8s_exec_record.seq IS '块序号，从 1 递增';
COMMENT ON COLUMN k8s_exec_record.data IS '块内容：若干行 [秒偏移, "o", "输出"] 的 JSON，gzip 压缩';
COMMENT ON COLUMN k8s_exec_record.create_time IS '写入时间';

CREATE TABLE k8s_exec_ticket (
    token_hash  bytea        PRIMARY KEY,
    user_id     uuid         NOT NULL,
    cluster_id  uuid         NOT NULL,
    params      jsonb        NOT NULL,
    expire_time timestamptz  NOT NULL
);
COMMENT ON TABLE k8s_exec_ticket IS '终端票据（物理删除）：一次性、30 秒有效；core 签发并核销，多个 core 实例共用';
COMMENT ON COLUMN k8s_exec_ticket.token_hash IS '票据明文的 SHA-256';
COMMENT ON COLUMN k8s_exec_ticket.user_id IS '签发给的用户';
COMMENT ON COLUMN k8s_exec_ticket.cluster_id IS '集群';
COMMENT ON COLUMN k8s_exec_ticket.params IS '连接参数（k8scontract.ExecParams）：命名空间、Pod、容器、会话 ID、类型等';
COMMENT ON COLUMN k8s_exec_ticket.expire_time IS '过期时间';
CREATE INDEX idx_k8s_exec_ticket_expire ON k8s_exec_ticket (expire_time);

CREATE TABLE k8s_notify_outbox (
    id          uuid         PRIMARY KEY,
    cluster_id  uuid         NOT NULL,
    event_type  varchar(128) NOT NULL,
    severity    smallint     NOT NULL DEFAULT 2,
    dedup_key   varchar(255) NOT NULL,
    group_key   varchar(255) NOT NULL DEFAULT '',
    recipients  uuid[]       NOT NULL DEFAULT '{}',
    payload     jsonb        NOT NULL DEFAULT '{}',
    status      smallint     NOT NULL DEFAULT 1,
    attempts    integer      NOT NULL DEFAULT 0,
    last_error  varchar(512) NOT NULL DEFAULT '',
    create_time timestamptz  NOT NULL DEFAULT now(),
    done_time   timestamptz  NULL,
    CONSTRAINT ck_k8s_notify_outbox_status CHECK (status IN (1, 2, 3)),
    CONSTRAINT ck_k8s_notify_outbox_severity CHECK (severity BETWEEN 1 AND 4)
);
COMMENT ON TABLE k8s_notify_outbox IS '通知发件箱：数据面写入，core 的 k8s-notify 循环领取后写通知事件';
COMMENT ON COLUMN k8s_notify_outbox.id IS '主键，UUIDv7';
COMMENT ON COLUMN k8s_notify_outbox.cluster_id IS '集群';
COMMENT ON COLUMN k8s_notify_outbox.event_type IS '通知事件类型（k8s.cluster.* / k8s.event.* / k8s.change.* / k8s.inspection.*）';
COMMENT ON COLUMN k8s_notify_outbox.severity IS '级别 1-4（同 notify）';
COMMENT ON COLUMN k8s_notify_outbox.dedup_key IS '幂等键';
COMMENT ON COLUMN k8s_notify_outbox.group_key IS '聚合键';
COMMENT ON COLUMN k8s_notify_outbox.recipients IS '指定收件人（用户 ID）；空=由通知路由与订阅决定';
COMMENT ON COLUMN k8s_notify_outbox.payload IS '事件内容体';
COMMENT ON COLUMN k8s_notify_outbox.status IS '状态：1=待派发 2=已派发 3=已丢弃';
COMMENT ON COLUMN k8s_notify_outbox.attempts IS '派发尝试次数';
COMMENT ON COLUMN k8s_notify_outbox.last_error IS '最近一次派发错误';
COMMENT ON COLUMN k8s_notify_outbox.create_time IS '写入时间';
COMMENT ON COLUMN k8s_notify_outbox.done_time IS '派发或丢弃时间';
CREATE UNIQUE INDEX uk_k8s_notify_outbox_dedup ON k8s_notify_outbox (dedup_key);
CREATE INDEX idx_k8s_notify_outbox_pending ON k8s_notify_outbox (create_time) WHERE status = 1;

-- ---------------------------------------------------------------------------
-- DB 角色（NOLOGIN 组角色；部署时各服务登录账号 GRANT 为成员，本地开发沿用 postgres 账号）
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'k8s_plane_role') THEN
        CREATE ROLE k8s_plane_role NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'k8s_core_role') THEN
        CREATE ROLE k8s_core_role NOLOGIN;
    END IF;
END
$$;

-- 数据面：配置表只读（Agent 令牌可增删：注册令牌换发长期令牌）；运行表读写；终端票据不可见。
GRANT SELECT ON k8s_cluster, k8s_role_scope, k8s_cfg_version TO k8s_plane_role;
GRANT SELECT, INSERT, DELETE ON k8s_agent_token TO k8s_plane_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON k8s_instance, k8s_cluster_runtime, k8s_exec_session, k8s_exec_record TO k8s_plane_role;
GRANT SELECT, INSERT ON k8s_audit TO k8s_plane_role;
GRANT SELECT, INSERT, DELETE ON k8s_notify_outbox TO k8s_plane_role;
-- 审计表的分区维护由数据面执行（做法同 0069 的判定记录表）。
GRANT CREATE ON SCHEMA public TO k8s_plane_role;
ALTER TABLE k8s_audit OWNER TO k8s_plane_role;
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT inhrelid::regclass AS part FROM pg_inherits WHERE inhparent = 'k8s_audit'::regclass LOOP
        EXECUTE format('ALTER TABLE %s OWNER TO k8s_plane_role', r.part);
    END LOOP;
END
$$;

-- core：配置表与终端票据全权；运行表只读；审计可追加（集群与授权的配置变更）；发件箱仅改派发状态。
GRANT SELECT, INSERT, UPDATE, DELETE ON k8s_cluster, k8s_agent_token, k8s_role_scope, k8s_cfg_version, k8s_exec_ticket TO k8s_core_role;
GRANT SELECT ON k8s_instance, k8s_cluster_runtime, k8s_exec_session, k8s_exec_record, k8s_notify_outbox TO k8s_core_role;
GRANT INSERT ON k8s_cluster_runtime TO k8s_core_role;
GRANT SELECT, INSERT ON k8s_audit TO k8s_core_role;
GRANT UPDATE (status, attempts, last_error, done_time) ON k8s_notify_outbox TO k8s_core_role;

-- ---------------------------------------------------------------------------
-- 内置数据
-- ---------------------------------------------------------------------------
INSERT INTO k8s_cfg_version (name, version) VALUES
('k8s_cluster', 1),
('k8s_role_scope', 1),
('k8s_agent_token', 1);

-- 容器平台通知的缺省配置（缺省业务命名空间 default）：一条路由 + 两个模板，做法同 0055 / 0069。
-- 只绑定迁移执行时已启用的邮件与群机器人渠道；没有则路由的渠道为空，之后在「通知管理 → 规则」里选择。
INSERT INTO notify_route (id, biz_id, name, description, event_pattern, severity_min, priority, channel_ids, recipient_policy, aggregate_window_s)
VALUES ('018f3e1a-0cd1-7000-8000-000000000001', 'default', '容器平台通知',
        '集群连通性变化、集群内 Warning 事件、变更执行结果、巡检报告', 'k8s.*', 1, 5,
        ARRAY(SELECT id FROM notify_channel
              WHERE biz_id = 'default' AND type IN ('email', 'feishu', 'dingtalk', 'wecom') AND enabled AND delete_time IS NULL ORDER BY create_time),
        '{"subscription": true}', 60);

INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body) VALUES
('018f3e1a-0cd2-7000-8000-000000000001', 'default', 'k8s.cluster.unreachable', '*', 'text',
 '【集群不可达】{{.Payload.cluster_name}}',
 '集群：{{.Payload.cluster_name}}{{if .Payload.display_name}}（{{.Payload.display_name}}）{{end}}
环境：{{.Payload.env}}
状态：不可达
原因：{{.Payload.error}}
时间：{{.Payload.time}}'),
('018f3e1a-0cd2-7000-8000-000000000002', 'default', 'k8s.cluster.recovered', '*', 'text',
 '【集群恢复】{{.Payload.cluster_name}}',
 '集群：{{.Payload.cluster_name}}{{if .Payload.display_name}}（{{.Payload.display_name}}）{{end}}
环境：{{.Payload.env}}
状态：已恢复
时间：{{.Payload.time}}')
ON CONFLICT (biz_id, event_type, channel_type) WHERE delete_time IS NULL DO NOTHING;

UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
