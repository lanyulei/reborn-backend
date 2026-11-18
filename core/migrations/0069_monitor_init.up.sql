-- 0069 监控告警域（monitor）：数据源 / 规则分组 / 角色数据范围 / 告警规则与版本 / 屏蔽规则 / 订阅规则，
-- 以及评估引擎的运行表（评估任务、判定状态、告警事件、判定记录、通知发件箱、通知记录）。
-- 设计：backend/docs/monitor-tech-design.md §3；jsonb 列结构与状态字面量的唯一事实源是 kit/monitorcontract。
-- 约定同 0001：id 为应用侧生成的 UUIDv7；时间一律 timestamptz；配置表软删 + 审计六列，
-- 唯一约束只约束未删除行。跨域引用（用户、角色、值班表、部门）不设外键。
-- 写入分工：配置表由 core 写、引擎只读；运行表由引擎（backend/monitor）写、core 读；
-- 发件箱由引擎写入、core 派发；通知记录由 core 写。

-- ---------------------------------------------------------------------------
-- 配置表
-- ---------------------------------------------------------------------------

CREATE TABLE monitor_datasource (
    id               uuid         PRIMARY KEY,
    name             varchar(64)  NOT NULL,
    type             varchar(32)  NOT NULL DEFAULT 'prometheus',
    url              varchar(512) NOT NULL,
    auth_type        smallint     NOT NULL DEFAULT 1,
    username         varchar(128) NOT NULL DEFAULT '',
    secret           bytea        NULL,
    headers          jsonb        NOT NULL DEFAULT '{}',
    timeout_ms       integer      NOT NULL DEFAULT 10000,
    tls_skip_verify  boolean      NOT NULL DEFAULT false,
    remote_write_url varchar(512) NOT NULL DEFAULT '',
    description      varchar(255) NOT NULL DEFAULT '',
    is_default       boolean      NOT NULL DEFAULT false,
    enabled          boolean      NOT NULL DEFAULT true,
    health_status    smallint     NOT NULL DEFAULT 0,
    health_error     varchar(512) NOT NULL DEFAULT '',
    health_time      timestamptz  NULL,
    version          integer      NOT NULL DEFAULT 1,
    create_time      timestamptz  NOT NULL DEFAULT now(),
    update_time      timestamptz  NOT NULL DEFAULT now(),
    delete_time      timestamptz  NULL,
    create_user      uuid         NULL,
    update_user      uuid         NULL,
    delete_user      uuid         NULL,
    CONSTRAINT ck_monitor_datasource_type CHECK (type IN ('prometheus')),
    CONSTRAINT ck_monitor_datasource_url CHECK (url ~ '^https?://'),
    CONSTRAINT ck_monitor_datasource_auth_type CHECK (auth_type IN (1, 2, 3)),
    CONSTRAINT ck_monitor_datasource_headers CHECK (jsonb_typeof(headers) = 'object'),
    CONSTRAINT ck_monitor_datasource_timeout CHECK (timeout_ms BETWEEN 1000 AND 30000),
    CONSTRAINT ck_monitor_datasource_health CHECK (health_status IN (0, 1, 2))
);
COMMENT ON TABLE monitor_datasource IS '监控数据源：Prometheus 兼容查询端（Prometheus / VictoriaMetrics / Thanos 等），软删；core 写，引擎只读并回写健康状态三列';
COMMENT ON COLUMN monitor_datasource.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_datasource.name IS '名称，未删除行内唯一';
COMMENT ON COLUMN monitor_datasource.type IS '类型，固定 prometheus（Prometheus HTTP API 兼容）';
COMMENT ON COLUMN monitor_datasource.url IS '查询地址（不含 /api/v1/query，可带路径前缀），仅 http / https';
COMMENT ON COLUMN monitor_datasource.auth_type IS '认证方式：1=无 2=Basic 3=Bearer';
COMMENT ON COLUMN monitor_datasource.username IS 'Basic 认证用户名';
COMMENT ON COLUMN monitor_datasource.secret IS 'Basic 密码或 Bearer 令牌密文（kit/secretbox，AES-256-GCM，nonce 前缀拼接，主密钥同通知渠道凭据）；只写不读，NULL=未配置';
COMMENT ON COLUMN monitor_datasource.headers IS '自定义请求头 {名称: 值}；写入时拒绝 Authorization / Cookie / Host 等凭据与逐跳头';
COMMENT ON COLUMN monitor_datasource.timeout_ms IS '查询超时毫秒，1000-30000';
COMMENT ON COLUMN monitor_datasource.tls_skip_verify IS '是否跳过 TLS 证书校验（自签证书的内网数据源）';
COMMENT ON COLUMN monitor_datasource.remote_write_url IS '远程写入地址；仅记录，当前不使用';
COMMENT ON COLUMN monitor_datasource.description IS '说明';
COMMENT ON COLUMN monitor_datasource.is_default IS '是否缺省数据源；未删除行内至多一行为 true';
COMMENT ON COLUMN monitor_datasource.enabled IS '是否启用；停用后不再评估引用它的规则';
COMMENT ON COLUMN monitor_datasource.health_status IS '健康状态：0=未知 1=正常 2=异常；由评估引擎探活写入';
COMMENT ON COLUMN monitor_datasource.health_error IS '最近一次探活失败原因（不含凭据，至多 512 字节）';
COMMENT ON COLUMN monitor_datasource.health_time IS '最近一次探活时间';
COMMENT ON COLUMN monitor_datasource.version IS '乐观锁版本号：每次保存 +1（探活回写不改）';
COMMENT ON COLUMN monitor_datasource.create_time IS '创建时间';
COMMENT ON COLUMN monitor_datasource.update_time IS '更新时间';
COMMENT ON COLUMN monitor_datasource.delete_time IS '软删时间';
COMMENT ON COLUMN monitor_datasource.create_user IS '创建人';
COMMENT ON COLUMN monitor_datasource.update_user IS '最近更新人';
COMMENT ON COLUMN monitor_datasource.delete_user IS '删除人';
CREATE UNIQUE INDEX uk_monitor_datasource_name ON monitor_datasource (name) WHERE delete_time IS NULL;
CREATE UNIQUE INDEX uk_monitor_datasource_default ON monitor_datasource (is_default) WHERE is_default AND delete_time IS NULL;

CREATE TABLE monitor_rule_group (
    id          uuid         PRIMARY KEY,
    parent_id   uuid         NULL REFERENCES monitor_rule_group (id),
    path        text         NOT NULL,
    name        varchar(64)  NOT NULL,
    description varchar(255) NOT NULL DEFAULT '',
    sort        integer      NOT NULL DEFAULT 0,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL,
    CONSTRAINT ck_monitor_rule_group_path CHECK (path LIKE '/%/' AND path LIKE '%/' || id::text || '/')
);
COMMENT ON TABLE monitor_rule_group IS '告警规则分组（树，至多 5 层、总数至多 500，应用层校验），软删；规则的归属与数据范围的授权单位';
COMMENT ON COLUMN monitor_rule_group.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_rule_group.parent_id IS '上级分组；NULL=顶级分组';
COMMENT ON COLUMN monitor_rule_group.path IS '物化路径 /<祖先 id>/…/<自身 id>/；查询某分组及其全部下级用 path LIKE ''<该分组 path>%''';
COMMENT ON COLUMN monitor_rule_group.name IS '名称，同一上级下未删除行内唯一';
COMMENT ON COLUMN monitor_rule_group.description IS '说明';
COMMENT ON COLUMN monitor_rule_group.sort IS '同级排序，升序';
COMMENT ON COLUMN monitor_rule_group.create_time IS '创建时间';
COMMENT ON COLUMN monitor_rule_group.update_time IS '更新时间';
COMMENT ON COLUMN monitor_rule_group.delete_time IS '软删时间';
COMMENT ON COLUMN monitor_rule_group.create_user IS '创建人';
COMMENT ON COLUMN monitor_rule_group.update_user IS '最近更新人';
COMMENT ON COLUMN monitor_rule_group.delete_user IS '删除人';
-- 顶级分组 parent_id 为 NULL：NULLS NOT DISTINCT 使顶级分组之间同样不能重名。
CREATE UNIQUE INDEX uk_monitor_rule_group_name ON monitor_rule_group (parent_id, name) NULLS NOT DISTINCT WHERE delete_time IS NULL;
CREATE INDEX idx_monitor_rule_group_path ON monitor_rule_group (path text_pattern_ops) WHERE delete_time IS NULL;

CREATE TABLE monitor_role_scope (
    role_id     uuid        NOT NULL,
    group_id    uuid        NOT NULL REFERENCES monitor_rule_group (id),
    level       smallint    NOT NULL,
    update_user uuid        NULL,
    update_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (role_id, group_id),
    CONSTRAINT ck_monitor_role_scope_level CHECK (level IN (1, 2))
);
COMMENT ON TABLE monitor_role_scope IS '监控角色数据范围（物理删除，按分组整组替换）：角色对规则分组的授权，授权某分组即包含其全部下级；用户的有效范围为其全部启用角色的并集，超级管理员不限';
COMMENT ON COLUMN monitor_role_scope.role_id IS '角色 ID，引用 system_role.id（不设外键：角色删除后授权行无害，解析时按启用角色读取）';
COMMENT ON COLUMN monitor_role_scope.group_id IS '规则分组 ID';
COMMENT ON COLUMN monitor_role_scope.level IS '授权级别：1=只读 2=可管理';
COMMENT ON COLUMN monitor_role_scope.update_user IS '最近授权人';
COMMENT ON COLUMN monitor_role_scope.update_time IS '最近授权时间';
CREATE INDEX idx_monitor_role_scope_group ON monitor_role_scope (group_id);

CREATE TABLE monitor_rule (
    id                uuid         PRIMARY KEY,
    group_id          uuid         NOT NULL REFERENCES monitor_rule_group (id),
    name              varchar(128) NOT NULL,
    description       varchar(512) NOT NULL DEFAULT '',
    enabled           boolean      NOT NULL DEFAULT true,
    labels            jsonb        NOT NULL DEFAULT '{}',
    datasource_scope  smallint     NOT NULL DEFAULT 1,
    datasource_ids    uuid[]       NOT NULL DEFAULT '{}',
    queries           jsonb        NOT NULL,
    level_inhibit     boolean      NOT NULL DEFAULT true,
    eval_interval_s   integer      NOT NULL DEFAULT 60,
    for_s             integer      NOT NULL DEFAULT 60,
    recover_s         integer      NOT NULL DEFAULT 0,
    nodata_keep_s     integer      NOT NULL DEFAULT 0,
    effective         jsonb        NOT NULL DEFAULT '{}',
    annotations       jsonb        NOT NULL DEFAULT '{}',
    recovery_expr     text         NOT NULL DEFAULT '',
    notify            jsonb        NOT NULL DEFAULT '{}',
    notify_recovery   boolean      NOT NULL DEFAULT true,
    repeat_interval_m integer      NOT NULL DEFAULT 0,
    max_notify        integer      NOT NULL DEFAULT 0,
    group_key_labels  text[]       NOT NULL DEFAULT '{}',
    version           integer      NOT NULL DEFAULT 1,
    spec_time         timestamptz  NOT NULL DEFAULT now(),
    create_time       timestamptz  NOT NULL DEFAULT now(),
    update_time       timestamptz  NOT NULL DEFAULT now(),
    delete_time       timestamptz  NULL,
    create_user       uuid         NULL,
    update_user       uuid         NULL,
    delete_user       uuid         NULL,
    CONSTRAINT ck_monitor_rule_labels CHECK (jsonb_typeof(labels) = 'object'),
    CONSTRAINT ck_monitor_rule_datasource CHECK (
        (datasource_scope = 1 AND cardinality(datasource_ids) BETWEEN 1 AND 20)
        OR (datasource_scope = 2 AND cardinality(datasource_ids) = 0)),
    CONSTRAINT ck_monitor_rule_queries CHECK (jsonb_typeof(queries) = 'array' AND jsonb_array_length(queries) BETWEEN 1 AND 4),
    CONSTRAINT ck_monitor_rule_eval_interval CHECK (eval_interval_s BETWEEN 15 AND 3600),
    CONSTRAINT ck_monitor_rule_for CHECK (for_s BETWEEN 0 AND 86400),
    CONSTRAINT ck_monitor_rule_recover CHECK (recover_s BETWEEN 0 AND 86400),
    CONSTRAINT ck_monitor_rule_nodata_keep CHECK (nodata_keep_s BETWEEN 0 AND 86400),
    CONSTRAINT ck_monitor_rule_effective CHECK (jsonb_typeof(effective) = 'object'),
    CONSTRAINT ck_monitor_rule_annotations CHECK (jsonb_typeof(annotations) = 'object'),
    CONSTRAINT ck_monitor_rule_notify CHECK (jsonb_typeof(notify) = 'object'),
    CONSTRAINT ck_monitor_rule_repeat_interval CHECK (repeat_interval_m = 0 OR repeat_interval_m BETWEEN 5 AND 10080),
    CONSTRAINT ck_monitor_rule_max_notify CHECK (max_notify BETWEEN 0 AND 1000)
);
COMMENT ON TABLE monitor_rule IS '告警规则：一组 PromQL 查询（每条对应一个级别）加判定与通知参数，软删；core 写，引擎每次评估读行';
COMMENT ON COLUMN monitor_rule.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_rule.group_id IS '所属规则分组';
COMMENT ON COLUMN monitor_rule.name IS '名称，同一分组未删除行内唯一';
COMMENT ON COLUMN monitor_rule.description IS '说明';
COMMENT ON COLUMN monitor_rule.enabled IS '是否启用；停用后引擎删除其评估任务并关闭活跃告警';
COMMENT ON COLUMN monitor_rule.labels IS '附加标签 {名称: 值}，至多 20 个；与序列标签合并（同名时附加标签优先）后成为事件标签';
COMMENT ON COLUMN monitor_rule.datasource_scope IS '数据源范围：1=指定数据源 2=全部启用的数据源';
COMMENT ON COLUMN monitor_rule.datasource_ids IS '指定的数据源 ID，范围为指定时 1-20 个，全部时为空';
COMMENT ON COLUMN monitor_rule.queries IS '查询与级别 [{"expr":"PromQL","severity":3}]，1-4 条，severity（1-4）互不相同；查询有结果即为异常';
COMMENT ON COLUMN monitor_rule.level_inhibit IS '级别抑制：同一序列同时命中多档时只保留最高级别；关闭时各级别是各自独立的事件';
COMMENT ON COLUMN monitor_rule.eval_interval_s IS '评估周期秒，15-3600';
COMMENT ON COLUMN monitor_rule.for_s IS '持续时长秒，0-86400：异常持续达到该时长才触发，0=立即触发';
COMMENT ON COLUMN monitor_rule.recover_s IS '恢复留观时长秒，0-86400：恢复正常并持续该时长才判恢复';
COMMENT ON COLUMN monitor_rule.nodata_keep_s IS '序列消失后保持告警的秒数，0-86400；0=立即进入恢复判定。实际留观时长取它与 recover_s 的较大者';
COMMENT ON COLUMN monitor_rule.effective IS '生效时间 {"timezone":"Asia/Shanghai","windows":[{"days":[1,2,3,4,5],"start":"09:00","end":"18:00"}]}；days 1-7=周一至周日，start>end 表示跨午夜，windows 为空=全时段';
COMMENT ON COLUMN monitor_rule.annotations IS '附加信息 {名称: 模板}，至多 10 个；触发时按 Go text/template 渲染（可用 $labels / $value）后写入事件';
COMMENT ON COLUMN monitor_rule.recovery_expr IS '恢复取值 PromQL（模板，可用 $labels），空=不取值；恢复时执行并把结果记为事件的恢复值';
COMMENT ON COLUMN monitor_rule.notify IS '通知对象 {"user_ids":[],"schedule_ids":[],"dept_ids":[]}：用户、值班表当前值班人、部门负责人';
COMMENT ON COLUMN monitor_rule.notify_recovery IS '恢复时是否通知';
COMMENT ON COLUMN monitor_rule.repeat_interval_m IS '重复通知间隔分钟：0=不重复，否则 5-10080';
COMMENT ON COLUMN monitor_rule.max_notify IS '单个事件的总通知次数上限（含首次，不含恢复通知），0=不限';
COMMENT ON COLUMN monitor_rule.group_key_labels IS '通知聚合键标签名；空=按规则聚合';
COMMENT ON COLUMN monitor_rule.version IS '版本号：每次保存 +1，同时用作乐观锁并对应 monitor_rule_version.version';
COMMENT ON COLUMN monitor_rule.spec_time IS '判定相关配置（查询、级别抑制、时间参数、数据源范围、附加标签）最近一次变更时间';
COMMENT ON COLUMN monitor_rule.create_time IS '创建时间';
COMMENT ON COLUMN monitor_rule.update_time IS '更新时间';
COMMENT ON COLUMN monitor_rule.delete_time IS '软删时间';
COMMENT ON COLUMN monitor_rule.create_user IS '创建人';
COMMENT ON COLUMN monitor_rule.update_user IS '最近更新人';
COMMENT ON COLUMN monitor_rule.delete_user IS '删除人';
CREATE UNIQUE INDEX uk_monitor_rule_name ON monitor_rule (group_id, name) WHERE delete_time IS NULL;
CREATE INDEX idx_monitor_rule_datasource ON monitor_rule USING gin (datasource_ids) WHERE delete_time IS NULL;

CREATE TABLE monitor_rule_version (
    id          uuid         PRIMARY KEY,
    rule_id     uuid         NOT NULL REFERENCES monitor_rule (id),
    version     integer      NOT NULL,
    snapshot    jsonb        NOT NULL,
    action      varchar(16)  NOT NULL,
    summary     varchar(255) NOT NULL DEFAULT '',
    operator    uuid         NULL,
    create_time timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_monitor_rule_version_action CHECK (
        action IN ('create', 'update', 'enable', 'disable', 'move', 'rollback', 'import', 'delete', 'batch'))
);
COMMENT ON TABLE monitor_rule_version IS '告警规则版本快照（append-only）：规则每次保存记一行，供变更历史、对比与回滚';
COMMENT ON COLUMN monitor_rule_version.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_rule_version.rule_id IS '规则 ID';
COMMENT ON COLUMN monitor_rule_version.version IS '规则版本号，等于保存后 monitor_rule.version';
COMMENT ON COLUMN monitor_rule_version.snapshot IS '保存后的整条规则（配置字段全集）';
COMMENT ON COLUMN monitor_rule_version.action IS '操作：create / update / enable / disable / move / rollback / import / delete / batch';
COMMENT ON COLUMN monitor_rule_version.summary IS '一句话变更说明';
COMMENT ON COLUMN monitor_rule_version.operator IS '操作人';
COMMENT ON COLUMN monitor_rule_version.create_time IS '操作时间';
CREATE UNIQUE INDEX uk_monitor_rule_version ON monitor_rule_version (rule_id, version);

CREATE TABLE monitor_mute (
    id             uuid         PRIMARY KEY,
    group_id       uuid         NULL REFERENCES monitor_rule_group (id),
    name           varchar(128) NOT NULL,
    reason         varchar(255) NOT NULL,
    rule_ids       uuid[]       NOT NULL DEFAULT '{}',
    datasource_ids uuid[]       NOT NULL DEFAULT '{}',
    severities     smallint[]   NOT NULL DEFAULT '{}',
    matchers       jsonb        NOT NULL DEFAULT '[]',
    mode           smallint     NOT NULL DEFAULT 1,
    time_type      smallint     NOT NULL DEFAULT 1,
    start_time     timestamptz  NULL,
    end_time       timestamptz  NULL,
    periodic       jsonb        NULL,
    enabled        boolean      NOT NULL DEFAULT true,
    version        integer      NOT NULL DEFAULT 1,
    create_time    timestamptz  NOT NULL DEFAULT now(),
    update_time    timestamptz  NOT NULL DEFAULT now(),
    delete_time    timestamptz  NULL,
    create_user    uuid         NULL,
    update_user    uuid         NULL,
    delete_user    uuid         NULL,
    CONSTRAINT ck_monitor_mute_reason CHECK (reason <> ''),
    CONSTRAINT ck_monitor_mute_rules CHECK (cardinality(rule_ids) <= 200),
    CONSTRAINT ck_monitor_mute_datasources CHECK (cardinality(datasource_ids) <= 50),
    CONSTRAINT ck_monitor_mute_severities CHECK (severities <@ ARRAY[1, 2, 3, 4]::smallint[]),
    CONSTRAINT ck_monitor_mute_matchers CHECK (jsonb_typeof(matchers) = 'array' AND jsonb_array_length(matchers) <= 20),
    CONSTRAINT ck_monitor_mute_mode CHECK (mode IN (1, 2)),
    CONSTRAINT ck_monitor_mute_time_type CHECK (time_type IN (1, 2)),
    CONSTRAINT ck_monitor_mute_time CHECK (
        (time_type = 1 AND start_time IS NOT NULL AND end_time IS NOT NULL AND periodic IS NULL)
        OR (time_type = 2 AND periodic IS NOT NULL AND jsonb_typeof(periodic) = 'object')),
    CONSTRAINT ck_monitor_mute_range CHECK (start_time IS NULL OR end_time IS NULL OR end_time > start_time)
);
COMMENT ON TABLE monitor_mute IS '屏蔽规则：在时间范围内屏蔽命中条件的告警（只屏蔽通知，或连事件一并不产生），软删；core 写，引擎只读';
COMMENT ON COLUMN monitor_mute.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_mute.group_id IS '作用的规则分组（含其全部下级）；NULL=全部分组，仅超级管理员可建';
COMMENT ON COLUMN monitor_mute.name IS '名称';
COMMENT ON COLUMN monitor_mute.reason IS '屏蔽原因，必填';
COMMENT ON COLUMN monitor_mute.rule_ids IS '限定的规则 ID，至多 200 个；空=不限';
COMMENT ON COLUMN monitor_mute.datasource_ids IS '限定的数据源 ID，至多 50 个；空=不限';
COMMENT ON COLUMN monitor_mute.severities IS '限定的级别（1-4）；空=不限';
COMMENT ON COLUMN monitor_mute.matchers IS '事件标签匹配条件 [{"label":"instance","op":"=~","value":"10\\..*"},{"label":"env","op":"in","values":["a","b"]}]，至多 20 行，行间为与；op 取 == / != / =~ / !~ / in / not in';
COMMENT ON COLUMN monitor_mute.mode IS '屏蔽方式：1=只屏蔽通知（事件照常产生并标记已屏蔽） 2=屏蔽事件（不产生事件）';
COMMENT ON COLUMN monitor_mute.time_type IS '时间类型：1=一次性 2=周期';
COMMENT ON COLUMN monitor_mute.start_time IS '开始时间（含）；一次性时必填，周期时可选（周期规则自该时刻起生效）';
COMMENT ON COLUMN monitor_mute.end_time IS '结束时间（不含）；一次性时必填，周期时可选（周期规则到该时刻失效）';
COMMENT ON COLUMN monitor_mute.periodic IS '周期时间窗，结构同 monitor_rule.effective；一次性时为 NULL';
COMMENT ON COLUMN monitor_mute.enabled IS '是否启用';
COMMENT ON COLUMN monitor_mute.version IS '乐观锁版本号：每次保存 +1';
COMMENT ON COLUMN monitor_mute.create_time IS '创建时间';
COMMENT ON COLUMN monitor_mute.update_time IS '更新时间';
COMMENT ON COLUMN monitor_mute.delete_time IS '软删时间';
COMMENT ON COLUMN monitor_mute.create_user IS '创建人';
COMMENT ON COLUMN monitor_mute.update_user IS '最近更新人';
COMMENT ON COLUMN monitor_mute.delete_user IS '删除人';
CREATE INDEX idx_monitor_mute_group ON monitor_mute (group_id) WHERE delete_time IS NULL;

CREATE TABLE monitor_subscription (
    id             uuid         PRIMARY KEY,
    name           varchar(128) NOT NULL,
    owner_id       uuid         NOT NULL,
    group_ids      uuid[]       NOT NULL,
    rule_ids       uuid[]       NOT NULL DEFAULT '{}',
    datasource_ids uuid[]       NOT NULL DEFAULT '{}',
    severities     smallint[]   NOT NULL,
    on_firing      boolean      NOT NULL DEFAULT true,
    on_resolved    boolean      NOT NULL DEFAULT false,
    matchers       jsonb        NOT NULL DEFAULT '[]',
    receivers      jsonb        NOT NULL DEFAULT '{"self": true}',
    follow_repeat  boolean      NOT NULL DEFAULT false,
    enabled        boolean      NOT NULL DEFAULT true,
    version        integer      NOT NULL DEFAULT 1,
    create_time    timestamptz  NOT NULL DEFAULT now(),
    update_time    timestamptz  NOT NULL DEFAULT now(),
    delete_time    timestamptz  NULL,
    create_user    uuid         NULL,
    update_user    uuid         NULL,
    delete_user    uuid         NULL,
    CONSTRAINT ck_monitor_subscription_groups CHECK (cardinality(group_ids) BETWEEN 1 AND 50),
    CONSTRAINT ck_monitor_subscription_rules CHECK (cardinality(rule_ids) <= 200),
    CONSTRAINT ck_monitor_subscription_datasources CHECK (cardinality(datasource_ids) <= 50),
    CONSTRAINT ck_monitor_subscription_severities CHECK (
        cardinality(severities) >= 1 AND severities <@ ARRAY[1, 2, 3, 4]::smallint[]),
    CONSTRAINT ck_monitor_subscription_event CHECK (on_firing OR on_resolved),
    CONSTRAINT ck_monitor_subscription_matchers CHECK (jsonb_typeof(matchers) = 'array' AND jsonb_array_length(matchers) <= 20),
    CONSTRAINT ck_monitor_subscription_receivers CHECK (jsonb_typeof(receivers) = 'object')
);
COMMENT ON TABLE monitor_subscription IS '告警订阅规则：按分组 / 规则 / 数据源 / 级别 / 标签订阅告警，命中时另发一条订阅通知，软删；仅 core 读写';
COMMENT ON COLUMN monitor_subscription.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_subscription.name IS '名称';
COMMENT ON COLUMN monitor_subscription.owner_id IS '所有者（创建人）；其当前对事件所在分组须有只读以上范围，订阅才生效';
COMMENT ON COLUMN monitor_subscription.group_ids IS '订阅的规则分组（含其全部下级），1-50 个';
COMMENT ON COLUMN monitor_subscription.rule_ids IS '限定的规则 ID，至多 200 个；空=不限';
COMMENT ON COLUMN monitor_subscription.datasource_ids IS '限定的数据源 ID，至多 50 个；空=不限';
COMMENT ON COLUMN monitor_subscription.severities IS '订阅的级别（1-4），至少一档';
COMMENT ON COLUMN monitor_subscription.on_firing IS '触发时通知';
COMMENT ON COLUMN monitor_subscription.on_resolved IS '恢复时通知';
COMMENT ON COLUMN monitor_subscription.matchers IS '事件标签匹配条件，结构同 monitor_mute.matchers，至多 20 行';
COMMENT ON COLUMN monitor_subscription.receivers IS '接收方 {"self":true,"user_ids":[],"schedule_ids":[],"groups":["$group:ops"]}：本人、指定用户、值班表当前值班人、通知群标识';
COMMENT ON COLUMN monitor_subscription.follow_repeat IS '是否跟随规则的重复通知';
COMMENT ON COLUMN monitor_subscription.enabled IS '是否启用';
COMMENT ON COLUMN monitor_subscription.version IS '乐观锁版本号：每次保存 +1';
COMMENT ON COLUMN monitor_subscription.create_time IS '创建时间';
COMMENT ON COLUMN monitor_subscription.update_time IS '更新时间';
COMMENT ON COLUMN monitor_subscription.delete_time IS '软删时间';
COMMENT ON COLUMN monitor_subscription.create_user IS '创建人';
COMMENT ON COLUMN monitor_subscription.update_user IS '最近更新人';
COMMENT ON COLUMN monitor_subscription.delete_user IS '删除人';
CREATE INDEX idx_monitor_subscription_owner ON monitor_subscription (owner_id) WHERE delete_time IS NULL;

CREATE TABLE monitor_cfg_version (
    name        varchar(64) PRIMARY KEY,
    version     bigint      NOT NULL DEFAULT 1,
    update_time timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE monitor_cfg_version IS '监控配置版本：core 每次写对应配置表时同事务 +1，评估引擎轮询比对后刷新进程内只读缓存';
COMMENT ON COLUMN monitor_cfg_version.name IS '配置表名：monitor_datasource / monitor_mute / monitor_rule_group';
COMMENT ON COLUMN monitor_cfg_version.version IS '版本号，单调递增';
COMMENT ON COLUMN monitor_cfg_version.update_time IS '最近一次变更时间';

-- ---------------------------------------------------------------------------
-- 运行表
-- ---------------------------------------------------------------------------

CREATE TABLE monitor_eval_task (
    rule_id          uuid         NOT NULL,
    datasource_id    uuid         NOT NULL,
    next_eval_time   timestamptz  NOT NULL DEFAULT now(),
    lease_owner      varchar(64)  NULL,
    lease_until      timestamptz  NULL,
    last_eval_time   timestamptz  NULL,
    last_status      smallint     NOT NULL DEFAULT 0,
    last_error       varchar(512) NOT NULL DEFAULT '',
    last_duration_ms integer      NOT NULL DEFAULT 0,
    firing_count     integer      NOT NULL DEFAULT 0,
    create_time      timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (rule_id, datasource_id),
    CONSTRAINT ck_monitor_eval_task_status CHECK (last_status BETWEEN 0 AND 5)
);
COMMENT ON TABLE monitor_eval_task IS '评估任务：一行 = 规则 × 数据源；引擎选主实例按「启用规则 × 其生效的启用数据源」物化，评估实例以租约领取';
COMMENT ON COLUMN monitor_eval_task.rule_id IS '规则 ID';
COMMENT ON COLUMN monitor_eval_task.datasource_id IS '数据源 ID';
COMMENT ON COLUMN monitor_eval_task.next_eval_time IS '下次评估时间（数据库时钟）';
COMMENT ON COLUMN monitor_eval_task.lease_owner IS '领取实例的属主令牌；NULL=未被领取。写回结果以它围栏';
COMMENT ON COLUMN monitor_eval_task.lease_until IS '租约到期时间；过期的任务可被其它实例重领';
COMMENT ON COLUMN monitor_eval_task.last_eval_time IS '最近一次评估时间';
COMMENT ON COLUMN monitor_eval_task.last_status IS '最近一次评估结果：0=未评估 1=正常 2=告警中 3=无数据 4=查询错误 5=不在生效时间';
COMMENT ON COLUMN monitor_eval_task.last_error IS '最近一次评估的错误信息（不含凭据，至多 512 字节）';
COMMENT ON COLUMN monitor_eval_task.last_duration_ms IS '最近一次评估耗时毫秒';
COMMENT ON COLUMN monitor_eval_task.firing_count IS '当前告警中的序列数';
COMMENT ON COLUMN monitor_eval_task.create_time IS '任务物化时间';
CREATE INDEX idx_monitor_eval_task_due ON monitor_eval_task (next_eval_time);
CREATE INDEX idx_monitor_eval_task_datasource ON monitor_eval_task (datasource_id);

CREATE TABLE monitor_eval_state (
    rule_id       uuid             NOT NULL,
    datasource_id uuid             NOT NULL,
    fingerprint   varchar(32)      NOT NULL,
    state         smallint         NOT NULL,
    severity      smallint         NOT NULL,
    labels        jsonb            NOT NULL DEFAULT '{}',
    value         double precision NULL,
    query_index   smallint         NOT NULL DEFAULT 0,
    active_since  timestamptz      NOT NULL,
    last_seen     timestamptz      NOT NULL,
    recover_since timestamptz      NULL,
    alert_id      uuid             NULL,
    PRIMARY KEY (rule_id, datasource_id, fingerprint),
    CONSTRAINT ck_monitor_eval_state_state CHECK (state IN (1, 2, 3)),
    CONSTRAINT ck_monitor_eval_state_severity CHECK (severity BETWEEN 1 AND 4)
);
COMMENT ON TABLE monitor_eval_state IS '判定状态：一行 = 规则 × 数据源 × 序列；引擎状态机的持久化，实例重启或任务换实例后接续判定';
COMMENT ON COLUMN monitor_eval_state.rule_id IS '规则 ID';
COMMENT ON COLUMN monitor_eval_state.datasource_id IS '数据源 ID';
COMMENT ON COLUMN monitor_eval_state.fingerprint IS '序列指纹（kit/monitorcontract.Fingerprint），32 位 hex';
COMMENT ON COLUMN monitor_eval_state.state IS '状态：1=待触发（异常未满持续时长） 2=告警中 3=留观中（已不异常，等待恢复留观期满）';
COMMENT ON COLUMN monitor_eval_state.severity IS '当前级别 1-4';
COMMENT ON COLUMN monitor_eval_state.labels IS '事件标签（序列标签去掉 __name__ 后与规则附加标签合并）';
COMMENT ON COLUMN monitor_eval_state.value IS '最近一次查询到的值';
COMMENT ON COLUMN monitor_eval_state.query_index IS '命中的查询在 monitor_rule.queries 中的下标（从 0 起）';
COMMENT ON COLUMN monitor_eval_state.active_since IS '本轮异常首次出现的时间';
COMMENT ON COLUMN monitor_eval_state.last_seen IS '最近一次查询到该序列的时间';
COMMENT ON COLUMN monitor_eval_state.recover_since IS '进入留观的时间；非留观中为 NULL';
COMMENT ON COLUMN monitor_eval_state.alert_id IS '对应的活跃告警事件；尚未触发或被「屏蔽事件」命中时为 NULL';

CREATE TABLE monitor_alert (
    id               uuid             PRIMARY KEY,
    fingerprint      varchar(32)      NOT NULL,
    rule_id          uuid             NOT NULL,
    rule_name        varchar(128)     NOT NULL,
    group_id         uuid             NOT NULL,
    datasource_id    uuid             NOT NULL,
    datasource_name  varchar(64)      NOT NULL,
    severity         smallint         NOT NULL,
    status           smallint         NOT NULL DEFAULT 1,
    labels           jsonb            NOT NULL DEFAULT '{}',
    annotations      jsonb            NOT NULL DEFAULT '{}',
    expr             text             NOT NULL DEFAULT '',
    value            double precision NULL,
    recovery_value   double precision NULL,
    first_fire_time  timestamptz      NOT NULL,
    last_fire_time   timestamptz      NOT NULL,
    resolve_time     timestamptz      NULL,
    muted            boolean          NOT NULL DEFAULT false,
    mute_id          uuid             NULL,
    notify_count     integer          NOT NULL DEFAULT 0,
    last_notify_time timestamptz      NULL,
    group_key        varchar(128)     NOT NULL DEFAULT '',
    CONSTRAINT ck_monitor_alert_severity CHECK (severity BETWEEN 1 AND 4),
    CONSTRAINT ck_monitor_alert_status CHECK (status IN (1, 2, 3)),
    CONSTRAINT ck_monitor_alert_labels CHECK (jsonb_typeof(labels) = 'object'),
    CONSTRAINT ck_monitor_alert_resolve CHECK ((status = 1) = (resolve_time IS NULL))
);
COMMENT ON TABLE monitor_alert IS '告警事件：一次从触发到恢复（或关闭）的告警；引擎写，core 读并回写通知计数。同一指纹至多一个活跃事件';
COMMENT ON COLUMN monitor_alert.id IS '主键，UUIDv7，引擎生成';
COMMENT ON COLUMN monitor_alert.fingerprint IS '事件指纹：sha256(规则 ID | 数据源 ID | 排序后的事件标签 [| 级别]) 的前 32 位 hex；未开启级别抑制时含级别';
COMMENT ON COLUMN monitor_alert.rule_id IS '规则 ID';
COMMENT ON COLUMN monitor_alert.rule_name IS '规则名称（冗余：规则删除或改名后事件仍可读）';
COMMENT ON COLUMN monitor_alert.group_id IS '触发时规则所属分组（数据范围过滤依据）';
COMMENT ON COLUMN monitor_alert.datasource_id IS '数据源 ID';
COMMENT ON COLUMN monitor_alert.datasource_name IS '数据源名称（冗余）';
COMMENT ON COLUMN monitor_alert.severity IS '级别：1=提示 2=警告 3=严重 4=致命；告警期间随最新判定更新';
COMMENT ON COLUMN monitor_alert.status IS '状态：1=告警中 2=已恢复 3=已关闭（规则被删除 / 停用、数据源移出范围时一并关闭，不发恢复通知）';
COMMENT ON COLUMN monitor_alert.labels IS '事件标签 {名称: 值}：序列标签（去掉 __name__）与规则附加标签合并';
COMMENT ON COLUMN monitor_alert.annotations IS '渲染后的附加信息 {名称: 文本}';
COMMENT ON COLUMN monitor_alert.expr IS '触发的 PromQL';
COMMENT ON COLUMN monitor_alert.value IS '最近一次触发值';
COMMENT ON COLUMN monitor_alert.recovery_value IS '恢复取值（恢复时执行 recovery_expr 的结果）；未配置或取值失败为 NULL';
COMMENT ON COLUMN monitor_alert.first_fire_time IS '首次触发时间';
COMMENT ON COLUMN monitor_alert.last_fire_time IS '最近一次判定仍在告警的时间';
COMMENT ON COLUMN monitor_alert.resolve_time IS '恢复或关闭时间；告警中为 NULL';
COMMENT ON COLUMN monitor_alert.muted IS '是否被「只屏蔽通知」的屏蔽规则命中（不发通知）';
COMMENT ON COLUMN monitor_alert.mute_id IS '命中的屏蔽规则 ID';
COMMENT ON COLUMN monitor_alert.notify_count IS '已发出的触发 / 重复通知次数（不含恢复通知）；由 core 通知派发回写';
COMMENT ON COLUMN monitor_alert.last_notify_time IS '最近一次通知时间；由 core 通知派发回写';
COMMENT ON COLUMN monitor_alert.group_key IS '交给通知管道的聚合键（kit/monitorcontract.GroupKey）';
CREATE UNIQUE INDEX uk_monitor_alert_active ON monitor_alert (fingerprint) WHERE status = 1;
CREATE INDEX idx_monitor_alert_status ON monitor_alert (status, last_fire_time DESC);
CREATE INDEX idx_monitor_alert_group ON monitor_alert (group_id, status);
CREATE INDEX idx_monitor_alert_rule ON monitor_alert (rule_id, first_fire_time DESC);
CREATE INDEX idx_monitor_alert_resolve ON monitor_alert (resolve_time) WHERE status <> 1;
-- 标签过滤一律用包含运算（labels @> '{"env":"prod"}'）才能走本索引。
CREATE INDEX idx_monitor_alert_labels ON monitor_alert USING gin (labels jsonb_path_ops);

-- 判定记录：按天 RANGE 分区（eval_time），保留 7 天，按分区 DROP 清理。分区表唯一约束须含分区键，
-- 故主键为 (id, eval_time)。DEFAULT 分区兜底：分区维护失效时写入不被拒绝。
CREATE TABLE monitor_eval_record (
    id            uuid         NOT NULL,
    rule_id       uuid         NOT NULL,
    datasource_id uuid         NOT NULL,
    eval_time     timestamptz  NOT NULL,
    duration_ms   integer      NOT NULL DEFAULT 0,
    status        smallint     NOT NULL,
    error         varchar(512) NOT NULL DEFAULT '',
    series        integer      NOT NULL DEFAULT 0,
    anomalies     integer      NOT NULL DEFAULT 0,
    recovered     integer      NOT NULL DEFAULT 0,
    detail        jsonb        NOT NULL DEFAULT '{}',
    PRIMARY KEY (id, eval_time),
    CONSTRAINT ck_monitor_eval_record_status CHECK (status BETWEEN 1 AND 5)
) PARTITION BY RANGE (eval_time);
COMMENT ON TABLE monitor_eval_record IS '判定记录：每次评估一行，供排查「为什么告警 / 为什么没告警」；按天 RANGE 分区，保留 7 天';
COMMENT ON COLUMN monitor_eval_record.id IS '记录 ID，UUIDv7，引擎生成';
COMMENT ON COLUMN monitor_eval_record.rule_id IS '规则 ID';
COMMENT ON COLUMN monitor_eval_record.datasource_id IS '数据源 ID';
COMMENT ON COLUMN monitor_eval_record.eval_time IS '评估时间（分区键，数据库时钟）';
COMMENT ON COLUMN monitor_eval_record.duration_ms IS '评估耗时毫秒';
COMMENT ON COLUMN monitor_eval_record.status IS '评估结果：1=正常 2=告警中 3=无数据 4=查询错误 5=不在生效时间';
COMMENT ON COLUMN monitor_eval_record.error IS '错误信息（不含凭据，至多 512 字节）';
COMMENT ON COLUMN monitor_eval_record.series IS '查询返回的序列总数';
COMMENT ON COLUMN monitor_eval_record.anomalies IS '本次判为异常的序列数';
COMMENT ON COLUMN monitor_eval_record.recovered IS '本次判为恢复的序列数';
COMMENT ON COLUMN monitor_eval_record.detail IS '明细 {"queries":[{"index","severity","series","truncated","error"}],"points":[{"fingerprint","labels","severity","value","step","alert_id","mute_id"}],"truncated":bool}；points 至多 50 条（kit/monitorcontract.EvalDetail）';
CREATE INDEX idx_monitor_eval_record_rule ON monitor_eval_record (rule_id, eval_time DESC);

CREATE TABLE monitor_eval_record_default PARTITION OF monitor_eval_record DEFAULT;
-- 预建当天与未来 2 天的分区（UTC 日界；边界字面量带显式 +00，不随会话时区漂移）。
-- 命名 monitor_eval_record_YYYY_MM_DD，之后由引擎的维护任务接管（预建 / 按保留期 DROP）。
DO $$
DECLARE
    d date := (now() AT TIME ZONE 'UTC')::date;
    i integer;
BEGIN
    FOR i IN 0..2 LOOP
        EXECUTE format(
            'CREATE TABLE monitor_eval_record_%s PARTITION OF monitor_eval_record FOR VALUES FROM (%L) TO (%L)',
            to_char(d + i, 'YYYY_MM_DD'),
            to_char(d + i, 'YYYY-MM-DD') || ' 00:00:00+00',
            to_char(d + i + 1, 'YYYY-MM-DD') || ' 00:00:00+00');
    END LOOP;
END
$$;

CREATE TABLE monitor_notify_outbox (
    id          uuid         PRIMARY KEY,
    alert_id    uuid         NOT NULL,
    kind        smallint     NOT NULL,
    seq         integer      NOT NULL DEFAULT 1,
    status      smallint     NOT NULL DEFAULT 1,
    attempts    integer      NOT NULL DEFAULT 0,
    last_error  varchar(512) NOT NULL DEFAULT '',
    create_time timestamptz  NOT NULL DEFAULT now(),
    done_time   timestamptz  NULL,
    CONSTRAINT ck_monitor_notify_outbox_kind CHECK (kind IN (1, 2, 3)),
    CONSTRAINT ck_monitor_notify_outbox_seq CHECK (seq >= 1),
    CONSTRAINT ck_monitor_notify_outbox_status CHECK (status IN (1, 2, 3))
);
COMMENT ON TABLE monitor_notify_outbox IS '通知发件箱：引擎需要通知时写一行，core 的 monitor-notify 循环领取后解析收件人并写通知事件';
COMMENT ON COLUMN monitor_notify_outbox.id IS '主键，UUIDv7，引擎生成';
COMMENT ON COLUMN monitor_notify_outbox.alert_id IS '告警事件 ID';
COMMENT ON COLUMN monitor_notify_outbox.kind IS '通知类型：1=触发 2=重复 3=恢复';
COMMENT ON COLUMN monitor_notify_outbox.seq IS '该事件的第几次通知（触发为 1，重复通知自 2 递增；恢复通知取恢复时已发出的次数 + 1）';
COMMENT ON COLUMN monitor_notify_outbox.status IS '状态：1=待派发 2=已派发 3=已丢弃（规则已删除、事件被屏蔽或重试超限）';
COMMENT ON COLUMN monitor_notify_outbox.attempts IS '派发失败次数；超过 10 次置为已丢弃';
COMMENT ON COLUMN monitor_notify_outbox.last_error IS '最近一次派发失败原因';
COMMENT ON COLUMN monitor_notify_outbox.create_time IS '入箱时间';
COMMENT ON COLUMN monitor_notify_outbox.done_time IS '派发或丢弃时间';
CREATE UNIQUE INDEX uk_monitor_notify_outbox ON monitor_notify_outbox (alert_id, kind, seq);
CREATE INDEX idx_monitor_notify_outbox_pending ON monitor_notify_outbox (create_time) WHERE status = 1;
CREATE INDEX idx_monitor_notify_outbox_done ON monitor_notify_outbox (done_time) WHERE status <> 1;

CREATE TABLE monitor_notify_log (
    id               uuid         PRIMARY KEY,
    alert_id         uuid         NOT NULL,
    kind             smallint     NOT NULL,
    seq              integer      NOT NULL,
    source           smallint     NOT NULL,
    subscription_id  uuid         NULL,
    recipients       jsonb        NOT NULL DEFAULT '[]',
    notify_event_ids jsonb        NOT NULL DEFAULT '[]',
    note             varchar(255) NOT NULL DEFAULT '',
    create_time      timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_monitor_notify_log_kind CHECK (kind IN (1, 2, 3)),
    CONSTRAINT ck_monitor_notify_log_source CHECK (source IN (1, 2)),
    CONSTRAINT ck_monitor_notify_log_subscription CHECK ((source = 2) = (subscription_id IS NOT NULL)),
    CONSTRAINT ck_monitor_notify_log_recipients CHECK (jsonb_typeof(recipients) = 'array'),
    CONSTRAINT ck_monitor_notify_log_events CHECK (jsonb_typeof(notify_event_ids) = 'array')
);
COMMENT ON TABLE monitor_notify_log IS '告警通知记录：每次派发按来源（规则 / 某条订阅）各记一行；事件详情的通知时间线、订阅命中统计与「通知对象可查看事件」的依据';
COMMENT ON COLUMN monitor_notify_log.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_notify_log.alert_id IS '告警事件 ID';
COMMENT ON COLUMN monitor_notify_log.kind IS '通知类型：1=触发 2=重复 3=恢复';
COMMENT ON COLUMN monitor_notify_log.seq IS '该事件的第几次通知';
COMMENT ON COLUMN monitor_notify_log.source IS '来源：1=规则的通知对象 2=订阅规则';
COMMENT ON COLUMN monitor_notify_log.subscription_id IS '命中的订阅规则 ID；来源为规则时 NULL';
COMMENT ON COLUMN monitor_notify_log.recipients IS '收件方字符串数组：用户 ID 或通知群标识（如 "$group:ops"）；按用户查询用 recipients @> ''["<用户 ID>"]''';
COMMENT ON COLUMN monitor_notify_log.notify_event_ids IS '写入的通知事件 ID 字符串数组（notify_event.id）';
COMMENT ON COLUMN monitor_notify_log.note IS '备注，如「未解析到通知对象」';
COMMENT ON COLUMN monitor_notify_log.create_time IS '通知时间';
CREATE INDEX idx_monitor_notify_log_alert ON monitor_notify_log (alert_id, create_time);
CREATE INDEX idx_monitor_notify_log_subscription ON monitor_notify_log (subscription_id, create_time DESC) WHERE subscription_id IS NOT NULL;
CREATE INDEX idx_monitor_notify_log_recipients ON monitor_notify_log USING gin (recipients jsonb_path_ops);

-- ---------------------------------------------------------------------------
-- DB 角色（NOLOGIN 组角色；部署时各服务登录账号 GRANT 为成员，本地开发沿用 postgres 账号）
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'monitor_engine_role') THEN
        CREATE ROLE monitor_engine_role NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'monitor_core_role') THEN
        CREATE ROLE monitor_core_role NOLOGIN;
    END IF;
END
$$;
-- 引擎：配置表只读（订阅 / 角色范围 / 规则版本不可见）、数据源仅可回写健康状态、运行表读写、通知记录仅清理。
GRANT SELECT ON monitor_datasource, monitor_rule_group, monitor_rule, monitor_mute, monitor_cfg_version TO monitor_engine_role;
GRANT UPDATE (health_status, health_error, health_time) ON monitor_datasource TO monitor_engine_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON monitor_eval_task, monitor_eval_state, monitor_alert, monitor_eval_record,
    monitor_notify_outbox TO monitor_engine_role;
GRANT SELECT, DELETE ON monitor_notify_log TO monitor_engine_role;
-- 判定记录的分区维护由引擎执行（独立建表 → 迁出 DEFAULT → ATTACH；按保留期 DROP），做法同通知管道
-- （0021）与 CMDB 数据面（0036）：ATTACH / DROP 须为父表属主，建表须有 schema 的 CREATE 权限。
GRANT CREATE ON SCHEMA public TO monitor_engine_role;
ALTER TABLE monitor_eval_record OWNER TO monitor_engine_role;
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT inhrelid::regclass AS part FROM pg_inherits WHERE inhparent = 'monitor_eval_record'::regclass LOOP
        EXECUTE format('ALTER TABLE %s OWNER TO monitor_engine_role', r.part);
    END LOOP;
END
$$;
-- core：配置表与通知记录全权、运行表只读、告警事件仅回写通知计数、发件箱仅改派发状态。
-- 判定记录的属主已是引擎角色，core 登录账号经本角色保留读权限（dev 超级用户不受影响）。
GRANT SELECT, INSERT, UPDATE, DELETE ON monitor_datasource, monitor_rule_group, monitor_role_scope, monitor_rule,
    monitor_rule_version, monitor_mute, monitor_subscription, monitor_cfg_version, monitor_notify_log TO monitor_core_role;
GRANT SELECT ON monitor_eval_task, monitor_eval_state, monitor_alert, monitor_eval_record, monitor_notify_outbox TO monitor_core_role;
GRANT UPDATE (notify_count, last_notify_time) ON monitor_alert TO monitor_core_role;
GRANT UPDATE (status, attempts, last_error, done_time) ON monitor_notify_outbox TO monitor_core_role;

-- ---------------------------------------------------------------------------
-- 内置数据
-- ---------------------------------------------------------------------------

-- 根分组：新建规则的缺省归属；可改名，有规则或下级时不可删除。
INSERT INTO monitor_rule_group (id, parent_id, path, name, description)
VALUES ('018f3e1a-0cc0-7000-8000-000000000001', NULL, '/018f3e1a-0cc0-7000-8000-000000000001/', '默认分组', '系统内置的缺省规则分组');

INSERT INTO monitor_cfg_version (name, version) VALUES
('monitor_datasource', 1),
('monitor_mute', 1),
('monitor_rule_group', 1);

-- 告警通知的缺省配置（缺省业务命名空间 default）：两条路由 + 四个模板，做法同 0055。
-- 渠道无法由迁移创建，这里只绑定迁移执行时 default 命名空间下已启用的邮件与群机器人渠道；
-- 没有对应渠道则路由的渠道为空（不投递），之后在「通知管理 → 规则」里为这两条路由选择渠道即可。
-- 事件自带收件人（用户 ID）；subscription=true 使未带收件人的事件退回通知订阅者，同时满足
-- 「邮件渠道须有收件策略」的校验。聚合窗口为 0：重复通知与聚合由监控侧自己控制。
INSERT INTO notify_route (id, biz_id, name, description, event_pattern, severity_min, priority, channel_ids, recipient_policy, aggregate_window_s)
VALUES ('018f3e1a-0cc1-7000-8000-000000000001', 'default', '监控告警通知',
        '告警触发、重复与恢复通知：发给规则配置的通知对象（用户 / 值班表当前值班人 / 部门负责人）', 'monitor.alert.*', 1, 5,
        ARRAY(SELECT id FROM notify_channel
              WHERE biz_id = 'default' AND type IN ('email', 'feishu', 'dingtalk', 'wecom') AND enabled AND delete_time IS NULL ORDER BY create_time),
        '{"subscription": true}', 0),
       ('018f3e1a-0cc1-7000-8000-000000000002', 'default', '监控告警订阅通知',
        '告警订阅规则命中后的通知：发给订阅规则的接收方', 'monitor.subscription.*', 1, 5,
        ARRAY(SELECT id FROM notify_channel
              WHERE biz_id = 'default' AND type IN ('email', 'feishu', 'dingtalk', 'wecom') AND enabled AND delete_time IS NULL ORDER BY create_time),
        '{"subscription": true}', 0);

-- 模板：纯文本、不限渠道类型（邮件与群机器人都能直接展示）；同一事件已有模板则保留现有的。
-- 载荷字段见 kit/monitorcontract.NotifyPayload（value / recovery_value / first_fire_time / duration 为已格式化的文本）。
INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body) VALUES
('018f3e1a-0cc2-7000-8000-000000000001', 'default', 'monitor.alert.firing', '*', 'text',
 '【{{if eq .Severity 4}}致命{{else if eq .Severity 3}}严重{{else if eq .Severity 2}}警告{{else}}提示{{end}}告警】{{.Payload.rule_name}}',
 '规则：{{.Payload.rule_name}}
分组：{{.Payload.group_name}}
级别：{{if eq .Severity 4}}致命{{else if eq .Severity 3}}严重{{else if eq .Severity 2}}警告{{else}}提示{{end}}
数据源：{{.Payload.datasource_name}}
触发值：{{.Payload.value}}
首次触发：{{.Payload.first_fire_time}}
已持续：{{.Payload.duration}}
通知次数：第 {{.Payload.notify_seq}} 次{{if .Payload.labels}}
标签：{{range $k, $v := .Payload.labels}}
  {{$k}} = {{$v}}{{end}}{{end}}{{if .Payload.annotations}}
附加信息：{{range $k, $v := .Payload.annotations}}
  {{$k}}：{{$v}}{{end}}{{end}}
表达式：{{.Payload.expr}}
详情：{{.Payload.alert_url}}'),
('018f3e1a-0cc2-7000-8000-000000000002', 'default', 'monitor.alert.resolved', '*', 'text',
 '【告警恢复】{{.Payload.rule_name}}',
 '规则：{{.Payload.rule_name}}
分组：{{.Payload.group_name}}
级别：{{if eq .Severity 4}}致命{{else if eq .Severity 3}}严重{{else if eq .Severity 2}}警告{{else}}提示{{end}}
数据源：{{.Payload.datasource_name}}
最近触发值：{{.Payload.value}}{{if .Payload.recovery_value}}
恢复值：{{.Payload.recovery_value}}{{end}}
首次触发：{{.Payload.first_fire_time}}
持续时长：{{.Payload.duration}}{{if .Payload.labels}}
标签：{{range $k, $v := .Payload.labels}}
  {{$k}} = {{$v}}{{end}}{{end}}{{if .Payload.annotations}}
附加信息：{{range $k, $v := .Payload.annotations}}
  {{$k}}：{{$v}}{{end}}{{end}}
详情：{{.Payload.alert_url}}'),
('018f3e1a-0cc2-7000-8000-000000000003', 'default', 'monitor.subscription.firing', '*', 'text',
 '【订阅 · {{if eq .Severity 4}}致命{{else if eq .Severity 3}}严重{{else if eq .Severity 2}}警告{{else}}提示{{end}}告警】{{.Payload.rule_name}}',
 '你订阅的告警已触发。
订阅：{{.Payload.subscription_name}}
规则：{{.Payload.rule_name}}
分组：{{.Payload.group_name}}
级别：{{if eq .Severity 4}}致命{{else if eq .Severity 3}}严重{{else if eq .Severity 2}}警告{{else}}提示{{end}}
数据源：{{.Payload.datasource_name}}
触发值：{{.Payload.value}}
首次触发：{{.Payload.first_fire_time}}
已持续：{{.Payload.duration}}{{if .Payload.labels}}
标签：{{range $k, $v := .Payload.labels}}
  {{$k}} = {{$v}}{{end}}{{end}}{{if .Payload.annotations}}
附加信息：{{range $k, $v := .Payload.annotations}}
  {{$k}}：{{$v}}{{end}}{{end}}
表达式：{{.Payload.expr}}
详情：{{.Payload.alert_url}}'),
('018f3e1a-0cc2-7000-8000-000000000004', 'default', 'monitor.subscription.resolved', '*', 'text',
 '【订阅 · 告警恢复】{{.Payload.rule_name}}',
 '你订阅的告警已恢复。
订阅：{{.Payload.subscription_name}}
规则：{{.Payload.rule_name}}
分组：{{.Payload.group_name}}
级别：{{if eq .Severity 4}}致命{{else if eq .Severity 3}}严重{{else if eq .Severity 2}}警告{{else}}提示{{end}}
数据源：{{.Payload.datasource_name}}
最近触发值：{{.Payload.value}}{{if .Payload.recovery_value}}
恢复值：{{.Payload.recovery_value}}{{end}}
首次触发：{{.Payload.first_fire_time}}
持续时长：{{.Payload.duration}}{{if .Payload.labels}}
标签：{{range $k, $v := .Payload.labels}}
  {{$k}} = {{$v}}{{end}}{{end}}{{if .Payload.annotations}}
附加信息：{{range $k, $v := .Payload.annotations}}
  {{$k}}：{{$v}}{{end}}{{end}}
详情：{{.Payload.alert_url}}')
ON CONFLICT (biz_id, event_type, channel_type) WHERE delete_time IS NULL DO NOTHING;

-- 配置版本 +1：通知管道据此刷新路由与模板缓存。
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
