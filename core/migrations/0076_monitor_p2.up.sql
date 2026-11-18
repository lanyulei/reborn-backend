-- 0076 监控告警域第二、三期：事件处理（认领 / 备注 / 关闭 / 转工单）与时间线、升级策略、规则间抑制与拓扑抑制、
-- CMDB 联动（资源索引、富化、资源范围）、标签改写与条件丢弃、事件动作（自动建单 / 触发流程 / AI 摘要）、
-- 工作日历、平台自监控、记录规则、聚合视图。
-- 设计：backend/docs/monitor-tech-design-p2p3.md；jsonb 列结构与状态字面量的唯一事实源是 kit/monitorcontract。
-- 只增不改：新列一律带缺省值，第一期的行保持合法；第一期的引擎 / core 在本迁移之后的表结构上照常工作
-- （滚动发布：先迁移，再升级 core 的全部实例，最后升级引擎）。
-- 写入分工沿用 0069：配置表 core 写、引擎只读；运行表引擎写、core 读；两者交界处用列级授权。

-- ---------------------------------------------------------------------------
-- 配置表（core 写，引擎只读）
-- ---------------------------------------------------------------------------

CREATE TABLE monitor_escalation (
    id           uuid         PRIMARY KEY,
    group_id     uuid         NULL REFERENCES monitor_rule_group (id),
    name         varchar(128) NOT NULL,
    description  varchar(255) NOT NULL DEFAULT '',
    mode         smallint     NOT NULL DEFAULT 1,
    min_severity smallint     NOT NULL DEFAULT 1,
    levels       jsonb        NOT NULL,
    enabled      boolean      NOT NULL DEFAULT true,
    version      integer      NOT NULL DEFAULT 1,
    create_time  timestamptz  NOT NULL DEFAULT now(),
    update_time  timestamptz  NOT NULL DEFAULT now(),
    delete_time  timestamptz  NULL,
    create_user  uuid         NULL,
    update_user  uuid         NULL,
    delete_user  uuid         NULL,
    CONSTRAINT ck_monitor_escalation_mode CHECK (mode IN (1, 2)),
    CONSTRAINT ck_monitor_escalation_min_severity CHECK (min_severity BETWEEN 1 AND 4),
    CONSTRAINT ck_monitor_escalation_levels CHECK (jsonb_typeof(levels) = 'array' AND jsonb_array_length(levels) BETWEEN 1 AND 10)
);
COMMENT ON TABLE monitor_escalation IS '升级策略：告警持续未认领 / 未恢复时逐级通知更多的人，规则经 escalation_id 引用，软删；core 写，引擎只读';
COMMENT ON COLUMN monitor_escalation.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_escalation.group_id IS '归属的规则分组：该分组（含下级）内的规则可引用；NULL=全局策略，所有分组可引用，仅超级管理员可维护';
COMMENT ON COLUMN monitor_escalation.name IS '名称，同一归属下未删除行内唯一';
COMMENT ON COLUMN monitor_escalation.description IS '说明';
COMMENT ON COLUMN monitor_escalation.mode IS '升级方式：1=叠加（升级后前面各级与规则通知对象的重复提醒照常继续） 2=接替（升级后只提醒当前这一级）';
COMMENT ON COLUMN monitor_escalation.min_severity IS '最低级别 1-4：事件级别低于它时不升级';
COMMENT ON COLUMN monitor_escalation.levels IS '各级 [{"trigger":1,"after_m":15,"targets":{"user_ids":[],"schedules":[{"id":"…","backup_only":true}],"oncall_group_ids":[],"depts":[{"id":"…","up":1}]},"repeat_interval_m":30,"max_notify":3}]，1-10 级，after_m 逐级递增；trigger 1=未认领 2=未恢复（kit/monitorcontract.EscalationLevel）';
COMMENT ON COLUMN monitor_escalation.enabled IS '是否启用；停用后引用它的规则不再升级';
COMMENT ON COLUMN monitor_escalation.version IS '乐观锁版本号：每次保存 +1';
COMMENT ON COLUMN monitor_escalation.create_time IS '创建时间';
COMMENT ON COLUMN monitor_escalation.update_time IS '更新时间';
COMMENT ON COLUMN monitor_escalation.delete_time IS '软删时间';
COMMENT ON COLUMN monitor_escalation.create_user IS '创建人';
COMMENT ON COLUMN monitor_escalation.update_user IS '最近更新人';
COMMENT ON COLUMN monitor_escalation.delete_user IS '删除人';
-- 全局策略 group_id 为 NULL：NULLS NOT DISTINCT 使全局策略之间同样不能重名。
CREATE UNIQUE INDEX uk_monitor_escalation_name ON monitor_escalation (group_id, name) NULLS NOT DISTINCT WHERE delete_time IS NULL;

CREATE TABLE monitor_inhibit (
    id           uuid         PRIMARY KEY,
    group_id     uuid         NULL REFERENCES monitor_rule_group (id),
    name         varchar(128) NOT NULL,
    description  varchar(255) NOT NULL DEFAULT '',
    source       jsonb        NOT NULL,
    target       jsonb        NOT NULL DEFAULT '{}',
    equal_labels text[]       NOT NULL DEFAULT '{}',
    enabled      boolean      NOT NULL DEFAULT true,
    version      integer      NOT NULL DEFAULT 1,
    create_time  timestamptz  NOT NULL DEFAULT now(),
    update_time  timestamptz  NOT NULL DEFAULT now(),
    delete_time  timestamptz  NULL,
    create_user  uuid         NULL,
    update_user  uuid         NULL,
    delete_user  uuid         NULL,
    CONSTRAINT ck_monitor_inhibit_source CHECK (jsonb_typeof(source) = 'object'),
    CONSTRAINT ck_monitor_inhibit_target CHECK (jsonb_typeof(target) = 'object'),
    CONSTRAINT ck_monitor_inhibit_equal CHECK (cardinality(equal_labels) <= 10)
);
COMMENT ON TABLE monitor_inhibit IS '抑制规则：存在匹配源条件的活跃告警时，抑制匹配目标条件且相等标签取值相同的告警的通知（事件照常留档），软删；core 写，引擎只读';
COMMENT ON COLUMN monitor_inhibit.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_inhibit.group_id IS '作用的规则分组（含其全部下级）：源告警与目标告警都须在其中；NULL=全部分组，仅超级管理员可建';
COMMENT ON COLUMN monitor_inhibit.name IS '名称';
COMMENT ON COLUMN monitor_inhibit.description IS '说明';
COMMENT ON COLUMN monitor_inhibit.source IS '源条件 {"rule_ids":[],"severities":[],"matchers":[{"label":"alertname","op":"==","value":"HostDown"}]}，各项之间为与，不得全空（kit/monitorcontract.InhibitSelector）';
COMMENT ON COLUMN monitor_inhibit.target IS '目标条件，结构同 source；可为空对象（不限），但不得与 equal_labels 同时为空';
COMMENT ON COLUMN monitor_inhibit.equal_labels IS '相等标签：源告警与目标告警在这些标签上的取值须全部相同（缺失按空串），至多 10 个';
COMMENT ON COLUMN monitor_inhibit.enabled IS '是否启用';
COMMENT ON COLUMN monitor_inhibit.version IS '乐观锁版本号：每次保存 +1';
COMMENT ON COLUMN monitor_inhibit.create_time IS '创建时间';
COMMENT ON COLUMN monitor_inhibit.update_time IS '更新时间';
COMMENT ON COLUMN monitor_inhibit.delete_time IS '软删时间';
COMMENT ON COLUMN monitor_inhibit.create_user IS '创建人';
COMMENT ON COLUMN monitor_inhibit.update_user IS '最近更新人';
COMMENT ON COLUMN monitor_inhibit.delete_user IS '删除人';
CREATE INDEX idx_monitor_inhibit_group ON monitor_inhibit (group_id) WHERE delete_time IS NULL;

CREATE TABLE monitor_record_rule (
    id            uuid         PRIMARY KEY,
    group_id      uuid         NOT NULL REFERENCES monitor_rule_group (id),
    name          varchar(128) NOT NULL,
    description   varchar(512) NOT NULL DEFAULT '',
    datasource_id uuid         NOT NULL,
    expr          text         NOT NULL,
    interval_s    integer      NOT NULL DEFAULT 60,
    labels        jsonb        NOT NULL DEFAULT '{}',
    enabled       boolean      NOT NULL DEFAULT true,
    version       integer      NOT NULL DEFAULT 1,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    delete_time   timestamptz  NULL,
    create_user   uuid         NULL,
    update_user   uuid         NULL,
    delete_user   uuid         NULL,
    CONSTRAINT ck_monitor_record_rule_name CHECK (name ~ '^[a-zA-Z_:][a-zA-Z0-9_:]*$'),
    CONSTRAINT ck_monitor_record_rule_expr CHECK (expr <> '' AND octet_length(expr) <= 8192),
    CONSTRAINT ck_monitor_record_rule_interval CHECK (interval_s BETWEEN 15 AND 3600),
    CONSTRAINT ck_monitor_record_rule_labels CHECK (jsonb_typeof(labels) = 'object')
);
COMMENT ON TABLE monitor_record_rule IS '记录规则：按固定周期执行一条 PromQL，把结果经数据源的 Remote Write 地址写回为新指标，软删；core 写，引擎只读';
COMMENT ON COLUMN monitor_record_rule.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_record_rule.group_id IS '所属规则分组（数据范围的依据）';
COMMENT ON COLUMN monitor_record_rule.name IS '名称，即写回的指标名（Prometheus 指标名）；同一分组、同一数据源的未删除行内唯一';
COMMENT ON COLUMN monitor_record_rule.description IS '说明';
COMMENT ON COLUMN monitor_record_rule.datasource_id IS '数据源：在其上查询并写回；须配置了 remote_write_url';
COMMENT ON COLUMN monitor_record_rule.expr IS 'PromQL 表达式，至多 8192 字节';
COMMENT ON COLUMN monitor_record_rule.interval_s IS '执行周期秒，15-3600';
COMMENT ON COLUMN monitor_record_rule.labels IS '附加标签 {名称: 值}，至多 20 个；写回时与结果序列的标签合并（同名时附加标签优先）';
COMMENT ON COLUMN monitor_record_rule.enabled IS '是否启用';
COMMENT ON COLUMN monitor_record_rule.version IS '乐观锁版本号：每次保存 +1';
COMMENT ON COLUMN monitor_record_rule.create_time IS '创建时间';
COMMENT ON COLUMN monitor_record_rule.update_time IS '更新时间';
COMMENT ON COLUMN monitor_record_rule.delete_time IS '软删时间';
COMMENT ON COLUMN monitor_record_rule.create_user IS '创建人';
COMMENT ON COLUMN monitor_record_rule.update_user IS '最近更新人';
COMMENT ON COLUMN monitor_record_rule.delete_user IS '删除人';
CREATE UNIQUE INDEX uk_monitor_record_rule_name ON monitor_record_rule (group_id, datasource_id, name) WHERE delete_time IS NULL;
CREATE INDEX idx_monitor_record_rule_datasource ON monitor_record_rule (datasource_id) WHERE delete_time IS NULL;

CREATE TABLE monitor_alert_view (
    id          uuid         PRIMARY KEY,
    owner_id    uuid         NOT NULL,
    name        varchar(64)  NOT NULL,
    by          varchar(140) NOT NULL,
    filters     jsonb        NOT NULL DEFAULT '{}',
    is_public   boolean      NOT NULL DEFAULT false,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_monitor_alert_view_name CHECK (name <> ''),
    CONSTRAINT ck_monitor_alert_view_by CHECK (by ~ '^(rule|severity|group|label:[a-zA-Z_][a-zA-Z0-9_]*)$'),
    CONSTRAINT ck_monitor_alert_view_filters CHECK (jsonb_typeof(filters) = 'object')
);
COMMENT ON TABLE monitor_alert_view IS '活跃告警聚合视图：保存的聚合维度与过滤条件，个人或公共（物理删除）；仅 core 读写';
COMMENT ON COLUMN monitor_alert_view.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_alert_view.owner_id IS '所有者（创建人）';
COMMENT ON COLUMN monitor_alert_view.name IS '名称，同一所有者下唯一';
COMMENT ON COLUMN monitor_alert_view.by IS '聚合维度：rule / severity / group / label:<标签名>';
COMMENT ON COLUMN monitor_alert_view.filters IS '过滤条件 {"group_id","datasource_id","severities":[],"muted","acked","inhibited","labels":["name=value"]}，各键可省略（kit/monitorcontract.AlertViewFilter）';
COMMENT ON COLUMN monitor_alert_view.is_public IS '是否公共视图：所有人可见，仅超级管理员可设置';
COMMENT ON COLUMN monitor_alert_view.create_time IS '创建时间';
COMMENT ON COLUMN monitor_alert_view.update_time IS '更新时间';
CREATE UNIQUE INDEX uk_monitor_alert_view_name ON monitor_alert_view (owner_id, name);
CREATE INDEX idx_monitor_alert_view_public ON monitor_alert_view (name) WHERE is_public;

-- 以下两张表是其它域数据在监控侧的物化副本：core 的 monitor-ci-sync 循环写，引擎只读。
CREATE TABLE monitor_ci_index (
    ci_id              uuid         PRIMARY KEY,
    model_code         varchar(64)  NOT NULL,
    name               varchar(128) NOT NULL,
    env                varchar(32)  NOT NULL DEFAULT '',
    status             varchar(16)  NOT NULL DEFAULT '',
    owner              varchar(64)  NOT NULL DEFAULT '',
    tags               text[]       NOT NULL DEFAULT '{}',
    attrs              jsonb        NOT NULL DEFAULT '{}',
    upstream_ids       uuid[]       NOT NULL DEFAULT '{}',
    valid              boolean      NOT NULL DEFAULT true,
    source_update_time timestamptz  NULL,
    sync_time          timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_monitor_ci_index_attrs CHECK (jsonb_typeof(attrs) = 'object')
);
COMMENT ON TABLE monitor_ci_index IS 'CMDB 资源索引：core 把资源的名称、环境、状态、负责人、标签、可匹配属性与上游资源物化到这里，引擎据此做告警关联、富化、维护态屏蔽、资源范围过滤与拓扑抑制（引擎不读 cmdb_* 表）';
COMMENT ON COLUMN monitor_ci_index.ci_id IS '资源 ID（cmdb_ci.id）';
COMMENT ON COLUMN monitor_ci_index.model_code IS '模型编码';
COMMENT ON COLUMN monitor_ci_index.name IS '资源名称';
COMMENT ON COLUMN monitor_ci_index.env IS '环境';
COMMENT ON COLUMN monitor_ci_index.status IS '状态：planning / active / maintenance / retired；maintenance 时关联到它的告警只屏蔽通知';
COMMENT ON COLUMN monitor_ci_index.owner IS '负责人：用户 ID 或自由文本（是可用用户的 ID 时才能作为通知对象）';
COMMENT ON COLUMN monitor_ci_index.tags IS '资源标签';
COMMENT ON COLUMN monitor_ci_index.attrs IS '可匹配属性 {属性名: [取值, …]}：只含部署配置里列出的匹配属性（缺省 ip），取值一律是字符串数组';
COMMENT ON COLUMN monitor_ci_index.upstream_ids IS '上游资源 ID（按配置的关系类型向上至多 3 层，去重）：其中任一资源有活跃告警时，开启拓扑抑制的规则在本资源上的告警被抑制';
COMMENT ON COLUMN monitor_ci_index.valid IS '是否有效：资源被删除或退役后置 false（保留行，供已有告警展示名称），引擎只加载有效的行';
COMMENT ON COLUMN monitor_ci_index.source_update_time IS '资源在 CMDB 的更新时间（增量同步的游标）';
COMMENT ON COLUMN monitor_ci_index.sync_time IS '最近一次物化时间';
CREATE INDEX idx_monitor_ci_index_source_update ON monitor_ci_index (source_update_time);

CREATE TABLE monitor_calendar (
    id          smallint    PRIMARY KEY DEFAULT 1,
    config      jsonb       NOT NULL DEFAULT '{}',
    update_time timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_monitor_calendar_single CHECK (id = 1),
    CONSTRAINT ck_monitor_calendar_config CHECK (jsonb_typeof(config) = 'object')
);
COMMENT ON TABLE monitor_calendar IS '工作日历快照（恒为一行）：core 把流程域的工作日历物化到这里，规则生效时间的「仅工作时间 / 仅非工作时间」据此判定；变更时 monitor_cfg_version 的 monitor_calendar 行 +1';
COMMENT ON COLUMN monitor_calendar.id IS '恒为 1';
COMMENT ON COLUMN monitor_calendar.config IS '日历 {"timezone":"Asia/Shanghai","weekdays":[1,2,3,4,5],"segments":[{"start":"09:00","end":"18:00"}],"holidays":["2026-10-01"],"workdays":["2026-10-10"]}；weekdays 0=周日…6=周六；空对象=尚未物化（kit/monitorcontract.CalendarSnapshot）';
COMMENT ON COLUMN monitor_calendar.update_time IS '最近一次物化时间';

-- ---------------------------------------------------------------------------
-- 已有配置表的新列
-- ---------------------------------------------------------------------------

ALTER TABLE monitor_rule
    ADD COLUMN ci_match         jsonb   NOT NULL DEFAULT '{}',
    ADD COLUMN resource_scope   jsonb   NOT NULL DEFAULT '{}',
    ADD COLUMN relabel          jsonb   NOT NULL DEFAULT '[]',
    ADD COLUMN drop_expr        text    NOT NULL DEFAULT '',
    ADD COLUMN topology_inhibit boolean NOT NULL DEFAULT false,
    ADD COLUMN escalation_id    uuid    NULL,
    ADD COLUMN actions          jsonb   NOT NULL DEFAULT '{}',
    ADD CONSTRAINT ck_monitor_rule_ci_match CHECK (jsonb_typeof(ci_match) = 'object'),
    ADD CONSTRAINT ck_monitor_rule_resource_scope CHECK (jsonb_typeof(resource_scope) = 'object'),
    ADD CONSTRAINT ck_monitor_rule_relabel CHECK (jsonb_typeof(relabel) = 'array' AND jsonb_array_length(relabel) <= 10),
    ADD CONSTRAINT ck_monitor_rule_drop_expr CHECK (octet_length(drop_expr) <= 2048),
    ADD CONSTRAINT ck_monitor_rule_actions CHECK (jsonb_typeof(actions) = 'object');
COMMENT ON COLUMN monitor_rule.ci_match IS '关联 CMDB 资源的方式 {"disabled","label","by":"name|attr","attr","model_codes":[],"enrich_tags":[]}；空对象=全局缺省（ident 标签→资源名称，其次 instance 标签→ip 属性），标签值形如 host:port 时去掉端口（kit/monitorcontract.CIMatch）';
COMMENT ON COLUMN monitor_rule.resource_scope IS '仅对指定资源生效 {"enabled","model_codes":[],"envs":[],"tags":[]}：启用后事件须关联到资源且满足各项条件，否则丢弃；空对象=不限（kit/monitorcontract.ResourceScope）';
COMMENT ON COLUMN monitor_rule.relabel IS '标签改写步骤 [{"action":"replace|labelmap|labeldrop|labelkeep","source_labels":[],"separator","regex","target_label","replacement"}]，至多 10 步，在指纹计算之前执行（kit/monitorcontract.RelabelStep）';
COMMENT ON COLUMN monitor_rule.drop_expr IS '条件丢弃表达式（与附加信息同一沙箱的模板，可用 $labels / $value），渲染结果为 true 即丢弃该事件；空=不丢弃，至多 2048 字节';
COMMENT ON COLUMN monitor_rule.topology_inhibit IS '拓扑抑制：事件关联的资源有任一上游资源存在活跃告警时，抑制本事件的通知';
COMMENT ON COLUMN monitor_rule.escalation_id IS '引用的升级策略（monitor_escalation.id）；NULL=不升级。不设外键：策略软删，引用关系由应用层校验（仍被引用的策略不可删除）';
COMMENT ON COLUMN monitor_rule.actions IS '事件动作 {"ticket":{"enabled","template_id","agent_user_id"},"workflow":{"enabled","definition_key","agent_user_id"},"ai_summary":bool}：触发时自动建单 / 发起流程 / 生成 AI 摘要；空对象=无（kit/monitorcontract.RuleActions）';
COMMENT ON COLUMN monitor_rule.effective IS '生效时间 {"timezone":"Asia/Shanghai","windows":[{"days":[1,2,3,4,5],"start":"09:00","end":"18:00"}],"mode":"windows|work_time|off_time"}；days 1-7=周一至周日，start>end 表示跨午夜，windows 为空=全时段；mode 省略=windows，work_time / off_time 按工作日历（monitor_calendar）判定且不带 windows';
COMMENT ON COLUMN monitor_rule.notify IS '通知对象 {"user_ids":[],"schedule_ids":[],"dept_ids":[],"ci_owner":bool}：用户、值班表当前值班人、部门负责人、事件关联资源的负责人';
CREATE INDEX idx_monitor_rule_escalation ON monitor_rule (escalation_id) WHERE escalation_id IS NOT NULL AND delete_time IS NULL;

ALTER TABLE monitor_datasource ADD COLUMN health_since timestamptz NULL;
COMMENT ON COLUMN monitor_datasource.health_since IS '当前健康状态（health_status）自何时起保持不变；由评估引擎探活写入，据此判断「持续不可用」';
COMMENT ON COLUMN monitor_datasource.remote_write_url IS 'Remote Write 的完整写入地址（如 http://vminsert:8480/insert/0/prometheus/api/v1/write）；记录规则经它写回，空=该数据源不能建记录规则';

-- ---------------------------------------------------------------------------
-- 运行表的新列
-- ---------------------------------------------------------------------------

ALTER TABLE monitor_alert
    ADD COLUMN ack_user             uuid         NULL,
    ADD COLUMN ack_time             timestamptz  NULL,
    ADD COLUMN closed_by            uuid         NULL,
    ADD COLUMN close_reason         varchar(255) NOT NULL DEFAULT '',
    ADD COLUMN inhibited            boolean      NOT NULL DEFAULT false,
    ADD COLUMN inhibit_kind         smallint     NOT NULL DEFAULT 0,
    ADD COLUMN inhibit_id           uuid         NULL,
    ADD COLUMN inhibit_alert_id     uuid         NULL,
    ADD COLUMN ci_id                uuid         NULL,
    ADD COLUMN escalation_level     smallint     NOT NULL DEFAULT 0,
    ADD COLUMN ticket_id            uuid         NULL,
    ADD COLUMN workflow_instance_id uuid         NULL,
    ADD COLUMN ai_summary           text         NOT NULL DEFAULT '',
    ADD CONSTRAINT ck_monitor_alert_ack CHECK ((ack_user IS NULL) = (ack_time IS NULL)),
    ADD CONSTRAINT ck_monitor_alert_inhibit CHECK (inhibit_kind IN (0, 1, 2) AND inhibited = (inhibit_kind <> 0)),
    ADD CONSTRAINT ck_monitor_alert_escalation_level CHECK (escalation_level BETWEEN 0 AND 10),
    ADD CONSTRAINT ck_monitor_alert_ai_summary CHECK (octet_length(ai_summary) <= 4096);
COMMENT ON TABLE monitor_alert IS '告警事件：一次从触发到恢复（或关闭）的告警；引擎写，core 读并回写通知计数、认领、关联工单 / 流程与 AI 摘要（列级授权）。同一指纹至多一个活跃事件';
COMMENT ON COLUMN monitor_alert.status IS '状态：1=告警中 2=已恢复 3=已关闭（手动关闭，或规则被删除 / 停用、数据源移出范围时一并关闭；不发恢复通知）';
COMMENT ON COLUMN monitor_alert.labels IS '事件标签 {名称: 值}：序列标签（去掉 __name__）与规则附加标签合并，经标签改写，再加上 ci_ 前缀的 CMDB 富化标签（富化标签不参与指纹）';
COMMENT ON COLUMN monitor_alert.muted IS '是否不发通知：被「只屏蔽通知」的屏蔽规则命中，或关联的资源处于维护态（此时 mute_id 为 NULL）';
COMMENT ON COLUMN monitor_alert.ack_user IS '认领人；未认领为 NULL。由 core 写入（认领 / 取消认领 / 转交）';
COMMENT ON COLUMN monitor_alert.ack_time IS '认领时间；非空时引擎停止规则的重复通知与「未认领」类升级。由 core 写入';
COMMENT ON COLUMN monitor_alert.closed_by IS '手动关闭的操作人；非手动关闭为 NULL。由引擎执行关闭指令时写入';
COMMENT ON COLUMN monitor_alert.close_reason IS '手动关闭的原因';
COMMENT ON COLUMN monitor_alert.inhibited IS '是否被抑制（事件照常留档，不发通知、不升级）';
COMMENT ON COLUMN monitor_alert.inhibit_kind IS '抑制方式：0=未被抑制 1=抑制规则 2=拓扑抑制（上游资源有活跃告警）';
COMMENT ON COLUMN monitor_alert.inhibit_id IS '命中的抑制规则（monitor_inhibit.id）；拓扑抑制或未被抑制时为 NULL';
COMMENT ON COLUMN monitor_alert.inhibit_alert_id IS '造成抑制的源告警（monitor_alert.id）；未被抑制时为 NULL';
COMMENT ON COLUMN monitor_alert.ci_id IS '关联的 CMDB 资源（monitor_ci_index.ci_id）；未关联为 NULL。触发时确定，之后不变';
COMMENT ON COLUMN monitor_alert.escalation_level IS '已升级到第几级，0=未升级';
COMMENT ON COLUMN monitor_alert.ticket_id IS '关联的工单（手动转工单或自动建单）；由 core 写入';
COMMENT ON COLUMN monitor_alert.workflow_instance_id IS '触发的流程实例；由 core 写入';
COMMENT ON COLUMN monitor_alert.ai_summary IS 'AI 生成的事件摘要与排查建议，至多 4096 字节；由 core 写入';
-- 按资源查告警（资源详情页的告警页签、拓扑抑制的活跃告警快照）。
CREATE INDEX idx_monitor_alert_ci ON monitor_alert (ci_id, first_fire_time DESC) WHERE ci_id IS NOT NULL;
-- 「我认领的」与认领人筛选。
CREATE INDEX idx_monitor_alert_ack_user ON monitor_alert (ack_user, first_fire_time DESC) WHERE ack_user IS NOT NULL;
-- 工单 → 告警的反查。
CREATE INDEX idx_monitor_alert_ticket ON monitor_alert (ticket_id) WHERE ticket_id IS NOT NULL;
-- 告警统计：按分组范围 + 首次触发时间范围聚合（趋势、排行、MTTA / MTTR、降噪效果）。
CREATE INDEX idx_monitor_alert_group_fire ON monitor_alert (group_id, first_fire_time DESC);

ALTER TABLE monitor_eval_task
    ADD COLUMN error_since timestamptz NULL,
    ADD COLUMN error_count integer     NOT NULL DEFAULT 0;
COMMENT ON COLUMN monitor_eval_task.error_since IS '连续查询错误自何时开始；最近一次评估不是查询错误时为 NULL。据此判断「规则持续评估失败」';
COMMENT ON COLUMN monitor_eval_task.error_count IS '连续查询错误的次数；评估成功即清零';

-- 发件箱：升级通知为 kind = 4。唯一键仍是 (alert_id, kind, seq)（第一期引擎的 ON CONFLICT 以它为仲裁索引，
-- 滚动发布期间不能换）：升级通知的 seq = level × 100000 + 级内序号（kit/monitorcontract.EscalationOutboxSeq），
-- 于是该唯一键对升级通知等价于 (alert_id, kind, level, 级内序号)；level 列冗余存一份，由 CHECK 与 seq 对账。
ALTER TABLE monitor_notify_outbox
    ADD COLUMN level smallint NOT NULL DEFAULT 0,
    DROP CONSTRAINT ck_monitor_notify_outbox_kind,
    ADD CONSTRAINT ck_monitor_notify_outbox_kind CHECK (kind IN (1, 2, 3, 4)),
    ADD CONSTRAINT ck_monitor_notify_outbox_level CHECK (
        (kind <> 4 AND level = 0)
        OR (kind = 4 AND level BETWEEN 1 AND 10 AND seq / 100000 = level AND seq % 100000 >= 1));
COMMENT ON COLUMN monitor_notify_outbox.kind IS '通知类型：1=触发 2=重复 3=恢复 4=升级（升级到某一级时的首次通知与该级的重复提醒）';
COMMENT ON COLUMN monitor_notify_outbox.seq IS '该事件的第几次通知（触发为 1，重复通知自 2 递增；恢复通知取恢复时已发出的次数 + 1）；升级通知为 level × 100000 + 该级的第几次通知';
COMMENT ON COLUMN monitor_notify_outbox.level IS '升级通知的级数 1-10；其余通知为 0';

ALTER TABLE monitor_notify_log
    ADD COLUMN level smallint NOT NULL DEFAULT 0,
    DROP CONSTRAINT ck_monitor_notify_log_kind,
    DROP CONSTRAINT ck_monitor_notify_log_source,
    ADD CONSTRAINT ck_monitor_notify_log_kind CHECK (kind IN (1, 2, 3, 4)),
    ADD CONSTRAINT ck_monitor_notify_log_source CHECK (source IN (1, 2, 3)),
    ADD CONSTRAINT ck_monitor_notify_log_level CHECK ((kind = 4) = (level BETWEEN 1 AND 10) AND (kind = 4 OR level = 0)),
    ADD CONSTRAINT ck_monitor_notify_log_escalation CHECK ((source = 3) = (kind = 4));
COMMENT ON COLUMN monitor_notify_log.kind IS '通知类型：1=触发 2=重复 3=恢复 4=升级';
COMMENT ON COLUMN monitor_notify_log.seq IS '该事件的第几次通知；升级通知为该级的第几次通知（级数在 level）';
COMMENT ON COLUMN monitor_notify_log.source IS '来源：1=规则的通知对象 2=订阅规则 3=升级策略某一级的通知对象';
COMMENT ON COLUMN monitor_notify_log.level IS '升级通知的级数 1-10；其余通知为 0';
-- 按时间范围统计实际发出的通知数（降噪效果、值班班次内的告警数）。
CREATE INDEX idx_monitor_notify_log_time ON monitor_notify_log (create_time);

-- ---------------------------------------------------------------------------
-- 新的运行表
-- ---------------------------------------------------------------------------

CREATE TABLE monitor_alert_event (
    id          uuid        PRIMARY KEY,
    alert_id    uuid        NOT NULL,
    type        varchar(16) NOT NULL,
    operator    uuid        NULL,
    detail      jsonb       NOT NULL DEFAULT '{}',
    create_time timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_monitor_alert_event_type CHECK (type IN (
        'fire', 'resolve', 'close', 'mute', 'unmute', 'inhibit', 'uninhibit', 'escalate', 'notify',
        'ack', 'unack', 'transfer', 'comment', 'ticket', 'ticket_note', 'workflow', 'ai_summary', 'action_failed')),
    CONSTRAINT ck_monitor_alert_event_detail CHECK (jsonb_typeof(detail) = 'object')
);
COMMENT ON TABLE monitor_alert_event IS '告警事件时间线（append-only）：触发 / 恢复 / 关闭 / 屏蔽 / 抑制 / 升级由引擎写入，认领 / 转交 / 备注 / 工单 / 流程 / AI 摘要由 core 写入；每次通知的记录在 monitor_notify_log，详情接口合并两者';
COMMENT ON COLUMN monitor_alert_event.id IS '主键，UUIDv7，写入方生成';
COMMENT ON COLUMN monitor_alert_event.alert_id IS '告警事件 ID';
COMMENT ON COLUMN monitor_alert_event.type IS '类型：fire 触发 / resolve 恢复 / close 关闭 / mute 被屏蔽 / unmute 屏蔽解除 / inhibit 被抑制 / uninhibit 抑制解除 / escalate 升级 / notify 通知说明 / ack 认领 / unack 取消认领 / transfer 转交 / comment 备注 / ticket 转工单 / ticket_note 工单追加记录 / workflow 触发流程 / ai_summary AI 摘要 / action_failed 事件动作失败';
COMMENT ON COLUMN monitor_alert_event.operator IS '操作人；系统产生的记录为 NULL';
COMMENT ON COLUMN monitor_alert_event.detail IS '细节，各类型用到的键见 kit/monitorcontract.AlertEventDetail（如 comment 的 text、transfer 的 from_user / to_user、inhibit 的 inhibit_alert_id、ticket 的 ticket_id）';
COMMENT ON COLUMN monitor_alert_event.create_time IS '发生时间';
CREATE INDEX idx_monitor_alert_event_alert ON monitor_alert_event (alert_id, create_time, id);

CREATE TABLE monitor_alert_cmd (
    id          uuid         PRIMARY KEY,
    alert_id    uuid         NOT NULL,
    kind        smallint     NOT NULL,
    operator    uuid         NULL,
    reason      varchar(255) NOT NULL DEFAULT '',
    status      smallint     NOT NULL DEFAULT 1,
    result      varchar(255) NOT NULL DEFAULT '',
    create_time timestamptz  NOT NULL DEFAULT now(),
    done_time   timestamptz  NULL,
    CONSTRAINT ck_monitor_alert_cmd_kind CHECK (kind IN (1)),
    CONSTRAINT ck_monitor_alert_cmd_status CHECK (status IN (1, 2, 3))
);
COMMENT ON TABLE monitor_alert_cmd IS '事件指令：core 写入、引擎消费。core 不写引擎的判定状态，手动关闭经这里传达：引擎把事件置为已关闭并删除对应的判定状态，条件仍成立时下一轮按新事件重新计时';
COMMENT ON COLUMN monitor_alert_cmd.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN monitor_alert_cmd.alert_id IS '告警事件 ID';
COMMENT ON COLUMN monitor_alert_cmd.kind IS '指令：1=手动关闭';
COMMENT ON COLUMN monitor_alert_cmd.operator IS '操作人';
COMMENT ON COLUMN monitor_alert_cmd.reason IS '原因（手动关闭的原因，写入事件的 close_reason）';
COMMENT ON COLUMN monitor_alert_cmd.status IS '状态：1=待处理 2=已执行 3=未执行（事件已不在告警中）';
COMMENT ON COLUMN monitor_alert_cmd.result IS '未执行的说明';
COMMENT ON COLUMN monitor_alert_cmd.create_time IS '下达时间';
COMMENT ON COLUMN monitor_alert_cmd.done_time IS '处理时间';
-- 同一事件同一种指令至多一条待处理：重复下达是空操作。
CREATE UNIQUE INDEX uk_monitor_alert_cmd_pending ON monitor_alert_cmd (alert_id, kind) WHERE status = 1;
CREATE INDEX idx_monitor_alert_cmd_pending ON monitor_alert_cmd (create_time) WHERE status = 1;
CREATE INDEX idx_monitor_alert_cmd_done ON monitor_alert_cmd (done_time) WHERE status <> 1;

CREATE TABLE monitor_alert_escalation (
    alert_id         uuid        NOT NULL,
    level            smallint    NOT NULL,
    notify_count     integer     NOT NULL DEFAULT 0,
    last_notify_time timestamptz NULL,
    PRIMARY KEY (alert_id, level),
    CONSTRAINT ck_monitor_alert_escalation_level CHECK (level BETWEEN 1 AND 10),
    CONSTRAINT ck_monitor_alert_escalation_count CHECK (notify_count >= 0)
);
COMMENT ON TABLE monitor_alert_escalation IS '事件在升级策略各级上的通知进度：core 派发升级通知成功时回写，引擎据此计算该级的重复提醒与次数上限';
COMMENT ON COLUMN monitor_alert_escalation.alert_id IS '告警事件 ID';
COMMENT ON COLUMN monitor_alert_escalation.level IS '第几级，1-10';
COMMENT ON COLUMN monitor_alert_escalation.notify_count IS '该级已发出的通知次数（含升级时的首次通知）';
COMMENT ON COLUMN monitor_alert_escalation.last_notify_time IS '该级最近一次通知时间';

CREATE TABLE monitor_alert_action (
    id          uuid         PRIMARY KEY,
    alert_id    uuid         NOT NULL,
    kind        smallint     NOT NULL,
    status      smallint     NOT NULL DEFAULT 1,
    payload     jsonb        NOT NULL DEFAULT '{}',
    attempts    integer      NOT NULL DEFAULT 0,
    next_time   timestamptz  NOT NULL DEFAULT now(),
    last_error  varchar(512) NOT NULL DEFAULT '',
    create_time timestamptz  NOT NULL DEFAULT now(),
    done_time   timestamptz  NULL,
    CONSTRAINT ck_monitor_alert_action_kind CHECK (kind IN (1, 2, 3, 4)),
    CONSTRAINT ck_monitor_alert_action_status CHECK (status IN (1, 2, 3, 4)),
    CONSTRAINT ck_monitor_alert_action_payload CHECK (jsonb_typeof(payload) = 'object')
);
COMMENT ON TABLE monitor_alert_action IS '事件动作：引擎在事件触发 / 恢复时按规则配置写入，core 的 monitor-action 循环领取执行（建单、发起流程、AI 摘要、在工单上追加恢复记录）；每个事件每种动作至多一次';
COMMENT ON COLUMN monitor_alert_action.id IS '主键，UUIDv7，引擎生成';
COMMENT ON COLUMN monitor_alert_action.alert_id IS '告警事件 ID';
COMMENT ON COLUMN monitor_alert_action.kind IS '动作：1=自动建单 2=触发流程 3=AI 摘要 4=告警恢复时在关联工单上追加记录';
COMMENT ON COLUMN monitor_alert_action.status IS '状态：1=待执行 2=已完成 3=重试耗尽已放弃（记入时间线） 4=无需执行（如未配置 AI 密钥）';
COMMENT ON COLUMN monitor_alert_action.payload IS '入队时从规则配置拍下的参数 {"template_id","agent_user_id","definition_key"}（kit/monitorcontract.ActionPayload）';
COMMENT ON COLUMN monitor_alert_action.attempts IS '已尝试次数';
COMMENT ON COLUMN monitor_alert_action.next_time IS '最早可再次领取的时间（失败后退避）';
COMMENT ON COLUMN monitor_alert_action.last_error IS '最近一次失败原因';
COMMENT ON COLUMN monitor_alert_action.create_time IS '入队时间';
COMMENT ON COLUMN monitor_alert_action.done_time IS '结束时间';
CREATE UNIQUE INDEX uk_monitor_alert_action ON monitor_alert_action (alert_id, kind);
CREATE INDEX idx_monitor_alert_action_pending ON monitor_alert_action (next_time) WHERE status = 1;
CREATE INDEX idx_monitor_alert_action_done ON monitor_alert_action (done_time) WHERE status <> 1;

CREATE TABLE monitor_record_task (
    record_rule_id   uuid         PRIMARY KEY,
    next_eval_time   timestamptz  NOT NULL DEFAULT now(),
    lease_owner      varchar(64)  NULL,
    lease_until      timestamptz  NULL,
    last_eval_time   timestamptz  NULL,
    last_status      smallint     NOT NULL DEFAULT 0,
    last_error       varchar(512) NOT NULL DEFAULT '',
    last_duration_ms integer      NOT NULL DEFAULT 0,
    last_series      integer      NOT NULL DEFAULT 0,
    create_time      timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_monitor_record_task_status CHECK (last_status BETWEEN 0 AND 4)
);
COMMENT ON TABLE monitor_record_task IS '记录规则的执行任务：一行 = 一条启用的记录规则；引擎选主实例物化，评估实例以租约领取（与告警规则的 monitor_eval_task 分开：两者的物化与清理互不影响）';
COMMENT ON COLUMN monitor_record_task.record_rule_id IS '记录规则 ID';
COMMENT ON COLUMN monitor_record_task.next_eval_time IS '下次执行时间（数据库时钟）';
COMMENT ON COLUMN monitor_record_task.lease_owner IS '领取实例的属主令牌；NULL=未被领取。写回结果以它围栏';
COMMENT ON COLUMN monitor_record_task.lease_until IS '租约到期时间；过期的任务可被其它实例重领';
COMMENT ON COLUMN monitor_record_task.last_eval_time IS '最近一次执行时间';
COMMENT ON COLUMN monitor_record_task.last_status IS '最近一次执行结果：0=未执行 1=已写回 2=查询无结果 3=查询失败 4=写回失败';
COMMENT ON COLUMN monitor_record_task.last_error IS '最近一次执行的错误信息（不含凭据，至多 512 字节）';
COMMENT ON COLUMN monitor_record_task.last_duration_ms IS '最近一次执行耗时毫秒';
COMMENT ON COLUMN monitor_record_task.last_series IS '最近一次写回的序列数';
COMMENT ON COLUMN monitor_record_task.create_time IS '任务物化时间';
CREATE INDEX idx_monitor_record_task_due ON monitor_record_task (next_eval_time);

CREATE TABLE monitor_engine_instance (
    instance    varchar(64) PRIMARY KEY,
    roles       varchar(64) NOT NULL DEFAULT '',
    version     varchar(64) NOT NULL DEFAULT '',
    leader      boolean     NOT NULL DEFAULT false,
    start_time  timestamptz NOT NULL DEFAULT now(),
    last_seen   timestamptz NOT NULL DEFAULT now(),
    task_count  integer     NOT NULL DEFAULT 0,
    eval_lag_ms bigint      NOT NULL DEFAULT 0,
    eval_count  bigint      NOT NULL DEFAULT 0,
    fail_count  bigint      NOT NULL DEFAULT 0
);
COMMENT ON TABLE monitor_engine_instance IS '评估引擎实例心跳：每个实例周期性写入自己的状态，供引擎状态页展示；长时间没有心跳的行由引擎的维护任务清理';
COMMENT ON COLUMN monitor_engine_instance.instance IS '实例标识（主机名 + 启动时生成的随机后缀）';
COMMENT ON COLUMN monitor_engine_instance.roles IS '承担的角色，逗号分隔：planner / evaluator';
COMMENT ON COLUMN monitor_engine_instance.version IS '程序版本';
COMMENT ON COLUMN monitor_engine_instance.leader IS '此刻是否持有选主锁（任务物化 / 探活）';
COMMENT ON COLUMN monitor_engine_instance.start_time IS '实例启动时间';
COMMENT ON COLUMN monitor_engine_instance.last_seen IS '最近一次心跳时间（数据库时钟）';
COMMENT ON COLUMN monitor_engine_instance.task_count IS '最近一个心跳周期内评估的任务数';
COMMENT ON COLUMN monitor_engine_instance.eval_lag_ms IS '最近一个心跳周期内的最大评估延迟毫秒（领取时刻 − 应评估时刻）';
COMMENT ON COLUMN monitor_engine_instance.eval_count IS '启动以来完成的评估次数';
COMMENT ON COLUMN monitor_engine_instance.fail_count IS '启动以来查询错误的评估次数';

CREATE TABLE monitor_engine_event (
    id          uuid         PRIMARY KEY,
    type        varchar(48)  NOT NULL,
    object_id   uuid         NOT NULL,
    dedup_key   varchar(160) NOT NULL,
    detail      jsonb        NOT NULL DEFAULT '{}',
    status      smallint     NOT NULL DEFAULT 1,
    attempts    integer      NOT NULL DEFAULT 0,
    last_error  varchar(512) NOT NULL DEFAULT '',
    create_time timestamptz  NOT NULL DEFAULT now(),
    done_time   timestamptz  NULL,
    CONSTRAINT ck_monitor_engine_event_type CHECK (type IN (
        'monitor.engine.datasource_down', 'monitor.engine.datasource_recovered',
        'monitor.engine.rule_failing', 'monitor.engine.rule_recovered')),
    CONSTRAINT ck_monitor_engine_event_detail CHECK (jsonb_typeof(detail) = 'object'),
    CONSTRAINT ck_monitor_engine_event_status CHECK (status IN (1, 2, 3))
);
COMMENT ON TABLE monitor_engine_event IS '平台自监控事件发件箱：引擎检测到数据源持续不可用 / 规则持续评估失败（及其恢复）时写一行，core 派发为 monitor.engine.* 通知事件';
COMMENT ON COLUMN monitor_engine_event.id IS '主键，UUIDv7，引擎生成';
COMMENT ON COLUMN monitor_engine_event.type IS '事件类型（即通知事件类型）：monitor.engine.datasource_down / datasource_recovered / rule_failing / rule_recovered';
COMMENT ON COLUMN monitor_engine_event.object_id IS '对象 ID：数据源或规则';
COMMENT ON COLUMN monitor_engine_event.dedup_key IS '去重键，全表唯一：同一次故障（同一对象、同一起始时刻）的同类事件只写一行，选主切换后不重复';
COMMENT ON COLUMN monitor_engine_event.detail IS '内容 {"object_type","object_id","object_name","datasource_name","error","since","duration","failures"}（kit/monitorcontract.EngineEventPayload）';
COMMENT ON COLUMN monitor_engine_event.status IS '状态：1=待派发 2=已派发 3=已丢弃';
COMMENT ON COLUMN monitor_engine_event.attempts IS '派发失败次数';
COMMENT ON COLUMN monitor_engine_event.last_error IS '最近一次派发失败原因';
COMMENT ON COLUMN monitor_engine_event.create_time IS '入箱时间';
COMMENT ON COLUMN monitor_engine_event.done_time IS '派发或丢弃时间';
CREATE UNIQUE INDEX uk_monitor_engine_event_dedup ON monitor_engine_event (dedup_key);
CREATE INDEX idx_monitor_engine_event_pending ON monitor_engine_event (create_time) WHERE status = 1;

-- ---------------------------------------------------------------------------
-- 授权（角色由 0069 创建）
-- ---------------------------------------------------------------------------

-- 引擎：新配置表只读；数据源多回写一列；自己的运行表读写；core 负责推进的表只读 + 清理。
GRANT SELECT ON monitor_escalation, monitor_inhibit, monitor_record_rule, monitor_ci_index, monitor_calendar TO monitor_engine_role;
GRANT UPDATE (health_since) ON monitor_datasource TO monitor_engine_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON monitor_record_task, monitor_engine_instance TO monitor_engine_role;
-- 时间线只追加；动作与自监控事件入箱后由 core 推进状态；引擎保留清理权限（随事件的保留期删除）。
GRANT SELECT, INSERT, DELETE ON monitor_alert_event, monitor_alert_action, monitor_engine_event TO monitor_engine_role;
-- 指令：引擎只回写处理结果。
GRANT SELECT, DELETE ON monitor_alert_cmd TO monitor_engine_role;
GRANT UPDATE (status, result, done_time) ON monitor_alert_cmd TO monitor_engine_role;
GRANT SELECT, DELETE ON monitor_alert_escalation TO monitor_engine_role;

-- core：新配置表全权；运行表只读，按列回写自己负责的部分。
GRANT SELECT, INSERT, UPDATE, DELETE ON monitor_escalation, monitor_inhibit, monitor_record_rule, monitor_alert_view,
    monitor_ci_index, monitor_calendar TO monitor_core_role;
GRANT SELECT ON monitor_record_task, monitor_engine_instance TO monitor_core_role;
GRANT UPDATE (ack_user, ack_time, ticket_id, workflow_instance_id, ai_summary) ON monitor_alert TO monitor_core_role;
GRANT SELECT, INSERT ON monitor_alert_event, monitor_alert_cmd TO monitor_core_role;
GRANT SELECT, INSERT, UPDATE ON monitor_alert_escalation TO monitor_core_role;
GRANT SELECT ON monitor_alert_action, monitor_engine_event TO monitor_core_role;
GRANT UPDATE (status, attempts, next_time, last_error, done_time) ON monitor_alert_action TO monitor_core_role;
GRANT UPDATE (status, attempts, last_error, done_time) ON monitor_engine_event TO monitor_core_role;

-- ---------------------------------------------------------------------------
-- 内置数据
-- ---------------------------------------------------------------------------

INSERT INTO monitor_calendar (id) VALUES (1);

INSERT INTO monitor_cfg_version (name, version) VALUES
('monitor_escalation', 1),
('monitor_inhibit', 1),
('monitor_ci_index', 1),
('monitor_calendar', 1);
COMMENT ON COLUMN monitor_cfg_version.name IS '配置表名：monitor_datasource / monitor_mute / monitor_rule_group / monitor_escalation / monitor_inhibit / monitor_ci_index / monitor_calendar';

-- 平台自监控通知的缺省路由，做法同 0069：只绑定迁移执行时已启用的邮件与群机器人渠道。升级通知
-- （monitor.alert.escalated）已被 0069 的「监控告警通知」路由（monitor.alert.*）覆盖，不另建路由。
INSERT INTO notify_route (id, biz_id, name, description, event_pattern, severity_min, priority, channel_ids, recipient_policy, aggregate_window_s)
VALUES ('018f3e1a-0cc1-7000-8000-000000000003', 'default', '监控平台自监控通知',
        '数据源持续不可用、规则持续评估失败及其恢复：发给有数据源管理权限的人', 'monitor.engine.*', 1, 5,
        ARRAY(SELECT id FROM notify_channel
              WHERE biz_id = 'default' AND type IN ('email', 'feishu', 'dingtalk', 'wecom') AND enabled AND delete_time IS NULL ORDER BY create_time),
        '{"subscription": true}', 0);

-- 模板：纯文本、不限渠道类型；同一事件已有模板则保留现有的。升级通知的载荷是 kit/monitorcontract.NotifyPayload
-- （多出 escalation_level / escalation_seq / escalation_name）；自监控通知的载荷是 EngineEventPayload。
INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body) VALUES
('018f3e1a-0cc2-7000-8000-000000000005', 'default', 'monitor.alert.escalated', '*', 'text',
 '【{{if eq .Severity 4}}致命{{else if eq .Severity 3}}严重{{else if eq .Severity 2}}警告{{else}}提示{{end}}告警升级 · 第 {{.Payload.escalation_level}} 级】{{.Payload.rule_name}}',
 '告警持续未处理，已升级到第 {{.Payload.escalation_level}} 级（该级第 {{.Payload.escalation_seq}} 次通知）。{{if .Payload.escalation_name}}
升级策略：{{.Payload.escalation_name}}{{end}}
规则：{{.Payload.rule_name}}
分组：{{.Payload.group_name}}
级别：{{if eq .Severity 4}}致命{{else if eq .Severity 3}}严重{{else if eq .Severity 2}}警告{{else}}提示{{end}}
数据源：{{.Payload.datasource_name}}
触发值：{{.Payload.value}}
首次触发：{{.Payload.first_fire_time}}
已持续：{{.Payload.duration}}{{if .Payload.ci_name}}
关联资源：{{.Payload.ci_name}}{{end}}{{if .Payload.labels}}
标签：{{range $k, $v := .Payload.labels}}
  {{$k}} = {{$v}}{{end}}{{end}}{{if .Payload.annotations}}
附加信息：{{range $k, $v := .Payload.annotations}}
  {{$k}}：{{$v}}{{end}}{{end}}
表达式：{{.Payload.expr}}
详情：{{.Payload.alert_url}}'),
('018f3e1a-0cc2-7000-8000-000000000006', 'default', 'monitor.engine.datasource_down', '*', 'text',
 '【监控平台】数据源持续不可用：{{.Payload.object_name}}',
 '监控数据源持续不可用，其上的告警规则无法评估（已有告警保持原状，不会判恢复）。
数据源：{{.Payload.object_name}}
开始时间：{{.Payload.since}}
已持续：{{.Payload.duration}}{{if .Payload.error}}
最近一次失败原因：{{.Payload.error}}{{end}}{{if .Payload.url}}
详情：{{.Payload.url}}{{end}}'),
('018f3e1a-0cc2-7000-8000-000000000007', 'default', 'monitor.engine.datasource_recovered', '*', 'text',
 '【监控平台】数据源已恢复：{{.Payload.object_name}}',
 '监控数据源已恢复可用。
数据源：{{.Payload.object_name}}
故障开始：{{.Payload.since}}
故障时长：{{.Payload.duration}}{{if .Payload.url}}
详情：{{.Payload.url}}{{end}}'),
('018f3e1a-0cc2-7000-8000-000000000008', 'default', 'monitor.engine.rule_failing', '*', 'text',
 '【监控平台】告警规则持续评估失败：{{.Payload.object_name}}',
 '告警规则持续评估失败，期间不会产生新的告警。
规则：{{.Payload.object_name}}{{if .Payload.datasource_name}}
数据源：{{.Payload.datasource_name}}{{end}}
开始时间：{{.Payload.since}}
已持续：{{.Payload.duration}}{{if .Payload.failures}}
连续失败：{{.Payload.failures}} 次{{end}}{{if .Payload.error}}
最近一次失败原因：{{.Payload.error}}{{end}}{{if .Payload.url}}
详情：{{.Payload.url}}{{end}}'),
('018f3e1a-0cc2-7000-8000-000000000009', 'default', 'monitor.engine.rule_recovered', '*', 'text',
 '【监控平台】告警规则评估已恢复：{{.Payload.object_name}}',
 '告警规则的评估已恢复正常。
规则：{{.Payload.object_name}}{{if .Payload.datasource_name}}
数据源：{{.Payload.datasource_name}}{{end}}
故障开始：{{.Payload.since}}
故障时长：{{.Payload.duration}}{{if .Payload.url}}
详情：{{.Payload.url}}{{end}}')
ON CONFLICT (biz_id, event_type, channel_type) WHERE delete_time IS NULL DO NOTHING;

-- 配置版本 +1：通知管道据此刷新路由与模板缓存。
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
