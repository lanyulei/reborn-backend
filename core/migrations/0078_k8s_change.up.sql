-- 0078 容器平台变更审批：变更单（k8s_change）与平台级设置（k8s_setting）。
-- 变更单由 core 建单并随工单的审批结果推进到「已批准」；负责集群的数据面领取并以发起人身份执行，
-- 回写结果；core 的 k8s-change 循环把结果写回工单并通知发起人。
-- exec 列是加密的执行计划（可能含 Secret 内容与 Helm values）：core 只加密写入，数据面解密。

CREATE TABLE k8s_change (
    id             uuid          PRIMARY KEY,
    kind           smallint      NOT NULL,
    cluster_id     uuid          NOT NULL,
    cluster_name   varchar(63)   NOT NULL DEFAULT '',
    namespace      varchar(63)   NOT NULL DEFAULT '',
    api_group      varchar(128)  NOT NULL DEFAULT '',
    api_version    varchar(32)   NOT NULL DEFAULT '',
    resource       varchar(128)  NOT NULL DEFAULT '',
    res_kind       varchar(128)  NOT NULL DEFAULT '',
    name           varchar(253)  NOT NULL DEFAULT '',
    action         varchar(32)   NOT NULL,
    op             varchar(64)   NOT NULL DEFAULT '',
    summary        varchar(512)  NOT NULL DEFAULT '',
    display        jsonb         NOT NULL DEFAULT '{}',
    exec           bytea         NULL,
    params         jsonb         NOT NULL DEFAULT '{}',
    status         smallint      NOT NULL DEFAULT 1,
    requester_id   uuid          NOT NULL,
    requester_name varchar(64)   NOT NULL DEFAULT '',
    requester      jsonb         NULL,
    client_ip      varchar(64)   NOT NULL DEFAULT '',
    ticket_id      uuid          NULL,
    ticket_number  varchar(64)   NOT NULL DEFAULT '',
    message        varchar(2000) NOT NULL DEFAULT '',
    result         jsonb         NOT NULL DEFAULT '{}',
    attempts       integer       NOT NULL DEFAULT 0,
    noted          boolean       NOT NULL DEFAULT false,
    lease_owner    varchar(64)   NOT NULL DEFAULT '',
    lease_until    timestamptz   NULL,
    create_time    timestamptz   NOT NULL DEFAULT now(),
    approve_time   timestamptz   NULL,
    expire_time    timestamptz   NULL,
    finish_time    timestamptz   NULL,
    CONSTRAINT ck_k8s_change_kind CHECK (kind IN (1, 2, 3)),
    CONSTRAINT ck_k8s_change_status CHECK (status BETWEEN 1 AND 8)
);
COMMENT ON TABLE k8s_change IS '变更单：需审批的集群上的一次写操作，或自助申请（命名空间 / 权限）；状态与结构见 kit/k8scontract/change.go';
COMMENT ON COLUMN k8s_change.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN k8s_change.kind IS '类型：1=写操作 2=申请命名空间 3=申请权限';
COMMENT ON COLUMN k8s_change.cluster_id IS '集群';
COMMENT ON COLUMN k8s_change.cluster_name IS '集群名称（冗余）';
COMMENT ON COLUMN k8s_change.namespace IS '目标命名空间；集群级资源为空';
COMMENT ON COLUMN k8s_change.api_group IS '目标资源的 API 组';
COMMENT ON COLUMN k8s_change.api_version IS '目标资源的 API 版本';
COMMENT ON COLUMN k8s_change.resource IS '目标资源类型（复数小写）';
COMMENT ON COLUMN k8s_change.res_kind IS '目标资源 Kind';
COMMENT ON COLUMN k8s_change.name IS '目标对象名称';
COMMENT ON COLUMN k8s_change.action IS '动作（k8scontract.Action*，或 namespace_request / permission_request）';
COMMENT ON COLUMN k8s_change.op IS '原始操作名（数据面内部接口的 op / stream）';
COMMENT ON COLUMN k8s_change.summary IS '一句话说明';
COMMENT ON COLUMN k8s_change.display IS '展示用的变更内容（k8scontract.ChangeDisplay）：提交时与预检后的对象、涉及的字段；Secret 已掩码';
COMMENT ON COLUMN k8s_change.exec IS '加密的执行计划（k8scontract.ChangeExec，kit/secretbox）；只有数据面解密';
COMMENT ON COLUMN k8s_change.params IS '自助申请的参数（k8scontract.NamespaceRequest / PermissionRequest）';
COMMENT ON COLUMN k8s_change.status IS '状态：1=待审批 2=已批准 3=执行中 4=成功 5=失败 6=审批未通过 7=已撤回 8=已过期';
COMMENT ON COLUMN k8s_change.requester_id IS '发起人用户 ID';
COMMENT ON COLUMN k8s_change.requester_name IS '发起人用户名（冗余）';
COMMENT ON COLUMN k8s_change.requester IS '发起人在批准时刻的身份（k8scontract.ChangeRequester：启用角色、是否超管）；数据面据此以发起人身份执行';
COMMENT ON COLUMN k8s_change.client_ip IS '提交时的来源 IP';
COMMENT ON COLUMN k8s_change.ticket_id IS '审批工单';
COMMENT ON COLUMN k8s_change.ticket_number IS '工单编号（冗余）';
COMMENT ON COLUMN k8s_change.message IS '结果说明：失败原因、审批未通过的原因等';
COMMENT ON COLUMN k8s_change.result IS '执行结果（k8scontract.ChangeResult）';
COMMENT ON COLUMN k8s_change.attempts IS '执行尝试次数（集群暂不可达时重试）';
COMMENT ON COLUMN k8s_change.noted IS '结果是否已写回工单并通知发起人';
COMMENT ON COLUMN k8s_change.lease_owner IS '领取该变更单的数据面实例';
COMMENT ON COLUMN k8s_change.lease_until IS '领取租约到期时间';
COMMENT ON COLUMN k8s_change.create_time IS '提交时间';
COMMENT ON COLUMN k8s_change.approve_time IS '批准时间';
COMMENT ON COLUMN k8s_change.expire_time IS '批准后的失效时间：过期不再执行';
COMMENT ON COLUMN k8s_change.finish_time IS '进入终态的时间';
CREATE INDEX idx_k8s_change_time ON k8s_change (create_time DESC);
CREATE INDEX idx_k8s_change_requester ON k8s_change (requester_id, create_time DESC);
CREATE INDEX idx_k8s_change_cluster ON k8s_change (cluster_id, create_time DESC);
CREATE INDEX idx_k8s_change_runnable ON k8s_change (cluster_id, approve_time) WHERE status IN (2, 3);
CREATE INDEX idx_k8s_change_unnoted ON k8s_change (finish_time) WHERE status >= 4 AND NOT noted;
CREATE UNIQUE INDEX uk_k8s_change_ticket ON k8s_change (ticket_id) WHERE ticket_id IS NOT NULL;

CREATE TABLE k8s_setting (
    name        varchar(64)  PRIMARY KEY,
    value       text         NOT NULL DEFAULT '',
    update_user uuid         NULL,
    update_time timestamptz  NOT NULL DEFAULT now()
);
COMMENT ON TABLE k8s_setting IS '容器平台的平台级设置（键值）';
COMMENT ON COLUMN k8s_setting.name IS '设置名：namespace_request_template_id / permission_request_template_id（自助申请所用的工单模板，空 = 未开放）';
COMMENT ON COLUMN k8s_setting.value IS '取值';
COMMENT ON COLUMN k8s_setting.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN k8s_setting.update_time IS '最后更新时间';

-- 数据面：只读变更单，并回写执行状态相关的列。
GRANT SELECT ON k8s_change TO k8s_plane_role;
GRANT UPDATE (status, message, result, attempts, lease_owner, lease_until, finish_time) ON k8s_change TO k8s_plane_role;
-- core：建单、推进审批状态、写回结果标记；平台设置全权。
GRANT SELECT, INSERT, UPDATE, DELETE ON k8s_change, k8s_setting TO k8s_core_role;

-- 变更执行结果的通知模板（发给发起人；路由沿用 0072 的 k8s.*）。
INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body) VALUES
('018f3e1a-0cd4-7000-8000-000000000001', 'default', 'k8s.change.succeeded', '*', 'text',
 '【变更已执行】{{.Payload.summary}}',
 '你提交的变更已审批通过并执行成功。
集群：{{.Payload.cluster_name}}{{if .Payload.namespace}}
命名空间：{{.Payload.namespace}}{{end}}{{if .Payload.target}}
对象：{{.Payload.target}}{{end}}
内容：{{.Payload.summary}}{{if .Payload.ticket_number}}
工单：{{.Payload.ticket_number}}{{end}}
时间：{{.Payload.time}}'),
('018f3e1a-0cd4-7000-8000-000000000002', 'default', 'k8s.change.failed', '*', 'text',
 '【变更未执行】{{.Payload.summary}}',
 '你提交的变更{{.Payload.status_text}}。
集群：{{.Payload.cluster_name}}{{if .Payload.namespace}}
命名空间：{{.Payload.namespace}}{{end}}{{if .Payload.target}}
对象：{{.Payload.target}}{{end}}
内容：{{.Payload.summary}}
原因：{{.Payload.message}}{{if .Payload.ticket_number}}
工单：{{.Payload.ticket_number}}{{end}}
时间：{{.Payload.time}}')
ON CONFLICT (biz_id, event_type, channel_type) WHERE delete_time IS NULL DO NOTHING;

UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
