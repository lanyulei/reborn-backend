-- 0077 容器平台治理（P3）：限时 kubeconfig 令牌、巡检（运行 + 发现项）、终端录制的补充列。
-- jsonb 列结构与状态字面量的唯一事实源是 kit/k8scontract（governance.go）。约定同 0072。

-- 限时 kubeconfig 令牌：kubeconfig 指向 core 的集群代理入口，Bearer 即本表的令牌（<令牌 ID>.<密钥>）。
-- 每个请求都校验：未吊销、未过期、用户仍可用；随后以该用户此刻的角色经数据面访问集群。
CREATE TABLE k8s_kube_token (
    id             uuid         PRIMARY KEY,
    user_id        uuid         NOT NULL,
    username       varchar(64)  NOT NULL DEFAULT '',
    cluster_id     uuid         NOT NULL REFERENCES k8s_cluster (id),
    token_hash     bytea        NOT NULL,
    note           varchar(128) NOT NULL DEFAULT '',
    expire_time    timestamptz  NOT NULL,
    last_used_time timestamptz  NULL,
    last_used_ip   varchar(64)  NOT NULL DEFAULT '',
    revoke_time    timestamptz  NULL,
    revoke_user    uuid         NULL,
    create_time    timestamptz  NOT NULL DEFAULT now()
);
COMMENT ON TABLE k8s_kube_token IS '限时 kubeconfig 令牌：只存密钥的 SHA-256；吊销或过期后立即失效';
COMMENT ON COLUMN k8s_kube_token.id IS '令牌 ID，UUIDv7；即令牌明文的前半段';
COMMENT ON COLUMN k8s_kube_token.user_id IS '签发给的用户';
COMMENT ON COLUMN k8s_kube_token.username IS '用户名（冗余）';
COMMENT ON COLUMN k8s_kube_token.cluster_id IS '集群';
COMMENT ON COLUMN k8s_kube_token.token_hash IS '密钥的 SHA-256';
COMMENT ON COLUMN k8s_kube_token.note IS '用途备注';
COMMENT ON COLUMN k8s_kube_token.expire_time IS '过期时间';
COMMENT ON COLUMN k8s_kube_token.last_used_time IS '最近一次使用时间（至多每分钟回写一次）';
COMMENT ON COLUMN k8s_kube_token.last_used_ip IS '最近一次使用的来源 IP';
COMMENT ON COLUMN k8s_kube_token.revoke_time IS '吊销时间；NULL=未吊销';
COMMENT ON COLUMN k8s_kube_token.revoke_user IS '吊销人用户 ID';
COMMENT ON COLUMN k8s_kube_token.create_time IS '签发时间';
CREATE INDEX idx_k8s_kube_token_user ON k8s_kube_token (user_id, create_time DESC);
CREATE INDEX idx_k8s_kube_token_expire ON k8s_kube_token (expire_time);

-- 巡检运行：一行一次巡检。手动触发由 core 写入待执行（status=1）的行，数据面领取执行；
-- 定时巡检由负责该集群的数据面实例自己建行。
CREATE TABLE k8s_inspection_run (
    id           uuid         PRIMARY KEY,
    cluster_id   uuid         NOT NULL,
    cluster_name varchar(63)  NOT NULL DEFAULT '',
    trigger      smallint     NOT NULL,
    status       smallint     NOT NULL DEFAULT 1,
    score        integer      NOT NULL DEFAULT 0,
    summary      jsonb        NOT NULL DEFAULT '{}',
    error        varchar(512) NOT NULL DEFAULT '',
    create_user  uuid         NULL,
    create_time  timestamptz  NOT NULL DEFAULT now(),
    start_time   timestamptz  NULL,
    finish_time  timestamptz  NULL,
    CONSTRAINT ck_k8s_inspection_run_trigger CHECK (trigger IN (1, 2)),
    CONSTRAINT ck_k8s_inspection_run_status CHECK (status IN (1, 2, 3, 4))
);
COMMENT ON TABLE k8s_inspection_run IS '巡检运行：每个集群保留最近若干次，更早的由数据面清理';
COMMENT ON COLUMN k8s_inspection_run.id IS '主键，UUIDv7';
COMMENT ON COLUMN k8s_inspection_run.cluster_id IS '集群';
COMMENT ON COLUMN k8s_inspection_run.cluster_name IS '集群名称（冗余）';
COMMENT ON COLUMN k8s_inspection_run.trigger IS '触发方式：1=定时 2=手动';
COMMENT ON COLUMN k8s_inspection_run.status IS '状态：1=待执行 2=执行中 3=完成 4=失败';
COMMENT ON COLUMN k8s_inspection_run.score IS '得分 0-100';
COMMENT ON COLUMN k8s_inspection_run.summary IS '摘要（k8scontract.InspectionSummary）：各检查项的发现数、检查的对象数、是否不完整';
COMMENT ON COLUMN k8s_inspection_run.error IS '失败原因';
COMMENT ON COLUMN k8s_inspection_run.create_user IS '手动触发人用户 ID';
COMMENT ON COLUMN k8s_inspection_run.create_time IS '创建时间';
COMMENT ON COLUMN k8s_inspection_run.start_time IS '开始执行时间';
COMMENT ON COLUMN k8s_inspection_run.finish_time IS '结束时间';
CREATE INDEX idx_k8s_inspection_run_cluster ON k8s_inspection_run (cluster_id, create_time DESC);
CREATE INDEX idx_k8s_inspection_run_pending ON k8s_inspection_run (create_time) WHERE status IN (1, 2);

CREATE TABLE k8s_inspection_finding (
    id        uuid         PRIMARY KEY,
    run_id    uuid         NOT NULL REFERENCES k8s_inspection_run (id) ON DELETE CASCADE,
    check_id  varchar(64)  NOT NULL,
    severity  smallint     NOT NULL,
    namespace varchar(63)  NOT NULL DEFAULT '',
    kind      varchar(128) NOT NULL DEFAULT '',
    name      varchar(253) NOT NULL DEFAULT '',
    message   varchar(512) NOT NULL DEFAULT '',
    CONSTRAINT ck_k8s_inspection_finding_severity CHECK (severity BETWEEN 1 AND 3)
);
COMMENT ON TABLE k8s_inspection_finding IS '巡检发现项：每个检查项至多保留 k8scontract.MaxFindingsPerCheck 条，总数见运行的摘要';
COMMENT ON COLUMN k8s_inspection_finding.id IS '主键，UUIDv7';
COMMENT ON COLUMN k8s_inspection_finding.run_id IS '所属巡检运行';
COMMENT ON COLUMN k8s_inspection_finding.check_id IS '检查项（k8scontract.Check*）';
COMMENT ON COLUMN k8s_inspection_finding.severity IS '级别：1=提示 2=警告 3=严重';
COMMENT ON COLUMN k8s_inspection_finding.namespace IS '命名空间；集群级对象为空';
COMMENT ON COLUMN k8s_inspection_finding.kind IS '对象 Kind';
COMMENT ON COLUMN k8s_inspection_finding.name IS '对象名称';
COMMENT ON COLUMN k8s_inspection_finding.message IS '说明';
CREATE INDEX idx_k8s_inspection_finding_run ON k8s_inspection_finding (run_id, check_id, namespace);

-- 终端录制：只录输出，不录输入。块内容为 gzip 压缩的若干行 [毫秒偏移,"o","<base64 输出>"]。
ALTER TABLE k8s_exec_session ADD COLUMN record_truncated boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN k8s_exec_session.record_truncated IS '录制超过单会话上限后停止（回放内容不完整）';
COMMENT ON COLUMN k8s_exec_record.data IS '块内容：若干行 [毫秒偏移, "o", "<base64 输出>"]（另有 "r" 行记录窗口大小 "列x行"）的 JSON，gzip 压缩';

-- 授权：令牌表只有 core 可见；巡检由数据面写、core 读，core 可插入待执行的运行。
GRANT SELECT, INSERT, UPDATE, DELETE ON k8s_kube_token TO k8s_core_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON k8s_inspection_run, k8s_inspection_finding TO k8s_plane_role;
GRANT SELECT, INSERT ON k8s_inspection_run TO k8s_core_role;
GRANT SELECT ON k8s_inspection_finding TO k8s_core_role;

-- 巡检报告的缺省通知模板（路由沿用 0072 的「容器平台通知」k8s.*）。
INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body) VALUES
('018f3e1a-0cd2-7000-8000-000000000077', 'default', 'k8s.inspection.report', '*', 'text',
 '【集群巡检】{{.Payload.cluster_name}} 得分 {{.Payload.score}}',
 '集群：{{.Payload.cluster_name}}
得分：{{.Payload.score}}
严重：{{.Payload.critical}}  警告：{{.Payload.warning}}  提示：{{.Payload.info}}
巡检时间：{{.Payload.time}}
详情：{{.Payload.url}}')
ON CONFLICT (biz_id, event_type, channel_type) WHERE delete_time IS NULL DO NOTHING;
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
