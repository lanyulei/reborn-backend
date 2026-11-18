-- 0073 容器平台 Warning 事件：K8s 事件缺省只保留 1 小时，数据面采集各集群的 Warning 事件落库，
-- 供跨集群检索与通知。同一集群同一对象同一原因只有一行：重复发生只累加次数与最近发生时间。
-- 普通表（不分区）：去重靠唯一索引，分区表的唯一索引必须含分区键，与「同一对象一行」冲突；
-- 行数受去重约束，按最近发生时间保留 7 天，由数据面的维护任务清理。

CREATE TABLE k8s_event (
    id           uuid          PRIMARY KEY,
    cluster_id   uuid          NOT NULL,
    cluster_name varchar(63)   NOT NULL DEFAULT '',
    namespace    varchar(63)   NOT NULL DEFAULT '',
    kind         varchar(128)  NOT NULL DEFAULT '',
    name         varchar(253)  NOT NULL DEFAULT '',
    reason       varchar(128)  NOT NULL DEFAULT '',
    message      varchar(1024) NOT NULL DEFAULT '',
    source       varchar(128)  NOT NULL DEFAULT '',
    count        bigint        NOT NULL DEFAULT 1,
    first_time   timestamptz   NOT NULL,
    last_time    timestamptz   NOT NULL,
    notify_time  timestamptz   NULL,
    create_time  timestamptz   NOT NULL DEFAULT now()
);
COMMENT ON TABLE k8s_event IS '集群 Warning 事件：同一集群同一对象同一原因一行，重复发生累加次数；按最近发生时间保留 7 天';
COMMENT ON COLUMN k8s_event.id IS '主键，UUIDv7，数据面生成';
COMMENT ON COLUMN k8s_event.cluster_id IS '集群 ID';
COMMENT ON COLUMN k8s_event.cluster_name IS '集群名称（冗余，集群移除后仍可读）';
COMMENT ON COLUMN k8s_event.namespace IS '关联对象的命名空间；集群级对象为空';
COMMENT ON COLUMN k8s_event.kind IS '关联对象的 Kind';
COMMENT ON COLUMN k8s_event.name IS '关联对象的名称';
COMMENT ON COLUMN k8s_event.reason IS '事件原因';
COMMENT ON COLUMN k8s_event.message IS '最近一次的事件说明（至多 1024 字符）';
COMMENT ON COLUMN k8s_event.source IS '事件来源组件';
COMMENT ON COLUMN k8s_event.count IS '累计发生次数';
COMMENT ON COLUMN k8s_event.first_time IS '首次发生时间';
COMMENT ON COLUMN k8s_event.last_time IS '最近发生时间';
COMMENT ON COLUMN k8s_event.notify_time IS '最近一次发出通知的时间；NULL=未通知过。同一行在通知窗口内至多通知一次';
COMMENT ON COLUMN k8s_event.create_time IS '入库时间';
CREATE UNIQUE INDEX uk_k8s_event_object ON k8s_event (cluster_id, namespace, kind, name, reason);
CREATE INDEX idx_k8s_event_time ON k8s_event (last_time DESC);
CREATE INDEX idx_k8s_event_cluster ON k8s_event (cluster_id, namespace, last_time DESC);
CREATE INDEX idx_k8s_event_reason ON k8s_event (reason, last_time DESC);

GRANT SELECT, INSERT, UPDATE, DELETE ON k8s_event TO k8s_plane_role;
GRANT SELECT ON k8s_event TO k8s_core_role;

-- 事件通知的缺省模板（缺省业务命名空间 default）：通知模板只能按精确的事件类型匹配，这里为缺省上报的
-- 各个原因（kit/k8scontract.DefaultEventReasons）各建一个；集群上自定义了其他原因时，在「通知管理 → 规则」
-- 里为对应的事件类型 k8s.event.<原因小写> 添加模板。路由沿用 0072 的「容器平台通知」（k8s.*）。
INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body)
SELECT gen_random_uuid(), 'default', 'k8s.event.' || r, '*', 'text',
 '【集群事件】{{.Payload.cluster_name}} {{.Payload.reason}}：{{.Payload.kind}}/{{.Payload.name}}',
 '集群：{{.Payload.cluster_name}}{{if .Payload.namespace}}
命名空间：{{.Payload.namespace}}{{end}}
对象：{{.Payload.kind}}/{{.Payload.name}}
原因：{{.Payload.reason}}
说明：{{.Payload.message}}
累计次数：{{.Payload.count}}
最近发生：{{.Payload.time}}'
FROM unnest(ARRAY['nodenotready', 'oomkilling', 'oomkilled', 'failedscheduling', 'failed', 'errimagepull',
                  'imagepullbackoff', 'failedmount', 'failedattachvolume', 'backoff']) AS r
ON CONFLICT (biz_id, event_type, channel_type) WHERE delete_time IS NULL DO NOTHING;

UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
