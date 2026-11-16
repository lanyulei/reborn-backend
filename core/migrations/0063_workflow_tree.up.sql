-- 0063 流程引擎：子流程继承根流程的业务对象引用、列表索引、回路兜底计数、流程通知的缺省路由与模板。

-- ---------------------------------------------------------------------------
-- 根流程引用：子流程实例不挂业务对象（business_type / business_id 的唯一约束只属于顶层流程），
-- 但要知道自己最终属于哪个业务对象——工单流程里的子流程，其处理人的待办要能回到那张工单。
ALTER TABLE workflow_instance
    ADD COLUMN root_id            uuid        NULL,
    ADD COLUMN root_business_type varchar(64) NOT NULL DEFAULT '',
    ADD COLUMN root_business_id   varchar(64) NOT NULL DEFAULT '',
    ADD COLUMN auto_skips         integer     NOT NULL DEFAULT 0;

-- 存量数据：顶层流程的根就是它自己；子流程沿 parent_instance_id 上溯到顶层。
WITH RECURSIVE tree AS (
    SELECT id, id AS root_id, business_type, business_id FROM workflow_instance WHERE parent_instance_id IS NULL
    UNION ALL
    SELECT c.id, t.root_id, t.business_type, t.business_id
    FROM workflow_instance c JOIN tree t ON c.parent_instance_id = t.id
)
UPDATE workflow_instance i
SET root_id = t.root_id, root_business_type = t.business_type, root_business_id = t.business_id
FROM tree t WHERE t.id = i.id;
-- 父实例已不存在的孤儿（不应出现）：按顶层处理。
UPDATE workflow_instance SET root_id = id WHERE root_id IS NULL;
ALTER TABLE workflow_instance ALTER COLUMN root_id SET NOT NULL;

COMMENT ON COLUMN workflow_instance.root_id IS '根流程实例 ID：顶层流程为自身，子流程为调用链最顶层的那个实例';
COMMENT ON COLUMN workflow_instance.root_business_type IS '根流程所挂业务对象的类型（子流程继承，只读；顶层流程与 business_type 相同）';
COMMENT ON COLUMN workflow_instance.root_business_id IS '根流程所挂业务对象的 ID（子流程继承，只读；顶层流程与 business_id 相同）';
COMMENT ON COLUMN workflow_instance.auto_skips IS '因处理人为空 / 定时已过而自动跳过节点的累计次数（回路失控的兜底上限）';
CREATE INDEX idx_workflow_instance_root ON workflow_instance (root_id);

-- 实例管理列表：缺省排序、标题的包含匹配（pg_trgm 已在 0047 启用）。
CREATE INDEX idx_workflow_instance_start ON workflow_instance (start_time DESC, id DESC);
CREATE INDEX idx_workflow_instance_title_trgm ON workflow_instance USING gin (title gin_trgm_ops);
-- 删除流程定义前判定「是否仍有进行中的实例」：只看进行中的，不必翻遍该定义的全部历史实例。
CREATE INDEX idx_workflow_instance_definition_running ON workflow_instance (definition_id) WHERE status = 1;

-- ---------------------------------------------------------------------------
-- 流程通知的缺省配置（缺省业务命名空间 default）：一条路由 + 五个模板，做法同 0055 的值班通知。
-- 流程事件由 core 在业务事务内直插 notify_event，没有匹配路由时事件只落库不投递。
-- 渠道无法由迁移创建：这里只绑定迁移执行时 default 命名空间下已启用的邮件渠道；执行时没有
-- 则路由的渠道为空（不投递），之后在「通知管理 → 规则」里为这条路由选择渠道即可。
-- 事件自带收件人（用户 ID）；subscription=true 满足「邮件渠道须有收件策略」的校验。
INSERT INTO notify_route (id, biz_id, name, description, event_pattern, severity_min, priority, channel_ids, recipient_policy)
VALUES ('018f3e1a-0cb1-7000-8000-000000000001', 'default', '流程通知',
        '待办到达、抄送、催办 / 超时提醒、流程结束、自动任务失败：发给事件指定的处理人 / 发起人 / 流程管理员', 'workflow.*', 1, 5,
        ARRAY(SELECT id FROM notify_channel
              WHERE biz_id = 'default' AND type = 'email' AND enabled AND delete_time IS NULL ORDER BY create_time),
        '{"subscription": true}');

INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body) VALUES
('018f3e1a-0cb2-7000-8000-000000000001', 'default', 'workflow.task.assigned', '*', 'text',
 '待办：{{.Title}}',
 '你有一个新的待办。
流程：{{.Payload.definitionName}}
标题：{{.Payload.title}}
环节：{{.Payload.nodeName}}'),
('018f3e1a-0cb2-7000-8000-000000000002', 'default', 'workflow.task.cc', '*', 'text',
 '抄送：{{.Title}}',
 '有一个流程抄送给你。
流程：{{.Payload.definitionName}}
标题：{{.Payload.title}}
环节：{{.Payload.nodeName}}'),
('018f3e1a-0cb2-7000-8000-000000000003', 'default', 'workflow.task.remind', '*', 'text',
 '待办提醒：{{.Title}}',
 '你的待办即将超时或已经超时，请尽快处理。
流程：{{.Payload.definitionName}}
标题：{{.Payload.title}}
环节：{{.Payload.nodeName}}'),
('018f3e1a-0cb2-7000-8000-000000000004', 'default', 'workflow.instance.finished', '*', 'text',
 '流程已结束：{{.Title}}',
 '你发起的流程已结束。
流程：{{.Payload.definitionName}}
标题：{{.Payload.title}}
请到「我发起的」查看处理结果。'),
('018f3e1a-0cb2-7000-8000-000000000005', 'default', 'workflow.service.failed', '*', 'text',
 '流程需要处理：{{.Title}}',
 '流程里有一个环节执行失败，流程停在原处，请到实例管理里重试或跳过。
流程：{{.Payload.definitionName}}
标题：{{.Payload.title}}{{if .Payload.nodeName}}
环节：{{.Payload.nodeName}}{{end}}')
ON CONFLICT (biz_id, event_type, channel_type) WHERE delete_time IS NULL DO NOTHING;

-- 配置版本 +1：通知管道据此刷新路由与模板缓存。
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
