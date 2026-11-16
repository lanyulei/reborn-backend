-- 0064 工单：按优先级筛选的索引、SLA 超时通知的缺省路由与模板。

-- ---------------------------------------------------------------------------
-- 管理列表按优先级筛选。
CREATE INDEX idx_ticket_order_priority ON ticket_order (priority, create_time DESC);

-- ---------------------------------------------------------------------------
-- SLA 超时通知的缺省配置（缺省业务命名空间 default）：一条路由 + 一个模板，做法同 0063 的流程通知。
-- 事件由 core 在巡检事务内直插 notify_event，没有匹配路由时事件只落库不投递。
-- 渠道无法由迁移创建：这里只绑定迁移执行时 default 命名空间下已启用的邮件渠道；执行时没有
-- 则路由的渠道为空（不投递），之后在「通知管理 → 规则」里为这条路由选择渠道即可。
-- 事件自带收件人（当前处理人 + 升级对象）；subscription=true 满足「邮件渠道须有收件策略」的校验。
INSERT INTO notify_route (id, biz_id, name, description, event_pattern, severity_min, priority, channel_ids, recipient_policy)
VALUES ('018f3e1a-0cb1-7000-8000-000000000002', 'default', '工单 SLA 通知',
        '工单的响应 / 解决时限已过：发给当前处理人与模板配置的升级对象（都没有时发给流程管理员）', 'ticket.sla.*', 1, 5,
        ARRAY(SELECT id FROM notify_channel
              WHERE biz_id = 'default' AND type = 'email' AND enabled AND delete_time IS NULL ORDER BY create_time),
        '{"subscription": true}');

INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body) VALUES
('018f3e1a-0cb3-7000-8000-000000000001', 'default', 'ticket.sla.breached', '*', 'text',
 '工单超时：{{.Title}}',
 '工单的{{if eq .Payload.kind "response"}}响应{{else}}解决{{end}}时限已过，请尽快处理。
工单号：{{.Payload.number}}
标题：{{.Payload.title}}')
ON CONFLICT (biz_id, event_type, channel_type) WHERE delete_time IS NULL DO NOTHING;

-- 配置版本 +1：通知管道据此刷新路由与模板缓存。
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
