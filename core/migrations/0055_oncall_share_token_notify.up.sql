-- 0055 值班域补充：公共地址可再次查看 + 值班通知的缺省路由与模板。
-- 单独成迁移而不是改 0054：0054 已在环境里执行过，改已执行的迁移不会生效。

-- 公共地址令牌密文：供有管理权的人再次查看地址。本迁移之前创建的地址只有哈希、无法还原，
-- 该列为 NULL——匿名访问不受影响，"查看地址"提示重置后可用。
ALTER TABLE oncall_share_link ADD COLUMN token_sealed bytea NULL;
COMMENT ON COLUMN oncall_share_link.token_sealed IS '访问令牌密文（AES-256-GCM，nonce 前缀拼接，主密钥同通知渠道凭据）：供有管理权的人再次查看地址；明文不落库。NULL = 本列引入前创建的地址，重置后写入';

-- ---------------------------------------------------------------------------
-- 值班通知的缺省配置（缺省业务命名空间 default，即界面上「业务组」留空）：两条路由 + 五个模板。
-- 与用户在通知管理里不指定业务组时创建的渠道同属一个命名空间，路由可以直接选到它们。
-- 值班事件由 core 在业务事务内直插 notify_event，没有匹配路由时事件只落库不投递。
--
-- 渠道无法由迁移创建（邮件服务器凭据、群机器人地址属部署环境，密文落库），这里只
-- 绑定迁移执行时 default 命名空间下已启用的渠道：个人通知绑定邮件渠道，每日播报绑定
-- 群机器人渠道。执行时没有对应渠道则路由的渠道为空（不投递），之后在
-- 「通知管理 → 规则」里为这两条路由选择渠道即可。

-- 个人通知：上班提醒 / 替班变更 / 值班表更新 / 排班异常。事件自带收件人（用户 ID，
-- 管道解析为邮箱）；subscription=true 使未带收件人的事件退回订阅者，同时满足
-- "邮件渠道须有收件策略"的校验。
INSERT INTO notify_route (id, biz_id, name, description, event_pattern, severity_min, priority, channel_ids, recipient_policy)
VALUES ('018f3e1a-0ca1-7000-8000-000000000001', 'default', '值班通知',
        '上班提醒、替班变更、值班表更新、排班异常：发给事件指定的值班人 / 分组管理员', 'oncall.*', 1, 5,
        ARRAY(SELECT id FROM notify_channel
              WHERE biz_id = 'default' AND type = 'email' AND enabled AND delete_time IS NULL ORDER BY create_time),
        '{"subscription": true}');

-- 每日播报：发到群里。优先级高于上面的通配路由（数值小者优先），否则会被 oncall.* 截走。
INSERT INTO notify_route (id, biz_id, name, description, event_pattern, severity_min, priority, channel_ids, recipient_policy)
VALUES ('018f3e1a-0ca1-7000-8000-000000000002', 'default', '值班每日播报',
        '按值班表设置的播报时间，把当日值班人发到群机器人', 'oncall.daily.digest', 1, 4,
        ARRAY(SELECT id FROM notify_channel
              WHERE biz_id = 'default' AND type IN ('feishu', 'dingtalk', 'wecom') AND enabled AND delete_time IS NULL ORDER BY create_time),
        '{}');

-- 模板：纯文本、不限渠道类型（邮件与群机器人都能直接展示）；同一事件已有模板则保留现有的。
INSERT INTO notify_template (id, biz_id, event_type, channel_type, format, subject, body) VALUES
('018f3e1a-0ca2-7000-8000-000000000001', 'default', 'oncall.shift.remind', '*', 'text',
 '值班提醒：{{.Payload.schedule_name}}（{{.Payload.role}}）',
 '{{if .Payload.started}}你的值班已经开始。{{else}}你的值班即将开始。{{end}}
值班表：{{.Payload.schedule_name}}
角色：{{.Payload.role}}
值班时间：{{.Payload.start_time}} ~ {{.Payload.end_time}}（{{.Payload.timezone}}）'),
('018f3e1a-0ca2-7000-8000-000000000002', 'default', 'oncall.shift.changed', '*', 'text',
 '{{if .Payload.revoked}}替班已撤销{{else}}值班替班{{end}}：{{.Payload.schedule_name}}（{{.Payload.role}}）',
 '{{if .Payload.revoked}}以下时间段的替班已撤销，恢复为原排班。{{else}}以下时间段的值班人已调整。{{end}}
值班表：{{.Payload.schedule_name}}
角色：{{.Payload.role}}
时间：{{.Payload.start_time}} ~ {{.Payload.end_time}}（{{.Payload.timezone}}）{{if .Payload.remark}}
原因：{{.Payload.remark}}{{end}}
请到值班日历查看最新排班。'),
('018f3e1a-0ca2-7000-8000-000000000003', 'default', 'oncall.schedule.published', '*', 'text',
 '值班表已更新：{{.Payload.schedule_name}}',
 '值班表「{{.Payload.schedule_name}}」的排班已更新，请到值班日历确认你的值班时间。'),
('018f3e1a-0ca2-7000-8000-000000000004', 'default', 'oncall.daily.digest', '*', 'text',
 '今日值班：{{.Payload.schedule_name}}',
 '{{.Payload.date}} {{.Payload.schedule_name}}
主值班：{{if .Payload.primary}}{{.Payload.primary}}{{else}}无人值班{{end}}{{if .Payload.backup}}
备值班：{{.Payload.backup}}{{end}}{{if .Payload.handover_time}}
下次交接：{{.Payload.handover_time}}（{{.Payload.timezone}}）{{end}}'),
('018f3e1a-0ca2-7000-8000-000000000005', 'default', 'oncall.issue.detected', '*', 'text',
 '值班排班异常：未来 7 天发现 {{.Payload.count}} 项',
 '未来 7 天的排班存在 {{.Payload.count}} 项异常，请到值班日历处理：
{{range .Payload.issues}}- {{.type}}｜{{.schedule_name}}{{if .user_name}}｜{{.user_name}}{{end}}{{if .other_schedule_name}}｜与「{{.other_schedule_name}}」冲突{{end}}｜{{.start_time}} ~ {{.end_time}}（{{.timezone}}）
{{end}}')
ON CONFLICT (biz_id, event_type, channel_type) WHERE delete_time IS NULL DO NOTHING;

-- 配置版本 +1：通知管道据此刷新路由与模板缓存。
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
