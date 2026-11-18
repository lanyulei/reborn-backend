-- 0076 down：撤销监控告警域第二、三期的表、列、约束、授权与内置数据，回到 0075 之后的结构。
-- 第二期才有的数据（升级通知、时间线、认领、关联工单、资源索引等）随之删除；第一期的数据不受影响。

-- 内置的通知路由与模板：只删除本迁移写入的行（固定 ID）。
DELETE FROM notify_route WHERE id = '018f3e1a-0cc1-7000-8000-000000000003';
DELETE FROM notify_template WHERE id IN (
    '018f3e1a-0cc2-7000-8000-000000000005', '018f3e1a-0cc2-7000-8000-000000000006',
    '018f3e1a-0cc2-7000-8000-000000000007', '018f3e1a-0cc2-7000-8000-000000000008',
    '018f3e1a-0cc2-7000-8000-000000000009');
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');

DELETE FROM monitor_cfg_version WHERE name IN ('monitor_escalation', 'monitor_inhibit', 'monitor_ci_index', 'monitor_calendar');
COMMENT ON COLUMN monitor_cfg_version.name IS '配置表名：monitor_datasource / monitor_mute / monitor_rule_group';

-- 新表（表级授权随表删除）。
DROP TABLE IF EXISTS monitor_engine_event;
DROP TABLE IF EXISTS monitor_engine_instance;
DROP TABLE IF EXISTS monitor_record_task;
DROP TABLE IF EXISTS monitor_alert_action;
DROP TABLE IF EXISTS monitor_alert_escalation;
DROP TABLE IF EXISTS monitor_alert_cmd;
DROP TABLE IF EXISTS monitor_alert_event;
DROP TABLE IF EXISTS monitor_calendar;
DROP TABLE IF EXISTS monitor_ci_index;
DROP TABLE IF EXISTS monitor_alert_view;
DROP TABLE IF EXISTS monitor_record_rule;
DROP TABLE IF EXISTS monitor_inhibit;
DROP TABLE IF EXISTS monitor_escalation;

-- 通知记录与发件箱：先删掉第一期的约束容不下的升级通知行，再恢复原约束。
DROP INDEX IF EXISTS idx_monitor_notify_log_time;
DELETE FROM monitor_notify_log WHERE kind = 4 OR source = 3;
ALTER TABLE monitor_notify_log
    DROP CONSTRAINT IF EXISTS ck_monitor_notify_log_escalation,
    DROP CONSTRAINT IF EXISTS ck_monitor_notify_log_level,
    DROP CONSTRAINT IF EXISTS ck_monitor_notify_log_source,
    DROP CONSTRAINT IF EXISTS ck_monitor_notify_log_kind,
    DROP COLUMN IF EXISTS level,
    ADD CONSTRAINT ck_monitor_notify_log_kind CHECK (kind IN (1, 2, 3)),
    ADD CONSTRAINT ck_monitor_notify_log_source CHECK (source IN (1, 2));
COMMENT ON COLUMN monitor_notify_log.kind IS '通知类型：1=触发 2=重复 3=恢复';
COMMENT ON COLUMN monitor_notify_log.seq IS '该事件的第几次通知';
COMMENT ON COLUMN monitor_notify_log.source IS '来源：1=规则的通知对象 2=订阅规则';

DELETE FROM monitor_notify_outbox WHERE kind = 4;
ALTER TABLE monitor_notify_outbox
    DROP CONSTRAINT IF EXISTS ck_monitor_notify_outbox_level,
    DROP CONSTRAINT IF EXISTS ck_monitor_notify_outbox_kind,
    DROP COLUMN IF EXISTS level,
    ADD CONSTRAINT ck_monitor_notify_outbox_kind CHECK (kind IN (1, 2, 3));
COMMENT ON COLUMN monitor_notify_outbox.kind IS '通知类型：1=触发 2=重复 3=恢复';
COMMENT ON COLUMN monitor_notify_outbox.seq IS '该事件的第几次通知（触发为 1，重复通知自 2 递增；恢复通知取恢复时已发出的次数 + 1）';

ALTER TABLE monitor_eval_task
    DROP COLUMN IF EXISTS error_count,
    DROP COLUMN IF EXISTS error_since;

-- 告警事件：索引与约束随列删除；列级授权随列删除。
ALTER TABLE monitor_alert
    DROP COLUMN IF EXISTS ai_summary,
    DROP COLUMN IF EXISTS workflow_instance_id,
    DROP COLUMN IF EXISTS ticket_id,
    DROP COLUMN IF EXISTS escalation_level,
    DROP COLUMN IF EXISTS ci_id,
    DROP COLUMN IF EXISTS inhibit_alert_id,
    DROP COLUMN IF EXISTS inhibit_id,
    DROP COLUMN IF EXISTS inhibit_kind,
    DROP COLUMN IF EXISTS inhibited,
    DROP COLUMN IF EXISTS close_reason,
    DROP COLUMN IF EXISTS closed_by,
    DROP COLUMN IF EXISTS ack_time,
    DROP COLUMN IF EXISTS ack_user;
DROP INDEX IF EXISTS idx_monitor_alert_group_fire;
COMMENT ON TABLE monitor_alert IS '告警事件：一次从触发到恢复（或关闭）的告警；引擎写，core 读并回写通知计数。同一指纹至多一个活跃事件';
COMMENT ON COLUMN monitor_alert.status IS '状态：1=告警中 2=已恢复 3=已关闭（规则被删除 / 停用、数据源移出范围时一并关闭，不发恢复通知）';
COMMENT ON COLUMN monitor_alert.labels IS '事件标签 {名称: 值}：序列标签（去掉 __name__）与规则附加标签合并';
COMMENT ON COLUMN monitor_alert.muted IS '是否被「只屏蔽通知」的屏蔽规则命中（不发通知）';

ALTER TABLE monitor_datasource DROP COLUMN IF EXISTS health_since;
COMMENT ON COLUMN monitor_datasource.remote_write_url IS '远程写入地址；仅记录，当前不使用';

-- 规则：第二期的配置随列删除。生效时间里的 mode 键与通知对象里的 ci_owner 键一并去掉——
-- 第一期的引擎不认识它们，留着会把「仅工作时间」的规则当成全时段生效而不自知。
ALTER TABLE monitor_rule
    DROP COLUMN IF EXISTS actions,
    DROP COLUMN IF EXISTS escalation_id,
    DROP COLUMN IF EXISTS topology_inhibit,
    DROP COLUMN IF EXISTS drop_expr,
    DROP COLUMN IF EXISTS relabel,
    DROP COLUMN IF EXISTS resource_scope,
    DROP COLUMN IF EXISTS ci_match;
UPDATE monitor_rule SET effective = effective - 'mode' WHERE effective ? 'mode';
UPDATE monitor_rule SET notify = notify - 'ci_owner' WHERE notify ? 'ci_owner';
COMMENT ON COLUMN monitor_rule.effective IS '生效时间 {"timezone":"Asia/Shanghai","windows":[{"days":[1,2,3,4,5],"start":"09:00","end":"18:00"}]}；days 1-7=周一至周日，start>end 表示跨午夜，windows 为空=全时段';
COMMENT ON COLUMN monitor_rule.notify IS '通知对象 {"user_ids":[],"schedule_ids":[],"dept_ids":[]}：用户、值班表当前值班人、部门负责人';
