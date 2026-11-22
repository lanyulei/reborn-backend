-- 0080 down：撤销故障管理域的全部表与内置数据，并撤销对监控域的两处改动。

DELETE FROM notify_route WHERE id = '018f3e1a-0cc1-7000-8000-000000000004';
DELETE FROM notify_template WHERE biz_id = 'default' AND event_type LIKE 'incident.%' AND id::text LIKE '018f3e1a-0cc4-7000-8000-%';
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');

DROP TABLE IF EXISTS incident_cursor;
DROP TABLE IF EXISTS incident_action;
DROP TABLE IF EXISTS incident_outbox;
DROP TABLE IF EXISTS incident_webhook;
DROP TABLE IF EXISTS incident_source;
DROP TABLE IF EXISTS incident_action_item;
DROP TABLE IF EXISTS incident_postmortem_version;
DROP TABLE IF EXISTS incident_postmortem;
DROP TABLE IF EXISTS incident_share;
DROP TABLE IF EXISTS incident_update;
DROP TABLE IF EXISTS incident_proposal;
DROP TABLE IF EXISTS incident_playbook;
DROP TABLE IF EXISTS incident_rule;
DROP TABLE IF EXISTS incident_candidate;
DROP TABLE IF EXISTS incident_change_ref;
DROP TABLE IF EXISTS incident_alert;
DROP TABLE IF EXISTS incident_event;
DROP TABLE IF EXISTS incident_member;
DROP TABLE IF EXISTS incident_record;
DROP TABLE IF EXISTS incident_counter;
DROP TABLE IF EXISTS incident_business;
DROP TABLE IF EXISTS incident_setting;

-- 监控域：先删掉故障域写入的时间线行，再恢复原约束与列。
DELETE FROM monitor_alert_event WHERE type = 'incident';
ALTER TABLE monitor_alert_event DROP CONSTRAINT ck_monitor_alert_event_type;
ALTER TABLE monitor_alert_event ADD CONSTRAINT ck_monitor_alert_event_type CHECK (type IN (
    'fire', 'resolve', 'close', 'mute', 'unmute', 'inhibit', 'uninhibit', 'escalate', 'notify',
    'ack', 'unack', 'transfer', 'comment', 'ticket', 'ticket_note', 'workflow', 'ai_summary', 'action_failed'));
COMMENT ON COLUMN monitor_alert_event.type IS '类型：fire 触发 / resolve 恢复 / close 关闭 / mute 被屏蔽 / unmute 屏蔽解除 / inhibit 被抑制 / uninhibit 抑制解除 / escalate 升级 / notify 通知说明 / ack 认领 / unack 取消认领 / transfer 转交 / comment 备注 / ticket 转工单 / ticket_note 工单追加记录 / workflow 触发流程 / ai_summary AI 摘要 / action_failed 事件动作失败';
DROP INDEX IF EXISTS idx_monitor_alert_event_seq;
ALTER TABLE monitor_alert_event DROP COLUMN seq;
