-- 0069 down：删除监控告警域的全部表、授权与内置的通知路由 / 模板。
-- 角色 monitor_engine_role / monitor_core_role 保留：角色是集群级对象，可能被其它库引用（同 0021 / 0036）。

-- 告警通知的缺省路由与模板：只删除本迁移写入的行（固定 ID），不动用户自建的配置。
DELETE FROM notify_route WHERE id IN ('018f3e1a-0cc1-7000-8000-000000000001', '018f3e1a-0cc1-7000-8000-000000000002');
DELETE FROM notify_template WHERE id IN (
    '018f3e1a-0cc2-7000-8000-000000000001', '018f3e1a-0cc2-7000-8000-000000000002',
    '018f3e1a-0cc2-7000-8000-000000000003', '018f3e1a-0cc2-7000-8000-000000000004');
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');

-- schema 级授权不随表删除而消失，须显式收回；表级授权随 DROP TABLE 一并清除。
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'monitor_engine_role') THEN
        REVOKE CREATE ON SCHEMA public FROM monitor_engine_role;
    END IF;
END
$$;

-- 判定记录连同全部分区（含引擎后续建出的分区）一并删除。
DROP TABLE IF EXISTS monitor_eval_record;
DROP TABLE IF EXISTS monitor_notify_log;
DROP TABLE IF EXISTS monitor_notify_outbox;
DROP TABLE IF EXISTS monitor_alert;
DROP TABLE IF EXISTS monitor_eval_state;
DROP TABLE IF EXISTS monitor_eval_task;
DROP TABLE IF EXISTS monitor_cfg_version;
DROP TABLE IF EXISTS monitor_subscription;
DROP TABLE IF EXISTS monitor_mute;
DROP TABLE IF EXISTS monitor_rule_version;
DROP TABLE IF EXISTS monitor_rule;
DROP TABLE IF EXISTS monitor_role_scope;
DROP TABLE IF EXISTS monitor_rule_group;
DROP TABLE IF EXISTS monitor_datasource;
