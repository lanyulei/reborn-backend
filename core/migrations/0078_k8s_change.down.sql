-- 0078 down：删除变更单与平台设置、内置的通知模板。
DELETE FROM notify_template WHERE id IN ('018f3e1a-0cd4-7000-8000-000000000001', '018f3e1a-0cd4-7000-8000-000000000002');
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
DROP TABLE IF EXISTS k8s_setting;
DROP TABLE IF EXISTS k8s_change;
