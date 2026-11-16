-- 0064 down
-- SLA 通知的缺省路由与模板：只删除本迁移写入的行（固定 ID），不动用户自建的配置。
DELETE FROM notify_route WHERE id = '018f3e1a-0cb1-7000-8000-000000000002';
DELETE FROM notify_template WHERE id = '018f3e1a-0cb3-7000-8000-000000000001';
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
DROP INDEX IF EXISTS idx_ticket_order_priority;
