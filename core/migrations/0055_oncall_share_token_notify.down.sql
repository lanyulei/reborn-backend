-- 0055 down
-- 值班通知的缺省路由与模板：只删除本迁移写入的行（固定 ID），不动用户自建的配置。
DELETE FROM notify_route WHERE id IN ('018f3e1a-0ca1-7000-8000-000000000001', '018f3e1a-0ca1-7000-8000-000000000002');
DELETE FROM notify_template WHERE id::text LIKE '018f3e1a-0ca2-7000-8000-00000000000_';
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
ALTER TABLE oncall_share_link DROP COLUMN IF EXISTS token_sealed;
