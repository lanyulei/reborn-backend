-- 回滚 0023。
REVOKE SELECT, INSERT, UPDATE ON notify_dedup FROM notify_core_role;
DROP TABLE IF EXISTS notify_channel_health;
DROP TABLE IF EXISTS notify_quota_counter;
ALTER TABLE notify_channel DROP COLUMN IF EXISTS sign_secret;
ALTER TABLE notify_producer DROP COLUMN IF EXISTS qps_limit, DROP COLUMN IF EXISTS daily_limit;
ALTER TABLE system_role DROP COLUMN IF EXISTS biz_scope;
