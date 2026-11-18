-- 0071 down：删除本迁移加的索引。
DROP INDEX IF EXISTS idx_monitor_subscription_groups;
DROP INDEX IF EXISTS idx_monitor_alert_first_fire;
