-- 0088 回滚：去掉本迁移补的三个索引。

DROP INDEX IF EXISTS idx_notify_subscription_biz;
DROP INDEX IF EXISTS idx_notify_delivery_time;
DROP INDEX IF EXISTS idx_notify_dispatch_time;
