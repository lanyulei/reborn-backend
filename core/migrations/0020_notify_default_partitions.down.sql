-- 回滚 0020：移除 DEFAULT 分区（其中若有数据将随之丢弃，回滚前先迁移）。
DROP TABLE IF EXISTS notify_delivery_log_default;
DROP TABLE IF EXISTS notify_dispatch_default;
DROP TABLE IF EXISTS notify_event_default;
