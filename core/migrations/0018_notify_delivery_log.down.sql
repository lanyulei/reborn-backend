-- 0018 回滚：删除投递日志表（分区随父表级联删除；运行数据不保留）。
DROP TABLE IF EXISTS notify_delivery_log CASCADE;
