-- 0016 回滚：删除事件主表（分区随父表级联删除；运行数据不保留）。
DROP TABLE IF EXISTS notify_event CASCADE;
