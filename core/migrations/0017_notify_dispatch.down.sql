-- 0017 回滚：删除发送任务表（分区随父表级联删除；运行数据不保留）。
DROP TABLE IF EXISTS notify_dispatch CASCADE;
