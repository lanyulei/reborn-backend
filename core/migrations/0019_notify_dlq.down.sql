-- 0019 回滚：删除死信表（运行数据不保留）。
DROP TABLE IF EXISTS notify_dlq CASCADE;
