-- 0009_login_log.down.sql 回滚 0009：删除登录日志表（索引随表删除）。

DROP TABLE IF EXISTS system_login_log;
