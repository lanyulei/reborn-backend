-- 0014 回滚：删除用户订阅表（无 cfg_version 行可清——订阅不参与版本轮询）。
DROP TABLE IF EXISTS notify_subscription;
