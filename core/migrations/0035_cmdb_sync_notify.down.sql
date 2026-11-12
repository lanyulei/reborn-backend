-- 0035 回滚：收回数据面对通知事件表的写权限。
REVOKE INSERT ON notify_event FROM cmdb_sync_role;
REVOKE SELECT, INSERT, UPDATE ON notify_dedup FROM cmdb_sync_role;
