-- 0007_permission_restore.down.sql 回滚 0007：再次删除权限两张表。

DROP TABLE IF EXISTS system_grant;
DROP TABLE IF EXISTS system_permission;
