-- 0012_notify_route.down.sql 回滚 0012：删除通知路由表与配置版本行。

DELETE FROM notify_cfg_version WHERE table_name = 'notify_route';
DROP TABLE IF EXISTS notify_route;
