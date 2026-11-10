-- 0011_notify_template.down.sql 回滚：删除模板表与配置版本行。
DROP TABLE IF EXISTS notify_template;
DELETE FROM notify_cfg_version WHERE table_name = 'notify_template';
