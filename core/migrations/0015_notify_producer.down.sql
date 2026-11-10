-- 0015 回滚：删除生产者凭据表与其配置版本行。
DELETE FROM notify_cfg_version WHERE table_name = 'notify_producer';
DROP TABLE IF EXISTS notify_producer;
