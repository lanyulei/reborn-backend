-- 0013_notify_silence.down.sql 回滚 0013：删除静默规则表与配置版本行。

DELETE FROM notify_cfg_version WHERE table_name = 'notify_silence';
DROP TABLE IF EXISTS notify_silence;
