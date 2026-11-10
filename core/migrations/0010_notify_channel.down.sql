-- 0010_notify_channel.down.sql 回滚 notify 域：通知渠道表 + 配置版本表。
-- 注意：secret 密文随表删除不可恢复，回滚前请确认无在用渠道。

DROP TABLE IF EXISTS notify_cfg_version;
DROP TABLE IF EXISTS notify_channel;
