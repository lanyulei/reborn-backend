-- 0035 CMDB 数据面写通知事件：同步失败 / 资源消失 / 陈旧 直插 notify_event（与 core 内部事件同一路径），
-- 去重门闩共用 notify_dedup。
GRANT INSERT ON notify_event TO cmdb_sync_role;
GRANT SELECT, INSERT, UPDATE ON notify_dedup TO cmdb_sync_role;
