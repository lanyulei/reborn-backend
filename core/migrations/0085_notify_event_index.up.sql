-- 0085 运行表查询索引补充。
-- notify_event：不带 biz_id 的事件列表（管理员全局视图）按 (create_time DESC, id DESC) 排序分页，此前只有
-- (biz_id, create_time) 索引可用，全局视图退化为整表排序。分区表上的索引：父表建索引会在各分区上同步建出。
-- monitor_alert：事件列表关键字只匹配规则名称（rule_name ILIKE），pg_trgm（0047 已启用）三元组索引供其预筛。

CREATE INDEX IF NOT EXISTS idx_notify_event_time ON notify_event (create_time DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_monitor_alert_rule_name_trgm ON monitor_alert USING gin (rule_name gin_trgm_ops);
