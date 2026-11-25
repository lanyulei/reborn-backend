-- 0088 通知运行表 / 订阅表查询索引补充（只加索引，不改表结构）。
--
-- 1) notify_dispatch / notify_delivery_log：不带渠道 / 事件过滤的全局列表（管理员视图）按
--    (create_time DESC, id DESC) 排序分页，此前只有 (event_id / channel_id / dispatch_id, create_time) 索引，
--    每一页都是时间窗内的 top-N 排序。与 0085 给 notify_event 补的 idx_notify_event_time 同一口径；
--    分区表上父表建索引会在各分区上同步建出，之后新挂的分区自动继承。
-- 2) notify_subscription：管道每个路由批次按 (biz_id, event_type) 解析订阅者，现有唯一索引以 user_id 打头，
--    biz_id 用不上首列，每次都是顺序扫描。补 (biz_id, event_pattern) 的部分索引（只含未删除行）。
--
-- 大表上建索引期间会短暂阻塞对应表的写入（非 CONCURRENTLY）：通知任务量很大的环境建议在低峰执行迁移。
-- 幂等：IF NOT EXISTS。

CREATE INDEX IF NOT EXISTS idx_notify_dispatch_time ON notify_dispatch (create_time DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_notify_delivery_time ON notify_delivery_log (create_time DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_notify_subscription_biz ON notify_subscription (biz_id, event_pattern) WHERE delete_time IS NULL;
