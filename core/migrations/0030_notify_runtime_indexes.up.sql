-- 0030 运行表控制台查询索引（通知优化第二批）。
-- 受限用户（角色 biz_scope）查任务/明细/死信必须按 channel_id 过滤，而原索引只覆盖
-- event_id / dispatch_id：WHERE channel_id = $1 ORDER BY create_time DESC 是全分区扫描。
-- 事件按状态排障（失败/抑制）不带 biz 时同样无索引可用；部分索引只收录两种非正常终态。
-- 分区父表上建索引会自动在全部分区（含 DEFAULT 与后续 ATTACH 的分区）落地。
CREATE INDEX IF NOT EXISTS idx_notify_dispatch_channel ON notify_dispatch (channel_id, create_time DESC);
CREATE INDEX IF NOT EXISTS idx_notify_delivery_channel ON notify_delivery_log (channel_id, create_time DESC);
CREATE INDEX IF NOT EXISTS idx_notify_dlq_channel ON notify_dlq (channel_id, create_time DESC);
CREATE INDEX IF NOT EXISTS idx_notify_event_status ON notify_event (status, create_time DESC) WHERE status IN (3, 4);
