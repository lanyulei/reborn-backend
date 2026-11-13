-- 0044 通知评审修复：relay 超期回拨扫描的部分索引。
-- nats 骨干下 relay 每轮（缺省 200ms）扫描「已发布但仍 received」的事件
-- （publish_state = 1 AND status = 1 AND next_publish_time < now() - 2×AckWait）；
-- 既有 idx_notify_event_pending 只收录 publish_state = 0，该扫描此前是全分区顺序扫描。
-- 部分索引只收录在途事件（路由完成即离开），大小与历史总量无关。
-- 任务侧同类扫描（publish_state = 1 AND status IN (1, 4) AND next_retry_time < …）已由
-- idx_notify_dispatch_retry (next_retry_time) WHERE status IN (1, 4) 覆盖，不另建。
-- 分区父表上建索引会自动在全部分区（含 DEFAULT 与后续 ATTACH 的分区）落地（同 0030）。
CREATE INDEX IF NOT EXISTS idx_notify_event_published ON notify_event (next_publish_time) WHERE publish_state = 1 AND status = 1;
