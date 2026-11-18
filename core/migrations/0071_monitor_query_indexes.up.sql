-- 0071 监控告警域的查询索引（评审第一轮 C10）。只加索引，不改表结构与授权。

-- 告警事件按首次触发时间的范围查询（告警列表带时间范围时 first_fire_time BETWEEN …，跨规则）：
-- 原有的 (rule_id, first_fire_time DESC) 只在限定了规则时可用。
CREATE INDEX idx_monitor_alert_first_fire ON monitor_alert (first_fire_time DESC);

-- 订阅规则按订阅分组查找：通知派发的候选预筛、订阅列表的范围过滤（group_ids && …），以及删除分组前
-- 检查是否仍被订阅引用（group_ids @> …）。
CREATE INDEX idx_monitor_subscription_groups ON monitor_subscription USING gin (group_ids) WHERE delete_time IS NULL;
