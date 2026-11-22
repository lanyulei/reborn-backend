-- 0084 故障管理域（incident）关联器已处理序号：回看窗口（游标之前 feedLookback 个序号）里的告警事件
-- 此前每轮都整段重放，只靠「告警已属于活跃故障」幂等——解除关联的告警会被重新归并、已关闭的故障被重开、
-- 已忽略的候选被重建、候选 update_time 被不断刷新而永不过期。记下已处理的 seq，回看只补「取号早、提交晚」的漏网事件。

CREATE TABLE incident_feed_seen (
    seq         bigint      PRIMARY KEY,
    create_time timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE incident_feed_seen IS '关联器已处理的 monitor_alert_event.seq（回看窗口去重；低于游标 - 回看窗口的行由关联器每轮清理）';
