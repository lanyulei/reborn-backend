-- 0048 通知接入防重放登记（跨副本共享）。
-- 进程内防重放表只覆盖单个副本；多副本部署时同一已签名请求打到另一副本需靠本表识别。
-- (producer_id, signature) 主键冲突即重放；行保留到签名时间戳滑出时间窗（±5min），由
-- 数据面维护循环按 expire_time 清理。高频写入、无需崩溃后保留，可接受 UNLOGGED 的取舍
-- 但为简单与主从一致保持普通表。
CREATE TABLE IF NOT EXISTS notify_ingress_replay (
    producer_id uuid        NOT NULL,
    signature   text        NOT NULL,
    expire_time timestamptz NOT NULL,
    PRIMARY KEY (producer_id, signature)
);
COMMENT ON TABLE notify_ingress_replay IS '通知接入防重放登记：时间窗内已受理的 (生产者, 请求签名)；跨副本共享';
CREATE INDEX IF NOT EXISTS idx_notify_ingress_replay_expire ON notify_ingress_replay (expire_time);
