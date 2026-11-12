-- 0038 NATS 骨干（可切换，缺省仍 pg）：发送任务的发布游标。
-- pg 骨干下恒 0 且无人读取；nats 骨干下 relay 把 0 → 1（已发布到 JetStream），
-- 消费侧仍以 status/next_retry_time 条件 UPDATE 做门闩与围栏（PG 为事实源）。
ALTER TABLE notify_dispatch ADD COLUMN IF NOT EXISTS publish_state smallint NOT NULL DEFAULT 0;
COMMENT ON COLUMN notify_dispatch.publish_state IS '0 未发布 / 1 已发布到消息骨干（仅 backbone=nats 使用；pg 骨干恒 0）';
CREATE INDEX IF NOT EXISTS idx_notify_dispatch_unpublished ON notify_dispatch (next_retry_time) WHERE publish_state = 0 AND status IN (1, 4);
