-- 0018 notify 域：投递日志表（运行表，管道写 / core 读，设计 §5.2）
-- 语义：一行 = 发送任务（dispatch）的一次投递尝试结果（append-only，sender
-- 每次尝试后追加）；core 控制台经 ListDeliveryLogs 只读查询。core 对本表
-- 无写路径（§5.3：管道角色 INSERT/UPDATE，core 角色仅 SELECT）。
-- 分区：按月 RANGE(create_time)，本迁移预建当月与未来 2 个月分区（§5.4
-- 「预建未来 2 个月」）；常态化分区维护 job 属管道服务 P1，落地前人工补建。
-- 注意：分区表唯一约束须含分区键，故 PK 为 (id, create_time)；日志不更新
-- 不删除（清理走 DROP PARTITION，保留期 90 天，§5.4）。
CREATE TABLE notify_delivery_log (
    id              uuid        NOT NULL,           -- UUIDv7（时间有序，B-tree 右侧追加）
    dispatch_id     uuid        NOT NULL,           -- 所属发送任务（notify_dispatch.id）
    channel_id      uuid        NOT NULL,           -- 投递渠道实例（notify_channel.id）
    attempt         int         NOT NULL,           -- 第几次尝试（从 1 起，与 dispatch.attempts 对齐）
    ok              boolean     NOT NULL,           -- 本次尝试是否成功
    latency_ms      int         NOT NULL,           -- 本次耗时（毫秒）
    provider_msg_id varchar(128),                   -- 渠道侧消息标识（可空=渠道未返回/失败）
    error           varchar(512),                   -- 本次失败原因（可空=成功）
    create_time     timestamptz NOT NULL,
    PRIMARY KEY (id, create_time)
) PARTITION BY RANGE (create_time);

COMMENT ON TABLE notify_delivery_log IS '投递日志表（运行表）：管道写 / core 只读；append-only，按月 RANGE 分区，PK 含分区键';
COMMENT ON COLUMN notify_delivery_log.id IS '日志唯一标识（UUIDv7）';
COMMENT ON COLUMN notify_delivery_log.dispatch_id IS '所属发送任务标识（notify_dispatch.id，查询下钻维度）';
COMMENT ON COLUMN notify_delivery_log.channel_id IS '投递渠道实例标识（notify_channel.id）';
COMMENT ON COLUMN notify_delivery_log.attempt IS '第几次尝试（从 1 起，与 notify_dispatch.attempts 对齐）';
COMMENT ON COLUMN notify_delivery_log.ok IS '本次尝试是否成功';
COMMENT ON COLUMN notify_delivery_log.latency_ms IS '本次投递耗时（毫秒）';
COMMENT ON COLUMN notify_delivery_log.provider_msg_id IS '渠道侧消息标识（如 SMTP 响应/钉钉 messageId；可空=未返回）';
COMMENT ON COLUMN notify_delivery_log.error IS '本次失败原因（可空=成功）';
COMMENT ON COLUMN notify_delivery_log.create_time IS '落库时间（分区键）';

-- 控制台查询索引：任务下钻（dispatch → 其逐次尝试明细）是排障主路径（§5.2）
CREATE INDEX idx_notify_delivery_query ON notify_delivery_log (dispatch_id, create_time DESC);

-- 预建分区：当月 + 未来 2 个月（§5.4；后续由管道 P1 维护 job 接管）
CREATE TABLE notify_delivery_log_2026_11 PARTITION OF notify_delivery_log
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE notify_delivery_log_2026_12 PARTITION OF notify_delivery_log
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');
CREATE TABLE notify_delivery_log_2027_01 PARTITION OF notify_delivery_log
    FOR VALUES FROM ('2027-01-01') TO ('2027-02-01');
