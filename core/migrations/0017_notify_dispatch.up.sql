-- 0017 notify 域：发送任务表（运行表，管道写 / core 读，设计 §5.2）
-- 语义：一行 = 路由命中后生成的一个发送任务（事件 × 渠道 × 收件人）；
-- core 控制台经 ListDispatches 只读查询（含重试/死信状态）。core 对本表
-- 无写路径（§5.3：管道角色 INSERT/UPDATE，core 角色仅 SELECT）。
-- 分区：按月 RANGE(create_time)，本迁移预建当月与未来 2 个月分区（§5.4
-- 「预建未来 2 个月」）；常态化分区维护 job 属管道服务 P1，落地前人工补建。
-- 注意：分区表唯一约束须含分区键，故 PK 为 (id, create_time)；幂等靠事件
-- 状态门（router）+ 行条件更新（sender）+ 对外幂等键 send_key（§7.2）。
CREATE TABLE notify_dispatch (
    id              uuid        NOT NULL,           -- UUIDv7（时间有序，B-tree 右侧追加）
    event_id        uuid        NOT NULL,           -- 源事件（notify_event.id）
    channel_id      uuid        NOT NULL,           -- 目标渠道实例（notify_channel.id）
    recipient       varchar(255) NOT NULL,          -- 邮箱/群标识/webhook url/用户ID
    template_id     uuid,                           -- 渲染用模板版本（可空=无模板）
    content         jsonb,                          -- 渲染快照（排障用，可空=未渲染）
    status          smallint    NOT NULL DEFAULT 1, -- 1 pending / 2 sending / 3 sent / 4 failed / 5 dead
    attempts        int         NOT NULL DEFAULT 0, -- 已尝试次数（失败重试累加）
    next_retry_time timestamptz,                    -- 下次重试时间（退避后；可空=无待重试）
    last_error      varchar(512),                   -- 最近一次失败原因（可空=未失败）
    send_key        varchar(32) NOT NULL,           -- 对外幂等键 sha256(event|channel|recipient) 截断
    create_time     timestamptz NOT NULL,
    PRIMARY KEY (id, create_time)
) PARTITION BY RANGE (create_time);

COMMENT ON TABLE notify_dispatch IS '通知发送任务表（运行表）：管道写 / core 只读；按月 RANGE 分区，PK 含分区键';
COMMENT ON COLUMN notify_dispatch.id IS '任务唯一标识（UUIDv7）';
COMMENT ON COLUMN notify_dispatch.event_id IS '源事件标识（notify_event.id，查询下钻维度）';
COMMENT ON COLUMN notify_dispatch.channel_id IS '目标渠道实例标识（notify_channel.id）';
COMMENT ON COLUMN notify_dispatch.recipient IS '收件目标：邮箱/群标识/webhook url/用户ID';
COMMENT ON COLUMN notify_dispatch.template_id IS '渲染用模板版本（可空=无模板纯文本）';
COMMENT ON COLUMN notify_dispatch.content IS '渲染快照 jsonb（排障复现用；可空=未渲染）';
COMMENT ON COLUMN notify_dispatch.status IS '任务状态：1 pending / 2 sending / 3 sent / 4 failed（退避重试中）/ 5 dead（重试超限转死信）';
COMMENT ON COLUMN notify_dispatch.attempts IS '已尝试次数（失败重试累加，上限 5 次重试）';
COMMENT ON COLUMN notify_dispatch.next_retry_time IS '下次重试时间（指数退避后；可空=无待重试）';
COMMENT ON COLUMN notify_dispatch.last_error IS '最近一次失败原因（可空=未失败）';
COMMENT ON COLUMN notify_dispatch.send_key IS '对外幂等键 sha256(event|channel|recipient) 截断；接收端去重（如 Webhook 签名头）';
COMMENT ON COLUMN notify_dispatch.create_time IS '生成时间（分区键）';

-- sender 重试扫描部分索引：仅 pending/failed 行，大小与历史总量无关（§5.2）
CREATE INDEX idx_notify_dispatch_retry ON notify_dispatch (next_retry_time) WHERE status IN (1, 4);
-- 控制台查询索引：事件下钻（event → 其全部投递任务）是排障主路径
CREATE INDEX idx_notify_dispatch_query ON notify_dispatch (event_id, create_time DESC);

-- 预建分区：当月 + 未来 2 个月（§5.4；后续由管道 P1 维护 job 接管）
CREATE TABLE notify_dispatch_2026_11 PARTITION OF notify_dispatch
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE notify_dispatch_2026_12 PARTITION OF notify_dispatch
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');
CREATE TABLE notify_dispatch_2027_01 PARTITION OF notify_dispatch
    FOR VALUES FROM ('2027-01-01') TO ('2027-02-01');
