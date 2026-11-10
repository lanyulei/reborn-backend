-- 0016 notify 域：事件主表（运行表，管道写 / core 读，设计 §5.2）
-- 语义：一行 = 一条已落库的事件（先落库后应答，§7.1 不丢闭环第一道防线）；
-- core 控制台经 ListEvents 只读查询（本迁移只建 notify_event——dispatch/
-- delivery_log/dlq 等其余运行表随各自查询功能落地）。
-- 分区：按月 RANGE(create_time)，本迁移预建当月与未来 2 个月分区（§5.4
-- 「预建未来 2 个月」）；常态化分区维护 job（pg_partman 或自研 SQL+分布式锁
-- 选主）属管道服务 P1，落地前人工补建。
-- 注意：分区表唯一约束须含分区键，故 PK 为 (id, create_time)；幂等靠
-- notify_dedup 门闩 + 行状态机（dedup 表随管道 ingress 落地）。
CREATE TABLE notify_event (
    id                uuid        NOT NULL,           -- UUIDv7（时间有序，B-tree 右侧追加）
    biz_id            varchar(64) NOT NULL,
    event_type        varchar(128) NOT NULL,
    severity          smallint    NOT NULL,           -- 1 info / 2 warning / 3 critical / 4 fatal
    title             varchar(255) NOT NULL,
    payload           jsonb,                          -- JSON 载荷（≤32KB）
    dedup_key         varchar(128),                   -- 重复指纹（幂等返回）
    group_key         varchar(128),                   -- 聚合分组（可空=不聚合）
    recipients        jsonb,                          -- 显式收件（可空=交路由）
    source            varchar(16) NOT NULL,           -- api / internal(core 直插)
    status            smallint    NOT NULL DEFAULT 1, -- 1 received / 2 routed / 3 suppressed / 4 failed
    publish_state     smallint    NOT NULL DEFAULT 0, -- 0 pending / 1 published
    publish_attempts  int         NOT NULL DEFAULT 0,
    next_publish_time timestamptz NOT NULL DEFAULT now(),
    create_time       timestamptz NOT NULL,
    PRIMARY KEY (id, create_time)
) PARTITION BY RANGE (create_time);

COMMENT ON TABLE notify_event IS '通知事件主表（运行表）：管道写 / core 只读；按月 RANGE 分区，PK 含分区键';
COMMENT ON COLUMN notify_event.id IS '事件唯一标识（UUIDv7）';
COMMENT ON COLUMN notify_event.biz_id IS '业务租户标识';
COMMENT ON COLUMN notify_event.event_type IS '事件类型：如 monitor.alert.firing / permission.change';
COMMENT ON COLUMN notify_event.severity IS '严重级：1 info / 2 warning / 3 critical / 4 fatal';
COMMENT ON COLUMN notify_event.title IS '事件标题（≤255，渲染变量 Title）';
COMMENT ON COLUMN notify_event.payload IS 'JSON 载荷（≤32KB；模板沙箱白名单内以 Payload.xxx 访问）';
COMMENT ON COLUMN notify_event.dedup_key IS '重复指纹；(biz_id, dedup_key) 由 notify_dedup 门闩去重';
COMMENT ON COLUMN notify_event.group_key IS '聚合分组键；命中聚合路由时同键事件微批合并';
COMMENT ON COLUMN notify_event.recipients IS '显式收件列表（jsonb 数组）；空=走路由解析';
COMMENT ON COLUMN notify_event.source IS '事件来源：api（生产者经 ingress）/ internal（core 同事务直插）';
COMMENT ON COLUMN notify_event.status IS '路由状态：1 received / 2 routed / 3 suppressed（静默命中）/ 4 failed（无匹配路由）';
COMMENT ON COLUMN notify_event.publish_state IS 'NATS 发布状态：0 pending / 1 published（relay 退避推进）';
COMMENT ON COLUMN notify_event.publish_attempts IS 'NATS 发布尝试次数（失败退避累加）';
COMMENT ON COLUMN notify_event.next_publish_time IS '下次发布时间（退避后）';
COMMENT ON COLUMN notify_event.create_time IS '落库时间（分区键）';

-- relay 扫描部分索引：仅 pending 行，大小与历史总量无关（§4.3）
CREATE INDEX idx_notify_event_pending ON notify_event (next_publish_time) WHERE publish_state = 0;
-- 控制台查询索引（§5.2）
CREATE INDEX idx_notify_event_query ON notify_event (biz_id, create_time DESC);

-- 预建分区：当月 + 未来 2 个月（§5.4；后续由管道 P1 维护 job 接管）
CREATE TABLE notify_event_2026_11 PARTITION OF notify_event
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE notify_event_2026_12 PARTITION OF notify_event
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');
CREATE TABLE notify_event_2027_01 PARTITION OF notify_event
    FOR VALUES FROM ('2027-01-01') TO ('2027-02-01');
