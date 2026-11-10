-- 0012_notify_route.up.sql notify 域：通知路由表。
-- 路由 = 事件如何变成通知任务：event_pattern（精确/前缀通配/'*'）+ severity 下限
-- + 标签匹配 + priority 决胜，命中的 channel_ids 并发生成 dispatch（见
-- backend/docs/notify-design.md §4.4/§5.1）。设计即无唯一约束：多条路由可
-- 重叠匹配，router 取最高优先级启用路由；软删后全字段可重建。
-- 每次写操作同事务 bump notify_cfg_version('notify_route')：管道 10s 轮询刷新缓存。

CREATE TABLE notify_route (
    id                  uuid         PRIMARY KEY,
    biz_id              varchar(64)  NOT NULL DEFAULT 'default',
    event_pattern       varchar(128) NOT NULL,
    severity_min        smallint     NOT NULL DEFAULT 1,
    tag_match           jsonb        NULL,
    priority            smallint     NOT NULL DEFAULT 5,
    channel_ids         uuid[]       NOT NULL,
    recipient_policy    jsonb        NOT NULL,
    aggregate_window_s  integer      NOT NULL DEFAULT 0,
    suppress_threshold  integer      NOT NULL DEFAULT 500,
    suppress_duration_m integer      NOT NULL DEFAULT 30,
    enabled             boolean      NOT NULL DEFAULT true,
    create_time         timestamptz  NOT NULL DEFAULT now(),
    update_time         timestamptz  NOT NULL DEFAULT now(),
    delete_time         timestamptz  NULL,
    create_user         uuid         NULL,
    update_user         uuid         NULL,
    delete_user         uuid         NULL
);

COMMENT ON TABLE notify_route IS '通知路由表：事件模式→渠道集合分发策略，软删；多路由可重叠，priority 决胜';
COMMENT ON COLUMN notify_route.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN notify_route.biz_id IS '业务租户标识，默认 default；渠道绑定与匹配按 biz 隔离';
COMMENT ON COLUMN notify_route.event_pattern IS '事件匹配模式：精确（monitor.alert.firing）/前缀通配（monitor.alert.* 等价前缀 monitor.alert.）/全匹配（*）';
COMMENT ON COLUMN notify_route.severity_min IS '严重级下限（1 info/2 warning/3 critical/4 fatal），事件低于下限不命中';
COMMENT ON COLUMN notify_route.tag_match IS 'payload 标签匹配条件（可选）：扁平键值对象，全部相等才命中（AND）';
COMMENT ON COLUMN notify_route.priority IS '优先级 1-9，1 最高；多条路由命中时取最高优先级，sender 侧优先插队';
COMMENT ON COLUMN notify_route.channel_ids IS '命中的渠道实例集合（同 biz 未删除行），每个启用实例并发生成 dispatch';
COMMENT ON COLUMN notify_route.recipient_policy IS '收件策略 {"fixed":[...],"subscription":true}：fixed 固定收件人先行，subscription 订阅者按渠道偏好追加并去重';
COMMENT ON COLUMN notify_route.aggregate_window_s IS '聚合窗口秒数，0=不聚合；>0 且事件带 group_key 时进入聚合微批（30-300）';
COMMENT ON COLUMN notify_route.suppress_threshold IS '聚合抑制阈值：分组计数超过后汇总转摘要模式（默认 500）';
COMMENT ON COLUMN notify_route.suppress_duration_m IS '抑制时长分钟：超阈值后同 group_key 静默窗口（默认 30）';
COMMENT ON COLUMN notify_route.enabled IS '启用状态；停用后不参与匹配（≤10s 生效，配置版本轮询）';
COMMENT ON COLUMN notify_route.create_time IS '创建时间';
COMMENT ON COLUMN notify_route.update_time IS '更新时间';
COMMENT ON COLUMN notify_route.delete_time IS '软删时间；删除后不参与匹配，各字段可重建路由';
COMMENT ON COLUMN notify_route.create_user IS '创建人（操作者 ID 注入待补，当前 NULL）';
COMMENT ON COLUMN notify_route.update_user IS '更新人（操作者 ID 注入待补，当前 NULL）';
COMMENT ON COLUMN notify_route.delete_user IS '删除人（操作者 ID 注入待补，当前 NULL）';

CREATE INDEX idx_notify_route_biz_pattern
    ON notify_route (biz_id, event_pattern) WHERE delete_time IS NULL;

INSERT INTO notify_cfg_version (table_name, version) VALUES ('notify_route', 1);
