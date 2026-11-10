-- 0013_notify_silence.up.sql notify 域：静默（免打扰）规则表。
-- router 处理事件的第 2 步（§4.4）：事件命中任一启用静默规则（event_pattern
-- 匹配 + severity ≤ severity_max + 当前时间落时间窗，无窗即全程）→
-- event.status=suppressed，ack 结束（见 backend/docs/notify-design.md §4.4/§5.1）。
-- 设计即无唯一约束：多条静默规则可重叠，任一命中即抑制。
-- 每次写操作同事务 bump notify_cfg_version('notify_silence')：管道 10s 轮询刷新缓存。

CREATE TABLE notify_silence (
    id            uuid         PRIMARY KEY,
    biz_id        varchar(64)  NOT NULL DEFAULT 'default',
    event_pattern varchar(128) NOT NULL DEFAULT '*',
    severity_max  smallint     NOT NULL DEFAULT 2,
    time_window   jsonb        NULL,
    enabled       boolean      NOT NULL DEFAULT true,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    delete_time   timestamptz  NULL,
    create_user   uuid         NULL,
    update_user   uuid         NULL,
    delete_user   uuid         NULL
);

COMMENT ON TABLE notify_silence IS '通知静默规则表：事件模式+严重级+时间窗命中即抑制，软删；多条规则可重叠';
COMMENT ON COLUMN notify_silence.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN notify_silence.biz_id IS '业务租户标识，默认 default；静默匹配按 biz 隔离';
COMMENT ON COLUMN notify_silence.event_pattern IS '事件匹配模式：精确（monitor.alert.firing）/前缀通配（monitor.alert.*）/全匹配（*），与 notify_route 同式';
COMMENT ON COLUMN notify_silence.severity_max IS '严重级上限（1 info/2 warning/3 critical/4 fatal），该级及以下的事件才可被静默';
COMMENT ON COLUMN notify_silence.time_window IS '免打扰时间窗 {"start":"22:00","end":"08:00","tz":"Asia/Shanghai"}，支持跨午夜；NULL=全程静默；tz 缺省 UTC';
COMMENT ON COLUMN notify_silence.enabled IS '启用状态；停用后不参与匹配（≤10s 生效，配置版本轮询）';
COMMENT ON COLUMN notify_silence.create_time IS '创建时间';
COMMENT ON COLUMN notify_silence.update_time IS '更新时间';
COMMENT ON COLUMN notify_silence.delete_time IS '软删时间；删除后不参与匹配，规则可重建';
COMMENT ON COLUMN notify_silence.create_user IS '创建人（操作者 ID 注入待补，当前 NULL）';
COMMENT ON COLUMN notify_silence.update_user IS '更新人（操作者 ID 注入待补，当前 NULL）';
COMMENT ON COLUMN notify_silence.delete_user IS '删除人（操作者 ID 注入待补，当前 NULL）';

CREATE INDEX idx_notify_silence_biz_pattern
    ON notify_silence (biz_id, event_pattern) WHERE delete_time IS NULL;

INSERT INTO notify_cfg_version (table_name, version) VALUES ('notify_silence', 1);
