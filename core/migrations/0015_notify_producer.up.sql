-- 0015 notify 域：生产者凭据管理（token 签发/轮换/启停）
-- 语义：一行 = 一个事件生产者（如监控引擎）的接入凭据；token 明文仅在
-- core 签发（创建/轮换）响应中出现一次，落库只存 SHA-256(token)（32 字节
-- bytea），任何读接口不回显。数据面鉴权由管道 ingress 执行（设计 §9：
-- Authorization: HMAC <id>:<sig>:<ts>，时间窗 ±5min 防重放）。
-- event_whitelist 为空 = 不限事件类型（token scope 仍限定 biz_id）；
-- 条目为事件模式（精确 / 前缀通配 / '*'，与路由同式）。
-- 与订阅不同：生产者属管道 ingress 鉴权缓存集（Disable/Rotate 须 ≤10s
-- 生效），故写路径同事务 bump notify_cfg_version。
CREATE TABLE notify_producer (
    id               uuid        PRIMARY KEY,
    name             varchar(64) NOT NULL,                    -- 同 biz 未删除行内唯一
    biz_id           varchar(64) NOT NULL DEFAULT 'default',  -- token scope 绑定 biz，创建后不可改
    event_whitelist  text[]      NOT NULL DEFAULT '{}',       -- 空=不限；条目为事件模式
    token_hash       bytea       NOT NULL,                    -- SHA-256(token 明文)，原文不落库
    expire_time      timestamptz,                             -- NULL=永不过期；过期即拒绝接入（管道判定）
    enabled          boolean     NOT NULL DEFAULT true,       -- 停用即拒绝接入（≤10s 生效）
    create_time      timestamptz NOT NULL DEFAULT now(),
    update_time      timestamptz NOT NULL DEFAULT now(),
    delete_time      timestamptz,                             -- 软删；唯一约束仅约束未删除行，删除后名称可复用
    create_user      uuid,
    update_user      uuid,
    delete_user      uuid
);

COMMENT ON TABLE notify_producer IS '通知生产者凭据：token 明文仅签发时返回一次，库存 SHA-256；数据面 HMAC 鉴权属管道 ingress';
COMMENT ON COLUMN notify_producer.id IS '生产者唯一标识（UUIDv7）';
COMMENT ON COLUMN notify_producer.name IS '生产者名称；同 biz 未删除行内唯一';
COMMENT ON COLUMN notify_producer.biz_id IS '业务租户标识；token scope 限定 biz（§9 多租户），创建后不可改';
COMMENT ON COLUMN notify_producer.event_whitelist IS '事件类型白名单（text[]）：空=不限；条目为事件模式（精确/前缀通配/*，与路由同式）';
COMMENT ON COLUMN notify_producer.token_hash IS 'token 的 SHA-256（32 字节）；token 明文仅签发响应返回一次，任何读接口不回显';
COMMENT ON COLUMN notify_producer.expire_time IS '过期时间；NULL=永不过期；过期后 ingress 拒绝接入';
COMMENT ON COLUMN notify_producer.enabled IS '启用状态；停用后 ingress 拒绝接入（≤10s 生效，经 cfg_version 轮询）';
COMMENT ON COLUMN notify_producer.create_time IS '创建时间';
COMMENT ON COLUMN notify_producer.update_time IS '更新时间';
COMMENT ON COLUMN notify_producer.delete_time IS '软删时间；NULL=未删除';
COMMENT ON COLUMN notify_producer.create_user IS '创建人（操作者），暂未落值';
COMMENT ON COLUMN notify_producer.update_user IS '最后更新人（操作者），暂未落值';
COMMENT ON COLUMN notify_producer.delete_user IS '删除人（操作者），暂未落值';

CREATE UNIQUE INDEX uk_notify_producer_name ON notify_producer (biz_id, name) WHERE delete_time IS NULL;
