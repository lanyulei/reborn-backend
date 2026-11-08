-- 0002_refresh_token.up.sql 认证域：刷新令牌表。
-- 仅存令牌原文的 SHA-256 hex（64 字符）；轮换/登出/删号物理删除，不做软删
-- （行的存在即代表凭证有效，非业务实体，无审计/恢复语义）。
-- 过期行由签发路径在同事务惰性清理（expire_time < now()），无后台任务。

CREATE TABLE system_refresh_token (
    id          uuid        PRIMARY KEY,
    user_id     uuid        NOT NULL REFERENCES "system_user" (id),
    token_hash  char(64)    NOT NULL,
    expire_time timestamptz NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE system_refresh_token IS '刷新令牌表：SHA-256 hex 落库，物理删除，不做软删';
COMMENT ON COLUMN system_refresh_token.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN system_refresh_token.user_id IS '用户 ID，引用 "system_user".id（表名 PG 保留字须双引号）';
COMMENT ON COLUMN system_refresh_token.token_hash IS '刷新令牌原文的 SHA-256 hex，恒 64 字符';
COMMENT ON COLUMN system_refresh_token.expire_time IS '过期时间；过期行由签发路径惰性清理';
COMMENT ON COLUMN system_refresh_token.create_time IS '签发时间';

CREATE UNIQUE INDEX uk_system_refresh_token_hash ON system_refresh_token (token_hash);
CREATE INDEX idx_system_refresh_token_user_id ON system_refresh_token (user_id);
