-- 0009_login_log.up.sql 认证域：登录日志表。
-- append-only 审计：记录登录成功与全部认证失败路径（用户不存在/密码错误/
-- 账号禁用/签发令牌失败），无 update/delete 语义，不做软删、不加更新审计列。
-- user_id 刻意不设外键（与 0002 刷新令牌表的 FK 模式不同）：审计行独立于
-- 用户生命周期，用户清理不影响历史日志；用户不存在时为 NULL。
-- 登录路径同步写、写失败仅告警不阻塞登录；无后台清理任务（保留全量历史）。

CREATE TABLE system_login_log (
    id          uuid        PRIMARY KEY,
    user_id     uuid        NULL,
    username    varchar(64) NOT NULL,
    result      smallint    NOT NULL,
    ip          varchar(45) NULL,
    user_agent  varchar(255) NULL,
    create_time timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE system_login_log IS '登录日志表：append-only 审计，记录登录成功与全部认证失败路径';
COMMENT ON COLUMN system_login_log.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN system_login_log.user_id IS '用户 ID；用户不存在时为 NULL（其余结果均回填）；不设外键，审计行独立于用户生命周期';
COMMENT ON COLUMN system_login_log.username IS '尝试的登录名原文（与登录请求同规则，最长 64）';
COMMENT ON COLUMN system_login_log.result IS '登录结果：1=成功 2=用户不存在 3=密码错误 4=账号禁用 5=签发令牌失败；对外登录接口 2/3 统一口径防枚举';
COMMENT ON COLUMN system_login_log.ip IS '客户端 IP（HTTP 侧 X-Forwarded-For 首段，缺省 RemoteAddr；gRPC 侧为空），最长 45（IPv6）';
COMMENT ON COLUMN system_login_log.user_agent IS '客户端 User-Agent 原文截断至 255';
COMMENT ON COLUMN system_login_log.create_time IS '登录尝试时间';

CREATE INDEX idx_system_login_log_create_time ON system_login_log (create_time);
CREATE INDEX idx_system_login_log_user_id ON system_login_log (user_id);
CREATE INDEX idx_system_login_log_username ON system_login_log (username);
