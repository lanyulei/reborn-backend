-- 0079 共享密钥指纹：core 与各数据面服务共用的密钥（凭据加密主密钥、core 与容器平台数据面之间的
-- 内部签名密钥）必须取值相同。core 启动时把指纹登记在这里（首次写入为准），其余服务启动时比对，
-- 不一致直接启动失败，而不是等到运行期解密凭据 / 校验签名时才报错。见 kit/keycheck。
-- 轮换密钥：停掉相关服务 → 删除对应行 → 用新密钥启动 core。

CREATE TABLE system_key_check (
    name        varchar(64)  PRIMARY KEY,
    fingerprint char(32)     NOT NULL,
    create_time timestamptz  NOT NULL DEFAULT now()
);

COMMENT ON TABLE system_key_check IS '共享密钥指纹：各服务启动时核对本机密钥与 core 登记的是否一致（kit/keycheck）';
COMMENT ON COLUMN system_key_check.name IS '密钥名：master=凭据加密主密钥 k8s_internal=core 与容器平台数据面的内部签名密钥';
COMMENT ON COLUMN system_key_check.fingerprint IS '指纹：HMAC-SHA256(密钥, 固定标签) 前 16 字节的十六进制，不可逆';
COMMENT ON COLUMN system_key_check.create_time IS '登记时间';

-- 数据面服务只读（比对）；登记由 core 完成。
GRANT SELECT ON system_key_check TO notify_pipe_role, cmdb_sync_role, job_engine_role, k8s_plane_role, monitor_engine_role;
