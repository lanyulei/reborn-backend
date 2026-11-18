-- 0074 容器平台 Helm 仓库（k8s_helm_repo）。
-- 写入分工同 0072：core 写（凭据只加密写入），数据面（backend/k8s）只读并解密——Chart 的下载与渲染、
-- Release 的全部操作都在数据面进行。类型字面量的唯一事实源是 kit/k8scontract（HelmRepo*）。

CREATE TABLE k8s_helm_repo (
    id                   uuid         PRIMARY KEY,
    name                 varchar(64)  NOT NULL,
    type                 smallint     NOT NULL,
    url                  varchar(512) NOT NULL,
    username             varchar(128) NOT NULL DEFAULT '',
    secret               bytea        NULL,
    ca_cert              text         NOT NULL DEFAULT '',
    insecure_skip_verify boolean      NOT NULL DEFAULT false,
    description          varchar(255) NOT NULL DEFAULT '',
    enabled              boolean      NOT NULL DEFAULT true,
    version              integer      NOT NULL DEFAULT 1,
    create_time          timestamptz  NOT NULL DEFAULT now(),
    update_time          timestamptz  NOT NULL DEFAULT now(),
    delete_time          timestamptz  NULL,
    create_user          uuid         NULL,
    update_user          uuid         NULL,
    delete_user          uuid         NULL,
    CONSTRAINT ck_k8s_helm_repo_type CHECK (type IN (1, 2))
);
COMMENT ON TABLE k8s_helm_repo IS 'Helm 仓库，软删；凭据由 core 加密写入、数据面下载 Chart 时解密';
COMMENT ON COLUMN k8s_helm_repo.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN k8s_helm_repo.name IS '名称，未删除行内唯一';
COMMENT ON COLUMN k8s_helm_repo.type IS '类型：1=HTTP 仓库（index.yaml） 2=OCI 仓库';
COMMENT ON COLUMN k8s_helm_repo.url IS '仓库地址：https:// 、http:// 或 oci://；不含凭据、query、fragment';
COMMENT ON COLUMN k8s_helm_repo.username IS '用户名；空=匿名访问';
COMMENT ON COLUMN k8s_helm_repo.secret IS '密码或令牌的密文（kit/secretbox）；只写不读，NULL=未配置';
COMMENT ON COLUMN k8s_helm_repo.ca_cert IS 'CA 证书（PEM）；空=使用系统根证书';
COMMENT ON COLUMN k8s_helm_repo.insecure_skip_verify IS '跳过 TLS 证书校验';
COMMENT ON COLUMN k8s_helm_repo.description IS '说明';
COMMENT ON COLUMN k8s_helm_repo.enabled IS '是否启用；停用后不可用于安装与升级';
COMMENT ON COLUMN k8s_helm_repo.version IS '乐观锁版本号，每次更新 +1';
COMMENT ON COLUMN k8s_helm_repo.create_time IS '创建时间';
COMMENT ON COLUMN k8s_helm_repo.update_time IS '最后更新时间';
COMMENT ON COLUMN k8s_helm_repo.delete_time IS '软删除时间，NULL=未删除';
COMMENT ON COLUMN k8s_helm_repo.create_user IS '创建人用户 ID';
COMMENT ON COLUMN k8s_helm_repo.update_user IS '最后更新人用户 ID';
COMMENT ON COLUMN k8s_helm_repo.delete_user IS '删除人用户 ID';
CREATE UNIQUE INDEX uk_k8s_helm_repo_name ON k8s_helm_repo (name) WHERE delete_time IS NULL;

GRANT SELECT ON k8s_helm_repo TO k8s_plane_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON k8s_helm_repo TO k8s_core_role;
