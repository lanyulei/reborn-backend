-- 0040 CMDB 对标配置平台的管理能力补齐：模型分组可自定义、模型收藏、模型变更审计。
-- cmdb_model.category 语义由"写死的 6 值枚举"变为"分组编码"：现有 6 个分类原样
-- 预置为内置分组，存量模型零迁移；分组存在性由应用层校验（不加外键，保持旧数据面兼容）。

CREATE TABLE cmdb_model_group (
    code        varchar(64) PRIMARY KEY,
    name        varchar(64) NOT NULL,
    sort        int         NOT NULL DEFAULT 0,
    builtin     boolean     NOT NULL DEFAULT false,
    create_time timestamptz NOT NULL DEFAULT now(),
    update_time timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE cmdb_model_group IS 'CMDB 模型分组：cmdb_model.category 取值为分组编码；内置分组不可删除';
COMMENT ON COLUMN cmdb_model_group.sort IS '展示顺序（升序，其次按编码）';
COMMENT ON COLUMN cmdb_model_group.builtin IS '内置分组：随迁移预置，可改名/调序，不可删除';

INSERT INTO cmdb_model_group (code, name, sort, builtin) VALUES
    ('compute', '计算', 10, true),
    ('network', '网络', 20, true),
    ('storage', '存储', 30, true),
    ('cloud', '云资源', 40, true),
    ('component', '配件', 50, true),
    ('other', '其他', 900, true)
ON CONFLICT (code) DO NOTHING;

CREATE TABLE cmdb_model_favorite (
    user_id     uuid        NOT NULL,
    model_code  varchar(64) NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, model_code)
);
COMMENT ON TABLE cmdb_model_favorite IS 'CMDB 模型收藏：按用户记录常用模型（资源目录置顶）；不设外键，模型删除后查询时过滤';

CREATE TABLE cmdb_schema_audit (
    id          uuid         PRIMARY KEY,
    target_type varchar(16)  NOT NULL,
    target_code varchar(160) NOT NULL,
    target_name varchar(128) NOT NULL DEFAULT '',
    model_code  varchar(64)  NOT NULL DEFAULT '',
    action      varchar(16)  NOT NULL,
    diff        jsonb        NOT NULL DEFAULT '{}',
    operator    varchar(64)  NOT NULL DEFAULT '',
    request_id  varchar(64)  NOT NULL DEFAULT '',
    create_time timestamptz  NOT NULL DEFAULT now()
);
COMMENT ON TABLE cmdb_schema_audit IS 'CMDB 模型变更审计：分组 / 模型 / 字段 / 关系类型的增删改，append-only';
COMMENT ON COLUMN cmdb_schema_audit.target_type IS 'group / model / field / relation_type';
COMMENT ON COLUMN cmdb_schema_audit.target_code IS '对象编码：分组编码 / 模型编码 / 字段编码 / 关系类型编码';
COMMENT ON COLUMN cmdb_schema_audit.model_code IS '所属模型编码（模型与字段有值；关系类型为源端模型，可能为通配 *）';
COMMENT ON COLUMN cmdb_schema_audit.action IS 'create / update / delete';
COMMENT ON COLUMN cmdb_schema_audit.diff IS '{before:{...}, after:{...}}；update 仅含变化的键';
CREATE INDEX idx_cmdb_schema_audit_time ON cmdb_schema_audit (create_time DESC, id DESC);
CREATE INDEX idx_cmdb_schema_audit_model ON cmdb_schema_audit (model_code, create_time DESC);
