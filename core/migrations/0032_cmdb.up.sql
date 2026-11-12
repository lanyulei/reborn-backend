-- 0032_cmdb.up.sql CMDB 域：资源模型（元数据）、资源实例、关系、变更历史。
-- 模型层：cmdb_model / cmdb_model_field / cmdb_relation_type 由界面维护，新增一类
-- 资源无需改代码；字段定义只增不改类型（废弃用 deprecated 标记），保证历史可解释。
-- 实例层：cmdb_ci 固定列 + attrs jsonb 动态属性；name 与 unique_key 在类型内唯一
-- （部分唯一索引，软删行不参与）；version 乐观锁；不物理删除。
-- 关系层：cmdb_relation (type, src, dst) 唯一，外键 RESTRICT——删资源须先解关系。
-- 历史：cmdb_ci_history append-only，按月 RANGE 分区 + DEFAULT 兜底分区（口径同
-- notify 运行表），仅记录真实变化。

CREATE TABLE cmdb_model (
    code        varchar(64)  PRIMARY KEY,
    name        varchar(64)  NOT NULL,
    category    varchar(32)  NOT NULL,
    icon        varchar(64)  NOT NULL DEFAULT '',
    description varchar(512) NOT NULL DEFAULT '',
    status      smallint     NOT NULL DEFAULT 1,
    builtin     boolean      NOT NULL DEFAULT false,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL
);
COMMENT ON TABLE cmdb_model IS 'CMDB 资源类型（元数据）';
COMMENT ON COLUMN cmdb_model.code IS '类型编码，创建后不可改；也是 cmdb_ci.model_code 的外键目标';
COMMENT ON COLUMN cmdb_model.category IS '分类：compute / network / storage / cloud / component / other';
COMMENT ON COLUMN cmdb_model.status IS '1 启用 / 2 停用（停用后不可新建该类型资源）';
COMMENT ON COLUMN cmdb_model.builtin IS '内置类型不可删除';

CREATE TABLE cmdb_model_field (
    id           uuid         PRIMARY KEY,
    model_code   varchar(64)  NOT NULL REFERENCES cmdb_model (code),
    code         varchar(64)  NOT NULL,
    name         varchar(64)  NOT NULL,
    type         varchar(16)  NOT NULL,
    required     boolean      NOT NULL DEFAULT false,
    "unique"     boolean      NOT NULL DEFAULT false,
    enum_values  jsonb        NULL,
    ref_model    varchar(64)  NOT NULL DEFAULT '',
    rule         jsonb        NULL,
    owner        varchar(8)   NOT NULL DEFAULT 'manual',
    list_visible boolean      NOT NULL DEFAULT false,
    sort         integer      NOT NULL DEFAULT 0,
    description  varchar(256) NOT NULL DEFAULT '',
    deprecated   boolean      NOT NULL DEFAULT false,
    create_time  timestamptz  NOT NULL DEFAULT now(),
    update_time  timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_cmdb_model_field_code UNIQUE (model_code, code)
);
COMMENT ON TABLE cmdb_model_field IS 'CMDB 类型字段定义：attrs 的键、数据类型、校验规则与归属';
COMMENT ON COLUMN cmdb_model_field.type IS 'string / int / float / bool / enum / datetime / json / ip / ref';
COMMENT ON COLUMN cmdb_model_field.owner IS 'manual 人工 / sync 同步只读 / both 同步写入可人工覆盖';
COMMENT ON COLUMN cmdb_model_field.rule IS '{pattern,min,max,minLen,maxLen}';

CREATE TABLE cmdb_relation_type (
    code          varchar(64)  PRIMARY KEY,
    name          varchar(64)  NOT NULL,
    forward_name  varchar(64)  NOT NULL,
    backward_name varchar(64)  NOT NULL,
    src_model     varchar(64)  NOT NULL,
    dst_model     varchar(64)  NOT NULL,
    cardinality   varchar(16)  NOT NULL,
    on_delete     varchar(16)  NOT NULL DEFAULT 'restrict',
    description   varchar(256) NOT NULL DEFAULT '',
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    delete_time   timestamptz  NULL
);
COMMENT ON TABLE cmdb_relation_type IS 'CMDB 关系类型（有向 src → dst）';
COMMENT ON COLUMN cmdb_relation_type.src_model IS '源端类型编码，* 任意';
COMMENT ON COLUMN cmdb_relation_type.dst_model IS '目标端类型编码，* 任意';
COMMENT ON COLUMN cmdb_relation_type.cardinality IS 'one_to_one / one_to_many（一 src 多 dst，dst 只属一 src）/ many_to_many';
COMMENT ON COLUMN cmdb_relation_type.on_delete IS 'src 退役时对 dst：restrict 禁止 / cascade_retire 级联退役';

CREATE TABLE cmdb_ci (
    id             uuid         PRIMARY KEY,
    model_code     varchar(64)  NOT NULL REFERENCES cmdb_model (code),
    name           varchar(128) NOT NULL,
    status         varchar(16)  NOT NULL DEFAULT 'active',
    env            varchar(32)  NOT NULL DEFAULT '',
    owner          varchar(64)  NOT NULL DEFAULT '',
    tags           text[]       NOT NULL DEFAULT '{}',
    source         varchar(80)  NOT NULL DEFAULT 'manual',
    external_id    varchar(128) NOT NULL DEFAULT '',
    unique_key     varchar(512) NOT NULL DEFAULT '',
    attrs          jsonb        NOT NULL DEFAULT '{}',
    last_synced_at timestamptz  NULL,
    stale          boolean      NOT NULL DEFAULT false,
    version        bigint       NOT NULL DEFAULT 1,
    create_user    uuid         NULL,
    update_user    uuid         NULL,
    create_time    timestamptz  NOT NULL DEFAULT now(),
    update_time    timestamptz  NOT NULL DEFAULT now(),
    delete_time    timestamptz  NULL
);
COMMENT ON TABLE cmdb_ci IS 'CMDB 资源实例（配置项）：固定列 + attrs 动态属性';
COMMENT ON COLUMN cmdb_ci.status IS 'planning / active / maintenance / retired';
COMMENT ON COLUMN cmdb_ci.source IS 'manual 或 sync:<采集源 ID>';
COMMENT ON COLUMN cmdb_ci.external_id IS '外部系统标识（同步对账锚点）';
COMMENT ON COLUMN cmdb_ci.unique_key IS '按类型唯一字段拼接（code=value|...），空串不参与唯一约束';
COMMENT ON COLUMN cmdb_ci.version IS '乐观锁版本号，每次更新 +1';

CREATE INDEX idx_cmdb_ci_model_status ON cmdb_ci (model_code, status) WHERE delete_time IS NULL;
CREATE UNIQUE INDEX uq_cmdb_ci_model_name ON cmdb_ci (model_code, name) WHERE delete_time IS NULL;
CREATE UNIQUE INDEX uq_cmdb_ci_model_unique_key ON cmdb_ci (model_code, unique_key) WHERE delete_time IS NULL AND unique_key <> '';
CREATE INDEX idx_cmdb_ci_external ON cmdb_ci (model_code, external_id) WHERE delete_time IS NULL AND external_id <> '';
CREATE INDEX idx_cmdb_ci_attrs ON cmdb_ci USING gin (attrs jsonb_path_ops);
CREATE INDEX idx_cmdb_ci_tags ON cmdb_ci USING gin (tags);

CREATE TABLE cmdb_relation (
    id          uuid        PRIMARY KEY,
    type_code   varchar(64) NOT NULL REFERENCES cmdb_relation_type (code),
    src_id      uuid        NOT NULL REFERENCES cmdb_ci (id) ON DELETE RESTRICT,
    dst_id      uuid        NOT NULL REFERENCES cmdb_ci (id) ON DELETE RESTRICT,
    source      varchar(80) NOT NULL DEFAULT 'manual',
    create_user uuid        NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_cmdb_relation UNIQUE (type_code, src_id, dst_id),
    CONSTRAINT ck_cmdb_relation_self CHECK (src_id <> dst_id)
);
COMMENT ON TABLE cmdb_relation IS 'CMDB 资源关系实例（有向）';
CREATE INDEX idx_cmdb_relation_src ON cmdb_relation (src_id, type_code);
CREATE INDEX idx_cmdb_relation_dst ON cmdb_relation (dst_id, type_code);

CREATE TABLE cmdb_ci_history (
    id          uuid        NOT NULL,
    ci_id       uuid        NOT NULL,
    action      varchar(16) NOT NULL,
    diff        jsonb       NOT NULL DEFAULT '{}',
    operator    varchar(64) NOT NULL DEFAULT '',
    source      varchar(80) NOT NULL DEFAULT 'manual',
    request_id  varchar(64) NOT NULL DEFAULT '',
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, create_time)
) PARTITION BY RANGE (create_time);
COMMENT ON TABLE cmdb_ci_history IS 'CMDB 资源变更历史：append-only，按月分区，仅记录真实变化';
COMMENT ON COLUMN cmdb_ci_history.action IS 'create / update / transition / delete / link / unlink';
COMMENT ON COLUMN cmdb_ci_history.diff IS '{before:{...}, after:{...}}；关系动作含 relation_type/direction/target';
CREATE INDEX idx_cmdb_ci_history_ci ON cmdb_ci_history (ci_id, create_time DESC);

CREATE TABLE cmdb_ci_history_2026_11 PARTITION OF cmdb_ci_history
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE cmdb_ci_history_2026_12 PARTITION OF cmdb_ci_history
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');
CREATE TABLE cmdb_ci_history_2027_01 PARTITION OF cmdb_ci_history
    FOR VALUES FROM ('2027-01-01') TO ('2027-02-01');
CREATE TABLE cmdb_ci_history_default PARTITION OF cmdb_ci_history DEFAULT;

-- 内置类型与字段（可在界面继续扩展字段；内置类型不可删除）。
INSERT INTO cmdb_model (code, name, category, icon, description, builtin) VALUES
    ('physical_server', '物理机', 'compute', 'hdd', '机房物理服务器', true),
    ('virtual_machine', '虚拟机', 'compute', 'cloud-server', '运行于物理机/虚拟化平台的虚拟机', true),
    ('network_device', '网络设备', 'network', 'apartment', '交换机 / 路由器 / 防火墙', true),
    ('cloud_host', '云主机', 'cloud', 'cloud', '云厂商弹性计算实例', true),
    ('component', '配件', 'component', 'appstore', '安装在主机或网络设备上的配件（内存/硬盘/网卡等）', true);

INSERT INTO cmdb_model_field (id, model_code, code, name, type, required, "unique", enum_values, rule, list_visible, sort) VALUES
    (gen_random_uuid(), 'physical_server', 'ip', '管理 IP', 'ip', true, true, NULL, NULL, true, 10),
    (gen_random_uuid(), 'physical_server', 'serial_number', '序列号', 'string', false, true, NULL, '{"maxLen":64}', true, 20),
    (gen_random_uuid(), 'physical_server', 'vendor', '厂商', 'string', false, false, NULL, '{"maxLen":64}', true, 30),
    (gen_random_uuid(), 'physical_server', 'cpu_cores', 'CPU 核数', 'int', false, false, NULL, '{"min":1,"max":1024}', true, 40),
    (gen_random_uuid(), 'physical_server', 'memory_gb', '内存（GB）', 'int', false, false, NULL, '{"min":1}', true, 50),
    (gen_random_uuid(), 'physical_server', 'rack', '机柜', 'string', false, false, NULL, '{"maxLen":64}', false, 60),
    (gen_random_uuid(), 'virtual_machine', 'ip', 'IP', 'ip', true, true, NULL, NULL, true, 10),
    (gen_random_uuid(), 'virtual_machine', 'os', '操作系统', 'string', false, false, NULL, '{"maxLen":64}', true, 20),
    (gen_random_uuid(), 'virtual_machine', 'vcpu', 'vCPU', 'int', false, false, NULL, '{"min":1}', true, 30),
    (gen_random_uuid(), 'virtual_machine', 'memory_gb', '内存（GB）', 'int', false, false, NULL, '{"min":1}', true, 40),
    (gen_random_uuid(), 'network_device', 'ip', '管理 IP', 'ip', true, true, NULL, NULL, true, 10),
    (gen_random_uuid(), 'network_device', 'device_type', '设备类型', 'enum', true, false, '["switch","router","firewall","load_balancer"]', NULL, true, 20),
    (gen_random_uuid(), 'network_device', 'vendor', '厂商', 'string', false, false, NULL, '{"maxLen":64}', true, 30),
    (gen_random_uuid(), 'network_device', 'serial_number', '序列号', 'string', false, true, NULL, '{"maxLen":64}', false, 40),
    (gen_random_uuid(), 'cloud_host', 'provider', '云厂商', 'enum', true, false, '["aliyun","tencent","aws","huawei","other"]', NULL, true, 10),
    (gen_random_uuid(), 'cloud_host', 'instance_id', '实例 ID', 'string', true, true, NULL, '{"maxLen":128}', true, 20),
    (gen_random_uuid(), 'cloud_host', 'region', '地域', 'string', false, false, NULL, '{"maxLen":64}', true, 30),
    (gen_random_uuid(), 'cloud_host', 'private_ip', '内网 IP', 'ip', false, false, NULL, NULL, true, 40),
    (gen_random_uuid(), 'cloud_host', 'public_ip', '公网 IP', 'ip', false, false, NULL, NULL, false, 50),
    (gen_random_uuid(), 'component', 'component_type', '配件类型', 'enum', true, false, '["memory","disk","nic","cpu","power","other"]', NULL, true, 10),
    (gen_random_uuid(), 'component', 'model', '型号', 'string', false, false, NULL, '{"maxLen":128}', true, 20),
    (gen_random_uuid(), 'component', 'serial_number', '序列号', 'string', false, true, NULL, '{"maxLen":64}', true, 30),
    (gen_random_uuid(), 'component', 'capacity', '容量/规格', 'string', false, false, NULL, '{"maxLen":64}', true, 40);

INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality, on_delete, description) VALUES
    ('host_runs_vm', '物理机运行虚拟机', '运行的虚拟机', '所在物理机', 'physical_server', 'virtual_machine', 'one_to_many', 'restrict', '一台物理机运行多台虚拟机'),
    ('installed_component', '安装配件', '安装的配件', '安装于', '*', 'component', 'one_to_many', 'cascade_retire', '配件安装在主机或网络设备上；主机退役时配件随之退役'),
    ('uplink', '上联网络设备', '上联网络设备', '下联设备', '*', 'network_device', 'many_to_many', 'restrict', '主机/设备接入的网络设备');
