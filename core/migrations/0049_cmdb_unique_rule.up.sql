-- 0049 CMDB 唯一规则：一个类型可配置多条唯一规则，每条规则 = 1 个（单字段唯一）或
-- 多个（联合唯一）字段，各规则互相独立。取代"全部 unique 字段拼成一个唯一键"的旧口径
-- （cmdb_ci.unique_key + uq_cmdb_ci_model_unique_key）。
--
-- 结构：
--   cmdb_model_unique_rule  规则定义（类型、名称、字段组）
--   cmdb_ci_unique          资源在某规则下的键（sha256 摘要）；(rule_id, key) 唯一索引是唯一性的最终保证
-- 维护：cmdb_ci 上的行级触发器按当前规则同步 cmdb_ci_unique——任何写 cmdb_ci 的路径
--   （core 新建/更新/软删、数据面对账）都自动保持一致，不依赖各调用方自行计算。
-- 并发：触发器取类型级共享 advisory lock，规则变更（core）取排他锁并在同事务内重算，
--   两者互斥，保证"规则 ↔ 键"始终对应。
-- 语义：规则内任一字段为空（缺失 / null / 空串）的资源不受该规则约束。

-- 键函数：字段按 code 的 C 序拼接 `<len>:<code><len>:<value>` 后取 sha256。长度前缀
-- 消除分隔符歧义（旧口径 `code=value|...` 在值含 `|` 时可碰撞），摘要定长、无 512 字符上限。
-- 值取 jsonb 的文本形式（`attrs ->> code`）；数值先去掉小数末尾的 0（1 / 1.0 / 1.00 视为同值，
-- jsonb 会保留写入方给出的小数位数）。对象 / 数组按 jsonb 规范化后的文本参与（新规则不允许
-- 这类字段，仅为存量迁移保持原有约束）。任一字段为空时返回 NULL（不参与约束）。
-- convert_to 为 STABLE（依赖库编码），故本函数声明 STABLE；不用于表达式索引。
CREATE OR REPLACE FUNCTION cmdb_unique_key(attrs jsonb, codes text[])
RETURNS bytea LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN count(*) = 0 OR bool_or(v.val IS NULL OR v.val = '') THEN NULL
        ELSE sha256(convert_to(string_agg(
            length(v.code)::text || ':' || v.code || length(v.val)::text || ':' || v.val, ''
            ORDER BY v.code COLLATE "C"), 'UTF8'))
    END
    FROM (
        SELECT u.code,
               CASE jsonb_typeof(attrs -> u.code)
                   WHEN 'number' THEN trim_scale((attrs ->> u.code)::numeric)::text
                   WHEN 'null' THEN NULL
                   ELSE attrs ->> u.code
               END AS val
        FROM unnest(codes) AS u(code)
    ) v
$$;
COMMENT ON FUNCTION cmdb_unique_key(jsonb, text[]) IS 'CMDB 唯一规则键：字段组取值的 sha256 摘要；任一字段为空返回 NULL';

CREATE TABLE cmdb_model_unique_rule (
    id          uuid        PRIMARY KEY,
    model_code  varchar(64) NOT NULL REFERENCES cmdb_model (code),
    name        varchar(64) NOT NULL,
    field_codes text[]      NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    update_time timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_cmdb_model_unique_rule_fields UNIQUE (model_code, field_codes),
    -- 库层只保证非空；业务上限（每规则 ≤5 字段、每类型 ≤8 条）在 core 校验——存量迁移来的
    -- 规则可能超出上限，照常生效。
    CONSTRAINT ck_cmdb_model_unique_rule_fields CHECK (cardinality(field_codes) >= 1)
);
-- 规则名在类型内唯一（不区分大小写）。用表达式索引而非 UNIQUE 约束：name 不成为行的"键列"，
-- 改名只取普通行锁，不与在途资源写入因外键持有的 KEY SHARE 锁冲突（否则改名要等这些事务
-- 结束，期间新的资源写入还会在其后排队）。
CREATE UNIQUE INDEX uq_cmdb_model_unique_rule_name ON cmdb_model_unique_rule (model_code, lower(name));
COMMENT ON TABLE cmdb_model_unique_rule IS 'CMDB 唯一规则：field_codes 这组字段的取值在类型内不可重复';
COMMENT ON COLUMN cmdb_model_unique_rule.field_codes IS '字段编码（C 序、不重复）；1 个 = 单字段唯一，多个 = 联合唯一';

CREATE TABLE cmdb_ci_unique (
    ci_id   uuid  NOT NULL REFERENCES cmdb_ci (id) ON DELETE CASCADE,
    rule_id uuid  NOT NULL REFERENCES cmdb_model_unique_rule (id) ON DELETE CASCADE,
    key     bytea NOT NULL,
    PRIMARY KEY (ci_id, rule_id),
    CONSTRAINT uq_cmdb_ci_unique_rule_key UNIQUE (rule_id, key)
);
COMMENT ON TABLE cmdb_ci_unique IS 'CMDB 资源唯一键：未删除资源在每条适用规则下一行，由 cmdb_ci 触发器与规则变更维护';
COMMENT ON COLUMN cmdb_ci_unique.key IS 'cmdb_unique_key(attrs, 规则字段组)';

-- 触发器：按当前规则同步资源的键。软删即移除（与旧部分唯一索引 delete_time IS NULL 同口径）。
-- 违反 uq_cmdb_ci_unique_rule_key 时原语句失败，调用方映射为"唯一规则冲突"。
CREATE OR REPLACE FUNCTION cmdb_ci_unique_sync()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- 与规则变更互斥（见文件头）；锁键须与 core 的 uniqueRuleLockSQL 一致。软删同样要取锁：
    -- 否则并发的规则重算看到的仍是"未删除"的资源，会给它补上键，删除提交后残留——此后
    -- 同值的新资源被误判为重复。
    PERFORM pg_advisory_xact_lock_shared(hashtextextended('cmdb_unique_rule:' || NEW.model_code, 0));
    IF NEW.delete_time IS NOT NULL THEN
        DELETE FROM cmdb_ci_unique WHERE ci_id = NEW.id;
        RETURN NULL;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        DELETE FROM cmdb_ci_unique u
        WHERE u.ci_id = NEW.id
          AND NOT EXISTS (
              SELECT 1 FROM cmdb_model_unique_rule r
              WHERE r.id = u.rule_id AND r.model_code = NEW.model_code
                AND cmdb_unique_key(NEW.attrs, r.field_codes) = u.key);
    END IF;
    INSERT INTO cmdb_ci_unique (ci_id, rule_id, key)
    SELECT NEW.id, r.id, k.key
    FROM cmdb_model_unique_rule r
    CROSS JOIN LATERAL (SELECT cmdb_unique_key(NEW.attrs, r.field_codes) AS key) k
    WHERE r.model_code = NEW.model_code AND k.key IS NOT NULL
    ON CONFLICT (ci_id, rule_id) DO NOTHING;
    RETURN NULL;
END
$$;

CREATE TRIGGER trg_cmdb_ci_unique_insert AFTER INSERT ON cmdb_ci
    FOR EACH ROW EXECUTE FUNCTION cmdb_ci_unique_sync();
-- 只在影响键的列变化时触发：状态流转、同步心跳（last_synced_at / stale）等更新零开销。
CREATE TRIGGER trg_cmdb_ci_unique_update AFTER UPDATE OF attrs, delete_time, model_code ON cmdb_ci
    FOR EACH ROW
    WHEN (OLD.attrs IS DISTINCT FROM NEW.attrs OR OLD.delete_time IS DISTINCT FROM NEW.delete_time
          OR OLD.model_code IS DISTINCT FROM NEW.model_code)
    EXECUTE FUNCTION cmdb_ci_unique_sync();

-- 存量迁移：每个类型现有的唯一字段（未废弃）合为一条规则——字段组与旧唯一键相同。
-- 与旧口径的唯一差异是空值：旧口径跳过空字段、用其余字段比较；新口径任一字段为空即不约束
-- （只会变宽松，故回填不会因此失败）。
INSERT INTO cmdb_model_unique_rule (id, model_code, name, field_codes)
SELECT gen_random_uuid(), f.model_code,
       left(string_agg(f.name, ' + ' ORDER BY f.code COLLATE "C"), 64),
       array_agg(f.code::text ORDER BY f.code COLLATE "C")
FROM cmdb_model_field f
WHERE f."unique" AND NOT f.deprecated
GROUP BY f.model_code;

-- cmdb_model_field."unique" 自此为派生标记（= 被任一规则引用），由规则变更同事务维护。
UPDATE cmdb_model_field SET "unique" = false WHERE "unique" AND deprecated;

-- 存量规则可能不满足新建规则的限制（敏感字段、浮点 / 布尔 / JSON 类型、超过 5 个字段）：
-- 原有约束照常保留并生效，仅提示——这类规则可以改名 / 删除，改字段组时须满足新限制。
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT ur.model_code, ur.name FROM cmdb_model_unique_rule ur
        WHERE cardinality(ur.field_codes) > 5
           OR EXISTS (SELECT 1 FROM cmdb_model_field f
                      WHERE f.model_code = ur.model_code AND f.code = ANY (ur.field_codes)
                        AND (f.sensitive OR f.type IN ('float', 'bool', 'json')))
        ORDER BY 1, 2
    LOOP
        RAISE NOTICE 'cmdb unique rule "%" of model % was migrated as-is but exceeds the limits for new rules (sensitive / float / bool / json fields, or more than 5 fields)',
            r.name, r.model_code;
    END LOOP;
END
$$;

-- 回填前校验：旧唯一键在个别历史场景下可能漏判（如浮点值的文本形式不一致）。存量若已有
-- 重复，明确报错并列出样例，由运维先清理数据再迁移——不静默跳过（否则留下无键资源）。
DO $$
DECLARE
    dup record;
BEGIN
    SELECT r.model_code, r.name AS rule_name, string_agg(c.name, ', ' ORDER BY c.name) AS names
    INTO dup
    FROM cmdb_model_unique_rule r
    JOIN cmdb_ci c ON c.model_code = r.model_code AND c.delete_time IS NULL
    CROSS JOIN LATERAL (SELECT cmdb_unique_key(c.attrs, r.field_codes) AS key) k
    WHERE k.key IS NOT NULL
    GROUP BY r.model_code, r.id, r.name, k.key
    HAVING count(*) > 1
    LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'cmdb unique rule backfill: model % rule "%" has duplicate resources (%); deduplicate them before migrating',
            dup.model_code, dup.rule_name, dup.names;
    END IF;
END
$$;

INSERT INTO cmdb_ci_unique (ci_id, rule_id, key)
SELECT c.id, r.id, k.key
FROM cmdb_model_unique_rule r
JOIN cmdb_ci c ON c.model_code = r.model_code AND c.delete_time IS NULL
CROSS JOIN LATERAL (SELECT cmdb_unique_key(c.attrs, r.field_codes) AS key) k
WHERE k.key IS NOT NULL;

-- 旧唯一索引下线（新约束已生效）。cmdb_ci.unique_key / cmdb_sync_staging.unique_key 两列保留
-- 但不再读写（down 迁移据规则重算），待后续版本清理。
DROP INDEX IF EXISTS uq_cmdb_ci_model_unique_key;
COMMENT ON COLUMN cmdb_ci.unique_key IS '已停用（0049）：唯一约束见 cmdb_model_unique_rule / cmdb_ci_unique';
COMMENT ON COLUMN cmdb_model_field."unique" IS '派生标记：被任一唯一规则引用（由规则变更同事务维护）';

-- DB 角色：控制面维护规则与键；数据面只读规则、经触发器写键（触发器以调用者身份执行）。
GRANT SELECT, INSERT, UPDATE, DELETE ON cmdb_model_unique_rule, cmdb_ci_unique TO cmdb_core_role;
GRANT SELECT ON cmdb_model_unique_rule TO cmdb_sync_role;
GRANT SELECT, INSERT, DELETE ON cmdb_ci_unique TO cmdb_sync_role;

-- 暂存表归数据面角色所有（同 cmdb_ci_history / notify 运行表的做法）：数据面在对账前要对它
-- 做 ANALYZE（刚批量写入的暂存行没有统计信息，规划器会把行数估成 1、把集合语句排成逐行
-- 嵌套循环——两万行的对账因此从秒级退化到分钟级），而 ANALYZE 只有属主能做。
ALTER TABLE cmdb_sync_staging OWNER TO cmdb_sync_role;

-- 接口权限：已持有"更新类型字段"接口的角色补授唯一规则的写接口与预检（接口权限在保存
-- 授权时逐条物化，新增接口不会自动获得）；读取随 GetModel 返回，无需补授。
INSERT INTO system_permission (id, role_id, operation)
SELECT gen_random_uuid(), p.role_id, n.op
FROM system_permission p
CROSS JOIN (VALUES
    ('/cmdb.v1.CmdbModelService/CreateUniqueRule'),
    ('/cmdb.v1.CmdbModelService/UpdateUniqueRule'),
    ('/cmdb.v1.CmdbModelService/DeleteUniqueRule'),
    ('/cmdb.v1.CmdbModelService/CheckUniqueRule')) AS n(op)
WHERE p.operation = '/cmdb.v1.CmdbModelService/UpdateField'
ON CONFLICT (role_id, operation) DO NOTHING;

INSERT INTO system_grant (id, role_id, resource_type, resource_code)
SELECT gen_random_uuid(), g.role_id, 'api', md5(n.op)
FROM system_grant g
CROSS JOIN (VALUES
    ('/cmdb.v1.CmdbModelService/CreateUniqueRule'),
    ('/cmdb.v1.CmdbModelService/UpdateUniqueRule'),
    ('/cmdb.v1.CmdbModelService/DeleteUniqueRule'),
    ('/cmdb.v1.CmdbModelService/CheckUniqueRule')) AS n(op)
WHERE g.resource_type = 'api' AND g.resource_code = md5('/cmdb.v1.CmdbModelService/UpdateField')
ON CONFLICT (role_id, resource_type, resource_code) DO NOTHING;
