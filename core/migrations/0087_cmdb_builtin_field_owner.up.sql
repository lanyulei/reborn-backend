-- 0087 内置类型的采集字段：归属与缺失字段（只动 cmdb_model_field 的种子行，不改表结构）。
--
-- 背景：0032 预置内置字段时没有指定 owner，全部按缺省 'manual' 落库；而数据面对 manual 归属的字段
-- 一律不写（cmdb 数据面 stagingSink.Put）。结果是采集器写入 cloud_host / physical_server /
-- virtual_machine 的 provider、instance_id、内外网 IP 等被静默丢弃：同步进来的资源 attrs 为空，
-- 必填字段全部被质量看板计为缺必填。
--
-- 1) 采集器固定写入的内置字段改为 both（同步可写，人工仍可编辑——存量人工维护的数据与操作习惯不变）。
--    只改「仍是 manual」的行：管理员已经调成 sync / both 的保持原样；已废弃的字段不动。
-- 2) 采集器会输出、内置类型却没有定义的属性补上字段（三朵云的规格 / 状态，vCenter 主机的型号 / UUID，
--    虚拟机的电源状态），否则这些属性同样被静默丢弃。不进列表（list_visible = false），不影响现有列表布局；
--    同编码字段已存在（管理员自建）则不重复插入。
-- 3) 回滚过（本迁移的 down 把补的字段废弃）再升级：这些字段恢复可用。
--
-- 幂等：全部语句可重复执行。

-- 1)
UPDATE cmdb_model_field
   SET owner = 'both', update_time = now()
 WHERE owner = 'manual'
   AND deprecated = false
   AND (model_code, code) IN (
       ('cloud_host',       'provider'),
       ('cloud_host',       'instance_id'),
       ('cloud_host',       'region'),
       ('cloud_host',       'private_ip'),
       ('cloud_host',       'public_ip'),
       ('physical_server',  'ip'),
       ('physical_server',  'serial_number'),
       ('physical_server',  'vendor'),
       ('physical_server',  'cpu_cores'),
       ('physical_server',  'memory_gb'),
       ('virtual_machine',  'ip'),
       ('virtual_machine',  'os'),
       ('virtual_machine',  'vcpu'),
       ('virtual_machine',  'memory_gb'));

-- 2)
INSERT INTO cmdb_model_field (id, model_code, code, name, type, required, "unique", enum_values, ref_model, rule, owner, list_visible, sort, description)
SELECT gen_random_uuid(), v.model_code, v.code, v.name, v.type, false, false, NULL, '', v.rule::jsonb, 'both', false, v.sort, v.description
  FROM (VALUES
    ('cloud_host',      'zone',         '可用区',       'string', '{"maxLen":64}',    32, '云厂商可用区'),
    ('cloud_host',      'instance_type','实例规格',     'string', '{"maxLen":64}',    34, '云厂商实例规格'),
    ('cloud_host',      'cpu_cores',    'CPU 核数',     'int',    '{"min":1,"max":1024}', 60, ''),
    ('cloud_host',      'memory_gb',    '内存（GB）',   'int',    '{"min":1}',         70, ''),
    ('cloud_host',      'os',           '操作系统',     'string', '{"maxLen":128}',   80, ''),
    ('cloud_host',      'power_state',  '电源状态',     'string', '{"maxLen":32}',    90, '云厂商返回的实例状态原文'),
    ('physical_server', 'model',        '型号',         'string', '{"maxLen":128}',   35, ''),
    ('physical_server', 'uuid',         '硬件 UUID',    'string', '{"maxLen":128}',   65, ''),
    ('virtual_machine', 'power_state',  '电源状态',     'string', '{"maxLen":32}',    50, 'on / off / suspended')
  ) AS v(model_code, code, name, type, rule, sort, description)
 WHERE EXISTS (SELECT 1 FROM cmdb_model m WHERE m.code = v.model_code)
   AND NOT EXISTS (
     SELECT 1 FROM cmdb_model_field f WHERE f.model_code = v.model_code AND f.code = v.code);

-- 3) 名称、类型、说明与本迁移插入时完全一致，且仍是非必填 / 非唯一 / 不进列表的 both 字段，只是被废弃了：
--    管理员改过任何一项都不匹配。
UPDATE cmdb_model_field f
   SET deprecated = false, update_time = now()
  FROM (VALUES
    ('cloud_host', 'zone', '可用区', 'string', '云厂商可用区'),
    ('cloud_host', 'instance_type', '实例规格', 'string', '云厂商实例规格'),
    ('cloud_host', 'cpu_cores', 'CPU 核数', 'int', ''),
    ('cloud_host', 'memory_gb', '内存（GB）', 'int', ''),
    ('cloud_host', 'os', '操作系统', 'string', ''),
    ('cloud_host', 'power_state', '电源状态', 'string', '云厂商返回的实例状态原文'),
    ('physical_server', 'model', '型号', 'string', ''),
    ('physical_server', 'uuid', '硬件 UUID', 'string', ''),
    ('virtual_machine', 'power_state', '电源状态', 'string', 'on / off / suspended')
  ) AS v(model_code, code, name, type, description)
 WHERE f.model_code = v.model_code AND f.code = v.code AND f.name = v.name AND f.type = v.type
   AND f.description = v.description
   AND f.deprecated AND f.owner = 'both' AND f.required = false AND f."unique" = false AND f.list_visible = false;
