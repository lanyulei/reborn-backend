-- 0087 回滚。
--
-- 1) 本迁移新补的字段：废弃（与字段管理里的「废弃」同一语义），不删行。同步进来的资源 attrs 里可能已经有
--    这些键，而 (model_code, code) 唯一，删行没有意义；废弃后不再进表单 / 校验 / 唯一规则。
--    只处理「与插入时完全一致」的行（名称、类型、说明相同，仍是非必填 / 非唯一 / 不进列表的 both 字段）：
--    管理员之后编辑过的字段保持现状。识别方式与 up 的第 3) 步对称，回滚后重新升级即恢复。
-- 2) 归属不回滚：'both' 对旧版本代码同样合法（旧数据面只是不写 manual 字段），改回 manual 会让采集在
--    回滚后重新丢数据。管理员如需调整，可在字段管理里自行修改。

UPDATE cmdb_model_field f
   SET deprecated = true, update_time = now()
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
   AND NOT f.deprecated AND f.owner = 'both' AND f.required = false AND f."unique" = false AND f.list_visible = false;
