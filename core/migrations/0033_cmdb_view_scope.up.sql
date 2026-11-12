-- 0033_cmdb_view_scope.up.sql CMDB 域：详情页布局配置与角色数据范围。
-- cmdb_model_view：每种资源类型一行，sections jsonb 描述详情页的属性分组与
-- 关联面板（顺序、标题、展示列）；无行时前端按字段定义与适用关系自动生成。
-- cmdb_role_scope：角色可管理的资源类型 / 环境范围；任一启用角色未配置或为空
-- 即不限（现有角色缺省不限，行为与引入前一致）；超管恒不限。

CREATE TABLE cmdb_model_view (
    model_code  varchar(64) PRIMARY KEY REFERENCES cmdb_model (code),
    sections    jsonb       NOT NULL DEFAULT '[]',
    update_user uuid        NULL,
    update_time timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE cmdb_model_view IS 'CMDB 资源类型详情页布局：属性分组 + 关联面板';
COMMENT ON COLUMN cmdb_model_view.sections IS '[{type:attrs,title,fields[]} | {type:relation,relationType,direction,title,columns[],collapsed}]';

CREATE TABLE cmdb_role_scope (
    role_id     uuid        PRIMARY KEY,
    model_codes text[]      NOT NULL DEFAULT '{}',
    envs        text[]      NOT NULL DEFAULT '{}',
    update_user uuid        NULL,
    update_time timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE cmdb_role_scope IS 'CMDB 角色数据范围：可见的资源类型与环境；空 = 不限';
COMMENT ON COLUMN cmdb_role_scope.role_id IS '角色 ID（不设外键：角色删除后范围行无害，解析时按启用角色读取）';
