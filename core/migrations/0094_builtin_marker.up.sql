-- 内置标记：流程定义 / 工单分类 / 工单模板三张表加 builtin。内置的可以修改（模板、流程还可以停用），
-- 不能删除；后续版本按固定 ID 升级内置内容。做法与 system_role.builtin（0025）、cmdb_model.builtin
-- （0032）相同：缺省 false，只有迁移预置的行才置 true；接口创建的一律是 false。
-- 只加列且带缺省值：滚动发布期间旧版本实例照常读写（PG 11+ 常量缺省值不改写存量行）。

ALTER TABLE workflow_definition ADD COLUMN builtin boolean NOT NULL DEFAULT false;
ALTER TABLE ticket_category ADD COLUMN builtin boolean NOT NULL DEFAULT false;
ALTER TABLE ticket_template ADD COLUMN builtin boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN workflow_definition.builtin IS '是否内置流程（迁移预置）：可修改、停用，不可删除';
COMMENT ON COLUMN ticket_category.builtin IS '是否内置分类（迁移预置）：可修改，不可删除';
COMMENT ON COLUMN ticket_template.builtin IS '是否内置模板（迁移预置）：可修改、停用，不可删除';
