-- 回退内置内容的默认接线，再删除内置模板、分类与流程。已被工单 / 流程实例引用的内置内容不删（外键，
-- 也确实还在用），其余照常回退。
UPDATE job_policy SET approval_workflow_key = '', version = version + 1, update_time = now()
WHERE approval_workflow_key = 'builtin_approval_admin';

UPDATE k8s_setting SET value = '', update_time = now()
WHERE (name = 'namespace_request_template_id' AND value = '018f3e1a-0ce4-7000-8000-000000000011')
	OR (name = 'permission_request_template_id' AND value = '018f3e1a-0ce4-7000-8000-000000000012');

DELETE FROM ticket_template WHERE builtin AND id = ANY (ARRAY['018f3e1a-0ce4-7000-8000-000000000001', '018f3e1a-0ce4-7000-8000-000000000002', '018f3e1a-0ce4-7000-8000-000000000003', '018f3e1a-0ce4-7000-8000-000000000004', '018f3e1a-0ce4-7000-8000-000000000005', '018f3e1a-0ce4-7000-8000-000000000006', '018f3e1a-0ce4-7000-8000-000000000007', '018f3e1a-0ce4-7000-8000-000000000008', '018f3e1a-0ce4-7000-8000-000000000009', '018f3e1a-0ce4-7000-8000-000000000010', '018f3e1a-0ce4-7000-8000-000000000011', '018f3e1a-0ce4-7000-8000-000000000012']::uuid[])
	AND NOT EXISTS (SELECT 1 FROM ticket_order o WHERE o.template_id = ticket_template.id);

DELETE FROM ticket_category WHERE builtin AND id = ANY (ARRAY['018f3e1a-0ce3-7000-8000-000000000001', '018f3e1a-0ce3-7000-8000-000000000002', '018f3e1a-0ce3-7000-8000-000000000003', '018f3e1a-0ce3-7000-8000-000000000004']::uuid[])
	AND NOT EXISTS (SELECT 1 FROM ticket_template t WHERE t.category_id = ticket_category.id AND t.delete_time IS NULL);

DELETE FROM workflow_definition_version WHERE id = ANY (ARRAY['018f3e1a-0ce2-7000-8000-000000000001', '018f3e1a-0ce2-7000-8000-000000000002', '018f3e1a-0ce2-7000-8000-000000000003', '018f3e1a-0ce2-7000-8000-000000000004', '018f3e1a-0ce2-7000-8000-000000000005', '018f3e1a-0ce2-7000-8000-000000000006']::uuid[])
	AND NOT EXISTS (SELECT 1 FROM workflow_instance i WHERE i.version_id = workflow_definition_version.id);

DELETE FROM workflow_definition WHERE builtin AND id = ANY (ARRAY['018f3e1a-0ce1-7000-8000-000000000001', '018f3e1a-0ce1-7000-8000-000000000002', '018f3e1a-0ce1-7000-8000-000000000003', '018f3e1a-0ce1-7000-8000-000000000004', '018f3e1a-0ce1-7000-8000-000000000005', '018f3e1a-0ce1-7000-8000-000000000006']::uuid[])
	AND NOT EXISTS (SELECT 1 FROM workflow_instance i WHERE i.definition_id = workflow_definition.id)
	AND NOT EXISTS (SELECT 1 FROM workflow_definition_version v WHERE v.definition_id = workflow_definition.id);
