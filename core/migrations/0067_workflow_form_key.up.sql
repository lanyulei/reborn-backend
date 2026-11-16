-- 0067 流程实例：所用表单的标识（节点上按表单区分的字段权限据此取用）。
ALTER TABLE workflow_instance ADD COLUMN form_key varchar(64) NOT NULL DEFAULT '';
COMMENT ON COLUMN workflow_instance.form_key IS '实例所用表单的标识：发起时由业务域给出（工单为模板 ID），子流程继承根流程的；空 = 只用通用的字段权限';

-- 存量：工单发起的流程（及其子流程）补上工单的模板 ID。
UPDATE workflow_instance i
SET form_key = o.template_id::text
FROM ticket_order o
WHERE i.root_business_type = 'ticket' AND i.root_business_id = o.id::text;
