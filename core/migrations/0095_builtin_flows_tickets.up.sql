-- 内置流程（6 个）、工单分类（4 个）与工单模板（12 个）：开箱即用。
--
-- 全部按固定 UUIDv7 预置、builtin = true：内置的可以修改（模板、流程还可以停用），不能删除；后续版本按
-- 固定 ID 升级内置内容。重复执行安全：标识 / 名称已被占用（用户自建）或已经存在时不动。
-- 流程图与随图保存的设计器结构（graph.design）由前端设计器的数据模型生成，打开即可可视化编辑。
-- 审批人一律是「系统管理员」规则（不依赖任何账号、角色、部门），新装机也能走通。
--
-- 新装机默认接线（只对「库里只有预置的超管一个用户」的库生效，存量库的行为不变）：
--   作业审批流程指向内置的管理员审批；K8s 自助申请（命名空间 / 权限）指向内置模板。

-- ---------------------------------------------------------------------------
-- 流程定义：先写定义（草稿 = 流程图），再由草稿生成版本 1。标识被占用时两条都不写。
-- ---------------------------------------------------------------------------
-- builtin_approval_admin：管理员审批
INSERT INTO workflow_definition (id, key, name, category, description, status, draft, revision, published_version,
	published_version_id, create_time, update_time, builtin)
SELECT '018f3e1a-0ce1-7000-8000-000000000001', 'builtin_approval_admin', '管理员审批', '审批', '由系统管理员（超级管理员）审批，一步。K8s 变更审批、权限申请等内置模板用它，作业审批默认也用它。找不到审批人时流程停住，不会自动通过。', 1, $flow${"nodes":[{"id":"start","type":"start","name":"发起"},{"id":"approve","type":"userTask","name":"系统管理员审批","assignee":{"mode":"any","onEmpty":"block","rules":[{"kind":"superAdmin"}]}},{"id":"end","type":"end","name":"结束"}],"edges":[{"id":"e_start_approve","source":"start","target":"approve"},{"id":"e_approve_end","source":"approve","target":"end"}],"settings":{"admins":[]},"design":{"nodes":[{"kind":"task","id":"approve","name":"系统管理员审批","assignee":{"mode":"any","onEmpty":"block"}}],"admins":[],"slim":1}}$flow$, 1, 1,
	'018f3e1a-0ce2-7000-8000-000000000001', now(), now(), true
WHERE NOT EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_approval_admin' AND delete_time IS NULL);
INSERT INTO workflow_definition_version (id, definition_id, version, graph, create_time)
SELECT '018f3e1a-0ce2-7000-8000-000000000001', '018f3e1a-0ce1-7000-8000-000000000001', 1, draft, now() FROM workflow_definition
	WHERE id = '018f3e1a-0ce1-7000-8000-000000000001' AND published_version_id = '018f3e1a-0ce2-7000-8000-000000000001'
	AND NOT EXISTS (SELECT 1 FROM workflow_definition_version WHERE id = '018f3e1a-0ce2-7000-8000-000000000001');

-- builtin_approval_leader：部门负责人审批
INSERT INTO workflow_definition (id, key, name, category, description, status, draft, revision, published_version,
	published_version_id, create_time, update_time, builtin)
SELECT '018f3e1a-0ce1-7000-8000-000000000002', 'builtin_approval_leader', '部门负责人审批', '审批', '由发起人所在部门的负责人审批；发起人没有部门负责人时由系统管理员审批。', 1, $flow${"nodes":[{"id":"start","type":"start","name":"发起"},{"id":"approve","type":"userTask","name":"部门负责人审批","assignee":{"mode":"any","onEmpty":"block","rules":[{"kind":"deptLeader","level":1},{"kind":"superAdmin"}]}},{"id":"end","type":"end","name":"结束"}],"edges":[{"id":"e_start_approve","source":"start","target":"approve"},{"id":"e_approve_end","source":"approve","target":"end"}],"settings":{"admins":[]},"design":{"nodes":[{"kind":"task","id":"approve","name":"部门负责人审批","assignee":{"mode":"any","onEmpty":"block"}}],"admins":[],"slim":1}}$flow$, 1, 1,
	'018f3e1a-0ce2-7000-8000-000000000002', now(), now(), true
WHERE NOT EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_approval_leader' AND delete_time IS NULL);
INSERT INTO workflow_definition_version (id, definition_id, version, graph, create_time)
SELECT '018f3e1a-0ce2-7000-8000-000000000002', '018f3e1a-0ce1-7000-8000-000000000002', 1, draft, now() FROM workflow_definition
	WHERE id = '018f3e1a-0ce1-7000-8000-000000000002' AND published_version_id = '018f3e1a-0ce2-7000-8000-000000000002'
	AND NOT EXISTS (SELECT 1 FROM workflow_definition_version WHERE id = '018f3e1a-0ce2-7000-8000-000000000002');

-- builtin_request_admin：管理员审批并办理
INSERT INTO workflow_definition (id, key, name, category, description, status, draft, revision, published_version,
	published_version_id, create_time, update_time, builtin)
SELECT '018f3e1a-0ce1-7000-8000-000000000003', 'builtin_request_admin', '管理员审批并办理', '申请', '系统管理员审批后由管理员办理，适用于账号开通、权限申请这类由平台管理员处理的申请。', 1, $flow${"nodes":[{"id":"start","type":"start","name":"发起"},{"id":"approve","type":"userTask","name":"系统管理员审批","assignee":{"mode":"any","onEmpty":"block","rules":[{"kind":"superAdmin"}]}},{"id":"handle","type":"userTask","name":"管理员办理","assignee":{"mode":"any","onEmpty":"block","rules":[{"kind":"superAdmin"}]}},{"id":"end","type":"end","name":"结束"}],"edges":[{"id":"e_start_approve","source":"start","target":"approve"},{"id":"e_approve_handle","source":"approve","target":"handle"},{"id":"e_handle_end","source":"handle","target":"end"}],"settings":{"admins":[]},"design":{"nodes":[{"kind":"task","id":"approve","name":"系统管理员审批","assignee":{"mode":"any","onEmpty":"block"}},{"kind":"task","id":"handle","name":"管理员办理","assignee":{"mode":"any","onEmpty":"block"}}],"admins":[],"slim":1}}$flow$, 1, 1,
	'018f3e1a-0ce2-7000-8000-000000000003', now(), now(), true
WHERE NOT EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_request_admin' AND delete_time IS NULL);
INSERT INTO workflow_definition_version (id, definition_id, version, graph, create_time)
SELECT '018f3e1a-0ce2-7000-8000-000000000003', '018f3e1a-0ce1-7000-8000-000000000003', 1, draft, now() FROM workflow_definition
	WHERE id = '018f3e1a-0ce1-7000-8000-000000000003' AND published_version_id = '018f3e1a-0ce2-7000-8000-000000000003'
	AND NOT EXISTS (SELECT 1 FROM workflow_definition_version WHERE id = '018f3e1a-0ce2-7000-8000-000000000003');

-- builtin_request_leader：部门负责人审批并办理
INSERT INTO workflow_definition (id, key, name, category, description, status, draft, revision, published_version,
	published_version_id, create_time, update_time, builtin)
SELECT '018f3e1a-0ce1-7000-8000-000000000004', 'builtin_request_leader', '部门负责人审批并办理', '申请', '部门负责人审批（没有负责人时由系统管理员审批），再由管理员办理。', 1, $flow${"nodes":[{"id":"start","type":"start","name":"发起"},{"id":"approve","type":"userTask","name":"部门负责人审批","assignee":{"mode":"any","onEmpty":"block","rules":[{"kind":"deptLeader","level":1},{"kind":"superAdmin"}]}},{"id":"handle","type":"userTask","name":"管理员办理","assignee":{"mode":"any","onEmpty":"block","rules":[{"kind":"superAdmin"}]}},{"id":"end","type":"end","name":"结束"}],"edges":[{"id":"e_start_approve","source":"start","target":"approve"},{"id":"e_approve_handle","source":"approve","target":"handle"},{"id":"e_handle_end","source":"handle","target":"end"}],"settings":{"admins":[]},"design":{"nodes":[{"kind":"task","id":"approve","name":"部门负责人审批","assignee":{"mode":"any","onEmpty":"block"}},{"kind":"task","id":"handle","name":"管理员办理","assignee":{"mode":"any","onEmpty":"block"}}],"admins":[],"slim":1}}$flow$, 1, 1,
	'018f3e1a-0ce2-7000-8000-000000000004', now(), now(), true
WHERE NOT EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_request_leader' AND delete_time IS NULL);
INSERT INTO workflow_definition_version (id, definition_id, version, graph, create_time)
SELECT '018f3e1a-0ce2-7000-8000-000000000004', '018f3e1a-0ce1-7000-8000-000000000004', 1, draft, now() FROM workflow_definition
	WHERE id = '018f3e1a-0ce1-7000-8000-000000000004' AND published_version_id = '018f3e1a-0ce2-7000-8000-000000000004'
	AND NOT EXISTS (SELECT 1 FROM workflow_definition_version WHERE id = '018f3e1a-0ce2-7000-8000-000000000004');

-- builtin_task_handle：任务处理
INSERT INTO workflow_definition (id, key, name, category, description, status, draft, revision, published_version,
	published_version_id, create_time, update_time, builtin)
SELECT '018f3e1a-0ce1-7000-8000-000000000005', 'builtin_task_handle', '任务处理', '处理', '不审批，由系统管理员直接处理，适用于咨询、服务请求与复盘改进任务。', 1, $flow${"nodes":[{"id":"start","type":"start","name":"发起"},{"id":"handle","type":"userTask","name":"处理","assignee":{"mode":"any","onEmpty":"block","rules":[{"kind":"superAdmin"}]}},{"id":"end","type":"end","name":"结束"}],"edges":[{"id":"e_start_handle","source":"start","target":"handle"},{"id":"e_handle_end","source":"handle","target":"end"}],"settings":{"admins":[]},"design":{"nodes":[{"kind":"task","id":"handle","name":"处理","assignee":{"mode":"any","onEmpty":"block"}}],"admins":[],"slim":1}}$flow$, 1, 1,
	'018f3e1a-0ce2-7000-8000-000000000005', now(), now(), true
WHERE NOT EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_task_handle' AND delete_time IS NULL);
INSERT INTO workflow_definition_version (id, definition_id, version, graph, create_time)
SELECT '018f3e1a-0ce2-7000-8000-000000000005', '018f3e1a-0ce1-7000-8000-000000000005', 1, draft, now() FROM workflow_definition
	WHERE id = '018f3e1a-0ce1-7000-8000-000000000005' AND published_version_id = '018f3e1a-0ce2-7000-8000-000000000005'
	AND NOT EXISTS (SELECT 1 FROM workflow_definition_version WHERE id = '018f3e1a-0ce2-7000-8000-000000000005');

-- builtin_event_handle：事件处置
INSERT INTO workflow_definition (id, key, name, category, description, status, draft, revision, published_version,
	published_version_id, create_time, update_time, builtin)
SELECT '018f3e1a-0ce1-7000-8000-000000000006', 'builtin_event_handle', '事件处置', '处理', '受理、处理、确认恢复三步，由系统管理员处理。告警转工单、故障转工单，以及告警规则的「触发流程」、故障处置手册都可以用它。', 1, $flow${"nodes":[{"id":"start","type":"start","name":"发起"},{"id":"accept","type":"userTask","name":"受理","assignee":{"mode":"any","onEmpty":"block","rules":[{"kind":"superAdmin"}]}},{"id":"handle","type":"userTask","name":"处理","assignee":{"mode":"any","onEmpty":"block","rules":[{"kind":"superAdmin"}]}},{"id":"confirm","type":"userTask","name":"确认恢复","assignee":{"mode":"any","onEmpty":"block","rules":[{"kind":"superAdmin"}]}},{"id":"end","type":"end","name":"结束"}],"edges":[{"id":"e_start_accept","source":"start","target":"accept"},{"id":"e_accept_handle","source":"accept","target":"handle"},{"id":"e_handle_confirm","source":"handle","target":"confirm"},{"id":"e_confirm_end","source":"confirm","target":"end"}],"settings":{"admins":[]},"design":{"nodes":[{"kind":"task","id":"accept","name":"受理","assignee":{"mode":"any","onEmpty":"block"}},{"kind":"task","id":"handle","name":"处理","assignee":{"mode":"any","onEmpty":"block"}},{"kind":"task","id":"confirm","name":"确认恢复","assignee":{"mode":"any","onEmpty":"block"}}],"admins":[],"slim":1}}$flow$, 1, 1,
	'018f3e1a-0ce2-7000-8000-000000000006', now(), now(), true
WHERE NOT EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_event_handle' AND delete_time IS NULL);
INSERT INTO workflow_definition_version (id, definition_id, version, graph, create_time)
SELECT '018f3e1a-0ce2-7000-8000-000000000006', '018f3e1a-0ce1-7000-8000-000000000006', 1, draft, now() FROM workflow_definition
	WHERE id = '018f3e1a-0ce1-7000-8000-000000000006' AND published_version_id = '018f3e1a-0ce2-7000-8000-000000000006'
	AND NOT EXISTS (SELECT 1 FROM workflow_definition_version WHERE id = '018f3e1a-0ce2-7000-8000-000000000006');

-- ---------------------------------------------------------------------------
-- 工单分类：名称已被用户占用时不建，内置模板改挂到同名的分类下。
-- ---------------------------------------------------------------------------
INSERT INTO ticket_category (id, name, description, sort, builtin)
SELECT '018f3e1a-0ce3-7000-8000-000000000001', '运维申请', '账号、权限、资源等日常申请', 10, true
WHERE NOT EXISTS (SELECT 1 FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000001')
	AND NOT EXISTS (SELECT 1 FROM ticket_category WHERE name = '运维申请' AND delete_time IS NULL);

INSERT INTO ticket_category (id, name, description, sort, builtin)
SELECT '018f3e1a-0ce3-7000-8000-000000000002', '变更管理', '生产环境变更的申请与审批', 20, true
WHERE NOT EXISTS (SELECT 1 FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000002')
	AND NOT EXISTS (SELECT 1 FROM ticket_category WHERE name = '变更管理' AND delete_time IS NULL);

INSERT INTO ticket_category (id, name, description, sort, builtin)
SELECT '018f3e1a-0ce3-7000-8000-000000000003', '故障管理', '事件与故障的处理、复盘改进项', 30, true
WHERE NOT EXISTS (SELECT 1 FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000003')
	AND NOT EXISTS (SELECT 1 FROM ticket_category WHERE name = '故障管理' AND delete_time IS NULL);

INSERT INTO ticket_category (id, name, description, sort, builtin)
SELECT '018f3e1a-0ce3-7000-8000-000000000004', '容器平台', 'Kubernetes 集群的变更审批与自助申请', 40, true
WHERE NOT EXISTS (SELECT 1 FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000004')
	AND NOT EXISTS (SELECT 1 FROM ticket_category WHERE name = '容器平台' AND delete_time IS NULL);

-- ---------------------------------------------------------------------------
-- 工单模板：绑定的内置流程可用才建；名称已被用户占用时不建。三张容器平台模板的字段码是
-- 容器平台模块自动填单的约定，且一律不必填。
-- ---------------------------------------------------------------------------
-- 通用审批申请（编号前缀 BAPPROV）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000001',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000001' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '运维申请' AND delete_time IS NULL LIMIT 1)),
	'通用审批申请', '需要留痕的审批：由部门负责人审批，没有负责人时由系统管理员审批。', 'builtin_approval_leader', 'BAPPROV', $form$[{"code":"summary","name":"事由","type":"string","required":true,"rule":{"maxLen":128},"placeholder":"一句话说明要审批的事项"},{"code":"detail","name":"详细说明","type":"text","required":true,"rule":{"maxLen":4000}},{"code":"link","name":"相关链接","type":"string","rule":{"maxLen":255},"format":"url"},{"code":"expect_time","name":"期望完成时间","type":"date","rule":{},"description":"不填表示无明确期限"}]$form$, 1, 10, now(), '[]'::jsonb, '[]'::jsonb, 0, '',
	2, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_approval_leader' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '通用审批申请' AND delete_time IS NULL);

-- 账号开通申请（编号前缀 BACCOUNT）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000002',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000001' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '运维申请' AND delete_time IS NULL LIMIT 1)),
	'账号开通申请', '开通系统或平台账号：系统管理员审批后由管理员开通。', 'builtin_request_admin', 'BACCOUNT', $form$[{"code":"account_name","name":"姓名","type":"string","required":true,"rule":{"maxLen":64}},{"code":"dept","name":"所属部门","type":"dept","required":true,"rule":{}},{"code":"role_needed","name":"需要的角色与权限","type":"text","required":true,"rule":{"maxLen":2000}},{"code":"reason","name":"申请事由","type":"text","required":true,"rule":{"maxLen":2000}},{"code":"expire_time","name":"使用期限","type":"date","rule":{},"description":"不填表示长期有效"}]$form$, 1, 20, now(), '[]'::jsonb, '[]'::jsonb, 0, '',
	2, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_request_admin' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '账号开通申请' AND delete_time IS NULL);

-- 权限申请（编号前缀 BACCESS）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000003',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000001' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '运维申请' AND delete_time IS NULL LIMIT 1)),
	'权限申请', '申请某个系统、集群或项目的权限：系统管理员审批后由管理员开通。', 'builtin_request_admin', 'BACCESS', $form$[{"code":"target","name":"申请对象","type":"string","required":true,"rule":{"maxLen":128},"placeholder":"系统、集群或项目的名称"},{"code":"permission_needed","name":"需要的权限","type":"text","required":true,"rule":{"maxLen":2000}},{"code":"reason","name":"申请事由","type":"text","required":true,"rule":{"maxLen":2000}},{"code":"expire_time","name":"使用期限","type":"date","rule":{},"description":"不填表示长期有效"}]$form$, 1, 30, now(), '[]'::jsonb, '[]'::jsonb, 0, '',
	2, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_request_admin' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '权限申请' AND delete_time IS NULL);

-- 资源开通申请（编号前缀 BRES）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000004',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000001' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '运维申请' AND delete_time IS NULL LIMIT 1)),
	'资源开通申请', '申请云主机、数据库、域名等资源：部门负责人审批后由管理员开通。', 'builtin_request_leader', 'BRES', $form$[{"code":"resource_type","name":"资源类型","type":"enum","required":true,"enumValues":["cloud_host","virtual_machine","physical_server","database","storage","domain","certificate","other"],"rule":{},"enumLabels":{"cloud_host":"云主机","virtual_machine":"虚拟机","physical_server":"物理机","database":"数据库","storage":"存储","domain":"域名","certificate":"证书","other":"其他"}},{"code":"spec","name":"规格与数量","type":"text","required":true,"rule":{"maxLen":2000}},{"code":"purpose","name":"用途","type":"text","required":true,"rule":{"maxLen":2000}},{"code":"expect_time","name":"期望完成时间","type":"date","rule":{},"description":"不填表示无明确期限"}]$form$, 1, 40, now(), '[]'::jsonb, '[]'::jsonb, 0, '',
	2, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_request_leader' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '资源开通申请' AND delete_time IS NULL);

-- 咨询 / 服务请求（编号前缀 BSRVREQ）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000005',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000001' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '运维申请' AND delete_time IS NULL LIMIT 1)),
	'咨询 / 服务请求', '咨询问题或提出服务需求：由系统管理员解答、处理。', 'builtin_task_handle', 'BSRVREQ', $form$[{"code":"summary","name":"问题或需求","type":"text","required":true,"rule":{"maxLen":4000}},{"code":"expect_time","name":"期望完成时间","type":"date","rule":{},"description":"不填表示无明确期限"}]$form$, 1, 50, now(), '[]'::jsonb, '[]'::jsonb, 0, '',
	1, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_task_handle' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '咨询 / 服务请求' AND delete_time IS NULL);

-- 值班调整申请（编号前缀 BONCALL）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000006',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000001' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '运维申请' AND delete_time IS NULL LIMIT 1)),
	'值班调整申请', '换班、请假或替班：部门负责人审批后由管理员调整值班表。', 'builtin_request_leader', 'BONCALL', $form$[{"code":"adjust_type","name":"调整类型","type":"enum","required":true,"enumValues":["swap","leave","cover"],"rule":{},"enumLabels":{"swap":"换班","leave":"请假","cover":"替班"}},{"code":"shift_time","name":"涉及的班次","type":"string","required":true,"rule":{"maxLen":128},"placeholder":"如：11 月 28 日 09:00–18:00"},{"code":"counterpart","name":"换班对象","type":"user","rule":{},"description":"换班时填写"},{"code":"reason","name":"事由","type":"text","required":true,"rule":{"maxLen":2000}}]$form$, 1, 60, now(), '[]'::jsonb, '[]'::jsonb, 0, '',
	2, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_request_leader' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '值班调整申请' AND delete_time IS NULL);

-- 生产变更申请（编号前缀 BCHANGE）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000007',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000002' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '变更管理' AND delete_time IS NULL LIMIT 1)),
	'生产变更申请', '生产环境的变更：部门负责人审批，没有负责人时由系统管理员审批。', 'builtin_approval_leader', 'BCHANGE', $form$[{"code":"summary","name":"变更内容","type":"text","required":true,"rule":{"maxLen":4000}},{"code":"change_window","name":"变更窗口","type":"string","required":true,"rule":{"maxLen":128},"placeholder":"如：11 月 28 日 22:00–23:00"},{"code":"risk_level","name":"风险等级","type":"enum","required":true,"enumValues":["low","medium","high"],"rule":{},"enumLabels":{"low":"低","medium":"中","high":"高"}},{"code":"rollback_plan","name":"回滚方案","type":"text","required":true,"rule":{"maxLen":4000}}]$form$, 1, 70, now(), '[]'::jsonb, '[]'::jsonb, 1, '',
	2, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_approval_leader' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '生产变更申请' AND delete_time IS NULL);

-- K8s 变更审批（编号前缀 BKCHANGE）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000008',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000002' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '变更管理' AND delete_time IS NULL LIMIT 1)),
	'K8s 变更审批', '集群上写操作的审批工单，由容器平台在发起变更时自动创建，不需要手动提单。', 'builtin_approval_admin', 'BKCHANGE', $form$[{"code":"summary","name":"一句话说明","type":"string","rule":{"maxLen":255},"placeholder":"一句话说明要做什么"},{"code":"cluster","name":"集群","type":"string","rule":{"maxLen":255}},{"code":"namespace","name":"命名空间","type":"string","rule":{"maxLen":255}},{"code":"target","name":"目标对象","type":"string","rule":{"maxLen":255},"description":"资源类型与名称，如 Deployment/default/nginx"},{"code":"detail","name":"详细内容","type":"text","rule":{"maxLen":4000}},{"code":"link","name":"变更单链接","type":"string","rule":{"maxLen":255}},{"code":"reason","name":"申请理由","type":"text","rule":{"maxLen":4000}}]$form$, 1, 80, now(), '[]'::jsonb, '[]'::jsonb, 0, '',
	2, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_approval_admin' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = 'K8s 变更审批' AND delete_time IS NULL);

-- 事件工单（编号前缀 BEVENT）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000009',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000003' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '故障管理' AND delete_time IS NULL LIMIT 1)),
	'事件工单', '告警或故障的处理工单：受理、处理、确认恢复。可从告警或故障页面转入，也可以手动提单。', 'builtin_event_handle', 'BEVENT', $form$[{"code":"summary","name":"问题描述","type":"text","rule":{"maxLen":4000}},{"code":"impact","name":"影响范围","type":"text","rule":{"maxLen":2000}}]$form$, 1, 90, now(), '[]'::jsonb, '[]'::jsonb, 1, '',
	1, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_event_handle' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '事件工单' AND delete_time IS NULL);

-- 任务 / 改进项（编号前缀 BTASK）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000010',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000003' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '故障管理' AND delete_time IS NULL LIMIT 1)),
	'任务 / 改进项', '需要跟进的任务或复盘改进项：由系统管理员处理，可从复盘的改进项转入。', 'builtin_task_handle', 'BTASK', $form$[{"code":"summary","name":"任务内容","type":"text","rule":{"maxLen":4000}},{"code":"due_time","name":"截止时间","type":"date","rule":{}}]$form$, 1, 100, now(), '[]'::jsonb, '[]'::jsonb, 0, '',
	1, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_task_handle' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '任务 / 改进项' AND delete_time IS NULL);

-- 申请命名空间与配额（编号前缀 BKNSREQ）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000011',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000004' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '容器平台' AND delete_time IS NULL LIMIT 1)),
	'申请命名空间与配额', '在集群上申请命名空间与资源配额：系统管理员审批后由平台自动创建。', 'builtin_approval_admin', 'BKNSREQ', $form$[{"code":"summary","name":"一句话说明","type":"string","rule":{"maxLen":255},"placeholder":"一句话说明要做什么"},{"code":"cluster","name":"集群","type":"string","rule":{"maxLen":255}},{"code":"namespace","name":"命名空间","type":"string","rule":{"maxLen":255}},{"code":"target","name":"目标对象","type":"string","rule":{"maxLen":255},"description":"资源类型与名称，如 Deployment/default/nginx"},{"code":"detail","name":"详细内容","type":"text","rule":{"maxLen":4000}},{"code":"link","name":"变更单链接","type":"string","rule":{"maxLen":255}},{"code":"reason","name":"申请理由","type":"text","rule":{"maxLen":4000}}]$form$, 1, 110, now(), '[]'::jsonb, '[]'::jsonb, 0, '',
	2, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_approval_admin' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '申请命名空间与配额' AND delete_time IS NULL);

-- 申请集群权限（编号前缀 BKACCESS）
INSERT INTO ticket_template (id, category_id, name, description, workflow_key, number_prefix, form, status, sort,
	update_time, sla, sla_escalate, ci_link, icon, form_columns, builtin)
SELECT '018f3e1a-0ce4-7000-8000-000000000012',
	COALESCE((SELECT id FROM ticket_category WHERE id = '018f3e1a-0ce3-7000-8000-000000000004' AND delete_time IS NULL),
	(SELECT id FROM ticket_category WHERE name = '容器平台' AND delete_time IS NULL LIMIT 1)),
	'申请集群权限', '申请集群或命名空间的访问权限：系统管理员审批后由平台自动授权。', 'builtin_approval_admin', 'BKACCESS', $form$[{"code":"summary","name":"一句话说明","type":"string","rule":{"maxLen":255},"placeholder":"一句话说明要做什么"},{"code":"cluster","name":"集群","type":"string","rule":{"maxLen":255}},{"code":"namespace","name":"命名空间","type":"string","rule":{"maxLen":255}},{"code":"target","name":"目标对象","type":"string","rule":{"maxLen":255},"description":"资源类型与名称，如 Deployment/default/nginx"},{"code":"detail","name":"详细内容","type":"text","rule":{"maxLen":4000}},{"code":"link","name":"变更单链接","type":"string","rule":{"maxLen":255}},{"code":"reason","name":"申请理由","type":"text","rule":{"maxLen":4000}}]$form$, 1, 120, now(), '[]'::jsonb, '[]'::jsonb, 0, '',
	2, true
WHERE EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_approval_admin' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM ticket_template WHERE name = '申请集群权限' AND delete_time IS NULL);

-- ---------------------------------------------------------------------------
-- 新装机默认接线（存量库不动）
-- ---------------------------------------------------------------------------
UPDATE job_policy SET approval_workflow_key = 'builtin_approval_admin', version = version + 1, update_time = now()
WHERE approval_workflow_key = '' AND EXISTS (SELECT 1 FROM workflow_definition WHERE key = 'builtin_approval_admin' AND status = 1
		AND published_version_id IS NOT NULL AND delete_time IS NULL)
	AND NOT EXISTS (SELECT 1 FROM "system_user" WHERE delete_time IS NULL AND id <> '01a1b987-129c-7186-9a25-92bd159a765f');

WITH fresh AS (
	SELECT NOT EXISTS (SELECT 1 FROM "system_user" WHERE delete_time IS NULL AND id <> '01a1b987-129c-7186-9a25-92bd159a765f') AS yes
)
INSERT INTO k8s_setting (name, value, update_time)
SELECT 'namespace_request_template_id', '018f3e1a-0ce4-7000-8000-000000000011', now() FROM fresh
WHERE fresh.yes AND EXISTS (SELECT 1 FROM ticket_template WHERE id = '018f3e1a-0ce4-7000-8000-000000000011' AND delete_time IS NULL AND status = 1)
ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, update_time = now() WHERE k8s_setting.value = '';

WITH fresh AS (
	SELECT NOT EXISTS (SELECT 1 FROM "system_user" WHERE delete_time IS NULL AND id <> '01a1b987-129c-7186-9a25-92bd159a765f') AS yes
)
INSERT INTO k8s_setting (name, value, update_time)
SELECT 'permission_request_template_id', '018f3e1a-0ce4-7000-8000-000000000012', now() FROM fresh
WHERE fresh.yes AND EXISTS (SELECT 1 FROM ticket_template WHERE id = '018f3e1a-0ce4-7000-8000-000000000012' AND delete_time IS NULL AND status = 1)
ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, update_time = now() WHERE k8s_setting.value = '';
