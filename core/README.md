# monitor-backend / core

用户管理中心服务（**单服务多域 / 模块化单体**路线，模块路径 `github.com/lanyulei/reborn/backend/core`）：
用户管理、角色管理与用户-角色绑定（一个用户可绑定多个角色）、认证授权、系统配置与登录审计。
后续新业务域（如 workflow）在同一服务内以 `internal/<layer>/<domain>/` 子包接入，不拆分部署。

基于 [Kratos v3](https://github.com/go-kratos/kratos)。
本仓库的行为契约见工作区共享的 [AGENTS.md](../notify/AGENTS.md)，优先级：稳定性 > 安全性 > 正确性 > 性能 > 可维护性 > 交付速度。

## 技术栈

| 项 | 选型 |
| --- | --- |
| 语言/运行时 | Go 1.26 |
| 框架 | Kratos v3（HTTP、Wire DI、OTel 可观测；gRPC 已停用，proto 保留可随时生成） |
| IDL | Protobuf + Buf v2（`buf.yaml` / `buf.gen.yaml`） |
| DI | google/wire（编译期注入，禁止运行时 DI） |
| 数据库 | PostgreSQL（pgx v5 连接池，手写参数化 SQL，无 ORM） |
| 密码存储 | Argon2id（PHC 格式，参数走配置并校验 OWASP 下限） |
| 日志 | 标准库 log/slog，经 Kratos `log.NewLogger` 包装，JSON 输出、统一脱敏 |
| 可观测 | OpenTelemetry（tracing 走 `contrib/otel/v3`，未配置 Provider 时自动降级 noop） |
| 校验 | protovalidate（规则集中在 `.proto` 的 `(buf.validate.field)` 注解） |

## 目录结构

```
.
├── api/system/v1            # Protobuf 契约（system 域：User/Role/Auth/SystemConfig/LoginLog 五个 Service + ErrorReason）
├── buf.yaml                 # Buf v2 工作区：api + internal + third_party
├── buf.gen.yaml             # api 生成配置（go / go-http / go-errors / openapiv2；gRPC 插件已停用）
├── buf.gen.config.yaml      # internal/conf 生成配置
├── cmd/server               # 入口：main.go + wire（仅装配，无业务）
├── configs/config.yaml      # 配置文件（密钥一律环境变量注入）
├── migrations/              # SQL 迁移脚本（手动执行）
└── internal/
    ├── conf/                # 配置结构（conf.proto → conf.pb.go）
    ├── server/              # HTTP Server 构造、中间件、编解码（跨域共享）
    ├── service/
    │   ├── service.go       # service 层聚合 ProviderSet（各域在此追加）
    │   └── system/          # system 域 proto 接口实现（参数编排 + 模型转换）
    ├── biz/
    │   ├── biz.go           # biz 层聚合 ProviderSet（各域在此追加）
    │   └── system/          # system 域业务核心（领域模型 + Repo 接口 + 用例）
    ├── data/
    │   ├── data.go          # 跨域共享连接池 Data（域子包引用，字段导出）
    │   └── system/          # system 域 Repo 实现（pgx + PostgreSQL，唯一允许 import 驱动的层）
    └── testfake/            # 仓储内存实现（仅供单测组装链路）
```

依赖方向（只允许单向向下）：`api → service → biz → data`。

**分域规则（单服务多域 / 模块化单体）**：api 与 internal 三层（service/biz/data）均按
`<layer>/<domain>/` 子包分域，子包包名保持层名（biz/data/service）不变；server/conf 为跨域
共享设施不分会。域内代码同包互引不加前缀，跨域引用加别名（如 `basedata`、`bizsystem`）。
跨域协作走依赖倒置：消费方域在 biz 定义窄接口，另一域实现；禁止跨域 JOIN（表归属唯一）。
注意 data 层因域子包需引用顶层共享的 `Data` 类型，ProviderSet 不在顶层聚合（避免 import 环），
各域 data ProviderSet 在 `cmd/server/wire.go` 显式列出；biz/service 顶层正常聚合。

## 数据模型（库 reborn，表前缀 system_，单数）

| 表 | 说明 |
| --- | --- |
| `system_user` | 用户：id(UUIDv7)/username/password(Argon2id)/nickname/email/phone/gender/is_admin/status（1=可用 2=停用 3=暂停 4=离职） |
| `system_role` | 角色：id/name/code(不可改)/status |
| `system_user_role` | 绑定关系：id/user_id/role_id + create_time/create_user（物理删除） |
| `system_login_log` | 登录日志（append-only 审计）：id/user_id(可空，无外键)/username/result（1=成功 2=用户不存在 3=密码错误 4=账号禁用 5=签发令牌失败）/ip/user_agent/create_time |
| `system_dept` | 部门（组织架构树，软删）：id/parent_id/path（物化路径，层级 ≤10）/name（同一上级下唯一）/code（可空，唯一）/sort/status（1=启用 0=停用）/description；总数 ≤2000 |
| `system_dept_leader` | 部门负责人（一个部门可多人，按 sort 排序，物理删除）：id/dept_id/user_id/sort；负责人不要求是本部门成员 |
| `system_user_dept` | 用户所属部门（一个用户可属多个部门，至多一个主部门，物理删除）：id/user_id/dept_id/is_primary；单用户 ≤20 个部门 |

- 业务表软删除（`delete_time` 置位），唯一约束（username/email/phone/code）只约束未删除行；
- 关联表仅保留 create_time/create_user，解绑即物理删除；
- 审计字段：`create_time, update_time, delete_time, create_user, update_user, delete_user`
  （关联表仅前两者中的 create_time/create_user）。操作者 ID 由鉴权后的 `operatorMiddleware`
  注入中性上下文 `internal/pkg/operator`（data 层 `operator.FromContext` 读取，不触碰 claims）：
  notify 域六张配置表的写路径已落库；system_permission/system_grant/system_user_role 写路径
  尚未接入（create_user 仍为 NULL，待补）。

## 快速开始

```bash
# 1. 安装依赖与锁定版本的工具链
go mod download
make init        # golangci-lint / govulncheck / gofumpt

# 2. 初始化数据库（库 reborn 需已存在）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0001_system_init.up.sql     # 系统域建表（用户/角色/关联）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0002_refresh_token.up.sql   # 认证域建表（刷新令牌）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0009_login_log.up.sql       # 认证域建表（登录日志）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0010_notify_channel.up.sql # 通知域建表（渠道 + 配置版本）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0011_notify_template.up.sql # 通知域建表（模板）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0012_notify_route.up.sql  # 通知域建表（路由）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0013_notify_silence.up.sql  # 通知域建表（静默）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0014_notify_subscription.up.sql  # 通知域建表（订阅）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0015_notify_producer.up.sql  # 通知域建表（生产者凭据）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0016_notify_event.up.sql  # 通知域建表（事件运行表，按月分区）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0017_notify_dispatch.up.sql  # 通知域建表（发送任务运行表，按月分区）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0018_notify_delivery_log.up.sql  # 通知域建表（投递日志运行表，按月分区）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0019_notify_dlq.up.sql  # 通知域建表（死信表，量小不分区）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0020_notify_default_partitions.up.sql  # 三张分区运行表补 DEFAULT 分区兜底
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0021_notify_pipeline.up.sql  # 管道服务运行表（dedup/聚合组/令牌桶）+ DB 角色矩阵
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0022_notify_pipeline_comments.up.sql  # 运行表列注释对齐 PG 骨干语义
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0023_notify_tenancy.up.sql  # 角色 biz_scope / 生产者配额 / 机器人签名密钥 / 配额与健康表
# 0001 内置超级管理员：admin / Admin123!（生产环境部署后应立即改密或停用）
# 推荐用 make 目标代替逐个 psql（golang-migrate，脚本随二进制 embed，DSN 读 configs/config.yaml，
# 版本记录在 schema_migrations 表，执行期 pg_advisory_lock 互斥，可重复运行）：
make migrate                     # 应用全部未执行版本
make migrate-version             # 当前版本与 dirty 状态
make migrate-down                # 回退最近一个版本
# 已手工执行到 0023 的旧库先设基线（只写版本号不执行 SQL），之后 make migrate 只跑新增脚本（如 0024）
make migrate-force VERSION=23
# 某个脚本执行失败会把版本标为 dirty：修复数据库后 make migrate-force VERSION=<失败前的版本> 再重跑

# 3. 生成代码（修改 .proto 或 provider 后必须执行）
make gen

# 4. 本地运行（make run = kratos run 指向 cmd/server；cmd/ 下另有 migrate 子命令，裸 kratos run 会交互询问）
make run

# 5. 静态检查 + 安全扫描（提交前必须全绿）
golangci-lint run ./...
govulncheck ./...

# 6. 测试（data 集成测试需 TEST_DATABASE_SOURCE 指向专用测试库；未设置时自动跳过）
#    集成测试会 TRUNCATE 全部业务表——测试入口有护栏：库名不含 "test" 直接失败，
#    严禁指向 configs/config.yaml 的开发库（曾因此清空过开发数据）。
createdb -h 127.0.0.1 -U postgres reborn_test
for f in migrations/*.up.sql; do psql -h 127.0.0.1 -U postgres -d reborn_test -f "$f"; done
export TEST_DATABASE_SOURCE="host=127.0.0.1 user=postgres password=<你的密码> dbname=reborn_test port=5432 sslmode=disable"
go test ./internal/biz/...  -race -count=1
go test ./internal/data/... -race -count=1 -tags=integration -p 1   # -p 1：system/notify 两包共用同一测试库且都会 TRUNCATE，须串行
go test ./...               -race -shuffle=on
```

## 配置与密钥

- 配置按三层合并，后者覆盖前者：`../kit/bootstrap/shared.yaml`（全部服务共用的基线：数据库连接、
  凭据加密主密钥，编译进二进制）→ `configs/config.yaml`（本服务特有的段）→ `KRATOS_` 前缀环境变量
  （占位符 `${FOO_BAR}` 的取值，如 `KRATOS_DATA_DATABASE_SOURCE`）。
- 口令与密钥只走环境变量，**禁止提交进仓库**：本地写在 `backend/.env`（不入库，模板
  `backend/.env.example`，各服务 Makefile 自动读取），部署时对应一份共享的 Secret。
- 必须与其它服务一致的密钥（主密钥、与容器平台数据面的内部签名密钥）在启动时核对：core 把指纹登记进
  `system_key_check`（迁移 0079，首次写入为准），其余服务比对，不一致直接启动失败。轮换密钥时先停服务、
  删除对应行，再用新密钥启动 core。
- 日志侧由 `log.WithFilter(log.FilterKey("password", "token", "secret", "authorization"))`
  在 handler 构建处统一脱敏。
- 通知渠道凭据主密钥经 `KRATOS_NOTIFY_MASTER_KEY` 注入（config.yaml 占位
  `${NOTIFY_MASTER_KEY}`；`openssl rand -hex 32` 生成）。AES-256-GCM 加密落库；
  **留空时服务照常启动**，仅在写入带 secret 的渠道处报 `NOTIFY_SECRET_UNAVAILABLE`
  （不阻断未启用通知能力的部署），webhook 等 secret 渠道不受影响。
- 容器平台 kube 代理入口的认证失败限速以来源地址为键。缺省（及 `KRATOS_TRUSTED_PROXIES` 未设置时）对端是
  回环 / 私网地址就采信 `X-Forwarded-For`——部署在会改写该头的反向代理后面时，直连的调用方可以伪造它换限速键。
  经环境变量 `KRATOS_TRUSTED_PROXIES` 配置可信代理清单（逗号分隔的 CIDR 或 IP，如 `10.0.0.0/8,192.168.1.10`）
  后，只有 TCP 对端属于清单时才采信该头，其余一律用对端地址。只在启动时读取；清单里没有有效条目时按未配置
  处理并打 WARN。

## API 概览（HTTP）

```
用户：
  POST   /api/v1/system/user                     创建用户
  PUT    /api/v1/system/user/{id}                更新资料（可选字段；username 不可改）
  POST   /api/v1/system/user/{id}/status         变更状态（1=可用 2=停用 3=暂停 4=离职）
  POST   /api/v1/system/user/{id}/password       管理员重置密码
  DELETE /api/v1/system/user/{id}                删除（软删，级联清绑定）
  GET    /api/v1/system/user/{id}                详情（绝不返回密码）
  GET    /api/v1/system/user                     分页列表（keyword/status 过滤，page_size ≤ 100）
  PUT    /api/v1/system/user/{id}/role          全量覆盖角色绑定（空数组清空）
  GET    /api/v1/system/user/{id}/role          查询用户角色
  守卫（service 层，身份取 claims）：授予/撤销 is_admin、停用/删除/重置超管账号须由超管操作（否则 403
  PERMISSION_DENIED）；不能停用/删除自己、不能改自己的 is_admin（400 USER_SELF_OPERATION）；
  重置密码/停用/撤销超管后吊销该用户全部刷新令牌，停用账号的 access token 立即失去授权。

部门（组织架构；迁移 0056。树结构写——新建/移动/删除——经同一把事务级 advisory lock 跨实例串行）：
  POST   /api/v1/system/dept                     创建部门（可带 parent_id / code / sort / leader_ids；同一上级下名称唯一，层级 ≤10）
  PUT    /api/v1/system/dept/{id}                更新（可选字段；leaders 出现即整组替换负责人，空列表 = 清空；返回更新后的实体）
  POST   /api/v1/system/dept/{id}/move           调整上级（连同整棵子树；不传 parent_id = 移为根；移到自身/下级之下 400 DEPT_MOVE_INVALID）
  DELETE /api/v1/system/dept/{id}                删除（软删；仍有下级/成员返回 409 DEPT_HAS_CHILDREN / DEPT_HAS_MEMBERS）
  GET    /api/v1/system/dept/{id}                详情（含负责人与直属成员数）
  GET    /api/v1/system/dept                     全部部门（平铺不分页，上级在前；前端按 parent_id 组树）
  POST   /api/v1/system/dept/{id}/member         添加成员（已在部门内的跳过；用户尚无主部门时该部门成为主部门）
  GET    /api/v1/system/dept/{id}/member         分页查询成员（最小披露：不含邮箱/手机号；include_sub_depts 含下级；keyword 只匹配登录名/昵称）
  GET    /api/v1/system/dept-user-candidate      可选用户（可用用户的 ID 与显示名，≤50；供选负责人/加成员，不需要用户管理权限）
  DELETE /api/v1/system/dept/{id}/member/{user_id}  移出成员（幂等；移出主部门时最早加入的其余部门顶替）
  用户侧：创建/更新用户可带 depts{dept_ids, primary_dept_id}（与资料写同事务整组替换；不传 = 不改）；
  用户详情/列表返回 depts（主部门在前）；列表支持 dept_id + include_sub_depts 过滤。删除用户同事务清除其部门归属与负责人身份。

通用附件（file 域；迁移 0057。元数据表 file_object，内容经存储驱动存取：local（本地磁盘 <upload.dir>/files，缺省）
  或 s3（S3 兼容对象存储：AWS S3 / MinIO / 阿里云 OSS 的 S3 兼容端点；手写 SigV4，无第三方 SDK）。
  `internal/pkg/storage.ObjectStore` 为驱动接口，新增驱动只需在 data/file.NewStores 登记。
  切换：upload.driver 置为 s3 并填 upload.s3（endpoint / region / bucket / path_style / prefix），凭据经
  KRATOS_DATA_UPLOAD_S3_ACCESS_KEY / KRATOS_DATA_UPLOAD_S3_SECRET_KEY 注入；配置不全装配阶段快速失败。桶须事先建好。
  每个文件记录自己的驱动，切换只影响新上传的文件，旧文件仍按原驱动读取（不迁移存量，两边的存储都要保留）：
  POST   /api/v1/file                            上传（JSON：filename + Base64 data，≤5MiB；类型按内容嗅探并校验白名单，不含 HTML/SVG；
                                               单用户未绑定的临时文件 ≤100 个，超出 429 FILE_QUOTA_EXCEEDED）
  GET    /api/v1/file/{id}                       文件元数据
  GET    /api/v1/file/{id}/content               下载（恒以附件方式返回：attachment + nosniff + CSP sandbox，不进共享缓存）
  三个接口免接口授权但须登录；访问控制按归属判定：未绑定的临时文件仅上传者本人可读，已绑定文件由所属业务域登记的
  AccessChecker 判定（biz.NewFileAccessCheckers，未登记的业务类型一律拒绝）。业务域在自己的写事务里调
  bizfile.Usecase.Bind/Release 绑定/释放附件。后台循环 file-purge 每 10 分钟清理超过 24h 未绑定的临时文件与
  软删超过 24h 的文件（SKIP LOCKED 领取，多实例安全；本地驱动多实例须共享卷）。

流程引擎（workflow 域；迁移 0058。通用引擎，不认识任何上层业务：实例只记 business_type + business_id + 变量）：
  流程定义（草稿可反复保存，发布生成不可变版本；实例绑定发起时的版本）：
  POST   /api/v1/workflow/definition                       创建（key 创建后不可改；草稿为开始/结束骨架）
  PUT    /api/v1/workflow/definition/{id}                  更新名称/分类/说明/启停
  PUT    /api/v1/workflow/definition/{id}/draft            保存草稿流程图（可未完成；revision 乐观锁；响应带校验问题清单）
  POST   /api/v1/workflow/definition/{id}/publish          发布（草稿须通过校验；revision 须与所见一致）
  DELETE /api/v1/workflow/definition/{id}                  删除（软删；有进行中的实例返回 409）
  GET    /api/v1/workflow/definition[/{id}]                列表 / 详情（含草稿与校验问题）
  GET    /api/v1/workflow/definition/{id}/version[/{n}]    版本列表 / 某版本的流程图
  流程实例与任务：
  POST   /api/v1/workflow/instance                         发起独立流程（发起部门缺省取主部门，发起时固化；可带 request_id 幂等键，重复提交返回首次发起的实例）
  GET    /api/v1/workflow/instance/{id}                    实例详情（流程图 + 任务 + 流转历史；参与者可看，持 ListInstances 权限者可看全部）
  GET    /api/v1/workflow/my-instance | /instance          我发起的 / 全部（管理视图）
  POST   /api/v1/workflow/instance/{id}/cancel|terminate   发起人撤回 / 管理员终止（原因必填）
  GET    /api/v1/workflow/task/todo|done|cc                我的待办 / 已办 / 抄送
  POST   /api/v1/workflow/task/{id}/approve|reject|return|transfer|reassign   同意 / 拒绝 / 退回 / 转办 / 管理员改派
  GET    /api/v1/workflow/task/{id}/return-target          可退回到的节点
  GET    /api/v1/workflow/user-candidate                   可选用户（最小披露）
  流程图 DSL（internal/biz/workflow/graph.go）：节点 start / end / userTask / cc / exclusiveGateway / parallelGateway；
  处理人规则 user / role / initiator / deptLeader(level) / deptLeaderOf / formUser（排除发起人本人）/ formDeptLeader，多人模式 any（或签）/
  all（会签）/ sequential（依次）；分支条件为 CEL（变量 form / initiator / initiatorDept，有成本上限，求值出错视为不命中走默认分支）。
  一致性：每条写命令 = 一个事务（实例行 FOR UPDATE → 状态机推进 → 回写 → 通知事件直插 → 业务监听器），失败整体回滚；
  同一实例的命令因行锁串行，重复提交返回 409 WORKFLOW_TASK_NOT_PENDING。上限：单实例 500 个任务、单次推进 200 步。
  同一业务对象同一时刻至多一个进行中的流程，结束后可再次发起。回写只写变化的执行位置 / 任务；事务内的只读查询
  （定义、组织目录）经 txrun.NewContext 复用事务连接，一条命令只占一条数据库连接。
  发布校验要求并行网关成严格的块结构（分叉 / 汇聚一一配对、分支互不相交、连线不得穿越并行分支边界），
  否则执行位置会等不齐或提前合并。
  接入方式（进程内）：业务域在自己的写事务里调 RuntimeUsecase.StartTx 发起；在 biz.NewWorkflowListeners 登记监听器，
  实例结束时同事务回调。通知事件类型 workflow.task.assigned / workflow.task.cc / workflow.instance.finished
  实例结束时同事务回调；在 biz.NewWorkflowViewers 登记查看权限，让业务侧的管理视图也能看流程进度。
  通知事件类型 workflow.task.assigned / workflow.task.cc / workflow.instance.finished
  （biz = default，显式收件人；投递需在通知管理里为这些事件类型配置路由与模板）。

进程角色（可选的分开部署；缺省单进程承担全部角色，行为与以往一致）：
  同一份程序按角色启动：api = 对外提供业务接口；worker = 运行后台循环（值班延展 / 提醒 / 汇总 / 巡检、
  附件清理、流程作业与清理、工单 SLA 巡检）。配置 server.roles，或环境变量 SERVER_ROLES=api / worker / api,worker
  （与 cmdb、notify 服务同名同语义）；角色名写错则启动失败。两种角色连同一个数据库、用同一个版本，
  后台循环处理每件事仍是一个本地事务（领取均为 SKIP LOCKED，多实例不重复），数据一致性与单进程部署相同。
  注意：
  - 至少要有一个进程承担 worker 角色，否则定时节点、任务超时、自动任务、SLA 通知、值班提醒都不会发生且不报错；
  - 只承担 worker 角色的进程不注册任何业务接口，只保留 /healthz 与 /metrics；
  - 数据库连接总数 = 各进程数 × data.database.max_conns，须小于 PostgreSQL 的 max_connections；
  - 发布时两种角色一起升级（共用表结构）；停机宽限期应大于 15 秒（自动任务单次调用的上限）。
  探活与指标（任何角色都有；只应对集群内的探针 / 采集器开放，不要经网关暴露到公网）：
  GET /healthz   数据库连通返回 200 ok，否则 503
  GET /metrics   Prometheus 指标：core_loop_runs_total{loop,result} / core_loop_items_total{loop}（各后台循环的
                 迭代次数与处理条数）、core_workflow_jobs_overdue（到期超过一分钟仍待执行的流程作业数）、
                 core_workflow_jobs_overdue_seconds（其中最早一条拖了多久）、core_workflow_jobs_dead（死信数）
  建议告警：core_workflow_jobs_overdue > 0 持续 5 分钟（worker 没起来或处理不过来）；
  core_loop_runs_total{result="error"} 持续增长；core_workflow_jobs_dead > 0（有作业等人工处理）。

流程引擎增强（迁移 0060 / 0062）：
  节点：inclusiveGateway（包容网关：走全部条件成立的分支，都不成立走默认分支；须与汇聚成对）、timer（定时 / 等待：
  延时、等到表单里的某个时刻、等外部信号）、serviceTask（自动任务：调用已登记的 Webhook 端点）、subProcess（子流程）；
  userTask 可配 timeout（处理时限：仅提醒 / 自动同意 / 自动拒绝 / 转交流程管理员，可提前提醒）与
  formPermissions（字段码 → hidden / readonly / editable / required，由上层业务域解释）。
  超时「自动同意」与人工同意走同一道业务校验：业务域不放行（如工单本环节必填的字段还空着）时不自动同意，改为提醒。
  作业队列 workflow_job：定时到期、任务超时、自动任务、父子流程衔接都排成作业，由后台执行器领取
  （SKIP LOCKED + 租约，多实例安全；每条作业只锁自己所属的实例；作业天然幂等）。两个循环分开领取：
  workflow-job 逐条执行引擎内部作业，workflow-service 并发（上限 4）执行自动任务——外部端点再慢也不会
  挡住定时与超时。自动任务在事务外调用端点，
  投递语义为至少一次（请求头 X-Workflow-Job-Id 供接收端去重；配置了密钥时带 X-Workflow-Signature =
  sha256=HMAC-SHA256(secret, 时间戳.请求体)），失败退避重试，耗尽进死信并通知流程管理员，管理员可重试或跳过。
  端点只能是 https、不跟随跳转，连接前对解析出的真实地址做内网校验；地址与密钥在 workflow_webhook 里维护
  （密钥 AES-256-GCM 加密，与通知渠道共用主密钥），流程图里只引用端点 ID。
  子流程：在父流程的事务里发起，继承发起人 / 发起部门 / 变量；父子之间只经作业衔接（子流程结束唤醒父流程、
  父流程结束终止子流程），任何事务都只锁一个实例。嵌套深度上限 5，发布时拒绝互相调用成环。
  子流程自己不挂业务对象，但带着根流程的引用（root_id / root_business_type / root_business_id）：子流程里
  的待办按业务类型能查到、其处理人算作业务对象的参与者，字段权限与同意前校验照常生效；业务域的
  「实例结束」监听只对顶层流程触发。父子实例各有一份变量（发起时拷贝，之后互不同步）。
  协作：加签（前 / 后）、委派（对方只给意见，决定权回到自己）、代理设置（一段时间内新到的待办交给代理人，
  只转一层）、交接（把某人名下全部未了结的待办换给另一人）。
  工作日历（全局一份）：时区、每周工作日、每日工作时段、节假日与调休；按工作时间计的时限据此推算。
  POST   /api/v1/workflow/task/{id}/add-sign|delegate              加签 / 委派
  POST   /api/v1/workflow/instance/{id}/signal                     投递外部信号（可带数据并入流程变量）
  POST   /api/v1/workflow/instance/{id}/job/{job_id}/retry|skip    重试死信作业 / 跳过定时或自动任务节点
  POST   /api/v1/workflow/handover                                 交接待办（一次至多 100 个实例，more=true 时再调）
  POST   /api/v1/workflow/definition/{id}/simulate                 模拟运行（内存里走一遍，不落库、不通知、不外调）
  GET|PUT /api/v1/workflow/calendar                                工作日历
  GET|POST|DELETE /api/v1/workflow/delegation[/{id}]               我的代理设置
  GET|POST|PUT|DELETE /api/v1/workflow/webhook[/{id}]              Webhook 端点
  GET|POST|PUT|DELETE /api/v1/workflow/client[/{id}]、POST …/{id}/token   接入凭据（令牌明文只在创建 / 重置时返回一次）
  开放接口（供其他系统接入；不走用户登录，请求头 Authorization: Bearer <接入令牌>；库内只存令牌的 SHA-256）：
  POST   /api/v1/open/workflow/instance                    发起（只能发起凭据允许的流程；以凭据的代表用户为发起人；建议带 request_id）
  GET    /api/v1/open/workflow/instance/{id}               查询（只能查自己发起的）
  POST   /api/v1/open/workflow/instance/{id}/signal|cancel 投递信号 / 撤回
  新增通知事件类型 workflow.task.remind（超时提醒）/ workflow.service.failed（作业进入死信）。

工单（ticket 域；迁移 0059 / 0061。流转完全交给流程引擎，本域只管服务目录、表单数据、评论与由流程同步来的状态）：
  GET    /api/v1/ticket/catalog                            可提单的服务目录：只含启用的模板
  GET    /api/v1/ticket/managed-catalog                    完整的服务目录（含已停用的模板；管理页用，持此权限者才能读已停用模板的详情）
  POST|PUT|DELETE /api/v1/ticket/category[/{id}]           分类维护（分类下仍有模板时删除返回 409）
  POST|PUT|DELETE|GET /api/v1/ticket/template[/{id}]       模板维护 / 详情（表单字段 + 绑定的流程标识 + 编号前缀）
  POST   /api/v1/ticket/order                              提单（可带附件 file_ids、发起部门、request_id 幂等键）
  GET    /api/v1/ticket/order/{id}                         详情（表单快照 + 数据 + 附件 + 人员 / 部门显示名）
  GET    /api/v1/ticket/my-order | /api/v1/ticket/order    我提交的 / 全部（管理视图）
  GET|POST /api/v1/ticket/order/{id}/comment               评论（只追加，单张工单至多 500 条；单人每分钟至多 30 条，超出返回 429）
  待办 / 已办、同意 / 拒绝 / 退回 / 转办 / 撤回直接用流程引擎的任务接口（列表带 business_type=ticket，business_id 即工单 ID）。
  表单字段类型 string / text / int / float / bool / enum / datetime / user / dept，校验复用 internal/pkg/formschema；
  流程变量 = 表单数据 + priority（条件表达式写 form.<字段码> / form.priority）。
  一致性：提单在一个事务内完成「校验表单 → 取编号 → 落工单 → 绑定附件 → 发起流程」，任一步失败整体回滚；
  工单状态由流程引擎在实例结束的同一事务里回写。编号 <前缀>-<yyyymmdd>-<4 位序号>，按前缀 + 日期行锁自增，回滚不留空号。
  工单保存提单时的表单定义快照，模板之后的修改不影响已有工单。
  可见性：提单人、流程参与者（处理人 / 抄送人）、持「全部工单」权限者；附件与流程进度的可见性跟随工单。
  SLA：模板按优先级配置响应 / 解决时限（可按工作时间计，用流程引擎的工作日历），提单时算出并固化；
  处理人第一次有实质动作即「已响应」（由流程引擎同事务回写）。后台每分钟巡检，超时的工单各通知一次
  当前处理人与模板配置的升级对象（事件类型 ticket.sla.breached；多实例不重复通知）。
  字段权限：流程节点的 formPermissions 决定处理人在该环节能改哪些字段（PUT /api/v1/ticket/order/{id}/form，
  改动同事务同步到流程变量）、哪些必须填了才能同意、哪些不给看（只是处理人 / 抄送人时不返回；提单人与管理视图看全部）。
  关联资源：模板可要求提单时关联 CMDB 资源（可选 / 必填）；只能关联提单人有权查看的资源，名称存快照。
  满意度：工单结束后提单人评价一次（POST /api/v1/ticket/order/{id}/rating）。
  统计：GET /api/v1/ticket/stats?days=30（数量与分布、每日趋势、平均处理时长、SLA 达成率、满意度）。

角色：
  POST   /api/v1/system/role                     创建角色（可带 biz_scope：可管理的通知 biz 列表，空=不限）
  PUT    /api/v1/system/role/{id}                更新（name/status/biz_scope 整组替换；code 不可改）
  DELETE /api/v1/system/role/{id}                删除（软删，级联清绑定）
  GET    /api/v1/system/role/{id}                详情
  GET    /api/v1/system/role                     分页列表

登录日志（登录链路自动落库，此处仅审计检索）：
  GET    /api/v1/system/login-log                分页列表（username 模糊/result/时间范围过滤，page_size ≤ 100）

通知域接口按 biz 隔离：角色 biz_scope 为空（现有角色缺省）或超管 = 不限；受限用户的创建/改/删/查按目标 biz
  判定（403 NOTIFY_BIZ_FORBIDDEN），列表须带 biz_id（任务/明细/死信须带 channel_id；400 NOTIFY_BIZ_REQUIRED）。

通知渠道（notify 域；一行 = 一个可独立发送/限流/启停的端点实例，secret 只写不读）：
  POST   /api/v1/notify/channel                  创建（type 与 config 结构匹配；email/机器人必传 secret，webhook 可选；
                                               dingtalk/feishu 可带 sign_secret 启用加签/签名校验）
  PUT    /api/v1/notify/channel/{id}             部分更新（type 不可改；secret 不传=保留旧密文，传新值=轮换）
  DELETE /api/v1/notify/channel/{id}             删除（软删，名称可复用；仍被未删除路由引用返回 409 NOTIFY_CHANNEL_IN_USE）
  GET    /api/v1/notify/channel/{id}             详情（不含 secret）
  GET    /api/v1/notify/channel                  分页列表（keyword 模糊/type/enabled 过滤，page_size ≤ 100）
  GET    /api/v1/notify/channel-health           渠道健康（共享熔断状态只读视图：打开/探测中/60s 滑窗样本与失败数；迁移 0031 授 SELECT；受限用户须带 biz_id）
  （六类配置资源的 PUT 更新均返回更新后的实体，免二次 GET；路由/静默 spec 新增 name/description，keyword 同时匹配 name；
  webhook URL 拒绝本机/私网字面量（SSRF，管道连接时再校验解析地址）；rate_limit 新增 per_minute（qps=0 时生效，
  两者皆 0 按类型缺省：钉钉/企微 20/min、飞书 100/min）；模板可用函数 fmtTime/severity/json/trunc/upper/lower）
  POST   /api/v1/notify/channel/{id}:test        发送测试事件（须启用；直插 source=internal 事件，类型/标题/收件/载荷可覆盖，
                                                   同一渠道 10s 冷却，过频 429 NOTIFY_TEST_TOO_FREQUENT；
                                               缺省 notify.test；投递结果经事件/任务/明细查询异步跟踪，链路随管道 P1 生效）

通知模板（notify 域；biz × event_type × channel_type 三维覆盖，'*' 通配，渲染解析链精确 > 通配）：
  POST   /api/v1/notify/template                 创建（Go template 语法 + 变量白名单写入校验；组合唯一）
  PUT    /api/v1/notify/template/{id}            部分更新（恒 version+1；spec 可改，组合仍须唯一）
  DELETE /api/v1/notify/template/{id}            删除（软删，组合可复用）
  GET    /api/v1/notify/template/{id}            详情
  POST   /api/v1/notify/template:render          模板预览（只读：按管道口径渲染给定样例，白名单函数/顶层变量严格/载荷宽松/≤64KB；可带 format）
  模板 format（迁移 0037）：text（缺省）/ markdown / html / card；html 走 html/template 自动转义并发 multipart/alternative 邮件；
  渠道按能力降级：email 只认 html，群机器人 card→卡片（钉钉 actionCard / 企微 template_card / 飞书 interactive，需渠道 config.card_url；
  钉钉/企微缺 card_url 降级 markdown）、markdown/html→markdown、text→各平台既有格式；webhook 透传 body.format。
  GET    /api/v1/notify/template                 分页列表（keyword 模糊/eventType/channelType/enabled 过滤，page_size ≤ 100）

通知路由（notify 域；事件→通知任务的分发决策：模式+严重级+标签匹配，多路由重叠由 priority 决胜）：
  POST   /api/v1/notify/route                    创建（channel_ids 须为同 biz 未删除渠道；调优项缺省 1/5/0/500/30）
  PUT    /api/v1/notify/route/{id}               更新（spec 整组替换规则字段；enabled 独立启停，至少传其一）
  DELETE /api/v1/notify/route/{id}               删除（软删；在途 dispatch 照常完成）
  GET    /api/v1/notify/route/{id}               详情
  POST   /api/v1/notify/route:preview            路由试算（只读：静默 → 选路 → 渠道实例 + 模板解析 + 是否聚合，与管道同一匹配实现 kit/notifycontract）
  GET    /api/v1/notify/route                    分页列表（keyword 模糊匹配 event_pattern/enabled 过滤，page_size ≤ 100）

通知静默（notify 域；免打扰：命中即抑制为 suppressed，多条静默可重叠，任一命中即生效）：
  POST   /api/v1/notify/silence                  创建（模式与路由同式；severity_max 缺省 2，time_window 缺省全程静默）
  PUT    /api/v1/notify/silence/{id}             更新（spec 整组替换：不传 time_window 即清除窗口；enabled 独立启停）
  DELETE /api/v1/notify/silence/{id}             删除（软删）
  GET    /api/v1/notify/silence/{id}             详情
  GET    /api/v1/notify/silence                  分页列表（keyword 模糊匹配 event_pattern/enabled 过滤，page_size ≤ 100）

订阅中心（notify 域；用户自助 + admin 代管：user_id 缺省=操作者本人，指定他人须 is_admin；
  自然键 user×biz×event_pattern，同键保存=替换偏好与启停；偏好 key 限 email/dingtalk/wecom/feishu；
  角色 biz_scope 只约束**代管路径**：代管写/删目标 biz 须在范围内（403），代管列表受限用户须带 biz_id（400）；
  本人自助不受限）：
  POST   /api/v1/notify/subscription             保存订阅（Upsert；偏好值 subscribed 开关 / target 目标群恰其一）
  DELETE /api/v1/notify/subscription/{id}        删除（软删，同键可重建；仅本人或 admin）
  GET    /api/v1/notify/subscription             分页列表（user_id 缺省=本人；keyword 模糊匹配 event_pattern/enabled/biz_id 过滤）

生产者管理（notify 域；事件接入凭据：token 明文仅签发响应出现一次，库存 SHA-256；
  数据面 HMAC 鉴权属管道 ingress，spec 整组替换 + enabled 独立启停，Disable/Rotate ≤10s 生效）：
  POST   /api/v1/notify/producer                 创建并签发 token（本次响应唯一一次回显明文；spec 可带 qps_limit/daily_limit 配额，0=不限）
  PUT    /api/v1/notify/producer/{id}            更新（spec 整组替换：名称/白名单/过期时间；enabled 独立启停）
  POST   /api/v1/notify/producer/{id}/token:rotate  轮换令牌（旧 token 立即失效，新明文仅本次响应）
  DELETE /api/v1/notify/producer/{id}            删除（软删，token 作废；同 biz 名称可复用）
  GET    /api/v1/notify/producer/{id}            详情（不含 token 与哈希）
  GET    /api/v1/notify/producer                 分页列表（keyword 模糊匹配 name/enabled 过滤，page_size ≤ 100）

事件查询（notify 域；只读运行表 notify_event——管道写 / core 读，事件 append-only 无管理写路径；
  全量排障视图含 status/publish_state/publish_attempts；payload 原生 JSON 对象渲染）：
  GET    /api/v1/notify/event                     分页列表（biz/event_type 精确 + status + 落库时间区间过滤，
  GET    /api/v1/notify/event/{id}              事件下钻：事件 + 发送任务（≤100）+ 每任务最近一次投递明细
  POST   /api/v1/notify/event/{id}:replay       事件级重发（新事件 source=internal、新 id、不带 dedup_key；独立 RBAC 元素 notify.event.replay）
                                                  page_size ≤ 100，落库时间倒序）

发送任务查询（notify 域；只读运行表 notify_dispatch——管道 router 生成 / core 读；
  一行 = 事件 × 渠道 × 收件人的一个发送任务，含重试/死信状态与渲染快照排障视图）：
  GET    /api/v1/notify/dispatch                  分页列表（event_id 事件下钻/channel_id/status + 生成时间
  GET    /api/v1/notify/dispatch/{id}           发送任务详情（含渲染快照）
                                                  区间过滤，page_size ≤ 100，生成时间倒序）

投递明细查询（notify 域；只读运行表 notify_delivery_log——管道 sender 每次尝试后追加 /
  core 读；一行 = 发送任务的一次投递尝试：成功/失败、耗时、渠道侧消息标识与失败原因）：
  GET    /api/v1/notify/delivery-log              分页列表（dispatch_id 任务下钻/channel_id/ok 结果 +
                                                  落库时间区间过滤，page_size ≤ 100，落库时间倒序）

死信重放（notify 域；运行表 notify_dlq——sender 在 dispatch 重试超限后写入；重放是
  控制台唯一写动作：标记 replay_state none→pending，dlq worker（管道 P1）轮询后重新
  生成 dispatch 并置 replayed；仅 none 可标记，重复/终态标记返回 409）：
  GET    /api/v1/notify/dead-letter               分页列表（event_id/channel_id/replay_state + 转入
  GET    /api/v1/notify/dead-letter/{id}        死信详情
                                                  时间区间过滤，page_size ≤ 100，转入时间倒序）
  POST   /api/v1/notify/dead-letter/{id}:replay   标记待重放（返回更新后的行；独立 RBAC 元素）
```

事件的实际投递由管道服务 `../notify`（独立模块，`SERVER_ROLES` 角色化单二进制）完成：
router/sender/dlq worker/分区维护经 PostgreSQL 队列消费本域运行表，ingress 提供外部
生产者 HMAC 接入；见 `../notify/README.md`。

跨域业务事件直插（非 HTTP 接口，接口数不变）：`NotificationWriter` 窄接口
（`biz/notify`）供业务域在自身写事务内追加通知事件——业务提交 = 事件必然落库，
任一失败整体回滚（中性事务抽象 `internal/pkg/txrun`，data 层 TxManager 适配）。
system 域 `PUT /role/{id}/permission` 已接入（permission.change，operator 取
JWT claims）；其他业务域接入方式见设计文档 §11。

示例：

```bash
# 创建用户
curl -X POST localhost:8000/api/v1/system/user \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"plain-secret","nickname":"Alice","gender":"female"}'

# 创建角色并绑定
curl -X POST localhost:8000/api/v1/system/role -H 'Content-Type: application/json' \
  -d '{"name":"管理员","code":"admin"}'
curl -X PUT localhost:8000/api/v1/system/user/<user-id>/role \
  -H 'Content-Type: application/json' -d '{"role_ids":["<role-id>"]}'
```

错误响应为 Kratos 结构化错误（`code` / `reason` / `message`），reason 与
`api/system/v1.ErrorReason` 枚举对齐（如 `USER_NOT_FOUND` → 404、`USER_EXISTS` → 409、
`ROLE_CODE_EXISTS` → 409）。

### OpenAPI（Swagger）规格

`make gen` 会用 `protoc-gen-openapiv2` 把全部 proto 合并生成**单一规格文件**
（proto 是唯一事实来源，规格只是构建产物，不手改）：

```
api/openapi.swagger.json   # 62 个接口（auth 4 + system 23 + notify 35），按 tag 分组
```

- 合并文件的全局元信息（title/version）在 `api/system/v1/error_reason.proto` 的
  `openapiv2_swagger` 文件级注解里（注解 vendored 于 `third_party/protoc-gen-openapiv2/`）。
- proto 字段注释会自动带入规格（作为字段说明）；protovalidate 校验规则**不进**规格，
  校验语义以 proto 为准。
- 导入 [Apifox](https://apifox.com) / Swagger UI：数据导入 → OpenAPI/Swagger → 选上述 JSON。
  插件配置见 `buf.gen.yaml`（`allow_merge` + 命名策略 `simple`，版本由 go.mod `tool` 锁定）。

## 接入新业务域

完整清单共 8 处改动（缺一则编译失败或 RBAC 拒绝访问）：

1. **proto**：`api/<domain>/v1/` 新建 `.proto`（service + 消息 + validate 注解 + 该域自己的 ErrorReason），`make gen`。proto 包名一经发布不可改（操作码 = md5(proto full method) 已入库）。
2. **biz**：`internal/biz/<domain>/` 领域模型 + Repo 接口（≤3 方法）+ 用例 + 域 ProviderSet；错误用带 code 的 Kratos 哨兵错误；在 `internal/biz/biz.go` 聚合处追加一行。
3. **data**：`internal/data/<domain>/` 实现 Repo（PO/DO 显式转换；驱动只允许在本层 import；引用共享 `*basedata.Data`，事务/ID 用 `kit/dbx.WithTx`/`kit/idgen.NewID`）+ 域 ProviderSet（含 wire.Bind）；**在 `cmd/server/wire.go` 的 wire.Build 显式追加**（data 顶层不聚合，防 import 环）。
4. **service**：`internal/service/<domain>/` 实现生成的 Server 接口（只做编排与转换）+ 域 ProviderSet；在 `internal/service/service.go` 聚合处追加一行。
5. **RBAC 登记**：在 `internal/service/system/operations.go` 的 `operationFiles` 追加新域 proto descriptor——不登记则 `validateOperations` 拒绝该域操作入库、任何角色都无权调用；前端页面权限同时登记 `internal/biz/system/catalog.go`（system 域是平台域，中心登记制）。
6. **路由注册**：`internal/server/http.go` 加构造参数与一行 Register（Kratos 显式注册惯例）；中间件顺序固定 recovery → tracing → logging → validate → rate-limit → auth → authz，无需改动。
7. **豁免清单（仅匿名接口需要）**：`internal/server/auth.go` 的 `authExempt`（免认证）与 `internal/service/system/operations.go` 的 `authzExemptOperations`（免授权）两处同步——守护测试 `TestAuthExemptWithinAuthzExempt` 强制前者 ⊆ 后者。
8. **收尾**：`make gen` 重生成 wire → `make check`（format + lint + vuln + test）全绿后提交。

### 运行表（事件/任务/明细/死信）查询口径

- 列表缺省只回看 **7 天**（`create_time_start` 缺省 = 有效 `create_time_end` − 7d），跨度上限 **90 天**
  （400 `NOTIFY_TIME_RANGE_INVALID`）：运行表按月分区，无界 `count(*)` + 排序会扫全部分区。
- 索引（迁移 0030）：任务/明细/死信 `(channel_id, create_time DESC)`（受限用户必填 channel_id 的查询路径），
  事件 `(status, create_time DESC) WHERE status IN (3,4)`（无 biz 的失败/抑制排障）。

### 共享工具提取规则

- 第二个使用方出现时才提取，不预先抽象。两级落位：
  - **core 内跨域**：顶层 `internal/biz` 聚合包不能承载共享代码（域子包反向 import 顶层 biz 会与聚合成环，与 data 同理）；顶层 `internal/data` 可以（域子包本就 import basedata，`TxManager`/txrun 适配在此）；biz/service 层的跨域中性工具放 `internal/pkg/<name>`（如 `txrun`）。
  - **core 与 notify 两模块共用**：放同仓的 `backend/kit` 模块（`go.mod` 以 `replace ../kit` 引用，不发版本号；本地可建 `backend/go.work`，已 gitignore）。现有包：`httpx`（protojson 编解码 / 8MiB body / protovalidate / 日志掩码 / 服务器加固 / slog 装配，仅 server/cmd）、`dbx`（pgxpool / `WithTx` / `LikeContains` / `NilIfEmpty` / advisory lock，**仅 data**，depguard 禁 biz/service）、`secretbox`（AES-256-GCM，core 只 Seal）、`idgen`、`paging`（缺省 20 / 上限 100）、`notifycontract`（与管道共享的数据契约：jsonb 结构、状态常量、事件正则与匹配、`notify_event` INSERT 与 dedup 门闩 SQL）。
  - **契约变更**（jsonb 字段、状态字面量、事件正则、共享 SQL）只改 `kit/notifycontract`，两模块同时发布；proto 里重复的正则字面量由 `internal/biz/notify/contract_guard_test.go` 与契约常量比对。
- 身份提取统一走 system 域 `service.UserIDFromContext`（平台域能力，禁止复制副本）；传输上下文提取（clientIP/userAgent 等）目前为 system 域私有，域 #2 需要时一并提取。
- 分层与分域 import 边界由 `.golangci.yml` 的 depguard 在构建期强制（biz 禁 transport/驱动/下层，service 禁 data/驱动，data 禁 service/server，api 禁 internal）。
