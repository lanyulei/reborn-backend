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
| 框架 | Kratos v3（gRPC + HTTP 双协议、Wire DI、OTel 可观测） |
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
├── buf.gen.yaml             # api 生成配置（go / go-grpc / go-http / go-errors）
├── buf.gen.config.yaml      # internal/conf 生成配置
├── cmd/server               # 入口：main.go + wire（仅装配，无业务）
├── configs/config.yaml      # 配置文件（密钥一律环境变量注入）
├── migrations/              # SQL 迁移脚本（手动执行）
└── internal/
    ├── conf/                # 配置结构（conf.proto → conf.pb.go）
    ├── server/              # HTTP/gRPC Server 构造、中间件、编解码（跨域共享）
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

- 业务表软删除（`delete_time` 置位），唯一约束（username/email/phone/code）只约束未删除行；
- 关联表仅保留 create_time/create_user，解绑即物理删除；
- 审计字段：`create_time, update_time, delete_time, create_user, update_user, delete_user`
  （关联表仅前两者中的 create_time/create_user）；认证中间件已上线，但 system_permission/
  system_grant/system_user_role 写路径尚未注入操作者 ID，create_user 当前为 NULL（待补）。

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

# 3. 生成代码（修改 .proto 或 provider 后必须执行）
make gen

# 4. 本地运行（必须用 kratos CLI）
kratos run

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

- 配置入口：`configs/config.yaml`，可用 `KRATOS_` 前缀环境变量覆盖（如
  `KRATOS_DATA_DATABASE_SOURCE` 覆盖数据库 DSN）。
- 当前 `config.yaml` 中的 DSN 为本地开发环境配置；**生产环境一律通过环境变量注入**，
  禁止将口令提交进仓库。
- 日志侧由 `log.WithFilter(log.FilterKey("password", "token", "secret", "authorization"))`
  在 handler 构建处统一脱敏。
- 通知渠道凭据主密钥经 `KRATOS_NOTIFY_MASTER_KEY` 注入（config.yaml 占位
  `${NOTIFY_MASTER_KEY}`；`openssl rand -hex 32` 生成）。AES-256-GCM 加密落库；
  **留空时服务照常启动**，仅在写入带 secret 的渠道处报 `NOTIFY_SECRET_UNAVAILABLE`
  （不阻断未启用通知能力的部署），webhook 等无 secret 渠道不受影响。

## API 概览（HTTP + gRPC 双协议）

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
  POST   /api/v1/notify/channel/{id}:test        发送测试事件（须启用；直插 source=internal 事件，类型/标题/收件/载荷可覆盖，
                                               缺省 notify.test；投递结果经事件/任务/明细查询异步跟踪，链路随管道 P1 生效）

通知模板（notify 域；biz × event_type × channel_type 三维覆盖，'*' 通配，渲染解析链精确 > 通配）：
  POST   /api/v1/notify/template                 创建（Go template 语法 + 变量白名单写入校验；组合唯一）
  PUT    /api/v1/notify/template/{id}            部分更新（恒 version+1；spec 可改，组合仍须唯一）
  DELETE /api/v1/notify/template/{id}            删除（软删，组合可复用）
  GET    /api/v1/notify/template/{id}            详情
  GET    /api/v1/notify/template                 分页列表（keyword 模糊/eventType/channelType/enabled 过滤，page_size ≤ 100）

通知路由（notify 域；事件→通知任务的分发决策：模式+严重级+标签匹配，多路由重叠由 priority 决胜）：
  POST   /api/v1/notify/route                    创建（channel_ids 须为同 biz 未删除渠道；调优项缺省 1/5/0/500/30）
  PUT    /api/v1/notify/route/{id}               更新（spec 整组替换规则字段；enabled 独立启停，至少传其一）
  DELETE /api/v1/notify/route/{id}               删除（软删；在途 dispatch 照常完成）
  GET    /api/v1/notify/route/{id}               详情
  GET    /api/v1/notify/route                    分页列表（keyword 模糊匹配 event_pattern/enabled 过滤，page_size ≤ 100）

通知静默（notify 域；免打扰：命中即抑制为 suppressed，多条静默可重叠，任一命中即生效）：
  POST   /api/v1/notify/silence                  创建（模式与路由同式；severity_max 缺省 2，time_window 缺省全程静默）
  PUT    /api/v1/notify/silence/{id}             更新（spec 整组替换：不传 time_window 即清除窗口；enabled 独立启停）
  DELETE /api/v1/notify/silence/{id}             删除（软删）
  GET    /api/v1/notify/silence/{id}             详情
  GET    /api/v1/notify/silence                  分页列表（keyword 模糊匹配 event_pattern/enabled 过滤，page_size ≤ 100）

订阅中心（notify 域；用户自助 + admin 代管：user_id 缺省=操作者本人，指定他人须 is_admin；
  自然键 user×biz×event_pattern，同键保存=替换偏好与启停；偏好 key 限 email/dingtalk/wecom/feishu）：
  POST   /api/v1/notify/subscription             保存订阅（Upsert；偏好值 subscribed 开关 / target 目标群恰其一）
  DELETE /api/v1/notify/subscription/{id}        删除（软删，同键可重建；仅本人或 admin）
  GET    /api/v1/notify/subscription             分页列表（user_id 缺省=本人；keyword 模糊匹配 event_pattern/enabled 过滤）

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
                                                  page_size ≤ 100，落库时间倒序）

发送任务查询（notify 域；只读运行表 notify_dispatch——管道 router 生成 / core 读；
  一行 = 事件 × 渠道 × 收件人的一个发送任务，含重试/死信状态与渲染快照排障视图）：
  GET    /api/v1/notify/dispatch                  分页列表（event_id 事件下钻/channel_id/status + 生成时间
                                                  区间过滤，page_size ≤ 100，生成时间倒序）

投递明细查询（notify 域；只读运行表 notify_delivery_log——管道 sender 每次尝试后追加 /
  core 读；一行 = 发送任务的一次投递尝试：成功/失败、耗时、渠道侧消息标识与失败原因）：
  GET    /api/v1/notify/delivery-log              分页列表（dispatch_id 任务下钻/channel_id/ok 结果 +
                                                  落库时间区间过滤，page_size ≤ 100，落库时间倒序）

死信重放（notify 域；运行表 notify_dlq——sender 在 dispatch 重试超限后写入；重放是
  控制台唯一写动作：标记 replay_state none→pending，dlq worker（管道 P1）轮询后重新
  生成 dispatch 并置 replayed；仅 none 可标记，重复/终态标记返回 409）：
  GET    /api/v1/notify/dead-letter               分页列表（event_id/channel_id/replay_state + 转入
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
3. **data**：`internal/data/<domain>/` 实现 Repo（PO/DO 显式转换；驱动只允许在本层 import；引用共享 `*basedata.Data`，事务/ID 用 `basedata.WithTx`/`basedata.NewID`）+ 域 ProviderSet（含 wire.Bind）；**在 `cmd/server/wire.go` 的 wire.Build 显式追加**（data 顶层不聚合，防 import 环）。
4. **service**：`internal/service/<domain>/` 实现生成的 Server 接口（只做编排与转换）+ 域 ProviderSet；在 `internal/service/service.go` 聚合处追加一行。
5. **RBAC 登记**：在 `internal/service/system/operations.go` 的 `operationFiles` 追加新域 proto descriptor——不登记则 `validateOperations` 拒绝该域操作入库、任何角色都无权调用；前端页面权限同时登记 `internal/biz/system/catalog.go`（system 域是平台域，中心登记制）。
6. **路由注册**：`internal/server/http.go` + `grpc.go` 各加构造参数与一行 Register（Kratos 显式注册惯例）；中间件顺序固定 recovery → tracing → logging → validate → rate-limit → auth → authz，无需改动。
7. **豁免清单（仅匿名接口需要）**：`internal/server/auth.go` 的 `authExempt`（免认证）与 `internal/service/system/operations.go` 的 `authzExemptOperations`（免授权）两处同步——守护测试 `TestAuthExemptWithinAuthzExempt` 强制前者 ⊆ 后者。
8. **收尾**：`make gen` 重生成 wire → `make check`（format + lint + vuln + test）全绿后提交。

### 共享工具提取规则

- 第二个域要用某工具时才提取，不预先抽象。提取时注意：**顶层 `internal/biz` 聚合包不能承载共享代码**（域子包反向 import 顶层 biz 会与聚合成环，与 data 同理）；**顶层 `internal/data` 可以**（域子包本就 import basedata，事务 `WithTx`/`NewID`/`nilIfEmpty` 已落位于此）；biz/service 层的跨域工具需独立中性包（如 `internal/pkg/<name>`）。
- 身份提取统一走 system 域 `service.UserIDFromContext`（平台域能力，禁止复制副本）；传输上下文提取（clientIP/userAgent 等）目前为 system 域私有，域 #2 需要时一并提取。
- 分层与分域 import 边界由 `.golangci.yml` 的 depguard 在构建期强制（biz 禁 transport/驱动/下层，service 禁 data/驱动，data 禁 service/server，api 禁 internal）。
