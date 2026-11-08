# monitor-backend / core

用户管理中心微服务（多仓多服务路线，模块路径 `github.com/lanyulei/monitor/backend/core`）：
用户管理、角色管理与用户-角色绑定（一个用户可绑定多个角色）。

基于 [Kratos v3](https://github.com/go-kratos/kratos)。
本仓库的行为契约见 [AGENTS.md](./AGENTS.md)，优先级：稳定性 > 安全性 > 正确性 > 性能 > 可维护性 > 交付速度。

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
├── api/system/v1            # Protobuf 契约（UserService + RoleService + ErrorReason）
├── buf.yaml                 # Buf v2 工作区：api + internal + third_party
├── buf.gen.yaml             # api 生成配置（go / go-grpc / go-http / go-errors）
├── buf.gen.config.yaml      # internal/conf 生成配置
├── cmd/server               # 入口：main.go + wire（仅装配，无业务）
├── configs/config.yaml      # 配置文件（密钥一律环境变量注入）
├── migrations/              # SQL 迁移脚本（手动执行）
└── internal/
    ├── conf/                # 配置结构（conf.proto → conf.pb.go）
    ├── server/              # HTTP/gRPC Server 构造、中间件、编解码
    ├── service/             # proto 接口实现：参数编排 + 模型转换
    ├── biz/                 # 业务核心：领域模型 + Repo 接口 + 用例 + 密码哈希
    ├── data/                # Repo 实现（pgx + PostgreSQL，唯一允许 import 驱动的层）
    └── testfake/            # 仓储内存实现（仅供单测组装链路）
```

依赖方向（只允许单向向下）：`api → service → biz → data`。

## 数据模型（库 reborn，表前缀 system_，单数）

| 表 | 说明 |
| --- | --- |
| `system_user` | 用户：id(UUIDv7)/username/password(Argon2id)/nickname/email/phone/gender/is_admin/status（1=可用 2=停用 3=暂停 4=离职） |
| `system_role` | 角色：id/name/code(不可改)/status |
| `system_user_role` | 绑定关系：id/user_id/role_id + create_time/create_user（物理删除） |

- 业务表软删除（`delete_time` 置位），唯一约束（username/email/phone/code）只约束未删除行；
- 关联表仅保留 create_time/create_user，解绑即物理删除；
- 审计字段：`create_time, update_time, delete_time, create_user, update_user, delete_user`
  （关联表仅前两者中的 create_time/create_user）；操作者 ID 待认证中间件上线后注入，当前为 NULL。

## 快速开始

```bash
# 1. 安装依赖与锁定版本的工具链
go mod download
make init        # golangci-lint / govulncheck / gofumpt

# 2. 初始化数据库（库 reborn 需已存在）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0001_system_init.up.sql     # 系统域建表（用户/角色/关联）
psql -h 127.0.0.1 -U postgres -d reborn -f migrations/0002_refresh_token.up.sql   # 认证域建表（刷新令牌）
# 0001 内置超级管理员：admin / Admin123!（生产环境部署后应立即改密或停用）

# 3. 生成代码（修改 .proto 或 provider 后必须执行）
make gen

# 4. 本地运行（必须用 kratos CLI）
kratos run

# 5. 静态检查 + 安全扫描（提交前必须全绿）
golangci-lint run ./...
govulncheck ./...

# 6. 测试（data 集成测试需 TEST_DATABASE_SOURCE，未设置时自动跳过）
go test ./internal/biz/...  -race -count=1
go test ./internal/data/... -race -count=1 -tags=integration
go test ./...               -race -shuffle=on
```

## 配置与密钥

- 配置入口：`configs/config.yaml`，可用 `KRATOS_` 前缀环境变量覆盖（如
  `KRATOS_DATA_DATABASE_SOURCE` 覆盖数据库 DSN）。
- 当前 `config.yaml` 中的 DSN 为本地开发环境配置；**生产环境一律通过环境变量注入**，
  禁止将口令提交进仓库。
- 日志侧由 `log.WithFilter(log.FilterKey("password", "token", "secret", "authorization"))`
  在 handler 构建处统一脱敏。

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

角色：
  POST   /api/v1/system/role                     创建角色
  PUT    /api/v1/system/role/{id}                更新（name/status；code 不可改）
  DELETE /api/v1/system/role/{id}                删除（软删，级联清绑定）
  GET    /api/v1/system/role/{id}                详情
  GET    /api/v1/system/role                     分页列表
```

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
api/openapi.swagger.json   # 14 个接口（UserService 9 + RoleService 5），按 tag 分组
```

- 合并文件的全局元信息（title/version）在 `api/system/v1/error_reason.proto` 的
  `openapiv2_swagger` 文件级注解里（注解 vendored 于 `third_party/protoc-gen-openapiv2/`）。
- proto 字段注释会自动带入规格（作为字段说明）；protovalidate 校验规则**不进**规格，
  校验语义以 proto 为准。
- 导入 [Apifox](https://apifox.com) / Swagger UI：数据导入 → OpenAPI/Swagger → 选上述 JSON。
  插件配置见 `buf.gen.yaml`（`allow_merge` + 命名策略 `simple`，版本由 go.mod `tool` 锁定）。

## 接入新业务

1. 在 `api/<domain>/v1/` 新建 `.proto`（service + 消息 + validate 注解 + ErrorReason），`make gen`。
2. `internal/biz`：领域模型 + Repo 接口（≤3 方法）+ 用例；错误用带 code 的 Kratos 哨兵错误。
3. `internal/data`：实现 Repo（PO/DO 显式转换；驱动只允许在本层 import）。
4. `internal/service`：实现生成的 Server 接口，只做编排与转换。
5. `internal/server`：注册路由；中间件顺序固定 recovery → tracing → logging → validate → rate-limit。
