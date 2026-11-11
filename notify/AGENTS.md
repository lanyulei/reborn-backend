# AGENTS.md

本文件是本仓库 AI 编码代理（Codex / Cursor / Copilot / Claude 等）的强制行为契约。
所有规则按优先级排序：稳定性 > 安全性 > 正确性 > 性能 > 可维护性 > 交付速度。
与项目实际情况冲突时，先停下询问维护者，不得自行猜测。

## 0. 项目概览

- 语言/运行时：Go 1.26.x（go.mod 中声明 go 1.26，禁止放宽到更低版本）
- 框架：Kratos github.com/go-kratos/kratos/v3 v3.x（HTTP 协议、Wire DI、OTel 可观测；gRPC 已停用——无调用方，proto 保留 service 定义，需要时加回 protoc-gen-go-grpc 插件与 server/grpc.go 即可恢复）
- IDL：Protobuf，通过 Buf v2（buf.yaml / buf.gen.yaml）生成代码
- DI：github.com/google/wire（编译期依赖注入，禁止引入运行时 DI 库如 dig/fx）
- 日志：标准库 log/slog，经 Kratos log.NewLogger / log.NewHandler 包装
- 可观测：OpenTelemetry（trace/metrics/logs），OTel 日志走 contrib otel handler
- 存储/中间件：以 internal/data 内实现为准，禁止在 biz/service 层直接 import 驱动包
- 部署形态：默认多副本水平部署，滚动发布期间多版本实例共存；一切设计默认多实例成立（见第 8 节）

## 1. 目录结构与分层契约（高内聚、低耦合的核心）

目录结构：

    .
    ├── api/                    # Protobuf 契约（api/<domain>/v1/*.proto），只定义，不实现
    ├── cmd/<server>/main.go    # 极简入口：只组装 wire，禁止业务逻辑
    ├── configs/                # 配置文件（YAML）
    └── internal/
        ├── server/             # http.go：Server 构造、路由注册、中间件挂载
        ├── service/            # 实现 proto 生成的接口，只做参数校验+编排，调用 biz
        ├── biz/                # 业务核心：定义 Repo 接口 + Usecase；零框架/驱动 import
        └── data/               # 实现 biz 定义的 Repo 接口；唯一允许 import DB/Redis 驱动

分层依赖方向（只允许单向向下，禁止反向/跨层）：

    api → service → biz → data

- service 层：薄编排层。禁止 import internal/data，禁止 import 任何数据库/缓存驱动。
- biz 层：业务规则与事务边界。定义 Repo interface（依赖倒置），禁止 import Kratos 传输层（transport/http、transport/grpc）与任何具体驱动。
- data 层：实现 biz 的 Repo interface。数据模型（PO/DO）与 API 模型（proto 生成）必须显式转换，禁止结构体直接复用跨层传递。
- main.go / wire：只做装配，禁止出现 if/for 业务分支。
- 违反依赖方向的一律退回重写。
- 表命名随域走：数据库表名前缀必须等于其所属 api 域名（`api/system` → `system_*`，`api/notify` → `notify_*`，`api/assistant` → `assistant_*`），分区子表、索引、约束名同样带域前缀；一个域不得把表挂到别的域前缀下（如 assistant 域的表不得命名为 `system_assistant_*`）。新增域即新增前缀。此契约由 `core/migrations/naming_test.go` 扫描全部 `*.up.sql` 的 CREATE TABLE 锁死，前缀清单直接取自 `core/api/` 目录。

## 2. 构建与常用命令

    # 依赖与工具（首次 / go.mod 变更后）
    go mod download
    make init

    # 生成 proto / wire / errors（改动 .proto 或 provider 后必须执行）
    make gen

    # 重新生成 Kratos 生成文件（v3 生成代码不再 import 已移除的 http binding 包）
    go generate ./...

    # 本地运行（必须用 kratos CLI，不要直接 go run main.go）
    kratos run

    # 静态检查 + 安全扫描（提交前必须全绿）
    golangci-lint run ./...
    govulncheck ./...

    # 测试（分层执行）
    go test ./internal/biz/... -race -count=1
    go test ./internal/data/... -race -count=1 -tags=integration
    go test ./... -race -shuffle=on

    # 依赖治理
    go mod tidy
    go mod verify

命令准则：只使用上面列出的命令；修改命令本身属于架构变更，需先与维护者确认。

## 3. 通用 Go 代码风格

- 遵循 Uber Go Style Guide + 本仓库既有风格；新增文件必须通过 gofumpt 格式化。
- 错误处理：
  - 使用 fmt.Errorf("%w", err) 包装，保留错误链；禁止用 _ 丢弃 error。
  - 包级错误用 errors.New 哨兵变量；跨层业务错误用 Kratos errors 包返回带 code 的结构化错误。
  - panic 只允许在进程初始化阶段（配置缺失等不可恢复场景）；运行期一律返回 error。
- 并发：优先用 errgroup 管理生命周期；每个 goroutine 必须有明确的退出路径，禁止裸 go func() 不加管理；共享状态优先用不可变值 / atomic，必要时 mutex 并注释不变式。
- context：所有跨层方法第一个参数必须是 context.Context；所有 IO/DB/RPC 调用必须透传 ctx；请求链路中禁止 context.Background()（后台任务用 context.WithoutCancel 并注明）。
- 资源：所有 io.Closer / 连接 / 事务必须 defer 关闭；事务必须 defer rollback，成功路径显式 commit。
- 注释：导出符号必须有 godoc 注释，以符号名开头；TODO 格式为 TODO(owner): 描述。
- 命名：接口名与实现名的依赖方向统一（biz 定义 Repo，data 实现 repo）；禁止拼音或不一致缩写。

## 4. Go 封装规范（用类型系统锁住不变式）

封装服务于"让错误用法在编译期暴露"，其优先级低于稳定性与性能（见文件头排序）。
任何抽象（接口 / 泛型 / 包装类型）引入前先过三问，任何一问答不上来就不抽象：

1. 它让哪类错误从运行期提前到了编译期？
2. 热路径是否引入额外成本（any 装箱、itab 间接调用、逃逸、实例化膨胀）？
3. 是否已有 ≥ 2 个可预见的具体实现或明确复用场景（YAGNI）？

### 4.1 最小导出面

- 标识符默认 unexported；导出即冻结的公共契约，未稳定前一律小写。
- struct 字段默认不导出，通过方法暴露只读视图。例外：data 层 PO/DO、proto 生成类型、需序列化的配置结构。
- 禁止导出可变全局变量（slice/map/指针）；包级可变状态收敛为 unexported 变量 + 受控方法，或改为构造注入。
- 构造函数 New* 必须完成全部校验并返回完全初始化的对象，禁止半初始化对象外泄；零值可用的类型必须在 godoc 标注。
- 遵循 accept interfaces, return concrete types：参数收窄为当前包用到的最小接口，返回具体类型；接口在消费侧定义（biz 定义 Repo），实现侧用编译期断言锁住契约：

    var _ biz.RuleRepo = (*ruleRepo)(nil)

### 4.2 内部状态防泄漏

- 对外方法禁止返回内部 slice/map 的直接引用（调用方 append/改写会污染内部状态）：返回 slices.Clone / maps.Clone 拷贝，或 iter.Seq 只读迭代器。
- 大对象热路径拷贝代价高时：允许只读视图或所有权转移约定，但必须在 godoc 显式注明"返回值禁止修改 / 所有权已转移"，且不得出现在跨团队公共 API。
- 构造函数 / Setter 接收 slice/map 并存储时必须克隆，或以参数命名与注释显式约定所有权。
- 迭代器（iter.Seq / iter.Seq2）禁止在持锁状态下执行 yield：调用方代码不可控（死锁、长临界区，违反第 6 节锁规则）；先快照再迭代，稳定性优先于一份拷贝。

```go
// Registry.All 返回只读迭代器：临界区内只做快照，
// 绝不在持锁状态下执行调用方的 yield（稳定性 > 一份 map 拷贝的成本）。
func (r *Registry) All() iter.Seq2[string, Rule] {
    r.mu.RLock()
    snap := maps.Clone(r.rules)
    r.mu.RUnlock()
    return maps.All(snap)
}
```

### 4.3 接口与泛型取舍（能且值得用才用）

- 接口方法数 ≤ 3，超过即拆分；热路径避免 any/interface{} 装箱与 itab 间接调用，类型转换收敛到边界处一次完成。
- 泛型优先用于三类场景：
  1. 替代 any 装箱的类型安全工具/容器：Unique / Filter、Set、LRU、分页结果、泛型 singleflight / batcher；
  2. 与业务类型无关的通用算法：环形缓冲、双缓冲、滑动窗口限流；
  3. 幽灵类型 ID（type ID[T any] string 派生 RuleID / SilenceID），编译期防传错实体 ID，零运行时成本。
- 约束能窄则窄：优先 comparable / cmp.Ordered / 最小约束接口；禁止随手写 any 约束（等于放弃类型安全）。
- 单一实现且无复用计划禁止上泛型/接口抽象（YAGNI）；禁止用泛型模拟继承或虚方法派发（方法不能带类型参数，不得用包装类型绕过）。
- 标准库优先（slices / maps / cmp），其次 internal/pkg 既有实现，最后才自研。

### 4.4 泛型性能红线（承接第 6 节）

- 泛型 vs 接口：值类型场景泛型通常省去装箱与堆分配；但每个类型参数组合会实例化独立代码（二进制膨胀），指针类型共享 gcshape 实例化、内联受限。
- 热路径泛型函数警惕闭包捕获与参数逃逸；不确定时保留具体类型特化 fast path。
- 泛型抽象的引入/替换一律附 go test -bench 对比（无基准不合并，同第 6 节）。

```go
// 好：类型安全、零装箱、容量预分配，泛型价值明确
func Unique[T comparable](s []T) []T {
    seen := make(map[T]struct{}, len(s))
    out := make([]T, 0, len(s))
    for _, v := range s {
        if _, ok := seen[v]; ok {
            continue
        }
        seen[v] = struct{}{}
        out = append(out, v)
    }
    return out
}

// 坏：any 装箱 —— 编译期丢失类型信息，调用方被迫类型断言
func First(s []any) any { return s[0] }

// 坏：T 永远只有一个实参的"炫技泛型"，纯增加复杂度
type ruleCache[T any] struct{ v T }
```

### 4.5 复用与去重（DRY，零重复）

前提：去重服务于可维护性，不得以牺牲稳定性、正确性、性能为代价（见文件头优先级）。在此前提下，重复代码零容忍。

- 动手前先检索：新写函数 / 类型前，先查同层既有代码与 internal/pkg（尚不存在时新建该包沉淀）；已有实现直接复用或在原处扩展，禁止平行再造一份。
- 第二份即提取：同一逻辑出现第 2 份时立即提取（同层提私有函数，跨包提到 internal/pkg），不要等第 3 份。与 4.3 节"≥2 个实现才抽象"不冲突：去重提取的是普通函数，不引入新的接口 / 泛型抽象层。
- 重复高发区重点自查：PO/DO ↔ proto 转换、错误码与错误文案映射、分页与参数校验片段、SQL 查询构造、常量与魔法数字。转换函数按实体各保留一对（toProto / toDO），禁止散落多份。
- 改代码时优先收敛：改动某处发现既有重复，顺手合并，禁止再新增第 3 个变体；复制粘贴后仅改命名 / 文案同样计为重复。
- 去重不得引入新的错误面：禁止为凑通用引入 any 装箱或"配置大杂烩"参数（参数超过 5 个改用选项模式或拆分函数）；行为有差异的两段代码宁可保留两份明确实现，也不做布尔参数分叉的伪合并。
- 热路径豁免：为保证内联 / 零分配而保留的少量重复，必须加 `// dup: <理由>` 注释并控制在同一文件内；无注释的重复一律按违规处理。

## 5. Kratos v3 专项规则

- 模块路径：所有 import 一律使用 github.com/go-kratos/kratos/v3；contrib 模块同样以 /v3 结尾（如 contrib/middleware/jwt/v3）。禁止出现任何 /v2 残留 import。
- 日志：统一使用 log/slog。经 Kratos 包装：log.NewLogger(slog.NewJSONHandler(...), log.WithFilter(...))，并用 .With() 附加 service.name / service.version 等固定属性；app 与中间件选项接收 *slog.Logger（kratos.Logger(logger)）。禁止使用 v2 的 log.Helper / log.NewStdLogger / log.Valuer；禁止 fmt.Println / log.Print 直接输出业务日志。日志中输出请求/响应前必须用 log.WithFilter(log.FilterKey(...)) 脱敏。
- JSON 编解码：v3 拆分为 encoding/json（标准库语义）与 encoding/protojson（protobuf JSON 语义），二者必须显式注册 import。规则：proto.Message 响应一律走 protojson；普通 Go 值走 encoding/json。禁止在同一进程注册 contrib/encoding/json/v3 兼容编解码器（那是迁移期过渡方案，新代码禁用），更禁止两个 json 编解码器同时注册导致初始化顺序覆盖。
- HTTP 绑定：v3 已移除 transport/http/binding 包。生成代码无需手改（重新生成即可）；手写代码中：路径参数用 http.BuildPath("/v1/users/{id}", req)，请求绑定用 transport/http.Context 的 Bind / BindVars / BindQuery / BindForm。禁止手写 query 拼接。
- JWT 中间件：使用 github.com/go-kratos/kratos/contrib/middleware/jwt/v3；只在真正用到 JWT 的服务引入（v3 已将其移出核心以减少依赖）。
- 熔断：v3 默认熔断不再依赖 aegis，默认用法 circuitbreaker.Client()(next) 零改动。仅当注入自定义 breaker 时，显式添加 aegis 依赖并用 circuitbreaker.WithBreakerFactory(func() circuitbreaker.CircuitBreaker { return newBreaker() })。
- 接口契约：所有对外方法必须先在 api/<domain>/v1/*.proto 定义，再生成代码；禁止手写 HTTP handler 或手写 gRPC 方法签名。
- 参数校验：统一用 proto 的 validate 规则（[(validate.rules)...]）在边界完成校验；不要在 service 层重复手写 if 校验，也不要引入第三方 binding 包。
- 中间件：统一挂载在 internal/server/http.go（或 Group 级），顺序：recovery → tracing → logging → validate → rate-limit → 业务。日志中间件注入 *slog.Logger。
- 错误码：proto 内集中定义 enum ErrorReason，由 protoc-gen-go-errors 生成，service 层禁止手拼 JSON 错误响应。
- 配置：全部走 configs/*.yaml + config.Load，禁止硬编码 IP/端口/密钥/魔法数字。
- Wire：新增依赖时在对应 internal/*/provider.go 补充 ProviderSet，并运行 make gen / go generate ./... 重新生成 wire_gen.go；禁止手改 wire_gen.go（生成文件顶部注释声明）。
- 生成代码升级：从 v2 迁移过来的文件，改完 import 后必须执行 go generate ./... 重新生成（v3 生成代码不再引用已移除的 http binding 包），并核对 git diff 中生成文件的变化符合预期。

## 6. 性能硬指标（高性能）

- 热路径（每请求都执行的代码）禁止：反射、fmt.Sprintf 拼日志、深拷贝大结构、请求内新建大缓冲。
- 批量：DB 查询禁止 N+1；列表接口必须分页且默认有上限（如 page_size ≤ 100）。
- 池化：高频大对象使用 sync.Pool，并在 Get 后重置状态。
- 锁：临界区只包含必要写操作，禁止在持锁状态下做网络/磁盘 IO。
- goroutine 数量：受控（固定 worker / errgroup.SetLimit），禁止无上限 fan-out。
- 序列化：proto 优先；如需 JSON，按第 5 节选对编解码器（protojson vs encoding/json），并预热 encoder。
- 任何性能敏感改动必须附带基准测试（go test -bench）对比结果，无基准不合并。

## 7. 安全红线（高安全，一票否决）

- 密钥/凭证：禁止写入代码、配置文件明文、日志、注释、错误信息。一律从环境变量/Secret 管理读取；日志脱敏用 log.WithFilter(log.FilterKey("password")) 在 handler 构建处统一挂载，禁止逐条手删。
- SQL：禁止字符串拼接 SQL；必须使用参数绑定或 query builder；动态字段名走白名单。
- 输入校验：所有外部输入（HTTP/gRPC/消息）必须在 proto validate 规则里约束长度/范围/格式，默认不信任。
- 反序列化：proto 以外的反序列化需限制深度/大小，防资源耗尽。
- 网络：出站请求必须带超时（默认 ≤ 3s）与重试上限；禁止无超时的 http.Client{}。
- 依赖：新增第三方依赖必须过 govulncheck 与 license 审查；锁死版本，禁止 @latest 提交。
- CI 安全：govulncheck、golangci-lint（含 gosec 规则）全绿是合并前置条件。

## 8. 多实例部署约束（设计期强制，一票否决）

本服务默认多副本水平部署，滚动发布期间多版本实例共存。任何功能设计与实现，必须先回答一个问题：
"这段代码 / 这份数据 / 这个任务被 N 个实例同时执行或访问时，结果是否仍然正确？"——回答不了即视为设计未完成。

- 进程内状态：跨请求、跨实例共享的状态（登录态、验证码、幂等去重表、序列号、任务队列）禁止放进程内存，必须落共享存储（当前为 PG，接入 Redis 后同理）；进程内存仅允许作带 TTL 的只读缓存，且逻辑正确性不得依赖各实例间缓存一致。
- 互斥与锁：跨实例互斥必须用分布式锁或 DB 原子约束（唯一索引、条件更新 UPDATE ... WHERE version = ...、PG Advisory Lock）；sync.Mutex / 进程内标记只保护单进程。分布式锁必须带 TTL 与属主校验（释放时比对 token），实例崩溃不得造成永久死锁。
- 定时与后台任务：cron / ticker / 轮询等长驻任务默认每个副本都会启动一份，必须二选一：分布式锁 / 选主保证同一时刻仅一个实例执行，或任务幂等 + 可分片（多副本并发安全）。不做任何处理直接上线视为缺陷。
- 幂等：所有写入口（HTTP / gRPC / 消息消费 / 回调）默认会被重复触发（客户端重试、超时重发、消费者 rebalance），必须以唯一键（可用客户端生成的 UUIDv7）、乐观锁版本号或状态机保证幂等；DB 唯一约束是最后防线，冲突路径返回确定结果而不是 500。
- 并发写：同一行的并发修改依赖事务 + 乐观锁（version 列）或行锁（SELECT ... FOR UPDATE）；禁止无保护的 check-then-act（先查后写）竞态；跨实例长事务禁止。
- 限流与配额：单机限流只用于实例自我保护；全局配额、频控、防刷必须基于共享存储实现。
- 发布兼容：任意实例可被随时杀死替换——后台任务必须响应 ctx 取消并在期限内退出；接口与表结构变更必须向后兼容（先加列后删列、双写灰度），禁止假设"全部实例同时升级"。
- 交付物：设计方案 / PR 描述必须包含一段"多实例影响说明"：新增了哪些共享状态、锁、后台任务，幂等如何保证；写不出这段说明按设计未完成退回。

## 9. 禁止清单（红线，违反即重做）

1. 修改/手写任何生成文件：*.pb.go、*_grpc.pb.go、*_http.pb.go、*errors.pb.go、wire_gen.go、buf.lock（除通过 make gen / go generate 再生成）。
2. 跨层依赖：service 直接 import data；biz import 传输层或驱动。
3. 引入 gin / echo / dig / fx 或其它 DI、Web 框架。
4. 绕过 proto 契约新增对外 HTTP/gRPC 方法。
5. 删除/跳过测试、linter、-race 以"让构建通过"。
6. 提交含密钥、私钥、token 的任何文件。
7. go.mod 使用 @latest 或主分支伪版本而不锁版本。
8. 修改 .github/workflows、Makefile、本 AGENTS.md（需维护者批准）。
9. 在 main.go / server 层写业务逻辑。
10. 使用任何 github.com/go-kratos/kratos/v2 的 import 路径（v3 仓库禁止 v2 混用）。
11. 在同一进程注册 contrib/encoding/json/v3 兼容编解码器，或同时注册两个 json codec。
12. 导出可变全局变量；对外方法返回内部 slice/map/指针字段的可写引用（godoc 明确标注所有权转移/只读的除外）。
13. 为单一实现引入接口/泛型抽象层；用 any 装箱或 any 约束的泛型替代具体类型与窄约束泛型。
14. 忽略本文件任何一条而不说明理由。
15. 功能设计或实现基于单实例假设：用进程内存承载跨实例共享状态、用本地锁做跨实例互斥、后台任务在多副本下重复执行且不幂等（详见第 8 节）。
16. 明知已有实现仍重复造轮子；同一逻辑出现第 2 份拷贝而不提取（带 `// dup:` 豁免注释的热路径除外，详见第 4.5 节）。

## 10. 多轮 Code Review 强制闭环（核心流程，不可跳过）

任何代码变更（无论大小）在提交给用户/PR 之前，必须循环执行以下评审流程，
直到连续一轮零错误、零告警、零建议为止。单轮通过不算完成，最少执行 2 轮。

### 每轮评审的固定检查清单（逐项确认，输出到最终答复）

1. go build ./... 通过
2. go vet ./... 无输出
3. golangci-lint run ./... 退出码 0（不允许任何无理由跳过）
4. govulncheck ./... 无漏洞（或已确认误报并说明）
5. go test ./... -race -shuffle=on 全绿
6. 新增/修改的函数有对应测试（覆盖率不低于同层平均水平）
7. 分层依赖方向逐文件核查（可用 go list -deps 抽查 import）
8. 生成文件未被手改（git diff --name-only 与 make gen / go generate 后 diff 一致）
9. 无 TODO/FIXME 残留（或已在答复中列明）
10. 无 v2 残留 import（grep -r "kratos/v2" 输出为空）
11. JSON 编解码器注册正确（仅 encoding/json + encoding/protojson，二者职责不混用）
12. 安全清单逐项自查（第 7 节）
13. 性能红线逐项自查（第 6 节）
14. 封装规范逐项自查（第 4 节）：最小导出面、内部状态无泄漏、泛型与接口取舍符合约束。
15. 多实例部署逐项自查（第 8 节）：无进程内跨实例状态、互斥用分布式锁/DB 原子约束、后台任务多副本安全、写路径幂等、变更说明含"多实例影响说明"。
16. 复用与去重逐项自查（第 4.5 节）：新增代码未复制既有实现；不存在未标注 `// dup:` 豁免的第 2 份相同逻辑。

### 循环终止条件

连续一整轮上述清单全绿 + 自我复查未发现任何新问题，方可结束。
若第 N 轮发现新问题，修复后回到第 1 轮重新计数（不累计）。

### 评审产出格式（最终答复必须包含）

1. 变更摘要（涉及文件 + 行数）
2. 每轮评审的检查表结果与发现的问题
3. 修复记录（问题 → 修复 → 验证）
4. 最终一轮全绿证明（粘贴关键命令输出尾部 5 行）

### 明确禁止

- 只跑一轮就宣称"无 bug"。
- 跳过 lint/test 以加速交付。
- 用 nolint / nosec 注释压制问题而不写明理由与豁免范围。
- 在未完成闭环前提交 commit 或创建 PR。

## 11. Git 提交与交付

- 分支：feat/<ticket>-<slug> / fix/<ticket>-<slug>；禁止直接 push 主干。
- Commit message：遵循 Conventional Commits（feat: / fix: / refactor: / test: / chore:），正文说明"为什么"。
- 交付前自检：git status 干净、go mod tidy 无 diff、第 10 节闭环已完成并附证据。

## 12. 上下文外事项

- 依赖升级、框架大版本迁移、目录结构调整、删除公共工具函数：一律先提问确认，不要直接动手。
- 任何与本文件冲突的既往代码：先报告冲突点，由维护者裁决以哪边为准。


## 13. 管道服务（backend/notify）P1 实现契约

本章随 P1 骨架落地新增（2026-11），是 §1-§12 通用契约在管道服务上的具体化；设计依据
`backend/docs/notify-design.md`（§12 工程规范、§14 多实例影响说明）。core 的 Makefile /
.golangci.yml / README 引用的 `../notify/AGENTS.md §x` 仍指本文件 §0-§12。

### 13.1 分层与依赖方向

Kratos 四层：`api → service → biz → data`（非 DDD 四层）。
- `biz` 定义领域模型、仓储/骨干接口与用例；禁止 import `data`/`service`/`server`/
  `kratos/transport`/`pgx`（depguard 构建期强制）。
- `data` 是唯一允许 import 数据库驱动与出站 I/O 的层：PG 仓储、渠道适配器
  （`internal/data/channel`）、advisory lock。
- `service` 只做 proto ↔ 领域模型转换与错误透传；鉴权在 `server` 中间件。
- `worker` 把 biz 用例包成 kratos `transport.Server` 循环；角色门控在 `server.NewServers`。
- 与 core **不跨模块 import**：共享的只有数据库 schema 与 jsonb 编码契约
  （`internal/biz/config.go` 的 json 标签与 core `internal/biz/notify` 完全一致）。

### 13.2 多实例强制（设计 §14）

- 无进程内共享状态作为正确性依赖：熔断器与「渠道跳过至」缓存是优化（N 副本 = N 组），
  正确性由 PG 保证。
- 所有状态流转为**原子条件更新**，禁止 check-then-act：
  - router：`FOR UPDATE SKIP LOCKED` 声明一批事件，匹配/聚合/生成任务与事件终态
    **同一事务**提交；崩溃即回滚、事件仍 pending。
  - sender：声明 = `status→2, attempts+1, next_retry_time=now()+lease`；所有后续写
    以 `(id, create_time, status=2, attempts=声明值)` 围栏；租约过期由回收器转重试。
  - 聚合：批内按 `group_key` 排序后 upsert（确定锁序）；sweeper `UPDATE…RETURNING`
    为汇总唯一数据源。
  - dedup：单语句 `INSERT … ON CONFLICT DO UPDATE … RETURNING`。
  - dlq 重放：`UPDATE…RETURNING` + 新 dispatch 同事务。
- 选主：PG advisory lock 绑定 `Hijack` 的专用连接；连接断即失主；leader-only SQL 自身幂等。
- 时间列以数据库 `now()` 写入，与到期判定同一时钟源（应用/DB 时钟偏差不影响可见性）。

### 13.3 可靠性口径（设计 §7）

- 先落库后应答；at-least-once + `send_key` 幂等键（同一 dispatch 重试同键，重放新键）。
- 退避 `30s→2m→8m→30m→2h` + 0~20% jitter，首发 + 5 次重试；`retryable=false` 立即死信。
- 每次尝试都写 `notify_delivery_log`（含租约回收与死信那次）。
- 限流/熔断/渠道停用/secret 不可用 → 放回 pending 且**不计尝试**。

### 13.4 代码规范补充

- 无裸 goroutine：并发用 `errgroup`；后台任务 ctx 用 `context.WithoutCancel`。
- 出站超时 ≤3s（渠道 `timeout_s` 不得超过）；渲染 100ms、输出 64KB。
- 日志 slog JSON，`password/token/secret/authorization/signature` 字段打码；webhook
  凭据类请求头由 core 写入校验拒绝。
- 工具版本锁定（Makefile），`make check` 全绿后提交；`nolint` 必须写明理由。
- 迁移在 `backend/core/migrations` 统一编号（单库单一 schema 事实源）。

### 13.5 测试

- 单测：biz 用例经接口桩；适配器经 httptest / 进程内 SMTP 桩。
- 集成（`-tags=integration`，`TEST_DATABASE_SOURCE` 库名须含 `test`，会 TRUNCATE）：
  真实 PG 验证声明/围栏/回滚/聚合/门闩/重放/分区维护，以及端到端
  「事件 → router → sender → webhook 收到签名请求」与 1 万事件对账。
- 与 core 的集成测试共用 `reborn_test`，两个模块的集成测试不要并行运行。

### 13.6 共享库 kit（backend/kit）

- core 与 notify 已各写一份的代码才进 kit；kit 包不得 import 任一模块的 internal。
- 分层边界随包走：`kit/dbx`（含 pgx）仅 data 层；`kit/httpx` 仅 server/cmd；`idgen`/`paging`/`notifycontract` biz 可用（depguard 强制）。
- 与 core 对齐的任何契约（jsonb json 标签、状态/来源字面量、事件正则、`InsertEventSQL`/`DedupGateSQL`）只改 `kit/notifycontract`，禁止在本模块复制字面量；biz 侧用类型别名/常量再导出保持包内名称不变。
- `SilenceWindow` 以契约结构为底层类型（非别名）以挂 `Contains` 方法，json 标签随契约。

