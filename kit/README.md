# kit — core / notify 共享基础库

单仓多模块：被 `backend/core`、`backend/notify` 以 `replace ../kit` 引用（不发版本号）。
只收纳**两个模块已经各自写了一份**的代码（README「共享工具提取规则」：第二个使用方出现才提取）。

| 包 | 内容 | 允许 import 的层 |
|---|---|---|
| `httpx` | protojson 编解码、8MiB body 上限（413）、protovalidate 钩子、日志敏感字段掩码、服务器超时加固、slog 日志装配 | server / cmd |
| `dbx` | pgxpool 构造 + Ping、`WithTx/WithTxOptions`、`LikeContains` 转义、`NilIfEmpty/NilIfEmptyBytes/TextArray`、advisory lock 专用连接 | **仅 data**（含 pgx 依赖，depguard 禁止 biz/service import） |
| `secretbox` | AES-256-GCM 凭据加密（key = sha256(主密钥)，密文 nonce‖ct）；core 只用 Seal、管道只用 Open——"只写不读"靠调用面约束 | data |
| `idgen` / `paging` | UUIDv7；分页规整（缺省 20、上限 100、页码夹紧防 OFFSET 溢出） | biz |
| `notifycontract` | core 控制面与管道共享的**数据契约**：jsonb 结构体、状态常量、事件类型/模式正则与匹配、`notify_event` INSERT 与 `notify_dedup` 门闩 SQL | biz / data |

契约包的守护：两模块的 proto 里重复的正则字面量由各模块的守护测试与 `notifycontract` 常量比对。

约束：kit 不 import 任一模块的 `internal`；本地多模块开发可建 `backend/go.work`（`use ./core ./kit ./notify`，已 gitignore）。
守护测试：`core/internal/biz/notify/contract_guard_test.go` 比对 proto 正则字面量；`secretbox` 向量测试锁定密文格式（两端互通）。

