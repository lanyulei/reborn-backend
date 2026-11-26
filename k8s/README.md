# k8s — 容器平台数据面

Kratos v3 单二进制。持有到各集群 API Server 的连接，执行 core 转发来的资源请求，承载资源缓存、日志流、
终端会话、授权同步与审计落库。**浏览器不直接访问本服务**：core 完成登录与接口授权后，把请求连同签名的
用户身份转发过来；能否对具体资源操作成功，由集群按该用户的数据范围判定（Impersonation）。

本服务**只读写 `k8s_*` 表**，不 import core / notify / cmdb / monitor / job。与 core 共享的只有表结构
（`core/migrations/0072_k8s_init`）与契约包 `backend/kit/k8scontract`（内部接口的路径与请求 / 响应结构、
身份头的签名与校验、状态字面量、档位到集群角色的映射）。设计见 `../docs/k8s-tech-design.md`，
工程规范见 [`../notify/AGENTS.md`](../notify/AGENTS.md)。

## 组件

| 组件 | 说明 |
|---|---|
| 内部接口监听（8060，仅 core 可达） | `/internal/v1/op/*`（JSON）、`/internal/v1/stream/*`（SSE）、`/internal/v1/exec`（WebSocket）、`/healthz`、`/metrics` |
| `heartbeat` 循环（5s） | 刷新 `k8s_instance`；core 据此找到存活实例 |
| `ownership` 循环（租约 / 3） | 续租 + 领取无主或租约过期的集群（`SKIP LOCKED`，按存活实例数均分），对齐连接 |
| `config-poll` 循环（5s） | 轮询 `k8s_cfg_version`：集群配置变化 → 重建连接；数据范围变化 → 清空权限询问缓存 |
| `probe` 循环（`probe_interval`）、`probe-new`（2s） | 读版本与节点数；在线集群连续 3 次失败判为不可达；状态翻转写 `k8s_notify_outbox`。新领取 / 配置刚变更的集群由 `probe-new` 立即探活并做接入自检 |
| `scope-sync` 循环（2s 检查） | 把 `k8s_role_scope` 对账为集群内的 RoleBinding / ClusterRoleBinding；触发条件：数据范围版本变化、命名空间集合变化、距上次对账 10 分钟 |
| `instance-purge`（15s，选主）、`maintenance`（1h，选主，启动即跑一轮） | 清理下线实例；审计月分区预建 / 按保留期 DROP；清理过期的注册令牌与终端会话记录 |

Agent 隧道入口（8061）尚未实现：`access_mode = 1` 的集群目前恒为不可达。接缝是
`internal/data/kube.TunnelDialer`（`DialCluster(ctx, clusterID) (net.Conn, error)`，在隧道上开一条到该集群
API Server 的明文 HTTP/1.1 流）；隧道服务端实现它并在 `internal/data.NewTunnelDialer` 里替换缺省的
`OfflineDialer` 即可，其余代码不感知接入方式（终端的协议升级同样走这个拨号函数）。

## 身份与权限

- 每个内部请求都带 `X-Reborn-Identity` + `X-Reborn-Signature`（HMAC-SHA256，密钥 `plane.internal_key`，
  有效期 60 秒）；校验失败一律 401。除「测试连接」外还要带 `X-Reborn-Cluster`
- 访问集群有两套互不可达的客户端（`internal/data/kube/conn.go`）：
  - **用户身份**：只能经 `conn.asUser(ctx, id)` 取得；传输层对每个请求加 `Impersonate-User: reborn:<用户 ID>`
    与 `Impersonate-Group: reborn:role:<角色 ID>`…，请求上下文里没有合法身份则**拒绝发出**；调用方自带的
    `Impersonate-*` 与 `Authorization` 一律丢弃
  - **平台身份**：探活、接入自检、资源缓存、授权同步、权限询问。传输层去掉任何 `Impersonate-*` 头
- 读缓存前先以平台身份向集群提交 SubjectAccessReview（结果缓存 30 秒，数据范围变化或授权同步落地即清空）。
  请求全部命名空间而用户没有集群级权限时，只返回其数据范围内且集群确认可读的命名空间（`partialNamespaces`）
- 详情、YAML、事件、未缓存的资源与 CRD、全部写操作、日志、终端：以用户身份直接访问集群
- 集群内由平台管理的对象以 `reborn:` 为名称前缀（并带标签 `app.kubernetes.io/managed-by=reborn`）。对账
  **按名称前缀**认定归属：标签可以被有权编辑该对象的人摘掉，名称不能改；`reborn:` 因此是平台的保留命名，
  集群里不要用它给别的对象起名（Agent 自己的绑定 `reborn:agent` 除外，对账不碰它）
- 档位到集群角色：只读 → `view`，开发 → `edit`，命名空间管理员 → **`reborn:ns-admin`**（平台维护：内置 `admin`
  的规则去掉对 RBAC 对象的写权限与 escalate / bind / impersonate），集群管理员 → `cluster-admin`。不直接绑内置的
  `admin`：它能写 RoleBinding，持有者可以把权限绑给自己的平台用户名，平台收回授权后依然有效
- 不归平台管理、但主体是平台用户 / 分组（`reborn:…`）的绑定，平台不删除，只写进集群的 `scope_error` 供人工处理
- Secret / ConfigMap 的列表缓存用**元数据客户端**（PartialObjectMetadata）：平台凭据从不请求它们的内容
  （因此 Secret 列表没有「类型」列）；内容只在用户以自己的身份查看单个对象时读取

集群刚被领取（或配置刚变更）后，要等接入自检通过且**成功完成**一次授权对账才开始承载请求（此前返回 503
`K8S_CLUSTER_UNREACHABLE`）：超管的绑定也是由对账建立的。数据范围配置变更后有 60 秒宽限期；对账一直失败
（原因见 `scope_error`）超过宽限期即不再放行——此时集群里生效的还是旧授权。各集群并行对账、各有 30 秒时限
与失败退避，一个挂起的集群不拖住其余集群。

## 读路径

- 走缓存的资源（清单在 `k8scontract.CachedResources()`）：首次访问时启动 reflector，同步完成前请求等待至多
  `cache_sync_timeout`（超时 503 `K8S_CACHE_SYNCING`）；`cache_idle` 内无人访问且没有订阅则释放。
  **缓存里只存列表行**（列的取值、状态、标签、所有者 UID），不存对象原文——`managedFields`、
  last-applied 注解、ConfigMap / Secret 的内容都不进内存
- 列表：名称关键字、标签选择器、状态 / 节点 / 所有者筛选、任意列排序、分页、准确总数
- 未缓存的资源与 CRD：向集群请求表格格式（列与 `kubectl get` 一致），用 `limit` / `continue` 翻页，
  `capabilities` 告知前端哪些能力可用
- `memory_limit_mb` > 0 时，堆内存超过它则拒绝**新建**缓存（已有的不受影响）

## 写路径与审计

预检（`dryRun=All`，返回前后两份 YAML）→ 提交（带预检时的 `resourceVersion`，冲突返回 409）。快捷操作在本服务内
翻译为集群调用。Secret：读到的 `data` 是掩码，原样提交时掩码值会还原为当前值，不会把真实内容覆盖掉。

每个写操作（成功与失败）、Secret 明文查看、打开终端都写 `k8s_audit`：对象去掉 `managedFields` / `status`，
Secret 的值掩码，单个对象超过 256KB 置空并标记 `truncated`。审计写入失败不回滚已完成的集群操作——记错误日志
并计入 `k8s_audit_write_failures_total`（应对此指标告警）。

## 日志与终端

- 日志：SSE，每行一个 `log` 事件，事件 id 是续接游标 `<时间戳>/<该时间戳下的行序号>`（同一时间戳可以有多行）。
  续接时带上最后一个 id，不丢不重。单条流最长 1 小时（到点下发 `end: max_duration`，客户端续接）；
  每用户至多 10 条并发日志流；单行超过 16KB 截断
- 终端：只用 WebSocket 协议 `v5.channel.k8s.io`（K8s ≥ 1.32），优先 bash、没有则 sh。连接异常中断后会话保留
  60 秒，期间的输出进 256KB 环形缓冲，同一用户带 `sessionId` 接回时先回放；WebSocket 正常关闭视为用户主动结束。
  空闲 15 分钟、最长 8 小时自动结束；每用户至多 5 个会话；集群 `exec_disabled` 时拒绝

## 应用（Helm）与命名空间配额

- **仓库与 Chart**（`helm-repo-test` / `helm-charts` / `helm-chart`，与集群无关，任一实例都可执行）：仓库配置读自
  `k8s_helm_repo`（core 写，凭据在这里解密）。HTTP 仓库的 `index.yaml` ≤ 48MiB、Chart 包 ≤ 20MiB、缓存 5 分钟
  （按仓库的 version 失效，至多 8 个仓库）；下载时校验索引里的摘要。凭据只发给与仓库同主机的地址。
  不拦私网地址（仓库由持有仓库管理权限的人登记，立场同监控数据源），拨号时拒绝链路本地 / 云元数据地址，重定向至多 5 次。
  OCI 仓库不读本机的 docker / helm 凭据文件。
- **Release**（`helm-list` / `helm-get` / `helm-preview` / `helm-install` / `helm-upgrade` / `helm-rollback` / `helm-uninstall`）：
  用 Helm SDK（`helm.sh/helm/v3`，只在 `internal/data/kube`）执行，`RESTClientGetter` 建在**用户身份**的传输层上——读写
  Release 的 Secret、创建资源、执行钩子都由集群按调用者的数据范围判定，平台身份不参与。只读档位读不了 Secret，因此看不到 Release（403 并说明原因）。
  动作配置每次请求新建，不按集群缓存。预检是服务端 dry-run，不写 Release。
- **写操作**在脱离请求的上下文里执行（时限 5 分钟，不 `--wait`、不 `--force`、允许钩子）：请求至多等 20 秒，
  未完成返回 `pending`，操作继续。审计恰好一条（`helm_install` / `helm_upgrade` / `helm_rollback` / `helm_uninstall`，
  资源 `helmreleases`）：按时完成随响应落库，否则结束时补记；values 进审计前把键名像凭据的值替换为掩码。
- 不检查 Chart 的模板是否越出目标命名空间：越界的对象由集群按用户的数据范围拒绝。
- `namespace-quota`：以用户身份读取 ResourceQuota 的用量，同一资源取使用率最高的上限。

## 配置

见 `configs/config.yaml`。敏感项只走环境变量：

| 环境变量 | 说明 |
|---|---|
| `KRATOS_DATA_DATABASE_SOURCE` | PostgreSQL DSN（与 core 同库；生产环境的登录账号应是 `k8s_plane_role` 的成员） |
| `KRATOS_K8S_INTERNAL_KEY` | 内部密钥，≥ 32 字节，与 core 读同一个环境变量（原 `KRATOS_PLANE_INTERNAL_KEY` 已改名）；缺失时所有内部请求都被拒绝，与 core 登记的指纹不一致时启动失败 |
| `KRATOS_NOTIFY_MASTER_KEY` | 与 core 一致的加密主密钥（解密直连集群的凭据） |
| `KRATOS_SERVER_HTTP_ADVERTISE_ADDR` | core 访问本实例的地址；多实例部署时每个实例填自己的可达地址 |
| `K8S_PPROF=1` | 在内部接口监听上挂出 `/debug/pprof/*`，**只应答来自本机回环地址的请求**（堆里有集群令牌与内部密钥） |
| `K8S_HELM_ALLOW_LOOPBACK=1` | 允许 Helm 仓库指向回环地址（仅本地开发与端到端测试；缺省拒绝回环、链路本地与元数据地址） |

**审计表只追加**（迁移 0092 / 0093）：`k8s_audit` 的属主是 `k8s_plane_role`（审计月分区的预建与清理由数据面执行），
但该角色对自己这张表的 UPDATE / DELETE / TRUNCATE 已被收回——只能插入与查询，审计记录不能被改、删、清空；
core 角色（`k8s_core_role`）同样只有 SELECT / INSERT。例外是 DEFAULT 分区保留 DELETE：新月份的分区建好时要把
此前落进 DEFAULT 的行搬过去。数据面每建出一个新分区都会对新分区做同样的收回（`ensurePartition`）。这只是防误操作：
持有该角色凭据的人仍可以自己把权限授回去。

## 运行与检查

```bash
export KRATOS_DATA_DATABASE_SOURCE='<DSN>' KRATOS_K8S_INTERNAL_KEY='<32+ 字节>' KRATOS_NOTIFY_MASTER_KEY='<主密钥>'
go run ./cmd/k8s -conf configs
curl http://127.0.0.1:8060/healthz
curl -s http://127.0.0.1:8060/metrics | grep '^k8s_'

make gen     # conf.proto 与 wire
make check   # gofumpt + golangci-lint + govulncheck + go test -race
```

## Agent 隧道

无法从数据面直连的集群经 Agent 接入：Agent（`cmd/agent`）部署在目标集群内或其转发节点上，**主动向外**连到数据面的
隧道入口（`server.tunnel`，缺省 8061），数据面的请求经这条隧道到达该集群的 API Server。

| 路径 | 作用 |
|---|---|
| `POST /agent/v1/register` | 用一次性的注册令牌换取长期令牌（单事务：校验 → 删除注册令牌 → 写长期令牌；每个集群至多 2 个，超出删最旧的） |
| `GET /agent/v1/connect` | WebSocket（子协议 `reborn.k8s.tunnel.v1`，`Authorization: Bearer <长期令牌>`），其上跑 yamux |

- **只有数据面能开流**（数据面是 yamux 客户端）。Agent 主动开流视为违规，隧道立即关闭
- 每条流是一条到 API Server 的明文 HTTP/1.1 连接。Agent 把请求反向代理到**启动时配置的那一个**地址：请求里的绝对 URI、
  Host 头不参与选路，不跟随跳转，拒绝 CONNECT；入站的 `Authorization` / `Cookie` 一律丢弃，换上 Agent 自己的凭据，
  `Impersonate-*` 原样保留
- 入口只有上述两个路径（其余 404）。认证之前：监听层连接数上限 4096；按来源限速（120 次 / 分钟，IPv6 按 /64；
  通过认证的请求不计入）、单个来源在途请求 ≤ 8、查库比对令牌的并发 ≤ 64（只包住查库那一步）、注册请求体 ≤ 4KiB
  且须在 3 秒内读完。令牌不存在、过期、密钥不对的应答完全相同（401）；令牌正确但集群被停用返回 403。不记请求日志
- 入口前面有负载均衡 / TLS 终结时，把它们的网段填进 `server.tunnel.trusted_proxies`：来源取 `X-Forwarded-For`
  的最后一跳；不填则所有 Agent 共用代理的限额
- Agent 是不受信的一端：经隧道的请求等待响应头至多 60 秒，非流式响应体至多 256MiB（watch、日志跟随、终端不限）
- 心跳 15 秒，对端无响应至多 45 秒判定断开
- **归属**：持有隧道的实例负责该集群（Agent 集群不参与租约领取）。同一集群的第二条隧道作为备用保持连接，
  承载的那条断开后接替。隧道全部断开超过 20 秒，集群标为不可达（`agent is not connected`）并发通知
- **吊销**：core 删除令牌并 bump `k8s_cfg_version`；数据面每 5 秒核对在用令牌，失效的隧道立即断开。
  Agent 连续被拒（401）至少 5 次且持续 5 分钟以上，才删除本地令牌并以非零状态退出（需重新生成安装命令）；
  偶发的 401（前置代理、WAF）与集群被停用（403）只按最大间隔重试，令牌保留

### Agent 配置

| 环境变量 / 参数 | 说明 |
|---|---|
| `REBORN_SERVER_URL` / `-server-url` | 隧道入口地址（`https://…`；`http` 仅限开发） |
| `REBORN_REGISTER_TOKEN` | 注册令牌；已有长期令牌时不使用 |
| `REBORN_KUBECONFIG` / `-kubeconfig` | 转发节点模式：集群的 kubeconfig。为空 = 集群内模式（服务账号） |
| `REBORN_STATE_DIR` / `-state-dir` | 转发节点模式：长期令牌存为该目录下的 `token`（0600） |
| `REBORN_NAMESPACE` / `-namespace` | 集群内模式：长期令牌存为该命名空间的 Secret `reborn-agent-token`（缺省 `reborn-system`） |
| `REBORN_CA_FILE` / `-ca-file` | 校验隧道入口证书的 CA；空 = 系统根证书 |
| `REBORN_INSECURE_SKIP_VERIFY` / `-insecure-skip-verify` | 跳过入口证书校验（仅限开发） |

构建：`make agent`（二进制）、`make agent-image AGENT_VERSION=<版本>`（镜像，distroless nonroot，可只读根文件系统运行）。

### 端到端验证

`internal/e2e`（构建标签 `e2e`）对着一个真实集群与一个已启动的数据面，经内部接口走完读、写、数据范围、
日志续接、终端断线接回与审计。它会在集群里创建并删除 `e2e-*` 命名空间与一个 CRD，往数据库写一行集群登记
——只对测试库与测试集群运行：

```bash
E2E_PLANE_URL=http://127.0.0.1:8060 E2E_INTERNAL_KEY=... E2E_MASTER_KEY=... \
E2E_DSN='<测试库 DSN>' E2E_KUBECONFIG=/path/to/kubeconfig \
go test -tags e2e ./internal/e2e -count=1 -v
```

加 `E2E_AGENT=1` 时集群以 Agent 方式登记（需另行启动 Agent 并写入注册令牌），同一套用例经隧道执行。

`TestApps` 与 `TestChange` 另需 `E2E_HELM_REPO`（一个 HTTP Helm 仓库，含 Chart `demo` 的 0.1.0 / 0.2.0；
说明见 `apps_e2e_test.go`），仓库在本机时数据面要带 `K8S_HELM_ALLOW_LOOPBACK=1` 启动。`TestChange` 直接往
`k8s_change` 写「已批准」的变更单，验证数据面的执行：不可重入的操作不会执行两次、计划与变更单绑定、
Helm 的 Chart 与渲染结果须与审批时一致、命名空间申请可重试。

## 多实例

| 状态 | 存储 | 正确性 |
|---|---|---|
| 集群归属 | `k8s_cluster_runtime` 租约 | `SKIP LOCKED` 领取 + 续租；过期可被重领；不负责的集群返回 421 `K8S_NOT_OWNER`，core 重新选实例 |
| 探活结果、授权同步结果 | PG | 写入以 `owner_instance` 围栏，归属已转移的实例写不进去 |
| 资源缓存、权限询问缓存 | 负责实例的进程内 | 归属转移后在新实例重建；写与详情由集群当场判定 |
| 终端会话、日志流 | 负责实例的进程内 | 随归属结束；core 重启可续接 |
| 审计、通知发件箱 | PG | 只增；发件箱 `dedup_key` 唯一 |
| 维护任务 | PG advisory lock 选主 | 语句自身幂等 |

## 治理（P3）：集群代理、文件传输、终端录制、调试容器、巡检

| 能力 | 入口 | 要点 |
|---|---|---|
| 集群 API 透传（限时 kubeconfig） | `/internal/v1/kube/<集群 API 路径>` | core 逐请求校验 kubeconfig 令牌后转来；数据面以该用户的身份原样转给集群，不缓冲（watch、日志、exec / attach 的协议升级都可用）。只允许 `/api` `/apis` `/version` `/openapi` `/healthz` `/livez` `/readyz`；拒绝 `portforward` 与 `proxy`；集群禁止终端时拒绝 exec / attach。入站的 `Authorization`、`Impersonate-*` 与平台内部头一律丢弃。写请求与 exec 记审计（方法、资源、名称、令牌 ID；不记请求体） |
| 容器文件传输 | `GET /internal/v1/file/download`、`POST /internal/v1/file/upload` | 经容器内的 `tar`（以用户身份 exec）。下载时把容器产生的 tar 当作不可信输入逐条目解析：单文件只下发内容；目录重新编码，丢弃绝对路径、含 `..`、目标之外的条目、指向外部的链接与设备文件。上传由数据面构造只含一个条目的 tar。上限 `plane.max_file_mb`（缺省 100）。集群禁止终端时同样禁止 |
| 终端录制 | `k8s_exec_record` | 只录输出与窗口大小，不录输入；输出路径上只做内存追加，由会话管理循环每 3 秒或满 256KiB 压缩落库；单会话上限 `plane.exec_record_max_mb`（缺省 8），超过即停止并标记不完整。`plane.exec_record_disabled` 关闭 |
| 调试容器 | 终端 `kind = 2` | 经 `pods/ephemeralcontainers` 给 Pod 加一个临时容器（共享目标容器的进程命名空间）并进入；镜像须在 `plane.debug_images` 内（第一个为缺省）。终端里的 shell 退出时临时容器随之结束；一个 Pod 至多 8 个。权限来自平台管理的 ClusterRole `reborn:debug`，随「开发」「命名空间管理员」档位的授权一并绑定（内置的 edit / admin 不含该子资源） |
| 巡检 | `inspection` 循环（15s 检查一次，每轮至多一个集群） | 手动触发的运行由 core 写入待执行行；定时运行按 `plane.inspection_interval`（缺省 24h）由负责实例自己建。检查项见 `kit/k8scontract` 的 `Check*`。每个集群保留最近 30 次 |

巡检的开销：节点、Pod、Deployment、StatefulSet、PVC 各一次分页遍历（每页 500），TLS Secret 一次（每页 100，字段选择器只取
`type=kubernetes.io/tls`）。全部以平台身份直接读集群、逐页处理后丢弃，内存与一页的大小成正比，不依赖也不建立缓存。
1200 节点 / 5 万 Pod 的集群约 110 次列表请求，受该集群的客户端限速约束。私钥不解析、不保留。

## 已批准变更单的执行

`change-exec` 循环领取本实例负责的集群上「已批准」的变更单（`k8s_change`），以发起人的身份执行。保证：

- **不重跑已经开始的变更**：比对与预检都通过、第一次真正改动集群之前，先把 `result.started` 落库（写不进去就不动手）。
  此后执行中断（进程退出、超时、租约过期）的变更单记为失败并注明「可能已部分生效，请核实」，不再放回队列——
  触发任务、驱逐、重启、排空、Helm 都不可重入。尚未开始时遇到集群不可用，才放回队列重试
- 手动触发 CronJob 时，Job 名由变更单 ID 决定：万一同一张变更单被执行第二次，创建因重名而失败
- **结果先于审计回写**，并重试（脱离请求取消的上下文）；结果实在写不进去时，租约过期后按上一条处理
- Helm：等到真正的结果才记成功（不转后台）；安装 / 升级时 Chart 包的摘要与渲染出的清单须与生成计划时一致
  （`HelmCheck.chartDigest` / `manifestHash`），否则拒绝并要求重新提交。模板里有随机内容的 Chart 无法固定渲染结果，
  此时只固定 Chart 包
- 排空有 Pod 驱逐失败或未完成时，变更单记为失败
- 计划里带变更单 ID 与集群 ID，解密后核对（`ChangeExec.changeId` / `clusterId`；为空不核对）
- 申请命名空间可重试：命名空间带注解 `reborn.io/change-id`，重试时认出是自己建的就继续补齐配额；保留名
  （`kube-*`、`default`、`reborn-system`）与标签在数据面再校验一次

## Helm 的 values 与清单

Release 详情与预检返回的 values、清单一律掩码（键名像口令的值、`{name: X_PASSWORD, value: …}` 形态的条目、
清单里 Secret 的 `data` / `stringData`）。升级时提交的 values 里，等于掩码（`******`）的标量表示「保持现值」，
数据面按路径从当前 Release 换回（列表元素带 `name` 的按名对应）；现值里没有该路径（含安装）则 400 并指出路径。
掩码绝不会作为真实的值写进 Release。
