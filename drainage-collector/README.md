# 市政排水入流口下游设施采集服务

值班员选定一个入流口后，服务从该入口出发，沿各养护站台账页面中的下游链接遍历设施网络，
核实可达设施并支持进度查询。路线可**跨站、汇合、回流成环**；支持**全量**与**增量**两种采集；
进程崩溃后重启可**接着完成原轮次**。

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 每轮开始确定各站登记截止点 | `Engine::start_run` 逐站读取 `latest_seq`，固化进 `runs.watermarks`（JSON），轮次内不变 |
| 变更清单按登记顺序可续读，业务时间可早于登记 | 一切以登记 `seq` 排序；`change_log (station_id, seq)` 唯一，分页游标 `after` 续读；页面用 `effective_seq` 支持按截止点 `as_of` 取版本 |
| 全量从入口重走 | 从入口页入队，按下游链接做去重遍历 |
| 增量复用未修订页面、补齐修订与新路线 | 同一套遍历引擎；目标修订由“截止点+变更清单”解析，未变页命中 `page_revisions` 缓存零请求，变化页才抓取（测试实证增量仅抓变化页） |
| 同一页面同一修订跨入口共享抓取结果 | `page_revisions` 对 `(station, page, revision)` 唯一约束，全局共享 |
| 常驻服务与独立 CLI 冷缓存并发只抓一次 | `fetch_leases` 跨进程租约单飞：同一 `(站点,页面,目标)` 唯一租约，持租者才发请求，等待者轮询共享，**响应再慢也只发一次** |
| 抓取方退出且结果未落库时等待者自动接手 | 持租者周期性续租；进程退出后租约过期，等待者用一条条件 UPSERT 原子换主接手；`done/gone` 终态继续共享不重抓 |
| 实际请求仍走站点限速/重试，进度按轮次独立维护 | 租约层只做汇合协调，请求仍经 `Fetcher` 的每站间隔/在途闸门/退避；汇合与成环去重仍在各轮次自己的 `run_pages` |
| 同截止点两种模式可达关系相同 | 两种模式共用同一遍历代码路径，差异仅在缓存命中；`tests/equivalence.rs` 逐页比对 |
| 跨站、汇合、成环 | 链接带 `station_id`；`run_pages (run, station, page)` 唯一约束天然去重，环不会死循环 |
| 每站独立频率与在途上限 | `StationController`：每站独立的最小发起间隔 + `Semaphore` 在途闸门 |
| 暂时失败退避重试 | 指数退避 + 抖动，最多 5 次 |
| 站点要求等待则暂停该站、其他站继续 | 识别 `Retry-After`（秒数或 HTTP-date），只暂停对应站的控制器，其余站不受影响 |
| 进程退出后重启续跑，保留进度与抓取结果 | 所有状态确认后落库；启动时 `reset_open_run_pages` 把残留 `fetching/failed` 收回 `pending`，`resume_on_boot` 续跑 |
| 生产 PostgreSQL、本地嵌入式持久化替身 | sqlx `Any` 驱动同一套 SQL 同时支持 Postgres 与 SQLite（本地文件型，零外部依赖） |
| 分页 HTML 与 JSON 两种模拟站 | `src/mock.rs` + `mock-server` 二进制，两种格式各一站，支持慢响应/阻塞注入与实际请求计数 |

## 目录结构

```
src/
  main.rs            CLI 与服务入口
  lib.rs             模块汇总
  config.rs          TOML 配置
  model.rs           领域模型
  store.rs           持久层（PostgreSQL / SQLite，schema 自动初始化与迁移）
  schema/schema.sql  建表语句（方言占位符在连接时替换）
  schema/fetch_leases.sql 跨进程抓取租约表迁移（schema v2）
  fetch.rs           抓取：每站频率/并发、退避、Retry-After 暂停
  parse.rs           HTML / JSON 页面解析
  engine.rs          轮次编排、截止点、遍历、断点续跑、跨进程在途单飞与接手
  mock.rs            内存模拟站与 HTTP 模拟站（慢响应/阻塞注入、请求计数）
  api.rs             值班员 HTTP 接口
  seed.rs            演示数据
  bin/mock-server.rs 本地 HTML/JSON 模拟站服务
tests/               16 个集成测试（一致性、缓存、续跑、退避、跨进程单飞/接手、HTTP API）
scripts/             一键测试、崩溃续跑与跨进程单飞接手演示脚本
config/              本地、生产与单飞演示配置示例
```

## 快速开始

### 前置

- Rust stable（含 cargo）。本机若 `/usr/local/bin/cc` 不可用，项目 `.cargo/config.toml`
  已把链接器指向 `/usr/bin/gcc`；正常环境可删除该文件。

### 一条命令跑完本地测试（依赖就绪后 80 秒内）

```bash
./scripts/run_tests.sh
```

依赖已编译时测试阶段约 2–3 秒；即使包含本项目增量编译也在 30 秒左右。
首次运行会先编译第三方依赖，耗时取决于网络与机器，不计入 80 秒口径。

### 启动服务（本地演示，零外部依赖）

本地配置使用**进程内模拟站 + 文件型 SQLite**，无需起任何外部服务：

```bash
cargo run -- serve
# 监听 127.0.0.1:8080，数据库文件 ./data/drainage.db
```

启动时若发现上次未完成的轮次，会自动恢复并继续执行。

### 操作采集（HTTP）

```bash
# 查看入流口
curl -s http://127.0.0.1:8080/entries

# 启动全量采集
curl -s -X POST http://127.0.0.1:8080/runs/start \
  -H 'content-type: application/json' \
  -d '{"entry_id":1,"mode":"full"}'
# -> {"run_id":1,"status":"running"}

# 启动增量采集
curl -s -X POST http://127.0.0.1:8080/runs/start \
  -H 'content-type: application/json' \
  -d '{"entry_id":1,"mode":"incremental"}'

# 查询轮次进度
curl -s http://127.0.0.1:8080/runs/1/progress

# 查询该轮次已核实可达设施
curl -s http://127.0.0.1:8080/runs/1/reachable

# 按入口查询最近成功轮次的可达设施
curl -s http://127.0.0.1:8080/entries/1/reachable
```

### 操作采集（CLI，无需常驻服务）

```bash
cargo run -- entries
cargo run -- start 1 --mode full --wait          # 全量并等待结束
cargo run -- start 1 --mode incremental --wait   # 增量
cargo run -- progress 1
cargo run -- reachable 1
cargo run -- entry-reachable 1
```

### 独立的 HTML / JSON 模拟站点（可选）

需要验证真实 HTTP 链路、分页与故障注入时，单独起模拟站：

```bash
cargo run --bin mock-server
# HTML 站 http://127.0.0.1:9100 （station-a）
# JSON 站 http://127.0.0.1:9101 （station-b）

# 按修订取页 / 按登记截止点取页
curl 'http://127.0.0.1:9100/pages/a1?revision=1'
curl 'http://127.0.0.1:9100/pages/a1?as_of=2'
# 分页变更清单（每页 3 条，next_after 为下一页游标）
curl 'http://127.0.0.1:9100/changes?after=0'
# 故障注入：临时失败 / Retry-After
curl -X POST 'http://127.0.0.1:9100/inject/fail?times=2'
curl -X POST 'http://127.0.0.1:9101/inject/retry-after?secs=2'
# 慢响应/阻塞注入与实际请求计数（跨进程单飞演示用）
curl -X POST 'http://127.0.0.1:9100/inject/delay?page=a1&delay_ms=200'
curl -X POST 'http://127.0.0.1:9100/inject/hold?page=a1&waiters=1'
curl -X POST 'http://127.0.0.1:9100/inject/release?page=a1'
curl 'http://127.0.0.1:9100/requests'
```

### 跨进程单飞与接手演示（真实双进程）

```bash
./scripts/demo_single_flight.sh
```

脚本启动真实 HTTP 模拟站，再分别以**常驻服务进程**与**独立 CLI 进程**（`--wait`）共用
同一个 SQLite 采集库，从两个入口同时冷启动：先数实际请求证明同页同修订只发一次，
再 `SIGKILL` 抓取方，验证等待者在租约过期后自动接手并完成，最后重启服务续跑、
确认结果共享且无重复修订/残留租约。

采集库诊断（只读，不依赖外部 sqlite3）：

```bash
cargo run -- --config config/single_flight.toml db lease a1        # 某页当前租约
cargo run -- --config config/single_flight.toml db lease-count fetching
cargo run -- --config config/single_flight.toml db dup-revisions   # 应为 0
```

### 崩溃—重启续跑演示

```bash
./scripts/demo_resume.sh
```

脚本会启动服务、发起轮次、在中途 `SIGKILL` 进程，再重启服务并展示原轮次自动续跑到完成、
已确认的抓取结果不丢失。

## 生产部署（PostgreSQL）

1. 准备 Postgres 并建库（表结构由服务启动时自动创建）：

   ```sql
   CREATE DATABASE drainage;
   ```

2. 复制并修改配置：

   ```bash
   cp config/production.example.toml config/production.toml
   # 填写 database.url（postgres://...）与各站 base_url、频率、并发
   ```

3. 启动：

   ```bash
   cargo run --release -- --config config/production.toml serve
   ```

SQL 方言差异（自增主键等）在连接初始化时按驱动自动替换，业务代码与查询语句保持一套。

## 核心设计说明

### 为什么全量/增量天然等价

引擎不区分两种模式的遍历逻辑：

1. 轮次开始固化各站截止点 `watermarks`；
2. 续读变更清单到截止点，构建 `(站点, 页面) -> 目标修订` 索引；
3. 从入口按链接去重遍历，每页先查共享缓存：
   - 命中目标修订 → 零请求直接用；
   - 未命中 → 按站节奏抓取并落库；
4. 解析下游链接并入队，直到队列排空。

“全量从入口重走”与“增量补齐变化”因此只是**缓存命中率不同**，到达相同截止点时遍历的图、
每页的目标修订、最终可达关系逐页相同。

### 断点续跑的正确性边界

- 页面状态机：`pending → fetching → done/failed`，全部在数据库中；
- 抓取结果、链接、设施、可达关系均在确认后立即落库；
- 重启只把没有走完的 `fetching`（进程死亡时不可能有结论）和 `failed`（允许重试）收回；
- 已 `done` 的页面与已缓存修订不会重复抓取，唯一约束兜底幂等。

### 抓取节奏与隔离

每个站点一个 `StationController`，各自维护最小请求间隔、在途信号量、暂停时刻。
`Retry-After` 只写入对应站点的控制器，因此 A 站被要求等待时，B 站请求照常进行。

### 跨进程在途单飞与抓取方接手

常驻服务（`serve`）与独立 CLI（`start`）共用同一个采集库，可能从不同入口**同时碰到冷缓存**。
仅靠 `page_revisions` 唯一约束无法消除“结果落库之前”的重复抓取，因此新增 `fetch_leases`
租约表，在数据库层做跨进程单飞（single-flight）：

1. 冷缓存的抓取目标 `(station, page, target_kind, target_value)` 对应一行唯一租约，
   通过一条带 `WHERE` 的条件 UPSERT 抢租：
   - 行不存在 → 插入并持租，**只有持租者**经 `Fetcher` 发实际请求；
   - 行是 `done/gone` 终态 → 不覆盖，直接共享，不发请求；
   - 行在途且未过期 → 不覆盖，进入轮询等待（即使响应很慢也不会发起第二次请求）；
   - 行在途但已过期（持租进程退出/卡死）→ 原子换主，等待者接手。
2. 持租者后台周期性续租（心跳），把 `expires_at` 向后推；进程退出则心跳停摆，
   最迟一个 TTL 后租约可被接手。租约 TTL/心跳间隔可用
   `DRAINAGE_LEASE_TTL_MS` / `DRAINAGE_LEASE_HEARTBEAT_MS` 调整。
3. 抓取结果确认后先落 `page_revisions`，再把租约置 `done` 并写入实际修订号；
   等待者读到终态后从共享缓存取结果。已确认结果因此继续被所有进程/轮次共享。
4. 可重试失败时持租者主动删除自己的在途租约，等待者可立即抢租，不必干等 TTL。
5. 汇合与成环的去重进度仍按轮次维护在各自的 `run_pages`；租约只协调“同一目标抓一次”，
   不替代轮次状态。实际请求仍 100% 经过 `Fetcher` 的站点限速、在途闸门与退避重试。

启动时 `resume_on_boot` 会把残留的过期在途租约立即标记为可接手，并清理超过保留期的终态租约。

## HTTP 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/stations` | 站点配置（含水位） |
| GET | `/entries` | 入流口列表 |
| POST | `/runs/start` | 启动轮次，body `{"entry_id":1,"mode":"full|incremental"}` |
| GET | `/runs?limit=20` | 轮次列表 |
| GET | `/runs/:id` | 轮次详情（含固化截止点） |
| GET | `/runs/:id/progress` | 进度（总数/完成/在途/失败/可达数） |
| GET | `/runs/:id/reachable` | 该轮次已核实可达设施 |
| GET | `/entries/:id/reachable` | 该入口最近成功轮次的可达设施 |

## 测试覆盖

| 文件 | 覆盖点 |
| --- | --- |
| `tests/equivalence.rs` | 全量/增量同截止点可达关系一致；截止点固化使旧轮次不受后续登记影响 |
| `tests/cache.rs` | 同修订跨入口/跨轮次只抓一次；增量只抓变化页（请求计数实证）；新路线补齐 |
| `tests/resume.rs` | SIGKILL 后续跑且保留进度；Retry-After 只暂停本站；临时失败退避重试 |
| `tests/single_flight.rs` | 双引擎冷缓存并发同目标只抓一次（慢响应实证）；过期租约原子接手；终态共享不覆盖；汇合/成环按轮次独立；限速不被绕过 |
| `tests/http_api.rs` | 真实 HTTP 监听下端到端采集与查询、参数校验 |
| `scripts/demo_single_flight.sh` | 真实双进程（serve + CLI）：数实际请求、SIGKILL 抓取方后自动接手、结果共享 |
