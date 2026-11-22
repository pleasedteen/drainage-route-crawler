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
| 同截止点两种模式可达关系相同 | 两种模式共用同一遍历代码路径，差异仅在缓存命中；`tests/equivalence.rs` 逐页比对 |
| 跨站、汇合、成环 | 链接带 `station_id`；`run_pages (run, station, page)` 唯一约束天然去重，环不会死循环 |
| 每站独立频率与在途上限 | `StationController`：每站独立的最小发起间隔 + `Semaphore` 在途闸门 |
| 暂时失败退避重试 | 指数退避 + 抖动，最多 5 次 |
| 站点要求等待则暂停该站、其他站继续 | 识别 `Retry-After`（秒数或 HTTP-date），只暂停对应站的控制器，其余站不受影响 |
| 进程退出后重启续跑，保留进度与抓取结果 | 所有状态确认后落库；启动时 `reset_open_run_pages` 把残留 `fetching/failed` 收回 `pending`，`resume_on_boot` 续跑 |
| 生产 PostgreSQL、本地嵌入式持久化替身 | sqlx `Any` 驱动同一套 SQL 同时支持 Postgres 与 SQLite（本地文件型，零外部依赖） |
| 分页 HTML 与 JSON 两种模拟站 | `src/mock.rs` + `mock-server` 二进制，两种格式各一站 |
| 历史成功轮次可按轮次号修复误收的截止点后设施 | `repair <run_id>`：用轮次**原已固化**的截止点重走路线，结果写入新代次，成功后才一次切换；修复期间查询保持原已发布结果 |

## 目录结构

```
src/
  main.rs            CLI 与服务入口
  lib.rs             模块汇总
  config.rs          TOML 配置
  model.rs           领域模型
  store.rs           持久层（PostgreSQL / SQLite，schema 自动初始化）
  schema/schema.sql  建表语句（方言占位符在连接时替换）
  fetch.rs           抓取：每站频率/并发、退避、Retry-After 暂停
  parse.rs           HTML / JSON 页面解析
  engine.rs          轮次编排、截止点、遍历、断点续跑
  mock.rs            内存模拟站与 HTTP 模拟站
  api.rs             值班员 HTTP 接口
  seed.rs            演示数据
  bin/mock-server.rs 本地 HTML/JSON 模拟站服务
tests/               9 个集成测试（一致性、缓存、续跑、退避、HTTP API）
scripts/             一键测试与崩溃续跑演示脚本
config/              本地与生产配置示例
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

# 按轮次号修复历史轮次（用原截止点重新核实，成功后一次切换）
cargo run -- repair 1
cargo run -- repair-status 1
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
```

### 崩溃—重启续跑演示

```bash
./scripts/demo_resume.sh
```

脚本会启动服务、发起轮次、在中途 `SIGKILL` 进程，再重启服务并展示原轮次自动续跑到完成、
已确认的抓取结果不丢失。

### 历史轮次修复：一条命令验证（本地站点 + 旧库夹具）

```bash
./scripts/verify_repair.sh
```

脚本用真实 HTTP 本地模拟站与一份预置的旧库夹具（一个“声称在早期截止点成功、结果却含
截止点后登记设施”的历史轮次）完整走一遍：注入站点故障使修复失败（查询仍返回原已发布结果、
代次不切换）→ 故障消除后一条命令接着修 → 结果一次切换 → 校验轮次编号/顺序/截止点不变、
误收设施消失、再次修复幂等。

### 修复历史成功轮次（误收了截止点之后登记的设施）

```bash
cargo run -- repair 1            # 按轮次号修复
cargo run -- repair-status 1     # 查看修复进度/代次
```

- **按原登记截止点重走**：使用 `runs.watermarks` 里固化的截止点，重新构建目标修订索引并从
  入口遍历；截止点之后登记的页面/修订不会被误收。
- **保留轮次编号与历史顺序**：只写新的结果代次，不改 `runs.id`、`created_at`、`mode`、
  `status`、`watermarks`，也不新增轮次。
- **修复期间查询不变**：新结果先写下一代 `reachable.generation`，`runs.published_generation`
  指针在全部页面核实成功后才翻转；失败/中断不翻转，按轮次 `/runs/:id/reachable` 与按入口
  `/entries/:id/reachable` 始终返回原已发布结果。
- **重启续修、不重抓**：修复队列（`repair_pages`）与“截止点→修订”记忆（`page_as_of`）都持久化；
  重启后把残留 `fetching/failed` 收回 `pending` 接着修，已核实的目标修订命中缓存/记忆，零请求。
- **一次切换、写入量与设施数无关**：切换只是一条 `UPDATE runs SET published_generation=...`，
  与可达设施数量无关（测试用 `total_changes()` 实测：7 项与 500 项均只写 1 行）。
- **幂等**：已修复且已切换的轮次再次执行 `repair` 为空操作（`already_repaired=true`）。

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
- 只能“按截止点取版本”的页面，其核实结果写入 `page_as_of`；重启/再次修复时直接复用，
  不会因为站点后来又登记了更新的修订而取错或重抓。

### 结果代次与“一次切换”

`reachable` 表带 `generation` 列，`runs.published_generation` 指向当前对外发布的代次。
普通轮次首次发布即第 1 代；修复写入其后一代。查询（按轮次、按入口、进度计数）都只读已发布代次，
因此**构建新结果的过程中旧结果完全可见且不变**。全部页面核实成功后，用一条只改一行指针的
`UPDATE` 完成切换；若修复失败或进程中断，指针不翻转，接着修即可。旧代次行保留在库中，
便于核对与回滚。

### 抓取节奏与隔离

每个站点一个 `StationController`，各自维护最小请求间隔、在途信号量、暂停时刻。
`Retry-After` 只写入对应站点的控制器，因此 A 站被要求等待时，B 站请求照常进行。

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
| POST | `/runs/:id/repair` | 按原截止点修复历史轮次（后台执行，一次切换） |
| GET | `/runs/:id/repair-status` | 修复状态（代次/进度/是否已切换） |
| GET | `/entries/:id/reachable` | 该入口最近成功轮次的可达设施 |

## 测试覆盖

| 文件 | 覆盖点 |
| --- | --- |
| `tests/equivalence.rs` | 全量/增量同截止点可达关系一致；截止点固化使旧轮次不受后续登记影响 |
| `tests/cache.rs` | 同修订跨入口/跨轮次只抓一次；增量只抓变化页（请求计数实证）；新路线补齐 |
| `tests/resume.rs` | SIGKILL 后续跑且保留进度；Retry-After 只暂停本站；临时失败退避重试 |
| `tests/repair.rs` | 截止点后设施不被误收（回归）；按原截止点修复并一次切换；修复期间/失败后查询不变；重启续修不重抓；切换写入量与设施数无关 |
| `tests/http_api.rs` | 真实 HTTP 监听下端到端采集与查询、参数校验 |
