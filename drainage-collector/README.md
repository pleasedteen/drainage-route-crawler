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
| 站点访问边界：下游页键只能定位到目标站点配置的台账范围 | `boundary::page_locator` 由 `base_url` 推导台账根 `{base}/pages/`：页键字符白名单 + URL 规范化后仍须落在台账根内；引擎入队前校验，越界链接跳过；`Fetcher` 抓取前再校验一次兜底 |
| 自动跳转留在发起站点范围内 | HTTP 重定向策略只跟随同源（scheme/host/端口一致）跳转；跨源 3xx 判为越界错误不重试；合法站内跳转照常跟随（`tests/boundary.rs` 实证） |
| 缓存来源证明：旧缓存首次用到重新核实，核实后跨入口跨重启复用 | `page_revisions` 增加 `source_url/verified_run_id/verified_at`（schema v2 自动迁移，旧行保持 NULL）；命中无证明缓存时按未命中重新抓取并写回证明，此后零请求复用 |
| 历史轮次仍能原样查询 | 迁移只加列不改行；核实只写缓存与证明，不改历史 `runs/reachable` 行（`tests/provenance.rs` 用旧库夹具逐字段比对） |
| 每站独立频率与在途上限 | `StationController`：每站独立的最小发起间隔 + `Semaphore` 在途闸门 |
| 暂时失败退避重试 | 指数退避 + 抖动，最多 5 次 |
| 站点要求等待则暂停该站、其他站继续 | 识别 `Retry-After`（秒数或 HTTP-date），只暂停对应站的控制器，其余站不受影响 |
| 进程退出后重启续跑，保留进度与抓取结果 | 所有状态确认后落库；启动时 `reset_open_run_pages` 把残留 `fetching/failed` 收回 `pending`，`resume_on_boot` 续跑 |
| 生产 PostgreSQL、本地嵌入式持久化替身 | sqlx `Any` 驱动同一套 SQL 同时支持 Postgres 与 SQLite（本地文件型，零外部依赖） |
| 分页 HTML 与 JSON 两种模拟站 | `src/mock.rs` + `mock-server` 二进制，两种格式各一站 |

## 目录结构

```
src/
  main.rs            CLI 与服务入口
  lib.rs             模块汇总
  config.rs          TOML 配置
  model.rs           领域模型
  store.rs           持久层（PostgreSQL / SQLite，schema 自动初始化与迁移）
  schema/schema.sql  v1 建表语句（方言占位符在连接时替换；v2+ 迁移在 store.rs）
  boundary.rs        站点访问边界（页键台账定位、自动跳转同源限制）
  fetch.rs           抓取：每站频率/并发、退避、Retry-After 暂停、边界执行
  parse.rs           HTML / JSON 页面解析
  engine.rs          轮次编排、截止点、遍历、断点续跑、缓存核实
  mock.rs            内存模拟站与 HTTP 模拟站（含故障与跳转注入）
  api.rs             值班员 HTTP 接口
  seed.rs            演示数据
  bin/mock-server.rs 本地 HTML/JSON 模拟站服务
tests/               13 个集成测试（一致性、缓存、边界、来源证明、续跑、HTTP API）
scripts/             一键测试、边界与来源证明验证、崩溃续跑演示脚本
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

### 一条命令验证站点边界与缓存来源证明

用本地模拟站（进程内 + HTTP 监听）与旧库夹具（schema v1 的 SQLite 旧库）验证：

```bash
./scripts/verify_boundary.sh
```

覆盖：越界页键拦截、合法站内跳转可采、跨站自动跳转拒绝、显式跨站链接可采；
旧库自动迁移、旧缓存首次用到重新核实、核实后跨入口/跨重启复用、历史轮次原样可查。

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
# 故障注入：临时失败 / Retry-After / 302 跳转（to 需 URL 编码）
curl -X POST 'http://127.0.0.1:9100/inject/fail?times=2'
curl -X POST 'http://127.0.0.1:9101/inject/retry-after?secs=2'
curl -X POST 'http://127.0.0.1:9100/inject/redirect?to=http%3A%2F%2F127.0.0.1%3A9100%2Fpages%2Fa2'
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

### 站点访问边界

边界由站点配置的 `base_url` 推导，不需要额外配置：

1. **页键定位**：台账根为 `{base_url}/pages/`。页键先过字符白名单（单段安全字符，
   拒绝 `/ \ ? # %` 与 `.`/`..`），再经 URL 规范化校验仍落在台账根内——
   `../state` 之类的路径逃逸无法定位到台账外的资源。引擎在下游链接入队前校验，
   `Fetcher` 抓取前再校验一次兜底；越界链接记警告并跳过，不影响其他页面。
2. **自动跳转**：HTTP 重定向策略只跟随与发起请求同源的跳转；跨源 3xx 判为越界错误，
   不重试。站内 302（如页面搬家）照常跟随。
3. **显式跨站链接不受影响**：`data-station` 指向外站的链接以目标站自己的边界发起
   全新请求，继续可采。

### 缓存来源证明

- `page_revisions` 在 schema v2 增加 `source_url / verified_run_id / verified_at`
  （旧库打开时自动迁移，只加列不改行，旧行这些列为 NULL）。
- 每次抓取落库时一并写入来源证明（最终请求 URL、核实轮次、核实时间）。
- 命中**无证明**的旧缓存时按未命中处理：重新抓取核实并写回证明——
  旧缓存只在“后续轮次首次用到”时付出一次核实代价。
- 证明随库持久化，核实后的修订跨入口、跨轮次、跨进程重启零请求复用。
- 核实只更新缓存与证明，历史轮次的 `runs/reachable` 行不被触碰，原样可查。

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
| GET | `/entries/:id/reachable` | 该入口最近成功轮次的可达设施 |

## 测试覆盖

| 文件 | 覆盖点 |
| --- | --- |
| `tests/equivalence.rs` | 全量/增量同截止点可达关系一致；截止点固化使旧轮次不受后续登记影响 |
| `tests/cache.rs` | 同修订跨入口/跨轮次只抓一次；增量只抓变化页（请求计数实证）；新路线补齐 |
| `tests/boundary.rs` | 越界页键（路径逃逸/多段路径/未配置站点）拦截不入队；合法站内跳转跟随；跨站自动跳转拒绝；显式跨站链接可采 |
| `tests/provenance.rs` | 旧库夹具（schema v1）自动迁移；旧缓存首次用到逐页核实（请求计数实证）；核实后跨入口/跨重启零请求复用；历史轮次原样可查 |
| `tests/resume.rs` | SIGKILL 后续跑且保留进度；Retry-After 只暂停本站；临时失败退避重试 |
| `tests/http_api.rs` | 真实 HTTP 监听下端到端采集与查询、参数校验 |
