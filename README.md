# K8s 测试命名空间清退留存演练（ns-retain-drill）

值班员清退测试命名空间时，**指定 PVC 的数据要留下**，其余卷随环境回收。本服务用
FastAPI + 文件模拟集群 + SQLite 进度，完整演练这一流程：开单 → 逐卷留存并独立读回
校验 → 按 PV 回收策略删除 → 删除命名空间 → 凭单号读回数据。支持删除丢包注入、
重启核对续办、按开单 UID 清理（同名重建不受影响）。

**DONE 工单还可启用缺件保护**：对 2~4 个等长留存卷生成 XOR 奇偶保护卷；任一留存
文件被移走后重启，仍可凭原下载链接重建取回，整卷通过开单 SHA-256 才发送正文。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

数据（模拟集群、留存副本、SQLite）持久化在命名卷 `drill-data` 中，容器重启不丢进度。

### 本地运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
DATA_DIR=./data .venv/bin/uvicorn app.main:app --port 8000
```

### 自测（原 44 项 + 缺件保护 66 项断言，含丢包后续办、重启核对与并发抢锁）

```bash
python3 -m pip install httpx   # 沙箱无 venv 时可用 --user --break-system-packages
python3 tests/selftest.py
python3 tests/selftest_protection.py
```

## 演练流程（全部可在 /docs 操作）

1. `POST /sim/seed` 造一个命名空间和若干已停写绑定卷（文件保存，可指定 PV 回收策略）。
2. `POST /tickets` 开单：`{"namespace": "demo", "retain_pvcs": ["data-a"]}`（至少一个）。
   开单时快照命名空间 UID、全部 PVC 的 UID/大小/sha256，记录基线字节数。
3. 可选：`POST /sim/fault` `{"drop_delete_responses": 1}` 注入删除丢包
   （服务端生效但回包丢失）。
4. `POST /tickets/{id}/advance` 推进清理；`GET /tickets/{id}` 查看进度、阻塞原因、
   存储水位（baseline / cap / current / peak）。
5. 丢包后工单阻塞并注明“结果未明”；再次 advance 或**重启服务**即自动核对：
   目标已消失则标记确认（**不重发**），仍在才重发。
6. `DONE` 后 `GET /tickets/{id}/retained/{pvc}` 凭单号读回所选数据。

## 缺件保护演练（DONE 工单，全部可在 /docs 操作）

1. `POST /tickets/{id}/protection/enable`：为 **DONE** 工单的 2~4 个**等长**留存卷
   生成 XOR 奇偶保护卷 `retained/<id>/.protection/parity.bin`。生成后对保护卷做
   暂存回读（长度+SHA-256），**完整落盘并核对通过后才把状态置为 `protected`**，
   在此之前 `/protection` 只显示 `enabling`/`interrupted`。
2. `GET /tickets/{id}/protection`：查看保护状态、各卷存活/缺失、可否单件重建、
   保护正文+临时正文水位与读取/缓冲预算。
3. 演练停服改动正文：停服后把某个 `retained/<id>/<pvc>/data.bin` 移走，再启动服务。
   状态中该卷变为缺失；访问**原链接** `GET /tickets/{id}/retained/{pvc}` 即自动
   重建：用保护卷 XOR 其余存活卷，内联计算整卷 SHA-256，**与开单快照一致才落盘并
   发送正文**（连 meta.json 一起移走也会按开单快照补写）。
4. 同单缺两件（或保护卷本身缺失）→ 缺件下载返回 409；重建哈希不符 → 409 且清除
   临时正文，可在修复后重试。
5. 启用中断（进程被杀/重启）：遗留的 `parity.tmp`/`.rebuild.tmp` 在启动时清除
   （临时正文不累积），状态置 `interrupted`，再次调用 enable 即可成功。

### 缺件保护的资源纪律（代码内强制计量，非口头约束）

| 约束 | 实现 |
| --- | --- |
| 保护正文+临时正文 ≤ 3 个卷大小 | 阶段边界 `_assert_protection_disk_budget` 统计 `.protection` 与各 `.tmp` 落盘字节，超 3V 即中止 |
| 一次缺件下载最多读 4 个卷大小（含暂存回读） | `ReadMeter` 逐字节计量，预算 4V，超限 `ReadBudgetExceeded`→409；N=4 时 4 个输入卷恰读满，故重建在异或输出流上**内联**做整卷哈希而非独立回读（启用路径仍有真实暂存回读，读 (N+1)V） |
| 正文缓冲 ≤ 1 MiB | 分块 128 KiB 流式异或，最多 4 个输入块同时在内存（512 KiB），绝不整卷载入 |
| 同单启用/缺件下载同时只处理一次 | 每工单一把非阻塞任务锁，抢不到立即 409；**现存卷的正常下载不持该锁**，互不阻塞 |
| 演练只在停服时改动正文 | 服务自身只写 `.protection` 与 `.tmp` 并原子改名；对正式留存卷的“移走”动作由演练在停服时手工完成 |

## 需求映射

| 要求 | 实现 |
| --- | --- |
| /docs 指定命名空间 + ≥1 个保留 PVC | `POST /tickets`，`retain_pvcs` `min_length=1` |
| 推进清理、查看进度与阻塞原因 | `POST /tickets/{id}/advance`；`GET /tickets/{id}` 返回阶段、`blocked_reason`、逐步 ops |
| 文件保存已停写绑定卷 | `data/cluster/pvs/<pv>/data.bin` + `meta.json`（`stopped_writing: true`） |
| 按 PV 回收策略处理 PVC 删除 | Delete 删数据 / Recycle 清空转 Available / Retain 保留转 Released（`simcluster._remove_pvc_locked`） |
| 模拟删除生效后丢回包 | 故障注入计数器：删除落盘后抛 `DroppedResponseError`，操作记为 `unknown` |
| 独立读回确认后才删源 PVC | 留存副本落在集群目录之外，sha256 与**开单快照**比对通过（`verified`）后才发删除 |
| 合计字节 ≤ 开单基线 + 最大选中卷 | 逐卷“拷贝→验证→删源”，拷贝前检查 `current+size ≤ cap`，`peak_bytes` 全程记录 |
| 原命名空间与未选卷移除后才完成 | `DONE` 门槛：命名空间不存在且该 UID 下无任何 PVC；Retain 残留的 Released PV 在 `leftover_pvs` 中明示 |
| 凭单号读回所选数据 | `GET /tickets/{id}/retained[/{pvc}]`，DONE 后仍可下载；留存缺失且已保护时自动重建 |
| 进度入 SQLite，重启核对未明删除 | `tickets/pvcs/ops` 三表；启动与每次推进前 `reconcile()`：GET 核对，消失即确认、仍在才重发 |
| 已确认的删除不再发送 | ops 幂等键 `ticket:kind:uid`，`confirmed` 直接短路；审计日志可证每目标仅一条删除 |
| 按开单 UID 清理，同名重建免疫 | 所有删除/查询按开单快照 UID 寻址；重建命名空间 UID 不同，旧单触碰不到 |
| 四个等长留存卷的缺件保护 | `protection` 表 + XOR 奇偶卷（`app/protection.py`）；`POST .../protection/enable`、`GET .../protection` |
| 核对各卷开单 SHA-256，完整落盘才显示已保护 | 启用时对保护卷暂存回读核对，通过后原子改名并置 `protected`；重建以内联整卷哈希比对开单快照 |
| 启用中断可重试、临时正文不累积 | 启动清理 `parity.tmp`/`.rebuild.tmp`，`enabling` 置 `interrupted`；重试先清旧临时文件 |
| 移走任一留存文件重启后原链接取回 | 原下载链接检测到缺失即走 `_rebuild_missing`：保护卷 XOR 存活卷，整卷过开单 SHA-256 才发送 |
| 同单缺两件/重建校验失败报错 | 缺 ≥2 件或保护卷缺失 → 409；哈希/长度不符 → 409 并清临时正文，可重试 |
| 同单启用/缺件下载串行，现存卷照常下载 | 每工单非阻塞任务锁（409 冲突）；现存文件路径直接返回，不抢任务锁 |
| 3V/4V/1MiB 资源上限 | 磁盘 3V 阶段检查、读取 4V `ReadMeter` 硬上限（回读计入）、128KiB 分块峰值 512KiB |

## 目录结构

```
app/
  main.py        FastAPI 路由（演练面 + 模拟端管理面），启动时核对未明删除
  engine.py      清理状态机 + 缺件保护/重建编排（任务锁、预算检查、重启恢复）
  protection.py  XOR 奇偶卷：流式异或、ReadMeter 读取计量、暂存回读、原子落盘
  simcluster.py  文件模拟集群：命名空间/PVC/PV、回收策略、丢包注入、审计日志
  db.py          SQLite（WAL），含旧库 protection 表平滑升级
  models.py      请求模型
tests/selftest.py             端到端自测（起停真实服务进程，含重启核对，44 项）
tests/selftest_protection.py  缺件保护端到端自测（重启取回/缺两件/校验失败/并发/中断，66 项）
Dockerfile / docker-compose.yml
```

## 验证

- `tests/selftest.py` 共 44 项断言，全部通过，要点：

- 注入 1 次丢包 → 删除生效、回包丢失、工单阻塞且原因可读；
- **重启服务** → 启动核对把未明删除标记确认且**不重发**（审计日志每目标仅 1 条删除）；
- 续办推进至 DONE：原命名空间与未选卷移除，Retain 的 PV 保留为 Released；
- 存储峰值**恰好等于** 基线+最大选中卷（逐卷拷贝、验后即删的边界）；
- 凭单号读回 data-a/data-b，sha256 与开单快照一致；
- 同名重建命名空间 UID 不同，旧单重复推进不影响新空间；新空间可独立开单，
  并再次演练 PVC 删除与命名空间删除两次丢包的核对续办。

- `tests/selftest_protection.py` 共 66 项断言，全部通过（连续多次运行稳定），要点：

- 仅 DONE 工单可启用，等长校验、2~4 卷范围、重复启用幂等；
- 启用读取 (N+1)V（含保护卷暂存回读），落盘 1V、临时为 0，保护+临时 ≤ 3V；
- 移走单卷（含整目录连 meta.json 移走）后**重启**，原链接 200 取回且 sha 与开单一致，
  重建读取精确 4V、无 `.tmp` 残留；
- 同单缺两件 → 409，现存卷照常 200；污染保护卷 → 重建 SHA-256 失败 409 且无临时残留，
  修复后重试成功；
- 持锁并发确定性验证：同单第二个启用/缺件请求立即 409，第一个成功；
- 中断现场（enabling 行 + 遗留临时正文）重启后被标 `interrupted`、临时文件清零，重试成功；
- 大卷（20MiB×4）重建读取仍精确 4V，分块 128KiB、峰值缓冲 512KiB ≤ 1MiB。

## 未完成项 / 限制

- 单进程同步推进；并发推进同一工单靠进程内锁（含保护任务锁），多副本部署需外部锁/队列。
- 丢包注入计数器在内存中，重启清零（业务进度在 SQLite，不受影响）。
- Recycle 策略以“截零”模拟 scrub；未模拟 PV 动态供给、finalizer 卡顿等更多 K8s 细节。
- 拷贝为整文件复制（单卷 ≤64MB 限制），未做分块流式与断点续拷。
- 缺件保护为**单奇偶 XOR**：只能重建任意单件丢失；同单缺两件数学上不可恢复（按要求报错）。
  仅支持 2~4 个等长留存卷；保护卷与留存卷同长，保护/临时磁盘预算按 3V 强制。
- 4V 读取预算在 N=4 时被 4 个输入卷读满，因此重建采用“异或输出流内联整卷哈希 +
  落盘后 stat 复核”，不另做独立暂存回读（独立回读必然 5V 超限）；启用路径不受此限，
  保留真实暂存回读。任何回读字节仍统一经 `ReadMeter` 计量。
- 无认证鉴权；`/sim/*` 是演练管理面，真实环境需隔离。
- 本沙箱无 Docker 守护进程，镜像未实际构建；Dockerfile/compose 已经静态核对，
  本地自测以 uvicorn 直跑验证（`docker compose up --build` 即可在有 Docker 的环境启动）。
