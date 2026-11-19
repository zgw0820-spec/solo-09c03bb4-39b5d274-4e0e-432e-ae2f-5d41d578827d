# 机房设备校时前诊断

值班员在给设备校时前，用本系统核对**诊断机相对两台内网 NTP 时间源的钟差**
是否在容许范围内。网页上可实时调整两源的钟差与回包延迟，填写容许偏差后
发起**真实 UDP 探测**，系统按本轮 NTP 请求/回应的四个时间戳计算各源钟差
及网络延迟引入的不确定区间，给出四态判定。

## 组成

| 服务 | 说明 | 端口 |
|---|---|---|
| `diag` | FastAPI 诊断服务：操作网页 + REST API + UDP 探测 | 8000 (HTTP) |
| `ntp1` / `ntp2` | 本地 NTP 时间源：UDP 应答 + HTTP 控制口（调钟差/回包延迟） | 123/udp、9000 (控制口) |

```
┌────────┐  HTTP 配置   ┌──────────────┐
│ 浏览器  │ ◄──────────► │    diag      │ ──UDP NTP 探测──► ntp1 (源1)
└────────┘              │  FastAPI     │ ──UDP NTP 探测──► ntp2 (源2)
                        └──────┬───────┘
                               │ HTTP（控制口 9000）实时调整
                               ▼
                        ntp1 / ntp2 的 offset_ms、reply_delay_ms
```

## 运行

```bash
docker compose up --build
# 打开 http://localhost:8000
```

本地开发（无 Docker）：

```bash
pip install -r diag/requirements.txt -r source/requirements.txt pytest
# 终端 1、2：两台时间源
SOURCE_ID=ntp-source-1 UDP_PORT=10123 HTTP_PORT=9001 python -m uvicorn source.app.server:app --port 9001
SOURCE_ID=ntp-source-2 UDP_PORT=10124 HTTP_PORT=9002 python -m uvicorn source.app.server:app --port 9002
# 终端 3：诊断服务
NTP1_UDP_PORT=10123 NTP1_HTTP_URL=http://127.0.0.1:9001 \
NTP2_UDP_PORT=10124 NTP2_HTTP_URL=http://127.0.0.1:9002 \
python -m uvicorn diag.app.main:app --port 8000
```

## 判定逻辑

对每台源取一轮四时间戳：t1 本机发送、t2 源接收、t3 源发送、t4 本机接收。

- 钟差点估计 `θ = ((t2-t1)+(t3-t4))/2`
- 往返网络时延 `δ = (t4-t1)-(t3-t2)`
- 真实钟差的不确定区间 `[θ-δ/2, θ+δ/2]`（由 `|θ-θ*| ≤ (d去+d回)/2` 推出）

综合两源区间与容许偏差 `±tol`：

| 条件 | 判定 |
|---|---|
| 任一源超时 / 回包无效 / 迟到串包 | **无法判断** |
| 两源区间不相交（互相矛盾） | **无法判断** |
| 交集 ⊆ [-tol, +tol] | **无需校时** |
| 交集 ∩ [-tol, +tol] = ∅ | **需要校时** |
| 交集跨越 ±tol 边界 | **待核**（人工复核或重测） |

## 回包校验（无效/迟到拒绝）

- 报文不足 48 字节、mode≠4（非服务器应答）、stratum 不在 1..15 → 拒绝
- originate 时间戳与本轮请求的 t1 不符（迟到/串包）→ 拒绝
- t3<t2、往返为负、推算网络时延为负、往返超过超时 → 拒绝
- 超时未收到应答 → 该源判失败

## 并发不串包

- 每次诊断为每个源**新建独立 UDP socket**（内核分配临时端口，用完即关），
  请求之间无共享状态，内核按四元组投递，物理上不会串包；
- 回包必须原样回显本轮 t1（含微秒级小数），对不上即拒收；
- 超时后到达的迟到包随 socket 关闭被内核丢弃；
- 源端为无状态应答器，每个数据报独立任务处理，互不干扰。

## 回包延迟的模拟方式

源端在收到请求时即给 t3 盖章，随后让应答包滞留 `reply_delay_ms` 再发出，
使延迟体现为**回程网络时延**而进入 δ，从而拉宽不确定区间（若先睡眠再盖
t3，延迟会被当作服务器处理时间从 δ 中扣除，失去模拟意义）。把延迟调到
超过探测超时即可模拟源超时。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 操作网页 |
| GET | `/api/sources` | 两源当前配置（实时取自源端控制口） |
| POST | `/api/sources/{1,2}/config` | 调整 `offset_ms` / `reply_delay_ms` |
| POST | `/api/diagnose` |  body：`tolerance_ms`、`timeout_ms`；返回四时间戳、θ、δ、区间、交集与四态判定 |
| GET | `/api/health` | 健康检查 |

源端控制口：`GET/POST /config`、`GET /health`（端口 9000）。

## 测试

```bash
pytest tests/            # 29 项：单元（换算/编解码/判定矩阵/回包校验/并发）+ 端到端集成
```

集成测试会真实拉起两台源与诊断服务，覆盖：网页加载、配置下发回读、
四种判定分支、源超时、12 路并发不串包、非法参数拒绝。

## 目录

```
├── docker-compose.yml
├── common/ntp_proto.py      # NTP 时间戳换算与 48 字节报文编解码（两端共用）
├── diag/                    # 诊断服务
│   ├── Dockerfile
│   └── app/{main.py, probe.py, verdict.py, static/index.html}
├── source/                  # 本地 NTP 时间源
│   ├── Dockerfile
│   └── app/server.py
└── tests/{test_units.py, test_integration.py}
```

## 已知限制

- 时间源为教学用模拟实现（stratum=2，时钟即容器系统时钟 + 配置偏移），
  不实现完整 NTP 服务的时钟驯服；真实部署可替换为 chrony/ntpd 容器，
  诊断端无需改动。
- 单容器单事件循环，延迟模拟精度受宿主机调度影响（毫秒级）。
