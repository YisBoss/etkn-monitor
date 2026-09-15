# etkn-monitor —— ETKN 监控（v2.1）

手机友好的 ETKN（ETK vNext）监控面板：队列规模、入库进度、完成速率与 ETA、分类任务统计、手动链路测速、失败任务重试、手动整理触发、异常媒体明细。

**v2.1 变化**（相对 v2）：
- **重试按钮收敛**：仅「刮削入库」类（workflow_type=batch_ingest）显示重试按钮，其余类型不显示（ETKN 官方白名单仅刮削类，其余必 409）；按 workflow_type 过滤，不硬编码标题；列表标题去「（可重试）」
- **手动整理网盘文件**：任务统计页一键触发，原样转发 ETKN 原生接口 `POST /api/task-center/tasks/organize-p115/runs`（参数形态取自历史运行），确认弹窗→成功 toast→自动刷新；复用 mGo 忙锁防连点
- **异常媒体明细**：入库页「异常（媒体）」数字可点击，弹出当日明细（文件名+原因+时间），数据源 `GET /api/p115/records?status=unrecognized|failed`，无数据显示空态，不硬造数据

**v2 变化**（相对 v1）：
- 新增**手动链路测速**（仅按钮触发、无定时）：对 image.tmdb.org / api.themoviedb.org / api.telegram.org / shared.55565576.xyz 发起普通请求——不设代理、不强制直连，流量自然走软路由当前策略；每域名输出 TCP 建连 / TLS 握手 / HTTP 总耗时与状态，超时 10 秒。
- 任务统计页失败列表每行新增**重试按钮**：确认弹窗 → 调用 ETKN 官方重试接口（`POST /api/workflows/{id}/retry-failed`）→ 成功后任务自动进 ETKN 队列并刷新；ETKN 返回 409 等拒绝时**原样展示**其错误信息，不隐藏不绕过。
- 两页口径统一：完成/异常媒体全站以 p115 记录为准；分类表「任务项」列为运行条目数（含重试重算），仅作任务维度参考。

**v1 特性保留**：
- 入库进度页：待处理（当前活跃队列，未终态、当日创建）、今日完成、异常、ETA（滚动窗口速率外推，默认 10 分钟可配；速率为 0 显示「计算中」防除零）、分类队列。
- 卡片口径：媒体数主口径（p115 记录），任务数副标（如「356媒体·41任务」），每卡标注单位。
- 轮询间隔默认 15 秒（可配）；前端显示「更新于 XX:XX」。
- 登录会话存内存、401 自动重登。
- v1 的清空/一键排优按钮仍不提供（v2 亦未加）。

## 部署（飞牛 fnOS）

```bash
# 1. 克隆（或复制）项目
cd /vol1/1000/docker
git clone https://github.com/YisBoss/etkn-monitor.git
cd etkn-monitor

# 2. 创建 .env（密码自填，绝不入仓）
cat > .env <<'EOF'
ETKN_BASE_URL=http://192.168.1.22:5257
ETKN_USERNAME=YisBoss
ETKN_PASSWORD=你的ETKN密码
MONITOR_PORT=8620
POLL_INTERVAL=15
EOF
chmod 600 .env

# 3. 启动
docker compose up -d

# 4. 验证
curl http://127.0.0.1:8620/api/meta
```

浏览器访问 `http://<NAS-IP>:8620`。

## 更新版本

```bash
cd /vol1/1000/docker/etkn-monitor
git pull          # 或手动覆盖 monitor.py / static/index.html
docker compose restart
```

## 配置

| 变量 | 说明 | 默认 |
|---|---|---|
| ETKN_BASE_URL | ETKN 地址 | http://192.168.1.22:5257 |
| ETKN_USERNAME | 登录用户名 | YisBoss |
| ETKN_PASSWORD | 登录密码（必填） | 无 |
| ETKN_ETA_WINDOW | ETA 滚动窗口（分钟） | 10 |
| POLL_INTERVAL | 轮询间隔（秒） | 15 |
| MONITOR_PORT | 监听端口 | 8620 |

## 安全

- 零密钥设计：密码只放 `.env`（600 权限），不入代码、不入仓库；Git 已忽略 `.env`。
- 测速与重试均为手动触发，无任何定时任务。
- 重试仅转发 ETKN 官方接口；ETKN 的拒绝（409 等）原样透传展示。

## 接口

| 端点 | 方法 | 说明 |
|---|---|---|
| `/` | GET | 前端页面 |
| `/api/meta` | GET | 版本与配置 |
| `/api/status` | GET | 监控快照（轮询缓存） |
| `/api/speedtest` | POST | 手动测速（4 域名，无代理自然路由） |
| `/api/retry/{id}` | POST | 转发 ETKN retry-failed，透传状态与错误 |

## v1 → v2 规划落实

- ~~清空/一键排优按钮~~：v1/v2 均未提供；如需将在后续版本带确认实现。
