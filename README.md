# etkn-monitor —— ETKN 只读监控（v1）

手机友好的 ETKN（ETK vNext）监控面板：队列规模、入库进度、完成速率与 ETA、分类任务统计。**v1 纯只读**：不提供清空/排优等任何写操作，也无需任何密钥。

## 功能

**入库进度页**
- 待处理（媒体数·任务数）：当前活跃队列（未终态、当日创建，排除历史）
- 今日完成 / 异常（媒体数主口径，任务数副标）
- ETA：滚动窗口（默认 10 分钟）完成速率外推剩余队列，速率为 0 显示「计算中」
- 分类队列明细（共享登记/追剧刷新/刮削入库/网盘整理…各排几个、跑几个、多少媒体）

**任务统计页**
- 今日分类统计（任务/媒体/成功/异常）
- 今日失败与部分失败任务清单
- 系统状态（全局队列、历史累计）

顶部显示「更新于 XX:XX」，前端按后端轮询间隔自动刷新。

## 部署（飞牛 NAS / fnOS）

1. 把本仓库克隆或上传到 NAS 的任意目录，例如 `/vol1/1000/docker/etkn-monitor`
2. 在该目录创建 `.env` 文件（与 docker-compose.yml 同目录）：

```
ETKN_BASE_URL=http://192.168.1.22:5257
ETKN_USERNAME=YisBoss
ETKN_PASSWORD=你的ETKN登录密码
```

可选变量（有默认值）：

```
ETKN_ETA_WINDOW=10     # ETA 滚动窗口（分钟）
POLL_INTERVAL=15       # 轮询间隔（秒）
MONITOR_PORT=8620      # 面板端口
```

3. 启动：

```
cd /vol1/1000/docker/etkn-monitor
docker compose up -d
```

4. 浏览器访问 `http://NAS的IP:8620`（手机加主屏即可当 App 用）

> 密码只进容器环境变量（compose 从 `.env` 注入），不写入代码、不落盘到仓库。`.env` 已被 `.gitignore` 排除。

## 配置原则

- 零密钥：不内置任何凭据；一切走环境变量
- 只读：对 ETKN 只发 GET 与登录请求
- 会话存内存：401 自动重登，重启后自动恢复

## 接口来源（ETKN，只读）

| 用途 | 接口 |
|---|---|
| 队列/健康 | `/api/health`、`/api/diagnostics/summary` |
| 任务列表 | `/api/workflows?status=queued|running|succeeded|failed|partial` |
| 入库记录 | `/api/p115/records?status=success|unrecognized&processed_from=…` |
| 认证 | `POST /api/auth/login` |

## 版本

- v1：只读监控（本版）。清空队列/一键排优等操作按钮计划 v2 加确认后提供。
