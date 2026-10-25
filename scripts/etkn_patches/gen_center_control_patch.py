#!/usr/bin/env python3
"""生成 center_control 补丁（资源中心长连接：可诊断 + 秒级重连）。

背景（2026-10-25）：
  资源中心「签名能力状态」间歇性显示异常；本机 115 签名链路实测完全正常
  （rapid_upload 返回 sign_required → rapid_sign_value 出 40 位 HEX → 秒传成功）。
  中心侧 share_heartbeat_meta_json.rapid_sign_probe 显示探测是经 control 通道下发、
  正常时 latency_ms≈1523、超时预算 3 秒。而本机日志里「资源中心长连接暂不可用」
  成簇出现（每次 6~7 次重试），且
      delay = random.uniform(1, min(120, 2 ** min(failures + 1, 7)))
  在 failures 变大后退避上限直接到 120 秒 —— 单次掉线要 1~2 分钟才回来，
  掉线期间：
    1) 中心的签名探测落空 → availability.signing.online=false → 前端「签名能力状态 异常」；
    2) 中心按秒累计 weekly_offline_seconds（当时已 15h01m，24h 封禁线只剩 8h58m）。

  两个根因都出自本文件的实现：
    · 断线原因字节被吞：except Exception 只打一句「暂不可用」，不记录 close code/原因，
      无法判断是 keepalive 超时、中心 1012 重启还是中间设备掐断；
    · 退避上限 120s 过长，把亚秒级的瞬断放大成分钟级离线；
    · 内层 recv 用 asyncio.wait_for(…, 15) 每 15 秒取消一次 recv，并顺带查一次数据库，
      既有无谓 DB 压力，也可能在收包瞬间被取消而漏掉一帧 RPC（漏掉的正是签名探测）。

修法：
  1) 断线日志补 close code / reason / 异常类型 / 本次会话存活秒数；
  2) 重连退避改为 0.2~3 秒（上限硬压），瞬断几百毫秒即恢复；
  3) 内层改为直接 await ws.recv()，活性交给 ping_interval=30/ping_timeout=20，
     配置变动由已有的 watch_configuration 任务负责关连接，删掉每 15 秒的 DB 查询；
  4) 记录每次重连成功时的断开时长与连续失败次数（可用作监控指标）；
  5) RPC 派发失败不再静默：记 method + status + 异常类型（不落 ticket/凭证）；
     传输失败记 stage（fetch/report），区分「取任务失败」与「回传结果失败」。

用法：
  python3 gen_center_control_patch.py             # 从当前镜像抽取上游原版
  python3 gen_center_control_patch.py --stdin f   # 用本地文件 f 作为上游原版
"""
import hashlib
import os
import py_compile
import subprocess
import sys

ETKN_DIR = "/vol1/1000/docker/etkn"
IMAGE = "hbq0405/etkn:latest"
REL = "integrations/center_control.py"
CTR_PATH = "/usr/local/lib/python3.12/site-packages/etk_vnext/" + REL
PATCH = os.path.join(ETKN_DIR, "center_control_patch.py")
ORIG = os.path.join(ETKN_DIR, "center_control.py.orig")

REPLACEMENTS = [
    # 0) 引入 time
    (
        "import threading\nfrom urllib.parse import",
        "import threading\nimport time\nfrom urllib.parse import",
    ),
    # 1) RPC 派发失败不再静默 + 慢派发可见
    (
        """            try:
                result = await self._dispatch(body["method"], body["payload"])
                response = {"result": result}
            except Exception as exc:
                # Never send exception text, candidates, or credentials into logs/results.
                response = {"error": "execution_failed", "status": getattr(exc, "status_code", 502)}
""",
        """            method = body.get("method")
            started = time.monotonic()
            try:
                result = await self._dispatch(method, body["payload"])
                response = {"result": result}
                # [etkn-fix 2026-10-25] 慢派发可见：中心的签名探测预算只有 3 秒，
                # 派发耗时一旦逼近就必须能从日志里看出来。
                elapsed = time.monotonic() - started
                if elapsed > 2.0:
                    logger.warning("中心控制任务派发偏慢：method=%s elapsed=%.2fs", method, elapsed)
            except Exception as exc:
                # Never send exception text, candidates, or credentials into logs/results.
                response = {"error": "execution_failed", "status": getattr(exc, "status_code", 502)}
                # [etkn-fix 2026-10-25] 只记录方法名/状态码/异常类型与本机自建的 detail 文案，
                # 不落 ticket、不落凭证、不落第三方响应正文。
                detail = getattr(exc, "detail", None)
                logger.warning("中心控制任务执行失败：method=%s status=%s kind=%s detail=%s",
                               method, response["status"], type(exc).__name__,
                               str(detail)[:80] if detail is not None else "")
""",
    ),
    # 1b) RPC 传输失败记录阶段（fetch/report），否则「未完成」依旧是黑盒
    (
        """    async def _rpc(self, request_id, client, session):
        headers = {"X-Control-Session": session}
        try:
""",
        """    async def _rpc(self, request_id, client, session):
        headers = {"X-Control-Session": session}
        stage = "fetch"
        try:
""",
    ),
    (
        """            await asyncio.to_thread(client._request, "POST", f"/api/v1/control/requests/{request_id}",
                                    payload=response, additional_headers=headers)
        except Exception:
            logger.warning("中心控制任务未完成，将由原任务重试")
""",
        """            stage = "report"
            await asyncio.to_thread(client._request, "POST", f"/api/v1/control/requests/{request_id}",
                                    payload=response, additional_headers=headers)
        except Exception as exc:
            # [etkn-fix 2026-10-25] 区分取任务失败与回传结果失败，避免「未完成」继续当黑盒
            logger.warning("中心控制任务未完成，将由原任务重试：stage=%s kind=%s", stage, type(exc).__name__)
""",
    ),
    # 2) 断线日志带 close code / reason，并记录断开起点
    (
        """            except Exception:
                failures += 1
                logger.warning("资源中心长连接暂不可用，正在退避重连（第%s次）", failures)
""",
        """            except Exception as exc:
                failures += 1
                # [etkn-fix 2026-10-25] 记录断线原因，否则永远分不清 keepalive 超时、
                # 中心 1012 重启、还是中间设备掐断长连接。
                code = getattr(exc, "code", None)
                reason = str(getattr(exc, "reason", "") or "")
                logger.warning("资源中心长连接暂不可用（第%s次）：kind=%s code=%s reason=%s",
                               failures, type(exc).__name__, code, reason[:80])
""",
    ),
    # 3) finally 里记断开时刻（只对真正连上过的那一轮生效）
    (
        """            finally:
                self.session = ""
""",
        """            finally:
                if connected_this_attempt and down_since is None:
                    down_since = time.monotonic()
                self.session = ""
""",
    ),
    # 4) 快重连 + 恢复日志 + 内层去 wait_for
    (
        """            delay = random.uniform(1, min(120, 2 ** min(failures + 1, 7)))
""",
        """            # [etkn-fix 2026-10-25] 原为 uniform(1, min(120, 2**…))，失败几次后单次等待可到 120 秒，
            # 把亚秒级瞬断放大成分钟级离线：中心按秒累计 weekly_offline_seconds，
            # 且掉线窗口内的签名探测必然落空 → 前端「签名能力状态 异常」。硬压到 3 秒。
            delay = random.uniform(0.2, 3.0)
""",
    ),
    (
        """                    self.session = ready["session"]
                    failures = 0
                    logger.info("资源中心加密长连接已建立：签名优先P2P直连，失败由中心中转；不发布客户端域名")
""",
        """                    self.session = ready["session"]
                    recovered = round(time.monotonic() - down_since, 1) if down_since is not None else None
                    failed_attempts = failures
                    down_since = None
                    failures = 0
                    logger.info("资源中心加密长连接已建立：签名优先P2P直连，失败由中心中转；不发布客户端域名")
                    if recovered is not None:
                        logger.warning("资源中心长连接已恢复：断开 %ss，连续失败 %s 次",
                                       recovered, failed_attempts)
""",
    ),
    (
        """                    while not self.stop_event.is_set():
                        try:
                            message = json.loads(await asyncio.wait_for(ws.recv(), 15))
                        except asyncio.TimeoutError:
                            enabled = await asyncio.to_thread(lambda: SharedPoolService(self.database).configuration().payload.get("enabled"))
                            if not enabled:
                                break
                            continue
""",
        """                    while not self.stop_event.is_set():
                        # [etkn-fix 2026-10-25] 原来每 15 秒 wait_for 取消一次 recv 并查一次库：
                        # 取消收包可能在帧到达瞬间丢消息（丢的正是中心的签名探测 → 判异常），
                        # 且平白给数据库加每 15 秒一次的查询。活性交给 ping_interval/ping_timeout，
                        # 配置变动由 watch_configuration 负责 close。
                        message = json.loads(await ws.recv())
""",
    ),
    # 5) down_since / connected_this_attempt 初始化
    (
        """        self.loop = asyncio.get_running_loop()
        failures = 0
        while not self.stop_event.is_set():
            tasks = set()
""",
        """        self.loop = asyncio.get_running_loop()
        failures = 0
        down_since = None
        while not self.stop_event.is_set():
            tasks = set()
            connected_this_attempt = False
""",
    ),
    # 6) 连上即置位，供 finally 判断
    (
        """                    self.session = ready["session"]
                    recovered =""",
        """                    self.session = ready["session"]
                    connected_this_attempt = True
                    recovered =""",
    ),
]


def read_upstream(argv):
    if "--stdin" in argv:
        with open(argv[argv.index("--stdin") + 1], "r", encoding="utf-8") as fh:
            return fh.read()
    with open(ORIG, "r", encoding="utf-8") as fh:
        return fh.read()


def main():
    content = read_upstream(sys.argv)
    for index, (old, new) in enumerate(REPLACEMENTS):
        if content.count(old) != 1:
            raise SystemExit("anchor #%d matched %d times, expected 1" % (index, content.count(old)))
        content = content.replace(old, new)
    with open(PATCH, "w", encoding="utf-8") as fh:
        fh.write(content)
    py_compile.compile(PATCH, doraise=True)
    digest = hashlib.md5(open(PATCH, "rb").read()).hexdigest()
    with open(PATCH + ".md5", "w", encoding="utf-8") as fh:
        fh.write("%s  %s\n" % (digest, PATCH))
    print("patched ->", PATCH)
    print("md5:", digest)
    print("bytes:", os.path.getsize(PATCH))


if __name__ == "__main__":
    main()
