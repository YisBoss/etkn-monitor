#!/usr/bin/env python3
"""ETKN 镜像更新后自动 rebase bind-mount 补丁（宿主守护）。v1.0.0  2026-10-25

为什么需要：
    ETKN 的 7 个补丁都是以「整文件覆盖」方式 bind-mount 进容器的（`:ro`）。
    一旦上游镜像更新（docker pull + 重建容器），容器内的原版文件会被换成新版本，
    而我们挂进去的仍是旧版补丁 → 新功能缺失 / 补丁与镜像语义不再匹配。
    `etkn-patch-check.sh --json` 的 `upstream_*` 项会报不一致（need_rebase=true）。

本脚本的做法（保守、可观察）：
    1. 跑 etkn-patch-check.sh --json；
    2. 无 drift → 静默退出（只写一行日志）；
    3. 有 drift → 先看 ETKN 是否闲着（platform.workflow_runs 无 running/queued），
       忙就跳过等下一轮；
    4. 跑 auto_rebase_patch.py（重新从镜像抽原版 → 重新打补丁 → 重建容器 → 自检）；
    5. 再跑一次自检，按结果推飞书（复用 em 的 webhook 配置）。

调用：systemd timer `etkn-patch-auto.timer` 每 20 分钟一次。
退出码：0 = 无需处理/已处理成功；1 = 自检或 rebase 失败（供 journal 检索）。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request

CHECK = "/vol1/1000/docker/etkn-monitor/scripts/etkn-patch-check.sh"
REBASE = "/vol1/1000/docker/etkn/auto_rebase_patch.py"
LOG = "/vol1/1000/docker/etkn/data/patch-autorebase.log"
STAMP = "/vol1/1000/docker/etkn/data/.patch-autorebase.last"
EM_SETTINGS = "/vol1/1000/docker/etkn-monitor/data/settings.json"
COOLDOWN = 1800          # 两轮 rebase 最小间隔（秒）
CHECK_TIMEOUT = 900
REBASE_TIMEOUT = 2400


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    try:
        os.makedirs(os.path.dirname(LOG), exist_ok=True)
        # 超过 2MB 就轮转一次，避免无限增长
        if os.path.exists(LOG) and os.path.getsize(LOG) > 2 * 1024 * 1024:
            os.replace(LOG, LOG + ".1")
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def run_json(cmd: list[str], timeout: int) -> dict:
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    out = (p.stdout or "").strip()
    start = out.find("{")
    if start < 0:
        raise RuntimeError(f"无 JSON 输出 rc={p.returncode} stderr={p.stderr[-400:]}")
    return json.loads(out[start:])


def patch_check() -> dict:
    return run_json(["sh", CHECK, "--json"], CHECK_TIMEOUT)


def drift_items(res: dict) -> list[dict]:
    return [c for c in (res.get("checks") or [])
            if not c.get("ok") and str(c.get("key", "")).startswith("upstream_")]


def workflows_busy() -> tuple[bool, str]:
    """通过容器内 etk_vnext 的 Database 查平台任务是否在跑。"""
    py = (
        "from etk_vnext.db.connection import Database\n"
        "with Database().connect() as c:\n"
        "    with c.cursor() as cur:\n"
        "        cur.execute(\"select status, count(*) from platform.workflow_runs \"\n"
        "                    \"where status in ('running','queued') group by status\")\n"
        "        rows = cur.fetchall()\n"
        "print(rows)\n"
    )
    try:
        p = subprocess.run(["docker", "exec", "etkn", "python", "-c", py],
                           capture_output=True, text=True, timeout=120)
        rows = (p.stdout or "").strip()
        if p.returncode != 0:
            return False, "查询失败，按空闲处理：" + (p.stderr or "")[-200:]
        return bool(rows and rows not in ("[]", "()", "None")), rows or "[]"
    except Exception as exc:  # noqa: BLE001
        return False, f"查询异常，按空闲处理：{exc}"


def notify(text: str) -> None:
    try:
        cfg = json.load(open(EM_SETTINGS, encoding="utf-8"))
        url = cfg.get("webhook_url") or ""
        if not (url and cfg.get("feishu_enabled", True)):
            return
        body = json.dumps({"msg_type": "text", "content": {"text": text}}).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as exc:  # noqa: BLE001
        log(f"通知失败：{exc}")


def main() -> int:
    try:
        res = patch_check()
    except Exception as exc:  # noqa: BLE001
        log(f"自检执行失败：{exc}")
        return 1

    drift = drift_items(res)
    if not drift and not res.get("need_rebase"):
        log(f"无 drift（image={str(res.get('image_id',''))[:19]}）")
        return 0

    keys = ", ".join(c["key"] for c in drift) or "need_rebase=true"
    log(f"检测到 drift：{keys}")

    last = 0.0
    try:
        last = os.path.getmtime(STAMP)
    except OSError:
        pass
    if time.time() - last < COOLDOWN:
        log(f"距上次 rebase 不足 {COOLDOWN}s，跳过本轮")
        return 0

    busy, detail = workflows_busy()
    if busy:
        log(f"ETKN 正在跑任务 {detail}，跳过本轮，等空闲再 rebase")
        return 0

    log("开始 auto_rebase_patch.py（重建容器约 1~3 分钟）")
    try:
        open(STAMP, "w").write(str(int(time.time())))
    except OSError:
        pass
    try:
        p = subprocess.run([sys.executable, REBASE], capture_output=True, text=True, timeout=REBASE_TIMEOUT)
        ok = p.returncode == 0
        tail = ((p.stdout or "") + (p.stderr or ""))[-600:]
    except Exception as exc:  # noqa: BLE001
        ok, tail = False, f"执行异常：{exc}"
    log(f"rebase 结束 ok={ok}\n{tail}")

    # rebase 之后必须用自检复核，不以 rebase 脚本退出码为唯一判据
    try:
        res2 = patch_check()
        still = [c["key"] for c in (res2.get("checks") or []) if not c.get("ok")]
    except Exception as exc:  # noqa: BLE001
        still = [f"自检复核失败：{exc}"]
    if ok and not still:
        log("复核通过：全部补丁已跟随新镜像")
        notify(f"🔧 ETKN 补丁已自动跟随镜像更新\n镜像 {str(res.get('image_id',''))[:19]}\n处理项：{keys}\n自检：全部通过")
        return 0

    log(f"复核未通过：{still}")
    notify("❌ ETKN 补丁自动 rebase 失败\n"
           f"镜像 {str(res.get('image_id',''))[:19]}\ndrift：{keys}\n未通过：{', '.join(still)[:300]}\n"
           "请到 em 面板「补丁自检」手动点「立即修复」，或看日志 " + LOG)
    return 1


if __name__ == "__main__":
    sys.exit(main())
