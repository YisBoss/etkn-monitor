#!/usr/bin/env python3
"""生成 auth_patch.py：管理员账号不参与到期/禁用判定（etkn-fix18 2026-11-10）。

背景：2026-11-10 15:15，YisBoss 账号因 expiration_date 到期被置为 disabled，
登录接口直接 403「账号已禁用或已过期」，后台完全进不去。管理员是自建库的
唯一后台账号，到期/禁用语义对它是纯风险，因此在认证层豁免。
"""
from __future__ import annotations

import hashlib
import pathlib
import sys

MARK = "etkn-fix18 2026-11-10"
ROOT = pathlib.Path("/vol1/1000/docker/etkn")
SRC = ROOT / "auth.py.orig"
OUT = ROOT / "auth_patch.py"

REPLACEMENTS = [
    # 会话校验：状态拦截
    (
        '        if row.get("status") in {"disabled", "expired"}:\n',
        f'        # {MARK} 管理员账号豁免到期/禁用判定\n'
        '        if row.get("is_administrator") is not True and row.get("status") in {"disabled", "expired"}:\n',
    ),
    # 会话校验：到期拦截
    (
        '        expiration = row.get("expiration_date")\n'
        '        if expiration and expiration <= now:\n',
        '        expiration = row.get("expiration_date")\n'
        '        if row.get("is_administrator") is not True and expiration and expiration <= now:\n',
    ),
    # 登录：状态拦截
    (
        '    if status in {"disabled", "expired"}:\n',
        '    if local.get("is_administrator") is not True and status in {"disabled", "expired"}:\n',
    ),
    # 登录：到期拦截
    (
        '    if expiration and expiration <= datetime.now(timezone.utc):\n',
        '    if local.get("is_administrator") is not True and expiration and expiration <= datetime.now(timezone.utc):\n',
    ),
]


def main() -> int:
    text = SRC.read_text(encoding="utf-8")
    for old, new in REPLACEMENTS:
        count = text.count(old)
        if count != 1:
            print(f"[ERR] 锚点命中 {count} 次，期望 1 次：{old.splitlines()[0][:60]}", file=sys.stderr)
            return 6
        text = text.replace(old, new)
    if MARK not in text:
        print("[ERR] 标记未写入", file=sys.stderr)
        return 7
    OUT.write_text(text, encoding="utf-8")
    digest = hashlib.md5(OUT.read_bytes()).hexdigest()
    (ROOT / "auth_patch.py.md5").write_text(digest + "\n", encoding="utf-8")
    print(f"written {OUT} md5={digest} bytes={OUT.stat().st_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
