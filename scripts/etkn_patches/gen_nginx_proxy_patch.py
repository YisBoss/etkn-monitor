#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成 nginx_proxy_patch.py（etkn-fix19 2026-11-09）

背景：1.4.3 起对外只监听 8097，nginx 模板里 `location = / { return 302 /web/; }`
是相对跳转，但 nginx 默认 absolute_redirect on，会把 Location 补成绝对地址
`http://<内网IP>:8097/web/`（经 Lucky 反代后变成 `https://etkn.zclyjb.top:8097/web/`，
公网 8097 不通 → 点根域名打不开）。修法：在 server 块里 `absolute_redirect off;`
+ `port_in_redirect off;`，让 nginx 原样输出相对 Location `/web/`。

用法：
  python3 gen_nginx_proxy_patch.py          # 生成 patch + md5 边车
"""
from __future__ import annotations

import hashlib
from pathlib import Path

HERE = Path(__file__).resolve().parent
ORIG = HERE / "nginx_proxy.py.orig"
OUT = HERE / "nginx_proxy_patch.py"
MARK = "etkn-fix19 2026-11-09"

OLD = '        "    client_max_body_size 0;\\n\\n"\n'
NEW = (
    '        "    client_max_body_size 0;\\n\\n"\n'
    f'        "    # {MARK}: 反代后不要让 nginx 把相对跳转补成 内网IP:8097\\n"\n'
    '        "    absolute_redirect off;\\n"\n'
    '        "    port_in_redirect off;\\n\\n"\n'
)


def main() -> int:
    src = ORIG.read_text(encoding="utf-8")
    n = src.count(OLD)
    if n != 1:
        print(f"[!] 锚点命中 {n} 次（期望 1 次），上游模板已变，需要重新核对")
        return 2
    out = src.replace(OLD, NEW)
    if MARK not in out or "absolute_redirect off;" not in out:
        print("[!] 替换后校验失败")
        return 3
    OUT.write_text(out, encoding="utf-8")
    md5 = hashlib.md5(out.encode("utf-8")).hexdigest()
    (HERE / "nginx_proxy_patch.py.md5").write_text(md5 + "\n", encoding="utf-8")
    print(f"[ok] {OUT.name} md5={md5} bytes={len(out.encode())}")
    print(f"     上游 {ORIG.name} md5={hashlib.md5(src.encode()).hexdigest()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
