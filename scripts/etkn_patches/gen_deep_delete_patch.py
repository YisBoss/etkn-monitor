#!/usr/bin/env python3
"""生成 deep_delete.py 补丁（第 14 补丁）。

问题：integration.emby_deep_delete_queue 领取任务时，due_delete_files 的 RETURNING 里带
一个标量子查询把 snapshot.payload 挂在每一行上。一个 386 项的联动删除，payload 5,023 kB，
386 行 = 1.9 GB JSONB，psycopg 再展开成 Python 对象可达数 GB → 容器 anon 2 分钟冲到 9.6 GB
→ mem-watch 在 9 GB 处 docker restart etkn → 整批删除事务在 processing 状态被打断
（attempt_count 还是 0，连 fail_delete_files 都没跑到）→ 下次循环重新领取同一批
→ 永远循环，任务中心永远显示「运行中 0/1」。

修法：领取时只取队列行本身（RETURNING queue.*），再按 token 一次性取一份 payload 回填。
内存从 1.9 GB 级降到 5 MB 级。
"""
from __future__ import annotations

import ast
import hashlib
import os
import shutil
import sys

ETKN_DIR = os.path.dirname(os.path.abspath(__file__))
ORIG = os.path.join(ETKN_DIR, "deep_delete.py.orig")
PATCH = os.path.join(ETKN_DIR, "deep_delete_patch.py")
MARK = "[etkn-fix14 2026-11-04]"

ANCHOR_IMPORT = "from .repository import EmbyEventRepository, is_p115_version\n"
NEW_IMPORT = (
    "from .repository import EmbyEventRepository, is_p115_version\n"
    "\n"
    f"# {MARK} 只按 token 取一次 payload，避免逐行回填 GB 级 JSONB\n"
    "def _claim_delete_rows_light(service, limit: int, snapshot_token: str) -> list[dict[str, Any]]:\n"
    "    with service.database.transaction() as connection:\n"
    "        rows = connection.execute(\n"
    '            """\n'
    "            WITH due AS (\n"
    "                SELECT id FROM integration.emby_deep_delete_queue\n"
    "                WHERE next_attempt_at <= now()\n"
    "                  AND (%s = '' OR snapshot_token = %s)\n"
    "                  AND (\n"
    "                      (status IN ('pending', 'processing') AND attempt_count < 5)\n"
    "                      OR (\n"
    "                          status = 'failed'\n"
    "                          AND attempt_count = 5\n"
    "                          AND external_file_id !~ '^[0-9]{1,30}$'\n"
    "                      )\n"
    "                  )\n"
    "                ORDER BY next_attempt_at, id\n"
    "                FOR UPDATE SKIP LOCKED\n"
    "                LIMIT %s\n"
    "            )\n"
    "            UPDATE integration.emby_deep_delete_queue queue\n"
    "            SET status = 'processing', updated_at = now()\n"
    "            FROM due\n"
    "            WHERE queue.id = due.id\n"
    "            RETURNING queue.*\n"
    '            """,\n'
    "            (snapshot_token, snapshot_token, max(1, min(limit, 1000))),\n"
    "        ).fetchall()\n"
    "    claimed = [dict(row) for row in rows]\n"
    "    if not claimed:\n"
    "        return []\n"
    "    tokens = sorted({str(row.get('snapshot_token') or '') for row in claimed} - {''})\n"
    "    payloads: dict[str, Any] = {}\n"
    "    if tokens:\n"
    "        with service.database.connect() as connection:\n"
    "            for record in connection.execute(\n"
    '                "SELECT token, payload FROM integration.emby_deep_delete_snapshots"\n'
    '                " WHERE token = ANY(%s)",\n'
    "                (tokens,),\n"
    "            ).fetchall():\n"
    "                payloads[str(record['token'])] = record['payload']\n"
    "    for row in claimed:\n"
    "        row['snapshot_payload'] = payloads.get(str(row.get('snapshot_token') or '')) or {}\n"
    "    return claimed\n"
)

OLD_CALL = "        rows = self.repository.due_delete_files(limit, snapshot_token=snapshot_token)\n"
NEW_CALL = (
    f"        # {MARK} 领取不再逐行回填 payload（见文件顶部 _claim_delete_rows_light）\n"
    "        rows = _claim_delete_rows_light(self, limit, snapshot_token)\n"
)


def transform(code: str) -> str:
    if MARK in code and "_claim_delete_rows_light(self, limit, snapshot_token)" in code:
        return code
    if ANCHOR_IMPORT not in code:
        raise SystemExit("deep_delete: 找不到 import 锚点")
    if OLD_CALL not in code:
        raise SystemExit("deep_delete: 找不到 due_delete_files 调用行")
    code = code.replace(ANCHOR_IMPORT, NEW_IMPORT, 1)
    code = code.replace(OLD_CALL, NEW_CALL, 1)
    if "_claim_delete_rows_light(self, limit, snapshot_token)" not in code:
        raise SystemExit("deep_delete: 替换后校验失败")
    return code


def main() -> int:
    if not os.path.exists(ORIG):
        print(f"缺少上游原版：{ORIG}", file=sys.stderr)
        return 1
    source = open(ORIG, encoding="utf-8").read()
    patched = transform(source)
    ast.parse(patched)
    with open(PATCH, "w", encoding="utf-8") as handle:
        handle.write(patched)
    digest = hashlib.md5(patched.encode("utf-8")).hexdigest()
    with open(PATCH + ".md5", "w", encoding="utf-8") as handle:
        handle.write(digest + "\n")
    os.chmod(PATCH, 0o644)
    os.chmod(PATCH + ".md5", 0o644)
    print(f"生成 {PATCH} md5={digest} 字符={len(patched)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
