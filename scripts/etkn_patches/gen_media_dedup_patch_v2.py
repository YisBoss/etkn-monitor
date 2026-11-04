#!/usr/bin/env python3
"""media_dedup 补丁生成器 v2（基线：1.3.78 的 service.py）

v1（gen_media_dedup_patch.py，基线 1.3.73）在 1.3.78 上锚点全部失配：
上游把 scan() 重写成「临时表 + 服务端游标」流式实现，已经解决了我们 fix18 想解决的
大结果集内存问题。所以 v2 只保留一件事：让「指定媒体库」真正生效，而且生效到文件级。

fix19（1.3.73 时代）：只按 media.items.id 过滤 —— 结果「选 iso 原盘库」仍会把
同一部片在别的库的版本一起 PK 并删掉，用户看到计划里全是 欧美电影/日本电影 的路径，
以为范围没生效。
fix21（本次）：再叠一层 version.path LIKE 库目录前缀，库外文件既不参与比较也不会被删。

只改三处：
  1) scan() 头部解析 library_ids；
  2) 事务里解析出「媒体项集合 + 目录前缀」，把谓词注入 _SCAN_IDENTITIES_SQL 后执行；
  3) 插入两个 helper（_dedup_scope_item_ids / _dedup_scope_paths）。
其余一律保持上游原样。
"""

from __future__ import annotations

import hashlib
import os
import sys

ETKN_DIR = os.path.dirname(os.path.abspath(__file__))
ORIG = os.path.join(ETKN_DIR, "media_dedup_service.orig")
PATCH = os.path.join(ETKN_DIR, "media_dedup_patch.py")

ANCHOR_SCAN_HEAD = '''    def scan(self, step: ClaimedStep | None = None) -> dict[str, int]:
        self.configuration()
        washing = OrganizeSettingsService(self.database).get("washing")["value"]
'''

NEW_SCAN_HEAD = '''    def scan(self, step: ClaimedStep | None = None) -> dict[str, int]:
        # [etkn-fix21 2026-11-04] 「指定媒体库」以前是死配置：configuration() 读了却不用。
        _config = self.configuration()
        _scope_library_ids: list[int] = []
        for _value in (_config.get("library_ids") or []):
            _text = str(_value).strip()
            if _text.isdigit():
                _scope_library_ids.append(int(_text))
        washing = OrganizeSettingsService(self.database).get("washing")["value"]
'''

ANCHOR_SCAN_EXEC = '''        with self.database.transaction() as connection:
            # Materialize scalar identity data in PostgreSQL, never the library JSON in Python.
            connection.execute(_SCAN_IDENTITIES_SQL)
'''

NEW_SCAN_EXEC = '''        with self.database.transaction() as connection:
            # [etkn-fix21 2026-11-04] 范围双重收窄：先到媒体项，再到库内目录。
            _scope_item_ids = self._dedup_scope_item_ids(connection, _scope_library_ids)
            _scope_paths = self._dedup_scope_paths(connection, _scope_library_ids)
            if _scope_library_ids and not _scope_item_ids:
                raise RuntimeError(
                    "扫描范围未解析出任何媒体项（媒体库 "
                    + ", ".join(str(value) for value in _scope_library_ids)
                    + " 可能为空或尚未同步）"
                )
            _scope_sql = _SCAN_IDENTITIES_SQL
            _scope_params: list[Any] = []
            _scope_predicates: list[str] = []
            if _scope_item_ids is not None:
                _scope_predicates.append("item.id = ANY(%s::bigint[])")
                _scope_params.append(_scope_item_ids)
            if _scope_paths is not None:
                _scope_predicates.append("version.path LIKE ANY(%s::text[])")
                _scope_params.append(_scope_paths)
            if _scope_predicates:
                _scope_anchor = "WHERE item.item_type IN ('movie', 'episode')"
                if _scope_sql.count(_scope_anchor) != 1:
                    raise RuntimeError("_SCAN_IDENTITIES_SQL 结构与预期不符，无法限定扫描范围")
                _scope_sql = _scope_sql.replace(
                    _scope_anchor,
                    _scope_anchor + "\\n  AND " + "\\n  AND ".join(_scope_predicates),
                    1,
                )
                task_log(
                    f"扫描范围：{len(_scope_library_ids)} 个媒体库、{len(_scope_item_ids or [])} 个媒体项"
                    + (
                        f"、库内目录 {len(_scope_paths)} 个（库外文件不参与比较、不会删除）。"
                        if _scope_paths
                        else "（库未配置目录，仅按媒体项过滤）。"
                    ),
                    {
                        "library_ids": _scope_library_ids,
                        "scoped_items": len(_scope_item_ids or []),
                        "scoped_paths": len(_scope_paths or []),
                    },
                )
            # Materialize scalar identity data in PostgreSQL, never the library JSON in Python.
            if _scope_params:
                connection.execute(_scope_sql, tuple(_scope_params))
            else:
                connection.execute(_scope_sql)
'''

ANCHOR_CONFIG = '''    def configuration(self) -> dict[str, Any]:
'''

NEW_HELPERS = '''    def _dedup_scope_item_ids(self, connection: Any, library_ids: list[int]) -> list[int] | None:
        """[etkn-fix21] 把「指定媒体库」解析成 media.items.id 集合；未配置范围返回 None。"""
        if not library_ids:
            return None
        ids: set[int] = set()
        rows = connection.execute(
            """
            SELECT DISTINCT entry.media_item_id AS item_id
            FROM virtual_library.entries entry
            WHERE entry.virtual_library_id = ANY(%s)
            """,
            (library_ids,),
        ).fetchall()
        for row in rows:
            if row.get("item_id"):
                ids.add(int(row["item_id"]))
        # entries 为空的库（尚未刷新或纯路径库）退回 definition_json.paths 前缀匹配
        empty_libs = connection.execute(
            """
            SELECT lib.id AS library_id,
                   COALESCE(lib.definition_json -> 'paths', '[]'::jsonb) AS paths
            FROM virtual_library.libraries lib
            WHERE lib.id = ANY(%s)
              AND NOT EXISTS (
                  SELECT 1 FROM virtual_library.entries entry
                  WHERE entry.virtual_library_id = lib.id
              )
            """,
            (library_ids,),
        ).fetchall()
        for lib in empty_libs:
            prefixes = [
                str(path).rstrip("/") + "/%"
                for path in (lib.get("paths") or [])
                if str(path).strip()
            ]
            if not prefixes:
                continue
            fetched = connection.execute(
                """
                SELECT DISTINCT item.id AS item_id
                FROM media.items item
                JOIN media.item_versions version
                  ON version.item_id = item.id AND version.is_current
                WHERE version.path LIKE ANY(%s)
                """,
                (prefixes,),
            ).fetchall()
            for row in fetched:
                if row.get("item_id"):
                    ids.add(int(row["item_id"]))
        return sorted(ids)

    def _dedup_scope_paths(self, connection: Any, library_ids: list[int]) -> list[str] | None:
        """[etkn-fix21] 把「指定媒体库」解析成目录前缀（LIKE 模式）。

        仅当所选每个库都有 definition_json.paths 时才生效；任一库没有目录定义
        （例如榜单/list 型虚拟库）就整段返回 None，退回「只按媒体项过滤」，
        避免把版本全过滤掉变成零计划。
        """
        if not library_ids:
            return None
        rows = connection.execute(
            """
            SELECT lib.id AS library_id,
                   COALESCE(lib.definition_json -> 'paths', '[]'::jsonb) AS paths
            FROM virtual_library.libraries lib
            WHERE lib.id = ANY(%s)
            """,
            (library_ids,),
        ).fetchall()
        prefixes: list[str] = []
        for lib in rows:
            paths = [
                str(path).strip()
                for path in (lib.get("paths") or [])
                if str(path).strip()
            ]
            if not paths:
                return None
            prefixes.extend(path.rstrip("/") + "/%" for path in paths)
        if len(rows) != len({int(value) for value in library_ids}):
            return None
        return prefixes or None

'''

REPLACEMENTS = [
    ("scan_head", ANCHOR_SCAN_HEAD, NEW_SCAN_HEAD, "fix21 2026-11-04] 「指定媒体库」以前是死配置"),
    ("scan_exec", ANCHOR_SCAN_EXEC, NEW_SCAN_EXEC, "fix21 2026-11-04] 范围双重收窄"),
    ("helpers", ANCHOR_CONFIG, NEW_HELPERS + ANCHOR_CONFIG, "def _dedup_scope_paths(self, connection: Any"),
]

MARKERS = (
    "def _dedup_scope_item_ids(self, connection: Any",
    "def _dedup_scope_paths(self, connection: Any",
    "fix21 2026-11-04] 范围双重收窄",
    "version.path LIKE ANY(%s::text[])",
    "connection.execute(_scope_sql, tuple(_scope_params))",
    "_SCAN_IDENTITIES_SQL",
)


def main() -> int:
    with open(ORIG, encoding="utf-8") as handle:
        source = handle.read()

    patched = source
    for name, anchor, replacement, marker in REPLACEMENTS:
        if marker in patched:
            print(f"[{name}] 已存在，跳过")
            continue
        hits = patched.count(anchor)
        if hits != 1:
            print(f"[{name}] 锚点命中 {hits} 次（应为 1），中止", file=sys.stderr)
            return 1
        patched = patched.replace(anchor, replacement, 1)
        print(f"[{name}] 已替换")

    for marker in MARKERS:
        if marker not in patched:
            print(f"校验失败：缺少标记 {marker}", file=sys.stderr)
            return 1
    if "fix18" in patched:
        print("校验失败：v2 不应包含 1.3.73 时代的 fix18 改动", file=sys.stderr)
        return 1

    with open(PATCH, "w", encoding="utf-8") as handle:
        handle.write(patched)

    digest = hashlib.md5(patched.encode("utf-8")).hexdigest()
    with open(PATCH + ".md5", "w", encoding="utf-8") as handle:
        handle.write(digest + "\n")
    os.chmod(PATCH + ".md5", 0o644)

    import py_compile

    py_compile.compile(PATCH, doraise=True)
    print(f"media_dedup_patch.py 已生成 {len(patched)} 字符 md5={digest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
