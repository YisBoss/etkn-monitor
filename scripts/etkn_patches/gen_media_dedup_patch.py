#!/usr/bin/env python3
"""生成 media_dedup 补丁（幂等可重跑）。

背景（2026-10-29）：
    POST /api/media-dedup/scan 会在一个事务里把「全部当前版本」的行 fetchall 出来，
    SELECT 里带了 media.version_analysis.raw_ffprobe_json / mediainfo_json。
    实测单行约 33KB（jsonb 文本 8KB，Python 对象化后放大 4 倍以上），
    353,000 个当前版本外推 ≈ 11GB，直接把 10g 限额的容器 OOM 杀掉（23:12、23:18 各一次，anon 10.4GB）。

修法（不改判定逻辑，只改取数方式）：
    1) 主查询去掉 raw_ffprobe_json / mediainfo_json / washing_snapshot 以及 version_analysis 的 JOIN，
       只取轻量列（约 1.5~2KB/行）。
    2) 分组之后，只对「同身份组内版本数 ≥ 2」和「判定为完结季」的行，按 400 个一批补取大字段，
       再走原有的洗版/完结季判定。重复组通常只有几百到几千行，内存从 11GB 降到 1GB 以内。

用法：python3 gen_media_dedup_patch.py
产物：media_dedup_patch.py + .md5（由 compose 挂载覆盖
      etk_vnext/modules/media_dedup/service.py）
"""
from __future__ import annotations

import hashlib
import os
import sys

ETKN_DIR = "/vol1/1000/docker/etkn"
ORIG = os.path.join(ETKN_DIR, "media_dedup_service.orig")
PATCH = os.path.join(ETKN_DIR, "media_dedup_patch.py")

# ---------------------------------------------------------------- 1. 主查询只取轻量列
ANCHOR_SELECT = '''                       analysis.raw_ffprobe_json, analysis.mediainfo_json,
                       version.washing_snapshot,
'''

NEW_SELECT = '''                       -- [etkn-fix18 fix18-select-light] 主查询不再取分析大字段：
                       -- 353k 行 × 33KB ≈ 11GB（jsonb 对象化后），会把容器直接 OOM；
                       -- 改为分组后只给「可能出计划」的版本补取，见 _load_details。
'''

ANCHOR_JOIN = '''                LEFT JOIN media.version_analysis analysis ON analysis.media_version_id = version.id
'''

NEW_JOIN = '''                -- [etkn-fix18 fix18-select-light] version_analysis 的 JOIN 挪到 _load_details 里按需执行。
'''

# ---------------------------------------------------------------- 2. 分组后按需补取大字段
ANCHOR_SEASON_LOOP = '''            season_decisions: dict[tuple[str, int], set[int]] = {}
'''

NEW_SEASON_LOOP = '''            # [etkn-fix18 fix18-lazy-details] 只为「可能出计划」的版本补取大字段：
            #   1) 同身份组内版本数 ≥ 2（要按洗版优先级 PK）
            #   2) 完结季且该季确实有多套来源（要按整套缺口/一致性 PK）
            # 完结季整季行有 27.9 万行（占比 78%），若一起补取大字段仍是 8GB+（实测：
            # detail 行数 279096，RSS 每 400 行涨 11MB，撑到 6.6GB 仍在涨）；
            # 所以先用轻量键（存储商 + 发行组 + 目录）判出哪些季真的有多套来源。
            completed_season_keys = {
                key
                for key, season_rows_ in season_grouped.items()
                if self._is_etkn_completed_season(season_rows_)
            }
            multi_source_seasons: set[tuple[str, int]] = set()
            if completed_season_keys:
                season_rows_all = [
                    row
                    for key_ in completed_season_keys
                    for row in season_grouped[key_]
                ]
                season_keys_map = self._load_season_source_keys(connection, season_rows_all)
                for key_ in completed_season_keys:
                    distinct = {
                        f"{row.get('category_cid') or ''}|"
                        f"{season_keys_map.get(int(row['media_version_id'])) or ''}"
                        for row in season_grouped[key_]
                    }
                    if len(distinct) >= 2:
                        multi_source_seasons.add(key_)
            detail_candidates: list[dict[str, Any]] = [
                row for rows_ in grouped.values() if len(rows_) >= 2 for row in rows_
            ]
            for key_ in multi_source_seasons:
                detail_candidates.extend(season_grouped[key_])
            self._load_details(connection, detail_candidates)
            logger.info(
                "[媒体去重] 完结季 %s 个，其中多套来源 %s 个，需补取详情的行 %s。",
                len(completed_season_keys),
                len(multi_source_seasons),
                len(detail_candidates),
            )

            season_decisions: dict[tuple[str, int], set[int]] = {}
'''

# ---------------------------------------------------------------- 3. 新增 _load_details
ANCHOR_LIST_PLANS = '''    def list_plans(self) -> list[dict[str, Any]]:
'''

NEW_LIST_PLANS = '''    def _load_season_source_keys(
        self, connection: Any, rows: list[dict[str, Any]]
    ) -> dict[int, str]:
        """[etkn-fix18 fix18-season-key-light] 完结季判「是否多套来源」用的轻量键。

        只取存储商 + 发行组 + 目标目录三项文本，避免把 27.9 万行的 jsonb 快照全部对象化。
        键里不含品类（category_cid 取自主查询的行，外层拼上即可）。
        """
        version_ids = sorted({
            int(row["media_version_id"])
            for row in rows
            if row.get("media_version_id")
        })
        keys: dict[int, str] = {}
        for start in range(0, len(version_ids), 1000):
            chunk = version_ids[start:start + 1000]
            placeholders = ",".join(["%s"] * len(chunk))
            fetched = connection.execute(
                f"""
                SELECT version.id AS media_version_id,
                       lower(COALESCE(version.washing_snapshot->>'release_group_alias',
                                      version.washing_snapshot->>'release_group', '')) AS release_group,
                       COALESCE(version.washing_snapshot->>'target_directory_cid', '') AS target_directory_cid,
                       lower(COALESCE(version.storage_provider, '')) AS storage_provider
                FROM media.item_versions version
                WHERE version.id IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            for item in fetched:
                keys[int(item["media_version_id"])] = "|".join((
                    str(item["storage_provider"] or ""),
                    str(item["release_group"] or ""),
                    str(item["target_directory_cid"] or ""),
                ))
        return keys

    def _load_details(self, connection: Any, rows: list[dict[str, Any]]) -> None:
        """[etkn-fix18 fix18-lazy-details] 按需补取版本大字段：洗版快照 / ffprobe / mediainfo。

        主扫描会遍历全部当前版本（35 万级），但真正需要看清晰度、音轨、字幕的只有
        「组内有多份、可能生成清理计划」的那部分。这里按 400 个一批回查，避免
        jsonb 全量对象化（实测 33KB/行，全量 ≈ 11GB）。
        """
        version_ids = sorted({
            int(row["media_version_id"])
            for row in rows
            if row.get("media_version_id")
        })
        details: dict[int, dict[str, Any]] = {}
        for start in range(0, len(version_ids), 400):
            chunk = version_ids[start:start + 400]
            placeholders = ",".join(["%s"] * len(chunk))
            fetched = connection.execute(
                f"""
                SELECT version.id AS media_version_id, version.washing_snapshot,
                       analysis.raw_ffprobe_json, analysis.mediainfo_json
                FROM media.item_versions version
                LEFT JOIN media.version_analysis analysis ON analysis.media_version_id = version.id
                WHERE version.id IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            for detail in fetched:
                details[int(detail["media_version_id"])] = dict(detail)
        for row in rows:
            detail = details.get(int(row["media_version_id"])) or {}
            row["washing_snapshot"] = detail.get("washing_snapshot")
            row["raw_ffprobe_json"] = detail.get("raw_ffprobe_json")
            row["mediainfo_json"] = detail.get("mediainfo_json")
        logger.info(
            "[媒体去重] 按需补取版本详情 %s 个（需判定的行数 %s）。",
            len(details),
            len(rows),
        )

    def list_plans(self) -> list[dict[str, Any]]:
'''

# ---------------------------------------------------------------- 4. 单一来源的完结季直接判空
ANCHOR_SEASON_DECISION = '''                if not self._is_etkn_completed_season(season_rows):
                    continue
'''

NEW_SEASON_DECISION = '''                if not self._is_etkn_completed_season(season_rows):
                    continue
                # [etkn-fix18 fix18-season-single-source] 该季只有一套来源时，淘汰集合必为空，
                # 不必为它加载 jsonb 大字段；登记空集合，判定结果与原逻辑一致。
                if season_key not in multi_source_seasons:
                    season_decisions[season_key] = set()
                    continue
'''

# ---------------------------------------------------------------- 5. 无淘汰项的完结季直接跳过
ANCHOR_EMPTY_LOSERS = '''                if season_key in season_decisions:
                    loser_ids = season_decisions[season_key]
                    versions = [self._version_snapshot(row) for row in item_rows]
'''

NEW_EMPTY_LOSERS = '''                if season_key in season_decisions:
                    loser_ids = season_decisions[season_key]
                    # [etkn-fix18 fix18-skip-empty-losers] 完结季没有淘汰项时，下面必定 continue；
                    # 这里直接跳过，省掉整季每个版本的快照与字幕语言识别（27.9 万行的正则开销，
                    # 实测这一处就占扫描总耗时的大头，py-spy 栈停在 subtitles.detect_language_kind）。
                    if not loser_ids:
                        continue
                    versions = [self._version_snapshot(row) for row in item_rows]
'''

# ---------------------------------------------------------------- 6. [fix19] 扫描范围（library_ids）此前是死配置
ANCHOR_SCAN_HEAD = '''        self.configuration()
        washing = OrganizeSettingsService(self.database).get("washing")["value"]
'''

NEW_SCAN_HEAD = '''        config = self.configuration()
        # [etkn-fix19 2026-10-30] 「指定媒体库」之前完全没生效：scan() 读了配置却从不使用，
        # 于是不管选哪个库都扫全库（实测选「iso原盘电影」仍生成 4094 组跨库计划）。
        _scope_library_ids: list[int] = []
        for _value in (config.get("library_ids") or []):
            _text = str(_value).strip()
            if _text.isdigit():
                _scope_library_ids.append(int(_text))
        washing = OrganizeSettingsService(self.database).get("washing")["value"]
'''

ANCHOR_SCOPE_RESOLVE = '''        with self.database.transaction() as connection:
            rows = connection.execute(
'''

NEW_SCOPE_RESOLVE = '''        with self.database.transaction() as connection:
            # [etkn-fix19 2026-10-30] 把扫描范围解析成媒体项 ID 集合；未配置范围时为 None（不过滤）。
            _scope_item_ids = self._dedup_scope_item_ids(connection, _scope_library_ids)
            if _scope_library_ids:
                if not _scope_item_ids:
                    raise RuntimeError(
                        "扫描范围未解析出任何媒体项（媒体库 "
                        + ", ".join(str(item) for item in _scope_library_ids)
                        + " 可能为空或尚未同步）"
                    )
                task_log(
                    f"扫描范围：{len(_scope_library_ids)} 个媒体库，共 {len(_scope_item_ids)} 个媒体项。",
                    {"library_ids": _scope_library_ids, "scoped_items": len(_scope_item_ids)},
                )
            rows = connection.execute(
'''

ANCHOR_WHERE = """                WHERE item.item_type IN ('movie', 'episode')
                  AND item.is_active
"""

NEW_WHERE = """                WHERE item.item_type IN ('movie', 'episode')
                  AND item.is_active
                  -- [etkn-fix19 2026-10-30] 只扫配置范围内媒体库的媒体项；未配置范围时参数为 NULL。
                  AND (%s::bigint[] IS NULL OR item.id = ANY(%s::bigint[]))
"""

ANCHOR_FETCH = '''                ORDER BY item.id, version.id
                """,
            ).fetchall()
'''

NEW_FETCH = '''                ORDER BY item.id, version.id
                """,
                (_scope_item_ids, _scope_item_ids),
            ).fetchall()
'''

ANCHOR_SCOPE_HELPER = '''    def configuration(self) -> dict[str, Any]:
'''

NEW_SCOPE_HELPER = '''    def _dedup_scope_item_ids(self, connection: Any, library_ids: list[int]) -> list[int] | None:
        """[etkn-fix19 2026-10-30] 把「指定媒体库」解析成 media.items.id 集合。"""
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

    def configuration(self) -> dict[str, Any]:
'''

# (标识, 原片段, 替换片段, 幂等标记)
REPLACEMENTS = [
    ("select_light", ANCHOR_SELECT, NEW_SELECT, "fix18-select-light] 主查询不再取分析大字段"),
    ("join_light", ANCHOR_JOIN, NEW_JOIN, "fix18-select-light] version_analysis 的 JOIN 挪到"),
    ("lazy_details", ANCHOR_SEASON_LOOP, NEW_SEASON_LOOP, "fix18-lazy-details] 只为「可能出计划」"),
    ("season_single_source", ANCHOR_SEASON_DECISION, NEW_SEASON_DECISION,
     "fix18-season-single-source] 该季只有一套来源时"),
    ("skip_empty_losers", ANCHOR_EMPTY_LOSERS, NEW_EMPTY_LOSERS,
     "fix18-skip-empty-losers] 完结季没有淘汰项时"),
    ("load_details_method", ANCHOR_LIST_PLANS, NEW_LIST_PLANS,
     "def _load_season_source_keys("),
    ("scope_helper", ANCHOR_SCOPE_HELPER, NEW_SCOPE_HELPER,
     "def _dedup_scope_item_ids(self, connection: Any, library_ids"),
    ("scope_head", ANCHOR_SCAN_HEAD, NEW_SCAN_HEAD, "fix19] 「指定媒体库」之前完全没生效"),
    ("scope_resolve", ANCHOR_SCOPE_RESOLVE, NEW_SCOPE_RESOLVE, "fix19] 把扫描范围解析成媒体项 ID 集合"),
    ("scope_where", ANCHOR_WHERE, NEW_WHERE, "fix19 2026-10-30] 只扫配置范围内媒体库的媒体项"),
    ("scope_fetch", ANCHOR_FETCH, NEW_FETCH, "(_scope_item_ids, _scope_item_ids),"),
]

MARKERS = (
    "def _load_details(self, connection: Any",
    "def _load_season_source_keys(",
    "self._load_details(connection, detail_candidates)",
    "fix18-season-single-source",
    "fix18-select-light",
    "fix19 2026-10-30] 只扫配置范围内媒体库的媒体项",
    "(_scope_item_ids, _scope_item_ids),",
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
    if "analysis.raw_ffprobe_json" in patched.split("def _load_details")[0]:
        print("校验失败：主查询仍在取 raw_ffprobe_json", file=sys.stderr)
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
