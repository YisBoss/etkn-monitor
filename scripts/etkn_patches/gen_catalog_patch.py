#!/usr/bin/env python3
"""生成 catalog.py 补丁（两处，均为「规划器悬崖」的结构性修法）。

背景（2026-10-26：用户报「剧集追新 / IMDb 剧集250 两个媒体库打不开」）：

A. child_count() —— 列表页每条 Series/Season 都要算一次子项数
   上游 SQL：`... AND (EXISTS(直接版本) OR EXISTS(孙子版本))`
   只要有一个子项没有 current 版本，规划器就把第二个 EXISTS 变成 hashed SubPlan：
   media.items(37.8 万) × media.item_versions(35.8 万) 全量 Hash Join，单次 0.38~0.43s。
   实测 parent=265443 / 47637 / 6334 / 55247 全部 ~0.40s；反过来「子项都有版本」的
   大父节点反而 2~8ms —— 于是 library-21/26 一页 60 条 = 22.6s，客户端直接判「打不开」。
   修法：kids → 两条 LATERAL（强制逐行走 idx_media_item_versions_current_item）后
   UNION 去重计数。实测 1.1~11.2ms，且与父节点大小无关；结果与上游逐条实现完全一致。

B. collections() / collections_page() / collections_for_item_ids() —— 合集成员数
   上游对每个合集算一次相关子查询，规划器同样退化：Hash Join 的探测端是整个
   media.items（37.8 万行）。1037 个 active 合集 × 0.035s = 36.2s，
   而 Emby 客户端每次启动/切库都会请求 Views（内含合集分组）→ 前端表现为「库打不开」。
   修法：先按 tmdb_collection_id 一次聚合，再 LEFT JOIN 回合集表。实测 12.5ms。
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

BASE = Path("/vol1/1000/docker/etkn")
ORIG = BASE / "catalog.py.orig"
PATCH = BASE / "catalog_patch.py"
SIDECAR = BASE / "catalog_patch.py.md5"


def slice_function(source: str, head: str, next_head: str) -> str:
    start = source.index(head)
    end = source.index(next_head, start)
    return source[start:end]


NEW_COLLECTIONS = '''    def collections(self) -> list[dict[str, Any]]:
        # [etkn-fix15 2026-10-26] 勿删本标记：成员数改成「一次聚合 + LEFT JOIN」。
        # 上游的相关子查询会让规划器每次都全表扫 media.items（37.8 万行）当 Hash Join 探测端，
        # 1037 个合集 × 0.035s = 36.2s；改后统一走 items_pkey 索引探测（实测 12.5ms）。
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT collection.id, collection.tmdb_id, collection.name,
                       collection.metadata_json, collection.updated_at,
                       coalesce(member_counts.total, 0) AS member_count
                FROM collection.tmdb_collections collection
                LEFT JOIN (
                    SELECT member.tmdb_collection_id AS collection_id, count(*) AS total
                    FROM collection.tmdb_members member
                    JOIN LATERAL (
                        SELECT 1
                        FROM media.items item
                        WHERE item.id = member.media_item_id AND item.is_active
                        LIMIT 1
                    ) playable ON true
                    WHERE member.is_active
                    GROUP BY member.tmdb_collection_id
                ) member_counts ON member_counts.collection_id = collection.id
                WHERE collection.status = 'active'
                ORDER BY collection.name, collection.id
                """
            ).fetchall()
        return [dict(row) for row in rows]

'''

NEW_COLLECTIONS_PAGE = '''    def collections_page(self, start: int, limit: int) -> tuple[list[dict[str, Any]], int]:
        """Return one collection page and its total without hydrating every collection."""
        # [etkn-fix15 2026-10-26] 同 collections()：成员数一次聚合，别逐行相关子查询。
        offset = max(0, int(start))
        page_limit = max(1, int(limit))
        with self.database.connect() as connection:
            count_row = connection.execute(
                """
                SELECT count(*) AS total
                FROM collection.tmdb_collections
                WHERE status = 'active'
                """
            ).fetchone()
            rows = connection.execute(
                """
                SELECT collection.id, collection.tmdb_id, collection.name,
                       collection.metadata_json, collection.updated_at,
                       coalesce(member_counts.total, 0) AS member_count
                FROM collection.tmdb_collections collection
                LEFT JOIN (
                    SELECT member.tmdb_collection_id AS collection_id, count(*) AS total
                    FROM collection.tmdb_members member
                    JOIN LATERAL (
                        SELECT 1
                        FROM media.items item
                        WHERE item.id = member.media_item_id AND item.is_active
                        LIMIT 1
                    ) playable ON true
                    WHERE member.is_active
                    GROUP BY member.tmdb_collection_id
                ) member_counts ON member_counts.collection_id = collection.id
                WHERE collection.status = 'active'
                ORDER BY collection.name, collection.id
                OFFSET %s LIMIT %s
                """,
                (offset, page_limit),
            ).fetchall()
        return [dict(row) for row in rows], int((count_row or {}).get("total") or 0)

'''

NEW_COLLECTIONS_FOR_ITEMS = '''    def collections_for_item_ids(self, item_ids: list[int]) -> list[dict[str, Any]]:
        """Return active collections containing at least one requested media item."""
        # [etkn-fix15 2026-10-26] 同 collections()：命中集合一次聚合，成员数别再逐行子查询。
        if not item_ids:
            return []
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT collection.id, collection.tmdb_id, collection.name,
                       collection.metadata_json, collection.updated_at,
                       coalesce(member_counts.total, 0) AS member_count
                FROM collection.tmdb_collections collection
                JOIN (
                    SELECT DISTINCT member.tmdb_collection_id AS collection_id
                    FROM collection.tmdb_members member
                    WHERE member.is_active AND member.media_item_id = ANY(%s)
                ) hits ON hits.collection_id = collection.id
                LEFT JOIN (
                    SELECT member.tmdb_collection_id AS collection_id, count(*) AS total
                    FROM collection.tmdb_members member
                    JOIN LATERAL (
                        SELECT 1
                        FROM media.items item
                        WHERE item.id = member.media_item_id AND item.is_active
                        LIMIT 1
                    ) playable ON true
                    WHERE member.is_active
                    GROUP BY member.tmdb_collection_id
                ) member_counts ON member_counts.collection_id = collection.id
                WHERE collection.status = 'active'
                ORDER BY collection.name, collection.id
                """,
                (item_ids,),
            ).fetchall()
        return [dict(row) for row in rows]

'''

NEW_CHILD_COUNT = '''    def child_count(self, parent_id: int) -> int:
        """Count playable direct children without loading their media payloads."""
        # [etkn-fix15 2026-10-26] 勿删本标记：上游 `EXISTS OR EXISTS` 会退化成
        # 「items × item_versions 全量 Hash Join」（单次 0.4s，列表 60 条要 22.6s）。
        # 这里两条分支都改成 JOIN LATERAL(...LIMIT 1)，强制逐行走 item_versions 索引；
        # UNION 去重后计数，语义与上游一致（直接可播 或 有可播孙项）。实测 1~11ms。
        with self.database.connect() as connection:
            row = connection.execute(
                """
                WITH kids AS (
                    SELECT id
                    FROM media.items
                    WHERE parent_id = %s AND is_active
                ),
                direct AS (
                    SELECT kids.id AS child_id
                    FROM kids
                    JOIN LATERAL (
                        SELECT 1
                        FROM media.item_versions version
                        WHERE version.item_id = kids.id AND version.is_current
                          AND (NULLIF(btrim(version.path), '') IS NOT NULL
                               OR NULLIF(btrim(version.pick_code), '') IS NOT NULL)
                        LIMIT 1
                    ) playable ON true
                ),
                nested AS (
                    SELECT kids.id AS child_id
                    FROM kids
                    JOIN LATERAL (
                        SELECT 1
                        FROM media.items child
                        JOIN LATERAL (
                            SELECT 1
                            FROM media.item_versions version
                            WHERE version.item_id = child.id AND version.is_current
                              AND (NULLIF(btrim(version.path), '') IS NOT NULL
                                   OR NULLIF(btrim(version.pick_code), '') IS NOT NULL)
                            LIMIT 1
                        ) playable_child ON true
                        WHERE child.parent_id = kids.id AND child.is_active
                        LIMIT 1
                    ) playable ON true
                )
                SELECT count(*) AS total
                FROM (
                    SELECT child_id FROM direct
                    UNION
                    SELECT child_id FROM nested
                ) counted
                """,
                (parent_id,),
            ).fetchone()
        return int((row or {}).get("total") or 0)

'''

NEW_ITEMS_FOR_FACETS = '''    def items_for_facets(self, item_ids: list[int]) -> list[dict[str, Any]]:
        """Load only metadata needed by library facet endpoints."""
        # [etkn-fix20 2026-10-31] 行集缓存加硬边界。上游把整份 item_ids 展开进 cache key，
        # 且只在「键数 > 64」时才清过期项；rating_index_revision 每前进一步就多存一份全库
        # 行集（29,727 条 ≈ 682MB），11 个历史 revision 直接顶到 7GB+，10-29 21:56 因此把
        # 整机内存吃穿、NAS 卡死。现在：①key 改成 (revision, len, hash(ids))；
        # ②revision 变化时整体清空旧行集；③最多留 _FACET_ROW_CACHE_MAX_ENTRIES 份，
        # 超出按过期时间淘汰一条（不再等 64 条）。上层还有 15 分钟的 317 值快照兜底。
        ids = list(dict.fromkeys(int(item_id) for item_id in item_ids))
        if not ids:
            return []
        revision = rating_index_revision(self.database)
        cache_key = (revision, len(ids), hash(tuple(ids)))
        now = time.monotonic()
        with _facet_cache_lock:
            cached = _facet_cache.get(cache_key)
            if cached and cached[0] > now:
                return list(cached[1])
            resolve_lock = _facet_locks.setdefault(cache_key, threading.Lock())
        with resolve_lock:
            now = time.monotonic()
            with _facet_cache_lock:
                cached = _facet_cache.get(cache_key)
                if cached and cached[0] > now:
                    return list(cached[1])
            facet_payload = """
                jsonb_strip_nulls(jsonb_build_object(
                    'genres', {alias}.payload -> 'genres',
                    'genres_json', {alias}.payload -> 'genres_json',
                    '_manual_tags', {alias}.payload -> '_manual_tags',
                    'keywords', {alias}.payload -> 'keywords',
                    'keywords_json', {alias}.payload -> 'keywords_json',
                    'production_companies', {alias}.payload -> 'production_companies',
                    'production_companies_json', {alias}.payload -> 'production_companies_json',
                    'networks', {alias}.payload -> 'networks',
                    'networks_json', {alias}.payload -> 'networks_json',
                    'official_rating', {alias}.payload -> 'official_rating',
                    'official_rating_json', {alias}.payload -> 'official_rating_json',
                    'custom_rating', {alias}.payload -> 'custom_rating',
                    'content_ratings', {alias}.payload -> 'content_ratings',
                    'release_dates', {alias}.payload -> 'release_dates',
                    'origin_country', {alias}.payload -> 'origin_country',
                    'production_countries', {alias}.payload -> 'production_countries',
                    'adult', {alias}.payload -> 'adult',
                    'year', {alias}.payload -> 'year'
                ))
            """
            metadata_payload = rating_cache_projection("metadata", facet_payload.format(alias="metadata"))
            parent_metadata_payload = rating_cache_projection("parent_metadata", facet_payload.format(alias="parent_metadata"))
            grandparent_metadata_payload = rating_cache_projection("grandparent_metadata", facet_payload.format(alias="grandparent_metadata"))
            with self.database.connect() as connection:
                rows = connection.execute(
                f"""
                SELECT item.id, item.item_type, item.release_date,
                       COALESCE({metadata_payload}, '{{}}'::jsonb) AS metadata,
                       COALESCE({parent_metadata_payload}, '{{}}'::jsonb) AS parent_metadata,
                       COALESCE({grandparent_metadata_payload}, '{{}}'::jsonb) AS grandparent_metadata
                FROM media.items item
                LEFT JOIN media.item_metadata metadata
                  ON metadata.item_id = item.id AND metadata.is_current
                LEFT JOIN media.items parent
                  ON parent.id = item.parent_id
                LEFT JOIN media.item_metadata parent_metadata
                  ON parent_metadata.item_id = parent.id AND parent_metadata.is_current
                LEFT JOIN media.items grandparent
                  ON grandparent.id = parent.parent_id
                LEFT JOIN media.item_metadata grandparent_metadata
                  ON grandparent_metadata.item_id = grandparent.id AND grandparent_metadata.is_current
                WHERE item.id = ANY(%s) AND item.is_active
                """,
                    (ids,),
                ).fetchall()
            by_id = {int(row["id"]): dict(row) for row in rows}
            result = [by_id[item_id] for item_id in ids if item_id in by_id]
            cached_count = 0
            with _facet_cache_lock:
                for key in [key for key in _facet_cache if key[0] != revision]:
                    _facet_cache.pop(key, None)
                _facet_cache[cache_key] = (time.monotonic() + _FACET_CACHE_TTL_SECONDS, result)
                while len(_facet_cache) > _FACET_ROW_CACHE_MAX_ENTRIES:
                    oldest = min(_facet_cache, key=lambda key: _facet_cache[key][0])
                    _facet_cache.pop(oldest, None)
                if len(_facet_locks) > 32:
                    for lock_key in [key for key in _facet_locks if key != cache_key][:16]:
                        _facet_locks.pop(lock_key, None)
                cached_count = len(_facet_cache)
            try:
                est_mb = round(sum(len(str(value)) for row in result for value in row.values()) / 1048576.0, 1)  # 载荷字符量，非对象开销
            except Exception:
                est_mb = -1.0
            logger.log(
                logging.WARNING if (cached_count > 1 or est_mb > 1500 or len(result) > 5000) else logging.INFO,
                "facet 行集 rows=%s 载荷=%sMB revision=%s 缓存份数=%s 上限=%s",
                len(result), est_mb, revision, cached_count, _FACET_ROW_CACHE_MAX_ENTRIES,
            )
            return list(result)

'''

SIMPLE_REPLACEMENTS = [
    (
        "facet_row_cache_max_entries",
        '_facet_cache: dict[tuple[Any, ...], tuple[float, list[dict[str, Any]]]] = {}\n',
        '_facet_cache: dict[tuple[Any, ...], tuple[float, list[dict[str, Any]]]] = {}\n_FACET_ROW_CACHE_MAX_ENTRIES = 2  # [etkn-fix20 2026-10-31] 行集最多留两份，见 items_for_facets 注释\n',
    ),
]


REPLACEMENTS = [
    ("collections", ("    def collections(self)", "    def collections_page("), NEW_COLLECTIONS),
    ("collections_page", ("    def collections_page(", "    def collection(self,"), NEW_COLLECTIONS_PAGE),
    ("collections_for_item_ids", ("    def collections_for_item_ids(", "    def person_item_ids("),
     NEW_COLLECTIONS_FOR_ITEMS),
    ("items_for_facets", ("    def items_for_facets(", "    def child_count(self"),
     NEW_ITEMS_FOR_FACETS),
    ("child_count", ("    def child_count(self", "    def screenshot_item_ids("), NEW_CHILD_COUNT),
]


def main() -> int:
    if not ORIG.exists():
        print(f"缺少上游原文：{ORIG}", file=sys.stderr)
        return 2
    source = ORIG.read_text(encoding="utf-8")
    patched = source
    for name, old_text, new_text in SIMPLE_REPLACEMENTS:
        if old_text not in patched:
            print(f"[{name}] 锚点缺失", file=sys.stderr)
            return 3
        patched = patched.replace(old_text, new_text, 1)
        print(f"[{name}] 已替换")
    for name, (head, next_head), new_text in REPLACEMENTS:
        try:
            old_text = slice_function(patched, head, next_head)
        except ValueError:
            print(f"[{name}] 锚点缺失（head={head!r}）", file=sys.stderr)
            return 3
        patched = patched.replace(old_text, new_text, 1)
        print(f"[{name}] 已替换（{len(old_text)} → {len(new_text)} 字符）")
    PATCH.write_text(patched, encoding="utf-8")
    digest = hashlib.md5(patched.encode("utf-8")).hexdigest()
    SIDECAR.write_text(f"{digest}  {PATCH}\n", encoding="utf-8")
    import py_compile

    py_compile.compile(str(PATCH), doraise=True)
    print(f"已生成 {PATCH}（{len(patched)} 字符，md5 {digest}）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
