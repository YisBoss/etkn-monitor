#!/usr/bin/env python3
"""生成 virtual_library/service.py 补丁：新建媒体库弹窗的筛选项改 SQL 聚合 + 缓存。

背景（2026-10-25 用户反馈「点新建媒体库要等很久才弹窗」）：
  浏览器实测：点击 → 28.9 秒才出现 `.n-modal`，期间唯一的慢请求是
  `GET /api/virtual-libraries/catalog`（30.6s / 33.0s，响应体仅 13.6KB）。
  服务端拆解（容器内直接调用）：

    catalog_payload()      0.00s
    list_emby_users()      0.02s
    _local_options()      27.04s   ← 元凶
    _enrich_catalog()      3.08s   （两次 TMDB genre 请求）
    _vocabularies()        0.00s

  `_local_options()` 的原实现：

    SELECT snapshot.payload FROM media.item_metadata snapshot WHERE snapshot.is_current

  把全库 382,717 行的 metadata 全量拉进 Python（解压后约 823MB JSON：episode 334,629 行
  230MB、season 19,388 行 283MB、movie 18,153 行 222MB、series 10,547 行 88MB），
  逐行 set.add() 出 7 组筛选项。

  但紧接着这些结果里 countries / languages / studios / keywords / ratings 五组
  会被 `_vocabularies()` 的配置词表**整组覆盖丢弃**，真正活下来的只有：
    genres → 27 个（再经 translated_genre_name 翻译）
    tags   → 0 个
  也就是说 27 秒里有 99% 是在给丢掉的字段做无用功。

修法：
  ① 只算 genres / tags，改为 SQL 侧 jsonb 聚合，且限定 item_type in ('movie','series')
     （episode/season 的 payload 与所属剧集重复；实测两种范围的 distinct 结果完全一致：
      genres 27/27、tags 0/0）。
     实测 genres 单查 0.72s（全表 1.39s），tags 0.02s。
  ② 结果进进程内 TTL 缓存（默认 600 秒），过期先返旧值、后台线程刷新，请求不等待。
  ③ `_enrich_catalog()` 的两次 TMDB `genre/*/list` 也缓存（默认 3600 秒 + 后台刷新），
     消掉每个弹窗固定 3.08 秒的外网开销。
  ④ 输出结构与顺序不变（genres 仍走 translated_genre_name，其余键盘沿用 _vocabularies）。
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

BASE = Path("/vol1/1000/docker/etkn")
ORIG = BASE / "virtual_library_service.py.orig"
PATCH = BASE / "virtual_library_service_patch.py"
SIDECAR = BASE / "virtual_library_service_patch.py.md5"

MARKER = "[etkn-fix 2026-10-25 媒体库筛选项]"

OLD_IMPORT = "from typing import Any, Iterator\n"
NEW_IMPORT = "from typing import Any, Iterator\nfrom concurrent.futures import ThreadPoolExecutor\nfrom threading import Lock, Thread\n\nimport time\n"

ANCHOR_LOGGER = "logger = logging.getLogger(__name__)\n"

CACHE_BLOCK = '''
# [etkn-fix 2026-10-25 媒体库筛选项] 进程内缓存：筛选项集合 + TMDB 类型表。
# 原 _local_options() 每次弹窗都全表扫 media.item_metadata（823MB JSON，27.0s），
# _enrich_catalog() 再打两次 TMDB（3.08s）；两者结果都是小时级才变一次的东西。
_FILTER_VALUES_CACHE: dict[str, object] = {}
_FILTER_VALUES_CACHE_LOCK = Lock()
_FILTER_VALUES_REFRESHING: set[int] = set()
_FILTER_VALUES_TTL_SECONDS = float(os.environ.get("ETK_FILTER_VALUES_TTL_SECONDS", "600"))
_FILTER_VALUES_STALE_SECONDS = float(os.environ.get("ETK_FILTER_VALUES_STALE_SECONDS", "21600"))

_TMDB_GENRES_CACHE: dict[str, object] = {}
_TMDB_GENRES_CACHE_LOCK = Lock()
_TMDB_GENRES_REFRESHING: set[int] = set()
_TMDB_GENRES_TTL_SECONDS = float(os.environ.get("ETK_TMDB_GENRES_TTL_SECONDS", "3600"))
_TMDB_GENRES_STALE_SECONDS = float(os.environ.get("ETK_TMDB_GENRES_STALE_SECONDS", "86400"))
# 冷启动兜底：容器重启后先读磁盘快照秒回，再后台刷新（TMDB 两次请求实测 3~5 秒）
_TMDB_GENRES_CACHE_FILE = os.environ.get(
    "ETK_VL_GENRES_CACHE_FILE", "/config/virtual-library-tmdb-genres.json"
)

_SQL_FILTER_GENRES = """
    SELECT DISTINCT coalesce(genre->>'name', genre #>> '{}') AS name
    FROM media.item_metadata snapshot
    JOIN media.items item ON item.id = snapshot.item_id
    CROSS JOIN LATERAL jsonb_array_elements(coalesce(snapshot.payload->'genres', '[]'::jsonb)) AS genre
    WHERE snapshot.is_current
      AND item.item_type IN ('movie', 'series')
"""

_SQL_FILTER_TAGS = """
    SELECT DISTINCT coalesce(tag->>'name', tag->>'Name', tag #>> '{}') AS name
    FROM media.item_metadata snapshot
    JOIN media.items item ON item.id = snapshot.item_id
    CROSS JOIN LATERAL jsonb_array_elements(coalesce(snapshot.payload->'tags', '[]'::jsonb)) AS tag
    WHERE snapshot.is_current
      AND item.item_type IN ('movie', 'series')
"""

'''

NEW_LOCAL_OPTIONS = '''    def _scan_filter_values(self) -> dict[str, set[str]]:
        """返回筛选项候选集合。

        [etkn-fix 2026-10-25 媒体库筛选项] 原实现全表拉 media.item_metadata.payload
        在 Python 里逐行解析（27.0s），但 countries/languages/studios/keywords/ratings
        随后会被 _vocabularies() 整组覆盖，真正用到的只有 genres 与 tags。
        这里只算这两个，并且按 movie/series 限定范围（结果与全表一致）。
        """
        now = time.time()
        with _FILTER_VALUES_CACHE_LOCK:
            cached = _FILTER_VALUES_CACHE.get("values")
        if cached is not None:
            stamp, stored = cached  # type: ignore[misc]
            age = now - stamp
            if age < _FILTER_VALUES_TTL_SECONDS:
                return {key: set(items) for key, items in stored.items()}  # type: ignore[union-attr]
            if age < _FILTER_VALUES_STALE_SECONDS:
                self._refresh_filter_values_async()
                return {key: set(items) for key, items in stored.items()}  # type: ignore[union-attr]
        return self._compute_filter_values()

    def _compute_filter_values(self) -> dict[str, set[str]]:
        values: dict[str, set[str]] = {key: set() for key in ("genres", "countries", "languages", "studios", "keywords", "tags", "ratings")}
        with self.database.connect() as connection:
            for row in connection.execute(_SQL_FILTER_GENRES).fetchall():
                name = row["name"]
                if name:
                    values["genres"].add(str(name))
            for row in connection.execute(_SQL_FILTER_TAGS).fetchall():
                name = row["name"]
                if name:
                    values["tags"].add(str(name))
        with _FILTER_VALUES_CACHE_LOCK:
            _FILTER_VALUES_CACHE["values"] = (
                time.time(),
                {key: frozenset(items) for key, items in values.items()},
            )
        return values

    def _refresh_filter_values_async(self) -> None:
        with _FILTER_VALUES_CACHE_LOCK:
            if _FILTER_VALUES_REFRESHING:
                return
            _FILTER_VALUES_REFRESHING.add(1)

        def _run() -> None:
            try:
                self._compute_filter_values()
            except Exception:
                logger.warning("[媒体库筛选项] 后台刷新失败", exc_info=True)
            finally:
                with _FILTER_VALUES_CACHE_LOCK:
                    _FILTER_VALUES_REFRESHING.discard(1)

        Thread(target=_run, name="filter-values-refresh", daemon=True).start()

    def _local_options(self) -> dict[str, list[dict[str, str]]]:
        values = self._scan_filter_values()
'''

OLD_ENRICH_HEAD = '''        try:
            tmdb = self._tmdb()
            movie_genres = tmdb.get("genre/movie/list").get("genres") or []
            series_genres = tmdb.get("genre/tv/list").get("genres") or []
'''

NEW_ENRICH_HEAD = '''        try:
            movie_genres, series_genres = self._tmdb_genre_lists()
'''

TMDB_METHODS = '''    def _tmdb_genre_lists(self) -> tuple[list[Any], list[Any]]:
        """[etkn-fix 2026-10-25 媒体库筛选项] TMDB 类型表缓存（两次外网请求 3~5s/次）。"""
        now = time.time()
        with _TMDB_GENRES_CACHE_LOCK:
            cached = _TMDB_GENRES_CACHE.get("genres")
        if cached is None:
            # 冷启动（容器刚起来）先吃磁盘快照，别让用户为两次外网请求买单
            cached = self._load_tmdb_genres_snapshot()
            if cached is not None:
                with _TMDB_GENRES_CACHE_LOCK:
                    _TMDB_GENRES_CACHE["genres"] = cached
        if cached is not None:
            stamp, movie, series = cached  # type: ignore[misc]
            age = now - stamp
            if age < _TMDB_GENRES_TTL_SECONDS:
                return movie, series  # type: ignore[return-value]
            if age < _TMDB_GENRES_STALE_SECONDS:
                self._refresh_tmdb_genres_async()
                return movie, series  # type: ignore[return-value]
        return self._compute_tmdb_genres()

    def _compute_tmdb_genres(self) -> tuple[list[Any], list[Any]]:
        tmdb = self._tmdb()
        # 两次 TMDB 请求彼此独立，串行要 4~5 秒，并行一半
        with ThreadPoolExecutor(max_workers=2) as pool:
            movie_future = pool.submit(lambda: tmdb.get("genre/movie/list").get("genres") or [])
            series_future = pool.submit(lambda: tmdb.get("genre/tv/list").get("genres") or [])
            movie_genres = movie_future.result()
            series_genres = series_future.result()
        with _TMDB_GENRES_CACHE_LOCK:
            _TMDB_GENRES_CACHE["genres"] = (time.time(), movie_genres, series_genres)
        self._store_tmdb_genres_snapshot(movie_genres, series_genres)
        return movie_genres, series_genres

    def _load_tmdb_genres_snapshot(self) -> tuple[float, list[Any], list[Any]] | None:
        try:
            path = Path(_TMDB_GENRES_CACHE_FILE)
            if not path.exists():
                return None
            payload = json.loads(path.read_text(encoding="utf-8"))
            movie = payload.get("movie")
            series = payload.get("series")
            if not isinstance(movie, list) or not isinstance(series, list) or not movie:
                return None
            stamp = float(payload.get("saved_at") or path.stat().st_mtime)
            return stamp, movie, series
        except Exception:
            return None

    def _store_tmdb_genres_snapshot(self, movie_genres: list[Any], series_genres: list[Any]) -> None:
        try:
            path = Path(_TMDB_GENRES_CACHE_FILE)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(
                    {"saved_at": time.time(), "movie": movie_genres, "series": series_genres},
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            tmp.replace(path)
        except Exception:
            logger.warning("[媒体库筛选项] TMDB 类型表落盘失败", exc_info=True)

    def _refresh_tmdb_genres_async(self) -> None:
        with _TMDB_GENRES_CACHE_LOCK:
            if _TMDB_GENRES_REFRESHING:
                return
            _TMDB_GENRES_REFRESHING.add(1)

        def _run() -> None:
            try:
                self._compute_tmdb_genres()
            except Exception:
                logger.warning("[媒体库筛选项] TMDB 类型表后台刷新失败", exc_info=True)
            finally:
                with _TMDB_GENRES_CACHE_LOCK:
                    _TMDB_GENRES_REFRESHING.discard(1)

        Thread(target=_run, name="tmdb-genres-refresh", daemon=True).start()

    def _enrich_catalog(self, catalog: dict[str, Any]) -> None:
'''


def build(source: str) -> str:
    if MARKER in source:
        raise SystemExit("源文件已含补丁标记，别重复打补丁")

    if source.count(OLD_IMPORT) != 1:
        raise SystemExit("找不到唯一的 typing 导入行")
    text = source.replace(OLD_IMPORT, NEW_IMPORT, 1)

    if text.count(ANCHOR_LOGGER) != 1:
        raise SystemExit("找不到唯一的 logger 定义")
    text = text.replace(ANCHOR_LOGGER, ANCHOR_LOGGER + CACHE_BLOCK, 1)

    start = text.find("    def _local_options(self) -> dict[str, list[dict[str, str]]]:\n")
    if start < 0:
        raise SystemExit("找不到 _local_options")
    end_marker = '                values["ratings"].add(str(payload["official_rating"]))\n'
    end = text.find(end_marker, start)
    if end < 0:
        raise SystemExit("找不到 _local_options 扫描循环结尾")
    end += len(end_marker)
    text = text[:start] + NEW_LOCAL_OPTIONS + text[end:]

    if text.count(OLD_ENRICH_HEAD) != 1:
        raise SystemExit("找不到唯一的 _enrich_catalog 头部")
    text = text.replace(OLD_ENRICH_HEAD, NEW_ENRICH_HEAD, 1)

    anchor_enrich = "    def _enrich_catalog(self, catalog: dict[str, Any]) -> None:\n"
    if text.count(anchor_enrich) != 1:
        raise SystemExit("找不到唯一的 _enrich_catalog 定义")
    text = text.replace(anchor_enrich, TMDB_METHODS, 1)

    for needed in (
        "_scan_filter_values",
        "_SQL_FILTER_GENRES",
        "_FILTER_VALUES_TTL_SECONDS",
        "_tmdb_genre_lists",
        "_TMDB_GENRES_TTL_SECONDS",
    ):
        if needed not in text:
            raise SystemExit(f"生成结果缺标记 {needed}")
    return text


def main() -> int:
    if not ORIG.exists():
        raise SystemExit(f"缺少上游原文件 {ORIG}")
    source = ORIG.read_text(encoding="utf-8")
    patched = build(source)
    PATCH.write_text(patched, encoding="utf-8")
    digest = hashlib.md5(patched.encode("utf-8")).hexdigest()
    SIDECAR.write_text(digest + "\n", encoding="utf-8")
    Path(SIDECAR).chmod(0o644)
    print(f"wrote {PATCH} ({len(patched.encode('utf-8'))} bytes, md5 {digest})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
