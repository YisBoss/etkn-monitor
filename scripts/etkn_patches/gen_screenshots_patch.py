#!/usr/bin/env python3
"""生成 screenshots.py 补丁：截图范围解析加「TTL 缓存 + 后台刷新」（起播每次白付 1.5 秒）。

背景（2026-10-25 起播深挖）：
  用户反馈「作者说秒播，我这边 5 秒」，py-spy 抓到的调用链不是播放链路本身：

    stream_video (api/media_server.py:4129)
      → catalog.item() (modules/media_server/catalog.py:2241)
      → items_by_ids() (catalog.py:3428)
      → _screenshot_configs_by_item_id() (modules/scrape/screenshots.py:682)
      → capture_item_ids_for_library() (screenshots.py:699)
      → _expand_capture_item_ids() (screenshots.py:706)   ← 递归 CTE 展开整库后代

  实测单个 item 的截图配置解析 1.497s（与观测到的 1.57s 起播完全吻合），拆解：

    lib 1  国外剧   roots=3279  根解析 0.071s + 展开 76071 条 0.311s
    lib 2  国产剧   roots=3156  0.080s + 108950 条 0.343s
    lib 7  儿童动漫 roots=414   0.026s + 29800 条 0.059s
    lib 17 iso原盘  roots=13098 0.033s + 13098 条 0.104s
    …（共 11 个库开了 extract_episode_screenshot）
    lib 18 智能推荐 roots=0     14.721s（来源为 personal_recommendation）

  也就是说：每取一条 item 详情、每起播一次，都要把 11 个媒体库的后代集合
  （38.3 万 items 的递归 CTE）重算一遍。库越大越慢；作者的库只有 1 万多条，
  所以他那边「秒播」——不是播放链路快，而是这笔固定开销小。

修法：
  截图配置只用来判断「这条 item 属于哪个库、要不要显示截图入口」，允许分钟级陈旧。
  capture_item_ids_for_library() 改为：
    ① 命中且未过期 → 直接返回副本；
    ② 命中但已过期（宽限 1 小时）→ 先返回旧值，后台线程刷新，请求不等待；
    ③ 未命中 → 同步计算一次（进程启动后每个库仅一次）。
  缓存上限 64 库、按最旧淘汰；TTL 由 ETK_CAPTURE_IDS_TTL_SECONDS 控制（默认 300 秒）。
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

BASE = Path("/vol1/1000/docker/etkn")
ORIG = BASE / "screenshots.py.orig"
PATCH = BASE / "screenshots_patch.py"
SIDECAR = BASE / "screenshots_patch.py.md5"

ANCHOR_IMPORT = "from threading import Lock\n"
OLD_IMPORT = "from concurrent.futures import ThreadPoolExecutor, as_completed\n"
NEW_IMPORT = "from concurrent.futures import ThreadPoolExecutor, as_completed\nfrom threading import Thread\n"

OLD_BLOCK = '''    def capture_item_ids_for_library(self, library_id: int) -> list[str]:
        from etk_vnext.modules.media_server import MediaServerCatalog

        roots = MediaServerCatalog(self.database).library_item_ids(int(library_id))
        expanded = self._expand_capture_item_ids(roots)
        return [f"item-{item_id}" for item_id in expanded]
'''

NEW_BLOCK = '''    def capture_item_ids_for_library(self, library_id: int) -> list[str]:
        # [etkn-fix 2026-10-25 起播提速] 原实现每次调用都重算整库后代集合：
        # library_item_ids() 逐个库做路径/筛选匹配（lib1 0.071s / lib2 0.080s），
        # _expand_capture_item_ids() 再对 38 万条 media.items 跑递归 CTE
        # （lib1 76071 条 0.311s、lib2 108950 条 0.343s）。11 个开了截图提取的库累计
        # 1.497s，而这段开销挂在 catalog.item() 上——每次取 item 详情、每次起播
        # 都要白付一遍（实测起播 1.57s 全是它，与播放链路无关）。
        # 截图配置只决定「要不要显示截图入口」，允许分钟级陈旧：
        # 过期后先返回旧值、后台刷新，请求不再为此等待。
        try:
            cache_key = int(library_id)
        except (TypeError, ValueError):
            cache_key = 0
        now = time.time()
        cached = _CAPTURE_IDS_CACHE.get(cache_key)
        if cached is not None:
            age = now - cached[0]
            if age < _CAPTURE_IDS_TTL_SECONDS:
                return list(cached[1])
            if age < _CAPTURE_IDS_STALE_SECONDS:
                self._refresh_capture_ids_async(cache_key)
                return list(cached[1])
        result = self._compute_capture_ids(cache_key)
        return list(result)

    def _compute_capture_ids(self, library_id: int) -> tuple[str, ...]:
        from etk_vnext.modules.media_server import MediaServerCatalog

        roots = MediaServerCatalog(self.database).library_item_ids(int(library_id))
        expanded = self._expand_capture_item_ids(roots)
        result = tuple(f"item-{item_id}" for item_id in expanded)
        with _CAPTURE_IDS_CACHE_LOCK:
            _CAPTURE_IDS_CACHE[int(library_id)] = (time.time(), result)
            if len(_CAPTURE_IDS_CACHE) > 64:
                for stale_key in sorted(
                    _CAPTURE_IDS_CACHE,
                    key=lambda key: _CAPTURE_IDS_CACHE[key][0],
                )[:32]:
                    _CAPTURE_IDS_CACHE.pop(stale_key, None)
        return result

    def _refresh_capture_ids_async(self, library_id: int) -> None:
        with _CAPTURE_IDS_CACHE_LOCK:
            if library_id in _CAPTURE_IDS_REFRESHING:
                return
            _CAPTURE_IDS_REFRESHING.add(library_id)

        def _run() -> None:
            try:
                self._compute_capture_ids(library_id)
            except Exception:
                logger.warning("[视频截图] 后台刷新媒体库截图范围失败：library=%s", library_id, exc_info=True)
            finally:
                with _CAPTURE_IDS_CACHE_LOCK:
                    _CAPTURE_IDS_REFRESHING.discard(library_id)

        Thread(target=_run, name=f"capture-ids-refresh-{library_id}", daemon=True).start()
'''

CACHE_BLOCK = '''
# [etkn-fix 2026-10-25 起播提速] 截图范围解析的进程内缓存（TTL + 后台刷新）。
# 见 capture_item_ids_for_library()：11 个库整库展开累计 1.5s，且每次 item()/起播都跑。
try:
    _CAPTURE_IDS_TTL_SECONDS = float(os.environ.get("ETK_CAPTURE_IDS_TTL_SECONDS") or 300)
except (TypeError, ValueError):
    _CAPTURE_IDS_TTL_SECONDS = 300.0
_CAPTURE_IDS_STALE_SECONDS = max(_CAPTURE_IDS_TTL_SECONDS * 12.0, 3600.0)
_CAPTURE_IDS_CACHE: dict[int, tuple[float, tuple[str, ...]]] = {}
_CAPTURE_IDS_REFRESHING: set[int] = set()
_CAPTURE_IDS_CACHE_LOCK = Lock()

'''

ANCHOR_LOCK = "_PRECAPTURE_LOCK = Lock()\n"


def main() -> int:
    if not ORIG.exists():
        print(f"缺少上游原文：{ORIG}", file=sys.stderr)
        return 2
    content = ORIG.read_text(encoding="utf-8")
    if content.count(OLD_BLOCK) != 1:
        print("锚点不唯一或缺失：capture_item_ids_for_library", file=sys.stderr)
        return 3
    if content.count(ANCHOR_LOCK) != 1:
        print("锚点不唯一或缺失：_PRECAPTURE_LOCK", file=sys.stderr)
        return 4
    if content.count(OLD_IMPORT) != 1:
        print("锚点不唯一或缺失：ThreadPoolExecutor import", file=sys.stderr)
        return 5
    if content.count(ANCHOR_IMPORT) != 1:
        print("锚点不唯一或缺失：from threading import Lock", file=sys.stderr)
        return 6
    patched = content.replace(OLD_BLOCK, NEW_BLOCK)
    patched = patched.replace(ANCHOR_LOCK, ANCHOR_LOCK + CACHE_BLOCK, 1)
    patched = patched.replace(OLD_IMPORT, NEW_IMPORT, 1)
    PATCH.write_text(patched, encoding="utf-8")
    digest = hashlib.md5(patched.encode("utf-8")).hexdigest()
    SIDECAR.write_text(f"{digest}  {PATCH}\n", encoding="utf-8")
    print(f"已生成 {PATCH}（{len(patched)} 字节，md5 {digest}）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
