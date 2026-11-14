#!/usr/bin/env python3
"""生成 media_server 补丁（同一份文件里两处改动，均可幂等重跑）：

1) 榜单型虚拟库（显示占位条目）列表页只取「紧凑行」
   ------------------------------------------------------------------
   `api/media_server.py` 的「库父节点 + placeholders_enabled」分支为了按榜单顺序输出，
   先用 `RankingPlaceholders.rows(library_id, include_available=True)` 拿到整库全部条目
   （含已入库与缺失），再对**全部**已入库条目调用 `_list_item_rows(...)` 取完整行，
   最后才在 Python 层做过滤与分页。也就是说：翻第 2 页、第 3 页都要把整个库的行重取一遍。
   实测（library-20「影史殿堂1001」，1262 条）：全量行 4.9s/页；同样的 ids 走紧凑行 0.79s。
   改法：只在该分支的 `_list_item_rows(...)` 调用上补 compact=（与上游其它分页分支一致）。

2) 合集分组（vgroup:）：把多个虚拟媒体库收进同一个合集
   ------------------------------------------------------------------
   上游合集（collection.tmdb_collections）只能装媒体条目，装不了媒体库。
   这里引入「分组合集」：tmdb_id 形如 `vgroup:rank8` 的合集，其 metadata_json.members
   是一串媒体库 id；浏览这个合集时直接返回这些媒体库的 DTO（Type=BoxSet），
   点进某个成员仍是该库自己的正常列表（榜单型库照旧带占位条目）。
   - tmdb_id 非纯数字 → 上游合集刷新 `refresh_one()` 首行即 return，不会被误删。
   - 成员 DTO 不调用 `_library_dto()`：那会为每个榜单库统计占位条目（0.04~1.2s/库），
     合集内 8 个库就要多花数秒；这里只用 in_library_count，合集页秒开。
"""
from __future__ import annotations

import hashlib
import os
import re
import sys

ETKN_DIR = '/vol1/1000/docker/etkn'
ORIG = os.path.join(ETKN_DIR, 'media_server.py.orig')
PATCH = os.path.join(ETKN_DIR, 'media_server_patch.py')

# ---------------------------------------------------------------- 1. 榜单库分页优先
OLD_COMPACT = '''        actual = {int(row["id"]): row for row in _list_item_rows(
            request,
            catalog,
            list(dict.fromkeys(int(row["actual_item_id"]) for row in ranked if row.get("actual_item_id")))
        )}
        rows = [actual[int(row["actual_item_id"])] if row.get("actual_item_id") else row for row in ranked
                if not row.get("actual_item_id") or int(row["actual_item_id"]) in actual]'''

NEW_COMPACT = '''        # [etkn-fix12 2026-10-26] 榜单型虚拟库分页（勿删本标记：rebase 靠它判断是否已应用）：
        # 上游此处对整库全部已入库条目取全量行（compact=False），而过滤/排序/分页都在
        # Python 层之后才做 → 每翻一页都把整个库重取一遍。library-20「影史殿堂1001」
        # 实测 4.7s/页且与 Limit 无关；同样 898 条走紧凑行 0.74s、本页 60 条 0.06s。
        # 两层修法：①请求无行级过滤且排序字段能用轻量键还原时，先切出本页再取行；
        # ②其余情况退回「整库紧凑行」，比全量行快约 6 倍，DTO 语义不变。
        _page_first = _placeholder_page_first(
            request, catalog, ranked, start, limit, filters, image_filter,
            include_types, favorite_filter, person_filters, permissions, include_boxsets,
        )
        if _page_first is not None:
            _page_total, _page_entries = _page_first
            actual = {int(row["id"]): row for row in _list_item_rows(
                request,
                catalog,
                list(dict.fromkeys(int(row["actual_item_id"]) for row in _page_entries if row.get("actual_item_id"))),
                # [etkn-fix16 2026-11-05] 本页只取 limit 条，必须走全量行：
                # 紧凑行（compact_item_rows）把 metadata 裁成只剩图片路径、provider_ids 置空，
                # 导致 DTO 里 CommunityRating/DoubanRating/ProviderIds/Overview 全丢 →
                # 榜单库（19~26）在客户端不显示豆瓣评分。分页收益来自「只取本页」而不是「裁字段」。
                compact=False,
            )}
            rows = [actual[int(row["actual_item_id"])] if row.get("actual_item_id") else row for row in _page_entries
                    if not row.get("actual_item_id") or int(row["actual_item_id"]) in actual]
            _prime_episode_counts(request, catalog, resolved_user_id, rows)
            return {
                "Items": [_item_dto(row, request, resolved_user_id) for row in rows],
                "TotalRecordCount": _page_total,
                "StartIndex": start,
            }
        actual = {int(row["id"]): row for row in _list_item_rows(
            request,
            catalog,
            list(dict.fromkeys(int(row["actual_item_id"]) for row in ranked if row.get("actual_item_id"))),
            compact=bool(getattr(request.state, "media_compact_list", False)),
        )}
        rows = [actual[int(row["actual_item_id"])] if row.get("actual_item_id") else row for row in ranked
                if not row.get("actual_item_id") or int(row["actual_item_id"]) in actual]'''

PAGE_FIRST_HELPERS = '''# [etkn-fix12 2026-10-26] 榜单型虚拟库分页优先：先定位本页，再只取本页的行。
# 只保留能用轻量字段逐位复刻 DTO 排序键的字段；其余排序交回上游全量路径。
# datecreated/dateadded 的 DTO 值取自 media_ingested_at（版本表），无法用 items 复刻。
_PLACEHOLDER_SORT_FIELDS = {
    "sortname", "productionyear", "premieredate",
}
_PLACEHOLDER_BLOCKING_PARAMS = (
    "Filters", "Genres", "GenreIds", "Tags", "TagIds", "Studios", "StudioIds",
    "OfficialRatings", "Years", "IsFavorite", "IsPlayed", "IsUnplayed", "IsResumable",
    "PersonIds", "personIds", "Person", "SearchTerm", "searchTerm", "ListItemIds",
)


def _placeholder_light_rows(
    catalog: MediaServerCatalog, item_ids: list[int]
) -> dict[int, dict[str, Any]]:
    if not item_ids:
        return {}
    with catalog.database.connect() as connection:
        rows = connection.execute(
            """
            SELECT item.id, item.title, item.created_at, item.release_date
            FROM media.items item
            WHERE item.id = ANY(%s)
            """,
            (list(item_ids),),
        ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def _placeholder_sort_value(
    entry: dict[str, Any], light: dict[int, dict[str, Any]], field: str
) -> tuple[int, Any] | None:
    """按 DTO 语义生成排序键；返回 None 表示无法安全复刻（交回全量路径）。"""
    actual = entry.get("actual_item_id")
    record = light.get(int(actual)) if actual else None
    if actual and record is None:
        return None
    record = record or {}
    if field == "sortname":
        if actual:
            name = str(record.get("title") or "")
        else:
            name = str(entry.get("title") or "") + " · " + str(entry.get("subscription_label") or "")
        return (1 if name else 0, name.casefold())
    value = record.get("release_date") if actual else entry.get("release_date")
    if value is None:
        return None
    if field == "productionyear":
        year = _year(value)
        return (1, int(year)) if year else None
    return (1 if value else 0, str(_iso(value) or "").casefold())


def _placeholder_page_first(
    request: Request,
    catalog: MediaServerCatalog,
    ranked: list[dict[str, Any]],
    start: int,
    limit: int,
    filters: set[str],
    image_filter: Any,
    include_types: set[str],
    favorite_filter: str,
    person_filters: Any,
    permissions: dict[str, Any],
    include_boxsets: bool,
) -> tuple[int, list[dict[str, Any]]] | None:
    """返回 (总条数, 本页榜单条目)；无法安全分页时返回 None 交回上游全量路径。"""
    if not ranked or start < 0 or limit <= 0 or include_boxsets:
        return None
    if filters or image_filter or person_filters:
        return None
    if favorite_filter in {"true", "false", "1", "0", "yes", "no"}:
        return None
    if not permissions.get("is_administrator"):
        return None
    query = request.query_params
    for key in _PLACEHOLDER_BLOCKING_PARAMS:
        value = query.get(key)
        if value is not None and str(value).strip():
            return None
    if str(query.get("IsMissing") or "").strip().casefold() in {"false", "0", "no"}:
        return None
    sort_field = str(query.get("SortBy") or "").split(",", 1)[0].strip().casefold()
    if sort_field and sort_field not in _PLACEHOLDER_SORT_FIELDS:
        return None
    entries = list(ranked)
    if include_types and any(
        str(row.get("item_type") or "").casefold() not in include_types
        for row in entries
    ):
        return None
    actual_ids = [int(row["actual_item_id"]) for row in entries if row.get("actual_item_id")]
    light = _placeholder_light_rows(catalog, actual_ids)
    if len(light) != len(set(actual_ids)):
        return None  # 有指向不存在条目的行，上游会丢弃，交回全量路径保持一致
    ordered = entries
    if sort_field:
        decorated: list[tuple[tuple[int, Any], dict[str, Any]]] = []
        for row in entries:
            value = _placeholder_sort_value(row, light, sort_field)
            if value is None:
                return None
            decorated.append((value, row))
        try:
            decorated.sort(key=lambda pair: pair[0], reverse=_sort_descending(query.get("SortOrder")))
        except TypeError:
            return None
        ordered = [row for _, row in decorated]
    # 占位符恒排在真实条目之后（与上游 _sort_with_placeholders_last 一致）。
    ordered = [row for row in ordered if row.get("actual_item_id")] + [
        row for row in ordered if not row.get("actual_item_id")
    ]
    return (len(ordered), ordered[start:start + limit])


'''

# ---------------------------------------------------------------- 2. 合集分组
ANCHOR_LIST_ITEMS = '''def _list_items(request: Request, user_id: str | None = None) -> dict[str, Any]:
    from . import library_groups

    _normalize_media_query(request)
    query = request.query_params
    parent_id = str(query.get("ParentId") or "")'''

GROUP_HELPERS = '''# [etkn-fix 2026-10-26] 合集分组（vgroup:）：把多个虚拟媒体库收进同一个合集。
# 上游合集只能装条目；这里让 tmdb_id 以 vgroup: 开头的合集直接输出成员媒体库。
_VGROUP_PREFIX = "vgroup:"


def _collection_group_members(collection: dict[str, Any] | None) -> list[int]:
    if not collection:
        return []
    if not str(collection.get("tmdb_id") or "").startswith(_VGROUP_PREFIX):
        return []
    metadata = dict(collection.get("metadata_json") or {})
    raw = metadata.get("members") or metadata.get("libraries") or []
    values = raw if isinstance(raw, (list, tuple)) else [raw]
    result: list[int] = []
    for value in values:
        try:
            result.append(int(value))
        except (TypeError, ValueError):
            continue
    return result


def _is_collection_group(collection: dict[str, Any] | None) -> bool:
    return bool(collection) and str(collection.get("tmdb_id") or "").startswith(_VGROUP_PREFIX)


def _collection_group_counts(catalog: Any, library_ids: list[int]) -> dict[int, int]:
    """批量取成员库的已入库条目数（virtual_library.libraries.in_library_count）。

    MediaServerCatalog.library() 不带这个字段；8 个库合并成一次 SQL，合集页不额外往返。
    """
    if not library_ids:
        return {}
    try:
        with catalog.database.connect() as connection:
            rows = connection.execute(
                "SELECT id, in_library_count FROM virtual_library.libraries WHERE id = ANY(%s)",
                (library_ids,),
            ).fetchall()
    except Exception:  # 统计失败不该让合集页 500，退化为 0
        return {}
    counts: dict[int, int] = {}
    for row in rows:
        try:
            counts[int(row["id"])] = int(row.get("in_library_count") or 0)
        except (TypeError, ValueError):
            counts[int(row["id"])] = 0
    return counts


def _collection_group_child_dto(
    library: dict[str, Any],
    request: Request,
    user_id: str | None = None,
    item_count: int = 0,
    parent_id: str | None = None,
) -> dict[str, Any]:
    """分组里的成员媒体库：以 BoxSet 形态出现在合集内，点进去仍是该库的正常列表。

    不调用 _library_dto()：那个函数会为榜单型库统计占位条目（0.04~1.2s/库），
    合集内 8 个库就要多花数秒；这里只用 in_library_count，合集页秒开。
    """
    resolved_user_id = str(user_id or "").strip() or _request_user_id(request)
    protocol_id = _protocol_library_id(library["id"])
    try:
        item_count = int(item_count or 0)
    except (TypeError, ValueError):
        item_count = 0
    dto: dict[str, Any] = {
        "Name": str(library["name"]),
        "SortName": str(library["name"]),
        "ServerId": _server_id(request),
        "Id": protocol_id,
        "Type": "BoxSet",
        "CollectionType": "boxsets",
        "IsFolder": True,
        "ChildCount": item_count,
        "RecursiveItemCount": item_count,
        "CanDelete": False,
        "DateCreated": _iso(library.get("updated_at")),
        "UserData": _user_data(resolved_user_id, f"library-{library['id']}"),
        "PrimaryImageAspectRatio": 0.6667,
    }
    if parent_id:
        dto["ParentId"] = str(parent_id)
    cover_hash = str((library.get("definition_json") or {}).get("local_cover_hash") or "").strip()
    if cover_hash:
        dto["ImageTags"] = {"Primary": cover_hash}
        dto["PrimaryImageTag"] = cover_hash
        dto["PrimaryImageItemId"] = protocol_id
    return dto


def _collection_group_for_library(library_id: int) -> dict[str, Any] | None:
    """该媒体库所属的分组合集（让 library-N 详情与合集内卡片保持同一形态）。

    [2026-11-15] 上游 1.3.99+ 的原生分组（virtual_library.library_groups + libraries.group_id）
    能承载同一批库。一旦该库已归入原生分组，本补丁必须让路：否则成员库详情被渲染成
    Type=BoxSet/CollectionType=boxsets，客户端按「合集」语义要 BoxSet 子项，而服务端的
    BoxSet 列表不按父库过滤（catalog.collections() 全库），于是每张榜单库卡点进去都是
    全库 1062 个合集——就是「榜单合集里的媒体库打开后内容都一样」。
    """
    try:
        library = _catalog().library(int(library_id))
    except Exception:
        library = None
    if library and library.get("group_id") is not None:
        return None
    try:
        collections = _catalog().collections()
    except Exception:
        return None
    for collection in collections:
        if int(library_id) in _collection_group_members(collection):
            return collection
    return None


def _collection_group_media_request(request: Request) -> tuple[bool, bool]:
    """(是否要媒体条目, 是否递归)。Emby 语义：BoxSet 里的片子要递归展开才可见。"""
    include = {
        value.strip().casefold()
        for value in str(request.query_params.get("IncludeItemTypes") or "").split(",")
        if value.strip()
    }
    if not include:
        return False, False
    folderish = {"boxset", "collectionfolder", "folder", "userview", "photoalbum"}
    mediatypes = {"movie", "series", "episode", "season", "video", "musicvideo", "trailer"}
    if include & folderish or not include & mediatypes:
        return False, False
    recursive = str(request.query_params.get("Recursive") or "").strip().casefold() in {"true", "1", "yes"}
    return True, recursive


def _collection_group_flat_items(
    request: Request,
    collection: dict[str, Any],
    user_id: str | None,
) -> dict[str, Any]:
    """把分组内各成员库的条目摊平成一层（对应 Emby 里 BoxSet 的递归查询）。

    1672 条全量建 DTO 要 8~12s，客户端只取 20 条 → 先用轻量 SQL 排序定位本页，
    再只为本页 20 条取版本与 DTO；带筛选/搜索时退回全量路径保证语义一致。
    """
    catalog = _catalog()
    resolved_user_id = _request_user_id(request, user_id)
    ids: list[int] = []
    seen: set[int] = set()
    for library_id in _collection_group_members(collection):
        try:
            item_ids = catalog.library_item_ids(int(library_id), user_id=resolved_user_id)
        except Exception:
            continue
        for item_id in item_ids:
            try:
                value = int(item_id)
            except (TypeError, ValueError):
                continue
            if value in seen:
                continue
            seen.add(value)
            ids.append(value)
    if not ids:
        return {"Items": [], "TotalRecordCount": 0, "StartIndex": 0}
    query = request.query_params
    start = _bounded_int(query.get("StartIndex"), 0, minimum=0, maximum=1_000_000)
    limit = _bounded_int(query.get("Limit"), DEFAULT_LIST_LIMIT, minimum=0, maximum=1000)
    sort_parts = [part.strip().casefold() for part in str(query.get("SortBy") or "").split(",") if part.strip()]
    # 上游只按第一个排序字段排（media_server.py.orig:3454 _sort_key(…, sort_by)），这里对齐同一语义。
    sort_field = sort_parts[0] if sort_parts else ""
    # DateCreated 在上游 _item_dto 里是 "media_ingested_at or created_at"（剧集还要取最新一集），
    # 轻量投影无法等价复刻 → 只对这里三种逐值可复刻的字段走快路径，其余退回全量路径。
    simple_sort = sort_field in {"premieredate", "sortname", "productionyear"}
    blocked = any(
        str(query.get(key) or "").strip()
        for key in (
            "Filters", "Genres", "GenreIds", "Tags", "TagIds", "Studios", "StudioIds",
            "OfficialRatings", "Years", "IsFavorite", "IsPlayed", "IsUnplayed", "IsResumable",
            "PersonIds", "Person", "SearchTerm", "ListItemIds",
        )
    )
    if not simple_sort or blocked:
        rows = catalog.list_item_rows(ids, include_versions=False)
        return _paged_items_response(request, rows, resolved_user_id, include_collections=False)
    with catalog.database.connect() as connection:
        light_rows = connection.execute(
            """
            SELECT item.id, item.title, item.release_date, item.created_at,
                   jsonb_strip_nulls(jsonb_build_object(
                       'title', metadata.payload -> 'title',
                       'name', metadata.payload -> 'name',
                       'year', metadata.payload -> 'year',
                       'release_date', metadata.payload -> 'release_date',
                       'first_air_date', metadata.payload -> 'first_air_date',
                       'air_date', metadata.payload -> 'air_date'
                   )) AS metadata
            FROM media.items item
            LEFT JOIN media.item_metadata metadata
              ON metadata.item_id = item.id AND metadata.is_current
            WHERE item.id = ANY(%s)
            """,
            (ids,),
        ).fetchall()
    light = {int(row["id"]): dict(row) for row in light_rows}
    reverse = _sort_descending(query.get("SortOrder"))

    def sort_value(item_id: int) -> tuple[int, Any]:
        # 与 _ranking_lightweight_sort_key 同源：键值必须和 _item_dto 派生结果一致，
        # 否则分页边界会和全量路径错位。
        record = light.get(item_id) or {"id": item_id}
        metadata = dict(record.get("metadata") or {})
        release_date = (
            record.get("release_date")
            or metadata.get("release_date")
            or metadata.get("first_air_date")
            or metadata.get("air_date")
        )
        name = str(record.get("title") or metadata.get("title") or metadata.get("name") or item_id)
        return _sort_key(
            {
                "Name": name,
                "ProductionYear": _year(release_date) or _number(metadata.get("year")),
                "PremiereDate": _iso(release_date),
                "DateCreated": _iso(record.get("created_at")),
            },
            sort_field,
        )

    try:
        ordered = sorted(ids, key=sort_value, reverse=reverse)
    except TypeError:
        rows = catalog.list_item_rows(ids, include_versions=False)
        return _paged_items_response(request, rows, resolved_user_id, include_collections=False)
    total = len(ordered)
    page_ids = ordered[start:start + limit]
    if not page_ids:
        return {"Items": [], "TotalRecordCount": total, "StartIndex": start}
    rows = catalog.list_item_rows(page_ids, include_versions=_fields_include_sources(request))
    page_order = {value: index for index, value in enumerate(page_ids)}
    rows = [row for row in rows if int(row["id"]) in page_order]
    rows.sort(key=lambda row: page_order[int(row["id"])])
    rows = _filter_rows(request, rows, resolved_user_id)
    _prime_episode_counts(request, catalog, resolved_user_id, rows)
    request.state.media_image_types = catalog.image_types([int(row["id"]) for row in rows])
    request.state.media_episode_screenshot_ids = catalog.screenshot_item_ids(
        [int(row["id"]) for row in rows if row.get("item_type") == "episode"]
    )
    return {
        "Items": [_item_dto(row, request, resolved_user_id) for row in rows],
        "TotalRecordCount": total,
        "StartIndex": start,
    }


def _collection_group_items(
    request: Request,
    collection: dict[str, Any],
    user_id: str | None = None,
) -> dict[str, Any]:
    catalog = _catalog()
    wants_media, recursive = _collection_group_media_request(request)
    if wants_media:
        # 客户端按「媒体库/合集」常规语义要片子时，BoxSet 空壳它认不出来，
        # 只会得到一堆没有流地址的条目，随后在详情页报 No active route。
        if recursive:
            return _collection_group_flat_items(request, collection, user_id)
        return {"Items": [], "TotalRecordCount": 0, "StartIndex": 0}
    member_ids = _collection_group_members(collection)
    counts = _collection_group_counts(catalog, member_ids)
    parent_id = f"collection-{int(collection['id'])}"
    items: list[dict[str, Any]] = []
    for library_id in member_ids:
        library = catalog.library(library_id)
        if library is None:
            continue
        items.append(
            _collection_group_child_dto(
                library, request, user_id,
                item_count=counts.get(int(library_id), 0),
                parent_id=parent_id,
            )
        )
    return {"Items": items, "TotalRecordCount": len(items), "StartIndex": 0}


'''

NEW_LIST_ITEMS = GROUP_HELPERS + PAGE_FIRST_HELPERS + ANCHOR_LIST_ITEMS + '''
    # [etkn-fix 2026-10-26] 合集分组：成员是媒体库，不是条目 → 直接返回成员库列表
    if parent_id.startswith("collection-"):
        group = _catalog().collection(_numeric_id(parent_id, "collection-"))
        if _is_collection_group(group):
            return _collection_group_items(request, group, user_id)'''

ANCHOR_COLLECTION_DTO = '''    metadata = dict(collection.get("metadata_json") or {})
    member_count = _number(collection.get("member_count"))
    if member_count is None:'''

NEW_COLLECTION_DTO = '''    metadata = dict(collection.get("metadata_json") or {})
    # [etkn-fix 2026-10-26] 合集分组：成员数量 = 成员媒体库个数
    group_members = _collection_group_members(collection)
    member_count: int | None = len(group_members) if group_members else _number(collection.get("member_count"))
    if member_count is None:'''

ANCHOR_COLLECTION_TYPE = '        "CollectionType": "movies",\n'
NEW_COLLECTION_TYPE = '        "CollectionType": "boxsets" if group_members else "movies",\n'

ANCHOR_GET_ITEM_LIB = """        return _library_dto(library, request)
    if item_id.startswith("collection-"):"""

NEW_GET_ITEM_LIB = """        # [etkn-fix16 2026-10-27] 分组合集内的成员库：详情与合集内卡片同一形态（BoxSet）
        group = _collection_group_for_library(int(library["id"]))
        if group is not None:
            counts = _collection_group_counts(_catalog(), [int(library["id"])])
            return _collection_group_child_dto(
                library, request, user_id,
                item_count=counts.get(int(library["id"]), 0),
                parent_id=f"collection-{int(group['id'])}",
            )
        return _library_dto(library, request)
    if item_id.startswith("collection-"):"""


ANCHOR_CHILDREN = '''    if item_id.startswith("collection-"):
        collection_id = _numeric_id(item_id, "collection-")
        if not _collection_allowed(request, collection_id, resolved_user_id):
            raise HTTPException(status_code=404, detail="合集不存在")
        return _paged_items_response(request, catalog.collection_members(collection_id), resolved_user_id)'''

NEW_CHILDREN = '''    if item_id.startswith("collection-"):
        collection_id = _numeric_id(item_id, "collection-")
        if not _collection_allowed(request, collection_id, resolved_user_id):
            raise HTTPException(status_code=404, detail="合集不存在")
        # [etkn-fix 2026-10-26] 合集分组：/Items/{id}/Children 也要返回成员媒体库
        group = catalog.collection(collection_id)
        if _is_collection_group(group):
            return _collection_group_items(request, group, resolved_user_id)
        return _paged_items_response(request, catalog.collection_members(collection_id), resolved_user_id)'''

# ---------------------------------------------------------------- 3. 合集分组：媒体库列表收起
# [etkn-fix13 2026-10-26] vgroup 合集的成员媒体库不再散落在「媒体库」列表里，
# 只保留一张合集卡片（Id=collection-N，点进去仍是成员库）；library-N 直达路径完全不变。
ANCHOR_USER_VIEWS = '''def user_views(request: Request, user_id: str) -> dict[str, Any]:
    from . import library_groups

    libraries = library_groups.view_entries(request, _visible_libraries(request, user_id))
    start = _bounded_int(request.query_params.get("StartIndex"), 0, minimum=0, maximum=1_000_000)
    limit_value = request.query_params.get("Limit")
    limit = (
        len(libraries)
        if limit_value is None
        else _bounded_int(limit_value, DEFAULT_LIST_LIMIT, minimum=0, maximum=1000)
    )
    items = []
    for library in libraries[start:start + limit]:
        dto = library_groups.view_dto(library, request, user_id)
        items.append(dto)
    return {"Items": items, "TotalRecordCount": len(libraries), "StartIndex": start}'''

NEW_USER_VIEWS = '''# [etkn-fix13 2026-10-26] 合集分组：vgroup 合集的成员库从「媒体库」列表收起，
# 只留一张合集卡片；点进去（ParentId=collection-N）仍是这些库，library-N 路径不变。
_GROUP_CARD_FLAG = "_vgroup_card"


def _collection_group_view_entries() -> list[tuple[dict[str, Any], list[int]]]:
    try:
        collections = _catalog().collections()
    except Exception:
        return []
    entries: list[tuple[dict[str, Any], list[int]]] = []
    for collection in collections:
        members = _collection_group_members(collection)
        if members:
            entries.append((collection, members))
    return entries


def _collection_group_view_card(
    collection: dict[str, Any],
    member_ids: list[int],
    libraries: list[dict[str, Any]],
    request: Request,
) -> dict[str, Any]:
    metadata = dict(collection.get("metadata_json") or {})
    wanted = {int(value) for value in member_ids}
    total = 0
    for library in libraries:
        if int(library["id"]) not in wanted:
            continue
        try:
            total += int(library.get("in_library_count") or 0)
        except (TypeError, ValueError):
            continue
    collection_id = int(collection["id"])
    protocol_id = f"collection-{collection_id}"
    card: dict[str, Any] = {
        "Name": str(collection.get("name") or "合集"),
        "SortName": str(collection.get("name") or "合集"),
        "ServerId": _server_id(request),
        "Id": protocol_id,
        "Type": "CollectionFolder",
        "CollectionType": "boxsets",
        "IsFolder": True,
        "ChildCount": len(wanted),
        "RecursiveItemCount": total,
        "CanDelete": False,
        "DateCreated": _iso(collection.get("updated_at")),
        "UserData": _user_data(_request_user_id(request), protocol_id),
        _GROUP_CARD_FLAG: True,
    }
    poster = metadata.get("poster_path")
    if poster:
        tag = f"collection-{collection_id}-{str(collection.get('updated_at') or '')}"
        card["ImageTags"] = {"Primary": tag}
        card["PrimaryImageTag"] = tag
        card["PrimaryImageItemId"] = protocol_id
    return card


def _apply_collection_groups(
    request: Request,
    libraries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    # [2026-11-15] 上游 1.3.99+ 原生分组（virtual_library.library_groups + libraries.group_id）
    # 已经接管同一批库：成员库照旧从「媒体库」列表收起，卡片由上游原生分组自己出，
    # 本补丁不再重复出卡，也不再改写成员库 DTO（改写会让详情变成 BoxSet，
    # 客户端按合集语义要 BoxSet 子项，服务端 BoxSet 列表不按父库过滤 → 每个库都一样）。
    native_members = {
        int(library["id"]) for library in libraries
        if library.get("group_id") is not None
    }
    groups = _collection_group_view_entries()
    by_id = {int(library["id"]): library for library in libraries}
    cards: dict[int, dict[str, Any]] = {}
    hidden: set[int] = set(native_members)
    for collection, members in groups:
        present = [value for value in members if value in by_id and value not in native_members]
        if not present:
            continue
        wanted = set(present)
        hidden.update(present)
        anchor_index = next(
            index for index, library in enumerate(libraries) if int(library["id"]) in wanted
        )
        cards[anchor_index] = _collection_group_view_card(collection, present, libraries, request)
    result: list[dict[str, Any]] = []
    for index, library in enumerate(libraries):
        card = cards.get(index)
        if card is not None:
            result.append(card)
        if int(library["id"]) not in hidden:
            result.append(library)
    return result


def user_views(request: Request, user_id: str) -> dict[str, Any]:
    libraries = _apply_collection_groups(request, _visible_libraries(request, user_id))
    start = _bounded_int(request.query_params.get("StartIndex"), 0, minimum=0, maximum=1_000_000)
    limit_value = request.query_params.get("Limit")
    limit = (
        len(libraries)
        if limit_value is None
        else _bounded_int(limit_value, DEFAULT_LIST_LIMIT, minimum=0, maximum=1000)
    )
    items = []
    for library in libraries[start:start + limit]:
        if library.get(_GROUP_CARD_FLAG):
            items.append({key: value for key, value in library.items() if key != _GROUP_CARD_FLAG})
            continue
        dto = _library_dto(library, request)
        items.append(dto)
    return {"Items": items, "TotalRecordCount": len(libraries), "StartIndex": start}'''

ANCHOR_MEDIA_FOLDERS = '''def library_media_folders(request: Request) -> dict[str, Any]:
    """Return visible libraries using the Emby MediaFolders envelope."""
    from . import library_groups

    user_id = _request_user_id(request)
    libraries = library_groups.view_entries(request, _visible_libraries(request, user_id))
    start = _bounded_int(request.query_params.get("StartIndex"), 0, minimum=0, maximum=1_000_000)
    limit = _bounded_int(request.query_params.get("Limit"), 100, minimum=0, maximum=1000)
    items = []
    for library in libraries[start:start + limit]:
        if "_group_libraries" in library:
            items.append({**library_groups.view_dto(library, request, user_id), "RefreshStatus": "Idle", "RefreshProgress": 0})
            continue
        items.append({
            "Id": _protocol_library_id(library["id"]),
            "Name": str(library["name"]),
            "Type": "CollectionFolder",
            "CollectionType": _library_collection_type(library),
            "RefreshStatus": "Idle",
            "RefreshProgress": 0,
        })
    return {"Items": items, "TotalRecordCount": len(libraries), "StartIndex": start}'''

NEW_MEDIA_FOLDERS = '''def library_media_folders(request: Request) -> dict[str, Any]:
    """Return visible libraries using the Emby MediaFolders envelope."""
    # [etkn-fix13 2026-10-26] 与 user_views 同口径：成员库收起、只留合集卡片
    libraries = _apply_collection_groups(request, _visible_libraries(request, _request_user_id(request)))
    start = _bounded_int(request.query_params.get("StartIndex"), 0, minimum=0, maximum=1_000_000)
    limit = _bounded_int(request.query_params.get("Limit"), 100, minimum=0, maximum=1000)
    items = []
    for library in libraries[start:start + limit]:
        if library.get(_GROUP_CARD_FLAG):
            items.append({
                "Id": library["Id"],
                "Name": library["Name"],
                "Type": "CollectionFolder",
                "CollectionType": "boxsets",
                "RefreshStatus": "Idle",
                "RefreshProgress": 0,
            })
            continue
        items.append({
            "Id": _protocol_library_id(library["id"]),
            "Name": str(library["name"]),
            "Type": "CollectionFolder",
            "CollectionType": _library_collection_type(library),
            "RefreshStatus": "Idle",
            "RefreshProgress": 0,
        })
    return {"Items": items, "TotalRecordCount": len(libraries), "StartIndex": start}'''

MARKERS = (
    'etkn-fix 2026-10-26',
    'etkn-fix12 2026-10-26',
    'etkn-fix13 2026-10-26',
    'etkn-fix16 2026-11-05',
    'compact=bool(getattr(request.state, "media_compact_list", False)),',
    '_collection_group_items',
    '_collection_group_child_dto',
    '_VGROUP_PREFIX',
)

# ---------------------------------------------------------------- 3. 批量 ChildCount 预热
ANCHOR_PRIME_COUNTS = 'def _prime_episode_counts(\n    request: Request,\n    catalog: MediaServerCatalog,\n    user_id: str,\n    rows: list[dict[str, Any]],\n) -> None:\n    parent_ids = ['

NEW_PRIME_COUNTS = 'def _prime_child_counts(\n    request: Request,\n    catalog: MediaServerCatalog,\n    user_id: str,\n    rows: list[dict[str, Any]],\n) -> None:\n    """[etkn-fix14 2026-10-26] 批量预热 ChildCount（勿删本标记）。\n\n    上游 DTO 对每个 Series/Season 都单独调用 catalog.child_count()，而那条 SQL 里\n    `EXISTS(直接版本) OR EXISTS(孙子版本)` 只要遇到「有子项但没有 current 版本」的条目，\n    规划器就把第二个 EXISTS 变成 hashed SubPlan（media.items 37 万 × item_versions 35 万\n    全量 Hash Join），单次 0.3~0.43s；列表 60 条就要 22.6s（library-21 实测 25~40s 打不开）。\n    这里一次性用逐行索引探测的 LATERAL + UNION 批量算完（60 个父节点 0.07s，语义一致），\n    结果放 request.state.media_child_counts，DTO 直接查表。\n    """\n    parent_ids: list[int] = []\n    for row in rows:\n        if str(row.get("item_type") or "").casefold() not in {"series", "season"}:\n            continue\n        if row.get("placeholder_id") or row.get("episode_placeholder_kind"):\n            continue\n        try:\n            parent_ids.append(int(row["id"]))\n        except (TypeError, ValueError):\n            continue\n    if not parent_ids:\n        request.state.media_child_counts = {}\n        return\n    sql = """\n        WITH pids AS (SELECT unnest(%s::bigint[]) AS pid),\n        direct AS (\n            SELECT p.pid AS pid, i.id AS cid\n            FROM pids p\n            JOIN media.items i ON i.parent_id = p.pid AND i.is_active\n            JOIN LATERAL (\n                SELECT 1 FROM media.item_versions v\n                WHERE v.item_id = i.id AND v.is_current\n                  AND (NULLIF(btrim(v.path), \'\') IS NOT NULL OR NULLIF(btrim(v.pick_code), \'\') IS NOT NULL)\n                LIMIT 1) ok ON true\n        ),\n        nested AS (\n            SELECT p.pid AS pid, i.id AS cid\n            FROM pids p\n            JOIN media.items i ON i.parent_id = p.pid AND i.is_active\n            JOIN LATERAL (\n                SELECT 1 FROM media.items c\n                JOIN LATERAL (\n                    SELECT 1 FROM media.item_versions v\n                    WHERE v.item_id = c.id AND v.is_current\n                      AND (NULLIF(btrim(v.path), \'\') IS NOT NULL OR NULLIF(btrim(v.pick_code), \'\') IS NOT NULL)\n                    LIMIT 1) ok2 ON true\n                WHERE c.parent_id = i.id AND c.is_active\n                LIMIT 1) ok ON true\n        )\n        SELECT pid, count(*) AS total\n        FROM (SELECT pid, cid FROM direct UNION SELECT pid, cid FROM nested) t\n        GROUP BY pid\n    """\n    try:\n        with catalog.database.connect() as connection:\n            result = connection.execute(sql, (list(dict.fromkeys(parent_ids)),)).fetchall()\n    except Exception:  # noqa: BLE001 - 预热失败退回逐条 child_count，不影响正确性\n        request.state.media_child_counts = {}\n        return\n    request.state.media_child_counts = {\n        int(row["pid"]): int(row["total"]) for row in result\n    }\n\n\ndef _prime_episode_counts(\n    request: Request,\n    catalog: MediaServerCatalog,\n    user_id: str,\n    rows: list[dict[str, Any]],\n) -> None:\n    _prime_child_counts(request, catalog, user_id, rows)  # [etkn-fix14] 批量 ChildCount 预热\n    parent_ids = ['

ANCHOR_DTO_CHILD_COUNT = '        child_count = _catalog().child_count(int(row["id"]))'

NEW_DTO_CHILD_COUNT = '        # [etkn-fix14 2026-10-26] 列表页已批量预热 ChildCount（上游逐条 child_count 有规划器悬崖，\n        # 60 条要 22.6s）；预热表里没有时才回退单条查询（详情页等未预热场景）。\n        _child_counts = getattr(request.state, "media_child_counts", None)\n        child_count = _child_counts.get(int(row["id"])) if _child_counts else None\n        if child_count is None:\n            child_count = _catalog().child_count(int(row["id"]))'

# ---------------------------------------------------------------- 4. 全库 facet 单飞 + 紧凑投影（fix17）
# 2026-10-29 21:56 ETKN 冲到 18.5G 触发全局 OOM、整机卡死。py-spy 抓到的栈是
#   facet_items → _facet_rows → _visible_library_rows → list_item_rows → items_by_ids
# 即五个 facet 端点（Genres/Studios/Tags/Years/OfficialRatings）各自把 38.6 万条完整媒体行装进内存，
# 且缓存/锁的 key 是「整库 id 拼成的 tuple」，_facet_value_locks 只增不减。
ANCHOR_FACET_CACHE = '''def _cached_facet_values(rows: list[dict[str, Any]], request: Request) -> dict[str, list[str]]:
    """Reuse facet computation for concurrent Hills requests over one library."""
    key = (rating_index_revision(Database()), *(int(row["id"]) for row in rows))
    now = time.monotonic()
    with _facet_value_cache_lock:
        cached = _facet_value_cache.get(key)
        if cached and cached[0] > now:
            return {name: list(values) for name, values in cached[1].items()}
        resolve_lock = _facet_value_locks.setdefault(tuple(int(row["id"]) for row in rows), threading.Lock())
    with resolve_lock:
        with _facet_value_cache_lock:
            cached = _facet_value_cache.get(key)
            if cached and cached[0] > time.monotonic():
                return {name: list(values) for name, values in cached[1].items()}
        values = _facet_values(rows, request)
        with _facet_value_cache_lock:
            _facet_value_cache[key] = (time.monotonic() + _FACET_VALUE_CACHE_TTL_SECONDS, values)
            if len(_facet_value_cache) > 64:
                expired = [cache_key for cache_key, (expires_at, _) in _facet_value_cache.items() if expires_at <= time.monotonic()]
                for cache_key in expired:
                    _facet_value_cache.pop(cache_key, None)
        return {name: list(items) for name, items in values.items()}
'''

NEW_FACET_CACHE = '''_FACET_SNAPSHOT_TTL_SECONDS = 900.0  # [etkn-fix19 2026-10-31] 全库 facet 值缓存时长（配合后台预热，正常情况不再有用户等冷算）
_FACET_PREFETCH_MIN_INTERVAL_SECONDS = 180.0  # [etkn-fix19] 后台预热最短间隔
_FACET_PREFETCH_MAX_THREADS = 4  # [etkn-fix19] 预热线程上限，防止按用户范围无限增长
_facet_snapshot_lock = threading.Lock()
_facet_compute_lock = threading.Lock()
_facet_snapshot_key: tuple[Any, ...] | None = None
_facet_snapshot_values: dict[str, list[str]] | None = None
_facet_snapshot_expires = 0.0
_facet_prefetch_threads: dict[Any, threading.Thread] = {}
_facet_prefetch_stop: dict[Any, bool] = {}
_facet_prefetch_events: dict[Any, threading.Event] = {}
_facet_desired_key: tuple[Any, ...] | None = None
_facet_last_compute_at = 0.0


def _facet_permission_scope(permissions: dict[str, Any]) -> tuple[Any, ...]:
    """[etkn-fix17 2026-10-29] 用户可见范围指纹：范围不同，facet 结果不同。"""
    return (
        bool(permissions.get("is_administrator")),
        bool(permissions.get("library_scope_restricted")),
        tuple(sorted(int(value) for value in (permissions.get("allowed_library_ids") or ()))),
        str(permissions.get("max_rating_label") or ""),
    )


def _facet_copy() -> dict[str, list[str]]:
    values = _facet_snapshot_values or {}
    return {name: list(items) for name, items in values.items()}


def _facet_compute(
    request: Request,
    catalog: MediaServerCatalog,
    permissions: dict[str, Any],
    user_id: str,
    key: tuple[Any, ...],
) -> dict[str, list[str]]:
    global _facet_snapshot_key, _facet_snapshot_values, _facet_snapshot_expires, _facet_last_compute_at
    started = time.monotonic()
    rows = _facet_rows(request, catalog, permissions, user_id)
    try:
        row_count = len(rows)
        values = {name: list(items) for name, items in _facet_values(rows, request).items()}
    finally:
        del rows
    _facet_snapshot_key = key
    _facet_snapshot_values = values
    _facet_snapshot_expires = time.monotonic() + _FACET_SNAPSHOT_TTL_SECONDS
    _facet_last_compute_at = time.monotonic()
    logger.warning(
        "facet 全库计算完成 rows=%s 耗时=%.2fs 值数=%s",
        row_count,
        time.monotonic() - started,
        sum(len(value) for value in values.values()),
    )
    return values


def _ensure_facet_prefetch(
    key: tuple[Any, ...],
    request: Request,
    catalog: MediaServerCatalog,
    permissions: dict[str, Any],
    user_id: str,
    immediate: bool = False,
) -> None:
    """[etkn-fix19 2026-10-31] 后台刷新：immediate=立刻补算（返回过旧值时用），否则按 TTL 周期重算。

    冷算（全库约 36 秒）只发生在后台线程，用户请求永远只读缓存；
    线程异常只打日志不抛出，绝不拖累主路径。
    """
    global _facet_desired_key
    try:
        _facet_desired_key = key
        if immediate and time.monotonic() - _facet_last_compute_at < _FACET_PREFETCH_MIN_INTERVAL_SECONDS:
            immediate = False  # 上游修订号抖动时不反复重算
        thread = _facet_prefetch_threads.get(key)
        if thread is not None and thread.is_alive():
            if immediate:
                _facet_prefetch_events[key].set()
            return
        for old_key, old_thread in list(_facet_prefetch_threads.items()):
            if not old_thread.is_alive():
                _facet_prefetch_threads.pop(old_key, None)
                _facet_prefetch_stop.pop(old_key, None)
                _facet_prefetch_events.pop(old_key, None)
        if len(_facet_prefetch_threads) >= _FACET_PREFETCH_MAX_THREADS:
            return
        _facet_prefetch_stop[key] = False
        event = threading.Event()
        _facet_prefetch_events[key] = event

        def _loop() -> None:
            interval = max(_FACET_PREFETCH_MIN_INTERVAL_SECONDS, _FACET_SNAPSHOT_TTL_SECONDS * 0.5)
            delay = 0.0 if immediate else interval
            while True:
                deadline = time.monotonic() + delay
                while True:
                    if _facet_desired_key != key:
                        return
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    if event.wait(min(5.0, remaining)):
                        event.clear()
                        break
                if _facet_desired_key != key:
                    return
                with _facet_compute_lock:
                    try:
                        _facet_compute(request, catalog, permissions, user_id, key)
                    except Exception as exc:
                        logger.warning("facet 后台刷新失败，停止该范围的刷新：%s", exc)
                        return
                delay = interval

        thread = threading.Thread(target=_loop, name="facet-prefetch", daemon=True)
        _facet_prefetch_threads[key] = thread
        thread.start()
    except Exception as exc:
        logger.warning("facet 刷新线程启动失败：%s", exc)


def _cached_facet_values(
    request: Request,
    catalog: MediaServerCatalog,
    permissions: dict[str, Any],
    user_id: str,
) -> dict[str, list[str]]:
    """[etkn-fix17 2026-10-29 / fix19 2026-10-31] 全库 facet：单飞计算 + 值级缓存 + 后台预热（勿删本标记）。

    上游有两个结构性毛病：
    1) 缓存 key 与锁 key 都是「整库全部条目 id」拼成的 tuple（38.6 万条，单个 key 十几 MB），
       且 `_facet_value_locks` 只增不减，每请求永久泄漏一份 id tuple；
    2) 五个 facet 端点各自 `_facet_rows()` 重装整库行，并发时内存相乘。
    fix17：按（rating 修订号 + 用户可见范围）缓存算好的值，一把锁做单飞，任何时刻最多一份全库行在内存。
    fix19：缓存 15 分钟 + 后台线程定时重算（过期后先返回旧值再刷新），用户请求不再命中 30 秒级冷算。
    """
    key = (rating_index_revision(Database()), _facet_permission_scope(permissions))
    with _facet_snapshot_lock:
        if _facet_snapshot_key is not None and _facet_snapshot_key[1] == key[1] and _facet_snapshot_values is not None:
            if _facet_snapshot_key == key and _facet_snapshot_expires > time.monotonic():
                return _facet_copy()
            # 仅 rating 修订号变了（或已过期）：先给旧值，后台重算，绝不让请求等 30 秒级冷算。
            stale = _facet_copy()
        else:
            stale = None
    if stale is not None:
        _ensure_facet_prefetch(key, request, catalog, permissions, user_id, immediate=True)
        return stale
    with _facet_compute_lock:
        with _facet_snapshot_lock:
            if _facet_snapshot_key is not None and _facet_snapshot_key[1] == key[1] and _facet_snapshot_values is not None:
                if _facet_snapshot_key == key and _facet_snapshot_expires > time.monotonic():
                    return _facet_copy()
                stale = _facet_copy()
            else:
                stale = None
        if stale is not None:
            _ensure_facet_prefetch(key, request, catalog, permissions, user_id, immediate=True)
            return stale
        values = _facet_compute(request, catalog, permissions, user_id, key)
        result = {name: list(items) for name, items in values.items()}
    _ensure_facet_prefetch(key, request, catalog, permissions, user_id)
    return result
'''

ANCHOR_FACET_ITEMS_CALL = '''    rows = _facet_rows(request, catalog, permissions, user_id)
    values = _cached_facet_values(rows, request).get(facet, [])'''

NEW_FACET_ITEMS_CALL = '''    values = _cached_facet_values(request, catalog, permissions, user_id).get(facet, [])  # [etkn-fix17 2026-10-29]'''

ANCHOR_ITEM_FILTERS_CALL = '''    rows = _facet_rows(request, catalog, permissions, user_id)
    values = _cached_facet_values(rows, request)
    return values'''

NEW_ITEM_FILTERS_CALL = '''    return _cached_facet_values(request, catalog, permissions, user_id)  # [etkn-fix17 2026-10-29]'''

ANCHOR_FACET_ELSE = '''    else:
        rows = _visible_library_rows(
            request, catalog, permissions, user_id, include_filters=True
        )
        rows_scoped_by_library_permissions = True'''

NEW_FACET_ELSE = '''    else:
        # [etkn-fix17 2026-10-29] 全库 facet 改走紧凑投影：只取 facet 需要的字段，
        # 不再把 38.6 万条完整媒体行装进内存（实测单次 3~4G，五端点并发直接顶到 18.5G）。
        rows = _facet_library_rows(request, catalog, permissions, user_id)
        rows_scoped_by_library_permissions = True'''

ANCHOR_VISIBLE_ROWS_TAIL = '''    unique_ids = list(dict.fromkeys(item_ids))
    return [
        row for row in catalog.list_item_rows(unique_ids)
        if str(row.get("item_type") or "").casefold() in browse_types
    ]'''

NEW_VISIBLE_ROWS_TAIL = ANCHOR_VISIBLE_ROWS_TAIL + '''


def _facet_library_rows(
    request: Request,
    catalog: MediaServerCatalog,
    permissions: dict[str, Any],
    user_id: str,
) -> list[dict[str, Any]]:
    """[etkn-fix17 2026-10-29] facet 专用行集：与 _visible_library_rows 同口径，但走紧凑投影。"""
    item_ids: list[int] = []
    browse_types: set[str] = set()
    allowed = permissions.get("allowed_library_ids") or set()
    for library in _request_libraries(request, catalog):
        library_id = int(library["id"])
        if (
            not permissions.get("is_administrator")
            and permissions.get("library_scope_restricted")
            and library_id not in allowed
        ):
            continue
        item_ids.extend(_permitted_library_item_ids(
            request,
            catalog,
            library_id,
            user_id,
            permissions,
        ))
        browse_types.update(_library_browse_types(library))
    unique_ids = list(dict.fromkeys(item_ids))
    return [
        row for row in catalog.items_for_facets(unique_ids)
        if str(row.get("item_type") or "").casefold() in browse_types
    ]'''

ANCHOR_GROUP_COVER = '''    if _is_library_id(_catalog(), item_id):
        library_id = _numeric_id(item_id, "library-")
        if image_type.casefold() != "primary":'''

NEW_GROUP_COVER = '''    if item_id.startswith("library-group-"):
        # [etkn-fix20 2026-11-05] 自建「库分组」封面：上游 view_dto 已把 cover hash 写进
        # ImageTags，但 /Items/library-group-N/Images/Primary 落进 _numeric_id("item-") 直接
        # 404，etkn-TV 因此显示空白卡片。这里按 library_groups.local_cover_hash 直接出图。
        if image_type.casefold() != "primary":
            raise HTTPException(status_code=404, detail="图片不存在")
        try:
            group_id = int(str(item_id).split("library-group-", 1)[1].split("/")[0])
        except (IndexError, ValueError):
            raise HTTPException(status_code=404, detail="图片不存在")
        group_row = None
        try:
            with Database().connect() as connection:
                group_row = connection.execute(
                    "SELECT local_cover_hash FROM virtual_library.library_groups WHERE id = %s",
                    (group_id,),
                ).fetchone()
        except Exception:  # noqa: BLE001 - 表缺失时按无封面处理，不影响其它图片
            group_row = None
        cover_hash = str((group_row or {}).get("local_cover_hash") or "").strip()
        cached = ImageArchiveService(Database()).cached_image(cover_hash) if cover_hash else None
        if cached is None:
            raise HTTPException(status_code=404, detail="图片不存在")
        return await _cached_image_response(request, cached)
''' + ANCHOR_GROUP_COVER

# (标识, 原片段, 替换片段, 幂等标记)
REPLACEMENTS = [
    ('group_cover_image', ANCHOR_GROUP_COVER, NEW_GROUP_COVER, 'etkn-fix20 2026-11-05'),
    # fix17：全库 facet 单飞 + 紧凑投影（顺序无关，锚点互不重叠）
    ('facet_rows_projection', ANCHOR_FACET_ELSE, NEW_FACET_ELSE,
     '_facet_library_rows(request, catalog, permissions, user_id)'),
    ('facet_library_rows_helper', ANCHOR_VISIBLE_ROWS_TAIL, NEW_VISIBLE_ROWS_TAIL,
     'def _facet_library_rows('),
    ('facet_snapshot_cache', ANCHOR_FACET_CACHE, NEW_FACET_CACHE,
     '_facet_permission_scope('),
    ('facet_items_call', ANCHOR_FACET_ITEMS_CALL, NEW_FACET_ITEMS_CALL,
     '_cached_facet_values(request, catalog, permissions, user_id).get(facet, [])'),
    ('facet_item_filters_call', ANCHOR_ITEM_FILTERS_CALL, NEW_ITEM_FILTERS_CALL,
     'return _cached_facet_values(request, catalog, permissions, user_id)'),
    ('compact', OLD_COMPACT, NEW_COMPACT,
     '_placeholder_page_first('),
    ('group_get_item', ANCHOR_GET_ITEM_LIB, NEW_GET_ITEM_LIB, 'etkn-fix16 2026-10-27'),
    ('group_helpers', ANCHOR_LIST_ITEMS, NEW_LIST_ITEMS,
     '_collection_group_items(request, group)'),
    ('group_dto', ANCHOR_COLLECTION_DTO, NEW_COLLECTION_DTO,
     'member_count: int | None = len(group_members)'),
    ('group_type', ANCHOR_COLLECTION_TYPE, NEW_COLLECTION_TYPE,
     'group_members else "movies"'),
    ('group_children', ANCHOR_CHILDREN, NEW_CHILDREN,
     'Children 也要返回成员媒体库'),
    ('group_views', ANCHOR_USER_VIEWS, NEW_USER_VIEWS,
     '_apply_collection_groups(request, _visible_libraries'),
    ('group_folders', ANCHOR_MEDIA_FOLDERS, NEW_MEDIA_FOLDERS,
     '与 user_views 同口径'),
    ('prime_child_counts', ANCHOR_PRIME_COUNTS, NEW_PRIME_COUNTS,
     '_prime_child_counts(request, catalog, user_id, rows)'),
    ('dto_child_count', ANCHOR_DTO_CHILD_COUNT, NEW_DTO_CHILD_COUNT,
     '_child_counts = getattr(request.state, \"media_child_counts\", None)'),
]


def main() -> int:
    if not os.path.exists(ORIG):
        print(f'缺少 {ORIG}（先从镜像里抽原文件）', file=sys.stderr)
        return 1
    source = open(ORIG, encoding='utf-8').read()
    patched = source
    for name, old, new, done_marker in REPLACEMENTS:
        if done_marker in patched:
            print(f'[{name}] 已存在，跳过')
            continue
        count = patched.count(old)
        if count != 1:
            print(f'[{name}] 定位失败：期望命中 1 次，实际 {count} 次', file=sys.stderr)
            return 1
        patched = patched.replace(old, new, 1)
        print(f'[{name}] 已替换')

    for marker in MARKERS:
        if marker not in patched:
            print(f'校验失败：缺少标记 {marker}', file=sys.stderr)
            return 1
    if patched.count('actual = {int(row["id"]): row for row in _list_item_rows(') != 2:
        print('校验失败：占位符分支锚点数量异常（应为 分页优先 + 整库回退 两处）', file=sys.stderr)
        return 1
    if patched.count('_placeholder_page_first(') != 2:
        print('校验失败：分页优先辅助函数未正确挂载', file=sys.stderr)
        return 1
    if 'def _placeholder_light_rows(' not in patched:
        print('校验失败：缺少 _placeholder_light_rows', file=sys.stderr)
        return 1
    for token in ('def _collection_group_items(', 'def _collection_group_child_dto(', 'def _collection_group_members('):
        if patched.count(token) != 1:
            print(f'校验失败：{token} 出现次数异常', file=sys.stderr)
            return 1
    if len(re.findall(r'_collection_group_items\(request, group', patched)) != 2:
        print('校验失败：分组分支未同时挂到 list_items 与 children', file=sys.stderr)
        return 1

    with open(PATCH, 'w', encoding='utf-8') as handle:
        handle.write(patched)

    digest = hashlib.md5(patched.encode('utf-8')).hexdigest()
    with open(PATCH + '.md5', 'w', encoding='utf-8') as handle:
        handle.write(digest + '\n')
    os.chmod(PATCH + '.md5', 0o644)

    import py_compile
    py_compile.compile(PATCH, doraise=True)
    print(f'media_server_patch.py 已生成 {len(patched)} 字符 md5={digest}')
    print(f'原文件 {len(source)} 字符 md5={hashlib.md5(source.encode()).hexdigest()}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
