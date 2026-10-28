#!/bin/sh
# etkn-patch-check.sh — ETKN bind-mount 补丁自检（12 补丁 × 部署链 + 上游一致性）
#
# 变更记录：
#   2026-10-13  上游 v1.3.33 已修复 p115 / tmdb / scrape_repository / washing_recalculate /
#               local_media 五个问题，对应 bind 已停用。
#   2026-10-13  worker（周期 malloc_trim）亦停用：上游 worker.py 已内建
#               release_unused_memory_if_due()（_MEMORY_RELEASE_INTERVAL_SECONDS=30.0，
#               且 runtime.py 在 handled 分支也调用），与我们补丁等价且多一次 gc.collect()。
#               本脚本校验仍启用的三个补丁：
#                 repository  —— 订阅需求汇总 SQL 提速（CTE 展开；上游至今仍是老写法）
#                 shared_pool —— 共享中心强制走 7890 代理 + 忽略自签证书 + 读超时钳 45s
#                 watchlist   —— 联动删除 SQL 提速 (外键直连 0.2s) + maintenance 优先级提升 (防 115 删除饿死)
#   2026-10-26  共享登记 409 幂等（handlers）—— 追剧刷新每轮的 register-shared-source 里，
#               同 SHA1 已被中心以其它身份登记会返回 409，上游把任一项失败当整批判 failed，
#               导致同一批文件每轮都红、永远重试（文件本身在池中可播）。补丁把该类项计入 skipped。
#   2026-10-25  第 4 个补丁 p115_workflow —— 整理结果体瘦身：rejected 只留 4 字段、
#               超 24MB 整体丢弃，避免 complete_step 5 倍写放大撑爆 libpq 缓冲区
#               （表现为整理全跑完却判 failed、enqueue-ingest 被 block、organize.jobs 留 pending 孤儿行）。
#   2026-10-25  第 8 个补丁 virtual_library_service —— 「新建媒体库」弹窗 28~33 秒才出现：
#               GET /api/virtual-libraries/catalog 里 _local_options() 全表拉
#               media.item_metadata.payload（382,717 行、约 823MB JSON）在 Python 逐行解析出
#               7 组筛选项，实测 27.0s，其中 5 组随后被 _vocabularies() 整组覆盖丢弃；
#               改为 SQL jsonb 聚合 genres/tags（1.37s）+ TTL 缓存，TMDB 类型表并行+落盘快照。
#   2026-10-25  第 5 个补丁 center_control —— 控制长连接退避 120s→≤3s、断线/RPC 失败记
#               kind/code/reason、去 15s recv 超时；配合 shared_pool 连接复用，把中心签名
#               探测（预算 3s）从超时边缘拉回来，修「签名能力状态 异常」与离线时长虚增。
#   维度精简：原 mount_/file_/inuse_ 三项合并为 deploy_ 一项——它们本就是同一条部署链
#               （compose 有 bind 行 → 补丁文件校验和正确 → 容器内已生效），任一环断即报，
#               detail 指明断在哪一步。12 项 → 4 项。
#               旧的 7 项版备份见同目录 etkn-patch-check.sh.bak-*。
#   2026-10-16  期望 md5 改为「边车优先」：补丁文件旁的 <patch>.md5（auto_rebase_patch.py 每次
#               rebase 时同步写入）= 单一真源，缺失才回退脚本内硬编码值。修掉复发问题：
#               rebase 成功但期望值没同步 → 自检永久假红（实证：auto_rebase 的更新正则
#               漏配带引号的 check_one 行，静默跳过）。
# 退出码：0 = 全部正常；1 = 有问题
# 用法：etkn-patch-check.sh [--json|--text]     默认 --json

ETKN_DIR=${ETKN_DIR:-/vol1/1000/docker/etkn}
COMPOSE=${COMPOSE:-$ETKN_DIR/docker-compose.yml}
IMAGE=${IMAGE:-hbq0405/etkn:latest}
CTR=${CTR:-etkn}
DBCTR=${DBCTR:-etkn-db}
SITE=/usr/local/lib/python3.12/site-packages/etk_vnext
MODE=json
[ "$1" = "--text" ] && MODE=text

md5_of() { [ -f "$1" ] && md5sum "$1" 2>/dev/null | cut -d' ' -f1; }

# 期望 md5：优先读补丁文件旁的 .md5 边车（auto_rebase_patch.py 在 rebase 时同步写入，
# 单一真源），没有边车才用脚本内硬编码的回退值（新克隆仓库的场景）。
expect_of() {  # <补丁文件> <回退md5>
    _side="$1.md5"
    if [ -s "$_side" ]; then
        awk '{print $1}' "$_side" | tr -d ' \t\r\n'
    else
        printf '%s' "$2"
    fi
}

CHECKS=""; PROBLEMS=""; FAIL=0
add() {
    _k="$1"; _o="$2"; _d="$3"
    _okstr=false; [ "$_o" = "0" ] && _okstr=true
    _d=$(printf '%s' "$_d" | sed 's/\\/\\\\/g; s/"/\\"/g')
    CHECKS="$CHECKS${CHECKS:+,}{\"key\":\"$_k\",\"ok\":$_okstr,\"detail\":\"$_d\"}"
    if [ "$_o" != "0" ]; then FAIL=1; PROBLEMS="$PROBLEMS${PROBLEMS:+,}\"$_d\""; fi
}

check_one() {  # <补丁文件> <容器内路径> <期望md5> <短名>
    # 部署链三环合一：compose bind 行 → 补丁文件校验和 → 容器内已生效
    _patch="$1"; _ctr="$2"; _exp="$3"; _name="$4"
    _base=$(basename "$_patch")
    _m=$(grep -c "$_base:" "$COMPOSE" 2>/dev/null)
    _got=$(md5_of "$_patch")
    _c=$(docker exec "$CTR" md5sum "$_ctr" 2>/dev/null | cut -d' ' -f1)
    _bad=""
    [ "${_m:-0}" -lt 1 ] && _bad="compose 缺 $_base 的 bind 行"
    if [ -z "$_got" ]; then
        _bad="${_bad:+$_bad; }$_base 不存在或不可读"
    elif [ "$_got" != "$_exp" ]; then
        _bad="${_bad:+$_bad; }$_base md5=$_got ≠ 期望 $_exp"
    fi
    if [ -z "$_c" ] || [ "$_c" != "$_got" ]; then
        _bad="${_bad:+$_bad; }容器内 md5=${_c:-读不到} ≠ 补丁 ${_got:-无}（未生效或需重建容器）"
    fi
    if [ -z "$_bad" ]; then
        add "deploy_$_name" 0 "$_base 部署链完整（compose bind + md5 + 容器内生效，md5=$_got）"
    else
        add "deploy_$_name" 1 "$_bad"
    fi
}

check_db_params() {  # 运行时参数：role/库级退化参数 + jit（2026-10-26 事故）
    _cfg=$(docker exec "$DBCTR" psql -U etkn -d etkn -Atc \
        "select coalesce(string_agg(coalesce(setconfig::text,''),' '),'') from pg_db_role_setting where setrole='etkn'::regrole" 2>&1)
    _rc1=$?
    _jit=$(docker exec "$DBCTR" psql -U etkn -d etkn -Atc "show jit" 2>&1 | tr -d ' \n')
    _rc2=$?
    if [ "$_rc1" != "0" ] || [ "$_rc2" != "0" ]; then
        add "db_params" 1 "数据库参数状态未知（psql 失败，容器可能重启中）：$(printf '%s' "$_cfg" | head -c 120)"
        return
    fi
    _bad=""
    for _k in enable_nestloop enable_hashjoin enable_seqscan enable_mergejoin; do
        case "$_cfg" in *"$_k=off"*) _bad="${_bad:+$_bad; }etkn 角色/库级 $_k=off";; esac
    done
    case "$_jit" in on) _bad="${_bad:+$_bad; }jit=on（曾触发 parallel worker 段错误）";; esac
    if [ -z "$_bad" ]; then
        add "db_params" 0 "运行参数正常（jit=$_jit，无角色/库级退化参数）"
    else
        add "db_params" 1 "$_bad → 点查退化成全表哈希连接（海报/详情/起播全线变慢）；跑 db_setting_guard.py 或点「立即修复」"
    fi
}

check_one "$ETKN_DIR/repository_patch.py"  "$SITE/modules/subscription/repository.py" "$(expect_of "$ETKN_DIR/repository_patch.py" 9c5277d8021fcc3ed84d5cd2c996de09)" repository
check_one "$ETKN_DIR/shared_pool_patch.py" "$SITE/integrations/shared_pool.py"          "$(expect_of "$ETKN_DIR/shared_pool_patch.py" 1965752ad40c8b367aaae8d941f35cb9)" shared_pool
check_one "$ETKN_DIR/watchlist_patch.py"   "$SITE/modules/subscription/watchlist.py"    "$(expect_of "$ETKN_DIR/watchlist_patch.py"   ff2a1bce4d1ebf1819d0e76ba08a1db9)" watchlist
check_one "$ETKN_DIR/p115_workflow_patch.py" "$SITE/modules/organize/p115_workflow.py"   "$(expect_of "$ETKN_DIR/p115_workflow_patch.py" 73ff4f7939bab37d41580faf86d3149f)" p115_workflow
check_one "$ETKN_DIR/ai_service_patch.py" "$SITE/modules/ai_translation/service.py" "$(expect_of "$ETKN_DIR/ai_service_patch.py" 6619646db744b24da85cb23c9f2d88ac)" ai_service
check_one "$ETKN_DIR/center_control_patch.py" "$SITE/integrations/center_control.py"      "$(expect_of "$ETKN_DIR/center_control_patch.py" bdbf50549384522e8eaa3b21c93068e7)" center_control
check_one "$ETKN_DIR/screenshots_patch.py"   "$SITE/modules/scrape/screenshots.py"      "$(expect_of "$ETKN_DIR/screenshots_patch.py" dcdef9d07a4549cef46ba20de586c22f)" screenshots
check_one "$ETKN_DIR/virtual_library_service_patch.py" "$SITE/modules/virtual_library/service.py" "$(expect_of "$ETKN_DIR/virtual_library_service_patch.py" dda522b8b8406f8e25e5a9764de9c77c)" virtual_library
check_one "$ETKN_DIR/placeholders_patch.py" "$SITE/modules/virtual_library/placeholders.py" "$(expect_of "$ETKN_DIR/placeholders_patch.py" 3de661889d331cff003a89d4756ab788)" placeholders
check_one "$ETKN_DIR/media_server_patch.py" "$SITE/api/media_server.py" "$(expect_of "$ETKN_DIR/media_server_patch.py" 591bae7f7138dc3e82603281eee8cda6)" media_server
check_one "$ETKN_DIR/catalog_patch.py" "$SITE/modules/media_server/catalog.py" "$(expect_of "$ETKN_DIR/catalog_patch.py" ed3b28f87b6f88930bab719080663d9e)" catalog
check_one "$ETKN_DIR/shared_register_patch.py" "$SITE/platform/tasks/handlers.py" "$(expect_of "$ETKN_DIR/shared_register_patch.py" ba5de21a5a26b14c59bd37db69319072)" shared_register
check_db_params

# 上游一致性：镜像内原版 vs 我们保存的 .orig 基准（改了就必须 rebase）
IMG_ID="$(docker image inspect "$IMAGE" --format '{{.Id}}' 2>/dev/null)"
IMG_CREATED="$(docker image inspect "$IMAGE" --format '{{.Created}}' 2>/dev/null)"
TMPD="$(mktemp -d 2>/dev/null || echo /tmp/etkn-pc.$$)"; mkdir -p "$TMPD"
CNAME="etkn-patchcheck-$$"
docker create --name "$CNAME" "$IMAGE" >/dev/null 2>&1
ANY_REBASE=0
for spec in \
    "$ETKN_DIR/repository.py.orig|$SITE/modules/subscription/repository.py|repository" \
    "$ETKN_DIR/shared_pool.py.orig|$SITE/integrations/shared_pool.py|shared_pool" \
    "$ETKN_DIR/watchlist.py.orig|$SITE/modules/subscription/watchlist.py|watchlist" \
    "$ETKN_DIR/p115_workflow.py.orig|$SITE/modules/organize/p115_workflow.py|p115_workflow" \
    "$ETKN_DIR/ai_service.py.orig|$SITE/modules/ai_translation/service.py|ai_service" \
    "$ETKN_DIR/center_control.py.orig|$SITE/integrations/center_control.py|center_control" \
    "$ETKN_DIR/screenshots.py.orig|$SITE/modules/scrape/screenshots.py|screenshots" \
    "$ETKN_DIR/virtual_library_service.py.orig|$SITE/modules/virtual_library/service.py|virtual_library" \
    "$ETKN_DIR/placeholders.py.orig|$SITE/modules/virtual_library/placeholders.py|placeholders" \
    "$ETKN_DIR/media_server.py.orig|$SITE/api/media_server.py|media_server" \
    "$ETKN_DIR/catalog.py.orig|$SITE/modules/media_server/catalog.py|catalog" \
    "$ETKN_DIR/handlers.py.orig|$SITE/platform/tasks/handlers.py|handlers"
do
    _orig="${spec%%|*}"; _rest="${spec#*|}"; _path="${_rest%%|*}"; _name="${_rest##*|}"
    _new="$TMPD/${_name}_new.py"
    docker cp "$CNAME:$_path" "$_new" >/dev/null 2>&1
    if [ ! -s "$_new" ] || [ ! -s "$_orig" ]; then
        add "upstream_$_name" 1 "$_name.py 状态未知（缺基准文件，或抽不出镜像内文件）"
    elif cmp -s "$_orig" "$_new"; then
        add "upstream_$_name" 0 "镜像内原版 $_name.py 与基准一致 → 补丁可直接沿用"
    else
        add "upstream_$_name" 1 "上游改过 $_name.py → 必须 rebase，否则上游修复会被旧副本静默盖掉"
        ANY_REBASE=1
    fi
done
docker rm -f "$CNAME" >/dev/null 2>&1
rm -rf "$TMPD" 2>/dev/null

OKSTR=false; [ "$FAIL" = "0" ] && OKSTR=true
TS="$(date -Iseconds 2>/dev/null || date)"
if [ "$MODE" = "text" ]; then
    printf 'ETKN 补丁自检 @ %s\n镜像 %s\n' "$TS" "${IMG_ID#sha256:}"
    printf '%s\n' "$CHECKS" | sed 's/},{/}\n{/g' | while IFS= read -r _c; do
        _k=$(printf '%s' "$_c" | sed -n 's/.*"key":"\([^"]*\)".*/\1/p')
        _o=$(printf '%s' "$_c" | sed -n 's/.*"ok":\([a-z]*\).*/\1/p')
        _d=$(printf '%s' "$_c" | sed -n 's/.*"detail":"\(.*\)"}.*/\1/p')
        [ "$_o" = "true" ] && _mk="[ OK ]" || _mk="[FAIL]"
        printf '  %s %-22s %s\n' "$_mk" "$_k" "$_d"
    done
    _nd=$(printf '%s' "$CHECKS" | grep -o '"key":"deploy_[^"]*","ok":true' | wc -l | tr -d ' ')
    _nu=$(printf '%s' "$CHECKS" | grep -o '"key":"upstream_[^"]*","ok":true' | wc -l | tr -d ' ')
    if [ "$FAIL" = "0" ]; then printf '结论：全部正常（%s 条部署链 + %s 条上游一致性 + 1 条运行参数）\n' "$_nd" "$_nu"; else printf '结论：有问题\n'; fi
else
    printf '{"ok":%s,"ts":"%s","image_id":"%s","image_created":"%s","need_rebase":%s,"checks":[%s],"problems":[%s]}\n' \
        "$OKSTR" "$TS" "$IMG_ID" "$IMG_CREATED" \
        "$([ "$ANY_REBASE" = "1" ] && echo true || echo false)" "$CHECKS" "$PROBLEMS"
fi
[ "$FAIL" = "0" ] && exit 0 || exit 1