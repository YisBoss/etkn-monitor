#!/bin/sh
# etkn-patch-check.sh — ETKN bind-mount 补丁自检（v1.3.34 起：3 补丁 × 2 维度）
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

check_one "$ETKN_DIR/repository_patch.py"  "$SITE/modules/subscription/repository.py" "$(expect_of "$ETKN_DIR/repository_patch.py"  df80c65f53c8be70027f101bf5bbfed0)" repository
check_one "$ETKN_DIR/shared_pool_patch.py" "$SITE/integrations/shared_pool.py"          "$(expect_of "$ETKN_DIR/shared_pool_patch.py" 1965752ad40c8b367aaae8d941f35cb9)" shared_pool
check_one "$ETKN_DIR/watchlist_patch.py"   "$SITE/modules/subscription/watchlist.py"    "$(expect_of "$ETKN_DIR/watchlist_patch.py"   ff2a1bce4d1ebf1819d0e76ba08a1db9)" watchlist

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
    "$ETKN_DIR/watchlist.py.orig|$SITE/modules/subscription/watchlist.py|watchlist"
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
    if [ "$FAIL" = "0" ]; then printf '结论：全部正常（3 补丁 × 部署链 + 上游一致性）\n'; else printf '结论：有问题\n'; fi
else
    printf '{"ok":%s,"ts":"%s","image_id":"%s","image_created":"%s","need_rebase":%s,"checks":[%s],"problems":[%s]}\n' \
        "$OKSTR" "$TS" "$IMG_ID" "$IMG_CREATED" \
        "$([ "$ANY_REBASE" = "1" ] && echo true || echo false)" "$CHECKS" "$PROBLEMS"
fi
[ "$FAIL" = "0" ] && exit 0 || exit 1
