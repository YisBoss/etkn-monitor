#!/bin/sh
# etkn-patch-check.sh — ETKN bind-mount 补丁自检（在 NAS 宿主机上跑，需要 docker CLI）
#
# 为什么要这个：ETKN 本体是 vendor 镜像 hbq0405/etkn:latest，飞牛上没有源码工程，
# 我们改代码的唯一路子是「往 docker-compose.yml 里追加 bind-mount 覆盖 vendor 模块」。
# 于是每次更新镜像都可能踩两个坑：
#   ① compose 被重写 → mount 行丢了 → 补丁静默失效（fulfilled 退回 ~22s，功能不坏但变慢）
#   ② 镜像里上游改了同一个文件 → 我们的补丁是「整文件副本」→ 会把新版盖回去，
#      上游的修复/新功能静默丢失（这个更危险）
#      五处补丁（repository / shared_pool / p115 / tmdb / scrape_repository）逐个比对，
#      任一跑偏都报 need_rebase=true
# 本脚本就是把这两件事查出来。
#
# 退出码：0 = 全部正常；1 = 有问题（详见输出里的 problems[]）
# 用法：etkn-patch-check.sh [--json|--text]     默认 --json
#
# 变更记录：
#   v2 覆盖范围 3 处 → 5 处（补上 tmdb / scrape_repository 两个盲区）

ETKN_DIR=${ETKN_DIR:-/vol1/1000/docker/etkn}
MON_DIR=${MON_DIR:-/vol1/1000/docker/etkn-monitor}
COMPOSE=${COMPOSE:-$ETKN_DIR/docker-compose.yml}
PATCH_REPO=${PATCH_REPO:-$ETKN_DIR/repository_patch.py}
PATCH_POOL=${PATCH_POOL:-$ETKN_DIR/shared_pool_patch.py}
PATCH_P115=${PATCH_P115:-$ETKN_DIR/p115_patch.py}
PATCH_TMDB=${PATCH_TMDB:-$ETKN_DIR/tmdb_patch.py}
PATCH_SCRAPE=${PATCH_SCRAPE:-$ETKN_DIR/scrape_repository_patch.py}
ORIG_REPO=${ORIG_REPO:-$ETKN_DIR/repository.py.orig}
ORIG_POOL=${ORIG_POOL:-$ETKN_DIR/shared_pool.py.orig}
ORIG_P115=${ORIG_P115:-$ETKN_DIR/p115.py.orig}
ORIG_TMDB=${ORIG_TMDB:-$ETKN_DIR/tmdb.py.orig}
ORIG_SCRAPE=${ORIG_SCRAPE:-$ETKN_DIR/scrape_repository.py.orig}
STATE=${STATE:-$MON_DIR/data/patch-check.state}
IMAGE=${IMAGE:-hbq0405/etkn:latest}
CTR=${CTR:-etkn}

# 期望值：repository_patch.py 的 md5（换补丁时必须同步改这里）
EXPECT_REPO=${EXPECT_REPO:-d08462056b43ef56793490b181286e7a}
# 期望值：p115_patch.py 的 md5（换补丁时必须同步改这里）
# 含两项修复：①网络层重试 ②堵住 115 /files 的 offset 回绕死循环
# 注意：这个值过去是「死变量」——定义了但脚本正文从没引用过，等于没在校验。
#       v2 起真正生效：补丁文件 md5 与它不一致会直接报错。
EXPECT_P115=${EXPECT_P115:-0d700a83359199c1c91c2363a627477f}
# 期望值：shared_pool / tmdb / scrape_repository 三处补丁的 md5
EXPECT_POOL=${EXPECT_POOL:-032f6abe62f258b50782958534d108cf}
EXPECT_TMDB=${EXPECT_TMDB:-2fe8956fe028be981021a70f92ac016f}
EXPECT_SCRAPE=${EXPECT_SCRAPE:-c30a9c3d4c3ec5e1d28790b95e7f84bb}

REPO_IN_CTR=/usr/local/lib/python3.12/site-packages/etk_vnext/modules/subscription/repository.py
POOL_IN_CTR=/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/shared_pool.py
P115_IN_CTR=/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/p115.py
TMDB_IN_CTR=/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/tmdb.py
SCRAPE_IN_CTR=/usr/local/lib/python3.12/site-packages/etk_vnext/modules/scrape/repository.py

MODE=json
[ "$1" = "--text" ] && MODE=text

md5_of() { [ -f "$1" ] && md5sum "$1" 2>/dev/null | cut -d' ' -f1; }

# 逐项结果累积（POSIX sh 没有数组，用换行分隔的字符串）
CHECKS=""
PROBLEMS=""
FAIL=0

add() {  # add <key> <ok:0|1> <detail>
    _k="$1"; _o="$2"; _d="$3"
    _okstr=false; [ "$_o" = "0" ] && _okstr=true
    # JSON 转义：引号与反斜杠
    _d=$(printf '%s' "$_d" | sed 's/\\/\\\\/g; s/"/\\"/g')
    CHECKS="$CHECKS${CHECKS:+,}{\"key\":\"$_k\",\"ok\":$_okstr,\"detail\":\"$_d\"}"
    if [ "$_o" != "0" ]; then
        FAIL=1
        PROBLEMS="$PROBLEMS${PROBLEMS:+,}\"$_d\""
    fi
}

# 通用：校验「部署补丁文件 md5 == 期望 md5」
check_expect() {  # check_expect <key> <补丁文件> <期望md5> <名字>
    _got="$(md5_of "$2")"
    if [ -z "$_got" ]; then
        add "$1" 1 "$4 补丁不存在或不可读：$2"
    elif [ "$_got" = "$3" ]; then
        add "$1" 0 "$4 md5=${_got}（与期望一致）"
    else
        add "$1" 1 "$4 md5=${_got} ≠ 期望 ${3}（补丁文件被改过或换过）"
    fi
}

# 通用：校验「容器内实际文件 md5 == 部署补丁 md5」
check_mounted() {  # check_mounted <key> <补丁文件> <容器内路径> <名字> <后果说明>
    _pf="$(md5_of "$2")"
    _ct="$(docker exec "$CTR" md5sum "$3" 2>/dev/null | cut -d' ' -f1)"
    if [ -z "$_ct" ]; then
        add "$1" 1 "读不到容器内 $3（容器未运行？）"
    elif [ -z "$_pf" ]; then
        add "$1" 1 "$4 补丁文件不可读：$2"
    elif [ "$_pf" = "$_ct" ]; then
        add "$1" 0 "容器内 $4 md5=${_ct}（== 部署补丁，已生效）"
    else
        add "$1" 1 "容器内 $4 md5=${_ct} ≠ 部署补丁 md5=${_pf} → $5"
    fi
}

# ---------- ① compose 里的 mount 行 ----------
MOUNT_REPO_LINE="$(grep -c 'repository_patch.py:/usr/local/lib/python3.12/site-packages/etk_vnext/modules/subscription/repository.py' "$COMPOSE" 2>/dev/null)"
MOUNT_POOL_LINE="$(grep -c 'shared_pool_patch.py:/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/shared_pool.py' "$COMPOSE" 2>/dev/null)"
MOUNT_P115_LINE="$(grep -c 'p115_patch.py:/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/p115.py' "$COMPOSE" 2>/dev/null)"
MOUNT_TMDB_LINE="$(grep -c 'tmdb_patch.py:/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/tmdb.py' "$COMPOSE" 2>/dev/null)"
MOUNT_SCRAPE_LINE="$(grep -c 'scrape_repository_patch.py:/usr/local/lib/python3.12/site-packages/etk_vnext/modules/scrape/repository.py' "$COMPOSE" 2>/dev/null)"
_miss=""
[ "${MOUNT_REPO_LINE:-0}" -lt 1 ] && _miss="$_miss repository"
[ "${MOUNT_POOL_LINE:-0}" -lt 1 ] && _miss="$_miss shared_pool"
[ "${MOUNT_P115_LINE:-0}" -lt 1 ] && _miss="$_miss p115"
[ "${MOUNT_TMDB_LINE:-0}" -lt 1 ] && _miss="$_miss tmdb"
[ "${MOUNT_SCRAPE_LINE:-0}" -lt 1 ] && _miss="$_miss scrape_repository"
if [ -z "$_miss" ]; then
    add mount_lines 0 "compose 五行 bind-mount 均在（repository + shared_pool + p115 + tmdb + scrape_repository）"
else
    add mount_lines 1 "compose 缺少 bind-mount 行：${_miss# } → 对应补丁已失效"
fi

# ---------- ② 部署目录里的补丁文件本身（五处都校验 md5） ----------
check_expect patch_file       "$PATCH_REPO"   "$EXPECT_REPO"   repository_patch.py
check_expect patch_file_pool  "$PATCH_POOL"   "$EXPECT_POOL"   shared_pool_patch.py
check_expect patch_file_p115  "$PATCH_P115"   "$EXPECT_P115"   p115_patch.py
check_expect patch_file_tmdb  "$PATCH_TMDB"   "$EXPECT_TMDB"   tmdb_patch.py
check_expect patch_file_scrape "$PATCH_SCRAPE" "$EXPECT_SCRAPE" scrape_repository_patch.py

# ---------- ③ 容器内实际生效的文件（五处都校验 == 部署补丁） ----------
check_mounted container_mount  "$PATCH_REPO"   "$REPO_IN_CTR"   repository.py   "补丁没挂上（fulfilled 会退回 ~22s）"
check_mounted shared_pool      "$PATCH_POOL"   "$POOL_IN_CTR"   shared_pool.py  "shared_pool 补丁没挂上"
check_mounted p115_patch       "$PATCH_P115"   "$P115_IN_CTR"   p115.py         "p115 补丁没挂上（115 会再现 offset 回绕死循环）"
check_mounted tmdb_patch       "$PATCH_TMDB"   "$TMDB_IN_CTR"   tmdb.py         "tmdb 补丁没挂上（TLS 读超时会 0 重试）"
check_mounted scrape_repository "$PATCH_SCRAPE" "$SCRAPE_IN_CTR" scrape/repository.py "scrape 补丁没挂上"

# 挂上文件还不够：ETKN 是单进程 uvicorn、不热加载，进程没重启就还是旧代码。
# 这里直接问运行中的进程有没有收敛闸。
GOT_P115_GUARD="$(docker exec "$CTR" python -c "import etk_vnext.integrations.p115 as m; print(hasattr(m.P115StorageProvider,'_guard_page_offset'))" 2>/dev/null | tr -d '\r\n')"
if [ "$GOT_P115_GUARD" = "True" ]; then
    add p115_guard 0 "运行中的 p115 客户端含 _guard_page_offset 收敛闸"
else
    add p115_guard 1 "运行中的 p115 客户端没有 _guard_page_offset → 补丁未加载（需 docker restart $CTR），115 offset 回绕会死循环刷接口"
fi

# ---------- ④ 上游是否改过同一个文件（需不需要 rebase） ----------
# 五处补丁都是「整文件副本」：镜像一更新，只要上游改过同名文件，我们的旧副本
# 就会把上游的修复/新功能静默盖回去（我们的补丁可能只有十几行，盖掉的却是几百行）。
# 这里逐个文件比对「镜像内原版」 vs 「我们保存的 .orig 基准」。
# docker create + docker cp 有点重，按「镜像 ID + 五个基准的 md5」缓存：都没变就复用上次结论
IMG_ID="$(docker image inspect "$IMAGE" --format '{{.Id}}' 2>/dev/null)"
IMG_CREATED="$(docker image inspect "$IMAGE" --format '{{.Created}}' 2>/dev/null)"
ORIG_KEY="$(md5_of "$ORIG_REPO")/$(md5_of "$ORIG_POOL")/$(md5_of "$ORIG_P115")/$(md5_of "$ORIG_TMDB")/$(md5_of "$ORIG_SCRAPE")"
CACHED_ID=""; CACHED_VERDICT=""; CACHED_DETAIL=""; CACHED_ORIG=""
if [ -f "$STATE" ]; then
    CACHED_ID="$(sed -n 's/^image_id=//p' "$STATE" | head -1)"
    CACHED_VERDICT="$(sed -n 's/^rebase_verdict=//p' "$STATE" | head -1)"
    CACHED_DETAIL="$(sed -n 's/^rebase_detail=//p' "$STATE" | head -1)"
    CACHED_ORIG="$(sed -n 's/^orig_key=//p' "$STATE" | head -1)"
fi

emit_rebase_file() {  # emit_rebase_file <短名> <ok|rebase|unknown>
    case "$2" in
        ok)     add "upstream_$1" 0 "镜像内原版 $1.py 与基准一致 → 补丁可直接沿用" ;;
        rebase) add "upstream_$1" 1 "上游改过 $1.py → 必须把补丁 rebase 到新版，否则上游修复/新功能会被旧副本静默盖掉" ;;
        *)      add "upstream_$1" 1 "$1.py 状态未知（缺基准文件，或抽不出镜像内文件）" ;;
    esac
}

if [ -n "$IMG_ID" ] && [ "$IMG_ID" = "$CACHED_ID" ] && [ -n "$CACHED_VERDICT" ] \
   && [ -n "$CACHED_ORIG" ] && [ "$CACHED_ORIG" = "$ORIG_KEY" ]; then
    VERDICT="$CACHED_VERDICT"
    add upstream_rebase_note 0 "镜像与基准均未变（${IMG_ID#sha256:} 前 12 位），沿用上次结论"
    if [ -n "$CACHED_DETAIL" ]; then
        for _kv in $(printf '%s' "$CACHED_DETAIL" | tr ',' ' '); do
            emit_rebase_file "${_kv%%=*}" "${_kv#*=}"
        done
    fi
else
    TMPD="$(mktemp -d 2>/dev/null || echo /tmp/etkn-pc.$$)"
    mkdir -p "$TMPD"
    CNAME="etkn-patchcheck-$$"
    docker create --name "$CNAME" "$IMAGE" >/dev/null 2>&1
    _detail=""; _any_rebase=0; _any_unknown=0
    # 每项格式：<基准.orig>|<镜像内路径>|<短名>
    for _spec in \
        "$ORIG_REPO|$REPO_IN_CTR|repository" \
        "$ORIG_POOL|$POOL_IN_CTR|shared_pool" \
        "$ORIG_P115|$P115_IN_CTR|p115" \
        "$ORIG_TMDB|$TMDB_IN_CTR|tmdb" \
        "$ORIG_SCRAPE|$SCRAPE_IN_CTR|scrape_repository"
    do
        _orig="${_spec%%|*}"; _rest="${_spec#*|}"
        _path="${_rest%%|*}"; _name="${_rest##*|}"
        _new="$TMPD/${_name}_new.py"
        docker cp "$CNAME:$_path" "$_new" >/dev/null 2>&1
        if [ ! -s "$_new" ]; then
            emit_rebase_file "$_name" unknown
            _detail="$_detail${_detail:+,}$_name=unknown"; _any_unknown=1
        elif [ ! -s "$_orig" ]; then
            emit_rebase_file "$_name" unknown
            _detail="$_detail${_detail:+,}$_name=unknown"; _any_unknown=1
        elif cmp -s "$_orig" "$_new"; then
            emit_rebase_file "$_name" ok
            _detail="$_detail${_detail:+,}$_name=ok"
        else
            _diffn="$(diff "$_orig" "$_new" 2>/dev/null | grep -c '^[<>]')"
            add "upstream_$_name" 1 "上游改过 $_name.py（与基准 ${_orig##*/} 差异 ${_diffn} 行）→ 必须 rebase，否则上游修复/新功能会被旧副本静默盖掉"
            _detail="$_detail${_detail:+,}$_name=rebase"; _any_rebase=1
        fi
    done
    docker rm -f "$CNAME" >/dev/null 2>&1
    if [ "$_any_unknown" = "1" ]; then
        VERDICT="unknown"
    elif [ "$_any_rebase" = "1" ]; then
        VERDICT="rebase"
    else
        VERDICT="ok"
    fi
    rm -rf "$TMPD" 2>/dev/null
    mkdir -p "$(dirname "$STATE")" 2>/dev/null
    {
        printf 'image_id=%s\n' "$IMG_ID"
        printf 'rebase_verdict=%s\n' "$VERDICT"
        printf 'rebase_detail=%s\n' "$_detail"
        printf 'orig_key=%s\n' "$ORIG_KEY"
        printf 'checked_at=%s\n' "$(date -Iseconds 2>/dev/null || date)"
    } > "$STATE" 2>/dev/null
fi

# ---------- 输出 ----------
OKSTR=false; [ "$FAIL" = "0" ] && OKSTR=true
TS="$(date -Iseconds 2>/dev/null || date)"
IMG_SHORT="${IMG_ID#sha256:}"

if [ "$MODE" = "text" ]; then
    printf 'ETKN 补丁自检 @ %s\n' "$TS"
    printf '镜像 %s (created %s)\n' "$(printf '%s' "$IMG_SHORT" | cut -c1-12)" "$IMG_CREATED"
    # CHECKS 是 {"key":..,"ok":..,"detail":".."} 拼接串，逐条拆开打印
    # 注意 printf 末尾要带 \n：否则 while read 会跳过最后一行（read 在无换行 EOF 时返回非 0）
    printf '%s\n' "$CHECKS" | sed 's/},{/}\n{/g' | while IFS= read -r _c; do
        _k=$(printf '%s' "$_c" | sed -n 's/.*"key":"\([^"]*\)".*/\1/p')
        _o=$(printf '%s' "$_c" | sed -n 's/.*"ok":\([a-z]*\).*/\1/p')
        _d=$(printf '%s' "$_c" | sed -n 's/.*"detail":"\(.*\)"}.*/\1/p')
        [ "$_o" = "true" ] && _m="[ OK ]" || _m="[FAIL]"
        printf '  %s %-20s %s\n' "$_m" "$_k" "$_d"
    done
    if [ "$FAIL" = "0" ]; then
        printf '结论：全部正常\n'
    else
        printf '结论：有问题\n'
        printf '%s\n' "$PROBLEMS" | sed 's/","/"\n"/g' | while IFS= read -r _p; do
            printf '  ! %s\n' "$_p"
        done
    fi
else
    printf '{"ok":%s,"ts":"%s","image_id":"%s","image_created":"%s","need_rebase":%s,"checks":[%s],"problems":[%s]}\n' \
        "$OKSTR" "$TS" "$IMG_ID" "$IMG_CREATED" \
        "$([ "$VERDICT" = "rebase" ] && echo true || echo false)" \
        "$CHECKS" "$PROBLEMS"
fi

[ "$FAIL" = "0" ] && exit 0 || exit 1
