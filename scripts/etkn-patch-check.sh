#!/bin/sh
# etkn-patch-check.sh — ETKN bind-mount 补丁自检（在 NAS 宿主机上跑，需要 docker CLI）
#
# 为什么要这个：ETKN 本体是 vendor 镜像 hbq0405/etkn:latest，飞牛上没有源码工程，
# 我们改代码的唯一路子是「往 docker-compose.yml 里追加 bind-mount 覆盖 vendor 模块」。
# 于是每次更新镜像都可能踩两个坑：
#   ① compose 被重写 → mount 行丢了 → 补丁静默失效（fulfilled 退回 ~22s，功能不坏但变慢）
#   ② 镜像里上游改了同一个文件 → 我们的补丁是「整文件副本」→ 会把新版盖回去，
#      上游的修复/新功能静默丢失（这个更危险）
#      三处补丁（repository / shared_pool / p115）逐个比对，任一跑偏都报 need_rebase=true
# 本脚本就是把这两件事查出来。
#
# 退出码：0 = 全部正常；1 = 有问题（详见输出里的 problems[]）
# 用法：etkn-patch-check.sh [--json|--text]     默认 --json

ETKN_DIR=${ETKN_DIR:-/vol1/1000/docker/etkn}
MON_DIR=${MON_DIR:-/vol1/1000/docker/etkn-monitor}
COMPOSE=${COMPOSE:-$ETKN_DIR/docker-compose.yml}
PATCH_REPO=${PATCH_REPO:-$ETKN_DIR/repository_patch.py}
PATCH_POOL=${PATCH_POOL:-$ETKN_DIR/shared_pool_patch.py}
PATCH_P115=${PATCH_P115:-$ETKN_DIR/p115_patch.py}
ORIG_REPO=${ORIG_REPO:-$ETKN_DIR/repository.py.orig}
ORIG_POOL=${ORIG_POOL:-$ETKN_DIR/shared_pool.py.orig}
ORIG_P115=${ORIG_P115:-$ETKN_DIR/p115.py.orig}
STATE=${STATE:-$MON_DIR/data/patch-check.state}
IMAGE=${IMAGE:-hbq0405/etkn:latest}
CTR=${CTR:-etkn}

# 期望值：repository_patch.py 的 md5（换补丁时必须同步改这里）
EXPECT_REPO=${EXPECT_REPO:-d08462056b43ef56793490b181286e7a}
# 期望值：p115_patch.py 的 md5（换补丁时必须同步改这里）
# 含两项修复：①网络层重试 ②堵住 115 /files 的 offset 回绕死循环
EXPECT_P115=${EXPECT_P115:-1350f9246e2059dc1fa2a3b9f9be8529}

REPO_IN_CTR=/usr/local/lib/python3.12/site-packages/etk_vnext/modules/subscription/repository.py
POOL_IN_CTR=/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/shared_pool.py
P115_IN_CTR=/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/p115.py

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

# ---------- ① compose 里的 mount 行 ----------
MOUNT_REPO_LINE="$(grep -c 'repository_patch.py:/usr/local/lib/python3.12/site-packages/etk_vnext/modules/subscription/repository.py' "$COMPOSE" 2>/dev/null)"
MOUNT_POOL_LINE="$(grep -c 'shared_pool_patch.py:/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/shared_pool.py' "$COMPOSE" 2>/dev/null)"
MOUNT_P115_LINE="$(grep -c 'p115_patch.py:/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/p115.py' "$COMPOSE" 2>/dev/null)"
_miss=""
[ "${MOUNT_REPO_LINE:-0}" -lt 1 ] && _miss="$_miss repository"
[ "${MOUNT_POOL_LINE:-0}" -lt 1 ] && _miss="$_miss shared_pool"
[ "${MOUNT_P115_LINE:-0}" -lt 1 ] && _miss="$_miss p115"
if [ -z "$_miss" ]; then
    add mount_lines 0 "compose 三行 bind-mount 均在（repository + shared_pool + p115）"
else
    add mount_lines 1 "compose 缺少 bind-mount 行：${_miss# } → 对应补丁已失效"
fi

# ---------- ② 部署目录里的补丁文件本身 ----------
GOT_REPO="$(md5_of "$PATCH_REPO")"
if [ "$GOT_REPO" = "$EXPECT_REPO" ]; then
    add patch_file 0 "repository_patch.py md5=${GOT_REPO}（与期望一致）"
elif [ -z "$GOT_REPO" ]; then
    add patch_file 1 "repository_patch.py 不存在或不可读：$PATCH_REPO"
else
    add patch_file 1 "repository_patch.py md5=${GOT_REPO} ≠ 期望 ${EXPECT_REPO}（补丁文件被改过或换过）"
fi

# ---------- ③ 运行中容器实际看到的 repository.py ----------
GOT_IN_CTR="$(docker exec "$CTR" md5sum "$REPO_IN_CTR" 2>/dev/null | cut -d' ' -f1)"
if [ "$GOT_IN_CTR" = "$EXPECT_REPO" ]; then
    add container_mount 0 "容器内 repository.py md5=${GOT_IN_CTR}（补丁已生效）"
elif [ -z "$GOT_IN_CTR" ]; then
    add container_mount 1 "读不到容器内 $REPO_IN_CTR（容器未运行？）"
else
    add container_mount 1 "容器内 repository.py md5=${GOT_IN_CTR} ≠ 期望 ${EXPECT_REPO} → 补丁没挂上（fulfilled 会退回 ~22s）"
fi

# ---------- ④ shared_pool 补丁是否也在生效 ----------
GOT_POOL_FILE="$(md5_of "$PATCH_POOL")"
GOT_POOL_CTR="$(docker exec "$CTR" md5sum "$POOL_IN_CTR" 2>/dev/null | cut -d' ' -f1)"
if [ -n "$GOT_POOL_FILE" ] && [ "$GOT_POOL_FILE" = "$GOT_POOL_CTR" ]; then
    add shared_pool 0 "容器内 shared_pool.py md5=${GOT_POOL_CTR}（== 部署补丁，已生效）"
elif [ -z "$GOT_POOL_CTR" ]; then
    add shared_pool 1 "读不到容器内 $POOL_IN_CTR（容器未运行？）"
else
    add shared_pool 1 "容器内 shared_pool.py md5=${GOT_POOL_CTR} ≠ 部署补丁 md5=${GOT_POOL_FILE} → shared_pool 补丁没挂上"
fi

# ---------- ⑤ p115 补丁（网络层重试 + 堵 115 /files offset 回绕死循环） ----------
GOT_P115_FILE="$(md5_of "$PATCH_P115")"
GOT_P115_CTR="$(docker exec "$CTR" md5sum "$P115_IN_CTR" 2>/dev/null | cut -d' ' -f1)"
if [ -z "$GOT_P115_FILE" ]; then
    add p115_patch 1 "p115_patch.py 不存在或不可读：$PATCH_P115"
elif [ "$GOT_P115_FILE" = "$GOT_P115_CTR" ]; then
    add p115_patch 0 "容器内 p115.py md5=${GOT_P115_CTR}（== 部署补丁，已生效）"
else
    add p115_patch 1 "容器内 p115.py md5=${GOT_P115_CTR} ≠ 部署补丁 md5=${GOT_P115_FILE} → p115 补丁没挂上（115 会再现 offset 回绕死循环）"
fi
# 挂上文件还不够：ETKN 是单进程 uvicorn、不热加载，进程没重启就还是旧代码。
# 这里直接问运行中的进程有没有收敛闸。
GOT_P115_GUARD="$(docker exec "$CTR" python -c "import etk_vnext.integrations.p115 as m; print(hasattr(m.P115StorageProvider,'_guard_page_offset'))" 2>/dev/null | tr -d '\r\n')"
if [ "$GOT_P115_GUARD" = "True" ]; then
    add p115_guard 0 "运行中的 p115 客户端含 _guard_page_offset 收敛闸"
else
    add p115_guard 1 "运行中的 p115 客户端没有 _guard_page_offset → 补丁未加载（需 docker restart $CTR），115 offset 回绕会死循环刷接口"
fi

# ---------- ⑥ 上游是否改过同一个文件（需不需要 rebase） ----------
# 三处补丁都是「整文件副本」：镜像一更新，只要上游改过同名文件，我们的旧副本
# 就会把上游的修复/新功能静默盖回去（我们的补丁可能只有十几行，盖掉的却是几百行）。
# 这里逐个文件比对「镜像内原版」 vs 「我们保存的 .orig 基准」。
# docker create + docker cp 有点重，按「镜像 ID + 三个基准的 md5」缓存：都没变就复用上次结论
IMG_ID="$(docker image inspect "$IMAGE" --format '{{.Id}}' 2>/dev/null)"
IMG_CREATED="$(docker image inspect "$IMAGE" --format '{{.Created}}' 2>/dev/null)"
ORIG_KEY="$(md5_of "$ORIG_REPO")/$(md5_of "$ORIG_POOL")/$(md5_of "$ORIG_P115")"
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
        "$ORIG_P115|$P115_IN_CTR|p115"
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
