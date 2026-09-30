#!/bin/sh
# etkn-patch-check.sh — ETKN bind-mount 补丁自检（在 NAS 宿主机上跑，需要 docker CLI）
#
# 为什么要这个：ETKN 本体是 vendor 镜像 hbq0405/etkn:latest，飞牛上没有源码工程，
# 我们改代码的唯一路子是「往 docker-compose.yml 里追加 bind-mount 覆盖 vendor 模块」。
# 于是每次更新镜像都可能踩两个坑：
#   ① compose 被重写 → mount 行丢了 → 补丁静默失效（fulfilled 退回 ~22s，功能不坏但变慢）
#   ② 镜像里上游改了同一个文件 → 我们的补丁是「整文件副本」→ 会把新版盖回去，
#      上游的修复/新功能静默丢失（这个更危险）
# 本脚本就是把这两件事查出来。
#
# 退出码：0 = 全部正常；1 = 有问题（详见输出里的 problems[]）
# 用法：etkn-patch-check.sh [--json|--text]     默认 --json

ETKN_DIR=${ETKN_DIR:-/vol1/1000/docker/etkn}
MON_DIR=${MON_DIR:-/vol1/1000/docker/etkn-monitor}
COMPOSE=${COMPOSE:-$ETKN_DIR/docker-compose.yml}
PATCH_REPO=${PATCH_REPO:-$ETKN_DIR/repository_patch.py}
PATCH_POOL=${PATCH_POOL:-$ETKN_DIR/shared_pool_patch.py}
ORIG_REPO=${ORIG_REPO:-$ETKN_DIR/repository.py.orig}
STATE=${STATE:-$MON_DIR/data/patch-check.state}
IMAGE=${IMAGE:-hbq0405/etkn:latest}
CTR=${CTR:-etkn}

# 期望值：repository_patch.py 的 md5（换补丁时必须同步改这里）
EXPECT_REPO=${EXPECT_REPO:-d08462056b43ef56793490b181286e7a}

REPO_IN_CTR=/usr/local/lib/python3.12/site-packages/etk_vnext/modules/subscription/repository.py
POOL_IN_CTR=/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/shared_pool.py

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
if [ "${MOUNT_REPO_LINE:-0}" -ge 1 ] && [ "${MOUNT_POOL_LINE:-0}" -ge 1 ]; then
    add mount_lines 0 "compose 两行 bind-mount 均在（repository + shared_pool）"
elif [ "${MOUNT_REPO_LINE:-0}" -ge 1 ]; then
    add mount_lines 1 "compose 缺少 shared_pool_patch.py 的 mount 行（补丁可能已部分失效）"
elif [ "${MOUNT_POOL_LINE:-0}" -ge 1 ]; then
    add mount_lines 1 "compose 缺少 repository_patch.py 的 mount 行 → fulfilled 性能补丁已失效"
else
    add mount_lines 1 "compose 两行 bind-mount 全部缺失 → compose 可能被重写过，补丁全部失效"
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

# ---------- ⑤ 上游是否改过同一个文件（需不需要 rebase） ----------
# docker create + docker cp 有点重，按镜像 ID 缓存：镜像没变就复用上次结论
IMG_ID="$(docker image inspect "$IMAGE" --format '{{.Id}}' 2>/dev/null)"
IMG_CREATED="$(docker image inspect "$IMAGE" --format '{{.Created}}' 2>/dev/null)"
CACHED_ID=""; CACHED_VERDICT=""
if [ -f "$STATE" ]; then
    CACHED_ID="$(sed -n 's/^image_id=//p' "$STATE" | head -1)"
    CACHED_VERDICT="$(sed -n 's/^rebase_verdict=//p' "$STATE" | head -1)"
fi

if [ -n "$IMG_ID" ] && [ "$IMG_ID" = "$CACHED_ID" ] && [ -n "$CACHED_VERDICT" ]; then
    VERDICT="$CACHED_VERDICT"
    add upstream_rebase_note 0 "镜像未变（${IMG_ID#sha256:} 前 12 位），沿用上次结论"
else
    TMPD="$(mktemp -d 2>/dev/null || echo /tmp/etkn-pc.$$)"
    mkdir -p "$TMPD"
    CNAME="etkn-patchcheck-$$"
    docker create --name "$CNAME" "$IMAGE" >/dev/null 2>&1
    docker cp "$CNAME:$REPO_IN_CTR" "$TMPD/repo_new.py" >/dev/null 2>&1
    docker rm -f "$CNAME" >/dev/null 2>&1
    if [ ! -s "$TMPD/repo_new.py" ]; then
        VERDICT="unknown"
        add upstream_rebase 1 "无法从镜像 $IMAGE 抽出原版 repository.py（docker create/cp 失败）"
    elif [ ! -s "$ORIG_REPO" ]; then
        VERDICT="unknown"
        add upstream_rebase 1 "缺基准文件 $ORIG_REPO，无法判断上游是否改过"
    elif cmp -s "$ORIG_REPO" "$TMPD/repo_new.py"; then
        VERDICT="ok"
        add upstream_rebase 0 "镜像内原版 repository.py 与基准 ${ORIG_REPO##*/} 一致 → 补丁可直接沿用"
    else
        VERDICT="rebase"
        _diffn="$(diff "$ORIG_REPO" "$TMPD/repo_new.py" 2>/dev/null | grep -c '^[<>]')"
        add upstream_rebase 1 "上游改过 repository.py（差异 ${_diffn} 行）→ 必须把补丁 rebase 到新版，否则上游修复会被旧文件盖掉"
    fi
    rm -rf "$TMPD" 2>/dev/null
    mkdir -p "$(dirname "$STATE")" 2>/dev/null
    {
        printf 'image_id=%s\n' "$IMG_ID"
        printf 'rebase_verdict=%s\n' "$VERDICT"
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
