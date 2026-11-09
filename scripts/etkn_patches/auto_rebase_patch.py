#!/usr/bin/env python3
"""
ETKN 补丁自动 rebase 脚本 (v4: 支持 shared_pool / repository / watchlist / p115_workflow / center_control 五补丁)
每次 ETKN 镜像升级后运行，自动将我们的改动按锚点打到新镜像的原始文件上并更新基准与边车。

改动清单：
  1. shared_pool.py:
     - 全局 session.request 走 7890 代理 + verify=False + urllib3 警告静音 + 读超时钳 max(t, 45)
     - 字幕 session.get 走 7890 代理 + verify=False + urllib3 警告静音 + timeout 30->45
  2. repository.py:
     - summary 查询优化: library_base AS MATERIALIZED 预聚合 + 等值 hash join（替代已废弃的临时表方案）
  4. p115_workflow.py（由 gen_p115_organize_patch.py 生成）:
     - _organize() 结果瘦身：rejected 压成 {name, external_file_id, reason, rejection_kind}，
       序列化后 >24MB 则整体丢弃只留 rejected_count，避免 complete_step 5 倍写放大撑爆 libpq
  5. center_control.py（由 gen_center_control_patch.py 生成）:
     - 控制长连接退避上限 120s → 3s；断线/RPC 失败记录 kind/code/reason/stage；
       去掉每 15s 取消一次 recv 的内层超时。配合 shared_pool 连接复用，保证中心下发的
       签名探测（3s 预算）不再因握手+DNS 开销超时，根治「签名能力状态 异常」
  3. watchlist.py:
     - 启动阶段动态提升 maintenance 运行时优先级至 4（与 background 同级），根治 115 物理删除与共享登记饿死
     - reconcile_library_state 查询优化: 使用 ep_by_season 预聚合与 media_item_id 外键直连，单次更新从 75 分钟降至 0.2 秒
"""
import subprocess, sys, hashlib, re, os, datetime, tempfile, shutil, py_compile, time

ETKN_DIR = '/vol1/1000/docker/etkn'
ETKN_IMAGE_TAG = 'hbq0405/etkn:latest'
CHECK_SH = '/vol1/1000/docker/etkn-monitor/scripts/etkn-patch-check.sh'


def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f'[{ts}] {msg}', flush=True)


def get_image_id():
    # 容器在崩溃重启/被重建的瞬间 docker inspect 会失败，这里必须重试并回退到镜像名，
    # 否则会把 image_id 变成字符串 'unknown'，后续 docker run unknown:latest 直接 pull denied。
    for _ in range(6):
        r = subprocess.run(
            ['docker', 'inspect', 'etkn', '--format', '{{.Image}}'],
            capture_output=True, text=True, timeout=10
        )
        value = r.stdout.strip()
        if r.returncode == 0 and value.startswith('sha256:'):
            return value[:20]
        time.sleep(2)
    r = subprocess.run(
        ['docker', 'image', 'inspect', ETKN_IMAGE_TAG, '--format', '{{.Id}}'],
        capture_output=True, text=True, timeout=10
    )
    value = r.stdout.strip()
    if r.returncode == 0 and value.startswith('sha256:'):
        return value[:20]
    raise RuntimeError('无法确定 ETKN 运行镜像（container etkn / %s 都取不到）' % ETKN_IMAGE_TAG)


def extract_from_image(mount_path: str) -> str:
    r = subprocess.run(
        ['docker', 'run', '--rm', '--entrypoint', 'cat', ETKN_IMAGE_TAG, mount_path],
        capture_output=True, text=True, timeout=60
    )
    if r.returncode != 0:
        raise RuntimeError(f'提取 {mount_path} 失败: {r.stderr.strip()[:200]}')
    return r.stdout


def _refresh_orig(mount_path: str, dest: str, image_id: str | None = None) -> str:
    """按当前镜像（优先 image_id，避免 tag 漂移）刷新 .orig 基准文件。

    生成器风格的 rebase 函数都读 .orig，基准必须来自正在运行的镜像，否则镜像更新后
    补丁会基于旧原版生成，把上游修复静默盖掉（upstream_* 自检项就是为此设的）。
    """
    ref = image_id or ETKN_IMAGE_TAG
    r = subprocess.run(
        ['docker', 'run', '--rm', '--entrypoint', 'cat', ref, mount_path],
        capture_output=True, text=True, timeout=90
    )
    if r.returncode != 0 or not r.stdout.strip():
        raise RuntimeError(f'从镜像 {ref} 提取 {mount_path} 失败: {(r.stderr or "")[:200]}')
    content = r.stdout
    with open(dest, 'w') as f:
        f.write(content)
    log(f'  基准已刷新 {os.path.basename(dest)} ({len(content)} 字符, 源 {ref[:19]})')
    return content


def validate_python_code(content: str):
    with tempfile.NamedTemporaryFile(suffix='.py', mode='w', delete=False) as f:
        f.write(content)
        fname = f.name
    try:
        py_compile.compile(fname, doraise=True)
    finally:
        os.unlink(fname)


def write_patch_and_sidecar(patch_path: str, content: str):
    with open(patch_path, 'w') as f:
        f.write(content)
    md5 = hashlib.md5(content.encode()).hexdigest()
    sidecar = patch_path + '.md5'
    with open(sidecar, 'w') as f:
        f.write(md5 + '\n')
    log(f'✅ {os.path.basename(patch_path)} 已更新 (md5={md5}, 行数={content.count(chr(10))})')
    return md5


CENTER_SESSION_HELPER = '''
# [etkn-fix 2026-10-25] 共享中心 HTTP 连接复用
# 原实现每个请求都 requests.Session()，等于每次重做一遍 TCP+TLS 握手；实测单次中位 670ms，
# 握手占了大头。控制通道处理一次 RPC 要连打 GET+POST 两次，而中心下发的签名探测预算只有
# 3 秒（share_heartbeat_meta_json.rapid_sign_probe.latency_ms 正常已是 1523ms），
# 握手开销直接吃掉一半预算，链路一抖就探测超时 → 前端「签名能力状态 异常」，
# 服务端则成簇打印「中心控制任务未完成，将由原任务重试」。
# 改为线程内复用连接（10 分钟强制换新，避免中间设备静默断连留下死连接）。
_CENTER_SESSION = threading.local()


def _center_session():
    holder = _CENTER_SESSION
    now = time.time()
    session = getattr(holder, "session", None)
    if session is not None and now - getattr(holder, "born", 0.0) < 600:
        return session
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=16)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    holder.session = session
    holder.born = now
    return session

'''


# 需要时设 EM_SHARED_PROXY=http://<路由器>:7890；本机由宿主脚本自行提供，仓库版不含任何私有地址。
SHARED_PROXY = os.environ.get('EM_SHARED_PROXY', '')


def _proxy_line() -> str:
    if not SHARED_PROXY:
        return ''
    return ("        session.proxies = {'http': '%s', 'https': '%s'}\n" % (SHARED_PROXY, SHARED_PROXY))


def rebase_shared_pool(image_id: str):
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/shared_pool.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'shared_pool.py.orig'), 'w') as f:
        f.write(orig)

    lines = orig.splitlines(keepends=True)
    ANCHOR1_A = 'session = requests.Session()'
    ANCHOR1_B = 'session.trust_env = False'
    ANCHOR1_C = 'timeout = (self.connect_timeout_seconds, timeout[1])'

    PATCH1 = (
        '        # [etkn-fix 2026-09-10] 共享中心经 7890 出网并关闭SSL校验(忽略自签证书)。\n'
        '        # 依据: 中心端域名在 Open-Box 有直连规则, 出口为路由器公网。\n'
        "        session.verify = False\n"
        + _proxy_line() +
        '        import urllib3 as _urllib3\n'
        '        _urllib3.disable_warnings(_urllib3.exceptions.InsecureRequestWarning)\n'
        '        # [etkn-fix 2026-09-10] 读超时下限钳到 45s 吸收链路抖动。\n'
        '        timeout = (self.connect_timeout_seconds, max(timeout[1], 45))\n'
    )

    ANCHOR2_B = 'session.trust_env = False'
    SUBTITLE_TIMEOUT = "(self.connect_timeout_seconds, 30)"

    PATCH2 = (
        '        # [etkn-fix 2026-09-10] 共享中心经 7890 出网并关闭SSL校验(忽略自签证书)。\n'
        '        # 依据: 中心端域名在 Open-Box 有直连规则, 出口为路由器公网。\n'
        "        session.verify = False\n"
        + _proxy_line() +
        '        import urllib3 as _urllib3\n'
        '        _urllib3.disable_warnings(_urllib3.exceptions.InsecureRequestWarning)\n'
    )

    session_blocks = []
    for i, line in enumerate(lines):
        if ANCHOR1_B in line:
            has_timeout = any(ANCHOR1_C in lines[j] for j in range(i+1, min(len(lines), i+5)))
            session_blocks.append((i, has_timeout))

    if len(session_blocks) < 2:
        raise RuntimeError(f'shared_pool: 找不到足够的锚点（找到 {len(session_blocks)} 处，需要 ≥2）')

    block1 = next((b for b in session_blocks if b[1]), None)
    block2 = next((b for b in session_blocks if not b[1]), None)
    if not block1 or not block2:
        raise RuntimeError('shared_pool: 无法区分两处 session 块')

    out = []
    i = 0
    patched1 = patched2 = False
    while i < len(lines):
        line = lines[i]
        if i == block1[0] and not patched1:
            # [etkn-fix 2026-10-25] 控制通道改用线程内复用的 Session，避免每次请求重做
            # TCP+TLS 握手（实测中位 670ms，握手占大头），把 3 秒的签名探测预算留出来。
            if out and out[-1].strip() == 'session = requests.Session()':
                out[-1] = '        session = _center_session()\n'
            out.append(line)
            out.append(PATCH1)
            patched1 = True
            i += 1
            if i < len(lines) and ANCHOR1_C in lines[i]:
                i += 1
            continue
        elif i == block2[0] and not patched2:
            out.append(line)
            out.append(PATCH2)
            patched2 = True
            i += 1
            continue
        elif patched2 and SUBTITLE_TIMEOUT in line:
            out.append(line.replace(SUBTITLE_TIMEOUT, '(self.connect_timeout_seconds, 45)'))
            i += 1
            continue
        else:
            out.append(line)
            i += 1

    if not patched1 or not patched2:
        raise RuntimeError('shared_pool: 补丁未完全应用')

    content = ''.join(out)
    content = content.replace('logger = logging.getLogger(',
                              CENTER_SESSION_HELPER + 'logger = logging.getLogger(', 1)
    # [etkn-fix 2026-10-25] _request 的 finally 里有 session.close()，会销毁连接池，
    # 导致「复用」名存实亡（实测每请求仍是完整握手）。只摘掉第一处（_request 内），
    # 字幕下载处的 session 保持原样。
    close_snippet = '        finally:\n            session.close()\n'
    if content.count(close_snippet) < 1:
        raise RuntimeError('shared_pool: 找不到 finally: session.close() 锚点')
    content = content.replace(
        close_snippet,
        '        finally:\n'
        '            # [etkn-fix 2026-10-25] 连接复用：此处不能 close，否则连接池被销毁、复用作废。\n'
        '            # 复用与换新统一由 _center_session() 管理（10 分钟强制换新）。\n'
        '            pass\n',
        1)
    validate_python_code(content)
    if content.count('7890') < 4 or 'max(timeout[1], 45)' not in content:
        raise RuntimeError('shared_pool: 关键校验失败')
    if '_center_session()' not in content:
        raise RuntimeError('shared_pool: 连接复用补丁未应用')
    if content.count('session.close()') != 2:
        raise RuntimeError(f'shared_pool: session.close() 应为 2 处（字幕/其他），实际 {content.count("session.close()")}')

    ts = datetime.datetime.now().strftime('%Y-%m-%d')
    header = (
        f'# [etkn-patch auto-rebase {ts}] 自动 rebase 到镜像 {image_id} (v1.3.x latest)\n'
        '# 本文件由 auto_rebase_patch.py 生成，请勿手动修改注释行以外的内容\n'
        '# 改动：① request处 verify=False+7890+max(45) ② 字幕处 verify=False+7890+timeout45\n'
    )
    final = header + content
    patch_path = os.path.join(ETKN_DIR, 'shared_pool_patch.py')
    write_patch_and_sidecar(patch_path, final)



LSC_HEAD = """                WITH {demand_rollups},
                library_items AS (
                    SELECT item.id,"""

LSC_TAIL = "                SELECT count(*) FILTER ("

LSC_NEW = """                WITH {demand_rollups},
                library_base AS MATERIALIZED (
                    SELECT item.id,
                           item.item_type,
                           item.parent_id,
                           state.requires_washing,
                           coalesce(series_external.provider, external.provider) AS join_provider,
                           CASE
                               WHEN item.item_type = 'season' THEN series_external.external_id
                               ELSE external.external_id
                           END AS join_external_id,
                           coalesce(
                               CASE
                                   WHEN item.item_type = 'season' THEN coalesce(
                                       CASE
                                           WHEN coalesce(snapshot.payload ->> 'season_number', '') ~ '^[0-9]+$'
                                           THEN (snapshot.payload ->> 'season_number')::integer
                                       END,
                                       NULLIF(substring(external.external_id FROM ':S([0-9]+)$'), '')::integer
                                   )
                               END,
                               -1
                           ) AS join_season
                    FROM media.items item
                    JOIN media.external_ids external
                      ON external.item_id = item.id
                     AND external.provider = 'tmdb'
                     AND external.entity_type = item.item_type
                    LEFT JOIN media.item_metadata snapshot
                      ON snapshot.item_id = item.id AND snapshot.is_current
                    LEFT JOIN media.external_ids series_external
                      ON series_external.item_id = item.parent_id
                     AND series_external.provider = 'tmdb'
                     AND series_external.entity_type = 'series'
                    LEFT JOIN media.item_washing_states state ON state.item_id = item.id
                    WHERE item.is_active
                      AND item.item_type IN ('movie', 'season')
                ),
                library_items AS (
                    SELECT base.id,
                           base.item_type,
                           base.parent_id,
                           coalesce(base.requires_washing, FALSE) AS requires_washing,
                           coalesce(demand.has_active_request, FALSE) AS has_active_request
                    FROM library_base base
                    LEFT JOIN demand_rollups demand
                      ON demand.provider = base.join_provider
                     AND demand.entity_type = base.item_type
                     AND demand.media_external_id = base.join_external_id
                     AND coalesce(demand.season_number, -1) = base.join_season
                )
"""


def optimize_library_status_counts(code: str) -> str:
    """把 library_status_counts 的相关子查询改成 MATERIALIZED 基表 + 等值 hash join。

    原实现用 demand.season_number IS NOT DISTINCT FROM <复杂表达式> 做 merge join filter，
    34,777 行 × 约 7 个候选 → 每次 overview 8 秒以上（实测 8.83s）。
    改后 CTE 物化出三个纯列 join key，合并成单次 hash join（实测 1.32s）。
    """
    marker = "def library_status_counts"
    if marker not in code:
        raise RuntimeError("repository: 找不到 library_status_counts")
    i = code.index(marker)
    start = code.find(LSC_HEAD, i)
    if start < 0:
        raise RuntimeError("repository: library_status_counts 里找不到 library_items 锚点")
    end = code.find(LSC_TAIL, start)
    if end < 0:
        raise RuntimeError("repository: library_status_counts 里找不到 SELECT count(*) FILTER 锚点")
    out = code[:start] + LSC_NEW + code[end:]
    if "library_base AS MATERIALIZED" not in out:
        raise RuntimeError("repository: library_status_counts 优化未生效")
    return out


REPO_OPT_DIFF = os.path.join(ETKN_DIR, 'repository_optimizations.diff')
REPO_OPT_MARKERS = (
    '_materialize_library_demand_index',
    '_materialize_library_media_meta',
    '_materialize_season_episode_flags',
    'dm_by_item',
    'se_flags',
    'season_keys',
)


def apply_repository_optimizations(code: str) -> str:
    """把订阅列表优化补丁打到 base 代码上。

    单一真源 = ETKN_DIR/repository_optimizations.diff。上游一旦改动到相关 SQL，
    patch 会失败并抛异常（宁可红，也不能像 10-24/10-25 两次那样静默回到 17s/37s）。
    """
    if not os.path.exists(REPO_OPT_DIFF):
        raise RuntimeError(f'repository: 缺少优化补丁 {REPO_OPT_DIFF}')
    workdir = tempfile.mkdtemp(prefix='etkn-repo-rebase-')
    try:
        path = os.path.join(workdir, 'repository.py')
        with open(path, 'w', encoding='utf-8') as fh:
            fh.write(code)
        res = subprocess.run(
            ['patch', '--batch', '--forward', '-p0', '-i', REPO_OPT_DIFF],
            cwd=workdir, capture_output=True, text=True,
        )
        if res.returncode != 0:
            raise RuntimeError(
                'repository: 优化补丁应用失败（上游可能改动了相关 SQL，请更新 repository_optimizations.diff）:\n'
                + (res.stdout or '') + (res.stderr or '')
            )
        with open(path, encoding='utf-8') as fh:
            out = fh.read()
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    missing = [marker for marker in REPO_OPT_MARKERS if marker not in out]
    if missing:
        raise RuntimeError('repository: 优化未全部生效，缺少标记: ' + ', '.join(missing))
    return out


def rebase_repository(image_id: str):
    """repository 补丁：三方合并到当前镜像原版（生成器负责刷新基准/合并/校验/写边车）。

    2026-10-26 之前这里是 extract → optimize_library_status_counts → patch diff，
    upstream 1.3.68 把同样的 SQL 段改写成 inline coalesce 后，patch 直接报
    "Reversed or previously applied"，我们误以为"上游已内建"就停用了补丁，
    结果订阅中心从 1.3s/7.3s 退化到 22s/39s。改用 gen_repository_patch.py 的
    三方合并（base/orig/ours），冲突只在"我们整段取代上游连接条件"时自动收口。
    """
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/subscription/repository.py'
    _refresh_orig(mount_path, os.path.join(ETKN_DIR, 'repository.py.orig'), image_id)
    script = os.path.join(ETKN_DIR, 'gen_repository_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-6:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('repository 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:800])
    if not os.path.exists(os.path.join(ETKN_DIR, 'repository_patch.py.md5')):
        raise RuntimeError('repository_patch.py.md5 未生成')


def rebase_watchlist(image_id: str):
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/subscription/watchlist.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'watchlist.py.orig'), 'w') as f:
        f.write(orig)

    prio_code = '''
# 2026-10-23 根治：将 maintenance 运行时优先级提升至与 background 同级 (rank 4)
# 彻底解决 DeepDeleteRuntime(115物理删除)、shared_registration(共享登记)被常规后台任务完全饿死问题
try:
    import etk_vnext.runtime_priority as _rp
    if _rp._PRIORITIES.get("maintenance") == 5:
        _rp._PRIORITIES["maintenance"] = 4
        _rp._NICE_TARGETS["maintenance"] = 10
        logger.info("WatchlistPatch: promoted maintenance runtime priority from 5 to 4")
except Exception as _exc:
    logger.warning("WatchlistPatch: failed to adjust runtime priority: %s", _exc)
'''
    anchor = 'WATCHING_STATUSES = {"Watching", "Paused", "Pending", "Completed"}\n'
    if anchor not in orig:
        raise RuntimeError('watchlist: 找不到 WATCHING_STATUSES 锚点')
    patched = orig.replace(anchor, anchor + prio_code)

    old_cte_anchor = '                WITH local_counts AS ('
    old_sql_tail = ') live ON true\n                )'
    if old_cte_anchor not in patched or old_sql_tail not in patched:
        raise RuntimeError('watchlist: 找不到 local_counts CTE 锚点')

    new_cte = '''                WITH ep_by_season AS (
                    SELECT parent_id AS season_id,
                           count(id) FILTER (WHERE is_active)::int AS local_count
                    FROM media.items
                    WHERE item_type = 'episode'
                    GROUP BY parent_id
                ),
                local_counts AS (
                    SELECT ws.id,
                           ms.is_active AS series_in_library,
                           COALESCE(
                               ep.local_count,
                               fallback.local_count,
                               0
                           )::int AS local_count
                    FROM subscription.watchlist_seasons ws
                    JOIN subscription.watchlist_series w ON w.id = ws.series_id
                    LEFT JOIN media.external_ids se
                      ON se.provider = 'tmdb'
                     AND se.external_id = w.tmdb_id
                     AND se.entity_type IN ('series', 'tv')
                    LEFT JOIN media.items ms
                      ON ms.id = se.item_id
                     AND ms.item_type = 'series'
                    LEFT JOIN ep_by_season ep
                      ON ep.season_id = ws.media_item_id
                    LEFT JOIN LATERAL (
                        SELECT count(e.id) FILTER (WHERE e.is_active)::int AS local_count
                        FROM media.items s
                        JOIN media.item_metadata meta ON meta.item_id = s.id AND meta.is_current
                        JOIN media.items e ON e.parent_id = s.id AND e.item_type = 'episode'
                        WHERE ws.media_item_id IS NULL
                          AND s.parent_id = ms.id
                          AND s.item_type = 'season'
                          AND COALESCE(
                              NULLIF(meta.payload ->> 'season_number', ''),
                              NULLIF(meta.payload ->> 'season', '')
                          )::integer = ws.season_number
                    ) fallback ON ws.media_item_id IS NULL
                )'''

    start_idx = patched.index(old_cte_anchor)
    end_idx = patched.index(old_sql_tail, start_idx) + len(old_sql_tail)
    patched = patched[:start_idx] + new_cte + patched[end_idx:]

    validate_python_code(patched)
    patch_path = os.path.join(ETKN_DIR, 'watchlist_patch.py')
    write_patch_and_sidecar(patch_path, patched)


def rebase_p115_workflow(image_id: str):
    """第 4 个补丁：p115_workflow.py 整理结果瘦身（生成器负责抽取/断言/校验/写边车）。"""
    # [2026-10-26] 生成器读的是 p115_workflow.py.orig，必须先按当前镜像刷新基准，
    # 否则镜像更新后补丁会基于旧原版生成，把上游修复静默盖掉。
    _refresh_orig('/usr/local/lib/python3.12/site-packages/etk_vnext/modules/organize/p115_workflow.py',
                  os.path.join(ETKN_DIR, 'p115_workflow.py.orig'), image_id)
    script = os.path.join(ETKN_DIR, 'gen_p115_organize_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=600)
    for ln in (r.stdout or '').strip().splitlines()[-5:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('p115_workflow 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'p115_workflow_patch.py.md5')):
        raise RuntimeError('p115_workflow_patch.py.md5 未生成')


def rebase_ai_service(image_id: str):
    """第 13 个补丁：ai_translation/service.py 的 AI 调用熔断（坏通道把批量整理拖成小时级卡死）。"""
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/ai_translation/service.py'
    _refresh_orig(mount_path, os.path.join(ETKN_DIR, 'ai_service.py.orig'), image_id)

    script = os.path.join(ETKN_DIR, 'gen_ai_service_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-5:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('ai_service 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'ai_service_patch.py.md5')):
        raise RuntimeError('ai_service_patch.py.md5 未生成')


def rebase_center_control(image_id: str):
    """第 5 个补丁：center_control.py 控制长连接健壮性（退避压到 3s + 断线原因可诊断）。"""
    # [2026-10-26] 同 p115_workflow：生成器读 center_control.py.orig，必须先按当前镜像刷新基准。
    _refresh_orig('/usr/local/lib/python3.12/site-packages/etk_vnext/integrations/center_control.py',
                  os.path.join(ETKN_DIR, 'center_control.py.orig'), image_id)
    script = os.path.join(ETKN_DIR, 'gen_center_control_patch_v2.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-5:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('center_control 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'center_control_patch.py.md5')):
        raise RuntimeError('center_control_patch.py.md5 未生成')


def rebase_play_pool(image_id: str):
    """第 6 个补丁：play_pool.py 小号转播跨播放会话复用（同文件复播 3.2s → 0.03~0.6s）。

    上游 _find_reusable_session() 里，PlaySessionId 不同就 continue；而 Emby/Hills 每次起播
    都生成新的 PlaySessionId，会话里存的是上一次的，两者永不相等 → 同一文件每次播放都
    重走 115 秒传 + 主号签名（实测冷启 3.2s，命中 31~44ms）。
    """
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/playback/play_pool.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'play_pool.py.orig'), 'w') as f:
        f.write(orig)

    script = os.path.join(ETKN_DIR, 'gen_play_pool_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-5:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('play_pool 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'play_pool_patch.py.md5')):
        raise RuntimeError('play_pool_patch.py.md5 未生成')


def rebase_screenshots(image_id: str):
    """第 7 个补丁：screenshots.py 截图范围解析缓存（catalog.item() 每次白付 1.5s）。"""
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/scrape/screenshots.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'screenshots.py.orig'), 'w') as f:
        f.write(orig)

    script = os.path.join(ETKN_DIR, 'gen_screenshots_patch_v2.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-5:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('screenshots 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'screenshots_patch.py.md5')):
        raise RuntimeError('screenshots_patch.py.md5 未生成')


def rebase_shared_register(image_id: str):
    """第 11 个补丁：platform/tasks/handlers.py 的共享登记 409 幂等（追剧刷新每轮必红的根因）。"""
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/platform/tasks/handlers.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'handlers.py.orig'), 'w') as f:
        f.write(orig)

    script = os.path.join(ETKN_DIR, 'gen_shared_register_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script, image_id], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-5:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('shared_register 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    sidecar = os.path.join(ETKN_DIR, 'shared_register_patch.py.md5')
    if not os.path.exists(sidecar):
        raise RuntimeError('shared_register_patch.py.md5 未生成')

    # handlers.py 是 3.12 语法，宿主 3.11 编译不了 → 在容器镜像里做语法校验
    chk = extract_from_image('/usr/local/lib/python3.12/site-packages/etk_vnext/platform/tasks/handlers.py')
    del chk  # 仅确认可抽取


def rebase_virtual_library(image_id: str):
    """第 8 个补丁：virtual_library/service.py 筛选项 SQL 聚合 + 缓存（弹窗 27s 的元凶）。"""
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/virtual_library/service.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'virtual_library_service.py.orig'), 'w') as f:
        f.write(orig)

    script = os.path.join(ETKN_DIR, 'gen_virtual_library_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-5:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('virtual_library 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'virtual_library_service_patch.py.md5')):
        raise RuntimeError('virtual_library_service_patch.py.md5 未生成')


def rebase_nginx_proxy(image_id: str):
    """第 17 个补丁：virtual_library/nginx_proxy.py（etkn-fix19 2026-11-09）。

    1.4.3 只对外开 8097，nginx 模板里 `location = / { return 302 /web/; }` 被 nginx
    绝对化后变成 http://<内网IP>:8097/web/，经反代出去就是 https://<你的域名>:8097/web/
    （公网 8097 不通）→ 根域名打不开。修法：server 块加 absolute_redirect off / port_in_redirect off。
    """
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/virtual_library/nginx_proxy.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'nginx_proxy.py.orig'), 'w') as f:
        f.write(orig)

    script = os.path.join(ETKN_DIR, 'gen_nginx_proxy_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-5:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('nginx_proxy 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'nginx_proxy_patch.py.md5')):
        raise RuntimeError('nginx_proxy_patch.py.md5 未生成')


def rebase_placeholders(image_id: str):
    """第 9 个补丁：virtual_library/placeholders.py 榜单占位符查询（1262 条榜单 >240s → 1.4s）。"""
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/virtual_library/placeholders.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'placeholders.py.orig'), 'w') as f:
        f.write(orig)

    script = os.path.join(ETKN_DIR, 'gen_placeholders_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-5:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('placeholders 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'placeholders_patch.py.md5')):
        raise RuntimeError('placeholders_patch.py.md5 未生成')


def rebase_media_server(image_id: str):
    """第 10 个补丁：api/media_server.py 榜单库（占位条目）列表页改用紧凑行（5s/页 → 1s 级）。"""
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/api/media_server.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'media_server.py.orig'), 'w') as f:
        f.write(orig)

    script = os.path.join(ETKN_DIR, 'gen_media_server_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-5:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('media_server 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'media_server_patch.py.md5')):
        raise RuntimeError('media_server_patch.py.md5 未生成')


def rebase_catalog(image_id: str):
    """第 11 个补丁：modules/media_server/catalog.py 两处「规划器悬崖」慢查询。

    ① child_count()：`EXISTS OR EXISTS` 退化成 items×item_versions 全量 Hash Join（0.4s/条，
       列表 60 条 22.6s → library-21/26 打不开）② collections() 系：成员数逐行相关子查询，
       1037 个合集 36.2s（客户端每次取 Views 卡住）。改为 LATERAL 索引探测 + 一次聚合。
    """
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/media_server/catalog.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'catalog.py.orig'), 'w') as f:
        f.write(orig)

    script = os.path.join(ETKN_DIR, 'gen_catalog_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-8:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('catalog 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'catalog_patch.py.md5')):
        raise RuntimeError('catalog_patch.py.md5 未生成')


def rebase_media_dedup(image_id: str):
    """第 14 个补丁：modules/media_dedup/service.py「指定媒体库」范围 + 大结果集内存优化。

    ① fix18：主查询不再取分析大字段（353k 行 × 33KB ≈ 11GB，会把容器 OOM），改为分组后按需补取。
    ② fix19：「指定媒体库」配置之前读了却从未使用 → 设了范围仍然全库扫描、跨库判重
       （4094 组，3.5 分钟）；现在解析成 media.items.id 集合并写进 WHERE
       （范围=1 个库时 13945 项 24 秒）。
    """
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/media_dedup/service.py'
    _refresh_orig(mount_path, os.path.join(ETKN_DIR, 'media_dedup_service.orig'), image_id)

    script = os.path.join(ETKN_DIR, 'gen_media_dedup_patch_v2.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-8:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('media_dedup 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'media_dedup_patch.py.md5')):
        raise RuntimeError('media_dedup_patch.py.md5 未生成')


def rebase_deep_delete(image_id: str):
    """第 15 个补丁：modules/emby_events/deep_delete.py 领取队列不再逐行回填 GB 级 payload。

    联动删除 386 项时，due_delete_files 的 RETURNING 把 5MB 的 snapshot.payload 挂在每一行上，
    合计 1.9GB JSONB，psycopg 展开成 Python 对象可达数 GB → mem-watch 在 9GB 处重启容器 →
    删除事务在 processing 被打断（attempt_count 还是 0）→ 下一轮重新领取同一批 → 死循环，
    任务中心永远「运行中」。改为 RETURNING queue.*，再按 token 一次性取回 payload。
    """
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/emby_events/deep_delete.py'
    _refresh_orig(mount_path, os.path.join(ETKN_DIR, 'deep_delete.py.orig'), image_id)

    script = os.path.join(ETKN_DIR, 'gen_deep_delete_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-8:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('deep_delete 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'deep_delete_patch.py.md5')):
        raise RuntimeError('deep_delete_patch.py.md5 未生成')


def rebase_tv_sdr(image_id: str):
    """第 16 个补丁：api/tv_transcode.py —— ETKN TV 播 HDR/DV 黑屏，服务端强制 SDR 转码。

    ETKN TV 0.1.x 在起播协商里声明 HevcHdr10 能力（服务端据此保留 HDR 直出、p010le 不 tone-map），
    但客户端从不请求 HDR 数据空间（logcat 无 u2EotfValue、无 bRequestingDataSpace），电视面板停在
    SDR → 有声音/字幕/弹幕、画面全黑；原画与降码率转码都黑。补丁对 UA 命中 ETKN TV 且片源为
    HDR/DV 的请求：user_bitrate 分支不再 DirectPlay + hdr_capabilities 置空强制 tone-map 出 SDR。
    """
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/api/tv_transcode.py'
    _refresh_orig(mount_path, os.path.join(ETKN_DIR, 'tv_transcode.py.orig'), image_id)

    script = os.path.join(ETKN_DIR, 'gen_tv_sdr_patch.py')
    if not os.path.exists(script):
        raise RuntimeError(f'缺生成器 {script}')
    r = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=300)
    for ln in (r.stdout or '').strip().splitlines()[-8:]:
        log('  ' + ln.strip()[:300])
    if r.returncode != 0:
        raise RuntimeError('tv_sdr 补丁生成失败: ' + ((r.stderr or r.stdout) or '').strip()[:600])
    if not os.path.exists(os.path.join(ETKN_DIR, 'tv_transcode_patch.py.md5')):
        raise RuntimeError('tv_transcode_patch.py.md5 未生成')


def _container_started_at() -> str:
    r = subprocess.run(['docker', 'inspect', 'etkn', '--format', '{{.State.StartedAt}}'],
                       capture_output=True, text=True, timeout=20)
    return (r.stdout or '').strip()


def restart_etkn():
    # 有挂载项变更时必须走 compose 重建；但 compose 在配置无变化时只会打印
    # "Container etkn  Running" 而**不重启**——此时进程里还是旧模块，自检却因为读文件
    # 而显示全绿（2026-10-26 踩到：rebase 重新生成了 center_control 补丁但没重启）。
    # 因此比较 StartedAt：没变就显式 docker restart。
    before = _container_started_at()
    r = subprocess.run(
        ['docker', 'compose', '-p', 'etkn', 'up', '-d', '--no-deps', 'etkn'],
        cwd=ETKN_DIR, capture_output=True, text=True, timeout=300,
    )
    if r.returncode != 0:
        log('  compose 重建失败，回退 docker restart: ' + ((r.stderr or r.stdout) or '').strip()[:400])
        subprocess.run(['docker', 'restart', 'etkn'], timeout=120)
        return
    after = _container_started_at()
    if after and after == before:
        log('  compose 未重建（配置无变化），改用 docker restart 让新补丁生效')
        subprocess.run(['docker', 'restart', 'etkn'], timeout=120)
    else:
        log(f'  compose 已重建容器（StartedAt {after}）')


def wait_ready(timeout: int = 180):
    """等容器真正可用：既要求能 exec，也要求 /api/health 返回 ok。

    只 exec md5sum 会在容器重启过程中就返回 True（旧进程还没完全退出），
    随后的自检于是读到旧状态，看起来像「rebase 成功但没生效」。
    """
    import time
    import urllib.request
    t0 = time.time()
    test_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/subscription/watchlist.py'
    exec_ok = False
    while time.time() - t0 < timeout:
        if not exec_ok:
            r = subprocess.run(['docker', 'exec', 'etkn', 'md5sum', test_path],
                               capture_output=True, text=True)
            exec_ok = r.returncode == 0
        if exec_ok:
            try:
                with urllib.request.urlopen('http://127.0.0.1:8097/api/health', timeout=3) as resp:
                    body = resp.read().decode('utf-8', 'ignore')
                if '"ok"' in body:
                    log(f'容器就绪（{int(time.time() - t0)}s，health ok）')
                    return True
            except Exception:
                pass
        time.sleep(3)
    log(f'⚠️ 容器 {timeout}s 内未就绪')
    return False


def final_selfcheck():
    r = subprocess.run(['sh', CHECK_SH, '--json'], capture_output=True, text=True, timeout=180)
    out = (r.stdout or '').strip()
    for ln in out.splitlines():
        log('  自检> ' + ln.strip()[:400])
    if r.returncode != 0:
        raise RuntimeError('rebase 完成但补丁自检仍未通过')
    log('✅ rebase 后自检通过')


def main():
    image_id = get_image_id()
    log(f'当前容器镜像: {image_id}')

    log('前置 0/14: 数据库运行参数哨兵（role/库级退化参数、jit）…')
    try:
        r = subprocess.run([sys.executable, '/vol1/1000/docker/etkn/db_setting_guard.py'],
                           capture_output=True, text=True, timeout=180)
        out = (r.stdout or '').strip()
        log('  参数> ' + (out[:400] or '(无输出)'))
        if r.returncode != 0:
            log('  ⚠️ 参数哨兵返回非零，继续后续 rebase')
    except Exception as exc:
        log(f'  ⚠️ 参数哨兵调用失败：{exc}')

    log('rebase 1/16: shared_pool.py …')
    rebase_shared_pool(image_id)

    log('rebase 2/16: repository.py …（三方合并：library_base 预计算 + dm_by_item）')
    rebase_repository(image_id)

    log('rebase 3/16: watchlist.py …')
    rebase_watchlist(image_id)

    log('rebase 4/16: p115_workflow.py …')
    rebase_p115_workflow(image_id)

    log('rebase 5/16: center_control.py …')
    rebase_center_control(image_id)

    log('rebase 6/16: play_pool.py —— 镜像 b5fffdcc 上游已内建跨会话复用，本补丁停用，跳过')

    log('rebase 7/16: screenshots.py …')
    rebase_screenshots(image_id)

    log('rebase 8/16: virtual_library/service.py …')
    rebase_virtual_library(image_id)

    log('rebase 8b/16: virtual_library/nginx_proxy.py …（反代根路径跳转不带内网端口）')
    rebase_nginx_proxy(image_id)

    log('rebase 9/16: virtual_library/placeholders.py …')
    rebase_placeholders(image_id)

    log('rebase 10/16: api/media_server.py …')
    rebase_media_server(image_id)

    log('rebase 11/16: modules/media_server/catalog.py …（child_count / collections 规划器悬崖）')
    rebase_catalog(image_id)

    log('rebase 12/16: platform/tasks/handlers.py …')
    rebase_shared_register(image_id)

    log('rebase 13/16: ai_translation/service.py …（AI 调用熔断）')
    rebase_ai_service(image_id)

    # rebase 14/16（media_dedup / 指定媒体库范围）已于 2026-11-09 退役：
    # 上游 1.3.78 已用 pg_temp + 服务端游标解决大结果集内存，1.4.3 原生实现「指定媒体库」
    # （_configured_library_ids + _library_scope_clause），无需再打补丁。函数 rebase_media_dedup 保留做存档。
    # rebase 14/16: rebase_media_dedup(image_id)

    log('rebase 15/16: modules/emby_events/deep_delete.py …（联动删除队列不再逐行回填 GB 级 payload）')
    rebase_deep_delete(image_id)

    # rebase 16/16（tv_transcode / etkn-fix17 强制 SDR）已于 2026-11-09 按用户要求退役：
    # 原画必须直连不转码，黑屏改由电视端手动规避。函数 rebase_tv_sdr 保留做存档。
    log('重启 etkn 容器 …')
    restart_etkn()
    wait_ready()

    log('运行补丁自检 …')
    final_selfcheck()

    log('🎉 全量补丁自动 rebase 成功完成')
    return {'ok': True, 'image_id': image_id}


if __name__ == '__main__':
    try:
        main()
        sys.exit(0)
    except Exception as e:
        log(f'❌ 失败: {e}')
        sys.exit(1)
