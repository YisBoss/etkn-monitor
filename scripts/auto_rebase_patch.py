#!/usr/bin/env python3
"""
ETKN 补丁自动 rebase 脚本 (v2: 支持 shared_pool / repository / watchlist 三补丁)
每次 ETKN 镜像升级后运行，自动将我们的改动按锚点打到新镜像的原始文件上并更新基准与边车。

改动清单：
  1. shared_pool.py:
     - 全局 session.request 走 7890 代理 + verify=False + urllib3 警告静音 + 读超时钳 max(t, 45)
     - 字幕 session.get 走 7890 代理 + verify=False + urllib3 警告静音 + timeout 30->45
  2. repository.py:
     - summary 查询优化: _extract_rollup_body 提取 CTE，创建 TEMP TABLE dr_rollup 规避 1.12 亿代价慢查
  3. watchlist.py:
     - 启动阶段动态提升 maintenance 运行时优先级至 4（与 background 同级），根治 115 物理删除与共享登记饿死
     - reconcile_library_state 查询优化: 使用 ep_by_season 预聚合与 media_item_id 外键直连，单次更新从 75 分钟降至 0.2 秒
"""
import subprocess, sys, hashlib, re, os, datetime, tempfile, py_compile

ETKN_DIR = '/vol1/1000/docker/etkn'
ETKN_IMAGE_TAG = 'hbq0405/etkn:latest'
CHECK_SH = '/vol1/1000/docker/etkn-monitor/scripts/etkn-patch-check.sh'


def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f'[{ts}] {msg}', flush=True)


def get_image_id():
    r = subprocess.run(
        ['docker', 'inspect', 'etkn', '--format', '{{.Image}}'],
        capture_output=True, text=True, timeout=10
    )
    return r.stdout.strip()[:20] if r.returncode == 0 else 'unknown'


def extract_from_image(mount_path: str) -> str:
    r = subprocess.run(
        ['docker', 'run', '--rm', '--entrypoint', 'cat', ETKN_IMAGE_TAG, mount_path],
        capture_output=True, text=True, timeout=60
    )
    if r.returncode != 0:
        raise RuntimeError(f'提取 {mount_path} 失败: {r.stderr.strip()[:200]}')
    return r.stdout


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
        '        # 依据: Open-Box 对 shared.55565576.xyz 有直连规则, 出口为路由器公网。\n'
        "        session.verify = False\n"
        "        session.proxies = {'http': 'http://192.168.31.1:7890', 'https': 'http://192.168.31.1:7890'}\n"
        '        import urllib3 as _urllib3\n'
        '        _urllib3.disable_warnings(_urllib3.exceptions.InsecureRequestWarning)\n'
        '        # [etkn-fix 2026-09-10] 读超时下限钳到 45s 吸收链路抖动。\n'
        '        timeout = (self.connect_timeout_seconds, max(timeout[1], 45))\n'
    )

    ANCHOR2_B = 'session.trust_env = False'
    SUBTITLE_TIMEOUT = "(self.connect_timeout_seconds, 30)"

    PATCH2 = (
        '        # [etkn-fix 2026-09-10] 共享中心经 7890 出网并关闭SSL校验(忽略自签证书)。\n'
        '        # 依据: Open-Box 对 shared.55565576.xyz 有直连规则, 出口为路由器公网。\n'
        "        session.verify = False\n"
        "        session.proxies = {'http': 'http://192.168.31.1:7890', 'https': 'http://192.168.31.1:7890'}\n"
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
    validate_python_code(content)
    if content.count('7890') < 4 or 'max(timeout[1], 45)' not in content:
        raise RuntimeError('shared_pool: 关键校验失败')

    ts = datetime.datetime.now().strftime('%Y-%m-%d')
    header = (
        f'# [etkn-patch auto-rebase {ts}] 自动 rebase 到镜像 {image_id} (v1.3.x latest)\n'
        '# 本文件由 auto_rebase_patch.py 生成，请勿手动修改注释行以外的内容\n'
        '# 改动：① request处 verify=False+7890+max(45) ② 字幕处 verify=False+7890+timeout45\n'
    )
    final = header + content
    patch_path = os.path.join(ETKN_DIR, 'shared_pool_patch.py')
    write_patch_and_sidecar(patch_path, final)


def rebase_repository(image_id: str):
    mount_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/subscription/repository.py'
    orig = extract_from_image(mount_path)
    with open(os.path.join(ETKN_DIR, 'repository.py.orig'), 'w') as f:
        f.write(orig)

    extract_func = '''def _extract_rollup_body(cte_sql: str) -> str:
    """从 _library_demand_rollups_cte() 的返回串里取出 CTE 的 SELECT 主体。"""
    marker = "demand_rollups AS ("
    i = cte_sql.index(marker)
    j = i + len("demand_rollups AS ")
    depth = 0
    k = j
    while True:
        ch = cte_sql[k]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
        k += 1
    return cte_sql[j + 1:k]'''

    class_anchor = 'class SubscriptionRepository(Repository):'
    parts = orig.split(class_anchor, 1)
    patched = parts[0].rstrip() + '\n\n\n' + extract_func + '\n\n\n' + class_anchor + parts[1]

    old_summary_code = '''        washing_active = "TRUE" if washing_enabled else "FALSE"
        demand_rollups = _library_demand_rollups_cte(active_download_demand_ids)
        conditions: list[str] = []'''

    new_summary_code = '''        washing_active = "TRUE" if washing_enabled else "FALSE"
        demand_rollups = _library_demand_rollups_cte(active_download_demand_ids)
        rollup_body = _extract_rollup_body(demand_rollups)
        conditions: list[str] = []'''

    patched = patched.replace(old_summary_code, new_summary_code, 1)
    patched = patched.replace('            WITH {demand_rollups},', '            WITH', 1)
    patched = patched.replace(
        '                LEFT JOIN demand_rollups demand\n                  ON demand.provider = series_external.provider',
        '                LEFT JOIN dr_rollup demand\n                  ON demand.provider = series_external.provider',
        1
    )
    patched = patched.replace(
        '                LEFT JOIN demand_rollups demand\n                  ON demand.provider = external.provider',
        '                LEFT JOIN dr_rollup demand\n                  ON demand.provider = external.provider',
        1
    )

    old_exec = '''        with self.database.connect() as connection:
            rows = connection.execute(sql, params).fetchall()'''

    new_exec = '''        with self.database.transaction() as connection:
            connection.execute(
                "CREATE TEMP TABLE dr_rollup ON COMMIT DROP AS " + rollup_body
            )
            connection.execute("ANALYZE dr_rollup")
            rows = connection.execute(sql, params).fetchall()'''

    patched = patched.replace(old_exec, new_exec, 1)

    validate_python_code(patched)
    patch_path = os.path.join(ETKN_DIR, 'repository_patch.py')
    write_patch_and_sidecar(patch_path, patched)


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


def restart_etkn():
    subprocess.run(['docker', 'restart', 'etkn'], timeout=30)


def wait_ready(timeout: int = 120):
    import time
    t0 = time.time()
    test_path = '/usr/local/lib/python3.12/site-packages/etk_vnext/modules/subscription/watchlist.py'
    while time.time() - t0 < timeout:
        r = subprocess.run(['docker', 'exec', 'etkn', 'md5sum', test_path], capture_output=True, text=True)
        if r.returncode == 0:
            log(f'容器就绪（{int(time.time() - t0)}s）')
            return True
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

    log('rebase 1/3: shared_pool.py …')
    rebase_shared_pool(image_id)

    log('rebase 2/3: repository.py …')
    rebase_repository(image_id)

    log('rebase 3/3: watchlist.py …')
    rebase_watchlist(image_id)

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
