#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
etkn-monitor v2.5.0 —— ETKN 监控服务（轮询+测速+重试+手动整理+异常明细+双速快照
                        +设置页+飞书Webhook告警中心：500检测/定时测速/积压告警）
配置全部走环境变量（零密钥）：
  ETKN_BASE_URL   ETKN 地址        默认 http://192.168.1.22:5257
  ETKN_USERNAME   登录用户名        默认 YisBoss
  ETKN_PASSWORD   登录密码          必填（部署者自填，不落盘）
  ETKN_ETA_WINDOW 分钟              ETA 滚动窗口，默认 10
  POLL_INTERVAL   秒                慢速全量轮询间隔，默认 120
  FAST_INTERVAL   秒                快速活跃队列轮询间隔，默认 2
  MONITOR_PORT    监听端口          默认 8620
v2.3.1 说明（滞后修复）：
  - 旧版单线程串行拉全部数据：诊断汇总固定 ~13s + succeeded 深翻页(千页级 ~97s)，
    单轮 110s+，快照滞后 2-3 分钟（2026-09-16 实测复现）。
  - 拆分为「快照」（running/queued，单请求 <1s，默认 2 秒一轮，任务出现 ≤60s 内可见）
    与「慢速全量」（succeeded/failed/partial 深翻页+诊断汇总+records，默认 120 秒一轮），
    两线程独立，互不阻塞。/api/status 同时返回两组数据与各自采集时间。
  - 深翻页提前停机：succeeded 首页最旧记录早于今日 0 点且已含窗口外数据时停止（今日口径不变）。
v2.1/v2.2/v2.3 功能（重试收敛/手动整理/异常明细/手动操作卡/主题）不变。
"""
import json
import os
import re
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone

BASE = os.environ.get('ETKN_BASE_URL', 'http://192.168.1.22:5257').rstrip('/')
USERNAME = os.environ.get('ETKN_USERNAME', 'YisBoss')
PASSWORD = os.environ.get('ETKN_PASSWORD', '')
ETA_WINDOW_MIN = float(os.environ.get('ETKN_ETA_WINDOW', '10'))
POLL_INTERVAL = float(os.environ.get('POLL_INTERVAL', '120'))   # 慢速全量轮询
FAST_INTERVAL = float(os.environ.get('FAST_INTERVAL', '2'))     # 快速活跃队列轮询
TZ = timezone(timedelta(hours=8))          # 展示时区固定北京
PAGE = 100                                  # workflows 翻页大小

SPEEDTEST_TARGETS = [                       # 手动测速目标（默认无代理，自然走当前路由策略）
    ('TMDB 图片', 'image.tmdb.org', None),
    ('TMDB 接口', 'api.themoviedb.org', None),
    ('TMDB 自建', 'tmdb.relay.example.com', '192.168.1.1:7890'),  # 模拟 ETKN 业务真实路径（经代理）
    ('Telegram', 'api.telegram.org', None),
    ('共享中心', 'shared.55565576.xyz', None),
]
SPEEDTEST_VIA_NOTE = {                      # 面板口径备注：经代理测量的域名
    'tmdb.relay.example.com': '经代理',
}

# ==================== V2.5 设置 / 飞书推送 / 告警引擎 ====================
# 设置持久化到本地 JSON；容器内 /app 只读挂载时按顺位降级：
#   SETTINGS_PATH 环境变量 > /app/data/ > /data/ > /tmp/（容器层，restart 后保留）
SETTINGS_DEFAULTS = {
    'webhook_url': '',            # 飞书自定义机器人 Webhook（零 token，不经过第三方）
    'push_enabled': False,        # 推送总开关
    'alert_500_enabled': True,    # TMDB HTTP 500 检测告警
    'alert_speed_enabled': False, # 定时测速异常告警（定时测速默认关闭）
    'alert_backlog_enabled': True,# 队列积压告警
    'interval_500_min': 10,       # 500 检测间隔（分钟）
    'interval_speed_min': 0,      # 定时测速间隔（分钟，0=关闭）
    'count_500_threshold': 5,     # 500 告警阈值（今日命中条数）
    'backlog_threshold': 200,     # 积压告警阈值（任一类型排队数）
    'speed_threshold_ms': 5000,   # 测速异常阈值（总耗时毫秒）
}


def _pick_settings_path() -> str:
    cands = []
    if os.environ.get('SETTINGS_PATH'):
        cands.append(os.environ['SETTINGS_PATH'])
    cands += ['/app/data/settings.json', '/data/etkn-monitor-settings.json',
              '/tmp/etkn-monitor-settings.json']
    for c in cands:                     # 已有文件优先（换部署方式不丢配置）
        if os.path.isfile(c):
            return c
    for c in cands:
        try:
            d = os.path.dirname(c)
            os.makedirs(d, exist_ok=True)
            if os.access(d, os.W_OK):
                return c
        except Exception:
            continue
    return cands[-1]


SETTINGS_PATH = _pick_settings_path()
SETTINGS = dict(SETTINGS_DEFAULTS)


def settings_load():
    try:
        with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
            d = json.loads(f.read() or '{}')
        if isinstance(d, dict):
            for k in SETTINGS_DEFAULTS:
                if k in d:
                    SETTINGS[k] = d[k]
    except Exception:
        pass
    for k in ('push_enabled', 'alert_500_enabled', 'alert_speed_enabled', 'alert_backlog_enabled'):
        SETTINGS[k] = bool(SETTINGS[k])
    for k in ('interval_500_min', 'interval_speed_min', 'count_500_threshold',
              'backlog_threshold', 'speed_threshold_ms'):
        try:
            v = type(SETTINGS_DEFAULTS[k])(SETTINGS[k])
            if v >= 0:
                SETTINGS[k] = v
        except Exception:
            SETTINGS[k] = SETTINGS_DEFAULTS[k]


def settings_save():
    tmp = SETTINGS_PATH + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(SETTINGS, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SETTINGS_PATH)


_push_hist = deque(maxlen=50)     # 推送历史（最近 50 条，含测试）
_speed_hist = deque(maxlen=50)    # 测速历史（最近 50 轮，手动+定时）


def record_push(kind: str, text: str, delivered: bool, err: str = ''):
    _push_hist.appendleft({'ts': _now().isoformat(timespec='seconds'), 'kind': kind,
                           'text': (text or '').replace('\n', ' ')[:200],
                           'delivered': bool(delivered), 'err': (err or '')[:120]})


def feishu_push(text: str):
    """飞书自定义机器人 Webhook（msg_type=text）。返回 (ok, err)。零 token，不经过第三方。"""
    url = (SETTINGS.get('webhook_url') or '').strip()
    if not url.startswith(('http://', 'https://')):
        return False, '未配置 Webhook URL'
    try:
        req = urllib.request.Request(
            url, data=json.dumps({'msg_type': 'text', 'content': {'text': text}}).encode('utf-8'),
            headers={'Content-Type': 'application/json'}, method='POST')
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode('utf-8', 'replace')
            ok = 200 <= resp.status < 300
            if ok:      # 飞书业务失败也返 200，需看 code 字段
                m = re.search(r'"code"\s*:\s*(\d+)', body)
                if m and m.group(1) != '0':
                    return False, body[:160]
            return ok, ('' if ok else f'HTTP {resp.status}')
    except Exception as e:
        return False, str(e)[:120]


# ---------- 告警引擎（三检测器，仅异常时推送，正常完全静默） ----------
_500_sig = re.compile(r'(?i)http[ _-]?500|status[=:]\s*500')
_alm = {'t500_fired': False, 't500_day': '', 'speed_fired': {}, 'backlog_fired': set(),
        'next_500': 0.0, 'next_speed': 0.0}


def _alert_text(kind_line: str, detail_lines: list) -> str:
    return ('⚠️ ETKN 告警 · ' + kind_line + '\n时间 ' + _now().strftime('%m-%d %H:%M') +
            '\n' + '\n'.join(detail_lines))


def check_500():
    """今日异常明细中 500 签名命中 ≥ 阈值 → 推送一次；归零自动重新布防。"""
    day = _today00().date().isoformat()
    if _alm['t500_day'] != day:
        _alm['t500_day'] = day
        _alm['t500_fired'] = False
    cnt, samples = 0, []
    for st in ('unrecognized', 'failed'):
        s, b = api_get(f'/api/p115/records?page=1&per_page=50&status={st}'
                       f'&processed_from={urllib.parse.quote(day)}')
        if s != 200 or not isinstance(b, dict):
            continue
        for x in (b.get('items') or [])[:50]:
            reason = str(x.get('reason') or x.get('error') or x.get('status') or '')
            if _500_sig.search(reason):
                cnt += 1
                if len(samples) < 3:
                    samples.append((x.get('original_name') or '-')[:28])
    if cnt >= SETTINGS['count_500_threshold'] and not _alm['t500_fired']:
        _alm['t500_fired'] = True
        text = _alert_text('TMDB HTTP 500', [
            f'今日命中 500 签名异常 {cnt} 条（阈值 {SETTINGS["count_500_threshold"]}）',
            '样例：' + ('、'.join(samples) if samples else '-'),
            '疑似出口节点对 TMDB 限流/拦截；建议复测节点后在任务中心重试失败子项'])
        ok, err = feishu_push(text)
        record_push('t500', text, ok, err)


def run_speed_round(alert: bool = True):
    """一轮测速：写历史；shared 域不参与自动告警（手动测速保留）。"""
    results = [speedtest_one(h, proxy=px) for _, h, px in SPEEDTEST_TARGETS]
    _speed_hist.appendleft({'ts': _now().isoformat(timespec='seconds'), 'results': results})
    if alert and SETTINGS['push_enabled'] and SETTINGS['alert_speed_enabled']:
        thr = SETTINGS['speed_threshold_ms']
        for r in results:
            if r['host'] == 'shared.55565576.xyz':
                continue
            bad = (not r['ok']) or (r['total_ms'] is not None and r['total_ms'] > thr)
            host = r['host']
            if bad and not _alm['speed_fired'].get(host):
                _alm['speed_fired'][host] = True
                desc = (f'连接失败：{r["error"]}' if not r['ok']
                        else f'总耗时 {r["total_ms"]}ms（阈值 {thr}ms）')
                text = _alert_text('链路测速异常', [
                    f'{host}　{desc}', '定时测速检出该域名异常，可能影响刮削/图片链路'])
                ok, err = feishu_push(text)
                record_push('speed', text, ok, err)
            elif not bad:
                _alm['speed_fired'][host] = False
    return results


def check_backlog(fast_snap):
    """分类队列任一类型排队数 ≥ 阈值 → 推送一次；回落后重新布防。"""
    if not (SETTINGS['push_enabled'] and SETTINGS['alert_backlog_enabled']):
        return
    thr = SETTINGS['backlog_threshold']
    by = ((fast_snap or {}).get('active') or {}).get('by_kind') or {}
    fired = _alm['backlog_fired']
    for k, v in by.items():
        q = v.get('queued') or 0
        if q >= thr and k not in fired:
            fired.add(k)
            text = _alert_text('队列积压', [
                f'{k} 排队 {q}（阈值 {thr}）', '该类任务积压，消费可能滞后，建议关注任务中心'])
            ok, err = feishu_push(text)
            record_push('backlog', text, ok, err)
        elif q < thr and k in fired:
            fired.discard(k)


def alarm_loop():
    settings_load()
    while True:
        try:
            now = time.time()
            if SETTINGS['alert_500_enabled'] and now >= _alm['next_500']:
                _alm['next_500'] = now + max(5, int(SETTINGS['interval_500_min'])) * 60
                check_500()
            iv = int(SETTINGS['interval_speed_min'] or 0)
            if iv and now >= _alm['next_speed']:
                _alm['next_speed'] = now + iv * 60
                run_speed_round(alert=True)
            check_backlog(_state.get('fast'))
        except Exception as e:
            record_push('engine', f'告警引擎异常：{str(e)[:150]}', False, 'engine')
        time.sleep(30)

# ---------- 登录会话（内存态，401 自动重登） ----------
_sess_lock = threading.Lock()
_cookie = {'value': '', 'ts': 0.0}


def _now() -> datetime:
    return datetime.now(TZ)


def _today00() -> datetime:
    return _now().replace(hour=0, minute=0, second=0, microsecond=0)


def login() -> str:
    data = json.dumps({'username': USERNAME, 'password': PASSWORD}).encode()
    req = urllib.request.Request(BASE + '/api/auth/login', data=data,
                                 headers={'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(req, timeout=15) as r:
        raw = r.headers.get('Set-Cookie', '')
        m = re.match(r'([^=]+=[^;]+)', raw)
        ck = m.group(1) if m else ''
    if not ck:
        raise RuntimeError('登录成功但未取到 Cookie')
    _cookie['value'] = ck
    _cookie['ts'] = time.time()
    return ck


def api_get(path: str, retry: bool = True):
    """带会话的 GET；401 重登一次。返回 (status, dict)。"""
    if not _cookie['value']:
        with _sess_lock:
            if not _cookie['value']:
                login()
    req = urllib.request.Request(BASE + path, headers={'Cookie': _cookie['value']})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode() or '{}')
    except urllib.error.HTTPError as e:
        if e.code == 401 and retry:
            with _sess_lock:
                login()
            return api_get(path, retry=False)
        return e.code, {}
    except Exception:
        return -1, {}


def api_post(path: str, payload=None, retry: bool = True):
    """带会话的 POST；401 重登一次；非 2xx 读取响应体原样透传。"""
    if not _cookie['value']:
        with _sess_lock:
            if not _cookie['value']:
                login()
    body = json.dumps(payload or {}).encode()
    req = urllib.request.Request(BASE + path, data=body,
                                 headers={'Cookie': _cookie['value'],
                                          'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode() or '{}')
    except urllib.error.HTTPError as e:
        if e.code == 401 and retry:
            with _sess_lock:
                login()
            return api_post(path, payload, retry=False)
        try:
            eb = json.loads(e.read().decode() or '{}')
        except Exception:
            eb = {}
        return e.code, eb
    except Exception as ex:
        return -1, {'error': str(ex)[:120]}


# ---------- 数据采集（只读） ----------
def _today_prefix() -> str:
    """今日 0 点（北京）ISO 前缀，用于 created_at 字符串比较。"""
    return _today00().isoformat()


def collect_active_queue(today_prefix: str):
    """当前活跃队列（queued+running，只统计当日创建）。"""
    tasks, media_total = [], 0
    for st in ('queued', 'running'):
        offset = 0
        while True:
            s, b = api_get(f'/api/workflows?status={st}&limit={PAGE}&offset={offset}')
            items = b.get('items', []) if isinstance(b, dict) else []
            if not items:
                break
            stop = False
            for it in items:
                if (it.get('created_at') or '') < today_prefix:   # 历史混入即止
                    stop = True
                    break
                media = it.get('item_count') or 0
                tasks.append({'id': it.get('id'), 'status': st,
                              'kind': kind_of(it), 'title': it.get('display_title') or '',
                              'media': media, 'created_at': it.get('created_at', ''),
                              'started_at': it.get('started_at') or '',
                              'succeeded': it.get('succeeded_count') or 0,
                              'failed': it.get('failed_count') or 0,
                              'active': it.get('active_count') or 0})
                media_total += media
            if stop:
                break
            offset += PAGE
            if offset >= 1000:      # 活跃队列不该翻很深，防失控
                break
    return tasks, media_total


def collect_today_done(today_prefix: str):
    """今日终态任务（succeeded/failed/partial），带 finished_at，供速率与统计。

    succeeded 深翻页优化（v2.3.1）：今日口径只需翻到「整页都早于今日 0 点」为止——
    服务端按时间倒序返回，一旦某页最旧记录已早于今日 0 点，更深的页必然更旧，直接停。
    """
    out = []
    for st in ('succeeded', 'failed', 'partial'):
        offset = 0
        while True:
            s, b = api_get(f'/api/workflows?status={st}&limit={PAGE}&offset={offset}')
            items = b.get('items', []) if isinstance(b, dict) else []
            if not items:
                break
            page_oldest = min((it.get('finished_at') or it.get('created_at') or '')
                              for it in items)
            for it in items:
                fin = it.get('finished_at') or it.get('created_at') or ''
                if fin < today_prefix:
                    continue
                out.append({'id': it.get('id'), 'status': st, 'kind': kind_of(it),
                            'wf': it.get('workflow_type') or '',
                            'title': it.get('display_title') or '',
                            'media': it.get('item_count') or 0,
                            'ok_media': it.get('succeeded_count') or 0,
                            'bad_media': it.get('failed_count') or 0,
                            'finished_at': fin})
            if page_oldest < today_prefix:
                break
            offset += PAGE
            if offset >= 3000:
                break
    return out


def collect_records_day(today_prefix: str):
    """今日媒体记录（p115/records）——全站统一媒体口径。"""
    out = {}
    for st in ('success', 'unrecognized'):
        s, b = api_get(f'/api/p115/records?page=1&per_page=1&status={st}'
                       f'&processed_from={urllib.parse.quote(today_prefix)}')
        out['success' if st == 'success' else 'unrecognized'] = (b or {}).get('total', 0) if s == 200 else 0
    s, b = api_get(f'/api/p115/records?page=1&per_page=1&processed_from={urllib.parse.quote(today_prefix)}')
    out['total'] = (b or {}).get('total', 0) if s == 200 else 0
    return out


def kind_of(it: dict) -> str:
    t = it.get('display_title') or ''
    for p in ('共享登记', '追剧刷新', '刮削入库', '网盘整理', '手动整理网盘文件', '频道转存', '订阅预处理'):
        if t.startswith(p):
            return p
    wt = it.get('workflow_type') or ''
    return {'batch_ingest': '刮削入库', 'p115_organize': '网盘整理', 'manual_task': '手动任务'}.get(wt, wt or '其他')


# ---------- 实际领取序（排优重验证结案：档位小=先领，同档按步骤/运行先后） ----------
# 档位映射源：etk_vnext/workflow/repository.py claim_next_step() ORDER BY CASE（只读取证 9/16）
TIER_BY_KIND = {'刮削入库': 10, '网盘整理': 10, '手动整理网盘文件': 10,
                '共享登记': 20, '追剧刷新': 20, '手动任务': 20,
                '订阅预处理': 20, '生成媒体库封面': 20, '频道转存': 20}
TIER_DEFAULT = 50


def apply_claim_order(tasks: list, by_kind: dict):
    """给 by_kind 每类补 tier（档位）与 rank（该类第一条 queued 任务在全局领取序中的排位）。
    领取序 = queued 任务按 (档位, 运行id) 升序；running 不占排位（已在消费）。"""
    queued = sorted((t for t in tasks if t['status'] == 'queued'),
                    key=lambda t: (TIER_BY_KIND.get(t['kind'], TIER_DEFAULT), t['id'] or 0))
    first_rank = {}
    for i, t in enumerate(queued, 1):
        first_rank.setdefault(t['kind'], i)
    for k, d in by_kind.items():
        d['tier'] = TIER_BY_KIND.get(k, TIER_DEFAULT)
        d['rank'] = first_rank.get(k)          # None=该类无排队（可能在 running 或已清空）


# ---------- ETA：滚动窗口速率外推（只看当前队列时期的完成记录） ----------
def compute_eta(done_recent: list, remaining_media: int, running_task: dict = None):
    """done_recent: 已按 finished_at 过滤到窗口内的记录（含 media=item_count）。
    速率 = 窗口内完成媒体数 / 窗口分钟；ETA = 剩余媒体 / 速率。
    速率为 0 且剩余>0：有运行中任务 → 实时进度（运行中 X 分钟 · 子项 N/M）；无 → 等待领取。"""
    wmin = max(ETA_WINDOW_MIN, 1.0)
    done_media = sum(d['media'] for d in done_recent)
    rate = done_media / wmin if wmin else 0.0
    remaining = remaining_media
    if rate <= 0:
        if remaining > 0 and running_task:
            mins = 0
            sa = running_task.get('started_at') or ''
            if sa:
                try:
                    t0 = datetime.datetime.fromisoformat(sa)
                    mins = max(0, int((_now() - t0).total_seconds() // 60))
                except Exception:
                    mins = 0
            n, m = running_task.get('succeeded', 0), running_task.get('media') or 0
            prog = f' · 子项 {n}/{m}' if m else ''
            return {'rate': 0.0, 'remaining_media': remaining, 'eta_min': None,
                    'eta_text': f'运行中 {mins} 分钟{prog}', 'fallback': 'running',
                    'window_min': wmin, 'done_media_in_window': done_media}
        if remaining > 0:
            return {'rate': 0.0, 'remaining_media': remaining, 'eta_min': None,
                    'eta_text': '等待领取', 'fallback': 'queued',
                    'window_min': wmin, 'done_media_in_window': done_media}
        return {'rate': 0.0, 'remaining_media': remaining, 'eta_min': None, 'eta_text': '计算中',
                'window_min': wmin, 'done_media_in_window': done_media}
    eta_min = remaining / rate
    if eta_min >= 600:
        eta_text = f'≥{int(eta_min // 60)} 小时'
    elif eta_min >= 60:
        h, m = int(eta_min // 60), int(eta_min % 60)
        eta_text = f'{h}小时{m:02d}分'
    else:
        eta_text = f'{max(1, int(round(eta_min)))} 分钟'
    return {'rate': round(rate, 2), 'remaining_media': remaining, 'eta_min': round(eta_min, 1),
            'eta_text': eta_text, 'window_min': wmin, 'done_media_in_window': done_media}


# ---------- 手动测速（无定时；默认直连，proxy 指定域经代理 CONNECT 隧道） ----------
def speedtest_one(host: str, timeout: float = 10.0, proxy: str | None = None):
    r = {'host': host, 'ok': False, 'tcp_ms': None, 'tls_ms': None, 'http_ms': None,
         'total_ms': None, 'status': None, 'error': None, 'via': 'proxy' if proxy else 'direct'}
    t0 = time.perf_counter()
    try:
        if proxy:
            # 经代理 CONNECT 隧道：模拟 ETKN 业务真实路径（socket 全程代持，分阶段仍可计时）
            phost, pport = proxy.rsplit(':', 1)
            sock = socket.create_connection((phost, int(pport)), timeout=timeout)
            t1 = time.perf_counter()
            r['tcp_ms'] = round((t1 - t0) * 1000)   # 阶段1：到代理的连接
            sock.sendall(f'CONNECT {host}:443 HTTP/1.1\r\nHost: {host}:443\r\n'
                         f'Proxy-Connection: keep-alive\r\n\r\n'.encode())
            buf = b''
            while b'\r\n\r\n' not in buf and time.perf_counter() - t1 < timeout:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf += chunk
            if not re.match(r'HTTP/1\.[01] 200', buf.split(b'\r\n', 1)[0].decode('latin1', 'replace')):
                raise ConnectionError('代理隧道建立失败: '
                                      + buf.split(b'\r\n', 1)[0].decode('latin1', 'replace')[:60])
            t2 = time.perf_counter()
            r['tls_ms'] = round((t2 - t1) * 1000)   # 阶段2：CONNECT 隧道建立（含代理侧回源）
            tls = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
            t3 = time.perf_counter()
            r['http_ms'] = round((t3 - t2) * 1000)  # 阶段3：TLS 握手（真目标）
        else:
            sock = socket.create_connection((host, 443), timeout=timeout)
            t1 = time.perf_counter()
            r['tcp_ms'] = round((t1 - t0) * 1000)
            tls = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
            t2 = time.perf_counter()
            r['tls_ms'] = round((t2 - t1) * 1000)
        tls.sendall(f'GET / HTTP/1.1\r\nHost: {host}\r\n'
                    f'User-Agent: etkn-monitor\r\nConnection: close\r\n\r\n'.encode())
        chunk = tls.recv(256)
        t3 = time.perf_counter()
        r['http_ms'] = round((t3 - t2) * 1000)  # 代理路径：末段=GET 响应；直连路径：=首字节
        line = chunk.decode('latin1', 'replace').split('\r\n', 1)[0]
        m = re.search(r'\b(\d{3})\b', line)
        r['status'] = int(m.group(1)) if m else None
        r['ok'] = bool(r['status'])
        r['total_ms'] = round((t3 - t0) * 1000)
        try:
            tls.close()
        except Exception:
            pass
    except Exception as e:
        t3 = time.perf_counter()
        r['total_ms'] = round((t3 - t0) * 1000)
        r['error'] = str(e)[:80] or type(e).__name__
    return r


# ---------- 后台轮询（双速双快照） ----------
_state = {'snapshot': None, 'error': None, 'ts': None, 'hist': deque(maxlen=2000),
          'fast': None, 'fast_ts': None, 'fast_error': None}


def fast_once():
    """快速快照：只拉 running+queued（与 ETKN 任务中心 UI 同源同参），单请求级成本。"""
    today_prefix = _today_prefix()
    snap = {'healthy': False, 'active': {'tasks': 0, 'media': 0, 'by_kind': {}}, 'diag': {}}
    s, b = api_get('/api/health')
    snap['healthy'] = (s == 200)
    tasks, media_total = collect_active_queue(today_prefix)
    snap['active']['tasks'] = len(tasks)
    snap['active']['media'] = media_total
    for t in tasks:
        k = t['kind']
        d = snap['active']['by_kind'].setdefault(k, {'queued': 0, 'running': 0, 'media': 0, 'tasks': 0})
        d[t['status']] += 1
        d['tasks'] += 1
        d['media'] += t['media']
    apply_claim_order(tasks, snap['active']['by_kind'])   # v2.4.3 补：fast 快照同样带档位/排位
    return snap


def poll_loop():
    while True:
        try:
            snap = poll_once()
            _state['snapshot'] = snap
            _state['error'] = None
            _state['ts'] = snap['ts']
            _state['hist'].append({'ts': snap['ts'], 'active_media': snap['active']['media'],
                                   'done_media': snap['records'].get('success', 0)})
        except Exception as e:
            _state['error'] = f'{e}'[:200]
        time.sleep(max(POLL_INTERVAL, 30))


def fast_loop():
    """快速线程：2 秒级刷新活跃队列；历史 done/records 用慢线程最后一次结果合并展示。"""
    # 先等首份慢快照就绪（合并展示需要）
    while _state['snapshot'] is None and _state['error'] is None:
        time.sleep(0.5)
    while True:
        try:
            fs = fast_once()
            _state['fast'] = fs
            _state['fast_ts'] = _now().isoformat(timespec='seconds')
            _state['fast_error'] = None
        except Exception as e:
            _state['fast_error'] = f'{e}'[:200]
        time.sleep(max(FAST_INTERVAL, 1))


def poll_once():
    today_prefix = _today_prefix()
    snap = {'ts': _now().isoformat(timespec='seconds'), 'healthy': False, 'diag': {},
            'active': {'tasks': 0, 'media': 0, 'by_kind': {}}, 'done': {}, 'records': {},
            'eta': {}, 'failed_today': []}
    s, b = api_get('/api/health')
    snap['healthy'] = (s == 200)
    s, b = api_get('/api/diagnostics/summary')
    if s == 200:
        snap['diag'] = {k: b.get(k, 0) for k in ('queued_runs', 'running_runs', 'failed_runs',
                                                 'succeeded_runs', 'partial_runs', 'problem_runs')}

    tasks, media_total = collect_active_queue(today_prefix)
    snap['active']['tasks'] = len(tasks)
    snap['active']['media'] = media_total
    for t in tasks:
        k = t['kind']
        d = snap['active']['by_kind'].setdefault(k, {'queued': 0, 'running': 0, 'media': 0, 'tasks': 0})
        d[t['status']] += 1
        d['tasks'] += 1
        d['media'] += t['media']
    apply_claim_order(tasks, snap['active']['by_kind'])   # v2.4.2 排位口径=实际领取序

    done = collect_today_done(today_prefix)
    win_cut = (_now() - timedelta(minutes=ETA_WINDOW_MIN)).isoformat()
    recent = [d for d in done if (d.get('finished_at') or '') >= win_cut]
    # v2.4.3 速率=0 兜底：剩余>0 时取「最早开始且仍在运行」的任务实时进度
    running_tasks = sorted((t for t in tasks if t['status'] == 'running' and t.get('started_at')),
                           key=lambda t: t['started_at'])
    snap['eta'] = compute_eta(recent, media_total, running_tasks[0] if running_tasks else None)
    # 整理类预计（v2.4.2）：按 10 档类（网盘整理/手动整理/刮削入库）排队媒体与同窗口速率外推
    tier10 = [k for k, d in snap['active']['by_kind'].items() if d.get('tier') == 10]
    org_media = sum(snap['active']['by_kind'][k]['media'] for k in tier10)
    org_recent = [d for d in recent if d.get('wf') in ('p115_organize', 'batch_ingest', 'core_ingest')
                  or d['kind'] in tier10]
    org_run = next((t for t in running_tasks if t['kind'] in tier10), None)
    org_eta = compute_eta(org_recent, org_media, org_run)
    org_eta['kinds'] = tier10
    snap['eta']['organize'] = org_eta
    by_kind_done = {}
    for d in done:
        k = d['kind']
        e = by_kind_done.setdefault(k, {'tasks': 0, 'items': 0, 'ok_media': 0, 'bad_media': 0})
        e['tasks'] += 1
        e['items'] += d['media']
        e['ok_media'] += d['ok_media']
        e['bad_media'] += d['bad_media']
    snap['done'] = {'tasks': len(done), 'by_kind': by_kind_done,
                    'items': sum(d['media'] for d in done),
                    'ok_media': sum(d['ok_media'] for d in done),
                    'bad_media': sum(d['bad_media'] for d in done)}
    snap['records'] = collect_records_day(today_prefix)
    snap['failed_today'] = [
        {'id': d['id'], 'kind': d['kind'], 'wf': d['wf'], 'title': d['title'][:40],
         'media': d['media'], 'bad_media': d['bad_media'], 'finished_at': d['finished_at'][:19]}
        for d in done if d['status'] in ('failed', 'partial')][:50]
    return snap


def poll_loop():
    while True:
        try:
            snap = poll_once()
            _state['snapshot'] = snap
            _state['error'] = None
            _state['ts'] = snap['ts']
            _state['hist'].append({'ts': snap['ts'], 'active_media': snap['active']['media'],
                                   'done_media': snap['records'].get('success', 0)})
        except Exception as e:
            _state['error'] = f'{e}'[:200]
        time.sleep(max(POLL_INTERVAL, 5))


# ---------- HTTP ----------
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer   # noqa: E402

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):      # 静默访问日志
        pass

    def _send(self, code, body: bytes, ctype='application/json; charset=utf-8'):
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        p = urllib.parse.urlparse(self.path).path
        if p in ('/', '/index.html'):
            try:
                with open(os.path.join(STATIC, 'index.html'), 'rb') as f:
                    return self._send(200, f.read(), 'text/html; charset=utf-8')
            except FileNotFoundError:
                return self._send(404, '前端未构建'.encode(), 'text/plain; charset=utf-8')
        if p == '/api/status':
            snap = _state.get('snapshot')
            fs = _state.get('fast')
            if not snap:
                return self._send(503, json.dumps({'error': _state.get('error') or '首轮采集中，请稍候'},
                                                  ensure_ascii=False).encode())
            out = dict(snap)
            if fs:
                # 活跃队列/健康用快速快照（2s 级），历史数据用慢快照
                out['healthy'] = fs['healthy'] and snap['healthy']
                out['active'] = fs['active']
                out['fast_ts'] = _state.get('fast_ts')
                out['ts'] = _state.get('fast_ts') or snap['ts']   # 展示口径=最近成功采集时刻
                if fs.get('diag'):
                    out['diag'] = {**snap.get('diag', {}), **fs['diag']}
            out['slow_ts'] = snap.get('ts')
            return self._send(200, json.dumps(out, ensure_ascii=False).encode())
        if p == '/api/meta':
            return self._send(200, json.dumps({
                'base_url': BASE, 'eta_window_min': ETA_WINDOW_MIN,
                'poll_interval': FAST_INTERVAL, 'slow_poll_interval': POLL_INTERVAL,
                'version': 'v2.5.2', 'readonly': False,
                'actions': ['speedtest', 'retry-failed', 'run-organize-p115',
                            'run-generate-covers', 'purge-register-queued', 'bad-media',
                            'settings', 'test-push', 'check-500-now', 'speed-now'],
            }, ensure_ascii=False).encode())
        if p == '/api/bad-media':
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            date = (q.get('date') or [''])[0]
            today_prefix = _today00().date().isoformat() if not date else date
            items, total = [], 0
            for st in ('unrecognized', 'failed'):
                s, b = api_get(f'/api/p115/records?page=1&per_page=50&status={st}'
                               f'&processed_from={urllib.parse.quote(today_prefix)}')
                if s != 200 or not isinstance(b, dict):
                    continue
                total += (b.get('total') or 0) if st == 'unrecognized' else 0
                for x in (b.get('items') or [])[:50]:
                    items.append({'id': x.get('id'), 'name': x.get('original_name') or x.get('current_name') or '-',
                                  'status': x.get('status'), 'reason': (x.get('reason') or x.get('error') or '')[:120],
                                  'at': (x.get('processed_at') or x.get('created_at') or '')[:19]})
            items.sort(key=lambda x: x['at'], reverse=True)
            return self._send(200, json.dumps({'date': today_prefix, 'total': total or len(items),
                                               'items': items[:60]}, ensure_ascii=False).encode())
        if p == '/api/settings':
            return self._do_settings_get()
        if p == '/api/push-history':
            return self._send(200, json.dumps({'items': list(_push_hist)},
                                              ensure_ascii=False).encode())
        if p == '/api/speed-history':
            return self._send(200, json.dumps({'items': list(_speed_hist)},
                                              ensure_ascii=False).encode())
        return self._send(404, '{"error":"not found"}'.encode())

    def _body(self):
        try:
            n = int(self.headers.get('Content-Length') or 0)
            return json.loads(self.rfile.read(n).decode('utf-8') or '{}') if n else {}
        except Exception:
            return {}

    def _do_settings_get(self):
        d = dict(SETTINGS)
        if d.get('webhook_url'):        # 脱敏展示：协议+域名+尾部4位
            m = re.match(r'^(https?://[^/]+/)(.*)$', d['webhook_url'])
            d['webhook_masked'] = (m.group(1) + '***' + m.group(2)[-4:]) if m else '***'
        else:
            d['webhook_masked'] = ''
        d['settings_path'] = SETTINGS_PATH
        return self._send(200, json.dumps(d, ensure_ascii=False).encode())

    def _do_settings_post(self):
        b = self._body()
        if not isinstance(b, dict):
            return self._send(400, '{"error":"bad body"}'.encode())
        if 'webhook_url' in b and isinstance(b['webhook_url'], str):
            SETTINGS['webhook_url'] = b['webhook_url'].strip()
        for k in ('push_enabled', 'alert_500_enabled', 'alert_speed_enabled', 'alert_backlog_enabled'):
            if k in b:
                SETTINGS[k] = bool(b[k])
        for k, lo in (('interval_500_min', 5), ('interval_speed_min', 0),
                      ('count_500_threshold', 1), ('backlog_threshold', 1),
                      ('speed_threshold_ms', 1000)):
            if k in b:
                try:
                    v = int(b[k])
                    if v >= lo:
                        SETTINGS[k] = v
                except Exception:
                    pass
        try:
            settings_save()
        except Exception as e:
            return self._send(500, json.dumps({'error': f'保存失败：{e}'[:120]},
                                              ensure_ascii=False).encode())
        return self._do_settings_get()

    def do_POST(self):
        p = urllib.parse.urlparse(self.path).path
        if p == '/api/settings':
            return self._do_settings_post()
        if p == '/api/test-push':
            text = _alert_text('测试推送', ['设置页手动触发 · 验证 Webhook 链路',
                                    '收到本条说明 etkn-monitor → 飞书 推送链路可达'])
            ok, err = feishu_push(text)
            record_push('test', text, ok, err)
            return self._send(200, json.dumps({'ok': ok, 'err': err},
                                              ensure_ascii=False).encode())
        if p == '/api/check-500-now':
            check_500()
            return self._send(200, json.dumps({'fired': _alm['t500_fired']},
                                              ensure_ascii=False).encode())
        if p == '/api/speed-now':
            results = run_speed_round(alert=False)
            return self._send(200, json.dumps(
                {'ts': _now().isoformat(timespec='seconds'), 'results': results},
                ensure_ascii=False).encode())
        if p == '/api/speedtest':
            results = [speedtest_one(h, proxy=px) for _, h, px in SPEEDTEST_TARGETS]
            _speed_hist.appendleft({'ts': _now().isoformat(timespec='seconds'), 'results': results})
            return self._send(200, json.dumps(
                {'ts': _now().isoformat(timespec='seconds'), 'results': results},
                ensure_ascii=False).encode())
        m = re.match(r'^/api/retry/(\d+)$', p)
        if m:
            wid = m.group(1)
            s, b = api_post(f'/api/workflows/{wid}/retry-failed')
            return self._send(s if s > 0 else 502, json.dumps(
                {'etkn_status': s, 'etkn_body': b}, ensure_ascii=False).encode())
        if p == '/api/run-task/organize-p115':
            # 原样转发 ETKN 原生「手动整理网盘文件」运行接口（参数取自历史运行形态）
            payload = {'parameters': {'trigger': 'telegram', 'task_key': 'organize-p115',
                                      'module_key': 'p115_organize', 'handoff_mode': 'independent'}}
            s, b = api_post('/api/task-center/tasks/organize-p115/runs', payload)
            return self._send(s if s > 0 else 502, json.dumps(
                {'etkn_status': s, 'etkn_body': b}, ensure_ascii=False).encode())
        if p == '/api/run-task/generate-virtual-library-covers':
            # 原样转发 ETKN 原生「生成媒体库封面」运行接口（Pro 任务；本机 is_pro=true 已验证；
            # 无历史运行，用最小载荷走服务端面板默认值）
            s, b = api_post('/api/task-center/tasks/generate-virtual-library-covers/runs',
                            {'parameters': {}})
            return self._send(s if s > 0 else 502, json.dumps(
                {'etkn_status': s, 'etkn_body': b}, ensure_ascii=False).encode())
        if p == '/api/purge-register-queued':
            # 清空共享登记积压：只取消 status=queued 的共享登记运行（绝不碰 running）。
            # 「共享登记」=display_title 前缀（其 workflow_type 是 manual_task，与追剧刷新同型），
            # 因此按标题前缀识别而非 workflow_type。逐条调用原生 cancel；单条失败不中断。
            limit = int(urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                        .get('limit', ['500'])[0])
            targets, offset = [], 0
            while offset < 1000:
                s0, b0 = api_get(f'/api/workflows?status=queued&limit={PAGE}&offset={offset}')
                items = b0.get('items', []) if isinstance(b0, dict) else []
                if not items:
                    break
                for x in items:
                    if (x.get('status') == 'queued'
                            and (x.get('display_title') or '').startswith('共享登记')):
                        targets.append(x['id'])
                offset += PAGE
                if len(items) < PAGE:
                    break
            ok_ids, fails = [], []
            for rid in targets:
                try:
                    s1, b1 = api_post(f'/api/workflows/{rid}/cancel', {})
                    if s1 in (200, 201, 202):
                        ok_ids.append(rid)
                    else:
                        fails.append({'id': rid, 'status': s1, 'body': b1})
                except Exception as e:  # 单条失败不中断
                    fails.append({'id': rid, 'error': str(e)[:120]})
            return self._send(200, json.dumps(
                {'found': len(targets), 'cancelled': len(ok_ids), 'failed': fails,
                 'ids': ok_ids}, ensure_ascii=False).encode())
        return self._send(404, '{"error":"not found"}'.encode())


def main():
    if not PASSWORD:
        raise SystemExit('缺少环境变量 ETKN_PASSWORD（只存环境，不落盘）')
    threading.Thread(target=poll_loop, daemon=True).start()
    threading.Thread(target=fast_loop, daemon=True).start()
    threading.Thread(target=alarm_loop, daemon=True).start()
    port = int(os.environ.get('MONITOR_PORT', '8620'))
    srv = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    print(f'etkn-monitor v2.5.0，端口 {port}，快轮询 {FAST_INTERVAL}s（活跃队列）/'
          f'慢轮询 {POLL_INTERVAL}s（全量），ETA 窗口 {ETA_WINDOW_MIN}min，'
          f'测速/重试/手动整理=手动', flush=True)
    srv.serve_forever()


if __name__ == '__main__':
    main()
