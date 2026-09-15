#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
etkn-monitor v1 —— ETKN 只读监控服务
配置全部走环境变量（零密钥）：
  ETKN_BASE_URL   ETKN 地址        默认 http://192.168.1.22:5257
  ETKN_USERNAME   登录用户名        默认 YisBoss
  ETKN_PASSWORD   登录密码          必填（部署者自填，不落盘）
  ETKN_ETA_WINDOW 分钟              ETA 滚动窗口，默认 10
  POLL_INTERVAL   秒                轮询间隔，默认 15
  MONITOR_PORT    监听端口          默认 8620
v1 只读：只 GET ETKN 接口 + 本地计算，绝不写任何状态。
"""
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, deque
from datetime import datetime, timedelta, timezone

BASE = os.environ.get('ETKN_BASE_URL', 'http://192.168.1.22:5257').rstrip('/')
USERNAME = os.environ.get('ETKN_USERNAME', 'YisBoss')
PASSWORD = os.environ.get('ETKN_PASSWORD', '')
ETA_WINDOW_MIN = float(os.environ.get('ETKN_ETA_WINDOW', '10'))
POLL_INTERVAL = float(os.environ.get('POLL_INTERVAL', '15'))
TZ = timezone(timedelta(hours=8))          # 展示时区固定北京
PAGE = 100                                  # workflows 翻页大小

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


# ---------- 数据采集 ----------
def _today_prefix() -> str:
    """今日 0 点（北京）ISO 前缀，用于 created_at 字符串比较。"""
    return _today00().isoformat()


def collect_active_queue(today_prefix: str):
    """当前活跃队列（queued+running，只统计当日创建）。"""
    tasks, media_total, by_kind = [], 0, {}
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
                              'started_at': it.get('started_at') or ''})
                media_total += media
            if stop:
                break
            offset += PAGE
            if offset >= 1000:      # 活跃队列不该翻很深，防失控
                break
    return tasks, media_total


def collect_today_done(today_prefix: str):
    """今日终态任务（succeeded/failed/partial），带 finished_at，供速率与统计。"""
    out = []
    for st in ('succeeded', 'failed', 'partial'):
        offset = 0
        while True:
            s, b = api_get(f'/api/workflows?status={st}&limit={PAGE}&offset={offset}')
            items = b.get('items', []) if isinstance(b, dict) else []
            if not items:
                break
            stop = False
            for it in items:
                fin = it.get('finished_at') or it.get('created_at') or ''
                if fin < today_prefix:
                    stop = True
                    break
                out.append({'id': it.get('id'), 'status': st, 'kind': kind_of(it),
                            'title': it.get('display_title') or '',
                            'media': it.get('item_count') or 0,
                            'ok_media': it.get('succeeded_count') or 0,
                            'bad_media': it.get('failed_count') or 0,
                            'finished_at': fin})
            if stop:
                break
            offset += PAGE
            if offset >= 3000:
                break
    return out


def collect_records_day(today_prefix: str):
    """今日媒体记录（p115/records）——卡片口径主数据。"""
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
    for p in ('共享登记', '追剧刷新', '刮削入库', '网盘整理', '手动整理网盘文件', '频道转存'):
        if t.startswith(p):
            return p
    wt = it.get('workflow_type') or ''
    return {'batch_ingest': '刮削入库', 'p115_organize': '网盘整理', 'manual_task': '手动任务'}.get(wt, wt or '其他')


# ---------- ETA：滚动窗口速率外推（只看当前队列时期的完成记录） ----------
def compute_eta(done_recent: list, remaining_media: int):
    """done_recent: 已按 finished_at 过滤到窗口内的记录（含 media=item_count）。
    速率 = 窗口内完成媒体数 / 窗口分钟；ETA = 剩余媒体 / 速率。速率为 0 → 计算中。"""
    wmin = max(ETA_WINDOW_MIN, 1.0)
    done_media = sum(d['media'] for d in done_recent)
    rate = done_media / wmin if wmin else 0.0
    remaining = remaining_media
    if rate <= 0:
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


# ---------- 后台轮询线程 ----------
_state = {'snapshot': None, 'error': None, 'ts': None, 'hist': deque(maxlen=2000)}


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

    done = collect_today_done(today_prefix)
    win_cut = (_now() - timedelta(minutes=ETA_WINDOW_MIN)).isoformat()
    recent = [d for d in done if (d.get('finished_at') or '') >= win_cut]
    snap['eta'] = compute_eta(recent, media_total)
    by_kind_done = {}
    for d in done:
        k = d['kind']
        e = by_kind_done.setdefault(k, {'tasks': 0, 'media': 0, 'ok_media': 0, 'bad_media': 0})
        e['tasks'] += 1
        e['media'] += d['media']
        e['ok_media'] += d['ok_media']
        e['bad_media'] += d['bad_media']
    snap['done'] = {'tasks': len(done), 'by_kind': by_kind_done,
                    'media': sum(d['media'] for d in done),
                    'ok_media': sum(d['ok_media'] for d in done),
                    'bad_media': sum(d['bad_media'] for d in done)}
    snap['records'] = collect_records_day(today_prefix)
    snap['failed_today'] = [
        {'id': d['id'], 'kind': d['kind'], 'title': d['title'][:40],
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
                                   'done_media': snap['done'].get('media', 0)})
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
            if not snap:
                return self._send(503, json.dumps({'error': _state.get('error') or '首轮采集中，请稍候'},
                                                  ensure_ascii=False).encode())
            return self._send(200, json.dumps(snap, ensure_ascii=False).encode())
        if p == '/api/meta':
            return self._send(200, json.dumps({
                'base_url': BASE, 'eta_window_min': ETA_WINDOW_MIN,
                'poll_interval': POLL_INTERVAL, 'version': 'v1.0', 'readonly': True,
            }, ensure_ascii=False).encode())
        return self._send(404, '{"error":"not found"}'.encode())


def main():
    if not PASSWORD:
        raise SystemExit('缺少环境变量 ETKN_PASSWORD（只存环境，不落盘）')
    threading.Thread(target=poll_loop, daemon=True).start()
    port = int(os.environ.get('MONITOR_PORT', '8620'))
    srv = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    print(f'etkn-monitor v1 只读模式，端口 {port}，轮询 {POLL_INTERVAL}s，ETA 窗口 {ETA_WINDOW_MIN}min', flush=True)
    srv.serve_forever()


if __name__ == '__main__':
    main()
