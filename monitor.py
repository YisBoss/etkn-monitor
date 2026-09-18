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
import secrets
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
LAN_HOST = os.environ.get('MONITOR_LAN_HOST', '192.168.1.22:8620')   # 内网面板地址（清空提醒链接用）
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
    # ---- v2.5.3 整理任务静止告警 + 完成提醒 ----
    'alert_stall_enabled': True,   # 整理任务静止告警开关
    'stall_threshold_min': 10,     # 静止判定阈值（分钟）
    'stall_repeat_min': 30,        # 持续静止重复提醒间隔（分钟）
    'stall_grace_enabled': True,   # 大文件宽限开关（运行>1小时阈值放宽到 30 分钟）
    'alert_finish_enabled': True,  # 整理任务清空提醒开关
    'finish_scope': 'all',         # 推送范围：ok=汇总隐藏失败行 / all=含失败行
    # ---- v2.5.5 一键整理入口（清空提醒富文本） ----
    'trigger_enabled': False,      # 清空提醒附「整理下一批」按钮（令牌链接）
    'trigger_public_base': '',     # 外网基础地址（如 https://etknjk.example.com），空=内网地址
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
    # v2.5.5 修复：无既有文件时**优先宿主机挂载卷**（/app/data 经 compose 挂宿主机
    # data/，容器重建不丢）；旧顺序曾把新装设置落进容器可写层，重建即丢。
    if os.environ.get('SETTINGS_PATH'):
        return cands[0]
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
    for k in ('push_enabled', 'alert_500_enabled', 'alert_speed_enabled', 'alert_backlog_enabled',
              'alert_stall_enabled', 'stall_grace_enabled', 'alert_finish_enabled',
              'trigger_enabled'):
        SETTINGS[k] = bool(SETTINGS[k])
    for k in ('interval_500_min', 'interval_speed_min', 'count_500_threshold',
              'backlog_threshold', 'speed_threshold_ms',
              'stall_threshold_min', 'stall_repeat_min'):
        try:
            v = type(SETTINGS_DEFAULTS[k])(SETTINGS[k])
            if v >= 0:
                SETTINGS[k] = v
        except Exception:
            SETTINGS[k] = SETTINGS_DEFAULTS[k]
    if SETTINGS['finish_scope'] not in ('ok', 'all'):
        SETTINGS['finish_scope'] = 'all'
    if not isinstance(SETTINGS['trigger_public_base'], str):
        SETTINGS['trigger_public_base'] = ''


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


# ---------- v2.5.3 整理任务静止告警 + 完成提醒 ----------
# 整理类按 display_title 前缀区分（共享登记/追剧刷新的 workflow_type 同为 manual_task，按类型筛会误伤）
ORGANIZE_PREFIXES = ('网盘整理', '刮削入库', '手动整理网盘文件')
STALL_GRACE_MIN = 30          # 大文件宽限阈值（分钟）
STALL_GRACE_RUNTIME_MIN = 60  # 触发宽限的运行时长下限（分钟）

_organize_state = {'snap': {}, 'stall_fired': {}, 'stall_last_push': {}, 'prev_queued': 0,
                   'last_clear_at': None, 'pending_clear': False,
                   'batch': {'active': False, 'done': 0, 'failed': 0, 'cancelled': 0,
                             'm_ok': 0, 'm_bad': 0, 'started_at': None,
                             'last': None, 'last_ts': None}}
_trigger_lock = threading.Lock()          # 忙锁：整理下一批触发期间拒绝并发/连点
_trigger_token = {'val': None, 'exp': 0}  # 一次性令牌（30 分钟有效，每次清空提醒轮换）


def _log_last_time(run_id: int):
    """任务日志接口最后一条（=最新，实测 limit=1 返回新→旧首条）的 (ISO时间, message)。"""
    try:
        s, b = api_get(f'/api/workflows/{run_id}/logs?limit=1')
        if s != 200:
            return None
        items = b.get('items') if isinstance(b, dict) else b
        it = (items or [None])[0]
        if it and it.get('created_at'):
            return it.get('created_at'), (it.get('message') or '')
    except Exception:
        pass
    return None


def _parse_ts(v):
    try:
        return datetime.fromisoformat((v or '').replace('Z', '+00:00')).astimezone(TZ)
    except Exception:
        return None


def _fmt_hhmm(v):
    if isinstance(v, datetime):            # v2.5.5 修复：生产里 started_at 是 datetime 对象
        try:                               # （原实现当字符串再解析→异常→恒 --:--）
            return v.astimezone(TZ).strftime('%H:%M')
        except Exception:                  # naive datetime 无时区，直接格式化
            return v.strftime('%H:%M')
    d = _parse_ts(v)
    return d.strftime('%H:%M') if d else '--:--'


def check_organize_running(now=None):
    """静止告警 + 完成提醒（对整理类 running 任务）。

    最后推进时间：优先任务日志接口最后一条时间；接口失败/无日志时回退
    「succeeded+failed+active 计数快照对比」（计数变化即视为推进）。
    """
    now = now or _now()
    s, b = api_get(f'/api/workflows?status=running&limit={PAGE}&offset=0')
    if s != 200 or not isinstance(b, dict):
        return
    items = b.get('items') or []
    org = [t for t in items if (t.get('display_title') or '').startswith(ORGANIZE_PREFIXES)]
    cur = {}
    for t in org:
        cnt = ((t.get('succeeded_count') or 0) + (t.get('failed_count') or 0)
               + (t.get('active_count') or 0))
        cur[t['id']] = {'title': t.get('display_title') or '', 'item': t.get('item_count') or 0,
                        'ok': t.get('succeeded_count') or 0, 'bad': t.get('failed_count') or 0,
                        'cnt': cnt, 'started_at': t.get('started_at') or ''}
    for rid, c in cur.items():                    # 每任务一次日志尾时间（判据主信号，判据与快照共用）
        info = _log_last_time(rid)
        c['last_at'] = info[0] if info else None

    prev = _organize_state['snap']
    gone = [rid for rid in prev if rid not in cur]

    # ---- v2.5.4/5 批次累计 + 清空提醒（取代单任务完成即推） ----
    # v2.5.5 统计漏项修复：快照差分只做「批开始/批结束」判据；任务/媒体统计改为
    # 清空时按批时间窗（batch.started_at→now）从 ETKN 查询终态整理类任务累加，
    # 不再依赖 running 快照捕获（快任务在两次检查之间完成/监控重启时旧机制必漏）。
    batch = _organize_state['batch']
    any_final_seen = bool(gone)          # 本轮有快照任务离开 running（旧机制信号）

    if SETTINGS['push_enabled'] and SETTINGS['alert_finish_enabled']:
        nq = -1                               # -1=本轮排队数未知（未取/取失败）
        if not cur:
            s3, b3 = api_get(f'/api/workflows?status=queued&limit={PAGE}&offset=0')
            if s3 == 200 and isinstance(b3, dict):
                qitems = b3.get('items') or []
                nq = sum(1 for t in qitems
                         if (t.get('display_title') or '').startswith(ORGANIZE_PREFIXES))
        total_prev = len(prev) + _organize_state['prev_queued']
        total_cur = len(cur) + max(nq, 0)
        known = bool(cur) or nq >= 0          # 排队数未知时不评估，防误报清空
        if (batch['active'] and known and total_cur == 0
                and (total_prev > 0 or _organize_state.get('pending_clear'))):
            # ---- v2.5.5 统计漏项修复：权威口径=批时间窗查询（清空瞬间一次，低频） ----
            # 任务/媒体不再依赖 running 快照捕获（快任务两轮检查间完成/重启会漏），
            # 改为按 finished_at ∈ [批开始, now] 查全部终态整理类任务累加。
            w0 = (batch['started_at'] or _organize_state.get('last_clear_at')
                  or (now - timedelta(hours=24)))   # 批开始缺失→上次清空时刻→兜底24h
            t_done = t_failed = t_cancelled = m_ok = m_bad = 0
            last = None
            last_fin = None
            win_ok = False
            for st in ('succeeded', 'partial', 'failed', 'cancelled'):
                off = 0
                while off < 500:                  # 上限 500 条，防异常爆量
                    s4, b4 = api_get(f'/api/workflows?status={st}&limit={PAGE}&offset={off}')
                    if s4 != 200 or not isinstance(b4, dict):
                        win_ok = False            # 任一状态页失败=窗口数据不完整
                        break
                    win_ok = True
                    its = b4.get('items') or []
                    for t in its:
                        fin = _parse_ts(t.get('finished_at'))
                        if fin is None or fin < w0 or fin > now:
                            continue
                        if not (t.get('display_title') or '').startswith(ORGANIZE_PREFIXES):
                            continue
                        if st in ('succeeded', 'partial'):
                            t_done += 1
                        elif st == 'failed':
                            t_failed += 1
                        else:
                            t_cancelled += 1
                        m_ok += int(t.get('succeeded_count') or 0)
                        m_bad += int(t.get('failed_count') or 0)
                        if last_fin is None or fin > last_fin:
                            last_fin = fin
                            d0l = _parse_ts(t.get('started_at'))
                            last = {'id': t['id'], 'title': t.get('display_title') or '',
                                    'min': max(1, round((fin - d0l).total_seconds() / 60)) if d0l else 0}
                    if len(its) < PAGE:
                        break
                    off += PAGE
            if not win_ok:                        # 窗口查询失败：挂起重试（下轮窗口成功再推），
                _organize_state['pending_clear'] = True   # 不推错误数据、不重置批次状态
                return                            # 本轮到此为止（静止告警也不推：数据不明）
            _organize_state['pending_clear'] = False
            batch['done'], batch['failed'], batch['cancelled'] = t_done, t_failed, t_cancelled
            batch['m_ok'], batch['m_bad'] = m_ok, m_bad
            if last:
                batch['last'] = last
            fast = _state.get('fast') or {}
            by = ((fast.get('active') or {}).get('by_kind') or {})
            def _k(k):                       # 全类型快照行（复用分类队列同源数据）
                d = by.get(k) or {}
                return (d.get('running', 0), d.get('queued', 0))
            qr, qq = _k('刮削入库'); nr, nq2 = _k('网盘整理')
            sr, sq = _k('共享登记'); wr, wq = _k('追剧刷新')
            org_keys = ('网盘整理', '刮削入库', '手动整理网盘文件')
            a_run = sum((by.get(k) or {}).get('running', 0) for k in org_keys)
            a_que = sum((by.get(k) or {}).get('queued', 0) for k in org_keys)
            lines = ['✅ ETKN 整理任务已清空，可以整理下一批']
            scope = SETTINGS['finish_scope']
            task_part = f"任务 完成{batch['done']}·失败{batch['failed']}·取消{batch['cancelled']}"
            media_part = f"媒体 完成{batch['m_ok']}·失败{batch['m_bad']}"
            if scope == 'ok':                # 仅成功范围：失败数值归零展示，隐藏失败细节行
                media_part = f"媒体 完成{batch['m_ok']}·失败0"
            lines.append(f'本批：{task_part} ｜ {media_part}')
            if batch['last']:
                lines.append(f"最后任务：#{batch['last']['id']}「{batch['last']['title'][:40]}」"
                             f"· 耗时 {batch['last']['min']} 分钟")
            if batch['started_at']:
                tmin = max(1, round((now - batch['started_at']).total_seconds() / 60))
                lines.append(f'本批总耗时 {tmin} 分钟（{_fmt_hhmm(batch["started_at"])} 开始 → '
                             f'{_fmt_hhmm(now)} 清空）')
            lines.append(f'当前队列：刮削 运行{qr}/排队{qq} · 网盘 {nr}/{nq2} · '
                         f'共享 运行{sr}/排队{sq} · 追剧 运行{wr}/排队{wq}')
            if batch['failed'] and scope != 'ok':
                lines.append(f"⚠ 本批 {batch['failed']} 个失败，可在面板任务统计页查看并重试")
            if SETTINGS['trigger_enabled'] and not a_run and not a_que:
                # 富文本入口：一次性令牌 30 分钟；每次清空轮换，旧令牌作废
                tok = secrets.token_urlsafe(24)
                _trigger_token['val'] = tok
                _trigger_token['exp'] = time.time() + 1800
                base = (SETTINGS['trigger_public_base'] or f'http://{LAN_HOST}').rstrip('/')
                lines.append(f'👉 [整理下一批]({base}/trigger/organize?token={tok})'
                             '（30 分钟内有效，点击确认后触发）')
            text = '\n'.join(lines)
            ok, err = feishu_push(text)
            record_push('clear', text, ok, err)
            _organize_state['last_clear_at'] = now
            _organize_state['batch'] = {'active': False, 'done': 0, 'failed': 0,
                                        'cancelled': 0, 'm_ok': 0, 'm_bad': 0,
                                        'started_at': None, 'last': None, 'last_ts': None}
        elif total_cur > 0:
            batch['active'] = True            # 有整理任务在跑/在排=批次进行中
            if batch['started_at'] is None:
                # 批开始=最早任务（运行中∪排队中）开始时刻（回填：重启后仍覆盖重启前的任务）
                d0s = [d for d in (_parse_ts(c.get('started_at')) for c in cur.values()) if d]
                if not d0s and nq > 0:
                    d0s = [d for d in (_parse_ts(t.get('started_at'))
                                       for t in ((b3.get('items') or []) if s3 == 200 else [])) if d]
                batch['started_at'] = min(d0s) if d0s else now
        if nq >= 0:                           # 评估完成后才更新排队快照（v2.5.5 修复：
            _organize_state['prev_queued'] = nq   # 原先提前覆盖导致纯排队批次清空永不触发）

    # ---- 静止告警 ----
    if SETTINGS['push_enabled'] and SETTINGS['alert_stall_enabled']:
        thr_min = int(SETTINGS['stall_threshold_min'])
        rep_min = int(SETTINGS['stall_repeat_min'])
        grace = bool(SETTINGS['stall_grace_enabled'])
        fast = _state.get('fast') or {}
        by_kind = ((fast.get('active') or {}).get('by_kind') or {})
        a_run = sum((by_kind.get(k) or {}).get('running', 0) for k in ('网盘整理', '刮削入库', '手动整理网盘文件'))
        a_que = sum((by_kind.get(k) or {}).get('queued', 0) for k in ('网盘整理', '刮削入库', '手动整理网盘文件'))
        a_shr = (by_kind.get('共享登记') or {}).get('running', 0)
        qline = f'当前队列：运行 {a_run} · 排队 {a_que}（共享登记 {a_shr}）'
        for rid, c in cur.items():
            last_iso = c.get('last_at')
            old = prev.get(rid)
            if last_iso:                                  # 主判据：日志尾时间变化=有推进
                advanced = (old is None) or (old.get('last_at') != last_iso)
            else:                                         # 回退：计数快照对比
                advanced = bool(old) and c['cnt'] != old.get('cnt')
                last_iso = old.get('last_at') if old else None
            if advanced:
                _organize_state['stall_fired'].pop(rid, None)
                _organize_state['stall_last_push'].pop(rid, None)
            d_last = _parse_ts(last_iso)
            if d_last is None:
                continue                              # 无任何时间信号，不妄报
            still_min = (now - d_last).total_seconds() / 60
            eff_thr = thr_min
            if grace:
                d0g = _parse_ts(c['started_at'])
                if d0g:
                    run_min = (now - d0g).total_seconds() / 60
                    if run_min >= STALL_GRACE_RUNTIME_MIN:  # 大文件宽限：运行超1小时阈值放宽
                        eff_thr = STALL_GRACE_MIN
            if still_min >= eff_thr:
                last_push = _organize_state['stall_last_push'].get(rid, 0)
                now_ts = time.time()
                if rid not in _organize_state['stall_fired']:
                    _organize_state['stall_fired'][rid] = True
                    need = True
                else:
                    need = now_ts - last_push >= rep_min * 60
                if need:
                    _organize_state['stall_last_push'][rid] = now_ts
                    remain = max(0, c['item'] - c['ok'] - c['bad'])
                    text = _alert_text('整理任务静止', [
                        f'任务 #{rid}「{c["title"][:40]}」已静止 {int(still_min)} 分钟',
                        f'进度 {c["ok"]}/{c["item"]}（剩 {remain}）· 最后推进 {_fmt_hhmm(last_iso)}',
                        qline,
                        '建议：查最后推进时间前后是否卡在 TMDB 外呼；必要时取消重派或重启容器'])
                    ok, err = feishu_push(text)
                    record_push('stall', text, ok, err)
            else:
                _organize_state['stall_fired'].pop(rid, None)
                _organize_state['stall_last_push'].pop(rid, None)

    _organize_state['snap'] = {rid: {'cnt': c['cnt'], 'last_at': c.get('last_at')}
                               for rid, c in cur.items()}


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
            check_organize_running()          # v2.5.3：整理静止告警 + 完成提醒
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

# v2.5.5 一键整理确认页（极简内联样式；取消为默认焦点；确认走 POST）
_TRIGGER_PAGE = """<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>整理下一批 · 确认</title><style>
body{{font-family:system-ui,-apple-system,'PingFang SC','Microsoft YaHei',sans-serif;
background:#0f1420;color:#e8ecf3;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}}
.card{{background:#171e2e;border:1px solid #2a3450;border-radius:14px;padding:28px 30px;max-width:420px;width:92%}}
h1{{font-size:19px;margin:0 0 6px}} .sub{{color:#8b96ad;font-size:13px;margin-bottom:14px}}
.snap{{background:#0f1420;border:1px solid #2a3450;border-radius:8px;padding:10px 12px;font-size:13px;
color:#aebad0;margin-bottom:18px;font-family:ui-monospace,monospace}}
.btns{{display:flex;gap:10px}} button{{flex:1;padding:11px 0;border-radius:9px;border:0;font-size:15px;cursor:pointer}}
.b-ok{{background:#2f81f7;color:#fff}} .b-no{{background:#232c42;color:#c3cbdc}}
#msg{{margin-top:14px;font-size:13px;min-height:18px}}
.ok{{color:#3fb950}} .bad{{color:#f85149}}</style></head><body>
<div class="card"><h1>✅ 确认整理下一批？</h1>
<div class="sub">将触发 ETKN 原生「手动整理网盘文件」（与面板按钮同一接口）</div>
<div class="snap">当前队列快照：{snap}</div>
<div class="btns">
<form id="f" method="post" action="/trigger/organize" style="flex:1;display:contents">
<input type="hidden" name="token" value="{token}">
<button type="button" class="b-no" id="bNo" autofocus onclick="location.href='about:blank'">取消</button>
<button type="button" class="b-ok" id="bOk" onclick="doGo()">确认整理</button>
</form></div><div id="msg"></div></div>
<script>
async function doGo(){{
  const m=document.getElementById('msg'),o=document.getElementById('bOk');
  o.disabled=true;m.textContent='触发中…';m.className='';
  try{{
    const r=await fetch('/trigger/organize',{{method:'POST',
      headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{token:'{token}'}})}});
    const d=await r.json().catch(()=>({{}}));
    if(r.ok&&d.ok){{m.textContent='✅ 已触发，网盘整理任务已入队';m.className='ok';}}
    else{{m.textContent='❌ '+(d.error||('失败 '+r.status));m.className='bad';o.disabled=false;}}
  }}catch(e){{m.textContent='❌ '+e;m.className='bad';o.disabled=false;}}
}}
</script></body></html>"""

_TRIGGER_PAGE_BAD = """<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>链接已失效</title>
<style>body{font-family:system-ui,sans-serif;background:#0f1420;color:#e8ecf3;display:flex;
min-height:100vh;align-items:center;justify-content:center;margin:0}
.c{text-align:center;color:#8b96ad} b{color:#f85149;font-size:17px}</style></head><body>
<div class="c"><b>链接已失效或已使用</b><div style="margin-top:8px">令牌一次性、30 分钟有效；<br>请使用最新一条清空提醒里的按钮</div></div>
</body></html>"""

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
                'version': 'v2.5.5', 'readonly': False,
                'actions': ['speedtest', 'retry-failed', 'run-organize-p115',
                            'run-generate-covers', 'purge-register-queued', 'bad-media',
                            'settings', 'test-push', 'check-500-now', 'speed-now',
                            'trigger-organize'],
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
        m = re.match(r'^/trigger/organize$', p)
        if m:                                 # v2.5.5 一键整理入口（GET 确认页）
            tok_q = (urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                     .get('token') or [''])[0]
            valid = (SETTINGS['trigger_enabled'] and _trigger_token['val']
                     and tok_q == _trigger_token['val'] and time.time() < _trigger_token['exp'])
            if not valid:
                return self._send(403, _TRIGGER_PAGE_BAD.encode(), 'text/html; charset=utf-8')
            fast = _state.get('fast') or {}
            by = ((fast.get('active') or {}).get('by_kind') or {})
            def _kk(k):
                d = by.get(k) or {}
                return f"{d.get('running', 0)}/{d.get('queued', 0)}"
            snap_line = (f"刮削 {_kk('刮削入库')} · 网盘 {_kk('网盘整理')} · "
                         f"共享 {_kk('共享登记')} · 追剧 {_kk('追剧刷新')}")
            return self._send(200, _TRIGGER_PAGE.format(
                token=tok_q, snap=snap_line, gen=secrets.token_hex(8)).encode(),
                'text/html; charset=utf-8')
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
        for k in ('push_enabled', 'alert_500_enabled', 'alert_speed_enabled', 'alert_backlog_enabled',
                  'alert_stall_enabled', 'stall_grace_enabled', 'alert_finish_enabled',
                  'trigger_enabled'):
            if k in b:
                SETTINGS[k] = bool(b[k])
        for k, lo in (('interval_500_min', 5), ('interval_speed_min', 0),
                      ('count_500_threshold', 1), ('backlog_threshold', 1),
                      ('speed_threshold_ms', 1000),
                      ('stall_threshold_min', 1), ('stall_repeat_min', 5)):
            if k in b:
                try:
                    v = int(b[k])
                    if v >= lo:
                        SETTINGS[k] = v
                except Exception:
                    pass
        if 'finish_scope' in b and b['finish_scope'] in ('ok', 'all'):
            SETTINGS['finish_scope'] = b['finish_scope']
        if 'trigger_public_base' in b and isinstance(b['trigger_public_base'], str):
            SETTINGS['trigger_public_base'] = b['trigger_public_base'].strip().rstrip('/')
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
        if p == '/trigger/organize':          # v2.5.5 确认执行（忙锁+令牌+防连点）
            b = self._body()
            tok_q = str(b.get('token') or '')
            valid = (SETTINGS['trigger_enabled'] and _trigger_token['val']
                     and tok_q == _trigger_token['val'] and time.time() < _trigger_token['exp'])
            if not valid:
                return self._send(403, json.dumps({'error': '令牌无效或已过期（30 分钟有效期），'
                                                  '请使用最新一条清空提醒里的链接'},
                                                  ensure_ascii=False).encode())
            if _trigger_lock.locked():
                return self._send(429, json.dumps({'error': '整理已在触发中，请勿连点'},
                                                  ensure_ascii=False).encode())
            with _trigger_lock:               # 拿锁后二次复核令牌（防近同时双击竞态）与队列，再发
                if not (SETTINGS['trigger_enabled'] and _trigger_token['val']
                        and tok_q == _trigger_token['val'] and time.time() < _trigger_token['exp']):
                    return self._send(403, json.dumps({'error': '令牌无效或已过期'},
                                                      ensure_ascii=False).encode())
                fast = _state.get('fast') or {}
                by = ((fast.get('active') or {}).get('by_kind') or {})
                busy = any((by.get(k) or {}).get('running', 0) or (by.get(k) or {}).get('queued', 0)
                           for k in ('网盘整理', '刮削入库', '手动整理网盘文件'))
                if busy:
                    return self._send(409, json.dumps(
                        {'error': '整理队列非空，未触发；请稍后再看'}, ensure_ascii=False).encode())
                _trigger_token['val'] = None  # 一次性：触发即作废
                _trigger_token['exp'] = 0
                payload = {'parameters': {'trigger': 'telegram', 'task_key': 'organize-p115',
                                          'module_key': 'p115_organize',
                                          'handoff_mode': 'independent'}}
                s, b2 = api_post('/api/task-center/tasks/organize-p115/runs', payload)
            return self._send(s if s > 0 else 502, json.dumps(
                {'ok': s in (200, 201, 202), 'etkn_status': s, 'etkn_body': b2},
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
