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
import base64
import json
import os
import re
import secrets
import shlex
import socket
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import deque
from datetime import datetime, timedelta, timezone

BASE = os.environ.get('ETKN_BASE_URL', 'http://192.168.1.22:5257').rstrip('/')
LAN_HOST = os.environ.get('MONITOR_LAN_HOST', '192.168.1.22:8620')   # 内网面板地址（清空提醒链接用）
ETKN_PUBLIC_URL = 'https://etkn.example.com'   # v2.6 卡片「打开ETKN」按钮（ETKN 主程序外网入口）

CARD_LINKS_DEFAULT = [  # v2.7（六）：卡片按钮可配置，「整理下一批」固定带令牌不在此列
    {'text': 'ETKN监控', 'url': 'https://etknjk.example.com/'},
    {'text': 'ETKN', 'url': 'https://etkn.example.com/'},
    {'text': 'CloudDrive2', 'url': 'https://cd2.example.com/'},
]
USERNAME = os.environ.get('ETKN_USERNAME', 'YisBoss')
PASSWORD = os.environ.get('ETKN_PASSWORD', '')
ETA_WINDOW_MIN = float(os.environ.get('ETKN_ETA_WINDOW', '10'))
POLL_INTERVAL = float(os.environ.get('POLL_INTERVAL', '120'))   # 慢速全量轮询
FAST_INTERVAL = float(os.environ.get('FAST_INTERVAL', '2'))     # 快速活跃队列轮询
TZ = timezone(timedelta(hours=8))          # 展示时区固定北京
PAGE = 100                                  # workflows 翻页大小

# v2.8.7 测速目标全面配置化：代码零默认域名（全新安装=空列表，用户在设置页自增）。
# 用户目标持久化在 settings.json 的 speed_targets（部署时从旧常量迁移，不丢项）。
SPEEDTEST_VIA_NOTE: dict = {}               # （v2.8.7 退役）面板口径备注改为 per-target proxy 字段


def _norm_speed_targets(raw) -> list:
    """清洗用户配置的测速目标列表：{host:域名或URL, note:可选备注, proxy:可选代理}。
    host 支持裸域名或 http(s) URL（剥协议取域名）；备注≤20 字；proxy 仅接受 host:port；
    host 重复去重；最多 12 项。非法项剔除（不静默整表拒存）。"""
    out, seen = [], set()
    if not isinstance(raw, list):
        return out
    for it in raw:
        if not isinstance(it, dict):
            continue
        h = str(it.get('host') or '').strip()
        if not h:
            continue
        m = re.match(r'^(?:https?://)?([^/:?#]+)', h, re.I)
        if not m:
            continue
        h = m.group(1).strip('.').lower()
        # host 必须是合法域名形态（字母/数字/点/连字符，带至少一个点）——空格等非法输入剔除
        if not h or not re.fullmatch(r'[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)+', h):
            continue
        if h in seen:
            continue
        seen.add(h)
        note = str(it.get('note') or '').strip()[:20]
        px = str(it.get('proxy') or '').strip()
        if px and not re.fullmatch(r'[A-Za-z0-9._-]+:\d{1,5}', px):
            px = ''
        out.append({'host': h, 'note': note, 'proxy': px or None})
        if len(out) >= 12:
            break
    return out


def _speed_targets() -> list:
    """当前测速目标（settings.speed_targets），空=无目标（不测速不推送）。"""
    return _norm_speed_targets(SETTINGS.get('speed_targets'))

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
    # ---- v2.7 自动喂料（清空后源目录→待整理目录自动分批转移） ----
    'feed_enabled': False,         # 自动喂料开关（默认关）
    'feed_src_dir': '/115/自动整理入库',                                   # 源目录（CD2 WebDAV 路径）
    'feed_dst_dir': '/115/媒体库-ETKN/待整理目录',                         # 目标目录（CD2 WebDAV 路径）
    'feed_batch_limit': 500,       # 每批文件夹上限（v2.8.6：一个剧夹=1 项，整夹转移）
    # ---- v2.8 喂料通道：CloudDrive2 WebDAV（不碰 fuse，凭据存设置不硬编码） ----
    'cd2_dav_url': 'http://192.168.1.22:19798/dav',
    'cd2_user': '',
    'cd2_pass': '',
    # ---- v2.8 hosts 自动更新（域名漂移自愈） ----
    'hosts_enabled': False,        # hosts 自动巡检开关（默认关）
    # ---- v2.8.8 hosts 域名配置化：默认空=不监控；代码不再写死 shared 域 ----
    'hosts_domain': '',            # 要巡检的域名（用户自填；空=跳过巡检）
    # ---- v2.8.10 路由器连接公版化：4 项全从设置读（不硬编码 IP/用户/凭据） ----
    'router_ip': '',               # 软路由 IP（空=巡检直接跳过，不误报）
    'router_port': 22,             # SSH 端口
    'router_user': '',             # SSH 用户名
    'router_pass': '',             # SSH 密码（存本地 settings.json；GET 接口掩码回传）
    # ---- v2.8.8 喂料触发延迟可配置：转移成功后隔 N 秒触发整理（0=立即） ----
    'feed_trigger_delay': 10,      # 默认 10 秒（旧版固定 90 秒退役；防 115 限流留缓冲）
    # ---- v2.8.7 测速目标配置化：默认空列表（零预设域名），设置页编辑器增删改 ----
    'speed_targets': [],           # [{'host':'域名','note':'备注','proxy':'host:port'|None}]
    # ---- v2.7 静止告警自动处置（只 restart，禁重建） ----
    'auto_restart_enabled': False, # 自动重启开关（默认关）
    # ---- v2.7 卡片按钮可配置（六）：[{'text','url'}]，空/非法剔除，≤6 个 ----
    'card_links': CARD_LINKS_DEFAULT,
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
              'trigger_enabled', 'feed_enabled', 'auto_restart_enabled'):
        SETTINGS[k] = bool(SETTINGS[k])
    for k in ('interval_500_min', 'interval_speed_min', 'count_500_threshold',
              'backlog_threshold', 'speed_threshold_ms',
              'stall_threshold_min', 'stall_repeat_min', 'feed_batch_limit'):
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
    for k in ('feed_src_dir', 'feed_dst_dir'):
        if not isinstance(SETTINGS[k], str) or not SETTINGS[k].strip():
            SETTINGS[k] = SETTINGS_DEFAULTS[k]
    cl = _norm_card_links(SETTINGS.get('card_links'))   # v2.7（六）：脏数据兜底
    SETTINGS['card_links'] = cl if cl else [dict(x) for x in CARD_LINKS_DEFAULT]
    # v2.8.7 测速目标：载入即清洗（空列表合法=用户还没配；脏数据剔除不整表拒收）
    SETTINGS['speed_targets'] = _norm_speed_targets(SETTINGS.get('speed_targets'))


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


def feishu_push(text: str, buttons: list = None, title: str = '', tcolor: str = 'blue'):
    """飞书自定义机器人 Webhook。v2.6：buttons 非空 → msg_type=interactive 卡片
    （title=卡片标题；正文=原文全文保留，口径零变化），否则 msg_type=text 兼容旧通道。
    v2.8.2⑤：所有卡片正文最末尾追加小字时间戳行（YYYY-MM-DD HH:MM:SS，北京时区），
    飞书消息列表自带时间不明显，方便对照推送史/日志。
    buttons=[{'tag':'default','text':'整理下一批','url':'https://…','type':'primary'},…]。
    返回 (ok, err)。零 token，不经过第三方。"""
    url = (SETTINGS.get('webhook_url') or '').strip()
    if not url.startswith(('http://', 'https://')):
        return False, '未配置 Webhook URL'
    ts_line = _now().strftime('%Y-%m-%d %H:%M:%S')
    if buttons:
        # 卡片：header 标题 + markdown 正文（原文全量）+ 链接行 + 时间戳小字
        first, _, rest = text.partition('\n')
        body = rest.strip() or first
        if not title:
            title = first
        # v2.8.9：按钮行退役（webhook 通道按钮只能竖排，9-19 四结构实证），
        # buttons 转为正文最底部一行 markdown 链接（与正文空一行独立，紧凑不占高度）。
        # 链接来源=buttons 参数（_card_buttons() 从设置 card_links 生成，令牌按钮首位），
        # 功能不丢：令牌一键整理也变成可点链接。
        link_line = ' · '.join(
            f"[{b.get('text', '打开')}]({b['url']})" for b in buttons if b.get('url'))
        if link_line:
            body = f"{body}\n\n{link_line}"
        elements = [{'tag': 'div', 'text': {'tag': 'lark_md', 'content': body}}]
        # v2.8.2⑤：正文最末尾小字时间戳（note 元素，灰色小号）
        elements.append({'tag': 'note', 'elements': [
            {'tag': 'plain_text', 'content': f'推送时间 {ts_line}'}]})
        payload = {'msg_type': 'interactive', 'card': {
            'header': {'template': tcolor, 'title': {'tag': 'plain_text', 'content': title}},
            'elements': elements}}
    else:
        payload = {'msg_type': 'text', 'content': {'text': text}}
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode('utf-8'),
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


def _alert_text(kind_line: str, detail_lines: list, icon: str = '⚠️') -> str:
    """v2.8.7：图标参数化（默认 ⚠️ 兼容旧调用；成功通知传 ✅/ℹ️）。
    v2.8.7⑥：正文「时间」行删除——卡片底部 note 已有统一「推送时间」，正文再报一次重复。"""
    return (icon + ' ETKN 告警 · ' + kind_line + '\n' + '\n'.join(detail_lines))


def _alert_push(kind: str, kind_line: str, detail_lines: list, buttons: list = None,
                tcolor: str = 'yellow'):
    """v2.6 卡片化告警推送：文本口径不变，加标题/按钮。
    v2.8.7：标题图标随卡片颜色走——绿=✅成功通知、黄=⚠️告警、蓝=ℹ️信息；
    文本首行同步（推送史/纯文本通道口径一致）。失败/限流仍黄三角，成功才绿对勾。"""
    icon = {'green': '✅', 'blue': 'ℹ️'}.get(tcolor, '⚠️')
    text = _alert_text(kind_line, detail_lines, icon=icon)
    if buttons:
        ok, err = feishu_push(text, buttons=buttons, title=f'{icon} ' + kind_line, tcolor=tcolor)
    else:
        ok, err = feishu_push(text)
    record_push(kind, text, ok, err)
    return ok, err


# ================= v2.8 CD2 WebDAV 客户端（喂料通道） =================
def _cd2_dav(method: str, path: str, dest: str = None, data: bytes = None,
             depth: str = None) -> tuple:
    """CloudDrive2 WebDAV 调用（Basic 认证）。返回 (status, body)。凭据读设置，不落日志。"""
    from urllib.parse import quote
    user = SETTINGS.get('cd2_user') or ''
    pwd = SETTINGS.get('cd2_pass') or ''
    base = (SETTINGS.get('cd2_dav_url') or '').rstrip('/')
    if not base:
        return 0, '未配置 cd2_dav_url'.encode('utf-8')
    token = base64.b64encode(f'{user}:{pwd}'.encode()).decode()
    h = {'Authorization': 'Basic ' + token}
    if depth:
        h['Depth'] = depth
    if dest:
        h['Destination'] = base + quote(dest)
    req = urllib.request.Request(base + quote(path), method=method, headers=h, data=data)
    try:
        r = urllib.request.urlopen(req, timeout=60)
        return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:
        return 0, str(e).encode()


_NS_DAV = '{DAV:}'


def _cd2_list_entries(dav_dir: str):
    """WebDAV 列第一层（Depth=1，一次请求），返回 (夹名列表, 散文件名列表)。
    v2.8.12：散视频文件与夹同权参与喂料——按 href 顺序分两类收集（CD2 返回序）。"""
    s, b = _cd2_dav('PROPFIND', dav_dir, depth='1')
    if s != 207:
        return None
    try:
        root = ET.fromstring(b)
    except ET.ParseError:
        return None
    dirs, files = [], []
    self_seg = urllib.parse.unquote(dav_dir.rstrip('/').rsplit('/', 1)[-1])
    for resp in root.iter(_NS_DAV + 'response'):
        href = resp.findtext(_NS_DAV + 'href') or ''
        gt = resp.findtext(_NS_DAV + 'propstat/' + _NS_DAV + 'prop/' +
                           _NS_DAV + 'getcontenttype') or ''
        name = urllib.parse.unquote(href.rstrip('/').rsplit('/', 1)[-1])
        if not name or name == self_seg:
            continue                     # 排除目录自身条目（首条=目录本身）
        if 'unix-directory' in gt:
            dirs.append(name)
        else:
            files.append(name)
    return dirs, files


def _cd2_list_dirs(dav_dir: str) -> list:
    """v2.8.12 起为兼容包装：只返回夹列表。"""
    r = _cd2_list_entries(dav_dir)
    return None if r is None else r[0]


def _cd2_move(src_dir: str, dst_dir: str, name: str) -> tuple:
    """WebDAV MOVE 整夹移动（CD2 同挂载=115 秒级移动）。返回 (ok, err)。"""
    src = src_dir.rstrip('/') + '/' + name
    dst = dst_dir.rstrip('/') + '/' + name
    s, b = _cd2_dav('MOVE', src, dest=dst)
    if s in (201, 204, 250):
        return True, ''
    return False, f'{_clean_dir_name(name)[:40]}：HTTP {s} {(b or b"")[:60].decode(errors="replace", ).strip()}'


def _cd2_exists(dst_dir: str, name: str) -> bool:
    """目标同名夹探测（PROPFIND Depth=0）。"""
    s, _b = _cd2_dav('PROPFIND', dst_dir.rstrip('/') + '/' + name, depth='0')
    return s in (207, 200)


# ================= v2.7 自动喂料（清空提醒后自动分批转移） =================
_feed_lock = threading.Lock()          # 忙锁：防并发喂料
_feed_state = {'last_run': 0.0, 'moving': False}
_feed_scan_cache = {'ts': 0.0, 'src': None, 'dirs': None}   # 短缓存（防抖动，10 分钟）
_feed_trigger = {'timer': None}        # v2.7.2①：延迟触发整理的定时器

FEED_TREE_TTL = 600                    # v2.8.6：回到短缓存（v2.8.4/v2.8.5 逐夹计数已撤销——
                                       # 每次喂料只打 1 次列目录请求，无需 24h 树缓存）


def _feed_scan_entries(src: str):
    """v2.8.12：扫描源目录（经 CD2 WebDAV，不碰 fuse），一次 PROPFIND 同时返回
    （夹列表, 散文件列表）——散视频文件与剧夹同权参与喂料（用户 9/19 晚定案）。
    排序保持 CD2 返回序（旧→新）；扫描失败返回 None。短缓存 10min 保留（防抖动）。
    缓存兼容：旧缓存条目 dirs 非 None 时视为 (dirs, [])（升级窗口内不重复打请求）。"""
    now = time.time()
    if (_feed_scan_cache['dirs'] is not None and _feed_scan_cache['src'] == src
            and now - _feed_scan_cache['ts'] < FEED_TREE_TTL):
        return _feed_scan_cache['dirs'], _feed_scan_cache.get('files') or []
    r = _cd2_list_entries(src)
    if r is None:
        return None
    dirs, files = r
    _feed_scan_cache.update({'ts': now, 'src': src, 'dirs': dirs, 'files': files})
    return dirs, files


def _feed_scan_dirs(src: str) -> list:
    """v2.8.12 起为兼容包装：只返回夹列表（面板/回归旧调用点用）。"""
    r = _feed_scan_entries(src)
    return None if r is None else r[0]


def _clean_dir_name(name: str) -> str:
    """v2.8.7：剥掉夹名尾部的刮削后缀 {tmdb-xxx}/{tmd-xxx} 及半截花括号残渣。
    源目录夹名形如「小美人鱼2：重返大海 (2000) {tmdb-693134}」，推送里只需可读主名。"""
    out = re.sub(r'\s*\{[^{}]*\}\s*$', '', name or '').strip()
    out = re.sub(r'\s*\{[^{}]*$', '', out).strip()   # 已被截断的半截后缀（如「{tmd…」）
    return out or (name or '')


def _feed_cache_drop(moved_names: list) -> None:
    """转移成功后从缓存里剔除已转走的条目（v2.8.16：夹+散文件都剔）。
    9/20 实锤：只剔夹不剔散文件时，10 分钟缓存窗口内的下一轮喂料会把已转走的
    散文件当真（幽灵文件）→ WebDAV MOVE 404 → 整批中止（Grow Up Show/不是你的恋爱两批）。"""
    if _feed_scan_cache['dirs'] is None:
        return
    gone = set(moved_names)
    _feed_scan_cache['dirs'] = [n for n in _feed_scan_cache['dirs'] if n not in gone]
    _feed_scan_cache['files'] = [n for n in (_feed_scan_cache.get('files') or [])
                                 if n not in gone]


def _feed_plan(dirs: list, limit: int) -> list:
    """分批规则（v2.8.6 口径=文件夹数；纯函数，回归用）：
    正常：按顺序贪心累加夹数，≤上限就继续；加下一个会超 → 停（不拆剧）。
    返回 [夹名]；空列表=源目录无剧。（夹数口径下不存在「单剧超限」——1 ≤ 上限恒成立，
    上限经 _feed_run 里 max(1, ...) 钳制后每个剧夹天然可整批独占。）"""
    return list(dirs[:max(1, limit)])


def _feed_move_batch(src: str, dst: str, picks: list) -> tuple:
    """整夹剪切 src→dst（v2.8：经 CD2 WebDAV MOVE，同挂载=115 秒级移动）。
    v2.8.16：MOVE 返回 404=源文件在 115 端已不存在（CD2 列表缓存残影/已消失）——
    跳过该项继续转剩余（9/20 Grow Up Show 批实证：整批 404 全是幽灵文件）；
    其它错误仍即停回报。返回 (成功名单, 失败描述或 '', 跳过名单)。"""
    ok_names, skipped, err = [], [], ''
    for name in picks:
        if _cd2_exists(dst, name):   # 目标同名已存在：不覆盖（防数据破坏）
            # v2.8.11a：错误文案用清洗后主名（剥 {tmdb-xxx} 后缀），失败卡不再出现刮削残渣
            err = f'{_clean_dir_name(name)[:40]}：目标目录已存在同名文件夹'
            break
        ok, e2 = _cd2_move(src, dst, name)
        if not ok:
            if 'HTTP 404' in (e2 or ''):
                skipped.append(name)   # 源已无：跳过（不是喂料故障）
                continue
            err = e2 or f'{_clean_dir_name(name)[:40]}：移动失败'
            break
        ok_names.append(name)
    return ok_names, err, skipped


def _recent_organize_running(window_min: int = 3) -> bool:
    """v2.8.16b：最近 N 分钟内有没有新网盘整理（p115_organize）任务。
    v2.8.1④ 背景：#21843 明细口 chain_runs=[]（count=1 也空），只能从列表反查。
    v2.8.16b 关键修正：不带 status 的 /api/workflows?page=N 形式 page 参数被服务端
    无视（9/20 实证 page1/2/3 返回同一批 100 条），200 条固定集在任务爆量期不保证含
    最近 3 分钟的新任务 → 判据②失效（#31278 误报根因）。改用 status=running/
    succeeded 两种带状态查询（翻页/limit 正常，13:44 实证），偏移翻到窗口边界即停。"""
    cutoff = _now() - timedelta(minutes=window_min)
    cut_iso = cutoff.isoformat()
    for st in ('running', 'succeeded'):
        off = 0
        while off < 400:                      # 4 页保险：窗口内任务量异常大也不至于无限翻
            s, b = api_get(f'/api/workflows?status={st}&limit={PAGE}&offset={off}')
            if s != 200 or not isinstance(b, dict):
                break
            its = b.get('items') or []
            if not its:
                break
            page_newest = max((x.get('created_at') or '') for x in its)
            for x in its:
                if x.get('workflow_type') != 'p115_organize':
                    continue
                ca = _parse_ts(x.get('created_at'))
                if ca and ca >= cutoff:
                    return True
            if page_newest < cut_iso:          # 整页都早于窗口 → 该状态查完了
                break
            off += PAGE
    return False


def _watch_shell_run(rid: int, source: str) -> None:
    """v2.8.1④：壳任务（手动整理网盘文件）监视——succeeded 后迟迟无派生才告警。
    派生判据二选一即静默：①chain_runs 非空；②任务中心最近 3 分钟有新 p115 整理任务
    （chain_runs 明细口缺陷实证 #21843：count=1 但返回空数组，必须补第二判据）。
    节奏：每 10 秒一查，累计满 180 秒仍判不出派生才告警「疑似 115 配额受限」。
    时间按「轮询次数 × 10 秒」计（虚拟时钟），生产语义等价且可回归快进。"""
    def _watch():
        waited = 0
        try:
            while True:
                time.sleep(10)
                waited += 10
                s, d = api_get(f'/api/workflows/{rid}')
                derived = False
                if s == 200 and isinstance(d, dict):
                    st = d.get('status')
                    if st in ('failed', 'cancelled'):
                        return                        # 失败/取消：静默（另有失败卡）
                    if st == 'succeeded' and (d.get('chain_runs') or []):
                        derived = True                # 判据①：明细口给了派生
                    # 判据②（无论壳状态）：最近 3 分钟任务中心有新网盘整理任务
                    if not derived and _recent_organize_running(3):
                        derived = True
                else:
                    derived = _recent_organize_running(3)   # 明细查询失败也用判据②兜底
                if derived:
                    return                            # 已派生真实整理：静默
                if waited >= 180:
                    # v2.8.16b：按壳任务实际状态出文案（v2.8.1④ 起无条件写「成功」，
                    # #31278 running 中即报「成功未派生」=文案失实）；且壳仍在 running
                    # 时不算空转（还没扫完），只有 succeeded 后满 180 秒无派生才告警
                    if st == 'succeeded':
                        _alert_push('feed_err', '疑似 115 配额受限，未真正开始整理', [
                            f'手动整理壳任务 #{rid} 已成功，但 3 分钟内未派生任何整理任务（引擎扫描 115 空转）',
                            '待整理目录文件不会丢，全部原样等待',
                            '建议等 115 配额窗口恢复后再点「整理下一批」；持续出现请夜间低峰重试'])
                    return
        except Exception:
            pass                                # 监视线程绝不影响主流程
    threading.Thread(target=_watch, daemon=True).start()


def _feed_err_partial(picks_dirs: list, picks_files: list, moved_dirs: list,
                      moved_files: list, total_items: int, err: str) -> None:
    """v2.8.12：转移失败卡（夹/散文件分列，已完成=夹+散合计；夹名清洗沿用 _clean_dir_name）。"""
    done = len(moved_dirs or []) + len(moved_files or [])
    names = [_clean_dir_name(n)[:20] for n in (list(picks_dirs[:3]) + list(picks_files[:2]))]
    brief = '、'.join(names) + ('…' if total_items > 5 else '')
    _alert_push('feed_err', '自动喂料转移失败', [
        f'本批计划：{total_items} 项（剧夹 {len(picks_dirs)} + 散文件 {len(picks_files)}，{brief}）',
        f'已完成 {done} 项后中止：{err[:80]}',
        '已转移部分保留在目标目录，可在面板手动触发整理'])

def _feed_delayed_trigger(moved_n: int, total_files: int, n_dirs: int = 0, n_files: int = 0) -> None:
    """v2.7.2①：转移成功后延迟触发原生整理；③失败时识别 115 限流给重试指引。
    v2.8.14：成功不再单独推卡（合并进「自动喂料完成」单卡）；n_dirs/n_files 仅留签名兼容。"""
    payload = {'parameters': {'trigger': 'telegram', 'task_key': 'organize-p115',
                              'module_key': 'p115_organize', 'handoff_mode': 'independent'}}
    s, b = api_post('/api/task-center/tasks/organize-p115/runs', payload)
    if s in (200, 201, 202):
        rid = (b or {}).get('workflow_run_id') if isinstance(b, dict) else None
        if rid:
            _watch_shell_run(int(rid), 'feed')  # v2.7.3②：空转监视
        return
    body = json.dumps(b, ensure_ascii=False) if isinstance(b, (dict, list)) else str(b or '')
    if ('访问上限' in body) or ('429' in body) or ('限流' in body) or ('Too Many' in body):
        _alert_push('feed_err', '自动喂料触发整理失败（115 限流）', [
            f'本批 {moved_n} 项（剧夹+散文件）已转移成功，但整理触发撞 115 访问上限',
            '文件不会丢：全部在待整理目录等待', '稍后（约 10-30 分钟）到面板点「整理下一批」即可',
            f'错误：{body[:90]}'])
    else:
        _alert_push('feed_err', '自动喂料触发整理失败', [
            f'转移 {moved_n} 项（剧夹+散文件）已成功，但整理触发失败（HTTP {s}）',
            body[:100], '请到面板手动触发「手动整理网盘文件」'])


def _feed_run() -> None:
    """喂料主流程（清空提醒推送成功后调用）：扫描→计划→转移→触发原生整理。
    转移失败/触发失败→飞书告警卡片；空源目录→飞书提示。全程忙锁+冷却。"""
    if not (SETTINGS['push_enabled'] and SETTINGS['feed_enabled']):
        return
    if _feed_state['moving'] or time.time() - _feed_state['last_run'] < 120:
        return                         # 忙锁 + 2 分钟冷却（清空重试窗口内不重复进）
    with _feed_lock:
        if _feed_state['moving']:
            return
        _feed_state['moving'] = True
    try:
        src = SETTINGS['feed_src_dir'].rstrip('/')
        dst = SETTINGS['feed_dst_dir'].rstrip('/')
        limit = max(1, int(SETTINGS['feed_batch_limit'] or 500))   # v2.8.6：每批文件夹数
        scanned = _feed_scan_entries(src)
        if scanned is None:
            _alert_push('feed_err', '自动喂料失败', [
                f'源目录不可读：{src}', '多半是 /cloud115 挂载未生效或权限变化，请检查容器挂载'])
            return
        dirs, loose = scanned
        # v2.8.12：夹+散文件都空才算「源目录已空」（用户 9/19 晚定案：散文件参与转移）
        if not dirs and not loose:
            _alert_push('feed_empty', '源目录已空，可放新文件', [
                f'{src} 当前没有待整理文件夹', '放入新剧/电影后，下次清空提醒会自动喂料'],
                buttons=_card_buttons(), tcolor='blue')
            return
        # v2.8.12 计划：夹优先、散文件补足配额，合计 ≤ limit（按个数累加，一个夹=1 项=一个散文件）
        picks_dirs = dirs[:limit]
        picks_files = loose[:max(0, limit - len(picks_dirs))]
        total_items = len(picks_dirs) + len(picks_files)
        # 转移顺序：先夹后散文件；v2.8.16：404（源已无）跳过继续，其余错误即停回报
        moved_dirs, err, skip_d = _feed_move_batch(src, dst, picks_dirs)
        if err:
            _feed_err_partial(picks_dirs, picks_files, moved_dirs, [], total_items, err)
            return
        moved_files, err, skip_f = _feed_move_batch(src, dst, picks_files)
        if err:
            _feed_err_partial(picks_dirs, picks_files, moved_dirs, moved_files, total_items, err)
            return
        if not moved_dirs and not moved_files:
            _feed_cache_drop(list(skip_d) + list(skip_f))   # 全是幽灵文件：剔缓存防空转
            _alert_push('feed_err', '自动喂料转移失败', [
                f'本批 {total_items} 项全部跳过：源文件在 115 端已不存在（多半是列表缓存残影，'
                '上一批已转走或源已删除）', '已把这些条目从扫描缓存剔除，下轮不再误报'],
                buttons=_card_buttons(), tcolor='yellow')
            return
        _feed_cache_drop(list(moved_dirs) + list(moved_files) + list(skip_d) + list(skip_f))
        # v2.8.14：喂料合并单卡——转移完成+触发整理合一推送，不再连发两张卡
        _delay = int(SETTINGS.get('feed_trigger_delay', 10) or 0)
        _n_d, _n_f = len(moved_dirs), len(moved_files)
        # v2.8.16a 修正：列的是源目录第一层（单次请求，不加 115 负担）——
        # 语义=这批转走后源目录还剩多少待喂；v2.8.16 误写成目标目录
        _left = _cd2_list_entries(src)
        _left_line = None
        if _left is not None:
            _xl, _yl = len(_left[0]), len(_left[1])
            _left_line = f'源目录剩余：{_xl} 个文件夹 + {_yl} 个散文件'
        _lines = ['已转移 ' + (f'{_n_d} 个剧夹' if _n_d else '') +
                  (' + ' if _n_d and _n_f else '') +
                  (f'{_n_f} 个散文件' if _n_f else '') + '到待整理目录']
        if _left_line:
            _lines.append(_left_line)
        if skip_d or skip_f:
            _lines.append(f'另有 {len(skip_d) + len(skip_f)} 项源文件已不存在，已跳过')
        _lines.append('整理已自动提交，完成后推清空提醒')
        _alert_push('feed', '自动喂料完成', _lines,
                    buttons=_card_buttons(), tcolor='green')
        old = _feed_trigger.get('timer')
        if old:
            old.cancel()
        t = threading.Timer(float(_delay), _feed_delayed_trigger,
                            args=(_n_d + _n_f, total_items, _n_d, _n_f))
        t.daemon = True
        _feed_trigger['timer'] = t
        t.start()
    finally:
        _feed_state['moving'] = False
        _feed_state['last_run'] = time.time()


# ============ v2.7 静止告警自动处置（只 restart etkn，禁重建） ============
_auto_state = {'restarts': [], 'pending': None}   # restarts=24h 窗口时刻表；pending=观察期任务


def _auto_restart_allowed() -> str:
    """护栏检查：返回 ''=允许；否则返回拒绝原因（写入告警）。"""
    now = time.time()
    _auto_state['restarts'] = [t for t in _auto_state['restarts'] if now - t < 86400]
    if len(_auto_state['restarts']) >= 2:
        return '24 小时内已自动重启 2 次，超出上限，不再自动重启'
    return ''


def _auto_restart_etkn() -> tuple:
    """执行 docker restart etkn（SSH 白名单单命令；monitor 容器内无 docker/sock）。
    返回 (是否成功, 输出摘要)。"""
    PW = str(os.environ.get('SUDO_PASSWORD') or '')   # v2.8.17：compose env_file 注入优先
    for line in ('/hermes/.env', '/app/hermes/.env', '/vol1/@appdata/trim.hermes/hermes/.env'):
        try:
            with open(line, encoding='utf-8') as f:
                for ln in f:
                    if ln.startswith('SUDO_PASSWORD='):
                        PW = ln.split('=', 1)[1].strip()
                        break
        except OSError:
            continue
        if PW:
            break
    if not PW:
        return False, '宿主凭据不可读（hermes/.env 缺 SUDO_PASSWORD）'
    cmd = ("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
           "%s@%s \"echo %s | sudo -S -p '' docker restart etkn\"" % (
               shlex.quote(PW), shlex.quote(os.environ.get('SSH_PORT', '66')),
               shlex.quote(os.environ.get('SSH_USER', 'YisBoss')),
               shlex.quote(os.environ.get('SSH_HOST', '192.168.1.22')),
               shlex.quote(PW)))
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=90)
        ok = r.returncode == 0 and 'etkn' in (r.stdout or '')
        return ok, (r.stdout or r.stderr).strip().replace('\n', ' ')[:120]
    except Exception as e:
        return False, str(e)[:120]


def _auto_stall_handler(rid, c) -> None:
    """静止告警触发时的自动处置（设置 auto_restart_enabled 开启才动手）：
    观察期（重启后 10 分钟）内再静止 → 升级告警不再动手；否则按护栏 restart。"""
    if not (SETTINGS['push_enabled'] and SETTINGS['auto_restart_enabled']):
        return
    pend = _auto_state['pending']
    now = time.time()
    if pend and rid == pend['rid']:
        if now - pend['ts'] < 600:     # 重启后 10 分钟观察期内又静止 → 升级人工
            _auto_state['pending'] = None
            _alert_push('stall', '自动重启未恢复，需人工介入', [
                f'任务 #{rid}「{(c or {}).get("title", "")[:40]}」在自动重启后再次静止',
                '已按护栏停止自动重启（24h≤2 次且不重复动手）',
                '建议标准流程：取消任务 → 三轮验僵尸 → 低峰重启 etkn → 重派整理',
                f'任务 ID：{rid}'])
            return
        _auto_state['pending'] = None  # 观察期已过才告警 → 视为新事件，走正常护栏
    why = _auto_restart_allowed()
    if why:
        _alert_push('stall', '整理任务静止（自动重启受限）', [
            f'任务 #{rid} 触发自动处置，但{why}', '请人工按标准流程处置'])
        return
    ok, out = _auto_restart_etkn()
    _auto_state['restarts'].append(now)
    _auto_state['pending'] = {'rid': rid, 'ts': now}
    if ok:
        _alert_push('auto_restart', '已自动重启 etkn，观察中', [
            f'因整理任务 #{rid} 静止触发（现有判据）',
            '观察 10 分钟：任务继续推进则静默恢复；再次静止将升级告警需人工介入'],
            tcolor='green')
    else:
        _alert_push('stall', '自动重启 etkn 失败，需人工介入', [
            f'任务 #{rid} 触发自动处置，但执行失败', f'错误：{out}'])


# ============ v2.8 hosts 自动更新（域名漂移自愈；v2.8.8 起域名可配置） ============
# v2.8.8：域名改 settings['hosts_domain']（用户自填，默认空=不监控）；
# _HOSTS_DOMAIN 兼容包装：优先取设置，空时回退旧常量（存量部署不受影响）。
_HOSTS_DOMAIN_CONST = 'shared.55565576.xyz'
_HOSTS_MARKER = '# etkn-monitor-managed'   # 我们负责的行的标记，其他行绝不碰
_hosts_state = {'last_run': 0.0, 'timer': None}


def _HOSTS_DOMAINS() -> list:
    """v2.8.17：监控域名列表——设置 hosts_domain 支持逗号/分号/空白分隔多个；
    未配置回退旧常量（保存量部署）。逐个清洗（小写、去尾点、非空去重）。"""
    raw = str(SETTINGS.get('hosts_domain') or _HOSTS_DOMAIN_CONST)
    out = []
    for tok in __import__('re').split(r'[,，;；\s]+', raw):
        d = tok.strip().lower().rstrip('.')
        if d and d not in out:
            out.append(d)
    return out


def _HOSTS_DOMAIN() -> str:
    """兼容包装：首个监控域名（v2.8.17 起多域名用 _HOSTS_DOMAINS）。"""
    ds = _HOSTS_DOMAINS()
    return ds[0] if ds else '' 


def _resolve_public(name: str, server: str = '223.5.5.5') -> str:
    """用指定 DNS 服务器解析 A 记录（UDP 53 直连指定服务器，绕过本机 DNS/劫持）。
    返回首个 A 记录字符串；失败返回 ''。socket/struct 用模块级导入（可 monkeypatch 测试）。
    v2.8.3：修复 qname 构造 bug——旧写法 bytes([len(x)…])+b''.join(标签) 会把所有
    长度字节集中放在最前（06 08 03 shared'55565576'xyz=畸形报文，服务器不回→超时），
    必须逐段交错：\\x06shared\\x0855565576\\x03xyz\\x00。10:42 hosts_err 每小时误报实证。"""
    import struct as _s
    qname = b''.join(bytes([len(p)]) + p.encode() for p in name.split('.')) + b'\x00'
    q = b'\x12\x34\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00' + qname + b'\x00\x01\x00\x01'
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(8)
        s.sendto(q, (server, 53))
        d, _a = s.recvfrom(1024)
        s.close()
        # 跳 header(12B) + question（扫到 0x00 结束）
        i = 12
        while d[i]:
            i += d[i] + 1
        i += 5                          # 0x00(qname 结尾) + qtype(2) + qclass(2)
        while i + 12 <= len(d):
            # answer 记录：name(2 压缩指针) type(2) class(2) ttl(4) rdlen(2)
            typ = _s.unpack('>H', d[i+2:i+4])[0]
            rdl = _s.unpack('>H', d[i+10:i+12])[0]
            if typ == 1 and rdl == 4:
                return '.'.join(str(x) for x in d[i+12:i+16])
            i += 12 + rdl
    except Exception:
        pass
    return ''


def _ssh_router(cmd: str, timeout: int = 30) -> str:
    """在路由器（设置 router_ip/port/user/pass，不硬编码）上执行 SSH 命令，返回 stdout。
    v2.8.10 公版化：凭据全部读设置；未配置（router_ip/user/pass 任一为空）返回空串，
    由调用方按「未配置」处理（巡检跳过或告警），绝不猜默认 IP。"""
    rip = str(SETTINGS.get('router_ip') or '').strip()
    ruser = str(SETTINGS.get('router_user') or '').strip()
    rpass = str(SETTINGS.get('router_pass') or '')
    if not (rip and ruser and rpass):
        return ''
    try:
        rport = int(SETTINGS.get('router_port') or 22)
    except (TypeError, ValueError):
        rport = 22
    env = dict(os.environ)
    env['SSHPASS'] = rpass
    r = subprocess.run(['sshpass', '-e', 'ssh', '-p', str(rport),
                        '-o', 'StrictHostKeyChecking=no',
                        '-o', 'UserKnownHostsFile=/dev/null', '-o', 'ConnectTimeout=10',
                        f'{ruser}@{rip}', cmd],
                       capture_output=True, env=env, timeout=timeout)
    return (r.stdout + r.stderr).replace(b'\x00', b'').decode('utf-8', errors='replace')


def _hosts_check_once() -> None:
    """单次巡检（v2.8.17 多域名）：路由器连通性只查一次，然后逐域名执行巡检。
    每域名独立：公网解析 → 对比 /etc/hosts → 不同则原子更新该行+重启 dnsmasq+推送；
    解析失败=该域名挂了：不更新 hosts，只推告警。只动带标记的那一行。
    v2.8.10 公版化：路由器连接信息全读设置；未配置/解析失败→跳过不误报。"""
    now = time.time()
    _hosts_state['last_run'] = now
    domains = _HOSTS_DOMAINS()
    if not domains:
        return                      # v2.8.8：未配置域名=不监控，静默
    _r = _ssh_router('true', timeout=15)
    _rconfigured = bool(SETTINGS.get('router_ip') and SETTINGS.get('router_user')
                        and SETTINGS.get('router_pass'))
    if not _rconfigured or 'Permission denied' in _r:
        _alert_push('hosts_err', '路由器连接未配置或认证失败（hosts 未改动）', [
            '请到设置页填写：软路由 IP / SSH 端口 / SSH 用户名 / SSH 密码',
            '巡检需要这些信息登录路由器修改 /etc/hosts；本次仅告警不改行'])
        return
    for _d in domains:              # v2.8.17：逐域名巡检（一域一 IP，互不影响）
        try:
            _hosts_check_one(_d)
        except Exception:
            pass


def _hosts_check_one(domain: str) -> None:
    """单域名巡检核心（v2.8.17 自 _hosts_check_once 抽出，逻辑不变）。"""
    ip = _resolve_public(domain)
    if not ip:
        _alert_push('hosts_err', f'{domain} 域名解析失败（hosts 未改动）', [
            f'223.5.5.5 解析 {domain} 无 A 记录——可能域名/源站故障',
            '/etc/hosts 保持原值，业务暂按旧 IP 走，请人工确认'])
        return
    cur = _ssh_router(f"grep -n '{domain}' /etc/hosts || true")
    found = []                       # (行号, 行内容)
    for ln in cur.splitlines():
        if domain in ln:
            no, _, rest = ln.partition(':')
            if no.isdigit() and rest.strip():
                found.append((int(no), rest))
    mark = [x for x in found if _HOSTS_MARKER in x[1]]      # 优先我们管理的行
    pick = mark[0] if mark else next(
        (x for x in found if len(x[1].split()) >= 2), None)
    if not pick:
        # v2.8.17：无绑定行 → 自动新增带标记行（公网 IP），推蓝卡告知；失败才告警
        new_line = f"{ip} {domain} {_HOSTS_MARKER}"
        script = (f"cp /etc/hosts /etc/hosts.bak-monitor && "
                  f"echo '{new_line}' >> /etc/hosts && "
                  f"/etc/init.d/dnsmasq restart >/dev/null 2>&1; "
                  f"grep -n '{domain}' /etc/hosts")
        out = _ssh_router(script, timeout=45)
        if f'{ip} {domain}' in out:
            _alert_push('hosts', f'{domain}：hosts 新增绑定 {ip}', [
                f'路由器 /etc/hosts 原无 {domain} 独立行，已按公网解析新增并重启 dnsmasq',
                '原文件已备份 /etc/hosts.bak-monitor'],
                buttons=_card_buttons(), tcolor='blue')
        else:
            _alert_push('hosts_err', 'hosts 新增绑定失败（未生效）', [
                f'期望新增 {ip} {domain}，路由器回执异常', f'回执：{(out or "(空)")[:100]}'])
        return
    line_no, rest = pick
    old_ip = rest.split()[0]
    if old_ip == ip:
        return                          # 一致：静默
    # 原子更新：替换该行 + 追加标记注释
    new_line = f"{ip} {domain} {_HOSTS_MARKER}"
    esc = new_line.replace('/', r'\/')
    script = (f"cp /etc/hosts /etc/hosts.bak-monitor && "
              f"sed -i '{line_no}s/.*/{esc}/' /etc/hosts && "
              f"/etc/init.d/dnsmasq restart >/dev/null 2>&1; "
              f"grep -n '{domain}' /etc/hosts")
    out = _ssh_router(script, timeout=45)
    ok = f'{ip} {domain}' in out
    if ok:
        # v2.8.3①：只在 hosts 真改了才推送，卡片写清具体改动（旧IP → 新IP）
        _alert_push('hosts', f'{domain}：{old_ip} → {ip}，hosts 已更新', [
            f'223.5.5.5 公网解析与 hosts 绑定不一致，已将该行改为 {ip} 并重启 dnsmasq',
            '原行已备份 /etc/hosts.bak-monitor'],
            buttons=_card_buttons(), tcolor='green')
    else:
        _alert_push('hosts_err', 'hosts 更新失败（未生效）', [
            f'期望改到 {ip}，路由器回执异常', f'回执：{(out or "(空)")[:100]}'])


def _hosts_recheck() -> list:
    """v2.8.19 手动「重新检测 IP」：逐域名跑巡检（复用 _hosts_check_one，
    改行/新增照旧推送卡片），然后回读路由器 hosts 当前绑定 IP 返回结构化结果。
    返回 [{'domain','dns_ip','hosts_ip','changed'}]，异常域名 hosts_ip=''。"""
    out = []
    domains = _HOSTS_DOMAINS()
    if not domains:
        return out
    for d in domains:
        ip = _resolve_public(d)
        before = _ssh_router(f"grep '{d}' /etc/hosts || true")
        old_ip = ''
        for ln in before.splitlines():
            if d in ln:
                parts = ln.split()
                if len(parts) >= 2:
                    old_ip = parts[0]
        _hosts_check_one(d)
        after = _ssh_router(f"grep '{d}' /etc/hosts || true")
        new_ip = ''
        for ln in after.splitlines():
            if d in ln:
                parts = ln.split()
                if len(parts) >= 2:
                    new_ip = parts[0]
        out.append({'domain': d, 'dns_ip': ip, 'hosts_ip': new_ip,
                    'changed': bool(old_ip and new_ip and old_ip != new_ip)})
    return out


def _hosts_loop() -> None:
    """每小时巡检线程（守护，绝不影响主流程）。"""
    while True:
        try:
            if SETTINGS['push_enabled'] and SETTINGS.get('hosts_enabled'):
                _hosts_check_once()
        except Exception:
            pass
        time.sleep(3600)


def _panel_base() -> str:
    """面板外网基址（卡片「打开面板」按钮用）。"""
    return (SETTINGS.get('trigger_public_base') or f'http://{LAN_HOST}').rstrip('/')


def _norm_card_links(raw) -> list:
    """清洗用户配置的按钮列表：非空名+合法 http(s) URL 才保留，最多 6 个。"""
    out = []
    if not isinstance(raw, list):
        return []
    for it in raw:
        if not isinstance(it, dict):
            continue
        name = str(it.get('text') or '').strip()
        url = str(it.get('url') or '').strip()
        if name and url.startswith(('http://', 'https://')) and ' ' not in url:
            out.append({'text': name[:12], 'url': url})
        if len(out) >= 6:
            break
    return out


def _card_buttons(tok: str = '') -> list:
    """卡片按钮组：[整理下一批(有令牌时)] + 可配置链接（设置 card_links，默认三项）。
    空/非法 URL 的按钮在 _norm_card_links 已剔除，不影响其他按钮。"""
    btns = []
    if tok:
        btns.append({'text': '整理下一批',
                     'url': _panel_base() + '/trigger/organize?token=' + tok,
                     'type': 'primary'})
    for it in _norm_card_links(SETTINGS.get('card_links')) or CARD_LINKS_DEFAULT:
        btns.append({'text': it['text'], 'url': it['url'], 'type': 'default'})
    return btns


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
        _alert_push('t500', 'TMDB HTTP 500', [
            f'今日命中 500 签名异常 {cnt} 条（阈值 {SETTINGS["count_500_threshold"]}）',
            '样例：' + ('、'.join(samples) if samples else '-'),
            '疑似出口节点对 TMDB 限流/拦截；建议复测节点后在任务中心重试失败子项'],
            buttons=_card_buttons())


def run_speed_round(alert: bool = True):
    """一轮测速：写历史；目标=settings.speed_targets（v2.8.7 配置化）。
    列表为空=不测速不写历史不推送（面板显示「未配置测速目标」）。"""
    targets = _speed_targets()
    if not targets:
        return []
    results = []
    for t in targets:
        r = speedtest_one(t['host'], proxy=t['proxy'])
        if t.get('note'):
            r['note'] = t['note']
        results.append(r)
    _speed_hist.appendleft({'ts': _now().isoformat(timespec='seconds'), 'results': results})
    if alert and SETTINGS['push_enabled'] and SETTINGS['alert_speed_enabled']:
        thr = SETTINGS['speed_threshold_ms']
        for r in results:
            bad = (not r['ok']) or (r['total_ms'] is not None and r['total_ms'] > thr)
            host = r['host']
            if bad and not _alm['speed_fired'].get(host):
                _alm['speed_fired'][host] = True
                desc = (f'连接失败：{r["error"]}' if not r['ok']
                        else f'总耗时 {r["total_ms"]}ms（阈值 {thr}ms）')
                _alert_push('speed', '链路测速异常', [
                    f'{host}　{desc}', '定时测速检出该域名异常，可能影响刮削/图片链路'],
                    buttons=_card_buttons())
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
            _alert_push('backlog', '队列积压', [
                f'{k} 排队 {q}（阈值 {thr}）', '该类任务积压，消费可能滞后，建议关注任务中心'],
                buttons=_card_buttons())
        elif q < thr and k in fired:
            fired.discard(k)


# ---------- v2.5.3 整理任务静止告警 + 完成提醒 ----------
# 整理类按 display_title 前缀区分（共享登记/追剧刷新的 workflow_type 同为 manual_task，按类型筛会误伤）
ORGANIZE_PREFIXES = ('网盘整理', '刮削入库', '手动整理网盘文件')
# v2.5.5 口径修复：媒体入库语义任务（succeeded_count=真实入库媒体数）仅「刮削入库」；
# 手动整理/网盘整理=流程任务（succeeded_count 恒 1=流程成功，取证 16/20 例全为 1）
INGEST_PREFIXES = ('刮削入库',)
FLOW_PREFIXES = ('手动整理网盘文件', '网盘整理')
STALL_GRACE_MIN = 30          # 大文件宽限阈值（分钟）
STALL_GRACE_RUNTIME_MIN = 60  # 触发宽限的运行时长下限（分钟）

_organize_state = {'snap': {}, 'stall_fired': {}, 'stall_last_push': {}, 'prev_queued': 0,
                   'last_clear_at': None, 'pending_clear': False,
                   'batch': {'active': False, 'done': 0, 'failed': 0, 'cancelled': 0,
                             'm_ok': 0, 'm_bad': 0, 'started_at': None,
                             'last': None, 'last_ts': None, 'flow': {},
                             'other_fail': []}}
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
            flow_cnt = {}                         # 流程任务分列：{'手动': [d,f,c], '网盘': [d,f,c]}
            fail_log = []                         # v2.8.13：本批失败明细（清空卡汇总展示）
            other_fail = []                       # v2.8.18：非整理类失败明细
            last = None
            last_fin = None
            win_ok = False
            _seen_ids = set()                     # v2.8.20b：现场页已收任务 id（缓存去重）
            for st in ('succeeded', 'partial', 'failed', 'cancelled'):
                off = 0
                while off < 30000:                # v2.8.20b：先查 _today_done 缓存
                    #   （整轮全量今日明细，新鲜度≤1 轮），缓存已覆盖窗口的绝大部分；
                    #   这里只翻顶部 2 页现场补漏（缓存未及的最新完成任务），去重合并。
                    #   原 v2.8.18 全翻 17 分钟（succeeded 百页级）=清空卡延迟 8~10 分根因
                    s4, b4 = api_get(f'/api/workflows?status={st}&limit={PAGE}&offset={off}')
                    if s4 != 200 or not isinstance(b4, dict):
                        win_ok = False            # 任一状态页失败=窗口数据不完整
                        break
                    win_ok = True
                    its = b4.get('items') or []
                    for t in its:
                        if t.get('id') is not None:
                            _seen_ids.add(t['id'])
                        fin = _parse_ts(t.get('finished_at'))
                        if fin is None or fin < w0 or fin > now:
                            continue
                        ttl = t.get('display_title') or ''
                        if not ttl.startswith(ORGANIZE_PREFIXES):
                            continue
                        # v2.5.5 口径修复：入库语义（刮削入库）才计任务/媒体；
                        # 流程任务（手动/网盘整理，succ恒1=流程成功）单独计数不进媒体口径
                        if ttl.startswith(INGEST_PREFIXES):
                            if st in ('succeeded', 'partial'):
                                t_done += 1
                            elif st == 'failed':
                                t_failed += 1
                                # v2.8.13：失败明细（标题清洗+阶段+原因截断，卡片最多展开 5 条）
                                fail_log.append({'title': _clean_dir_name(ttl)[:24],
                                                 'stage': (t.get('failure_stage_title') or '')[:12],
                                                 'err': (t.get('failure_summary') or '')[:60]})
                            else:
                                t_cancelled += 1
                            m_ok += int(t.get('succeeded_count') or 0)
                            m_bad += int(t.get('failed_count') or 0)
                        elif ttl.startswith(FLOW_PREFIXES):
                            fk = '手动' if ttl.startswith('手动整理') else '网盘'
                            fc = flow_cnt.setdefault(fk, [0, 0, 0])
                            if st in ('succeeded', 'partial'):
                                fc[0] += 1
                            elif st == 'failed':
                                fc[1] += 1
                            else:
                                fc[2] += 1
                        elif st in ('failed', 'partial'):
                            # v2.8.18：批次窗口内其他类型失败（共享登记/追剧刷新等）也入卡
                            other_fail.append({'title': _clean_dir_name(ttl)[:24],
                                               'stage': (t.get('failure_stage_title') or '')[:12],
                                               'err': (t.get('failure_summary') or '')[:60]})
                        if last_fin is None or fin > last_fin:
                            last_fin = fin
                            d0l = _parse_ts(t.get('started_at'))
                            last = {'id': t['id'], 'title': t.get('display_title') or '',
                                    'min': max(1, round((fin - d0l).total_seconds() / 60)) if d0l else 0}
                    if len(its) < PAGE:
                        break
                    off += PAGE
                    if off >= PAGE * 2:           # v2.8.20b：顶部 2 页=缓存未及的新鲜任务
                        break                     #（缓存为主+补漏；旧全翻 17 分钟已废弃）
            if not win_ok:                        # 窗口查询失败：挂起重试（下轮窗口成功再推），
                _organize_state['pending_clear'] = True   # 不推错误数据、不重置批次状态
                return                            # 本轮到此为止（静止告警也不推：数据不明）
            # v2.8.20b：缓存补漏——_today_done 整轮全量今日明细与现场 2 页去重合并，
            # 覆盖「排序不稳导致窗口任务藏深页」场景（9/21 实锤），计数口径与原全翻一致。
            try:
                for _d in list(_today_done.get('items') or []):
                    if (_d.get('title') or '') not in ('',) and not (
                            _d.get('title') or '').startswith(ORGANIZE_PREFIXES):
                        continue
                    _fdt = _parse_ts(_d.get('finished_at') or '')
                    if _fdt is None or _fdt < w0 or _fdt > now:
                        continue
                    _did = _d.get('id')
                    if _did in _seen_ids:
                        continue
                    # 现场页是否已收（重查窗口任务的原始查询结果不可得，用时间近似：
                    # 只补「fin 大于现场页最大 fin」的缓存任务——顶部 2 页之外的深页漏网）
                    _st = _d.get('status')
                    _ttl = _d.get('title') or ''
                    if _ttl.startswith(INGEST_PREFIXES):
                        if _st in ('succeeded', 'partial'):
                            t_done += 1
                            m_ok += _d.get('ok_media') or 0
                            m_bad += _d.get('bad_media') or 0
                        elif _st == 'failed':
                            t_failed += 1
                            m_bad += _d.get('bad_media') or 0
                    else:
                        _fk = '手动' if _ttl.startswith('手动整理') else '网盘'
                        _fc = flow_cnt.setdefault(_fk, [0, 0, 0])
                        if _st in ('succeeded', 'partial'):
                            _fc[0] += 1
                        elif _st == 'failed':
                            _fc[1] += 1
                        else:
                            _fc[2] += 1
            except Exception:
                pass
            _organize_state['pending_clear'] = False
            batch['done'], batch['failed'], batch['cancelled'] = t_done, t_failed, t_cancelled
            batch['m_ok'], batch['m_bad'] = m_ok, m_bad
            batch['flow'] = {k: tuple(v) for k, v in flow_cnt.items()}
            batch['fail_log'] = fail_log          # v2.8.13：失败明细随批次窗口落定
            batch['other_fail'] = other_fail      # v2.8.18：非整理类失败随窗口落定
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
            # v2.8.11 排版：每行一字段、左对齐（预览样卡用户已确认）
            f_show = 0 if scope == 'ok' else batch['failed']
            c_show = 0 if scope == 'ok' else batch['cancelled']
            # v2.5.5 口径：入库/媒体只含刮削入库；流程任务（手动/网盘整理）单独标注，
            # 按类分列计数；全部为 0 则整行省略
            def _fbit(name, d, f, c):
                s = f'{name}{d}'
                if f:
                    s += f'·失败{f}'
                if c:
                    s += f'·取消{c}'
                return s
            flow_bits = [_fbit(k, v[0], v[1], v[2])
                         for k, v in (('手动整理', batch['flow'].get('手动', (0, 0, 0))),
                                      ('网盘整理', batch['flow'].get('网盘', (0, 0, 0))))
                         if any(v)]
            _mbad = 0 if scope == 'ok' else batch['m_bad']
            _fail_bits = f'失败 {f_show}'
            if _mbad:
                _fail_bits += f' · 媒体失败 {_mbad}'
            if c_show:
                _fail_bits += f' · 取消 {c_show}'
            lines.append(f'本批入库：{batch["done"]} 任务 · {batch["m_ok"]} 媒体 · {_fail_bits}')
            if flow_bits:
                lines.append('流程任务：' + ' · '.join(flow_bits))
            if batch['started_at']:
                tmin = max(1, round((now - batch['started_at']).total_seconds() / 60))
                lines.append(f'本批耗时：{tmin} 分钟'
                             f'（{_fmt_hhmm(batch["started_at"])} → {_fmt_hhmm(now)}）')
            # v2.8.15a：队列两行，第二行共享对齐「当前队列：」之后（5 全角空格缩进）
            lines.append('当前队列：'
                         f'刮削 排{qq}/行{qr} · 网盘 排{nq2}/行{nr}')
            lines.append('　　　　　'
                         f'共享 排{sq}/行{sr} · 追剧 排{wq}/行{wr}')
            if batch['failed'] and scope != 'ok':
                lines.append(f"⚠ 本批 {batch['failed']} 个失败，可在面板任务统计页查看并重试")
                # v2.8.13：失败明细汇总进卡（≤5 条逐行「标题｜阶段｜原因」，超出折叠计数）
                for fl in (batch.get('fail_log') or [])[:5]:
                    _seg = fl['title']
                    if fl.get('stage'):
                        _seg += f"｜{fl['stage']}"
                    if fl.get('err'):
                        _seg += f"｜{fl['err']}"
                    lines.append(f"· {_seg}")
                _more = len(batch.get('fail_log') or []) - 5
                if _more > 0:
                    lines.append(f"· …等 {_more + 5} 条失败，详见面板任务统计页")
            # v2.8.18：非整理类失败（共享登记/追剧刷新等）单独一块，防「失败0」体感误差
            _of = batch.get('other_fail') or []
            if _of and scope != 'ok':
                lines.append(f'⚠ 其他类型失败 {len(_of)} 条：')
                for fl in _of[:3]:
                    _seg = fl['title']
                    if fl.get('stage'):
                        _seg += f"｜{fl['stage']}"
                    if fl.get('err'):
                        _seg += f"｜{fl['err']}"
                    lines.append(f'· {_seg}')
                if len(_of) > 3:
                    lines.append(f'· …共 {len(_of)} 条，详见面板任务统计页')
            btns = _card_buttons()
            if SETTINGS['trigger_enabled'] and not a_run and not a_que:
                # v2.6 卡片按钮入口：一次性令牌 30 分钟；每次清空轮换，旧令牌作废
                tok = secrets.token_urlsafe(24)
                _trigger_token['val'] = tok
                _trigger_token['exp'] = time.time() + 1800
                btns = _card_buttons(tok)
                lines.append('（30 分钟内有效，点击「整理下一批」确认后触发）')
            text = '\n'.join(lines)
            ok, err = feishu_push(text, buttons=btns,
                                  title='✅ 整理任务已清空', tcolor='green')
            record_push('clear', text, ok, err)
            _organize_state['last_clear_at'] = now
            _organize_state['batch'] = {'active': False, 'done': 0, 'failed': 0,
                                        'cancelled': 0, 'm_ok': 0, 'm_bad': 0,
                                        'started_at': None, 'last': None, 'last_ts': None,
                                        'flow': {}, 'other_fail': []}
            if ok:
                _feed_run()               # v2.7：清空提醒送达后自动喂料（事件驱动）
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
                    _alert_push('stall', '整理任务静止', [
                        f'任务 #{rid}「{c["title"][:40]}」已静止 {int(still_min)} 分钟',
                        f'进度 {c["ok"]}/{c["item"]}（剩 {remain}）· 最后推进 {_fmt_hhmm(last_iso)}',
                        qline,
                        '建议：查最后推进时间前后是否卡在 TMDB 外呼；必要时取消重派或重启容器'],
                        buttons=_card_buttons())
                    _auto_stall_handler(rid, c)   # v2.7 五：告警后自动处置（开关+护栏内）
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


_today_done = {'ts': 0.0, 'items': [], 'scanning': False, 'progress': ''}


def _today_done_loop() -> None:
    """v2.8.20b：今日终态明细独立重线程（边翻边更新）。
    背景：succeeded 全量已涨到百页级（页均 ~7s，整轮 >10 分钟），挂在 poll_once 里
    导致慢轮 17+ 分钟一轮、面板「统计截至」滞后 19 分（9/21 晚实测）。
    改为独立线程整轮重扫：逐状态翻页，每页即时并入发布缓存（边翻边更新——
    新任务在列表顶部先翻先见），整轮完成原子替换+盖时间戳。
    poll_once 只读缓存（0 翻页），慢轮周期回归 2 分钟级。"""
    while True:
        if _today_done['scanning']:
            time.sleep(5)
            continue
        _today_done['scanning'] = True
        try:
            today_prefix = _today_prefix()
            fresh = []
            for st in ('succeeded', 'failed', 'partial'):
                offset = 0
                while offset < 30000:
                    s, b = api_get(f'/api/workflows?status={st}&limit={PAGE}&offset={offset}')
                    items = b.get('items', []) if isinstance(b, dict) else []
                    if not items:
                        break
                    _n_new = 0
                    for it in items:
                        fin = it.get('finished_at') or it.get('created_at') or ''
                        if fin < today_prefix:
                            continue
                        fresh.append({'id': it.get('id'), 'status': st, 'kind': kind_of(it),
                                      'wf': it.get('workflow_type') or '',
                                      'title': it.get('display_title') or '',
                                      'media': it.get('item_count') or 0,
                                      'ok_media': it.get('succeeded_count') or 0,
                                      'bad_media': it.get('failed_count') or 0,
                                      'err': (it.get('failure_summary') or '')[:60],
                                      'stage': (it.get('failure_stage_title') or '')[:12],
                                      'finished_at': fin})
                        _n_new += 1
                    offset += PAGE
                    _today_done['progress'] = f'{st}:{offset//PAGE}页'   # 边翻边更新进度
                    # 边翻边发布：今日明细渐进可见（新页先翻先并入）
                    _today_done['items'] = fresh
                if offset >= 30000:
                    break
            _today_done['items'] = fresh
            _today_done['ts'] = time.time()
            _today_done['progress'] = f'完成 {len(fresh)} 条'
        except Exception:
            pass
        finally:
            _today_done['scanning'] = False
        time.sleep(60)   # 整轮后休 1 分钟再扫（新鲜度 ≤ 一轮耗时+1 分钟）


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
            # v2.8.17b：不再停页——订阅预处理批量完成时单页时间跨度可跨天，
            # page_oldest 停页会漏算深页今日任务（9/21 实锤今日任务数同步漏报）；
            # 代价=每轮多翻几十页（succeeded 全量 52 页≈350s），换来精确。
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
                            'err': (it.get('failure_summary') or '')[:60],
                            'stage': (it.get('failure_stage_title') or '')[:12],
                            'finished_at': fin})
            offset += PAGE
            if offset >= 30000:   # v2.7.1 安全闸：防接口异常时的无限翻页（正常今日远小于此）
                break
    return out


_dayweek_cache = {'ts': 0.0, 'date': '', 'day': None, 'week': None, 'rebuilding': False}
_DAYWEEK_INTERVAL = 900   # v2.8.17b：15 分钟一轮（全量翻页 ~350s/轮，无停页精确口径）


def _dayweek_daily_path() -> str:
    """v2.8.17：每日完成量滚动文件（data/daily.json），容器重启不丢。"""
    base = os.environ.get('SETTINGS_PATH') or '/app/data/settings.json'
    return os.path.join(os.path.dirname(base), 'daily.json')


def _snapshot_cache_path() -> str:
    """v2.8.20：最近一次完整快照落盘路径（data/last_snapshot.json），重启秒开旧数据。"""
    base = os.environ.get('SETTINGS_PATH') or '/app/data/settings.json'
    return os.path.join(os.path.dirname(base), 'last_snapshot.json')


def _snapshot_cache_save(snap: dict) -> None:
    try:
        tmp = _snapshot_cache_path() + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(snap, f, ensure_ascii=False)
        os.replace(tmp, _snapshot_cache_path())
    except Exception:
        pass


def _snapshot_cache_load() -> dict:
    try:
        with open(_snapshot_cache_path(), encoding='utf-8') as f:
            d = json.load(f)
        return d if isinstance(d, dict) and d.get('ts') else {}
    except Exception:
        return {}


def _dayweek_daily_load() -> dict:
    """读 {date: succ_media} 表（仅刮削入库口径，与今日完成同源）。"""
    try:
        with open(_dayweek_daily_path(), encoding='utf-8') as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _dayweek_daily_save(tbl: dict) -> None:
    try:
        tmp = _dayweek_daily_path() + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(tbl, f, ensure_ascii=False)
        os.replace(tmp, _dayweek_daily_path())
    except Exception:
        pass


def _dayweek_scan_day(day_prefix: str, stop_line: str, offset: int = 0,
                      end_prefix: str = None) -> tuple:
    """v2.8.17 核心扫描：带状态翻页累加 [day_prefix, end_prefix) 界内 succ（刮削入库口径）。
    安全停页=页内全部 created_at 最小值 < stop_line（界-1h 余量吸收页内批间乱序；
    带 status 翻页跨页单调已实证 9/21）。end_prefix=次日 0 点（回填用，今日扫描=None=无上界）。
    返回 (succ合计, 是否触底)。"""
    ok = 0
    while offset < 20000:
        s, b = api_get(f'/api/workflows?status=succeeded&limit={PAGE}&offset={offset}')
        items = b.get('items', []) if isinstance(b, dict) else []
        if not items:
            return ok, True
        cas = [(it.get('created_at') or it.get('finished_at') or '') for it in items]
        for it, ca in zip(items, cas):
            if not (it.get('display_title') or '').startswith(INGEST_PREFIXES):
                continue
            fin = it.get('finished_at') or ca or ''
            if fin >= day_prefix and (not end_prefix or fin < end_prefix):
                ok += it.get('succeeded_count') or 0
        offset += PAGE
        if stop_line and min(cas) < stop_line:   # v2.8.17b：stop_line=None=全量无停页
            return ok, False
    return ok, False


def _dayweek_rebuild() -> None:
    """v2.8.17 终版：今日/本周 = 「今日 fast 翻页（秒级）+ daily.json 7 日滚动」。
    历史方案缺陷（今日/本周反复清零根因）：
    15c 全量翻页随任务量线性变慢（51 页 342 秒，每天 +50 页）→ 超新鲜度闸永真清零；
    周窗口 fast 翻页在数据大头面前≈全量（翻穿全部较新任务 8 分钟+，同样恶化）；
    容器重启内存缓存清零+0 点翻转空窗。
    终版：每天 0 点把当日值归档进 daily.json（持久化，重启不丢）；
    今日值=扫到今日 0 点-1h 即停（通常第 1~2 页，5 秒）；本周=Σ(历史 6 天)+今日。
    首次部署回填：daily.json 缺历史日时按日回填（每日一次停页扫描，只跑一次）。"""
    if _dayweek_cache['rebuilding']:
        return
    _dayweek_cache['rebuilding'] = True
    try:
        today0 = _today_prefix()
        today_d = today0[:10]
        tbl = _dayweek_daily_load()
        # ①回填/归档：缺的日期一次翻页分桶补齐（翻到最早缺日-1h 停页，绝不逐日重翻）
        need = []
        for k in range(1, 7):
            dd = (datetime.strptime(today_d, '%Y-%m-%d') - timedelta(days=k)).strftime('%Y-%m-%d')
            if dd not in tbl:
                need.append(dd)
        if need:
            oldest = min(need)               # 最早缺日
            oldest0 = oldest + 'T00:00:00+08:00'
            stop = (datetime.fromisoformat(oldest0) - timedelta(hours=1)).isoformat()
            buckets = {dd: 0 for dd in need}
            offset = 0
            while offset < 20000:
                s, b = api_get(f'/api/workflows?status=succeeded&limit={PAGE}&offset={offset}')
                items = b.get('items', []) if isinstance(b, dict) else []
                if not items:
                    break
                cas = [(it.get('created_at') or it.get('finished_at') or '') for it in items]
                for it, ca in zip(items, cas):
                    if not (it.get('display_title') or '').startswith(INGEST_PREFIXES):
                        continue
                    fin = it.get('finished_at') or ca or ''
                    dd = fin[:10]
                    if dd in buckets:
                        buckets[dd] += it.get('succeeded_count') or 0
                offset += PAGE
                if min(cas) < stop:
                    break
            tbl.update(buckets)
            _dayweek_daily_save(tbl)
        # ②今日扫描：**全量翻页无停页**（v2.8.17b 根治：停页规则在订阅预处理等
        # 小任务批量完成时单页时间跨度可跨 1.5 天→提前停漏算→今日清零，9/21 11 点实锤；
        # 无 status 过滤参数可用（created_after 等均被无视，9/21 实测），只能全翻到空页）
        day_ok, _ = _dayweek_scan_day(today0, None)
        tbl[today_d] = day_ok
        _dayweek_daily_save(tbl)
        # ③本周=今日+昨日~6 天前（口径=含今日共 7 天）
        week_ok = day_ok
        for k in range(1, 7):
            dd = (datetime.strptime(today_d, '%Y-%m-%d') - timedelta(days=k)).strftime('%Y-%m-%d')
            week_ok += tbl.get(dd) or 0
        _dayweek_cache.update({'ts': time.time(), 'date': today_d,
                               'day': day_ok, 'week': week_ok})
    except Exception:
        pass
    finally:
        _dayweek_cache['rebuilding'] = False


def _dayweek_loop() -> None:
    _last_date = _dayweek_cache.get('date')
    while True:
        _dayweek_rebuild()
        # v2.8.17：0 点日期翻转后立刻补一轮（消除翻转空窗，从最长 5 分钟缩到秒级）
        if _dayweek_cache.get('date') != _last_date:
            _last_date = _dayweek_cache.get('date')
            continue
        time.sleep(_DAYWEEK_INTERVAL)


def collect_records_day(today_prefix: str):
    """今日媒体记录（p115/records）——全站统一媒体口径。
    v2.8.13 修复：不再信 API 的 total 字段——ETKN records 列表 total 与实收条数严重
    不符（实证 9/20：total=130 而实收 1299），改为一页 per_page=1000 实收 len() 计数
    （今日千条量级一页可收全；深页 page=2 实测 0 条，无需翻页）。"""
    out = {}
    for st, key in (('success', 'success'), ('unrecognized', 'unrecognized')):
        s, b = api_get(f'/api/p115/records?page=1&per_page=1000&status={st}'
                       f'&processed_from={urllib.parse.quote(today_prefix)}')
        items = (b or {}).get('items') or [] if s == 200 else []
        out[key] = len(items)
    s, b = api_get(f'/api/p115/records?page=1&per_page=1000&processed_from={urllib.parse.quote(today_prefix)}')
    items = (b or {}).get('items') or [] if s == 200 else []
    out['total'] = len(items)
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
            'stale': False, 'stale_note': '',
            'active': {'tasks': 0, 'media': 0, 'by_kind': {}}, 'done': {}, 'records': {},
            'eta': {}, 'failed_today': [],
            'backlog_threshold': int(SETTINGS['backlog_threshold'])}  # v2.8.7 横条与告警同源
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

    # v2.8.20b：今日终态明细改读独立重线程缓存（_today_done_loop 边翻边更新），
    # poll_once 不再同步全翻（原 10+ 分钟/轮是面板滞后 19 分与清空卡延迟的共同根因）；
    # 缓存从未就绪时退回同步扫（仅冷启动第一轮，此后 60s 节流由重线程接管）。
    if _today_done['items'] or _today_done['ts'] > 0:
        done = [d for d in _today_done['items'] if (d.get('finished_at') or '') >= today_prefix]
        done = list(done)   # 拷贝防并发遍历
    else:
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
    # v2.8.15c：今日/本周完成直读后台重建缓存（_dayweek_rebuild 全量翻页，15 分钟一轮）
    _dw_ok = (_dayweek_cache['date'] == _today_prefix()[:10]
              and _dayweek_cache['day'] is not None)
    _dw_fresh = time.time() - _dayweek_cache['ts'] < 3600   # v2.8.17：fast 5 分钟一轮，1h 闸=宁显旧不显零
    # v2.8.16 需求①：未识别口径=records unrecognized 的 total（累计，与 ETKN 界面同源；
    # per_page=1 轻量请求 0.1s；实测该状态下 total 可信：2263==12 页翻页实收）
    _s_ur, _b_ur = api_get('/api/p115/records?per_page=1&page=1&status=unrecognized')
    _ur_total = (_b_ur or {}).get('total') or 0 if _s_ur == 200 else None
    snap['records'] = {'success': _dayweek_cache['day'] if (_dw_ok and _dw_fresh) else None,
                       'unrecognized': _ur_total,
                       'total': _dayweek_cache['day'] if (_dw_ok and _dw_fresh) else None}
    snap['week'] = {'media': _dayweek_cache['week'] if _dw_fresh else None}
    snap['failed_today'] = [
        {'id': d['id'], 'kind': d['kind'], 'wf': d['wf'], 'title': d['title'][:40],
         'media': d['media'], 'bad_media': d['bad_media'],
         'err': d.get('err') or '', 'stage': d.get('stage') or '',
         'finished_at': d['finished_at'][:19]}
        for d in done if d['status'] in ('failed', 'partial')][:50]
    # v2.8.11：喂料转移失败（monitor 自身动作，ETKN 无任务记录）也进「今日失败」——
    # 从推送史派生今日 feed_err 合成行（id=0、wf='feed' 不可重试；重启后随推送史清空）
    for ph in _push_hist:
        if ph.get('kind') != 'feed_err':
            continue
        ts = ph.get('ts') or ''
        if ts[:10] != today_prefix[:10]:   # 同一天（ISO 前缀含 T00:00:00，不能直接 startswith）
            continue
        t = (ph.get('text') or '').replace('⚠️ ETKN 告警 · ', '')
        snap['failed_today'].append({'id': 0, 'kind': '喂料', 'wf': 'feed',
                                     'title': t[:40], 'media': 0, 'bad_media': 0,
                                     'finished_at': ts})
    snap['failed_today'] = snap['failed_today'][:50]
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
            _snapshot_cache_save(snap)   # v2.8.20：重启后先用旧快照秒开，后台全量更新
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
            out.setdefault('stale', False)
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
                'version': 'v2.8.20', 'readonly': False,
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
            # v2.8.16：累计未识别（与面板异常卡同源；ETKN 界面同口径）
            _s_ur, _b_ur = api_get('/api/p115/records?per_page=1&page=1&status=unrecognized')
            _ur_all = (_b_ur or {}).get('total') or 0 if _s_ur == 200 else 0
            return self._send(200, json.dumps({'date': today_prefix, 'total': total or len(items),
                                               'unrecognized_all': _ur_all,
                                               'items': items[:60]}, ensure_ascii=False).encode())
        if p == '/api/settings':
            return self._do_settings_get()
        if p == '/api/push-history':
            return self._send(200, json.dumps({'items': list(_push_hist)},
                                              ensure_ascii=False).encode())
        if p == '/api/hosts-status':
            # v2.8.19：只读回显当前绑定（grep 路由器 hosts，不解析不改）——GET 版；
            # 匹配口径与 _hosts_check_one 一致：含域名的任意行（不限标记行，兼容旧绑定）
            res = []
            if SETTINGS.get('hosts_enabled') and _HOSTS_DOMAINS():
                for d in _HOSTS_DOMAINS():
                    cur = _ssh_router(f"grep '{d}' /etc/hosts || true")
                    hip = ''
                    for ln in cur.splitlines():
                        if d in ln:
                            parts = ln.split()
                            if len(parts) >= 2:
                                hip = parts[0]
                                break
                    res.append({'domain': d, 'hosts_ip': hip})
            return self._send(200, json.dumps({'results': res}, ensure_ascii=False).encode())
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
        d.pop('cd2_pass', None)         # v2.8：CD2 密码永不回传前端（留空=不修改）
        d['router_pass_set'] = bool(d.get('router_pass'))  # v2.8.10：掩码态回传
        d.pop('router_pass', None)      # v2.8.10：路由器 SSH 密码同样不回传
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
                  'trigger_enabled', 'feed_enabled', 'auto_restart_enabled', 'hosts_enabled'):
            if k in b:
                SETTINGS[k] = bool(b[k])
        for k, lo in (('interval_500_min', 5), ('interval_speed_min', 0),
                      ('count_500_threshold', 1), ('backlog_threshold', 1),
                      ('speed_threshold_ms', 1000),
                      ('stall_threshold_min', 1), ('stall_repeat_min', 5),
                      ('feed_batch_limit', 1), ('feed_trigger_delay', 0)):
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
        for k in ('feed_src_dir', 'feed_dst_dir'):
            if k in b and isinstance(b[k], str) and b[k].strip():
                SETTINGS[k] = b[k].strip()
        if 'cd2_dav_url' in b and isinstance(b['cd2_dav_url'], str):   # v2.8 CD2 WebDAV
            u = b['cd2_dav_url'].strip().rstrip('/')
            if u and (u.startswith('http://') or u.startswith('https://')):
                SETTINGS['cd2_dav_url'] = u
        if 'cd2_user' in b and isinstance(b['cd2_user'], str):
            SETTINGS['cd2_user'] = b['cd2_user'].strip()
        if b.get('cd2_pass'):               # 密码：留空=不修改（前端 undefined 则整个键缺失）
            SETTINGS['cd2_pass'] = str(b['cd2_pass'])
        if 'card_links' in b:               # v2.7（六）：卡片按钮列表，空/非法剔除
            cl = _norm_card_links(b['card_links'])
            SETTINGS['card_links'] = cl if cl else [dict(x) for x in CARD_LINKS_DEFAULT]
        if 'speed_targets' in b:            # v2.8.7 测速目标：清洗落盘（空列表合法=清空全部）
            SETTINGS['speed_targets'] = _norm_speed_targets(b['speed_targets'])
        if 'hosts_domain' in b and isinstance(b['hosts_domain'], str):
            hd = b['hosts_domain'].strip().lower().rstrip('.')
            if hd and not re.fullmatch(r'[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)+', hd):
                hd = ''                     # 非法域名=拒收置空（巡检自动跳过）
            SETTINGS['hosts_domain'] = hd
        # v2.8.10 路由器连接 4 项（公版化）：写前清洗；密码原样存本地 settings.json
        if 'router_ip' in b and isinstance(b['router_ip'], str):
            SETTINGS['router_ip'] = b['router_ip'].strip()
        if 'router_port' in b:
            try:
                SETTINGS['router_port'] = max(1, min(65535, int(b['router_port'])))
            except (TypeError, ValueError):
                pass
        if 'router_user' in b and isinstance(b['router_user'], str):
            SETTINGS['router_user'] = b['router_user'].strip()
        if 'router_pass' in b and isinstance(b['router_pass'], str):
            SETTINGS['router_pass'] = b['router_pass']   # 空串=清空；掩码回传不外泄
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
                                    '收到本条说明 etkn-monitor → 飞书 推送链路可达',
                                    'v2.6：本条为交互卡片，按钮可在飞书内置浏览器打开'])
            ok, err = feishu_push(text, buttons=_card_buttons(),
                                  title='✅ 测试推送', tcolor='blue')
            record_push('test', text, ok, err)
            return self._send(200, json.dumps({'ok': ok, 'err': err},
                                              ensure_ascii=False).encode())
        if p == '/api/hosts-check':
            # v2.8.19：手动「重新检测 IP」——立即解析+比对+必要时改 hosts+重启 dnsmasq
            if not (SETTINGS.get('hosts_enabled') and _HOSTS_DOMAINS()):
                return self._send(200, json.dumps({'ok': False, 'err': '未启用 hosts 监控或未配置域名'},
                                                  ensure_ascii=False).encode())
            res = _hosts_recheck()
            return self._send(200, json.dumps({'ok': True, 'results': res},
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
            results = run_speed_round(alert=False)
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
            if s in (200, 201, 202) and isinstance(b, dict) and b.get('workflow_run_id'):
                try:
                    _watch_shell_run(int(b['workflow_run_id']), 'panel')  # v2.7.3②：空转监视
                except Exception:
                    pass
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


def _warmup_from_cache() -> None:
    """v2.8.20：重启后先用落盘缓存填充展示数据，用户不再干等「首轮采集中…」。
    ①快照预热：data/last_snapshot.json（上次运行最后一轮完整快照，含 done 明细/ETA）；
    ②今日/本周预热：daily.json 里今日键直接喂 _dayweek_cache（1h 新鲜度闸内即可显示；
    今日旧值误差=停机期间完成的量，后台首轮全量 ~6 分钟后自动纠正）。"""
    snap = _snapshot_cache_load()
    if snap:
        snap['stale'] = True
        snap['stale_note'] = '重启恢复中，显示为上次运行缓存'
        _state['snapshot'] = snap
        try:
            _state['hist'].append({'ts': snap['ts'], 'active_media': snap['active']['media'],
                                   'done_media': snap['records'].get('success', 0)})
        except Exception:
            pass
    tbl = _dayweek_daily_load()
    today_d = _today_prefix()[:10]
    if today_d in tbl and tbl[today_d] is not None:
        week = tbl.get(today_d) or 0
        for k in range(1, 7):
            dd = (datetime.strptime(today_d, '%Y-%m-%d') - timedelta(days=k)).strftime('%Y-%m-%d')
            week += tbl.get(dd) or 0
        _dayweek_cache.update({'ts': time.time(), 'date': today_d,
                               'day': tbl[today_d], 'week': week})
        # 预热值标注 stale_ts：poll_once 的 _dw_fresh 判定用 ts，这里给足新鲜度窗口
        print('预热完成：快照 ts=%s，今日=%s，本周=%s' % (snap.get('ts', '无'),
              _dayweek_cache['day'], _dayweek_cache['week']), flush=True)


def main():
    if not PASSWORD:
        raise SystemExit('缺少环境变量 ETKN_PASSWORD（只存环境，不落盘）')
    _warmup_from_cache()
    threading.Thread(target=poll_loop, daemon=True).start()
    threading.Thread(target=fast_loop, daemon=True).start()
    threading.Thread(target=alarm_loop, daemon=True).start()
    threading.Thread(target=_hosts_loop, daemon=True).start()   # v2.8 hosts 每小时巡检
    threading.Thread(target=_dayweek_loop, daemon=True).start()  # v2.8.15c 今日/本周后台重建
    threading.Thread(target=_today_done_loop, daemon=True).start()  # v2.8.20b 今日明细重线程
    port = int(os.environ.get('MONITOR_PORT', '8620'))
    srv = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    print(f'etkn-monitor v2.8.20，端口 {port}，快轮询 {FAST_INTERVAL}s（活跃队列）/'
          f'慢轮询 {POLL_INTERVAL}s（全量），ETA 窗口 {ETA_WINDOW_MIN}min，'
          f'喂料=CD2 WebDAV 通道，hosts 巡检=每小时', flush=True)
    srv.serve_forever()


if __name__ == '__main__':
    main()
