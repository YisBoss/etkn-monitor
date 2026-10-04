#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
etkn-monitor v3.12.0 —— ETKN 监控服务（轮询+测速+重试+手动整理+异常明细+双速快照
                        +设置页+飞书Webhook/企业微信应用 双通道告警中心）
配置全部走环境变量（零密钥，仓库内不含任何私有地址/域名）：
  ETKN_BASE_URL     ETKN 地址        默认 http://127.0.0.1:5257
  ETKN_USERNAME     登录用户名        默认空（部署者自填）
  ETKN_PASSWORD     登录密码          必填（部署者自填，不落盘）
  ETKN_PUBLIC_URL   ETKN 外网入口     默认同 ETKN_BASE_URL（卡片「打开ETKN」按钮）
  ETKN_SITE_URL     ETKN 站点根地址   默认空（面板「打开 ETKN 原站」按钮，空则提示未配置）
  MONITOR_LAN_HOST  面板内网地址      默认空（回落 127.0.0.1:<MONITOR_PORT>）
  ETKN_ETA_WINDOW   分钟              ETA 滚动窗口，默认 10
  POLL_INTERVAL     秒                慢速全量轮询间隔，默认 120
  FAST_INTERVAL     秒                快速活跃队列轮询间隔，默认 2
  MONITOR_PORT      监听端口          默认 8620
  CLOUD115_DIR      115 目录挂载点    默认 ./cloud115-not-configured（只读挂载用）
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
import hashlib
import json
import os
import re
import secrets
import shlex
import socket
import ssl
import struct
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import deque
from datetime import datetime, timedelta, timezone

BASE = os.environ.get('ETKN_BASE_URL', 'http://127.0.0.1:5257').rstrip('/')
LAN_HOST = os.environ.get('MONITOR_LAN_HOST', '')   # 内网面板地址（清空提醒链接用），空则回落 127.0.0.1:端口
ETKN_PUBLIC_URL = os.environ.get('ETKN_PUBLIC_URL', BASE).rstrip('/')   # 卡片「打开ETKN」按钮（ETKN 主程序外网入口）
ETKN_SITE_URL = os.environ.get('ETKN_SITE_URL', '').rstrip('/')         # 面板「打开 ETKN 原站」按钮（空=提示未配置）

CARD_LINKS_DEFAULT = []   # v2.7（六）：卡片按钮可配置，全新安装默认空，部署者在设置页自增
USERNAME = os.environ.get('ETKN_USERNAME', '')
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
    host 重复去重；最多 20 项（v2.9.2.9 从 12 提到 20——ETKN 真实依赖已不止 12 个，
    旧上限会让第 13 项起**静默消失**）。非法项剔除（不静默整表拒存）。"""
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
        # v2.9.2.8：放行哨兵 "etkn" = 按 ETKN 当前口径试算（给「要不要在 ETKN 里切换」
        # 做同口径对比用；代理地址仍从 ETKN 动态取样，不在 em 侧硬编码）；其余仍只收 host:port
        if px.lower() == 'etkn':
            px = 'etkn'
        elif px and not re.fullmatch(r'[A-Za-z0-9._-]+:\d{1,5}', px):
            px = ''
        out.append({'host': h, 'note': note, 'proxy': px or None})
        if len(out) >= 20:      # v2.9.2.9：原为 12，会静默丢弃第 13 项起的目标
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
    'push_enabled': False,        # 推送总开关（所有通道的总闸）
    'feishu_enabled': True,       # v2.9.7 飞书通道独立开关（与 wecom_enabled 对称；默认开=行为不变）
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
    'trigger_public_base': '',     # 外网基础地址（如 https://monitor.example.com），空=内网地址
    # ---- v2.7 自动喂料（清空后源目录→待整理目录自动分批转移） ----
    'feed_enabled': False,         # 自动喂料开关（默认关）
    'feed_src_dir': '/115/自动整理入库',                                   # 源目录（CD2 WebDAV 路径）
    'feed_dst_dir': '/115/媒体库-ETKN/待整理目录',                         # 目标目录（CD2 WebDAV 路径）
    'feed_batch_limit': 500,       # 每批文件夹上限（v2.8.6：一个剧夹=1 项，整夹转移）
    # ---- v2.8 喂料通道：CloudDrive2 WebDAV（不碰 fuse，凭据存设置不硬编码） ----
    'cd2_dav_url': '',             # 例：http://<CD2主机>:19798/dav
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
    # ---- v2.9.5 企业微信自建应用（通知渠道 + 回调 + 自定义菜单） ----
    'wecom_enabled': False,        # 企微渠道总开关（与飞书 webhook 各自独立，可同时开）
    'wecom_corpid': '',            # 企业ID（ww 开头）
    'wecom_agentid': '',           # 应用 AgentId
    'wecom_secret': '',            # 应用 Secret（不回传前端；留空=不修改）
    'wecom_token': '',             # 回调 Token（不回传前端；留空=不修改）
    'wecom_aeskey': '',            # 回调 EncodingAESKey（不回传前端；留空=不修改）
    'wecom_touser': '@all',        # 默认接收人（@all，或 userid 多个用 | 分隔）
    'wecom_api_proxy': '',         # 调企微 API 走的 HTTP 代理（对齐「企业可信IP」用，空=直连）
    # v2.9.8：原 wecom_panel_url（菜单「看面板」用）已移除——菜单不再有 view 项，
    #         回调 URL 由前端按当前访问地址生成，无需再配一个面板公网地址。
    # ---- v2.9.20 外部中转池自检/解限流（可选功能；默认关，开源仓库不含任何私有地址与凭据）----
    'relay_enabled': False,        # 中转池自检开关（默认关；关掉时菜单点了只提示去设置页填）
    'relay_base_url': '',          # 中转池地址，例 http://<主机>:8045（部署者自填）
    'relay_admin_pass': '',        # 中转池管理员密码（管 /api/*；只存本机，不回传前端）
    'relay_api_key': '',           # 中转池 /v1 调用密钥（探针用；与管理员密码是两套，实测不通用）
    'relay_model': '',             # 探针模型（留空=跳过探针，只报池状态）
    'relay_min_quota': 50,         # 配额门槛（%）：5 小时配额低于它视为真实耗尽，不自动清锁
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
            # v2.9.32：run_token 不在 SETTINGS_DEFAULTS 里（不想被设置页来回传），
            # 但它必须跨重启保留——否则每次重启换令牌，微信里旧消息的任务链接全部失效
            # （实际踩到：手机点任务报「失败 525/403」）。这里单独捞。
            if isinstance(d.get('run_token'), str) and d['run_token'].strip():
                SETTINGS['run_token'] = d['run_token'].strip()
    except Exception:
        pass
    for k in ('push_enabled', 'alert_500_enabled', 'alert_speed_enabled', 'alert_backlog_enabled',
              'alert_stall_enabled', 'stall_grace_enabled', 'alert_finish_enabled',
              'trigger_enabled', 'feed_enabled', 'auto_restart_enabled', 'hosts_enabled',
              'relay_enabled'):          # v2.9.20
        SETTINGS[k] = bool(SETTINGS[k])
    for k in ('interval_500_min', 'interval_speed_min', 'count_500_threshold',
              'backlog_threshold', 'speed_threshold_ms',
              'stall_threshold_min', 'stall_repeat_min', 'feed_batch_limit',
              'relay_min_quota'):        # v2.9.20
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
    文本首行同步（推送史/纯文本通道口径一致）。失败/限流仍黄三角，成功才绿对勾。
    v2.9.5：新增企业微信通道——wecom_enabled 打开时同时推一条应用消息。
    两通道互相独立：任一条送达即算成功，两边都挂才记失败（错误里带企微原因）。"""
    icon = {'green': '✅', 'blue': 'ℹ️'}.get(tcolor, '⚠️')
    text = _alert_text(kind_line, detail_lines, icon=icon)
    ok, err = push_both(text, buttons=buttons, title=f'{icon} ' + kind_line, tcolor=tcolor)
    record_push(kind, text, ok, err)
    return ok, err


# ============ v2.9.5 企业微信自建应用（应用消息 / 回调验签解密 / 自定义菜单） ============
# 为什么自带 AES：镜像是 python:3.12-alpine，没有 cryptography / pycryptodome，
# 而企微回调的 echostr 校验与消息解密必须用 AES-256-CBC。这里内嵌一份纯 Python 实现，
# 保持本仓库「单文件、零第三方依赖」的风格（已用 FIPS-197 C.3 与 NIST SP800-38A
# CBC-AES256 官方向量自检通过），不引入镜像构建依赖。

_AES_SBOX = bytes.fromhex(
    '637c777bf26b6fc53001672bfed7ab76ca82c97dfa5947f0add4a2af9ca472c0'
    'b7fd9326363ff7cc34a5e5f171d8311504c723c31896059a071280e2eb27b275'
    '09832c1a1b6e5aa0523bd6b329e32f8453d100ed20fcb15b6acbbe394a4c58cf'
    'd0efaafb434d338545f9027f503c9fa851a3408f929d38f5bcb6da2110fff3d2'
    'cd0c13ec5f974417c4a77e3d645d197360814fdc222a908846eeb814de5e0bdb'
    'e0323a0a4906245cc2d3ac629195e479e7c8376d8dd54ea96c56f4ea657aae08'
    'ba78252e1ca6b4c6e8dd741f4bbd8b8a703eb5664803f60e613557b986c11d9e'
    'e1f8981169d98e949b1e87e9ce5528df8ca1890dbfe6426841992d0fb054bb16')
_AES_ISBOX = [0] * 256
for _i, _v in enumerate(_AES_SBOX):
    _AES_ISBOX[_v] = _i
_AES_RCON = (0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1B, 0x36, 0x6C, 0xD8, 0xAB, 0x4D)


def _aes_mul(a, b):
    r = 0
    while b:
        if b & 1:
            r ^= a
        a = (a << 1) ^ 0x1B if a & 0x80 else (a << 1)
        a &= 0xFF
        b >>= 1
    return r


def _aes_expand(key: bytes):
    """AES-256 密钥扩展（Nk=8, Nr=14），返回 15 个轮密钥。"""
    nk, nr, w = 8, 14, [list(key[i * 4:i * 4 + 4]) for i in range(8)]
    for i in range(nk, 4 * (nr + 1)):
        t = list(w[i - 1])
        if i % nk == 0:
            t = [(_AES_SBOX[x] if j else (_AES_SBOX[t[1]] ^ _AES_RCON[i // nk - 1]))
                 for j, x in enumerate(t[1:] + t[:1])]
        elif i % nk == 4:
            t = [_AES_SBOX[x] for x in t]
        w.append([w[i - nk][j] ^ t[j] for j in range(4)])
    return [bytes(b for word in w[4 * r:4 * r + 4] for b in word) for r in range(nr + 1)]


def _aes_shift(s, inv=False):
    out = list(s)
    for r in range(1, 4):
        row = [s[r + 4 * c] for c in range(4)]
        row = (row[-r:] + row[:-r]) if inv else (row[r:] + row[:r])
        for c in range(4):
            out[r + 4 * c] = row[c]
    return out


def _aes_mix(s, inv=False):
    m = (((14, 11, 13, 9), (9, 14, 11, 13), (13, 9, 14, 11), (11, 13, 9, 14)) if inv
         else ((2, 3, 1, 1), (1, 2, 3, 1), (1, 1, 2, 3), (3, 1, 1, 2)))
    out = [0] * 16
    for c in range(4):
        col = [s[r + 4 * c] for r in range(4)]
        for r in range(4):
            out[r + 4 * c] = (_aes_mul(col[0], m[r][0]) ^ _aes_mul(col[1], m[r][1]) ^
                              _aes_mul(col[2], m[r][2]) ^ _aes_mul(col[3], m[r][3]))
    return out


def _aes_block(block, rks, enc):
    box, n = (_AES_SBOX, 14) if enc else (_AES_ISBOX, 14)
    s = [block[i] ^ rks[0][i] for i in range(16)] if enc else [block[i] ^ rks[14][i] for i in range(16)]
    rng = range(1, 14) if enc else range(13, 0, -1)
    for r in rng:
        if enc:
            s = [x ^ y for x, y in zip(_aes_mix(_aes_shift([box[v] for v in s])), rks[r])]
        else:
            s = _aes_shift([box[v] for v in s], inv=True)
            s = [x ^ y for x, y in zip(s, rks[r])]
            s = _aes_mix(s, inv=True)
    if enc:
        s = [x ^ y for x, y in zip(_aes_shift([box[v] for v in s]), rks[n])]
    else:
        s = _aes_shift([box[v] for v in s], inv=True)
        s = [x ^ y for x, y in zip(s, rks[0])]
    return bytes(s)


def _aes_cbc(key: bytes, iv: bytes, data: bytes, enc: bool):
    rks, out, prev = _aes_expand(key), bytearray(), iv
    for i in range(0, len(data), 16):
        blk = data[i:i + 16]
        if enc:
            prev = _aes_block(bytes(a ^ b for a, b in zip(blk, prev)), rks, True)
            out += prev
        else:
            out += bytes(a ^ b for a, b in zip(_aes_block(blk, rks, False), prev))
            prev = blk
    return bytes(out)


# ---------- 企微回调加解密（对标官方 WXBizMsgCrypt） ----------
def _wecom_pkcs7(data: bytes, bs: int = 32):
    if not data:
        return data
    pad = data[-1]
    return data[:-pad] if 1 <= pad <= bs and len(data) > pad else data


def _wecom_aeskey(k: str) -> bytes:
    return base64.b64decode((k or '').strip() + '=')


def wecom_signature(token: str, timestamp, nonce, encrypt: str) -> str:
    return hashlib.sha1(''.join(sorted([token, str(timestamp), str(nonce), encrypt]))
                        .encode('utf-8')).hexdigest()


def wecom_decrypt(aeskey: str, encrypt_b64: str, receiveid: str = '') -> str:
    key = _wecom_aeskey(aeskey)
    if len(key) != 32:
        raise ValueError('EncodingAESKey 长度不对（应为 43 字符）')
    plain = _wecom_pkcs7(_aes_cbc(key, key[:16], base64.b64decode(encrypt_b64), False))
    if len(plain) < 20:
        raise ValueError('解密结果过短')
    n = struct.unpack('>I', plain[16:20])[0]
    msg, rid = plain[20:20 + n], plain[20 + n:]
    if receiveid and rid.decode('utf-8', 'replace') != receiveid:
        raise ValueError('receiveid 不匹配（AESKey 或 企业ID 填错）')
    return msg.decode('utf-8')


def wecom_encrypt(aeskey: str, text: str, receiveid: str = '') -> str:
    key = _wecom_aeskey(aeskey)
    body = (os.urandom(16) + struct.pack('>I', len(text.encode('utf-8')))
            + text.encode('utf-8') + receiveid.encode('utf-8'))
    n = 32 - (len(body) % 32)
    body += bytes([n]) * n
    return base64.b64encode(_aes_cbc(key, key[:16], body, True)).decode()


# ---------- 企微 API 客户端 ----------
_WECOM_BASE = 'https://qyapi.weixin.qq.com'
_wecom_tok = {'v': '', 'exp': 0.0}
_wecom_lock = threading.Lock()
_wecom_last = {'ts': '', 'ok': False, 'err': ''}   # 最近一次发送结果（设置页回显）
_wecom_menu_last = {'ts': '', 'ok': False, 'msg': ''}   # v2.9.7 最近一次菜单下发结果


def _menu_record(ok, msg):
    """记录菜单下发结果并原样返回，供设置页回显「菜单到底更新没」。"""
    _wecom_menu_last.update({'ts': _now().isoformat(timespec='seconds'),
                             'ok': bool(ok), 'msg': (msg or '')[:200]})
    return ok, msg


def _wecom_cfg() -> dict:
    return {'corpid': (SETTINGS.get('wecom_corpid') or '').strip(),
            'agentid': str(SETTINGS.get('wecom_agentid') or '').strip(),
            'secret': (SETTINGS.get('wecom_secret') or '').strip(),
            'token': (SETTINGS.get('wecom_token') or '').strip(),
            'aeskey': (SETTINGS.get('wecom_aeskey') or '').strip(),
            'touser': (SETTINGS.get('wecom_touser') or '').strip() or '@all',
            'proxy': (SETTINGS.get('wecom_api_proxy') or '').strip()}


def _notify_state() -> dict:
    """通知渠道状态（v2.9.6：面板首页要能直接看到「告警发得出去吗」）。

    只做静态配置判断 + 回显最近一次企微发送结果，不发任何网络请求
    （这个函数挂在 /api/status 上，每 2 秒就会被调一次）。
    """
    c = _wecom_cfg()
    fx_cfg = bool((SETTINGS.get('webhook_url') or '').strip())
    fx_on = bool(SETTINGS.get('feishu_enabled', True))   # v2.9.7 飞书独立开关
    wc_on = bool(SETTINGS.get('wecom_enabled'))
    wc_cfg = bool(c['corpid'] and c['secret'] and c['agentid'])
    return {'enabled': bool(SETTINGS.get('push_enabled')),
            'feishu_on': fx_on,
            'feishu_cfg': fx_cfg,
            'feishu': fx_on and fx_cfg,         # 真正能送达飞书
            'wecom_on': wc_on,
            'wecom_cfg': wc_cfg,
            'wecom': wc_on and wc_cfg,          # 真正能送达企微
            'wecom_proxy': bool(c['proxy']),
            'menu_last': dict(_wecom_menu_last),
            'last': dict(_wecom_last)}


def _wecom_http(url: str, payload=None, timeout: int = 15):
    """企微 API 请求。wecom_api_proxy 非空时走 HTTP 代理——
    用途：把调用来源 IP 固定成「企业可信IP」里登记的那个（NAS 直连出网 IP 会漂）。"""
    px = (SETTINGS.get('wecom_api_proxy') or '').strip()
    op = (urllib.request.build_opener(urllib.request.ProxyHandler({'http': px, 'https': px}))
          if px else urllib.request.build_opener())
    if payload is None:
        req = urllib.request.Request(url, headers={'Accept': 'application/json'})
    else:
        req = urllib.request.Request(url, data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
                                     headers={'Content-Type': 'application/json'}, method='POST')
    with op.open(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8', 'replace') or '{}')


def wecom_token(force: bool = False):
    """access_token 缓存（企微 7200s 有效，提前 300s 刷新）。返回 (token, err)。"""
    c = _wecom_cfg()
    if not (c['corpid'] and c['secret']):
        return '', '未配置 企业ID / Secret'
    now = time.time()
    with _wecom_lock:
        if not force and _wecom_tok['v'] and now < _wecom_tok['exp']:
            return _wecom_tok['v'], ''
        try:
            d = _wecom_http('%s/cgi-bin/gettoken?%s' % (_WECOM_BASE, urllib.parse.urlencode(
                {'corpid': c['corpid'], 'corpsecret': c['secret']})))
        except Exception as e:
            return '', 'gettoken 请求失败：%s' % str(e)[:100]
        if d.get('errcode') != 0 or not d.get('access_token'):
            return '', 'gettoken 失败：%s %s' % (d.get('errcode'), d.get('errmsg'))
        _wecom_tok['v'] = d['access_token']
        _wecom_tok['exp'] = now + max(60, int(d.get('expires_in') or 7200) - 300)
        return _wecom_tok['v'], ''


# 企微应用消息的字节上限（官方口径，UTF-8，一个汉字 3 字节）：
_WECOM_TEXT_MAX = 2048       # text.content
_WECOM_TC_TITLE_MAX = 128    # textcard.title
_WECOM_TC_DESC_MAX = 512     # textcard.description
_WECOM_TC_BTNTXT = '打开面板'  # textcard.btntxt，官方限 4 个汉字


def _wecom_trim(s: str, limit: int) -> str:
    """按【字节】裁剪——企微按字节计数，中文一个字 3 字节；
    且不能把多字节字符截成半个（decode ignore 兜底）。"""
    b = s.encode('utf-8')
    if len(b) <= limit:
        return s
    return b[:limit].decode('utf-8', 'ignore')


_MD_LINK_RE = re.compile(r'\[([^\]\n]+)\]\((https?://[^)\s]+)\)')


def _wecom_plain(s: str) -> str:
    """把「给飞书写的 markdown 正文」转成企微能正常显示的纯文本。

    v2.9.10：企微的 text 与 textcard 都**不渲染 markdown**（而 markdown 类型微信端
    又不支持），不转的话用户会看到字面的 `**加粗**` 与 `[文字](链接)`——
    实际踩到过：菜单「立即测速」的结果消息显示成 `**⚡ 测速结果**`。
    """
    s = _MD_LINK_RE.sub(lambda m: '%s %s' % (m.group(1), m.group(2)), s)
    s = re.sub(r'\*\*(.+?)\*\*', r'\1', s, flags=re.S)
    s = re.sub(r'`([^`\n]+)`', r'\1', s)
    s = re.sub(r'<font[^>]*>(.*?)</font>', r'\1', s, flags=re.S)
    s = re.sub(r'^#{1,6}[ \t]*', '', s, flags=re.M)
    s = re.sub(r'^>[ \t]?', '', s, flags=re.M)
    return s


def _wecom_drop_dup_head(body: str, title: str) -> str:
    """标题已经单独显示了，正文首行若又是同一件事，去掉它（见 wecom_push 里的说明）。"""
    if not title or '\n' not in body:
        return body
    first, rest = body.split('\n', 1)
    key = re.sub(r'^[^\w]+', '', title).strip()      # 去掉标题开头的 emoji/符号
    return rest if ('ETKN' in first or (key and key in first)) else body


def wecom_push(text: str, title: str = '', touser: str = '', buttons: list = None):
    """企业微信应用消息。返回 (ok, err)。

    msgtype 选型（2026-09-27 真机对照实验定的）：

    | msgtype | 企业微信 App | 微信 App（微信插件） |
    |---|---|---|
    | `markdown` | 正常 | ❌「暂不支持此消息类型」 |
    | `text` | 正常 | 正常，但**不渲染 markdown**（`**粗体**`/`[名字](url)` 原样露出） |
    | `textcard` | 正常（标题 + 描述 + 一个可点 URL） | 正常（同上）；🔴 btntxt 按钮**不渲染**（9/27 实锤）——整条消息可点=开 URL，文案别再写「点下方按钮」 |
    | `news` | 正常（标题 + 描述 + 一行行可点名字） | ⚠️ **公众号图文版式**：大图占位 + 标题横幅，**description 不显示** |

    🔴 v2.9.13 踩过的坑（**别再来一遍**）：把告警改成 `news` 之后，`news` 在**部分客户端**
    会被渲染成「公众号图文」——顶上一个大图占位（没配 `picurl` 就是空白块）、标题压在灰底横幅上、
    **正文 description 整段不显示**。告警的关键信息（入库数/耗时/队列）就这样丢了。

    所以 v2.9.14 定版：
      ① **告警 → `textcard`**：标题 + 完整正文 + **一个**可点 URL（默认面板；若这条告警带
         「整理下一批」令牌，URL 直接指向确认页，按钮文案改「确认整理」）——正文两端都能读全。
      ② 正文超 512 字节 → `text` 兜底：2048 字节按字节裁，链接行与「推送时间」页脚永远保留。
      ③ **`news` 只给「任务中心」那种「每行自解释」的列表用**（见 wecom_push_news）——
         那里每行就是一个任务名，不需要正文，图文版式反而合适。
    """
    c = _wecom_cfg()
    if not c['agentid']:
        return False, '未配置 AgentId'
    tok, err = wecom_token()
    if not tok:
        return False, err

    aid = int(c['agentid']) if c['agentid'].isdigit() else c['agentid']
    touser = touser or c['touser']
    ts = '推送时间 %s' % _now().strftime('%Y-%m-%d %H:%M:%S')
    title = _wecom_plain(title)
    text = _wecom_plain(text)
    btns = [b for b in (buttons or []) if b.get('url')]
    # 链接行用纯文本（企微两端都不渲染 markdown，写 [文字](url) 会原样露出来）
    link_line = ' · '.join('%s %s' % (b.get('text', '打开'), b['url']) for b in btns)

    # ---- 正文：去掉与标题重复的首行 ----
    # 传了 title 的调用点，正文首行都是一句「标题的另一种写法」：
    #   _alert_push   title「⚠️ 500 检测」      首行「⚠️ ETKN 告警 · 500 检测」
    #   整理清空       title「✅ 整理任务已清空」  首行「✅ ETKN 整理任务已清空，可以整理下一批」
    #   测试推送       title「✅ 测试推送」      首行「⚠️ ETKN 告警 · 测试推送」
    #   企微自检       title「ℹ️ 企业微信通道自检」首行「ℹ️ ETKN 告警 · 企微通道自检」
    # 注意图标不一定一致（测试推送就是 ✅ 标题配 ⚠️ 正文），所以判定用「含 ETKN」或
    # 「含标题去掉开头符号后的文字」，两条命中任一即认为是重复首行。
    # v2.9.11 只判了 'ETKN 告警'，漏掉「✅ ETKN 整理任务已清空…」这种（实际踩到过）。
    body = _wecom_drop_dup_head(text, title)

    # ---- ① textcard：标题 + 完整正文 + 一个可点 URL ----
    # v2.9.14：从 news 改回 textcard。news 在部分客户端走「公众号图文」版式，正文整段丢失。
    # 这条告警若带「整理下一批」令牌，就把卡片 URL 直接指向确认页（一次点击即到操作页）。
    act = next((b for b in btns if b.get('text') == '整理下一批'), None)
    url = (act or {}).get('url') or _panel_base()
    btntxt = '确认整理' if act else _WECOM_TC_BTNTXT
    desc = ('%s\n%s' % (body, ts)) if body else ts
    if title and len(desc.encode('utf-8')) <= _WECOM_TC_DESC_MAX:
        payload = {'touser': touser, 'msgtype': 'textcard', 'agentid': aid,
                   'textcard': {'title': _wecom_trim(title, _WECOM_TC_TITLE_MAX),
                                'description': desc, 'url': url, 'btntxt': btntxt}, 'safe': 0}
    else:
        # ---- ② text 兜底（2048 字节；正文先裁，链接行与页脚必须活下来）----
        # v2.9.13 修：旧写法把「正文 + 链接行」当一整块裁，正文一长链接行就被截没了——
        # 而链接恰恰是这条消息唯一能操作的东西。现在先算尾巴占多少字节，再从正文里扣。
        plain = ('%s\n%s' % (title, body)) if title else text
        tail = ('\n\n%s' % link_line) if link_line else ''
        tail += '\n\n%s' % ts
        room = max(0, _WECOM_TEXT_MAX - len(tail.encode('utf-8')))
        payload = {'touser': touser, 'msgtype': 'text', 'agentid': aid,
                   'text': {'content': _wecom_trim(plain, room) + tail}, 'safe': 0}

    def _once(t):
        try:
            return _wecom_http('%s/cgi-bin/message/send?access_token=%s' % (_WECOM_BASE, t), payload)
        except Exception as e:
            return {'errcode': -1, 'errmsg': str(e)[:100]}
    d = _once(tok)
    if d.get('errcode') in (40014, 42001, 41001):      # token 失效 → 强刷一次
        tok, err = wecom_token(force=True)
        if tok:
            d = _once(tok)
    if d.get('errcode') == 0:
        iu = d.get('invaliduser') or ''
        ok, e = (True, '') if not iu else (False, '部分接收人无效：%s' % iu)
    else:
        ok, e = False, '%s %s' % (d.get('errcode'), d.get('errmsg'))
    _wecom_last.update({'ts': _now().isoformat(timespec='seconds'), 'ok': ok, 'err': e})
    return ok, e


def wecom_push_news(articles: list, touser: str = ''):
    """图文消息（news）——v2.9.10 新增，用于「任务中心」点分类展开子任务。

    为什么用 news：只有它能让**每条子任务各带一个可点 URL**（text/textcard 都只有一个
    URL，template_card 微信端收不到）。

    🔴 v2.9.13/14 实测的两个限制（**只适合「每行自解释」的列表，不要拿它发告警**）：
      · articles 最多 8 篇（企微官方），调用方自己保证；
      · **部分客户端把 news 渲染成「公众号图文」**：第一条变成大图占位（没配 picurl 就是
        空白块）+ 标题压在灰底横幅上，**description 完全不显示**，其余各条只显示 title。
        所以每条 title 必须自包含，绝不能把关键信息只放 description——
        也正因如此，告警改回了 textcard（见 wecom_push）。
    """
    c = _wecom_cfg()
    if not c['agentid']:
        return False, '未配置 AgentId'
    arts = [{'title': _wecom_plain(a.get('title', ''))[:120],
             'description': _wecom_plain(a.get('description', ''))[:500],
             'url': a['url']} for a in (articles or [])[:8] if a.get('url')]
    if not arts:
        return False, '没有可发送的图文条目'
    tok, err = wecom_token()
    if not tok:
        return False, err
    payload = {'touser': touser or c['touser'], 'msgtype': 'news',
               'agentid': int(c['agentid']) if c['agentid'].isdigit() else c['agentid'],
               'news': {'articles': arts}, 'safe': 0}

    def _once(t):
        try:
            return _wecom_http('%s/cgi-bin/message/send?access_token=%s' % (_WECOM_BASE, t), payload)
        except Exception as e:
            return {'errcode': -1, 'errmsg': str(e)[:100]}
    d = _once(tok)
    if d.get('errcode') in (40014, 42001, 41001):
        tok, err = wecom_token(force=True)
        if tok:
            d = _once(tok)
    if d.get('errcode') == 0:
        iu = d.get('invaliduser') or ''
        ok, e = (True, '') if not iu else (False, '部分接收人无效：%s' % iu)
    else:
        ok, e = False, '%s %s' % (d.get('errcode'), d.get('errmsg'))
    _wecom_last.update({'ts': _now().isoformat(timespec='seconds'), 'ok': ok, 'err': e})
    return ok, e


def push_both(text: str, buttons: list = None, title: str = '', tcolor: str = 'blue',
              wecom_text: str = None):
    """v2.9.5 双通道分发：飞书 webhook + 企业微信应用消息（各自按独立开关）。
    返回 (ok, err)——任一条送达即 ok；两条都失败时 err 里带上两边原因。

    v2.9.7：飞书补上独立开关 feishu_enabled（与 wecom_enabled 对称，默认开=行为不变）。
    开关层级：push_enabled 是总闸（各告警线程的闸），feishu_enabled / wecom_enabled
    是两条通道各自的开关，互不影响。

    v2.9.16：新增 wecom_text——两条通道的**按钮形态不同**，涉及「按钮叫什么」的文案必须分开写。
    例：飞书是 markdown 交互卡片，按钮叫「整理下一批」；企微是 textcard，那个唯一按钮叫「确认整理」。
    不传 = 两条用同一份正文（默认行为不变）。"""
    res = []
    if SETTINGS.get('feishu_enabled', True):
        if buttons:
            f_ok, f_err = feishu_push(text, buttons=buttons, title=title, tcolor=tcolor)
        else:
            f_ok, f_err = feishu_push(text)
        res.append(('飞书', f_ok, f_err))
    if SETTINGS.get('wecom_enabled'):
        w_ok, w_err = wecom_push(wecom_text if wecom_text is not None else text,
                                 title=title, buttons=buttons)
        res.append(('企微', w_ok, w_err))
    if not res:
        return False, '飞书与企微通道都已关闭'
    ok = any(r[1] for r in res)
    errs = ' / '.join('%s:%s' % (n, e) for n, o, e in res if not o and e)
    return ok, errs


# ---------- 菜单（3 个一级：查状态 / 运维操作 / 任务中心） ----------
# ⚠️ 企微限制：一级/二级菜单 name 均**不超过 16 个字节**（UTF-8；一个汉字 3 字节、
# 一个 emoji 4 字节）——注意是字节不是字符，超了直接 40058 拒收。
# 下面每个名字都按 ≤16 字节设计，改名字前先 `len(name.encode('utf-8'))` 数一遍。
# v2.9.8：去掉原第一项「📊看面板」（view）——它和告警消息底部的 markdown 链接重复，
#         且为它维护「面板公网地址」设置不值当；腾出的位置给「任务中心」。
# v2.9.10：「任务中心」二级改成 ETKN 工具箱的 5 个分类（正好用满二级上限 5 个）。
#         点分类 → 服务端推一条图文消息（news）展开该类的子任务（企微菜单只有两级，
#         「分类里再展开子任务」菜单本身做不到，只能靠消息展开）。
# v2.9.15 菜单重排（用户要求）：
#   ① 一级「📈查状态」→「🔗快捷入口」：二级 = **面板设置里配的卡片链接**（view 型，点了直接跳）。
#      链接是动态的，所以菜单不再固定 → 改设置后要重下发（见 _wecom_menu_sig）。
#   ② 一级「🔧运维操作」重做：只留「面板上真有的动作」+ 一个合并的体检，按常用度排。
#      原「🔍检测500」「🌐重检IP」两个各占一格、都只推一条报告，合并成「🩺一键体检」一条出全结论。
# ========== v2.9.20 外部中转池（可选）：池状态查询 + 假死自愈 ==========
# v2.9.21：性能重排 —— 面板走 EdgeOne CDN（**源站 15s 超时**），
#  v2.9.20 的自检要 ~20s（串行查 5 个号的配额 16.5s + 探针 3.6s）→ 被掐成 524、空 body，
#  前端 r.json() 抛裸 SyntaxError。修法：① 配额改从 /api/accounts 的 payload 里白拿
#  （零额外请求）② 池状态与探针并发 ③ 探针 timeout 90→25 ④ 前端非 JSON 也给人话。
# 背景（2026-09-29 实测定性）：某类中转池的「限流记录」不会自己过期 —— 账号 5 小时配额桶
# 明明还是满的（remaining_fraction=1.0），网关却持续返回 503 all_accounts_limited、还报要等
# 两小时。清掉限流记录后立刻恢复 200（日志原文 Optimistic reset: Cleared all 5 rate limit record(s)）。
# 本功能把「查状态 → 清锁 → 复测」做成企微菜单一键动作，并且**只在配额健康时才清锁**，
# 避免把真实额度耗尽也一起"解"掉、白挨上游 429。
# ⚠️ 开源口径：地址 / 管理员密码 / 调用密钥 / 模型 全部走设置项，代码里不留任何私有地址与凭据；
#    默认关，未配置时菜单点了只提示去设置页。
def _relay_cfg() -> dict:
    base = (SETTINGS.get('relay_base_url') or '').strip().rstrip('/')
    if base and not (base.startswith('http://') or base.startswith('https://')):
        base = 'http://' + base
    return {'base': base,
            'admin': SETTINGS.get('relay_admin_pass') or '',
            'key': SETTINGS.get('relay_api_key') or '',
            'model': (SETTINGS.get('relay_model') or '').strip()}


def relay_min_frac() -> float:
    """配额门槛（比例）。低于它视为真实耗尽，不自动清锁。"""
    try:
        v = float(SETTINGS.get('relay_min_quota'))
    except (TypeError, ValueError):
        v = 50.0            # 注意别写 `or 50`：门槛 0 是合法值（=永远清锁），会被 or 吃掉
    return max(0.0, min(100.0, v)) / 100.0


def relay_enabled() -> bool:
    c = _relay_cfg()
    return bool(SETTINGS.get('relay_enabled') and c['base'] and c['admin'])


def _relay_http(path: str, method: str = 'GET', token: str = '', timeout: int = 30):
    """调中转池管理接口。返回 (status, body)；status=0 表示连不上。"""
    c = _relay_cfg()
    req = urllib.request.Request(
        c['base'] + path, method=method,
        data=(b'' if method in ('POST', 'DELETE') else None),
        headers={'Authorization': 'Bearer ' + (token or c['admin'])})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode('utf-8', 'replace')
            try:
                return r.status, json.loads(raw)
            except Exception:
                return r.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode('utf-8', 'replace')
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw
    except Exception as e:
        return 0, str(e)[:120]


def _relay_min_quota_par(ids: list):
    """并发查各账号的「5 小时配额桶最低剩余比例」，返回最小者（查不到返回 None）。

    🔴 v2.9.21：**必须并发**。中转池的 `/api/accounts/<id>/quota` 单次要 ~3.3s（它会去上游刷），
    5 个号串行就是 16.5s —— 加上探针 3.6s 一共 ~20s，而面板走 CDN
    （面板域名走 EdgeOne，**源站 15s 超时**）会被掐成 524、前端拿到空 body
    报 `SyntaxError: Failed to execute 'json'`。并发后 ~3.5s。
    """
    from concurrent.futures import ThreadPoolExecutor

    def one(aid):
        s2, q2 = _relay_http('/api/accounts/%s/quota' % aid, timeout=20)
        if s2 != 200 or not isinstance(q2, dict):
            return None
        vals = []
        for g in (q2.get('quota_groups') or []):
            for bk in (g.get('buckets') or []):
                if bk.get('window') == '5h':
                    f = bk.get('remaining_fraction')
                    if isinstance(f, (int, float)):
                        vals.append(f)
        return min(vals) if vals else None

    mn = None
    if not ids:
        return None
    with ThreadPoolExecutor(max_workers=max(1, min(6, len(ids)))) as ex:
        for v in ex.map(one, ids):
            if v is not None:
                mn = v if mn is None else min(mn, v)
    return mn


def _relay_pool(with_quota: bool = True) -> dict:
    """池状态：账号总数 / 可用数 / 被标记的账号 / 5 小时配额桶最低剩余比例。

    🔴 v2.9.21：配额**直接从 `/api/accounts` 的 `accounts[].quota.quota_groups[].buckets[]`
    里读，零额外请求**。原来逐号打 `/api/accounts/<id>/quota`，单次要 ~3.3s
    （那个端点会去上游刷），5 个号即使并发也要 ~3.6s，串行更是 16.5s。
    实测两个来源的 `remaining_fraction` 一致（差异只是刷新间隔造成的浮点漂移）。
    只有整个 payload 都读不到 5h 桶时（AM 改版）才走 `_relay_min_quota_par` 兜底。
    """
    out = {'total': 0, 'active': None, 'marked': [], 'min_5h': None, 'err': '', 'ids': []}
    s, b = _relay_http('/api/proxy/status')
    if s == 200 and isinstance(b, dict):
        out['active'] = b.get('active_accounts')
    s, b = _relay_http('/api/accounts')
    if s != 200 or not isinstance(b, dict):
        out['err'] = '读账号列表失败（HTTP %s）' % s
        return out
    accs = b.get('accounts') or []
    out['total'] = len(accs)
    for a in accs:
        q = a.get('quota') or {}
        marks = []
        if q.get('is_forbidden'):
            marks.append('被拒')
        if a.get('validation_blocked'):
            marks.append('待验证')
        if a.get('proxy_disabled'):
            marks.append('已禁用')
        if marks:
            out['marked'].append('%s（%s）' % (a.get('email') or '?', '/'.join(marks)))
        for g in (q.get('quota_groups') or []):
            for bk in (g.get('buckets') or []):
                if bk.get('window') == '5h':
                    f = bk.get('remaining_fraction')
                    if isinstance(f, (int, float)):
                        out['min_5h'] = f if out['min_5h'] is None else min(out['min_5h'], f)
        aid = a.get('id')
        if aid:
            out['ids'].append(aid)
    if with_quota and out['min_5h'] is None and out['ids']:
        out['min_5h'] = _relay_min_quota_par(out['ids'])   # 兜底：payload 里没有 5h 桶时
    return out


def _relay_probe(model: str) -> str:
    """打一发真实请求探针。返回 'OK' / 'LIMITED' / 'HTTP<code>' / 'ERR'。

    🔴 v2.9.21：timeout 90 → 25。上游真挂住时，等 90s 毫无意义 ——
    面板走 CDN 15s 就被掐，企微那侧也要尽早给结论。25s 足够覆盖正常的一次 8-token 探针
    （实测 ~3.6s）。
    """
    c = _relay_cfg()
    if not c['key']:
        return 'ERR'
    body = json.dumps({'model': model, 'max_tokens': 8,
                       'messages': [{'role': 'user', 'content': 'ping'}]}).encode()
    req = urllib.request.Request(
        c['base'] + '/v1/chat/completions', data=body, method='POST',
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + c['key']})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return 'OK' if r.status == 200 else 'HTTP%d' % r.status
    except urllib.error.HTTPError as e:
        txt = e.read().decode('utf-8', 'replace')
        if e.status == 503 and 'all_accounts_limited' in txt:
            return 'LIMITED'
        return 'HTTP%d' % e.status
    except Exception:
        return 'ERR'


def relay_report(heal: bool = True) -> list:
    """中转池自检报告（菜单「🔄解限流」与面板按钮共用）。返回报告行列表。

    🔴 v2.9.21 性能重排：池状态 + 配额（最慢的一步，5 个号并发）与探针**同时并发**跑，
    所以总耗时 ≈ max(配额 3.5s, 探针 3.6s) ≈ 3.7s，而不是相加。
    （v2.9.20 是「串行查 5 个号配额 16.5s + 探针 3.6s ≈ 20s」，面板走 CDN
    —— 面板域名走 EdgeOne、**源站 15s 超时** —— 会被掐成 524、前端拿到空 body。
    用户 09-29 截图报的 `SyntaxError: Failed to execute 'json'` 就是这个。）
    """
    from concurrent.futures import ThreadPoolExecutor

    lines = ['🔄 中转池自检']
    if not relay_enabled():
        return ['⚠️ 中转池自检未启用',
                '· 到面板「设置 → 中转池」填：启用开关、地址、管理员密码',
                '· 探针还需要 /v1 调用密钥（与管理员密码是两套，实测不通用）']
    c = _relay_cfg()
    can_probe = bool(c['model'] and c['key'])
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_pool = ex.submit(_relay_pool, True)           # 池状态 + 配额（内部 5 个号并发）
        f_probe = ex.submit(_relay_probe, c['model']) if can_probe else None
        p = f_pool.result()
        r1 = f_probe.result() if f_probe else None
    if p['err']:
        return ['⚠️ 中转池连不上', '· %s' % p['err'], '· 检查设置页里的地址与管理密码']
    act = p['active']
    lines.append('· 账号：%s%s'
                 % ('%s/%s 可用' % (act, p['total']) if act is not None else '共 %s 个' % p['total'],
                    '，标记异常 %d 个' % len(p['marked']) if p['marked'] else '，标记全干净'))
    for m in p['marked'][:3]:
        lines.append('　　%s' % m)
    if p['min_5h'] is not None:
        lines.append('· 5 小时配额：最低 %.0f%%' % (p['min_5h'] * 100))
    if not can_probe:
        if not c['model']:
            lines.append('· 探针：未配置模型，已跳过（只报池状态）')
        else:
            lines.append('· 探针：未配置 /v1 调用密钥，已跳过')
        return lines
    if r1 == 'OK':
        lines.append('· 探针 %s：200 ✅ 无需处理' % c['model'])
        return lines
    if r1 != 'LIMITED':
        lines.append('· 探针 %s：%s（非限流类错误，未动限流记录）' % (c['model'], r1))
        return lines
    frac = p['min_5h']
    if frac is None:
        lines.append('· 探针 %s：报「全部限流」，但读不到配额，未自动清锁' % c['model'])
        return lines
    if frac < relay_min_frac():
        lines.append('· 探针 %s：报「全部限流」，配额仅剩 %.0f%%（低于门槛 %.0f%%）'
                     % (c['model'], frac * 100, relay_min_frac() * 100))
        lines.append('　→ 属真实额度耗尽，未清锁，等官方重置')
        return lines
    if not heal:
        lines.append('· 探针 %s：报「全部限流」，但配额还有 %.0f%% → 疑似假死'
                     % (c['model'], frac * 100))
        lines.append('　→ 可点「🔄解限流」清锁（本次未执行）')
        return lines
    s, b = _relay_http('/api/proxy/rate-limits', method='DELETE')
    if s not in (200, 204):
        lines.append('· 清锁失败：HTTP %s %s' % (s, str(b)[:80]))
        return lines
    r2 = _relay_probe(c['model'])
    if r2 == 'OK':
        lines.append('· 探针 %s：报「全部限流」但配额还有 %.0f%% → 判定假死'
                     % (c['model'], frac * 100))
        lines.append('· 已清除限流记录 → 复测 200 ✅ 已恢复')
    else:
        lines.append('· 探针 %s：报「全部限流」，已清锁但复测仍 %s' % (c['model'], r2))
        lines.append('　→ 可能是上游真限流，稍后会自动重试')
    return lines


# ---- v2.9.21 异步自检任务：面板走 CDN（源站 15s 超时），同步等一个 ~3s 的请求会偶发 524 ----
# 源站实测稳定 2.5~3.2s，但经隧道/CDN 后抖动可达 13.6s。所以 POST 只负责「开跑」并立刻返回，
# 结果由前端轮询 GET 取。企微菜单那条路径不走 CDN，仍直接调 relay_report() 同步出结果。
_relay_job = {'running': False, 'lines': None, 'ok': None, 'ts': 0.0, 'started': 0.0}
_relay_job_lock = threading.Lock()


def relay_job_start(heal: bool = True) -> bool:
    """开一个后台自检任务。已有任务在跑就返回 False（去重，避免连点打爆上游）。"""
    with _relay_job_lock:
        if _relay_job['running']:
            return False
        _relay_job['running'] = True
        _relay_job['lines'] = None
        _relay_job['ok'] = None
        _relay_job['started'] = time.time()

    def work():
        lines = None
        try:
            lines = relay_report(heal=heal)
        except Exception as e:                      # noqa: BLE001 —— 后台线程必须兜住
            lines = ['⚠️ 自检异常', '· %s' % str(e)[:160]]
        with _relay_job_lock:
            _relay_job['lines'] = lines
            _relay_job['ok'] = not lines[0].startswith('⚠️')
            _relay_job['running'] = False
            _relay_job['ts'] = time.time()

    threading.Thread(target=work, daemon=True).start()
    return True


def relay_job_state() -> dict:
    """当前自检任务状态（前端轮询用）。"""
    with _relay_job_lock:
        d = {'running': _relay_job['running'], 'ok': _relay_job['ok'],
             'lines': _relay_job['lines']}
        if _relay_job['started']:
            d['elapsed'] = round((time.time() - _relay_job['started']) if _relay_job['running']
                                 else (_relay_job['ts'] - _relay_job['started']), 1)
        return d


_WECOM_LINK_MENU_NAME = '🔗快捷入口'      # 4+12 = 16B
_WECOM_LINK_MENU_MAX = 5                 # 企微二级菜单上限 5（面板卡片链接最多可配 6）
_WECOM_TC_MENU_NAME = '📚任务中心'        # v2.9.30：菜单尾「任务中心」项名（子按钮按动态分类重填）
_WECOM_TASK_LIST_PER_MSG = 8             # v2.9.31：微信里「点分类发子任务」每条消息最多几项
                                         #（企微 news 单条硬上限 8 篇；超出就分成多条消息续发）
_WECOM_CAT_MENU_MAX = 5                  # v2.9.30：任务中心二级按钮上限（企微二级最多 5 个）
_WECOM_MENU_TAIL = [
    {'name': '🔧运维操作', 'sub_button': [                       # 16B
        {'type': 'click', 'name': '📁整理一批', 'key': 'ORGANIZE'},   # 16B
        {'type': 'click', 'name': '⚡立即测速', 'key': 'SPEED'},      # 15B
        {'type': 'click', 'name': '🗂清空登记', 'key': 'PURGE'},      # 16B
        {'type': 'click', 'name': '🩺一键体检', 'key': 'HEALTH'},     # 16B
        # v2.9.20：二级上限 5 个已满，用「🔄解限流」替掉「🧹清理临时」
        #（原项改成文字关键词「清理临时」触发，功能不丢，见 _wecom_handle_msg）。
        {'type': 'click', 'name': '🔄解限流', 'key': 'RELAY'},        # 13B
    ]},
    {'name': '📚任务中心', 'sub_button': [                       # 16B
        {'type': 'click', 'name': '🎬媒体维护', 'key': 'CAT_MEDIA'},     # 16B
        {'type': 'click', 'name': '📁115整理', 'key': 'CAT_ORGANIZE'},  # 13B
        {'type': 'click', 'name': '📺订阅追剧', 'key': 'CAT_SUBSCRIPTION'},  # 16B
        {'type': 'click', 'name': '📚媒体库', 'key': 'CAT_LIBRARY'},     # 13B
        {'type': 'click', 'name': '🔑账号系统', 'key': 'CAT_ACCOUNT'},   # 16B
    ]},
]
# 本版菜单的「指纹」：卡片链接变了就重下发（v2.9.15 恢复 v2.9.7 的自动重下发，只针对链接）
_wecom_menu_sig = {'v': None}


def _menu_links_sig() -> str:
    """快捷入口当前内容的指纹（没配链接时是空串）。"""
    links = (_norm_card_links(SETTINGS.get('card_links')) or CARD_LINKS_DEFAULT)
    links = links[:_WECOM_LINK_MENU_MAX]
    return '|'.join('%s>%s' % (x['text'], x['url']) for x in links)
    # 说明（v2.9.30）：任务中心二级按钮已改为「按动态分类生成」，分类一变菜单就得重下发。
    # 但指纹只在「下发前」更新（见 wecom_menu_apply），本轮启动若不为空即触发一次重下发，
    # 之后各轮指纹一致不再打扰企微——不需要额外的定时对比。


def _wecom_build_menu() -> dict:
    """按当前设置生成菜单。快捷入口为空时**整项不出现**（一级只剩 2 个，企微允许）。"""
    links = (_norm_card_links(SETTINGS.get('card_links')) or CARD_LINKS_DEFAULT)
    links = links[:_WECOM_LINK_MENU_MAX]
    btns = []
    if links:
        btns.append({'name': _WECOM_LINK_MENU_NAME,
                     'sub_button': [{'type': 'view',
                                     'name': _wecom_trim(x['text'], _WECOM_NAME_MAX),
                                     'url': x['url']} for x in links]})
    btns += json.loads(json.dumps(_WECOM_MENU_TAIL))
    return {'button': btns}


_WECOM_NAME_MAX = 16     # 企微 menu name 字节上限（不是字符数）


def _wecom_menu_check(menu):
    """按企微口径先本地校验菜单（字节长度/数量），返回错误串，合法返回 ''。"""
    def _walk(btns, top):
        if len(btns) > (3 if top else 5):
            return '一级菜单最多 3 个、二级最多 5 个'
        for b in btns:
            n = b.get('name') or ''
            if len(n.encode('utf-8')) > _WECOM_NAME_MAX:
                return ('菜单名「%s」为 %d 字节，超过企微 %d 字节上限'
                        % (n, len(n.encode('utf-8')), _WECOM_NAME_MAX))
            subs = b.get('sub_button')
            if subs:
                e = _walk(subs, False)
                if e:
                    return e
        return ''
    return _walk(menu.get('button') or [], True)


def wecom_menu_apply():
    """把 _WECOM_MENU 推到企微（自定义菜单 create 是覆盖式）。返回 (ok, msg)。

    v2.9.7：结果记进 _wecom_menu_last，设置页可回显「菜单最后一次下发成功没」。
    v2.9.8：菜单不再含动态 URL（去掉 view 型「看面板」），下发内容与设置无关，
            因此也不需要「改设置自动重下发」那套逻辑了。"""
    c = _wecom_cfg()
    if not c['agentid']:
        return _menu_record(False, '未配置 AgentId')
    tok, err = wecom_token()
    if not tok:
        return _menu_record(False, err)
    _menu_update_task_buttons()          # v2.9.30：二级「任务中心」按当前动态分类重填
    menu = _wecom_build_menu()
    _e = _wecom_menu_check(menu)
    if _e:
        return _menu_record(False, _e)
    try:
        d = _wecom_http('%s/cgi-bin/menu/create?access_token=%s&agentid=%s'
                        % (_WECOM_BASE, tok, c['agentid']), menu)
    except Exception as e:
        return _menu_record(False, '菜单请求失败：%s' % str(e)[:100])
    if d.get('errcode') == 0:
        _wecom_menu_sig['v'] = _menu_links_sig()      # v2.9.15：记下已下发的链接指纹
        return _menu_record(True, '菜单已下发（企微端需重新进入应用生效）')
    return _menu_record(False, '%s %s' % (d.get('errcode'), d.get('errmsg')))


# ---------- v2.9.10 ETKN 原生任务（面板「任务中心」+ 企微菜单「任务中心」同源） ----------
# 白名单而非透传：/api/run-task/<key> 与 /run-task/<key> 都会原样转发到 ETKN，
# 不做白名单等于给面板开了个「任意任务代理」，别人拿到面板地址就能触发删除类任务。
#
# v2.9.10：按 ETKN 任务中心「工具箱」的 5 个分类组织（categories 接口返回 4 个 label，
# 但 items 里实际有第 5 个 account，以 items 为准）。每类只收**业务主链路 + 非破坏性**任务：
#   · 排除破坏性：delete-115-shares（删除分享入库）、execute-duplicate-media（执行去重）、
#     apply-auto-tags（自动打标）、manually-correct-organize-records（手动重组记录）等；
#   · 排除未实现：ETKN 里 implementation_status=planned 的一律不收。
# 每类条数 ≤8：企微图文消息（news）单条最多 8 篇，多了塞不下。
TASK_CATALOG = [
    {'key': 'media', 'label': '媒体维护', 'icon': '🎬', 'tasks': [
        ('backfill-media-metadata', '补齐媒体元数据', '把缺元数据的媒体补齐'),
        ('fill-video-screenshots', '补齐视频截图', '为缺截图的媒体补图'),
        ('refresh-tmdb-ratings', '刷新 TMDb 评分', '重新拉取 TMDb 评分并写回'),
        ('sync-douban-ratings', '同步豆瓣评分', '同步豆瓣评分到媒体信息'),
        ('enrich-actor-data', '补充演员数据', '补齐演员信息'),
        ('rebuild-search-index', '重建索引', '重建媒体搜索索引'),
        ('scan-media-library', '扫描媒体目录', '扫描媒体目录变更'),
    ]},
    {'key': 'organize', 'label': '115 与整理', 'icon': '📁', 'tasks': [
        ('organize-p115', '手动整理网盘文件', '触发一批网盘整理'),
        ('import-115-share', '分享入库', '把 115 分享入库'),
        ('check-115-shares', '检查分享链接', '检查分享链接是否有效'),
        ('sync-115-directory-tree', '同步网盘目录', '同步 115 目录树'),
        ('rebuild-strm', '全量生成 STRM', '全量重建 STRM（耗时较长）'),
        ('repair-organize-records', '补齐整理记录', '补齐缺失的整理记录'),
        ('cleanup-p115-temp-directory', '清理播放临时目录', '清理 3 小时前的临时视频'),
    ]},
    {'key': 'subscription', 'label': '订阅', 'icon': '📺', 'tasks': [
        ('refresh-watchlist', '刷新智能追剧', '刷新智能追剧订阅'),
        ('refresh-completed-series', '刷新完结剧集', '刷新完结剧集'),
        ('refresh-actor-subscriptions', '刷新演员订阅', '刷新演员订阅'),
        ('process-subscriptions', '统一订阅处理', '统一处理订阅'),
        ('subscription-assistant-maintenance', '订阅助手巡检', '订阅助手巡检'),
    ]},
    {'key': 'library', 'label': '媒体库与封面', 'icon': '📚', 'tasks': [
        ('refresh-virtual-libraries', '刷新媒体库', '刷新全部虚拟媒体库'),
        ('generate-virtual-library-covers', '生成媒体库封面', '按封面配置批量更新'),
        ('refresh-native-collections', '刷新合集', '刷新原生合集'),
    ]},
    {'key': 'account', 'label': '账号与系统', 'icon': '🔑', 'tasks': [
        ('re0-checkin', 're0 自动签到', '触发 re0 签到'),
        ('notify-library-success', '发送入库通知', '补发入库成功通知'),
        ('register-shared-source', '登记共享资源', '登记共享资源'),
        ('shared-resource-maintenance', '共享资源维护', '共享资源维护'),
    ]},
]
# 由分类目录派生平铺白名单（/api/run-task 校验用；改分类只改上面一处）
ETKN_TASK_WHITELIST = {t[0]: t[1] for c in TASK_CATALOG for t in c['tasks']}
# 企微菜单 click key → 分类（点分类 = 推一条图文消息展开该类的子任务）
_WECOM_CAT_KEYS = {'CAT_%s' % c['key'].upper(): c for c in TASK_CATALOG}


# ---------- v2.9.30 任务目录「EM 启动时跟随 ETKN」（明确不做定时跟随） ----------
# 上面的 TASK_CATALOG 是 v2.9.10 人工挑的白名单，ETKN 作者改任务后 em 不会跟。v2.9.30：
# EM **每次启动时**拉一次 ETKN /api/task-center/catalog，自动重建目录；面板「任务中心」、
# 企微菜单「任务中心」、/api/task-catalog 三处都读同一个 TASK_CATALOG，因此一次刷新三处同步。
# 不做定时跟随（按用户要求）；拉不到 ETKN 用上次落盘缓存，再不行才用上面的静态兜底。
# 白名单安全策略**保留**：只收「就绪(ready) + 非破坏性 + 免参数可一键触发」的任务。
TASK_CATALOG_SOURCE = {'from': 'static', 'ts': '', 'reason': '尚未刷新'}
# 破坏性任务：删数据/改配置，绝不放进面板与企微菜单（点一下就执行，不可回退）
_DANGEROUS_TASK_KEYS = {'delete-115-shares', 'execute-duplicate-media', 'apply-auto-tags',
                        'manually-correct-organize-records', 'scan-duplicate-media',
                        'acquire-cloud-resource'}
# 需先选目标/带参数的任务：面板只会发空参数 {"parameters":{}} → 收进来是死按钮
_NEEDS_PARAM_TASK_KEYS = {'refresh-one-virtual-library', 'maintain-virtual-library-path',
                          'process-local-media-paths', 'process-virtual-media-paths',
                          'create-logical-season-share'}
_TASK_ICON_BY_CAT = {'media': '🎬', 'organize': '📁', 'subscription': '📺',
                     'library': '📚', 'account': '🔑'}
# ETKN 的 categories 接口只声明 4 类，account 类没给 label，这里兜底中文名
_TASK_LABEL_FALLBACK = {'account': '账号与系统'}
# 企微菜单二级按钮只有 16 字节，动态分类名太长会被截成「115 与整」这种；已知分类给短名，
# 未知分类（ETKN 将来新增）退回「图标+label 截断」，仍然跟着变。
_CAT_MENU_SHORT = {'media': '🎬媒体维护', 'organize': '📁115整理', 'subscription': '📺订阅追剧',
                   'library': '📚媒体库', 'account': '🔑账号系统'}
# 我方静态短描述（更贴面板口径）；ETKN 没有的 key 用它的 description 压成一行
_TASK_DESC_STATIC = {t[0]: t[2] for c in TASK_CATALOG for t in c['tasks']}
# 每类只放 8 项（企微图文单条上限），但**原先 v2.9.10 人工挑的那批必须优先保住**——
# 否则自动跟随会把「清理临时目录」这类用户已在用的任务挤出去。这就是那份优先级。
_STATIC_PRIORITY = {t[0] for c in TASK_CATALOG for t in c['tasks']}


def _one_line(s: str, limit: int = 46) -> str:
    s = re.sub(r'\s+', ' ', (s or '').strip())
    return s if len(s) <= limit else s[:limit - 1] + '…'


def _task_catalog_cache_path() -> str:
    base = os.environ.get('SETTINGS_PATH') or '/app/data/settings.json'
    return os.path.join(os.path.dirname(base), 'task_catalog_cache.json')


def _task_catalog_cache_write(cat: list) -> None:
    try:
        p = _task_catalog_cache_path()
        tmp = p + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({'ts': _now().isoformat(timespec='seconds'), 'categories': cat},
                      f, ensure_ascii=False, indent=1)
        os.replace(tmp, p)
    except Exception as e:
        print('任务目录缓存落盘失败：%s' % str(e)[:120], flush=True)


def _task_catalog_cache_load() -> list:
    try:
        with open(_task_catalog_cache_path(), encoding='utf-8') as f:
            d = json.load(f)
        cats = d.get('categories') if isinstance(d, dict) else None
        if isinstance(cats, list) and cats:
            TASK_CATALOG_SOURCE.update({'from': 'cache', 'ts': d.get('ts') or '',
                                        'reason': 'ETKN 不可达，沿用上次缓存'})
            return cats
    except Exception:
        pass
    return []


def _task_catalog_from_etkn() -> list:
    """拉 ETKN 目录，按安全策略转成 em 的 TASK_CATALOG 结构；失败返回 []。"""
    if not _cookie['value']:
        with _sess_lock:
            if not _cookie['value']:
                login()
    s, d = api_get('/api/task-center/catalog')
    if s != 200 or not isinstance(d, dict):
        return []
    items = d.get('items') or []
    cats_meta = {c.get('key'): c for c in (d.get('categories') or []) if isinstance(c, dict)}
    order, labels, buckets = [], {}, {}
    for it in items:
        if it.get('implementation_status') != 'ready':
            continue
        k = it.get('key') or ''
        if not k or k in _DANGEROUS_TASK_KEYS:
            continue
        ck = it.get('category') or 'other'
        if ck not in buckets:
            buckets[ck] = []
            order.append(ck)
        labels[ck] = ((cats_meta.get(ck) or {}).get('label')
                      or _TASK_LABEL_FALLBACK.get(ck) or ck)
        desc = _TASK_DESC_STATIC.get(k) or _one_line(it.get('description') or it.get('title') or '')
        buckets[ck].append((k, it.get('title') or k, desc))
    out = []
    for ck in order:
        # v2.9.31：面板不再受「企微图文单条 8 篇」约束（微信侧改成分批续发），上限放宽到 40。
        # 原 v2.9.10 已收录的排在前面（保底不丢），ETKN 新增的排后面。
        _kept = [t for t in buckets[ck] if t[0] not in _NEEDS_PARAM_TASK_KEYS]
        tasks = ([t for t in _kept if t[0] in _STATIC_PRIORITY]
                 + [t for t in _kept if t[0] not in _STATIC_PRIORITY])[:40]
        if not tasks:
            continue
        out.append({'key': ck, 'label': labels.get(ck, ck),
                    'icon': _TASK_ICON_BY_CAT.get(ck, '🧩'), 'tasks': tasks})
    return out


def refresh_task_catalog_cache(force: bool = False) -> bool:
    """EM 启动时对齐 ETKN 任务目录（也可被 /api/task-catalog-refresh 手动触发）。"""
    global TASK_CATALOG, ETKN_TASK_WHITELIST, _WECOM_CAT_KEYS
    new = []
    try:
        new = _task_catalog_from_etkn()
    except Exception as e:
        print('任务目录跟随失败：%s' % str(e)[:140], flush=True)
    if new:
        TASK_CATALOG = new
        TASK_CATALOG_SOURCE.update({'from': 'etkn',
                                    'ts': _now().isoformat(timespec='seconds'),
                                    'reason': '已对齐 ETKN 实时目录'})
        _task_catalog_cache_write(new)
        ok = True
    else:
        cached = _task_catalog_cache_load()
        if cached:
            TASK_CATALOG = cached
            ok = True
        else:
            ok = False
    ETKN_TASK_WHITELIST = {t[0]: t[1] for c in TASK_CATALOG for t in c['tasks']}
    _WECOM_CAT_KEYS = {'CAT_%s' % c['key'].upper(): c for c in TASK_CATALOG}
    print('任务目录：来源=%s，分类=%d，任务=%d（%s）'
          % (TASK_CATALOG_SOURCE['from'], len(TASK_CATALOG),
             len(ETKN_TASK_WHITELIST), TASK_CATALOG_SOURCE['reason']), flush=True)
    return ok


def _menu_task_cat_keys() -> list:
    """企微菜单「任务中心」二级按钮（按当前动态分类生成，最多 5 个）。"""
    out = []
    for c in TASK_CATALOG[:_WECOM_CAT_MENU_MAX]:
        key = str(c.get('key') or '')
        if not key:
            continue
        label = _CAT_MENU_SHORT.get(key) or ('%s%s' % (c.get('icon') or '🧩', c.get('label') or key))
        out.append({'type': 'click',
                    'name': _wecom_trim(label, _WECOM_NAME_MAX),
                    'key': 'CAT_%s' % key.upper()})
    return out


def _menu_update_task_buttons() -> None:
    """把菜单尾「任务中心」的子按钮换成当前动态分类（改的是模块级 _WECOM_MENU_TAIL，
    之后每次下发菜单都带着新分类）。"""
    subs = _menu_task_cat_keys()
    if not subs:
        return
    for m in _WECOM_MENU_TAIL:
        if m.get('name') == _WECOM_TC_MENU_NAME:
            m['sub_button'] = subs
            return
    # 兜底：尾菜单里没有该项就补一个（正常不会发生）
    _WECOM_MENU_TAIL.append({'name': _WECOM_TC_MENU_NAME, 'sub_button': subs})
# 「运维操作」里点了直接触发的任务（不走「分类→图文消息」这一层）
_WECOM_ACTION_TASKS = {'CLEAN_TMP': 'cleanup-p115-temp-directory'}

# 任务中心链接令牌：进程级长期有效（图文消息里点开可能过很久），容器重启即轮换。
# 只防「链接被预取/被猜到」，与面板其它接口同级的暴露面。
_run_token = {'val': ''}


def _get_run_token() -> str:
    """子任务链接令牌：**持久化到设置**，重启复用。

    v2.9.32：原先每次启动随机生成 → 一重启，微信里旧消息的链接全部失效
    （实际踩到：手机点「小号池测速」报「失败 525/403」，因为那条消息是重启前发的）。
    现在存 SETTINGS['run_token']，只在首次生成，之后重启不变，旧消息长期可用。"""
    if _run_token['val']:
        return _run_token['val']
    v = ''
    try:
        v = (SETTINGS.get('run_token') or '').strip()
    except Exception:
        v = ''
    if not v:
        v = secrets.token_urlsafe(24)
        try:
            SETTINGS['run_token'] = v
            settings_save()
        except Exception:
            pass
    _run_token['val'] = v
    return v


def _run_task_url(task_key: str) -> str:
    """子任务的可点链接：**点开即直接下发 ETKN**（v2.9.32 起不再需要二次确认）。

    为什么保留一个「页面」而不是把触发塞进链接本身：微信消息里的链接只能是 GET，
    而微信/企微会对链接做**服务端预取**——若 GET 即触发，一条消息就会把整类任务跑光。
    现在的页面在加载后用 **JavaScript** 自动 POST 触发：预取器不执行 JS → 不会误触发；
    用户真点开 → 立刻下发，只看到一条结果，少点一次「确认执行」。"""
    return '%s/run-task/%s?token=%s' % (_panel_base(), task_key, _get_run_token())



def etkn_run_task(task_key: str):
    """触发 ETKN 原生任务（toolbox 型，走服务端默认参数）。返回 (ok, msg, status, body)。

    v2.9.8 实测契约（NAS 上真跑过）：
      POST /api/task-center/tasks/<key>/runs  body {"parameters":{}}
        → 202 {"workflow_run_id": 53861, "created": true}
      无效 key → 404 {"detail":"任务不存在"}
    """
    if task_key not in ETKN_TASK_WHITELIST:
        return False, '不在允许的任务白名单内：%s' % task_key, 404, {}
    s, b = api_post('/api/task-center/tasks/%s/runs' % task_key, {'parameters': {}})
    rid = b.get('workflow_run_id') if isinstance(b, dict) else None
    if s in (200, 201, 202) and rid:
        return True, '已触发（run %s）' % rid, s, b
    det = ''
    if isinstance(b, dict):
        det = str(b.get('detail') or b.get('error') or '')[:120]
    return False, 'ETKN 返回 %s %s' % (s, det), s, b


# ---------- 回调消息处理 ----------
def _xml_field(xml: str, name: str) -> str:
    m = re.search(r'<%s>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</%s>' % (name, name), xml, re.S)
    return m.group(1).strip() if m else ''


def _wecom_status_text() -> str:
    snap = _state.get('snapshot') or {}
    fast = _state.get('fast') or {}
    rec, act, dw = snap.get('records') or {}, fast.get('active') or {}, _dayweek_cache
    by = act.get('by_kind') or {}
    _d = dw.get('day') if dw.get('day') is not None else '-'
    _w = dw.get('week') if dw.get('week') is not None else '-'
    lines = ['📈 ETKN 状态 %s' % (_state.get('fast_ts') or snap.get('ts') or '-'),
             '今日完成 %s 媒体 ｜ 本周完成 %s 媒体' % (_d, _w),
             '未识别累计 %s ｜ 今日新增 %s' % (rec.get('unrecognized', '-'),
                                         rec.get('unrecognized_today', '-')),
             '活跃队列：运行 %s / 排队 %s ｜ 活跃媒体 %s'
             % (act.get('running', 0), act.get('queued', 0), act.get('media', 0))]
    for k, v in list(by.items())[:6]:
        lines.append('· %s 运行%s/排队%s' % (k, v.get('running', 0), v.get('queued', 0)))
    if dw.get('week') is None:
        lines.append('（今日/本周统计重建中）')
    return '\n'.join(lines)


_WECOM_HELP = ('🤖 ETKN 机器人\n'
               '直接回复关键词即可：\n'
               '· 状态 — 今日/本周完成 + 队列\n'
               '· 体检 — 一键体检（hosts + 500 + 队列）\n'
               '· 解限流 — 中转池自检 + 假死自愈（v2.9.20）\n'
               '· 清理临时 — 清理 115 临时目录（v2.9.20 起改为关键词触发）\n'
               '· 菜单 — 显示这条帮助\n'
               '也可以点应用底部的自定义菜单。')


def _wecom_run_action(key: str):
    """菜单点击/关键词触发的动作，跑在后台线程里，结果用应用消息推回。"""
    try:
        if key == 'STATUS':
            wecom_push(_wecom_status_text())
        elif key == 'CHECK500':
            wecom_push('已触发 TMDB 500 检测，结果见随后的告警（无异常则静默）。')
            check_500()
        elif key == 'SPEED':
            wecom_push('已触发链路测速，稍后推送结果…')
            res = run_speed_round(alert=False) or []
            if not res:
                wecom_push('测速未产生结果（可能未配置测速目标）。')
            else:
                rows = ['⚡ 测速结果']
                # v2.9.10 修：旧代码读 r['ms']，但测速结果里根本没有 ms 字段（是 total_ms），
                # 取不到就一律落到 else 分支 → **成功也报「失败」**（实际踩到过：6 个域名
                # 全 ok，消息里却全是「失败」）。改成看 ok/total_ms。
                for r in res[:12]:
                    if r.get('ok') and r.get('total_ms') is not None:
                        rows.append('· %s %s ms' % (r.get('host', '-'), r['total_ms']))
                    else:
                        rows.append('· %s 失败%s' % (r.get('host', '-'),
                                                    '（%s）' % r['error'] if r.get('error') else ''))
                wecom_push('\n'.join(rows))
        elif key == 'HOSTS':
            if not (SETTINGS.get('hosts_enabled') and _HOSTS_DOMAINS()):
                wecom_push('未启用 hosts 监控或未配置域名，已跳过。')
            else:
                wecom_push('已触发 hosts 重新检测…')
                out = _hosts_recheck() or []
                rows = ['🌐 hosts 检测结果']
                for r in out[:12]:
                    # v2.9.20 修：旧代码读 r['ip']（该键不存在，_hosts_recheck 给的是 hosts_ip），
                    # 所以 IP 永远显示 '-'；且解析失败被当成「没漂移」悄悄放过。
                    if not (r.get('hosts_ip') or ''):
                        rows.append('· %s → 解析失败（DNS %s）'
                                    % (r.get('domain', '-'), r.get('dns_ip') or '空'))
                    else:
                        rows.append('· %s → %s%s' % (r.get('domain', '-'), r['hosts_ip'],
                                                     '（已改写）' if r.get('changed') else ''))
                wecom_push('\n'.join(rows))
        elif key == 'ORGANIZE':
            fast = _state.get('fast') or {}
            by = ((fast.get('active') or {}).get('by_kind') or {})
            busy = any((by.get(k) or {}).get('running', 0) or (by.get(k) or {}).get('queued', 0)
                       for k in ('网盘整理', '刮削入库', '手动整理网盘文件'))
            if busy:
                wecom_push('整理队列非空，未触发。')
                return
            s, b = api_post('/api/task-center/tasks/organize-p115/runs',
                            {'parameters': {'trigger': 'telegram', 'task_key': 'organize-p115',
                                            'module_key': 'p115_organize',
                                            'handoff_mode': 'independent'}})
            if s in (200, 201, 202) and isinstance(b, dict) and b.get('workflow_run_id'):
                try:
                    _watch_shell_run(int(b['workflow_run_id']), 'wecom')
                except Exception:
                    pass
                wecom_push('✅ 已触发整理下一批（run %s）。' % b['workflow_run_id'])
            else:
                wecom_push('⚠️ 触发整理失败：ETKN 返回 %s %s' % (s, str(b)[:120]))
        elif key in _WECOM_CAT_KEYS:
            # v2.9.10 任务中心：点分类 → 推图文消息，每条 = 该分类下一个子任务（点选即到确认页）。
            # v2.9.31：一条 news 最多 8 篇 → 超过就**分成多条消息续发**，所以面板/目录里
            # 每类可以多于 8 项。子任务链接指向**确认页**（GET 只渲染按钮，预取不会误触发）。
            cat = _WECOM_CAT_KEYS[key]
            arts = [{'title': '%s %s' % (cat['icon'], t[1]),
                     'description': '%s（点开即下发）' % t[2],
                     'url': _run_task_url(t[0])} for t in cat['tasks']]
            if not arts:
                wecom_push('⚠️「%s」下暂无可触发的任务。' % cat['label'])
            else:
                _n = _WECOM_TASK_LIST_PER_MSG
                _parts = (len(arts) + _n - 1) // _n
                for _i in range(0, len(arts), _n):
                    ok, err = wecom_push_news(arts[_i:_i + _n])
                    if not ok:
                        wecom_push('⚠️ 展开「%s」（第 %d/%d 条）失败：%s'
                                   % (cat['label'], _i // _n + 1, _parts, err))
                        break
        elif key == 'PURGE':
            # v2.9.15：与面板「清空共享登记积压」同一个函数，绝不碰 running
            wecom_push('已触发清空共享登记积压，稍后推送结果…')
            r = _purge_register_queued()
            lines = ['🗂 清空共享登记积压',
                     '· 找到排队 %d 条，已取消 %d 条' % (r['found'], r['cancelled'])]
            if r['failed']:
                lines.append('· 失败 %d 条（可在 ETKN 任务中心手动处理）' % len(r['failed']))
            if not r['found']:
                lines.append('· 当前没有排队的共享登记任务，无需清理')
            wecom_push('\n'.join(lines))
        elif key == 'HEALTH':
            # v2.9.15：把原「🔍检测500」+「🌐重检IP」合成一条体检报告，一次点出全结论
            wecom_push('已触发一键体检，稍后推送结果…')
            lines = ['🩺 一键体检']
            # ① hosts 解析
            if SETTINGS.get('hosts_enabled') and _HOSTS_DOMAINS():
                try:
                    out = _hosts_recheck() or []
                    ch = [r for r in out if r.get('changed')]
                    # v2.9.20 修：解析失败（hosts_ip 为空）以前被并进「无需改写」里蒙混过关，
                    # 且 IP 读错键恒显示 '-'。现在单列「解析/绑定异常」并计入异常条数。
                    bad = [r for r in out if not (r.get('hosts_ip') or '')]
                    tail = ('，已改写 %d 个' % len(ch)) if ch else ''
                    if bad:
                        lines.append('· hosts：%d 个域名，%d 个解析/绑定异常%s'
                                     % (len(out), len(bad), tail))
                    else:
                        lines.append('· hosts：%d 个域名%s' % (len(out), tail or '，无需改写'))
                    for r in out[:3]:
                        if not (r.get('hosts_ip') or ''):
                            lines.append('　　%s → 解析失败（DNS %s）'
                                         % (r.get('domain', '-'), r.get('dns_ip') or '空'))
                        else:
                            lines.append('　　%s → %s%s' % (r.get('domain', '-'), r['hosts_ip'],
                                                          '（已改写）' if r.get('changed') else ''))
                except Exception as e:
                    lines.append('· hosts：检测失败（%s）' % str(e)[:60])
            else:
                lines.append('· hosts：未启用或未配置域名，已跳过')
            # ② TMDB 500 签名
            try:
                cnt, samples = _count_500()
                th = SETTINGS['count_500_threshold']
                lines.append('· TMDB 500：今日命中 %d 条（阈值 %d）%s'
                             % (cnt, th, '，正常' if cnt < th else '，偏高'))
                if samples:
                    lines.append('　　样例：' + '、'.join(samples))
            except Exception as e:
                lines.append('· TMDB 500：检测失败（%s）' % str(e)[:60])
            # ③ 队列一句话
            try:
                fast = _state.get('fast') or {}
                by = ((fast.get('active') or {}).get('by_kind') or {})

                def _kk(k):
                    d = by.get(k) or {}
                    return '%s/%s' % (d.get('running', 0), d.get('queued', 0))
                lines.append('· 队列：刮削 %s · 网盘 %s · 共享 %s · 追剧 %s'
                             % (_kk('刮削入库'), _kk('网盘整理'), _kk('共享登记'), _kk('追剧刷新')))
            except Exception:
                pass
            wecom_push('\n'.join(lines))
        elif key == 'RELAY':
            # v2.9.20：中转池自检 + 假死自愈（配额健康却报限流时自动清锁并复测）
            if not relay_enabled():
                wecom_push('⚠️ 中转池自检未启用\n到面板「设置 → 中转池」填：启用开关、地址、管理员密码。')
                return
            wecom_push('已触发中转池自检，稍后推送结果…')
            wecom_push('\n'.join(relay_report(heal=True)))
        elif key in _WECOM_ACTION_TASKS:
            # 「运维操作」里直连的任务（🧹清理临时已改为文字关键词触发），点了直接触发
            tkey = _WECOM_ACTION_TASKS[key]
            name = ETKN_TASK_WHITELIST.get(tkey, tkey)
            ok, msg, _s, _b = etkn_run_task(tkey)
            if ok:
                wecom_push('✅ 已触发「%s」\n%s\n结果会在任务结束后按现有告警规则推送。'
                           % (name, msg))
            else:
                wecom_push('⚠️ 触发「%s」失败：%s' % (name, msg))
        else:
            wecom_push('未知操作：%s' % key)
    except Exception as e:
        try:
            wecom_push('⚠️ 操作 %s 执行异常：%s' % (key, str(e)[:120]))
        except Exception:
            pass


def _wecom_handle_msg(xml: str):
    """收到用户消息/菜单事件后（后台线程）：一律用应用消息主动回复，回调本身返空。"""
    mtype = _xml_field(xml, 'MsgType')
    user = _xml_field(xml, 'FromUserName')
    if mtype == 'event':
        ev, key = _xml_field(xml, 'Event'), _xml_field(xml, 'EventKey')
        if ev == 'click':
            threading.Thread(target=_wecom_run_action, args=(key,), daemon=True).start()
        elif ev in ('subscribe', 'enter_agent'):
            wecom_push(_WECOM_HELP, touser=user)
        return
    if mtype == 'text':
        txt = _xml_field(xml, 'Content')
        if txt in ('状态', 'status', 'STATUS'):
            wecom_push(_wecom_status_text(), touser=user)
        elif txt in ('体检', '一键体检', 'health', 'HEALTH'):
            threading.Thread(target=_wecom_run_action, args=('HEALTH',), daemon=True).start()
        elif txt in ('解限流', '中转池', '自愈', 'relay'):
            # v2.9.20：菜单二级满了，解限流进菜单；清理临时改为关键词触发
            threading.Thread(target=_wecom_run_action, args=('RELAY',), daemon=True).start()
        elif txt in ('清理临时', '清理 临时', 'clean_tmp'):
            threading.Thread(target=_wecom_run_action, args=('CLEAN_TMP',), daemon=True).start()
        else:
            wecom_push(_WECOM_HELP, touser=user)
        return


def _wecom_callback_verify(query: dict):
    """GET 回调 URL 校验：验签 → 解密 echostr → 返回明文。返回 (code, body, ctype)。"""
    g = lambda k: (query.get(k) or [''])[0]
    c = _wecom_cfg()
    if not (c['token'] and c['aeskey']):
        return 500, '未配置回调 Token / EncodingAESKey', 'text/plain; charset=utf-8'
    if wecom_signature(c['token'], g('timestamp'), g('nonce'), g('echostr')) != g('msg_signature'):
        return 401, '签名校验失败', 'text/plain; charset=utf-8'
    try:
        return 200, wecom_decrypt(c['aeskey'], g('echostr'), c['corpid']), 'text/plain; charset=utf-8'
    except Exception as e:
        return 400, '解密失败：%s' % str(e)[:120], 'text/plain; charset=utf-8'


def _wecom_callback_msg(raw: str, query: dict):
    """POST 回调：验签 → 解密 → 后台处理。返回 (code, body)。"""
    g = lambda k: (query.get(k) or [''])[0]
    c = _wecom_cfg()
    enc = _xml_field(raw, 'Encrypt')
    if not (c['token'] and c['aeskey'] and enc):
        return 200, b''
    if wecom_signature(c['token'], g('timestamp'), g('nonce'), enc) != g('msg_signature'):
        return 401, b''
    try:
        xml = wecom_decrypt(c['aeskey'], enc, c['corpid'])
    except Exception:
        return 200, b''
    threading.Thread(target=_wecom_handle_msg, args=(xml,), daemon=True).start()
    return 200, b''


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
FEED_FALLBACK_INTERVAL = 150           # v2.9.3：喂料兜底轮询间隔（秒）。
                                       # 原实现只有「清空提醒推送成功」一个触发点，被 120s 冷却
                                       # 拦下就永久停摆（9/26 07:53 实证：清空距上轮仅 67s →
                                       # 静默丢弃 → 之后 35 分钟零记录）。兜底轮询补这个洞。


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
                            '建议等 115 配额窗口恢复后再触发一次整理；持续出现请夜间低峰重试'])
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
            '文件不会丢：全部在待整理目录等待', '稍后（约 10-30 分钟）再触发一次整理即可（面板「手动整理网盘文件」）',
            f'错误：{body[:90]}'])
    else:
        _alert_push('feed_err', '自动喂料触发整理失败', [
            f'转移 {moved_n} 项（剧夹+散文件）已成功，但整理触发失败（HTTP {s}）',
            body[:100], '请到面板手动触发「手动整理网盘文件」'])


def _feed_run(force: bool = False, quiet_empty: bool = False) -> str:
    """喂料主流程：扫描→计划→转移→触发原生整理。
    转移失败/触发失败→飞书告警卡片；空源目录→飞书提示。全程忙锁+冷却。

    两个入口：
      ① 事件驱动——清空提醒推送成功后（原唯一入口，v2.7 起）；
      ② 兜底轮询——_feed_loop 每 FEED_FALLBACK_INTERVAL 秒（v2.9.3 新增，修死锁）。
    force=True        绕过 2 分钟冷却（v2.9.4 起：清空事件驱动与兜底轮询都用——
                      清空=新批次起点必须立即喂；兜底防的是与事件双触发重复，
                      由忙锁+last_run 双保险兜住）
    quiet_empty=True  源目录为空时不推卡（兜底轮询每 150s 就扫一次，推卡会刷屏）
    返回值仅供 _feed_loop 记日志用，其它调用点忽略。"""
    if not (SETTINGS['push_enabled'] and SETTINGS['feed_enabled']):
        return 'off'
    if _feed_state['moving'] or (not force and time.time() - _feed_state['last_run'] < 120):
        return 'busy'                  # 忙锁 + 2 分钟冷却（清空重试窗口内不重复进）
    with _feed_lock:
        if _feed_state['moving']:
            return 'busy'
        _feed_state['moving'] = True
    try:
        src = SETTINGS['feed_src_dir'].rstrip('/')
        dst = SETTINGS['feed_dst_dir'].rstrip('/')
        limit = max(1, int(SETTINGS['feed_batch_limit'] or 500))   # v2.8.6：每批文件夹数
        scanned = _feed_scan_entries(src)
        if scanned is None:
            _alert_push('feed_err', '自动喂料失败', [
                f'源目录不可读：{src}', '多半是 /cloud115 挂载未生效或权限变化，请检查容器挂载'])
            return 'err'
        dirs, loose = scanned
        # v2.8.12：夹+散文件都空才算「源目录已空」（用户 9/19 晚定案：散文件参与转移）
        if not dirs and not loose:
            if not quiet_empty:
                _alert_push('feed_empty', '源目录已空，可放新文件', [
                    f'{src} 当前没有待整理文件夹', '放入新剧/电影后，下次清空提醒会自动喂料'],
                    buttons=_card_buttons(), tcolor='blue')
            return 'empty'
        # v2.8.12 计划：夹优先、散文件补足配额，合计 ≤ limit（按个数累加，一个夹=1 项=一个散文件）
        picks_dirs = dirs[:limit]
        picks_files = loose[:max(0, limit - len(picks_dirs))]
        total_items = len(picks_dirs) + len(picks_files)
        # 转移顺序：先夹后散文件；v2.8.16：404（源已无）跳过继续，其余错误即停回报
        moved_dirs, err, skip_d = _feed_move_batch(src, dst, picks_dirs)
        if err:
            _feed_err_partial(picks_dirs, picks_files, moved_dirs, [], total_items, err)
            return 'fail'
        moved_files, err, skip_f = _feed_move_batch(src, dst, picks_files)
        if err:
            _feed_err_partial(picks_dirs, picks_files, moved_dirs, moved_files, total_items, err)
            return 'fail'
        if not moved_dirs and not moved_files:
            _feed_cache_drop(list(skip_d) + list(skip_f))   # 全是幽灵文件：剔缓存防空转
            _alert_push('feed_err', '自动喂料转移失败', [
                f'本批 {total_items} 项全部跳过：源文件在 115 端已不存在（多半是列表缓存残影，'
                '上一批已转走或源已删除）', '已把这些条目从扫描缓存剔除，下轮不再误报'],
                buttons=_card_buttons(), tcolor='yellow')
            return 'skip'
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
        return 'ok'
    finally:
        _feed_state['moving'] = False
        _feed_state['last_run'] = time.time()


def _feed_loop() -> None:
    """v2.9.3：喂料兜底轮询——修「事件驱动单点」死锁（2026-09-26 实证）。

    缺陷：_feed_run 全项目只有一个调用点（清空提醒推送成功之后），入口还有 120 秒冷却。
    9/26 07:52:02 转完 7 项 → 07:53:09 清空提醒触发 → 距上轮仅 67 秒 → 被冷却静默丢弃；
    此后没有新的整理任务、也就没有新的清空事件 → _feed_run 永不再被调用，喂料彻底停摆
    （07:53 之后 35 分钟零记录，源目录 259 个散文件 + 2 个剧夹原地不动）。

    本线程每 FEED_FALLBACK_INTERVAL 秒兜底一次，条件与「清空」同口径：
      ① 喂料/推送开关都开；
      ② 当前没有整理类任务在跑或排队（网盘整理/刮削入库/手动整理网盘文件）——
         与清空判定同源数据，避免整理还没做完就往待整理目录里灌；
      ③ 快轮询已出过至少一轮（防启动竞速把「还没采集」误判成「队列为空」）。
    满足则 _feed_run(force=True, quiet_empty=True)：绕冷却、源目录为空不推卡。
    三条都不满足时安静跳过，不产生任何请求与推送。
    """
    while True:
        time.sleep(FEED_FALLBACK_INTERVAL)
        try:
            if not (SETTINGS.get('push_enabled') and SETTINGS.get('feed_enabled')):
                continue
            if _feed_state['moving']:
                continue
            fast = _state.get('fast') or {}
            if not fast:
                continue                       # 快轮询还没出第一轮，等下一轮再判
            by = ((fast.get('active') or {}).get('by_kind') or {})
            busy = sum((by.get(k) or {}).get('running', 0) + (by.get(k) or {}).get('queued', 0)
                       for k in ('网盘整理', '刮削入库', '手动整理网盘文件'))
            if busy > 0:
                continue                       # 整理任务没清空，等清空事件或下轮兜底
            # v2.9.19：批次尾声竞态抑制——整理批次还没判清空（active=True）时不抢先喂料，
            # 等清空卡推出后的事件驱动喂料（9/28 实证：兜底 16:55:50 与清空事件 16:56:10
            # 双跑两轮各 8 夹，用户同时收到两张喂料完成卡）。停摆场景无批次进行中，不受影响。
            if _organize_state.get('batch', {}).get('active'):
                continue
            if time.time() - _feed_state['last_run'] < 90:
                continue                       # 距上次喂料不足 90s（多半刚被事件驱动喂过），
                                               # 不抢——避开「喂料→延迟触发整理」之间的空窗
            _before = _feed_state['last_run']
            st = _feed_run(force=True, quiet_empty=True)
            if st == 'ok':
                _bt = (datetime.fromtimestamp(_before, TZ).strftime('%H:%M')
                       if _before else '无')
                print(f'[feed_loop] 兜底轮询补跑喂料成功（上次喂料 {_bt}）', flush=True)
        except Exception as e:                 # 兜底线程绝不允许因单次异常退出
            print(f'[feed_loop] 兜底轮询异常：{type(e).__name__}: {e}', flush=True)


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
               shlex.quote(PW), shlex.quote(os.environ.get('SSH_PORT', '22')),
               shlex.quote(os.environ.get('SSH_USER', 'root')),
               shlex.quote(os.environ.get('SSH_HOST', '')),
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
_HOSTS_DOMAIN_CONST = 'shared.example.com'
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


_DOH_ENDPOINTS = (                          # v2.8.26：DoH 端点顺位（任一成功即返回）
    'https://dns.alidns.com/resolve',
    'https://doh.pub/dns-query',
    'https://cloudflare-dns.com/dns-query',
)


def _hosts_ts() -> str:
    """巡检日志时间戳（北京时区），配合 [hosts] 前缀做日志审计（v2.8.26）。"""
    return '[hosts] ' + _now().strftime('%H:%M:%S')


def _doh_resolve(name: str, timeout: int = 6) -> str:
    """v2.8.26：DoH（DNS over HTTPS）解析 A 记录——HTTPS/443 直连端点，不经本机解析器，
    绕开软路由对明文 53 端口的 DNAT 劫持。依次尝试 _DOH_ENDPOINTS，任一成功即返回首个
    A 记录；全部失败返回 ''。标准库 urllib+json，零第三方依赖。
    JSON 口径：Status==0 且 Answer[].type==1（RFC 8484 的 JSON 变体）。"""
    for base in _DOH_ENDPOINTS:
        try:
            req = urllib.request.Request(
                f'{base}?name={urllib.parse.quote(name, safe="")}&type=A',
                headers={'Accept': 'application/dns-json'})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode('utf-8', 'replace'))
            if data.get('Status') != 0:
                continue                    # 该端点判失败（NXDOMAIN 等），换下一个
            for ans in data.get('Answer') or []:
                if ans.get('type') == 1 and ans.get('data'):
                    return str(ans['data'])
        except Exception:
            continue
    return ''


def _udp53_resolve(name: str, server: str = '223.5.5.5') -> str:
    """明文 UDP/53 解析（v2.8.26 自 _resolve_public 抽出，解析逻辑不变）。
    本网络该通道被软路由 DNAT 到本地解析器（读路由器 /etc/hosts），结果只作兜底且不可信，
    信任判定见 _resolve_public / _hosts_check_one。socket/struct 用模块级导入（可 monkeypatch）。
    v2.8.3：qname 必须逐段交错（每段=长度前缀+标签体），集中前置=畸形报文，服务器不回。"""
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


def _resolve_public(name: str, server: str = '223.5.5.5') -> str:
    """解析域名公网 A 记录，返回首个 A 记录字符串；失败返回 ''（签名与返回语义不变）。
    v2.8.26 缺陷①修复：旧实现明文 UDP/53 直连 223.5.5.5，本网络被软路由 nft 把 dport 53
    全量 DNAT 到本地解析器（读路由器 /etc/hosts）——对被 hosts 钉住的域名，查回的恒为
    hosts 里的值，dns_ip==hosts_ip 恒成立，漂移永远检不出（假阴性）。
    新实现：主路 DoH（HTTPS/443），alidns→doh.pub→cloudflare 任一成功即返回；明文 UDP/53
    仅最后兜底，兜底结果视为不可信——调用方不得据「兜底值==hosts 值」判无漂移。"""
    ip = _doh_resolve(name)
    if ip:
        return ip
    return _udp53_resolve(name, server)


def _hosts_alert(kind: str, kind_line: str, detail_lines: list,
                 buttons: list = None, tcolor: str = 'yellow') -> None:
    """v2.8.26 缺陷②修复：hosts 巡检推送统一出口——修复照做，发不发卡片只由推送总开关
    push_enabled 决定（自动巡检不再被 push_enabled 连坐停摆）。"""
    if SETTINGS.get('push_enabled'):
        _alert_push(kind, kind_line, detail_lines, buttons=buttons, tcolor=tcolor)


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
        _hosts_alert('hosts_err', '路由器连接未配置或认证失败（hosts 未改动）', [
            '请到设置页填写：软路由 IP / SSH 端口 / SSH 用户名 / SSH 密码',
            '巡检需要这些信息登录路由器修改 /etc/hosts；本次仅告警不改行'])
        return
    for _d in domains:              # v2.8.17：逐域名巡检（一域一 IP，互不影响）
        try:
            _hosts_check_one(_d)
        except Exception as e:
            print(_hosts_ts(), f'{_d} 巡检异常：{type(e).__name__} {e}', flush=True)


def _hosts_check_one(domain: str) -> None:
    """单域名巡检核心（v2.8.17 自 _hosts_check_once 抽出；v2.8.26 增加解析可信度判定）。"""
    doh_ip = _doh_resolve(domain)           # v2.8.26 主路：DoH（可信）
    ip = doh_ip or _udp53_resolve(domain)   # 兜底：明文 UDP/53（本网被 DNAT 劫持，不可信）
    trusted = bool(doh_ip)                  # 唯有 DoH 直出才允许判「无漂移」
    if not ip:
        print(_hosts_ts(), f'{domain} 解析失败（DoH 与明文兜底均无 A 记录），hosts 未改动',
              flush=True)
        _hosts_alert('hosts_err', f'{domain} 域名解析失败（hosts 未改动）', [
            f'公网解析 {domain} 无 A 记录——可能域名/源站故障',
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
            print(_hosts_ts(), f'{domain}：hosts 新增绑定 {ip} 成功', flush=True)
            _hosts_alert('hosts', f'{domain}：hosts 新增绑定 {ip}', [
                f'路由器 /etc/hosts 原无 {domain} 独立行，已按公网解析新增并重启 dnsmasq',
                '原文件已备份 /etc/hosts.bak-monitor'],
                buttons=_card_buttons(), tcolor='blue')
        else:
            print(_hosts_ts(), f'{domain}：hosts 新增绑定失败，回执：{(out or "(空)")[:100]}',
                  flush=True)
            _hosts_alert('hosts_err', 'hosts 新增绑定失败（未生效）', [
                f'期望新增 {ip} {domain}，路由器回执异常', f'回执：{(out or "(空)")[:100]}'])
        return
    line_no, rest = pick
    old_ip = rest.split()[0]
    if old_ip == ip:
        if not trusted:                 # v2.8.26：明文兜底值不可信，不判「无漂移」防假阴性
            print(_hosts_ts(), f'{domain}：DoH 全败，明文兜底值与 hosts 一致但不可信，'
                               f'本轮不判无漂移，下一轮重试', flush=True)
        return                          # 一致且可信：静默
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
        print(_hosts_ts(), f'{domain}：{old_ip} → {ip}，hosts 已更新并重启 dnsmasq', flush=True)
        _hosts_alert('hosts', f'{domain}：{old_ip} → {ip}，hosts 已更新', [
            f'223.5.5.5 公网解析与 hosts 绑定不一致，已将该行改为 {ip} 并重启 dnsmasq',
            '原行已备份 /etc/hosts.bak-monitor'],
            buttons=_card_buttons(), tcolor='green')
    else:
        print(_hosts_ts(), f'{domain}：hosts 更新失败，期望 {ip}，回执：{(out or "(空)")[:100]}',
              flush=True)
        _hosts_alert('hosts_err', 'hosts 更新失败（未生效）', [
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
    """每小时巡检线程（守护，绝不影响主流程）。
    v2.8.26 缺陷②修复：自动巡检只由 hosts_enabled 控制，与推送总开关 push_enabled 解耦
    （旧判定 push_enabled and hosts_enabled → 用户关推送后巡检线程每小时空转，自动巡检
    形同死亡，且被手动「重新检测 IP」的正常表象掩盖）。巡检/修复照跑；发不发推送卡片
    由 _hosts_alert 按 push_enabled 决定。"""
    while True:
        try:
            if SETTINGS.get('hosts_enabled') and _HOSTS_DOMAINS():
                print(_hosts_ts(), '自动巡检开始（每小时，push_enabled=%s）'
                      % SETTINGS.get('push_enabled'), flush=True)
                _hosts_check_once()
                print(_hosts_ts(), '自动巡检完成', flush=True)
        except Exception as e:
            print(_hosts_ts(), f'巡检线程异常：{type(e).__name__} {e}', flush=True)
        time.sleep(3600)


def _panel_base() -> str:
    """面板外网基址（卡片「打开面板」按钮用）。未配置内网地址时回落 127.0.0.1:端口。"""
    fb = LAN_HOST or ('127.0.0.1:%d' % int(os.environ.get('MONITOR_PORT', '8620')))
    return (SETTINGS.get('trigger_public_base') or f'http://{fb}').rstrip('/')


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


def _purge_register_queued(limit: int = 500) -> dict:
    """清空共享登记积压：只取消 status=queued 的共享登记运行（绝不碰 running）。

    v2.9.15：从 /api/purge-register-queued 抽出来，供企微菜单「🗂清空登记」复用。
    「共享登记」=display_title 前缀（其 workflow_type 是 manual_task，与追剧刷新同型），
    因此按标题前缀识别而非 workflow_type。逐条调用原生 cancel；单条失败不中断。
    """
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
    for rid in targets[:max(1, int(limit))]:
        try:
            s1, b1 = api_post(f'/api/workflows/{rid}/cancel', {})
            if s1 in (200, 201, 202):
                ok_ids.append(rid)
            else:
                fails.append({'id': rid, 'status': s1, 'body': b1})
        except Exception as e:      # 单条失败不中断
            fails.append({'id': rid, 'error': str(e)[:120]})
    return {'found': len(targets), 'cancelled': len(ok_ids), 'failed': fails, 'ids': ok_ids}


def _count_500():
    """今日异常明细里命中 500 签名的条数 + 样例。返回 (cnt, samples)。

    v2.9.15：从 check_500 抽出来，供「一键体检」复用（体检要拿到数字，不能只看是否触发告警）。"""
    day = _today00().date().isoformat()
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
    return cnt, samples


def check_500():
    """今日异常明细中 500 签名命中 ≥ 阈值 → 推送一次；归零自动重新布防。"""
    day = _today00().date().isoformat()
    if _alm['t500_day'] != day:
        _alm['t500_day'] = day
        _alm['t500_fired'] = False
    cnt, samples = _count_500()
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
        px, via = _speedtest_proxy(t['host'], t.get('proxy'))   # v2.9.2 跟随 ETKN 口径
        r = speedtest_one(t['host'], proxy=px)
        r['via'] = via
        r['proxy_used'] = px
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
            # v2.9.16：这句「怎么触发」的提示必须按通道分开写——两条通道的按钮形态不一样：
            #   飞书 = markdown 交互卡片，按钮文字就是「整理下一批」；
            #   企微 = textcard，那个唯一按钮的文案是「确认整理」（btntxt）。
            # 旧版共用一句「点击「整理下一批」确认后触发」，在企微/微信端对不上按钮名（用户报的）。
            tail_feishu = tail_wecom = ''
            if SETTINGS['trigger_enabled'] and not a_run and not a_que:
                # v2.6 卡片按钮入口：一次性令牌 30 分钟；每次清空轮换，旧令牌作废
                tok = secrets.token_urlsafe(24)
                _trigger_token['val'] = tok
                _trigger_token['exp'] = time.time() + 1800
                btns = _card_buttons(tok)
                tail_feishu = '（30 分钟内有效，点卡片上的「整理下一批」按钮）'
                # v2.9.18：微信插件端 textcard 不渲染 btntxt 按钮（9/27 用户截图实锤），
                # 整条消息可点=打开确认页。文案改成「点本条消息」——企微 App 里点按钮
                # 或点消息体、微信插件里点消息体，都到同一个确认页。
                tail_wecom = '（30 分钟内有效，点本条消息确认整理）'
            base = '\n'.join(lines)
            text = '%s\n%s' % (base, tail_feishu) if tail_feishu else base
            text_w = '%s\n%s' % (base, tail_wecom) if tail_wecom else base
            ok, err = push_both(text, buttons=btns, wecom_text=text_w,
                                title='✅ 整理任务已清空', tcolor='green')
            record_push('clear', text, ok, err)
            _organize_state['last_clear_at'] = now
            _organize_state['batch'] = {'active': False, 'done': 0, 'failed': 0,
                                        'cancelled': 0, 'm_ok': 0, 'm_bad': 0,
                                        'started_at': None, 'last': None, 'last_ts': None,
                                        'flow': {}, 'other_fail': []}
            if ok:
                # v2.9.4：事件驱动改 force 绕冷却——清空=新批次起点，理应立即喂料。
                # 原先走 120s 冷却：批次提速后（2~3 分钟/轮）36% 轮次被拒（9/26 实证
                # 11 轮中 4 轮延迟 60~100s，全落在「清空距上轮喂料 <120s」窗口），
                # 拖到 150s 兜底轮询才补跑。忙锁仍在，无重复进风险。
                _feed_run(force=True)
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
    poll_once 只读缓存（0 翻页），慢轮周期回归 2 分钟级。
    v2.8.24 根治：succeeded 服务端无索引深翻页每页 10 秒（9/22 实测 118 页 918 秒），
    全量重扫会把发布缓存滞后拉到 15 分钟级。改为「停页线扫描」。
    v2.9.2.13 修正停页线方向：旧版判「整页最旧 finished_at 早于今日 0 点-1h 即停」，
    但该列表并非按 finished_at 单调（第 1 页就混着 4 天前的条目）→ 第 1 页即命中 →
    整轮只扫 100 条（实测今日 451 条只收进 90 条）。改为「整页最新都早于今日 0 点」才停，
    实测今日任务集中在 offset 0~600，7 页收全。"""
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
                    _page_max = ''
                    for it in items:
                        fin = it.get('finished_at') or it.get('created_at') or ''
                        if not it.get('actionable'):
                            if not _page_max or fin > _page_max:
                                _page_max = fin
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
                    # 边翻边发布：今日明细渐进可见（新页先翻先并入）。
                    # v2.9.2.14：只在「还没出过完整轮次」时发布半截值（冷启动首轮先给点东西）。
                    # v2.9.2.13 把停页线从「整页最旧」改成「整页最新」后，今日要翻 3 页×15s≈45s
                    # 才收全（旧口径 1 页即停），而每轮 = 45s 扫描 + 60s 休眠 ≈105s，
                    # 于是半截值会占掉 ~43% 的时间——实测面板显示 90 任务，完整轮次是 121，
                    # 用户看到的「今日完成 90 任务」是假的。有过完整轮次后，整轮扫描期间
                    # 一律保留上一轮的完整值（宁旧勿缺），收全后再原子替换。
                    if _today_done['ts'] == 0:
                        _today_done['items'] = fresh
                    # v2.9.2.13 停页线改用「整页最新」：该列表并非按 finished_at 单调
                    # （第 1 页里就混着 4 天前的条目，见 _dayweek_rebuild 注释），
                    # 用「整页最旧」会在第 1 页就命中 → 整轮只扫 100 条。
                    # 改成「整页最新都早于今日 0 点」才停，才真正收全今日。
                    if items and _page_max and _page_max < today_prefix:
                        break
                    if len(items) < PAGE:
                        break
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
    # v2.9.2.13：停页线由「整页最旧」改为「整页最新都早于今日 0 点」才停。
    # 旧版用整页最旧 + 今日 0 点-1h 余量，但该列表并非按 finished_at 单调
    # （第 1 页混着 4 天前的条目）→ 第 1 页就命中 → 只扫 100 条。
    for st in ('succeeded', 'failed', 'partial'):
        offset = 0
        _pages = 0
        while True:
            _pages += 1
            if _pages > 15:
                break
            s, b = api_get(f'/api/workflows?status={st}&limit={PAGE}&offset={offset}')
            items = b.get('items', []) if isinstance(b, dict) else []
            if not items:
                break
            _page_max = ''
            for it in items:
                fin = it.get('finished_at') or it.get('created_at') or ''
                # [v2.9.26] 忽略置顶项(actionable=True)的时间，仅以普通项时间作为停页线
                if not it.get('actionable'):
                    if not _page_max or fin > _page_max:
                        _page_max = fin
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
            if items and _page_max and _page_max < today_prefix:   # v2.9.26：整页普通项最新早于今日即停
                break
            if len(items) < PAGE:  # 已经翻到底
                break
            if offset >= 30000:   # v2.7.1 安全闸：防接口异常时的无限翻页（正常今日远小于此）
                break
    return out


# v2.8.23：缓存扩异常三元——unrec=累计未识别（全局计数器，跨日有效）；
# bad_tasks=今日失败/部分任务数；unrec_today=今日新增未识别（后两项为日内口径，跨日须清零）
_dayweek_cache = {'ts': 0.0, 'date': '', 'day': None, 'week': None, 'rebuilding': False,
                  'unrec': 0, 'bad_tasks': 0, 'unrec_today': 0, 'by_kind_fail': {}}
_DAYWEEK_INTERVAL = 60    # v2.9.2.13：1 分钟一轮（旧值 900 是为全量翻页 ~350s/轮让路；
                          # 现改为 2 个轻量记录接口，一轮 0.2s，今日完成可近实时——
                          # 该数字每天都在涨，刷新间隔直接决定它与 ETKN 记录页的偏差）


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
    """读 {date: 统计} 表。v2.8.22 值升级三元组 {'succ','unrec','bad_tasks'}
    （succ=今日完成 succ 媒体，unrec=未识别累计，bad_tasks=今日失败/部分任务数）；
    v2.8.23 扩四元：+unrec_today（今日新增未识别，processed_from 当日界内实收计数）；
    v2.8.24 再扩 by_kind_fail（分类失败数 map，透传不归一化）；
    历史日 int 值归一化为 {'succ': v, 'unrec': 0, 'bad_tasks': 0, 'unrec_today': 0}。"""
    try:
        with open(_dayweek_daily_path(), encoding='utf-8') as f:
            d = json.load(f)
        if not isinstance(d, dict):
            return {}
        out = {}
        for k, v in d.items():
            if isinstance(v, dict):
                out[k] = {'succ': v.get('succ') or 0, 'unrec': v.get('unrec') or 0,
                          'bad_tasks': v.get('bad_tasks') or 0,
                          'unrec_today': v.get('unrec_today') or 0}
                if isinstance(v.get('by_kind_fail'), dict):
                    out[k]['by_kind_fail'] = v['by_kind_fail']
            else:
                out[k] = {'succ': v or 0, 'unrec': 0, 'bad_tasks': 0, 'unrec_today': 0}
        return out
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


def _records_success_since(iso_from: str):
    """v2.9.2.13 整理记录口径：返回 processed_at >= iso_from 且 status=success 的记录条数。

    ⚠️ 必须用 record_total，不能用 total —— ETKN 的 total 是「分页分组数」
    （电视剧 success 记录按 tmdb_id+分类+季+入库方式归成一组，p115_repository.list_records
    的 page_key），record_total 才是 sum(group_size)=记录条数。实测同一请求
    total=7520 而 record_total=77500。
    请求失败返回 None（调用方负责保旧值，绝不落 0）。"""
    q = urllib.parse.quote(iso_from)
    s, b = api_get(f'/api/p115/records?per_page=1&page=1&status=success&processed_from={q}')
    if s != 200 or not isinstance(b, dict):
        return None
    try:
        return int(b.get('record_total'))
    except (TypeError, ValueError):
        return None


def _dayweek_rebuild() -> None:
    """v2.9.2.13：今日/本周完成改为「整理记录口径」直读 ETKN 记录接口（2 个轻量请求）。

    为什么废弃 v2.8.17 的 workflows 翻页扫描（今日值长期偏低的真根因）：
    旧方案按 `/api/workflows?status=succeeded` 翻页、累加 batch_ingest(刮削入库) 的
    succeeded_count，并用「整页最旧 finished_at 早于今日 0 点-1h 即停」当停页线。
    但实测该列表**不是按 finished_at 单调排序**——第 1 页里就混着 finished_at 为 4 天前
    的条目（2026-09-23 实测第 1 页 min=09-19T23:02，而同页 max=09-24T00:02，
    疑似「链仍活跃」的旧 run 被顶到列表顶部）。于是停页线在第 1 页就命中 →
    整轮只扫了 100 条 → 今日值严重偏低。实测对照（09-23 当日）：

        面板显示            672
        旧口径真值(刮削入库)  1681
        整理记录口径真值      1597

    而卡片上「本周完成」的文案一直写着「整理记录口径」——即实现与文案本来就不一致。
    新实现与文案对齐，只取 status=success（已识别/完成），与「完成」二字一致。

    v2.9.4.1（2026-09-26 用户拍板）：窗口由「近 7 天滚动」改为**自然周**
    （周一 00:00 起，北京时间）。⚠️ 这与 ETKN 自己 UI 的「本周处理」口径**不再一致**——
    实测 ETKN `stats.thisWeek` = 「近 7 天滚动 × 全状态（含未识别）」，
    而这里是「自然周 × 仅 success」。这是**有意分歧**，别再照着 ETKN UI 去「对齐」。
    另：daily.json 里仍写今日 succ（记录口径），供历史回溯与预热使用。"""
    if _dayweek_cache['rebuilding']:
        return
    _dayweek_cache['rebuilding'] = True
    try:
        today0 = _today_prefix()
        today_d = today0[:10]
        tbl = _dayweek_daily_load()
        _prev = tbl.get(today_d) or {}
        _prev_succ = _prev.get('succ', 0) if isinstance(_prev, dict) else (_prev or 0)

        # ①今日完成（整理记录口径：success 且 processed_from=今日 0 点）
        day_ok = _records_success_since(today0)
        if day_ok is None:                       # 接口失败 → 宁显旧不显零
            day_ok = _prev_succ
        # ②本周完成（整理记录口径 × 自然周：本周一 00:00 起，北京时间）
        # v2.9.4.1：原为 (_now() - 7天) 滚动窗口；用户 2026-09-26 拍板改自然周。
        week_from = (_today00() - timedelta(days=_today00().weekday())).isoformat(timespec='seconds')
        week_ok = _records_success_since(week_from)
        if week_ok is None:
            week_ok = _dayweek_cache.get('week')

        # ③异常值：unrec=累计未识别（全局计数器，跨日有效）；bad_tasks=今日失败/部分任务数；
        #    unrec_today=今日新增未识别（后两项日内口径，跨日清零）。
        # v2.9.23 跨日守卫（修「今日异常」跨日继承 Bug）：缓存日期非今日时，日内口径
        # 一律从 0 重计，绝不把昨日 bad_tasks/unrec_today/by_kind_fail 带进新一天。
        # 旧实现直接读缓存 → 0 点后首轮 rebuild 把昨日值原样写进今日键，随后被下面的
        # 「防跳水」闸反复固化，面板「今日异常」长期虚高（2026-10-01 实测 1388，真值 ~250）。
        _cache_is_today = (_dayweek_cache.get('date') == today_d)
        # v3.12.0 修复：进程重启后 _dayweek_cache 为空 → _cache_is_today=False → 首轮 rebuild
        # 把 daily.json「今日键」的日内口径（unrec_today/bad_tasks/by_kind_fail）写成 0；
        # 而 poll 回填只改内存缓存、要等下一轮 rebuild 才落盘 → 实测今日 unrec_today 长期落 0
        # （历史日正常，页面显示的是实时值所以看不出）。改为以「磁盘今日键」为下限，绝不回退。
        _p_ut  = ((_prev.get('unrec_today', 0) or 0) if isinstance(_prev, dict) else 0)
        _p_bt  = ((_prev.get('bad_tasks', 0) or 0) if isinstance(_prev, dict) else 0)
        _p_bkf = dict(_prev.get('by_kind_fail') or {}) if isinstance(_prev, dict) else {}
        if not _cache_is_today:   # 跨日翻转 / 重启：清缓存里的昨日日内口径，但用今日磁盘值垫底
            _dayweek_cache['bad_tasks'] = _p_bt
            _dayweek_cache['unrec_today'] = _p_ut
            _dayweek_cache['by_kind_fail'] = dict(_p_bkf)
        _cur_unrec = _dayweek_cache.get('unrec') or 0   # 累计口径，跨日保留
        _cur_bad = _dayweek_cache.get('bad_tasks') or 0
        _cur_urtd = _dayweek_cache.get('unrec_today') or 0
        _new_rec = {'succ': day_ok, 'unrec': _cur_unrec, 'bad_tasks': _cur_bad,
                    'unrec_today': _cur_urtd,
                    'by_kind_fail': dict(_dayweek_cache.get('by_kind_fail') or {})}
        # 异常值闸：现值异常跳水（比持久旧值少 50%+）→ 保旧值（etkn 重启窗口不许洗掉异常数）。
        # v2.9.23：仅当缓存属今日时才允许持久键压制——跨日首轮不得用昨日值洗回今日真值。
        _prev_unrec = _prev.get('unrec', 0) if isinstance(_prev, dict) else 0
        _prev_bad = _prev.get('bad_tasks', 0) if isinstance(_prev, dict) else 0
        _prev_urtd = _prev.get('unrec_today', 0) if isinstance(_prev, dict) else 0
        if _prev_unrec > 0 and _cur_unrec < _prev_unrec * 0.5:
            _new_rec['unrec'] = _prev_unrec
        if _cache_is_today and _prev_bad > 0 and _cur_bad < _prev_bad * 0.5:
            _new_rec['bad_tasks'] = _prev_bad
        if _cache_is_today and _prev_urtd > 0 and _cur_urtd < _prev_urtd * 0.5:
            _new_rec['unrec_today'] = _prev_urtd
        # 分类失败数兜底——持久旧值各分类取 max（重启窗口不许洗掉）；v2.9.23 跨日不继承
        _prev_bkf = _prev.get('by_kind_fail', {}) if isinstance(_prev, dict) else {}
        if _cache_is_today and isinstance(_prev_bkf, dict) and _prev_bkf:
            _cur_bkf = dict(_new_rec.get('by_kind_fail') or {})
            for _k, _v in _prev_bkf.items():
                if _v > (_cur_bkf.get(_k) or 0):
                    _cur_bkf[_k] = _v
            _new_rec['by_kind_fail'] = _cur_bkf
        tbl[today_d] = _new_rec
        _dayweek_daily_save(tbl)

        _today_rec = tbl.get(today_d) or {}
        # v2.8.24：缓存刷新时保留 by_kind_fail（rebuild 刷缓存不洗掉 poll 回填的分类失败数）
        _dayweek_cache.update({'ts': time.time(), 'date': today_d,
                               'day': _new_rec['succ'], 'week': week_ok,
                               'unrec': _today_rec.get('unrec', 0) if isinstance(_today_rec, dict) else 0,
                               'bad_tasks': _today_rec.get('bad_tasks', 0) if isinstance(_today_rec, dict) else 0,
                               'unrec_today': _today_rec.get('unrec_today', 0) if isinstance(_today_rec, dict) else 0})
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
    （今日千条量级一页可收全；深页 page=2 实测 0 条，无需翻页）。
    v2.8.24 修复：per_page=1000 大页在 ETKN 繁忙时可 >30s 超时（9/22 实锤，卡死
    poll_once 首轮 55 分钟）→ 改 200/页×最多 5 页实收计数（今日千条 2 页可收全，
    单页负载低不再触发服务端慢查询；页满 200 且不足 5 页时继续，收全即停）。"""
    out = {}
    for st, key in (('success', 'success'), ('unrecognized', 'unrecognized')):
        s, b = api_get(f'/api/p115/records?page=1&per_page=1&status={st}'
                       f'&processed_from={urllib.parse.quote(today_prefix)}')
        out[key] = (b or {}).get('total') or 0 if s == 200 else 0
    s, b = api_get(f'/api/p115/records?per_page=1&page=1&processed_from={urllib.parse.quote(today_prefix)}')
    out['total'] = (b or {}).get('total') or 0 if s == 200 else 0
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


# ---------- v2.9.2 测速口径跟随 ETKN（不硬编码代理） ----------
# 面板的测速必须和 ETKN 业务走同一条路：ETKN 的代理写在它容器的
# HTTP_PROXY/HTTPS_PROXY/NO_PROXY 里（compose env）。这里通过已有的宿主 SSH 通道读
# etkn 容器的**实时 env**（不落盘、不硬编码）——用户在 ETKN 侧换代理，面板下一轮自动跟随。
_etkn_net_cache = {'ts': 0.0, 'proxy': None, 'noproxy': [], 'src': u'未取到 ETKN 代理（按直连）'}


def _host_password() -> str:
    """宿主凭据：compose env_file 注入优先，其次 hermes/.env（与自动重启同一来源）。"""
    pw = str(os.environ.get('SUDO_PASSWORD') or '')
    for line in ('/hermes/.env', '/app/hermes/.env', '/vol1/@appdata/trim.hermes/hermes/.env'):
        try:
            with open(line, encoding='utf-8') as f:
                for ln in f:
                    if ln.startswith('SUDO_PASSWORD='):
                        pw = ln.split('=', 1)[1].strip()
                        break
        except OSError:
            continue
        if pw:
            break
    return pw


def _host_ssh(cmd: str, timeout: int = 20):
    """走宿主通道跑一条只读命令（monitor 容器内无 docker/sock）。返回 (ok, stdout)。"""
    pw = _host_password()
    if not pw:
        return False, ''
    full = ("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no -o ConnectTimeout=8 "
            "-o LogLevel=ERROR %s@%s %s") % (
                shlex.quote(pw), shlex.quote(os.environ.get('SSH_PORT', '22')),
                shlex.quote(os.environ.get('SSH_USER', 'root')),
                shlex.quote(os.environ.get('SSH_HOST', '')),
                shlex.quote(cmd))
    try:
        r = subprocess.run(full, shell=True, capture_output=True, text=True, timeout=timeout)
        return (r.returncode == 0, (r.stdout or '').strip())
    except Exception:
        return False, ''


# ---------- v2.9.22 ETKN bind-mount 补丁自检（v2.9.24 覆盖第三处 p115；v2.9.25 上游 rebase 检查覆盖全部三处） ----------
# 背景：ETKN 本体是 vendor 镜像 hbq0405/etkn:latest，飞牛上没有源码工程，我们改代码的
# 唯一路子是往 docker-compose.yml 里追加 bind-mount 覆盖 vendor 模块。于是每次更新镜像
# 都可能踩两个坑：
#   ① compose 被重写 → mount 行丢了 → 补丁静默失效（fulfilled 退回 ~22s，功能不坏但变慢）
#   ② 镜像里上游改了同一个文件 → 我们的是「整文件副本」补丁 → 会把新版盖回去，上游修复丢失
#      三处补丁（repository / shared_pool / p115）逐个比对，任一跑偏都报 need_rebase=true
# 检查逻辑放在**宿主脚本**里（monitor 容器内没有 docker CLI，也没挂 docker.sock）：
#   /vol1/1000/docker/etkn-monitor/scripts/etkn-patch-check.sh
# 脚本输出 JSON、退出码 0=正常 / 1=有问题。本模块只负责调用 + 推送。
# ⚠️ 面板走 EdgeOne CDN（源站 15s 超时）→ 自检必须异步，绝不能在请求线程里等 SSH。
_PATCH_SCRIPT = str(os.environ.get('PATCH_CHECK_SCRIPT') or '').strip()
_PATCH_INTERVAL = int(os.environ.get('PATCH_CHECK_INTERVAL', '21600'))   # 默认 6 小时
_patch_job = {'running': False, 'result': None, 'err': '', 'ts': 0.0,
              'started': 0.0, 'alerted': ''}
_patch_job_lock = threading.Lock()


def _patch_check_run(timeout: int = 120):
    """跑一次宿主上的补丁自检脚本。返回 (ok, dict|None, err)。"""
    if not _PATCH_SCRIPT:
        return False, None, '未配置 PATCH_CHECK_SCRIPT（见 .env.example）'
    pw = _host_password()
    if not pw:
        return False, None, '宿主凭据不可读（缺 SUDO_PASSWORD）'
    cmd = ("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
           "-o ConnectTimeout=8 -o LogLevel=ERROR %s@%s "
           "\"echo %s | sudo -S -p '' sh %s --json 2>/dev/null\"") % (
        shlex.quote(pw), shlex.quote(os.environ.get('SSH_PORT', '22')),
        shlex.quote(os.environ.get('SSH_USER', 'root')),
        shlex.quote(os.environ.get('SSH_HOST', '')),
        shlex.quote(pw), shlex.quote(_PATCH_SCRIPT))
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    except Exception as e:
        return False, None, str(e)[:120]
    out = (r.stdout or '').strip()
    for ln in reversed(out.splitlines()):
        ln = ln.strip()
        if ln.startswith('{'):
            try:
                return True, json.loads(ln), ''
            except Exception as e:
                return False, None, '解析自检输出失败：%s' % str(e)[:80]
    return False, None, ((out or r.stderr or '').strip().replace('\n', ' ')[:120] or '自检无输出')


def _patch_failed_keys(res) -> str:
    """失败项 key 排序拼接——告警去重用（失败集合不变就不重复推）。"""
    return '|'.join(sorted(str(c.get('key')) for c in (res or {}).get('checks') or []
                           if not c.get('ok')))


def _patch_alert_lines(res) -> list:
    """把自检结果里失败的项抽成告警正文行。"""
    lines = ['镜像 %s' % (str((res or {}).get('image_id') or '未知')[:19])]
    for c in (res or {}).get('checks') or []:
        if not c.get('ok'):
            lines.append('· ' + str(c.get('detail') or c.get('key') or ''))
    if len(lines) == 1:
        lines.append('· 自检未通过但未给出明细')
    return lines


def _patch_do(push: str = 'none') -> dict:
    """跑一次自检并更新状态。push='dedup' 失败集合变化才推；'always' 每次都推。"""
    ok, res, err = _patch_check_run()
    if not ok or res is None:
        with _patch_job_lock:
            _patch_job['running'] = False
            _patch_job['ts'] = time.time()
            _patch_job['err'] = err or '自检失败'
            return {'ok': False, 'err': _patch_job['err']}
    res['ts'] = _now().isoformat(timespec='seconds')
    key = _patch_failed_keys(res)
    with _patch_job_lock:
        _patch_job['running'] = False
        _patch_job['ts'] = time.time()
        _patch_job['result'] = res
        _patch_job['err'] = ''
        changed = (key != _patch_job['alerted'])
        _patch_job['alerted'] = '' if res.get('ok') else key
    if push != 'none' and (push == 'always' or changed) and SETTINGS.get('push_enabled'):
        if res.get('ok'):
            _alert_push('patch', 'ETKN 补丁自检通过',
                        ['compose mount 行 / 补丁 md5 / 容器内生效 / 上游一致性 全部正常'],
                        tcolor='green')
        else:
            _alert_push('patch', 'ETKN 补丁自检未通过（更新镜像后必查）',
                        _patch_alert_lines(res), tcolor='yellow')
    return res


def _patch_job_start(push: str = 'none') -> bool:
    """开一个后台自检任务。已有任务在跑就返回 False（去重，避免连点）。"""
    with _patch_job_lock:
        if _patch_job['running']:
            return False
        _patch_job['running'] = True
        _patch_job['started'] = time.time()

    def work():
        try:
            _patch_do(push)
        except Exception as e:                  # noqa: BLE001 —— 后台线程必须兜住
            with _patch_job_lock:
                _patch_job['running'] = False
                _patch_job['ts'] = time.time()
                _patch_job['err'] = '%s: %s' % (type(e).__name__, str(e)[:120])

    threading.Thread(target=work, daemon=True).start()
    return True


def _patch_job_state() -> dict:
    """当前自检状态（前端轮询用；纯内存读取，不触发 SSH）。"""
    with _patch_job_lock:
        d = {'running': _patch_job['running'], 'ok': None, 'err': _patch_job['err'],
             'result': _patch_job['result'], 'ts': _patch_job['ts']}
        if isinstance(_patch_job['result'], dict):
            d['ok'] = bool(_patch_job['result'].get('ok'))
        if _patch_job['started']:
            d['elapsed'] = round((time.time() - _patch_job['started']) if _patch_job['running']
                                 else max(0.0, _patch_job['ts'] - _patch_job['started']), 1)
        return d


def _patch_loop() -> None:
    """每 _PATCH_INTERVAL 跑一次自检（失败集合变化才推送）。"""
    time.sleep(300)                          # 启动 5 分钟后再跑，别和首轮采集抢
    while True:
        try:
            _patch_job_start('dedup')
        except Exception as e:
            print('%s [patch] 自检线程异常：%s %s'
                  % (_now().strftime('%H:%M:%S'), type(e).__name__, e), flush=True)
        time.sleep(_PATCH_INTERVAL)


def _noproxy_hit(host: str, patterns) -> bool:
    """host 是否命中 ETKN 的 no_proxy（精确 / 后缀 / *.后缀）。"""
    h = (host or '').lower().rstrip('.')
    for p in patterns or []:
        q = str(p).strip().lower().rstrip('.')
        if not q:
            continue
        if q.startswith('*.'):
            q = q[2:]
        if h == q or h.endswith('.' + q):
            return True
    return False


def _etkn_net_env(force: bool = False):
    """etkn 容器当前的代理口径。{'proxy':'host:port'|None,'noproxy':[...],'src':说明}。缓存 300s。"""
    now = time.time()
    if not force and _etkn_net_cache['ts'] and now - _etkn_net_cache['ts'] < 300:
        return _etkn_net_cache
    cname = os.environ.get('ETKN_CONTAINER', 'etkn')
    ok, out = _host_ssh("docker inspect %s --format '{{json .Config.Env}}'" % cname)
    proxy, noproxy, src = None, [], u'未取到 ETKN 代理（按直连）'
    if ok and out:
        try:
            d = {}
            for kv in json.loads(out.splitlines()[-1]):
                if isinstance(kv, str) and '=' in kv:
                    k, v = kv.split('=', 1)
                    d[k.strip().lower()] = v.strip()
            m = re.match(r'^[a-z0-9]+://([^/\s]+)', d.get('https_proxy') or d.get('http_proxy') or '', re.I)
            if m:
                proxy = m.group(1)
            noproxy = [x for x in re.split(r'[,\s]+', d.get('no_proxy', '')) if x]
            src = (u'跟随 ETKN 代理 ' + proxy) if proxy else u'ETKN 未设代理（按直连）'
        except Exception:
            src = u'解析 ETKN env 失败（按直连）'
    _etkn_net_cache.update({'ts': now, 'proxy': proxy, 'noproxy': noproxy, 'src': src})
    return _etkn_net_cache


# v2.9.2.7 ETKN「在用域名」取样。用户规则：
#   目标 ∈ ETKN 在用的域名  → 跟随 ETKN 口径（经 ETKN 的代理 CONNECT）
#   目标 ∉ ETKN 在用的域名  → 走软路由口径（不经 ETKN 代理），含 em 以后新加的目标
# 这个集合必须**动态取样**：用户改了 ETKN 的配置，em 自动跟着变，em 侧不改任何配置。
_etkn_deps_cache = {'ts': 0.0, 'hosts': set(),
                    'src': u'未取到 ETKN 在用域名（回退：一律跟随 ETKN）'}

# ETKN 配置里带 URL 的字段（GET /api/configuration/<key> 的 payload）
_ETKN_URL_SOURCES = (
    ('/api/configuration/tmdb', 'base_url'),
    ('/api/configuration/bangumi', 'base_url'),
    ('/api/configuration/fanart', 'base_url'),
    ('/api/configuration/p115', 'base_url'),
    ('/api/configuration/ai', 'ai_base_url'),
    ('/api/shared-pool/status', 'center_url'),
)
# v2.9.2.9：某些取样源还隐含「同模块的另一个域名」，配置里不体现，按源补上
#   fanart 模块在用 → 元数据走 webservice.fanart.tv，图片走 assets.fanart.tv（两张网）
_ETKN_SOURCE_EXTRA = {'/api/configuration/fanart': ('assets.fanart.tv',)}
# 隐含域名：ETKN 配置里只有开关、URL 写在 ETKN 代码里，配置查不到，只能内置映射
_ETKN_IMPLIED = {'tmdb': 'image.tmdb.org', 'fanart': 'assets.fanart.tv'}
# v2.9.2.9 硬编码域名：ETKN 代码里的常量 URL，配置里完全没有，只能内置
#   auth.example.com   = platform/entitlements.py 的 ETK_PRO_AUTH_URL（Pro 授权）
#   hdhive.example.com = integrations/re0.py 的 DEFAULT_RELAY_URL（re0 订阅中继）
_ETKN_HARDCODED = ('auth.example.com', 'hdhive.example.com')


def _pick(d, key):
    """取配置值：可能在顶层，也可能在 payload 里。

    实测（v2.9.2.7）：`/api/configuration/<key>` 把当前值放在 **payload** 内
    （如 payload.base_url），而 `/api/shared-pool/status` 的 center_url 在**顶层**。
    两种都得兼容，否则 tmdb/bangumi/fanart/p115 会全部漏掉。
    """
    if not isinstance(d, dict):
        return None
    v = d.get(key)
    if v not in (None, ''):
        return v
    p = d.get('payload')
    return (p or {}).get(key) if isinstance(p, dict) else None


def _url_host(v) -> str:
    """从 http(s)://host[:port]/path 取 host（小写、去端口与用户信息）。"""
    m = re.match(r'^[a-z][a-z0-9+.-]*://([^/\s?#]+)', str(v or ''), re.I)
    if not m:
        return ''
    return m.group(1).split('@')[-1].split(':')[0].strip().lower().rstrip('.')


def _etkn_deps(force: bool = False):
    """ETKN 当前在用的外部域名集合（从 ETKN 自己的 API 取样，缓存 300s）。

    取样失败时 hosts 为空 —— 调用方必须回退到 v2.9.2.2 的旧行为（一律跟随 ETKN），
    绝不能把空集合当成「ETKN 什么都不用」，否则会把全部目标误判成走软路由。
    总预算 12s：ETKN 挂掉时不能把测速拖死。
    """
    now = time.time()
    if not force and _etkn_deps_cache['ts'] and now - _etkn_deps_cache['ts'] < 300:
        return _etkn_deps_cache
    hosts, hit, miss = set(), 0, []
    deadline = time.time() + 12
    for path, key in _ETKN_URL_SOURCES:
        if time.time() > deadline:
            miss.append(u'超时')
            break
        try:
            st, d = api_get(path)
        except Exception:
            st, d = -1, {}
        if st == 200:
            hit += 1
            h = _url_host(_pick(d, key))
            if h:
                hosts.add(h)
                hosts.update(_ETKN_SOURCE_EXTRA.get(path, ()))  # v2.9.2.9
        else:
            miss.append(path.rsplit('/', 1)[-1])
    if time.time() <= deadline:
        try:      # 隐含：metadata.image_source → 图片域名
            st, md = api_get('/api/configuration/metadata')
            if st == 200:
                hit += 1
                mp = (md or {}).get('payload') or {}
                src = str(mp.get('image_source') or '')
                if _ETKN_IMPLIED.get(src):
                    hosts.add(_ETKN_IMPLIED[src])
                # v2.9.2.9：豆瓣评分域名硬编码在 ETKN 代码里，配置里只有开关
                if mp.get('douban_rating_enabled'):
                    hosts.add('frodo.douban.com')
        except Exception:
            pass
        try:      # 隐含：telegram_notifications 配了频道 = 在用 api.telegram.org
            st, tg = api_get('/api/configuration/telegram_notifications')
            if st == 200:
                hit += 1
                if ((tg or {}).get('payload') or {}).get('telegram_channel_id'):
                    hosts.add('api.telegram.org')
        except Exception:
            pass
        hosts.update(_ETKN_HARDCODED)   # v2.9.2.9：代码常量 URL，配置查不到
    if hit:
        src = u'ETKN 在用 %d 个域名' % len(hosts)
        if miss:
            src += u'（%s 取样失败）' % ','.join(sorted(set(miss)))
    else:
        src = u'未取到 ETKN 在用域名（回退：一律跟随 ETKN）'
    _etkn_deps_cache.update({'ts': now, 'hosts': hosts, 'src': src})
    return _etkn_deps_cache


_etkn_deps_lock = threading.Lock()
_etkn_deps_busy = {'on': False}


def _etkn_deps_refresh_bg():
    try:
        _etkn_deps(force=True)
    finally:
        _etkn_deps_busy['on'] = False


def _etkn_deps_async():
    """非阻塞取用：缓存新鲜就直接给；过期就踢一个后台线程去刷，本次仍返回旧值。

    必须有这一层 —— `/api/speed-history` 是**页面加载**时调用的，若在那里同步取样，
    ETKN 慢或挂掉会把首屏拖住最多 12s。冷启动时 hosts 为空，_speedtest_proxy 会
    安全回退到「一律跟随 ETKN」。
    """
    d = _etkn_deps_cache
    if time.time() - (d['ts'] or 0) < 300:
        return d
    with _etkn_deps_lock:
        if not _etkn_deps_busy['on']:
            _etkn_deps_busy['on'] = True
            threading.Thread(target=_etkn_deps_refresh_bg, daemon=True).start()
    return d


def _etkn_deps_public():
    """给接口用（set 不能直接 json.dumps）。"""
    d = _etkn_deps_async()
    return {'hosts': sorted(d['hosts']), 'src': d['src']}


def _speedtest_proxy(host: str, explicit):
    """定这一项走不走代理（v2.9.2.7 用户规则）：
       1) 显式配置优先（手动覆盖）
       2) ETKN 在用的域名 → 跟随 ETKN 代理（命中 ETKN no_proxy 则直连）
       3) 其余（ETKN 不用的，含 em 以后新加的）→ 直连，由软路由按自己的规则出
    取样失败（hosts 为空）时回退到 v2.9.2.2 行为：一律跟随 ETKN。"""
    if explicit:
        if str(explicit).strip().lower() == 'etkn':
            # v2.9.2.8 哨兵：按 ETKN 当前口径试算 —— ETKN 还没用这个域名时，
            # 也能拿到「如果切过去会怎样」的同口径数字（代理地址仍取自 ETKN 取样）
            env = _etkn_net_env()
            if env['proxy']:
                return env['proxy'], u'proxy·ETKN 试算'
            return None, u'direct·ETKN 未设代理'
        return explicit, u'proxy·指定'
    env = _etkn_net_env()
    deps = _etkn_deps_async()
    h = (host or '').lower().rstrip('.')
    if deps['hosts'] and h not in deps['hosts']:
        return None, u'direct·软路由'
    if not env['proxy']:
        return None, u'direct'
    if _noproxy_hit(host, env['noproxy']):
        return None, u'direct·ETKN no_proxy'
    return env['proxy'], u'proxy·ETKN'


# ---------- 手动测速（无定时；默认直连，proxy 指定域经代理 CONNECT 隧道） ----------
def speedtest_one(host: str, timeout: float = 10.0, proxy: str | None = None):
    r = {'host': host, 'ok': False, 'tcp_ms': None, 'connect_ms': None, 'tls_ms': None,
         'http_ms': None, 'proxy_ms': None,
         'total_ms': None, 'status': None, 'error': None, 'via': 'proxy' if proxy else 'direct'}
    t0 = time.perf_counter()
    try:
        if proxy:
            # 经代理 CONNECT 隧道：模拟 ETKN 业务真实路径（socket 全程代持，分阶段仍可计时）
            phost, pport = proxy.rsplit(':', 1)
            sock = socket.create_connection((phost, int(pport)), timeout=timeout)
            t1 = time.perf_counter()
            # v2.9.2.7：这一跳只是 em→本地代理（局域网，实测中位 0.115ms），不是目标建连，
            # 单列为 proxy_ms。tcp_ms 保持 None —— 代理口径下目标建连由代理侧完成、本地测不到，
            # UI 显示「—」。原来把它填进 tcp_ms，整列恒 0ms，误导成"所有目标建连都是零"。
            r['proxy_ms'] = round((t1 - t0) * 1000)
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
            # v2.9.2.6 阶段2：CONNECT 隧道建立。注意代理（sing-box）收到 CONNECT 就秒回
            # 200、此时尚未回源，所以这个值恒在 1ms 量级，不能当"延迟"看，故不再占用 tls_ms。
            r['connect_ms'] = round((t2 - t1) * 1000)
            tls = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
            t2b = time.perf_counter()
            # v2.9.2.6 阶段3：真目标 TLS 握手（与直连分支同口径，UI「握手」列读的就是它）
            r['tls_ms'] = round((t2b - t2) * 1000)
        else:
            sock = socket.create_connection((host, 443), timeout=timeout)
            t1 = time.perf_counter()
            r['tcp_ms'] = round((t1 - t0) * 1000)
            tls = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
            t2b = time.perf_counter()
            r['tls_ms'] = round((t2b - t1) * 1000)
        tls.sendall(f'GET / HTTP/1.1\r\nHost: {host}\r\n'
                    f'User-Agent: etkn-monitor\r\nConnection: close\r\n\r\n'.encode())
        chunk = tls.recv(256)
        t3 = time.perf_counter()
        r['http_ms'] = round((t3 - t2b) * 1000)  # v2.9.2.6 末段：GET 首字节（两路径同基准）
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
        # v2.8.24：分类统计扩 fail_tasks（该类失败/部分任务数）——与入库进度页失败明细
        # 同一 done 源（status∈failed/partial 计数），口径天然对应；重启后由持久化兜底
        e = by_kind_done.setdefault(k, {'tasks': 0, 'items': 0, 'ok_media': 0, 'bad_media': 0,
                                        'fail_tasks': 0})
        e['tasks'] += 1
        e['items'] += d['media']
        e['ok_media'] += d['ok_media']
        e['bad_media'] += d['bad_media']
        if d['status'] in ('failed', 'partial'):
            e['fail_tasks'] += 1
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
    # v2.8.23：今日新增未识别=带当日 processed_from 过滤的 total（与累计同源同可信度；
    # total=当日界内新增条数，与面板「今日完成」processed_from 口径一致）
    _s_ut, _b_ut = api_get(f'/api/p115/records?per_page=1&page=1&status=unrecognized'
                           f'&processed_from={urllib.parse.quote(today_prefix)}')
    _ur_today = (_b_ut or {}).get('total') or 0 if _s_ut == 200 else None
    # v2.9.27：实时「当前未识别」快照（持久化保护前）——与「累计新增」分开展示（用户 2026-10-04 定版 C）
    _ur_live = _ur_total
    # v2.8.22：异常值持久化消费——持久缓存（daily.json 今日键）1h 闸内优先；
    # 源头值异常跳水（< 持久值 50%）时保持久值（etkn 重启窗口不许洗掉异常数）
    _p_unrec = _dayweek_cache.get('unrec') or 0
    if _dw_fresh and _p_unrec > 0:
        if _ur_total is None or _ur_total < _p_unrec * 0.5:
            _ur_total = _p_unrec
    # v2.8.23：今日新增未识别同闸；跨日守卫——缓存日期非今日时持久值属昨日，不许压制今日真值
    _p_urtd = _dayweek_cache.get('unrec_today') or 0
    _same_day = _dayweek_cache.get('date') == today_prefix[:10]
    if _dw_fresh and _same_day and _p_urtd > 0:
        if _ur_today is None or _ur_today < _p_urtd * 0.5:
            _ur_today = _p_urtd
    _p_bad = _dayweek_cache.get('bad_tasks') or 0
    snap['records'] = {'success': _dayweek_cache['day'] if (_dw_ok and _dw_fresh) else None,
                       'unrecognized': _ur_total,          # 累计新增未识别（历史高水位，跨日保留）
                       'unrecognized_live': _ur_live,      # 当前未识别（实时真值，未过持久化保护）
                       'unrecognized_today': _ur_today,
                       'total': _dayweek_cache['day'] if (_dw_ok and _dw_fresh) else None}
    snap['week'] = {'media': _dayweek_cache['week'] if _dw_fresh else None}
    snap['failed_today'] = [
        {'id': d['id'], 'kind': d['kind'], 'wf': d['wf'], 'title': d['title'][:40],
         'media': d['media'], 'bad_media': d['bad_media'],
         'err': d.get('err') or '', 'stage': d.get('stage') or '',
         'finished_at': d['finished_at'][:19]}
        for d in done if d['status'] in ('failed', 'partial')][:50]
    # v2.8.22：今日失败/部分任务数持久闸——源头（_today_done 缓存）比持久值少 50%+
    # （etkn 重启窗口扫描不全）时保持久值；面板 done.bad_tasks=口径唯一来源。
    # v2.8.23：跨日守卫同 unrec_today——缓存日期非今日时昨日持久值不许压制今日真值
    _live_bad = len([d for d in done if d['status'] in ('failed', 'partial')])
    if _dw_fresh and _same_day and _p_bad > 0 and _live_bad < _p_bad * 0.5:
        _live_bad = _p_bad
    snap['done']['bad_tasks'] = _live_bad
    # v2.8.24：分类统计（by_kind）重启不丢——etkn 重启窗口 _today_done 缓存清空时，
    # live 扫描只能看到重启后完成的少量任务，各分类 fail_tasks 会归零；
    # 用持久今日键中的 by_kind_fail 做兜底取大（每类独立取 max，新完成任务正常累加）。
    # v2.8.25 修复：tbl 未定义（v2.8.24 误用 _dayweek_rebuild 局部变量名）导致
    # poll_once 首轮必炸 NameError、快照永驻预热旧值——改读持久载入函数。
    _persist_kind = {}
    if _same_day:
        try:
            _persist_kind = ((_dayweek_daily_load().get(today_prefix[:10]) or {}).get('by_kind_fail') or {})
        except Exception:
            _persist_kind = {}
    if isinstance(_persist_kind, dict) and _persist_kind:
        for _pk, _pv in _persist_kind.items():
            _pe = by_kind_done.setdefault(_pk, {'tasks': 0, 'items': 0, 'ok_media': 0,
                                                'bad_media': 0, 'fail_tasks': 0})
            if _pv > (_pe.get('fail_tasks') or 0):
                _pe['fail_tasks'] = _pv
    for _bk in (snap['done'].get('by_kind') or {}).values():
        _bk.setdefault('fail_tasks', 0)
    # v2.8.24：回填缓存——下轮 rebuild 落盘 daily.json 时带上分类失败数（否则首轮丢）
    _dayweek_cache['by_kind_fail'] = {k: v.get('fail_tasks', 0)
                                      for k, v in (snap['done'].get('by_kind') or {}).items()}
    # v2.8.22b：异常值回填缓存——poll 直读的 unrec 真值与 done 统计的 bad_tasks
    # 写回 _dayweek_cache，下轮 rebuild 落盘 daily.json 时带上（否则首轮落 0）。
    # v2.8.23：unrec_today 同回填；跨日翻转（缓存日期≠今日）时日内口径清零重计，
    # 防昨日值带进新一天（0 点翻转瞬间缓存尚未 rebuild 的空窗）。
    if _ur_total and _ur_total > (_dayweek_cache.get('unrec') or 0):
        _dayweek_cache['unrec'] = _ur_total
    if not _same_day:
        _dayweek_cache['bad_tasks'] = _live_bad   # 0 点翻转：昨日日内值清零重计
    elif _live_bad > (_dayweek_cache.get('bad_tasks') or 0):
        _dayweek_cache['bad_tasks'] = _live_bad
    if not _same_day:
        _dayweek_cache['unrec_today'] = 0   # 0 点翻转：昨日日内值清零
    if _ur_today and _ur_today > (_dayweek_cache.get('unrec_today') or 0):
        _dayweek_cache['unrec_today'] = _ur_today
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
            # v2.8.21 闸3（快照闸）：今日值比上一份落盘快照掉 50%+ 且非跨日翻转 →
            # 疑似空窗口污染，跳过落盘（旧快照保命，预热链路不被污染）。
            try:
                _old_snap = _snapshot_cache_load()
                _old_day = ((_old_snap or {}).get('records') or {}).get('success') or 0
                _new_day = (snap.get('records') or {}).get('success') or 0
                _same_day = (_old_snap or {}).get('ts', '')[:10] == snap.get('ts', '')[:10]
                _old_ur = ((_old_snap or {}).get('records') or {}).get('unrecognized') or 0
                _new_ur = (snap.get('records') or {}).get('unrecognized') or 0
                _old_bt = ((_old_snap or {}).get('done') or {}).get('bad_tasks') or 0
                _new_bt = (snap.get('done') or {}).get('bad_tasks') or 0
                # v2.8.23：今日新增未识别纳入快照闸
                _old_ut = ((_old_snap or {}).get('records') or {}).get('unrecognized_today') or 0
                _new_ut = (snap.get('records') or {}).get('unrecognized_today') or 0
                _dive = ((_same_day and _old_day > 0 and _new_day < _old_day * 0.5)
                         or (_same_day and _old_ur > 0 and _new_ur < _old_ur * 0.5)
                         or (_same_day and _old_bt > 0 and _new_bt < _old_bt * 0.5)
                         or (_same_day and _old_ut > 0 and _new_ut < _old_ut * 0.5))
                if _dive:
                    print('统计异常跳水（今日/未识别/失败任务/今日未识别），快照不落盘（防污染）'
                          ' %s→%s / %s→%s / %s→%s / %s→%s' % (_old_day, _new_day, _old_ur, _new_ur,
                                                              _old_bt, _new_bt, _old_ut, _new_ut), flush=True)
                else:
                    _snapshot_cache_save(snap)
            except Exception:
                _snapshot_cache_save(snap)
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

# v2.9.10 任务中心：子任务确认页（企微图文消息里点开的落地页）。
# 刻意做成「先确认再执行」而不是点开即跑——微信/企微打开链接会预取，
# 点开即跑等于打开一条消息就把整类任务全触发。
_RUN_PAGE = """<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} · 执行</title><style>
body{{font-family:system-ui,-apple-system,'PingFang SC','Microsoft YaHei',sans-serif;
background:#0f1420;color:#e8ecf3;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}}
.card{{background:#171e2e;border:1px solid #2a3450;border-radius:14px;padding:28px 30px;max-width:420px;width:92%}}
h1{{font-size:19px;margin:0 0 6px}} .sub{{color:#8b96ad;font-size:13px;margin-bottom:14px}}
.tag{{display:inline-block;background:#1d2740;border:1px solid #33436b;border-radius:8px;
padding:2px 8px;font-size:12px;color:#9fb3d9;margin-bottom:12px}}
#msg{{margin-top:6px;font-size:15px;min-height:20px}}
.ok{{color:#3fb950}} .bad{{color:#f85149}} .wait{{color:#9fb3d9}}</style></head><body>
<div class="card"><div class="tag">{cat}</div>
<h1>{title}</h1><div class="sub">{desc}</div>
<div id="msg" class="wait">正在下发…</div></div>
<script>
/* v2.9.32：点开即下发（不再要二次确认）。用 JS 自动 POST 而不是 GET 触发——
   微信/企微会预取链接，预取器不执行 JS，所以不会「一收到消息就跑光整类任务」。 */
(async function(){{
  const m=document.getElementById('msg');
  try{{
    const r=await fetch(location.pathname,{{method:'POST',
      headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{token:new URLSearchParams(location.search).get('token')||''}})}});
    const d=await r.json().catch(()=>({{}}));
    if(r.ok&&d.ok){{m.textContent='✅ '+d.msg;m.className='ok';}}
    else{{m.textContent='❌ '+(d.error||d.msg||('失败 '+r.status));m.className='bad';}}
  }}catch(e){{m.textContent='❌ '+e;m.className='bad';}}
}})();
</script></body></html>"""

_RUN_PAGE_BAD = """<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>链接已失效</title>
<style>body{font-family:system-ui,sans-serif;background:#0f1420;color:#e8ecf3;display:flex;
min-height:100vh;align-items:center;justify-content:center;margin:0}
.c{text-align:center;color:#8b96ad} b{color:#f85149;font-size:17px}</style></head><body>
<div class="c"><b>链接令牌无效</b><div style="margin-top:8px">请回到「任务中心」重新点分类，<br>用最新一条图文消息里的子任务</div></div>
</body></html>"""


def _find_cat_of(task_key: str):
    """返回该任务所属分类的 (icon+label, task_tuple)，找不到返回 (None, None)。"""
    for c in TASK_CATALOG:
        for t in c['tasks']:
            if t[0] == task_key:
                return '%s %s' % (c['icon'], c['label']), t
    return None, None

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
        u = urllib.parse.urlparse(self.path)
        p = u.path
        if p == '/wecom/callback':        # v2.9.5 企微回调 URL 校验（GET 回明文 echostr）
            code, body, ctype = _wecom_callback_verify(urllib.parse.parse_qs(u.query))
            return self._send(code, body.encode('utf-8'), ctype)
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
            out['notify'] = _notify_state()          # v2.9.6：通知渠道状态（面板首页展示）
            return self._send(200, json.dumps(out, ensure_ascii=False).encode())
        if p == '/api/meta':
            return self._send(200, json.dumps({
                'base_url': BASE, 'eta_window_min': ETA_WINDOW_MIN,
                'poll_interval': FAST_INTERVAL, 'slow_poll_interval': POLL_INTERVAL,
                'version': 'v3.12.0', 'readonly': False,
                'etkn_site': ETKN_SITE_URL or BASE,
                'actions': ['speedtest', 'retry-failed', 'run-organize-p115',
                            'run-generate-covers', 'purge-register-queued', 'bad-media',
                            'settings', 'test-push', 'check-500-now', 'speed-now',
                            'trigger-organize', 'wecom-test', 'wecom-menu',
                            # v2.9.8 任务中心（白名单见 ETKN_TASK_WHITELIST）
                            # v2.9.10 分类目录 + 子任务确认页（/run-task/<key>）
                            'run-task', 'task-catalog', 'task-catalog-refresh',
                            'run-task-page', 'relay-check',
                            # v2.9.22 补丁自检
                            'patch-check'],
            }, ensure_ascii=False).encode())
        if p == '/api/bad-media':
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            date = (q.get('date') or [''])[0]
            today_prefix = _today00().date().isoformat() if not date else date
            items, total = [], 0
            _ur_today_cnt = 0
            for st in ('unrecognized', 'failed'):
                s, b = api_get(f'/api/p115/records?page=1&per_page=50&status={st}'
                               f'&processed_from={urllib.parse.quote(today_prefix)}')
                if s != 200 or not isinstance(b, dict):
                    continue
                total += (b.get('total') or 0) if st == 'unrecognized' else 0
                if st == 'unrecognized':
                    _ur_today_cnt = b.get('total') or 0
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
                                               'unrecognized_today': _ur_today_cnt,
                                               'items': items[:60]}, ensure_ascii=False).encode())
        if p == '/api/settings':
            return self._do_settings_get()
        if p == '/api/push-history':
            return self._send(200, json.dumps({'items': list(_push_hist)},
                                              ensure_ascii=False).encode())
        if p == '/api/patch-check':
            # v2.9.22 ETKN bind-mount 补丁自检：GET 只读内存状态（异步任务见 _patch_loop）
            return self._send(200, json.dumps(_patch_job_state(),
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
            return self._send(200, json.dumps({'items': list(_speed_hist),
                                               'etkn_net': _etkn_net_env(),
                                               'etkn_deps': _etkn_deps_public()},
                                              ensure_ascii=False).encode())
        if p == '/api/daily-trend':       # v3.12.0：近 N 天趋势（读 data/daily.json；今日用实时值覆盖）
            try:
                _n = int(urllib.parse.parse_qs(u.query).get('days', ['14'])[0])
            except Exception:
                _n = 14
            _n = max(3, min(90, _n))
            _tbl = _dayweek_daily_load()
            _today = _today_prefix()[:10]
            # 今日一律以实时快照为准（与首屏「今日完成/异常」严格同源，不受落盘时序影响）
            _snap = (_state.get('snapshot') or {})
            _rec = (_snap.get('records') or {})
            _fut = {}
            if _rec.get('success') is not None:
                _fut['succ'] = _rec.get('success')
            if _rec.get('unrecognized_today') is not None:
                _fut['unrec_today'] = _rec.get('unrecognized_today')
            _dbt = (_snap.get('done') or {}).get('bad_tasks')
            if _dbt is not None:
                _fut['bad_tasks'] = _dbt
            _days = []
            for _d in sorted(_tbl.keys())[-_n:]:
                _v = _tbl.get(_d) or {}
                _it = {'date': _d, 'succ': _v.get('succ') or 0,
                       'unrec_today': _v.get('unrec_today') or 0,
                       'bad_tasks': _v.get('bad_tasks') or 0}
                if _d == _today:
                    _it.update(_fut)
                _days.append(_it)
            return self._send(200, json.dumps({'days': _days, 'today': _today},
                                              ensure_ascii=False).encode())
        if p == '/api/task-catalog':        # v2.9.10：面板「任务中心」与企微菜单同源
            return self._send(200, json.dumps(
                {'source': dict(TASK_CATALOG_SOURCE),   # v2.9.30：来自 etkn/缓存/静态
                 'categories': [{'key': c['key'], 'label': c['label'], 'icon': c['icon'],
                                 'tasks': [{'key': t[0], 'title': t[1], 'desc': t[2]}
                                           for t in c['tasks']]} for c in TASK_CATALOG]},
                ensure_ascii=False).encode())
        if p == '/api/task-catalog-refresh':    # v2.9.30：手动对齐一次（不改变「不定时」的约定）
            _ok = refresh_task_catalog_cache(force=True)
            _menu_update_task_buttons()
            return self._send(200, json.dumps(
                {'ok': _ok, 'source': dict(TASK_CATALOG_SOURCE),
                 'categories': len(TASK_CATALOG), 'tasks': len(ETKN_TASK_WHITELIST)},
                ensure_ascii=False).encode())
        m = re.match(r'^/run-task/([a-z0-9][a-z0-9\-]*)$', p)
        if m:                               # v2.9.10：企微图文消息里点子任务的确认页
            tkey = m.group(1)
            tok_q = (urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                     .get('token') or [''])[0]
            if not (tok_q and tok_q == _get_run_token()):
                return self._send(403, _RUN_PAGE_BAD.encode(), 'text/html; charset=utf-8')
            cat, t = _find_cat_of(tkey)
            if not t:
                return self._send(404, _RUN_PAGE_BAD.encode(), 'text/html; charset=utf-8')
            return self._send(200, _RUN_PAGE.format(
                title=t[1], desc=t[2], cat=cat).encode(), 'text/html; charset=utf-8')
        if p == '/api/relay-check':       # v2.9.21 异步自检：GET 取结果（前端轮询）
            return self._send(200, json.dumps(relay_job_state(), ensure_ascii=False).encode())
        return self._send(404, '{"error":"not found"}'.encode())

    def _body(self):
        try:
            n = int(self.headers.get('Content-Length') or 0)
            return json.loads(self.rfile.read(n).decode('utf-8') or '{}') if n else {}
        except Exception:
            return {}

    def _do_settings_get(self, extra=None):
        d = dict(SETTINGS)
        if d.get('webhook_url'):        # 脱敏展示：协议+域名+尾部4位
            m = re.match(r'^(https?://[^/]+/)(.*)$', d['webhook_url'])
            d['webhook_masked'] = (m.group(1) + '***' + m.group(2)[-4:]) if m else '***'
        else:
            d['webhook_masked'] = ''
        d.pop('cd2_pass', None)         # v2.8：CD2 密码永不回传前端（留空=不修改）
        d['router_pass_set'] = bool(d.get('router_pass'))  # v2.8.10：掩码态回传
        d.pop('router_pass', None)      # v2.8.10：路由器 SSH 密码同样不回传
        # v2.9.5 企微三件套（Secret / 回调 Token / EncodingAESKey）永不回传前端，
        # 只回「是否已配置」布尔位；前端对应输入框留空 = 不修改（照抄 cd2_pass 模式）。
        for _k in ('wecom_secret', 'wecom_token', 'wecom_aeskey'):
            d[_k + '_set'] = bool(d.get(_k))
            d.pop(_k, None)
        # v2.9.20 中转池两套凭据（管理员密码 / /v1 调用密钥）同样只回布尔位，留空=不修改
        for _k in ('relay_admin_pass', 'relay_api_key'):
            d[_k + '_set'] = bool(d.get(_k))
            d.pop(_k, None)
        d.pop('wecom_panel_url', None)   # v2.9.8：该设置已移除（旧 settings.json 里的残留不回传）
        d.pop('run_token', None)         # v2.9.32：子任务链接令牌是触发凭据，永不回传前端
        d['wecom_last'] = dict(_wecom_last)      # 最近一次企微发送结果（设置页回显）
        d['wecom_menu_last'] = dict(_wecom_menu_last)   # v2.9.7 最近一次菜单下发结果
        d['wecom_callback_path'] = '/wecom/callback'
        d['settings_path'] = SETTINGS_PATH
        if extra:
            d.update(extra)
        return self._send(200, json.dumps(d, ensure_ascii=False).encode())

    def _do_settings_post(self):
        b = self._body()
        if not isinstance(b, dict):
            return self._send(400, '{"error":"bad body"}'.encode())
        if 'webhook_url' in b and isinstance(b['webhook_url'], str):
            SETTINGS['webhook_url'] = b['webhook_url'].strip()
        for k in ('push_enabled', 'feishu_enabled', 'alert_500_enabled', 'alert_speed_enabled',
                  'alert_backlog_enabled',
                  'alert_stall_enabled', 'stall_grace_enabled', 'alert_finish_enabled',
                  'trigger_enabled', 'feed_enabled', 'auto_restart_enabled', 'hosts_enabled',
                  'wecom_enabled'):
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
        # v2.9.5 企业微信：非密字段直写；三件套「空值或掩码 = 保留原值」
        # （否则「不动表单直接保存」会把已存好的 Secret/Token/AESKey 覆盖成掩码串）
        _w_before = (SETTINGS.get('wecom_corpid'), SETTINGS.get('wecom_secret'))
        for _k in ('wecom_corpid', 'wecom_agentid', 'wecom_touser', 'wecom_api_proxy'):
            if _k in b and isinstance(b[_k], str):
                SETTINGS[_k] = b[_k].strip()
        for _k in ('wecom_secret', 'wecom_token', 'wecom_aeskey'):
            _v = b.get(_k)
            if isinstance(_v, str) and _v.strip() and '***' not in _v:
                SETTINGS[_k] = _v.strip()
        if (SETTINGS.get('wecom_corpid'), SETTINGS.get('wecom_secret')) != _w_before:
            _wecom_tok['v'] = ''        # 换了企业/应用 → 缓存的 access_token 立即作废
        SETTINGS.pop('wecom_panel_url', None)   # v2.9.8：设置已移除，顺手清掉旧残留
        # v2.9.20 中转池（可选）：开关/地址/模型/门槛直写；两套凭据「空或掩码=保留原值」
        if 'relay_enabled' in b:
            SETTINGS['relay_enabled'] = bool(b['relay_enabled'])
        for _k in ('relay_base_url', 'relay_model'):
            if _k in b and isinstance(b[_k], str):
                SETTINGS[_k] = b[_k].strip()
        if 'relay_min_quota' in b:
            try:
                SETTINGS['relay_min_quota'] = max(0, min(100, int(b['relay_min_quota'])))
            except (TypeError, ValueError):
                pass
        for _k in ('relay_admin_pass', 'relay_api_key'):
            _v = b.get(_k)
            if isinstance(_v, str) and _v.strip() and '***' not in _v:
                SETTINGS[_k] = _v.strip()
        # v2.9.15：「🔗快捷入口」的 URL 直接来自 card_links，链接变了菜单必须重下发。
        # 旧版（v2.9.8）菜单里没有动态 URL，所以没有这套逻辑；现在按指纹判断，没变就不打扰企微。
        _need_menu = (SETTINGS.get('wecom_enabled')
                      and _menu_links_sig() != _wecom_menu_sig['v'])
        try:
            settings_save()
        except Exception as e:
            return self._send(500, json.dumps({'error': f'保存失败：{e}'[:120]},
                                              ensure_ascii=False).encode())
        if _need_menu:
            threading.Thread(target=wecom_menu_apply, daemon=True).start()
        return self._do_settings_get()

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        p = u.path
        if p == '/wecom/callback':        # v2.9.5 企微回调（用户消息 / 菜单点击事件）
            _n = int(self.headers.get('Content-Length') or 0)
            _raw = self.rfile.read(_n).decode('utf-8', 'replace') if _n else ''
            code, body = _wecom_callback_msg(_raw, urllib.parse.parse_qs(u.query))
            return self._send(code, body)
        if p == '/api/settings':
            return self._do_settings_post()
        if p == '/api/test-push':
            text = _alert_text('测试推送', ['设置页手动触发 · 验证推送链路',
                                    '飞书：收到本条说明 Webhook 可达',
                                    '企微：收到本条说明应用消息可达',
                                    'v2.6：飞书侧为交互卡片，按钮可在内置浏览器打开'])
            ok, err = push_both(text, buttons=_card_buttons(),
                                title='✅ 测试推送', tcolor='blue')
            record_push('test', text, ok, err)
            return self._send(200, json.dumps({'ok': ok, 'err': err},
                                              ensure_ascii=False).encode())
        if p == '/api/wecom-menu':        # v2.9.5：一键下发/覆盖自定义菜单
            ok, msg = wecom_menu_apply()
            return self._send(200, json.dumps({'ok': ok, 'msg': msg,
                                               'menu_last': dict(_wecom_menu_last)},
                                              ensure_ascii=False).encode())
        if p == '/api/wecom-test':        # v2.9.5：只测企微这一路（不惊动飞书）
            c = _wecom_cfg()
            tok, terr = wecom_token()
            if not tok:
                return self._send(200, json.dumps({'ok': False, 'token': False, 'err': terr},
                                                  ensure_ascii=False).encode())
            ok, err = wecom_push(_alert_text('企微通道自检',
                                             ['本条由设置页「测试企业微信」触发',
                                              '收到说明 gettoken + message/send 全通'],
                                             icon='ℹ️'),
                                 title='ℹ️ 企业微信通道自检',
                                 buttons=_card_buttons())
            return self._send(200, json.dumps({'ok': ok, 'token': True, 'agentid': c['agentid'],
                                               'touser': c['touser'], 'err': err,
                                               'last': dict(_wecom_last)},
                                              ensure_ascii=False).encode())
        if p == '/api/relay-check':       # v2.9.21 中转池自检：**只负责开跑，立刻返回**
            # 面板走 EdgeOne CDN（源站 15s 超时），同步等一个 ~3s 的请求会偶发 524
            # （源站实测稳定 2.5~3.2s，但经隧道抖动可达 13.6s）。结果由前端轮询 GET 取。
            body = self._body()
            heal = not (isinstance(body, dict) and body.get('heal') is False)
            started = relay_job_start(heal=heal)
            d = relay_job_state()
            d['started'] = started        # False = 已有任务在跑（连点去重）
            return self._send(200, json.dumps(d, ensure_ascii=False).encode())
        if p == '/api/patch-check':
            # v2.9.22 面板按钮：开一个后台自检并推卡片，立刻返回（面板走 CDN，不能同步等 SSH）
            started = _patch_job_start('always')
            d = _patch_job_state()
            d['started'] = started
            return self._send(200, json.dumps(d, ensure_ascii=False).encode())
        if p == '/api/hermes/switch':
            b = self._body()
            mod = str((b or {}).get('model') or '').strip()
            if not mod:
                return self._send(400, json.dumps({'ok': False, 'err': 'missing model'}).encode())
            pw = _host_password()
            cmd = ("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
                   "-o ConnectTimeout=8 -o LogLevel=ERROR %s@%s "
                   "\"echo %s | sudo -S -p '' python3 -c 'import yaml;p=\\\"/root/.hermes/config.yaml\\\";"
                   "d=yaml.safe_load(open(p));d[\\\"model\\\"][\\\"default\\\"]=\\\"%s\\\";"
                   "yaml.dump(d,open(p,\\\"w\\\"),allow_unicode=True,sort_keys=False)' "
                   "&& nohup sh -c 'echo %s | sudo -S -p \'\' systemctl restart hermes-gateway' >/dev/null 2>&1 &\"") % (
                shlex.quote(pw), shlex.quote(os.environ.get('SSH_PORT', '22')),
                shlex.quote(os.environ.get('SSH_USER', 'root')),
                shlex.quote(os.environ.get('SSH_HOST', '')),
                shlex.quote(pw), mod, shlex.quote(pw))
            r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=25)
            return self._send(200, json.dumps({'ok': r.returncode == 0, 'err': (r.stderr or '').strip()[:120]}).encode())
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
                {'ts': _now().isoformat(timespec='seconds'), 'results': results,
                 'etkn_net': _etkn_net_env(),
                 'etkn_deps': _etkn_deps_public()},
                ensure_ascii=False).encode())
        if p == '/api/speedtest':
            results = run_speed_round(alert=False)
            return self._send(200, json.dumps(
                {'ts': _now().isoformat(timespec='seconds'), 'results': results,
                 'etkn_net': _etkn_net_env(),
                 'etkn_deps': _etkn_deps_public()},
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
        m = re.match(r'^/run-task/([a-z0-9][a-z0-9\-]*)$', p)
        if m:                                 # v2.9.10 任务中心：确认页点「确认执行」
            tkey = m.group(1)
            b = self._body()
            if str(b.get('token') or '') != _get_run_token():
                return self._send(403, json.dumps(
                    {'ok': False, 'error': '令牌无效，请回任务中心重新点分类'},
                    ensure_ascii=False).encode())
            ok, msg, s, b2 = etkn_run_task(tkey)
            if ok and tkey == 'organize-p115':      # 与面板/菜单一致：整理任务挂空转监视
                try:
                    _watch_shell_run(int(b2['workflow_run_id']), 'panel')
                except Exception:
                    pass
            # v2.9.33：把结果**推回微信**——用户从微信消息点进来，只看到网页反馈会以为「没反应」
            # （实际踩到：任务已成功下发到 ETKN，但微信应用里一条回执都没有）。
            try:
                _tt = ETKN_TASK_WHITELIST.get(tkey) or tkey
                _cat, _tup = _find_cat_of(tkey)
                _who = ('%s · ' % _cat) if _cat else ''
                if ok:
                    wecom_push('✅ 已下发：%s%s\n%s' % (_who, _tt, msg))
                else:
                    wecom_push('⚠️ 下发失败：%s%s\n%s' % (_who, _tt, msg))
            except Exception as _e:
                print('任务回执推送失败：%s' % str(_e)[:120], flush=True)
            return self._send(200 if ok else (s if s > 0 else 502), json.dumps(
                {'ok': ok, 'msg': msg, 'task': tkey, 'etkn_status': s, 'etkn_body': b2},
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
        # v2.9.8 通用任务触发：/api/run-task/<task_key>（白名单内，见 ETKN_TASK_WHITELIST）。
        # 取代原先「一个任务一个 if」的写法；organize-p115 保留上面那条（它要带专属参数 +
        # 空转监视），其余任务走服务端默认参数。
        m = re.match(r'^/api/run-task/([a-z0-9][a-z0-9\-]*)$', p)
        if m:
            tkey = m.group(1)
            if tkey == 'organize-p115':
                pass          # 已在上面的专属分支处理（不会走到这里）
            elif tkey not in ETKN_TASK_WHITELIST:
                return self._send(404, json.dumps(
                    {'ok': False, 'error': '任务不在白名单内：%s' % tkey,
                     'allowed': sorted(ETKN_TASK_WHITELIST.keys())},
                    ensure_ascii=False).encode())
            else:
                ok, msg, s, b = etkn_run_task(tkey)
                return self._send(s if s > 0 else 502, json.dumps(
                    {'ok': ok, 'msg': msg, 'task': tkey,
                     'task_title': ETKN_TASK_WHITELIST.get(tkey, ''),
                     'etkn_status': s, 'etkn_body': b}, ensure_ascii=False).encode())
        if p == '/api/purge-register-queued':
            limit = int(urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                        .get('limit', ['500'])[0])
            return self._send(200, json.dumps(_purge_register_queued(limit),
                                              ensure_ascii=False).encode())
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
    # v2.8.21 闸2（预热闸）：快照今日值若明显低于 daily.json 持久键（etkn 重启窗口期
    # 污染的快照，9/22 实锤 0 vs 4397），预热以持久键为准——重启后直接显示正确值。
    if snap and tbl.get(today_d):
        _persist = tbl[today_d]
        _p_succ = _persist.get('succ', 0) if isinstance(_persist, dict) else (_persist or 0)
        _snap_day = ((snap.get('records') or {}).get('success')) or 0
        if _snap_day < _p_succ:
            snap['records']['success'] = _p_succ
            snap['records']['total'] = _p_succ
            snap['stale_note'] = '重启恢复中（以持久统计为准）'
        # v2.8.22 闸2 扩展：异常值同保——快照 unrec/bad_tasks 低于持久键→以持久键为准。
        # v2.8.23：unrec_today 同保；快照持久键跨日守卫（持久键日期非今日则跳过日内口径压制）
        if isinstance(_persist, dict):
            _persist_is_today = True   # tbl[today_d] 即今日键，天生日效，无跨日风险
            _sr = snap.setdefault('records', {})
            if ((_sr.get('unrecognized') or 0) or 0) < (_persist.get('unrec') or 0):
                _sr['unrecognized'] = _persist.get('unrec') or 0
            _sd = snap.setdefault('done', {})
            if ((_sd.get('bad_tasks') or 0) or 0) < (_persist.get('bad_tasks') or 0):
                _sd['bad_tasks'] = _persist.get('bad_tasks') or 0
            if _persist_is_today and ((_sr.get('unrecognized_today') or 0) or 0) < (_persist.get('unrec_today') or 0):
                _sr['unrecognized_today'] = _persist.get('unrec_today') or 0
            # v2.8.24：分类失败数预热兜底（每类独立取 max，与快照 live 值融合）
            _persist_bkf = _persist.get('by_kind_fail') or {}
            if _persist_is_today and isinstance(_persist_bkf, dict) and _persist_bkf:
                _sd_by = _sd.setdefault('by_kind', {})
                for _pk, _pv in _persist_bkf.items():
                    _pe = _sd_by.setdefault(_pk, {'tasks': 0, 'items': 0, 'ok_media': 0,
                                                  'bad_media': 0, 'fail_tasks': 0})
                    if _pv > (_pe.get('fail_tasks') or 0):
                        _pe['fail_tasks'] = _pv
    # v2.9.2.13：今日/本周不再按 daily.json 逐日求和预热——口径已换成整理记录，
    # 而 daily.json 里的历史键是旧口径残留值，求和会先显示一个错的周值。
    # 直接同步跑一轮 rebuild（2 个轻量请求，0.2s 级）拿到正确值。
    _dayweek_rebuild()
    print('预热完成：快照 ts=%s，今日=%s，本周=%s' % (snap.get('ts', '无'),
          _dayweek_cache['day'], _dayweek_cache['week']), flush=True)


def main():
    if not PASSWORD:
        raise SystemExit('缺少环境变量 ETKN_PASSWORD（只存环境，不落盘）')
    settings_load()   # v2.8.26：主线程先加载设置再拉起各线程。原只在 alarm_loop 首行加载，
                      # 与 _hosts_loop 存在启动竞速——本轮重启实测 hosts 线程抢跑读到默认值
                      # hosts_enabled=False，首轮巡检被静默跳过睡 3600 秒（幸被新增日志暴露）。
                      # 各后台线程从此一律读到已加载设置（poll/fast 同享此修复）。
    # v2.8.25 排障：每 180 秒转储一次全线程栈到日志（定位 poll_loop 阻塞点用；
    # 输出量小，仅 7 线程；确认根因后可移除）
    import faulthandler, sys
    faulthandler.dump_traceback_later(180, repeat=True, file=sys.stderr)
    _warmup_from_cache()
    # v2.9.30：EM 启动时对齐一次 ETKN 任务目录（用户明确要求：只随启动跟随，不做定时跟随）。
    # 放在各轮询线程 / HTTP 服务之前完成——启动即带最新目录，面板与企微菜单三处同步。
    # 拉不到 ETKN 时回退上次落盘缓存，再不行沿用静态白名单（绝不因跟随失败而起不来）。
    try:
        refresh_task_catalog_cache()
        _menu_update_task_buttons()
    except Exception as e:
        print('启动跟随任务目录失败：%s' % str(e)[:140], flush=True)
    threading.Thread(target=poll_loop, daemon=True).start()
    threading.Thread(target=fast_loop, daemon=True).start()
    threading.Thread(target=alarm_loop, daemon=True).start()
    threading.Thread(target=_hosts_loop, daemon=True).start()   # v2.8 hosts 每小时巡检
    threading.Thread(target=_dayweek_loop, daemon=True).start()  # v2.8.15c 今日/本周后台重建
    threading.Thread(target=_today_done_loop, daemon=True).start()  # v2.8.20b 今日明细重线程
    threading.Thread(target=_feed_loop, daemon=True).start()   # v2.9.3 喂料兜底轮询（修死锁）
    threading.Thread(target=_patch_loop, daemon=True).start()   # v2.9.22 ETKN bind-mount 补丁自检（6h）
    port = int(os.environ.get('MONITOR_PORT', '8620'))
    srv = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    print(f'etkn-monitor v3.12.0，端口 {port}，快轮询 {FAST_INTERVAL}s（活跃队列）/'
          f'慢轮询 {POLL_INTERVAL}s（全量），ETA 窗口 {ETA_WINDOW_MIN}min，'
          f'喂料=CD2 WebDAV 通道（兜底轮询 {FEED_FALLBACK_INTERVAL}s），hosts 巡检=每小时',
          flush=True)
    srv.serve_forever()


if __name__ == '__main__':
    main()
