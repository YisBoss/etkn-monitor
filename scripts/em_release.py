#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""etkn-monitor 统一发版（v3.18.3）

为什么有它：以前发版靠临时敲 git 命令，**只推了代码和 tag、没建 GitHub Release**，
于是 release 页面长期停在旧版本（用户发现时已落后 30 个版本）。本脚本把
「提交 → 推 main → 打 tag → 建 Release → 补历史缺失的 Release」收在一条命令里，
并在推送前跑一道**隐私闸门**（开源仓库绝不能带部署者的私人域名/IP/密钥）。

用法：
  python3 scripts/em_release.py v3.19.0 --note-file .msg.txt
  python3 scripts/em_release.py v3.19.0 --note "一句话改动说明"
  python3 scripts/em_release.py --sync            # 只补齐缺失的 Release，不发新版

退出码非 0 = 未完成，可直接看 stderr。
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

REPO = 'YisBoss/etkn-monitor'
PAT_FILE = os.path.expanduser('~/.hermes/workspace/.creds/github_etkn_monitor.txt')
# 走代理才需要（直连 GitHub 畅通就留空）。默认不设，避免把任何一个部署者的内网地址写进仓库。
PROXY = os.environ.get('EM_RELEASE_PROXY', '')

# ---------- 隐私闸门：命中即中止（开源口径） ----------
PRIVACY_PATTERNS = [
    # 只拦「非文档占位段」的私网地址：192.168.1.x 是本文档统一的占位网段，放行。
    (r'\b(?:192\.168\.(?!1\.)\d{1,3}\.\d{1,3}'
     r'|10\.\d{1,3}\.\d{1,3}\.\d{1,3}'
     r'|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b',
     '局域网 IP（文档占位请用 192.168.1.x）'),
    (r'gh[ps]_[A-Za-z0-9_]{20,}', 'GitHub 令牌'),
    (r'github_pat_[A-Za-z0-9_]{20,}', 'GitHub 令牌'),
    (r'sk-[A-Za-z0-9]{20,}', 'API 密钥'),
    (r'ark-[A-Za-z0-9-]{20,}', 'API 密钥'),
    (r'open\.feishu\.cn/open-apis/bot/v2/hook/[\w-]{10,}', '飞书 Webhook'),
    (r'\bww[a-f0-9]{12,}\b', '企业微信企业ID'),
]
# 文档示例网段/域名，允许出现
ALLOW = ('example.com', 'your_', '<你的')


# 「部署者自己的」域名与公网 IP 放在**仓库之外**的本地清单里（每行一条正则，'#' 开头为注释）。
# 为什么不写在本文件：把私人域名当检测规则写进脚本，脚本一进公开仓库就等于泄露它自己。
# 文件不存在时只管通用模式（私网段/IP 形态/密钥形态/Webhook 形态），照样能拦住绝大多数泄露。
DENYLIST_FILE = os.path.expanduser('~/.hermes/workspace/.creds/em_privacy_denylist.txt')


def _owner_patterns():
    out = []
    try:
        for line in open(DENYLIST_FILE, encoding='utf-8'):
            line = line.strip()
            if line and not line.startswith('#'):
                out.append((line, '部署者的私有信息（本地清单）'))
    except FileNotFoundError:
        pass
    return out


def sh(args, cwd=None, env=None, check=False):
    r = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit('命令失败: %s\n%s' % (' '.join(args), (r.stderr or r.stdout)[:400]))
    return r


def git(repo, *args):
    return sh(['git'] + list(args), cwd=repo, check=True).stdout.strip()


def pat():
    for line in open(PAT_FILE):
        line = line.strip()
        if line:
            return line
    raise SystemExit('PAT 文件为空：%s' % PAT_FILE)


def privacy_scan(repo, extra_texts=()):
    """扫描所有被跟踪文件 + 版本说明文本。返回命中列表。"""
    hits = []
    patterns = list(PRIVACY_PATTERNS) + _owner_patterns()
    files = git(repo, 'ls-files').split('\n')
    for f in files:
        p = os.path.join(repo, f)
        if not os.path.isfile(p) or os.path.getsize(p) > 3_000_000:
            continue
        try:
            t = open(p, encoding='utf-8', errors='replace').read()
        except Exception:
            continue
        for pat_s, label in patterns:
            for m in set(re.findall(pat_s, t)):
                if any(a in m for a in ALLOW):
                    continue
                hits.append('%s: %s（%s）' % (f, m[:60], label))
    for t in extra_texts:
        for pat_s, label in patterns:
            for m in set(re.findall(pat_s, t or '')):
                if any(a in m for a in ALLOW):
                    continue
                hits.append('版本说明: %s（%s）' % (m[:60], label))
    return hits


def gh_api(path, method='GET', body=None):
    req = urllib.request.Request(
        'https://api.github.com/repos/%s/%s' % (REPO, path), method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={'Authorization': 'Bearer ' + pat(),
                 'Accept': 'application/vnd.github+json',
                 'Content-Type': 'application/json',
                 'User-Agent': 'em-release'})
    if PROXY:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({'http': PROXY, 'https': PROXY}))
    else:
        opener = urllib.request.build_opener()
    for attempt in range(3):
        try:
            return json.loads(opener.open(req, timeout=60).read())
        except urllib.error.HTTPError as e:
            return {'_err': e.code, '_body': e.read()[:200].decode('utf-8', 'replace')}
        except Exception:
            time.sleep(3)
    return {'_err': 'timeout'}


def push(repo, refs):
    tok = pat()
    orig = git(repo, 'remote', 'get-url', 'origin') if sh(
        ['git', 'remote', 'get-url', 'origin'], cwd=repo).returncode == 0 else None
    if orig is None:
        sh(['git', 'remote', 'add', 'origin',
            'https://github.com/%s.git' % REPO], cwd=repo)
    sh(['git', 'remote', 'set-url', 'origin',
        'https://x-access-token:%s@github.com/%s.git' % (tok, REPO)], cwd=repo, check=True)
    env = dict(os.environ, GIT_TERMINAL_PROMPT='0')
    if PROXY:
        env.update(https_proxy=PROXY, http_proxy=PROXY)
    try:
        for ref in refs:
            r = sh(['git', 'push', '--force', 'origin', ref], cwd=repo, env=env)
            out = (r.stdout + r.stderr).replace(tok, '[REDACTED]')
            print('  push %s → %s' % (ref, out.strip().splitlines()[-1] if out.strip() else 'ok'))
            if r.returncode != 0:
                raise SystemExit('推送失败：%s' % out[:400])
    finally:
        sh(['git', 'remote', 'set-url', 'origin',
            'https://github.com/%s.git' % REPO], cwd=repo)


def sync_releases(extra_name=None, extra_body=None):
    """把「有 tag 但没有 Release」的全部补上；顺带确保 extra 那个也在。"""
    print('· 校验 tag 与 Release 是否一一对应…')
    tags = [t['name'] for t in (gh_api('tags?per_page=100') or []) if isinstance(t, dict)]
    rel = [r for r in (gh_api('releases?per_page=100') or []) if isinstance(r, dict)]
    have = {r['tag_name'] for r in rel}
    missing = [t for t in tags if t not in have]
    if extra_name and extra_name not in have and extra_name not in missing:
        missing.append(extra_name)
    if not missing:
        print('  ✓ 无缺失（tag %d / release %d）' % (len(tags), len(rel)))
        return 0
    print('  ! 缺失 %d 个：%s' % (len(missing), ' '.join(missing)))
    for t in missing:
        body = extra_body if (extra_name and t == extra_name and extra_body) else \
            ('本版补录发行记录（源码与 tag 早已上传，此前漏建 Release）。\n\n'
             '---\n部署：NAS 本地执行 `docker compose restart etkn-monitor`'
             '（本仓库仅作源码存档与版本记录）。')
        d = gh_api('releases', 'POST', {'tag_name': t, 'name': t, 'body': body,
                                        'draft': False, 'prerelease': False,
                                        'make_latest': 'true'})
        print('  %s %s' % ('✓' if '_err' not in d else '✗ %s' % d, t))
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('version', nargs='?', default='')
    ap.add_argument('--note', default='')
    ap.add_argument('--note-file', default='')
    ap.add_argument('--repo', default=os.getcwd())
    ap.add_argument('--sync', action='store_true', help='只补缺失的 Release')
    ap.add_argument('--skip-privacy', action='store_true')
    a = ap.parse_args()
    repo = os.path.abspath(a.repo)

    if a.sync:
        return sync_releases()

    if not re.fullmatch(r'v\d+(\.\d+)+[a-z]?', a.version or ''):
        raise SystemExit('版本号格式应为 vX.Y.Z（收到 %r）' % a.version)

    note = a.note
    if a.note_file:
        note = open(a.note_file, encoding='utf-8').read().strip()

    print('=== etkn-monitor 发版 %s ===' % a.version)
    print('· 同步版本号到代码/界面/README…')
    for fn, pat_s, rpl in (
        ('monitor.py',       r"^(VERSION = ')v[\d.]+[a-z]?(')",  r'\g<1>%s\g<2>' % a.version),
        ('monitor.py',       r"(etkn-monitor )v[\d.]+[a-z]?( ——)", r'\g<1>%s\g<2>' % a.version),
        ('static/index.html', r"(界面 )v[\d.]+[a-z]?( ——)",        r'\g<1>%s\g<2>' % a.version),
        ('README.md',        r"^(# etkn-monitor —— ETKN 监控（)v[\d.]+[a-z]?(）)",
                             r'\g<1>%s\g<2>' % a.version),
    ):
        p = os.path.join(repo, fn)
        if not os.path.exists(p):
            continue
        t = open(p, encoding='utf-8').read()
        t2 = re.sub(pat_s, rpl, t, count=1, flags=re.M)
        if t2 != t:
            open(p, 'w', encoding='utf-8').write(t2)
            print('  · %s → %s' % (fn, a.version))
    print('· 隐私闸门…')
    if not a.skip_privacy:
        hits = privacy_scan(repo, (note,))
        if hits:
            print('  ✗ 发现 %d 处私人信息，已中止（开源仓库不允许）：' % len(hits))
            for h in hits[:20]:
                print('     -', h)
            return 2
        print('  ✓ 干净')

    print('· 提交…')
    git(repo, 'add', '-A')
    if git(repo, 'status', '--porcelain'):
        env = dict(os.environ, GIT_AUTHOR_NAME='Hermes', GIT_AUTHOR_EMAIL='hermes@local',
                   GIT_COMMITTER_NAME='Hermes', GIT_COMMITTER_EMAIL='hermes@local')
        sh(['git', 'commit', '-m', '%s: %s' % (a.version, (note or '发版').split('\n')[0][:90])],
           cwd=repo, env=env, check=True)
        print('  ✓ 已提交')
    else:
        print('  · 无待提交改动')
    git(repo, 'tag', '-f', a.version)
    print('· 推送 main 与 tag…')
    push(repo, ['main', a.version])

    print('· 建 Release…')
    body = (note or a.version) + ('\n\n---\n部署：NAS 本地执行 `docker compose restart etkn-monitor`'
                                  '（本仓库仅作源码存档与版本记录）。')
    rc = sync_releases(a.version, body)
    print('=== 完成：%s ===' % a.version)
    print('  发行页：https://github.com/%s/releases' % REPO)
    return rc


if __name__ == '__main__':
    sys.exit(main())
