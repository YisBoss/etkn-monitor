#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v2.8.3② 发版自动上传 GitHub Releases：
用法：python3 release_upload.py <tag> <标题> <摘要文件路径> [--latest]
- tag 必须已随 git push 推到远端（或给 --sha 指定提交）；
- 幂等：Release 已存在则跳过（返回 0）；
- --latest 把该版标为 Latest（调用 update release make_latest）。
凭据运行时读取 .creds/github_etkn_monitor.txt，不落盘不回显。"""
import json, subprocess, sys, urllib.request

REPO = 'YisBoss/etkn-monitor'
GIT_DIR = '/vol1/@appdata/trim.hermes/workspace/etkn-monitor'
PAT_FILE = '/vol1/@appdata/trim.hermes/workspace/.creds/github_etkn_monitor.txt'


def _pat():
    for line in open(PAT_FILE):
        line = line.strip()
        if line:
            return line
    raise SystemExit('PAT 文件为空')


def api(method, path, data=None):
    req = urllib.request.Request(
        f'https://api.github.com/repos/{REPO}/{path}', method=method,
        data=json.dumps(data).encode() if data is not None else None,
        headers={'Authorization': f"token {_pat()}", 'Accept': 'application/vnd.github+json',
                 'Content-Type': 'application/json', 'User-Agent': 'etkn-release'})
    try:
        r = urllib.request.build_opener(
            urllib.request.ProxyHandler({})).open(req, timeout=30)
        return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}


def main():
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    flags = {a for a in sys.argv[1:] if a.startswith('--')}
    tag, title, body_file = args[0], args[1], args[2]
    body = open(body_file, encoding='utf-8').read().strip()
    # tag → SHA（本地仓库解析）
    sha = subprocess.run(['git', '-C', GIT_DIR, 'rev-parse', tag],
                         capture_output=True, text=True).stdout.strip()
    if len(sha) != 40:
        sha = subprocess.run(['git', '-C', GIT_DIR, 'rev-parse', 'HEAD'],
                             capture_output=True, text=True).stdout.strip()
    # ① 建 tag（幂等：已存在忽略 422）
    st, _ = api('POST', 'git/refs', {'ref': f'refs/tags/{tag}', 'sha': sha})
    print(f'tag {tag} → {sha[:10]} ({st})')
    # ② 建 Release（幂等：已存在跳过）
    st, resp = api('POST', 'releases', {
        'tag_name': tag, 'name': title or tag, 'body': body,
        'draft': False, 'prerelease': False,
        'make_latest': 'true' if '--latest' in flags else 'false'})
    if st in (201, 200):
        print(f'Release 创建成功: {resp.get("html_url", "")}')
        return 0
    if st == 422:  # 已存在 → 若 --latest 则更新 make_latest
        if '--latest' in flags:
            s2, ls = api('GET', f'releases/tags/{tag}')
            if s2 == 200:
                s3, _ = api('PATCH', f'releases/{ls["id"]}', {'make_latest': 'true'})
                print(f'已存在，Latest 标记更新 ({s3})')
        else:
            print('已存在，跳过')
        return 0
    print(f'FAIL {st}: {str(resp)[:200]}')
    return 1


if __name__ == '__main__':
    sys.exit(main())
