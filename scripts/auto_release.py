#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v2.8.3② 部署收尾自动发版：解析部署脚本传入的版本号→写摘要→git commit/push→推 Release(Latest)。
供 deploy 脚本末尾调用：python3 auto_release.py <版本号，如 v2.8.3>
摘要=git 上一发版 tag 到 HEAD 的提交说明汇总（写清改动）。"""
import re, subprocess, sys, os

GIT_DIR = '/vol1/@appdata/trim.hermes/workspace/etkn-monitor'
SCRIPT = '/vol1/@appdata/trim.hermes/workspace/etkn-monitor/scripts/release_upload.py'


def git(*args):
    return subprocess.run(['git', '-C', GIT_DIR] + list(args),
                          capture_output=True, text=True).stdout.strip()


def _tag_key(t):
    """tag → 可比较的键，容忍字母后缀（v2.8.22b 之类）。

    旧实现 [int(x) for x in t.lstrip('v').split('.')] 遇到 v2.8.11a 会 int('11a')
    直接 ValueError 崩掉。以前没暴露是因为本地 tag 长期只有 v2.9.0（tag 由
    release_upload.py 走 GitHub API 建，从不 git push --tags）；一旦 fetch --tags
    把全量 tag 拉下来就必崩。这里改成：数字段元组为主键、其余字符为次键。
    """
    return (tuple(int(x) for x in re.findall(r'\d+', t)),
            re.sub(r'\d+', '', t.lstrip('v')))


def fetch_tags():
    """同步远端 tag（尽力而为，失败不阻断发版）。

    tag 由 release_upload.py 走 GitHub API 建、从不 git push --tags，所以本地
    tag 会滞后 → prev 会算成更早的版本，发版摘要覆盖过多提交。--force 是因为
    远端才是 tag 的权威来源（重打 tag 时本地旧值会让 fetch 被拒）。
    """
    r = subprocess.run(['git', '-C', GIT_DIR, 'fetch', '--tags', '--force', 'origin'],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        err = (r.stderr or '').strip().splitlines()
        print('git fetch --tags 失败（按本地 tag 继续）：' + (err[-1] if err else '未知错误'))
    return r.returncode == 0


def main():
    tag = sys.argv[1] if len(sys.argv) > 1 else ''
    if not tag.startswith('v'):
        print('用法: auto_release.py v2.8.3')
        return 1
    fetch_tags()          # v2.9.2.4：先同步远端 tag，否则 prev 会滞后（失败不阻断）
    # 上一个 tag（语义化排序）
    tags = sorted([t for t in git('tag', '--list').splitlines() if t.startswith('v')],
                  key=_tag_key)
    # 排除本次要发的 tag 本身：远端可能已经有了（重跑/幂等场景），
    # 否则 tags[-1] == tag → prev 变 None → 摘要退化成 HEAD~5..HEAD
    prev = next((t for t in reversed(tags) if t != tag), None)
    rng = f'{prev}..HEAD' if prev else 'HEAD~5..HEAD'
    log = git('log', '--format=- %s', rng)
    if not log:
        log = '- 见提交记录'
    body = f'{tag} 自动发版\n\n**改动摘要**（自 {prev or "起点"} 以来的提交）：\n{log}\n'
    body += '\n部署方式：docker compose restart etkn-monitor（NAS 本地部署，本仓库为源码存档与版本记录）。\n'
    body_file = f'/tmp/release_{tag}.md'
    open(body_file, 'w', encoding='utf-8').write(body)
    # 确保 git 已提交干净
    dirty = git('status', '--porcelain')
    if dirty:
        subprocess.run(['git', '-C', GIT_DIR, 'add', '-A'], check=True)
        subprocess.run(['git', '-C', GIT_DIR, 'commit', '-m', f'{tag}: 自动发版收尾'],
                       capture_output=True)
    # 推 main
    r = subprocess.run(['git', '-C', GIT_DIR, 'push', 'origin', 'main'],
                       capture_output=True, text=True, timeout=120)
    print('git push:', (r.stdout or r.stderr).strip()[-120:])
    # 推 Release
    r = subprocess.run(['python3', SCRIPT, tag, tag, body_file, '--latest'],
                       capture_output=True, text=True, timeout=120)
    print(r.stdout.strip())
    return r.returncode


if __name__ == '__main__':
    sys.exit(main())
