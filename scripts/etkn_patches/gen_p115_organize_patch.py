#!/usr/bin/env python3
"""生成 p115_workflow 补丁（整理结果瘦身），供 bind-mount 覆盖上游模块。

背景（2026-10-25）：
  /usr/local/lib/python3.12/site-packages/etk_vnext/modules/organize/p115_workflow.py 的
  _organize() 返回 {"job_id","items","rejected","media_item_count"}，其中 rejected 每条是
  完整 plan["group"]（含 video/root 与 TMDB 元数据）。实测 798 项 = 32.3MB；1528 项 +
  2072 条 rejection 上百 MB。而 workflow/repository.py:complete_step 把同一份
  json.dumps(output) 作为 5 个 %s::jsonb 参数传入同一条 UPDATE → 写放大 5 倍，
  libpq 报 "sending query and params failed: cannot allocate memory for output buffer"，
  整理本体已跑完（86 个入库、90 个 item 全部移交成功）却把 step 判 failed、
  enqueue-ingest 被 block、organize.jobs 留成 pending 孤儿行。

修法：
  rejected 压缩为 {name, external_file_id, reason, rejection_kind} 四字段；
  压缩后若仍超 24MB，则整体丢弃 rejected 只留 rejected_count（下游只用 items 与 reason）。
  下游 _enqueue_ingest() 只读 organized["items"] 与 rejected[].reason，结构变更安全。

用法：
  python3 gen_p115_organize_patch.py            # 从当前镜像抽取上游原版 → 生成补丁
  python3 gen_p115_organize_patch.py --stdin f  # 用本地文件 f 作为上游原版（离线调试）
"""
import hashlib
import os
import py_compile
import subprocess
import sys
import tempfile

ETKN_DIR = '/vol1/1000/docker/etkn'
IMAGE = 'hbq0405/etkn:latest'
REL = 'modules/organize/p115_workflow.py'
CTR_PATH = '/usr/local/lib/python3.12/site-packages/etk_vnext/' + REL
PATCH = os.path.join(ETKN_DIR, 'p115_workflow_patch.py')
ORIG = os.path.join(ETKN_DIR, 'p115_workflow.py.orig')

OLD_RETURN = '''        return {
            "job_id": job_id,
            "items": organized,
            "rejected": rejected,
            "media_item_count": media_item_count,
        }
'''

NEW_RETURN = '''        return _compact_organize_output({
            "job_id": job_id,
            "items": organized,
            "rejected": rejected,
            "media_item_count": media_item_count,
        })
'''

HELPER = '''

# [etkn-fix 2026-10-25] p115-organize 结果体瘦身
# _organize() 的 output 里 rejected 每项是完整 plan["group"]（video/root + TMDB 元数据），
# 而 workflow/repository.py:complete_step() 把同一份 json.dumps 传 5 次写同一行，
# 上百 MB 的输出会被放大 5 倍 → libpq "cannot allocate memory for output buffer"，
# 整理已跑完却把 step 判 failed。下游 _enqueue_ingest() 只读 items 与 rejected[].reason，
# 故压缩 rejected 不影响任何业务分支。
_ORGANIZE_OUTPUT_SOFT_LIMIT = 24 * 1024 * 1024


def _compact_organize_rejection(value: dict[str, Any]) -> dict[str, Any]:
    detail = value.get("video")
    if not isinstance(detail, dict):
        detail = value.get("root")
    if not isinstance(detail, dict):
        detail = {}
    return {
        "name": str(detail.get("name") or ""),
        "external_file_id": str(detail.get("external_file_id") or ""),
        "reason": str(value.get("reason") or ""),
        "rejection_kind": str(value.get("rejection_kind") or "unrecognized"),
    }


def _compact_organize_output(output: dict[str, Any]) -> dict[str, Any]:
    rejected = output.get("rejected") or []
    compact = [
        _compact_organize_rejection(value)
        for value in rejected
        if isinstance(value, dict)
    ]
    result = {**output, "rejected": compact, "rejected_count": len(compact)}
    if len(json.dumps(result, ensure_ascii=False, default=str)) > _ORGANIZE_OUTPUT_SOFT_LIMIT:
        result["rejected"] = []
        result["rejected_compacted"] = True
    return result
'''


def extract() -> str:
    if len(sys.argv) >= 3 and sys.argv[1] == '--stdin':
        with open(sys.argv[2], 'r', encoding='utf-8') as f:
            return f.read()
    r = subprocess.run(
        ['docker', 'run', '--rm', '--entrypoint', 'cat', IMAGE, CTR_PATH],
        capture_output=True, text=True, timeout=120,
    )
    if r.returncode != 0:
        raise RuntimeError('抽取上游文件失败: ' + (r.stderr or '').strip()[:300])
    return r.stdout


def transform(orig: str) -> str:
    assert orig.count(OLD_RETURN) == 1, (
        '_organize 返回块锚点不唯一/未找到，count=%d' % orig.count(OLD_RETURN)
    )
    assert 'def _compact_organize_output' not in orig, '上游已内建压缩，无需补丁'
    patched = orig.replace(OLD_RETURN, NEW_RETURN)
    if not patched.endswith('\n'):
        patched += '\n'
    patched += HELPER
    return patched


def main():
    orig = extract()
    with open(ORIG, 'w', encoding='utf-8') as f:
        f.write(orig)
    os.chmod(ORIG, 0o644)

    patched = transform(orig)

    with tempfile.NamedTemporaryFile('w', suffix='.py', delete=False, encoding='utf-8') as f:
        f.write(patched)
        tmp = f.name
    try:
        py_compile.compile(tmp, doraise=True)
    finally:
        os.unlink(tmp)

    with open(PATCH, 'w', encoding='utf-8') as f:
        f.write(patched)
    os.chmod(PATCH, 0o644)
    md5 = hashlib.md5(patched.encode()).hexdigest()
    with open(PATCH + '.md5', 'w') as f:
        f.write(md5 + '\n')
    os.chmod(PATCH + '.md5', 0o644)
    print('OK p115_workflow_patch.py %d bytes md5=%s' % (len(patched.encode()), md5))


if __name__ == '__main__':
    main()
