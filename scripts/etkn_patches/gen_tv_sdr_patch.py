#!/usr/bin/env python3
"""etkn-fix17 2026-11-09 生成 api/tv_transcode.py 补丁：ETKN TV 播 HDR/DV 黑屏 → 服务端强制 SDR 转码。

背景（实测，见 04-文档/ETKN-TV-0.1.99-HDR黑屏-给作者的问题说明-20261109.md）：
  ETKN TV 0.1.x 在起播协商里声明 HevcHdr10 能力，服务端据此保留 HDR 直出（p010le、不 tone-map）；
  但客户端播放时从不请求 HDR 数据空间（logcat 全程无 u2EotfValue、无 bRequestingDataSpace），
  电视面板停在 SDR 制式 → 有声音、有字幕、有弹幕，画面全黑。原画与降码率转码都黑。

本补丁只在「UA 命中 ETKN TV」且「片源是 HDR/DV」时生效，全部可用环境变量关闭：
  A. Reason=user_bitrate 分支不再无条件返回 DirectPlay，落下去走转码；
  B. hdr_capabilities 置空 → choose_profile 判定 preserve_hdr=False → tone-map 出 8bit bt709 SDR。

开关：ETKN_TV_FORCE_SDR=1（默认开）/ 0|off|false|no 关闭。
"""
import hashlib
import py_compile
import shutil
import sys

BASE = '/vol1/1000/docker/etkn'
ORIG = f'{BASE}/tv_transcode.py.orig'
OUT = f'{BASE}/tv_transcode_patch.py'
MARK = 'etkn-fix17 2026-11-09'

IMPORT_ANCHOR = 'import math\n'
IMPORT_NEW = 'import math\nimport os\n'

HELPER_ANCHOR = 'def original_input(request, row, user, playback):'
HELPER_NEW = f'''# {MARK}: ETKN TV 黑屏规避用的三个判定（UA / 开关 / 片源 HDR）
def _etkn_tv_ua(request) -> bool:
    try:
        ua = str(request.headers.get('user-agent') or '').lower()
    except Exception:
        return False
    return 'etkn tv' in ua or 'etkntv' in ua


def _force_sdr() -> bool:
    raw = str(os.environ.get('ETKN_TV_FORCE_SDR', '1')).strip().lower()
    return raw not in ('0', 'off', 'false', 'no')


def _hdr_source(source) -> bool:
    streams = (source or {{}}).get('MediaStreams') or []
    video = next((s for s in streams if str(s.get('Type')).lower() == 'video'), {{}})
    marks = ' '.join(str(video.get(k) or '') for k in
                     ('VideoRange', 'ExtendedVideoType', 'ColorTransfer', 'ColorSpace')).lower()
    if any(m in marks for m in ('hdr', 'dolby', 'dovi', 'hlg', 'smpte2084', 'arib-std-b67', 'bt2020')):
        return True
    try:
        return int(video.get('BitDepth') or 8) >= 10
    except Exception:
        return False


{HELPER_ANCHOR}'''

OLD_A = """        if payload.Bitrate == 0 or not exceeds_bitrate and not exceeds_size:
            return {'DirectPlay': True}
"""
NEW_A = f"""        if payload.Bitrate == 0 or not exceeds_bitrate and not exceeds_size:
            # {MARK}: ETKN TV 播 HDR/DV 原画会黑屏（客户端不请求 HDR 制式）→ 拒绝 DirectPlay，落到下面转码
            if not (_force_sdr() and _etkn_tv_ua(request) and _hdr_source(source)):
                return {{'DirectPlay': True}}
"""

OLD_B = "        hdr_capabilities = [c.model_dump() for c in payload.HevcHdr10] if hevc_supported else []\n"
NEW_B = (
    "        hdr_capabilities = [c.model_dump() for c in payload.HevcHdr10] if hevc_supported else []\n"
    f"        if _force_sdr() and _etkn_tv_ua(request):\n"
    f"            hdr_capabilities = []  # {MARK}: 强制走 tone-map，出 8bit bt709 SDR\n"
)


def sub_once(text, old, new, tag):
    n = text.count(old)
    if n != 1:
        print(f'❌ 锚点不唯一（{tag}）: 命中 {n} 次，应为 1 次')
        sys.exit(1)
    return text.replace(old, new, 1)


def main():
    src = open(ORIG, encoding='utf-8').read()
    if MARK in src:
        print('⚠️ 原始文件里已含本补丁标记，先刷新 tv_transcode.py.orig')
        sys.exit(2)

    text = sub_once(src, IMPORT_ANCHOR, IMPORT_NEW, 'import os')
    text = sub_once(text, HELPER_ANCHOR, HELPER_NEW, 'helpers')
    text = sub_once(text, OLD_A, NEW_A, 'user_bitrate DirectPlay')
    text = sub_once(text, OLD_B, NEW_B, 'hdr_capabilities')

    for must in ('_etkn_tv_ua', '_force_sdr', '_hdr_source', 'ETKN_TV_FORCE_SDR'):
        if must not in text:
            print(f'❌ 生成结果缺关键符号: {must}')
            sys.exit(3)

    shutil.copy2(OUT, OUT + '.prev') if __import__('os').path.exists(OUT) else None
    with open(OUT, 'w', encoding='utf-8') as handle:
        handle.write(text)
    try:
        py_compile.compile(OUT, doraise=True)
    except py_compile.PyCompileError as exc:
        print(f'❌ 语法检查失败: {exc}')
        sys.exit(4)

    digest = hashlib.md5(open(OUT, 'rb').read()).hexdigest()
    with open(OUT + '.md5', 'w', encoding='utf-8') as handle:
        handle.write(digest + '\n')
    print(f'✅ {OUT}')
    print(f'   md5 {digest}  size {len(text)}B')


if __name__ == '__main__':
    main()
