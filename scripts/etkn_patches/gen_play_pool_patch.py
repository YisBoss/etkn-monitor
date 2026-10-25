#!/usr/bin/env python3
"""生成 play_pool 补丁（小号转播：跨播放会话复用，起播从 3.2 秒压到 0.3~0.6 秒）。

背景（2026-10-25）：
  作者侧起播 1~2 秒，本机稳定 5 秒左右。实测拆解（库里 38.3 万 items、1.9 万季、
  1.65 万季追剧记录，跟作者 1 万量级不同，但播放这条路上并没有全库扫描）：

    GET /emby/Users/{uid}/Items?Ids=item-x        5~8 ms
    POST /emby/Items/item-x/PlaybackInfo         13~15 ms
    GET  /emby/Videos/item-x/stream (首次)       3197 ms   ← 起播慢的全在这里
    GET  /emby/Videos/item-x/stream (同会话)     31~44 ms
    GET  /emby/Videos/item-x/stream (新会话)     1198 ms
    CDN 首包                                    约 300~400 ms

  首包的 3.2 秒 = 115 秒传链：get_info（源文件信息）→ rapid_upload 首次
  （p115client 内 1.39 秒，43 次 TLS read）→ 主号 rapid_sign_value 出签名（约 0.7 秒）
  → 带 sign_val 二次 rapid_upload → 换直链。这部分是 115 服务端的活，本机无优化空间。

  真正能省掉的是「第二次、第三次……播放同一个文件」也要重走一遍这件事。原因在
  _find_reusable_session()：

      if play_session_id and session_play_session_id and play_session_id == session_play_session_id:
          return dict(session)
      if play_session_id and session_play_session_id:
          continue          # ← PlaySessionId 不同就彻底放弃已有临时副本

  而 Emby 客户端每次起播都会生成新的 PlaySessionId，会话记录里也存着上一次的
  PlaySessionId，两者永远不相等 → 命中不了 → 每次都当新片重新秒传。
  实测：同一 item 换一个 PlaySessionId，耗时立刻从 44ms 回到 1198ms；再换一次仍是冷。

  临时副本（temp_pick_code / temp_fid）与直链（direct_url）只跟「源文件 + 小号 +
  用户 + 客户端」绑定，跟播放会话无关；会话只用来记「这个文件已经传过一份」。
  直链是否过期由 get_direct_url() 内部的 DIRECT_URL_TTL_SECONDS(300s) 校验兜底，
  过期会重新换链（1 次 115 调用，约 0.3~0.6 秒），不会返回死链。

修法：
  放行跨播放会话复用（保留 item_id / user_id / client_key 三个绑定校验，以及
  「临时副本或直链至少有一个」的可用性校验），并在命中时 debug 打点。
  预期：同文件二次起播 3.2s → 0.03~0.06s（直链仍在 5 分钟内）或 0.3~0.6s（需换链）。
"""
from __future__ import annotations

import hashlib
import shutil
import sys
from pathlib import Path

BASE = Path("/vol1/1000/docker/etkn")
ORIG = BASE / "play_pool.py.orig"
PATCH = BASE / "play_pool_patch.py"
SIDECAR = BASE / "play_pool_patch.py.md5"

OLD_BLOCK = '''        if play_session_id and session_play_session_id:
            continue
'''

NEW_BLOCK = '''        # [etkn-fix 2026-10-25 起播提速] 原实现：PlaySessionId 不同就 continue。
        # 但 Emby/Hills 每次起播都会生成新的 PlaySessionId，而会话记录里存的是上一次的，
        # 两者永远不相等 → 同一部剧的第 2 次播放也被判成「不可复用」，必须重走
        # 115 秒传 + 主号签名（实测 3.2s vs 命中 40ms）。
        # 临时副本（temp_pick_code/temp_fid）与直链都只跟「源文件 + 小号 + 用户 + 客户端」
        # 绑定，与播放会话无关；直链过期由 get_direct_url() 的 DIRECT_URL_TTL_SECONDS
        # 校验兜底（过期会重新换链，1 次 115 调用约 0.3~0.6s），不会返回死链。
        if play_session_id and session_play_session_id and play_session_id != session_play_session_id:
            logger.debug(
                "  ➜ [小号播放] 跨播放会话复用临时副本：pc=%s... session=%s -> %s",
                str(session.get("temp_pick_code") or "")[:8],
                session_play_session_id,
                play_session_id,
            )
'''


def main() -> int:
    if not ORIG.exists():
        print(f"缺少上游原文：{ORIG}", file=sys.stderr)
        return 2
    content = ORIG.read_text(encoding="utf-8")
    if OLD_BLOCK not in content:
        print("锚点未找到：_find_reusable_session 的 PlaySessionId 跳过分支", file=sys.stderr)
        return 3
    if content.count(OLD_BLOCK) != 1:
        print("锚点不唯一，拒绝打补丁", file=sys.stderr)
        return 4
    patched = content.replace(OLD_BLOCK, NEW_BLOCK)
    PATCH.write_text(patched, encoding="utf-8")
    digest = hashlib.md5(patched.encode("utf-8")).hexdigest()
    SIDECAR.write_text(f"{digest}  {PATCH}\n", encoding="utf-8")
    print(f"已生成 {PATCH}（{len(patched)} 字节，md5 {digest}）")
    print(f"已写边车 {SIDECAR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
