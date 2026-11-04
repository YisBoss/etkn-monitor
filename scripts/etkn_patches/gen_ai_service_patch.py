#!/usr/bin/env python3
"""生成 ai_translation/service.py 补丁（AI 调用熔断），供 bind-mount 覆盖上游模块。

背景（2026-10-27）：
  整理任务 run 150433 从 07:18 到 09:43 静默 2 小时 25 分钟。py-spy 抓栈定位在
  _recognize_staging_serial → parse_media_filename → AITranslator._request_json，
  即 AI 文件名识别。真实原因：AI 通道（Lucky 667 → antigravity-manager:8045）的
  代理出口地区不被上游接受，每个请求先等 10 秒网关超时才回落常规识别；796 集
  《神奇宝贝》就是 796 × ~9.5s ≈ 2 小时。AITranslator 只有 socket 级 timeout
  （ai_request_timeout，默认 60），没有任何「连续失败就停手」的机制，坏通道会把
  批量整理线性拖成小时级卡死。

修法：
  在 _request_json 外层加进程级熔断器——连续失败 N 次（默认 5）后静默冷却 C 秒
  （默认 180），期间所有 AI 调用立即抛 RuntimeError 走降级分支（常规识别/直译），
  一次成功即复位。可用环境变量 ETK_AI_BREAKER_THRESHOLD / ETK_AI_BREAKER_COOLDOWN
  调整；threshold=0 表示关闭熔断。

用法：
  python3 gen_ai_service_patch.py             # 从镜像抽上游原版 → 生成补丁
  python3 gen_ai_service_patch.py --stdin f   # 用本地文件作为上游原版（离线调试）
"""
import hashlib
import os
import py_compile
import subprocess
import sys
import tempfile

ETKN_DIR = '/vol1/1000/docker/etkn'
def _running_image() -> str:
    try:
        r = subprocess.run(['docker', 'inspect', '-f', '{{.Image}}', 'etkn'],
                           capture_output=True, text=True, timeout=20)
        ref = (r.stdout or '').strip().replace('sha256:', '')
        if ref:
            return ref[:12]
    except Exception:
        pass
    return 'hbq0405/etkn:latest'


IMAGE = os.environ.get('ETKN_IMAGE') or _running_image()
REL = 'modules/ai_translation/service.py'
CTR_PATH = '/usr/local/lib/python3.12/site-packages/etk_vnext/' + REL
PATCH = os.path.join(ETKN_DIR, 'ai_service_patch.py')
ORIG = os.path.join(ETKN_DIR, 'ai_service.py.orig')

OLD_IMPORTS = '''import logging
import re
'''

NEW_IMPORTS = '''import logging
import os
import re
import time
'''

OLD_REQUEST = '''    def _request_json(self, request: Request) -> dict[str, Any]:
        try:
            with self.opener(request, timeout=self.timeout) as response:
                payload = json.loads(response.read())
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"AI 接口返回 HTTP {exc.code}: {detail}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("AI 接口未返回 JSON 对象")
        return payload
'''

NEW_REQUEST = '''    def _request_json(self, request: Request) -> dict[str, Any]:
        blocked = _ai_breaker_blocked_reason()
        if blocked:
            raise RuntimeError(blocked)
        try:
            with self.opener(request, timeout=self.timeout) as response:
                payload = json.loads(response.read())
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            _ai_breaker_record_failure(f"HTTP {exc.code}")
            raise RuntimeError(f"AI 接口返回 HTTP {exc.code}: {detail}") from exc
        except Exception as exc:
            _ai_breaker_record_failure(type(exc).__name__)
            raise
        if not isinstance(payload, dict):
            _ai_breaker_record_failure("非 JSON 对象响应")
            raise RuntimeError("AI 接口未返回 JSON 对象")
        _ai_breaker_record_success()
        return payload
'''

HELPER = '''# [etkn-fix 2026-10-27] AI 调用熔断器
# 坏通道（网关 502 / 地区受限 400 / 连接超时）会让每个 AI 请求先等满超时才降级，
# 批量整理时被线性放大成小时级"卡死"。连续失败达到阈值后直接熔断，期间所有调用
# 立即失败并走降级分支，避免把时间浪费在确定不通的通道上。
_AI_BREAKER_STATE: dict[str, float] = {"failures": 0.0, "open_until": 0.0}
_AI_BREAKER_THRESHOLD = int(os.environ.get("ETK_AI_BREAKER_THRESHOLD", "5") or 5)
_AI_BREAKER_COOLDOWN = float(os.environ.get("ETK_AI_BREAKER_COOLDOWN", "180") or 180)


def _ai_breaker_blocked_reason() -> str:
    until = _AI_BREAKER_STATE["open_until"]
    if not until:
        return ""
    now = time.monotonic()
    if now < until:
        return (
            "AI 通道熔断中（连续失败 %d 次，剩余 %d 秒），本次直接降级"
            % (int(_AI_BREAKER_STATE["failures"]), int(until - now))
        )
    _AI_BREAKER_STATE["open_until"] = 0.0
    _AI_BREAKER_STATE["failures"] = 0.0
    return ""


def _ai_breaker_record_failure(reason: str) -> None:
    _AI_BREAKER_STATE["failures"] += 1
    failures = int(_AI_BREAKER_STATE["failures"])
    if _AI_BREAKER_THRESHOLD > 0 and failures >= _AI_BREAKER_THRESHOLD and not _AI_BREAKER_STATE["open_until"]:
        _AI_BREAKER_STATE["open_until"] = time.monotonic() + _AI_BREAKER_COOLDOWN
        logger.warning(
            "AI 通道连续失败 %d 次（最近一次：%s），熔断 %d 秒；期间直接降级，不再等待超时",
            failures,
            reason,
            int(_AI_BREAKER_COOLDOWN),
        )


def _ai_breaker_record_success() -> None:
    _AI_BREAKER_STATE["failures"] = 0.0
    _AI_BREAKER_STATE["open_until"] = 0.0


class AITranslator:'''


def extract() -> str:
    if len(sys.argv) >= 3 and sys.argv[1] == '--stdin':
        with open(sys.argv[2], 'r', encoding='utf-8') as f:
            return f.read()
    r = subprocess.run(
        ['docker', 'run', '--rm', '--entrypoint', 'cat', IMAGE, CTR_PATH],
        capture_output=True, text=True, timeout=180,
    )
    if r.returncode != 0:
        raise RuntimeError('抽取上游文件失败: ' + (r.stderr or '').strip()[:300])
    return r.stdout


OLD_EMBED_BLOCK = '''        else:
            payload = self._post_json(
                f"{self._openai_root()}/embeddings",
                {"model": model, "input": value},
                self._bearer_headers(),
            )
'''

NEW_EMBED_BLOCK = '''        else:
            embed_root, embed_key = self._embedding_endpoint()
            payload = self._post_json(
                f"{embed_root}/embeddings",
                {"model": model, "input": value},
                {"Accept": "application/json", "Content-Type": "application/json",
                 "Authorization": f"Bearer {embed_key}"},
            )
'''

EMBED_METHOD = '''    def _embedding_endpoint(self) -> tuple[str, str]:
        """[etkn-fix16 2026-11-04] 向量的端点与密钥可与对话分离。

        WB 中转池只代理 chat（/v1/embeddings 恒 404），向量因此改走独立供应商
        （SiliconFlow BAAI/bge-m3，1024 维）。两项凭据放在 integration_credentials.ai 的
        embedding_base_url / embedding_api_key 字段：UI 保存 AI 表单只合并 api_key，
        不会冲掉它们；缺省时整体回退主端点 + 主密钥。
        """
        base, key = "", ""
        try:
            creds = CredentialStore(self.database).get("ai")
            base = str(creds.get("embedding_base_url") or "").strip().rstrip("/")
            key = str(creds.get("embedding_api_key") or "").strip()
        except Exception:
            base, key = "", ""
        return (base or self._openai_root()), (key or self.api_key)

    def _bearer_headers(self) -> dict[str, str]:'''


def transform(orig: str) -> str:
    if '_ai_breaker_blocked_reason' in orig:
        raise SystemExit('上游已内建熔断，无需补丁')
    assert orig.count(OLD_REQUEST) == 1, 'AI _request_json 锚点不唯一，count=%d' % orig.count(OLD_REQUEST)
    assert orig.count('class AITranslator:') == 1, 'class AITranslator 锚点不唯一'
    patched = orig.replace(OLD_REQUEST, NEW_REQUEST)
    if 'import os\n' not in patched:
        assert patched.count(OLD_IMPORTS) == 1, 'import 锚点不唯一'
        patched = patched.replace(OLD_IMPORTS, NEW_IMPORTS)
    patched = patched.replace('class AITranslator:', HELPER, 1)
    if '_embedding_endpoint' not in patched:
        assert patched.count('    def _bearer_headers(self) -> dict[str, str]:') == 1, 'bearer_headers 锚点不唯一'
        assert patched.count(OLD_EMBED_BLOCK) == 1, 'embeddings 调用锚点不唯一 count=%d' % patched.count(OLD_EMBED_BLOCK)
        patched = patched.replace('    def _bearer_headers(self) -> dict[str, str]:', EMBED_METHOD, 1)
        patched = patched.replace(OLD_EMBED_BLOCK, NEW_EMBED_BLOCK, 1)
    if not patched.endswith('\n'):
        patched += '\n'
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
    print('OK ai_service_patch.py %d bytes md5=%s' % (len(patched.encode()), md5))


if __name__ == '__main__':
    main()
