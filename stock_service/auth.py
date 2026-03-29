import time

from starlette.types import ASGIApp, Receive, Scope, Send

from stock_service.database import get_pool

# 不需要认证的路径
_PUBLIC_PATHS = {"/health"}

# API Key 验证缓存：key -> expire_time
_key_cache: dict[str, float] = {}
_CACHE_TTL = 300  # 5 分钟


class ApiKeyMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in _PUBLIC_PATHS:
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        api_key = headers.get(b"x-api-key", b"").decode()

        if not api_key:
            await _send_json(send, 401, '{"detail":"Missing API key. Provide it via X-API-Key header."}')
            return

        if not _verify_api_key(api_key):
            await _send_json(send, 403, '{"detail":"Invalid API key."}')
            return

        await self.app(scope, receive, send)


async def _send_json(send: Send, status: int, body: str) -> None:
    body_bytes = body.encode()
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body_bytes)).encode()),
        ],
    })
    await send({"type": "http.response.body", "body": body_bytes})


def _verify_api_key(key: str) -> bool:
    """检查 api_key 表中是否存在该 key，结果缓存 5 分钟。"""
    now = time.monotonic()
    expire = _key_cache.get(key)
    if expire is not None and now < expire:
        return True

    pool = get_pool()
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM api_key WHERE key = %s LIMIT 1",
            (key,),
        ).fetchone()

    if row is not None:
        _key_cache[key] = now + _CACHE_TTL
        return True

    _key_cache.pop(key, None)
    return False
