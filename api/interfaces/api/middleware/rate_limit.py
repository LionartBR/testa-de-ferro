# api/interfaces/api/middleware/rate_limit.py
from __future__ import annotations

import time
from collections import defaultdict

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from api.infrastructure.config import get_settings


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: object) -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self._requests: dict[str, list[float]] = defaultdict(list)

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        settings = get_settings()

        # 0 = sem limite (usado em testes)
        if settings.rate_limit_per_minute == 0:
            return await call_next(request)

        # API key bypass
        api_key = request.headers.get("X-API-Key")
        if api_key:
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"
        now = time.time()
        window = 60.0

        # Limpar requests antigos
        self._requests[client_ip] = [t for t in self._requests[client_ip] if now - t < window]

        if len(self._requests[client_ip]) >= settings.rate_limit_per_minute:
            return Response(
                content='{"detail": "Rate limit excedido. Tente novamente em 1 minuto."}',
                status_code=429,
                media_type="application/json",
            )

        self._requests[client_ip].append(now)
        return await call_next(request)
