from __future__ import annotations

from fastapi import HTTPException, Request, status

from app.config import get_settings


def require_api_key(request: Request) -> str:
    settings = get_settings()

    if not settings.api_keys:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server is not configured with BENCH_API_KEYS",
        )

    provided = request.headers.get(settings.api_key_header)
    if not provided or provided not in settings.api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    return provided
