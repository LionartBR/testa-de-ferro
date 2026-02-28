# api/interfaces/api/main.py
from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    from api.infrastructure.duckdb_connection import get_connection
    get_connection()  # valida conexao no startup
    yield


app = FastAPI(
    title="Testa de Ferro API",
    debug=False,  # NUNCA True em producao
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url=None,
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next: object) -> Response:
    response = await call_next(request)  # type: ignore[misc]
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response  # type: ignore[return-value]


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

from api.interfaces.api.routes.fornecedor_routes import router as fornecedor_router  # noqa: E402

app.include_router(fornecedor_router, prefix="/api")
