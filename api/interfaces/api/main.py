# api/interfaces/api/main.py
from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from api.interfaces.api.middleware.rate_limit import RateLimitMiddleware


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


app.add_middleware(RateLimitMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Routers — ranking ANTES de fornecedor (path conflict: /fornecedores/ranking vs /fornecedores/{cnpj_raw})
from api.interfaces.api.routes.alerta_routes import router as alerta_router  # noqa: E402
from api.interfaces.api.routes.busca_routes import router as busca_router  # noqa: E402
from api.interfaces.api.routes.contrato_routes import router as contrato_router  # noqa: E402
from api.interfaces.api.routes.export_routes import router as export_router  # noqa: E402
from api.interfaces.api.routes.fornecedor_routes import router as fornecedor_router  # noqa: E402
from api.interfaces.api.routes.grafo_routes import router as grafo_router  # noqa: E402
from api.interfaces.api.routes.orgao_routes import router as orgao_router  # noqa: E402
from api.interfaces.api.routes.ranking_routes import router as ranking_router  # noqa: E402
from api.interfaces.api.routes.stats_routes import router as stats_router  # noqa: E402

app.include_router(ranking_router, prefix="/api")
app.include_router(fornecedor_router, prefix="/api")
app.include_router(alerta_router, prefix="/api")
app.include_router(busca_router, prefix="/api")
app.include_router(contrato_router, prefix="/api")
app.include_router(grafo_router, prefix="/api")
app.include_router(export_router, prefix="/api")
app.include_router(orgao_router, prefix="/api")
app.include_router(stats_router, prefix="/api")
