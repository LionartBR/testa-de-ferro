# Fase 2a — Todos os Endpoints REST + Rate Limiting

## Contexto

Fase 1 entregou: `GET /api/fornecedores/{cnpj}` funcional com domínio completo (VOs, entities, alertas, score), 62 testes passando, 97% cobertura. Todos os outros route files existem como stubs vazios.

**Objetivo:** API navegável completa — 8 endpoints novos + rate limiting middleware + testes de integração para cada um.

**Escopo explicitamente excluído da Fase 2a:**
- Alertas novos (RODIZIO, DOACAO, SOCIO_SANCIONADO, TESTA_DE_FERRO)
- Indicadores novos (CNAE_INCOMPATIVEL, SOCIO_EM_MULTIPLAS, etc.)
- Entidades doacao/servidor (repos + infra)
- Pipeline
- Frontend components

---

## Step 1 — DTOs novos (FornecedorResumo, AlertaFeed, Grafo, Stats, Export)

**Depende de:** nada
**Arquivos:**
- `api/application/dtos/fornecedor_dto.py` (editar — adicionar FornecedorResumoDTO)
- `api/application/dtos/alerta_dto.py` (editar — adicionar AlertaFeedItemDTO)
- `api/application/dtos/grafo_dto.py` (novo)
- `api/application/dtos/stats_dto.py` (novo)
- `api/application/dtos/export_dto.py` (novo)

### fornecedor_dto.py — adicionar:

```python
class FornecedorResumoDTO(BaseModel):
    cnpj: str
    razao_social: str
    situacao: str
    score_risco: int
    faixa_risco: str
    qtd_alertas: int
    max_severidade: str | None
    total_contratos: int
    valor_total: str
```

### alerta_dto.py — adicionar:

```python
class AlertaFeedItemDTO(BaseModel):
    tipo: str
    severidade: str
    descricao: str
    evidencia: str
    detectado_em: str
    cnpj: str
    razao_social: str
    socio_nome: str | None = None
```

### grafo_dto.py:

```python
class NoDTO(BaseModel):
    id: str
    tipo: str            # "empresa" | "socio"
    label: str
    score: int | None = None
    qtd_alertas: int | None = None

class ArestaDTO(BaseModel):
    source: str
    target: str
    tipo: str            # "socio_de"
    label: str | None = None

class GrafoDTO(BaseModel):
    nos: list[NoDTO]
    arestas: list[ArestaDTO]
    truncado: bool = False   # True se > 50 nós
```

### stats_dto.py:

```python
class FonteMetadataDTO(BaseModel):
    ultima_atualizacao: str | None
    registros: int

class StatsDTO(BaseModel):
    total_fornecedores: int
    total_contratos: int
    total_alertas: int
    fontes: dict[str, FonteMetadataDTO]
```

### export_dto.py:

```python
from typing import Literal

class ExportRequestDTO(BaseModel):
    formato: Literal["csv", "json", "pdf"]
```

---

## Step 2 — Repositórios novos (Alerta, Stats, Contrato ampliado, Grafo)

**Depende de:** Step 1
**Arquivos:**
- `api/infrastructure/repositories/duckdb_alerta_repo.py` (novo)
- `api/infrastructure/repositories/duckdb_stats_repo.py` (novo)
- `api/infrastructure/repositories/duckdb_contrato_repo.py` (editar — adicionar `listar`)
- `api/infrastructure/repositories/duckdb_societario_repo.py` (editar — adicionar `grafo_2_niveis`)
- `api/infrastructure/repositories/duckdb_fornecedor_repo.py` (editar — adicionar `contar_total`)

### duckdb_alerta_repo.py:

```python
class DuckDBAlertaRepo:
    def listar_feed(self, limit: int, offset: int) -> list[dict]:
        """JOIN fato_alerta_critico + dim_fornecedor + dim_socio
        ORDER BY detectado_em DESC"""

    def listar_por_tipo(self, tipo: str, limit: int, offset: int) -> list[dict]:
        """WHERE tipo_alerta = ? ORDER BY detectado_em DESC"""

    def contar(self) -> int:
        """SELECT count(*) FROM fato_alerta_critico"""
```

SQL: prepared statements (`?`), JOIN dim_fornecedor para CNPJ/razão, LEFT JOIN dim_socio para nome do sócio.

### duckdb_stats_repo.py:

```python
class DuckDBStatsRepo:
    def obter_stats(self) -> dict:
        """Agrega contagens de dim_fornecedor, fato_contrato, fato_alerta_critico.
        Fontes metadata: por enquanto retorna counts — freshness real vem do pipeline."""
```

### duckdb_contrato_repo.py — adicionar:

```python
def listar(self, limit: int, offset: int,
           cnpj: str | None = None, orgao_codigo: str | None = None) -> list[Contrato]:
    """Filtro opcional por CNPJ e/ou orgao. LIMIT/OFFSET com prepared statements."""
```

### duckdb_societario_repo.py — adicionar:

```python
def grafo_2_niveis(self, cnpj: CNPJ, max_nos: int = 50) -> tuple[list[dict], list[dict]]:
    """CTE recursiva: nível 0 = fornecedor, nível 1 = sócios e suas empresas,
    nível 2 = sócios dessas empresas e suas empresas. Retorna (nós, arestas).
    Limita a max_nos nós."""
```

SQL da CTE (2 níveis via bridge_fornecedor_socio):
```sql
-- Nível 0: fornecedor original
-- Nível 1: sócios do fornecedor + empresas desses sócios
-- Nível 2: sócios dessas empresas + empresas desses sócios
WITH RECURSIVE grafo AS (
    -- seed: fornecedor original
    SELECT df.pk_fornecedor, df.cnpj, df.razao_social, 0 as nivel
    FROM dim_fornecedor df WHERE df.cnpj = ?
    UNION
    -- expansão: sócios → empresas
    SELECT df2.pk_fornecedor, df2.cnpj, df2.razao_social, g.nivel + 1
    FROM grafo g
    JOIN bridge_fornecedor_socio bfs1 ON g.pk_fornecedor = bfs1.fk_fornecedor
    JOIN bridge_fornecedor_socio bfs2 ON bfs1.fk_socio = bfs2.fk_socio
    JOIN dim_fornecedor df2 ON bfs2.fk_fornecedor = df2.pk_fornecedor
    WHERE g.nivel < 2 AND df2.pk_fornecedor != g.pk_fornecedor
)
SELECT DISTINCT * FROM grafo LIMIT ?
```

### duckdb_fornecedor_repo.py — adicionar:

```python
def contar_total(self) -> int:
    """SELECT count(*) FROM dim_fornecedor"""
```

---

## Step 3 — Services novos (Ranking, Busca, Grafo, Export)

**Depende de:** Steps 1, 2
**Arquivos:**
- `api/application/services/ranking_service.py` (novo)
- `api/application/services/busca_service.py` (novo)
- `api/application/services/grafo_service.py` (novo)
- `api/application/services/export_service.py` (novo)

### ranking_service.py:

Thin wrapper: chama `fornecedor_repo.ranking_por_score(limit, offset)`, mapeia para `FornecedorResumoDTO`.

### busca_service.py:

Chama `fornecedor_repo.buscar_por_nome_ou_cnpj(query, limit)`, mapeia para `FornecedorResumoDTO`.

### grafo_service.py:

Chama `societario_repo.grafo_2_niveis(cnpj, max_nos=50)`, mapeia para `GrafoDTO`.

### export_service.py:

Recebe `FichaCompletaDTO` + formato:
- `json`: retorna o DTO serializado
- `csv`: gera CSV com seções (dados cadastrais, contratos, sócios, alertas)
- `pdf`: placeholder que retorna 501 Not Implemented (requer `pdf_generator.py` que é infra pesada)

---

## Step 4 — Rate Limit Middleware

**Depende de:** nada (paralelo)
**Arquivo:** `api/interfaces/api/middleware/rate_limit.py`

Implementação simples com dicionário in-memory (sem Redis — o projeto roda read-only DuckDB):

```python
import time
from collections import defaultdict
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from api.infrastructure.config import get_settings

class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self._requests: dict[str, list[float]] = defaultdict(list)

    async def dispatch(self, request: Request, call_next):
        # API key bypass
        api_key = request.headers.get("X-API-Key")
        if api_key:  # qualquer chave válida bypassa (validação real em fase futura)
            return await call_next(request)

        settings = get_settings()
        client_ip = request.client.host if request.client else "unknown"
        now = time.time()
        window = 60.0

        # Limpar requests antigos
        self._requests[client_ip] = [
            t for t in self._requests[client_ip] if now - t < window
        ]

        if len(self._requests[client_ip]) >= settings.rate_limit_per_minute:
            return Response(
                content='{"detail": "Rate limit excedido. Tente novamente em 1 minuto."}',
                status_code=429,
                media_type="application/json",
            )

        self._requests[client_ip].append(now)
        return await call_next(request)
```

Registrar em `main.py` ANTES do CORS middleware. Desabilitar em testes via variável de ambiente `API_RATE_LIMIT_PER_MINUTE=0` (0 = sem limite).

---

## Step 5 — Routes (8 endpoints)

**Depende de:** Steps 1-4
**Arquivos:** todos os route files + `main.py` (registrar routers)

### 5a. ranking_routes.py — `GET /api/fornecedores/ranking`

```python
@router.get("/fornecedores/ranking", response_model=list[FornecedorResumoDTO])
def get_ranking(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
): ...
```

**ATENÇÃO:** este router deve ser registrado ANTES do fornecedor_routes para evitar que `/fornecedores/ranking` case com `/fornecedores/{cnpj_raw}`.

### 5b. alerta_routes.py — `GET /api/alertas` e `GET /api/alertas/{tipo}`

```python
@router.get("/alertas", response_model=list[AlertaFeedItemDTO])
def get_alertas_feed(limit, offset): ...

@router.get("/alertas/{tipo}", response_model=list[AlertaFeedItemDTO])
def get_alertas_por_tipo(tipo: str, limit, offset):
    # Validar tipo contra TipoAlerta enum → 422 se inválido
```

### 5c. busca_routes.py — `GET /api/busca`

```python
@router.get("/busca", response_model=list[FornecedorResumoDTO])
def buscar(
    q: str = Query(..., min_length=1, max_length=200),
    limit: int = Query(default=20, ge=1, le=100),
): ...
```

### 5d. contrato_routes.py — `GET /api/contratos`

```python
@router.get("/contratos", response_model=list[ContratoResumoDTO])
def get_contratos(
    cnpj: str | None = None,
    orgao_codigo: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
): ...
```

### 5e. grafo_routes.py — `GET /api/fornecedores/{cnpj}/grafo`

```python
@router.get("/fornecedores/{cnpj_raw}/grafo", response_model=GrafoDTO)
def get_grafo(cnpj_raw: str): ...
    # Validar CNPJ → 422
    # Buscar grafo 2 níveis, max 50 nós
    # 404 se fornecedor não existe
```

### 5f. export_routes.py — `GET /api/fornecedores/{cnpj}/export`

```python
@router.get("/fornecedores/{cnpj_raw}/export")
def export_ficha(
    cnpj_raw: str,
    formato: Literal["csv", "json", "pdf"] = Query(...),
): ...
    # json → retorna FichaCompletaDTO como JSON
    # csv → retorna StreamingResponse com text/csv
    # pdf → retorna 501 Not Implemented (placeholder)
```

### 5g. orgao_routes.py — `GET /api/orgaos/{codigo}/dashboard`

```python
@router.get("/orgaos/{codigo}/dashboard")
def get_dashboard_orgao(codigo: str): ...
    # Resumo: nome, total contratado, qtd fornecedores
    # Top 10 fornecedores por valor
```

### 5h. stats_routes.py — `GET /api/stats`

```python
@router.get("/stats", response_model=StatsDTO)
def get_stats(): ...
```

### 5i. main.py — registrar todos os routers

Ordem importa: ranking_routes ANTES de fornecedor_routes (para `/fornecedores/ranking` não casar com `{cnpj_raw}`).

```python
from api.interfaces.api.routes.ranking_routes import router as ranking_router
from api.interfaces.api.routes.alerta_routes import router as alerta_router
from api.interfaces.api.routes.busca_routes import router as busca_router
from api.interfaces.api.routes.contrato_routes import router as contrato_router
from api.interfaces.api.routes.grafo_routes import router as grafo_router
from api.interfaces.api.routes.export_routes import router as export_router
from api.interfaces.api.routes.orgao_routes import router as orgao_router
from api.interfaces.api.routes.stats_routes import router as stats_router

# ranking ANTES de fornecedor (path conflict)
app.include_router(ranking_router, prefix="/api")
app.include_router(fornecedor_router, prefix="/api")
app.include_router(alerta_router, prefix="/api")
app.include_router(busca_router, prefix="/api")
app.include_router(contrato_router, prefix="/api")
app.include_router(grafo_router, prefix="/api")
app.include_router(export_router, prefix="/api")
app.include_router(orgao_router, prefix="/api")
app.include_router(stats_router, prefix="/api")
```

---

## Step 6 — Dependencies (DI para novos services)

**Depende de:** Steps 2, 3
**Arquivo:** `api/interfaces/api/dependencies.py`

Adicionar factory functions:

```python
def get_alerta_repo() -> DuckDBAlertaRepo: ...
def get_ranking_service() -> RankingService: ...
def get_busca_service() -> BuscaService: ...
def get_grafo_service() -> GrafoService: ...
def get_export_service() -> ExportService: ...
def get_stats_repo() -> DuckDBStatsRepo: ...
def get_contrato_repo() -> DuckDBContratoRepo: ...
def get_orgao_dashboard() -> ...:  # inline no route, sem service dedicado
```

---

## Step 7 — Fixtures de teste (expandir conftest de integração)

**Depende de:** nada (paralelo)
**Arquivo:** `tests/integration/conftest.py`

Adicionar dados ao `test_db`:

```python
# --- Candidato (para futura doação) ---
conn.execute("""
    INSERT INTO dim_candidato VALUES
    (1, 'Dep. Fulano', 'hmac_candidato1', 'PXX', 'Deputado Federal', 'SP', 2022)
""")

# --- Doação (para futuro alerta DOACAO_PARA_CONTRATANTE) ---
conn.execute("""
    INSERT INTO fato_doacao VALUES
    (1, 1, NULL, 1, 1, 15000.00, 'Transferência', 2022)
""")

# --- Mais um orgão para teste de dashboard ---
conn.execute("""
    INSERT INTO dim_orgao VALUES
    (2, '54000', 'Ministerio da Fazenda', 'MF', 'Executivo', 'Federal', 'DF')
""")

# --- Contrato do fornecedor 2 (para ranking ter dados variados) ---
conn.execute("""
    INSERT INTO fato_contrato VALUES
    (3, 2, 2, 1, NULL, 1000000.00, 'Compra de material', 'PE-003/2025', '2025-06-20', '2026-06-20'),
    (4, 2, 2, 1, NULL, 500000.00, 'Compra de equipamento', 'PE-003/2025', '2025-07-01', '2026-07-01'),
    (5, 2, 1, 1, NULL, 500000.00, 'Servicos gerais', 'PE-004/2025', '2025-08-01', '2026-08-01')
""")

# --- Score detalhe (para stats) ---
conn.execute("""
    INSERT INTO fato_score_detalhe VALUES
    (1, 1, 'CAPITAL_SOCIAL_BAIXO', 15, 'Capital desproporcional', 'capital=1000', CURRENT_TIMESTAMP)
""")
```

---

## Step 8 — Testes de integração (8 arquivos)

**Depende de:** Steps 5, 7
**Arquivos:**

### test_api_ranking.py (~5 testes):
- `test_ranking_retorna_200_com_lista`
- `test_ranking_ordenado_por_score_desc`
- `test_ranking_respeita_limit`
- `test_ranking_limit_maximo_100`
- `test_ranking_offset`

### test_api_alertas.py (~5 testes):
- `test_alertas_feed_retorna_200`
- `test_alertas_ordenados_por_detectado_em_desc`
- `test_alertas_por_tipo_valido`
- `test_alertas_tipo_invalido_retorna_422`
- `test_alertas_contem_cnpj_e_razao_social`

### test_api_busca.py (~4 testes):
- `test_busca_por_nome_retorna_resultado`
- `test_busca_por_cnpj_retorna_resultado`
- `test_busca_query_vazia_retorna_422`
- `test_busca_query_muito_longa_retorna_422`

### test_api_grafo.py (~4 testes):
- `test_grafo_retorna_nos_e_arestas`
- `test_grafo_contem_tipo_empresa_e_socio`
- `test_grafo_cnpj_invalido_retorna_422`
- `test_grafo_cnpj_inexistente_retorna_404`

### test_api_export.py (~4 testes):
- `test_export_json_retorna_200`
- `test_export_csv_retorna_text_csv`
- `test_export_pdf_retorna_501`
- `test_export_formato_invalido_retorna_422`

### test_api_orgao.py (~3 testes):
- `test_dashboard_orgao_retorna_200`
- `test_dashboard_orgao_contem_top_fornecedores`
- `test_dashboard_orgao_inexistente_retorna_404`

### test_api_stats.py (~3 testes):
- `test_stats_retorna_200`
- `test_stats_contem_totais`
- `test_stats_contem_fontes`

### test_rate_limit.py (~3 testes):
- `test_rate_limit_permite_dentro_do_limite`
- `test_rate_limit_bloqueia_apos_limite`
- `test_rate_limit_bypass_com_api_key`

---

## Grafo de Dependências

```
Step 1 (DTOs)
  └→ Step 2 (Repos) ─→ Step 3 (Services)
                              └→ Step 5 (Routes) ← Step 6 (Dependencies)
Step 4 (Rate Limit) ──────────→ Step 5 (main.py)
Step 7 (Fixtures) ────────────→ Step 8 (Testes)
```

Steps 1, 4, 7 são independentes entre si — podem ser implementados em paralelo.

---

## Arquivos a Editar/Criar (resumo)

| Step | Arquivos | Tipo |
|------|----------|------|
| 1 | `fornecedor_dto.py`, `alerta_dto.py` (editar); `grafo_dto.py`, `stats_dto.py`, `export_dto.py` (criar) | DTOs |
| 2 | `duckdb_alerta_repo.py`, `duckdb_stats_repo.py` (criar); `duckdb_contrato_repo.py`, `duckdb_societario_repo.py`, `duckdb_fornecedor_repo.py` (editar) | Repos |
| 3 | `ranking_service.py`, `busca_service.py`, `grafo_service.py`, `export_service.py` (criar) | Services |
| 4 | `middleware/rate_limit.py` (criar) | Middleware |
| 5 | `ranking_routes.py`, `alerta_routes.py`, `busca_routes.py`, `contrato_routes.py`, `grafo_routes.py`, `export_routes.py`, `orgao_routes.py`, `stats_routes.py` (criar); `main.py` (editar) | Routes |
| 6 | `dependencies.py` (editar) | DI |
| 7 | `tests/integration/conftest.py` (editar) | Fixtures |
| 8 | 8 arquivos `test_api_*.py` + `test_rate_limit.py` (criar) | Testes |

**Total: ~25 arquivos, ~31 testes de integração novos.**

---

## Verificação End-to-End

```bash
# 1. Testes de domínio (não devem quebrar)
python -m pytest tests/domain/ -v
# Esperado: 54 passando (inalterados)

# 2. Testes de integração (novos + existentes)
python -m pytest tests/integration/ -v
# Esperado: ~39 testes passando (8 existentes + ~31 novos)

# 3. Lint
ruff check api/ tests/

# 4. Todos
python -m pytest tests/ -v
# Esperado: ~85 testes passando

# 5. Teste manual
uvicorn api.interfaces.api.main:app --reload
curl http://localhost:8000/api/fornecedores/ranking?limit=10
curl http://localhost:8000/api/alertas
curl http://localhost:8000/api/busca?q=Empresa
curl http://localhost:8000/api/stats
curl http://localhost:8000/api/fornecedores/11222333000181/grafo
curl http://localhost:8000/api/fornecedores/11222333000181/export?formato=json
curl http://localhost:8000/api/orgaos/26000/dashboard
```
