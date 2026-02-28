# Plano de Implementação — Testa de Ferro

## Status das Fases

| Fase | Status | Testes | Descrição |
|------|--------|--------|-----------|
| **Fase 1** | CONCLUÍDA | 54 domínio + 8 integração = 62 | Primeiro vertical slice E2E: `GET /api/fornecedores/{cnpj}` |
| **Fase 2a** | CONCLUÍDA | +31 integração = 93 total | Todos endpoints REST + rate limiting |
| **Fase 2b** | CONCLUÍDA | +20 domínio = 113 total | Alertas e indicadores restantes + bounded contexts doação/servidor |
| **Fase 3** | CONCLUÍDA | 23 frontend + 27 todo = 50 | Frontend completo: types, services, hooks, 8 páginas, router |
| **Fase 4** | FUTURA | — | Pipeline de dados |

---

## O que já funciona (Fases 1 + 2a)

### Domínio (api/domain/)
- **fornecedor**: CNPJ VO, RazaoSocial, CapitalSocial, Endereco, Fornecedor (aggregate root), AlertaCritico, ScoreDeRisco, IndicadorCumulativo, enums completos
- **contrato**: Contrato, ValorContrato, ModalidadeLicitacao, NumeroLicitacao, Protocol ContratoRepository
- **sancao**: Sancao (vigente/expirada), TipoSancao, Protocol SancaoRepository
- **societario**: Socio, VinculoSocietario, CPF, CPFMascarado, QualificacaoSocio, Protocol SocietarioRepository
- **doacao**: VAZIO (stub)
- **servidor**: VAZIO (stub)

### Application (api/application/)
- **alerta_service**: `detectar_alertas()` — SOCIO_SERVIDOR_PUBLICO, EMPRESA_SANCIONADA_CONTRATANDO
- **score_service**: `calcular_score_cumulativo()` — CAPITAL_SOCIAL_BAIXO, EMPRESA_RECENTE, SANCAO_HISTORICA
- **ficha_service**: orquestra repos + core para montar FichaCompletaDTO
- **ranking_service**, **busca_service**, **grafo_service**, **export_service**: thin wrappers
- **DTOs**: FichaCompletaDTO, FornecedorResumoDTO, AlertaCriticoDTO, AlertaFeedItemDTO, ScoreDTO, ContratoResumoDTO, SocioDTO, SancaoDTO, GrafoDTO, StatsDTO, ExportRequestDTO

### Infraestrutura (api/infrastructure/)
- **Config**, **DuckDB connection** (read_only, singleton)
- **Repos**: DuckDBFornecedorRepo, DuckDBContratoRepo, DuckDBSancaoRepo, DuckDBSocietarioRepo (com grafo_2_niveis), DuckDBAlertaRepo, DuckDBStatsRepo
- **Vazios**: hmac_service.py, pdf_generator.py, duckdb_doacao_repo.py, duckdb_servidor_repo.py

### API (api/interfaces/)
- **9 endpoints**: fornecedor ficha, ranking, alertas feed, alertas por tipo, busca, contratos, grafo, export (json/csv/pdf), orgao dashboard, stats
- **Middleware**: rate_limit (60 req/min, API key bypass), security headers (nosniff, DENY, referrer-policy), CORS
- **Dependencies**: DI completo para todos services/repos

### Frontend (web/)
- **Tooling**: Vite + React 18 + TypeScript strict + Tailwind + Vitest configurados
- **Types**: 10 arquivos espelhando DTOs do backend (fornecedor, alerta, score, contrato, grafo, doacao, sancao, orgao, stats, api)
- **Services**: api.ts (fetch wrapper) + 6 services tipados (fornecedor, alerta, contrato, orgao, busca, stats)
- **Hooks**: useApi (discriminated union state), usePagination, useDebounce
- **Lib**: formatters (CNPJ, currency, date), colors (faixa→cor, severidade→cor), constants (labels, pesos)
- **Layout**: Header (nav + busca rápida), Footer (disclaimer), PageContainer
- **UI primitivos**: Button, Badge, Card, Table<T>, Pagination, Loading, ErrorState, EmptyState
- **Componentes domínio**: ScoreBadge, AlertaCriticoBadge, SeveridadeBadge, CNPJFormatado, ValorMonetario, FreshnessBanner
- **8 páginas completas**: Home, FichaFornecedor (16 sub-componentes), Ranking, Busca, Alertas, GrafoSocietario, DashboardOrgao, Metodologia
- **Router**: createBrowserRouter com 8 rotas aninhadas no layout App
- **105 de 109 arquivos implementados** (4 restantes: charts Nivo + Sidebar — dependem de libs externas)

### Testes
- **74 domínio** (puros, sem IO, <0.1s)
- **39 integração** (API + DuckDB in-memory)
- **113 backend total**, lint limpo
- **23 frontend** (FichaFornecedor) + 27 todo stubs
- **tsc --noEmit**: 0 erros (strict mode)
- **vite build**: 267 kB JS + 21 kB CSS

---

# Fase 2b — Alertas e Indicadores Restantes + Bounded Contexts

## Contexto

A API está navegável com 9 endpoints funcionais, mas os serviços de alertas e score só implementam um subconjunto das regras de negócio. Faltam 4 tipos de alerta e 6 indicadores de score. Os bounded contexts `doacao` e `servidor` estão vazios.

**Objetivo:** Completar a lógica de domínio — todos os alertas e indicadores do CLAUDE.md implementados com TDD, mais as entidades doação/servidor necessárias.

**Escopo explicitamente excluído:**
- Pipeline de download/ingestão de dados
- Frontend (componentes, páginas)
- PDF export (permanece 501)
- HMAC service (infra do pipeline, não da API de leitura)

---

## Step 1 — Bounded Context: Doacao (TDD)

**Depende de:** nada
**Arquivos:**
- `api/domain/doacao/value_objects.py`
- `api/domain/doacao/entities.py`
- `api/domain/doacao/repository.py`
- `tests/domain/test_doacao_entity.py`

### 1a. Testes

```python
# tests/domain/test_doacao_entity.py
def test_doacao_acima_threshold_materialidade(): ...
def test_doacao_abaixo_threshold_nao_e_material(): ...
def test_doacao_sem_valor_nao_e_material(): ...
```

### 1b. Implementação

```python
# api/domain/doacao/value_objects.py
@dataclass(frozen=True)
class ValorDoacao:
    valor: Decimal  # nunca float, nunca negativo

@dataclass(frozen=True)
class AnoCampanha:
    valor: int  # 2018, 2020, 2022, ...

# api/domain/doacao/entities.py
@dataclass(frozen=True)
class DoacaoEleitoral:
    fornecedor_cnpj: CNPJ | None
    socio_cpf_hmac: str | None
    candidato_nome: str
    candidato_partido: str
    candidato_cargo: str
    valor: ValorDoacao
    ano_eleicao: int

    def material(self, threshold: Decimal = Decimal("10000")) -> bool:
        return self.valor.valor > threshold

# api/domain/doacao/repository.py
class DoacaoRepository(Protocol):
    def listar_por_fornecedor(self, cnpj: CNPJ) -> list[DoacaoEleitoral]: ...
```

---

## Step 2 — Bounded Context: Servidor (entidade mínima)

**Depende de:** nada (paralelo com Step 1)
**Arquivos:**
- `api/domain/servidor/value_objects.py`
- `api/domain/servidor/entities.py`
- `api/domain/servidor/repository.py`

Nota: o match servidor×sócio já está resolvido pelo campo `is_servidor_publico` em Socio. Aqui criamos as entidades para completude do modelo, mas a lógica de alerta usa Socio diretamente (já implementado).

```python
# api/domain/servidor/value_objects.py
@dataclass(frozen=True)
class Cargo:
    valor: str

@dataclass(frozen=True)
class OrgaoLotacao:
    valor: str

# api/domain/servidor/entities.py
@dataclass(frozen=True)
class ServidorPublico:
    cpf_hmac: str
    nome: str
    cargo: str | None = None
    orgao_lotacao: str | None = None

# api/domain/servidor/repository.py
class ServidorRepository(Protocol):
    def buscar_por_cpf_hmac(self, cpf_hmac: str) -> ServidorPublico | None: ...
```

---

## Step 3 — Alertas Restantes (TDD)

**Depende de:** Step 1 (DoacaoEleitoral)
**Arquivos:**
- `tests/domain/test_alertas.py` (expandir)
- `api/application/services/alerta_service.py` (expandir)

### Alertas a implementar:

| Alerta | Condição | Severidade |
|--------|----------|------------|
| DOACAO_PARA_CONTRATANTE | Doação > R$10k para candidato E contrato > R$500k com órgão vinculado | GRAVE |
| RODIZIO_LICITACAO | (placeholder — requer análise temporal complexa, simplificar na Fase 2b) | GRAVE |
| SOCIO_SANCIONADO_EM_OUTRA | Sócio que é sócio de outra empresa sancionada | GRAVE |
| TESTA_DE_FERRO | (placeholder — requer grafo + múltiplos indicadores, simplificar) | GRAVISSIMO |

### 3a. Testes novos em test_alertas.py

```python
# DOACAO_PARA_CONTRATANTE
def test_doacao_material_com_contrato_alto_gera_alerta(): ...
def test_doacao_abaixo_threshold_nao_gera_alerta(): ...
def test_doacao_sem_contrato_alto_nao_gera_alerta(): ...

# SOCIO_SANCIONADO_EM_OUTRA (se dados disponíveis via Socio.is_sancionado)
def test_socio_sancionado_gera_alerta_grave(): ...
def test_socio_nao_sancionado_nao_gera_alerta(): ...
```

### 3b. Implementação

Adicionar ao `alerta_service.py`:
- `_detectar_doacao_para_contratante(fornecedor, doacoes, contratos)`
- `_detectar_socio_sancionado(fornecedor, socios)`

Nota: RODIZIO_LICITACAO e TESTA_DE_FERRO são complexos e envolvem análise temporal/grafo. Na Fase 2b, criar stubs documentados que retornam `[]` com ADR explicando que a implementação completa requer dados do pipeline.

---

## Step 4 — Indicadores de Score Restantes (TDD)

**Depende de:** nada (paralelo com Step 3)
**Arquivos:**
- `tests/domain/test_score.py` (expandir)
- `api/application/services/score_service.py` (expandir)

### Indicadores a implementar:

| Indicador | Peso | Condição |
|-----------|------|----------|
| SANCAO_HISTORICA | 5 | JÁ IMPLEMENTADO |
| CAPITAL_SOCIAL_BAIXO | 15 | JÁ IMPLEMENTADO |
| EMPRESA_RECENTE | 10 | JÁ IMPLEMENTADO |
| SOCIO_EM_MULTIPLAS_FORNECEDORAS | 20 | `Socio.qtd_empresas_governo >= 3` |
| FORNECEDOR_EXCLUSIVO | 10 | Fornecedor tem contratos com apenas 1 órgão |
| SEM_FUNCIONARIOS | 10 | Placeholder (dado não disponível sem pipeline) |
| CRESCIMENTO_SUBITO | 10 | Placeholder (requer série temporal) |
| CNAE_INCOMPATIVEL | 10 | Placeholder (requer tabela de mapeamento CNAE) |
| MESMO_ENDERECO | 15 | Placeholder (requer cruzamento com outros fornecedores) |

### 4a. Testes novos em test_score.py

```python
# SOCIO_EM_MULTIPLAS_FORNECEDORAS
def test_socio_em_3_ou_mais_empresas_ativa_indicador(): ...
def test_socio_em_2_empresas_nao_ativa(): ...

# FORNECEDOR_EXCLUSIVO
def test_fornecedor_todos_contratos_mesmo_orgao_ativa(): ...
def test_fornecedor_contratos_multiplos_orgaos_nao_ativa(): ...
```

### 4b. Implementação

Adicionar ao `score_service.py`:
- `_avaliar_socio_em_multiplas(socios)` — ativa se algum sócio tem `qtd_empresas_governo >= 3`
- `_avaliar_fornecedor_exclusivo(contratos)` — ativa se todos contratos têm mesmo `orgao_codigo`

Indicadores que dependem de dados do pipeline (CNAE_INCOMPATIVEL, MESMO_ENDERECO, SEM_FUNCIONARIOS, CRESCIMENTO_SUBITO) ficam como funções que retornam `None` com docstring explicando a dependência.

---

## Step 5 — Atualizar FichaService e DTOs para incluir doações

**Depende de:** Steps 1, 3
**Arquivos:**
- `api/application/services/ficha_service.py` (expandir para receber doacoes)
- `api/application/dtos/ficha_dto.py` (adicionar campo doacoes)
- `api/application/dtos/fornecedor_dto.py` (adicionar DoacaoDTO)

---

## Step 6 — Infra repos para doação (opcional)

**Depende de:** Step 1
**Arquivo:** `api/infrastructure/repositories/duckdb_doacao_repo.py`

SQL: `fato_doacao JOIN dim_candidato` com prepared statements.

---

## Step 7 — Testes de domínio: grafo service

**Depende de:** nada
**Arquivo:** `tests/domain/test_grafo_service.py`

Testar `api/domain/societario/services.py` (travessia 2 níveis, limite de nós). Alternativamente, documentar que a lógica de grafo está no repo (SQL) e não precisa de service de domínio puro na Fase 2b.

---

## Verificação End-to-End

```bash
# 1. Testes de domínio
python -m pytest tests/domain/ -v
# Esperado: ~70 testes (54 existentes + ~16 novos)

# 2. Testes de integração
python -m pytest tests/integration/ -v
# Esperado: 39 (inalterados — novos alertas/indicadores testados no domínio)

# 3. Todos
python -m pytest tests/ -v
# Esperado: ~109 testes

# 4. Lint
ruff check api/ tests/
```

---

## Arquivos a Editar/Criar (resumo Fase 2b)

| Step | Arquivos | Tipo |
|------|----------|------|
| 1 | `doacao/value_objects.py`, `entities.py`, `repository.py` + `test_doacao_entity.py` | domain+test |
| 2 | `servidor/value_objects.py`, `entities.py`, `repository.py` | domain |
| 3 | `test_alertas.py` (expandir), `alerta_service.py` (expandir) | test+impl |
| 4 | `test_score.py` (expandir), `score_service.py` (expandir) | test+impl |
| 5 | `ficha_service.py`, `ficha_dto.py`, `fornecedor_dto.py` (editar) | impl |
| 6 | `duckdb_doacao_repo.py` (criar) | infra |
| 7 | `test_grafo_service.py` (avaliar necessidade) | test |

**Total: ~12 arquivos, ~16 testes novos de domínio.**

---

# Fase 3 — Frontend (CONCLUÍDA)

## O que foi implementado

### 3a. Foundation — 19 arquivos
- **types/**: 10 arquivos espelhando DTOs (api, fornecedor, alerta, score, contrato, grafo, doacao, sancao, orgao, stats)
- **services/**: api.ts (apiFetch + apiFetchBlob, baseURL /api, ApiError) + 6 services tipados
- **hooks/**: useApi (discriminated union: idle|loading|success|error + refetch), usePagination, useDebounce
- **lib/**: formatters (CNPJ, currency, date, number), colors (FAIXA_COLORS, SEVERIDADE_COLORS), constants (labels, pesos)

### 3b. Layout + UI primitivos — 17 arquivos
- **Layout**: Header (nav + busca rápida), Footer (disclaimer + links), PageContainer (max-w-7xl)
- **UI**: Button (3 variants), Badge, Card+CardHeader, Table<T> (genérica), Pagination, Loading+LoadingSkeleton, ErrorState, EmptyState
- **Domínio**: ScoreBadge, AlertaCriticoBadge, SeveridadeBadge, CNPJFormatado (com link), ValorMonetario, FreshnessBanner

### 3c. Páginas — 67 arquivos (8 páginas, cada vertical slice completa)
- **Home**: AlertaFeed + AlertaFeedItem + ResumoGeral + hooks (useAlertasFeed, useStats)
- **FichaFornecedor** (16 arquivos): SecaoDadosCadastrais, SecaoAlertas (agrupamento por tipo), AlertaGrupo (details/summary), SecaoScore, SecaoContratos, SecaoSocios, SecaoSancoes, SecaoDoacoes, GrafoMini, ExportButtons, DisclaimerBanner, NotaOficial + hooks (useFicha, useExport) + types.ts — **23 testes passando**
- **Ranking**: RankingTabela + RankingFiltros (faixa, client-side) + useRanking
- **Busca**: BuscaInput (debounce 300ms) + ResultadoLista + useBusca (idle quando query < 2 chars, URL sync)
- **Alertas**: AlertaLista + AlertaFiltros (tipo server-side, severidade client-side) + useAlertas
- **GrafoSocietario**: GrafoCanvas (placeholder SVG), GrafoControles, GrafoLegenda, NoTooltip + hooks (useGrafo, useGrafoExpansion) + types.ts
- **DashboardOrgao**: OrgaoResumo + TopFornecedores (Table) + ContratosChart (placeholder) + useDashboardOrgao
- **Metodologia** (estática): ExplicacaoIndicadores, ExplicacaoAlertas, FontesDados, Limitacoes, Changelog

### 3d. Router + App shell — 3 arquivos
- **router.tsx**: createBrowserRouter com 8 rotas aninhadas
- **App.tsx**: Header + Outlet + Footer, flex min-h-screen
- **main.tsx**: RouterProvider + index.css (Tailwind directives)

### Pendências menores (4 arquivos vazios)
- `components/charts/ScoreGauge.tsx` — requer lib de charts (Nivo)
- `components/charts/ContratosTimeline.tsx` — requer lib de charts (Nivo)
- `components/charts/TreemapContratos.tsx` — requer lib de charts (Nivo)
- `components/layout/Sidebar.tsx` — não necessário no design atual

---

# Fase 4 — Pipeline (futuro)

Completamente fora de escopo das fases atuais. 12 test files vazios em `tests/pipeline/`. Todo o diretório `pipeline/` (exceto `output/schema.sql`) está por implementar.

Prioridade sugerida: CNPJ (Receita) → PNCP (contratos) → Sanções (CEIS/CNEP) → Servidores → TSE (doações) → Juntas Comerciais.
