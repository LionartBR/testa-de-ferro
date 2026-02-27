# CLAUDE.md — Testa de Ferro

> Plataforma para identificar empresas com perfil suspeito que vendem ao governo federal.

## Comandos

```bash
# Backend
cd api && python -m pytest                    # todos os testes
cd api && python -m pytest tests/domain/      # testes de domínio (roda em <5s)
cd api && python -m pytest --cov=testa_de_ferro    # cobertura
cd api && uvicorn testa_de_ferro.interfaces.api.main:app --reload  # dev server

# Pipeline
cd pipeline && python main.py                 # executa pipeline completo

# Frontend
cd web && npm run dev                         # dev server
cd web && npm run test                        # vitest
cd web && npm run typecheck                   # tsc --noEmit

# Tudo
docker compose up                             # sobe API + frontend + banco
```

## Stack

- **Python 3.12+** — Backend (FastAPI + Polars + DuckDB)
- **React 18 + TypeScript strict + Tailwind** — Frontend
- **DuckDB** — Banco analítico, arquivo local, read_only em produção
- **Parquet** — Formato intermediário do pipeline
- **Pytest** — Testes backend | **Vitest** — Testes frontend

## Segurança (obrigatório desde o dia 0)

Este é um projeto open source que lida com dados públicos sensíveis. Todo código mergeado deve seguir estas práticas.

### Secrets e Variáveis de Ambiente

- **Nunca** commitar segredos (`.env`, `CPF_HMAC_SALT`, API keys). Use `.env.example` com valores placeholder.
- `.env` está no `.gitignore`. Sem exceção. Se o CI precisa de secrets, usa GitHub Secrets / variáveis de ambiente do runner.
- O salt HMAC (`CPF_HMAC_SALT`) é a chave mais crítica do projeto — se vazar, todos os CPF-HMACs do `.duckdb` viram reversíveis. Tratar como credencial de produção.
- Antes de cada commit, verificar que nenhum secret foi staged (`git diff --cached` não deve conter tokens, salts ou senhas).

### Prevenção de Injection

- **SQL (DuckDB):** Toda query usa parâmetros preparados (`?` ou `$1`). Nunca interpolar variáveis em strings SQL.
  ```python
  # CERTO
  conn.execute("SELECT * FROM dim_fornecedor WHERE cnpj = ?", [cnpj])

  # ERRADO — injection via CNPJ malicioso
  conn.execute(f"SELECT * FROM dim_fornecedor WHERE cnpj = '{cnpj}'")
  ```
- **XSS (React):** Nunca usar `dangerouslySetInnerHTML`. Dados da API são exibidos via JSX (sanitizado por padrão). Se precisar renderizar HTML (ex: evidência de alerta), sanitizar com `DOMPurify`.
- **Path traversal:** Endpoints de export não devem aceitar paths arbitrários. O formato de saída é enum (`csv | json | pdf`), nunca um path do usuário.

### Validação de Input

- Toda entrada que chega pela API é validada no Pydantic model **antes** de tocar o domínio.
- CNPJ validado com dígitos verificadores. CPF idem. Rejeitar com 422 se inválido.
- Parâmetros de paginação (`limit`, `offset`) têm máximo hard-coded (ex: `limit` max 100). Nunca permitir `limit=999999`.
- Parâmetros de busca (`q`) têm tamanho máximo (ex: 200 chars) para evitar regex DoS.

### Dependências

- `pyproject.toml` e `package-lock.json` versionados. Instalar com versões pinadas.
- Rodar `npm audit` e `pip audit` no CI. Build falha se houver vulnerabilidade crítica.
- Dependabot ou Renovate habilitado no repositório para PRs automáticas de atualização.
- Mínimo de dependências. Cada dep nova é superfície de ataque — justificar no PR.

### Dados Pessoais e LGPD

- CPFs **nunca** em texto claro no `.duckdb` distribuído — somente HMAC (ver seção HMAC abaixo).
- CPFs **nunca** em logs, mensagens de erro ou stack traces. Logar apenas o HMAC ou os últimos 4 dígitos.
- Nomes de sócios/servidores são dados públicos da Receita/Portal da Transparência — armazenados em claro.
- Headers de resposta da API não expõem versão do servidor, stack traces ou informação interna.

### Configuração da API em Produção

- CORS restrito às origens do frontend (`http://localhost:5173` em dev, domínio real em prod). Nunca `*`.
- Rate limit (60 req/min por IP) ativo por padrão. Desabilitar apenas em testes.
- Sem modo debug em produção. `FastAPI(debug=False)`.
- Respostas de erro retornam mensagem genérica (ex: `{"detail": "Fornecedor não encontrado"}`), nunca stack traces.
- Headers de segurança: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: strict-origin-when-cross-origin`.

### Git e CI

- Branch `main` protegida: requer PR com review + CI verde.
- Pre-commit hooks obrigatórios:
  - `ruff check` + `ruff format` (Python)
  - `mypy --strict` (Python)
  - `eslint` + `tsc --noEmit` (TypeScript)
  - `detect-secrets` (detecta tokens/senhas acidentalmente commitados)
- CI roda: lint → type check → testes unitários → testes de integração → `pip audit` → `npm audit`.
- Docker images usam user não-root. Nunca rodar containers como root.

### SECURITY.md

O repositório deve conter um `SECURITY.md` com:
- Como reportar vulnerabilidades (email ou GitHub Security Advisories — nunca issue pública)
- Escopo: o que é considerado vulnerabilidade (SQL injection, XSS, leak de CPF, bypass de rate limit)
- Tempo de resposta esperado (ex: 72h para triagem)

---

## Princípios Obrigatórios

### 1. TDD — Test-Driven Development

Toda funcionalidade começa pelo teste. Sem exceção.

- Escreva o teste **antes** da implementação
- Teste falha → implemente o mínimo → teste passa → refatore
- Testes de domínio são **funções puras**: sem mock, sem IO, sem setup complexo
- Cobertura mínima de 80% em domínio e serviços
- Nomes de teste descrevem comportamento, não implementação:
  - `test_empresa_sancionada_vigente_gera_alerta` (bom)
  - `test_funcao_retorna_true` (ruim)

### 2. DDD — Domain-Driven Design

O domínio é a verdade central. Infraestrutura é detalhe.

- **6 Bounded Contexts:** fornecedor, contrato, societario, sancao, doacao, servidor
- Cada contexto tem: entities, value_objects, repository (interface)
- **Fornecedor é o Aggregate Root** — alertas e score são derivados dele
- Value Objects validam no construtor e são imutáveis (CNPJ, CPF, RazaoSocial)
- Repositories são interfaces no domínio, implementações concretas em infrastructure/
- Application services orquestram; domain services contêm lógica que cruza entidades

### 3. Type Safety

Nunca confie em primitivos genéricos para dados de domínio.

**Python (backend):**
- Use `dataclass(frozen=True)` para Value Objects
- Use `Enum` para tipos finitos (TipoAlerta, Severidade, TipoIndicador)
- Use `NewType` ou classes wrapper para CNPJ, CPF — nunca `str` cru
- Tipagem completa com `mypy --strict` passando sem erros
- `Decimal` para valores monetários, nunca `float`

**TypeScript (frontend):**
- `strict: true` no tsconfig, zero `any`
- Tipos de API gerados ou espelhados do backend (DTOs tipados)
- Discriminated unions para estados de componente (loading | error | success)
- Props de componentes sempre com interface explícita

### 4. ExMA — Explicit Modular Architecture (adaptado)

#### Pilar 1: Vertical Slice por Feature

Código organizado por funcionalidade, não por camada técnica.

```
# Backend — cada bounded context é um slice vertical
api/domain/fornecedor/         # entidades, VOs, interface de repo
api/application/services/      # orquestração entre contextos
api/infrastructure/repositories/  # implementações DuckDB

# Frontend — cada página/feature é auto-contida
web/src/pages/FichaFornecedor/
  ├── FichaFornecedor.tsx       # página
  ├── components/               # componentes locais
  ├── hooks/                    # hooks locais (useFicha, useGrafo)
  ├── types.ts                  # tipos locais
  └── FichaFornecedor.test.tsx  # testes da feature

# Pipeline — cada fonte é um slice
pipeline/sources/cnpj.py       # download + parse + validação do CNPJ
pipeline/sources/pncp.py       # idem para contratos
```

**Regra:** se precisar abrir mais de 3-4 arquivos para entender uma feature, refatore.

#### Pilar 2: Functional Core, Imperative Shell

Toda lógica de negócio é pura. IO fica na borda.

```python
# CORE (puro) — domain/ e application/services/
def calcular_score_cumulativo(fornecedor, contratos) -> ScoreDeRisco:
    """Função pura. Mesma entrada = mesma saída. Sem DB, sem IO."""
    ...

def detectar_alertas(fornecedor, sancoes, contratos) -> list[AlertaCritico]:
    """Função pura. Testável sem mock."""
    ...

# SHELL (impuro) — infrastructure/ e interfaces/
class DuckDBFornecedorRepo:
    """Lê do DuckDB. Implementa interface do domínio."""
    ...

@router.get("/fornecedores/{cnpj}")
async def get_ficha(cnpj: str):
    """Endpoint HTTP. Impuro: recebe request, chama core, retorna response."""
    ...
```

**Regra:** se uma função de domínio recebe conexão de banco ou faz `await`, ela está no lugar errado.

#### Pilar 3: Deep Modules

Interfaces simples, implementação rica por trás.

```python
# Interface simples (domínio)
class FornecedorRepository(Protocol):
    def buscar_por_cnpj(self, cnpj: CNPJ) -> Fornecedor | None: ...
    def ranking_por_score(self, limit: int) -> list[Fornecedor]: ...

# Implementação profunda (infraestrutura) — SQL complexo, cache, joins
class DuckDBFornecedorRepo:
    def buscar_por_cnpj(self, cnpj: CNPJ) -> Fornecedor | None:
        # 50 linhas de SQL com joins, parsing, hidratação
        ...
```

| Métrica                   | Limite       | Por quê                         |
| ------------------------- | ------------ | ------------------------------- |
| Complexidade ciclomática  | < 10/função  | Limita caminhos de execução     |
| Profundidade de nesting   | < 4 níveis   | Reduz carga cognitiva           |
| Linhas por arquivo        | 200-400      | Cabe na janela de contexto      |
| Linhas por função         | < 50         | Compreensível de uma vez        |

#### Pilar 4: Types as Documentation

```python
# Ruim — o que é cada str?
def processar(texto: str, inicio: int, fim: int, tipo: str) -> dict: ...

# Bom — tipos comunicam intent
def detectar_alerta(fornecedor: Fornecedor, sancoes: list[Sancao]) -> list[AlertaCritico]: ...
```

Se precisa de comentário para explicar o que um parâmetro significa, deveria ser um tipo.

#### Pilar 5: ADRs Inline

Documente decisões não-óbvias perto do código afetado.

```python
# ADR: Score e Alertas são dimensões INDEPENDENTES
#
# Contexto: Um fornecedor pode ter score 0 mas alerta GRAVÍSSIMO
# (ex: sócio é servidor público). O alerta NUNCA alimenta o score.
#
# Consequências:
#   - Feed de alertas e ranking por score são navegações separadas
#   - Ficha mostra ambos mas em seções distintas
#   - detectar_alertas() e calcular_score() nunca se chamam mutuamente
```

**Regra:** se escolheu A em vez de B por uma razão específica, documente. O AI vai sugerir B.

#### Pilar 6: Tests as Specification

```python
# O nome do teste É a especificação
def test_alerta_socio_servidor_publico_gera_gravissimo(): ...
def test_alerta_nao_contamina_score_cumulativo(): ...
def test_sancao_expirada_nao_gera_alerta_critico(): ...
def test_sancao_expirada_gera_indicador_cumulativo_peso_5(): ...
def test_doacao_abaixo_threshold_nao_gera_alerta(): ...
```

Alguém deve poder ler **só os nomes** dos testes e entender o que o sistema faz.

## Decisões Arquiteturais Não-Óbvias

### Sistema Dual: Alertas vs Score

As duas dimensões são **completamente independentes**. Não se alimentam mutuamente.

- **Alertas Críticos** — flags binárias. Detectou = alerta. Independe do score.
- **Score Cumulativo** — soma ponderada de indicadores fracos que combinados revelam padrão.
- Um fornecedor com score 0 e alerta GRAVÍSSIMO aparece no feed de alertas com destaque total.
- Um fornecedor com score 85 e 0 alertas aparece no ranking. Perfil de fachada.

### CPF: HMAC com Salt Secreto

CPFs nunca são armazenados em texto claro no `.duckdb` distribuído. Usa-se `HMAC-SHA256` com salt em variável de ambiente (`CPF_HMAC_SALT`). SHA-256 simples seria brute-forceable em minutos para 200M CPFs — HMAC torna inviável sem o salt.

### Match Servidor × Sócio

O CPF dos servidores é parcialmente mascarado (`***.222.333-**`). O match é por **nome completo + dígitos visíveis do CPF** batendo com o CPF completo do sócio. A probabilidade de homônimo com mesmos dígitos é ~nula.

### Grafo: 2 Níveis de Indireção

Pessoa → Holding → Empresa fornecedora. Travessia vai até 2 níveis para capturar CNPJs em cascata. Na UI, mostra 1 nível por padrão, expande sob demanda. Limite visual de 50 nós.

### Pipeline Atômico

O DuckDB só é substituído se **todas** as fontes completaram. Gerado em arquivo temporário → validação de completude → rename atômico. Nunca servir dados parciais.

### MESMO_ENDERECO sem Complemento

Match por logradouro + número, **sem** complemento (sala/andar). Aceita-se ruído em prédios comerciais — o indicador só pesa combinado com outros (score cumulativo, não alerta).

### CAPITAL_SOCIAL_BAIXO Cruzado com CNAE

O threshold de capital varia por setor. Empresas de serviço (TI, consultoria) operam legitimamente com capital baixo. O indicador cruza com CNAE antes de ativar.

### Sanções Expiradas

Sanção vigente (data_fim NULL ou futura) → alerta crítico GRAVÍSSIMO.
Sanção expirada → indicador cumulativo SANCAO_HISTORICA (peso 5). Nunca alerta.

### Doação: Threshold de Materialidade

Alerta DOACAO_PARA_CONTRATANTE só dispara quando doação > R$10.000 **E** contrato > R$500.000. Doações menores são registradas mas não geram alerta.

### CNAE_INCOMPATIVEL: Tabela Manual

Implementado via tabela de mapeamento manual curada (top 50 CNAEs → categorias de objeto). Sem NLP ou LLM no pipeline. Comunidade expande o mapeamento via contribuição.

### DuckDB: Read-Only + Múltiplos Workers

API abre DuckDB com `read_only=True`. Múltiplos workers do uvicorn podem coexistir. Zero escrita em produção — o banco é artefato do pipeline.

### Rate Limiting

60 req/min por IP sem autenticação. API key opcional para pesquisadores/jornalistas com limite maior.

## Estrutura de Pastas

```
testa-de-ferro/
│
├── pipeline/                                # OFFLINE — gera o .duckdb
│   ├── __init__.py
│   ├── main.py                              # Orquestrador: roda fontes → transform → build
│   ├── config.py                            # URLs das fontes, paths, timeouts
│   │
│   ├── sources/                             # Um subpacote por fonte de dados
│   │   ├── __init__.py
│   │   ├── base.py                          # Protocol SourcePipeline (download → parse → validate)
│   │   ├── cnpj/
│   │   │   ├── __init__.py
│   │   │   ├── download.py                  # Baixa CSVs compactados da Receita (~5GB)
│   │   │   ├── parse_empresas.py            # Parse CSV de empresas → Polars DataFrame
│   │   │   ├── parse_qsa.py                 # Parse QSA (sócios) → DataFrame com cpf, nome, qualificação
│   │   │   └── validate.py                  # Dedup, tipagem, rejeição de registros inválidos
│   │   ├── juntas_comerciais/
│   │   │   ├── __init__.py
│   │   │   ├── download.py                  # Baixa alterações societárias históricas
│   │   │   ├── parse.py                     # Parse diffs do QSA (entrada/saída de sócios)
│   │   │   └── validate.py
│   │   ├── pncp/
│   │   │   ├── __init__.py
│   │   │   ├── download.py                  # API REST do PNCP (paginação, retry)
│   │   │   ├── parse.py                     # Parse JSON → contratos + licitações
│   │   │   └── validate.py
│   │   ├── comprasnet/
│   │   │   ├── __init__.py
│   │   │   ├── download.py                  # CSVs históricos de dados.gov.br
│   │   │   ├── parse.py
│   │   │   └── validate.py
│   │   ├── tse/
│   │   │   ├── __init__.py
│   │   │   ├── download.py                  # Prestação de contas por ano eleitoral
│   │   │   ├── parse.py                     # Parse doações eleitorais (CPF/CNPJ doador → candidato)
│   │   │   └── validate.py
│   │   ├── sancoes/
│   │   │   ├── __init__.py
│   │   │   ├── download.py                  # Download CEIS + CNEP + CEPIM
│   │   │   ├── parse_ceis.py                # Empresas impedidas
│   │   │   ├── parse_cnep.py                # Penalidades Lei Anticorrupção
│   │   │   ├── parse_cepim.py               # Entidades sem fins lucrativos
│   │   │   └── validate.py
│   │   └── servidores/
│   │       ├── __init__.py
│   │       ├── download.py                  # CSVs do Portal da Transparência
│   │       ├── parse.py                     # Parse com CPF mascarado (***. 222.333-**)
│   │       └── validate.py
│   │
│   ├── staging/                             # Dados limpos em Parquet
│   │   ├── __init__.py
│   │   └── parquet_writer.py                # Escrita padronizada com schema enforcement
│   │
│   ├── transform/                           # Cruzamentos e cálculos
│   │   ├── __init__.py
│   │   ├── hmac_cpf.py                      # HMAC-SHA256 com salt (CPF_HMAC_SALT)
│   │   ├── match_servidor_socio.py          # Match nome + dígitos visíveis do CPF
│   │   ├── grafo_societario.py              # Monta grafo com travessia de 2 níveis
│   │   ├── cruzamentos.py                   # Fornecedores × sócios × servidores × sanções
│   │   ├── cnae_mapping.py                  # Tabela manual: top 50 CNAEs → categorias de objeto
│   │   ├── alertas.py                       # Detecta alertas críticos (regras binárias)
│   │   └── score.py                         # Calcula score cumulativo (soma ponderada)
│   │
│   ├── output/
│   │   ├── __init__.py
│   │   ├── schema.sql                       # DDL do star schema (executado no build)
│   │   ├── build_duckdb.py                  # Build atômico: temp → validação → rename
│   │   └── completude.py                    # Valida que todas as fontes completaram
│   │
│   └── data/                                # .gitignore — NÃO versionado
│       ├── raw/                             # CSVs/JSONs baixados das fontes
│       ├── staging/                         # Parquets limpos e tipados
│       └── output/                          # .duckdb final
│
├── api/                                     # ONLINE — lê o .duckdb (read_only=True)
│   ├── __init__.py
│   │
│   ├── domain/                              # PURO — zero IO, zero dependência externa
│   │   ├── __init__.py
│   │   ├── fornecedor/
│   │   │   ├── __init__.py
│   │   │   ├── entities.py                  # Fornecedor (Aggregate Root), AlertaCritico
│   │   │   ├── value_objects.py             # CNPJ, RazaoSocial, CapitalSocial, Endereco
│   │   │   ├── enums.py                     # TipoAlerta, Severidade, TipoIndicador, FaixaRisco, SituacaoCadastral
│   │   │   ├── score.py                     # ScoreDeRisco (VO), IndicadorCumulativo (VO)
│   │   │   └── repository.py               # Protocol FornecedorRepository
│   │   ├── contrato/
│   │   │   ├── __init__.py
│   │   │   ├── entities.py                  # Contrato, Licitacao
│   │   │   ├── value_objects.py             # ValorContrato, ModalidadeLicitacao, NumeroLicitacao
│   │   │   └── repository.py               # Protocol ContratoRepository
│   │   ├── societario/
│   │   │   ├── __init__.py
│   │   │   ├── entities.py                  # Socio, VinculoSocietario
│   │   │   ├── value_objects.py             # CPF, QualificacaoSocio, PercentualCapital
│   │   │   ├── services.py                  # GrafoSocietarioService (travessia 2 níveis)
│   │   │   └── repository.py               # Protocol SocietarioRepository
│   │   ├── sancao/
│   │   │   ├── __init__.py
│   │   │   ├── entities.py                  # Sancao (com data_inicio, data_fim nullable)
│   │   │   ├── value_objects.py             # TipoSancao, OrgaoSancionador
│   │   │   └── repository.py               # Protocol SancaoRepository
│   │   ├── doacao/
│   │   │   ├── __init__.py
│   │   │   ├── entities.py                  # DoacaoEleitoral
│   │   │   ├── value_objects.py             # AnoCampanha, Candidato, ValorDoacao
│   │   │   └── repository.py               # Protocol DoacaoRepository
│   │   └── servidor/
│   │       ├── __init__.py
│   │       ├── entities.py                  # ServidorPublico
│   │       ├── value_objects.py             # Cargo, OrgaoLotacao, CPFMascarado
│   │       └── repository.py               # Protocol ServidorRepository
│   │
│   ├── application/                         # PURO — orquestração entre contextos
│   │   ├── __init__.py
│   │   ├── services/
│   │   │   ├── __init__.py
│   │   │   ├── alerta_service.py            # Detecta alertas (funções puras, sem IO)
│   │   │   ├── score_service.py             # Calcula score cumulativo (funções puras)
│   │   │   ├── ficha_service.py             # Monta ficha completa (orquestra repos)
│   │   │   ├── grafo_service.py             # Monta grafo de relacionamentos
│   │   │   ├── ranking_service.py           # Ranking por score + feed de alertas
│   │   │   ├── busca_service.py             # Full-text search por nome/CNPJ
│   │   │   └── export_service.py            # Gera CSV / JSON / PDF da ficha
│   │   └── dtos/
│   │       ├── __init__.py
│   │       ├── fornecedor_dto.py            # FornecedorResumoDTO, FornecedorListaDTO
│   │       ├── ficha_dto.py                 # FichaCompletaDTO (alertas + score + contratos + grafo)
│   │       ├── alerta_dto.py                # AlertaCriticoDTO, AlertaFeedItemDTO
│   │       ├── grafo_dto.py                 # GrafoDTO, NoDTO, ArestaDTO
│   │       ├── contrato_dto.py              # ContratoDTO, ContratoResumoDTO
│   │       ├── score_dto.py                 # ScoreDTO, IndicadorDTO
│   │       ├── export_dto.py                # ExportRequestDTO (formato: csv|json|pdf)
│   │       └── stats_dto.py                 # StatsDTO, FonteMetadataDTO (freshness)
│   │
│   ├── infrastructure/                      # IMPURO — IO, banco, config
│   │   ├── __init__.py
│   │   ├── duckdb_connection.py             # Singleton read_only=True, path configurável
│   │   ├── config.py                        # Settings: DUCKDB_PATH, CPF_HMAC_SALT, RATE_LIMIT
│   │   ├── hmac_service.py                  # HMAC de CPF para queries (usa salt do config)
│   │   ├── pdf_generator.py                 # Gera PDF da ficha (para exportação)
│   │   └── repositories/
│   │       ├── __init__.py
│   │       ├── duckdb_fornecedor_repo.py    # SQL: dim_fornecedor + joins + hidratação
│   │       ├── duckdb_contrato_repo.py      # SQL: fato_contrato + dim_orgao + dim_tempo
│   │       ├── duckdb_societario_repo.py    # SQL: bridge_fornecedor_socio + travessia
│   │       ├── duckdb_sancao_repo.py        # SQL: dim_sancao (vigentes vs expiradas)
│   │       ├── duckdb_doacao_repo.py        # SQL: fato_doacao + dim_candidato
│   │       ├── duckdb_servidor_repo.py      # SQL: match por nome + CPF parcial
│   │       ├── duckdb_alerta_repo.py        # SQL: fato_alerta_critico (feed, filtros)
│   │       └── duckdb_stats_repo.py         # SQL: metadata de freshness por fonte
│   │
│   └── interfaces/                          # IMPURO — HTTP, middleware
│       └── api/
│           ├── __init__.py
│           ├── main.py                      # FastAPI app, lifespan, CORS, error handlers
│           ├── dependencies.py              # Dependency injection (repos → services)
│           ├── routes/
│           │   ├── __init__.py
│           │   ├── fornecedor_routes.py     # GET /fornecedores/{cnpj}
│           │   ├── ranking_routes.py        # GET /fornecedores/ranking
│           │   ├── grafo_routes.py          # GET /fornecedores/{cnpj}/grafo
│           │   ├── export_routes.py         # GET /fornecedores/{cnpj}/export?formato=
│           │   ├── alerta_routes.py         # GET /alertas, GET /alertas/{tipo}
│           │   ├── contrato_routes.py       # GET /contratos
│           │   ├── orgao_routes.py          # GET /orgaos/{codigo}/dashboard
│           │   ├── busca_routes.py          # GET /busca?q=
│           │   └── stats_routes.py          # GET /stats (freshness metadata)
│           └── middleware/
│               ├── __init__.py
│               ├── cors.py                  # Origens permitidas
│               └── rate_limit.py            # 60 req/min por IP + API key opcional
│
├── web/                                     # SPA React + TypeScript strict + Tailwind
│   ├── public/
│   │   └── favicon.ico
│   ├── index.html
│   ├── package.json
│   ├── tsconfig.json                        # strict: true, zero any
│   ├── vite.config.ts
│   ├── tailwind.config.ts
│   ├── vitest.config.ts
│   │
│   └── src/
│       ├── App.tsx
│       ├── main.tsx
│       ├── router.tsx                       # React Router — rotas da aplicação
│       │
│       ├── pages/                           # VERTICAL SLICE — cada página é auto-contida
│       │   ├── Home/
│       │   │   ├── Home.tsx                 # Feed de alertas recentes + resumo geral
│       │   │   ├── Home.test.tsx
│       │   │   ├── components/
│       │   │   │   ├── AlertaFeed.tsx       # Lista paginada de alertas recentes
│       │   │   │   ├── AlertaFeedItem.tsx   # Card individual do alerta
│       │   │   │   └── ResumoGeral.tsx      # Números: total fornecedores, alertas, contratos
│       │   │   └── hooks/
│       │   │       ├── useAlertasFeed.ts    # Fetch + paginação do feed
│       │   │       └── useStats.ts          # Fetch /stats (freshness)
│       │   │
│       │   ├── Busca/
│       │   │   ├── Busca.tsx                # Campo de busca + resultados
│       │   │   ├── Busca.test.tsx
│       │   │   ├── components/
│       │   │   │   ├── BuscaInput.tsx       # Input com debounce + ícone
│       │   │   │   └── ResultadoLista.tsx   # Lista de resultados com score badge
│       │   │   └── hooks/
│       │   │       └── useBusca.ts          # Fetch /busca?q= com debounce
│       │   │
│       │   ├── FichaFornecedor/
│       │   │   ├── FichaFornecedor.tsx      # Página completa da ficha
│       │   │   ├── FichaFornecedor.test.tsx
│       │   │   ├── types.ts                 # Tipos locais da ficha
│       │   │   ├── components/
│       │   │   │   ├── SecaoAlertas.tsx      # Alertas agrupados por tipo + contador
│       │   │   │   ├── AlertaGrupo.tsx       # Grupo expandível ("Rodízio (7)")
│       │   │   │   ├── SecaoScore.tsx        # Score badge + indicadores detalhados
│       │   │   │   ├── SecaoDadosCadastrais.tsx  # CNPJ, razão social, endereço, CNAE
│       │   │   │   ├── SecaoContratos.tsx    # Tabela paginada de contratos
│       │   │   │   ├── SecaoSocios.tsx       # Lista de sócios com qualificação
│       │   │   │   ├── SecaoSancoes.tsx      # Sanções vigentes e históricas
│       │   │   │   ├── SecaoDoacoes.tsx      # Doações eleitorais vinculadas
│       │   │   │   ├── GrafoMini.tsx         # Preview do grafo (1 nível, clicável)
│       │   │   │   ├── NotaOficial.tsx       # Contestação da empresa (link externo)
│       │   │   │   ├── DisclaimerBanner.tsx  # "Dados automáticos, não constituem acusação"
│       │   │   │   └── ExportButtons.tsx     # Botões CSV / JSON / PDF
│       │   │   └── hooks/
│       │   │       ├── useFicha.ts           # Fetch /fornecedores/{cnpj}
│       │   │       └── useExport.ts          # Fetch /fornecedores/{cnpj}/export
│       │   │
│       │   ├── GrafoSocietario/
│       │   │   ├── GrafoSocietario.tsx      # Página full-screen do grafo
│       │   │   ├── GrafoSocietario.test.tsx
│       │   │   ├── types.ts                 # GrafoNo, GrafoAresta locais
│       │   │   ├── components/
│       │   │   │   ├── GrafoCanvas.tsx       # React Force Graph (force-directed layout)
│       │   │   │   ├── GrafoControles.tsx    # Zoom, reset, filtro por tipo de nó
│       │   │   │   ├── GrafoLegenda.tsx      # Cores: empresa, sócio, servidor
│       │   │   │   └── NoTooltip.tsx         # Tooltip ao hover: nome, score, alertas
│       │   │   └── hooks/
│       │   │       ├── useGrafo.ts           # Fetch /fornecedores/{cnpj}/grafo
│       │   │       └── useGrafoExpansion.ts  # Progressive disclosure (expandir nó)
│       │   │
│       │   ├── Ranking/
│       │   │   ├── Ranking.tsx              # Tabela de ranking por score cumulativo
│       │   │   ├── Ranking.test.tsx
│       │   │   ├── components/
│       │   │   │   ├── RankingTabela.tsx     # Tabela com CNPJ, razão, score, alertas, valor
│       │   │   │   └── RankingFiltros.tsx    # Filtros: faixa de risco, UF, CNAE
│       │   │   └── hooks/
│       │   │       └── useRanking.ts         # Fetch /fornecedores/ranking + paginação
│       │   │
│       │   ├── Alertas/
│       │   │   ├── Alertas.tsx              # Feed de alertas filtrado por tipo/severidade
│       │   │   ├── Alertas.test.tsx
│       │   │   ├── components/
│       │   │   │   ├── AlertaLista.tsx       # Lista paginada
│       │   │   │   └── AlertaFiltros.tsx     # Filtros: tipo, severidade, período
│       │   │   └── hooks/
│       │   │       └── useAlertas.ts         # Fetch /alertas + /alertas/{tipo}
│       │   │
│       │   ├── DashboardOrgao/
│       │   │   ├── DashboardOrgao.tsx       # Visão por órgão contratante
│       │   │   ├── DashboardOrgao.test.tsx
│       │   │   ├── components/
│       │   │   │   ├── OrgaoResumo.tsx       # Nome, total contratado, qtd fornecedores
│       │   │   │   ├── TopFornecedores.tsx   # Top 10 por valor + score
│       │   │   │   └── ContratosChart.tsx    # Timeline de contratos (Nivo)
│       │   │   └── hooks/
│       │   │       └── useDashboardOrgao.ts  # Fetch /orgaos/{codigo}/dashboard
│       │   │
│       │   └── Metodologia/
│       │       ├── Metodologia.tsx           # Página completa de transparência
│       │       ├── Metodologia.test.tsx
│       │       └── components/
│       │           ├── ExplicacaoIndicadores.tsx  # Cada indicador: fórmula + threshold + peso
│       │           ├── ExplicacaoAlertas.tsx      # Cada alerta: condição + severidade
│       │           ├── FontesDados.tsx            # Lista de fontes com URL + frequência
│       │           ├── Limitacoes.tsx             # Limitações conhecidas (QSA, CPF, etc.)
│       │           └── Changelog.tsx              # Histórico de mudanças nos critérios
│       │
│       ├── components/                      # COMPARTILHADOS entre pages
│       │   ├── ui/                          # Componentes genéricos de UI
│       │   │   ├── Button.tsx
│       │   │   ├── Badge.tsx
│       │   │   ├── Card.tsx
│       │   │   ├── Table.tsx
│       │   │   ├── Pagination.tsx
│       │   │   ├── Loading.tsx              # Skeleton / spinner
│       │   │   ├── ErrorState.tsx           # Mensagem de erro padrão
│       │   │   └── EmptyState.tsx           # "Nenhum resultado encontrado"
│       │   ├── layout/
│       │   │   ├── Header.tsx               # Nav principal + busca rápida
│       │   │   ├── Footer.tsx               # Links + disclaimer geral
│       │   │   ├── Sidebar.tsx              # Nav lateral (se aplicável)
│       │   │   └── PageContainer.tsx        # Wrapper padrão (max-width, padding)
│       │   ├── ScoreBadge.tsx               # Badge colorido por faixa (verde→vermelho)
│       │   ├── AlertaCriticoBadge.tsx       # Badge GRAVE (laranja) / GRAVÍSSIMO (vermelho)
│       │   ├── SeveridadeBadge.tsx          # Badge de severidade genérico
│       │   ├── CNPJFormatado.tsx            # Formata + linka para ficha
│       │   ├── ValorMonetario.tsx           # Formata R$ com separadores
│       │   ├── FreshnessBanner.tsx          # Banner: "CNPJ: jan/2026, Contratos: 25/fev/2026"
│       │   └── charts/                     # Componentes Nivo reutilizáveis
│       │       ├── ScoreGauge.tsx           # Gauge visual do score (0-100)
│       │       ├── ContratosTimeline.tsx    # Linha do tempo de contratos
│       │       └── TreemapContratos.tsx     # Treemap por órgão/valor
│       │
│       ├── hooks/                           # COMPARTILHADOS
│       │   ├── useApi.ts                    # Fetch genérico tipado (loading | error | data)
│       │   ├── usePagination.ts             # Lógica de paginação reutilizável
│       │   └── useDebounce.ts               # Debounce para inputs de busca
│       │
│       ├── services/                        # CHAMADAS À API (tipadas)
│       │   ├── api.ts                       # Axios/fetch config: baseURL, headers, retry
│       │   ├── fornecedorService.ts         # getFicha, getRanking, getGrafo, exportar
│       │   ├── alertaService.ts             # getAlertas, getAlertasPorTipo
│       │   ├── contratoService.ts           # getContratos (com filtros)
│       │   ├── orgaoService.ts              # getDashboard
│       │   ├── buscaService.ts              # buscar(query)
│       │   └── statsService.ts              # getStats (freshness metadata)
│       │
│       ├── types/                           # TIPOS GLOBAIS
│       │   ├── fornecedor.ts                # Fornecedor, FornecedorResumo
│       │   ├── alerta.ts                    # AlertaCritico, TipoAlerta, Severidade
│       │   ├── score.ts                     # ScoreDeRisco, Indicador, FaixaRisco
│       │   ├── contrato.ts                  # Contrato, Modalidade
│       │   ├── grafo.ts                     # GrafoNo, GrafoAresta, GrafoData
│       │   ├── doacao.ts                    # Doacao, Candidato
│       │   ├── sancao.ts                    # Sancao, TipoSancao
│       │   ├── orgao.ts                     # Orgao, DashboardOrgao
│       │   ├── stats.ts                     # Stats, FonteMetadata
│       │   └── api.ts                       # ApiResponse<T>, PaginatedResponse<T>, ApiError
│       │
│       └── lib/                             # UTILITÁRIOS puros
│           ├── formatters.ts                # formatCNPJ, formatCurrency, formatDate
│           ├── colors.ts                    # Mapa faixa de risco → cor, severidade → cor
│           └── constants.ts                 # Labels de alertas/indicadores, thresholds
│
├── tests/                                   # Testes Python (Pytest)
│   ├── conftest.py                          # Fixtures globais: criar_fornecedor, criar_contrato, etc.
│   │
│   ├── domain/                              # FUNÇÕES PURAS — sem mock, sem IO
│   │   ├── __init__.py
│   │   ├── test_alertas.py                  # Cada regra de alerta testada isoladamente
│   │   ├── test_score.py                    # Cada indicador cumulativo testado
│   │   ├── test_score_alerta_independencia.py  # Dimensões nunca se cruzam
│   │   ├── test_cnpj_vo.py                  # Validação dígitos, formatação, edge cases
│   │   ├── test_cpf_vo.py                   # Validação CPF, mascaramento
│   │   ├── test_fornecedor_entity.py        # Aggregate root, invariantes
│   │   ├── test_contrato_entity.py          # Contrato, ValorContrato
│   │   ├── test_sancao_entity.py            # Vigente vs expirada, datas
│   │   ├── test_doacao_entity.py            # Threshold materialidade
│   │   └── test_grafo_service.py            # Travessia 2 níveis, limite de nós
│   │
│   ├── integration/                         # API + DuckDB in-memory
│   │   ├── __init__.py
│   │   ├── conftest.py                      # DuckDB :memory: + fixtures parquet
│   │   ├── test_api_fornecedor.py           # GET /fornecedores/{cnpj} → ficha completa
│   │   ├── test_api_ranking.py              # GET /fornecedores/ranking → ordenado por score
│   │   ├── test_api_grafo.py                # GET /fornecedores/{cnpj}/grafo → nós + arestas
│   │   ├── test_api_export.py               # GET /fornecedores/{cnpj}/export → CSV/JSON/PDF
│   │   ├── test_api_alertas.py              # GET /alertas → feed, GET /alertas/{tipo} → filtro
│   │   ├── test_api_busca.py                # GET /busca?q= → resultados
│   │   ├── test_api_stats.py                # GET /stats → freshness por fonte
│   │   ├── test_api_orgao.py                # GET /orgaos/{codigo}/dashboard
│   │   └── test_rate_limit.py               # 60 req/min, API key bypass
│   │
│   ├── pipeline/                            # Parse, transformação, build
│   │   ├── __init__.py
│   │   ├── test_ingestao_cnpj.py            # Parse CSV empresas + QSA
│   │   ├── test_ingestao_pncp.py            # Parse JSON contratos
│   │   ├── test_ingestao_tse.py             # Parse doações eleitorais
│   │   ├── test_ingestao_sancoes.py         # Parse CEIS + CNEP + CEPIM
│   │   ├── test_ingestao_servidores.py      # Parse com CPF mascarado
│   │   ├── test_match_servidor_socio.py     # Match nome + dígitos CPF parcial
│   │   ├── test_hmac_cpf.py                 # HMAC determinístico, salt diferente = hash diferente
│   │   ├── test_cnae_mapping.py             # Mapeamento CNAE → categoria
│   │   ├── test_cruzamentos.py              # Cruzamentos entre fontes
│   │   ├── test_build_atomico.py            # Temp → validação → rename, falha = rollback
│   │   └── test_completude.py               # Validação: todas as fontes presentes
│   │
│   └── fixtures/                            # Dados de teste
│       ├── fornecedores.parquet             # ~50 fornecedores com cenários variados
│       ├── contratos.parquet                # Contratos vinculados aos fornecedores
│       ├── socios.parquet                   # QSA com sócios compartilhados
│       ├── sancoes.parquet                  # Vigentes + expiradas
│       ├── doacoes.parquet                  # Com valores acima e abaixo do threshold
│       ├── servidores.parquet               # Com CPFs mascarados para teste de match
│       ├── sample_cnpj.csv                  # CSV raw da Receita (amostra)
│       ├── sample_qsa.csv                   # CSV raw de QSA (amostra)
│       └── sample_pncp.json                 # JSON raw do PNCP (amostra)
│
├── docker/
│   ├── Dockerfile.api                       # Python 3.12 + uvicorn
│   ├── Dockerfile.web                       # Node 20 + nginx
│   └── Dockerfile.pipeline                  # Python 3.12 + deps de download
│
├── docker-compose.yml                       # api + web + (opcionalmente pipeline)
├── pyproject.toml                           # Deps Python + config mypy/pytest/ruff
├── .env.example                             # CPF_HMAC_SALT, DUCKDB_PATH, API_RATE_LIMIT
├── .gitignore                               # data/, *.duckdb, .env, node_modules/, etc.
├── .pre-commit-config.yaml                  # ruff, mypy, eslint, detect-secrets
├── CLAUDE.md                                # Este arquivo
├── SECURITY.md                              # Como reportar vulnerabilidades
├── testa-de-ferro-spec.md                   # Spec completo do projeto
├── LICENSE                                  # MIT
└── README.md
```

## O Que NÃO Fazer

- **Nunca** usar `float` para dinheiro — use `Decimal`
- **Nunca** usar `str` cru para CNPJ/CPF — use Value Objects tipados
- **Nunca** colocar lógica de domínio em routes ou repositories
- **Nunca** fazer `detectar_alertas()` chamar `calcular_score()` ou vice-versa
- **Nunca** armazenar CPF em texto claro no `.duckdb` distribuído
- **Nunca** substituir o `.duckdb` sem validação de completude de todas as fontes
- **Nunca** usar `any` no TypeScript ou ignorar erros do `mypy --strict`
- **Nunca** escrever teste que dependa de estado externo (DB real, rede, filesystem)
- **Nunca** criar herança profunda — composição sempre
- **Nunca** fazer mock de funções de domínio — elas são puras, teste direto
