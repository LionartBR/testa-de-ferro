# Testa de Ferro

Plataforma para identificar empresas com perfil suspeito que vendem ao governo federal.

> **Status: Em desenvolvimento inicial.** A estrutura do projeto existe mas a implementacao ainda nao comecou. Nada esta funcional no momento.

## O que e

O Testa de Ferro cruza dados publicos de multiplas fontes federais para detectar padroes de risco em fornecedores do governo:

- **Receita Federal** (CNPJ, QSA) — dados cadastrais e quadro societario
- **PNCP / ComprasNet** — contratos e licitacoes publicas
- **Portal da Transparencia** — servidores publicos (CPF parcialmente mascarado)
- **TSE** — doacoes eleitorais
- **CEIS / CNEP / CEPIM** — sancoes administrativas

A plataforma opera em duas dimensoes independentes:

1. **Alertas Criticos** — flags binarias para situacoes graves (socio e servidor publico, sancao vigente, doacao para contratante). Detectou = alerta.
2. **Score Cumulativo** — soma ponderada de indicadores fracos (capital social baixo, endereco compartilhado, CNAE incompativel) que isolados nao significam nada, mas combinados revelam perfil de fachada.

Um fornecedor pode ter score 0 e alerta GRAVISSIMO, ou score 85 e zero alertas. As dimensoes nunca se alimentam mutuamente.

## Stack

| Camada | Tecnologia |
| --- | --- |
| Backend | Python 3.12, FastAPI, Pydantic |
| Processamento | Polars, DuckDB |
| Frontend | React 18, TypeScript (strict), Tailwind |
| Formato intermediario | Parquet |
| Banco analitico | DuckDB (read-only em producao) |
| Testes | Pytest (backend), Vitest (frontend) |

## Estrutura do projeto

```
testa-de-ferro/
├── pipeline/     # OFFLINE — baixa fontes, cruza dados, gera .duckdb
├── api/          # ONLINE — le o .duckdb (read-only) e serve via REST
├── web/          # SPA React — consome a API
├── tests/        # Testes Python (dominio, integracao, pipeline)
└── docker/       # Dockerfiles para api, web e pipeline
```

- **pipeline/** executa offline: baixa CSVs/JSONs das fontes, limpa e cruza os dados, calcula alertas e scores, e gera um arquivo `.duckdb` atomicamente (temp -> validacao -> rename).
- **api/** serve o `.duckdb` em modo read-only via FastAPI. Multiplos workers coexistem sem conflito.
- **web/** e a interface: feed de alertas, ranking por score, ficha completa do fornecedor com grafo societario.

## Como rodar (quando pronto)

```bash
# Tudo via Docker
docker compose up

# Ou separadamente:

# Backend
cd api && uvicorn testa_de_ferro.interfaces.api.main:app --reload

# Frontend
cd web && npm run dev

# Pipeline
cd pipeline && python main.py

# Testes
cd api && python -m pytest              # backend
cd web && npm run test                   # frontend
cd web && npm run typecheck              # typescript
```

Sera necessario um arquivo `.env` com as variaveis do `.env.example` (incluindo `CPF_HMAC_SALT`).

## Contribuindo

PRs sao bem-vindos. O projeto segue TDD (teste antes da implementacao), DDD (dominio como verdade central) e TypeScript strict (zero `any`).

Para reportar vulnerabilidades de seguranca, **nunca** abra uma issue publica — consulte o [SECURITY.md](SECURITY.md).

## Licenca

[MIT](LICENSE)
