# SPEC — Testa de Ferro

> Plataforma open source para identificar automaticamente empresas com perfil suspeito que vendem para o governo federal, mapeando redes de sócios, empresas de fachada e padrões de rodízio em licitações.

---

## 1. Objetivo

Cruzar bases de dados públicas para responder uma pergunta central: **quem são as empresas que vivem do governo e por que elas parecem suspeitas?**

O sistema deve:

- Ingerir e processar dados abertos de contratações, CNPJs, sanções, doações eleitorais e servidores públicos.
- Detectar **alertas críticos** — situações que isoladamente já são graves o suficiente para merecer investigação (ex: sócio é servidor público, rodízio em licitação, empresa sancionada contratando).
- Calcular um **score de risco cumulativo** para cada fornecedor do governo federal com base em indicadores de perfil que, combinados, pintam um padrão suspeito.
- Exibir a "ficha completa" de qualquer fornecedor: dados cadastrais, contratos, rede societária (grafo), sanções, doações, alertas e score.
- Oferecer **duas formas de navegação**: feed de alertas críticos (achados graves independentes de score) e ranking por score cumulativo (perfis suspeitos em agregado).
- Visualizar o **grafo de relacionamentos** entre empresas e sócios.
- Ser 100% open source, reproduzível localmente com `docker compose up`.

---

## 2. Fontes de Dados

### 2.1 Contratos e Licitações

| Fonte                                           | URL                                   | Formato         | Volume estimado | Atualização   |
| ----------------------------------------------- | ------------------------------------- | --------------- | --------------- | ------------- |
| PNCP (Portal Nacional de Contratações Públicas) | https://pncp.gov.br/app/dados-abertos | API REST / JSON | ~2M contratos   | Diária        |
| ComprasNet (histórico)                          | https://dados.gov.br                  | CSVs            | ~10M registros  | Arquivo morto |

**Campos relevantes:** CNPJ fornecedor, valor contratado, órgão contratante, objeto, modalidade, data de abertura e homologação, número do pregão/licitação.

### 2.2 Cadastro de Empresas (CNPJ + QSA)

| Fonte                       | URL                                                                                    | Formato          | Volume estimado   | Atualização |
| --------------------------- | -------------------------------------------------------------------------------------- | ---------------- | ----------------- | ----------- |
| Receita Federal — Base CNPJ | https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj | CSVs compactados | ~55M CNPJs / ~5GB | Mensal      |
| Juntas Comerciais (histórico QSA) | https://dados.gov.br                                                              | CSVs / APIs      | Variável          | Variável    |

**Campos relevantes:** CNPJ, razão social, data abertura, capital social, CNAE principal/secundários, endereço completo, situação cadastral, QSA (CPF/CNPJ do sócio, nome, qualificação).

> Esta é a base mais importante do projeto. O QSA permite construir o grafo societário.

> **Histórico societário:** A base CNPJ da Receita mostra apenas a composição societária **atual**. Para detectar evasão temporal (ex: sócio-servidor que sai do quadro dias antes do contrato), o pipeline deve ingerir dados históricos de alterações societárias via Juntas Comerciais. Cada snapshot mensal do QSA também será armazenado para capturar mudanças futuras.

### 2.3 Sanções e Impedimentos

| Fonte                                 | URL                                                    | Formato | Volume estimado | Atualização |
| ------------------------------------- | ------------------------------------------------------ | ------- | --------------- | ----------- |
| CEIS (Empresas Impedidas)             | https://portaldatransparencia.gov.br/download-de-dados | CSV     | ~10K registros  | Mensal      |
| CNEP (Penalidades Lei Anticorrupção)  | https://portaldatransparencia.gov.br/download-de-dados | CSV     | ~5K registros   | Mensal      |
| CEPIM (Entidades sem fins lucrativos) | https://portaldatransparencia.gov.br/download-de-dados | CSV     | ~3K registros   | Mensal      |

**Campos relevantes:** CNPJ/CPF sancionado, tipo de sanção, órgão sancionador, data início/fim, motivo.

### 2.4 Doações Eleitorais

| Fonte                     | URL                                                                    | Formato      | Volume estimado               | Atualização |
| ------------------------- | ---------------------------------------------------------------------- | ------------ | ----------------------------- | ----------- |
| TSE — Prestação de Contas | https://dadosabertos.tse.jus.br/dataset/prestacao-de-contas-eleitorais | CSVs por ano | ~5M registros (todos os anos) | Pós-eleição |

**Campos relevantes:** CPF/CNPJ doador, nome doador, valor, candidato, partido, cargo, UF, ano eleição.

### 2.5 Servidores Públicos Federais

| Fonte                                | URL                                                               | Formato | Volume estimado  | Atualização |
| ------------------------------------ | ----------------------------------------------------------------- | ------- | ---------------- | ----------- |
| Portal da Transparência — Servidores | https://portaldatransparencia.gov.br/download-de-dados/servidores | CSVs    | ~1.2M servidores | Mensal      |

**Campos relevantes:** CPF (parcial), nome, órgão, cargo, data de ingresso, remuneração.

> **Estratégia de cruzamento:** o CPF dos servidores é parcialmente mascarado (ex: `***.222.333-**`). O cruzamento com QSA será por **nome completo + dígitos visíveis do CPF mascarado batendo com o CPF completo do sócio**. A probabilidade matemática de um homônimo ter os mesmos dígitos visíveis é próxima de zero, tornando este match de alta confiança sem necessidade de heurísticas adicionais.

---

## 3. Stack

| Camada                | Tecnologia                        | Justificativa                                                                                                                                |
| --------------------- | --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| **Banco analítico**   | DuckDB                            | Colunar, sem servidor, arquivo local, consulta bilhões de linhas em segundos. Ideal para open source (clone + rode).                         |
| **Pipeline de dados** | Python + Polars                   | Ingestão, limpeza e transformação. Polars é multithreaded, escrito em Rust, ordens de magnitude mais rápido que Pandas para volumes grandes. |
| **API**               | FastAPI                           | Assíncrona, tipada, auto-documentada (Swagger). DuckDB roda in-process dentro da API.                                                        |
| **Frontend**          | React + TypeScript + Tailwind CSS | SPA com tipagem forte. Tailwind para UI consistente e rápida.                                                                                |
| **Gráficos**          | Nivo                              | Biblioteca React-native com treemaps, barras, linhas, sunburst. Melhor estética que Recharts para dashboards complexos.                      |
| **Grafo visual**      | React Force Graph ou vis-network  | Visualização de redes societárias. Force-directed layout para mostrar clusters de sócios/empresas.                                           |
| **Formato de dados**  | Parquet                           | Colunar, compacto (~10x menor que CSV), leitura parcial. Padrão de mercado para dados analíticos.                                            |
| **Containerização**   | Docker + Docker Compose           | Um comando sobe API + frontend + banco. Essencial para contribuidores.                                                                       |
| **CI/CD**             | GitHub Actions                    | Lint, testes, e opcionalmente pipeline de atualização de dados.                                                                              |
| **Testes**            | Pytest (back) + Vitest (front)    | Cobertura mínima de 80% em domínio e serviços.                                                                                               |

---

## 4. Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                      PIPELINE (offline)                     │
│                                                             │
│  [Fontes]  →  [Download]  →  [Limpeza]  →  [Transformação] │
│   PNCP         Scripts       Polars         Polars/SQL      │
│   CNPJ         Python        Validação      Cruzamentos     │
│   TSE                        Dedup          Score cálculo   │
│   CEIS                       Tipagem        Parquet output  │
│   Servidores                                                │
│                                      ↓                      │
│                              [DuckDB .duckdb]               │
│                              Dados processados              │
│                              Star schema                    │
└─────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────┐
│                      API (online)                           │
│                                                             │
│  [FastAPI]                                                  │
│   ├── GET /fornecedores/{cnpj}         → ficha completa     │
│   ├── GET /fornecedores/ranking        → por score cumulat. │
│   ├── GET /fornecedores/{cnpj}/grafo   → rede societária (2 níveis) │
│   ├── GET /fornecedores/{cnpj}/export  → CSV/JSON/PDF da ficha     │
│   ├── GET /alertas                     → feed de alertas    │
│   ├── GET /alertas/{tipo}              → filtro por tipo    │
│   ├── GET /contratos                   → busca/filtro       │
│   ├── GET /orgaos/{codigo}/dashboard   → visão por órgão    │
│   ├── GET /busca?q=                    → full-text search   │
│   └── GET /stats                       → números gerais + metadata de freshness │
│                                                             │
│   DuckDB in-process (read_only=True, múltiplos workers)     │
│                                                             │
│   Rate limiting: 60 req/min por IP (sem key)                │
│   API key opcional para bulk/pesquisadores (limite maior)   │
└─────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────┐
│                      FRONTEND (SPA)                         │
│                                                             │
│  [React + TypeScript + Tailwind]                            │
│   ├── Home (feed de alertas críticos + resumo geral)        │
│   ├── Página de busca (CNPJ / nome)                         │
│   ├── Ficha do fornecedor (alertas + score + grafo)         │
│   ├── Feed de alertas críticos (filtro por tipo/severidade) │
│   ├── Ranking por score cumulativo                          │
│   ├── Dashboard por órgão                                   │
│   ├── Explorador de grafo (rede societária interativa)      │
│   ├── Exportação (CSV / JSON / PDF por fornecedor)          │
│   └── Sobre / Metodologia (completa)                        │
│                                                             │
│   UX: alertas agrupados por tipo + progressive disclosure   │
│   Grafo: 1 nível visível, expandir sob demanda (max 50 nós)│
│   Banner de freshness: idade dos dados por fonte            │
│   Contestação: link para nota oficial da empresa + disclaimer│
└─────────────────────────────────────────────────────────────┘
```

### 4.1 Separação Pipeline vs API

O pipeline roda **offline** (manualmente ou via cron/GitHub Actions). Ele baixa, processa e gera o arquivo DuckDB. A API apenas lê esse arquivo. Isso significa:

- Zero dependência de infraestrutura de banco em produção.
- O arquivo `.duckdb` pode ser distribuído como release no GitHub (GitHub Premium para releases > 2GB) — qualquer pessoa baixa e tem o banco completo.
- A API é stateless e horizontalmente escalável (se necessário no futuro).
- **Concorrência:** DuckDB abre em modo `read_only=True`. Múltiplos workers do uvicorn podem abrir conexões simultâneas ao mesmo arquivo sem conflito.
- **Atomicidade do pipeline:** O arquivo `.duckdb` só é substituído se **todas** as fontes de dados completaram com sucesso. Se qualquer fonte falha, o pipeline mantém a versão anterior intacta. Nunca servir dados parciais.

### 4.2 Pipeline: etapas detalhadas

```
1. download/       → Scripts que baixam os CSVs/JSONs das fontes oficiais.
2. raw/            → Dados brutos baixados (não versionados, .gitignore).
3. staging/        → Dados limpos em Parquet (validados, tipados, deduplicados).
4. transform/      → Cruzamentos e cálculo de scores.
5. output/         → Arquivo DuckDB final com star schema populado.
```

Cada etapa é idempotente: pode ser reexecutada sem efeitos colaterais.

**Regra de atomicidade:** O pipeline gera o DuckDB em um arquivo temporário. Ao final, valida que todas as fontes foram processadas (checklist interno). Só então renomeia o temporário para o arquivo definitivo (operação atômica no filesystem). Se qualquer fonte falhar, o build é abortado e o arquivo anterior permanece inalterado.

---

## 5. DDD — Domain-Driven Design

### 5.1 Domínios

```
testa_de_ferro/
├── domain/
│   ├── fornecedor/          # Bounded Context principal
│   │   ├── entities.py      # Fornecedor, ScoreDeRisco
│   │   ├── value_objects.py  # CNPJ, RazaoSocial, CapitalSocial
│   │   └── repository.py    # Interface FornecedorRepository
│   │
│   ├── contrato/            # Bounded Context
│   │   ├── entities.py      # Contrato, Licitacao
│   │   ├── value_objects.py  # ValorContrato, ModalidadeLicitacao
│   │   └── repository.py    # Interface ContratoRepository
│   │
│   ├── societario/          # Bounded Context
│   │   ├── entities.py      # Socio, VinculoSocietario
│   │   ├── value_objects.py  # CPF, QualificacaoSocio
│   │   ├── services.py      # GrafoSocietarioService (travessia até 2 níveis de indireção)
│   │   └── repository.py    # Interface SocietarioRepository
│   │
│   ├── sancao/              # Bounded Context
│   │   ├── entities.py      # Sancao
│   │   ├── value_objects.py  # TipoSancao
│   │   └── repository.py    # Interface SancaoRepository
│   │
│   ├── doacao/              # Bounded Context
│   │   ├── entities.py      # DoacaoEleitoral
│   │   ├── value_objects.py  # AnoCampanha, Candidato
│   │   └── repository.py    # Interface DoacaoRepository
│   │
│   └── servidor/            # Bounded Context
│       ├── entities.py      # ServidorPublico
│       ├── value_objects.py  # Cargo, OrgaoLotacao
│       └── repository.py    # Interface ServidorRepository
│
├── application/
│   ├── services/
│   │   ├── alerta_service.py          # Detecta alertas críticos (flags binárias)
│   │   ├── score_service.py           # Calcula score cumulativo de risco
│   │   ├── ficha_service.py           # Monta ficha completa do fornecedor
│   │   ├── grafo_service.py           # Monta grafo de relacionamentos
│   │   └── ranking_service.py         # Gera ranking e feed de alertas
│   └── dtos/
│       ├── fornecedor_dto.py
│       ├── ficha_dto.py
│       ├── alerta_dto.py
│       └── grafo_dto.py
│
├── infrastructure/
│   ├── repositories/
│   │   ├── duckdb_fornecedor_repo.py
│   │   ├── duckdb_contrato_repo.py
│   │   ├── duckdb_societario_repo.py
│   │   ├── duckdb_sancao_repo.py
│   │   ├── duckdb_doacao_repo.py
│   │   └── duckdb_servidor_repo.py
│   ├── duckdb_connection.py
│   └── config.py
│
└── interfaces/
    └── api/
        ├── main.py
        ├── routes/
        │   ├── fornecedor_routes.py
        │   ├── contrato_routes.py
        │   ├── alerta_routes.py
        │   ├── orgao_routes.py
        │   └── busca_routes.py
        └── middleware/
            └── cors.py
```

### 5.2 Entidades principais

**Fornecedor** (Aggregate Root)

- cnpj: CNPJ
- razao_social: str
- data_abertura: date
- capital_social: Decimal
- cnae_principal: str
- endereco: Endereco
- situacao_cadastral: SituacaoCadastral
- alertas_criticos: list[AlertaCritico]
- score_risco: ScoreDeRisco
- socios: list[VinculoSocietario]

**AlertaCritico** (Entity)

- id: UUID
- tipo: TipoAlerta (enum)
- severidade: Severidade (GRAVE, GRAVISSIMO)
- descricao: str
- evidencia: str
- fornecedor_cnpj: CNPJ
- detectado_em: datetime

> Alertas críticos existem independentemente do score. Um fornecedor com score 0 em indicadores cumulativos mas com um alerta SOCIO_SERVIDOR_PUBLICO aparece no feed de alertas com destaque total.

**ScoreDeRisco** (Value Object)

- valor: int (0–100)
- indicadores: list[IndicadorCumulativo]
- calculado_em: datetime

**IndicadorCumulativo** (Value Object)

- tipo: TipoIndicador (enum)
- peso: float
- descricao: str
- evidencia: str

### 5.3 Sistema de Detecção Dual (Domain Service)

O sistema opera em **duas dimensões independentes**. Isso garante que um achado grave nunca se perca no fundo de um ranking numérico.

#### Dimensão 1: Alertas Críticos (flags binárias)

Situações que **isoladamente** já são graves o suficiente para merecer destaque e investigação. Detectou = alerta disparado, independente de qualquer outro fator.

| Alerta                         | Severidade | Condição                                                                                 |
| ------------------------------ | ---------- | ---------------------------------------------------------------------------------------- |
| SOCIO_SERVIDOR_PUBLICO         | GRAVÍSSIMO | Sócio é servidor público federal, especialmente se do órgão que contrata                 |
| RODIZIO_LICITACAO              | GRAVÍSSIMO | Empresas com sócios em comum participando da mesma licitação                             |
| EMPRESA_SANCIONADA_CONTRATANDO | GRAVÍSSIMO | Empresa ou sócio consta no CEIS/CNEP **com sanção vigente** (data_fim NULL ou futura) e ainda recebe contratos. Sanções expiradas não geram alerta crítico — viram indicador cumulativo leve (peso 5) no score |
| DOACAO_PARA_CONTRATANTE        | GRAVE      | Empresa/sócio doou para político com influência sobre o órgão que contrata. **Threshold de materialidade:** doação > R$10.000 E contrato > R$500.000. Doações abaixo do threshold são registradas mas não geram alerta |
| SOCIO_SANCIONADO_EM_OUTRA      | GRAVE      | Sócio aparece como sancionado em outra empresa e agora atua em nova fornecedora          |
| TESTA_DE_FERRO                 | GRAVE      | Sócio com perfil de laranja: (a) idade < 20 ou > 80, OU (b) sem histórico em outras empresas + capital social desproporcional à renda presumida + contratos governamentais altos. Critérios combinados para capturar além de idades extremas |

Cada alerta gera um registro individual com evidência rastreável (quais CNPJs, quais licitações, quais servidores). Alertas **não** alimentam o score — vivem em um catálogo próprio.

#### Dimensão 2: Score Cumulativo (perfil suspeito)

Indicadores que **isoladamente** não significam muito, mas **combinados** revelam um padrão de empresa criada exclusivamente para vender ao governo. O score é a soma ponderada dos indicadores ativos.

| Indicador                       | Peso | Condição                                                  |
| ------------------------------- | ---- | --------------------------------------------------------- |
| CAPITAL_SOCIAL_BAIXO            | 15   | Capital abaixo do threshold do setor (cruzado com CNAE) e contratos > R$100.000. Threshold varia: serviços (TI, consultoria) toleram capital menor que comércio. Ex: TI ativa com capital < R$1.000; comércio ativa com capital < R$10.000 |
| EMPRESA_RECENTE                 | 10   | Abertura < 6 meses antes do primeiro contrato             |
| CNAE_INCOMPATIVEL               | 10   | CNAE principal diverge do objeto contratado. Implementado via **tabela de mapeamento manual curada** (top 50 CNAEs → categorias de objeto). Comunidade pode expandir o mapeamento via contribuição |
| SOCIO_EM_MULTIPLAS_FORNECEDORAS | 20   | Sócio aparece em 3+ empresas que vendem ao governo        |
| MESMO_ENDERECO                  | 15   | Compartilha endereço (logradouro + número, **sem** complemento) com outra fornecedora do governo. Aceita-se ruído em prédios comerciais — o indicador só é relevante combinado com outros |
| FORNECEDOR_EXCLUSIVO            | 10   | 90%+ dos contratos com um único órgão                     |
| SEM_FUNCIONARIOS                | 10   | Empresa sem vínculos na RAIS mas com contratos de serviço |
| CRESCIMENTO_SUBITO              | 10   | Valor contratado cresceu 10x+ em um ano                   |
| SANCAO_HISTORICA                | 5    | Empresa ou sócio teve sanção no passado (já expirada). Não é alerta crítico, mas agrega ao perfil de risco |

O score final é a soma direta dos pesos dos indicadores ativos (máximo teórico = 100):

```
score = soma dos pesos dos indicadores ativos
```

Faixas:

- 0–20: Baixo risco
- 21–40: Risco moderado
- 41–65: Alto risco
- 66–100: Risco crítico

#### Como as duas dimensões se complementam

```
Fornecedor A: Score 75 (muitos indicadores cumulativos), 0 alertas
→ Aparece no ranking de score. Perfil clássico de empresa de fachada.

Fornecedor B: Score 10 (só capital baixo), 1 alerta SOCIO_SERVIDOR_PUBLICO
→ Aparece no feed de alertas com destaque. Score baixo não esconde o achado.

Fornecedor C: Score 60, 2 alertas (rodízio + doação para contratante)
→ Aparece em ambos. Caso mais grave, destaque máximo.
```

Na UI, a ficha do fornecedor mostra ambos: alertas críticos em destaque no topo (com badge vermelho), score cumulativo como contexto adicional abaixo. O feed de alertas na home é independente do ranking por score.

#### Profundidade do Grafo Societário

O grafo de relacionamentos opera com **2 níveis de indireção**:

```
Nível 0: Fornecedor consultado
Nível 1: Sócios diretos do fornecedor + outras empresas desses sócios
Nível 2: Sócios dessas outras empresas + empresas desses sócios
```

Isso permite detectar estruturas como: Pessoa A → Holding X → Empresa Y (fornecedora do governo). Na UI, o grafo exibe 1 nível por padrão, com botão de expansão sob demanda. Limite visual de 50 nós para manter usabilidade — nós adicionais são acessíveis via paginação ou filtros.

#### Contestação e Transparência

- **Disclaimer:** Toda ficha de fornecedor exibe: *"Dados gerados automaticamente a partir de bases públicas. Não constituem acusação. Correlação não implica causalidade."*
- **Nota oficial:** Campo opcional para a empresa vincular uma nota de resposta (link para PDF ou URL). A nota é exibida na ficha sem edição.
- **Metodologia:** A página "Sobre / Metodologia" deve conter:
  - Explicação de cada indicador e alerta, com fórmula e threshold
  - Lista de todas as fontes de dados com URLs e frequência de atualização
  - Limitações conhecidas (QSA apenas atual, CPF mascarado, homônimos, etc.)
  - Changelog de quando critérios mudaram (com data)

#### Data Freshness

O endpoint `GET /stats` retorna metadata de atualização por fonte:

```json
{
  "fontes": {
    "cnpj": { "ultima_atualizacao": "2026-01-15", "registros": 55000000 },
    "pncp": { "ultima_atualizacao": "2026-02-26", "registros": 2100000 },
    "ceis": { "ultima_atualizacao": "2026-02-01", "registros": 10500 }
  }
}
```

Na UI, um banner mostra a idade dos dados de cada fonte. Na ficha do fornecedor, os campos mostram de qual fonte vieram e quando foram atualizados.

---

## 6. TDD — Estratégia de Testes

### 6.1 Pirâmide de testes

```
         ╱ E2E (Cypress/Playwright) ╲        → 5% — fluxos críticos
        ╱   Integração (Pytest)      ╲       → 25% — API + DuckDB
       ╱     Unitários (Pytest/Vitest) ╲     → 70% — domínio + componentes
```

### 6.2 Testes unitários — Domínio (Pytest)

Cada regra de alerta e score é uma função pura testável isoladamente.

```python
# tests/domain/test_alertas.py

def test_alerta_socio_servidor_publico():
    """Sócio que é servidor público SEMPRE gera alerta, independente de score."""
    socio = criar_socio(cpf="111.222.333-44", is_servidor_publico=True)
    fornecedor = criar_fornecedor(socios=[socio])
    alertas = detectar_alertas(fornecedor)
    assert len(alertas) == 1
    assert alertas[0].tipo == TipoAlerta.SOCIO_SERVIDOR_PUBLICO
    assert alertas[0].severidade == Severidade.GRAVISSIMO

def test_alerta_rodizio_licitacao():
    """Duas empresas com sócios em comum na mesma licitação geram alerta."""
    socio = criar_socio(cpf="111.222.333-44")
    empresa_a = criar_fornecedor(cnpj="11.111.111/0001-11", socios=[socio])
    empresa_b = criar_fornecedor(cnpj="22.222.222/0001-22", socios=[socio])
    licitacao = criar_licitacao(
        participantes=[empresa_a.cnpj, empresa_b.cnpj]
    )
    alertas = detectar_alertas_licitacao(licitacao, grafo_societario)
    assert any(a.tipo == TipoAlerta.RODIZIO_LICITACAO for a in alertas)
    assert alertas[0].severidade == Severidade.GRAVISSIMO

def test_alerta_empresa_sancionada_contratando():
    """Empresa no CEIS que ainda recebe contratos gera alerta."""
    fornecedor = criar_fornecedor(cnpj="11.111.111/0001-11")
    sancao = criar_sancao(cnpj="11.111.111/0001-11", data_fim=None)  # vigente
    contrato = criar_contrato(cnpj="11.111.111/0001-11", data_assinatura=date.today())
    alertas = detectar_alertas(fornecedor, sancoes=[sancao], contratos=[contrato])
    assert any(a.tipo == TipoAlerta.EMPRESA_SANCIONADA_CONTRATANDO for a in alertas)

def test_alerta_nao_contamina_score():
    """Alertas críticos NÃO devem afetar o score cumulativo."""
    socio = criar_socio(cpf="111.222.333-44", is_servidor_publico=True)
    fornecedor = criar_fornecedor(capital_social=1_000_000, socios=[socio])
    contratos = [criar_contrato(valor=50_000)]
    alertas = detectar_alertas(fornecedor)
    score = calcular_score_cumulativo(fornecedor, contratos)
    assert len(alertas) == 1  # alerta existe
    assert score.valor == 0   # score não foi inflado pelo alerta


# tests/domain/test_score.py

def test_capital_social_baixo_com_contratos_altos():
    """Empresa com capital de R$1.000 e contrato de R$500.000 deve ativar indicador."""
    fornecedor = criar_fornecedor(capital_social=1000)
    contratos = [criar_contrato(valor=500_000)]
    score = calcular_score_cumulativo(fornecedor, contratos)
    assert any(i.tipo == TipoIndicador.CAPITAL_SOCIAL_BAIXO for i in score.indicadores)
    assert score.valor >= 15

def test_capital_social_adequado():
    """Empresa com capital compatível não ativa indicador."""
    fornecedor = criar_fornecedor(capital_social=1_000_000)
    contratos = [criar_contrato(valor=500_000)]
    score = calcular_score_cumulativo(fornecedor, contratos)
    assert not any(i.tipo == TipoIndicador.CAPITAL_SOCIAL_BAIXO for i in score.indicadores)

def test_multiplos_indicadores_cumulativos():
    """Vários indicadores fracos combinados geram score alto."""
    fornecedor = criar_fornecedor(
        capital_social=1000,
        data_abertura=date.today() - timedelta(days=90),
        cnae_principal="9999-9",
    )
    contratos = [criar_contrato(valor=500_000, objeto="Serviço de TI")]
    score = calcular_score_cumulativo(fornecedor, contratos)
    assert score.valor >= 35  # capital_baixo(15) + recente(10) + cnae(10)
    assert score.faixa == "Risco moderado"

def test_score_nunca_excede_100():
    """Score deve ser limitado a 100 mesmo com todos os indicadores ativos."""
    score = calcular_score_cumulativo(fornecedor_todos_indicadores)
    assert 0 <= score.valor <= 100

def test_cnpj_valido():
    """Value object CNPJ deve validar dígitos verificadores."""
    assert CNPJ("11.222.333/0001-81").is_valid()
    with pytest.raises(ValueError):
        CNPJ("00.000.000/0000-00")
```

### 6.3 Testes de integração — API + DuckDB (Pytest)

```python
# tests/integration/test_api_fornecedor.py

@pytest.fixture
def db_com_dados_teste():
    """Cria DuckDB in-memory com dados de teste."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE dim_fornecedor AS SELECT * FROM 'tests/fixtures/fornecedores.parquet'")
    conn.execute("CREATE TABLE fato_contrato AS SELECT * FROM 'tests/fixtures/contratos.parquet'")
    yield conn
    conn.close()

def test_ficha_fornecedor_retorna_dados_completos(client, db_com_dados_teste):
    response = client.get("/api/fornecedores/11222333000181")
    assert response.status_code == 200
    data = response.json()
    assert data["cnpj"] == "11.222.333/0001-81"
    assert "alertas_criticos" in data
    assert "score_risco" in data
    assert "socios" in data
    assert "contratos" in data

def test_ficha_mostra_alerta_mesmo_com_score_baixo(client, db_com_dados_teste):
    """Fornecedor com score 0 mas com alerta crítico deve ter o alerta na ficha."""
    response = client.get("/api/fornecedores/99888777000100")  # fixture com sócio servidor
    data = response.json()
    assert data["score_risco"]["valor"] < 20
    assert len(data["alertas_criticos"]) > 0
    assert data["alertas_criticos"][0]["tipo"] == "SOCIO_SERVIDOR_PUBLICO"

def test_feed_alertas_retorna_mais_recentes(client, db_com_dados_teste):
    response = client.get("/api/alertas?limit=10")
    data = response.json()
    assert len(data) <= 10
    assert all("tipo" in a for a in data)
    assert all("severidade" in a for a in data)
    datas = [a["detectado_em"] for a in data]
    assert datas == sorted(datas, reverse=True)

def test_feed_alertas_filtra_por_tipo(client, db_com_dados_teste):
    response = client.get("/api/alertas/RODIZIO_LICITACAO")
    data = response.json()
    assert all(a["tipo"] == "RODIZIO_LICITACAO" for a in data)

def test_ranking_ordenado_por_score(client, db_com_dados_teste):
    response = client.get("/api/fornecedores/ranking?limit=10")
    data = response.json()
    scores = [f["score_risco"]["valor"] for f in data]
    assert scores == sorted(scores, reverse=True)

def test_grafo_retorna_nos_e_arestas(client, db_com_dados_teste):
    response = client.get("/api/fornecedores/11222333000181/grafo")
    data = response.json()
    assert "nos" in data
    assert "arestas" in data
    assert any(n["tipo"] == "empresa" for n in data["nos"])
    assert any(n["tipo"] == "socio" for n in data["nos"])
```

### 6.4 Testes de frontend (Vitest + Testing Library)

```typescript
// tests/components/ScoreBadge.test.tsx

describe("ScoreBadge", () => {
  it("exibe cor vermelha para score crítico", () => {
    render(<ScoreBadge valor={85} />);
    expect(screen.getByText("85")).toHaveClass("bg-red-600");
  });

  it("exibe cor verde para score baixo", () => {
    render(<ScoreBadge valor={15} />);
    expect(screen.getByText("15")).toHaveClass("bg-green-600");
  });
});

// tests/components/AlertaCriticoBadge.test.tsx

describe("AlertaCriticoBadge", () => {
  it("exibe badge gravíssimo com ícone de alerta", () => {
    render(<AlertaCriticoBadge tipo="SOCIO_SERVIDOR_PUBLICO" severidade="GRAVISSIMO" />);
    expect(screen.getByText("Sócio é servidor público")).toBeInTheDocument();
    expect(screen.getByTestId("badge-gravissimo")).toHaveClass("bg-red-700");
  });

  it("exibe badge grave com estilo diferenciado", () => {
    render(<AlertaCriticoBadge tipo="DOACAO_PARA_CONTRATANTE" severidade="GRAVE" />);
    expect(screen.getByTestId("badge-grave")).toHaveClass("bg-orange-600");
  });
});

// tests/components/FichaFornecedor.test.tsx

describe("FichaFornecedor", () => {
  it("mostra alertas críticos acima do score", () => {
    const dados = {
      alertas_criticos: [{ tipo: "SOCIO_SERVIDOR_PUBLICO", severidade: "GRAVISSIMO" }],
      score_risco: { valor: 10, faixa: "Baixo" },
    };
    render(<FichaFornecedor dados={dados} />);
    const alerta = screen.getByTestId("secao-alertas");
    const score = screen.getByTestId("secao-score");
    expect(alerta.compareDocumentPosition(score)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
  });
});

// tests/components/GrafoSocietario.test.tsx

describe("GrafoSocietario", () => {
  it("renderiza nós de empresa e sócio", () => {
    const dados = {
      nos: [
        { id: "1", tipo: "empresa", label: "Empresa Teste LTDA" },
        { id: "2", tipo: "socio", label: "João da Silva" },
      ],
      arestas: [{ source: "2", target: "1", tipo: "socio_de" }],
    };
    render(<GrafoSocietario dados={dados} />);
    expect(screen.getByText("Empresa Teste LTDA")).toBeInTheDocument();
  });
});
```

### 6.5 Testes de pipeline (Pytest)

```python
# tests/pipeline/test_ingestao_cnpj.py

def test_parse_cnpj_csv_extrai_campos_corretos():
    df = processar_arquivo_cnpj("tests/fixtures/sample_cnpj.csv")
    assert "cnpj" in df.columns
    assert "data_abertura" in df.columns
    assert df["cnpj"].dtype == pl.Utf8
    assert df.filter(pl.col("cnpj").is_null()).height == 0

def test_qsa_vincula_socio_a_empresa():
    df = processar_qsa("tests/fixtures/sample_qsa.csv")
    assert df.filter(pl.col("cnpj_empresa") == "11222333000181").height > 0
    assert "cpf_socio" in df.columns
```

---

## 7. Schema do Banco — Star Schema

### 7.1 Diagrama

```
                    ┌──────────────────┐
                    │  dim_fornecedor  │
                    │──────────────────│
                    │ pk_fornecedor    │
                    │ cnpj             │
                    │ razao_social     │
                    │ data_abertura    │
                    │ capital_social   │
                    │ cnae_principal   │
                    │ cnae_descricao   │
                    │ logradouro       │
                    │ municipio        │
                    │ uf               │
                    │ cep              │
                    │ situacao         │
                    │ score_risco      │
                    │ faixa_risco      │
                    │ qtd_alertas      │
                    │ max_severidade   │
                    └──────┬───────────┘
                           │
       ┌───────────────┬───┼───────┬────────────────┐
       │               │   │       │                │
       ▼               ▼   ▼       ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────────────────┐
│fato_alerta   │ │fato_contrato │ │ fato_doacao  │ │ fato_score_detalhe  │
│_critico      │ │──────────────│ │──────────────│ │─────────────────────│
│──────────────│ │pk_contrato   │ │ pk_doacao    │ │ pk_score_detalhe    │
│pk_alerta     │ │fk_fornecedor │ │ fk_fornecedor│ │ fk_fornecedor       │
│fk_fornecedor │ │fk_orgao      │ │ fk_socio     │ │ indicador           │
│fk_socio      │ │fk_tempo      │ │ fk_candidato │ │ peso                │
│tipo_alerta   │ │fk_modalidade │ │ fk_tempo     │ │ descricao           │
│severidade    │ │valor         │ │ valor        │ │ evidencia           │
│descricao     │ │objeto        │ │ tipo_recurso │ │ calculado_em        │
│evidencia     │ │num_licitacao │ │ ano_eleicao  │ └─────────────────────┘
│detectado_em  │ │data_assinatura│└──────────────┘
└──────────────┘ │data_vigencia │
                 └──────────────┘

┌──────────────────┐  ┌────────────────────┐  ┌─────────────────┐
│  dim_orgao       │  │  dim_socio          │  │  dim_tempo      │
│──────────────────│  │────────────────────│  │─────────────────│
│ pk_orgao         │  │ pk_socio            │  │ pk_tempo        │
│ codigo           │  │ cpf_hmac            │  │ data            │
│ nome             │  │ nome                │  │ ano             │
│ sigla            │  │ qualificacao        │  │ mes             │
│ poder            │  │ is_servidor_publico │  │ trimestre       │
│ esfera           │  │ orgao_lotacao       │  │ semestre        │
│ uf               │  │ is_sancionado       │  └─────────────────┘
└──────────────────┘  │ qtd_empresas        │
                      └────────────────────┘

┌───────────────────────┐  ┌─────────────────────┐  ┌──────────────────┐
│  bridge_fornecedor    │  │  dim_sancao          │  │  dim_candidato   │
│  _socio               │  │─────────────────────│  │──────────────────│
│───────────────────────│  │ pk_sancao            │  │ pk_candidato     │
│ fk_fornecedor         │  │ fk_fornecedor        │  │ nome             │
│ fk_socio              │  │ tipo_sancao          │  │ cpf_hmac         │
│ data_entrada          │  │ orgao_sancionador    │  │ partido          │
│ data_saida            │  │ motivo               │  │ cargo            │
│ percentual_capital    │  │ data_inicio          │  │ uf               │
└───────────────────────┘  │ data_fim             │  │ ano_eleicao      │
                           └─────────────────────┘  └──────────────────┘

┌──────────────────────────┐
│  dim_modalidade          │
│──────────────────────────│
│ pk_modalidade            │
│ codigo                   │
│ descricao                │
│ (Pregão, Dispensa,       │
│  Concorrência, etc.)     │
└──────────────────────────┘
```

### 7.2 DDL (DuckDB)

```sql
-- =============================================
-- DIMENSÕES
-- =============================================

CREATE TABLE dim_tempo (
    pk_tempo        INTEGER PRIMARY KEY,
    data            DATE NOT NULL,
    ano             SMALLINT NOT NULL,
    mes             TINYINT NOT NULL,
    trimestre       TINYINT NOT NULL,
    semestre        TINYINT NOT NULL,
    dia_semana      TINYINT NOT NULL,
    nome_mes        VARCHAR(20) NOT NULL
);

CREATE TABLE dim_fornecedor (
    pk_fornecedor   INTEGER PRIMARY KEY,
    cnpj            VARCHAR(18) NOT NULL UNIQUE,
    razao_social    VARCHAR(255) NOT NULL,
    data_abertura   DATE,
    capital_social  DECIMAL(18,2),
    cnae_principal  VARCHAR(10),
    cnae_descricao  VARCHAR(255),
    logradouro      VARCHAR(255),
    municipio       VARCHAR(100),
    uf              CHAR(2),
    cep             VARCHAR(9),
    situacao        VARCHAR(50),
    score_risco     SMALLINT DEFAULT 0,
    faixa_risco     VARCHAR(20) DEFAULT 'Baixo',
    qtd_alertas     SMALLINT DEFAULT 0,
    max_severidade  VARCHAR(20),          -- NULL, 'GRAVE', 'GRAVISSIMO'
    total_contratos INTEGER DEFAULT 0,
    valor_total     DECIMAL(18,2) DEFAULT 0,
    atualizado_em   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_orgao (
    pk_orgao        INTEGER PRIMARY KEY,
    codigo          VARCHAR(20) NOT NULL UNIQUE,
    nome            VARCHAR(255) NOT NULL,
    sigla           VARCHAR(20),
    poder           VARCHAR(20),
    esfera          VARCHAR(20),
    uf              CHAR(2)
);

CREATE TABLE dim_socio (
    pk_socio              INTEGER PRIMARY KEY,
    cpf_hmac              VARCHAR(64) NOT NULL,   -- HMAC-SHA256 com salt secreto (ver seção LGPD)
    nome                  VARCHAR(255) NOT NULL,
    qualificacao          VARCHAR(100),
    is_servidor_publico   BOOLEAN DEFAULT FALSE,
    orgao_lotacao         VARCHAR(255),
    is_sancionado         BOOLEAN DEFAULT FALSE,
    qtd_empresas_governo  INTEGER DEFAULT 0
);

CREATE TABLE dim_modalidade (
    pk_modalidade   INTEGER PRIMARY KEY,
    codigo          VARCHAR(10) NOT NULL UNIQUE,
    descricao       VARCHAR(100) NOT NULL
);

CREATE TABLE dim_candidato (
    pk_candidato    INTEGER PRIMARY KEY,
    nome            VARCHAR(255) NOT NULL,
    cpf_hmac        VARCHAR(64),
    partido         VARCHAR(50),
    cargo           VARCHAR(100),
    uf              CHAR(2),
    ano_eleicao     SMALLINT
);

-- =============================================
-- TABELAS PONTE (BRIDGE)
-- =============================================

CREATE TABLE bridge_fornecedor_socio (
    fk_fornecedor       INTEGER NOT NULL REFERENCES dim_fornecedor(pk_fornecedor),
    fk_socio            INTEGER NOT NULL REFERENCES dim_socio(pk_socio),
    data_entrada        DATE,
    data_saida          DATE,
    percentual_capital  DECIMAL(5,2),
    PRIMARY KEY (fk_fornecedor, fk_socio)
);

-- =============================================
-- FATOS
-- =============================================

CREATE TABLE fato_contrato (
    pk_contrato     INTEGER PRIMARY KEY,
    fk_fornecedor   INTEGER NOT NULL REFERENCES dim_fornecedor(pk_fornecedor),
    fk_orgao        INTEGER NOT NULL REFERENCES dim_orgao(pk_orgao),
    fk_tempo        INTEGER NOT NULL REFERENCES dim_tempo(pk_tempo),
    fk_modalidade   INTEGER REFERENCES dim_modalidade(pk_modalidade),
    valor           DECIMAL(18,2) NOT NULL,
    objeto          VARCHAR(1000),
    num_licitacao   VARCHAR(50),
    data_assinatura DATE,
    data_vigencia   DATE
);

CREATE TABLE fato_doacao (
    pk_doacao       INTEGER PRIMARY KEY,
    fk_fornecedor   INTEGER REFERENCES dim_fornecedor(pk_fornecedor),
    fk_socio        INTEGER REFERENCES dim_socio(pk_socio),
    fk_candidato    INTEGER NOT NULL REFERENCES dim_candidato(pk_candidato),
    fk_tempo        INTEGER NOT NULL REFERENCES dim_tempo(pk_tempo),
    valor           DECIMAL(18,2) NOT NULL,
    tipo_recurso    VARCHAR(50),
    ano_eleicao     SMALLINT NOT NULL
);

CREATE TABLE fato_score_detalhe (
    pk_score_detalhe INTEGER PRIMARY KEY,
    fk_fornecedor    INTEGER NOT NULL REFERENCES dim_fornecedor(pk_fornecedor),
    indicador        VARCHAR(50) NOT NULL,
    peso             SMALLINT NOT NULL,
    descricao        VARCHAR(500),
    evidencia        VARCHAR(500),
    calculado_em     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fato_alerta_critico (
    pk_alerta       INTEGER PRIMARY KEY,
    fk_fornecedor   INTEGER NOT NULL REFERENCES dim_fornecedor(pk_fornecedor),
    fk_socio        INTEGER REFERENCES dim_socio(pk_socio),
    tipo_alerta     VARCHAR(50) NOT NULL,       -- SOCIO_SERVIDOR_PUBLICO, RODIZIO_LICITACAO, etc.
    severidade      VARCHAR(20) NOT NULL,        -- GRAVE, GRAVISSIMO
    descricao       VARCHAR(500) NOT NULL,
    evidencia       VARCHAR(1000) NOT NULL,      -- JSON com dados rastreáveis (CNPJs, licitações, nomes)
    detectado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_sancao (
    pk_sancao           INTEGER PRIMARY KEY,
    fk_fornecedor       INTEGER NOT NULL REFERENCES dim_fornecedor(pk_fornecedor),
    tipo_sancao         VARCHAR(50) NOT NULL,
    orgao_sancionador   VARCHAR(255),
    motivo              VARCHAR(500),
    data_inicio         DATE NOT NULL,
    data_fim            DATE
);

-- =============================================
-- ÍNDICES
-- =============================================

CREATE INDEX idx_fornecedor_cnpj ON dim_fornecedor(cnpj);
CREATE INDEX idx_fornecedor_score ON dim_fornecedor(score_risco DESC);
CREATE INDEX idx_fornecedor_alertas ON dim_fornecedor(qtd_alertas DESC);
CREATE INDEX idx_fornecedor_severidade ON dim_fornecedor(max_severidade);
CREATE INDEX idx_fornecedor_uf ON dim_fornecedor(uf);
CREATE INDEX idx_socio_cpf ON dim_socio(cpf_hmac);
CREATE INDEX idx_contrato_fornecedor ON fato_contrato(fk_fornecedor);
CREATE INDEX idx_contrato_orgao ON fato_contrato(fk_orgao);
CREATE INDEX idx_contrato_tempo ON fato_contrato(fk_tempo);
CREATE INDEX idx_doacao_fornecedor ON fato_doacao(fk_fornecedor);
CREATE INDEX idx_doacao_socio ON fato_doacao(fk_socio);
CREATE INDEX idx_bridge_socio ON bridge_fornecedor_socio(fk_socio);
CREATE INDEX idx_bridge_fornecedor ON bridge_fornecedor_socio(fk_fornecedor);
CREATE INDEX idx_score_detalhe_fornecedor ON fato_score_detalhe(fk_fornecedor);
CREATE INDEX idx_sancao_fornecedor ON dim_sancao(fk_fornecedor);
CREATE INDEX idx_alerta_fornecedor ON fato_alerta_critico(fk_fornecedor);
CREATE INDEX idx_alerta_tipo ON fato_alerta_critico(tipo_alerta);
CREATE INDEX idx_alerta_severidade ON fato_alerta_critico(severidade);
CREATE INDEX idx_alerta_detectado ON fato_alerta_critico(detectado_em DESC);
```

### 7.3 Queries analíticas de exemplo

```sql
-- Feed de alertas críticos mais recentes
SELECT
    ac.tipo_alerta,
    ac.severidade,
    ac.descricao,
    ac.evidencia,
    ac.detectado_em,
    f.cnpj,
    f.razao_social,
    s.nome AS socio_envolvido
FROM fato_alerta_critico ac
JOIN dim_fornecedor f ON ac.fk_fornecedor = f.pk_fornecedor
LEFT JOIN dim_socio s ON ac.fk_socio = s.pk_socio
ORDER BY ac.detectado_em DESC
LIMIT 50;

-- Alertas por tipo — ex: todos os casos de sócio servidor público
SELECT
    f.cnpj,
    f.razao_social,
    s.nome AS servidor,
    s.orgao_lotacao,
    f.valor_total AS total_contratado,
    ac.evidencia
FROM fato_alerta_critico ac
JOIN dim_fornecedor f ON ac.fk_fornecedor = f.pk_fornecedor
JOIN dim_socio s ON ac.fk_socio = s.pk_socio
WHERE ac.tipo_alerta = 'SOCIO_SERVIDOR_PUBLICO'
ORDER BY f.valor_total DESC;

-- Top 20 fornecedores por score cumulativo (perfil de fachada)
SELECT
    f.cnpj,
    f.razao_social,
    f.score_risco,
    f.faixa_risco,
    f.qtd_alertas,
    f.total_contratos,
    f.valor_total,
    COUNT(DISTINCT bs.fk_socio) AS qtd_socios
FROM dim_fornecedor f
LEFT JOIN bridge_fornecedor_socio bs ON f.pk_fornecedor = bs.fk_fornecedor
WHERE f.score_risco >= 40
GROUP BY ALL
ORDER BY f.score_risco DESC
LIMIT 20;

-- Visão combinada: fornecedores com AMBOS alertas críticos E score alto
SELECT
    f.cnpj,
    f.razao_social,
    f.score_risco,
    f.qtd_alertas,
    f.max_severidade,
    f.valor_total
FROM dim_fornecedor f
WHERE f.qtd_alertas > 0
  AND f.score_risco >= 40
ORDER BY f.qtd_alertas DESC, f.score_risco DESC;

-- Rede societária: encontrar todas as empresas conectadas por sócios
WITH socios_empresa AS (
    SELECT fk_socio
    FROM bridge_fornecedor_socio
    WHERE fk_fornecedor = ?  -- pk do fornecedor consultado
)
SELECT
    f.cnpj,
    f.razao_social,
    f.score_risco,
    s.nome AS socio_compartilhado
FROM bridge_fornecedor_socio bs
JOIN dim_fornecedor f ON bs.fk_fornecedor = f.pk_fornecedor
JOIN dim_socio s ON bs.fk_socio = s.pk_socio
WHERE bs.fk_socio IN (SELECT fk_socio FROM socios_empresa)
ORDER BY f.score_risco DESC;

-- Fornecedores que doaram para políticos dos órgãos que os contratam
SELECT
    f.cnpj,
    f.razao_social,
    c.nome AS candidato,
    c.partido,
    c.cargo,
    SUM(fd.valor) AS total_doado,
    SUM(fc.valor) AS total_contratado
FROM fato_doacao fd
JOIN dim_fornecedor f ON fd.fk_fornecedor = f.pk_fornecedor
JOIN dim_candidato c ON fd.fk_candidato = c.pk_candidato
JOIN fato_contrato fc ON fc.fk_fornecedor = f.pk_fornecedor
GROUP BY ALL
HAVING total_doado > 0 AND total_contratado > 0
ORDER BY total_contratado DESC;

-- Possível rodízio: empresas com sócios em comum na mesma licitação
SELECT
    fc1.num_licitacao,
    f1.cnpj AS empresa_a,
    f2.cnpj AS empresa_b,
    s.nome AS socio_comum,
    fc1.valor AS valor_a,
    fc2.valor AS valor_b
FROM fato_contrato fc1
JOIN fato_contrato fc2
    ON fc1.num_licitacao = fc2.num_licitacao
    AND fc1.fk_fornecedor < fc2.fk_fornecedor
JOIN bridge_fornecedor_socio bs1 ON fc1.fk_fornecedor = bs1.fk_fornecedor
JOIN bridge_fornecedor_socio bs2 ON fc2.fk_fornecedor = bs2.fk_fornecedor
    AND bs1.fk_socio = bs2.fk_socio
JOIN dim_fornecedor f1 ON fc1.fk_fornecedor = f1.pk_fornecedor
JOIN dim_fornecedor f2 ON fc2.fk_fornecedor = f2.pk_fornecedor
JOIN dim_socio s ON bs1.fk_socio = s.pk_socio;
```

---

## 8. LGPD e Proteção de Dados

### 8.1 CPFs — HMAC com salt secreto

O sistema **não armazena CPFs em texto claro** no banco distribuído. Todos os CPFs são transformados via **HMAC-SHA256** com um salt secreto:

```python
import hmac, hashlib

def cpf_hmac(cpf: str, salt: str) -> str:
    """Gera HMAC do CPF. Sem o salt, brute-force é inviável."""
    cpf_limpo = cpf.replace(".", "").replace("-", "").replace("/", "")
    return hmac.new(salt.encode(), cpf_limpo.encode(), hashlib.sha256).hexdigest()
```

- O salt é uma variável de ambiente (`CPF_HMAC_SALT`), **nunca** embarcada no código ou no `.duckdb`.
- O arquivo `.duckdb` distribuído contém apenas os HMACs — sem o salt, reverter é computacionalmente inviável (diferente de SHA-256 simples, que seria brute-forceable em minutos para o espaço de 200M CPFs).
- O pipeline roda com o salt para gerar os HMACs. A API usa o mesmo salt para consultas por CPF.
- **Base legal:** Art. 7º, III da LGPD (tratamento pela administração pública) e Art. 7º, IX (interesse legítimo). Dados originais já são públicos nas fontes oficiais.

### 8.2 Nomes

Nomes de sócios e servidores são dados públicos nas fontes oficiais (Receita Federal, Portal da Transparência). São armazenados em texto claro para viabilizar o cruzamento e a exibição na UI.

### 8.3 Exportação de Dados

O sistema oferece exportação em três formatos via `GET /fornecedores/{cnpj}/export?formato=csv|json|pdf`:

- **CSV:** Contratos, sócios e alertas em formato tabular.
- **JSON:** Ficha completa estruturada (mesma resposta da API).
- **PDF:** Ficha formatada para impressão/publicação, com disclaimer e data de geração.

A exportação respeita os mesmos campos disponíveis na API — não expõe CPFs nem dados além do que a interface mostra.

---

## 9. Estrutura do Repositório

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
│   │   │   ├── parse_qsa.py                 # Parse QSA (sócios)
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
│   │   │   ├── parse.py                     # Parse doações eleitorais
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
│   │       ├── parse.py                     # Parse com CPF mascarado
│   │       └── validate.py
│   │
│   ├── staging/
│   │   ├── __init__.py
│   │   └── parquet_writer.py                # Escrita padronizada com schema enforcement
│   │
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── hmac_cpf.py                      # HMAC-SHA256 com salt (CPF_HMAC_SALT)
│   │   ├── match_servidor_socio.py          # Match nome + dígitos visíveis do CPF
│   │   ├── grafo_societario.py              # Monta grafo com travessia de 2 níveis
│   │   ├── cruzamentos.py                   # Fornecedores × sócios × servidores × sanções
│   │   ├── cnae_mapping.py                  # Tabela manual: top 50 CNAEs → categorias
│   │   ├── alertas.py                       # Detecta alertas críticos (regras binárias)
│   │   └── score.py                         # Calcula score cumulativo (soma ponderada)
│   │
│   ├── output/
│   │   ├── __init__.py
│   │   ├── schema.sql                       # DDL do star schema
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
│   │   │   ├── enums.py                     # TipoAlerta, Severidade, TipoIndicador, FaixaRisco
│   │   │   ├── score.py                     # ScoreDeRisco (VO), IndicadorCumulativo (VO)
│   │   │   └── repository.py               # Protocol FornecedorRepository
│   │   ├── contrato/
│   │   │   ├── __init__.py
│   │   │   ├── entities.py                  # Contrato, Licitacao
│   │   │   ├── value_objects.py             # ValorContrato, ModalidadeLicitacao
│   │   │   └── repository.py               # Protocol ContratoRepository
│   │   ├── societario/
│   │   │   ├── __init__.py
│   │   │   ├── entities.py                  # Socio, VinculoSocietario
│   │   │   ├── value_objects.py             # CPF, QualificacaoSocio, PercentualCapital
│   │   │   ├── services.py                  # GrafoSocietarioService (2 níveis)
│   │   │   └── repository.py               # Protocol SocietarioRepository
│   │   ├── sancao/
│   │   │   ├── __init__.py
│   │   │   ├── entities.py                  # Sancao (data_inicio, data_fim nullable)
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
│   │   │   ├── alerta_service.py            # Detecta alertas (funções puras)
│   │   │   ├── score_service.py             # Calcula score cumulativo (funções puras)
│   │   │   ├── ficha_service.py             # Monta ficha completa (orquestra repos)
│   │   │   ├── grafo_service.py             # Monta grafo de relacionamentos
│   │   │   ├── ranking_service.py           # Ranking por score + feed de alertas
│   │   │   ├── busca_service.py             # Full-text search
│   │   │   └── export_service.py            # Gera CSV / JSON / PDF
│   │   └── dtos/
│   │       ├── __init__.py
│   │       ├── fornecedor_dto.py            # FornecedorResumoDTO, FornecedorListaDTO
│   │       ├── ficha_dto.py                 # FichaCompletaDTO
│   │       ├── alerta_dto.py                # AlertaCriticoDTO, AlertaFeedItemDTO
│   │       ├── grafo_dto.py                 # GrafoDTO, NoDTO, ArestaDTO
│   │       ├── contrato_dto.py              # ContratoDTO, ContratoResumoDTO
│   │       ├── score_dto.py                 # ScoreDTO, IndicadorDTO
│   │       ├── export_dto.py                # ExportRequestDTO
│   │       └── stats_dto.py                 # StatsDTO, FonteMetadataDTO
│   │
│   ├── infrastructure/                      # IMPURO — IO, banco, config
│   │   ├── __init__.py
│   │   ├── duckdb_connection.py             # Singleton read_only=True
│   │   ├── config.py                        # Settings: DUCKDB_PATH, CPF_HMAC_SALT
│   │   ├── hmac_service.py                  # HMAC de CPF para queries
│   │   ├── pdf_generator.py                 # Gera PDF da ficha (exportação)
│   │   └── repositories/
│   │       ├── __init__.py
│   │       ├── duckdb_fornecedor_repo.py
│   │       ├── duckdb_contrato_repo.py
│   │       ├── duckdb_societario_repo.py
│   │       ├── duckdb_sancao_repo.py
│   │       ├── duckdb_doacao_repo.py
│   │       ├── duckdb_servidor_repo.py
│   │       ├── duckdb_alerta_repo.py        # Feed de alertas, filtros
│   │       └── duckdb_stats_repo.py         # Metadata de freshness
│   │
│   └── interfaces/                          # IMPURO — HTTP, middleware
│       └── api/
│           ├── __init__.py
│           ├── main.py                      # FastAPI app, lifespan, error handlers
│           ├── dependencies.py              # Dependency injection
│           ├── routes/
│           │   ├── __init__.py
│           │   ├── fornecedor_routes.py     # GET /fornecedores/{cnpj}
│           │   ├── ranking_routes.py        # GET /fornecedores/ranking
│           │   ├── grafo_routes.py          # GET /fornecedores/{cnpj}/grafo
│           │   ├── export_routes.py         # GET /fornecedores/{cnpj}/export
│           │   ├── alerta_routes.py         # GET /alertas, GET /alertas/{tipo}
│           │   ├── contrato_routes.py       # GET /contratos
│           │   ├── orgao_routes.py          # GET /orgaos/{codigo}/dashboard
│           │   ├── busca_routes.py          # GET /busca?q=
│           │   └── stats_routes.py          # GET /stats
│           └── middleware/
│               ├── __init__.py
│               ├── cors.py
│               └── rate_limit.py            # 60 req/min IP + API key opcional
│
├── web/                                     # SPA React + TypeScript strict + Tailwind
│   ├── public/
│   │   └── favicon.ico
│   ├── index.html
│   ├── package.json
│   ├── tsconfig.json                        # strict: true
│   ├── vite.config.ts
│   ├── tailwind.config.ts
│   ├── vitest.config.ts
│   │
│   └── src/
│       ├── App.tsx
│       ├── main.tsx
│       ├── router.tsx                       # React Router
│       │
│       ├── pages/                           # Vertical slice por página
│       │   ├── Home/
│       │   │   ├── Home.tsx
│       │   │   ├── Home.test.tsx
│       │   │   ├── components/
│       │   │   │   ├── AlertaFeed.tsx
│       │   │   │   ├── AlertaFeedItem.tsx
│       │   │   │   └── ResumoGeral.tsx
│       │   │   └── hooks/
│       │   │       ├── useAlertasFeed.ts
│       │   │       └── useStats.ts
│       │   │
│       │   ├── Busca/
│       │   │   ├── Busca.tsx
│       │   │   ├── Busca.test.tsx
│       │   │   ├── components/
│       │   │   │   ├── BuscaInput.tsx
│       │   │   │   └── ResultadoLista.tsx
│       │   │   └── hooks/
│       │   │       └── useBusca.ts
│       │   │
│       │   ├── FichaFornecedor/
│       │   │   ├── FichaFornecedor.tsx
│       │   │   ├── FichaFornecedor.test.tsx
│       │   │   ├── types.ts
│       │   │   ├── components/
│       │   │   │   ├── SecaoAlertas.tsx
│       │   │   │   ├── AlertaGrupo.tsx      # Expandível ("Rodízio (7)")
│       │   │   │   ├── SecaoScore.tsx
│       │   │   │   ├── SecaoDadosCadastrais.tsx
│       │   │   │   ├── SecaoContratos.tsx
│       │   │   │   ├── SecaoSocios.tsx
│       │   │   │   ├── SecaoSancoes.tsx
│       │   │   │   ├── SecaoDoacoes.tsx
│       │   │   │   ├── GrafoMini.tsx        # Preview do grafo (clicável)
│       │   │   │   ├── NotaOficial.tsx      # Contestação da empresa
│       │   │   │   ├── DisclaimerBanner.tsx
│       │   │   │   └── ExportButtons.tsx
│       │   │   └── hooks/
│       │   │       ├── useFicha.ts
│       │   │       └── useExport.ts
│       │   │
│       │   ├── GrafoSocietario/
│       │   │   ├── GrafoSocietario.tsx
│       │   │   ├── GrafoSocietario.test.tsx
│       │   │   ├── types.ts
│       │   │   ├── components/
│       │   │   │   ├── GrafoCanvas.tsx      # React Force Graph
│       │   │   │   ├── GrafoControles.tsx   # Zoom, reset, filtros
│       │   │   │   ├── GrafoLegenda.tsx
│       │   │   │   └── NoTooltip.tsx
│       │   │   └── hooks/
│       │   │       ├── useGrafo.ts
│       │   │       └── useGrafoExpansion.ts # Progressive disclosure
│       │   │
│       │   ├── Ranking/
│       │   │   ├── Ranking.tsx
│       │   │   ├── Ranking.test.tsx
│       │   │   ├── components/
│       │   │   │   ├── RankingTabela.tsx
│       │   │   │   └── RankingFiltros.tsx
│       │   │   └── hooks/
│       │   │       └── useRanking.ts
│       │   │
│       │   ├── Alertas/
│       │   │   ├── Alertas.tsx
│       │   │   ├── Alertas.test.tsx
│       │   │   ├── components/
│       │   │   │   ├── AlertaLista.tsx
│       │   │   │   └── AlertaFiltros.tsx
│       │   │   └── hooks/
│       │   │       └── useAlertas.ts
│       │   │
│       │   ├── DashboardOrgao/
│       │   │   ├── DashboardOrgao.tsx
│       │   │   ├── DashboardOrgao.test.tsx
│       │   │   ├── components/
│       │   │   │   ├── OrgaoResumo.tsx
│       │   │   │   ├── TopFornecedores.tsx
│       │   │   │   └── ContratosChart.tsx   # Nivo
│       │   │   └── hooks/
│       │   │       └── useDashboardOrgao.ts
│       │   │
│       │   └── Metodologia/
│       │       ├── Metodologia.tsx
│       │       ├── Metodologia.test.tsx
│       │       └── components/
│       │           ├── ExplicacaoIndicadores.tsx
│       │           ├── ExplicacaoAlertas.tsx
│       │           ├── FontesDados.tsx
│       │           ├── Limitacoes.tsx
│       │           └── Changelog.tsx
│       │
│       ├── components/                      # Compartilhados entre pages
│       │   ├── ui/
│       │   │   ├── Button.tsx
│       │   │   ├── Badge.tsx
│       │   │   ├── Card.tsx
│       │   │   ├── Table.tsx
│       │   │   ├── Pagination.tsx
│       │   │   ├── Loading.tsx
│       │   │   ├── ErrorState.tsx
│       │   │   └── EmptyState.tsx
│       │   ├── layout/
│       │   │   ├── Header.tsx
│       │   │   ├── Footer.tsx
│       │   │   ├── Sidebar.tsx
│       │   │   └── PageContainer.tsx
│       │   ├── ScoreBadge.tsx
│       │   ├── AlertaCriticoBadge.tsx
│       │   ├── SeveridadeBadge.tsx
│       │   ├── CNPJFormatado.tsx
│       │   ├── ValorMonetario.tsx
│       │   ├── FreshnessBanner.tsx
│       │   └── charts/
│       │       ├── ScoreGauge.tsx            # Nivo
│       │       ├── ContratosTimeline.tsx
│       │       └── TreemapContratos.tsx
│       │
│       ├── hooks/
│       │   ├── useApi.ts
│       │   ├── usePagination.ts
│       │   └── useDebounce.ts
│       │
│       ├── services/
│       │   ├── api.ts                       # Config base: URL, headers, retry
│       │   ├── fornecedorService.ts
│       │   ├── alertaService.ts
│       │   ├── contratoService.ts
│       │   ├── orgaoService.ts
│       │   ├── buscaService.ts
│       │   └── statsService.ts
│       │
│       ├── types/
│       │   ├── fornecedor.ts
│       │   ├── alerta.ts
│       │   ├── score.ts
│       │   ├── contrato.ts
│       │   ├── grafo.ts
│       │   ├── doacao.ts
│       │   ├── sancao.ts
│       │   ├── orgao.ts
│       │   ├── stats.ts
│       │   └── api.ts                       # ApiResponse<T>, PaginatedResponse<T>
│       │
│       └── lib/
│           ├── formatters.ts                # formatCNPJ, formatCurrency, formatDate
│           ├── colors.ts                    # Faixa de risco → cor, severidade → cor
│           └── constants.ts                 # Labels, thresholds
│
├── tests/                                   # Testes Python (Pytest)
│   ├── conftest.py                          # Fixtures: criar_fornecedor, criar_contrato, etc.
│   │
│   ├── domain/                              # Funções puras — sem mock, sem IO
│   │   ├── __init__.py
│   │   ├── test_alertas.py
│   │   ├── test_score.py
│   │   ├── test_score_alerta_independencia.py
│   │   ├── test_cnpj_vo.py
│   │   ├── test_cpf_vo.py
│   │   ├── test_fornecedor_entity.py
│   │   ├── test_contrato_entity.py
│   │   ├── test_sancao_entity.py
│   │   ├── test_doacao_entity.py
│   │   └── test_grafo_service.py
│   │
│   ├── integration/                         # API + DuckDB in-memory
│   │   ├── __init__.py
│   │   ├── conftest.py                      # DuckDB :memory: + fixtures parquet
│   │   ├── test_api_fornecedor.py
│   │   ├── test_api_ranking.py
│   │   ├── test_api_grafo.py
│   │   ├── test_api_export.py
│   │   ├── test_api_alertas.py
│   │   ├── test_api_busca.py
│   │   ├── test_api_stats.py
│   │   ├── test_api_orgao.py
│   │   └── test_rate_limit.py
│   │
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── test_ingestao_cnpj.py
│   │   ├── test_ingestao_pncp.py
│   │   ├── test_ingestao_tse.py
│   │   ├── test_ingestao_sancoes.py
│   │   ├── test_ingestao_servidores.py
│   │   ├── test_match_servidor_socio.py
│   │   ├── test_hmac_cpf.py
│   │   ├── test_cnae_mapping.py
│   │   ├── test_cruzamentos.py
│   │   ├── test_build_atomico.py
│   │   └── test_completude.py
│   │
│   └── fixtures/
│       ├── fornecedores.parquet
│       ├── contratos.parquet
│       ├── socios.parquet
│       ├── sancoes.parquet
│       ├── doacoes.parquet
│       ├── servidores.parquet
│       ├── sample_cnpj.csv
│       ├── sample_qsa.csv
│       └── sample_pncp.json
│
├── docker/
│   ├── Dockerfile.api
│   ├── Dockerfile.web
│   └── Dockerfile.pipeline
│
├── docker-compose.yml
├── pyproject.toml                           # Deps + mypy + pytest + ruff config
├── .env.example                             # CPF_HMAC_SALT, DUCKDB_PATH, etc.
├── .gitignore
├── .pre-commit-config.yaml                  # ruff, mypy, eslint, detect-secrets
├── CLAUDE.md
├── SECURITY.md                              # Política de vulnerabilidades
├── testa-de-ferro-spec.md
├── LICENSE                                  # MIT
└── README.md
```

---

## 10. Projeto Direto Pra Produção (SEM MVP!)

### Fase 1: Dados

- [ ] Pipeline de ingestão da base CNPJ (empresas + QSA) com HMAC de CPFs
- [ ] Pipeline de ingestão do histórico QSA via Juntas Comerciais
- [ ] Pipeline de ingestão do PNCP/ComprasNet
- [ ] Tabela de mapeamento CNAE → categorias de objeto (top 50 CNAEs)
- [ ] Cruzamento: fornecedores do governo → sócios → outras empresas (2 níveis de indireção)
- [ ] Detecção de alertas críticos (sócio em múltiplas fornecedoras, mesmo endereço sem complemento)
- [ ] Cálculo de score cumulativo (CAPITAL_SOCIAL_BAIXO cruzado com CNAE, SANCAO_HISTORICA peso 5)
- [ ] Build atômico do DuckDB com star schema (temp → validação → rename)

### Fase 2: API

- [ ] `GET /fornecedores/{cnpj}` — ficha completa (alertas + score + disclaimer)
- [ ] `GET /fornecedores/{cnpj}/grafo` — rede societária (2 níveis)
- [ ] `GET /fornecedores/{cnpj}/export` — exportação CSV / JSON / PDF
- [ ] `GET /fornecedores/ranking` — ranking por score cumulativo
- [ ] `GET /alertas` — feed de alertas críticos
- [ ] `GET /alertas/{tipo}` — filtro por tipo de alerta
- [ ] `GET /busca?q=` — busca por nome ou CNPJ
- [ ] `GET /stats` — números gerais + metadata de freshness por fonte
- [ ] Rate limiting (60 req/min por IP) + API key opcional para bulk
- [ ] Modo `read_only=True` DuckDB com múltiplos workers

### Fase 3: Frontend

- [ ] Home com feed de alertas críticos recentes + banner de data freshness
- [ ] Página de busca
- [ ] Ficha do fornecedor com alertas agrupados por tipo + score visual + disclaimer
- [ ] Campo de nota oficial / contestação da empresa
- [ ] Grafo societário interativo (1 nível visível, expansão sob demanda, max 50 nós)
- [ ] Ranking de fornecedores por score
- [ ] Página de Metodologia completa (fórmulas, fontes, limitações, changelog)
- [ ] Exportação (CSV / JSON / PDF) por fornecedor

### Fase 4: Expansão

- [ ] Ingestão de sanções (CEIS/CNEP) — alerta só para vigentes, histórico no score
- [ ] Ingestão de doações eleitorais (TSE) — threshold: doação > R$10K E contrato > R$500K
- [ ] Ingestão de servidores públicos — match por nome + CPF parcial
- [ ] Alertas: SOCIO_SERVIDOR_PUBLICO, DOACAO_PARA_CONTRATANTE, EMPRESA_SANCIONADA_CONTRATANDO
- [ ] Alerta TESTA_DE_FERRO expandido (idade + sem histórico + capital desproporcional)
- [ ] Dashboard por órgão
- [ ] Score: indicadores SEM_FUNCIONARIOS e CRESCIMENTO_SUBITO (requer RAIS)
