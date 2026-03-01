-- Star Schema — Testa de Ferro
-- Executado pelo build_duckdb.py durante a construção do banco

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
    max_severidade  VARCHAR(20),
    total_contratos  INTEGER DEFAULT 0,
    valor_total      DECIMAL(18,2) DEFAULT 0,
    qtd_funcionarios INTEGER,
    atualizado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    cpf_hmac              VARCHAR(64),
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
    fk_fornecedor       INTEGER NOT NULL,
    fk_socio            INTEGER NOT NULL,
    data_entrada        DATE,
    data_saida          DATE,
    percentual_capital  DECIMAL(5,2),
    PRIMARY KEY (fk_fornecedor, fk_socio)
);

-- =============================================
-- FATOS
-- =============================================

-- ADR: FK constraints on fk_fornecedor/fk_orgao/fk_tempo/fk_modalidade are
-- relaxed (nullable, no FK reference) because the dimension lookup tables
-- (dim_orgao, dim_tempo, dim_modalidade) are not yet populated by the pipeline.
-- The raw data columns (cnpj_fornecedor, codigo_orgao, data_assinatura) are
-- used directly for queries until the full star schema build is implemented.
CREATE TABLE fato_contrato (
    pk_contrato     INTEGER PRIMARY KEY,
    fk_fornecedor   INTEGER,
    fk_orgao        INTEGER,
    fk_tempo        INTEGER,
    fk_modalidade   INTEGER,
    cnpj_fornecedor VARCHAR(18),
    codigo_orgao    VARCHAR(20),
    nome_orgao      VARCHAR(255),
    modalidade_nome VARCHAR(100),
    valor           DECIMAL(18,2) NOT NULL,
    objeto          VARCHAR(1000),
    num_licitacao   VARCHAR(50),
    data_assinatura DATE,
    data_vigencia   DATE
);

-- ADR: FK constraints relaxed — dim_candidato and dim_tempo not yet populated.
-- Raw columns (nome_candidato, data_receita etc.) stored directly.
CREATE TABLE fato_doacao (
    pk_doacao           INTEGER PRIMARY KEY,
    fk_fornecedor       INTEGER,
    fk_socio            INTEGER,
    fk_candidato        INTEGER,
    fk_tempo            INTEGER,
    ano_eleicao         SMALLINT NOT NULL,
    cnpj_doador         VARCHAR(18),
    cpf_doador          VARCHAR(14),
    nome_doador         VARCHAR(255),
    nome_candidato      VARCHAR(255),
    partido_candidato   VARCHAR(50),
    cargo_candidato     VARCHAR(100),
    uf_candidato        CHAR(2),
    tipo_recurso        VARCHAR(50),
    valor               DECIMAL(18,2) NOT NULL,
    data_receita        DATE
);

CREATE TABLE fato_score_detalhe (
    pk_score_detalhe INTEGER PRIMARY KEY,
    fk_fornecedor    INTEGER NOT NULL,
    indicador        VARCHAR(50) NOT NULL,
    peso             SMALLINT NOT NULL,
    descricao        VARCHAR(500),
    evidencia        VARCHAR(500),
    calculado_em     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fato_alerta_critico (
    pk_alerta       INTEGER PRIMARY KEY,
    fk_fornecedor   INTEGER NOT NULL,
    fk_socio        INTEGER,
    tipo_alerta     VARCHAR(50) NOT NULL,
    severidade      VARCHAR(20) NOT NULL,
    descricao       VARCHAR(500) NOT NULL,
    evidencia       VARCHAR(1000) NOT NULL,
    detectado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_sancao (
    pk_sancao           INTEGER PRIMARY KEY,
    fk_fornecedor       INTEGER,
    cnpj                VARCHAR(18),
    razao_social        VARCHAR(255),
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

-- Composite indexes for common API query patterns
CREATE INDEX idx_alerta_sev_tipo ON fato_alerta_critico(severidade, tipo_alerta);
CREATE INDEX idx_fornecedor_ranking ON dim_fornecedor(faixa_risco, uf, score_risco DESC);
CREATE INDEX idx_sancao_data_fim ON dim_sancao(data_fim);
CREATE INDEX idx_contrato_fornecedor_data ON fato_contrato(fk_fornecedor, data_assinatura);
