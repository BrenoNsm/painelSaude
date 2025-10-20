# create_db_and_tables.py
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql
from db_config import DBConfig, as_admin_dsn, as_dsn

DDL = """
-- =========================
--  SCHEMA NORMALIZADO CNES
-- =========================

-- -- (Opcional) Limpar tudo se estiver recriando o banco e quiser garantir
-- DROP VIEW IF EXISTS vw_cnes_tipo_unidade, vw_cnes_equipamento, vw_cnes_leito CASCADE;
-- DROP TABLE IF EXISTS fato_cnes_tipo_unidade, fato_cnes_equipamento, fato_cnes_leito CASCADE;
-- DROP TABLE IF EXISTS dim_item_cnes, dim_competencia, dim_municipio CASCADE;
-- DROP TABLE IF EXISTS siops_tabelas CASCADE;

-- ========= DIMENSÕES =========

-- Municípios (IBGE)
CREATE TABLE IF NOT EXISTS dim_municipio (
  municipio_id      SERIAL PRIMARY KEY,
  codigo_municipio  CHAR(6) UNIQUE NOT NULL,
  uf                CHAR(2) NOT NULL,
  nome              TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_municipio_uf_nome
  ON dim_municipio(uf, nome);

-- Competências (mês/ano) — AAAAMM
CREATE TABLE IF NOT EXISTS dim_competencia (
  competencia_id  SERIAL PRIMARY KEY,
  vcomp           CHAR(6) UNIQUE NOT NULL,         -- AAAAMM
  ano             INTEGER NOT NULL,
  mes             INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
  data_ref        DATE NOT NULL                    -- 1º dia do mês
);

CREATE INDEX IF NOT EXISTS idx_dim_competencia_ano_mes
  ON dim_competencia(ano, mes);

-- Catálogo de itens do CNES (código + tipo)
CREATE TABLE IF NOT EXISTS dim_item_cnes (
  item_id   SERIAL PRIMARY KEY,
  tipo      TEXT NOT NULL CHECK (tipo IN ('leito','equipamento','tipo_unidade')),
  codigo    TEXT NOT NULL,
  grupo     TEXT,
  descricao TEXT,
  UNIQUE (tipo, codigo)
);

CREATE INDEX IF NOT EXISTS idx_dim_item_cnes_tipo_grupo
  ON dim_item_cnes(tipo, grupo);

-- ========= FATOS =========
-- Leitos (valores “fixos” nas colunas)
CREATE TABLE IF NOT EXISTS fato_cnes_leito (
  competencia_id INTEGER NOT NULL REFERENCES dim_competencia(competencia_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  municipio_id   INTEGER NOT NULL REFERENCES dim_municipio(municipio_id)     ON UPDATE CASCADE ON DELETE RESTRICT,
  item_id        INTEGER NOT NULL REFERENCES dim_item_cnes(item_id)          ON UPDATE CASCADE ON DELETE RESTRICT, -- tipo='leito'
  existente      INTEGER,
  sus            INTEGER,
  habilitados    INTEGER,
  loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (competencia_id, municipio_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_fato_leito_mun_comp
  ON fato_cnes_leito(municipio_id, competencia_id);

-- Equipamentos (colunas variáveis em JSONB)
CREATE TABLE IF NOT EXISTS fato_cnes_equipamento (
  competencia_id INTEGER NOT NULL REFERENCES dim_competencia(competencia_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  municipio_id   INTEGER NOT NULL REFERENCES dim_municipio(municipio_id)     ON UPDATE CASCADE ON DELETE RESTRICT,
  item_id        INTEGER NOT NULL REFERENCES dim_item_cnes(item_id)          ON UPDATE CASCADE ON DELETE RESTRICT, -- tipo='equipamento'
  metrics        JSONB   NOT NULL DEFAULT '{}'::jsonb,
  loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (competencia_id, municipio_id, item_id),
  CONSTRAINT chk_metrics_obj CHECK (jsonb_typeof(metrics) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_fato_equip_mun_comp
  ON fato_cnes_equipamento(municipio_id, competencia_id);
CREATE INDEX IF NOT EXISTS idx_fato_equip_metrics_gin
  ON fato_cnes_equipamento USING GIN (metrics);

-- Tipo de Unidade (colunas variáveis em JSONB)
CREATE TABLE IF NOT EXISTS fato_cnes_tipo_unidade (
  competencia_id INTEGER NOT NULL REFERENCES dim_competencia(competencia_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  municipio_id   INTEGER NOT NULL REFERENCES dim_municipio(municipio_id)     ON UPDATE CASCADE ON DELETE RESTRICT,
  item_id        INTEGER NOT NULL REFERENCES dim_item_cnes(item_id)          ON UPDATE CASCADE ON DELETE RESTRICT, -- tipo='tipo_unidade'
  metrics        JSONB   NOT NULL DEFAULT '{}'::jsonb,
  loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (competencia_id, municipio_id, item_id),
  CONSTRAINT chk_metrics2_obj CHECK (jsonb_typeof(metrics) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_fato_tipoun_mun_comp
  ON fato_cnes_tipo_unidade(municipio_id, competencia_id);
CREATE INDEX IF NOT EXISTS idx_fato_tipoun_metrics_gin
  ON fato_cnes_tipo_unidade USING GIN (metrics);

-- ========= SIOPS (relacionado por município) =========
-- Mantém a matriz original, mas já com FK para município
CREATE TABLE IF NOT EXISTS siops_tabelas (
  municipio_id INTEGER NOT NULL REFERENCES dim_municipio(municipio_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  ano          INTEGER NOT NULL,
  periodo      TEXT    NOT NULL,            -- ex.: 1º bim, 2º bim, 1º sem, Anual (depende do site)
  tabela_idx   INTEGER NOT NULL,            -- ordem da tabela capturada
  titulo       TEXT,
  matrix       JSONB   NOT NULL,
  loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (municipio_id, ano, periodo, tabela_idx),
  CONSTRAINT chk_siops_matrix_arr_or_obj CHECK (jsonb_typeof(matrix) IN ('array','object'))
);

CREATE INDEX IF NOT EXISTS idx_siops_mun_ano
  ON siops_tabelas(municipio_id, ano);
CREATE INDEX IF NOT EXISTS idx_siops_matrix_gin
  ON siops_tabelas USING GIN (matrix);

-- ========= VIEWS AMIGÁVEIS =========

CREATE OR REPLACE VIEW vw_cnes_leito AS
SELECT
  f.competencia_id, f.municipio_id, f.item_id,
  d.vcomp, d.ano, d.mes, d.data_ref,
  m.nome AS municipio_nome, m.uf,
  i.codigo AS codigo_item, i.grupo AS grupo_item, i.descricao AS descricao_item,
  f.existente, f.sus, f.habilitados, f.loaded_at
FROM fato_cnes_leito f
JOIN dim_competencia d ON d.competencia_id = f.competencia_id
JOIN dim_municipio   m ON m.municipio_id   = f.municipio_id
JOIN dim_item_cnes   i ON i.item_id        = f.item_id;

CREATE OR REPLACE VIEW vw_cnes_equipamento AS
SELECT
  f.competencia_id, f.municipio_id, f.item_id,
  d.vcomp, d.ano, d.mes, d.data_ref,
  m.nome AS municipio_nome, m.uf,
  i.codigo AS codigo_item, i.grupo AS grupo_item, i.descricao AS descricao_item,
  f.metrics, f.loaded_at
FROM fato_cnes_equipamento f
JOIN dim_competencia d ON d.competencia_id = f.competencia_id
JOIN dim_municipio   m ON m.municipio_id   = f.municipio_id
JOIN dim_item_cnes   i ON i.item_id        = f.item_id;

CREATE OR REPLACE VIEW vw_cnes_tipo_unidade AS
SELECT
  f.competencia_id, f.municipio_id, f.item_id,
  d.vcomp, d.ano, d.mes, d.data_ref,
  m.nome AS municipio_nome, m.uf,
  i.codigo AS codigo_item, i.grupo AS grupo_item, i.descricao AS descricao_item,
  f.metrics, f.loaded_at
FROM fato_cnes_tipo_unidade f
JOIN dim_competencia d ON d.competencia_id = f.competencia_id
JOIN dim_municipio   m ON m.municipio_id   = f.municipio_id
JOIN dim_item_cnes   i ON i.item_id        = f.item_id;

-- ========= COMENTÁRIOS ÚTEIS =========
COMMENT ON TABLE dim_municipio IS 'Dimensão de municípios (IBGE).';
COMMENT ON COLUMN dim_municipio.codigo_municipio IS 'Código IBGE de 6 dígitos.';
COMMENT ON TABLE dim_competencia IS 'Dimensão de competências mensais (AAAAMM).';
COMMENT ON TABLE dim_item_cnes IS 'Catálogo de itens do CNES por tipo (leito/equipamento/tipo_unidade).';
COMMENT ON TABLE fato_cnes_leito IS 'Fato de leitos por competência/município/item.';
COMMENT ON TABLE fato_cnes_equipamento IS 'Fato de equipamentos por competência/município/item; métricas em JSONB.';
COMMENT ON TABLE fato_cnes_tipo_unidade IS 'Fato de tipos de unidade por competência/município/item; métricas em JSONB.';
COMMENT ON TABLE siops_tabelas IS 'Resultados do SIOPS por município/ano/período; cada tabela em JSONB (matrix).';

"""

def ensure_database(cfg: DBConfig):
    import psycopg2
    from psycopg2 import sql, extensions

    # Conecta no banco "postgres" para criar o alvo
    conn = psycopg2.connect(as_admin_dsn(cfg))
    try:
        # ⚠️ Liga AUTOCOMMIT de verdade (sem context manager)
        conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (cfg.database,))
        exists = cur.fetchone() is not None

        if not exists:
            # cria o DB fora de transação
            cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cfg.database)))
            print(f"✅ Banco criado: {cfg.database}")
        else:
            print(f"ℹ️ Banco já existe: {cfg.database}")
        cur.close()
    finally:
        conn.close()

def apply_ddl(cfg: DBConfig):
    with psycopg2.connect(as_dsn(cfg)) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
    print("✅ DDL aplicado.")

if __name__ == "__main__":
    cfg = DBConfig()
    ensure_database(cfg)
    apply_ddl(cfg)
