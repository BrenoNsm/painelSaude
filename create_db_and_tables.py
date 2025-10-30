# create_db_and_tables.py
"""
Cria/atualiza o schema do projeto (idempotente) usando db_config.py.

Dimensões:
  - dim_municipio(codigo_municipio, uf, nome)
  - dim_competencia(vcomp=AAAAMM, ano, mes, data_ref=1º dia do mês)
  - dim_item_cnes(tipo in ['leito','equipamento','tipo_unidade'], codigo, grupo, descricao)

Fatos CNES:
  - fato_cnes_leito(existente, sus, habilitados)
  - fato_cnes_equipamento(existentes, em_uso, existentes_sus, em_uso_sus)
  - fato_cnes_tipo_unidade(total)   << inteiro

SIOPS (raw):
  - siops_tabelas(matrix JSONB)

Views:
  - vw_cnes_leito, vw_cnes_equipamento, vw_cnes_tipo_unidade
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from db_config import DBConfig, as_admin_dsn, as_dsn

DDL = r"""
-- =========================
-- DIMENSÕES
-- =========================
CREATE TABLE IF NOT EXISTS dim_municipio (
  municipio_id      SERIAL PRIMARY KEY,
  codigo_municipio  CHAR(6) UNIQUE NOT NULL,
  uf                CHAR(2) NOT NULL,
  nome              TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dim_municipio_uf_nome ON dim_municipio(uf, nome);

CREATE TABLE IF NOT EXISTS dim_competencia (
  competencia_id  SERIAL PRIMARY KEY,
  vcomp           CHAR(6) UNIQUE NOT NULL,   -- AAAAMM
  ano             INTEGER NOT NULL,
  mes             INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
  data_ref        DATE NOT NULL              -- 1º dia do mês
);
CREATE INDEX IF NOT EXISTS idx_dim_competencia_ano_mes ON dim_competencia(ano, mes);

CREATE TABLE IF NOT EXISTS dim_item_cnes (
  item_id   SERIAL PRIMARY KEY,
  tipo      TEXT NOT NULL CHECK (tipo IN ('leito','equipamento','tipo_unidade')),
  codigo    TEXT NOT NULL,
  grupo     TEXT,
  descricao TEXT,
  UNIQUE (tipo, codigo)
);
CREATE INDEX IF NOT EXISTS idx_dim_item_cnes_tipo_grupo ON dim_item_cnes(tipo, grupo);

-- =========================
-- FATOS CNES
-- =========================

-- Leitos
CREATE TABLE IF NOT EXISTS fato_cnes_leito (
  competencia_id INTEGER NOT NULL REFERENCES dim_competencia(competencia_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  municipio_id   INTEGER NOT NULL REFERENCES dim_municipio(municipio_id)     ON UPDATE CASCADE ON DELETE RESTRICT,
  item_id        INTEGER NOT NULL REFERENCES dim_item_cnes(item_id)          ON UPDATE CASCADE ON DELETE RESTRICT,
  existente      INTEGER,
  sus            INTEGER,
  habilitados    INTEGER,
  loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (competencia_id, municipio_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_fato_leito_mun_comp ON fato_cnes_leito(municipio_id, competencia_id);

-- Equipamentos
CREATE TABLE IF NOT EXISTS fato_cnes_equipamento (
  competencia_id INTEGER NOT NULL REFERENCES dim_competencia(competencia_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  municipio_id   INTEGER NOT NULL REFERENCES dim_municipio(municipio_id)     ON UPDATE CASCADE ON DELETE RESTRICT,
  item_id        INTEGER NOT NULL REFERENCES dim_item_cnes(item_id)          ON UPDATE CASCADE ON DELETE RESTRICT,
  existentes     INTEGER,
  em_uso         INTEGER,
  existentes_sus INTEGER,
  em_uso_sus     INTEGER,
  loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (competencia_id, municipio_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_fato_equip_mun_comp ON fato_cnes_equipamento(municipio_id, competencia_id);

-- Tipo de unidade (TOTAL INTEGER)
CREATE TABLE IF NOT EXISTS fato_cnes_tipo_unidade (
  competencia_id INTEGER NOT NULL REFERENCES dim_competencia(competencia_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  municipio_id   INTEGER NOT NULL REFERENCES dim_municipio(municipio_id)     ON UPDATE CASCADE ON DELETE RESTRICT,
  item_id        INTEGER NOT NULL REFERENCES dim_item_cnes(item_id)          ON UPDATE CASCADE ON DELETE RESTRICT,
  total          INTEGER,
  loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (competencia_id, municipio_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_fato_tipoun_mun_comp ON fato_cnes_tipo_unidade(municipio_id, competencia_id);

-- =========================
-- SIOPS (RAW)
-- =========================
CREATE TABLE IF NOT EXISTS siops_tabelas (
  municipio_id INTEGER NOT NULL REFERENCES dim_municipio(municipio_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  ano          INTEGER NOT NULL,
  periodo      TEXT    NOT NULL,   -- '1º', '2º', '3º', '4º'
  tabela_idx   INTEGER NOT NULL,
  titulo       TEXT,
  matrix       JSONB   NOT NULL,
  loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (municipio_id, ano, periodo, tabela_idx),
  CONSTRAINT chk_siops_matrix_arr_or_obj CHECK (jsonb_typeof(matrix) IN ('array','object'))
);
CREATE INDEX IF NOT EXISTS idx_siops_mun_ano ON siops_tabelas(municipio_id, ano);
CREATE INDEX IF NOT EXISTS idx_siops_matrix_gin ON siops_tabelas USING GIN (matrix);

-- =========================
-- VIEWS
-- =========================
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
  f.existentes, f.em_uso, f.existentes_sus, f.em_uso_sus, f.loaded_at
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
  f.total, f.loaded_at
FROM fato_cnes_tipo_unidade f
JOIN dim_competencia d ON d.competencia_id = f.competencia_id
JOIN dim_municipio   m ON m.municipio_id   = f.municipio_id
JOIN dim_item_cnes   i ON i.item_id        = f.item_id;
"""

def ensure_database(cfg: DBConfig):
    """
    Cria o banco cfg.database se ainda não existir.
    Usa conexão no DB administrativo (por padrão 'postgres') sem transação.
    """
    admin_dsn = as_admin_dsn(cfg)
    conn = psycopg2.connect(admin_dsn)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (cfg.database,))
        if cur.fetchone():
            print(f"[DB] Banco '{cfg.database}' já existe.")
        else:
            print(f"[DB] Criando banco '{cfg.database}'...")
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cfg.database)))
    conn.close()

def create_schema(cfg: DBConfig):
    """
    Executa o DDL no banco alvo.
    """
    dsn = as_dsn(cfg)
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.close()
    print("✅ Schema criado/atualizado com sucesso (tipo_unidade.total = INTEGER).")

def main():
    cfg = DBConfig()  # usa PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE se definidos, senão os defaults do db_config.py
    ensure_database(cfg)
    create_schema(cfg)

if __name__ == "__main__":
    main()
