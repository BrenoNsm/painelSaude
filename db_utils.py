# db_utils.py
import psycopg2
from psycopg2.extras import execute_values
from typing import Iterable, Mapping, Any
from db_config import DBConfig, as_dsn
from datetime import date

# db_utils.py (acrescente ao final)
from psycopg2 import sql

def rows_exist(conn, table: str, where: dict) -> bool:
    """
    Retorna True se existir pelo menos 1 linha em 'table' que satisfaça 'where'.
    Ex.: rows_exist(conn, "cnes_tipo_leito", {"vcomp": "202401", "codigo_municipio": "140010"})
    """
    keys = list(where.keys())
    query = sql.SQL("SELECT 1 FROM {tbl} WHERE " + " AND ".join([f"{k} = %s" for k in keys]) + " LIMIT 1") \
            .format(tbl=sql.Identifier(table))
    vals = [where[k] for k in keys]
    with conn.cursor() as cur:
        cur.execute(query, vals)
        return cur.fetchone() is not None


def get_conn(cfg: DBConfig):
    return psycopg2.connect(as_dsn(cfg))

def get_or_create_municipio(conn, codigo_municipio: str, uf: str, nome: str) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dim_municipio (codigo_municipio, uf, nome)
            VALUES (%s, %s, %s)
            ON CONFLICT (codigo_municipio)
            DO UPDATE SET uf = EXCLUDED.uf, nome = EXCLUDED.nome
            RETURNING municipio_id;
        """, (codigo_municipio, uf, nome))
        return cur.fetchone()[0]

def get_or_create_competencia(conn, vcomp: str) -> int:
    ano = int(vcomp[:4]); mes = int(vcomp[4:6])
    data_ref = date(ano, mes, 1)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dim_competencia (vcomp, ano, mes, data_ref)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (vcomp)
            DO UPDATE SET ano=EXCLUDED.ano, mes=EXCLUDED.mes, data_ref=EXCLUDED.data_ref
            RETURNING competencia_id;
        """, (vcomp, ano, mes, data_ref))
        return cur.fetchone()[0]

def get_or_create_item(conn, tipo: str, codigo: str, grupo: str|None, descricao: str|None) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dim_item_cnes (tipo, codigo, grupo, descricao)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tipo, codigo)
            DO UPDATE SET grupo=EXCLUDED.grupo,
                         descricao=COALESCE(EXCLUDED.descricao, dim_item_cnes.descricao)
            RETURNING item_id;
        """, (tipo, codigo, grupo, descricao))
        return cur.fetchone()[0]

def upsert_dicts(
    conn,
    table: str,
    rows: Iterable[Mapping[str, Any]],
    pkey_cols: list[str],
    update_cols: list[str]
):
    rows = list(rows)
    if not rows:
        return 0

    cols = list(rows[0].keys())
    template = "(" + ",".join([f"%({c})s" for c in cols]) + ")"

    set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])
    pkeys = ", ".join(pkey_cols)

    sql = f"""
        INSERT INTO {table} ({", ".join(cols)})
        VALUES %s
        ON CONFLICT ({pkeys})
        DO UPDATE SET {set_clause}, loaded_at=NOW();
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template=template, page_size=1000)
    return len(rows)
