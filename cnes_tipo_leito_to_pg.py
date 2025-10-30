# cnes_tipo_leito_to_pg.py
import time
import re
import argparse
import pandas as pd

from db_config import DBConfig
from db_utils import (
    get_conn, upsert_dicts,
    get_or_create_competencia, get_or_create_municipio, get_or_create_item
)

# ========= importa do SEU scraper de leitos =========
from scrape_cnes_leito import (
    baixar_municipios_ibge,      # -> lista [{'codigo':'140010','nome':'Boa Vista'}, ...]
    gerar_competencias,          # -> lista ['201201', ..., 'AAAAmm']
    fetch_tabela_tipo_leito,     # -> DataFrame por (vmun6, vcomp)
    VCOMP_INICIO, VCOMP_FIM,
    SLEEP_ENTRE_REQUISICOES
)

# ========= helpers =========

def _to_int(v) -> int:
    if v is None:
        return 0
    t = str(v).strip()
    if t == "" or t in {"-", "NA", "N/A"}:
        return 0
    t = (t.replace("\u00A0", "")
           .replace(" ", "")
           .replace(".", "")
           .replace(",", ""))
    try:
        return int(float(t))
    except Exception:
        return 0

def _is_total_row(row: pd.Series) -> bool:
    # seu parser já remove TOTAL, mas deixo a defesa
    joined = " ".join(str(x) for x in row.values[:2]).upper()
    return "TOTAL" in joined

def _fix_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza cabeçalhos para: Codigo | Descricao | (opcional Grupo) | Existente | (SUS?) | (Habilitados?)
    O seu parser já entrega essas colunas, mas mantemos robusto.
    """
    def norm_token(s: str) -> str:
        s = str(s or "")
        s = (s.replace("<br>", " ").replace("<br/>", " ").replace("<br />", " ")
               .replace("\n", " ").replace("\r", " "))
        s = re.sub(r"\s+", " ", s).strip().lower()
        s = (s.replace("ç", "c").replace("á", "a").replace("ã", "a")
               .replace("â", "a").replace("é", "e").replace("ê", "e")
               .replace("í", "i").replace("ó", "o").replace("ô", "o")
               .replace("ú", "u").replace("ü", "u"))
        s = s.replace("_", " ").replace("-", " ")
        return s

    mapping = {}
    for c in df.columns:
        k = norm_token(c)
        if k in ("codigo", "código", "cod", "cod.", "codigo leito"):
            mapping[c] = "Codigo"
        elif k in ("descricao", "descrição", "tipo de leito", "leito"):
            mapping[c] = "Descricao"
        elif k in ("grupo", "categoria"):
            mapping[c] = "Grupo"
        elif k in ("existente", "existentes", "qtd existente", "qtd existentes"):
            mapping[c] = "Existente"
        elif k in ("sus", "leitos sus", "quantidade sus"):
            mapping[c] = "SUS"
        elif k in ("habilitado", "habilitados", "leitos habilitados", "qtd habilitados"):
            mapping[c] = "Habilitados"
        else:
            mapping[c] = c

    df2 = df.copy()
    df2.columns = [mapping[c] for c in df.columns]
    return df2

# ========= conversão p/ fato =========

def df_to_rows_fato(conn, df: pd.DataFrame, vcomp: str, codigo_municipio: str, uf: str, nome_municipio: str):
    """
    1) Corrige cabeçalho
    2) Garante colunas Existente/SUS/Habilitados (faltantes => 0)
    3) Agrega por Codigo/Grupo/Descricao
    4) Resolve FKs (competencia, municipio, item)
    5) Retorna linhas p/ fato_cnes_leito
    """
    if df is None or df.empty:
        return []

    df = _fix_headers(df)

    # chaves mínimas
    if "Codigo" not in df.columns or "Descricao" not in df.columns:
        raise ValueError(f"Esperava ao menos Codigo/Descricao (colunas={list(df.columns)})")
    if "Grupo" not in df.columns:
        df["Grupo"] = pd.NA

    # métricas (se faltarem, cria com zero)
    for col in ["Existente", "SUS", "Habilitados"]:
        if col not in df.columns:
            df[col] = 0

    # remove TOTALs defensivo
    df = df[~df.apply(_is_total_row, axis=1)]

    # coage para inteiro
    for col in ["Existente", "SUS", "Habilitados"]:
        df[col] = df[col].apply(_to_int).astype(int)

    # agrega por item
    agg = df.groupby(["Codigo", "Grupo", "Descricao"], dropna=False).agg({
        "Existente": "sum",
        "SUS": "sum",
        "Habilitados": "sum",
    }).reset_index()

    # FKs
    comp_id = get_or_create_competencia(conn, vcomp)
    mun_id  = get_or_create_municipio(conn, codigo_municipio, uf, nome_municipio)

    rows = []
    for _, r in agg.iterrows():
        item_id = get_or_create_item(
            conn, "leito",
            codigo=str(r["Codigo"]),
            grupo=(None if pd.isna(r.get("Grupo")) else str(r["Grupo"])),
            descricao=(None if pd.isna(r.get("Descricao")) else str(r["Descricao"]))
        )
        rows.append({
            "competencia_id": comp_id,
            "municipio_id": mun_id,
            "item_id": item_id,
            "existente": int(r["Existente"]),
            "sus": int(r["SUS"]),
            "habilitados": int(r["Habilitados"]),
        })
    return rows

def dedupe_leito_batch(rows):
    """
    Unicidade por (competencia_id, municipio_id, item_id).
    Se houver duplicatas, soma as 3 métricas.
    """
    merged = {}
    for r in rows:
        k = (r["competencia_id"], r["municipio_id"], r["item_id"])
        if k not in merged:
            merged[k] = dict(r)
        else:
            dst = merged[k]
            for mcol in ("existente", "sus", "habilitados"):
                dst[mcol] = int(dst.get(mcol, 0)) + int(r.get(mcol, 0))
    return list(merged.values())

# ========= main =========

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    cfg = DBConfig()
    municipios   = baixar_municipios_ibge()
    competencias = gerar_competencias(VCOMP_INICIO, VCOMP_FIM)

    total = 0
    with get_conn(cfg) as conn:
        for vcomp in competencias:
            # skip por competência (se já existe algo desse vcomp)
            if not args.force:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 1
                        FROM fato_cnes_leito f
                        JOIN dim_competencia d ON d.competencia_id = f.competencia_id
                        WHERE d.vcomp = %s
                        LIMIT 1
                    """, (vcomp,))
                    if cur.fetchone():
                        print(f"[SKIP] {vcomp}: já existe em fato_cnes_leito")
                        continue

            batch = []
            for m in municipios:
                try:
                    df = fetch_tabela_tipo_leito(m["codigo"], vcomp)
                except Exception as e:
                    print(f"[WARN] {vcomp}/{m.get('nome','?')}: erro no fetch ({e}) — pulando.")
                    continue

                time.sleep(SLEEP_ENTRE_REQUISICOES)
                if df is None or df.empty:
                    continue

                try:
                    rows = df_to_rows_fato(conn, df, vcomp, m["codigo"], "RR", m["nome"])
                except Exception as e:
                    print(f"[WARN] {vcomp}/{m['nome']}: falha na normalização ({e}) — pulando município.")
                    continue
                batch.extend(rows)

            if batch:
                batch = dedupe_leito_batch(batch)
                inserted = upsert_dicts(
                    conn,
                    table="fato_cnes_leito",
                    rows=batch,
                    pkey_cols=["competencia_id", "municipio_id", "item_id"],
                    update_cols=["existente", "sus", "habilitados"]
                )
                conn.commit()
                total += inserted
                print(f"[OK] {vcomp}: upsert {inserted} (acum={total})")
            else:
                print(f"[SKIP] {vcomp}: sem dados")

    print(f"Concluído. Total upsert: {total}")

if __name__ == "__main__":
    main()