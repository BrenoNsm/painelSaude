# cnes_tipo_unidade_to_pg.py
import time, pandas as pd, json
from db_config import DBConfig
import argparse
from db_utils import (
    get_conn, upsert_dicts,
    get_or_create_competencia, get_or_create_municipio, get_or_create_item
)
from scrape_cnes_rr_tipo_unidade import (
    baixar_municipios_ibge, gerar_competencias,
    fetch_tipos_unidade, VCOMP_INICIO, VCOMP_FIM, SLEEP_ENTRE_REQUISICOES
)

def df_to_rows(df: pd.DataFrame):
    fixed = {"Grupo","Codigo","Descricao","VComp","UF","Codigo_Municipio","Municipio"}
    metric_cols = [c for c in df.columns if c not in fixed]
    for _, r in df.iterrows():
        metrics = {}
        for c in metric_cols:
            v = r[c]
            if not pd.isna(v):
                metrics[c] = int(v)
        yield {
            "vcomp":            str(r["VComp"]),
            "uf":               str(r["UF"]),
            "codigo_municipio": str(r["Codigo_Municipio"]),
            "municipio":        str(r["Municipio"]),
            "grupo":            (None if pd.isna(r["Grupo"]) else str(r["Grupo"])),
            "codigo":           str(r["Codigo"]),
            "descricao":        (None if pd.isna(r["Descricao"]) else str(r["Descricao"])),
            "metrics":          json.dumps(metrics, ensure_ascii=False)
        }

def df_to_rows_fato(conn, df: pd.DataFrame, vcomp: str, codigo_municipio: str, uf: str, nome_municipio: str):
    comp_id = get_or_create_competencia(conn, vcomp)
    mun_id  = get_or_create_municipio(conn, codigo_municipio, uf, nome_municipio)

    fixed = {"Grupo","Codigo","Descricao"}
    metric_cols = [c for c in df.columns if c not in fixed]
    rows = []
    for _, r in df.iterrows():
        item_id = get_or_create_item(
            conn, "tipo_unidade",
            codigo=str(r["Codigo"]),
            grupo=(None if pd.isna(r.get("Grupo")) else str(r["Grupo"])),
            descricao=(None if pd.isna(r.get("Descricao")) else str(r["Descricao"]))
        )
        metrics = {}
        for c in metric_cols:
            v = r.get(c)
            if v is not None and not pd.isna(v):
                try:
                    metrics[c] = int(v)
                except Exception:
                    metrics[c] = str(v)
        rows.append({
            "competencia_id": comp_id,
            "municipio_id": mun_id,
            "item_id": item_id,
            "metrics": json.dumps(metrics, ensure_ascii=False)
        })
    return rows

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    cfg = DBConfig()
    municipios = baixar_municipios_ibge()
    competencias = gerar_competencias(VCOMP_INICIO, VCOMP_FIM)

    total = 0
    with get_conn(cfg) as conn:
        for vcomp in competencias:
            if not args.force:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM fato_cnes_tipo_unidade f JOIN dim_competencia d ON d.competencia_id=f.competencia_id WHERE d.vcomp=%s LIMIT 1", (vcomp,))
                    if cur.fetchone():
                        print(f"[SKIP] {vcomp}: já existe em fato_cnes_tipo_unidade")
                        continue

            batch = []
            for m in municipios:
                df = fetch_tipos_unidade(m["codigo"], vcomp)
                time.sleep(SLEEP_ENTRE_REQUISICOES)
                if df is None or df.empty:
                    continue
                batch.extend(df_to_rows_fato(conn, df, vcomp, m["codigo"], "RR", m["nome"]))

            if batch:
                inserted = upsert_dicts(
                    conn,
                    table="fato_cnes_tipo_unidade",
                    rows=batch,
                    pkey_cols=["competencia_id","municipio_id","item_id"],
                    update_cols=["metrics"]
                )
                conn.commit()
                total += inserted
                print(f"[OK] {vcomp}: upsert {inserted} (acum={total})")
            else:
                print(f"[SKIP] {vcomp}: sem dados")
    print(f"Concluído. Total upsert: {total}")

if __name__ == "__main__":
    main()
