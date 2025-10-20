# cnes_equipamentos_to_pg.py
import time, pandas as pd, json
from db_config import DBConfig
import argparse
from db_utils import (
    get_conn, upsert_dicts,
    get_or_create_competencia, get_or_create_municipio, get_or_create_item
)
from scrape_cnes_rr_equipamentos import (
    baixar_municipios_ibge, gerar_competencias,
    fetch_equipamentos, VCOMP_INICIO, VCOMP_FIM, SLEEP_ENTRE_REQUISICOES
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
    """
    1) Agrega por Codigo/Grupo/Descricao somando métricas numéricas
    2) Resolve FKs (competencia, municipio, item)
    3) Monta uma linha por item_id com metrics JSONB
    """
    comp_id = get_or_create_competencia(conn, vcomp)
    mun_id  = get_or_create_municipio(conn, codigo_municipio, uf, nome_municipio)

    # Colunas fixas que NÃO vão para metrics
    fixed = ["Codigo", "Grupo", "Descricao"]

    # Se alguma estiver faltando, cria vazia
    for col in fixed:
        if col not in df.columns:
            df[col] = pd.NA

    # Separa colunas numéricas candidatas a soma
    num_cols = [c for c in df.columns if c not in fixed and pd.api.types.is_numeric_dtype(df[c])]

    # Agrega por Codigo/Grupo/Descricao somando só as numéricas
    if num_cols:
        df_agg_num = df.groupby(["Codigo", "Grupo", "Descricao"], dropna=False)[num_cols].sum(min_count=1).reset_index()
        # Para colunas não numéricas extras (se existirem), ignoramos na métrica para evitar duplicidade
        df_agg = df_agg_num
    else:
        # Não há colunas numéricas → apenas drop de duplicatas
        df_agg = df.drop_duplicates(subset=["Codigo", "Grupo", "Descricao"], keep="first").copy()

    rows = []
    for _, r in df_agg.iterrows():
        item_id = get_or_create_item(
            conn, "equipamento",
            codigo=str(r["Codigo"]),
            grupo=(None if pd.isna(r.get("Grupo")) else str(r["Grupo"])),
            descricao=(None if pd.isna(r.get("Descricao")) else str(r["Descricao"]))
        )
        # monta metrics só com colunas numéricas agregadas
        metrics = {}
        for c in num_cols:
            v = r.get(c)
            if v is not None and not pd.isna(v):
                try:
                    metrics[c] = int(v)
                except Exception:
                    # fallback caso venha float não exato
                    try:
                        metrics[c] = int(round(float(v)))
                    except Exception:
                        metrics[c] = float(v)

        rows.append({
            "competencia_id": comp_id,
            "municipio_id": mun_id,
            "item_id": item_id,
            "metrics": json.dumps(metrics, ensure_ascii=False)
        })
    return rows

def dedupe_equip_batch(rows):
    """
    Garante unicidade por (competencia_id, municipio_id, item_id).
    Se houver duplicatas, mescla metrics somando chaves numéricas e
    sobrescrevendo chaves não numéricas pelo último valor.
    """
    merged = {}
    for r in rows:
        k = (r["competencia_id"], r["municipio_id"], r["item_id"])
        cur = json.loads(r["metrics"]) if isinstance(r["metrics"], str) else r["metrics"]
        if k not in merged:
            merged[k] = {"competencia_id": r["competencia_id"],
                         "municipio_id": r["municipio_id"],
                         "item_id": r["item_id"],
                         "metrics": dict(cur)}
        else:
            dst = merged[k]["metrics"]
            for mk, mv in cur.items():
                if mk in dst:
                    # tenta somar se ambos são números; senão, sobrescreve
                    try:
                        dst[mk] = (int(dst[mk]) if not isinstance(dst[mk], bool) else int(dst[mk])) + \
                                  (int(mv)      if not isinstance(mv, bool)      else int(mv))
                    except Exception:
                        dst[mk] = mv
                else:
                    dst[mk] = mv
    # volta para lista JSON-serializada
    out = []
    for v in merged.values():
        v["metrics"] = json.dumps(v["metrics"], ensure_ascii=False)
        out.append(v)
    return out



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
                    cur.execute("SELECT 1 FROM fato_cnes_equipamento f JOIN dim_competencia d ON d.competencia_id=f.competencia_id WHERE d.vcomp=%s LIMIT 1", (vcomp,))
                    if cur.fetchone():
                        print(f"[SKIP] {vcomp}: já existe em fato_cnes_equipamento")
                        continue

            batch = []
            for m in municipios:
                df = fetch_equipamentos(m["codigo"], vcomp)
                time.sleep(SLEEP_ENTRE_REQUISICOES)
                if df is None or df.empty:
                    continue
                batch.extend(df_to_rows_fato(conn, df, vcomp, m["codigo"], "RR", m["nome"]))

            if batch:
                batch = dedupe_equip_batch(batch)  # <--- ADICIONE ESTA LINHA
                inserted = upsert_dicts(
                    conn,
                    table="fato_cnes_equipamento",
                    rows=batch,
                    pkey_cols=["competencia_id", "municipio_id", "item_id"],
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
