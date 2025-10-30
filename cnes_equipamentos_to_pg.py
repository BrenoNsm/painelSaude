# cnes_equipamentos_to_pg.py
import time, re, argparse
import pandas as pd

from db_config import DBConfig
from db_utils import (
    get_conn, upsert_dicts,
    get_or_create_competencia, get_or_create_municipio, get_or_create_item
)

# ===== usa SEU scraper de equipamentos =====
from scrape_cnes_rr_equipamentos import (
    baixar_municipios_ibge,
    gerar_competencias,
    fetch_equipamentos,            # <- DataFrame por (codigo_municipio, vcomp)
    VCOMP_INICIO, VCOMP_FIM,
    SLEEP_ENTRE_REQUISICOES
)

# --------------- utils ---------------

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
    j = " ".join(str(x) for x in row.values[:3]).upper()
    return "TOTAL" in j

def _norm_token(s: str) -> str:
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

def _fix_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza cabeçalhos para:
      Grupo | Codigo | Descricao | Existentes | Em Uso | Existentes SUS | Em Uso SUS
    Aceita variações; se vier 'Valor1..Valor4' faz o mapeamento posicional.
    """
    cols_orig = list(df.columns)
    mapping = {}
    for c in df.columns:
        k = _norm_token(c)
        if k in ("grupo", "categoria"):
            mapping[c] = "Grupo"
        elif k in ("codigo", "código", "cod", "cod.", "codigo equipamento"):
            mapping[c] = "Codigo"
        elif k in ("descricao", "descrição", "equipamento", "nome do equipamento"):
            mapping[c] = "Descricao"
        elif k in ("existentes", "existente", "qtd existentes", "qtd existente"):
            mapping[c] = "Existentes"
        elif k in ("em uso", "uso", "emuso"):
            mapping[c] = "Em Uso"
        elif k in ("existentes sus", "existentes sus ", "sus existentes", "existentes  sus"):
            mapping[c] = "Existentes SUS"
        elif k in ("em uso sus", "emuso sus", "uso sus"):
            mapping[c] = "Em Uso SUS"
        else:
            mapping[c] = c  # pode ser Valor1..Valor4

    out = df.copy()
    out.columns = [mapping[c] for c in df.columns]

    # --- fallback por posição quando vierem Valor1..Valor4 ou rótulos faltando ---
    # Primeiro garanta as colunas-chave
    if "Codigo" not in out.columns and len(out.columns) >= 2:
        # heurística: primeira coluna depois de Grupo deve ser Codigo
        # (muitos scrapers já entregam 'Codigo')
        pass

    # Se métricas não existirem mas temos 4 colunas numéricas genéricas, assume a ordem CNES:
    # Codigo | Descricao | Valor1 | Valor2 | Valor3 | Valor4
    metric_expected = {"Existentes", "Em Uso", "Existentes SUS", "Em Uso SUS"}
    missing_metrics = [m for m in metric_expected if m not in out.columns]

    # Detecta possíveis "Valor1..Valor4"
    valor_cols = [c for c in out.columns if re.fullmatch(r"Valor[1-4]", str(c), flags=re.IGNORECASE)]
    if missing_metrics and valor_cols:
        # Garante ordem estável por índice original
        order = [(cols_orig.index(c), c) for c in valor_cols]
        order.sort()
        only_vals = [name for _, name in order]  # e.g., ['Valor1','Valor2','Valor3','Valor4']

        if len(only_vals) >= 4:
            # mapeamento posicional
            out = out.rename(columns={
                only_vals[0]: "Existentes",
                only_vals[1]: "Em Uso",
                only_vals[2]: "Existentes SUS",
                only_vals[3]: "Em Uso SUS",
            })
        elif len(only_vals) == 3:
            out = out.rename(columns={
                only_vals[0]: "Existentes",
                only_vals[1]: "Em Uso",
                only_vals[2]: "Existentes SUS",
            })

    # Se ainda faltar algo, cria com zero
    if "Grupo" not in out.columns:
        out["Grupo"] = pd.NA
    if "Codigo" not in out.columns:
        raise ValueError(f"Coluna 'Codigo' ausente após normalização | DF: {list(out.columns)}")
    if "Descricao" not in out.columns:
        raise ValueError(f"Coluna 'Descricao' ausente após normalização | DF: {list(out.columns)}")

    for mcol in ("Existentes", "Em Uso", "Existentes SUS", "Em Uso SUS"):
        if mcol not in out.columns:
            out[mcol] = 0

    return out

# --------------- conversão p/ fato ---------------

def df_to_rows_fato(conn, df: pd.DataFrame, vcomp: str, codigo_municipio: str, uf: str, nome_municipio: str):
    """
    1) Normaliza cabeçalho (inclui fallback Valor1..Valor4)
    2) Remove TOTAL
    3) Coage métricas a inteiro
    4) Agrega por (Codigo, Grupo, Descricao)
    5) Resolve FKs e monta linhas para fato_cnes_equipamento
    """
    if df is None or df.empty:
        return []

    df = _fix_headers(df)

    # remove totais de segurança
    df = df[~df.apply(_is_total_row, axis=1)].copy()

    for c in ["Existentes", "Em Uso", "Existentes SUS", "Em Uso SUS"]:
        df[c] = df[c].apply(_to_int).astype(int)

    agg = df.groupby(["Codigo", "Grupo", "Descricao"], dropna=False).agg({
        "Existentes": "sum",
        "Em Uso": "sum",
        "Existentes SUS": "sum",
        "Em Uso SUS": "sum",
    }).reset_index()

    comp_id = get_or_create_competencia(conn, vcomp)
    mun_id  = get_or_create_municipio(conn, codigo_municipio, uf, nome_municipio)

    rows = []
    for _, r in agg.iterrows():
        item_id = get_or_create_item(
            conn, "equipamento",
            codigo=str(r["Codigo"]),
            grupo=(None if pd.isna(r.get("Grupo")) else str(r["Grupo"])),
            descricao=(None if pd.isna(r.get("Descricao")) else str(r["Descricao"]))
        )
        rows.append({
            "competencia_id": comp_id,
            "municipio_id": mun_id,
            "item_id": item_id,
            "existentes": int(r["Existentes"]),
            "em_uso": int(r["Em Uso"]),
            "existentes_sus": int(r["Existentes SUS"]),
            "em_uso_sus": int(r["Em Uso SUS"]),
        })
    return rows

def dedupe_equip_batch(rows):
    """
    Garante unicidade por (competencia_id, municipio_id, item_id) somando métricas.
    """
    merged = {}
    for r in rows:
        k = (r["competencia_id"], r["municipio_id"], r["item_id"])
        if k not in merged:
            merged[k] = dict(r)
        else:
            dst = merged[k]
            for mcol in ("existentes", "em_uso", "existentes_sus", "em_uso_sus"):
                dst[mcol] = int(dst.get(mcol, 0)) + int(r.get(mcol, 0))
    return list(merged.values())

# --------------- main ---------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    cfg = DBConfig()
    municipios = baixar_municipios_ibge()
    competencias = gerar_competencias(VCOMP_INICIO, VCOMP_FIM)

    total = 0
    with get_conn(cfg) as conn:
        for vcomp in competencias:
            if not args.force:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 1
                        FROM fato_cnes_equipamento f
                        JOIN dim_competencia d ON d.competencia_id = f.competencia_id
                        WHERE d.vcomp = %s
                        LIMIT 1
                    """, (vcomp,))
                    if cur.fetchone():
                        print(f"[SKIP] {vcomp}: já existe em fato_cnes_equipamento")
                        continue

            batch = []
            for m in municipios:
                try:
                    df = fetch_equipamentos(m["codigo"], vcomp)
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
                batch = dedupe_equip_batch(batch)
                inserted = upsert_dicts(
                    conn,
                    table="fato_cnes_equipamento",
                    rows=batch,
                    pkey_cols=["competencia_id", "municipio_id", "item_id"],
                    update_cols=["existentes", "em_uso", "existentes_sus", "em_uso_sus"]
                )
                conn.commit()
                total += inserted
                print(f"[OK] {vcomp}: upsert {inserted} (acum={total})")
            else:
                print(f"[SKIP] {vcomp}: sem dados")

    print(f"Concluído. Total upsert: {total}")

if __name__ == "__main__":
    main()