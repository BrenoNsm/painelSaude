# cnes_tipo_unidade_to_pg.py
import time, re, argparse
import pandas as pd
from db_config import DBConfig
from db_utils import (
    get_conn, upsert_dicts,
    get_or_create_competencia, get_or_create_municipio, get_or_create_item
)

# ========= integrações com seu scraper =========
try:
    import scrape_cnes_rr_tipo_unidade as mod_tu  # seu arquivo existente
except Exception:
    mod_tu = None

def _resolve_fn(module, candidates):
    for name in candidates:
        if module is not None and hasattr(module, name):
            return getattr(module, name)
    return None

baixar_municipios_ibge   = _resolve_fn(mod_tu, ["baixar_municipios_ibge", "listar_municipios_rr", "get_municipios_rr"])
gerar_competencias       = _resolve_fn(mod_tu, ["gerar_competencias", "listar_competencias"])
fetch_tipo_unidade       = _resolve_fn(mod_tu, ["fetch_tipo_unidade", "fetch_unidade", "fetch_tipos_unidade", "baixar_tipo_unidade"])
VCOMP_INICIO             = getattr(mod_tu, "VCOMP_INICIO", "201201")
VCOMP_FIM                = getattr(mod_tu, "VCOMP_FIM", "203012")
SLEEP_ENTRE_REQUISICOES  = getattr(mod_tu, "SLEEP_ENTRE_REQUISICOES", 0.6)

if not baixar_municipios_ibge or not gerar_competencias or not fetch_tipo_unidade:
    raise SystemExit(
        "Ajuste os nomes: não encontrei funções esperadas no scrape_cnes_rr_tipo_unidade.py "
        "(preciso de baixar_municipios_ibge, gerar_competencias e uma fetch_* para tipo_unidade)."
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
    joined = " ".join(str(x) for x in row.values[:2]).upper()
    return "TOTAL" in joined

def _fix_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza cabeçalhos para: Codigo | Descricao | (opcional Grupo) | Total
    Aceita variações, acentos e <br>.
    """
    def norm_token(s: str) -> str:
        s = str(s or "")
        s = (s.replace("<br>", " ").replace("<br/>", " ").replace("<br />", " ")
               .replace("\n", " ").replace("\r", " "))
        s = re.sub(r"\s+", " ", s).strip().lower()
        # normalização leve de acentos
        s = (s.replace("ç", "c").replace("á", "a").replace("ã", "a")
               .replace("â", "a").replace("é", "e").replace("ê", "e")
               .replace("í", "i").replace("ó", "o").replace("ô", "o")
               .replace("ú", "u").replace("ü", "u"))
        s = s.replace("_", " ").replace("-", " ")
        return s

    mapping = {}
    for c in df.columns:
        k = norm_token(c)
        if k in ("codigo", "código", "cod", "cod.", "codigo tipo unidade", "codigo tipo de unidade"):
            mapping[c] = "Codigo"
        elif k in ("descricao", "descrição", "tipo de unidade", "unidade", "nome do tipo de unidade"):
            mapping[c] = "Descricao"
        elif k in ("grupo", "categoria", "subgrupo"):
            mapping[c] = "Grupo"
        elif k in ("total", "qtde total", "quantidade total", "qtd total"):
            mapping[c] = "Total"
        else:
            mapping[c] = c  # mantém, mas será ignorado

    out = df.copy()
    out.columns = [mapping[c] for c in df.columns]
    return out

# ========= conversão p/ fato =========

def df_to_rows_fato(conn, df: pd.DataFrame, vcomp: str, codigo_municipio: str, uf: str, nome_municipio: str):
    """
    1) Corrige cabeçalho
    2) Garante colunas Codigo/Descricao/(Grupo) e Total
    3) Converte Total para int
    4) Agrega por Codigo/Grupo/Descricao somando Total
    5) Resolve FKs e monta linhas para fato_cnes_tipo_unidade
    """
    if df is None or df.empty:
        return []

    df = _fix_headers(df)

    # chaves mínimas
    if "Codigo" not in df.columns or "Descricao" not in df.columns:
        raise ValueError(f"Esperava colunas Codigo e Descricao (colunas={list(df.columns)})")

    if "Grupo" not in df.columns:
        df["Grupo"] = pd.NA

    if "Total" not in df.columns:
        # às vezes a coluna vem como única métrica sem nome limpo: procure heurísticas comuns
        raise ValueError(f"Coluna 'Total' não encontrada após normalização (colunas={list(df.columns)})")

    # remove linhas TOTAL (se aparecer)
    df = df[~df.apply(_is_total_row, axis=1)]

    # coage Total
    df["Total"] = df["Total"].apply(_to_int).astype(int)

    # agrega
    agg = df.groupby(["Codigo", "Grupo", "Descricao"], dropna=False).agg({
        "Total": "sum"
    }).reset_index()

    # FKs
    comp_id = get_or_create_competencia(conn, vcomp)
    mun_id  = get_or_create_municipio(conn, codigo_municipio, uf, nome_municipio)

    rows = []
    for _, r in agg.iterrows():
        item_id = get_or_create_item(
            conn, "tipo_unidade",
            codigo=str(r["Codigo"]),
            grupo=(None if pd.isna(r.get("Grupo")) else str(r["Grupo"])),
            descricao=(None if pd.isna(r.get("Descricao")) else str(r["Descricao"]))
        )
        rows.append({
            "competencia_id": comp_id,
            "municipio_id": mun_id,
            "item_id": item_id,
            "total": int(r["Total"]),
        })
    return rows

def dedupe_batch(rows):
    """
    Unicidade por (competencia_id, municipio_id, item_id).
    Se houver duplicatas, soma 'total'.
    """
    merged = {}
    for r in rows:
        k = (r["competencia_id"], r["municipio_id"], r["item_id"])
        if k not in merged:
            merged[k] = dict(r)
        else:
            merged[k]["total"] = int(merged[k].get("total", 0)) + int(r.get("total", 0))
    return list(merged.values())

# ========= main =========

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    cfg = DBConfig()
    municipios   = baixar_municipios_ibge()
    competencias = gerar_competencias(VCOMP_INICIO, VCOMP_FIM)

    total_upserts = 0
    with get_conn(cfg) as conn:
        for vcomp in competencias:
            # Skip rápido por competência (se já existir algo nessa comp)
            if not args.force:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 1
                        FROM fato_cnes_tipo_unidade f
                        JOIN dim_competencia d ON d.competencia_id = f.competencia_id
                        WHERE d.vcomp = %s
                        LIMIT 1
                    """, (vcomp,))
                    if cur.fetchone():
                        print(f"[SKIP] {vcomp}: já existe em fato_cnes_tipo_unidade")
                        continue

            batch = []
            for m in municipios:
                try:
                    df = fetch_tipo_unidade(m["codigo"], vcomp)
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
                batch = dedupe_batch(batch)
                inserted = upsert_dicts(
                    conn,
                    table="fato_cnes_tipo_unidade",
                    rows=batch,
                    pkey_cols=["competencia_id", "municipio_id", "item_id"],
                    update_cols=["total"]
                )
                conn.commit()
                total_upserts += inserted
                print(f"[OK] {vcomp}: upsert {inserted} (acum={total_upserts})")
            else:
                print(f"[SKIP] {vcomp}: sem dados")

    print(f"Concluído. Total upsert: {total_upserts}")

if __name__ == "__main__":
    main()
