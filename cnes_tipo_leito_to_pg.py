# cnes_tipo_leito_to_pg.py
import time, pandas as pd
from db_config import DBConfig
import argparse
from db_utils import (
    get_conn, upsert_dicts,
    get_or_create_competencia, get_or_create_municipio, get_or_create_item
)

# ---- importa funções do seu scraper original ----
# (cole as mesmas funções de util/requests/parse do seu arquivo original)
# para brevidade, vou importar via módulo; se preferir, copie as defs.
from scrape_cnes_leito import (
    baixar_municipios_ibge, gerar_competencias,
    fetch_tabela_tipo_leito, VCOMP_INICIO, VCOMP_FIM, SLEEP_ENTRE_REQUISICOES
)


def df_to_rows(df: pd.DataFrame):
    base_cols = ["VComp","UF","Codigo_Municipio","Municipio","Grupo","Codigo","Descricao","Existente"]
    for c in ["SUS","Habilitados"]:
        if c not in df.columns:
            df[c] = pd.NA
    for _, r in df.iterrows():
        yield {
            "vcomp":            str(r["VComp"]),
            "uf":               str(r["UF"]),
            "codigo_municipio": str(r["Codigo_Municipio"]),
            "municipio":        str(r["Municipio"]),
            "grupo":            (None if pd.isna(r["Grupo"]) else str(r["Grupo"])),
            "codigo":           str(r["Codigo"]),
            "descricao":        (None if pd.isna(r["Descricao"]) else str(r["Descricao"])),
            "existente":        (None if pd.isna(r["Existente"]) else int(r["Existente"])),
            "sus":              (None if pd.isna(r["SUS"]) else int(r["SUS"])),
            "habilitados":      (None if ("Habilitados" not in df.columns or pd.isna(r["Habilitados"])) else int(r["Habilitados"]))
        }
def escolher_probe_municipios(municipios: list[dict], n=3) -> list[dict]:
    """
    Escolhe até n municípios para o probe.
    Damos preferência à capital (Boa Vista) se existir, e depois mais alguns.
    """
    if not municipios:
        return []
    capital_idx = next((i for i, m in enumerate(municipios) if "BOA VISTA" in m["nome"].upper()), None)
    escolhidos = []
    if capital_idx is not None:
        escolhidos.append(municipios[capital_idx])
    for m in municipios:
        if m not in escolhidos:
            escolhidos.append(m)
        if len(escolhidos) >= n:
            break
    return escolhidos

def df_to_rows_fato(conn, df: pd.DataFrame, vcomp: str, codigo_municipio: str, uf: str, nome_municipio: str):
    comp_id = get_or_create_competencia(conn, vcomp)
    mun_id  = get_or_create_municipio(conn, codigo_municipio, uf, nome_municipio)

    # garante colunas esperadas
    if "SUS" not in df.columns: df["SUS"] = pd.NA
    if "Habilitados" not in df.columns: df["Habilitados"] = pd.NA

    rows = []
    for _, r in df.iterrows():
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
            "existente": (None if pd.isna(r.get("Existente")) else int(r["Existente"])),
            "sus":       (None if pd.isna(r.get("SUS")) else int(r["SUS"])),
            "habilitados": (None if pd.isna(r.get("Habilitados")) else int(r["Habilitados"]))
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
            # (opcional) pulo por mês se já existir — se quiser manter
            if not args.force:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM fato_cnes_leito f JOIN dim_competencia d ON d.competencia_id=f.competencia_id WHERE d.vcomp=%s LIMIT 1", (vcomp,))
                    if cur.fetchone():
                        print(f"[SKIP] {vcomp}: já existe em fato_cnes_leito")
                        continue

            batch = []
            for m in municipios:
                df = fetch_tabela_tipo_leito(m["codigo"], vcomp)
                time.sleep(SLEEP_ENTRE_REQUISICOES)
                if df is None or df.empty:
                    continue
                batch.extend(df_to_rows_fato(conn, df, vcomp, m["codigo"], "RR", m["nome"]))

            if batch:
                inserted = upsert_dicts(
                    conn,
                    table="fato_cnes_leito",
                    rows=batch,
                    pkey_cols=["competencia_id","municipio_id","item_id"],
                    update_cols=["existente","sus","habilitados"]
                )
                conn.commit()
                total += inserted
                print(f"[OK] {vcomp}: upsert {inserted} (acum={total})")
            else:
                print(f"[SKIP] {vcomp}: sem dados")
    print(f"Concluído. Total upsert: {total}")

if __name__ == "__main__":
    main()