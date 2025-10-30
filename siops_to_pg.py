# siops_to_pg.py
import json
import time
import argparse
from datetime import datetime

import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from db_config import DBConfig
from db_utils import get_conn, upsert_dicts, get_or_create_municipio

# Tenta reaproveitar função de municípios dos scrapers CNES
try:
    from scrape_cnes_rr_tipo_unidade import baixar_municipios_ibge as _baixar_mun
except Exception:
    try:
        from scrape_cnes_rr_equipamentos import baixar_municipios_ibge as _baixar_mun
    except Exception:
        _baixar_mun = None

URL = "http://siops.datasus.gov.br/consleirespfiscal.php"

# ---------------------------------------------------------------------
# util

def setup_driver(headless: bool) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--window-size=1366,768")
    if headless:
        opts.add_argument("--headless=new")
    print(f"[DRV] Inicializando ChromeDriver (headless={headless})...")
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    print("[DRV] ChromeDriver pronto.")
    return drv

def switch_to_results_context(driver, wait) -> bool:
    """
    SIOPS às vezes abre em nova janela/iframe. Ajusta o contexto.
    """
    time.sleep(0.8)
    if len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1])

    titulo = "//*[contains(@class,'lbltitulo') and contains(., 'Lei de Responsabilidade Fiscal')]"
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, titulo)))
        return True
    except Exception:
        pass

    # tenta iframes
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    for fr in iframes:
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(fr)
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, titulo)))
            return True
        except Exception:
            continue
    driver.switch_to.default_content()
    return False

def table_to_matrix(tbl):
    """
    Converte <table> em matriz (respeitando colspan/rowspan).
    """
    rows = tbl.find_elements(By.XPATH, ".//tr")
    matrix, span_down = [], []
    for tr in rows:
        cells = tr.find_elements(By.XPATH, ".//th | .//td")
        if not span_down:
            span_down = [None] * 64
        row, col_idx = [], 0

        def advance(cidx):
            while True:
                if cidx >= len(span_down):
                    span_down.extend([None] * 16)
                if cidx >= len(row):
                    row.extend([""] * (cidx - len(row) + 1))
                if span_down[cidx]:
                    text, left = span_down[cidx]
                    row[cidx] = text
                    left -= 1
                    span_down[cidx] = (text, left) if left > 0 else None
                    cidx += 1
                else:
                    break
            return cidx

        col_idx = advance(col_idx)
        for cell in cells:
            txt = cell.get_attribute("innerText").strip()
            colspan = cell.get_attribute("colspan")
            rowspan = cell.get_attribute("rowspan")
            cspan = int(colspan) if colspan and colspan.isdigit() else 1
            rspan = int(rowspan) if rowspan and rowspan.isdigit() else 1

            if col_idx + cspan > len(row):
                row.extend([""] * (col_idx + cspan - len(row)))
            for k in range(cspan):
                row[col_idx + k] = txt
            if rspan > 1:
                for k in range(cspan):
                    j = col_idx + k
                    if j >= len(span_down):
                        span_down.extend([None] * (j - len(span_down) + 1))
                    span_down[j] = (txt, rspan - 1)
            col_idx += cspan
            col_idx = advance(col_idx)

        while row and row[-1] == "":
            row.pop()
        matrix.append(row)

    width = max((len(r) for r in matrix), default=0)
    for r in matrix:
        if len(r) < width:
            r.extend([""] * (width - len(r)))
    return matrix

def guess_title_from_table(matrix):
    for i in range(min(3, len(matrix))):
        line = " ".join(cell for cell in matrix[i][:3] if cell).strip()
        if line:
            return line[:150]
    return "tabela"

def _catalogo_municipios_rr():
    """
    Dict: NOME_UPPER -> (codigo_ibge, nome_fmt)
    """
    cat = {}
    if _baixar_mun:
        try:
            lista = _baixar_mun()  # [{codigo:'140010', nome:'Boa Vista'}, ...]
            for m in lista:
                nome_up = str(m.get("nome","")).strip().upper()
                cat[nome_up] = (str(m.get("codigo","")).strip(), m.get("nome",""))
        except Exception:
            pass
    return cat

# ---------------------------------------------------------------------
# núcleo

def run_and_store(headless=True, force=False, show=False):
    if show:
        headless = False

    print(f"[INIT] SIOPS: headless={headless} | force={force}")
    driver = setup_driver(headless=headless)
    wait = WebDriverWait(driver, 30)
    rows_total = 0

    # catálogo para resolver municipio_id
    catalogo = _catalogo_municipios_rr()

    try:
        driver.get(URL)
        wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
        Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
        print("[UI] UF selecionada: Roraima")

        # lista de municípios (como aparecem no SIOPS)
        Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbMunicipio[]"))))
        municipios = [
            opt.text.strip()
            for opt in Select(driver.find_element(By.NAME, "cmbMunicipio[]")).options
            if opt.text.strip()
        ]
        print(f"[UI] {len(municipios)} municípios carregados no combo.")

        # anos disponíveis
        anos_opts = [
            opt.text.strip()
            for opt in Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbAno")))).options
        ]
        ano_atual = datetime.now().year
        anos = [a for a in anos_opts if a.isdigit() and 2008 <= int(a) <= ano_atual]
        print(f"[UI] Anos disponíveis: {anos}")

        cfg = DBConfig()
        with get_conn(cfg) as conn:
            for ano in anos:
                # períodos dentro do ano
                select_periodo = Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbPeriodo"))))
                periodos = [opt.text.strip() for opt in select_periodo.options if opt.text.strip()]
                print(f"[LOOP] Ano {ano}: períodos={periodos}")

                for periodo in periodos:
                    print(f"[STEP] Probe {ano}-{periodo} ...")
                    try:
                        # ---------- PROBE: verificar se existe dado no site ----------
                        Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))).select_by_visible_text("Roraima")
                        Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbMunicipio[]")))).select_by_visible_text(municipios[0])
                        Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbAno")))).select_by_visible_text(ano)
                        Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbPeriodo")))).select_by_visible_text(periodo)
                        wait.until(EC.element_to_be_clickable((By.NAME, "BtConsultar"))).click()

                        if not switch_to_results_context(driver, wait):
                            print(f"[WARN] {ano}-{periodo}: sem contexto de resultado; pulando período.")
                            driver.get(URL); wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                            Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                            continue

                        wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.tam2.tdExterno")) > 0)
                        tabelas_probe = driver.find_elements(By.CSS_SELECTOR, "table.tam2.tdExterno")
                        if not tabelas_probe:
                            print(f"[SKIP] {ano}-{periodo}: site sem tabelas — pulando período.")
                            driver.get(URL); wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                            Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                            continue

                        # ---------- SKIP ÚNICO por (ano, periodo) ----------
                        if not force:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "SELECT 1 FROM siops_tabelas WHERE ano=%s AND periodo=%s LIMIT 1",
                                    (int(ano), str(periodo))
                                )
                                if cur.fetchone():
                                    print(f"[SKIP] {ano}-{periodo}: já existe no banco — pulando período.")
                                    driver.get(URL); wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                                    Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                                    continue

                        # ---------- RASPAGEM DE TODOS MUNICÍPIOS ----------
                        driver.get(URL)
                        wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                        Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")

                        for municipio in municipios:
                            try:
                                Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))).select_by_visible_text("Roraima")
                                Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbMunicipio[]")))).select_by_visible_text(municipio)
                                Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbAno")))).select_by_visible_text(ano)
                                Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbPeriodo")))).select_by_visible_text(periodo)
                                wait.until(EC.element_to_be_clickable((By.NAME, "BtConsultar"))).click()

                                if not switch_to_results_context(driver, wait):
                                    print(f"[WARN] {ano}-{periodo}/{municipio}: sem resultados; pulando município.")
                                    driver.get(URL); wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                                    Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                                    continue

                                wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.tam2.tdExterno")) > 0)
                                tabelas = driver.find_elements(By.CSS_SELECTOR, "table.tam2.tdExterno")

                                # resolve municipio_id via catálogo
                                nome_up = municipio.strip().upper()
                                cod_ibge, nome_fmt = catalogo.get(nome_up, ("000000", municipio))
                                mun_id = get_or_create_municipio(conn, cod_ibge, "RR", nome_fmt)

                                batch = []
                                for idx, tbl in enumerate(tabelas, start=1):
                                    matrix = table_to_matrix(tbl)
                                    titulo = guess_title_from_table(matrix)
                                    # ignora agregados UF
                                    tnorm = (titulo or "").strip().lower()
                                    if tnorm.startswith("uf:") or tnorm.startswith("uf_"):
                                        continue
                                    batch.append({
                                        "municipio_id": mun_id,
                                        "ano": int(ano),
                                        "periodo": str(periodo),
                                        "tabela_idx": idx,
                                        "titulo": titulo,
                                        "matrix": json.dumps(matrix, ensure_ascii=False)
                                    })

                                if batch:
                                    rows_total += upsert_dicts(
                                        conn,
                                        table="siops_tabelas",
                                        rows=batch,
                                        pkey_cols=["municipio_id","ano","periodo","tabela_idx"],
                                        update_cols=["titulo","matrix"]
                                    )
                                    conn.commit()
                                    print(f"[DB] {ano}-{periodo}/{municipio}: +{len(batch)} tabela(s).")

                                # volta para próxima iteração
                                driver.get(URL)
                                wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                                Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                            except Exception as e:
                                print(f"[WARN] {ano}-{periodo}/{municipio}: erro ({e}); continuando...")
                                driver.get(URL)
                                wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                                Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                                continue

                    except Exception as e:
                        print(f"[WARN] {ano}-{periodo}: falha no probe ({e}); pulando período.")
                        driver.get(URL)
                        wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                        Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                        continue

    finally:
        driver.quit()

    print(f"✅ SIOPS concluído. Total de linhas upsert: {rows_total}")

# ---------------------------------------------------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="Reprocessa anos/períodos já existentes no banco")
    ap.add_argument("--show", action="store_true", help="Mostra o navegador (sem headless)")
    args = ap.parse_args()
    run_and_store(headless=not args.show, force=args.force, show=args.show)
