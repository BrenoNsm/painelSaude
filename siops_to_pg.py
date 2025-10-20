# siops_to_pg.py
import os, json
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from db_config import DBConfig
import argparse
from db_utils import get_conn, upsert_dicts, get_or_create_municipio

# Tentamos reaproveitar a função de municípios dos scrapers CNES
try:
    from scrape_cnes_rr_tipo_unidade import baixar_municipios_ibge
except Exception:
    try:
        from scrape_cnes_rr_equipamentos import baixar_municipios_ibge
    except Exception:
        baixar_municipios_ibge = None

URL = "http://siops.datasus.gov.br/consleirespfiscal.php"


# ==============================
# Setup do webdriver (versão anterior + logs)
# ==============================
def setup_driver(headless=True):
    opts = webdriver.ChromeOptions()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--window-size=1366,768")
    if headless:
        opts.add_argument("--headless=new")

    print("[INFO] Iniciando ChromeDriver...")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    print("[INFO] ChromeDriver inicializado com sucesso!")
    return driver


# ==============================
# Helpers de scraping
# ==============================
def switch_to_results_context(driver, wait):
    import time
    time.sleep(1.0)
    # Nova janela?
    if len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1])

    titulo_xpath = "//*[contains(@class,'lbltitulo') and contains(., 'Lei de Responsabilidade Fiscal')]"
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, titulo_xpath)))
        return True
    except Exception:
        pass

    # Ou dentro de iframe?
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    for fr in iframes:
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(fr)
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, titulo_xpath)))
            return True
        except Exception:
            continue

    driver.switch_to.default_content()
    return False


def table_to_matrix(tbl):
    rows = tbl.find_elements(By.XPATH, ".//tr")
    matrix, span_down = [], []
    for tr in rows:
        cells = tr.find_elements(By.XPATH, ".//th | .//td")
        if not span_down:
            span_down = [None] * 50
        row, col_idx = [], 0

        def advance_to_next_free(cidx):
            while True:
                if cidx >= len(span_down):
                    span_down.extend([None] * 10)
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

        col_idx = advance_to_next_free(col_idx)
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
            col_idx = advance_to_next_free(col_idx)
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
            return line[:120]
    return "tabela"


def _build_catalogo_municipios():
    """
    Retorna dict {NOME_UPPER: (CODIGO_IBGE, NOME_FORMATADO)} para Roraima.
    Usa a função dos scrapers CNES; se indisponível, retorna dict vazio e usamos fallback.
    """
    if baixar_municipios_ibge is None:
        print("[WARN] baixar_municipios_ibge não disponível; mapearei municípios apenas pelo nome (código '000000').")
        return {}
    try:
        lista = baixar_municipios_ibge()  # [{codigo: '140010', nome: 'Boa Vista'}, ...]
        cat = {}
        for m in lista:
            nome_up = str(m.get("nome", "")).strip().upper()
            codigo = str(m.get("codigo", "")).strip()
            if nome_up:
                cat[nome_up] = (codigo, m.get("nome", ""))
        print(f"[INFO] Catálogo de municípios carregado ({len(cat)} entradas).")
        return cat
    except Exception as e:
        print(f"[WARN] Falha ao carregar catálogo de municípios: {e}")
        return {}


# ==============================
# Main
# ==============================
def run_and_store(headless=True):
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Reprocessa mesmo que o período já exista no banco")
    parser.add_argument("--show", action="store_true", help="Mostra o navegador (desliga headless)")
    parser.add_argument("--timeout", type=int, default=90, help="Timeout de carregamento de página (s)")
    args, _ = parser.parse_known_args()

    if args.show:
        headless = False

    print(f"[INFO] Iniciando scraping do SIOPS (headless={headless})...")
    driver = setup_driver(headless=headless)
    driver.set_page_load_timeout(args.timeout)
    wait = WebDriverWait(driver, 30)
    rows_total = 0

    print(f"[INFO] Iniciando SIOPS | headless={headless} timeout={args.timeout}s")

    # catálogo nome→(codigo_ibge, nome_fmt)
    catalogo = _build_catalogo_municipios()

    try:
        print(f"[INFO] Acessando página: {URL}")
        driver.get(URL)
        wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
        print("[INFO] Página carregada. Selecionando UF=Roraima...")
        Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")

        print("[INFO] Lendo lista de municípios...")
        Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbMunicipio[]"))))
        municipios = [opt.text.strip()
                      for opt in Select(driver.find_element(By.NAME, "cmbMunicipio[]")).options
                      if opt.text.strip()]
        print(f"[INFO] Municípios encontrados: {len(municipios)}")

        print("[INFO] Lendo anos disponíveis...")
        anos_opts = [opt.text.strip()
                     for opt in Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbAno")))).options]
        from datetime import datetime
        ano_atual = datetime.now().year
        anos = [a for a in anos_opts if a.isdigit() and 2008 <= int(a) <= ano_atual]
        print(f"[INFO] Anos filtrados: {anos}")

        cfg = DBConfig()
        with get_conn(cfg) as conn:
            for ano in anos:
                # períodos do ano
                select_periodo = Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbPeriodo"))))
                periodos = [opt.text.strip() for opt in select_periodo.options if opt.text.strip()]
                print(f"[INFO] Ano {ano}: períodos = {periodos}")

                for periodo in periodos:
                    try:
                        if not municipios:
                            print("[WARN] Lista de municípios vazia; pulando período.")
                            continue
                        m0 = municipios[0]

                        # ============== PROBE: UMA consulta (m0, ano, periodo) ==========
                        print(f"[INFO] Probe: município='{m0}', ano={ano}, período='{periodo}'")
                        Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))).select_by_visible_text("Roraima")
                        Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbMunicipio[]")))).select_by_visible_text(m0)
                        Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbAno")))).select_by_visible_text(ano)
                        Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbPeriodo")))).select_by_visible_text(periodo)
                        wait.until(EC.element_to_be_clickable((By.NAME, "BtConsultar"))).click()

                        if not switch_to_results_context(driver, wait):
                            print("[WARN] Não consegui alternar para o contexto de resultados (probe).")
                            driver.get(URL); wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                            continue

                        wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.tam2.tdExterno")) > 0)
                        tabelas_probe = driver.find_elements(By.CSS_SELECTOR, "table.tam2.tdExterno")
                        print(f"[INFO] Probe: {len(tabelas_probe)} tabelas detectadas.")

                        # se não veio tabela, pula período
                        if not tabelas_probe:
                            print(f"[SKIP] {ano}-{periodo}: site não retornou tabelas no probe; pulando período.")
                            driver.get(URL); wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                            Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                            continue

                        # ============== SKIP ÚNICO por (ano, periodo) ====================
                        if (not args.force):
                            with conn.cursor() as cur:
                                cur.execute(
                                    "SELECT 1 FROM siops_tabelas WHERE ano=%s AND periodo=%s LIMIT 1",
                                    (int(ano), str(periodo))
                                )
                                if cur.fetchone():
                                    print(f"[SKIP] {ano}-{periodo}: já existe no banco (pulo de uma vez).")
                                    driver.get(URL); wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                                    Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                                    continue

                        # ============== RASPAGEM DE TODOS MUNICÍPIOS =====================
                        print(f"[INFO] Raspando todos os municípios para {ano}-{periodo}...")
                        driver.get(URL)
                        wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                        Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")

                        for municipio in municipios:
                            try:
                                print(f"[INFO] Consultando município='{municipio}' ano={ano} período='{periodo}'")
                                Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))).select_by_visible_text("Roraima")
                                Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbMunicipio[]")))).select_by_visible_text(municipio)
                                Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbAno")))).select_by_visible_text(ano)
                                Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbPeriodo")))).select_by_visible_text(periodo)
                                wait.until(EC.element_to_be_clickable((By.NAME, "BtConsultar"))).click()

                                if not switch_to_results_context(driver, wait):
                                    print("[WARN] Não consegui alternar para resultados; voltando à página inicial.")
                                    driver.get(URL); wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                                    continue

                                wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.tam2.tdExterno")) > 0)
                                tabelas = driver.find_elements(By.CSS_SELECTOR, "table.tam2.tdExterno")
                                print(f"[INFO] Município='{municipio}': {len(tabelas)} tabelas.")

                                # resolve municipio_id via catálogo (nome -> código IBGE) e UPSERT em dim_municipio
                                nome_up = municipio.strip().upper()
                                cod_ibge, nome_fmt = catalogo.get(nome_up, (None, municipio))
                                codigo_final = cod_ibge if cod_ibge else "000000"  # fallback se não achou
                                mun_id = get_or_create_municipio(conn, codigo_final, "RR", nome_fmt)

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
                                    print(f"[DB] Inserindo {len(batch)} tabelas para {municipio}/{ano}/{periodo} ...")
                                    rows_total += upsert_dicts(
                                        conn,
                                        table="siops_tabelas",
                                        rows=batch,
                                        pkey_cols=["municipio_id","ano","periodo","tabela_idx"],
                                        update_cols=["titulo","matrix"]
                                    )
                                    conn.commit()
                                    print(f"[DB] OK. Total acumulado: {rows_total}")

                                # volta para próxima iteração
                                driver.get(URL)
                                wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                                Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")

                            except Exception as e:
                                print(f"[WARN] Erro ao processar município='{municipio}': {e}. Reiniciando tela.")
                                driver.get(URL)
                                wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                                Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                                continue

                    except Exception as e:
                        print(f"[WARN] Erro no período {ano}-{periodo}: {e}. Reiniciando período.")
                        driver.get(URL)
                        wait.until(EC.presence_of_element_located((By.NAME, "cmbUF")))
                        Select(driver.find_element(By.NAME, "cmbUF")).select_by_visible_text("Roraima")
                        continue
    finally:
        driver.quit()

    print(f"✅ SIOPS: upsert total {rows_total}")


if __name__ == "__main__":
    run_and_store(headless=True)
