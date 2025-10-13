from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from decimal import Decimal, InvalidOperation
from datetime import datetime
import csv, time, re, unicodedata, os

URL = "http://siops.datasus.gov.br/consleirespfiscal_uf.php?S=1&UF=14;&Ano=2025&Periodo=18"

# ---------- util ----------
def parse_brl_number(txt: str):
    if txt is None:
        return None
    s = txt.strip()
    if s.upper() in {"N/A", "NA"} or s in {"-", ""}:
        return None
    s = s.replace('.', '').replace(',', '.').replace('%', '')
    try:
        return Decimal(s)
    except InvalidOperation:
        return None

def slugify(text: str, maxlen: int = 60):
    if not text:
        return "tabela"
    t = unicodedata.normalize("NFKD", text)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = re.sub(r"[^a-zA-Z0-9]+", "_", t).strip("_").lower()
    if len(t) > maxlen:
        t = t[:maxlen].rstrip("_")
    if not t:
        t = "tabela"
    return t

def setup_driver(headless=False):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--window-size=1366,768")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def switch_to_results_context(driver, wait):
    time.sleep(1.0)
    if len(driver.window_handles) > 1:
        driver.switch_to.window(driver.window_handles[-1])
    titulo_xpath = "//*[contains(@class,'lbltitulo') and contains(., 'Lei de Responsabilidade Fiscal')]"
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, titulo_xpath)))
        return True
    except Exception:
        pass
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

# ---------- captura genérica de tabela ----------
def table_to_matrix(tbl):
    rows = tbl.find_elements(By.XPATH, ".//tr")
    matrix = []
    span_down = []
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

def save_matrix_csv(path, matrix):
    if not matrix:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(matrix)

# ---------- fluxo principal ----------
def main():
    driver = setup_driver(headless=False)
    wait = WebDriverWait(driver, 30)
    ano_atual = datetime.now().year

    try:
        driver.get(URL)
        wait.until(EC.presence_of_element_located((By.NAME, "cmbAno")))

        select_ano = Select(driver.find_element(By.NAME, "cmbAno"))
        todos_anos = [opt.text.strip() for opt in select_ano.options if opt.text.strip().isdigit()]
        anos = [a for a in todos_anos if 2008 <= int(a) <= ano_atual]
        print(f"📅 Anos filtrados: {anos}")

        for ano in anos:
            try:
                # seleciona o ano
                select_ano = Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbAno"))))
                select_ano.select_by_visible_text(ano)

                # obtém todos os períodos disponíveis para o ano
                select_periodo = Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbPeriodo"))))
                periodos = [opt.text.strip() for opt in select_periodo.options if opt.text.strip()]
                print(f"\n=== Ano {ano}: {len(periodos)} períodos encontrados: {periodos}")

                for periodo in periodos:
                    print(f"\n→ Processando Ano {ano}, Período {periodo}...")
                    try:
                        # precisa redefinir os selects a cada iteração
                        select_ano = Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbAno"))))
                        select_ano.select_by_visible_text(ano)
                        select_periodo = Select(wait.until(EC.presence_of_element_located((By.NAME, "cmbPeriodo"))))
                        select_periodo.select_by_visible_text(periodo)

                        btn = wait.until(EC.element_to_be_clickable((By.NAME, "BtConsultar")))
                        btn.click()

                        ok_ctx = switch_to_results_context(driver, wait)
                        if not ok_ctx:
                            print(f"⚠️ Não consegui confirmar o contexto de resultado ({ano}-{periodo})")
                            driver.get(URL)
                            continue

                        wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.tam2.tdExterno")) > 0)
                        tabelas = driver.find_elements(By.CSS_SELECTOR, "table.tam2.tdExterno")
                        print(f"🔎 {len(tabelas)} tabelas encontradas ({ano}-{periodo})")

                        out_dir = os.path.join("siops_csv", ano, f"periodo_{periodo}")
                        os.makedirs(out_dir, exist_ok=True)

                        for idx, tbl in enumerate(tabelas, start=1):
                            matrix = table_to_matrix(tbl)
                            titulo = guess_title_from_table(matrix)
                            slug = slugify(titulo)

                            # ignora a tabela "UF: ..."
                            titulo_norm = (titulo or "").strip().lower()
                            if slug.startswith("uf_") or titulo_norm.startswith("uf:"):
                                print(f"↷ Ignorando Tabela {idx:02d} ({titulo})")
                                continue

                            fname = f"siops_table{idx:02d}_{slug}.csv"
                            path = os.path.join(out_dir, fname)
                            save_matrix_csv(path, matrix)
                            nlin, ncol = len(matrix), max((len(r) for r in matrix), default=0)
                            print(f"✓ Tabela {idx:02d}: '{titulo}' ({nlin}x{ncol}) → {path}")

                        driver.get(URL)
                        wait.until(EC.presence_of_element_located((By.NAME, "cmbAno")))

                    except Exception as e:
                        print(f"❌ Erro ao processar Ano {ano}, Período {periodo}: {e}")
                        driver.get(URL)
                        wait.until(EC.presence_of_element_located((By.NAME, "cmbAno")))
                        continue

            except Exception as e:
                print(f"❌ Erro ao processar Ano {ano}: {e}")
                driver.get(URL)
                wait.until(EC.presence_of_element_located((By.NAME, "cmbAno")))
                continue

        print("\n✅ Finalizado. CSVs gerados em: siops_csv/")
        input("Pressione ENTER para sair…")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()