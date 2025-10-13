# scrape_cnes_rr_equipamentos.py
import re, time, unicodedata
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------- Config --------------------
UF_CODE = 14  # Roraima
VCOMP_INICIO = "201202"  # competência inicial (YYYYMM)
VCOMP_FIM    = "202508"  # competência final (YYYYMM)

SAIDA_POR_MES = False
SAIDA_ARQUIVO = "cnes_rr_equipamentos_201202_202508.csv"

SLEEP_ENTRE_REQUISICOES = 0.8
MAX_RETRIES = 3
TIMEOUT = 30

CNES_URL = "https://cnes2.datasus.gov.br/Mod_Ind_Equipamento.asp"
IBGE_MUN_URL = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{UF_CODE}/municipios"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CNES-scraper)"}

# -------------------- Utils ---------------------
def _strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', str(s)) if not unicodedata.combining(c))

def _norm(s: str) -> str:
    s = _strip_accents(str(s)).lower().strip()
    return re.sub(r"\s+", " ", s)

def _pretty(s: str) -> str:
    s = re.sub(r"\s+", " ", _strip_accents(str(s)).strip())
    return s.title().replace("Sus", "SUS")

def _to_int(x):
    s = re.sub(r"[^\d\-]", "", str(x or "")).strip()
    return pd.NA if s == "" else int(s)

def gerar_competencias(inicio_yyyymm: str, fim_yyyymm: str):
    y0, m0 = int(inicio_yyyymm[:4]), int(inicio_yyyymm[4:])
    y1, m1 = int(fim_yyyymm[:4]), int(fim_yyyymm[4:])
    cur = datetime(y0, m0, 1); end = datetime(y1, m1, 1)
    while cur <= end:
        yield f"{cur.year}{cur.month:02d}"
        cur += relativedelta(months=1)

def baixar_municipios_ibge():
    r = requests.get(IBGE_MUN_URL, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    # CNES usa 6 dígitos (remove o dígito verificador do IBGE)
    out = [{"codigo": str(it["id"])[:-1], "nome": it["nome"]} for it in data]
    out.sort(key=lambda x: int(x["codigo"]))
    return out

# -------- parser robusto ao <tbody> e cabeçalhos dinâmicos --------
def parse_equipamentos(html: str) -> pd.DataFrame | None:
    soup = BeautifulSoup(html, "lxml")

    # Escolhe a tabela mais promissora (com "codigo/descricao" e muitas linhas com 1º TD numérico)
    best_t, best_score = None, -1
    for t in soup.find_all("table"):
        txt = _norm(t.get_text(" ", strip=True))
        score = 0
        if "codigo" in txt and "descricao" in txt:
            score += 10
        c = 0
        for tr in t.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 3 and re.fullmatch(r"\d{1,4}", tds[0].get_text(strip=True)):
                c += 1
        score += c
        if score > best_score:
            best_score, best_t = score, t

    if best_t is None:
        return None

    registros = []
    grupo_atual = None
    metric_headers = []  # nomes das colunas numéricas após "Descrição"

    for tr in best_t.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        texts = [td.get_text(" ", strip=True) for td in tds]
        norms = [_norm(x) for x in texts]

        # 1) detectar GRUPO/SEÇÃO (linha com 1 td, texto não numérico)
        if (len(tds) == 1 or (len(tds) == 2 and tds[0].has_attr("colspan"))) and not re.search(r"\d", texts[0]):
            if not norms[0].startswith(("total", "sumario", "sumário")):
                grupo_atual = texts[0].strip()
            continue

        # 2) detectar CABEÇALHO (Codigo / Descrição / <métricas...>)
        if len(tds) >= 3 and norms[0] == "codigo" and norms[1].startswith("descricao"):
            metric_headers = [_pretty(x) for x in texts[2:]]  # p.ex.: ["Quantidade","SUS"] ou ["Total","Habilitados"]
            continue

        # 3) LINHA DE DADOS: 1º TD é numérico (código do equipamento)
        if len(tds) >= 3 and re.fullmatch(r"\d{1,4}", texts[0]):
            codigo = texts[0]
            # descrição pode vir com <a> dentro — pega apenas texto
            descricao = texts[1]
            nums = [_to_int(x) for x in texts[2:]]

            rec = {"Grupo": grupo_atual, "Codigo": codigo, "Descricao": descricao}

            if metric_headers:
                # aplica nomes da linha de cabeçalho
                for i, h in enumerate(metric_headers):
                    rec[h] = nums[i] if i < len(nums) else pd.NA
            else:
                # fallback sem cabeçalho explícito
                for i, v in enumerate(nums, start=1):
                    rec[f"Valor{i}"] = v

            registros.append(rec)
            continue

        # 4) ignorar totalizações / sumário
        if any(x.startswith("total") or "sumario" in x for x in norms):
            continue

    if not registros:
        return None

    df = pd.DataFrame(registros)
    # padroniza tipos numéricos
    for c in df.columns:
        if c not in {"Grupo", "Codigo", "Descricao"}:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df.reset_index(drop=True)

# ------------------ Scraper principal ------------------
def fetch_equipamentos(vmun6: str, vcomp: str) -> pd.DataFrame | None:
    params = {"VEstado": UF_CODE, "VMun": vmun6, "VComp": vcomp}
    last_ex = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(CNES_URL, params=params, headers=HEADERS, timeout=TIMEOUT, verify=False)
            r.raise_for_status()
            r.encoding = "latin-1"  # página em ISO-8859-1
            return parse_equipamentos(r.text)
        except Exception as e:
            last_ex = e
            time.sleep(1.2 * attempt)
    print(f"[ERRO] VMun={vmun6} VComp={vcomp} -> {last_ex}")
    return None

# ------------------------ Runner ------------------------
def main():
    municipios = baixar_municipios_ibge()  # [{'codigo': '140002', 'nome': 'Amajari'}, ...]
    comps = list(gerar_competencias(VCOMP_INICIO, VCOMP_FIM))

    linhas_total = 0
    first_write = False

    for vcomp in comps:
        registros_mes = []
        for m in municipios:
            df = fetch_equipamentos(m["codigo"], vcomp)
            time.sleep(SLEEP_ENTRE_REQUISICOES)
            if df is None or df.empty:
                continue
            # metadados
            df.insert(0, "VComp", vcomp)
            df.insert(1, "UF", "RR")
            df.insert(2, "Codigo_Municipio", m["codigo"])
            df.insert(3, "Municipio", m["nome"])
            registros_mes.append(df)

        if not registros_mes:
            print(f"[AVISO] Sem dados para {vcomp}")
            continue

        df_mes = pd.concat(registros_mes, ignore_index=True)
        if SAIDA_POR_MES:
            path = f"cnes_rr_equipamentos_{vcomp}.csv"
            df_mes.to_csv(path, index=False, encoding="utf-8")
            print(f"[OK] {vcomp}: {len(df_mes)} linhas -> {path}")
        else:
            df_mes.to_csv(SAIDA_ARQUIVO, mode="a", header=not first_write, index=False, encoding="utf-8")
            first_write = True
            linhas_total += len(df_mes)
            print(f"[OK] {vcomp}: +{len(df_mes)} linhas (acumulado em {SAIDA_ARQUIVO})")

    print(f"\nConcluído. Total de linhas: {linhas_total}")

if __name__ == "__main__":
    main()
