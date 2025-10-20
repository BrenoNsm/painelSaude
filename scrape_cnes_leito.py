import re, time, unicodedata
from io import StringIO
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------- Config --------------------
UF_CODE = 14  # Roraima
# defina claramente o intervalo:
VCOMP_INICIO = (2012, 1)   # jan/2012  -> ajuste conforme sua fonte
VCOMP_FIM    = None        # None = até o mês corrente; ou ex.: (2025, 6)

SAIDA_POR_MES = False
SAIDA_ARQUIVO = "cnes_rr_tipo_leito_201202_202508.csv"

SLEEP_ENTRE_REQUISICOES = 0.8
MAX_RETRIES = 3
TIMEOUT = 30

CNES_URL = "https://cnes2.datasus.gov.br/Mod_Ind_Tipo_Leito.asp"
IBGE_MUN_URL = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{UF_CODE}/municipios"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CNES-scraper)"}

# -------------------- Utils ---------------------
def _strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', str(s)) if not unicodedata.combining(c))

def _norm(s: str) -> str:
    s = _strip_accents(str(s)).lower().strip()
    return re.sub(r"\s+", " ", s)

def _to_int(x):
    s = re.sub(r"[^\d\-]", "", str(x or "")).strip()
    return pd.NA if s == "" else int(s)

def gerar_competencias(vcomp_inicio=None, vcomp_fim=None):
    """
    Gera lista de competencias AAAAMM (str) mês a mês, inclusive.
    vcomp_inicio/fim podem ser tupla (ano, mes) ou None.
    """
    if vcomp_inicio is None:
        vcomp_inicio = VCOMP_INICIO
    if vcomp_fim is None:
        if VCOMP_FIM is None:
            hoje = date.today()
            vcomp_fim = (hoje.year, hoje.month)
        else:
            vcomp_fim = VCOMP_FIM

    yi, mi = vcomp_inicio
    yf, mf = vcomp_fim

    comps = []
    y, m = yi, mi
    while (y < yf) or (y == yf and m <= mf):
        comps.append(f"{y:04d}{m:02d}")
        # incrementa mês
        m += 1
        if m > 12:
            m = 1
            y += 1
    return comps

def baixar_municipios_ibge():
    r = requests.get(IBGE_MUN_URL, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    # CNES usa 6 dígitos -> remove o dígito verificador do IBGE (7 dígitos)
    out = [{"codigo": str(it["id"])[:-1], "nome": it["nome"]} for it in data]
    out.sort(key=lambda x: int(x["codigo"]))
    return out

# ---------------- Parser linha-a-linha ----------------
GRUPOS = {"CIRÚRGICO","CLÍNICO","OBSTÉTRICO","PEDIATRICO","PEDIÁTRICO","OUTRAS ESPECIALIDADES","COMPLEMENTAR"}

def parse_tabela_tipo_leito(html: str) -> pd.DataFrame | None:
    soup = BeautifulSoup(html, "lxml")
    # pegue a TABELA com mais linhas de dados (heurística robusta)
    best_t = None
    best_score = -1
    for t in soup.find_all("table"):
        score = 0
        txt = _norm(t.get_text(" ", strip=True))
        if "codigo" in txt and "descricao" in txt and ("sus" in txt or "habilitados" in txt):
            score += 10
        # conta linhas com 4 tds e 1º td numérico
        c = 0
        for tr in t.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 4:
                first = tds[0].get_text(strip=True)
                if re.fullmatch(r"\d{1,3}", first):
                    c += 1
        score += c
        if score > best_score:
            best_score = score
            best_t = t
    if best_t is None:
        return None

    registros = []
    grupo_atual = None
    quarta_coluna = "SUS"  # por padrão; muda quando encontrarmos cabeçalho

    for tr in best_t.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        # 1) detectar GRUPO (linha com 1 td e texto em GRUPOS)
        if len(tds) == 1 or (len(tds) == 2 and tds[0].has_attr("colspan")):
            maybe = tds[0].get_text(strip=True)
            if _norm(maybe).upper().replace("PEDIATRICO","PEDIÁTRICO") in {g.upper() for g in GRUPOS}:
                grupo_atual = maybe.strip()
            continue

        # 2) detectar CABEÇALHO (Codigo / Descrição / Existente / Sus|Habilitados)
        texts = [td.get_text(" ", strip=True) for td in tds]
        norm  = [_norm(x) for x in texts]
        if len(tds) >= 4 and norm[0] == "codigo" and norm[1].startswith("descricao") and "existente" in norm[2]:
            if "habilitados" in norm[3]:
                quarta_coluna = "Habilitados"
            else:
                quarta_coluna = "SUS"
            continue

        # 3) linha de DADOS: 4 colunas, 1ª numérica
        if len(tds) >= 4 and re.fullmatch(r"\d{1,3}", texts[0]):
            codigo = texts[0]
            descricao = texts[1]
            existente = _to_int(texts[2])
            col4 = _to_int(texts[3])

            rec = {
                "Grupo": grupo_atual,
                "Codigo": codigo,
                "Descricao": descricao,
                "Existente": existente,
                "SUS": pd.NA,
                "Habilitados": pd.NA
            }
            if quarta_coluna == "SUS":
                rec["SUS"] = col4
            else:
                rec["Habilitados"] = col4

            registros.append(rec)
            continue

        # 4) ignora linhas TOTAL / Sumário etc.
        if any(x for x in norm if x.startswith("total") or "sumario" in x):
            continue

    if not registros:
        return None

    df = pd.DataFrame(registros)
    # remove colunas totalmente vazias
    if df["SUS"].isna().all():
        df = df.drop(columns=["SUS"])
    if "Habilitados" in df.columns and df["Habilitados"].isna().all():
        df = df.drop(columns=["Habilitados"])
    return df.reset_index(drop=True)

# ------------------ Scraper principal ------------------
def fetch_tabela_tipo_leito(vmun6: str, vcomp: str) -> pd.DataFrame | None:
    params = {"VEstado": UF_CODE, "VMun": vmun6, "VComp": vcomp}
    last_ex = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(CNES_URL, params=params, headers=HEADERS, timeout=TIMEOUT, verify=False)
            r.raise_for_status()
            r.encoding = "latin-1"  # a página é ISO-8859-1
            df = parse_tabela_tipo_leito(r.text)
            return df
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
            df = fetch_tabela_tipo_leito(m["codigo"], vcomp)
            time.sleep(SLEEP_ENTRE_REQUISICOES)
            if df is None or df.empty:
                continue
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
            path = f"cnes_rr_tipo_leito_{vcomp}.csv"
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
