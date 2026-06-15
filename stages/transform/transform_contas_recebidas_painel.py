from __future__ import annotations

import logging
import re

from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

AUXILIAR_PATH = (
        Path(__file__).resolve().parents[2]
        / "stages" / "extract" / "reference" / "auxiliar_contas_recebidas_painel.xlsx"
)

BASE_INPUT_DIR = (
        Path(__file__).resolve().parents[2]
        / "stages" / "transform" / "input"
)

BASE_OUTPUT_DIR = (
        Path(__file__).resolve().parents[2]
        / "stages" / "transform" / "output"
)

# ── Regex / constantes ────────────────────────────────────────────────────────

_RE_TS = re.compile(r"^\d{2}/\d{2}/\d{4} - \d{2}:\d{2}:\d{2}$")
_RE_COD = re.compile(r"\s*\(\d+\)\s*$")
RODAPE = ("Total geral", "Total da empresa", "Total de parcelas",
          "Total de títulos", "(*)", "(C)", "(S)", "SIENGE")

# ── Mapeamentos de colunas ────────────────────────────────────────────────────

COLUNAS_ANALITICO_JUROS = {
    0: "dt_baixa", 1: "cliente", 4: "documento", 7: "titulo",
    8: "parcela", 9: "tc", 10: "unidade_principal", 12: "portador",
    13: "operacao", 14: "data_vencimento", 15: "amortizacao", 16: "juros",
    18: "correcao", 19: "acrescimo", 20: "seguro", 21: "taxa_adm",
    22: "desconto", 23: "liquido",
}

COLUNAS_NUM_JUROS = ["amortizacao", "juros", "correcao", "acrescimo",
                     "seguro", "taxa_adm", "desconto", "liquido"]


# ═════════════════════════════════════════════════════════════════════════════
# HELPERS GERAIS
# ═════════════════════════════════════════════════════════════════════════════

def _slug(nome: str) -> str:
    return nome.strip().lower().replace(" ", "_").replace("-", "")[:40]


def _eh_rodape(v) -> bool:
    s = str(v).strip()
    return s.startswith(RODAPE) or bool(_RE_TS.match(s))


# ═════════════════════════════════════════════════════════════════════════════
# LEITURA DO AUXILIAR E EXTRATO
# ═════════════════════════════════════════════════════════════════════════════

def carregar_centros_ativos() -> list[dict]:
    df = pd.read_excel(AUXILIAR_PATH, sheet_name="centros_custo")
    df = df[df["ativo"].str.strip().str.lower() == "sim"]
    return df.to_dict(orient="records")


# ═════════════════════════════════════════════════════════════════════════════
# LEITURA DOS BRUTOS
# ═════════════════════════════════════════════════════════════════════════════

def ler_analitico(caminho: Path) -> pd.DataFrame:
    cols_ana, colunas_num = COLUNAS_ANALITICO_JUROS, COLUNAS_NUM_JUROS

    df_raw = pd.read_excel(caminho, sheet_name="Relatório", header=None)

    dados = df_raw.iloc[10:][list(cols_ana.keys())].copy()
    dados.columns = list(cols_ana.values())
    dados = dados[dados["cliente"].notna()]
    dados = dados[~dados["cliente"].astype(str).str.strip().eq("")]
    dados = dados[~dados["cliente"].astype(str).str.startswith("Total do cliente")]
    dados = dados[~dados["cliente"].astype(str).apply(_eh_rodape)]

    for col in colunas_num:
        if col in ["amortizacao", "vl_baixa"]:
            dados[col] = (dados[col].astype(str)
                          .str.replace(" P", "", regex=False).str.strip())
            mask = dados[col].str.contains(",", na=False)
            dados.loc[mask, col] = (dados.loc[mask, col]
                                    .str.replace(".", "", regex=False)
                                    .str.replace(",", ".", regex=False))
        dados[col] = pd.to_numeric(dados[col], errors="coerce").fillna(0.0)

    dados["dt_baixa"] = pd.to_datetime(dados["dt_baixa"], format="%d/%m/%Y", errors="coerce")

    return dados.reset_index(drop=True)


# ═════════════════════════════════════════════════════════════════════════════
# ORQUESTRADOR PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def transformar_centro(centro: dict) -> pd.DataFrame:
    nome_cc = str(centro["centro_custo"]).strip()
    slug_cc = _slug(nome_cc)

    dir_brutos = BASE_INPUT_DIR / "contas_recebidas" / slug_cc / "painel"

    # ── 1. Lê todos os analíticos brutos ──────────────────────────────────────
    arqs_ana = sorted(dir_brutos.glob(f"{slug_cc}_*_analitico.xlsx"))

    frames_ana = []
    for arq in arqs_ana:
        try:
            frames_ana.append(ler_analitico(arq))
            logger.info("  ✓ %s", arq.name)
        except Exception:

            logger.exception("  ✗ %s — pulando", arq.name)

    df_ana = pd.concat(frames_ana, ignore_index=True)

    df_ana = df_ana.sort_values(["cliente", "dt_baixa"]).reset_index(drop=True)

    return df_ana


# ═════════════════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    centros = carregar_centros_ativos()
    logger.info("%d centros de custo ativos", len(centros))

    resultados = []

    for centro in centros:
        nome_cc = str(centro["centro_custo"]).strip()
        try:

            res = transformar_centro(centro)
            resultados.append(res)
        except Exception:
            logger.exception("Erro em '%s'", nome_cc)

    df = pd.concat(resultados, ignore_index=True)

    df["data_carga"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    destino = BASE_OUTPUT_DIR / "fato_contas_recebidas_painel.csv"
    df.to_csv(destino, sep=";", decimal=",", index=False)

    logger.info("Transform concluído.")


if __name__ == "__main__":
    main()
