"""
stages/transform/transform_contas_recebidas.py
------------------------------------------------
Transform – Contas Recebidas

Para cada centro de custo ativo:
  1. Lê todos os sintéticos de dados_brutos/
  2. Extrai a tabela de clientes (linha 10 em diante, descarta totais/rodapé)
  3. Acrescenta colunas: pct_repasse e valor_liquido_repasse
  4. Empilha os meses e salva em dados_consolidados/{slug_cc}_consolidado.xlsx

Colunas do arquivo consolidado:
  periodo | cliente | amortizacao | juros | correcao | acrescimo |
  seguro | taxa_adm | desconto | liquido | pct_repasse | valor_liquido_repasse
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

AUXILIAR_PATH = (
        Path(__file__).resolve().parents[2]
        / "stages" / "rpa" / "files" / "reference" / "auxiliar.xlsx"
)
BASE_OUTPUT_DIR = (
        Path(__file__).resolve().parents[2]
        / "stages" / "rpa" / "output" / "contas_recebidas"
)

# Prefixos de linhas de rodapé/totalização a descartar
RODAPE_PREFIXOS = (
    "Total geral",
    "Total da empresa",
    "Total de parcelas",
    "Total de títulos",
    "(*)",
    "(C)",
    "(S)",
    "SIENGE",
)

# Timestamp do Sienge no rodapé: "07/05/2026 - 23:15:59"
_RE_TIMESTAMP = re.compile(r"^\d{2}/\d{2}/\d{4} - \d{2}:\d{2}:\d{2}$")


# ── Leitura do auxiliar ───────────────────────────────────────────────────────

def carregar_centros_ativos() -> list[dict]:
    df = pd.read_excel(AUXILIAR_PATH, sheet_name="centros_custo")
    df = df[df["ativo"].str.strip().str.lower() == "sim"]
    return df.to_dict(orient="records")


def slug_de(nome: str) -> str:
    return nome.strip().lower().replace(" ", "_").replace("-", "")[:40]


# ── Parser do sintético ───────────────────────────────────────────────────────

# Mapeamento posicional das colunas do sintético do Sienge
COLUNAS_SINTETICO = {
    0: "cliente",
    5: "amortizacao",
    7: "juros",
    8: "correcao",
    9: "acrescimo",
    11: "seguro",
    12: "taxa_adm",
    13: "desconto",
    14: "liquido",
}


def extrair_periodo_do_header(df_raw: pd.DataFrame) -> str:
    """
    Extrai o período do cabeçalho do Sienge (linha 6, coluna 2).
    Ex.: '01/09/2025 a 30/09/2025' → '202509'
    """
    try:
        texto = str(df_raw.iloc[6, 2])
        match = re.search(r"(\d{2})/(\d{2})/(\d{4})", texto)
        if match:
            return f"{match.group(3)}{match.group(2)}"  # AAAAMM
    except Exception:
        pass
    return "000000"


def _eh_rodape(valor: str) -> bool:
    """Retorna True se a célula é uma linha de rodapé/totalização do Sienge."""
    v = str(valor).strip()
    if v.startswith(RODAPE_PREFIXOS):
        return True
    if _RE_TIMESTAMP.match(v):
        return True
    return False


def ler_sintetico(caminho: Path) -> pd.DataFrame:
    """
    Lê um arquivo sintético do Sienge e retorna DataFrame limpo
    com colunas padronizadas + coluna 'periodo' (AAAAMM).
    """
    df_raw = pd.read_excel(caminho, sheet_name="Relatório", header=None)
    periodo = extrair_periodo_do_header(df_raw)

    # Dados começam na linha 10 (índice 10)
    dados = df_raw.iloc[10:][list(COLUNAS_SINTETICO.keys())].copy()
    dados.columns = list(COLUNAS_SINTETICO.values())

    # Remove linhas vazias, totais e rodapé
    dados = dados[dados["cliente"].notna()]
    dados = dados[~dados["cliente"].astype(str).str.strip().eq("")]
    dados = dados[~dados["cliente"].astype(str).apply(_eh_rodape)]

    # Converte colunas numéricas
    colunas_num = ["amortizacao", "juros", "correcao", "acrescimo",
                   "seguro", "taxa_adm", "desconto", "liquido"]
    for col in colunas_num:
        dados[col] = pd.to_numeric(dados[col], errors="coerce").fillna(0.0)

    dados.insert(0, "periodo", periodo)
    return dados.reset_index(drop=True)


# ── Transform principal ───────────────────────────────────────────────────────

def transformar_centro(centro: dict) -> None:
    nome_cc = str(centro["centro_custo"]).strip()
    slug_cc = slug_de(nome_cc)
    pct = float(centro.get("pct_repasse") or 0)

    dir_brutos = BASE_OUTPUT_DIR / slug_cc / "dados_brutos"
    dir_consol = BASE_OUTPUT_DIR / slug_cc / "dados_consolidados"
    dir_consol.mkdir(parents=True, exist_ok=True)

    arquivos_sin = sorted(dir_brutos.glob(f"{slug_cc}_*_sintetico.xlsx"))

    if not arquivos_sin:
        logger.warning("[%s] Nenhum sintético encontrado em %s", nome_cc, dir_brutos)
        return

    logger.info("[%s] %d arquivo(s) sintético(s) encontrado(s)", nome_cc, len(arquivos_sin))

    frames = []
    for arq in arquivos_sin:
        try:
            df = ler_sintetico(arq)
            frames.append(df)
            logger.info("  ✓ %s  (%d clientes)", arq.name, len(df))
        except Exception:
            logger.exception("  ✗ Erro ao ler %s — pulando", arq.name)

    if not frames:
        logger.warning("[%s] Nenhum dado válido — abortando transform", nome_cc)
        return

    consolidado = pd.concat(frames, ignore_index=True)

    # Aplica percentual de repasse
    consolidado["pct_repasse"] = pct
    consolidado["valor_liquido_repasse"] = (consolidado["liquido"] * pct).round(2)

    # Ordena por período e cliente
    consolidado = consolidado.sort_values(["periodo", "cliente"]).reset_index(drop=True)

    destino = dir_consol / f"{slug_cc}_consolidado.xlsx"
    _salvar_consolidado(consolidado, destino, nome_cc, pct)
    logger.info("[%s] Consolidado salvo → %s", nome_cc, destino.name)


# ── Geração do Excel consolidado ──────────────────────────────────────────────

def _salvar_consolidado(df: pd.DataFrame, destino: Path, nome_cc: str, pct: float) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Consolidado"

    thin = Side(style="thin", color="BFBFBF")
    borda = Border(left=thin, right=thin, top=thin, bottom=thin)

    # ── Linha 1: título ───────────────────────────────────────────────────────
    ws.merge_cells("A1:L1")
    ws["A1"] = f"Contas Recebidas – {nome_cc}"
    ws["A1"].font = Font(bold=True, size=12, color="FFFFFF")
    ws["A1"].fill = PatternFill("solid", start_color="1F4E79")
    ws["A1"].alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 22

    # ── Linha 2: metadados ────────────────────────────────────────────────────
    ws.merge_cells("A2:L2")
    ws["A2"] = (
        f"% de repasse: {pct:.2%}   |   "
        f"Clientes únicos: {df['cliente'].nunique()}   |   "
        f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"
    )
    ws["A2"].font = Font(italic=True, size=10, color="595959")
    ws["A2"].alignment = Alignment(horizontal="left", vertical="center")

    # ── Linha 3: cabeçalhos das colunas ──────────────────────────────────────
    cabecalhos = [
        "Período", "Cliente", "Amortização", "Juros", "Correção",
        "Acréscimo", "Seguro", "Taxa adm", "Desconto", "Líquido",
        "% Repasse", "Vlr. Líq. Repasse",
    ]
    colunas_df = [
        "periodo", "cliente", "amortizacao", "juros", "correcao",
        "acrescimo", "seguro", "taxa_adm", "desconto", "liquido",
        "pct_repasse", "valor_liquido_repasse",
    ]

    hdr_font = Font(bold=True, color="FFFFFF", size=10)
    hdr_fill = PatternFill("solid", start_color="2E75B6")

    for col_i, cab in enumerate(cabecalhos, 1):
        cell = ws.cell(row=3, column=col_i, value=cab)
        cell.font = hdr_font
        cell.fill = hdr_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = borda

    ws.row_dimensions[3].height = 30

    # ── Linhas de dados ───────────────────────────────────────────────────────
    alt_fill = PatternFill("solid", start_color="DCE6F1")

    for row_i, row in enumerate(df.itertuples(index=False), 4):
        fill = alt_fill if row_i % 2 == 0 else PatternFill()
        for col_i, col_nome in enumerate(colunas_df, 1):
            val = getattr(row, col_nome)
            cell = ws.cell(row=row_i, column=col_i, value=val)
            cell.border = borda
            cell.fill = fill
            cell.font = Font(size=10)

            if col_nome == "periodo":
                cell.alignment = Alignment(horizontal="center")
            elif col_nome == "cliente":
                cell.alignment = Alignment(horizontal="left")
            elif col_nome == "pct_repasse":
                cell.number_format = "0.00%"
                cell.alignment = Alignment(horizontal="center")
            elif col_nome in ("amortizacao", "juros", "correcao", "acrescimo",
                              "seguro", "taxa_adm", "desconto", "liquido",
                              "valor_liquido_repasse"):
                cell.number_format = "#,##0.00"
                cell.alignment = Alignment(horizontal="right")

    # ── Linha de totais ───────────────────────────────────────────────────────
    total_row = len(df) + 4
    total_fill = PatternFill("solid", start_color="1F4E79")
    total_font = Font(bold=True, color="FFFFFF", size=10)

    for col_i in range(1, 13):
        cell = ws.cell(row=total_row, column=col_i)
        cell.fill = total_fill
        cell.font = total_font
        cell.border = borda

    ws.cell(row=total_row, column=1).value = "TOTAL"
    ws.cell(row=total_row, column=1).alignment = Alignment(horizontal="center")

    colunas_soma = {
        3: "amortizacao", 4: "juros", 5: "correcao", 6: "acrescimo",
        7: "seguro", 8: "taxa_adm", 9: "desconto", 10: "liquido",
        12: "valor_liquido_repasse",
    }
    for col_i, col_nome in colunas_soma.items():
        cell = ws.cell(row=total_row, column=col_i, value=df[col_nome].sum())
        cell.number_format = "#,##0.00"
        cell.alignment = Alignment(horizontal="right")

    cell_pct = ws.cell(row=total_row, column=11, value=pct)
    cell_pct.number_format = "0.00%"
    cell_pct.alignment = Alignment(horizontal="center")

    # ── Larguras e freeze ─────────────────────────────────────────────────────
    larguras = [10, 42, 14, 10, 10, 12, 10, 10, 10, 14, 12, 18]
    for i, w in enumerate(larguras, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    ws.freeze_panes = "A4"
    wb.save(destino)


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    centros = carregar_centros_ativos()
    logger.info("%d centros de custo ativos", len(centros))

    for centro in centros:
        try:
            transformar_centro(centro)
        except Exception:
            logger.exception("Erro no transform de '%s'", centro.get("centro_custo"))

    logger.info("Transform concluído.")


if __name__ == "__main__":
    main()
