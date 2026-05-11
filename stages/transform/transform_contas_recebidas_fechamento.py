"""
stages/transform/transform_contas_recebidas.py
═══════════════════════════════════════════════
Transform unificado – Contas Recebidas

Uso
───
  python transform_contas_recebidas.py            → consolidar + fechamento + painel
  python transform_contas_recebidas.py --so-consolidar  → só lê brutos e salva consolidado
  python transform_contas_recebidas.py --so-painel      → só atualiza aba Painel

Saída por centro de custo
─────────────────────────
  dados_consolidados/
    {slug}_consolidado_sintetico_{AAAAMMDD_HHMM}.xlsx   ← 2 abas: Consolidado | Fechamento
    {slug}_consolidado_analitico_{AAAAMMDD_HHMM}.xlsx   ← 2 abas: Analítico   | Fechamento

  Cada execução gera arquivos novos com timestamp — histórico anterior preservado.

Aba Consolidado / Analítico
  Série histórica completa com % repasse e flag novo_cliente.
  Analítico tem subtotais por cliente.

Aba Fechamento  (filtrada automaticamente para o mês anterior ao vigente)
  Seção 1 → todos os clientes do mês
  Seção 2 → novos clientes (CRI mês ant.) com série histórica completa
  Resumo para WhatsApp impresso no terminal.

Consolidado geral (raiz de contas_recebidas/)
  consolidado_geral_analitico_{AAAAMMDD_HHMM}.xlsx
  Concatenação de todos os analíticos — dado granular para Power BI.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Tuple, Any

import pandas as pd
from dateutil.relativedelta import relativedelta
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

AUXILIAR_PATH = (
        Path(__file__).resolve().parents[2]
        / "stages" / "extract" / "reference" / "auxiliar_contas_recebidas.xlsx"
)

BASE_INPUT_DIR = (
        Path(__file__).resolve().parents[2]
        / "stages" / "transform" / "input" / "contas_recebidas"
)

BASE_OUTPUT_DIR = (
        Path(__file__).resolve().parents[2]
        / "stages" / "transform" / "output" / "contas_recebidas"
)

EXTRATO_PATH = (
        Path(__file__).resolve().parents[2]
        / "stages" / "extract" / "reference" / "extrato_empreendimentos.xls" # isso é retirado da caixa e todos os meses deve ser solicitado
)

# ── Estilos ───────────────────────────────────────────────────────────────────

_THIN = Side(style="thin", color="BFBFBF")
_MED = Side(style="medium", color="1F4E79")
_BORDA = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)
_BORDA_MED = Border(left=_MED, right=_MED, top=_MED, bottom=_MED)
_HDR_FONT = Font(bold=True, color="FFFFFF", size=10)
_HDR_FILL = PatternFill("solid", start_color="2E75B6")
_TITLE_FILL = PatternFill("solid", start_color="1F4E79")
_ALT_FILL = PatternFill("solid", start_color="DCE6F1")
_TOTAL_FILL = PatternFill("solid", start_color="1F4E79")
_SUBTOT_FILL = PatternFill("solid", start_color="BDD7EE")
_NOVO_FILL = PatternFill("solid", start_color="FFF2CC")
_NOVO_TOT = PatternFill("solid", start_color="FFD966")
_SECT_FILL = PatternFill("solid", start_color="E2EFDA")

# ── Regex / constantes ────────────────────────────────────────────────────────

_RE_TS = re.compile(r"^\d{2}/\d{2}/\d{4} - \d{2}:\d{2}:\d{2}$")
_RE_COD = re.compile(r"\s*\(\d+\)\s*$")
RODAPE = ("Total geral", "Total da empresa", "Total de parcelas",
          "Total de títulos", "(*)", "(C)", "(S)", "SIENGE")

# ── Mapeamentos de colunas ────────────────────────────────────────────────────

COLUNAS_SINTETICO_JUROS = {
    0: "cliente", 5: "amortizacao", 7: "juros", 8: "correcao",
    9: "acrescimo", 11: "seguro", 12: "taxa_adm", 13: "desconto", 14: "liquido",
}
COLUNAS_SINTETICO_PADRAO = {
    0: "cliente", 5: "vl_baixa", 7: "acrescimo", 8: "seguro",
    10: "taxa_adm", 11: "desconto", 12: "liquido",
}
COLUNAS_ANALITICO_JUROS = {
    0: "dt_baixa", 1: "cliente", 4: "documento", 7: "titulo",
    8: "parcela", 9: "tc", 10: "unidade_principal", 12: "portador",
    13: "operacao", 14: "data_vencimento", 15: "amortizacao", 16: "juros",
    18: "correcao", 19: "acrescimo", 20: "seguro", 21: "taxa_adm",
    22: "desconto", 23: "liquido",
}
COLUNAS_ANALITICO_PADRAO = {
    0: "dt_baixa", 1: "cliente", 4: "dt_emissao", 5: "documento",
    7: "titulo", 8: "parcela", 9: "tc", 10: "unidade_principal",
    11: "portador", 12: "operacao", 13: "data_vencimento", 14: "vl_baixa",
    15: "acrescimo", 16: "seguro", 17: "taxa_adm", 18: "desconto", 19: "liquido",
}
COLUNAS_NUM_JUROS = ["amortizacao", "juros", "correcao", "acrescimo",
                     "seguro", "taxa_adm", "desconto", "liquido"]
COLUNAS_NUM_PADRAO = ["vl_baixa", "acrescimo", "seguro",
                      "taxa_adm", "desconto", "liquido"]
LAYOUT = {
    "juros embutidos": (COLUNAS_SINTETICO_JUROS, COLUNAS_ANALITICO_JUROS, COLUNAS_NUM_JUROS),
    "padrão": (COLUNAS_SINTETICO_PADRAO, COLUNAS_ANALITICO_PADRAO, COLUNAS_NUM_PADRAO),
}


# ═════════════════════════════════════════════════════════════════════════════
# HELPERS GERAIS
# ═════════════════════════════════════════════════════════════════════════════

def _slug(nome: str) -> str:
    return nome.strip().lower().replace(" ", "_").replace("-", "")[:40]


def _nome_sienge(c: str) -> str:
    return _RE_COD.sub("", str(c)).strip().upper()


def _eh_rodape(v) -> bool:
    s = str(v).strip()
    return s.startswith(RODAPE) or bool(_RE_TS.match(s))


def _mes_anterior() -> tuple[date, date, str]:
    hoje = date.today()
    ini = (hoje.replace(day=1) - timedelta(days=1)).replace(day=1)
    fim = ini + relativedelta(months=1) - timedelta(days=1)
    return ini, fim, ini.strftime("%m/%Y")


def _layout(centro: dict) -> tuple[dict, dict, list[str]]:
    tipo = str(centro.get("tipo_coluna", "")).strip().lower()
    return LAYOUT.get(tipo, LAYOUT["juros embutidos"])


def _inicio_dados(centro: dict) -> int:
    return 10 if str(centro.get("com_centro", "sim")).strip().lower() == "sim" else 9


def _col(letra: str, ws) -> int:
    return ord(letra.upper()) - 64


# ── Estilos de célula ─────────────────────────────────────────────────────────

def _fmt_valor(cell):
    cell.number_format = "#,##0.00"
    cell.alignment = Alignment(horizontal="right")


def _fmt_pct(cell):
    cell.number_format = "0.00%"
    cell.alignment = Alignment(horizontal="center")


def _fmt_data(cell):
    cell.number_format = "dd/mm/yyyy"
    cell.alignment = Alignment(horizontal="center")


def _aplicar_celula(cell, col: str, colunas_valor: set, colunas_data: set,
                    e_novo: bool = False):
    cell.border = _BORDA
    cell.font = Font(size=10)
    if col in colunas_data:
        _fmt_data(cell)
    elif col == "pct_repasse":
        _fmt_pct(cell)
    elif col in colunas_valor:
        _fmt_valor(cell)
    elif col == "cliente":
        cell.alignment = Alignment(horizontal="left")
    elif col == "novo_cliente":
        cell.alignment = Alignment(horizontal="center")
        if e_novo:
            cell.font = Font(size=10, bold=True, color="7F6000")
    else:
        cell.alignment = Alignment(horizontal="center")


def _titulo_ws(ws, texto: str, n_cols: int, row: int = 1) -> None:
    end = get_column_letter(n_cols)
    ws.merge_cells(f"A{row}:{end}{row}")
    c = ws.cell(row=row, column=1, value=texto)
    c.font = Font(bold=True, size=12, color="FFFFFF")
    c.fill = _TITLE_FILL
    c.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[row].height = 22


def _meta_ws(ws, texto: str, n_cols: int, row: int = 2) -> None:
    end = get_column_letter(n_cols)
    ws.merge_cells(f"A{row}:{end}{row}")
    c = ws.cell(row=row, column=1, value=texto)
    c.font = Font(italic=True, size=10, color="595959")
    c.alignment = Alignment(horizontal="left", vertical="center")


def _cabecalhos_ws(ws, cabecalhos: list[str], row: int = 3) -> None:
    for ci, cab in enumerate(cabecalhos, 1):
        c = ws.cell(row=row, column=ci, value=cab)
        c.font = _HDR_FONT
        c.fill = _HDR_FILL
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border = _BORDA
    ws.row_dimensions[row].height = 30


def _secao_ws(ws, texto: str, n_cols: int, row: int,
              fill=None, cor: str = "1F4E79") -> None:
    fill = fill or _SECT_FILL
    end = get_column_letter(n_cols)
    ws.merge_cells(f"A{row}:{end}{row}")
    c = ws.cell(row=row, column=1, value=texto)
    c.font = Font(bold=True, size=11, color=cor)
    c.fill = fill
    c.alignment = Alignment(horizontal="left", vertical="center")
    c.border = _BORDA
    ws.row_dimensions[row].height = 18


def _linha_total(ws, ri: int, n_cols: int, label: str,
                 df: pd.DataFrame, colunas_df: list[str],
                 colunas_valor: set, pct: float,
                 fill=None, cor_fonte: str = "FFFFFF") -> None:
    fill = fill or _TOTAL_FILL
    tf = Font(bold=True, color=cor_fonte, size=10)
    for ci in range(1, n_cols + 1):
        c = ws.cell(row=ri, column=ci)
        c.fill = fill;
        c.font = tf;
        c.border = _BORDA_MED
    ws.cell(row=ri, column=1, value=label).alignment = Alignment(horizontal="center")
    for ci, col in enumerate(colunas_df, 1):
        if col in colunas_valor and col in df.columns:
            c = ws.cell(row=ri, column=ci, value=df[col].sum())
            c.number_format = "#,##0.00"
            c.alignment = Alignment(horizontal="right")
        elif col == "pct_repasse":
            c = ws.cell(row=ri, column=ci, value=pct)
            c.number_format = "0.00%"
            c.alignment = Alignment(horizontal="center")


# ═════════════════════════════════════════════════════════════════════════════
# LEITURA DO AUXILIAR E EXTRATO
# ═════════════════════════════════════════════════════════════════════════════

def carregar_centros_ativos() -> list[dict]:
    df = pd.read_excel(AUXILIAR_PATH, sheet_name="centros_custo")
    # df = df[df["ativo"].str.strip().str.lower() == "sim"]
    return df.to_dict(orient="records")


def carregar_extrato(nome_empreendimento: str) -> set[str]:
    if not EXTRATO_PATH.exists():
        logger.warning("Extrato não encontrado → novo_cliente sempre 'N'")
        return set()
    df = pd.read_excel(EXTRATO_PATH, sheet_name="UNIDADES", header=1)
    df.columns = ["cnpj", "empreendimento", "contrato",
                  "nome_mutuario", "cpf", "dt_assinatura", "dt_cri"]
    df["dt_cri"] = pd.to_datetime(df["dt_cri"], errors="coerce")
    df["nome_up"] = df["nome_mutuario"].astype(str).str.strip().str.upper()
    chave = re.escape(nome_empreendimento.upper()[:15])
    df = df[df["empreendimento"].astype(str).str.upper().str.contains(chave, regex=True)]
    ini, fim, label = _mes_anterior()
    mask = (df["dt_cri"].dt.date >= ini) & (df["dt_cri"].dt.date <= fim)
    novos = set(df.loc[mask, "nome_up"].tolist())
    logger.info("Extrato [%s]: %d novo(s) em %s", nome_empreendimento, len(novos), label)
    return novos


# ═════════════════════════════════════════════════════════════════════════════
# LEITURA DOS BRUTOS
# ═════════════════════════════════════════════════════════════════════════════

def _extrair_periodo(df_raw: pd.DataFrame) -> str:
    try:
        m = re.search(r"(\d{2})/(\d{2})/(\d{4})", str(df_raw.iloc[6, 2]))
        if m:
            return f"{m.group(3)}{m.group(2)}"
    except Exception:
        pass
    return "000000"


def ler_sintetico(caminho: Path, novos: set[str], centro: dict) -> pd.DataFrame:
    cols_sin, _, colunas_num = _layout(centro)
    df_raw = pd.read_excel(caminho, sheet_name="Relatório", header=None)
    periodo = _extrair_periodo(df_raw)
    inicio = _inicio_dados(centro)

    dados = df_raw.iloc[inicio:][list(cols_sin.keys())].copy()
    dados.columns = list(cols_sin.values())
    dados = dados[dados["cliente"].notna()]
    dados = dados[~dados["cliente"].astype(str).str.strip().eq("")]
    dados = dados[~dados["cliente"].astype(str).apply(_eh_rodape)]

    for col in colunas_num:
        dados[col] = pd.to_numeric(dados[col], errors="coerce").fillna(0.0)
    if "liquido" not in dados.columns and "vl_baixa" in dados.columns:
        dados["liquido"] = dados["vl_baixa"]

    dados.insert(0, "periodo", periodo)
    dados["novo_cliente"] = dados["cliente"].apply(
        lambda c: "S" if _nome_sienge(c) in novos else "N"
    )
    return dados.reset_index(drop=True)


def ler_analitico(caminho: Path, novos: set[str], centro: dict) -> pd.DataFrame:
    _, cols_ana, colunas_num = _layout(centro)
    df_raw = pd.read_excel(caminho, sheet_name="Relatório", header=None)
    periodo = _extrair_periodo(df_raw)
    inicio = _inicio_dados(centro)

    dados = df_raw.iloc[inicio:][list(cols_ana.keys())].copy()
    dados.columns = list(cols_ana.values())
    dados = dados[dados["cliente"].notna()]
    dados = dados[~dados["cliente"].astype(str).str.strip().eq("")]
    dados = dados[~dados["cliente"].astype(str).str.startswith("Total do cliente")]
    dados = dados[~dados["cliente"].astype(str).apply(_eh_rodape)]

    for col in colunas_num:
        if col == "amortizacao":
            dados[col] = (dados[col].astype(str)
                          .str.replace(" P", "", regex=False).str.strip())
            mask = dados[col].str.contains(",", na=False)
            dados.loc[mask, col] = (dados.loc[mask, col]
                                    .str.replace(".", "", regex=False)
                                    .str.replace(",", ".", regex=False))
        dados[col] = pd.to_numeric(dados[col], errors="coerce").fillna(0.0)
    if "liquido" not in dados.columns and "vl_baixa" in dados.columns:
        dados["liquido"] = dados["vl_baixa"]

    dados["dt_baixa"] = pd.to_datetime(dados["dt_baixa"], format="%d/%m/%Y", errors="coerce")
    dados.insert(0, "periodo", periodo)
    dados["novo_cliente"] = dados["cliente"].apply(
        lambda c: "S" if _nome_sienge(c) in novos else "N"
    )
    return dados.reset_index(drop=True)


# ═════════════════════════════════════════════════════════════════════════════
# CONSOLIDAR
# ═════════════════════════════════════════════════════════════════════════════

def _colunas_excel_sintetico(centro: dict) -> tuple[list[str], list[str], set]:
    _, _, colunas_num = _layout(centro)
    possui_juros = "juros" in colunas_num
    if possui_juros:
        cols_num_exib = ["amortizacao", "juros", "correcao",
                         "acrescimo", "seguro", "taxa_adm", "desconto", "liquido"]
        cabs_num = ["Amortização", "Juros", "Correção",
                    "Acréscimo", "Seguro", "Taxa adm", "Desconto", "Líquido"]
    else:
        cols_num_exib = ["vl_baixa", "acrescimo", "seguro",
                         "taxa_adm", "desconto", "liquido"]
        cabs_num = ["Vl. Baixa", "Acréscimo", "Seguro",
                    "Taxa adm", "Desconto", "Líquido"]
    cabecalhos = ["Cliente", "Novo?"] + cabs_num + ["% Repasse", "Vlr. Líq. Repasse"]
    colunas_df = ["cliente", "novo_cliente"] + cols_num_exib + ["pct_repasse", "valor_liquido_repasse"]
    return cabecalhos, colunas_df, set(cols_num_exib + ["valor_liquido_repasse"])


def _colunas_excel_analitico(centro: dict) -> tuple[list[str], list[str], set]:
    _, _, colunas_num = _layout(centro)
    possui_juros = "juros" in colunas_num
    cols_fixas = ["dt_baixa", "cliente", "novo_cliente", "documento", "titulo",
                  "parcela", "tc", "unidade_principal", "portador",
                  "operacao", "data_vencimento"]
    cabs_fixas = ["Dt. Baixa", "Cliente", "Novo?", "Documento", "Título",
                  "Parcela", "TC", "Unidade", "Portador", "Operação", "Vencimento"]
    if possui_juros:
        cols_num_exib = ["amortizacao", "juros", "correcao",
                         "acrescimo", "seguro", "taxa_adm", "desconto", "liquido"]
        cabs_num = ["Amortização", "Juros", "Correção",
                    "Acréscimo", "Seguro", "Taxa Adm", "Desconto", "Líquido"]
    else:
        cols_num_exib = ["vl_baixa", "acrescimo", "seguro",
                         "taxa_adm", "desconto", "liquido"]
        cabs_num = ["Vl. Baixa", "Acréscimo", "Seguro",
                    "Taxa Adm", "Desconto", "Líquido"]
    cabecalhos = cabs_fixas + cabs_num + ["% Repasse", "Vlr. Líq. Repasse"]
    colunas_df = cols_fixas + cols_num_exib + ["pct_repasse", "valor_liquido_repasse"]
    return cabecalhos, colunas_df, set(cols_num_exib + ["valor_liquido_repasse"])


def _salvar_sintetico(df: pd.DataFrame, ws, nome_cc: str, pct: float, centro: dict) -> None:
    cabecalhos, colunas_df, colunas_valor = _colunas_excel_sintetico(centro)
    n = len(cabecalhos)

    _titulo_ws(ws, f"Contas Recebidas – {nome_cc}", n, 1)
    _meta_ws(ws, (
        f"% de repasse: {pct:.2%}   |   "
        f"Clientes únicos: {df['cliente'].nunique()}   |   "
        f"Novos (CRI mês ant.): {(df['novo_cliente'] == 'S').sum()}   |   "
        f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"
    ), n, 2)
    _cabecalhos_ws(ws, cabecalhos, 3)

    for ri, row in enumerate(df.itertuples(index=False), 4):
        e_novo = getattr(row, "novo_cliente") == "S"
        fill = _NOVO_FILL if e_novo else (_ALT_FILL if ri % 2 == 0 else PatternFill())
        for ci, col in enumerate(colunas_df, 1):
            val = getattr(row, col)
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.fill = fill
            _aplicar_celula(cell, col, colunas_valor, set(), e_novo)

    tr = len(df) + 4
    _linha_total(ws, tr, n, "TOTAL", df, colunas_df, colunas_valor, pct)

    larguras = {"cliente": 42, "novo_cliente": 8, "pct_repasse": 12,
                "valor_liquido_repasse": 18}
    _, _, colunas_num = _layout(centro)
    for c in colunas_num:
        larguras[c] = 14
    for i, col in enumerate(colunas_df, 1):
        ws.column_dimensions[get_column_letter(i)].width = larguras.get(col, 12)
    ws.freeze_panes = "A4"


def _salvar_analitico(df: pd.DataFrame, ws, nome_cc: str, pct: float, centro: dict) -> None:
    cabecalhos, colunas_df, colunas_valor = _colunas_excel_analitico(centro)
    colunas_data = {"dt_baixa", "data_vencimento"}
    n = len(cabecalhos)

    _titulo_ws(ws, f"Contas Recebidas Analítico – {nome_cc}", n, 1)
    _meta_ws(ws, (
        f"% de repasse: {pct:.2%}   |   "
        f"Registros: {len(df)}   |   "
        f"Clientes únicos: {df['cliente'].nunique()}   |   "
        f"Novos (CRI mês ant.): {(df['novo_cliente'] == 'S').sum()}   |   "
        f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"
    ), n, 2)
    _cabecalhos_ws(ws, cabecalhos, 3)

    ri = 4
    for cliente, grupo in df.groupby("cliente", sort=True):
        e_novo = (grupo["novo_cliente"] == "S").any()
        for row in grupo.itertuples(index=False):
            fill = _NOVO_FILL if e_novo else (_ALT_FILL if ri % 2 == 0 else PatternFill())
            for ci, col in enumerate(colunas_df, 1):
                val = getattr(row, col)
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.fill = fill
                _aplicar_celula(cell, col, colunas_valor, colunas_data, e_novo)
            ri += 1

        # subtotal
        sf = Font(bold=True, size=10, color="7F6000" if e_novo else "1F4E79")
        for ci in range(1, n + 1):
            c = ws.cell(row=ri, column=ci)
            c.fill = _SUBTOT_FILL;
            c.font = sf;
            c.border = _BORDA
        ws.cell(row=ri, column=2, value=f"Subtotal – {cliente}").alignment = \
            Alignment(horizontal="left")
        ws.cell(row=ri, column=3, value="S" if e_novo else "N").alignment = \
            Alignment(horizontal="center")
        for ci, col in enumerate(colunas_df, 1):
            if col in colunas_valor and col in grupo.columns:
                c = ws.cell(row=ri, column=ci, value=grupo[col].sum())
                c.number_format = "#,##0.00";
                c.alignment = Alignment(horizontal="right")
            elif col == "pct_repasse":
                c = ws.cell(row=ri, column=ci, value=pct)
                c.number_format = "0.00%";
                c.alignment = Alignment(horizontal="center")
        ri += 1

    _linha_total(ws, ri, n, "TOTAL GERAL", df, colunas_df, colunas_valor, pct)

    larguras = {"dt_baixa": 12, "cliente": 40, "novo_cliente": 7,
                "documento": 16, "titulo": 14, "parcela": 10, "tc": 8,
                "unidade_principal": 14, "portador": 12, "operacao": 12,
                "data_vencimento": 12, "pct_repasse": 12, "valor_liquido_repasse": 18}
    _, _, colunas_num = _layout(centro)
    for c in colunas_num:
        larguras[c] = 14
    for i, col in enumerate(colunas_df, 1):
        ws.column_dimensions[get_column_letter(i)].width = larguras.get(col, 12)
    ws.freeze_panes = "A4"


# ═════════════════════════════════════════════════════════════════════════════
# FECHAMENTO
# ═════════════════════════════════════════════════════════════════════════════

def _adicionar_fechamento_sintetico(wb: Workbook, df: pd.DataFrame,
                                    nome_cc: str, pct: float, centro: dict) -> None:
    _, mes_fim, mes_label = _mes_anterior()

    if "Fechamento" in wb.sheetnames:
        del wb["Fechamento"]
    ws = wb.create_sheet("Fechamento")

    cabecalhos, colunas_df, colunas_valor = _colunas_excel_sintetico(centro)
    n = len(cabecalhos)

    ri = 1
    _titulo_ws(ws, f"Fechamento {mes_label}  –  {nome_cc}", n, ri);
    ri += 1
    _secao_ws(ws, f"▶  REPASSE DO MÊS ANTERIOR ({mes_label})  –  Todos os Clientes", n, ri);
    ri += 1
    _cabecalhos_ws(ws, cabecalhos, ri);
    ri += 1

    for idx, row in enumerate(df.itertuples(index=False)):
        e_novo = getattr(row, "novo_cliente") == "S"
        fill = _NOVO_FILL if e_novo else (_ALT_FILL if idx % 2 == 0 else PatternFill())
        for ci, col in enumerate(colunas_df, 1):
            val = getattr(row, col)
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.fill = fill
            _aplicar_celula(cell, col, colunas_valor, set(), e_novo)
        ri += 1

    _linha_total(ws, ri, n, "TOTAL GERAL", df, colunas_df, colunas_valor, pct)

    larguras = {"cliente": 42, "novo_cliente": 8, "pct_repasse": 12,
                "valor_liquido_repasse": 18}
    _, _, colunas_num = _layout(centro)
    for c in colunas_num:
        larguras[c] = 14
    for i, col in enumerate(colunas_df, 1):
        ws.column_dimensions[get_column_letter(i)].width = larguras.get(col, 12)
    ws.freeze_panes = "A4"


def _adicionar_fechamento_analitico(wb: Workbook, df: pd.DataFrame,
                                    nome_cc: str, pct: float, centro: dict) -> tuple[Any, Any] | tuple[float, float]:
    ini, fim, mes_label = _mes_anterior()

    if "Fechamento" in wb.sheetnames:
        del wb["Fechamento"]
    ws = wb.create_sheet("Fechamento")

    cabecalhos, colunas_df, colunas_valor = _colunas_excel_analitico(centro)
    colunas_data = {"dt_baixa", "data_vencimento"}
    n = len(cabecalhos)

    df_mes = df[df["dt_baixa"].dt.date.between(ini, fim)].copy()
    novos_nm = set(df[df["novo_cliente"] == "S"]["cliente"].unique())

    # Para novos clientes, exibe série histórica completa na tabela principal.
    # Para demais, exibe apenas os registros do mês anterior.
    df_exibir = pd.concat([
        df[df["cliente"].isin(novos_nm)],  # novos: série toda
        df_mes[~df_mes["cliente"].isin(novos_nm)],  # demais: só mês ant.
    ], ignore_index=True).sort_values(["cliente", "dt_baixa"])

    ri = 1
    _titulo_ws(ws, f"Fechamento {mes_label}  –  {nome_cc}", n, ri);
    ri += 1
    _secao_ws(ws, f"▶  REPASSE DO MÊS ANTERIOR ({mes_label})  –  Todos os Clientes", n, ri);
    ri += 1
    _cabecalhos_ws(ws, cabecalhos, ri)
    ri += 1

    if len(df_exibir):
        for cliente, grupo in df_exibir.groupby("cliente", sort=True):
            e_novo = (grupo["novo_cliente"] == "S").any()
            for idx, row in enumerate(grupo.itertuples(index=False)):
                fill = _NOVO_FILL if e_novo else (_ALT_FILL if idx % 2 == 0 else PatternFill())
                for ci, col in enumerate(colunas_df, 1):
                    val = getattr(row, col) if hasattr(row, col) else None
                    cell = ws.cell(row=ri, column=ci, value=val)
                    cell.fill = fill
                    _aplicar_celula(cell, col, colunas_valor, colunas_data, e_novo)
                ri += 1
            # subtotal por cliente
            sf = Font(bold=True, size=10, color="7F6000" if e_novo else "1F4E79")
            for ci in range(1, n + 1):
                c = ws.cell(row=ri, column=ci)
                c.fill = _SUBTOT_FILL;
                c.font = sf;
                c.border = _BORDA
            ws.cell(row=ri, column=2, value=f"Subtotal – {cliente}").alignment = \
                Alignment(horizontal="left")
            for ci, col in enumerate(colunas_df, 1):
                if col in colunas_valor and col in grupo.columns:
                    c = ws.cell(row=ri, column=ci, value=grupo[col].sum())
                    c.number_format = "#,##0.00";
                    c.alignment = Alignment(horizontal="right")
                elif col == "pct_repasse":
                    c = ws.cell(row=ri, column=ci, value=pct)
                    c.number_format = "0.00%";
                    c.alignment = Alignment(horizontal="center")
            ri += 1

        _linha_total(ws, ri, n, f"TOTAL GERAL – {mes_label}", df_exibir,
                     colunas_df, colunas_valor, pct)
    else:
        ws.merge_cells(f"A{ri}:{get_column_letter(n)}{ri}")
        ws.cell(row=ri, column=1,
                value="Nenhum registro encontrado para o mês anterior.").font = \
            Font(italic=True, color="595959")

    larguras = {"dt_baixa": 12, "cliente": 40, "novo_cliente": 7,
                "documento": 16, "titulo": 14, "parcela": 10, "tc": 8,
                "unidade_principal": 14, "portador": 12, "operacao": 12,
                "data_vencimento": 12, "pct_repasse": 12, "valor_liquido_repasse": 18}
    _, _, colunas_num = _layout(centro)
    for c in colunas_num:
        larguras[c] = 14
    for i, col in enumerate(colunas_df, 1):
        ws.column_dimensions[get_column_letter(i)].width = larguras.get(col, 12)
    ws.freeze_panes = "A4"

    return (df_exibir["liquido"].sum(), df_exibir["valor_liquido_repasse"].sum()) if len(df_exibir) else (0.0, 0.0)


# ═════════════════════════════════════════════════════════════════════════════
# ORQUESTRADOR PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def transformar_centro(centro: dict, novos: set[str]) -> dict:
    nome_cc = str(centro["centro_custo"]).strip()
    slug_cc = _slug(nome_cc)
    pct = float(centro.get("pct_repasse") or 0)
    _, _, colunas_num = _layout(centro)

    dir_brutos = BASE_INPUT_DIR / slug_cc / "dados_brutos"
    dir_consol = BASE_OUTPUT_DIR / slug_cc / "dados_consolidados"
    dir_consol.mkdir(parents=True, exist_ok=True)

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M")
    destino_sin = dir_consol / f"{slug_cc}_consolidado_sintetico_{ts}.xlsx"
    destino_ana = dir_consol / f"{slug_cc}_consolidado_analitico_{ts}.xlsx"

    resultado = {"nome_cc": nome_cc, "total_liquido_mes_ana": 0.0, "total_mes_repasse_ana": 0.0,
                 "pct_repasse": 0, "df_ana": pd.DataFrame()}

    # ── Sintético ─────────────────────────────────────────────────────────────
    arqs_sin = sorted(dir_brutos.glob(f"{slug_cc}_*_sintetico.xlsx"))
    if not arqs_sin:
        logger.warning("[%s] Nenhum sintético encontrado", nome_cc)
    else:
        frames_sin = []
        for arq in arqs_sin:
            try:
                frames_sin.append(ler_sintetico(arq, novos, centro))
                logger.info("  ✓ %s", arq.name)
            except Exception:
                logger.exception("  ✗ %s — pulando", arq.name)

        if frames_sin:
            raw = pd.concat(frames_sin, ignore_index=True)
            df_sin = (
                raw.groupby(["cliente", "novo_cliente"], as_index=False)
                .agg({c: "sum" for c in colunas_num if c in raw.columns})
            )
            if "liquido" not in df_sin.columns and "vl_baixa" in df_sin.columns:
                df_sin["liquido"] = df_sin["vl_baixa"]
            df_sin["pct_repasse"] = pct
            df_sin["valor_liquido_repasse"] = (df_sin["liquido"] * pct).round(2)
            df_sin = df_sin.sort_values("cliente").reset_index(drop=True)

            wb_sin = Workbook()
            ws_sin = wb_sin.active
            ws_sin.title = "Consolidado"
            _salvar_sintetico(df_sin, ws_sin, nome_cc, pct, centro)
            _adicionar_fechamento_sintetico(wb_sin, df_sin, nome_cc, pct, centro)
            wb_sin.save(destino_sin)
            logger.info("[%s] Sintético salvo → %s", nome_cc, destino_sin.name)

    # ── Analítico ─────────────────────────────────────────────────────────────
    arqs_ana = sorted(dir_brutos.glob(f"{slug_cc}_*_analitico.xlsx"))
    if not arqs_ana:
        logger.warning("[%s] Nenhum analítico encontrado", nome_cc)
    else:
        frames_ana = []
        for arq in arqs_ana:
            try:
                frames_ana.append(ler_analitico(arq, novos, centro))
                logger.info("  ✓ %s", arq.name)
            except Exception:
                logger.exception("  ✗ %s — pulando", arq.name)

        if frames_ana:
            df_ana = pd.concat(frames_ana, ignore_index=True)
            df_ana["pct_repasse"] = pct
            df_ana["valor_liquido_repasse"] = (df_ana["liquido"] * pct).round(2)
            df_ana = df_ana.sort_values(["cliente", "dt_baixa"]).reset_index(drop=True)

            wb_ana = Workbook()
            ws_ana = wb_ana.active
            ws_ana.title = "Analítico"
            _salvar_analitico(df_ana, ws_ana, nome_cc, pct, centro)
            total_mes, total_mes_repasse_mes = _adicionar_fechamento_analitico(wb_ana, df_ana, nome_cc, pct, centro)
            wb_ana.save(destino_ana)
            logger.info("[%s] Analítico salvo → %s", nome_cc, destino_ana.name)
            resultado["total_liquido_mes_ana"] = total_mes
            resultado["total_mes_repasse_ana"] = total_mes_repasse_mes
            resultado["pct_repasse"] = pct
            resultado["df_ana"] = df_ana  # acumula para o consolidado geral

    return resultado


# ═════════════════════════════════════════════════════════════════════════════
# CONSOLIDADO GERAL ANALÍTICO  (para Power BI)
# ═════════════════════════════════════════════════════════════════════════════

def _gerar_consolidado_geral(resultados: list[dict]) -> None:
    """
    Concatena os DataFrames analíticos de todos os centros e salva como
    consolidado_geral_analitico_{AAAAMMDD_HHMM}.xlsx na raiz de BASE_OUTPUT_DIR.
    Cada execução cria um arquivo novo — o histórico anterior é preservado.

    Colunas: centro_custo | periodo | dt_baixa | cliente | novo_cliente
             + todas as demais colunas numéricas e descritivas presentes.
    """
    frames = [
        r["df_ana"].assign(centro_custo=r["nome_cc"])
        for r in resultados
        if isinstance(r.get("df_ana"), pd.DataFrame) and len(r["df_ana"])
    ]

    if not frames:
        logger.warning("Nenhum analítico disponível para o consolidado geral")
        return

    df = pd.concat(frames, ignore_index=True)

    cols_ordem = ["centro_custo", "periodo", "dt_baixa", "cliente", "novo_cliente"]
    cols_resto = [c for c in df.columns if c not in cols_ordem]
    df = df[[c for c in cols_ordem if c in df.columns] + cols_resto]
    df = df.sort_values(
        [c for c in ["centro_custo", "cliente", "dt_baixa"] if c in df.columns]
    ).reset_index(drop=True)

    df["data_carga"] = df.now().strftime("%Y-%m-%d %H:%M:%S")

    destino = BASE_OUTPUT_DIR / f"consolidado_geral_analitico.csv"

    df.to_csv(destino, sep=';', decimal=',')
    logger.info("Consolidado geral → %s  (%d linhas, %d centros)",
                destino.name, len(df), len(frames))


# ═════════════════════════════════════════════════════════════════════════════
# RESUMO WHATSAPP
# ═════════════════════════════════════════════════════════════════════════════

def _imprimir_resumo(resultados: list[dict]) -> None:
    _, _, mes_label = _mes_anterior()

    print("\n" + "=" * 60)
    print(f"RESUMO – {mes_label}")
    print("=" * 60)

    for r in resultados:
        total_liquido = r.get("total_liquido_mes_ana", 0.0)
        total_mes_repasse_ana = r.get("total_mes_repasse_ana", 0.0)
        pct_repasse = r.get("pct_repasse", 0.0)

        print(f"\n*{r['nome_cc']}*")
        print(f"Competência: *{mes_label}*")
        print(f"• Total líquido do mês fechado: *R$ {total_liquido:,.2f}*")
        print(f"• Percentual de repasse: *{pct_repasse:.2%}*")
        print(f"• Valor líquido de repasse: *R$ {total_mes_repasse_ana:,.2f}*")

        print("—" * 40)

    print("=" * 60 + "\n")


# ═════════════════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="Transform – Contas Recebidas")
    parser.add_argument("--so-consolidar", action="store_true",
                        help="Apenas consolida os brutos (sem fechamento nem painel)")
    args = parser.parse_args()

    centros = carregar_centros_ativos()
    logger.info("%d centros de custo ativos", len(centros))

    resultados = []
    for centro in centros:
        nome_cc = str(centro["centro_custo"]).strip()
        try:
            novos = carregar_extrato(nome_cc)
            res = transformar_centro(centro, novos)
            resultados.append(res)
        except Exception:
            logger.exception("Erro em '%s'", nome_cc)

    if not args.so_consolidar:
        _gerar_consolidado_geral(resultados)
        _imprimir_resumo(resultados)

    logger.info("Transform concluído.")


if __name__ == "__main__":
    main()
