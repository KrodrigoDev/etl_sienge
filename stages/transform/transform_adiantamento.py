"""
transform/adiantamento.py
--------------------------
Transforma o relatório de adiantamentos do SIENGE em um DataFrame
tabular e normalizado, com as colunas:

    empresa | cod_credor | credor | documento_vinculado |
    data | vencto | documento | tipo_do_mov | vl_movimento | saldo | observacao |
    nome_arquivo
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.normalizer import (
    converter_valor_br,
    ler_dados,
    normalizar_colunas,
    salvar_tabela,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

pasta_origem = Path(__file__).resolve().parents[2]

INPUT_DIR = pasta_origem / 'stages' / 'transform' / 'input'
OUTPUT_DIR = pasta_origem / 'stages' / 'transform' / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

# Marcadores de linha que identificam cada tipo de registro
_MARCADOR_EMPRESA = 'empresa'
_MARCADOR_CREDOR = 'credor'
_MARCADOR_DOC_VINC = 'documento vinculado'
_MARCADOR_HEADER = 'data'  # linha de cabeçalho das colunas de detalhe
_MARCADOR_SALDO_CR = 'saldo de adiantamento do credor'
_MARCADOR_TOTAL = 'total de adiantamentos'


def _extrair_empresa(valor: str) -> tuple[str | None, str | None]:
    """
    '46 - CONSORCIO CRECHES PERNAMBUCO/2024'
    → cod_empresa='46', empresa='CONSORCIO CRECHES PERNAMBUCO/2024'
    """
    valor = str(valor).strip()
    if ' - ' in valor:
        cod, nome = valor.split(' - ', 1)
        return cod.strip(), nome.strip()
    return None, valor


def _extrair_credor(valor: str) -> tuple[str | None, str | None]:
    """
    '309 - CANSANCAO E FARIAS LTDA'
    → cod_credor='309', credor='CANSANCAO E FARIAS LTDA'
    """
    return _extrair_empresa(valor)  # mesma lógica de split


def _is_linha_dado(row: pd.Series, col_data: str) -> bool:
    """Retorna True se a linha parece ser um registro de movimentação."""
    val = str(row[col_data]).strip()

    # Data no formato DD/MM/AAAA
    return len(val) == 10 and val[2] == '/' and val[5] == '/'


# ─────────────────────────────────────────────────────────────────────────────
# PARSING PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def _parse_df_bruto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe o DataFrame bruto (logo após ler_dados + normalizar_colunas) e
    devolve um DataFrame tabular com uma linha por movimentação.

    Colunas esperadas após normalizar_colunas (baseado no CSV de validação):
        unnamed_0  unnamed_1  unnamed_2  unnamed_3  unnamed_4
        unnamed_5  unnamed_6  unnamed_7  unnamed_8  unnamed_9
        nome_arquivo

    Layout posicional das colunas no xlsx original (confirmado via CSV de validação):
        col[0]  → marcador de tipo (Empresa, Credor, Documento vinculado…) OU Data
        col[1]  → vazio nas linhas de marcador
        col[2]  → Vencto (nas linhas de dado) / 'Vencto' label no cabeçalho
        col[3]  → valor do marcador (empresa/credor/doc_vinculado) ← posição confirmada
                  OU valor de Documento quando é linha de dado
        col[4]  → vazio
        col[5]  → vazio / 'Tipo do mov.' label no cabeçalho
        col[6]  → valor de Tipo do mov. (nas linhas de dado)
        col[7]  → valor de Vl. movimento (nas linhas de dado)
        col[8]  → valor de Saldo (nas linhas de dado)
        col[9]  → valor de Observação (nas linhas de dado)
        col[10] → nome_arquivo
    """

    registros = []

    # Estado corrente
    empresa_cod: str | None = None
    empresa_nome: str | None = None
    credor_cod: str | None = None
    credor_nome: str | None = None
    doc_vinc: str | None = None
    nome_arq: str | None = None

    cols = df.columns.tolist()

    # Mapeia posições (mais robusto do que hardcode de nomes)
    # Após normalizar_colunas as colunas ficam: unnamed_0 … unnamed_9, nome_arquivo
    def cv(row, pos):
        """Valor da coluna na posição pos, ou '' se não existir."""
        if pos < len(cols):
            v = row[cols[pos]]
            return '' if pd.isna(v) else str(v).strip()
        return ''

    for _, row in df.iterrows():
        c0 = cv(row, 0).lower()  # marcador de tipo OU data
        c3 = cv(row, 3)  # valor do marcador (confirmado via CSV de validação)
        nome_arq = cv(row, 10)  # nome_arquivo (última coluna)

        # ── Empresa ──────────────────────────────────────────────────────────
        if c0 == _MARCADOR_EMPRESA:
            empresa_cod, empresa_nome = _extrair_empresa(c3)
            continue

        # ── Credor ───────────────────────────────────────────────────────────
        if c0 == _MARCADOR_CREDOR:
            credor_cod, credor_nome = _extrair_credor(c3)
            doc_vinc = None  # reset ao mudar de credor
            continue

        # ── Documento vinculado ───────────────────────────────────────────────
        if c0 == _MARCADOR_DOC_VINC:
            doc_vinc = c3
            continue

        # ── Cabeçalho / Saldo / Total → ignorar ──────────────────────────────
        if c0 in (_MARCADOR_HEADER, _MARCADOR_SALDO_CR, _MARCADOR_TOTAL):
            continue

        # ── Linha de dado (movimentação) ─────────────────────────────────────
        # Posições confirmadas pelo CSV de validação:
        #   col[0]=Data | col[2]=Vencto | col[3]=Documento | col[6]=Tipo_mov
        #   col[7]=Vl_movimento | col[8]=Saldo | col[9]=Observação
        if len(c0) == 10 and c0[2] == '/' and c0[5] == '/':
            registros.append({
                'empresa_cod': empresa_cod,
                'empresa': empresa_nome,
                'cod_credor': credor_cod,
                'credor': credor_nome,
                'documento_vinculado': doc_vinc,
                'data': c0,
                'vencto': cv(row, 2),
                'documento': cv(row, 3),
                'tipo_do_mov': cv(row, 5),
                'vl_movimento': cv(row, 6),
                'saldo': cv(row, 7),
                'observacao': cv(row, 9),
                'nome_arquivo': nome_arq,
            })
            continue

    return pd.DataFrame(registros)


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMAÇÕES PÓS-PARSE
# ─────────────────────────────────────────────────────────────────────────────

def _transformar(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica tipagens e limpezas finais ao DataFrame já parseado."""

    # Datas
    for col_dt in ('data', 'vencto'):
        df[col_dt] = pd.to_datetime(df[col_dt], format='%d/%m/%Y', errors='coerce')


    # IDs inteiros
    df['empresa_cod'] = pd.to_numeric(df['empresa_cod'], errors='coerce').astype('Int64')
    df['cod_credor'] = pd.to_numeric(df['cod_credor'], errors='coerce').astype('Int64')

    # Limpeza de texto
    df['observacao'] = df['observacao'].str.replace(r'\s+', ' ', regex=True).str.strip()

    df['tipo_vinculo'] =  df['documento_vinculado'].apply(lambda x: 'Contrato' if str(x).startswith('CTS') else 'Pedido de Compra')
    
    return df


# ─────────────────────────────────────────────────────────────────────────────
# PONTO DE ENTRADA
# ─────────────────────────────────────────────────────────────────────────────

def executar(
        input_dir: Path = INPUT_DIR,
        output_dir: Path = OUTPUT_DIR,
) -> None:
    # ── 1. Leitura ────────────────────────────────────────────────────────────
    print("\n── 1. Leitura (adiantamentos) ─────────────────────────────────────")

    arquivos = list((input_dir / 'adiantamento').glob('*.xlsx'))
    df_bruto = ler_dados(arquivos=arquivos, formato='excel', salto=0)

    # Remove linhas completamente vazias (Unnamed: 0 nulo = rodapé/espaçador)
    df_bruto.dropna(subset=['Unnamed: 0'], inplace=True)

    # Normaliza nomes de colunas para snake_case sem acentos
    df_bruto = normalizar_colunas(df_bruto)

    # ── 2. Parse hierárquico ──────────────────────────────────────────────────
    print("\n── 2. Parse hierárquico ───────────────────────────────────────────")
    df = _parse_df_bruto(df_bruto)
    print(f"  Linhas de movimentação extraídas: {len(df)}")

    # ── 3. Tipagem e limpeza ──────────────────────────────────────────────────
    print("\n── 3. Tipagem e limpeza ───────────────────────────────────────────")
    df = _transformar(df)
    print(df.dtypes)

    # ── 4. Salvar ─────────────────────────────────────────────────────────────
    print("\n── 4. Salvando ────────────────────────────────────────────────────")
    salvar_tabela(df, 'fato_adiantamento', output_dir)


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    executar()
