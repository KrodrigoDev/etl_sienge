"""
utils/normalizer.py
--------------------
Funções utilitárias reutilizadas por todos os módulos de transformação.

Importar com:
    from utils.normalizer import *
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# I/O
# ─────────────────────────────────────────────────────────────────────────────

def ler_dados(arquivos) -> pd.DataFrame:
    """Lê e concatena todos os CSVs de um glob em um único DataFrame."""
    dfs = []
    for arq in arquivos:
        df = pd.read_csv(arq, sep=';', low_memory=False)
        print(f"  Lido: {arq.name} → {df.shape}")
        dfs.append(df)
    if not dfs:
        raise FileNotFoundError("Nenhum arquivo encontrado no glob informado.")
    return pd.concat(dfs, ignore_index=True)


def salvar_tabela(df: pd.DataFrame, nome: str, destino: Path) -> None:
    """
    Salva tabela em CSV com separador ';' e decimal '.' (padrão internacional).
    Garante que colunas numéricas não sejam salvas como texto com formato pt-BR.
    """
    caminho = destino / f"{nome}.csv"
    df.to_csv(caminho, index=False, sep=';', decimal='.', float_format='%.6g')
    print(f"  Salvo: {nome}.csv → {df.shape}")


# ─────────────────────────────────────────────────────────────────────────────
# LIMPEZA E NORMALIZAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza os nomes de colunas para snake_case sem acentos.
    Exemplos:
        'Nº da Solicitação' → 'n_da_solicitacao'
        'Código Obra'       → 'codigo_obra'
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('.', '', regex=False)
        .str.replace('°', 'n', regex=False)
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
    )
    return df


def converter_valor_br(serie: pd.Series) -> pd.Series:
    """
    Converte coluna de valor no formato brasileiro (R$ 1.234,56) para float.
    Trata: símbolo R$, espaço não-quebrável (\\xa0 / \\u00a0),
    ponto como milhar, vírgula como decimal.
    Retorna NaN para valores não parseáveis.
    """
    return (
        serie.astype(str)
        .str.replace('R$',     '', regex=False)
        .str.replace('\xa0',   '', regex=False)
        .str.replace('\u00a0', '', regex=False)
        .str.replace('.',      '', regex=False)
        .str.replace(',',      '.', regex=False)
        .str.strip()
        .pipe(pd.to_numeric, errors='coerce')
    )


def converter_quantidade_br(serie: pd.Series) -> pd.Series:
    """
    Converte coluna de quantidade no formato brasileiro (1.234,5678) para float.
    Diferente de converter_valor_br: não trata o prefixo 'R$'.
    Usada nas colunas de estoque (Quantidade insumo, Quantidade reservada, etc.).
    """
    return (
        serie.astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
        .str.strip()
        .pipe(pd.to_numeric, errors='coerce')
    )


# ─────────────────────────────────────────────────────────────────────────────
# DIMENSÕES
# ─────────────────────────────────────────────────────────────────────────────

def criar_dimensao(
    df: pd.DataFrame,
    colunas: list[str],
    nome_id: str,
) -> pd.DataFrame:
    """
    Cria uma dimensão com surrogate key a partir das colunas naturais.
    Remove duplicatas e reinicia o índice antes de gerar o id.
    """
    dim = df[colunas].drop_duplicates().reset_index(drop=True).copy()
    dim.insert(0, nome_id, dim.index + 1)
    return dim


def expandir_dimensao(
    dim_existente: pd.DataFrame,
    df_novo: pd.DataFrame,
    colunas_naturais: list[str],
    nome_id: str,
    col_pk_natural: str,
) -> pd.DataFrame:
    """
    Expande uma dimensão existente com registros novos que ainda não existem nela.

    Parâmetros
    ----------
    dim_existente    : DataFrame da dimensão já criada (ex: dim_obra do painel)
    df_novo          : DataFrame com novos registros candidatos (ex: estoque)
    colunas_naturais : Colunas que compõem a chave natural (ex: ['cod_obra', 'obra'])
    nome_id          : Nome da surrogate key (ex: 'id_obra')
    col_pk_natural   : Coluna que identifica unicidade (ex: 'cod_obra')

    Retorna
    -------
    dim_expandida com os novos registros incorporados, surrogate keys contínuas.
    """
    pks_existentes = set(dim_existente[col_pk_natural].dropna().unique())

    novos = (
        df_novo[colunas_naturais]
        .drop_duplicates(subset=col_pk_natural)
        .dropna(subset=[col_pk_natural])
    )
    novos = novos[~novos[col_pk_natural].isin(pks_existentes)]

    if novos.empty:
        return dim_existente

    proximo_id = dim_existente[nome_id].max() + 1
    novos = novos.reset_index(drop=True).copy()


    if nome_id in novos.columns:
        novos = novos.drop(columns=[nome_id])

    novos.insert(0, nome_id, range(proximo_id, proximo_id + len(novos)))

    dim_expandida = pd.concat([dim_existente, novos], ignore_index=True)
    print(f"  {nome_id}: +{len(novos)} novos registros (total: {len(dim_expandida)})")


    return dim_expandida


def cod_grupo_to_id(cod: str | float) -> int | None:
    """
    Converte o código de grupo do SIENGE em inteiro único.
    Usa multiplicação p1 * 1000 + p2 para preservar zeros decimais.
    Exemplos:
        "02.029" → 2029
        "02.020" → 2020   (evita truncar o zero da parte decimal)
        "8.001"  → 8001
        2.034    → 2034   (aceita float direto)
    """
    try:
        partes = str(cod).strip().split('.')
        p1 = int(partes[0])
        p2 = int(partes[1]) if len(partes) > 1 else 0
        return p1 * 1000 + p2
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# VALIDAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

def checar_integridade(
    fato: pd.DataFrame,
    col_fk: str,
    dim: pd.DataFrame,
    col_pk: str,
    nome: str,
) -> None:
    """
    Verifica se todas as FKs do fato existem na dimensão correspondente.
    Imprime um resumo com contagem de orphans.
    """
    fks = set(fato[col_fk].dropna().unique())
    pks = set(dim[col_pk].dropna().unique())
    orphans = fks - pks
    status = "✓" if not orphans else f"ATENÇÃO → {list(orphans)[:5]}"
    print(f"  [{nome}] orphans: {len(orphans)} {status}")