from __future__ import annotations

import numpy as np
from pathlib import Path
from datetime import date

import pandas as pd

from utils.normalizer import salvar_tabela

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

pasta_origem = Path(__file__).resolve().parents[2]

INPUT_DIR = pasta_origem / 'stages' / 'transform' / 'input'
OUTPUT_DIR = pasta_origem / 'stages' / 'transform' / 'output'

THRESHOLD = 90  # score mínimo para considerar match
TOLERANCIA_DIAS = 45  # janela de datas aceita (em dias)

PESOS = {
    'cnpj': 50,
    'valor': 10,  # antes era 30
    'data': 10,
    'doc': 30,  # antes era 10
}

# ─────────────────────────────────────────────────────────────────────────────
# LEITURA
# ─────────────────────────────────────────────────────────────────────────────

df_titulo = pd.read_csv(OUTPUT_DIR / 'dim_titulo.csv', sep=';')
dim_credor_receita = pd.read_csv(OUTPUT_DIR / 'dim_credor_receita.csv', sep=';')

df_sienge = pd.merge(df_titulo, dim_credor_receita, on='credor', how='left')

files = (INPUT_DIR / 'servico_tomado' / '19.06.2026').rglob('*.csv*')  # alterar a data sempre que for atualizar

for file in files:
    try:
        df = pd.read_csv(file, sep=';', decimal=',')

        # Verifica se a coluna existe e se todos os valores estão vazios
        if 'valor' in df.columns and df['valor'].isna().all():
            file.unlink()  # apaga o arquivo
            print(f'Arquivo removido: {file}')

    except Exception as e:
        print(f'Erro ao processar {file}: {e}')


breakpoint()

df_giss = pd.concat([pd.read_csv(f, sep=';', decimal=',') for f in files],
                    ignore_index=True, )


def ler_auxiliar_empresas():
    df_empresa = pd.read_excel(
        INPUT_DIR / 'reference' / 'auxliar_empresas.xlsx',
        skiprows=3
    )

    # define índice
    df_empresa = df_empresa.set_index('Unnamed: 0')

    # pega somente coluna desejada
    serie = df_empresa['Unnamed: 2']

    # empresas
    empresas = (
        serie.loc['Empresa']
        .reset_index(drop=True)
        .rename('empresa')
    )

    # cnpjs
    cnpjs = (
        serie.loc['CNPJ']
        .reset_index(drop=True)
        .rename('cnpj')
    )

    # junta lado a lado
    df_normalizado = pd.concat(
        [empresas, cnpjs],
        axis=1
    )

    # remove linhas vazias
    df_normalizado = df_normalizado.dropna(
        subset=['empresa', 'cnpj'],
        how='all'
    )

    # limpa espaços
    df_normalizado['empresa'] = (
        df_normalizado['empresa']
        .astype(str)
        .str.strip()
    )

    df_normalizado['cnpj'] = (
        df_normalizado['cnpj']
        .astype(str)
        .str.strip()
    )

    df_normalizado = df_normalizado.drop_duplicates(
        subset='cnpj',
        keep='first'
    )

    return df_normalizado


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

def _prefixar_giss(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            c: f'{c}_giss'
            for c in df.columns
            if not c.startswith('_')
        }
    )


def _prefixar_sienge(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            c: f'{c}_sienge'
            for c in df.columns
            if not c.startswith('_')
        }
    )


def _limpar_cnpj(s: pd.Series) -> pd.Series:
    """Remove qualquer caractere não-dígito e retorna string."""
    return s.astype(str).str.replace(r'\D', '', regex=True)


def _extrair_numero_doc(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.extract(r'(\d+)(?!.*\d)')[0]
        .astype('Int64')
    )


def _normalizar_valor(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.strip()
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
    )


def _preparar_giss(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['_cnpj_norm'] = _limpar_cnpj(df['cnpj_cpf'])
    df['_valor'] = pd.to_numeric(_normalizar_valor(df['valor']), errors='coerce').pipe(np.floor).astype('Int64')
    df['_data'] = pd.to_datetime(df['emissao'], dayfirst=True, errors='coerce').dt.normalize()
    df['_num_doc'] = pd.to_numeric(df['nfs'], errors='coerce')
    df['_idx_giss'] = df.index  # guarda posição original

    df.drop_duplicates(subset=['_cnpj_norm', '_num_doc', '_data', '_valor'], inplace=True)

    auxiliar_empresa = ler_auxiliar_empresas()

    df = df.merge(auxiliar_empresa, left_on='cnpj_empresa', right_on='cnpj', how='left')

    df.drop(columns='cnpj', inplace=True)

    df = df[df['situacao'] == 'Ativa'].reset_index(drop=True)

    return df


def _preparar_sienge(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['_cnpj_norm'] = _limpar_cnpj(df['cnpj/cpf'])
    df['_valor'] = pd.to_numeric(df['valor_bruto'], errors='coerce').pipe(np.floor).astype('Int64')
    df['_data'] = pd.to_datetime(df['emissao_nf'], errors='coerce').dt.normalize()
    df['_num_doc'] = _extrair_numero_doc(df['documento'])
    df['_idx_sienge'] = df.index  # guarda posição original
    return df


# ─────────────────────────────────────────────────────────────────────────────
# SCORE VETORIZADO (opera em DataFrames já alinhados pelo cross-join)
# ─────────────────────────────────────────────────────────────────────────────

def _doc_similar(doc_g, doc_s):
    if pd.isna(doc_g) or pd.isna(doc_s):
        return False

    doc_g = str(int(doc_g))
    doc_s = str(int(doc_s))

    if doc_g == doc_s:
        return True

    # regra de "dígito faltando": só aplica para docs com >= 5 dígitos
    diff = abs(len(doc_g) - len(doc_s))
    if diff == 1 and min(len(doc_g), len(doc_s)) >= 5:
        menor, maior = (doc_g, doc_s) if len(doc_g) < len(doc_s) else (doc_s, doc_g)
        if maior.startswith(menor) or maior.endswith(menor):
            return True

    return False


def _calcular_scores(cross: pd.DataFrame) -> pd.Series:
    score = pd.Series(0, index=cross.index, dtype=int)

    # CNPJ (50 pts) — já garantido pelo bloco, mas pontuamos mesmo assim
    mask_cnpj = cross['_cnpj_norm_g'] == cross['_cnpj_norm_s']
    score += mask_cnpj * PESOS['cnpj']

    # Valor (30 pts)
    mask_valor = (cross['_valor_g'] - cross['_valor_s']).abs() < 0.01
    score += mask_valor.fillna(False) * PESOS['valor']

    # Data (10 pts)
    diff_dias = (cross['_data_g'] - cross['_data_s']).abs().dt.days
    mask_data = diff_dias <= TOLERANCIA_DIAS
    score += mask_data.fillna(False) * PESOS['data']

    # Número do documento (10 pts)
    mask_doc = cross.apply(
        lambda row: _doc_similar(
            row['_num_doc_g'],
            row['_num_doc_s']
        ),
        axis=1
    )

    score += mask_doc.fillna(False) * PESOS['doc']

    return score


# ─────────────────────────────────────────────────────────────────────────────
# MATCHING POR BLOCO DE CNPJ
# ─────────────────────────────────────────────────────────────────────────────

def match_bases(
        df_giss_prep: pd.DataFrame,
        df_sienge_prep: pd.DataFrame,
        threshold: int = THRESHOLD,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Retorna três DataFrames:
        matched      – pares com score >= threshold
        only_giss    – registros GISS sem correspondência
        only_sienge  – registros SIENGE sem correspondência
    """
    blocos_g = df_giss_prep.groupby('_cnpj_norm')
    blocos_s = df_sienge_prep.groupby('_cnpj_norm')

    cnpjs_comuns = set(blocos_g.groups) & set(blocos_s.groups)

    pares: list[pd.DataFrame] = []

    for cnpj in cnpjs_comuns:
        bloco_g = _prefixar_giss(
            blocos_g.get_group(cnpj).copy()
        )

        bloco_s = _prefixar_sienge(
            blocos_s.get_group(cnpj).copy()
        )

        # Cross-join dentro do bloco
        bloco_g['_key'] = 1
        bloco_s['_key'] = 1
        cross = bloco_g.merge(bloco_s, on='_key', suffixes=('_g', '_s')).drop(columns='_key')

        cross['score'] = _calcular_scores(cross)

        # if cnpj == "10829093000115":
        #     debug_cols = [
        #         '_cnpj_norm_g',
        #         '_cnpj_norm_s',
        #         '_valor_g',
        #         '_valor_s',
        #         '_data_g',
        #         '_data_s',
        #         '_num_doc_g',
        #         '_num_doc_s',
        #         'score'
        #     ]
        #
        #     print(f"\n========== DEBUG CNPJ {cnpj} ==========")
        #     print(
        #         cross[debug_cols]
        #         .sort_values('score', ascending=False)
        #         .head(1000)
        #         .to_string()
        #     )

        pares.append(cross[cross['score'] >= threshold])

    if pares:
        todos = pd.concat(pares, ignore_index=True)
    else:
        # nenhum par encontrado → DataFrame vazio com colunas corretas
        todos = pd.DataFrame()

    # ── greedy: cada linha de cada base usada no máximo uma vez ──────────────
    if not todos.empty:
        todos = (
            todos
            .sort_values('score', ascending=False)
            .drop_duplicates(subset='_idx_giss')
            .drop_duplicates(subset='_idx_sienge')
            .reset_index(drop=True)
        )

    matched_idx_giss = set(todos['_idx_giss']) if not todos.empty else set()
    matched_idx_sienge = set(todos['_idx_sienge']) if not todos.empty else set()

    # ── somente GISS / somente SIENGE ────────────────────────────────────────
    only_giss = df_giss_prep[~df_giss_prep['_idx_giss'].isin(matched_idx_giss)].copy()
    only_sienge = df_sienge_prep[~df_sienge_prep['_idx_sienge'].isin(matched_idx_sienge)].copy()

    return todos, only_giss, only_sienge


# ─────────────────────────────────────────────────────────────────────────────
# LIMPEZA DAS COLUNAS AUXILIARES
# ─────────────────────────────────────────────────────────────────────────────

_COLS_AUX = ['_cnpj_norm', '_valor', '_data', '_num_doc', '_idx_giss', '_idx_sienge', '_key']


def _limpar_aux(df: pd.DataFrame) -> pd.DataFrame:
    cols_remover = [c for c in df.columns if c in _COLS_AUX
                    or c.startswith('_cnpj_norm')
                    or c.startswith('_valor_')
                    or c.startswith('_data_')
                    or c.startswith('_num_doc_')
                    or c.startswith('_idx_')]
    return df.drop(columns=cols_remover, errors='ignore')


def _coluna_similaridade(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona coluna score_similaridade (0–100) e score_label."""
    df = df.copy()
    df['score_similaridade'] = df['score']
    df['score_label'] = pd.cut(
        df['score'],
        bins=[0, 49, 79, 89, 100],
        labels=['baixo', 'medio', 'alto', 'exato'],
        right=True,
    )
    return df.drop(columns='score')


# ─────────────────────────────────────────────────────────────────────────────
# EXECUÇÃO
# ─────────────────────────────────────────────────────────────────────────────

df_giss_prep = _preparar_giss(df_giss)
df_sienge_prep = _preparar_sienge(df_sienge)

df_matched, df_only_giss, df_only_sienge = match_bases(df_giss_prep, df_sienge_prep)

# ── matched: colunas originais de ambas + score ───────────────────────────────
if not df_matched.empty:
    df_matched = _coluna_similaridade(df_matched)
    df_matched = _limpar_aux(df_matched)

# ── only_giss / only_sienge: colunas originais apenas ────────────────────────
df_only_giss = _limpar_aux(df_only_giss)
df_only_sienge = _limpar_aux(df_only_sienge)

# ─────────────────────────────────────────────────────────────────────────────
# SAÍDA
# ─────────────────────────────────────────────────────────────────────────────


df_matched = df_matched[
    [
        'cnpj_empresa_giss', 'competencia_giss', 'cnpj_cpf_giss', 'prestador_giss', 'nfs_giss',
        'valor_giss', 'situacao_giss', 'declaracao_giss', 'titulo_sienge', 'credor_sienge',
        'cnpj/cpf_sienge', 'documento_sienge', 'emissao_nf_sienge', 'valor_bruto_sienge', 'score_similaridade',
        'score_label'
    ]
]

print(f"matched      : {len(df_matched):>6} registros  → match_merged.csv")
print(f"only_giss    : {len(df_only_giss):>6} registros  → match_only_giss.csv")
print(f"only_sienge  : {len(df_only_sienge):>6} registros  → match_only_sienge.csv")
print(f"\nThreshold usado : {THRESHOLD} pts")
print(f"Tolerância datas: {TOLERANCIA_DIAS} dias")
if not df_matched.empty:
    print(f"\nDistribuição de score_label:")
    print(df_matched['score_label'].value_counts().to_string())

# ─────────────────────────────────────────────────────────────────────────────
# FAIXA DE ANTIGUIDADE
# Calculada com base na data de emissão vs. hoje (data da carga).
# Persiste no CSV para uso direto no BI sem medida calculada adicional.
# ─────────────────────────────────────────────────────────────────────────────

_data_ref = pd.Timestamp(date.today())

df_only_giss['_emissao_dt'] = pd.to_datetime(
    df_only_giss['emissao'], dayfirst=True, errors='coerce'
).dt.normalize()

df_only_giss['dias_em_aberto'] = (
    (_data_ref - df_only_giss['_emissao_dt'])
    .dt.days
    .clip(lower=0)
)

df_only_giss['faixa_antiguidade'] = pd.cut(
    df_only_giss['dias_em_aberto'],
    bins=[-1, 15, 30, 45, 90, float('inf')],
    labels=['A. 0–15d', 'B. 16–30d', 'C. 31–45d', 'D. 46–90d', 'E. >90d (crítico)'],
    right=True,
).astype(str)

df_only_giss = df_only_giss.drop(columns='_emissao_dt')

hoje = date.today().isoformat()
df_only_giss['data_carga'] = hoje

caminho_fato = OUTPUT_DIR / 'fato_servico_tomado_giss.csv'

if caminho_fato.exists():
    df_historico = pd.read_csv(caminho_fato, sep=';')
    df_historico = df_historico[df_historico['data_carga'] != hoje]
    df_only_giss = pd.concat([df_historico, df_only_giss], ignore_index=True)
    print(f"  Histórico preservado: {df_historico['data_carga'].nunique()} data(s) anterior(es)")

salvar_tabela(df_only_giss, 'fato_servico_tomado_giss', OUTPUT_DIR)
