from __future__ import annotations

from pathlib import Path

import pandas as pd

from stages.transform.utils.normalizer import (
    ler_dados,
    normalizar_colunas,
    salvar_tabela,
    extrair_credor
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

pasta_origem = Path(__file__).resolve().parents[2]

INPUT_DIR = pasta_origem / 'stages' / 'transform' / 'input'
OUTPUT_DIR = pasta_origem / 'stages' / 'transform' / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)



df_titulo = pd.read_csv(OUTPUT_DIR / 'dim_titulo.csv', sep=';')
dim_credor_receita = pd.read_csv(OUTPUT_DIR / 'dim_credor_receita.csv', sep=';')


df_sienge = pd.merge(
    df_titulo,
    dim_credor_receita[['credor', 'cnpj/cpf', 'telefone']],
    on='credor',
    how='left'
)



