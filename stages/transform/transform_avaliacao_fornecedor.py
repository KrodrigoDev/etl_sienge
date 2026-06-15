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


def executar(input_dir: Path = INPUT_DIR, output_dir: Path = OUTPUT_DIR) -> None:
    # ── 1. Leitura ────────────────────────────────────────────────────────────
    print("\n── 1. Leitura (contratos) ──────────────────────────────────────────")

    df = ler_dados(
        (input_dir / 'avaliacao_fornecedor').glob('*.xlsx'), formato='excel', salto=7
    )

    df = normalizar_colunas(df)


    df = df[['fornecedor', 'numero_de_avaliacoes', 'avaliacao_no_periodo', 'nome_arquivo']]

    df.dropna(subset=['numero_de_avaliacoes'], inplace=True)

    df['periodo_avaliacao'] = pd.to_datetime(
        df['nome_arquivo']
        .str.extract(r'_(\d{2})_(\d{4})')
        .apply(lambda x: f'01/{x[0]}/{x[1]}', axis=1),
        format='%d/%m/%Y'
    )

    df['fornecedor'] = df['fornecedor'].astype(str).str.strip()

    df[['fornecedor_cod', 'fornecedor_nome']] = (
        df['fornecedor']
        .apply(extrair_credor)
        .apply(pd.Series)
    )



    df =  df[['fornecedor', 'fornecedor_nome', 'fornecedor_cod', 'numero_de_avaliacoes', 'avaliacao_no_periodo', 'periodo_avaliacao']]

    salvar_tabela(df=df, nome='fato_avaliacao_fornecedor', destino=output_dir)


if __name__ == '__main__':
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )
    executar()



