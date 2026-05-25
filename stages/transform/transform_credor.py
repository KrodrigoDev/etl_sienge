from __future__ import annotations

from pathlib import Path

import pandas as pd

from stages.transform.utils.normalizer import (
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


def _extrair_credor(valor: str) -> tuple[str | None, str | None]:
    """
    '1 - O2 ENGENHARIA LTDA - EPP'
    → credor_cod='1', credor_nome='O2 ENGENHARIA LTDA - EPP'
    """
    valor = str(valor).strip()

    if ' - ' in valor:
        cod, nome = valor.split(' - ', 1)
        return cod.strip(), nome.strip()

    return None, valor


def executar(
        input_dir: Path = INPUT_DIR,
        output_dir: Path = OUTPUT_DIR,
) -> None:
    # ── 1. Leitura ────────────────────────────────────────────────────────────
    print("\n── 1. Leitura (credor) ─────────────────────────────────────────")

    arquivos = list((input_dir / 'credor').glob('*.xlsx'))

    df_bruto = ler_dados(
        arquivos=arquivos,
        formato='excel',
        salto=4
    )

    # Normaliza nomes de colunas
    df_bruto = normalizar_colunas(df_bruto)

    df_bruto = df_bruto[
        ['credor', 'cnpj/cpf', 'ie/identidade',
         'endereco', 'municipio', 'cep',
         'telefone', 'ramal', 'tipo_de_credor',
         'classificacao_tributaria', 'avaliacao', 'nome_arquivo']
    ]

    df_bruto.dropna(subset=['cnpj/cpf'], inplace=True)

    # ── Extração de código e nome ────────────────────────────────────────────
    df_bruto.loc[:, 'credor_cod'] = pd.NA
    df_bruto.loc[:, 'credor_nome'] = pd.NA

    for index, row in df_bruto.iterrows():
        credor_cod, credor_nome = _extrair_credor(row['credor'])

        df_bruto.loc[index, 'credor_cod'] = credor_cod
        df_bruto.loc[index, 'credor_nome'] = credor_nome
        df_bruto.loc[index, 'credor'] = str(row['credor']).strip()

    # Exemplo de salvamento
    salvar_tabela(df=df_bruto, nome='dim_credor_receita', destino=output_dir)


if __name__ == '__main__':
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )
    executar()
