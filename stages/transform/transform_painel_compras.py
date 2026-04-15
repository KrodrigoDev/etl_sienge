"""
stages/transform/transform_painel_compras.py
---------------------------------------------
Transforma os CSVs brutos do Painel de Compras do SIENGE em:

  Dimensões : dim_obra, dim_grupo_insumo, dim_insumo,
              dim_fornecedor, dim_comprador, dim_solicitante
  Lead Time : dim_lead_times  (long: 1 linha por grupo × tipo_obra × estado)
  Fato      : fato_solicitacao_item

Relacionamentos gerados (todos 1:N, single direction)
------------------------------------------------------
  dim_obra[id_obra]             → fato_solicitacao_item[id_obra]
  dim_insumo[id_insumo]         → fato_solicitacao_item[id_insumo]
  dim_fornecedor[id_fornecedor] → fato_solicitacao_item[id_fornecedor]
  dim_comprador[id_comprador]   → fato_solicitacao_item[id_comprador]
  dim_solicitante[id_sol.]      → fato_solicitacao_item[id_solicitante]
  dim_grupo_insumo[id_grupo]    → fato_solicitacao_item[id_grupo]
  dim_lead_times[id_grupo]  → dim_grupo_insumo[id_grupo]
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import numpy as np

from utils.normalizer import (
    checar_integridade,
    cod_grupo_to_id,
    converter_valor_br,
    criar_dimensao,
    ler_dados,
    normalizar_colunas,
    salvar_tabela,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

pasta_origem = Path(__file__).resolve().parents[2]

INPUT_DIR = pasta_origem / 'stages' / 'transform' / 'input'
REFERENCE_DIR = pasta_origem / 'stages' / 'transform' / 'input' / 'reference'
OUTPUT_DIR = pasta_origem / 'stages' / 'transform' / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TIPOS_OBRA = ['PRIVADA', 'PÚBLICA']
ESTADOS = ['AL', 'PE', 'RS']

COLUNAS_DATA = [
    'data_da_solicitacao', 'data_autorizacao_da_solicitacao',
    'data_para_chegada_a_obra', 'data_do_pedido', 'previsao_de_entrega',
    'data_autorizacao_do_pedido', 'data_da_nota_fiscal', 'data_entrega_na_obra',
]


def executar(input_dir: Path = INPUT_DIR,
             reference_dir: Path = REFERENCE_DIR,
             output_dir: Path = OUTPUT_DIR) -> None:
    """
    Ponto de entrada do transform do painel de compras.
    Pode ser chamado diretamente ou pelo main.py.
    """

    # ── 1. Leitura ────────────────────────────────────────────────────────────
    print("\n── 1. Leitura (painel compras) ─────────────────────────────────────")

    df_painel = ler_dados((input_dir / 'painel_compras').glob('*.csv'))
    df_painel = normalizar_colunas(df_painel)

    for col in COLUNAS_DATA:
        if col in df_painel.columns:
            df_painel[col] = pd.to_datetime(df_painel[col], dayfirst=True, errors='coerce')

    df_painel['saldo'] = df_painel['saldo'].fillna(0)
    df_painel['detalhe'] = df_painel['detalhe'].fillna('0')

    print(f"  Total de linhas: {len(df_painel):,}")

    # NF composta para evitar duplicidade entre fornecedores
    mask = df_painel['nn_da_nota_fiscal'].notna() & (df_painel['nn_da_nota_fiscal'] != '')
    df_painel.loc[mask, 'nn_da_nota_fiscal'] = (
            df_painel.loc[mask, 'nn_da_nota_fiscal'].astype(str)
            + ' - '
            + df_painel.loc[mask, 'cod_fornecedor'].astype(str)
    )

    # ── 2. Chave do item ──────────────────────────────────────────────────────
    df_painel['id_item'] = (
            df_painel['no_da_solicitacao'].astype(str) + '-' +
            df_painel['cod_insumo'].astype(str) + '-' +
            df_painel['detalhe'].astype(str)
    )

    # ── 3. dim_obra ───────────────────────────────────────────────────────────
    print("\n── 3. dim_obra ─────────────────────────────────────────────────────")

    auxiliar_obra_gabriel = pd.read_csv(reference_dir / 'auxiliar_gabriel.csv', sep=',')

    # trazer as informações importantes

    dim_obra = criar_dimensao(df_painel, ['cod_obra', 'obra'], 'id_obra')

    dim_obra = dim_obra.merge(
        auxiliar_obra_gabriel[[
            'Cod. Centro de Custo', 'Classificação 1', 'Classificação 2', 'Tipo de Obra', 'Tipo de Obra 2 '
        ]].rename(columns={
            'Classificação 1': 'filial',
            'Classificação 2': 'classificacao',
            'Tipo de Obra': 'tipo_obra',
            'Tipo de Obra 2 ': 'tipo_obra_2',
            'Cod. Centro de Custo': '_cod_join',
        }).drop_duplicates(subset='_cod_join'),

        left_on='cod_obra', right_on='_cod_join', how='left'
    ).drop(columns='_cod_join', errors='ignore')

    for campo in ['nome_curto', 'estado', 'cidade', 'regiao',
                  'status_obra', 'coord_obra', 'data_inicio', 'data_prev_entrega', 'cnpj']:
        if campo not in dim_obra.columns:
            dim_obra[campo] = None

    print(f"  dim_obra: {dim_obra.shape}")

    # ── 4. dim_insumo ─────────────────────────────────────────────────────────
    print("\n── 4. dim_insumo ───────────────────────────────────────────────────")

    auxiliar_insumo = pd.read_excel(
        reference_dir / 'auxiliar_grupos_insumos.xlsx', skiprows=4
    )[['Referência', 'Descrição', 'Tipo']].dropna(subset=['Tipo'])
    auxiliar_insumo['Referência'] = pd.to_numeric(auxiliar_insumo['Referência'], errors='coerce')

    dim_insumo = criar_dimensao(
        df_painel.drop_duplicates(subset=['cod_insumo', 'detalhe']),
        ['cod_insumo', 'descricao_do_insumo', 'cod_grupo_de_insumo',
         'grupo_de_insumo', 'detalhe', 'marca'],
        'id_insumo'
    )

    dim_insumo['id_grupo'] = dim_insumo['cod_grupo_de_insumo'].apply(cod_grupo_to_id)
    dim_insumo['id_grupo_auxiliar'] = dim_insumo['id_grupo'].apply(
        lambda x: int(str(x)[0]) if pd.notna(x) else None
    )
    dim_insumo = dim_insumo.merge(
        auxiliar_insumo[['Referência', 'Tipo']].rename(
            columns={'Referência': 'ref_grupo', 'Tipo': 'tipo_grupo'}
        ),
        left_on='id_grupo_auxiliar', right_on='ref_grupo', how='left'
    ).drop(columns=['ref_grupo', 'id_grupo_auxiliar'], errors='ignore')

    print(f"  dim_insumo: {dim_insumo.shape}")

    # ── 5. dim_grupo_insumo ───────────────────────────────────────────────────
    print("\n── 5. dim_grupo_insumo ─────────────────────────────────────────────")

    dim_grupo_insumo = pd.read_excel('../transform/input/reference/auxiliar_grupo_insumo_luis.xlsx')  # colocar a planilha enviada pelo luis

    dim_grupo_insumo = (
        dim_grupo_insumo
        .drop_duplicates(subset='id_grupo')
        .dropna(subset=['id_grupo'])
        .sort_values('id_grupo')
        .reset_index(drop=True)
        .copy()
    )

    print(f"  dim_grupo_insumo: {dim_grupo_insumo.shape}")

    # ── 6. dim_lead_times (virou uma dim analista) ─────────────────────────────────────────────────────
    print("\n── 6. dim_lead_times ───────────────────────────────────────────────")

    dim_lead_times = dim_grupo_insumo[['id_grupo', 'cod_grupo_de_insumo',
                                       'Analista obras privadas', 'Analista Públicas', 'Analista Filial Sul']]

    dim_lead_times = dim_lead_times.melt(
        id_vars=[
            'id_grupo',
            'cod_grupo_de_insumo'
        ],
        value_vars=[
            'Analista obras privadas',
            'Analista Públicas',
            'Analista Filial Sul'
        ],
        var_name='tipo_analista',
        value_name='analista'
    )

    dim_lead_times.insert(0, 'id_lead_time', range(1, len(dim_lead_times) + 1))

    dim_lead_times = dim_lead_times[[
        'id_lead_time', 'id_grupo', 'cod_grupo_de_insumo',
        'tipo_analista', 'analista'
    ]]

    print(f"  dim_lead_times: {dim_lead_times.shape}")

    # ── 7. Demais dimensões ───────────────────────────────────────────────────
    print("\n── 7. Demais dimensões ─────────────────────────────────────────────")

    dim_fornecedor = criar_dimensao(df_painel, ['cod_fornecedor', 'fornecedor'], 'id_fornecedor')
    dim_comprador = criar_dimensao(df_painel, ['comprador'], 'id_comprador')
    dim_solicitante = criar_dimensao(df_painel, ['solicitante'], 'id_solicitante')

    print(f"  dim_fornecedor:  {dim_fornecedor.shape}")
    print(f"  dim_comprador:   {dim_comprador.shape}")
    print(f"  dim_solicitante: {dim_solicitante.shape}")

    # ── 8. Surrogate keys no fato ─────────────────────────────────────────────
    print("\n── 8. Surrogate keys ───────────────────────────────────────────────")

    _grp_lt = dim_grupo_insumo[['id_grupo', 'Lead Time', 'Curva ABC']].rename(
        columns={'Lead Time': 'lead_time_dias', 'Curva ABC': 'curva_abc'}
    )

    df_painel = (
        df_painel
        .merge(dim_obra[['cod_obra', 'id_obra', 'tipo_obra', 'estado']],
               on='cod_obra', how='left')
        .merge(dim_insumo[['cod_insumo', 'detalhe', 'id_insumo', 'id_grupo']],
               on=['cod_insumo', 'detalhe'], how='left')
        .merge(dim_fornecedor[['cod_fornecedor', 'id_fornecedor']],
               on='cod_fornecedor', how='left')
        .merge(dim_comprador[['comprador', 'id_comprador']],
               on='comprador', how='left')
        .merge(dim_solicitante[['solicitante', 'id_solicitante']],
               on='solicitante', how='left')
        .merge(_grp_lt, on='id_grupo', how='left')
    )

    # ── 9. Fato principal ─────────────────────────────────────────────────────
    print("\n── 9. fato_solicitacao_item ────────────────────────────────────────")

    fato_solicitacao_item = df_painel[[
        'id_item', 'no_da_solicitacao', 'nn_do_pedido', 'nn_da_nota_fiscal',
        'id_obra', 'id_insumo', 'id_grupo',
        'id_fornecedor', 'id_comprador', 'id_solicitante',
        'situacao_da_solicitacao', 'situacao_autorizacao_do_item',
        'situacao_do_pedido', 'situacao_autorizacao_do_pedido', 'situacao_pagamento',
        'quantidade_solicitada', 'quantidade_entregue', 'saldo', 'valor_da_nota',
        'data_da_solicitacao', 'data_autorizacao_da_solicitacao',
        'data_para_chegada_a_obra', 'data_do_pedido', 'previsao_de_entrega',
        'data_autorizacao_do_pedido', 'data_da_nota_fiscal', 'data_entrega_na_obra', 'lead_time_dias',
        'curva_abc'
    ]].copy()

    fato_solicitacao_item['valor_da_nota'] = converter_valor_br(
        fato_solicitacao_item['valor_da_nota']
    )
    fato_solicitacao_item['dias_solicitacao_ate_pedido'] = (
            fato_solicitacao_item['data_do_pedido'] -
            fato_solicitacao_item['data_da_solicitacao']
    ).dt.days
    fato_solicitacao_item['dias_atraso_entrega'] = (
            fato_solicitacao_item['data_entrega_na_obra'] -
            fato_solicitacao_item['previsao_de_entrega']
    ).dt.days

    fato_solicitacao_item['sla_atendido'] = np.where(
        fato_solicitacao_item['nn_do_pedido'].notna()
        & fato_solicitacao_item['dias_solicitacao_ate_pedido'].notna()
        & fato_solicitacao_item['lead_time_dias'].notna(),
        fato_solicitacao_item['dias_solicitacao_ate_pedido']
        <= fato_solicitacao_item['lead_time_dias'],
        None  # sem pedido ou sem lead_time: indeterminado
    )

    _val_max = fato_solicitacao_item['valor_da_nota'].max()
    print(f"  fato_solicitacao_item: {fato_solicitacao_item.shape}")
    print(f"  valor_da_nota max: R$ {_val_max:,.2f}")
    if _val_max < 100_000:
        print("  ATENÇÃO: valor máximo parece baixo — verificar conversão.")

    # ── 10. Validação ─────────────────────────────────────────────────────────
    print("\n── 10. Validação ───────────────────────────────────────────────────")

    checar_integridade(fato_solicitacao_item, 'id_obra', dim_obra, 'id_obra', 'fato → dim_obra')
    checar_integridade(fato_solicitacao_item, 'id_insumo', dim_insumo, 'id_insumo', 'fato → dim_insumo')
    checar_integridade(fato_solicitacao_item, 'id_fornecedor', dim_fornecedor, 'id_fornecedor', 'fato → dim_fornecedor')
    checar_integridade(fato_solicitacao_item, 'id_comprador', dim_comprador, 'id_comprador', 'fato → dim_comprador')
    checar_integridade(fato_solicitacao_item, 'id_solicitante', dim_solicitante, 'id_solicitante',
                       'fato → dim_solicitante')
    checar_integridade(fato_solicitacao_item, 'id_grupo', dim_grupo_insumo, 'id_grupo',
                       'fato_solicitacao_item → dim_grupo_insumo')
    checar_integridade(dim_lead_times, 'id_grupo', dim_grupo_insumo, 'id_grupo', 'dim_lead_times → dim_grupo_insumo')

    # ── 11. Exportação ────────────────────────────────────────────────────────
    print("\n── 11. Exportação ──────────────────────────────────────────────────")

    tabelas_dw = {
        'dim_obra': dim_obra,
        'dim_grupo_insumo': dim_grupo_insumo,
        'dim_insumo': dim_insumo,
        'dim_lead_times': dim_lead_times,
        'dim_fornecedor': dim_fornecedor,
        'dim_comprador': dim_comprador,
        'dim_solicitante': dim_solicitante,
        'fato_solicitacao_item': fato_solicitacao_item,
    }

    for nome, tabela in tabelas_dw.items():
        salvar_tabela(tabela, nome, output_dir)

    print("\n── Resumo ──────────────────────────────────────────────────────────")
    for nome, tabela in tabelas_dw.items():
        print(f"  {nome:<30} {str(tabela.shape):>15}")


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    executar()
