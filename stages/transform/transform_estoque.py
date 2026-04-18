"""
stages/transform/transform_estoque.py
---------------------------------------
Transforma o CSV bruto de estoque de obras do SIENGE em:

  fato_estoque         — posição atual do estoque por item/obra
  dim_obra             — expandida com obras que só existem no estoque
  dim_insumo           — expandida com insumos que só existem no estoque
  dim_grupo_insumo     — expandida com grupos novos do estoque

Dependência de execução
-----------------------
Este módulo DEVE ser chamado APÓS transform_painel_compras, pois:
  - Lê dim_obra, dim_insumo e dim_grupo_insumo já geradas pelo painel
  - Expande essas dimensões com registros novos do estoque
  - Sobrescreve os CSVs das dimensões no OUTPUT_DIR

Relacionamentos gerados (todos 1:N, single direction)
------------------------------------------------------
  dim_obra[id_obra]          → fato_estoque[id_obra]
  dim_insumo[id_insumo]      → fato_estoque[id_insumo]
  dim_grupo_insumo[id_grupo] → fato_estoque[id_grupo]

  dim_insumo[id_insumo]      → fato_solicitacao_item[id_insumo]  ← compartilhada
  dim_grupo_insumo[id_grupo] → fato_solicitacao_item[id_grupo]    ← compartilhada
  dim_obra[id_obra]          → fato_solicitacao_item[id_obra]     ← compartilhada
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from stages.transform.utils.normalizer import (
    checar_integridade,
    cod_grupo_to_id,
    converter_quantidade_br,
    converter_valor_br,
    expandir_dimensao,
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


def executar(input_dir: Path = INPUT_DIR, output_dir: Path = OUTPUT_DIR) -> None:
    """
    Ponto de entrada do transform de estoque.
    Pode ser chamado diretamente ou pelo main.py.
    """

    # ── 1. Leitura ────────────────────────────────────────────────────────────
    print("\n── 1. Leitura (estoque) ────────────────────────────────────────────")

    df_est = ler_dados((input_dir / 'estoque').glob('*.csv'))
    df_est = normalizar_colunas(df_est)

    # Remove linha de totais (última linha com Código Obra nulo)
    df_est = df_est.dropna(subset=['codigo_obra'])

    print(f"  Total de linhas: {len(df_est):,}")

    # ── 2. Conversão de tipos ─────────────────────────────────────────────────
    print("\n── 2. Conversão de tipos ───────────────────────────────────────────")

    COLUNAS_QTD = [
        'quantidade_insumo',
        'quantidade_reservada',
        'quantidade_apropriada',
        'estoque_minimo',
        'estoque_maximo',
    ]
    for col in COLUNAS_QTD:
        if col in df_est.columns:
            df_est[col] = converter_quantidade_br(df_est[col])

    COLUNAS_VALOR = ['custo_medio', 'custo_total']
    for col in COLUNAS_VALOR:
        if col in df_est.columns:
            df_est[col] = converter_valor_br(df_est[col])

    df_est['detalhe'] = df_est['detalhe'].fillna('0')
    df_est['codigo_do_detalhe'] = df_est['codigo_do_detalhe'].fillna(0).astype(int)

    # id_grupo a partir da Família ("2.034" → 2034)
    df_est['id_grupo'] = df_est['familia'].apply(cod_grupo_to_id)

    print(f"  custo_total — max: R$ {df_est['custo_total'].max():,.2f} "
          f"| total: R$ {df_est['custo_total'].sum():,.2f}")

    # ── 3. Carregar dimensões existentes (geradas pelo painel de compras) ─────
    print("\n── 3. Carregando dimensões existentes ──────────────────────────────")

    dim_obra = pd.read_csv(output_dir / 'dim_obra.csv', sep=';')
    dim_insumo = pd.read_csv(output_dir / 'dim_insumo.csv', sep=';')
    dim_grupo_insumo = pd.read_csv(output_dir / 'dim_grupo_insumo.csv', sep=';')

    print(f"  dim_obra:         {dim_obra.shape}")
    print(f"  dim_insumo:       {dim_insumo.shape}")
    print(f"  dim_grupo_insumo: {dim_grupo_insumo.shape}")

    # ── 4. Expandir dimensões com registros do estoque ────────────────────────
    print("\n── 4. Expandindo dimensões ─────────────────────────────────────────")
    # dim_obra: obras que só existem no estoque
    dim_obra = expandir_dimensao(
        dim_existente=dim_obra,
        df_novo=df_est.rename(columns={
            'codigo_obra': 'cod_obra',
            'obra': 'obra',
        }),
        colunas_naturais=['cod_obra', 'obra'],
        nome_id='id_obra',
        col_pk_natural='cod_obra',
    )

    # Garantindo que os insumos do estoque tenham seus tipos de grupo preenchidos

    auxiliar_insumo = pd.read_excel(
        REFERENCE_DIR / 'auxiliar_grupos_insumos.xlsx', skiprows=4
    )[['Referência', 'Descrição', 'Tipo']].dropna(subset=['Tipo'])

    auxiliar_insumo['Referência'] = pd.to_numeric(auxiliar_insumo['Referência'], errors='coerce')

    df_est['id_grupo_auxiliar'] = df_est['familia'].apply(cod_grupo_to_id)
    df_est['id_grupo_auxiliar'] = df_est['id_grupo_auxiliar'].apply(
        lambda x: int(str(x)[0]) if pd.notna(x) else None
    )

    df_est = df_est.merge(
        auxiliar_insumo[['Referência', 'Tipo']].rename(
            columns={'Referência': 'ref_grupo', 'Tipo': 'tipo_grupo'}
        ),
        left_on='id_grupo_auxiliar', right_on='ref_grupo', how='left'
    ).drop(columns=['ref_grupo', 'id_grupo_auxiliar'], errors='ignore')

    # dim_grupo_insumo: grupos que só existem no estoque
    # O estoque usa 'familia' (ex: 2.034) e 'grupo_de_insumo' (ex: FERRAGENS)
    # Normalizamos para o mesmo padrão da dim existente

    df_est_grp = df_est[['id_grupo', 'familia', 'grupo_de_insumo', 'tipo_grupo']].copy()
    df_est_grp = df_est_grp.rename(columns={
        'familia': 'cod_grupo_de_insumo',
        'grupo_de_insumo': 'grupo_de_insumo',
    })

    df_est_grp['cod_grupo_de_insumo'] = df_est_grp['cod_grupo_de_insumo'].astype(str)

    dim_grupo_insumo = expandir_dimensao(
        dim_existente=dim_grupo_insumo,
        df_novo=df_est_grp,
        colunas_naturais=['id_grupo', 'cod_grupo_de_insumo', 'grupo_de_insumo', 'tipo_grupo'],
        nome_id='id_grupo',
        col_pk_natural='id_grupo',
    )

    # dim_insumo: insumos que só existem no estoque
    # O estoque não tem 'marca' separada (cod_marca / descricao_marca)
    # Normalizamos para o padrão da dim_insumo existente
    df_est_ins = df_est.rename(columns={
        'codigo_do_insumo': 'cod_insumo',
        'insumo': 'descricao_do_insumo',
        'detalhe': 'detalhe',
        'marca': 'marca',
        'familia': 'cod_grupo_de_insumo'
    }).copy()

    df_est_ins['cod_insumo'] = (
        df_est_ins['cod_insumo']
        .fillna(0)
        .astype(int)
    )

    dim_insumo = expandir_dimensao(
        dim_existente=dim_insumo,
        df_novo=df_est_ins,
        colunas_naturais=['cod_insumo', 'descricao_do_insumo',
                          'cod_grupo_de_insumo', 'grupo_de_insumo',
                          'detalhe', 'marca', 'tipo_grupo'],
        nome_id='id_insumo',
        col_pk_natural='cod_insumo',
    )

    # ── 5. Mapear surrogate keys no fato ─────────────────────────────────────
    print("\n── 5. Surrogate keys ───────────────────────────────────────────────")

    # Mapas de lookup
    _obra_map = dim_obra.set_index('cod_obra')['id_obra'].to_dict()
    _insumo_map = (
        dim_insumo
        .drop_duplicates(subset='cod_insumo')
        .set_index('cod_insumo')['id_insumo']
        .to_dict()
    )

    df_est['id_obra'] = df_est['codigo_obra'].map(_obra_map)
    df_est['id_insumo'] = df_est['codigo_do_insumo'].map(_insumo_map)

    _matched_obra = df_est['id_obra'].notna().sum()
    _matched_insumo = df_est['id_insumo'].notna().sum()
    print(f"  id_obra:   {_matched_obra:,} de {len(df_est):,} ({_matched_obra / len(df_est):.1%})")
    print(f"  id_insumo: {_matched_insumo:,} de {len(df_est):,} ({_matched_insumo / len(df_est):.1%})")

    # ── 6. Fato estoque ───────────────────────────────────────────────────────
    print("\n── 6. fato_estoque ─────────────────────────────────────────────────")

    fato_estoque = df_est[[
        # surrogate keys
        'id_obra',
        'codigo_obra',
        'id_insumo',
        'codigo_do_insumo',
        'id_grupo',

        # situação
        'situacao',

        # métricas
        'quantidade_insumo',
        'quantidade_reservada',
        'quantidade_apropriada',
        'custo_medio',
        'custo_total',
        'estoque_minimo',
        'estoque_maximo',

        # dimensão degenerada (unidade construtiva — não vira dim separada)
        'unidade_construtiva',
        'item',

        # unidade de medida
        'unidade_de_medida',
    ]].copy()

    # Data de carga — permite rastrear evolução do estoque ao longo do tempo
    fato_estoque['data_carga'] = date.today().isoformat()

    print(f"  fato_estoque: {fato_estoque.shape}")
    print(f"  Ativos:       {(fato_estoque['situacao'] == 'ATIVO').sum():,}")
    print(f"  Com saldo:    {(fato_estoque['quantidade_insumo'] > 0).sum():,}")
    print(f"  Valor total:  R$ {fato_estoque['custo_total'].sum():,.2f}")

    # ── 7. Validação ──────────────────────────────────────────────────────────
    print("\n── 7. Validação ────────────────────────────────────────────────────")

    checar_integridade(fato_estoque, 'id_obra', dim_obra, 'id_obra', 'fato_estoque → dim_obra')
    checar_integridade(fato_estoque, 'id_insumo', dim_insumo, 'id_insumo', 'fato_estoque → dim_insumo')
    checar_integridade(fato_estoque, 'id_grupo', dim_grupo_insumo, 'id_grupo', 'fato_estoque → dim_grupo_insumo')

    # ── 8. Exportação ─────────────────────────────────────────────────────────
    print("\n── 8. Exportação ───────────────────────────────────────────────────")

    # Dimensões expandidas — sobrescreve os CSVs gerados pelo painel de compras
    dim_grupo_insumo.drop_duplicates(subset='cod_grupo_de_insumo', inplace=True)

    salvar_tabela(dim_obra, 'dim_obra', output_dir)
    salvar_tabela(dim_insumo, 'dim_insumo', output_dir)
    salvar_tabela(dim_grupo_insumo, 'dim_grupo_insumo', output_dir)

    # Novo fato
    salvar_tabela(fato_estoque, 'fato_estoque', output_dir)

    print("\n── Resumo ──────────────────────────────────────────────────────────")
    for nome, tabela in {
        'dim_obra (expandida)': dim_obra,
        'dim_insumo (expandida)': dim_insumo,
        'dim_grupo_insumo (expandida)': dim_grupo_insumo,
        'fato_estoque': fato_estoque,
    }.items():
        print(f"  {nome:<35} {str(tabela.shape):>12}")

    print("""
── Relacionamentos Power BI — fato_estoque (todos 1:N, single direction) ───
  dim_obra[id_obra]          → fato_estoque[id_obra]
  dim_insumo[id_insumo]      → fato_estoque[id_insumo]
  dim_grupo_insumo[id_grupo] → fato_estoque[id_grupo]

── Dimensões compartilhadas com fato_solicitacao_item ───────────────────────
  dim_obra e dim_insumo são as mesmas — o Power BI filtra os dois fatos
  simultaneamente quando o usuário seleciona uma obra ou insumo.
""")


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    executar()
