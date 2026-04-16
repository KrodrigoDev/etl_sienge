"""
stages/transform/transform_contratos.py
-----------------------------------------
Transforma o CSV bruto de contratos do SIENGE em:

  dim_contrato   — cadastro de contratos (1 linha por contrato)
  fato_contrato  — métricas financeiras e de status por contrato

Dimensões REUTILIZADAS (geradas pelo transform_painel_compras):
  dim_obra[id_obra]            → fato_contrato[id_obra]
  dim_fornecedor[id_fornecedor]→ fato_contrato[id_fornecedor]

Dependência de execução
-----------------------
Deve rodar APÓS transform_painel_compras (precisa de dim_obra e dim_fornecedor).
Não é necessário rodar após transform_estoque.

Relacionamentos gerados (todos 1:N, single direction)
------------------------------------------------------
  dim_contrato[id_contrato]     → fato_contrato[id_contrato]
  dim_obra[id_obra]             → fato_contrato[id_obra]
  dim_fornecedor[id_fornecedor] → fato_contrato[id_fornecedor]
  dim_empresa[id_empresa] → fato_contrato[id_empresa]
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from utils.normalizer import (
    checar_integridade,
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
OUTPUT_DIR = pasta_origem / 'stages' / 'transform' / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def executar(input_dir: Path = INPUT_DIR, output_dir: Path = OUTPUT_DIR) -> None:
    # ── 1. Leitura ────────────────────────────────────────────────────────────
    print("\n── 1. Leitura (contratos) ──────────────────────────────────────────")

    df = ler_dados((input_dir / 'contratos').glob('*.csv'))
    df = normalizar_colunas(df)

    # Remove linha de totais
    df = df.dropna(subset=['contrato'])
    print(f"  Total de linhas: {len(df):,}")

    # ── 2. Limpeza e conversão de tipos ──────────────────────────────────────
    print("\n── 2. Conversão de tipos ───────────────────────────────────────────")

    COLUNAS_DATA = [
        'data_do_contrato', 'data_de_inicio', 'data_de_termino',
        'data_da_alteracao_de_situacao',
    ]
    for col in COLUNAS_DATA:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

    COLUNAS_VALOR = [
        'total', 'total_medido', 'saldo', 'total_mao_de_obra',
        'total_material', 'total_em_aberto', 'total_fat_direto',
    ]
    for col in COLUNAS_VALOR:
        if col in df.columns:
            df[col] = converter_valor_br(df[col])

    # Extrai cod_obra do campo "Obras" ("137 - RESERVA DA PRATA - OBRA" → 137)
    df['cod_obra'] = (
        df['obras']
        .str.extract(r'^(\d+)')[0]
        .pipe(pd.to_numeric, errors='coerce')
    )

    # Duração em dias
    df['duracao_dias'] = (df['data_de_termino'] - df['data_de_inicio']).dt.days

    print(f"  Contratos únicos:   {df['contrato'].nunique():,}")
    print(f"  Obras únicas:       {df['cod_obra'].nunique():,}")
    print(f"  Fornecedores únicos:{df['cod_fornecedor'].nunique():,}")
    print(f"  Valor total:        R$ {df['total'].sum():,.2f}")
    print(f"  Total medido:       R$ {df['total_medido'].sum():,.2f}")
    print(f"  Saldo em aberto:    R$ {df['total_em_aberto'].sum():,.2f}")

    # ── 3. Carregar dimensões existentes ──────────────────────────────────────
    print("\n── 3. Carregando dimensões existentes ──────────────────────────────")

    dim_obra = pd.read_csv(output_dir / 'dim_obra.csv', sep=';')
    dim_fornecedor = pd.read_csv(output_dir / 'dim_fornecedor.csv', sep=';')

    print(f"  dim_obra:       {dim_obra.shape}")
    print(f"  dim_fornecedor: {dim_fornecedor.shape}")

    # ── 4. Expandir dim_fornecedor com fornecedores novos do contrato ─────────
    print("\n── 4. Expandindo dim_fornecedor ────────────────────────────────────")

    df_forn_novo = df[['cod_fornecedor', 'fornecedor*']].rename(
        columns={'fornecedor*': 'fornecedor'}
    ).copy()
    df_forn_novo['cod_fornecedor'] = pd.to_numeric(df_forn_novo['cod_fornecedor'], errors='coerce')

    dim_fornecedor = expandir_dimensao(
        dim_existente=dim_fornecedor,
        df_novo=df_forn_novo,
        colunas_naturais=['cod_fornecedor', 'fornecedor'],
        nome_id='id_fornecedor',
        col_pk_natural='cod_fornecedor',
    )

    # ── 5. dim_contrato ───────────────────────────────────────────────────────
    print("\n── 5. dim_contrato ─────────────────────────────────────────────────")

    print(df.columns)
    dim_contrato = (
        df[[
            'contrato',
            'objeto_do_contrato',
            'tipo_do_contrato',
            'situacao_de_assinatura',
            'cod_responsavel',
            'responsavel',
            'data_do_contrato',
            'data_de_inicio',
            'data_de_termino',
            'duracao_dias',
            'cpf/cnpj_fornecedor',
        ]]
        .drop_duplicates(subset='contrato')
        .reset_index(drop=True)
        .copy()
    )
    dim_contrato.insert(0, 'id_contrato', dim_contrato.index + 1)
    dim_contrato.rename(columns={'objeto_do_contrato': 'objeto'}, inplace=True)

    print(f"  dim_contrato: {dim_contrato.shape}")

    # ── 5.1. dim_empresa ───────────────────────────────────────────────────────
    dim_empresa = (
        df[['cod_empresa', 'empresa']]
        .drop_duplicates(subset='cod_empresa')
        .dropna(subset=['cod_empresa'])
        .sort_values('empresa')
        .reset_index(drop=True)
        .copy()
    )
    dim_empresa.insert(0, 'id_empresa', dim_empresa.index + 1)
    print(f"  dim_empresa: {dim_empresa.shape}")


    # ── 6. Surrogate keys no fato ─────────────────────────────────────────────
    print("\n── 6. Surrogate keys ───────────────────────────────────────────────")

    _contrato_map = dim_contrato.set_index('contrato')['id_contrato'].to_dict()
    _obra_map = dim_obra.set_index('cod_obra')['id_obra'].to_dict()
    _fornecedor_map = dim_fornecedor.set_index('cod_fornecedor')['id_fornecedor'].to_dict()
    _emp_map = dim_empresa.set_index('cod_empresa')['id_empresa'].to_dict()

    df['id_contrato'] = df['contrato'].map(_contrato_map)
    df['id_obra'] = df['cod_obra'].map(_obra_map)
    df['id_fornecedor'] = df['cod_fornecedor'].map(_fornecedor_map)
    df['id_empresa'] = df['cod_empresa'].map(_emp_map)

    for fk, nome in [('id_contrato', 'contrato'), ('id_obra', 'obra'), ('id_fornecedor', 'fornecedor'), ('id_empresa', 'empresa')]:
        matched = df[fk].notna().sum()
        print(f"  {fk}: {matched:,} de {len(df):,} ({matched / len(df):.1%})")

    # ── 7. fato_contrato ──────────────────────────────────────────────────────
    print("\n── 7. fato_contrato ────────────────────────────────────────────────")

    fato_contrato = df[[
        'id_contrato',
        'id_obra',
        'id_fornecedor',
        'id_empresa',

        # status
        'situacao_do_contrato',
        'situacao_de_autorizacao',

        # financeiro
        'total',
        'total_medido',
        'saldo',
        'total_em_aberto',
        'total_mao_de_obra',
        'total_material',
        'total_fat_direto',

        # rastreabilidade
        'data_da_alteracao_de_situacao',
        'descricao_da_alteracao_de_situacao',
        'cod_autorizado_por',
        'autorizado_por',
    ]].copy()

    fato_contrato['data_carga'] = date.today().isoformat()

    print(f"  fato_contrato: {fato_contrato.shape}")
    print(f"\n  Situações:")
    print(fato_contrato['situacao_do_contrato'].value_counts().to_string())
    print(f"\n  Autorização:")
    print(fato_contrato['situacao_de_autorizacao'].value_counts().to_string())

    # ── 8. Validação ──────────────────────────────────────────────────────────
    print("\n── 8. Validação ────────────────────────────────────────────────────")

    checar_integridade(fato_contrato, 'id_contrato', dim_contrato, 'id_contrato', 'fato_contrato → dim_contrato')
    checar_integridade(fato_contrato, 'id_obra', dim_obra, 'id_obra', 'fato_contrato → dim_obra')
    checar_integridade(fato_contrato, 'id_fornecedor', dim_fornecedor, 'id_fornecedor',
                       'fato_contrato → dim_fornecedor')
    checar_integridade(fato_contrato, 'id_empresa', dim_empresa, 'id_empresa',
                       'fato_contrato → dim_empresa')

    # ── 9. Exportação ─────────────────────────────────────────────────────────
    print("\n── 9. Exportação ───────────────────────────────────────────────────")

    salvar_tabela(dim_fornecedor, 'dim_fornecedor', output_dir)  # expandida
    salvar_tabela(dim_contrato, 'dim_contrato', output_dir)
    salvar_tabela(dim_empresa, 'dim_empresa', output_dir)  # nova
    salvar_tabela(fato_contrato, 'fato_contrato', output_dir)


    print("\n── Resumo ──────────────────────────────────────────────────────────")
    for nome, tabela in {
        'dim_fornecedor (expandida)': dim_fornecedor,
        'dim_contrato': dim_contrato,
        'fato_contrato': fato_contrato,
        'dim_empresa (nova)': dim_empresa,
    }.items():
        print(f"  {nome:<30} {str(tabela.shape):>12}")

    print("""
── Relacionamentos Power BI — fato_contrato (todos 1:N, single direction) ──
  dim_contrato[id_contrato]     → fato_contrato[id_contrato]
  dim_obra[id_obra]             → fato_contrato[id_obra]
  dim_fornecedor[id_fornecedor] → fato_contrato[id_fornecedor]
  dim_empresa[id_empresa] → fato_contrato[id_empresa]

── dim_fornecedor compartilhada ─────────────────────────────────────────────
  dim_fornecedor[id_fornecedor] → fato_solicitacao_item[id_fornecedor]
  dim_fornecedor[id_fornecedor] → fato_contrato[id_fornecedor]
  (mesmo fornecedor rastreado nos pedidos e nos contratos)
""")


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    executar()
