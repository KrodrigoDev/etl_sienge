"""
stages/transform/transform_servico.py
---------------------------------------
Transforma o CSV bruto de Solicitações de Serviço do SIENGE em:

  fato_servico        — 1 linha por solicitação de serviço
  dim_departamento    — nova dimensão (departamentos de engenharia)
  dim_autorizador     — nova dimensão (quem autoriza as solicitações)
  dim_obra            — expandida com obras que só existem em serviços
  dim_solicitante     — expandida com solicitantes novos

Dependência de execução
-----------------------
Deve rodar APÓS transform_painel_compras (precisa de dim_obra e dim_solicitante).

Relacionamentos (todos 1:N, single direction)
----------------------------------------------
  dim_obra[id_obra]                   → fato_servico[id_obra]
  dim_solicitante[id_solicitante]     → fato_servico[id_solicitante]
  dim_autorizador[id_autorizador]     → fato_servico[id_autorizador]
  dim_departamento[id_departamento]   → fato_servico[id_departamento]

Chaves de join
--------------
  dim_solicitante: col_pk = 'solicitante' (nome, ex: "VICTOR CORREA")
  dim_autorizador: col_pk = 'cod_autorizador' (código, ex: "MATHEUS")
  dim_departamento: col_pk = 'cod_departamento' (inteiro)
  dim_obra: col_pk = 'cod_obra' (inteiro extraído do campo Obras)
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from utils.normalizer import (
    checar_integridade,
    criar_dimensao,
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
    print("\n── 1. Leitura (serviços) ───────────────────────────────────────────")

    df = ler_dados((input_dir / 'servico').glob('*.csv'))
    df = normalizar_colunas(df)
    df = df.dropna(subset=['solicitacao'])

    print(f"  Total de linhas: {len(df):,}")

    # ── 2. Conversão de tipos ─────────────────────────────────────────────────
    print("\n── 2. Conversão de tipos ───────────────────────────────────────────")

    for col in ['data_do_cadastro', 'data', 'data_de_alteracao',
                'data_de_inicio', 'data_de_termino']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

    # Extrai cod_obra do campo "Obras" ("92 - RESIDENCIAL JARDIM..." → 92)
    df['cod_obra'] = (
        df['obras']
        .str.extract(r'^(\d+)')[0]
        .pipe(pd.to_numeric, errors='coerce')
    )

    # cod_departamento como inteiro
    df['cod_departamento'] = pd.to_numeric(df['cod_departamento'], errors='coerce')

    # Duração em dias (data_de_termino - data_de_inicio)
    df['duracao_dias'] = (df['data_de_termino'] - df['data_de_inicio']).dt.days

    # Limpeza do campo observação (remove \r\n)
    df['observacao'] = df['observacao'].astype(str).str.replace(r'\r\n', ' ', regex=True).str.strip()

    print(f"  Solicitações únicas:  {df['solicitacao'].nunique():,}")
    print(f"  Obras únicas:         {df['cod_obra'].nunique():,}")
    print(f"  Solicitantes únicos:  {df['nome_solicitante'].nunique():,}")
    print(f"  Autorizadores únicos: {df['nome_autorizador'].nunique():,}")
    print(f"  Departamentos únicos: {df['nome_departamento'].nunique():,}")
    print(f"\n  Situação:")
    print(df['situacao'].value_counts().to_string())
    print(f"\n  Autorização:")
    print(df['situacao_autorizacao'].value_counts().to_string())

    # ── 3. Carregar dimensões existentes ──────────────────────────────────────
    print("\n── 3. Carregando dimensões existentes ──────────────────────────────")

    dim_obra = pd.read_csv(output_dir / 'dim_obra.csv', sep=';')
    dim_solicitante = pd.read_csv(output_dir / 'dim_solicitante.csv', sep=';')

    print(f"  dim_obra:        {dim_obra.shape}")
    print(f"  dim_solicitante: {dim_solicitante.shape}")

    # ── 4. Expandir dim_obra com obras novas ──────────────────────────────────
    print("\n── 4. Expandindo dimensões existentes ──────────────────────────────")

    df_obras_novo = df[['cod_obra', 'obras']].rename(columns={'obras': 'obra'}).copy()

    # Normaliza o nome da obra para ficar igual ao padrão da dim
    # ("92 - RESIDENCIAL JARDIM SAO GONÇALO - OBRA" → mantém o nome original)


    dim_obra = expandir_dimensao(
        dim_existente=dim_obra,
        df_novo=df_obras_novo,
        colunas_naturais=['cod_obra', 'obra'],
        nome_id='id_obra',
        col_pk_natural='cod_obra',
    )

    # ── 5. Expandir dim_solicitante com solicitantes novos ────────────────────
    # A dim_solicitante usa 'nome_solicitante' como chave natural
    # (ex: "VICTOR CORREA") — não o código ("VICTORCORREA")
    df_sol_novo = df[['nome_solicitante']].rename(
        columns={'nome_solicitante': 'solicitante'}
    ).copy()

    dim_solicitante = expandir_dimensao(
        dim_existente=dim_solicitante,
        df_novo=df_sol_novo,
        colunas_naturais=['solicitante'],
        nome_id='id_solicitante',
        col_pk_natural='solicitante',
    )

    # ── 6. dim_departamento (nova) ────────────────────────────────────────────
    print("\n── 6. dim_departamento (nova) ──────────────────────────────────────")

    dim_departamento = (
        df[['cod_departamento', 'nome_departamento']]
        .drop_duplicates(subset='cod_departamento')
        .dropna(subset=['cod_departamento'])
        .sort_values('cod_departamento')
        .reset_index(drop=True)
        .copy()
    )
    dim_departamento.insert(0, 'id_departamento', dim_departamento.index + 1)
    print(f"  dim_departamento: {dim_departamento.shape}")
    print(dim_departamento[['id_departamento', 'cod_departamento', 'nome_departamento']].to_string(index=False))

    # ── 7. dim_autorizador (nova) ─────────────────────────────────────────────
    print("\n── 7. dim_autorizador (nova) ───────────────────────────────────────")

    dim_autorizador = (
        df[['cod_autorizador', 'nome_autorizador']]
        .drop_duplicates(subset='cod_autorizador')
        .dropna(subset=['cod_autorizador'])
        .sort_values('nome_autorizador')
        .reset_index(drop=True)
        .copy()
    )
    dim_autorizador.insert(0, 'id_autorizador', dim_autorizador.index + 1)
    print(f"  dim_autorizador: {dim_autorizador.shape}")

    # ── 8. Surrogate keys no fato ─────────────────────────────────────────────
    print("\n── 8. Surrogate keys ───────────────────────────────────────────────")

    _obra_map = dim_obra.set_index('cod_obra')['id_obra'].to_dict()
    _sol_map = dim_solicitante.set_index('solicitante')['id_solicitante'].to_dict()
    _dep_map = dim_departamento.set_index('cod_departamento')['id_departamento'].to_dict()
    _aut_map = dim_autorizador.set_index('cod_autorizador')['id_autorizador'].to_dict()

    df['id_obra'] = df['cod_obra'].map(_obra_map)
    df['id_solicitante'] = df['nome_solicitante'].map(_sol_map)
    df['id_departamento'] = df['cod_departamento'].map(_dep_map)
    df['id_autorizador'] = df['cod_autorizador'].map(_aut_map)

    for fk, total in [
        ('id_obra', len(df)),
        ('id_solicitante', len(df)),
        ('id_departamento', len(df)),
        ('id_autorizador', len(df)),
    ]:
        matched = df[fk].notna().sum()
        print(f"  {fk:<20}: {matched:,} de {total:,} ({matched / total:.1%})")

    # ── 9. fato_servico ───────────────────────────────────────────────────────
    print("\n── 9. fato_servico ─────────────────────────────────────────────────")

    fato_servico = df[[
        # chave natural
        'solicitacao',

        # FKs
        'id_obra',
        'id_solicitante',
        'id_departamento',
        'id_autorizador',

        # status
        'situacao',
        'situacao_autorizacao',
        'consistencia',

        # datas
        'data_do_cadastro',
        'data',
        'data_de_alteracao',
        'data_de_inicio',
        'data_de_termino',

        # métricas derivadas
        'duracao_dias',

        # texto livre
        'observacao',
    ]].copy()

    fato_servico['data_carga'] = date.today().isoformat()

    print(f"  fato_servico: {fato_servico.shape}")
    print(f"\n  Situação:")
    print(fato_servico['situacao'].value_counts().to_string())

    # ── 10. Validação ─────────────────────────────────────────────────────────
    print("\n── 10. Validação ───────────────────────────────────────────────────")

    checar_integridade(fato_servico, 'id_obra', dim_obra, 'id_obra', 'fato_servico → dim_obra')
    checar_integridade(fato_servico, 'id_solicitante', dim_solicitante, 'id_solicitante',
                       'fato_servico → dim_solicitante')
    checar_integridade(fato_servico, 'id_departamento', dim_departamento, 'id_departamento',
                       'fato_servico → dim_departamento')
    checar_integridade(fato_servico, 'id_autorizador', dim_autorizador, 'id_autorizador',
                       'fato_servico → dim_autorizador')

    # ── 11. Exportação ────────────────────────────────────────────────────────
    print("\n── 11. Exportação ──────────────────────────────────────────────────")

    salvar_tabela(dim_obra, 'dim_obra', output_dir)  # expandida
    salvar_tabela(dim_solicitante, 'dim_solicitante', output_dir)  # expandida
    salvar_tabela(dim_departamento, 'dim_departamento', output_dir)  # nova
    salvar_tabela(dim_autorizador, 'dim_autorizador', output_dir)  # nova
    salvar_tabela(fato_servico, 'fato_servico', output_dir)

    print("\n── Resumo ──────────────────────────────────────────────────────────")
    for nome, tabela in {
        'dim_obra (expandida)': dim_obra,
        'dim_solicitante (expandida)': dim_solicitante,
        'dim_departamento (nova)': dim_departamento,
        'dim_autorizador (nova)': dim_autorizador,
        'fato_servico': fato_servico,
    }.items():
        print(f"  {nome:<35} {str(tabela.shape):>12}")

    print("""
── Relacionamentos Power BI — fato_servico (todos 1:N, single direction) ────
  dim_obra[id_obra]                 → fato_servico[id_obra]
  dim_solicitante[id_solicitante]   → fato_servico[id_solicitante]
  dim_departamento[id_departamento] → fato_servico[id_departamento]
  dim_autorizador[id_autorizador]   → fato_servico[id_autorizador]

── Dimensões compartilhadas ─────────────────────────────────────────────────
  dim_obra       → fato_solicitacao_item, fato_estoque, fato_contrato, fato_servico
  dim_solicitante→ fato_solicitacao_item, fato_servico
""")


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    executar()
