"""
transform/titulo.py
--------------------
Transforma o relatório de títulos do SIENGE em dois DataFrames normalizados:

    dim_titulo
        Atributos do título em si — únicos por titulo.
        Colunas: titulo | credor | documento | origem | ct_oc | dt_ct_oc |
                 emissao_nf | cadastro | vencto | qtd | valor_bruto |
                 imposto | descontos | valor_liquido

    dim_titulo_obra
        Associação N:M entre título e centro de custo.
        Colunas: titulo | empresa_cod | empresa | cod_obra | obra | tipo_obra

    Relacionamento esperado no DW:
        fato_consulta_parcela  }o--||  dim_titulo       (via titulo)
        dim_titulo_obra        }o--||  dim_titulo       (via titulo)
        dim_titulo_obra        }o--||  dim_centro_custo (via cod_obra)
"""

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


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _extrair_par(valor: str) -> tuple[str | None, str | None]:
    """
    '146 - RESIDENCIAL ROSA DA FONSECA I'
    → ('146', 'RESIDENCIAL ROSA DA FONSECA I')

    Usado tanto para Empresa quanto para Centro de custo.
    """
    valor = str(valor).strip()
    if ' - ' in valor:
        cod, nome = valor.split(' - ', 1)
        return cod.strip(), nome.strip()
    return None, valor


# ─────────────────────────────────────────────────────────────────────────────
# PARSING PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def _parse_df_bruto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe o DataFrame bruto (logo após ler_dados + normalizar_colunas) e
    devolve um DataFrame tabular com uma linha por item de título.

    Layout real do xlsx (confirmado por inspeção do arquivo):
        col[0]  → Item (número inteiro) OU marcador de seção
        col[1]  → Credor
        col[3]  → Valor do marcador (empresa / centro de custo)
        col[4]  → Documento
        col[5]  → Titulo
        col[7]  → Origem (CP, ME, GI, AC…)
        col[9]  → CT/OC
        col[10] → Dt.CT/OC
        col[12] → Emis.NF
        col[15] → Cadastro
        col[19] → Vencto
        col[21] → Qtd
        col[22] → Valor bruto
        col[24] → Imposto
        col[26] → Descontos
        col[27] → Valor líquido
    """

    registros: list[dict] = []

    empresa_cod: str | None = None
    empresa_nome: str | None = None
    obra_cod: str | None = None
    obra_nome: str | None = None

    cols = df.columns.tolist()

    def cv(row, pos: int) -> str:
        """Valor da coluna na posição pos, ou '' se ausente/nulo."""
        if pos < len(cols):
            v = row[cols[pos]]
            return '' if pd.isna(v) else str(v).strip()
        return ''

    # nome_arquivo é adicionado pelo ler_dados() quando lê múltiplos arquivos
    col_nome_arq = cols[-1] if 'nome_arquivo' in str(cols[-1]) else None

    for _, row in df.iterrows():
        c0_raw = cv(row, 0)
        c0 = c0_raw.lower()
        c3 = cv(row, 3)

        # ── Marcador de Empresa ───────────────────────────────────────────────
        if c0 == 'empresa':
            empresa_cod, empresa_nome = _extrair_par(c3)
            continue

        # ── Marcador de Centro de Custo ───────────────────────────────────────
        if c0 == 'centro de custo':
            obra_cod, obra_nome = _extrair_par(c3)
            continue

        # ── Linha de dado: col[0] é o número do item (inteiro) ───────────────
        # Linhas de cabeçalho, totais e espaçadores falham na conversão → ignorar
        try:
            item_num = int(float(c0_raw))
        except (ValueError, TypeError):
            continue

        nome_arq = str(row[col_nome_arq]).strip() if col_nome_arq else ''

        registros.append({
            # contexto hierárquico
            'empresa_cod': empresa_cod,
            'empresa': empresa_nome,
            'cod_obra': obra_cod,
            'obra': obra_nome,
            # atributos do título
            'item': item_num,
            'credor': cv(row, 1),
            'documento': cv(row, 4),
            'titulo': cv(row, 5),
            'origem': cv(row, 7),
            'ct_oc': cv(row, 9),
            'dt_ct_oc': cv(row, 10),
            'emissao_nf': cv(row, 12),
            'cadastro': cv(row, 15),
            'vencto': cv(row, 19),
            'qtd': cv(row, 21),
            'valor_bruto': cv(row, 22),
            'imposto': cv(row, 24),
            'descontos': cv(row, 26),
            'valor_liquido': cv(row, 27),
            'nome_arquivo': nome_arq,
        })

    return pd.DataFrame(registros)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTRUÇÃO DAS DIMENSÕES
# ─────────────────────────────────────────────────────────────────────────────

def _build_dim_titulo(df: pd.DataFrame) -> pd.DataFrame:
    """
    dim_titulo — uma linha por título, sem duplicatas.

    Atributos do documento em si, independentes da obra onde foi lançado.
    Como confirmado nos dados: credor e documento são sempre iguais para
    o mesmo título, mesmo quando ele aparece em múltiplas obras.
    """
    colunas = [
        'titulo', 'credor', 'documento', 'origem',
        'ct_oc', 'dt_ct_oc', 'emissao_nf', 'cadastro', 'vencto',
        'qtd', 'valor_bruto', 'imposto', 'descontos', 'valor_liquido',
    ]

    dim = (
        df[colunas]
        .drop_duplicates(subset=['titulo'])
        .reset_index(drop=True)
    )

    # Tipagem
    dim['titulo'] = pd.to_numeric(dim['titulo'], errors='coerce').astype('Int64')
    dim['qtd'] = pd.to_numeric(dim['qtd'], errors='coerce').astype('Int64')
    dim['valor_bruto'] = pd.to_numeric(dim['valor_bruto'], errors='coerce')
    dim['imposto'] = pd.to_numeric(dim['imposto'], errors='coerce')
    dim['descontos'] = pd.to_numeric(dim['descontos'], errors='coerce')
    dim['valor_liquido'] = pd.to_numeric(dim['valor_liquido'], errors='coerce')

    for col_dt in ('dt_ct_oc', 'emissao_nf', 'cadastro', 'vencto'):
        dim[col_dt] = pd.to_datetime(dim[col_dt], format='%d/%m/%Y', errors='coerce')

    return dim


def _build_dim_titulo_obra(
        df: pd.DataFrame,
        df_auxiliar: pd.DataFrame,
) -> pd.DataFrame:
    """
    dim_titulo_obra — resolve o N:M entre título e centro de custo.

    Chave composta: (titulo, cod_obra).
    Aqui ficam os atributos que pertencem à relação título↔obra,
    incluindo o tipo de obra vindo da tabela auxiliar.
    """
    colunas = ['titulo', 'empresa_cod', 'empresa', 'cod_obra', 'obra']

    dim = (
        df[colunas]
        .drop_duplicates(subset=['titulo', 'cod_obra'])
        .reset_index(drop=True)
    )

    # Tipagem
    dim['titulo'] = pd.to_numeric(dim['titulo'], errors='coerce').astype('Int64')
    dim['cod_obra'] = pd.to_numeric(dim['cod_obra'], errors='coerce').astype('Int64')
    dim['empresa_cod'] = pd.to_numeric(dim['empresa_cod'], errors='coerce').astype('Int64')

    # Enriquece com tipo_obra da tabela de referência
    dim = pd.merge(
        dim,
        df_auxiliar[['Cod. Centro de Custo', 'Tipo de Obra 2 ']].rename(columns={
            'Cod. Centro de Custo': 'cod_obra',
            'Tipo de Obra 2 ': 'tipo_obra',
        }),
        on='cod_obra',
        how='left', 
    )

    return dim


def _build_dim_titulo_obra_dedup(dim_titulo_obra: pd.DataFrame) -> pd.DataFrame:
    """
    dim_titulo_obra_dedup — uma linha por título, para uso no Power BI.

    Resolve o fanout que ocorre quando fato_consulta_parcela.titulo se relaciona
    com dim_titulo_obra, que pode ter N linhas por título (quando um título é
    rateado entre obras).

    Estratégia:
      - Títulos com tipo_obra IGUAL em todas as obras → pega a 1ª linha
        (qualquer obra serve — o tipo_obra é o mesmo)
      - Títulos com tipo_obra DIVERGENTE entre obras  → marca como 'MISTO'
        (registra o conflito em vez de silenciá-lo)

    No Power BI:
      fato.titulo → dim_titulo_obra_dedup.titulo  (relacionamento 1:N limpo)
      Títulos da fato sem match aqui → tipo_obra = NULL (esperado e honesto:
      são títulos de relatórios/períodos ainda não carregados no pipeline)
    """
    # Detectar títulos com tipo_obra inconsistente entre obras
    tipo_por_titulo = dim_titulo_obra.groupby('titulo')['tipo_obra'].nunique()
    titulos_misto = tipo_por_titulo[tipo_por_titulo > 1].index

    # Consistentes: 1ª ocorrência já carrega o tipo_obra correto
    parte_ok = (
        dim_titulo_obra[~dim_titulo_obra['titulo'].isin(titulos_misto)]
        .drop_duplicates(subset=['titulo'], keep='first')
    )

    # Divergentes: registrar tipo_obra = 'MISTO' para não esconder o problema
    parte_misto = (
        dim_titulo_obra[dim_titulo_obra['titulo'].isin(titulos_misto)]
        .drop_duplicates(subset=['titulo'], keep='first')
        .copy()
    )
    parte_misto['tipo_obra'] = 'MISTO'

    dedup = pd.concat([parte_ok, parte_misto], ignore_index=True)

    return dedup


# ─────────────────────────────────────────────────────────────────────────────
# PONTO DE ENTRADA
# ─────────────────────────────────────────────────────────────────────────────

def executar(
        input_dir: Path = INPUT_DIR,
        output_dir: Path = OUTPUT_DIR,
) -> None:
    # ── 1. Leitura ────────────────────────────────────────────────────────────
    print("\n── 1. Leitura (titulos) ──────────────────────────────────────────")

    arquivos = list((input_dir / 'titulo').glob('*.xlsx'))
    df_bruto = ler_dados(arquivos=arquivos, formato='excel', salto=0)

    # Remove linhas completamente vazias (Unnamed: 0 nulo = rodapé/espaçador)
    df_bruto.dropna(subset=['Unnamed: 0'], inplace=True)

    # Normaliza nomes de colunas para snake_case sem acentos
    df_bruto = normalizar_colunas(df_bruto)

    # ── 2. Parse hierárquico ──────────────────────────────────────────────────
    print("\n── 2. Parse hierárquico ──────────────────────────────────────────")
    df = _parse_df_bruto(df_bruto)
    print(f"  Linhas de títulos extraídas: {len(df)}")

    # ── 3. Tabela auxiliar ────────────────────────────────────────────────────
    print("\n── 3. Carregando auxiliar de referência ──────────────────────────")
    df_auxiliar = pd.read_csv(
        input_dir / 'reference' / 'auxiliar_gabriel.csv',
        sep=',',
    )

    # ── 4. Construção das dimensões ───────────────────────────────────────────
    print("\n── 4. Construindo dimensões ──────────────────────────────────────")

    dim_titulo = _build_dim_titulo(df)
    dim_titulo_obra = _build_dim_titulo_obra(df, df_auxiliar)
    dim_titulo_obra_dedup = _build_dim_titulo_obra_dedup(dim_titulo_obra)

    # Diagnóstico de fanout
    n_multi = (dim_titulo_obra.groupby('titulo')['cod_obra'].nunique() > 1).sum()
    n_misto = (dim_titulo_obra_dedup['tipo_obra'] == 'MISTO').sum()

    print(f"  dim_titulo:               {len(dim_titulo):>6} linhas  (títulos únicos)")
    print(f"  dim_titulo_obra:          {len(dim_titulo_obra):>6} linhas  (pares titulo × obra)")
    print(f"  dim_titulo_obra_dedup:    {len(dim_titulo_obra_dedup):>6} linhas  (1 por titulo — uso no Power BI)")
    print(f"    → títulos com N obras:  {n_multi:>6}  (fanout resolvido por dedup)")
    print(f"    → tipo_obra MISTO:      {n_misto:>6}  (tipo divergente entre obras)")

    # ── 5. Salvar ─────────────────────────────────────────────────────────────
    print("\n── 5. Salvando ───────────────────────────────────────────────────")
    salvar_tabela(dim_titulo, 'dim_titulo', output_dir)
    salvar_tabela(dim_titulo_obra, 'dim_titulo_obra', output_dir)
    salvar_tabela(dim_titulo_obra_dedup, 'dim_titulo_obra_dedup', output_dir)

    print("""
── Relacionamentos Power BI ──────────────────────────────────────────────────
  USE dim_titulo_obra_dedup para visuais por tipo_obra:
    fato_consulta_parcela[titulo] → dim_titulo_obra_dedup[titulo]  (1:N, sem fanout)

  MANTENHA dim_titulo_obra para análises de rateio entre obras:
    dim_titulo_obra[titulo]  → dim_titulo[titulo]     (N:1)
    dim_titulo_obra[cod_obra]→ dim_centro_custo[cod_obra] (N:1)

  Títulos da fato sem match em dim_titulo_obra_dedup → tipo_obra = NULL
  (53% da fato: títulos de outras empresas/períodos não no relatório de títulos)
""")


if __name__ == '__main__':
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )
    executar()
