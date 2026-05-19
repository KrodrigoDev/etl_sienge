"""──────────────────────────────────────────────────────────────────────────
Substitui o pd.merge simples do transform_curva_abc_apropriacao.py por uma
cascata de estratégias que cobre os padrões de divergência observados nas bases.

Padrões identificados
─────────────────────
 S1  Diferença de espaços / capitalização
       val  "1552 - IM - Tela Soldada "  →  norm  →  "1552 - IM - TELA SOLDADA"
       dim  "1552 - IM - Tela Soldada"   →  norm  →  mesmo resultado

 S2  Marca/modelo acrescido ao final do detalhe com " / MARCA"
       val  detalhe = "LAREDO 67 X 67CM RETIFICADO HD ACETINADO  / ARIELLE"
       dim  detalhe = "LAREDO 67 X 67CM RETIFICADO HD ACETINADO"
       → strip tudo após o último " / "

 S3  Detalhe dobrado na descrição dentro da dim (sem split pelo "/")
       val  desc  = "2137 - IM - Telha … RAL9003"  detalhe = "PRIMER - 0,43MM X 6M"
       dim  desc  = "2137 - IM - Telha … RAL9003/PRIMER - 0,43MM X 6M"  detalhe = "0"
       → concatenar val(desc + " / " + det) e comparar com dim(desc)

 S4  Prefixo numérico no detalhe (número da norma alternativo)
       val  detalhe = "7481 / CORTADO, DOBRADO - 10.0 MM  CA - 50"
       dim  detalhe = "CORTADO, DOBRADO - 10.0 MM  CA - 50"
       → strip tudo antes do primeiro " / " quando começa por dígitos

 S5  Descrição abreviada / diferente, mas cod+detalhe são únicos na dim
       val  desc = "1133 - IM- ISOTELHA"      dim desc = "1133 - ISOTELHA"
       → match somente por cod_insumo + detalhe_normalizado quando
         a combinação (cod, det) identifica exatamente 1 linha na dim

 S6  detalhe = "0" e há só 1 linha na dim para aquele cod_insumo
       → match somente pelo cod (último recurso seguro)

Uso
───
    from merge_fuzzy_dim_insumo import merge_com_dim_insumo

    df = merge_com_dim_insumo(df, dim_insumo)
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from stages.transform.utils.normalizer import (
    ler_dados,
    normalizar_colunas,
    salvar_tabela
)

# ── Diretórios ────────────────────────────────────────────────────────────────

pasta_origem = Path(__file__).resolve().parents[2]

INPUT_DIR = pasta_origem / 'stages' / 'transform' / 'input'
OUTPUT_DIR = pasta_origem / 'stages' / 'transform' / 'output'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ── helpers ──────────────────────────────────────────────────────────────────

def _norm(series: pd.Series) -> pd.Series:
    """Strip, upper-case e colapsa espaços múltiplos."""
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
    )

def consolidar_colunas_merge(df: pd.DataFrame) -> pd.DataFrame:
    """
    Junta colunas *_x e *_y:
    - mantém valor de _x
    - se _x estiver vazio/nulo, usa _y
    - remove colunas auxiliares
    """

    cols_x = [c for c in df.columns if c.endswith('_x')]

    for col_x in cols_x:

        base = col_x[:-2]
        col_y = f'{base}_y'

        if col_y not in df.columns:
            continue

        # pega y quando x estiver vazio
        df[base] = (
            df[col_x]
            .replace('', pd.NA)
            .fillna(df[col_y])
        )

        # remove auxiliares
        df = df.drop(columns=[col_x, col_y])

    return df


def _strip_marca(series: pd.Series) -> pd.Series:
    """Remove ' / QUALQUERCOISA' do final do detalhe (marca/modelo)."""
    return (
        series.str.replace(r"\s*/\s*[^/]+$", "", regex=True)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
        .replace("", "0")
    )


def _strip_prefixo_numerico(series: pd.Series) -> pd.Series:
    """Remove prefixo 'NNNN / ' do detalhe quando começa por dígitos."""
    return (
        series.str.replace(r"^\d[\d\s]*/\s*", "", regex=True)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
        .replace("", "0")
    )


# ── função principal ──────────────────────────────────────────────────────────

def merge_com_dim_insumo(
    df: pd.DataFrame,
    dim_insumo: pd.DataFrame,
    *,
    col_id: str = "Unnamed: 0",       # coluna índice original de df
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Executa o merge em cascata entre df (curva ABC) e dim_insumo.

    Retorna df com as colunas da dim preenchidas para o máximo de linhas
    possível, e uma coluna extra 'estrategia_match' indicando qual estratégia
    resolveu cada linha ('original' = match exato da chamada anterior ao merge).

    Parâmetros
    ----------
    df          : DataFrame já com colunas cod_insumo, descricao_do_insumo, detalhe.
    dim_insumo  : DataFrame da dimensão de insumos.
    col_id      : coluna que serve de chave única de linha em df (índice).
    verbose     : imprime resumo de cada estratégia.
    """

    DIM_EXTRA = [
        "id_insumo", "cod_grupo_de_insumo", "grupo_de_insumo",
        "marca", "id_grupo", "tipo_grupo",
    ]
    dim_cols_existentes = [c for c in DIM_EXTRA if c in dim_insumo.columns]

    # Garante tipos
    dim = dim_insumo.copy()
    dim["cod_insumo"] = pd.to_numeric(dim["cod_insumo"], errors="coerce").astype("Int64")

    # Colunas normalizadas na dim (feito uma vez)
    dim["_desc_n"] = _norm(dim["descricao_do_insumo"])
    dim["_det_n"]  = _norm(dim["detalhe"])

    # Colunas normalizadas no df principal
    df = df.copy()
    df["_desc_n"] = _norm(df["descricao_do_insumo"])
    df["_det_n"]  = _norm(df["detalhe"])

    # Garante col_id único
    if col_id not in df.columns:
        df = df.reset_index().rename(columns={"index": col_id})

    # Acumuladores
    matched_parts: list[pd.DataFrame] = []
    matched_ids: set = set()

    def _log(label: str, n: int, remaining: int) -> None:
        if verbose:
            print(f"  {label}: +{n:>5} matched  |  restando {remaining}")

    def _apply(
        remaining_df: pd.DataFrame,
        dim_sub: pd.DataFrame,
        left_on: list[str],
        right_on: list[str],
        label: str,
        estrategia: str,
    ) -> pd.DataFrame:
        nonlocal matched_ids

        m = remaining_df.merge(
            dim_sub[right_on + dim_cols_existentes],
            left_on=left_on,
            right_on=right_on,
            how="inner",
        )
        # deduplicar: em caso de múltiplos hits na dim, pega o primeiro
        m = m.drop_duplicates(subset=[col_id])
        novos = m[~m[col_id].isin(matched_ids)].copy()
        novos["estrategia_match"] = estrategia
        matched_ids |= set(novos[col_id])
        matched_parts.append(novos)
        remaining_after = len(df) - len(matched_ids)
        _log(label, len(novos), remaining_after)
        return df[~df[col_id].isin(matched_ids)].copy()

    # ── S1: normalização de espaços / capitalização ───────────────────────────
    remaining = df.copy()
    if verbose:
        print("\n── Merge cascata dim_insumo ─────────────────────────────────────────")
    remaining = _apply(
        remaining, dim,
        ["cod_insumo", "_desc_n", "_det_n"],
        ["cod_insumo", "_desc_n", "_det_n"],
        "S1 normalização (espaços/case)",
        "S1_norm",
    )

    # ── S2: strip marca do detalhe (' / MARCA' no final) ─────────────────────
    remaining["_det_sem_marca"] = _strip_marca(remaining["detalhe"])
    remaining = _apply(
        remaining, dim,
        ["cod_insumo", "_desc_n", "_det_sem_marca"],
        ["cod_insumo", "_desc_n", "_det_n"],
        "S2 strip marca detalhe",
        "S2_strip_marca",
    )

    # ── S3: detalhe dobrado na descrição da dim ───────────────────────────────
    remaining["_desc_full"] = remaining["_desc_n"] + " / " + remaining["_det_n"]
    dim_det0 = dim[dim["_det_n"] == "0"].copy()
    remaining = _apply(
        remaining, dim_det0,
        ["cod_insumo", "_desc_full"],
        ["cod_insumo", "_desc_n"],
        "S3 det dobrado em desc (dim)",
        "S3_det_em_desc",
    )

    # ── S4: prefixo numérico no detalhe ('7481 / XXXX') ─────────────────────
    remaining["_det_sem_prefixo"] = _strip_prefixo_numerico(remaining["detalhe"])
    remaining = _apply(
        remaining, dim,
        ["cod_insumo", "_desc_n", "_det_sem_prefixo"],
        ["cod_insumo", "_desc_n", "_det_n"],
        "S4 strip prefixo numérico detalhe",
        "S4_strip_prefixo",
    )

    # ── S5: cod + detalhe únicos na dim (desc diverge) ───────────────────────
    # Conta quantas linhas únicas de desc existem por (cod, det) na dim
    dim_cod_det_unique = (
        dim.groupby(["cod_insumo", "_det_n"])["_desc_n"]
        .nunique()
        .reset_index(name="_n_desc")
    )
    dim_s5 = dim.merge(dim_cod_det_unique[dim_cod_det_unique["_n_desc"] == 1][["cod_insumo", "_det_n"]], on=["cod_insumo", "_det_n"])
    remaining = _apply(
        remaining, dim_s5,
        ["cod_insumo", "_det_n"],
        ["cod_insumo", "_det_n"],
        "S5 cod+det únicos (desc diverge)",
        "S5_cod_det",
    )

    # ── S5b: S4 + strip marca combinados ─────────────────────────────────────
    remaining["_det_s5b"] = _strip_prefixo_numerico(remaining["_det_sem_marca"] if "_det_sem_marca" in remaining.columns else _strip_marca(remaining["detalhe"]))
    remaining = _apply(
        remaining, dim_s5,
        ["cod_insumo", "_det_s5b"],
        ["cod_insumo", "_det_n"],
        "S5b S4+S2 combinados",
        "S5b_combo",
    )

    # ── S6: det='0' + único cod na dim ───────────────────────────────────────
    dim_cod_unique = (
        dim.groupby("cod_insumo")["id_insumo"]
        .nunique()
        .reset_index(name="_n")
    )
    dim_s6 = dim.merge(dim_cod_unique[dim_cod_unique["_n"] == 1][["cod_insumo"]], on="cod_insumo")
    remaining_det0 = remaining[remaining["_det_n"] == "0"].copy()
    remaining = _apply(
        remaining_det0, dim_s6,
        ["cod_insumo"],
        ["cod_insumo"],
        "S6 detalhe=0 + cod único na dim",
        "S6_cod_unico",
    )

    # ── S7:  ───────────────────────────────────────

    # fazer tentaiva pela coluna insumo inteira da df
    # exemplo 2137 - IM - Telha Galvanizada Trapezoidal TP-40 - RAL9003/PRIMER - 0,43MM X 6M
    # coluna descricao_do_insumo da dim_insumo 2137 - IM - Telha Galvanizada Trapezoidal TP-40 - RAL9003/PRIMER - 0,43MM X 6M
    # são exatamentes iguais


    # ── S8:  ───────────────────────────────────────

    # impelementar uma relação com similaridade com limiar de 97%


    # ── Consolidação ─────────────────────────────────────────────────────────
    # Linhas que não matcharam em nenhuma estratégia
    unmatched = df[~df[col_id].isin(matched_ids)].copy()
    unmatched["estrategia_match"] = "sem_match"
    for c in dim_cols_existentes:
        unmatched[c] = pd.NA

    result = pd.concat(matched_parts + [unmatched], ignore_index=True)

    # Remove colunas auxiliares
    aux_cols = [c for c in result.columns if c.startswith("_")]
    result = result.drop(columns=aux_cols)

    if verbose:
        total = len(df)
        n_matched = len(df) - len(unmatched)
        print(f"\n  ✓ Total matched : {n_matched}/{total} ({n_matched/total*100:.1f}%)")
        print(f"  ✗ Sem match     : {len(unmatched)}/{total} ({len(unmatched)/total*100:.1f}%)")
        if len(unmatched):
            print("\n  Estratégias usadas:")
            print(result["estrategia_match"].value_counts().to_string())

    return result


def executar(input_dir: Path = INPUT_DIR, output_dir: Path = OUTPUT_DIR) -> None:
    # ── 1. Leitura ────────────────────────────────────────────────────────────
    print("\n── 1. Leitura (apropriação ABC) ─────────────────────────────────")

    df = ler_dados((input_dir / 'curva_abc_apropriacao').glob('*.xlsx'), formato='excel', salto=6)

    df = normalizar_colunas(df)

    # Remove linhas inválidas
    df = df.dropna(subset=['insumo'])

    # Mantém apenas colunas necessárias
    df = df[
        ['tabela', 'insumo', 'un',
         'quantidade', 'preco_unit_medio',
         'preco_total', '%part',
         '%acum', 'nome_arquivo'
         ]
    ].copy()

    # ── 2. Ano de referência ─────────────────────────────────────────────────
    print("\n── 2. Extraindo ano de referência ───────────────────────────────")

    df['ano_referencia'] = (
        df['nome_arquivo']
        .str.extract(r'(\d{4})')
        .astype(int)
    )

    # ── 3. Extração do código do insumo ──────────────────────────────────────
    print("\n── 3. Extraindo código do insumo ────────────────────────────────")

    df['cod_insumo'] = (
        df['insumo']
        .str.extract(r'^(\d+)') .astype('Int64')
    )

    # ── 4. Separação descrição x detalhe ─────────────────────────────────────
    print("\n── 4. Separando descrição e detalhe ─────────────────────────────")

    # Remove o código do começo
    texto_sem_codigo = (
        df['insumo']
        .str.replace(r'^\d+\s*-\s*', '', regex=True)
        .str.strip()
    )

    # Divide pelo "/"
    partes = texto_sem_codigo.str.partition('/')

    # Descrição principal
    df['descricao_do_insumo'] = (
            df['cod_insumo'].astype(str)
            + ' - '
            + partes[0].str.strip()
    )

    # Detalhe
    df['detalhe'] = partes[2].str.strip()

    # Se não existir detalhe → vazio
    df['detalhe'] = (
        df['detalhe']
        .replace('', '0')
        .fillna('0')
    )


    dim_insumo = pd.read_csv(output_dir / 'dim_insumo.csv', sep=';')

    dim_insumo['cod_insumo'] = pd.to_numeric(dim_insumo['cod_insumo'], errors='coerce').astype('Int64')

    df = pd.merge(df, dim_insumo, on=['cod_insumo', 'descricao_do_insumo', 'detalhe'], how='left')

    df = merge_com_dim_insumo(df, dim_insumo)

    df = consolidar_colunas_merge(df)

    df_fato = df[['id_insumo', 'cod_insumo', 'descricao_do_insumo', 'detalhe', 'tabela', 'un', 'quantidade', 'preco_unit_medio',
                  'preco_total', '%part', '%acum', 'estrategia_match', 'ano_referencia', 'insumo']]


    salvar_tabela(df_fato, 'fato_curva_abc_insumo', output_dir)

if __name__ == '__main__':

    executar()
