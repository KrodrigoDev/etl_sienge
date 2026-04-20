"""
stages/transform/transform_consulta_parcela.py
-----------------------------------------------
Transforma o CSV bruto de Consulta de Parcelas do SIENGE em:

  fato_consulta_parcela   — estado atual de cada parcela (grain = 1 parcela)
  dim_empresa             — expandida com empresas que só existem aqui
  dim_fornecedor          — expandida com fornecedores (credores) que só existem aqui

Dependência de execução
-----------------------
Este módulo DEVE ser chamado APÓS transform_adiantamento, pois:
  - Lê dim_empresa e dim_fornecedor já geradas em OUTPUT_DIR
  - Expande essas dimensões com registros novos encontrados no CSV de parcelas
  - Sobrescreve os CSVs das dimensões no OUTPUT_DIR

Relacionamentos gerados (todos 1:N, single direction)
------------------------------------------------------
  dim_empresa[id_empresa]       → fato_consulta_parcela[id_empresa]
  dim_fornecedor[id_fornecedor] → fato_consulta_parcela[id_fornecedor]
  dim_status[id_status]         → fato_consulta_parcela[id_status]
  dim_origem[id_origem]         → fato_consulta_parcela[id_origem]
  dim_tipo_baixa[id_tipo_baixa] → fato_consulta_parcela[id_tipo_baixa]
  dim_forma_pagamento[id_forma_pagamento] → fato_consulta_parcela[id_forma_pagamento]
  dim_data[data_key]            → fato_consulta_parcela[id_data_vencimento]
  dim_data[data_key]            → fato_consulta_parcela[id_data_pagamento]
  dim_data[data_key]            → fato_consulta_parcela[id_data_emissao]

Notas sobre o CSV de origem (SIENGE — Consulta de Parcelas)
-----------------------------------------------------------
  - 74 colunas, ~13 k linhas por extração diária
  - Valores monetários no formato brasileiro: "R$\xa01.234,56"
  - Datas no formato DD/MM/YYYY
  - Cód. obra / Cód. departamento / Cód. centro de custo → 100 % NULL no dado atual
    (rateio por obra não disponível nesta consulta; tratar na camada semântica)
  - Cód. credor NULL em ~7,7 % dos registros (lançamentos de Caixa e Bancos sem
    fornecedor vinculado) → mapeados para id_fornecedor = 0 (credor INTERNO)
  - Informações bancárias e PIX chegam como texto livre — parseadas aqui
    e guardadas na dim_fornecedor
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from stages.transform.utils.normalizer import (
    checar_integridade,
    converter_valor_br,
    expandir_dimensao,
    ler_dados,
    normalizar_colunas,
    salvar_tabela,
    criar_dimensao
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

pasta_origem = Path(__file__).resolve().parents[2]

INPUT_DIR   = pasta_origem / "stages" / "transform" / "input"
OUTPUT_DIR  = pasta_origem / "stages" / "transform" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS PRIVADOS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date(series: pd.Series) -> pd.Series:
    """Converte strings DD/MM/YYYY para datetime; não-parseáveis → NaT."""
    return pd.to_datetime(series, format="%d/%m/%Y", errors="coerce")


def _parse_banco(texto: str | float) -> dict:
    """
    Extrai campos estruturados da coluna 'Informações bancárias do Credor'.
    Exemplo de entrada:
      'Banco: 001 Banco do Brasil / Nome da agência: 3057 / Agência: __ /
       N° conta: 64690 / DAC: __ / Tipo conta: Conta Corrente /
       CNPJ/CPF: 30.675.725/0001-40 / Favorecido: FULANO'
    """
    vazio = dict(banco_cod="", banco_nome="", agencia="", conta="",
                 tipo_conta="", cnpj_favorecido="", favorecido_banco="")
    if not isinstance(texto, str) or texto.startswith("Banco: __"):
        return vazio

    def _get(pattern: str) -> str:
        m = re.search(pattern, texto)
        return m.group(1).strip() if m else ""

    cod_nome = _get(r"Banco:\s*(\d+\s+[^/]+)")
    partes   = cod_nome.split(" ", 1) if cod_nome else ["", ""]

    return dict(
        banco_cod        = partes[0].strip(),
        banco_nome       = partes[1].strip() if len(partes) > 1 else "",
        agencia          = _get(r"Agência:\s*([^/]+)").replace("__", "").strip(),
        conta            = _get(r"N° conta:\s*([^/]+)").replace("__", "").strip(),
        tipo_conta       = _get(r"Tipo conta:\s*([^/]+)").replace("__", "").strip(),
        cnpj_favorecido  = _get(r"CNPJ/CPF:\s*([^/]+)").replace("__", "").strip(),
        favorecido_banco = _get(r"Favorecido:\s*(.+)$").strip(),
    )


def _parse_pix(texto: str | float) -> dict:
    """
    Extrai campos estruturados da coluna 'Pix do credor'.
    Exemplo: 'Tipo de chave: CPF / Chave pix: 123... / CNPJ/CPF: __ / Favorecido: __'
    """
    vazio = dict(pix_tipo_chave="", pix_chave="")
    if not isinstance(texto, str):
        return vazio
    def _get(pattern: str) -> str:
        m = re.search(pattern, texto)
        val = m.group(1).strip() if m else ""
        return "" if val == "__" else val

    return dict(
        pix_tipo_chave = _get(r"Tipo de chave:\s*([^/]+)"),
        pix_chave      = _get(r"Chave pix:\s*([^/]+)"),
    )


def _faixa_atraso(dias: pd.Series) -> pd.Series:
    """Categoriza dias de atraso em faixas para uso em visuais de BI."""
    bins   = [-1, 0, 7, 14, 21, 28, float("inf")]
    labels = ["Em dia", "1-7d", "8-14d", "15-21d", "22-28d", "29+d"]
    return pd.cut(dias.fillna(0), bins=bins, labels=labels, right=True)


def _faixa_saldo(saldo: pd.Series) -> pd.Series:
    """Categoriza valores de saldo em faixas para uso em visuais de BI."""

    bins = [0, 20000, 50000, 100000, float("inf")]

    labels = [
        "Até 20 mil",
        "20 mil a 50 mil",
        "50 mil a 100 mil",
        "Acima de 100 mil"
    ]

    return pd.cut(
        saldo.fillna(0),
        bins=bins,
        labels=labels,
        right=True
    )


# ─────────────────────────────────────────────────────────────────────────────
# PONTO DE ENTRADA
# ─────────────────────────────────────────────────────────────────────────────

def executar(input_dir: Path = INPUT_DIR, output_dir: Path = OUTPUT_DIR) -> None:
    """
    Ponto de entrada do transform de Consulta de Parcelas.
    Pode ser chamado diretamente ou pelo main.py do pipeline.
    """

    hoje = date.today()

    # ── 1. Leitura ────────────────────────────────────────────────────────────
    print("\n── 1. Leitura ──────────────────────────────────────────────────────")

    df = ler_dados((input_dir / "consulta_parcela").glob("*.csv"))
    df = normalizar_colunas(df)

    print(f"  Total de linhas no começo da fato: {len(df):,}")
    print(f"  Total de colunas: {len(df.columns)}")

    # ── 2. Conversão de tipos ─────────────────────────────────────────────────
    print("\n── 2. Conversão de tipos ───────────────────────────────────────────")

    # — Colunas monetárias (formato BR: "R$\xa01.234,56") —
    COLUNAS_VALOR = [
        "valor_no_vencimento",
        "valor_bruto",
        "acrescimo",
        "desconto",
        "valor_imposto_retido",
        "valor_liquido",
        "valor_da_baixa",
        "saldo_em_aberto",
    ]
    for col in COLUNAS_VALOR:
        if col in df.columns:
            df[col] = converter_valor_br(df[col])

    # — Colunas de data (DD/MM/YYYY) —
    COLUNAS_DATA = [
        "data_vencimento",
        "data_do_pagamento",
        "data_base",
        "data_emissao",
        "data_de_cadastro",
        "data_de_alteracao",
        "data_contabil",
        "data_de_competencia",
        "vencimento_original",
        "data_do_calculo",
    ]
    for col in COLUNAS_DATA:
        if col in df.columns:
            df[col] = _parse_date(df[col])

    # — Inteiros simples —
    df["titulo"]       = pd.to_numeric(df.get("titulo"), errors="coerce").astype("Int64")
    df["cod_empresa"]  = pd.to_numeric(df.get("cod_empresa"), errors="coerce").astype("Int64")
    df["cod_credor"]   = pd.to_numeric(df.get("cod_credor"), errors="coerce").astype("Int64")
    df["dias_de_atraso"] = pd.to_numeric(df.get("dias_de_atraso"), errors="coerce").fillna(0).astype(int)

    # diferenca_data_vencimento pode vir como "0", "-5", "14" etc.
    df["diferenca_data_vencimento"] = pd.to_numeric(
        df.get("diferenca_data_vencimento"), errors="coerce"
    ).fillna(0).astype(int)

    print(f"  Valores convertidos — saldo_em_aberto max: "
          f"R$ {df['saldo_em_aberto'].max():,.2f}")

    # ── 3. Flags calculadas ───────────────────────────────────────────────────
    print("\n── 3. Flags calculadas ─────────────────────────────────────────────")

    data_hoje = pd.Timestamp(hoje)

    df["flag_vencida"]         = df["status_da_parcela"] == "VENCIDA"
    df["flag_a_vencer"]        = df["status_da_parcela"] == "A_VENCER"
    df["flag_paga"]            = df["status_da_parcela"] == "PAGA"
    df["flag_vence_hoje"]      = df["data_vencimento"] == data_hoje
    df["flag_pago_antecipado"] = (
        df["data_do_pagamento"].notna()
        & df["data_vencimento"].notna()
        & (df["data_do_pagamento"] < df["data_vencimento"])
    )
    df["flag_pago_atraso"]     = (
        df["data_do_pagamento"].notna()
        & df["data_vencimento"].notna()
        & (df["data_do_pagamento"] > df["data_vencimento"])
    )
    df["flag_substituida"]     = df.get("tipo_de_baixa", "") == "SUBSTITUICAO"
    df["flag_critico"]         = df["dias_de_atraso"] >= 15
    df["flag_sem_credor"]      = df["cod_credor"].isna()
    # Obra/CC/Dpto chegam 100% NULL nesta consulta — flag de qualidade para o futuro
    df["flag_sem_obra"]        = df.get("cod_obra", pd.Series(np.nan, index=df.index)).isna()
    df["flag_autorizada"]      = df.get("parcela_autorizada", "").str.strip().str.lower() == "sim"
    df["faixa_atraso"]         = _faixa_atraso(df["dias_de_atraso"]).astype(str)
    df["faixa_saldo"]         = _faixa_saldo(df["saldo_em_aberto"])

    print(f"  flag_vencida:          {df['flag_vencida'].sum():,}")
    print(f"  flag_vence_hoje:       {df['flag_vence_hoje'].sum():,}")
    print(f"  flag_pago_antecipado:  {df['flag_pago_antecipado'].sum():,}")
    print(f"  flag_pago_atraso:      {df['flag_pago_atraso'].sum():,}")
    print(f"  flag_critico (≥15d):   {df['flag_critico'].sum():,}")
    print(f"  flag_sem_credor:       {df['flag_sem_credor'].sum():,}")

    # ── 4. Parsear informações bancárias e PIX ────────────────────────────────
    print("\n── 4. Parse de dados bancários / PIX ───────────────────────────────")

    banco_parsed = df["informacoes_bancarias_do_credor"].apply(_parse_banco)
    pix_parsed   = df["pix_do_credor"].apply(_parse_pix)

    df_banco = pd.DataFrame(list(banco_parsed), index=df.index)
    df_pix   = pd.DataFrame(list(pix_parsed),   index=df.index)

    df = pd.concat([df, df_banco, df_pix], axis=1)

    print(f"  Registros com banco preenchido: "
          f"{(df['banco_cod'] != '').sum():,}")
    print(f"  Registros com PIX preenchido:   "
          f"{(df['pix_chave'] != '').sum():,}")

    # ── 5. Carregar dimensões existentes ──────────────────────────────────────
    print("\n── 5. Carregando dimensões existentes ──────────────────────────────")

    dim_empresa    = pd.read_csv(output_dir / "dim_empresa.csv",    sep=";")

    print(f"  dim_empresa:    {dim_empresa.shape}")

    # ── 6. Expandir dim_empresa ───────────────────────────────────────────────
    print("\n── 6. Expandindo dim_empresa ───────────────────────────────────────")

    dim_empresa = expandir_dimensao(
        dim_existente  = dim_empresa,
        df_novo        = df.rename(columns={
            "cod_empresa": "cod_empresa",
            "empresa":     "empresa",
        }),
        colunas_naturais = ["cod_empresa", "empresa"],
        nome_id          = "id_empresa",
        col_pk_natural   = "cod_empresa",
    )

    print(f"  dim_empresa após expansão: {dim_empresa.shape}")

    # ── 7. Expandir dim_fornecedor (credor) ───────────────────────────────────
    print("\n── 7. Expandindo dim_fornecedor ────────────────────────────────────")

    # Deduplica credores do CSV — pega a linha mais recente por cod_credor
    # para ter as informações bancárias mais atualizadas
    print(df.columns)
    dim_fornecedor = (
        df[df["cod_credor"].notna()]
        [[
            "cod_credor", "credor", "cnpj/cpf", "tipo_credor",
            "banco_cod", "banco_nome", "agencia", "conta", "tipo_conta",
            "cnpj_favorecido", "favorecido_banco",
            "pix_tipo_chave", "pix_chave",
            "forma_de_pagamento",
        ]]
        .drop_duplicates(subset="cod_credor", keep="last")
        .drop_duplicates(subset="cod_credor", keep="last")
        .rename(columns={
            "credor":           "nome_fornecedor",
            "cnpj/cpf":         "cnpj_cpf",
            "tipo_credor":      "tipo_credor",
            "forma_de_pagamento": "forma_pagamento_padrao",
        })
    )

    # Garante credor INTERNO (id=0) para lançamentos sem credor (Caixa/Bancos)
    CREDOR_INTERNO = pd.DataFrame([{
        "cod_credor": 0, "nome_fornecedor": "INTERNO",
        "cnpj_cpf": "", "tipo_credor": "Interno",
        "banco_cod": "", "banco_nome": "", "agencia": "",
        "conta": "", "tipo_conta": "", "cnpj_favorecido": "",
        "favorecido_banco": "", "pix_tipo_chave": "", "pix_chave": "",
        "forma_pagamento_padrao": "",
    }])

    dim_fornecedor = pd.concat([CREDOR_INTERNO, dim_fornecedor], ignore_index=True)
    dim_fornecedor["cod_credor"] = dim_fornecedor["cod_credor"].astype("Int64")

    dim_fornecedor = criar_dimensao(dim_fornecedor, colunas=[
            "cod_credor", "nome_fornecedor", "cnpj_cpf", "tipo_credor",
            "banco_cod", "banco_nome", "agencia", "conta", "tipo_conta",
            "cnpj_favorecido", "favorecido_banco",
            "pix_tipo_chave", "pix_chave", "forma_pagamento_padrao",
        ], nome_id='id_fornecedor')


    # ── 8. Dimensões pequenas (geradas/sobrescritas integralmente aqui) ───────
    print("\n── 8. Dimensões de domínio ─────────────────────────────────────────")

    # dim_status
    dim_status = pd.DataFrame([
        {"id_status": 1, "status_parcela": "PAGA",     "grupo_status": "Quitado"},
        {"id_status": 2, "status_parcela": "VENCIDA",  "grupo_status": "Inadimplente"},
        {"id_status": 3, "status_parcela": "A_VENCER", "grupo_status": "Em dia"},
        {"id_status": 0, "status_parcela": "SEM_STATUS","grupo_status": "Indefinido"},
    ])

    # dim_tipo_baixa — derivada dos valores distintos presentes no dado
    tipos_baixa = (
        df["tipo_de_baixa"]
        .dropna()
        .unique()
        .tolist()
    )
    dim_tipo_baixa = pd.DataFrame({
        "id_tipo_baixa":  range(1, len(tipos_baixa) + 1),
        "tipo_baixa":     tipos_baixa,
    })
    # descrição legível
    _desc = {
        "PAGAMENTO":              "Pagamento normal",
        "SUBSTITUICAO":           "Substituição / renegociação",
        "CANCELAMENTO":           "Cancelamento",
        "ABATIMENTO_ADIANTAMENTO":"Abatimento de adiantamento",
        "ADIANTAMENTO":           "Adiantamento",
        "OUTROS":                 "Outros",
        "ESTORNO":                "Estorno de baixa",
        "DEVOLUCAO":              "Devolução",
    }
    dim_tipo_baixa["descricao"] = dim_tipo_baixa["tipo_baixa"].map(_desc).fillna(
        dim_tipo_baixa["tipo_baixa"]
    )

    # dim_origem
    origens = df["origem"].dropna().unique().tolist()
    dim_origem = pd.DataFrame({
        "id_origem":  range(1, len(origens) + 1),
        "origem":     origens,
    })

    # dim_forma_pagamento
    formas = df["forma_de_pagamento"].dropna().unique().tolist()
    dim_forma_pagamento = pd.DataFrame({
        "id_forma_pagamento": range(1, len(formas) + 1),
        "forma_pagamento":    formas,
    })

    for dim, nome in [
        (dim_status,          "dim_status"),
        (dim_tipo_baixa,      "dim_tipo_baixa"),
        (dim_origem,          "dim_origem"),
        (dim_forma_pagamento, "dim_forma_pagamento"),
    ]:
        print(f"  {nome}: {dim.shape}")

    # ── 9. Surrogate keys na tabela fato ──────────────────────────────────────
    print("\n── 9. Surrogate keys ───────────────────────────────────────────────")

    # Mapa cod_empresa → id_empresa
    _emp_map = (
        dim_empresa
        .drop_duplicates("cod_empresa")
        .set_index("cod_empresa")["id_empresa"]
        .to_dict()
    )

    # Mapa cod_credor → id_fornecedor  (NULL → 0 → id do credor INTERNO)
    _forn_map = (
        dim_fornecedor
        .drop_duplicates("cod_credor")
        .set_index("cod_credor")["id_fornecedor"]
        .to_dict()
    )

    _status_map   = dim_status.set_index("status_parcela")["id_status"].to_dict()
    _baixa_map    = dim_tipo_baixa.set_index("tipo_baixa")["id_tipo_baixa"].to_dict()
    _origem_map   = dim_origem.set_index("origem")["id_origem"].to_dict()
    _forma_map    = dim_forma_pagamento.set_index("forma_pagamento")["id_forma_pagamento"].to_dict()

    # Preenche cod_credor NULL com 0 (INTERNO) antes do mapeamento
    df["cod_credor_lookup"] = df["cod_credor"].fillna(0).astype(int)

    df["id_empresa"]          = df["cod_empresa"].map(_emp_map)
    df["id_fornecedor"]       = df["cod_credor_lookup"].map(_forn_map)
    df["id_status"]           = df["status_da_parcela"].map(_status_map).fillna(0).astype(int)
    df["id_tipo_baixa"]       = df["tipo_de_baixa"].map(_baixa_map)
    df["id_origem"]           = df["origem"].map(_origem_map)
    df["id_forma_pagamento"]  = df["forma_de_pagamento"].map(_forma_map)

    # Surrogate keys de data — usam a data como string YYYY-MM-DD para join
    # com a dim_data gerada separadamente (calendário)
    # df["id_data_vencimento"] = df["data_vencimento"].dt.strftime("%Y%m%d").astype("Int64")
    # df["id_data_pagamento"]  = df["data_do_pagamento"].dt.strftime("%Y%m%d").astype("Int64")
    # df["id_data_emissao"]    = df["data_emissao"].dt.strftime("%Y%m%d").astype("Int64")
    # df["id_data_competencia"]= df["data_de_competencia"].dt.strftime("%Y%m%d").astype("Int64")

    for col_id, total in [
        ("id_empresa",   len(df)),
        ("id_fornecedor",len(df)),
        ("id_status",    len(df)),
        ("id_origem",    len(df)),
    ]:
        matched = df[col_id].notna().sum()
        print(f"  {col_id:<22} {matched:,} / {total:,}  ({matched/total:.1%})")

    # ── 10. Montar fato_consulta_parcela ──────────────────────────────────────
    print("\n── 10. fato_consulta_parcela ───────────────────────────────────────")

    fato = df[[
        # ── Chaves surrogate ──────────────────────────────────────
        "id_empresa",
        "id_fornecedor",
        "id_status",
        "id_tipo_baixa",
        "id_origem",
        "id_forma_pagamento",
        # "id_data_vencimento", # verificar se vou retirar
        # "id_data_pagamento",
        # "id_data_emissao",
        # "id_data_competencia",

        # ── Chaves naturais (dimensões degeneradas) ───────────────
        "cod_empresa",
        "cod_credor",
        "titulo",
        "parcela",          # ex: "7/36"
        "grupo",            # ex: "396550/7" (titulo/parcela original)
        "documento",        # tipo do documento: NF, AV, NFS, APT…
        "nn_documento",
        "conta_contabil",

        # ── Datas (mantidas também como colunas diretas) ──────────
        "data_vencimento",
        "data_do_pagamento",
        "data_emissao",
        "data_de_competencia",
        "data_contabil",
        "data_de_cadastro",
        "vencimento_original",

        # ── Métricas financeiras ──────────────────────────────────
        "valor_no_vencimento",
        "valor_bruto",
        "acrescimo",
        "desconto",
        "valor_imposto_retido",
        "valor_liquido",
        "valor_da_baixa",
        "saldo_em_aberto",

        # ── Métricas de prazo ─────────────────────────────────────
        "dias_de_atraso",
        "diferenca_data_vencimento",
        "faixa_atraso",
        "faixa_saldo",

        # ── Flags calculadas ──────────────────────────────────────
        "flag_vencida",
        "flag_a_vencer",
        "flag_paga",
        "flag_vence_hoje",
        "flag_pago_antecipado",
        "flag_pago_atraso",
        "flag_substituida",
        "flag_critico",
        "flag_sem_credor",
        "flag_sem_obra",
        "flag_autorizada",

        # ── Atributos de workflow / auditoria ─────────────────────
        "ciencia_do_titulo",
        "parcela_autorizada",
        "parcela_agrupada",
        "titulo/parcela_agrupada",
        "nn_lote",
        "status_do_lote",
        "indexador",
        "tipo_de_operacao",
        "historico",
        "chave_nfe",
        "autenticacao_eletronica",
        "usuario_que_deu_ciencia",
        "usuario_que_autorizou",
        "usuario_que_cadastrou",
        "usuario_que_alterou",
        "observacao_do_titulo",
        "descricao_do_pagamento",

    ]].copy()

    # Data de carga — rastreia qual extração originou o registro
    fato["data_carga"] = hoje.isoformat()

    print(f"  fato_consulta_parcela: {fato.shape}")
    print(f"  PAGA:     {fato['flag_paga'].sum():,}")
    print(f"  VENCIDA:  {fato['flag_vencida'].sum():,}")
    print(f"  A_VENCER: {fato['flag_a_vencer'].sum():,}")
    print(f"  Saldo vencido total: "
          f"R$ {fato.loc[fato['flag_vencida'], 'saldo_em_aberto'].sum():,.2f}")
    print(f"  Vencem hoje:         {fato['flag_vence_hoje'].sum():,}")
    print(f"  Critico (≥15d):      {fato['flag_critico'].sum():,}")

    # ── 11. Validação de integridade ──────────────────────────────────────────
    print("\n── 11. Validação ───────────────────────────────────────────────────")

    checar_integridade(
        fato, "id_empresa",    dim_empresa,    "id_empresa",
        "fato_consulta_parcela → dim_empresa"
    )
    checar_integridade(
        fato, "id_fornecedor", dim_fornecedor, "id_fornecedor",
        "fato_consulta_parcela → dim_fornecedor"
    )
    checar_integridade(
        fato, "id_status",     dim_status,     "id_status",
        "fato_consulta_parcela → dim_status"
    )
    checar_integridade(
        fato, "id_origem",     dim_origem,     "id_origem",
        "fato_consulta_parcela → dim_origem"
    )

    # ── 12. Exportação ────────────────────────────────────────────────────────
    print("\n── 12. Exportação ──────────────────────────────────────────────────")

    # Dimensões expandidas — sobrescreve o que o transform_adiantamento gerou
    salvar_tabela(dim_empresa,    "dim_empresa",    output_dir)
    salvar_tabela(dim_fornecedor, "dim_fornecedor_consulta_parcela", output_dir)

    # Dimensões de domínio (geradas aqui, sobrescritas a cada run)
    salvar_tabela(dim_status,          "dim_status",          output_dir)
    salvar_tabela(dim_tipo_baixa,      "dim_tipo_baixa",      output_dir)
    salvar_tabela(dim_origem,          "dim_origem",          output_dir)
    salvar_tabela(dim_forma_pagamento, "dim_forma_pagamento", output_dir)

    # Fato principal
    salvar_tabela(fato, "fato_consulta_parcela", output_dir)

    print("\n── Resumo final ────────────────────────────────────────────────────")
    for nome, tabela in {
        "dim_empresa (expandida)":    dim_empresa,
        "dim_fornecedor (expandida)": dim_fornecedor,
        "dim_status":                 dim_status,
        "dim_tipo_baixa":             dim_tipo_baixa,
        "dim_origem":                 dim_origem,
        "dim_forma_pagamento":        dim_forma_pagamento,
        "fato_consulta_parcela":      fato,
    }.items():
        print(f"  {nome:<35} {str(tabela.shape):>12}")

    print("""
── Relacionamentos Power BI — fato_consulta_parcela (1:N, single direction) ──
  dim_empresa[id_empresa]              → fato_consulta_parcela[id_empresa]
  dim_fornecedor[id_fornecedor]        → fato_consulta_parcela[id_fornecedor]
  dim_status[id_status]                → fato_consulta_parcela[id_status]
  dim_tipo_baixa[id_tipo_baixa]        → fato_consulta_parcela[id_tipo_baixa]
  dim_origem[id_origem]                → fato_consulta_parcela[id_origem]
  dim_forma_pagamento[id_forma_pgto]   → fato_consulta_parcela[id_forma_pagamento]

── Dimensões compartilhadas com outros fatos do pipeline ─────────────────────
  dim_empresa e dim_fornecedor são as mesmas usadas em fato_adiantamento.
  O Power BI filtra os dois fatos simultaneamente quando o usuário seleciona
  uma empresa ou fornecedor no painel.
""")


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    executar()