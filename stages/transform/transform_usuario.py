"""
stages/transform/transform_usuarios.py
-----------------------------------------------
Transforma os dois arquivos de extração de usuários do SIENGE em:

  dim_usuario          — dimensão de usuários com atributos enriquecidos
  fato_acesso_usuario  — grain = 1 usuário, estado atual de engajamento

Fontes
------
  cadastro_usuario_<ano>.csv  — extraído via Selenium + BeautifulSoup
                                 (cadastro → lista de usuários)
  relatorio_usuario.xlsx      — relatório do SIENGE com cargo por usuário

Enriquecimentos calculados
--------------------------
  - dias_sem_acesso     → dias desde o último acesso até hoje
  - flag_ativo          → sem data de desativação
  - flag_nunca_acessou  → data_ultimo_acesso nula
  - flag_inativo_30d    → último acesso há mais de 30 dias
  - flag_inativo_60d    → último acesso há mais de 60 dias
  - flag_inativo_90d    → último acesso há mais de 90 dias
  - faixa_inatividade   → categorização textual de engajamento
  - dominio_email       → domínio extraído do email (telesil, externo, etc.)
  - tipo_usuario        → interno / externo / sistema / teste
  - antiguidade_dias    → dias desde a ativação
  - faixa_antiguidade   → faixa de tempo de conta
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import re

import numpy as np
import pandas as pd
from openpyxl import load_workbook

from stages.transform.utils.normalizer import (
    salvar_tabela,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

pasta_origem = Path(__file__).resolve().parents[2]

INPUT_DIR = pasta_origem / "stages" / "transform" / "input"
OUTPUT_DIR = pasta_origem / "stages" / "transform" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Domínios considerados internos (telesil e suas empresas)
DOMINIOS_INTERNOS = {
    "telesil.com.br",
    "telesilengenharia.com.br",
}

# Códigos de usuário que são contas de sistema/teste (não pessoas físicas)
CODIGOS_SISTEMA = {
    "ADMIN", "SUPER", "TESTE", "TESTE1", "PERMISSAO",
    "RPATELESIL", "TITELESIL", "SUPER",
    "LCT01", "LCT02", "LCT03", "LCT04",
    "JAPRENDIZ", "NG7", "NOTASMARKETING",
}

# Mapeamento funcao_id → descrição para permissões por obra
FUNCOES_OBRA_MAP: dict[int, str] = {
    1: "Acesso - Orçamento",
    2: "Acesso - Planejamento",
    3: "Acesso - Acompanhamento",
    4: "Acesso - Controle de Mão de Obra",
    5: "Acesso - Gerencial de Obras",
    6: "Acesso - Gerencial Financeiro",
    7: "Acesso - Gerencial Suprimentos",
    8: "Manutenção - Compras",
    9: "Manutenção - Contratos",
    10: "Manutenção - Medições",
    11: "Manutenção - Estoque",
    12: "Manutenção - Financeiro",
    13: "Manutenção - Gestão da Qualidade",
    14: "Manutenção - Administrativo",
    15: "Manutenção - Comercial",
    16: "Manutenção - Orçamento Empresarial",
    17: "Manutenção - Diário de Obra",
    18: "Manutenção - Integração SAP",
    19: "Acesso - Custo Orçado e Incorrido",
    20: "Acesso - Locações de Equipamentos",
    21: "Acesso - Nota Fiscal Eletrônica",
    22: "Exclusão de Anexos",
    23: "Acesso - Apoio",
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, format="%d/%m/%Y", errors="coerce")


def _dominio_email(email: str | float) -> str:
    if not isinstance(email, str) or "@" not in email:
        return "sem_email"
    return email.strip().lower().split("@")[-1]


def _tipo_usuario(row: pd.Series) -> str:
    codigo = str(row.get("codigo", "")).upper()
    dominio = row.get("dominio_email", "")

    if codigo in CODIGOS_SISTEMA:
        return "sistema_teste"
    if dominio in DOMINIOS_INTERNOS:
        return "interno"
    if dominio == "sem_email":
        return "sem_email"
    return "externo"


def _faixa_inatividade(dias: float | None) -> str:
    if dias is None or np.isnan(dias):
        return "Nunca acessou"
    if dias <= 7:
        return "Ativo (≤7d)"
    if dias <= 30:
        return "Recente (8-30d)"
    if dias <= 60:
        return "Alerta (31-60d)"
    if dias <= 90:
        return "Crítico (61-90d)"
    return "Inativo (>90d)"


def _faixa_antiguidade(dias: float | None) -> str:
    if dias is None or np.isnan(dias):
        return "Desconhecido"
    if dias <= 30:
        return "Novo (≤30d)"
    if dias <= 180:
        return "Recente (31-180d)"
    if dias <= 365:
        return "Intermediário (181-365d)"
    if dias <= 730:
        return "Experiente (1-2 anos)"
    return "Veterano (>2 anos)"

def _extrair_acao_id(acao: str) -> str | None:
    if not isinstance(acao, str):
        return None

    match = re.search(r"\((\d+)\)$", acao)
    return match.group(1) if match else None

# ─────────────────────────────────────────────────────────────────────────────
# LEITURA DAS FONTES
# ─────────────────────────────────────────────────────────────────────────────

def _ler_acoes_sistema(input_dir: Path) -> pd.DataFrame:
    arquivos = list((input_dir / "reference").glob("permissoes_sistema*.xlsx"))

    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum permissoes_sistema*.xlsx encontrado em {input_dir / 'reference'}"
        )

    arquivo = max(arquivos, key=lambda p: p.stat().st_mtime)

    print(f"  Lendo Auxiliar Ações Sistema: {arquivo.name}")

    df = pd.read_excel(arquivo, header=None)

    # Supondo que a primeira coluna contém "Sistema", "Código", etc.
    col0 = df.iloc[:, 0]

    # Identifica linhas que são o cabeçalho "Sistema"
    mask_sistema = col0.astype(str).str.strip().eq("Sistema")

    # Pega a linha seguinte (onde está o nome do sistema)
    df["sistema_temp"] = None

    for idx in df.index[mask_sistema]:
        if idx + 1 in df.index:
            nome_sistema = df.iloc[idx + 1, 0]
            df.loc[idx + 1:, "sistema_temp"] = nome_sistema

    # Preenche para baixo até o próximo sistema
    df["sistema"] = df["sistema_temp"].ffill()

    # Remove linhas que são cabeçalhos
    df = df[~col0.isin(["Sistema", "Código"])]

    df = df.drop(columns=["sistema_temp", 1, 3])
    df = df.reset_index(drop=True)


    df.columns = ['codigo', 'acao', 'sistema']
    df.dropna(subset=['acao'], inplace=True)

    return df




def _ler_permissao_usuario(input_dir: Path) -> pd.DataFrame:
    arquivos = list((input_dir / "usuario").glob("permissao_usuario*.csv"))

    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum permissao_usuario_*.csv encontrado em {input_dir / 'usuario'}"
        )

    arquivo = max(arquivos, key=lambda p: p.stat().st_mtime)

    print(f"  Lendo permissões: {arquivo.name}")

    df = pd.read_csv(arquivo, sep=';')

    # Renomeia a coluna da ação
    df = df.rename(columns={'Unnamed: 0': 'acao'})

    # Encontrar o index da linha
    idx = df.loc[df["acao"] == "Todas as ações"].index
    df = df.drop(index=idx)
    df = df.reset_index(drop=True)

    # MELT
    df_melt = df.melt(
        id_vars="acao",
        var_name="usuario",
        value_name="tem_permissao"
    )

    # Normalização opcional
    df_melt["usuario"] = (
        df_melt["usuario"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # Converter para boolean se vier como texto
    df_melt["tem_permissao"] = (
        df_melt["tem_permissao"]
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(["true", "1", "sim", "x"])
    )

    df_melt['acao_id'] = df_melt["acao"].apply(_extrair_acao_id)

    df_melt.dropna(subset=["acao_id"], inplace=True)

    return df_melt


def _ler_cadastro(input_dir: Path) -> pd.DataFrame:
    """Lê o CSV gerado pelo Selenium (cadastro de usuários)."""
    arquivos = list((input_dir / "usuario").glob("cadastro_usuario_*.csv"))
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum cadastro_usuario_*.csv encontrado em {input_dir / 'usuario'}"
        )
    # Pega o mais recente caso haja múltiplos
    arquivo = max(arquivos, key=lambda p: p.stat().st_mtime)
    print(f"  Lendo cadastro: {arquivo.name}")

    df = pd.read_csv(arquivo, encoding="utf-8-sig")

    # Remove duplicatas (o parser gera linha -1 + linha 0 com o mesmo usuário)
    df = df.drop_duplicates(subset="codigo", keep="first").reset_index(drop=True)

    return df


def _ler_relatorio(input_dir: Path) -> pd.DataFrame:
    """
    Lê o XLSX do relatório de usuários do SIENGE.
    O cabeçalho real está na linha 5 (índice 4).
    Colunas relevantes: Usuário, Nome, Email, Cargo, Admin,
                        Data de ativação, Data de desativação
    """
    arquivo = input_dir / "usuario" / "relatorio_usuario.xlsx"
    if not arquivo.exists():
        raise FileNotFoundError(f"Relatório não encontrado: {arquivo}")
    print(f"  Lendo relatório: {arquivo.name}")

    df = pd.read_excel(arquivo, header=4)

    # Mantém apenas as colunas úteis e renomeia
    df = df[["Usuário", "Nome", "Email", "Cargo", "Admin",
             "Data de ativação", "Data de desativação"]].copy()
    df.columns = [
        "codigo", "nome_relatorio", "email_relatorio", "cargo",
        "admin_relatorio", "data_ativacao_rel", "data_desativacao_rel",
    ]

    # Remove linhas completamente vazias
    df = df[df["codigo"].notna()].reset_index(drop=True)
    df["codigo"] = df["codigo"].astype(str).str.strip().str.upper()
    df["admin_relatorio"] = df["admin_relatorio"].fillna("").astype(str).str.strip()

    return df


def _ler_permissao_obra(input_dir: Path) -> pd.DataFrame:
    """
    Lê o XLSX de autorizações de usuários por obra (SIENGE).

    Estrutura do arquivo — grain externo = USUÁRIO (um bloco por usuário):
      row+0: 'Usuário' | 'CODIGO - NOME USUARIO'
      row+1: 'Funções' | (legenda texto, ignorada)
      row+2: 'Obra'    | 'Funções'
      row+3: None...   | 1 | 2 | 3 ... 23   ← funcao_id por coluna
      row+4..N: 'CODIGO-NOME OBRA' | 'Sim'/None ...

    Retorna fato_permissao_obra (grain = usuário × obra × função):
        codigo_usuario | nome_usuario | obra_codigo | obra_nome
        | funcao_id | funcao_nome | tem_acesso
    """
    arquivos = list((input_dir / "usuario").glob("permissao_obra*.xlsx"))
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum permissao_obra*.xlsx encontrado em {input_dir / 'usuario'}"
        )
    arquivo = max(arquivos, key=lambda p: p.stat().st_mtime)
    print(f"  Lendo permissão por obra: {arquivo.name}")

    wb = load_workbook(arquivo, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))

    # Início de cada bloco: linhas onde col 0 == 'Usuário'
    usuario_starts = [i for i, r in enumerate(rows) if r[0] == "Usuário"]
    print(f"    {len(usuario_starts)} usuários encontrados no arquivo")

    registros: list[dict] = []

    for bloco_idx, usr_row_i in enumerate(usuario_starts):
        # ── Código e nome do usuário ──────────────────────────────────────────
        usr_raw = next(
            (v for j, v in enumerate(rows[usr_row_i]) if v and v != "Usuário"),
            None,
        )
        partes_usr = str(usr_raw).split(" - ", 1) if usr_raw else ["", ""]
        codigo_usuario = partes_usr[0].strip().upper()
        nome_usuario = partes_usr[1].strip() if len(partes_usr) > 1 else None

        # ── Mapeamento col_index → funcao_id (linha usr_row_i + 3) ───────────
        num_row_i = usr_row_i + 3
        num_row = rows[num_row_i]
        col_to_funcao = {
            col_i: int(v)
            for col_i, v in enumerate(num_row)
            if isinstance(v, (int, float)) and int(v) in FUNCOES_OBRA_MAP
        }

        # ── Linhas de obras (até o próximo bloco de usuário) ─────────────────
        fim = (
            usuario_starts[bloco_idx + 1]
            if bloco_idx + 1 < len(usuario_starts)
            else len(rows)
        )

        for row in rows[num_row_i + 1: fim]:
            obra_raw = row[0]

            if not isinstance(obra_raw, str) or not obra_raw.strip():
                continue
            if obra_raw.strip() in ("Obra", "Funções", "Usuário"):
                continue

            # Código da obra: tudo antes do primeiro '-'
            partes_obra = str(obra_raw).split("-", 1)
            obra_codigo = partes_obra[0].strip()
            obra_nome = partes_obra[1].strip() if len(partes_obra) > 1 else obra_raw.strip()

            for col_i, funcao_id in col_to_funcao.items():
                val = row[col_i] if col_i < len(row) else None
                tem_acesso = str(val).strip().lower() == "sim"
                registros.append({
                    "codigo_usuario": codigo_usuario,
                    "nome_usuario": nome_usuario,
                    "obra_codigo": obra_codigo,
                    "obra_nome": obra_nome,
                    "funcao_id": funcao_id,
                    "funcao_nome": FUNCOES_OBRA_MAP[funcao_id],
                    "tem_acesso": tem_acesso,
                })

    return pd.DataFrame(registros)

# ─────────────────────────────────────────────────────────────────────────────
# PONTO DE ENTRADA
# ─────────────────────────────────────────────────────────────────────────────

def executar(input_dir: Path = INPUT_DIR, output_dir: Path = OUTPUT_DIR) -> None:
    """
    Ponto de entrada do transform de Usuários.
    Pode ser chamado diretamente ou pelo main.py do pipeline.
    """
    hoje = pd.Timestamp(date.today())

    # ── 1. Leitura ────────────────────────────────────────────────────────────
    print("\n── 1. Leitura ──────────────────────────────────────────────────────")

    df_cad = _ler_cadastro(input_dir)
    df_rel = _ler_relatorio(input_dir)
    df_perm = _ler_permissao_usuario(input_dir) # separar a lógica disso depois e pegar somente o cod_acao
    dim_acoes_sistema = _ler_acoes_sistema(INPUT_DIR)
    fato_perm_obra_raw = _ler_permissao_obra(input_dir)

    print(f"  Cadastro (CSV):    {len(df_cad):,} registros")
    print(f"  Relatório (XLSX):  {len(df_rel):,} registros")
    print(f"  Permissão (CSV):  {len(df_perm):,} registros")
    print(f"  Permissão obra:    {len(fato_perm_obra_raw):,} registros")

    # ── 2. Join entre as duas fontes ──────────────────────────────────────────
    print("\n── 2. Join cadastro + relatório ────────────────────────────────────")

    df_cad["codigo"] = df_cad["codigo"].astype(str).str.strip().str.upper()

    # Left join: cadastro é a fonte principal (tem provedor_identidade, etc.)
    # O relatório enriquece com cargo
    df = df_cad.merge(
        df_rel[["codigo", "cargo", "admin_relatorio"]],
        on="codigo",
        how="left",
    )

    # Usuários no relatório mas não no cadastro (edge case)
    apenas_rel = set(df_rel["codigo"]) - set(df_cad["codigo"])
    if apenas_rel:
        print(f"   {len(apenas_rel)} usuários só no relatório (ignorados): "
              f"{list(apenas_rel)[:5]}")

    in_ambos = df["cargo"].notna().sum()
    print(f"  Usuários com cargo preenchido: {in_ambos:,} / {len(df):,}")

    # ── 3. Conversão de tipos ─────────────────────────────────────────────────
    print("\n── 3. Conversão de tipos ───────────────────────────────────────────")

    df["data_ativacao"] = _parse_date(df["data_ativacao"])
    df["data_desativacao"] = _parse_date(df["data_desativacao"])
    df["data_ultimo_acesso"] = _parse_date(df["data_ultimo_acesso"])

    df["administrador"] = df["administrador"].astype(str).str.strip().str.lower() == "true"

    # ── 4. Campos derivados ───────────────────────────────────────────────────
    print("\n── 4. Campos derivados ─────────────────────────────────────────────")

    # Domínio do email
    df["dominio_email"] = df["email"].apply(_dominio_email)

    # Tipo de usuário
    df["tipo_usuario"] = df.apply(_tipo_usuario, axis=1)

    # Dias sem acesso (NaT → NaN → será "Nunca acessou")
    df["dias_sem_acesso"] = (hoje - df["data_ultimo_acesso"]).dt.days.astype("float64")

    # Antiguidade
    df["antiguidade_dias"] = (hoje - df["data_ativacao"]).dt.days.astype("float64")

    # Flags de status
    df["flag_ativo"] = df["data_desativacao"].isna()
    df["flag_nunca_acessou"] = df["data_ultimo_acesso"].isna()
    df["flag_inativo_30d"] = df["dias_sem_acesso"] > 30
    df["flag_inativo_60d"] = df["dias_sem_acesso"] > 60
    df["flag_inativo_90d"] = df["dias_sem_acesso"] > 90
    df["flag_acessou_hoje"] = df["data_ultimo_acesso"].dt.normalize() == hoje
    df["flag_sistema_teste"] = df["tipo_usuario"] == "sistema_teste"
    df["flag_externo"] = df["tipo_usuario"] == "externo"
    df["flag_admin"] = df["administrador"]
    df["flag_nunca_acessou_ativo"] = df["flag_nunca_acessou"] & df["flag_ativo"]

    # Faixas categóricas
    df["faixa_inatividade"] = df["dias_sem_acesso"].apply(_faixa_inatividade)
    df["faixa_antiguidade"] = df["antiguidade_dias"].apply(_faixa_antiguidade)

    # Engajamento resumido (para cartões de KPI)
    df["status_engajamento"] = np.select(
        [
            df["flag_nunca_acessou"] & df["flag_ativo"],
            df["dias_sem_acesso"] <= 7,
            df["dias_sem_acesso"] <= 30,
            df["dias_sem_acesso"] <= 90,
            df["dias_sem_acesso"] > 90,
            ~df["flag_ativo"],
        ],
        [
            "Nunca acessou",
            "Ativo recente",
            "Ativo mensal",
            "Em alerta",
            "Inativo",
            "Desativado",
        ],
        default="Desconhecido",
    )

    print(f"  Usuários ativos:          {df['flag_ativo'].sum():,}")
    print(f"  Nunca acessaram (ativos): {df['flag_nunca_acessou_ativo'].sum():,}")
    print(f"  Inativos >30d:            {df['flag_inativo_30d'].sum():,}")
    print(f"  Inativos >90d:            {df['flag_inativo_90d'].sum():,}")
    print(f"  Externos:                 {df['flag_externo'].sum():,}")
    print(f"  Administradores:          {df['flag_admin'].sum():,}")

    # ── 5. Montar dim_usuario ─────────────────────────────────────────────────
    print("\n── 5. dim_usuario ──────────────────────────────────────────────────")

    dim_usuario = df[[
        "codigo",
        "nome",
        "email",
        "cargo",
        "administrador",
        "provedor_identidade",
        "dominio_email",
        "tipo_usuario",
        "data_ativacao",
        "data_desativacao",
        "flag_ativo",
        "flag_admin",
        "flag_externo",
        "flag_sistema_teste",
        "faixa_antiguidade",
        "antiguidade_dias",
    ]].copy()

    dim_usuario.insert(0, "id_usuario", range(1, len(dim_usuario) + 1))
    print(f"  dim_usuario: {dim_usuario.shape}")

    # ── 6. Montar fato_acesso_usuario ─────────────────────────────────────────
    print("\n── 6. fato_acesso_usuario ──────────────────────────────────────────")

    fato = df[[
        "codigo",
        "nome",
        "cargo",
        "tipo_usuario",
        "dominio_email",
        "provedor_identidade",

        # Datas
        "data_ativacao",
        "data_desativacao",
        "data_ultimo_acesso",

        # Métricas
        "dias_sem_acesso",
        "antiguidade_dias",

        # Flags
        "flag_ativo",
        "flag_nunca_acessou",
        "flag_nunca_acessou_ativo",
        "flag_inativo_30d",
        "flag_inativo_60d",
        "flag_inativo_90d",
        "flag_acessou_hoje",
        "flag_admin",
        "flag_externo",
        "flag_sistema_teste",

        # Faixas
        "faixa_inatividade",
        "faixa_antiguidade",
        "status_engajamento",
    ]].copy()

    # Surrogate key via join com dim
    fato = fato.merge(
        dim_usuario[["id_usuario", "codigo"]],
        on="codigo",
        how="left",
    )
    fato.insert(0, "id_usuario", fato.pop("id_usuario"))

    fato["data_carga"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"  fato_acesso_usuario: {fato.shape}")
    print("\n  Distribuição status_engajamento:")
    for status, cnt in fato["status_engajamento"].value_counts().items():
        print(f"    {status:<20} {cnt:>4}")


    # ── 7. dim_permissao  ─────────────────
    fato_relacao = fato[fato['codigo'] != "ANACLARAAPRENDIZ"]

    dim_permissao = pd.merge(
        df_perm,
        fato_relacao[['nome', 'id_usuario']], left_on='usuario', right_on='nome', how='left'
    )

    dim_permissao = dim_permissao[
        dim_permissao['id_usuario'].notna()
    ]

    dim_permissao = dim_permissao[['id_usuario','nome', 'usuario', 'acao_id', 'tem_permissao']]

    dim_permissao['id_usuario'] = pd.to_numeric(dim_permissao['id_usuario'], errors='coerce')
    dim_permissao.dropna(subset=['id_usuario', 'nome'], inplace=True)

    # ── 8. fato_permissao_obra ────────────────────────────────────────────────
    print("\n── 8. fato_permissao_obra ──────────────────────────────────────────")

    # Tabela de lookup: codigo → id_usuario
    codigo_para_id = fato[["codigo", "id_usuario"]].drop_duplicates("codigo")

    fato_permissao_obra = fato_perm_obra_raw.merge(
        codigo_para_id,
        left_on="codigo_usuario",
        right_on="codigo",
        how="left",
    ).drop(columns=["codigo"])

    fato_permissao_obra.insert(0, "id_usuario", fato_permissao_obra.pop("id_usuario"))

    sem_match = fato_permissao_obra["id_usuario"].isna().sum()
    if sem_match:
        usuarios_sem_match = (
            fato_perm_obra_raw[
                ~fato_perm_obra_raw["codigo_usuario"].isin(codigo_para_id["codigo"])
            ]["codigo_usuario"].unique()
        )
        print(f"  ⚠ {len(usuarios_sem_match)} usuário(s) sem match no fato "
              f"(só na obra): {list(usuarios_sem_match)}")

    print(f"  fato_permissao_obra: {fato_permissao_obra.shape}")
    print(f"  Obras distintas:     {fato_permissao_obra['obra_nome'].nunique()}")
    print(f"  Usuários distintos:  {fato_permissao_obra['codigo_usuario'].nunique()}")
    print(f"  Com acesso=True:     {fato_permissao_obra['tem_acesso'].sum():,}")


    # ── 8. Exportação ─────────────────────────────────────────────────────────
    print("\n── 8. Exportação ───────────────────────────────────────────────────")

    salvar_tabela(dim_usuario, "dim_usuario", output_dir)
    salvar_tabela(fato, "fato_acesso_usuario", output_dir)
    salvar_tabela(dim_permissao, "dim_permissao", output_dir)
    salvar_tabela(dim_acoes_sistema, "dim_acoes_sistema", output_dir)
    salvar_tabela(fato_permissao_obra, "fato_permissao_obra", output_dir)

    print("\n── Resumo final ────────────────────────────────────────────────────")
    for nome, tabela in {
        "dim_usuario": dim_usuario,
        "fato_acesso_usuario": fato,
        "dim_permissao": dim_permissao,
    }.items():
        print(f"  {nome:<28} {str(tabela.shape):>12}")

    print("""
── Relacionamentos Power BI ──────────────────────────────────────────────────
  dim_usuario[id_usuario]          → fato_acesso_usuario[id_usuario]
  dim_permissao[id_usuario]     → fato_acesso_usuario[id_usuario]
    (relacionamento via coluna texto — ou criar surrogate key se necessário)
""")


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    executar()
