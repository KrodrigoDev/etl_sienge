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

import re
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from stages.transform.utils.normalizer import (
    normalizar_colunas,
    salvar_tabela,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

pasta_origem = Path(__file__).resolve().parents[2]

INPUT_DIR  = pasta_origem / "stages" / "transform" / "input"
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


# ─────────────────────────────────────────────────────────────────────────────
# LEITURA DAS FONTES
# ─────────────────────────────────────────────────────────────────────────────

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

    print(f"  Cadastro (CSV):    {len(df_cad):,} registros")
    print(f"  Relatório (XLSX):  {len(df_rel):,} registros")

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

    df["data_ativacao"]      = _parse_date(df["data_ativacao"])
    df["data_desativacao"]   = _parse_date(df["data_desativacao"])
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
    df["flag_ativo"]          = df["data_desativacao"].isna()
    df["flag_nunca_acessou"]  = df["data_ultimo_acesso"].isna()
    df["flag_inativo_30d"]    = df["dias_sem_acesso"] > 30
    df["flag_inativo_60d"]    = df["dias_sem_acesso"] > 60
    df["flag_inativo_90d"]    = df["dias_sem_acesso"] > 90
    df["flag_acessou_hoje"]   = df["data_ultimo_acesso"].dt.normalize() == hoje
    df["flag_sistema_teste"]  = df["tipo_usuario"] == "sistema_teste"
    df["flag_externo"]        = df["tipo_usuario"] == "externo"
    df["flag_admin"]          = df["administrador"]
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

    # ── 7. dim_faixa_inatividade (lookup para filtros no BI) ─────────────────
    ordem = [
        "Ativo (≤7d)", "Recente (8-30d)", "Alerta (31-60d)",
        "Crítico (61-90d)", "Inativo (>90d)", "Nunca acessou",
    ]
    dim_faixa_inatividade = pd.DataFrame({
        "id_faixa":          range(1, len(ordem) + 1),
        "faixa_inatividade": ordem,
        "limite_dias_min":   [0,   8,  31,  61,  91, None],
        "limite_dias_max":   [7,  30,  60,  90, None, None],
        "cor_hex":           [
            "#22c55e",  # verde — ativo
            "#86efac",  # verde claro — recente
            "#facc15",  # amarelo — alerta
            "#f97316",  # laranja — crítico
            "#ef4444",  # vermelho — inativo
            "#94a3b8",  # cinza — nunca acessou
        ],
    })

    # ── 8. Exportação ─────────────────────────────────────────────────────────
    print("\n── 8. Exportação ───────────────────────────────────────────────────")

    salvar_tabela(dim_usuario,          "dim_usuario",          output_dir)
    salvar_tabela(fato,                 "fato_acesso_usuario",  output_dir)
    salvar_tabela(dim_faixa_inatividade,"dim_faixa_inatividade",output_dir)

    print("\n── Resumo final ────────────────────────────────────────────────────")
    for nome, tabela in {
        "dim_usuario":           dim_usuario,
        "fato_acesso_usuario":   fato,
        "dim_faixa_inatividade": dim_faixa_inatividade,
    }.items():
        print(f"  {nome:<28} {str(tabela.shape):>12}")

    print("""
── Relacionamentos Power BI ──────────────────────────────────────────────────
  dim_usuario[id_usuario]          → fato_acesso_usuario[id_usuario]
  dim_faixa_inatividade[faixa]     → fato_acesso_usuario[faixa_inatividade]
    (relacionamento via coluna texto — ou criar surrogate key se necessário)
""")


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    executar()