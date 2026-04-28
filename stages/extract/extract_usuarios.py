"""
stages/extract/extract_usuarios.py
-----------------------------------------
Extrai o relatório de Usuários do SIENGE e salva como CSV.

Fluxo:
  1. Login via sessão salva no perfil Edge
  2. Navega para a URL do cadastro de usuário
  3. Clica em "Consultar"
  4. Aguarda a tabela carregar
  5. Extrai os dados da tabela via BeautifulSoup
  6. Cria um DataFrame e salva como CSV
  7. (Opcional) Navega para o relatório e baixa o XLSX
"""

from __future__ import annotations

import subprocess
import logging
import shutil
from datetime import date
from pathlib import Path
from time import sleep

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

# ── URLs ──────────────────────────────────────────────────────────────────────
URL_CADASTRO_USUARIO = (
    f"{BASE_URL}/8/index.html"
    "#/common/page/435"
)

URL_RELATORIO_USUARIO = (
    f"{BASE_URL}/8/index.html"
    "#/common/page/2987"
)

# ID da tabela no HTML
TABELA_ID = "tabelaUsuarioRow"

# Colunas que queremos extrair
COLUNAS = [
    "codigo",
    "nome",
    "email",
    "administrador",
    "provedor_identidade",
    "data_ativacao",
    "data_desativacao",
    "data_ultimo_acesso",
]


# ── Função auxiliar de parsing ────────────────────────────────────────────────

def _parsear_tabela_usuarios(html: str) -> pd.DataFrame:
    """
    Recebe o HTML da página e extrai as linhas da tabela de usuários.

    A tabela tem o id 'tabelaUsuarioRow'. Cada <tr> com o atributo
    linha="true" representa um usuário. A primeira coluna é um checkbox
    de seleção (ignorado). As colunas de dados começam na segunda <td>.

    Estrutura das colunas (índice 0-based, contando a partir da 2ª <td>):
      [0] checkbox seleção  → ignorado
      [1] checkbox + hidden com o código  → extrai value do hidden input
      [2] codigo (texto)
      [3] nome
      [4] email
      [5] checkbox administrador  → True se checked, False caso contrário
      [6] provedor_identidade
      [7] data_ativacao  (dentro de <span tipo="DATE">)
      [8] data_desativacao
      [9] data_ultimo_acesso
      [10] botão editar  → ignorado
    """
    soup = BeautifulSoup(html, "html.parser")
    tabela = soup.find("table", {"id": TABELA_ID})

    if tabela is None:
        logger.warning("Tabela '%s' não encontrada no HTML.", TABELA_ID)
        return pd.DataFrame(columns=COLUNAS)

    registros: list[dict] = []

    for tr in tabela.find_all("tr", attrs={"linha": "true"}):
        tds = tr.find_all("td", recursive=False)

        # A tabela tem 11 <td> por linha (incluindo a coluna de estado/imagem)
        # Índices relevantes (desconsiderando a 1ª td de estado):
        #   td[0]  → estado (img)          → ignorar
        #   td[1]  → checkbox seleção + hidden código
        #   td[2]  → código (texto)
        #   td[3]  → nome
        #   td[4]  → email
        #   td[5]  → checkbox administrador
        #   td[6]  → provedor de identidade
        #   td[7]  → data de ativação
        #   td[8]  → data de desativação
        #   td[9]  → data de último acesso
        #   td[10] → botão editar           → ignorar

        if len(tds) < 10:
            # linha incompleta, pular
            continue

        # ── Código ────────────────────────────────────────────────────────────
        # Pegamos o valor do input hidden dentro de td[1], que é mais confiável
        hidden = tds[1].find("input", {"type": "hidden"})
        codigo = hidden["value"].strip() if hidden else tds[2].get_text(strip=True)

        # ── Nome ──────────────────────────────────────────────────────────────
        nome = tds[3].get_text(strip=True)

        # ── E-mail ────────────────────────────────────────────────────────────
        email = tds[4].get_text(strip=True)

        # ── Administrador ─────────────────────────────────────────────────────
        chk_admin = tds[5].find("input", {"type": "checkbox"})
        administrador = chk_admin is not None and chk_admin.has_attr("checked")

        # ── Provedor de identidade ────────────────────────────────────────────
        provedor = tds[6].get_text(strip=True)

        # ── Datas (dentro de <span tipo="DATE">) ──────────────────────────────
        def _data(td) -> str | None:
            span = td.find("span", {"tipo": "DATE"})
            if span is None:
                return None
            texto = span.get_text(strip=True)
            return texto if texto and texto != "\xa0" else None

        data_ativacao = _data(tds[7])
        data_desativacao = _data(tds[8])
        data_ultimo = _data(tds[9])

        registros.append(
            {
                "codigo": codigo,
                "nome": nome,
                "email": email,
                "administrador": administrador,
                "provedor_identidade": provedor,
                "data_ativacao": data_ativacao,
                "data_desativacao": data_desativacao,
                "data_ultimo_acesso": data_ultimo,
            }
        )

    logger.info("Total de registros extraídos da tabela: %d", len(registros))
    return pd.DataFrame(registros, columns=COLUNAS)


def _entrar_iframe(driver) -> None:
    logger.info("Entrando no iframe do formulário")
    driver.switch_to.default_content()
    frame = driver.find_element(By.ID, "iFramePage")
    driver.switch_to.frame(frame)


# ── Função principal ──────────────────────────────────────────────────────────

def extrair_usuario(
        destino: Path | None = None,
) -> Path:
    """
    Extrai a listagem de usuários do SIENGE e salva como CSV.

    Parâmetros
    ----------
    data_inicio : str | None
        Reservado para filtros futuros (não utilizado nesta versão).
    destino : Path | None
        Pasta onde o CSV será salvo. Padrão: <download_dir>/usuario/

    Retorna
    -------
    Path
        Caminho completo do arquivo CSV gerado.
    """
    req = SeleniumRequester()
    req.ensure_login()

    destino = destino or (req.download_dir / "usuario")
    destino.mkdir(parents=True, exist_ok=True)

    driver = req.get_driver()
    wdw = req.waiter(driver)

    try:
        # ── 1. Navegação inicial (menu, etc.) ─────────────────────────────────
        req.navegacao_inicial(driver, wdw)

        # ── 2. Navega para o cadastro de usuário ──────────────────────────────
        logger.info("Navegando para o cadastro de usuário: %s", URL_CADASTRO_USUARIO)
        driver.get(URL_CADASTRO_USUARIO)
        sleep(2)

        # ── 3. Entra no iframe e preenche filtros fixos ──────────────────────
        _entrar_iframe(driver)

        # ── 4. Clica em "Consultar" ───────────────────────────────────────────
        logger.info("Clicando em Consultar...")
        req.aguardar_e_clicar(
            wdw,
            (By.XPATH, '//input[@type="submit" and @value="Consultar"]'),
            "Consultar",
        )

        # ── 4. Aguarda a tabela aparecer ──────────────────────────────────────
        logger.info("Aguardando tabela de usuários carregar...")
        wdw.until(
            EC.presence_of_element_located((By.ID, TABELA_ID)),
            message=f"Tabela '{TABELA_ID}' não apareceu após Consultar.",
        )
        # Pequena pausa extra para garantir que todas as linhas foram renderizadas
        sleep(2)

        # ── 5. Extrai o HTML atual e parseia a tabela ─────────────────────────
        logger.info("Extraindo HTML da página...")
        html_pagina = driver.page_source
        df = _parsear_tabela_usuarios(html_pagina)

        if df.empty:
            logger.warning("Nenhum registro encontrado na tabela. Verifique a consulta.")
        else:
            logger.info("Registros encontrados: %d", len(df))

        # ── 6. Salva como CSV ─────────────────────────────────────────────────
        nome_csv = f"cadastro_usuario_{date.today().year}.csv"
        arquivo_csv = destino / nome_csv
        df.to_csv(arquivo_csv, index=False, encoding="utf-8-sig")
        logger.info("CSV salvo em: %s", arquivo_csv)

        # ── 7. Relatório XLSX ──────────────────────────────────────
        logger.info("Navegando para o relatório de usuário: %s", URL_RELATORIO_USUARIO)
        driver.get(URL_RELATORIO_USUARIO)
        sleep(2)

        # ── 8. Entra no iframe e aperta no botão de visualizar ──────────────────────
        driver.switch_to.parent_frame()
        _entrar_iframe(driver)

        sleep(0.5)
        req.aguardar_e_clicar(
            wdw,
            (By.NAME, "btFiltrar"),
            "Botão Visualizar",
        )

        arquivo_baixado = req.aguardar_download(
            extensao=".xlsx",
            timeout=60,
        )

        nome_xlsx = "relatorio_usuario.xlsx"
        arquivo_xlsx = destino / nome_xlsx
        shutil.move(str(arquivo_baixado), str(arquivo_xlsx))
        logger.info("XLSX salvo em: %s", arquivo_xlsx)

        return arquivo_csv

    finally:

        try:
            driver.quit()
        except Exception:
            pass

        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "msedge.exe", "/T"],
                capture_output=True,
            )
        except Exception:
            pass

        logger.info("Driver encerrado.")


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    caminho = extrair_usuario()
    print(f"Extração concluída: {caminho}")
