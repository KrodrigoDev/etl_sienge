"""
stages/extract/extract_permissao_usuarios.py
-----------------------------------------
Extrai o relatório de Usuários do SIENGE e salva como CSV.

Observação: isso só deve rodar depois depos do trasnform_usuario, que por sua vez depende do extract_usuario

Fluxo:
  1. Login via sessão salva no perfil Edge
  2. Navega para a URL de permissão de usuário
  3. Pesquisa e preenche o nome de cada usuário
  3. Clica em "Consultar"
  4. Aguarda a tabela carregar
  5. Clica em gerar relatório
  6. Escolhe o formato  CSV e realiza o download
"""

from __future__ import annotations

import subprocess
import logging
import shutil
from pathlib import Path
from time import sleep

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException


from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)
# ── URLs ──────────────────────────────────────────────────────────────────────
URL_CADASTRO_USUARIO = (
    f"{BASE_URL}/8/index.html"
    "#/seguranca/autorizacao/gestao-de-permissao"
)


# ── Função principal ──────────────────────────────────────────────────────────

def extrair_permissao_usuario(
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
        logger.info("Navegando para o permissão de usuário: %s", URL_CADASTRO_USUARIO)
        driver.get(URL_CADASTRO_USUARIO)
        sleep(2)

        req.fechar_popup_novidade(wdw)
        sleep(0.5)
        req.fechar_popup_novidade(wdw, txt_locator='//button[normalize-space()="Entendi"]')

        # ── 3. Preenchendo o nome de cada usuário ──────────────────────────────
        df_usuario = pd.read_csv('../transform/output/dim_usuario.csv', sep=';')
        nomes_usuarios = df_usuario['nome'].unique().tolist()

        logger.info("Total de usuários a serem buscados: %s", len(nomes_usuarios))
        for nome in nomes_usuarios:

            logger.info("Selecionando usuário: %s", nome)

            campo_usuario = req.aguardar_e_clicar(
                wdw,
                (By.XPATH, '//input[@placeholder="Pesquisar usuário"]'),
                "Campo pesquisar usuário",
            )

            campo_usuario.send_keys(f"{nome}")

            try:
                # Aguarda aparecer opção
                req.aguardar_presenca(
                    wdw,
                    (
                        By.XPATH,
                        f'//li[@role="option" and starts-with(normalize-space(), "{nome}")]'
                    ),
                )

                # Clica na opção
                req.aguardar_e_clicar(
                    wdw,
                    (
                        By.XPATH,
                        f'//li[@role="option" and starts-with(normalize-space(), "{nome}")]'
                    ),
                )

            except TimeoutException:
                logger.warning("Usuário não encontrado: %s", nome)

                # limpa campo antes de ir para o próximo
                campo_usuario.send_keys(Keys.CONTROL, "a")
                campo_usuario.send_keys(Keys.DELETE)

                continue

            # limpa campo após sucesso
            campo_usuario.send_keys(Keys.CONTROL, "a")
            campo_usuario.send_keys(Keys.DELETE)

            sleep(1)

        # ── 4. Clica em "Consultar" ───────────────────────────────────────────
        logger.info("Clicando em Consultar...")
        req.aguardar_e_clicar(
            wdw,
            (By.ID, 'btn-consultar-autorizacoes'),
            "Consultar",
        )

        # ── 4. Aguarda a tabela aparecer ──────────────────────────────────────
        logger.info("Aguardando tabela de usuários carregar...")
        req.aguardar_carregamento_tabela(driver)
        # Pequena pausa extra para garantir que todas as linhas foram renderizadas
        sleep(10)

        # ── 5. Exporta CSV ────────────────────────────────────────────────────
        logger.info("Exportando CSV do estoque...")
        req.exportar_csv_modal(wdw)

        # ── 6. Aguarda download ─────────────────────────────────────────────────
        arquivo_baixado = req.aguardar_download(
            extensao=".csv",
            timeout=60,
        )

        # ── 10. Move para pasta de destino ────────────────────────────────────
        nome_csv = "permissao_usuario.csv"
        arquivo_csv = destino / nome_csv
        shutil.move(str(arquivo_baixado), str(arquivo_csv))
        logger.info("Csv salvo em: %s", arquivo_csv)

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
    caminho = extrair_permissao_usuario()
    print(f"Extração concluída: {caminho}")
