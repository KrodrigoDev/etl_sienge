"""
stages/extract/extract_adiantamento.py
----------------------------------
Extrai o relatório de Adiantamento do SIENGE e salva como XLSX.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path
from time import sleep

import pandas as pd

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    InvalidSessionIdException,
    WebDriverException,
)

from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

URL_ADIANTAMENTO = f"{BASE_URL}/8/index.html#/common/page/660"


# ─────────────────────────────────────────────────────────────────────────────
# EXTRAÇÃO PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def extrair_adiantamento(
        destino: Path | None = None,
        data_inicio: str | None = None,
) -> None:
    if data_inicio is None:
        data_inicio = "01/01/2014"

    req = SeleniumRequester()
    req.ensure_login()

    destino = destino or (req.download_dir / "adiantamento")
    destino.mkdir(parents=True, exist_ok=True)

    driver = req.get_driver()
    wdw = req.waiter(driver)

    try:
        # ── 1. Acesso inicial ────────────────────────────────────────────────
        req.navegacao_inicial(driver, wdw)

        # ── 2. Navega para o relatório ───────────────────────────────────────
        logger.info("Navegando para o relatório de adiantamentos...")
        driver.get(URL_ADIANTAMENTO)
        sleep(2)

        req.fechar_popup_novidade(wdw)

        # ── 3. Entra no iframe e preenche filtros fixos ──────────────────────
        logger.info("Entrando no iframe do formulário")
        driver.switch_to.default_content()
        frame = driver.find_element(By.ID, "iFramePage")
        driver.switch_to.frame(frame)

        logger.info("Preenchendo filtros fixos")
        req.aguardar_e_clicar(wdw, (By.CSS_SELECTOR, "#flTituloVinculadoA"))
        req.aguardar_e_clicar(wdw, (By.CSS_SELECTOR, "#flOrdenacaoA"))
        req.preencher_campo(
            wdw,
            (By.CSS_SELECTOR, 'input[name="dtEmissaoInicio"]'),
            data_inicio,
        )
        sleep(0.5)

        # ── 4. Carrega lista de empresas ─────────────────────────────────────
        dim_empresas = pd.read_csv(
            req.project_root / "stages/extract/reference/dim_empresa.csv",
            sep=";",
        )
        lista_empresas = (
            dim_empresas["cod_empresa"].dropna().astype(str).unique().tolist()
        )
        logger.info("Total de empresas: %d", len(lista_empresas))

        janela_principal = driver.current_window_handle

        # ── 5. Loop por empresa ──────────────────────────────────────────────
        for cod_empresa in lista_empresas:
            logger.info("Empresa: %s", cod_empresa)

            try:  # ── proteção por empresa ────────────────────────────────────

                input_empresa = req.aguardar_e_clicar(
                    wdw,
                    (By.ID, "entity.empresa.cdEmpresaView"),
                    "Campo Empresa",
                )
                input_empresa.send_keys(cod_empresa)
                input_empresa.send_keys(Keys.ENTER)
                sleep(1)

                req.aguardar_e_clicar(
                    wdw,
                    (By.XPATH, '//input[@type="submit" and @value="Visualizar"]'),
                    "Botão Visualizar",
                )

                if req.verificar_sem_dados(driver, wdw):
                    logger.info("  Sem dados — pulando empresa %s", cod_empresa)
                    driver.switch_to.window(janela_principal)
                    driver.switch_to.default_content()
                    driver.switch_to.frame(frame)
                    continue

                try:
                    arquivo_baixado = req.aguardar_download(
                        extensao=".xlsx",
                        timeout=240,
                    )
                except TimeoutError:
                    logger.warning(
                        "  Timeout no download — empresa %s, pulando", cod_empresa,
                    )
                    driver.switch_to.window(janela_principal)
                    driver.switch_to.default_content()
                    driver.switch_to.frame(frame)
                    continue

                nome_final = f"relatorio - {cod_empresa}.xlsx"
                arquivo_final = destino / nome_final
                shutil.move(str(arquivo_baixado), str(arquivo_final))
                logger.info("  Salvo: %s", arquivo_final)

                sleep(0.5)

                driver.switch_to.window(janela_principal)
                driver.switch_to.default_content()
                driver.switch_to.frame(frame)

                input_empresa = req.aguardar_e_clicar(
                    wdw,
                    (By.ID, "entity.empresa.cdEmpresaView"),
                    "Campo Empresa",
                )
                input_empresa.send_keys(Keys.CONTROL, "a")
                input_empresa.send_keys(Keys.DELETE)

            except (InvalidSessionIdException, WebDriverException) as e_sessao:
                # ── Sessão corrompida — reinicia driver e segue ───────────────
                logger.warning(
                    "Sessão corrompida na empresa %s — reiniciando driver: %s",
                    cod_empresa, e_sessao,
                )

                driver, wdw = req.reiniciar_driver(driver, URL_ADIANTAMENTO)

                driver.switch_to.default_content()
                frame = driver.find_element(By.ID, "iFramePage")
                driver.switch_to.frame(frame)
                janela_principal = driver.current_window_handle

                # Repreenche filtros fixos após reinício
                req.aguardar_e_clicar(wdw, (By.CSS_SELECTOR, "#flTituloVinculadoA"))
                req.aguardar_e_clicar(wdw, (By.CSS_SELECTOR, "#flOrdenacaoA"))
                req.preencher_campo(
                    wdw,
                    (By.CSS_SELECTOR, 'input[name="dtEmissaoInicio"]'),
                    data_inicio,
                )
                sleep(0.5)

                logger.info(
                    "Driver reiniciado — arquivo pendente da empresa %s "
                    "será recuperado no próximo run.",
                    cod_empresa,
                )
                continue

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
            logger.info("Processos msedge encerrados via taskkill.")
        except Exception as e_kill:
            logger.warning("taskkill falhou: %s", e_kill)

        logger.info("Driver encerrado.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    extrair_adiantamento()
    print("Extração concluída.")
