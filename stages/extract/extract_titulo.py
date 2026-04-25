"""
stages/extract/extract_titulo.py
----------------------------------
Extrai o relatório de Títulos do SIENGE e salva como XLSX.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from datetime import date
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

URL_TITULO = f"{BASE_URL}/8/index.html#/common/page/373"


# ─────────────────────────────────────────────────────────────────────────────
# EXTRAÇÃO PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def extrair_titulo(
        destino: Path | None = None,
) -> None:
    """
    Executa a extração do relatório de títulos empresa a empresa pelos anos de emissão.
    """

    req = SeleniumRequester()
    req.ensure_login()

    destino = destino or (req.download_dir / "titulo")
    destino.mkdir(parents=True, exist_ok=True)

    driver = req.get_driver()
    wdw = req.waiter(driver)

    try:
        # ── 1. Acesso inicial ────────────────────────────────────────────────
        req.navegacao_inicial(driver, wdw)

        # ── 2. Navega para o relatório ───────────────────────────────────────
        logger.info("Navegando para o relatório de títulos...")
        driver.get(URL_TITULO)
        sleep(2)

        req.fechar_popup_novidade(wdw)

        # ── 3. Entra no iframe ───────────────────────────────────────────────
        logger.info("Entrando no iframe do formulário")
        driver.switch_to.default_content()
        frame = driver.find_element(By.ID, "iFramePage")
        driver.switch_to.frame(frame)

        # ── 4. Carrega lista de empresas ─────────────────────────────────────
        dim_empresas = pd.read_csv(
            req.project_root / "stages/transform/output/fato_consulta_parcela.csv",
            sep=";",
        )

        lista_empresas = (
            dim_empresas["cod_empresa"].dropna().astype(int).unique().tolist()
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
                sleep(0.5)

                for ano in range(2022, date.today().year + 1):
                    ano_atual = date.today().year

                    # ── Skip ─────────────────────────────────────────────────
                    nome_final = f"relatorio-{cod_empresa}-{ano}.xlsx"
                    arquivo_final = destino / nome_final

                    if arquivo_final.exists():
                        baixado_hoje = (
                                date.fromtimestamp(arquivo_final.stat().st_mtime)
                                == date.today()
                        )
                        ano_fechado = ano < ano_atual
                        if ano_fechado or baixado_hoje:
                            motivo = "ano fechado" if ano_fechado else "já baixado hoje"
                            logger.info(
                                "  Pulando empresa %s ano %s (%s)",
                                cod_empresa, ano, motivo,
                            )
                            continue

                    # ── Datas do filtro ───────────────────────────────────────
                    dt_inicio = f"01/01/{ano}"
                    dt_fim = (
                        date.today().strftime("%d/%m/%Y")
                        if ano == ano_atual
                        else f"31/12/{ano}"
                    )

                    req.preencher_campo(
                        wdw,
                        (By.CSS_SELECTOR, 'input[name="dtInicio"]'),
                        dt_inicio,
                    )
                    req.preencher_campo(
                        wdw,
                        (By.CSS_SELECTOR, 'input[name="dtFim"]'),
                        dt_fim,
                    )

                    req.aguardar_e_clicar(
                        wdw,
                        (By.XPATH, '//input[@type="submit" and @value="Visualizar"]'),
                        "Botão Visualizar",
                    )

                    sleep(1.5)

                    # ── Sem dados ─────────────────────────────────────────────
                    if req.verificar_sem_dados(driver, wdw):
                        logger.info("Sem dados — empresa %s ano %s", cod_empresa, ano)
                        driver.switch_to.window(janela_principal)
                        driver.switch_to.default_content()
                        driver.switch_to.frame(frame)
                        continue

                    # ── Download ──────────────────────────────────────────────
                    try:
                        arquivo_baixado = req.aguardar_download(
                            extensao=".xlsx", timeout=240
                        )
                    except TimeoutError:
                        logger.warning(
                            "  Timeout no download — empresa %s ano %s, pulando",
                            cod_empresa, ano,
                        )
                        driver.switch_to.window(janela_principal)
                        driver.switch_to.default_content()
                        driver.switch_to.frame(frame)
                        continue

                    shutil.move(str(arquivo_baixado), str(arquivo_final))
                    logger.info("  Salvo: %s", arquivo_final)

                    driver.switch_to.window(janela_principal)
                    driver.switch_to.default_content()
                    driver.switch_to.frame(frame)

                # ── Limpa campo empresa ───────────────────────────────────────
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

                driver, wdw = req.reiniciar_driver(driver, URL_TITULO)

                driver.switch_to.default_content()
                frame = driver.find_element(By.ID, "iFramePage")
                driver.switch_to.frame(frame)
                janela_principal = driver.current_window_handle

                sleep(2)

                logger.info(
                    "Driver reiniciado — arquivos pendentes da empresa %s "
                    "serão recuperados no próximo run.",
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
    extrair_titulo()
    print("Extração concluída.")
