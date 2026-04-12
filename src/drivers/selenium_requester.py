"""
src/drivers/selenium_requester.py
----------------------------------
Driver Selenium configurado para o SIENGE.

Responsabilidades:
  - Criar e configurar o webdriver Edge
  - Gerenciar sessão de login (perfil persistido)
  - Expor helpers genéricos de interação (aguardar download, clicar, preencher)
    reutilizados por todos os extractors
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from time import sleep, time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

logger = logging.getLogger(__name__)

BASE_URL   = "https://telesil.sienge.com.br/sienge"
LOGIN_URL  = f"{BASE_URL}/index.jsp"
TIMEOUT    = 30   # segundos padrão para WebDriverWait
DL_TIMEOUT = 120  # segundos máximos para aguardar download


class SeleniumRequester:

    def __init__(self, download_dir: Path | None = None):

        self.project_root = Path(__file__).resolve().parents[3]

        self.path_profile = Path(r"C:\SeleniumPerfil\Edge")

        self.download_dir = download_dir or (
            self.project_root / "stages" / "extract" / "downloads"
        )

        self._create_directories()

    # ── Setup ──────────────────────────────────────────────────────────────────

    def _create_directories(self) -> None:
        self.path_profile.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def get_driver(self) -> webdriver.Edge:
        options = Options()
        options.add_argument(f"--user-data-dir={self.path_profile}")
        options.add_argument("--profile-directory=Default")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("prefs", {
            "download.default_directory":  str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade":  True,
            "safebrowsing.enabled":        True,
        })
        driver = webdriver.Edge(options=options)
        logger.info("Driver Edge iniciado → downloads em %s", self.download_dir)
        return driver

    def ensure_login(self) -> None:
        """
        Garante que existe uma sessão salva no perfil Edge.
        Se o perfil estiver vazio, abre o browser e aguarda login manual.
        """
        if not any(self.path_profile.iterdir()):
            driver = self.get_driver()
            driver.get(LOGIN_URL)
            input("Faça login manualmente e pressione Enter para salvar a sessão...")
            driver.quit()
            logger.info("Sessão salva em %s", self.path_profile)

    # ── Helpers genéricos ─────────────────────────────────────────────────────

    @staticmethod
    def waiter(driver: webdriver.Edge, timeout: int = TIMEOUT) -> WebDriverWait:
        return WebDriverWait(driver, timeout)

    @staticmethod
    def aguardar_e_clicar(
        wdw: WebDriverWait,
        locator: tuple,
        descricao: str = "",
    ) -> WebElement:
        """Aguarda elemento ficar clicável e clica."""
        elemento = wdw.until(EC.element_to_be_clickable(locator))
        elemento.click()
        if descricao:
            logger.debug("Clicou: %s", descricao)
        return elemento

    @staticmethod
    def preencher_campo(
        wdw: WebDriverWait,
        locator: tuple,
        valor: str,
        limpar: bool = True,
    ) -> WebElement:
        """Aguarda campo, limpa e preenche com o valor informado."""
        campo = wdw.until(EC.element_to_be_clickable(locator))
        campo.click()
        if limpar:
            campo.send_keys(Keys.CONTROL + "a")
        campo.send_keys(valor)
        return campo

    @staticmethod
    def aguardar_visivel(
        wdw: WebDriverWait,
        locator: tuple,
    ) -> WebElement:
        """Aguarda elemento ficar visível."""
        return wdw.until(EC.visibility_of_element_located(locator))

    @staticmethod
    def aguardar_presenca(
        wdw: WebDriverWait,
        locator: tuple,
    ) -> WebElement:
        """Aguarda elemento estar presente no DOM."""
        return wdw.until(EC.presence_of_element_located(locator))

    def aguardar_download(
        self,
        extensao: str = ".csv",
        timeout: int = DL_TIMEOUT,
    ) -> Path:
        """
        Aguarda até um arquivo com a extensão informada aparecer na pasta
        de downloads. Retorna o Path do arquivo baixado.

        Ignora arquivos .crdownload (download em andamento).
        """
        inicio = time()
        logger.info("Aguardando download (.%s) em %s ...", extensao, self.download_dir)

        while time() - inicio < timeout:
            arquivos = [
                f for f in self.download_dir.iterdir()
                if f.suffix == extensao and not f.name.endswith(".crdownload")
            ]
            if arquivos:
                # retorna o mais recente
                arquivo = max(arquivos, key=lambda f: f.stat().st_mtime)
                logger.info("Download concluído: %s", arquivo.name)
                return arquivo
            sleep(2)

        raise TimeoutError(
            f"Download não concluído em {timeout}s — "
            f"verifique a pasta {self.download_dir}"
        )

    @staticmethod
    def selecionar_opcao_combobox(
        wdw: WebDriverWait,
        locator_combobox: tuple,
        locator_opcao: tuple,
    ) -> None:
        """Abre um combobox MUI e seleciona uma opção."""
        SeleniumRequester.aguardar_e_clicar(wdw, locator_combobox)
        SeleniumRequester.aguardar_e_clicar(wdw, locator_opcao)

    @staticmethod
    def exportar_csv_modal(wdw: WebDriverWait) -> None:
        """
        Sequência padrão do modal de exportação do SIENGE:
          1. Clica em 'Gerar Relatório'
          2. Aguarda modal abrir
          3. Seleciona formato CSV
          4. Clica em 'Exportar'

        Reutilizável em qualquer extractor que use o mesmo padrão de modal.
        """
        # Abre modal
        SeleniumRequester.aguardar_e_clicar(
            wdw,
            (By.XPATH, '//button[contains(.,"Gerar Relatório")]'),
            "Gerar Relatório",
        )
        # Aguarda modal
        SeleniumRequester.aguardar_visivel(
            wdw,
            (By.XPATH, '//h6[text()="Gerar relatório"]'),
        )
        # Seleciona CSV
        SeleniumRequester.selecionar_opcao_combobox(
            wdw,
            locator_combobox=(
                By.XPATH,
                '//label[text()="Gerar relatório como"]'
                '/following::div[@role="combobox"][1]',
            ),
            locator_opcao=(
                By.XPATH,
                '//li[@role="option" and text()="CSV"]',
            ),
        )
        sleep(1)
        # Exporta
        SeleniumRequester.aguardar_e_clicar(
            wdw,
            (By.XPATH, '//button[.//text()="Exportar"]'),
            "Exportar",
        )
