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
from pathlib import Path
import subprocess
from time import sleep, time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import TimeoutException

logger = logging.getLogger(__name__)

BASE_URL = "https://telesil.sienge.com.br/sienge"
LOGIN_URL = f"{BASE_URL}/index.jsp"
TIMEOUT = 30  # segundos padrão para WebDriverWait
DL_TIMEOUT = 120  # segundos máximos para aguardar download


class SeleniumRequester:

    def __init__(self, download_dir: Path | None = None):

        self.project_root = Path(__file__).resolve().parents[2]

        self.path_profile = Path(r"C:\SeleniumPerfil\Edge")

        self.download_dir = download_dir or (
                self.project_root / "stages" / "transform" / "input"
        )

        self._create_directories()

    # ── Setup ──────────────────────────────────────────────────────────────────

    def _create_directories(self) -> None:
        self.path_profile.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def get_driver(self) -> webdriver.Edge:
        options = Options()

        # ── Perfil ────────────────────────────────────────────────────────────────
        options.add_argument(f"--user-data-dir={self.path_profile}")
        options.add_argument("--profile-directory=Default")

        # ── Headless (crítico para Agendador de Tarefas) ──────────────────────────
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")  # sem isso headless fica 0x0
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-software-rasterizer")

        # ── Estabilidade ──────────────────────────────────────────────────────────
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")

        # ── Downloads ─────────────────────────────────────────────────────────────
        options.add_experimental_option("prefs", {
            "download.default_directory": str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        })

        driver = webdriver.Edge(options=options)

        # Headless não herda permissão de download — precisa setar via CDP
        driver.execute_cdp_cmd(
            "Browser.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": str(self.download_dir),
            },
        )

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
    def navegacao_inicial(driver: webdriver.Edge, wdw: WebDriverWait) -> None:

        logger.info("Acessando SIENGE...")
        driver.get(f"{BASE_URL}/index.jsp")

        SeleniumRequester.aguardar_e_clicar(
            wdw,
            (By.CSS_SELECTOR, "#btnEntrarComSiengeID"),
            "Entrar com SIENGE ID",
        )
        sleep(2)

        # ── 2. Seleciona perfil ───────────────────────────────────────────────
        SeleniumRequester.aguardar_e_clicar(
            wdw,
            (
                By.XPATH,
                '//div[contains(@class,"relative") and contains(@class,"p-6")]'
                '//button[@tabindex="0"]',
            ),
            "Selecionar perfil",
        )

        try:
            # caso exista um aviso informado que já está conectado ir para aba
            aviso = SeleniumRequester.aguardar_presenca(wdw, (By.CSS_SELECTOR, '.spwAlertaAviso'))

            if aviso:
                driver.get(f"{BASE_URL}/removerUsuarioLogadoServlet?acao=S")

        except TimeoutException:
            logger.debug("Alerta não apareceu — fluxo normal.")

        sleep(2)

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
            sleep(0.5)

        raise TimeoutError(
            f"Download não concluído em {timeout}s — "
            f"verifique a pasta {self.download_dir}"
        )

    @staticmethod
    def entrar_iframe(wdw: WebDriverWait, nome_iframe: str):
        wdw.until(
            EC.frame_to_be_available_and_switch_to_it(
                (By.NAME, nome_iframe)
            )
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

    @staticmethod
    def selecionar_todas_colunas(wdw: WebDriverWait, pagina: str = 'painel_compras') -> None:
        locator_colunas = (
            By.CSS_SELECTOR,
            'button[aria-label="Exibir seletor de colunas"]',
        )

        locator_mostrar_todas = (
            By.XPATH, '//span[normalize-space()="Mostrar/Ocultar Todas"]'

        )

        locator_redefinir = (
            By.XPATH,
            '//button[normalize-space()="Redefinir"]'
        )

        # 1. Abre o menu
        SeleniumRequester.aguardar_e_clicar(wdw, locator_colunas)
        sleep(1)

        # 2. Verifica se já está tudo selecionado
        botao_redefinir = wdw.until(
            lambda d: d.find_element(*locator_redefinir)
        )

        if botao_redefinir.get_attribute("disabled") and not pagina in ['painel_compras', 'contratos', 'serviços']:
            logger.info("Todas as colunas já estão selecionadas. Nada a fazer.")

        else:
            logger.info("Selecionando todas as colunas...")

            SeleniumRequester.aguardar_e_clicar(wdw, locator_mostrar_todas)
            sleep(1)

        # 3. Fecha o menu clicando de novo no botão de colunas
        SeleniumRequester.aguardar_e_clicar(wdw, locator_colunas)
        sleep(1)

    @staticmethod
    def aguardar_carregamento_tabela(
            driver: webdriver.Edge,
            timeout: int = 150,
    ) -> None:
        """
        Aguarda o spinner de carregamento da MUI DataGrid desaparecer,
        indicando que todas as linhas foram renderizadas.

        Estratégia em duas fases:
          1. Aguarda o spinner aparecer (confirma que o carregamento iniciou)
          2. Aguarda o spinner desaparecer (confirma que o carregamento terminou)

        Deve ser chamado após selecionar "Todas" as linhas e antes de exportar.

        Recebe o driver diretamente (não o WebDriverWait) para criar
        WebDriverWaits com timeouts distintos para cada fase.

        O locator usa apenas classes semânticas do MUI (não as classes geradas
        como css-xxx) para garantir estabilidade entre versões do SIENGE.
        """
        locator_spinner = (
            By.CSS_SELECTOR,
            ".MuiDataGrid-overlay .MuiCircularProgress-root",
        )

        # Fase 1: aguarda o spinner aparecer — timeout curto (10s)
        # Após clicar em "Todas", o SIENGE demora alguns segundos para
        # iniciar o carregamento e exibir o spinner.
        try:
            WebDriverWait(driver, 12).until(
                EC.visibility_of_element_located(locator_spinner)
            )
            logger.info("Spinner detectado — carregamento em andamento...")
        except Exception:
            # Se o spinner não aparecer em 10s, os dados já podem ter
            # sido carregados instantaneamente (tabelas pequenas).
            logger.info("Spinner não detectado — tabela já pode estar carregada.")

        # Fase 2: aguarda o spinner desaparecer — timeout longo
        # Para tabelas grandes (27k+ linhas) o carregamento pode levar minutos.
        logger.info("Aguardando conclusão do carregamento da tabela...")
        WebDriverWait(driver, timeout).until(
            EC.invisibility_of_element_located(locator_spinner)
        )
        logger.info("Tabela carregada — spinner sumiu.")

    @staticmethod
    def scrollar_pagina(driver: webdriver.Edge) -> None:
        container = driver.find_element(By.ID, "main")

        driver.execute_script(
            "arguments[0].scrollBy(0, 350)",
            container
        )

    @staticmethod
    def fechar_popup_novidade(wdw: WebDriverWait) -> None:
        """
        Fecha o popup de "novidade" do SIENGE caso ele apareça após a navegação.

        O SIENGE exibe ocasionalmente um MuiDialog com vídeo do YouTube e um
        botão "Fechar" antes de liberar a tela principal. Sem tratamento, esse
        dialog intercepta qualquer clique subsequente (ElementClickInterceptedException).

        Estratégia: tenta localizar o botão "Fechar" dentro do dialog com um
        timeout curto — se não aparecer, assume que o popup não existe e segue.
        """
        locator_fechar = (
            By.XPATH,
            '//div[@role="dialog"]//button[normalize-space()="Fechar"]',
        )

        try:
            SeleniumRequester.aguardar_e_clicar(
                WebDriverWait(wdw._driver, 5),
                locator_fechar,
                "Popup novidade SIENGE",
            )
            logger.info("Popup de novidade fechado.")
        except TimeoutException:
            logger.debug("Nenhum popup de novidade detectado — seguindo.")

    def reiniciar_driver(self, driver, url_navegacao):
        """
        Encerra o driver atual com segurança e sobe um novo, já navegado.

        Deve ser utilizado principalmente na extração dos
        adiantamentos e títulos, porque baixam n arquivos

        driver:

        """
        try:
            driver.quit()
        except Exception:
            pass

        subprocess.run(
            ["taskkill", "/F", "/IM", "msedge.exe", "/T"],
            capture_output=True,
        )
        sleep(5)

        novo_driver = self.get_driver()
        novo_wdw = self.waiter(novo_driver)
        self.navegacao_inicial(novo_driver, novo_wdw)
        novo_driver.get(url_navegacao)

        return novo_driver, novo_wdw

    # em selenium_requester.py, adicionar como método estático da classe

    @staticmethod
    def verificar_sem_dados(driver, wdw) -> bool:
        """
        Retorna True se a empresa não tem dados e trata o aviso adequadamente.
        Retorna False se há dados para baixar.

        O SIENGE usa dois mecanismos distintos de "sem dados":
          1. Alert nativo do browser (window.alert) — "Nenhum registro encontrado."
             Aparece antes de qualquer interação com o DOM e trava todo find_element.
             Deve ser tratado PRIMEIRO via driver.switch_to.alert.
          2. div.spwAlertaAviso — alerta visual dentro da página ("Não há registros").
             Tratado normalmente via CSS selector.
        """
        sleep(0.2)

        try:
            alert = driver.switch_to.alert
            texto_alert = alert.text
            logger.info("Alert nativo detectado: '%s' — aceitando", texto_alert)
            alert.accept()
            return True
        except Exception:
            pass

        try:
            alerta = wdw.until(
                lambda d: d.find_element(By.CSS_SELECTOR, "div.spwAlertaAviso")
            )
            try:
                texto = alerta.text
            except EC.StaleElementReferenceException:
                logger.info("Alerta ficou stale — tratando como sem dados")
                texto = "Não há registros"

            if "Não há registros" in texto:
                logger.info("Empresa sem dados (div alerta) — fechando")
                try:
                    driver.find_element(
                        By.CSS_SELECTOR, 'img[name="fecharAlertas"]'
                    ).click()
                except Exception:
                    logger.warning("Não conseguiu clicar no fechar — ignorando")
                return True

        except TimeoutException:
            return False

        return False
