"""
stages/extract/extract_estoque.py
----------------------------------
Extrai o relatório de Adiantamento do SIENGE e salva como CSV.

Fluxo:
  1. Login via sessão salva
  2. Navega para o relatório de Adiantamento
  3. Aplica filtros necessários (cod_empresa, data)
  4. Exporta XLSX
  5. Aguarda download e move para pasta de destino

Reutiliza integralmente o SeleniumRequester e seus helpers —
nenhuma lógica de browser é duplicada aqui.
"""

from __future__ import annotations

import logging
import shutil
from datetime import date
from pathlib import Path
from time import sleep

import pandas as pd

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
)



from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

# URL do relatório de estoque de obras
# Ajuste o hash se a URL do seu ambiente for diferente
URL_ADIANTAMENTO = (
    f"{BASE_URL}/8/index.html"
    "#/common/page/660"
)



def verificar_sem_dados(driver, wdw):

    try:
        sleep(0.2)
        alerta = wdw.until(
            lambda d: d.find_element(
                By.CSS_SELECTOR,
                "div.spwAlertaAviso"
            )
        )

        try:

            texto = alerta.text

        except StaleElementReferenceException:

            logger.info(
                "Alerta ficou stale — tratando como sem dados"
            )

            texto = "Não há registros"

        if "Não há registros" in texto:

            logger.info(
                "Empresa sem dados — fechando alerta"
            )

            try:

                driver.find_element(
                    By.CSS_SELECTOR,
                    'img[name="fecharAlertas"]'
                ).click()

            except Exception:

                logger.warning(
                    "Não conseguiu clicar no fechar — ignorando"
                )

            return True

    except TimeoutException:

        return False

    return False


def extrair_estoque(
        destino: Path | None = None,
        data_inicio: str | None = None,
):
    """
    Executa a extração do relatório de estoque de obras.

    Parâmetros
    ----------
    destino : Path, opcional
        Pasta onde o CSV final será salvo.
        Padrão: stages/extract/downloads/estoque/
    situacao : str
        Filtro de situação: 'ATIVO', 'INATIVO' ou 'TODOS'.

    Retorna
    -------
    Path do arquivo CSV gerado.
    """

    if data_inicio is None:
        data_inicio = f"01/01/2014"

    req = SeleniumRequester()
    req.ensure_login()

    destino = destino or (req.download_dir / "adiantamento")
    destino.mkdir(parents=True, exist_ok=True)

    driver = req.get_driver()
    wdw = req.waiter(driver)

    try:
        # ── 1. Login e acesso ao perfil ──────────────────────────────────────────────────────────
        req.navegacao_inicial(driver, wdw)

        # ── 2. Navega para o relatório de estoque ─────────────────────────────
        logger.info("Navegando para o adiantamento...")
        driver.get(URL_ADIANTAMENTO)
        sleep(1.5)

        # ── 3. Selecionando o padrão de pesquisa ─────────────────────────────

        logger.info("Entrando no Iframe do formulário")

        driver.switch_to.default_content()

        frame = driver.find_element(By.ID, "iFramePage")
        driver.switch_to.frame(frame)

        logger.info("Selecionando campos do formulário")
        req.aguardar_e_clicar(wdw,  (By.CSS_SELECTOR, "#flTituloVinculadoA"))
        req.aguardar_e_clicar(wdw, (By.CSS_SELECTOR, "#flOrdenacaoA"))

        req.preencher_campo(
            wdw,
            (By.CSS_SELECTOR, 'input[name="dtEmissaoInicio"]'),
            data_inicio,
        )

        sleep(0.5)

        # ── 4. Selecionar Empresas  ─────────────────────────────

        dim_empresas = pd.read_csv(req.project_root / 'stages/extract/reference/dim_empresa.csv', sep=';')

        lista_empresas = dim_empresas['cod_empresa'].dropna().astype(str).unique().tolist()

        logger.info("Total de empresas: %s", len(lista_empresas))

        # ── selecionar empresa uma a uma ─────────────────────────

        janela_principal = driver.current_window_handle

        for cod_empresa in lista_empresas:

            logger.info("Selecionando empresa: %s", cod_empresa)

            input_empresa = req.aguardar_e_clicar(
                wdw,
                (By.ID, "entity.empresa.cdEmpresaView"),
                "Campo Empresa",
            )

            input_empresa.send_keys(cod_empresa)
            input_empresa.send_keys(Keys.ENTER)

            sleep(0.5)

            req.aguardar_e_clicar(
                wdw,
                (By.XPATH, '//input[@type="submit" and @value="Visualizar"]'),
                "Botão Visualizar",
            )


            if verificar_sem_dados(driver, wdw):

                driver.switch_to.window(janela_principal)

                driver.switch_to.default_content()
                driver.switch_to.frame(frame)

                continue

            # ── Verifica se abriu nova janela ─────────────────────

            handles = driver.window_handles

            if len(handles) > 1:

                logger.info("Nova janela detectada")

                nova_janela = [
                    h for h in handles
                    if h != janela_principal
                ][0]


                driver.switch_to.window(nova_janela)

                arquivo_baixado = req.aguardar_download(
                extensao=".xlsx",
                timeout=15
                )
                # 13, 17 é sem dados
                nome_final = f"adiantamento_{cod_empresa}_{date.today().year}.xlsx"
                arquivo_final = destino / nome_final
                shutil.move(str(arquivo_baixado), str(arquivo_final))
                logger.info("Arquivo salvo em: %s", arquivo_final)

            else:

                logger.info("Nenhuma janela aberta para esta empresa")


            driver.switch_to.window(janela_principal)
            # 1, 10, 11, 12, 13, 14,
            driver.switch_to.default_content()
            driver.switch_to.frame(frame)

            input_empresa = req.aguardar_e_clicar(
                wdw,
                (By.ID, "entity.empresa.cdEmpresaView"),
                "Campo Empresa",
            )

            input_empresa.send_keys(Keys.CONTROL, "a")
            input_empresa.send_keys(Keys.DELETE)

            sleep(0.5)




    finally:
        driver.quit()
        logger.info("Driver encerrado.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    extrair_estoque()

    print(f"Extração concluída")
