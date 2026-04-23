"""
stages/extract/extract_adiantamento.py
----------------------------------
Extrai o relatório de Adiantamento do SIENGE e salva como XLSX.

Fluxo:
  1. Login via sessão salva
  2. Navega para o relatório de Adiantamento
  3. Aplica filtros necessários (cod_empresa, data)
  4. Exporta XLSX
  5. Aguarda download, fecha a aba de loading e move para pasta de destino

Reutiliza integralmente o SeleniumRequester e seus helpers —
nenhuma lógica de browser é duplicada aqui.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from time import sleep

import pandas as pd

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException
)

from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

URL_ADIANTAMENTO = f"{BASE_URL}/8/index.html#/common/page/660"


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

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

    # ── Tipo 1: alert nativo do browser ──────────────────────────────────────
    try:
        alert = driver.switch_to.alert
        texto_alert = alert.text
        logger.info("Alert nativo detectado: '%s' — aceitando", texto_alert)
        alert.accept()
        return True
    except Exception:
        pass  # Nenhum alert nativo presente — segue para verificar o DOM

    # ── Tipo 2: div.spwAlertaAviso (alerta visual na página) ─────────────────
    try:
        alerta = wdw.until(
            lambda d: d.find_element(By.CSS_SELECTOR, "div.spwAlertaAviso")
        )
        try:
            texto = alerta.text
        except StaleElementReferenceException:
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



# ─────────────────────────────────────────────────────────────────────────────
# EXTRAÇÃO PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def extrair_adiantamento(
        destino: Path | None = None,
        data_inicio: str | None = None,
) -> None:
    """
    Executa a extração do relatório de adiantamentos empresa a empresa.

    Parâmetros
    ----------
    destino     : pasta onde os XLSX serão salvos.
                  Padrão: <download_dir>/adiantamento/
    data_inicio : filtro de data inicial no formato DD/MM/AAAA.
                  Padrão: 01/01/2014
    """
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

            # 5b. Digita o código e aciona a busca
            input_empresa = req.aguardar_e_clicar(
                wdw,
                (By.ID, "entity.empresa.cdEmpresaView"),
                "Campo Empresa",
            )

            input_empresa.send_keys(cod_empresa)
            input_empresa.send_keys(Keys.ENTER)

            sleep(1)

            # 5c. Clica em Visualizar
            req.aguardar_e_clicar(
                wdw,
                (By.XPATH, '//input[@type="submit" and @value="Visualizar"]'),
                "Botão Visualizar",
            )

            # 5d. Empresa sem dados → fecha alerta e vai para a próxima
            if verificar_sem_dados(driver, wdw):
                logger.info("  Sem dados — pulando empresa %s", cod_empresa)
                driver.switch_to.window(janela_principal)
                driver.switch_to.default_content()
                driver.switch_to.frame(frame)
                continue

            arquivo_baixado = req.aguardar_download(
                extensao=".xlsx",
                timeout=40
            )

            # 5g. Move o arquivo para o destino com nome identificável
            nome_final = f"relatorio - {cod_empresa}.xlsx"
            arquivo_final = destino / nome_final
            shutil.move(str(arquivo_baixado), str(arquivo_final))
            logger.info("  Salvo: %s", arquivo_final)

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

    finally:
        driver.quit()
        logger.info("Driver encerrado.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    extrair_adiantamento()
    print("Extração concluída.")
