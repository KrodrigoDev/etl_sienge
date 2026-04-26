"""
stages/extract/extract_contratos.py
-----------------------------------------
Extrai o relatório de Painel de Compras do SIENGE e salva como CSV.

Fluxo:
  1. Login via sessão salva no perfil Edge
  2. Navega para a URL do painel de compras
  3. Preenche data inicial
  4. Consulta
  5. Seleciona 'Todas' as linhas
  6. Exporta CSV via modal padrão
  7. Aguarda download e move para pasta de destino
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from datetime import date
from pathlib import Path
from time import sleep

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

# URL do painel de compras
URL_PAINEL = (
    f"{BASE_URL}/8/index.html"
    "#/suprimentos/contratos-e-medicoes/contratos/cadastros"
)


def extrair_contratos(
        data_inicio: str | None = None,
        destino: Path | None = None,
) -> Path:
    """
    Executa a extração do painel de compras.

    Parâmetros
    ----------
    data_inicio : str, opcional
        Data no formato 'DD/MM/AAAA'. Padrão: 01/01 do ano corrente.
    destino : Path, opcional
        Pasta onde o CSV final será salvo.
        Padrão: stages/extract/downloads/painel_compras/

    Retorna
    -------
    Path do arquivo CSV gerado.
    """

    if data_inicio is None:
        data_inicio = f"01/01/2024"

    req = SeleniumRequester()
    req.ensure_login()

    destino = destino or (req.download_dir / "contrato")
    destino.mkdir(parents=True, exist_ok=True)

    driver = req.get_driver()
    wdw = req.waiter(driver)

    try:
        # ── 1. Login e Acesso ao perfil ──────────────────────────────────────────────────────────
        req.navegacao_inicial(driver, wdw)

        # ── 2. Navega para o painel ───────────────────────────────────────────
        logger.info("Navegando para os contratos...")
        driver.get(URL_PAINEL)

        sleep(2)

        req.fechar_popup_novidade(wdw)

        # ── 3 Selecionar todas as colunas ───────────────────────────────────

        req.scrollar_pagina(driver)

        logger.info("Selecionando todas as colunas do relatório de contratos")
        req.selecionar_todas_colunas(wdw, pagina='contratos')

        sleep(1)

        # ── 4. Preenche data inicial ──────────────────────────────────────────
        logger.info("Preenchendo data inicial: %s", data_inicio)
        req.preencher_campo(
            wdw,
            (By.CSS_SELECTOR, 'input[name="dtContratoInicial"]'),
            data_inicio,
        )
        sleep(1)

        # ── 5. Consultar ──────────────────────────────────────────────────────
        logger.info("Consultando...")
        req.aguardar_e_clicar(
            wdw,
            (By.XPATH, '//button[@type="submit" and .//text()[contains(.,"Consultar")]]'),
            "Consultar",
        )

        req.aguardar_carregamento_tabela(driver)

        req.aguardar_presenca(
            wdw,
            (By.XPATH, '//div[contains(@class,"MuiTablePagination-select")]'),
        )
        sleep(2)

        # ── Seleciona 'Todas/Todos' as linhas ─────────────────────────────────
        logger.info("Selecionando todas as linhas...")

        select_paginacao = wdw.until(
            EC.element_to_be_clickable(
                (By.XPATH, '//div[contains(@class,"MuiTablePagination-select")]')
            )
        )
        select_paginacao.click()

        # Aguarda o item aparecer visível antes de clicar — evita click em elemento
        # ainda não renderizado em headless
        opcao_todas = wdw.until(
            EC.visibility_of_element_located(
                (By.XPATH, '//li[@role="option" and contains(.,"Todos")]')
            )
        )
        driver.execute_script("arguments[0].click();", opcao_todas)

        req.aguardar_carregamento_tabela(driver)
        sleep(3)

        req.aguardar_carregamento_tabela(driver)

        # ── 7. Exporta CSV ────────────────────────────────────────────────────
        logger.info("Exportando CSV...")
        req.exportar_csv_modal(wdw)

        # ── 8. Aguarda download ───────────────────────────────────────────────
        arquivo_baixado = req.aguardar_download(extensao=".csv")

        # ── 9. Move para pasta de destino ─────────────────────────────────────
        nome_final = f"contratos_{date.today().year}.csv"
        arquivo_final = destino / nome_final
        shutil.move(str(arquivo_baixado), str(arquivo_final))
        logger.info("Arquivo salvo em: %s", arquivo_final)

        return arquivo_final


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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    caminho = extrair_contratos()
    print(f"Extração concluída: {caminho}")
