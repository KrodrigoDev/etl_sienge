"""
stages/extract/extract_painel_compras.py
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
from datetime import date
from pathlib import Path
from time import sleep

from selenium.webdriver.common.by import By

from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

# URL do painel de compras
URL_PAINEL = (
    f"{BASE_URL}/8/index.html"
    "#/suprimentos/compras/painel-de-compras"
)


def extrair_painel_compras(
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
        data_inicio = f"01/01/{date.today().year}"

    req = SeleniumRequester()
    req.ensure_login()

    destino = destino or (req.download_dir / "painel_compras")
    destino.mkdir(parents=True, exist_ok=True)

    driver = req.get_driver()
    wdw    = req.waiter(driver)

    try:
        # ── 1. Login ──────────────────────────────────────────────────────────
        logger.info("Acessando SIENGE...")
        driver.get(f"{BASE_URL}/index.jsp")

        req.aguardar_e_clicar(
            wdw,
            (By.CSS_SELECTOR, "#btnEntrarComSiengeID"),
            "Entrar com SIENGE ID",
        )
        sleep(2)

        # ── 2. Seleciona perfil ───────────────────────────────────────────────
        req.aguardar_e_clicar(
            wdw,
            (
                By.XPATH,
                '//div[contains(@class,"relative") and contains(@class,"p-6")]'
                '//button[@tabindex="0"]',
            ),
            "Selecionar perfil",
        )
        sleep(2)

        # ── 3. Navega para o painel ───────────────────────────────────────────
        logger.info("Navegando para o painel de compras...")
        driver.get(URL_PAINEL)
        sleep(3)

        # ── 4. Preenche data inicial ──────────────────────────────────────────
        logger.info("Preenchendo data inicial: %s", data_inicio)
        req.preencher_campo(
            wdw,
            (By.CSS_SELECTOR, 'input[name="dataInicial"]'),
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

        req.aguardar_presenca(
            wdw,
            (By.XPATH, '//div[contains(@class,"MuiTablePagination-select")]'),
        )
        sleep(2)

        # ── 6. Seleciona 'Todas' as linhas ────────────────────────────────────
        logger.info("Selecionando todas as linhas...")
        driver.find_element(
            By.XPATH, '//div[contains(@class,"MuiTablePagination-select")]'
        ).click()
        sleep(2)

        req.aguardar_e_clicar(
            wdw,
            (By.XPATH, '//li[contains(.,"Todas")]'),
            "Todas as linhas",
        )
        sleep(10)  # aguarda tabela renderizar todas as linhas

        # ── 7. Exporta CSV ────────────────────────────────────────────────────
        logger.info("Exportando CSV...")
        req.exportar_csv_modal(wdw)

        # ── 8. Aguarda download ───────────────────────────────────────────────
        arquivo_baixado = req.aguardar_download(extensao=".csv")

        # ── 9. Move para pasta de destino ─────────────────────────────────────
        nome_final = f"painel_compras_{date.today():%Y%m%d}.csv"
        arquivo_final = destino / nome_final
        shutil.move(str(arquivo_baixado), str(arquivo_final))
        logger.info("Arquivo salvo em: %s", arquivo_final)

        return arquivo_final

    finally:
        driver.quit()
        logger.info("Driver encerrado.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    caminho = extrair_painel_compras()
    print(f"Extração concluída: {caminho}")
