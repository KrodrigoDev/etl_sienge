"""
stages/extract/extract_estoque.py
----------------------------------
Extrai o relatório de Estoque de Obras do SIENGE e salva como CSV.

Fluxo:
  1. Login via sessão salva
  2. Navega para o relatório de estoque
  3. Aplica filtros necessários (empresa, situação)
  4. Exporta CSV via modal padrão (mesmo padrão do painel de compras)
  5. Aguarda download e move para pasta de destino

Reutiliza integralmente o SeleniumRequester e seus helpers —
nenhuma lógica de browser é duplicada aqui.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from datetime import date
from pathlib import Path
from time import sleep

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException

from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

URL_ESTOQUE = (
    f"{BASE_URL}/8/index.html"
    "#/suprimentos/estoque/relatorios/posicoes-estoque"
)


def extrair_estoque(
        destino: Path | None = None
) -> Path:
    req = SeleniumRequester()
    req.ensure_login()

    destino = destino or (req.download_dir / "estoque")
    destino.mkdir(parents=True, exist_ok=True)

    driver = req.get_driver()
    wdw = req.waiter(driver)

    try:
        # ── 1. Acesso inicial ────────────────────────────────────────────────
        req.navegacao_inicial(driver, wdw)

        # ── 2. Navega para o relatório de estoque ────────────────────────────
        logger.info("Navegando para o estoque de obras...")
        driver.get(URL_ESTOQUE)
        sleep(2)

        req.fechar_popup_novidade(wdw)

        # ── 3. Limpa obras selecionadas anteriormente ─────────────────────────
        req.aguardar_e_clicar(
            wdw,
            (By.XPATH, '//input[@placeholder="Pesquisar obra"]'),
            "Campo Obras",
        )

        # Aguarda o botão Limpar aparecer no DOM (mais confiável que sleep fixo).
        # Se não aparecer em 5s, assume que não há filtros para limpar.
        logger.info("Verificando se existe filtro de obras para limpar...")
        try:
            from selenium.webdriver.support.wait import WebDriverWait
            req.aguardar_e_clicar(
                WebDriverWait(driver, 5),
                (By.XPATH, '//button[@aria-label="Limpar"]'),
                "Botão Limpar obras",
            )
            logger.info("Filtro de obras limpo com sucesso.")
        except Exception:
            logger.info("Nenhum filtro de obras para limpar.")

        # ── 4. Selecionar obras uma a uma ─────────────────────────────────────
        df_obras = pd.read_csv(
            req.project_root / 'stages/extract/reference/obras_estoque.csv',
            sep=';',
        )
        lista_obras = df_obras['cod_obra'].dropna().astype(str).unique().tolist()
        logger.info("Total de obras: %s", len(lista_obras))

        for cod_obra in lista_obras:
            logger.info("Selecionando obra: %s", cod_obra)

            input_obra = req.aguardar_e_clicar(
                wdw,
                (By.XPATH, '//input[@placeholder="Pesquisar obra"]'),
                "Campo Obras",
            )
            input_obra.send_keys(f"{cod_obra} ")

            # Aguarda a opção aparecer no dropdown antes de limpar o campo
            req.aguardar_presenca(
                wdw,
                (By.XPATH, f'//li[@role="option" and starts-with(normalize-space(), "{cod_obra} -")]'),
            )

            req.aguardar_e_clicar(
                wdw,
                (By.XPATH, f'//li[@role="option" and starts-with(normalize-space(), "{cod_obra} -")]'),
            )

            input_obra.send_keys(Keys.CONTROL, "a")
            input_obra.send_keys(Keys.DELETE)

            sleep(1)

        req.scrollar_pagina(driver)

        # ── 5. Selecionar todas as colunas ────────────────────────────────────
        logger.info("Selecionando todas as colunas do relatório de estoque")
        req.selecionar_todas_colunas(wdw, pagina='pagina')

        # ── 6. Consultar ──────────────────────────────────────────────────────
        logger.info("Consultando estoque...")
        try:
            req.aguardar_e_clicar(
                wdw,
                (By.XPATH, '//button[@type="submit" and .//text()[contains(.,"Consultar")]]'),
                "Consultar",
            )
            sleep(3)
        except Exception:
            logger.info("Botão Consultar não encontrado — tela carregou automaticamente.")

        req.aguardar_carregamento_tabela(driver)

        # ── 7. Seleciona 'Todas' as linhas ────────────────────────────────────
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
        req.aguardar_carregamento_tabela(driver)
        sleep(3)

        # ── 8. Exporta CSV ────────────────────────────────────────────────────
        logger.info("Exportando CSV do estoque...")
        req.exportar_csv_modal(wdw)

        # ── 9. Aguarda download ───────────────────────────────────────────────
        arquivo_baixado = req.aguardar_download(extensao=".csv")

        # ── 10. Move para pasta de destino ────────────────────────────────────
        nome_final = f"estoque_{date.today().year}.csv"
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
    caminho = extrair_estoque()
    print(f"Extração concluída: {caminho}")
