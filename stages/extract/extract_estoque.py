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
from datetime import date
from pathlib import Path
from time import sleep

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

# URL do relatório de estoque de obras
# Ajuste o hash se a URL do seu ambiente for diferente
URL_ESTOQUE = (
    f"{BASE_URL}/8/index.html"
    "#/suprimentos/estoque/relatorios/posicoes-estoque"
)


def extrair_estoque(
        destino: Path | None = None
) -> Path:
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

    req = SeleniumRequester()
    req.ensure_login()

    destino = destino or (req.download_dir / "estoque")
    destino.mkdir(parents=True, exist_ok=True)

    driver = req.get_driver()
    wdw = req.waiter(driver)

    try:
        # ── 1. Login e acesso ao perfil ──────────────────────────────────────────────────────────
        req.navegacao_inicial(driver, wdw)

        # ── 2. Navega para o relatório de estoque ─────────────────────────────
        logger.info("Navegando para o estoque de obras...")
        driver.get(URL_ESTOQUE)
        sleep(2)

        # ──  3. Limpando as obras selecionadas anteriomente  ─────────────────────────────

        req.aguardar_e_clicar(
            wdw,
            (By.XPATH, '//input[@placeholder="Pesquisar obra"]'),
            "Campo Obras",
        )

        sleep(1.5)
        logger.info("Verificando se existe filtro de obras para limpar...")

        try:
            req.aguardar_e_clicar(
                wdw,
                (
                    By.XPATH,
                    '//button[@aria-label="Limpar"]'
                ),
                "Botão Limpar obras",
            )

            logger.info("Filtro de obras limpo com sucesso.")

        except Exception:
            logger.info("Nenhum filtro de obras para limpar.")

        # ── 4. Selecionar Obras  ─────────────────────────────

        df_obras = pd.read_csv(req.project_root / 'stages/extract/reference/obras_estoque.csv', sep=';')

        lista_obras = df_obras['cod_obra'].dropna().astype(str).unique().tolist()

        logger.info("Total de obras: %s", len(lista_obras))

        # ── selecionar obras uma a uma ─────────────────────────

        for cod_obra in lista_obras:
            logger.info("Selecionando obra: %s", cod_obra)

            pesquisar = f"{cod_obra} "

            input_obra = req.aguardar_e_clicar(
                wdw,
                (By.XPATH, '//input[@placeholder="Pesquisar obra"]'),
                "Campo Obras",
            )

            input_obra.send_keys(pesquisar)

            sleep(1)

            input_obra.send_keys(Keys.CONTROL, "a")
            input_obra.send_keys(Keys.DELETE)

            req.aguardar_e_clicar(
                wdw,
                (
                    By.XPATH,
                    f'//li[@role="option" and starts-with(normalize-space(), "{cod_obra} -")]'
                )
            )

            sleep(1)

        # ── 4. Selecionar todas as colunas  ─────────────────────────────
        logger.info("Selecionando todas as colunas do relatório de estoque")
        req.selecionar_todas_colunas(wdw)

        # ── 5. Consultar ──────────────────────────────────────────────────────
        logger.info("Consultando estoque...")
        try:
            req.aguardar_e_clicar(
                wdw,
                (By.XPATH, '//button[@type="submit" and .//text()[contains(.,"Consultar")]]'),
                "Consultar",
            )
            sleep(3)
        except Exception:
            # Algumas telas do SIENGE carregam automaticamente sem botão de consulta
            logger.info("Botão Consultar não encontrado — tela carregou automaticamente.")

        # ── 6. Seleciona 'Todas' as linhas (se houver paginação) ─────────────
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

        # ── 7. Exporta CSV ────────────────────────────────────────────────────
        logger.info("Exportando CSV do estoque...")
        req.exportar_csv_modal(wdw)

        # ── 8. Aguarda download ───────────────────────────────────────────────
        arquivo_baixado = req.aguardar_download(extensao=".csv")

        # ── 9. Move para pasta de destino ─────────────────────────────────────
        nome_final = f"estoque_{date.today():%Y%m%d}.csv"
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
    caminho = extrair_estoque()

    print(f"Extração concluída: {caminho}")
