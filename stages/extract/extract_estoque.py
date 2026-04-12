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

from selenium.webdriver.common.by import By

from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

# URL do relatório de estoque de obras
# Ajuste o hash se a URL do seu ambiente for diferente
URL_ESTOQUE = (
    f"{BASE_URL}/8/index.html"
    "#/suprimentos/estoque/estoque-obras"
)


def extrair_estoque(
    destino: Path | None = None,
    situacao: str = "ATIVO",
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

        # ── 3. Navega para o relatório de estoque ─────────────────────────────
        logger.info("Navegando para o estoque de obras...")
        driver.get(URL_ESTOQUE)
        sleep(3)

        # ── 4. Aplica filtro de situação (se disponível na tela) ──────────────
        # O filtro de situação pode variar conforme a versão do SIENGE.
        # Se não existir na URL do seu ambiente, remova este bloco.
        if situacao and situacao.upper() != "TODOS":
            logger.info("Aplicando filtro de situação: %s", situacao)
            try:
                req.selecionar_opcao_combobox(
                    wdw,
                    locator_combobox=(
                        By.XPATH,
                        '//label[contains(text(),"Situação")]'
                        '/following::div[@role="combobox"][1]',
                    ),
                    locator_opcao=(
                        By.XPATH,
                        f'//li[@role="option" and '
                        f'translate(text(),"abcdefghijklmnopqrstuvwxyz",'
                        f'"ABCDEFGHIJKLMNOPQRSTUVWXYZ")="{situacao.upper()}"]',
                    ),
                )
                sleep(1)
            except Exception:
                logger.warning(
                    "Filtro de situação não encontrado — continuando sem filtrar."
                )

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
        try:
            paginacao = driver.find_element(
                By.XPATH, '//div[contains(@class,"MuiTablePagination-select")]'
            )
            paginacao.click()
            sleep(2)
            req.aguardar_e_clicar(
                wdw,
                (By.XPATH, '//li[contains(.,"Todas")]'),
                "Todas as linhas",
            )
            sleep(10)
        except Exception:
            logger.info("Paginação não encontrada — assumindo que todos os dados já estão visíveis.")
            sleep(3)

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
