"""
stages/extract/extract_avaliacoes_fornecedores.py
-----------------------------------------
Extrai o relatório de Avalições dos fornecedores do SIENGE e salva como CSV.

Fluxo:
  1. Login via sessão salva no perfil Edge
  2. Navega para a URL
  3. Preencher o campo "Período das avaliaçõe" como Período
  4. Marcar as flag "Avaliações de medições de contratos",  "Avaliações de notas fiscais de compra" e  "Avaliações de pedidos"
  4. Preencher Período fechado de cada ciclo, inciando o preenchimento pelo final
  4. clica em visualizar
  7. Aguarda download e move para pasta de destino
"""

from __future__ import annotations

import logging
import shutil
from datetime import date
from calendar import monthrange
from pathlib import Path
from time import sleep

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC


from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

# URL do painel de compras
URL_PAINEL = (
    f"{BASE_URL}/8/index.html"
    "#/common/page/1254"
)


def extrair_avaliacoes_fornecedores(
        destino: Path | None = None,
) -> None:
    """
    Executa a extração do painel de compras.

    Parâmetros
    ----------
    destino : Path, opcional
        Pasta onde o CSV final será salvo.
        Padrão: stages/transform/input/curva_abc_apropriacao/

    Retorna
    -------
    Path do arquivo CSV gerado.
    """

    req = SeleniumRequester()
    req.ensure_login()



    destino = destino or (req.download_dir / "avaliacao_fornecedor")
    destino.mkdir(parents=True, exist_ok=True)

    driver = req.get_driver()
    wdw = req.waiter(driver)

    try:
        # ── 1. Login e Acesso ao perfil ──────────────────────────────────────────────────────────
        req.navegacao_inicial(driver, wdw)

        # ── 2. Navega para o painel ───────────────────────────────────────────
        logger.info("Navegando para os curva abc apropriacao...")
        driver.get(URL_PAINEL)

        sleep(2)


        req.fechar_popup_novidade(wdw)

        # ── 4. Preenche data inicial ──────────────────────────────────────────
        req.entrar_iframe(driver)


        # filtros fixos
        req.aguardar_e_clicar(
            wdw,
            (By.NAME, 'entity.criterioAval.flMedicao'),
        )

        req.aguardar_e_clicar(
            wdw,
            (By.NAME, 'entity.criterioAval.flNotaFiscal'),
        )

        req.aguardar_e_clicar(
            wdw,
            (By.NAME, 'entity.criterioAval.flPedido'),
        )

        Select(wdw.until(EC.element_to_be_clickable((By.NAME, "entity.periodoAval")))).select_by_value("periodo")


        ano_vigente = date.today().year

        for ano in range(2025, ano_vigente + 1):

            # Para anos passados processa até dezembro.
            # Para o ano atual processa apenas até o mês corrente.
            ultimo_mes = 12 if ano < ano_vigente else date.today().month

            for mes in range(1, ultimo_mes + 1):
                ultimo_dia = monthrange(ano, mes)[1]

                input_f_periodo = req.aguardar_e_clicar(
                    wdw,
                    (By.NAME, 'dtFimPeriodo'),
                    'Período Final'
                )
                input_f_periodo.clear()
                input_f_periodo.send_keys(
                    f'{ultimo_dia:02d}/{mes:02d}/{ano}'
                )

                input_i_periodo = req.aguardar_e_clicar(
                    wdw,
                    (By.NAME, 'dtInicioPeriodo'),
                    'Período Inicial'
                )
                input_i_periodo.clear()
                input_i_periodo.send_keys(
                    f'01/{mes:02d}/{ano}'
                )

                req.aguardar_e_clicar(
                    wdw,
                    (By.NAME, "pbSalvar"),
                )

                # Aguarda download
                arquivo_baixado = req.aguardar_download(extensao=".xlsx")

                # Move para pasta de destino
                nome_final = f"avaliacao_fornecedor_{mes:02d}_{ano}.xlsx"
                arquivo_final = destino / nome_final

                shutil.move(str(arquivo_baixado), str(arquivo_final))

                logger.info(
                    "Arquivo salvo em: %s",
                    arquivo_final
                )

    finally:

        # try:
        #
        #     driver.quit()
        #
        # except Exception:
        #
        #     pass
        #
        # try:
        #
        #     subprocess.run(
        #
        #         ["taskkill", "/F", "/IM", "msedge.exe", "/T"],
        #
        #         capture_output=True,
        #
        #     )

        # except Exception:
        #
        #     pass

        logger.info("Driver encerrado.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    extrair_avaliacoes_fornecedores()
    print(f"Extração concluída.")
