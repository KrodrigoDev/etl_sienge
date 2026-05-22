"""
stages/extract/extract_curva_abc_apropriacao.py
-----------------------------------------
Extrai o relatório de Curva ABC DE APROPRIAÇÃO do SIENGE e salva como CSV.

Fluxo:
  1. Login via sessão salva no perfil Edge
  2. Navega para a URL
  3. Preencher o campo "correção até" por ciclo fechado de cada ano, iniciando em  2023
  4. Preencher Período fechado de cada ciclo, inciando o preenchimento pelo final
  5. Marcar a flag "Consolidar informações de insumos de todas as obras"
  6. Desmarcar a flag Contratos
  4. clica em visualizar
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


from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

# URL do painel de compras
URL_PAINEL = (
    f"{BASE_URL}/8/index.html"
    "#/common/page/1095"
)


def extrair_curva_abc(
        destino: Path | None = None,
) -> None:
    """
    Executa a extração do painel de compras.

    Parâmetros
    ----------
    data_inicio : str, opcional
        Data no formato 'DD/MM/AAAA'. Padrão: 01/01 do ano corrente.
    destino : Path, opcional
        Pasta onde o CSV final será salvo.
        Padrão: stages/transform/input/curva_abc_apropriacao/

    Retorna
    -------
    Path do arquivo CSV gerado.
    """

    req = SeleniumRequester()
    req.ensure_login()

    destino = destino or (req.download_dir / "curva_abc_apropriacao")
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
            (By.NAME, 'filter.consolidarTodasObras'),
            'Flag consolidar informações de todas as obras'
        )

        req.aguardar_e_clicar(
            wdw,
            (By.NAME, 'filter.considerarContratos'),
            'Flag que habilita ou desabilita os contratos'
        )



        ano_vigente = date.today().year

        for ano in range(2023, ano_vigente + 1):

            input_dt_correcao = req.aguardar_e_clicar(
                wdw,
                (By.NAME, 'filter.dtCorrecaoAte'),
                'Data de correção'
            )

            input_dt_correcao.send_keys(f'31/12/{ano}')

            input_f_periodo = req.aguardar_e_clicar(
                wdw,
                (By.NAME, 'filter.dataFinalPeriodo'),
                'Período Final'
            )

            input_f_periodo.send_keys(f'31/12/{ano}')

            input_i_periodo = req.aguardar_e_clicar(
                wdw,
                (By.NAME, 'filter.dataInicialPeriodo'),
                'Período Inicial'
            )

            input_i_periodo.send_keys(f'01/01/{ano}')


            req.aguardar_e_clicar(
                wdw,
                (By.XPATH,'//input[@type="submit" and @value="Visualizar"]'),

            )

            # ── 8. Aguarda download ───────────────────────────────────────────────
            arquivo_baixado = req.aguardar_download(extensao=".xlsx")

            # ── 9. Move para pasta de destino ─────────────────────────────────────
            nome_final = f"curva_abc_apropriacao_{ano}.xlsx"
            arquivo_final = destino / nome_final
            shutil.move(str(arquivo_baixado), str(arquivo_final))
            logger.info("Arquivo salvo em: %s", arquivo_final)

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
    extrair_curva_abc()
    print(f"Extração concluída:")
