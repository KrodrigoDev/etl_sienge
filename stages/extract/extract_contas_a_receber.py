from __future__ import annotations

import logging
import re
import shutil
from datetime import date, timedelta
from pathlib import Path
from time import sleep
import sys

import pandas as pd
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait

from src.drivers.selenium_requester import SeleniumRequester

ROOT = Path(__file__).resolve().parents[2]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT / "logs" / "contas_recebidas.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger(__name__)

REPORT_URL = "https://telesil.sienge.com.br/sienge/8/index.html#/common/page/4803"
IFRAME_NAME = "iFramePage"

CONDICOES_SIGLAS = [
    "AT", "CC", "CH", "CQ", "FG", "FI", "IT", "PA", "PB",
    "PM", "PS", "PT", "SG", "AS", "AM", "PV", "SI", "SC",
    "PU", "PI", "PP",
]

AUXILIAR_PATH = (
        Path(__file__).resolve().parents[2]
        / "stages" / "extract" / "reference" / "auxiliar_contas_recebidas.xlsx"
)
BASE_OUTPUT_DIR = (
        Path(__file__).resolve().parents[2]
        / "stages" / "transform" / "input" / "contas_a_receber"
)


def pasta_brutos(slug_cc: str) -> Path:
    p = BASE_OUTPUT_DIR / slug_cc / "dados_brutos"
    p.mkdir(parents=True, exist_ok=True)
    return p


def fim_de_mes(d: date) -> date:
    return (d.replace(day=1) + relativedelta(months=1)) - timedelta(days=1)


def mes_anterior_ao_vigente() -> date:
    return (date.today().replace(day=1) - timedelta(days=1)).replace(day=1)


def fmt(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def carregar_centros_ativos() -> list[dict]:
    df = pd.read_excel(AUXILIAR_PATH, sheet_name="centros_custo")
    df = df[df["ativo"].str.strip().str.lower() == "sim"]
    return df.to_dict(orient="records")


def selecionar_via_lupa(
        driver,
        wdw: WebDriverWait,
        locator_lupa: tuple,
        campo_pesquisa_name: str,
        codigo: str | list,
        descricao: str = "",
        busca_simple: bool = True,
) -> None:
    SeleniumRequester.aguardar_e_clicar(wdw, locator_lupa, f"Lupa [{descricao}]")
    sleep(1)

    wdw.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "layerFormConsulta")))

    if busca_simple:
        campo_busca = wdw.until(EC.element_to_be_clickable((By.NAME, campo_pesquisa_name)))
        campo_busca.clear()
        campo_busca.send_keys(codigo)
        sleep(0.3)

        SeleniumRequester.aguardar_e_clicar(wdw, (By.ID, "pbProcurar"), "Procurar")
        sleep(1)

        primeiro_cb = wdw.until(EC.element_to_be_clickable(
            (By.XPATH, '//input[@type="checkbox" and @name="rowSelect" and @value="0"]')
        ))
        if not primeiro_cb.is_selected():
            primeiro_cb.click()
        sleep(0.3)

    else:
        if isinstance(codigo, str):
            codigo = [codigo]

        for sigla in codigo:
            td = wdw.until(EC.element_to_be_clickable(
                (By.XPATH, f'//table[@id="tabelaResultado"]//td[@title="{sigla}"]')
            ))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", td)
            sleep(0.2)
            driver.execute_script("arguments[0].click();", td)
            logger.info("Sigla [%s] selecionada", sigla)
            sleep(0.2)

    SeleniumRequester.aguardar_e_clicar(wdw, (By.NAME, "pbSelecionar"), "Selecionar")
    sleep(0.5)

    driver.switch_to.default_content()
    SeleniumRequester.entrar_iframe(driver)
    logger.info("Lupa [%s] → seleção concluída", descricao)


def abrir_relatorio(driver, wdw: WebDriverWait) -> None:
    driver.get(REPORT_URL)
    sleep(3)
    SeleniumRequester.fechar_popup_novidade(wdw)
    SeleniumRequester.entrar_iframe(driver)
    logger.info("Iframe '%s' ativo", IFRAME_NAME)


def preencher_filtros_fixos(driver, wdw: WebDriverWait, centro: dict) -> None:
    selecionar_via_lupa(
        driver, wdw,
        locator_lupa=(By.XPATH, '//img[contains(@onclick, "empresaAut")]'),
        campo_pesquisa_name="entity.cdEmpresaView",
        codigo=str(int(centro["cod_empresa"])),
        descricao="Empresa",
    )

    Select(wdw.until(EC.element_to_be_clickable((By.NAME, "flOrdem")))).select_by_value("N")

    cod_cc = centro.get("cod_centro_custo")
    if cod_cc and not (isinstance(cod_cc, float) and pd.isna(cod_cc)):
        selecionar_via_lupa(
            driver, wdw,
            locator_lupa=(By.XPATH, '//img[contains(@onclick, "empreendCentroCusto")]'),
            campo_pesquisa_name="entity.cdEmpreendView",
            codigo=str(int(cod_cc)),
            descricao="Centro de custo",
        )

    valor_coluna = "P"
    Select(wdw.until(EC.element_to_be_clickable((By.NAME, "flColuna")))).select_by_value(valor_coluna)

    parametros_avancados = driver.find_elements(By.XPATH, '//img[contains(@name, "toggleFiltro")]')
    parametros_avancados[-1].click()
    sleep(0.5)

    selecionar_via_lupa(
        driver, wdw,
        locator_lupa=(By.XPATH, '//img[contains(@onclick, "documentoPK.cdDocumento")]'),
        campo_pesquisa_name="entity.documentoPK.cdDocumento",
        codigo="CT",
        descricao="Documento",
    )

    selecionar_via_lupa(
        driver, wdw,
        locator_lupa=(By.XPATH, '//img[contains(@onclick, "tipoCondicaoPK.cdTipoCondicao")]'),
        campo_pesquisa_name="tipoCondicaoPK.cdTipoCondica",
        codigo=CONDICOES_SIGLAS,
        descricao="Condições",
        busca_simple=False,
    )

    incuir_inadimplentes = wdw.until(EC.element_to_be_clickable(
        (By.XPATH, '//input[@type="checkbox" and (@name="flIncluirInadimplentes")]')
    ))
    incuir_inadimplentes.click()
    sleep(0.5)

    incuir_subjudice = wdw.until(EC.element_to_be_clickable(
        (By.XPATH, '//input[@type="checkbox" and (@name="flIncluirSubJudice")]')
    ))
    incuir_subjudice.click()

    logger.info("Filtros fixos preenchidos para '%s'", centro["centro_custo"])


def atualizar_periodo(wdw: WebDriverWait, inicio: date, fim: date) -> None:
    for name, valor in [("dtVenctoFim", fmt(fim)), ("dtVenctoInicio", fmt(inicio))]:
        campo = wdw.until(EC.element_to_be_clickable((By.NAME, name)))
        campo.click()
        campo.clear()
        campo.send_keys(valor)
        sleep(0.2)


def _toggle_sintetico(wdw: WebDriverWait, ativar: bool) -> None:
    cb = wdw.until(EC.element_to_be_clickable(
        (By.XPATH, '//input[@type="checkbox" and (@name="flSintetico" or @id="flSintetico")]')
    ))
    if cb.is_selected() != ativar:
        cb.click()
    sleep(0.3)


def _limpar_temp(requester: SeleniumRequester) -> None:
    for f in requester.download_dir.iterdir():
        if f.suffix in (".xlsx", ".crdownload"):
            try:
                f.unlink()
                logger.debug("Temp removido: %s", f.name)
            except Exception:
                pass


def _gerar_e_salvar(
        driver,
        wdw: WebDriverWait,
        requester: SeleniumRequester,
        slug_cc: str,
        label: str,
) -> bool:
    _limpar_temp(requester)

    SeleniumRequester.aguardar_e_clicar(
        wdw, (By.ID, "btFiltrar"), f"Gerar – {label}"
    )

    sleep(1.5)

    if SeleniumRequester.verificar_sem_dados(driver, wdw):
        logger.info("Sem registros → %s (pulando)", label)
        return False

    arquivo = requester.aguardar_download(extensao=".xlsx")

    destino = BASE_OUTPUT_DIR / f"{slug_cc}.xlsx"
    shutil.move(str(arquivo), str(destino))
    logger.info("Salvo → %s", destino.relative_to(BASE_OUTPUT_DIR))
    return True


def baixar_par(
        driver,
        wdw: WebDriverWait,
        requester: SeleniumRequester,
        slug_cc: str,
) -> None:
    label_ana = f"{slug_cc}_analitico"

    teve_dados = _gerar_e_salvar(driver, wdw, requester, slug_cc, label_ana)
    sleep(1)


def processar_centro(
        driver,
        wdw: WebDriverWait,
        requester: SeleniumRequester,
        centro: dict,
) -> None:
    nome_cc = str(centro["centro_custo"]).strip()
    slug_cc = nome_cc.lower().replace(" ", "_").replace("-", "")[:40]

    abrir_relatorio(driver, wdw)
    preencher_filtros_fixos(driver, wdw, centro)

    atualizar_periodo(wdw, date(day=1, month=1, year=2000), date(day=1, month=1, year=2090))

    baixar_par(driver, wdw, requester, slug_cc)


def main() -> None:
    centros = carregar_centros_ativos()
    logger.info("%d centros de custo ativos carregados", len(centros))

    requester = SeleniumRequester(download_dir=BASE_OUTPUT_DIR / "_temp_downloads")

    for i, centro in enumerate(centros, start=1):
        driver = None
        try:
            logger.info("────────────────────────────────────────────")
            logger.info(
                "[%d/%d] Iniciando: %s",
                i, len(centros), centro.get("centro_custo"),
            )

            driver = requester.get_driver()
            wdw = requester.waiter(driver)
            SeleniumRequester.navegacao_inicial(driver, wdw)

            processar_centro(driver, wdw, requester, centro)

            logger.info(
                "[%s] Finalizado com sucesso",
                centro.get("centro_custo"),
            )

        except Exception:
            logger.exception(
                "Erro no centro '%s'",
                centro.get("centro_custo"),
            )

        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("Driver encerrado")
                except Exception:
                    pass
                sleep(2)

    logger.info("Concluído. Arquivos em: %s", BASE_OUTPUT_DIR)


if __name__ == "__main__":
    main()