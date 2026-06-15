"""
stages/extract/extract_contas_recebidas.py
---------------------------------------
RPA – Relatório de Contas Recebidas (Sintético + Analítico)

Estrutura de saída por centro de custo:
  output/contas_recebidas/
    {slug_cc}/
      dados_brutos/          ← arquivos baixados do Sienge (sin + ana por mês)
      dados_consolidados/    ← gerado pelo transform (com % repasse aplicado)

Nomenclatura dos arquivos brutos:
  {slug_cc}_{AAAAMM}_sintetico.xlsx
  {slug_cc}_{AAAAMM}_analitico.xlsx
"""

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

# ── Constantes ────────────────────────────────────────────────────────────────

REPORT_URL = "https://telesil.sienge.com.br/sienge/8/index.html#/common/page/4929"
IFRAME_NAME = "iFramePage"

TIPOS_CLIENTE_IDS = [3, 6]  # 3 = Registrado, 6 = Venda Direta

CONDICOES_SIGLAS = [
    "AT", "CC", "CH", "CQ", "FG", "FI", "IT", "PA", "PB",
    "PM", "PS", "PT", "SG", "AS", "AM", "PV", "SI", "SC",
    "PU", "PI", "PP",
]

AUXILIAR_PATH_SOCIOS = (
        Path(__file__).resolve().parents[2]
        / "stages" / "extract" / "reference" / "auxiliar_contas_recebidas_socios.xlsx"
)

AUXILIAR_PATH_PAINEL = (
        Path(__file__).resolve().parents[2]
        / "stages" / "extract" / "reference" / "auxiliar_contas_recebidas_painel.xlsx"
)

BASE_OUTPUT_DIR = (
        Path(__file__).resolve().parents[2]
        / "stages" / "transform" / "input" / "contas_recebidas"
)


# ── Helpers de pasta ──────────────────────────────────────────────────────────

def pasta_brutos(slug_cc: str, tipo_path: str = 'socios') -> Path:
    p = BASE_OUTPUT_DIR / slug_cc / tipo_path
    p.mkdir(parents=True, exist_ok=True)
    return p


# ── Helpers de data ───────────────────────────────────────────────────────────

def fim_de_mes(d: date) -> date:
    return (d.replace(day=1) + relativedelta(months=1)) - timedelta(days=1)


def meses_no_intervalo(inicio: date, fim: date) -> list[tuple[date, date]]:
    periodos = []
    cursor = inicio.replace(day=1)
    fim_alvo = fim.replace(day=1)
    while cursor <= fim_alvo:
        periodos.append((cursor, fim_de_mes(cursor)))
        cursor = cursor + relativedelta(months=1)
    return periodos


def mes_anterior_ao_vigente() -> date:
    return (date.today().replace(day=1) - timedelta(days=1)).replace(day=1)


def fmt(d: date) -> str:
    return d.strftime("%d/%m/%Y")


# ── Leitura do auxiliar ───────────────────────────────────────────────────────

def carregar_centros_ativos(tipo_path: str = 'socios') -> list[dict]:
    path = AUXILIAR_PATH_SOCIOS if tipo_path == 'socios' else AUXILIAR_PATH_PAINEL

    df = pd.read_excel(path, sheet_name="centros_custo")
    df = df[df["ativo"].str.strip().str.lower() == "sim"]
    return df.to_dict(orient="records")


def parse_data(val) -> date | None:
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, pd.Timestamp):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


# ── Detecção de progresso anterior ────────────────────────────────────────────

_RE_PERIODO = re.compile(r'_(\d{6})_(sintetico|analitico)\.xlsx$')


def ultimo_periodo_completo(slug_cc: str) -> str | None:
    """
    Varre dados_brutos/ e retorna o AAAAMM do último mês onde AMBOS
    sintetico E analitico existem em disco.

    Retorna None se nenhum par completo encontrado (primeiro run).

    Usado para retomar o download a partir do mês com falha — evita
    rebaixar toda a série histórica. O último par completo é incluído
    no reprocessamento para garantir que pares incompletos (onde apenas
    um dos dois foi baixado) também sejam refeitos.
    """
    brutos = BASE_OUTPUT_DIR / slug_cc / "dados_brutos"
    if not brutos.exists():
        return None

    periodos: dict[str, set] = {}
    for f in brutos.iterdir():
        m = _RE_PERIODO.search(f.name)
        if m:
            aamm, tipo = m.group(1), m.group(2)
            periodos.setdefault(aamm, set()).add(tipo)

    completos = sorted(
        aamm for aamm, tipos in periodos.items()
        if {"sintetico", "analitico"}.issubset(tipos)
    )
    return completos[-1] if completos else None


# ── Helper genérico de lupa (botProcurar) ────────────────────────────────────

def selecionar_via_lupa(
        driver,
        wdw: WebDriverWait,
        locator_lupa: tuple,
        campo_pesquisa_name: str,
        codigo: str | list,
        descricao: str = "",
        busca_simple: bool = True,
) -> None:
    """
    Preenche qualquer campo de lookup do Sienge via botProcurar.

    busca_simple=True  → digita código + Procurar + marca 1º checkbox + Selecionar
    busca_simple=False → itera lista de siglas clicando nas tds da tabelaResultado
    """
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


# ── Navegação e iframe ────────────────────────────────────────────────────────

def abrir_relatorio(driver, wdw: WebDriverWait) -> None:
    driver.get(REPORT_URL)
    sleep(3)
    SeleniumRequester.fechar_popup_novidade(wdw)
    SeleniumRequester.entrar_iframe(driver)
    logger.info("Iframe '%s' ativo", IFRAME_NAME)


# ── Filtros fixos (executados uma única vez por centro de custo) ──────────────

def preencher_filtros_fixos(driver, wdw: WebDriverWait, centro: dict, tipo_path: str = 'socios') -> None:
    # Empresa
    selecionar_via_lupa(
        driver, wdw,
        locator_lupa=(By.XPATH, '//img[contains(@onclick, "empresaAut")]'),
        campo_pesquisa_name="entity.cdEmpresaView",
        codigo=str(int(centro["cod_empresa"])),
        descricao="Empresa",
    )

    # Ordem → Cliente
    Select(wdw.until(EC.element_to_be_clickable((By.NAME, "flOrdem")))).select_by_value("CLI")

    # Centro de custo (se definido)
    cod_cc = centro.get("cod_centro_custo")
    if cod_cc and not (isinstance(cod_cc, float) and pd.isna(cod_cc)):
        selecionar_via_lupa(
            driver, wdw,
            locator_lupa=(By.XPATH, '//img[contains(@onclick, "centroCusto")]'),
            campo_pesquisa_name="entity.cdEmpreendView",
            codigo=str(int(cod_cc)),
            descricao="Centro de custo",
        )

    # Coluna
    valor_coluna = "J" if "juros" in str(centro.get("tipo_coluna", "")).lower() else "P"
    Select(wdw.until(EC.element_to_be_clickable((By.NAME, "flColuna")))).select_by_value(valor_coluna)

    if tipo_path == 'socios':
        # Tipo de lançamento → Contas a receber
        Select(wdw.until(EC.element_to_be_clickable((By.NAME, "flTipoSelecao")))).select_by_value("CR")

        # Abre parâmetros avançados
        SeleniumRequester.aguardar_e_clicar(
            wdw, (By.XPATH, '//img[contains(@name, "toggleFiltro")]')
        )
        sleep(0.5)

        # Documento → CT
        selecionar_via_lupa(
            driver, wdw,
            locator_lupa=(By.XPATH, '//img[contains(@onclick, "docMultFilterContaRecebidas")]'),
            campo_pesquisa_name="entity.documentoPK.cdDocumento",
            codigo="CT",
            descricao="Documento",
        )

        # Tipos de cliente (3 = Registrado, 6 = Venda Direta)
        selecionar_via_lupa(
            driver, wdw,
            locator_lupa=(By.XPATH, '//img[contains(@onclick, "tipoCliente")]'),
            campo_pesquisa_name="entity.tipoClientePK.cdTipoCliente",
            codigo=TIPOS_CLIENTE_IDS,
            descricao="Clientes",
            busca_simple=False,
        )

        # Condições de pagamento (21)
        selecionar_via_lupa(
            driver, wdw,
            locator_lupa=(By.XPATH, '//img[contains(@onclick, "tipoCondicao")]'),
            campo_pesquisa_name="tipoCondicaoPK.cdTipoCondicao",
            codigo=CONDICOES_SIGLAS,
            descricao="Condições",
            busca_simple=False,
        )

    logger.info("Filtros fixos preenchidos para '%s'", centro["centro_custo"])


# ── Período ───────────────────────────────────────────────────────────────────

def atualizar_periodo(wdw: WebDriverWait, inicio: date, fim: date) -> None:
    for name, valor in [("dtRectoFim", fmt(fim)), ("dtRectoInicio", fmt(inicio))]:
        campo = wdw.until(EC.element_to_be_clickable((By.NAME, name)))
        campo.click()
        campo.clear()
        campo.send_keys(valor)
        sleep(0.2)


# ── Download par Sintético + Analítico ───────────────────────────────────────

def _toggle_sintetico(wdw: WebDriverWait, ativar: bool) -> None:
    cb = wdw.until(EC.element_to_be_clickable(
        (By.XPATH, '//input[@type="checkbox" and (@name="flSintetico" or @id="flSintetico")]')
    ))
    if cb.is_selected() != ativar:
        cb.click()
    sleep(0.3)


def _limpar_temp(requester: SeleniumRequester) -> None:
    """
    Remove todos os .xlsx e .crdownload da pasta temp antes de cada
    geração — garante que aguardar_download nunca pegue um arquivo
    de uma rodada anterior que ainda não foi movido.
    """
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
        label: str,  # ex: grand_paladium__obra_202509_sintetico
        tipo_path: str = 'socios'
) -> bool:
    """
    Gera o relatório, verifica se há dados e, caso positivo, aguarda o
    download e move para dados_brutos/{slug_cc}/{label}.xlsx.

    Retorna True se o arquivo foi baixado, False se o Sienge indicou
    'Não há registros para os parâmetros informados.'

    Fluxo:
      1. Limpa a pasta temp  ← impede que arquivo anterior seja retornado
      2. Clica em Gerar
      3. Verifica alerta de sem dados
      4. Aguarda o download aparecer na pasta temp (agora vazia)
      5. Move e renomeia para dados_brutos/
    """
    # 1. Pasta temp vazia antes de disparar o download
    _limpar_temp(requester)

    # 2. Dispara a geração
    SeleniumRequester.aguardar_e_clicar(
        wdw, (By.ID, "btFiltrar"), f"Gerar – {label}"
    )

    sleep(1.5)
    # 3. Verifica alerta de sem dados
    if SeleniumRequester.verificar_sem_dados(driver, wdw):
        logger.info("Sem registros → %s (pulando)", label)
        return False

    # 4. Aguarda o arquivo aparecer na pasta temp (sabidamente vazia)
    arquivo = requester.aguardar_download(extensao=".xlsx")

    # 5. Move para o destino final com nomenclatura correta
    destino = pasta_brutos(slug_cc, tipo_path) / f"{label}.xlsx"
    shutil.move(str(arquivo), str(destino))
    logger.info("Salvo → %s", destino.relative_to(BASE_OUTPUT_DIR))
    return True


def baixar_par(
        driver,
        wdw: WebDriverWait,
        requester: SeleniumRequester,
        slug_cc: str,
        periodo_aamm: str,  # ex: "202509",
        tipo_path: str = 'socios'
) -> None:
    """
    Baixa Sintético e Analítico do período já preenchido.
    Se o sintético não tiver registros, pula o analítico também
    (ambos compartilham os mesmos filtros — se um está vazio, o outro também estará).
    """
    label_ana = f"{slug_cc}_serie_historica_ate_{periodo_aamm}_analitico"

    teve_dados = _gerar_e_salvar(driver, wdw, requester, slug_cc, label_ana, tipo_path)
    sleep(1)

    if not teve_dados:
        # Sem dados no sintético → analítico também estará vazio; pula o par inteiro
        return


# ── Fluxo principal por centro de custo ──────────────────────────────────────

def processar_centro(
        driver,
        wdw: WebDriverWait,
        requester: SeleniumRequester,
        centro: dict,
        tipo_path: str = 'socios'
) -> None:
    nome_cc = str(centro["centro_custo"]).strip()
    slug_cc = nome_cc.lower().replace(" ", "_").replace("-", "")[:40]

    # Garante estrutura de pastas para este centro
    pasta_brutos(slug_cc, tipo_path)

    # Filtros fixos: uma única vez
    abrir_relatorio(driver, wdw)
    preencher_filtros_fixos(driver, wdw, centro, tipo_path)

    fim_ultimo_mes = fim_de_mes(mes_anterior_ao_vigente())
    periodo_aamm = fim_ultimo_mes.strftime("%Y%m")

    atualizar_periodo(wdw, date(day=1, month=1, year=2000), fim_ultimo_mes)

    baixar_par(driver, wdw, requester, slug_cc, periodo_aamm, tipo_path)


# ── Entrypoint ────────────────────────────────────────────────────────────────


def main(tipo_path: str = 'socios') -> None:

    centros = carregar_centros_ativos(tipo_path=tipo_path)
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

            # Novo driver por centro — sessão sempre limpa, sem estado anterior
            driver = requester.get_driver()
            wdw = requester.waiter(driver)
            SeleniumRequester.navegacao_inicial(driver, wdw)

            processar_centro(driver, wdw, requester, centro, tipo_path)

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
            # Fecha o driver independentemente de sucesso ou erro
            if driver:
                try:
                    driver.quit()
                    logger.info("Driver encerrado")
                except Exception:
                    pass
                sleep(2)

    logger.info("Concluído. Arquivos em: %s", BASE_OUTPUT_DIR)


# criar um if que vai permitir escolher entre a extracao das empresas dos socios e do painel
# quando for referente ao painel od filtros preenchimentos serão os abaixos

# período de recebimento: 01/01/2000 a 31/12/2090
# empresa: pegar do auxiliar
# centro de custo: pegar do auxiliar
# ordem: Cliente

# Observação: no lagoon clube não filtrar pelo tipo de cliente

if __name__ == "__main__":
    main(tipo_path='socios')
