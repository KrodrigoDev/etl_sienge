"""
stages/extract/extract_usuarios.py
-----------------------------------------
Extrai empresas disponíveis no portal GissOnline
e permite acessar cada empresa posteriormente.
"""

from __future__ import annotations

import logging
import random
from pathlib import Path
from time import sleep
import os
import re

import pandas as pd
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException

from src.drivers.selenium_requester import SeleniumRequester

logger = logging.getLogger(__name__)

load_dotenv()

# ── Configurações ──────────────────────────────────────────────────────────────
URL_BASE = "https://maceio.giss.com.br/portal/home"

pasta_origem = Path(__file__).resolve().parents[2]

INPUT_DIR = pasta_origem / 'stages' / 'transform' / 'input' / 'servico_tomado'
INPUT_DIR.mkdir(parents=True, exist_ok=True)


# ── Helpers  ───────────────────────────────────────────────────

def normalizar_cnpj(cnpj: str) -> str:
    return re.sub(r'\D', '', cnpj)


def pausa_humana(minimo: float = 1.0, maximo: float = 3.5) -> None:
    """Pausa aleatória para simular comportamento humano."""
    tempo = random.uniform(minimo, maximo)
    logger.debug(f"Pausando {tempo:.2f}s")
    sleep(tempo)


def scroll_humano(driver, elemento=None) -> None:
    """Scroll suave até o elemento ou scroll aleatório na página."""
    if elemento:
        driver.execute_script(
            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
            elemento
        )
    else:
        # scroll aleatório para simular leitura
        y = random.randint(100, 400)
        driver.execute_script(f"window.scrollBy({{top: {y}, behavior: 'smooth'}});")
    pausa_humana(0.5, 1.5)


def mover_mouse_e_clicar(driver, elemento) -> None:
    """Move o mouse gradualmente até o elemento antes de clicar."""
    actions = ActionChains(driver)
    actions.move_to_element(elemento)
    actions.pause(random.uniform(0.3, 0.8))
    actions.click()
    actions.perform()


def digitar_humanamente(campo, texto: str) -> None:
    """
    Digita um texto caractere por caractere com delay
    variável entre teclas, como um humano faria.
    """
    campo.clear()
    pausa_humana(0.3, 0.7)

    for char in texto:
        campo.send_keys(char)
        sleep(random.uniform(0.05, 0.22))

    pausa_humana(0.3, 0.8)


# ── Funções do fluxo ──────────────────────────────────────────────────

def fechar_modal(driver) -> None:
    """Fecha o modal de aviso do GissOnline ao entrar no site."""

    pausa_humana(2.0, 3.5)

    xpaths_botoes = [
        '//div[contains(@class,"modal") and contains(@style,"display: block")]//button[contains(text(),"OK")]',
        '//div[contains(@class,"modal") and contains(@style,"display: block")]//button[contains(@class,"close")]',
    ]

    clicou = False

    for xpath in xpaths_botoes:
        try:
            botao = driver.find_element(By.XPATH, xpath)
            logger.info(f"Tentando fechar modal com xpath:\n{xpath}")

            driver.execute_script("""
                arguments[0].style.display = 'block';
                arguments[0].style.visibility = 'visible';
                arguments[0].style.opacity = 1;
            """, botao)

            pausa_humana(0.5, 1.0)
            driver.execute_script("arguments[0].click();", botao)
            logger.info("Modal fechado.")
            clicou = True
            break

        except Exception as e:
            logger.debug(f"Falhou no xpath: {xpath} → {e}")

    if not clicou:
        logger.info("Tentando fechar modal com ESC")
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)


def fazer_login(driver) -> None:
    pausa_humana(1.0, 2.0)

    campo_usuario = driver.find_element(By.ID, "usuario")
    scroll_humano(driver, campo_usuario)
    digitar_humanamente(campo_usuario, os.getenv('USUARIO_GISS'))

    pausa_humana(0.5, 1.2)

    campo_senha = driver.find_element(By.ID, "senha")
    digitar_humanamente(campo_senha, os.getenv('SENHA_GISS'))

    pausa_humana(0.8, 1.5)

    botao = driver.find_element(
        By.XPATH,
        '//button[@type="submit" and contains(text(),"Acessar")]'
    )
    mover_mouse_e_clicar(driver, botao)


def mostrar_itens_empresa(wdw) -> None:
    pausa_humana(1.5, 3.0)

    select_elem = wdw.until(
        EC.presence_of_element_located(
            (
                By.XPATH,
                '//select[contains(@name,"DataTables_Table_") and contains(@name,"_length")]'
            )
        )
    )

    pausa_humana(1.5, 3.0)

    Select(select_elem).select_by_visible_text("50")


def garantir_sem_backdrop(driver, wdw, timeout: int = 10) -> bool:
    """
    Aguarda o modal-backdrop sumir completamente antes de prosseguir.
    Se ainda estiver presente, tenta fechá-lo via ESC ou clique no botão de fechar.

    Retorna True se a tela ficou limpa, False se não conseguiu resolver.
    """
    XPATH_BACKDROP = '//div[contains(@class,"modal-backdrop") and contains(@class,"show")]'
    XPATH_BTN_FECHAR = (
        '//div[contains(@class,"modal") and contains(@class,"show")]'
        '//*[self::button[@data-dismiss="modal"] or self::button[contains(@class,"close")] '
        'or self::button[normalize-space()="Ok"] or self::button[normalize-space()="OK"]]'
    )

    # -- 1. Verifica se há backdrop ativo --
    backdrops = driver.find_elements(By.XPATH, XPATH_BACKDROP)

    if not backdrops:
        return True  # tela já limpa

    logger.warning("modal-backdrop detectado — tentando fechar modal antes de prosseguir.")

    # -- 2. Tenta clicar no botão de fechar do modal --
    try:
        btn = driver.find_element(By.XPATH, XPATH_BTN_FECHAR)
        driver.execute_script("arguments[0].click();", btn)
        logger.info("Botão de fechar modal clicado via JS.")
        pausa_humana(1.0, 2.0)
    except Exception:
        logger.info("Botão de fechar não encontrado — tentando ESC.")
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
        pausa_humana(1.0, 2.0)

    # -- 3. Aguarda backdrop desaparecer --
    try:
        wdw.until(
            EC.invisibility_of_element_located((By.XPATH, XPATH_BACKDROP))
        )
        logger.info("Backdrop removido com sucesso.")
        return True

    except TimeoutException:
        # -- 4. Último recurso: remove via JS --
        logger.warning("Backdrop persistente — removendo via JS.")
        driver.execute_script("""
            document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
            document.body.classList.remove('modal-open');
            document.body.style.overflow = '';
        """)
        pausa_humana(0.5, 1.0)

        backdrops_restantes = driver.find_elements(By.XPATH, XPATH_BACKDROP)
        if backdrops_restantes:
            logger.error("Não foi possível remover o backdrop.")
            return False

        logger.info("Backdrop removido via JS.")
        return True


def extrair_empresas(driver, wdw, req) -> list[dict]:
    """Retorna lista de empresas disponíveis na tabela."""

    mostrar_itens_empresa(wdw)

    req.aguardar_presenca(wdw, (By.ID, "DataTables_Table_0"))

    wdw.until(
        lambda d: len(
            d.find_elements(
                By.XPATH,
                '//table[@id="DataTables_Table_0"]/tbody/tr'
            )
        ) > 1
    )

    pausa_humana(1.0, 2.0)

    linhas = driver.find_elements(
        By.XPATH,
        '//table[@id="DataTables_Table_0"]/tbody/tr'
    )

    logger.info(f'Linhas encontradas: {len(linhas)}')
    empresas = []

    for i, linha in enumerate(linhas):
        try:
            colunas = linha.find_elements(By.TAG_NAME, 'td')
            if len(colunas) < 5:
                continue

            empresa = {
                'indice': i,
                'cnpj': colunas[0].text.strip(),
                'razao_social': colunas[1].text.strip(),
                'inscricao_municipal': colunas[2].text.strip(),
                'municipio': colunas[3].text.strip(),
            }
            empresas.append(empresa)

        except Exception as e:
            logger.warning(f'Erro na linha {i}: {e}')

    logger.info(f'{len(empresas)} empresas encontradas')
    for emp in empresas:
        logger.info(f"  [{emp['indice']}] {emp['cnpj']} — {emp['razao_social']}")

    return empresas


def selecionar_empresa(driver, wdw, cnpj: str) -> bool:
    """Clica no botão 'Selecionar Empresa' para o CNPJ informado."""

    xpath_botao = f'''
        //tr[
            td[contains(normalize-space(), "{cnpj}")]
        ]
        //button[@title="Selecionar Empresa"]
    '''

    try:
        botao = wdw.until(EC.presence_of_element_located((By.XPATH, xpath_botao)))
        scroll_humano(driver, botao)
        pausa_humana(0.5, 1.2)
        mover_mouse_e_clicar(driver, botao)
        logger.info(f"Empresa selecionada: {cnpj}")
        pausa_humana(2.0, 4.0)  # aguarda carregamento pós-clique
        return True

    except Exception as e:
        logger.error(f"Não foi possível selecionar {cnpj}: {e}")
        return False


def acessar_servicos_tomados(driver) -> None:
    """Navega até a tela de consulta de NFS-e de serviços tomados."""
    pausa_humana(1.5, 3.0)
    driver.get(f"{URL_BASE}#/operacao/servicos-comprados")
    pausa_humana(2.0, 3.5)
    driver.get(f"{URL_BASE}#/operacao/servicos-comprados/consultar-nfse")
    pausa_humana(2.0, 4.0)


def preencher_filtro_periodo(driver, competencia: str) -> None:
    """Seleciona o filtro por período e preenche as datas."""

    campo_competencia = driver.find_element(By.ID, 'competencia')
    scroll_humano(driver, campo_competencia)
    campo_competencia.clear()
    digitar_humanamente(campo_competencia, competencia)

    pausa_humana(0.5, 1.0)


def clicar_consultar(driver, wdw, req) -> None:

    garantir_sem_backdrop(driver, wdw)

    req.aguardar_e_clicar(wdw, locator=(By.ID, "botaoConsultar"))
    pausa_humana(2.0, 4.0)


def ajustar_paginacao(driver, wdw, tamanho: str = "50") -> None:
    """Seleciona quantidade de itens por página."""
    select_elem = wdw.until(
        EC.presence_of_element_located(
            (By.XPATH, '//select[@ng-model="size"]')
        )
    )
    scroll_humano(driver, select_elem)
    pausa_humana(0.5, 1.0)
    Select(select_elem).select_by_visible_text(tamanho)
    pausa_humana(1.5, 3.0)


def extrair_todas_paginas(driver, wdw) -> list[dict]:
    """
    Extrai notas de todas as páginas disponíveis,
    clicando em 'Próximo' até ele desaparecer ou ficar desabilitado.
    """
    todas_notas = []
    pagina = 1

    while True:
        logger.info(f"  Extraindo página {pagina}...")

        # extrai página atual
        notas_pagina = extrair_notas_tabela(driver)
        todas_notas.extend(notas_pagina)

        logger.info(f"  → {len(notas_pagina)} notas na página {pagina} | total acumulado: {len(todas_notas)}")

        # simula leitura da página
        # pausa_humana(1.5, 3.5) Sem pausas para realizar extração

        # ── verifica botão Próximo ────────────────────────────────────
        botoes_proximo = driver.find_elements(
            By.XPATH,
            '//button[@type="button" and contains(@class,"page-link") and contains(@ng-click,"pageatual +1")]'
        )

        if not botoes_proximo:
            logger.info("  Botão 'Próximo' não encontrado — última página atingida.")
            break

        botao_proximo = botoes_proximo[0]

        # checa se está desabilitado (atributo disabled ou classe disabled no pai li)
        desabilitado = (
                botao_proximo.get_attribute("disabled") is not None
                or not botao_proximo.is_enabled()
                or "disabled" in (botao_proximo.find_element(By.XPATH, "..").get_attribute("class") or "")
        )

        if desabilitado:
            logger.info("  Botão 'Próximo' desabilitado — última página atingida.")
            break

        # ── clica em próximo ──────────────────────────────────────────
        scroll_humano(driver, botao_proximo)
        pausa_humana(0.5, 1.2)
        mover_mouse_e_clicar(driver, botao_proximo)

        # aguarda nova página carregar (espera as linhas mudarem)
        pagina += 1
        pausa_humana(2.0, 4.0)

        try:
            wdw.until(
                EC.presence_of_element_located(
                    (By.XPATH, '//table[contains(@class,"table")]//tbody/tr[contains(@ng-repeat,"nota")]')
                )
            )
        except Exception:
            logger.warning("  Timeout aguardando próxima página — encerrando paginação.")
            break

    logger.info(f"  Paginação concluída: {pagina} página(s), {len(todas_notas)} notas no total.")
    return todas_notas


def extrair_notas_tabela(driver) -> list[dict]:
    """
    Extrai todas as linhas visíveis da tabela de NFS-e.
    Retorna lista de dicts com os campos de cada nota.
    """
    linhas = driver.find_elements(
        By.XPATH,
        '//table[contains(@class,"table")]//tbody/tr[contains(@ng-repeat,"nota")]'
    )

    logger.info(f"Notas encontradas na página: {len(linhas)}")
    notas = []

    for i, linha in enumerate(linhas):
        try:
            colunas = linha.find_elements(By.TAG_NAME, 'td')

            # índices conforme o HTML analisado:
            # 0=Competência, 1=NFS, 4=Controle, 5=Emissão,
            # 7=CNPJ/CPF, 8=Prestador, 9=Atividade, 11=Valor, 12=Situação, 13=Declaração

            nota = {
                'competencia': colunas[0].text.strip() if len(colunas) > 0 else '',
                'nfs': colunas[1].text.strip() if len(colunas) > 1 else '',
                'emissao': colunas[5].text.strip() if len(colunas) > 5 else '',
                'cnpj_cpf': colunas[7].text.strip() if len(colunas) > 7 else '',
                'prestador': colunas[8].text.strip() if len(colunas) > 8 else '',
                'atividade': colunas[9].text.strip() if len(colunas) > 9 else '',
                'valor': colunas[11].text.strip() if len(colunas) > 11 else '',
                'situacao': colunas[12].text.strip() if len(colunas) > 12 else '',
                'declaracao': colunas[13].text.strip() if len(colunas) > 13 else '',
            }

            notas.append(nota)

            # simula "leitura" ocasional da linha
            if random.random() < 0.15:
                pausa_humana(0.3, 1.0)

        except Exception as e:
            logger.warning(f"Erro na linha {i}: {e}")

    return notas


def remover_aviso(driver, wdw) -> bool:
    """
    Fecha o popup SweetAlert caso apareça após a consulta.

    Retorna:
        True  -> aviso apareceu e foi fechado
        False -> aviso não apareceu
    """

    try:
        logger.info("Verificando aviso de ausência de notas...")

        # aguarda o modal aparecer
        botao_ok = wdw.until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    '//div[contains(@class,"sweet-alert") and contains(@class,"visible")]'
                    '//button[contains(@class,"confirm") and normalize-space()="Ok"]'
                )
            )
        )

        pausa_humana(0.5, 1.2)

        scroll_humano(driver, botao_ok)

        # clique via JS evita problema de overlay
        driver.execute_script("arguments[0].click();", botao_ok)

        logger.info("Aviso encontrado e fechado.")
        pausa_humana(1.0, 2.0)

        garantir_sem_backdrop(driver, wdw)

        return True

    except TimeoutException:
        logger.info("Nenhum aviso encontrado.")
        return False

    except Exception as e:
        logger.warning(f"Erro ao remover aviso: {e}")
        return False


def processar_empresa(driver, wdw, req, cnpj: str, competencia: str, novo_cnpj: bool = True) -> list[dict]:
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Processando CNPJ: {cnpj} | Competência: {competencia}")
    logger.info(f"{'=' * 60}")

    if novo_cnpj:
        driver.get(f"{URL_BASE}#/login-portal")
        fechar_modal(driver)
        fazer_login(driver)
        pausa_humana(2.0, 4.0)

        mostrar_itens_empresa(wdw)


        ok = selecionar_empresa(driver, wdw, cnpj)
        if not ok:
            logger.error(f"Pulando CNPJ {cnpj} — não foi possível selecionar.")
            return []

    garantir_sem_backdrop(driver, wdw)

    # ← sempre executa, independente de novo_cnpj
    acessar_servicos_tomados(driver)
    preencher_filtro_periodo(driver, competencia)
    clicar_consultar(driver, wdw, req)

    teve_aviso = remover_aviso(driver, wdw)

    if teve_aviso:
        logger.info("Consulta sem notas fiscais.")
        return []

    ajustar_paginacao(driver, wdw, "50")

    notas = extrair_todas_paginas(driver, wdw)
    logger.info(f"  → {len(notas)} notas extraídas para {cnpj} na competência {competencia}")

    pausa_humana(3.0, 7.0)
    return notas


MES_INICIAL = 1
MES_FINAL = 5


def main():
    req = SeleniumRequester(profile='Edge_02', download_dir=None)
    driver = req.get_driver()
    wdw = req.waiter(driver)

    driver.get(f"{URL_BASE}#/login-portal")
    fechar_modal(driver)
    fazer_login(driver)

    empresas = extrair_empresas(driver, wdw, req)

    dfs = []

    for emp in empresas:
        cnpj = emp['cnpj']

        cnpj_normalizado = normalizar_cnpj(cnpj)

        path_cnpj = INPUT_DIR / cnpj_normalizado

        if path_cnpj.exists() and len(list(path_cnpj.glob('*.csv*'))) > 1:
            logger.info(f'Pulando o {cnpj} por já existir')
            continue

        path_cnpj.mkdir(parents=True, exist_ok=True)

        for mes in range(MES_INICIAL, MES_FINAL + 1):
            try:
                notas = processar_empresa(
                    driver, wdw, req,
                    cnpj,
                    competencia=f'{mes:02d}/2026',
                    novo_cnpj=(mes == MES_INICIAL),
                )

                if notas:
                    df_cnpj = pd.DataFrame(notas)
                    df_cnpj['cnpj_empresa'] = cnpj

                    df_cnpj.to_csv(
                        path_cnpj / f'competencia_{mes:02d}-2026.csv',
                        index=False,
                        sep=';'
                    )
                    dfs.append(df_cnpj)
                    logger.info(f"{cnpj} | {mes:02d}/2026: {len(notas)} notas salvas")
                else:
                    logger.info(f"{cnpj} | {mes:02d}/2026: sem notas")

            except Exception as e:
                logger.exception(f"Erro ao processar {cnpj} competência {mes:02d}/2026: {e}")

            pausa_humana(3.0, 10.0)

        pausa_humana(3.0, 10.0)  # pausa entre empresas

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    df_final = main()

    df_final.to_csv(INPUT_DIR / "df_final.csv", index=False, sep=';')
