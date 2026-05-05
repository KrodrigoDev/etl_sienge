import pandas as pd
import logging
from time import sleep

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)

URL_CADASTRO_CLIENTE = f"{BASE_URL}/8/index.html#/apoio/pessoas/clientes"

INPUT_DADOS = '../rpa/input/empreendimentos_2674489_4-5-2026.xlsx'

# necessário para saber o andamento e antes de rodar novamente verificar se já existe para pular os clientes que já foram feitos
# caso tenha algum erro e seja necessário rodar novamente
OUTPUT_DADOS = '../rpa/output/empreendimentos_2674489_4-5-2026_output.xlsx'

# Valor do option "REGISTRADO" no select de tipo cliente
CD_TIPO_CLIENTE_REGISTRADO = "3"


def processamento_dados() -> pd.DataFrame:
    """
    Considerar apenas os usuários que tiveram a data de inclusão dos dados de registro (CRI) e
    criar uma coluna chamada 'situacao' para conseguir acompanhar o progresso dos registros.
    Também cria coluna 'tipo' para indicar se é titular ou cônjuge.

    :return: Dataframe com os dados do registro válidos
    """
    df_clientes = pd.read_excel(INPUT_DADOS)

    df_clientes = df_clientes.dropna(subset=['Data de Inclusão dos Dados de Registro(CRI)'])

    colunas = df_clientes.columns.tolist()

    if 'situacao' not in colunas:
        df_clientes.loc[:, 'situacao'] = pd.NA

    if 'tipo' not in colunas:
        df_clientes.loc[:, 'tipo'] = 'titular'

    return df_clientes


def salvar_progresso(df: pd.DataFrame) -> None:
    """Persiste o DataFrame com o progresso atual no arquivo de output."""
    df.to_excel(OUTPUT_DADOS, index=False)
    logger.info(f"Progresso salvo em: {OUTPUT_DADOS}")


def verificar_unico_resultado(driver) -> bool:
    """
    Verifica se a tabela de resultados retornou exatamente 1 registro,
    checando o elemento de paginação '1–1 de 1'.

    :return: True se houver exatamente 1 resultado, False caso contrário
    """
    try:
        paginacao_elements = driver.find_elements(
            By.CSS_SELECTOR, "p.MuiTablePagination-displayedRows"
        )
        for el in paginacao_elements:
            texto = el.text.strip()
            logger.debug(f"Paginação encontrada: '{texto}'")
            # Aceita formatos como "1–1 de 1" ou "1-1 de 1"
            if texto.replace("–", "-").startswith("1-1 de 1"):
                return True
        return False
    except NoSuchElementException:
        return False


def clicar_botao_editar(wdw) -> bool:
    """
    Clica no botão de editar (ícone de lápis) da linha de resultado.

    :return: True se conseguiu clicar, False caso contrário
    """
    try:
        botao_editar = wdw.until(
            EC.element_to_be_clickable(
                (By.XPATH, '//button[@type="button" and .//*[@aria-label="Editar"]]')
            )
        )
        botao_editar.click()
        logger.info("Botão Editar clicado com sucesso.")
        return True
    except TimeoutException:
        logger.warning("Botão Editar não encontrado ou não clicável.")
        return False


def entrar_iframe(driver, wdw) -> bool:
    """
    Aguarda e entra no iframe do formulário de edição.

    :return: True se conseguiu entrar no iframe, False caso contrário
    """
    try:
        driver.switch_to.default_content()
        wdw.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "iFramePage")))
        logger.info("Entrou no iframe do formulário.")
        return True
    except TimeoutException:
        logger.warning("Iframe 'iFramePage' não encontrado.")
        return False


def atualizar_tipo_cliente_se_necessario(driver, wdw) -> bool:
    """
    Verifica o select de tipo cliente. Caso não esteja como REGISTRADO (value="3"),
    altera para REGISTRADO.

    :return: True se o campo estava correto ou foi alterado com sucesso, False em caso de erro
    """
    try:
        select_element = wdw.until(
            EC.presence_of_element_located((By.NAME, "entity.cdTipoCliente"))
        )
        select = Select(select_element)
        valor_atual = select.first_selected_option.get_attribute("value")

        logger.info(f"Tipo cliente atual: value='{valor_atual}' | texto='{select.first_selected_option.text}'")

        if valor_atual == CD_TIPO_CLIENTE_REGISTRADO:
            logger.info("Tipo cliente já está como REGISTRADO. Nenhuma alteração necessária.")
            return True

        # Altera para REGISTRADO
        select.select_by_value(CD_TIPO_CLIENTE_REGISTRADO)
        logger.info("Tipo cliente alterado para REGISTRADO.")
        return True

    except TimeoutException:
        logger.error("Select 'entity.cdTipoCliente' não encontrado no iframe.")
        return False


def clicar_salvar(driver, wdw) -> bool:
    """
    Clica no botão Salvar dentro do iframe.

    :return: True se conseguiu clicar, False caso contrário
    """
    try:
        botao_salvar = wdw.until(
            EC.element_to_be_clickable((By.NAME, "pbSalvar"))
        )
        botao_salvar.click()
        logger.info("Botão Salvar clicado com sucesso.")
        sleep(2)  # aguarda o processamento do save
        return True
    except TimeoutException:
        logger.error("Botão Salvar não encontrado ou não clicável.")
        return False


def obter_nome_conjuge(driver, wdw) -> str | None:
    """
    Dentro do iframe do formulário do cliente, clica na aba de Cônjuge
    e retorna o nome do cônjuge caso o campo esteja preenchido.

    :return: Nome do cônjuge como string, ou None se não houver
    """
    try:
        # Clica no link da aba Cônjuge (ainda dentro do iframe)
        link_conjuge = wdw.until(
            EC.element_to_be_clickable(
                (By.XPATH, '//a[normalize-space(text())="Cônjuge"]')
            )
        )
        link_conjuge.click()
        logger.info("Aba Cônjuge clicada.")
        sleep(1)

        # Lê o campo de nome do cônjuge
        campo_nome = wdw.until(
            EC.presence_of_element_located((By.NAME, "entity.conjuge.nmConjuge"))
        )
        nome_conjuge = campo_nome.get_attribute("value").strip()

        if nome_conjuge:
            logger.info(f"Cônjuge encontrado: '{nome_conjuge}'")
            return nome_conjuge

        logger.info("Campo nome do cônjuge está vazio.")
        return None

    except TimeoutException:
        logger.warning("Aba ou campo de cônjuge não encontrado.")
        return None


def processar_cliente(nome: str, tipo: str, driver, wdw, req) -> tuple[bool, str | None]:
    """
    Fluxo completo de busca, edição e salvamento de um cliente (titular ou cônjuge).

    :return: (sucesso, nome_conjuge_encontrado)
        - sucesso: True se o cliente foi processado com êxito
        - nome_conjuge_encontrado: nome do cônjuge se for titular e tiver cônjuge, senão None
    """
    logger.info(f"Processando [{tipo}]: '{nome}'")

    # ── Busca o cliente ──────────────────────────────────────────────────────
    driver.switch_to.default_content()
    campo_cliente = req.aguardar_e_clicar(wdw, (By.NAME, "nomeCliente"))
    campo_cliente.send_keys(Keys.CONTROL, "a")
    campo_cliente.send_keys(Keys.DELETE)
    campo_cliente.send_keys(nome)

    req.aguardar_e_clicar(
        wdw,
        (By.XPATH, '//button[@type="submit" and .//text()[contains(.,"Consultar")]]'),
        "Consultar",
    )

    req.aguardar_carregamento_tabela(driver)

    # ── Verifica resultado único ─────────────────────────────────────────────
    if not verificar_unico_resultado(driver):
        logger.warning(f"'{nome}' [{tipo}]: resultado não é exatamente 1 registro. Pulando...")
        return False, None

    # ── Abre edição ──────────────────────────────────────────────────────────
    if not clicar_botao_editar(wdw):
        return False, None

    sleep(1.5)

    # ── Entra no iframe ──────────────────────────────────────────────────────
    if not entrar_iframe(driver, wdw):
        return False, None

    # ── Atualiza tipo cliente ────────────────────────────────────────────────
    if not atualizar_tipo_cliente_se_necessario(driver, wdw):
        return False, None

    # ── Salva ────────────────────────────────────────────────────────────────
    if not clicar_salvar(driver, wdw):
        return False, None

    # ── Lê cônjuge apenas para titulares ────────────────────────────────────
    nome_conjuge = None
    if tipo == 'titular':
        # Após o save o iframe recarrega; re-entra para acessar a aba cônjuge
        if entrar_iframe(driver, wdw):
            nome_conjuge = obter_nome_conjuge(driver, wdw)

    return True, nome_conjuge


# ── Inicialização ────────────────────────────────────────────────────────────

req = SeleniumRequester()
req.ensure_login()

driver = req.get_driver()
wdw = req.waiter(driver)

try:
    # ── 1. Acesso inicial ────────────────────────────────────────────────
    req.navegacao_inicial(driver, wdw)

    # ── 2. Navega para o cadastro de clientes ────────────────────────────
    logger.info("Navegando para o cadastro de clientes...")
    driver.get(URL_CADASTRO_CLIENTE)
    sleep(2)

    df_clientes = processamento_dados()

    # Linhas de cônjuges descobertos durante o loop (adicionadas ao final)
    linhas_conjuge: list[dict] = []

    for index, row in df_clientes.iterrows():


        nome = row['Nome Mutuário']
        tipo = row.get('tipo', 'titular')

        # Pula registros já processados com sucesso
        if pd.notna(row['situacao']) and row['situacao'] == 'concluido':
            logger.info(f"[{index}] '{nome}' já processado. Pulando...")
            continue

        try:
            sucesso, nome_conjuge = processar_cliente(
                nome=nome,
                tipo=tipo,
                driver=driver,
                wdw=wdw,
                req=req,
            )

            if not sucesso:
                df_clientes.at[index, 'situacao'] = 'sem_resultado_unico'
                salvar_progresso(df_clientes)
                driver.get(URL_CADASTRO_CLIENTE)
                sleep(1.5)
                continue

            # ── Marca titular como concluído ─────────────────────────────
            df_clientes.at[index, 'situacao'] = 'concluido'
            logger.info(f"[{index}] '{nome}' [{tipo}]: processado com sucesso.")

            # ── Processa cônjuge encontrado ──────────────────────────────
            if nome_conjuge:
                # Evita duplicatas: checa tanto nos cônjuges já enfileirados
                # quanto nos titulares originais do DataFrame
                ja_existe = (
                    any(lc['Nome Mutuário'] == nome_conjuge for lc in linhas_conjuge)
                    or (df_clientes['Nome Mutuário'] == nome_conjuge).any()
                )

                if ja_existe:
                    logger.info(f"Cônjuge '{nome_conjuge}' já existe na tabela. Pulando.")
                else:
                    logger.info(f"Cônjuge '{nome_conjuge}' encontrado. Processando...")

                    driver.switch_to.default_content()
                    driver.get(URL_CADASTRO_CLIENTE)
                    sleep(1.5)

                    sucesso_conjuge, _ = processar_cliente(
                        nome=nome_conjuge,
                        tipo='conjuge',
                        driver=driver,
                        wdw=wdw,
                        req=req,
                    )

                    linhas_conjuge.append({
                        'Nome Mutuário': nome_conjuge,
                        'tipo': 'conjuge',
                        'situacao': 'concluido' if sucesso_conjuge else 'erro_conjuge',
                    })

                    logger.info(
                        f"Cônjuge '{nome_conjuge}': "
                        f"{'concluído' if sucesso_conjuge else 'erro'}."
                    )

            breakpoint()

        except Exception as e:
            logger.error(f"[{index}] '{nome}': erro inesperado — {e}", exc_info=True)
            df_clientes.at[index, 'situacao'] = f'erro: {str(e)[:100]}'

        finally:
            driver.switch_to.default_content()
            salvar_progresso(df_clientes)
            driver.get(URL_CADASTRO_CLIENTE)
            sleep(1.5)

    # ── Consolida cônjuges descobertos no DataFrame final ────────────────
    if linhas_conjuge:
        df_conjuges = pd.DataFrame(linhas_conjuge)
        df_clientes = pd.concat([df_clientes, df_conjuges], ignore_index=True)
        logger.info(f"{len(linhas_conjuge)} cônjuge(s) adicionado(s) ao relatório final.")

    logger.info("Processamento finalizado.")
    salvar_progresso(df_clientes)

finally:
    req.quit(driver)