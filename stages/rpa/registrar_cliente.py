import os
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
OUTPUT_DADOS = '../rpa/output/empreendimentos_2674489_4-5-2026_output.xlsx'

# Valor do option "REGISTRADO" no select de tipo cliente
CD_TIPO_CLIENTE_REGISTRADO = "3"


def normalizar_cpf_cnpj(valor) -> str:
    """
    Garante que CPF (11 dígitos) e CNPJ (14 dígitos) tenham os zeros à esquerda.
    Trata valores numéricos que o Excel converte para int/float perdendo o zero inicial.

    Exemplos:
        6171535400   -> '06171535400'   (CPF com zero faltando)
        61715354000  -> '061715354000'  (nunca deve ocorrer, mas seguro)
        12345678000195 -> '12345678000195' (CNPJ já correto)

    :return: String com zeros à esquerda preenchidos conforme tamanho (11 ou 14)
    """
    # Remove casas decimais caso o Excel tenha lido como float (ex: 6171535400.0)
    valor_str = str(valor).strip().split('.')[0]

    tamanho = len(valor_str)

    if tamanho <= 11:
        return valor_str.zfill(11)  # CPF
    else:
        return valor_str.zfill(14)  # CNPJ


def processamento_dados() -> pd.DataFrame:
    """
    Retorna o DataFrame de clientes a processar, priorizando o output já existente.

    - Se OUTPUT_DADOS existir: carrega ele (contém progresso anterior + cônjuges já
      descobertos) e filtra apenas as linhas com situacao diferente de 'concluido'.
    - Se não existir: carrega o INPUT_DADOS, filtra apenas quem tem data de CRI,
      e inicializa as colunas de controle 'situacao' e 'tipo'.

    Em ambos os casos normaliza a coluna 'CPF/CNPJ Mutuário' com zeros à esquerda.

    :return: DataFrame pronto para iterar
    """
    if os.path.exists(OUTPUT_DADOS):
        logger.info(f"Output anterior encontrado. Carregando '{OUTPUT_DADOS}'...")
        df = pd.read_excel(OUTPUT_DADOS, dtype={'CPF/CNPJ Mutuário': str})
        pendentes = df[df['situacao'] != 'concluido'].shape[0]
        logger.info(f"{pendentes} registro(s) pendente(s) para processar.")
    else:
        logger.info("Nenhum output anterior. Carregando input original...")
        df = pd.read_excel(INPUT_DADOS, dtype={'CPF/CNPJ Mutuário': str})
        df = df.dropna(subset=['Data de Inclusão dos Dados de Registro(CRI)'])

        colunas = df.columns.tolist()
        if 'situacao' not in colunas:
            df.loc[:, 'situacao'] = pd.NA
        if 'tipo' not in colunas:
            df.loc[:, 'tipo'] = 'titular'

    # Normaliza zeros à esquerda em toda execução (input e output)
    df['CPF/CNPJ Mutuário'] = df['CPF/CNPJ Mutuário'].apply(normalizar_cpf_cnpj)

    return df


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
            if texto.replace("–", "-").startswith("1-1 de 1"):
                return True
        return False
    except NoSuchElementException:
        return False


def clicar_botao_editar(wdw) -> bool:
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
    try:
        driver.switch_to.default_content()
        wdw.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "iFramePage")))
        logger.info("Entrou no iframe do formulário.")
        return True
    except TimeoutException:
        logger.warning("Iframe 'iFramePage' não encontrado.")
        return False


def atualizar_tipo_cliente_se_necessario(driver, wdw) -> bool:
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

        select.select_by_value(CD_TIPO_CLIENTE_REGISTRADO)
        logger.info("Tipo cliente alterado para REGISTRADO.")
        return True

    except TimeoutException:
        logger.error("Select 'entity.cdTipoCliente' não encontrado no iframe.")
        return False


def clicar_salvar(driver, wdw) -> bool:
    try:
        botao_salvar = wdw.until(
            EC.element_to_be_clickable((By.NAME, "pbSalvar"))
        )
        botao_salvar.click()
        logger.info("Botão Salvar clicado com sucesso.")
        sleep(2)
        return True
    except TimeoutException:
        logger.error("Botão Salvar não encontrado ou não clicável.")
        return False


def obter_cpf_conjuge(driver, wdw) -> str | None:
    """
    Dentro do iframe do formulário do cliente, clica na aba de Cônjuge
    e retorna o CPF do cônjuge normalizado (com zeros à esquerda) caso esteja preenchido.

    :return: CPF do cônjuge como string normalizada, ou None se não houver
    """
    try:
        sleep(0.5)
        link_conjuge = wdw.until(
            EC.element_to_be_clickable(
                (By.XPATH, '//a[normalize-space(text())="Cônjuge"]')
            )
        )
        link_conjuge.click()
        logger.info("Aba Cônjuge clicada.")
        sleep(1)

        campo_cpf = wdw.until(
            EC.presence_of_element_located((By.NAME, "entity.conjuge.nuCPF"))
        )
        cpf_bruto = campo_cpf.get_attribute("value").strip()

        if cpf_bruto:
            cpf_conjuge = normalizar_cpf_cnpj(cpf_bruto)
            logger.info(f"Cônjuge encontrado com CPF: '{cpf_conjuge}' (bruto: '{cpf_bruto}')")
            return cpf_conjuge

        logger.info("Campo CPF do cônjuge está vazio.")
        return None

    except TimeoutException:
        logger.warning("Aba ou campo de cônjuge não encontrado.")
        return None


def processar_cliente(cnpj_cpf: str, tipo: str, driver, wdw, req) -> tuple[bool, str | None]:
    """
    Fluxo completo de busca, edição e salvamento de um cliente (titular ou cônjuge).

    :return: (sucesso, cpf_conjuge_encontrado)
    """
    logger.info(f"Processando [{tipo}]: '{cnpj_cpf}'")

    driver.switch_to.default_content()
    campo_cnpj_cpf = req.aguardar_e_clicar(wdw, (By.NAME, "cnpjCpf"))
    campo_cnpj_cpf.send_keys(Keys.CONTROL, "a")
    campo_cnpj_cpf.send_keys(Keys.DELETE)
    campo_cnpj_cpf.send_keys(cnpj_cpf)

    req.aguardar_e_clicar(
        wdw,
        (By.XPATH, '//button[@type="submit" and .//text()[contains(.,"Consultar")]]'),
        "Consultar",
    )

    req.aguardar_carregamento_tabela(driver)

    if not verificar_unico_resultado(driver):
        logger.warning(f"'{cnpj_cpf}' [{tipo}]: resultado não é exatamente 1 registro. Pulando...")
        return False, None

    if not clicar_botao_editar(wdw):
        return False, None

    sleep(1.5)

    if not entrar_iframe(driver, wdw):
        return False, None

    if not atualizar_tipo_cliente_se_necessario(driver, wdw):
        return False, None

    if not clicar_salvar(driver, wdw):
        return False, None

    cpf_conjuge = None
    if tipo == 'titular':
        if entrar_iframe(driver, wdw):
            cpf_conjuge = obter_cpf_conjuge(driver, wdw)

    return True, cpf_conjuge


# ── Inicialização ────────────────────────────────────────────────────────────

req = SeleniumRequester()
req.ensure_login()

driver = req.get_driver()
wdw = req.waiter(driver)

try:
    req.navegacao_inicial(driver, wdw)

    logger.info("Navegando para o cadastro de clientes...")
    driver.get(URL_CADASTRO_CLIENTE)
    sleep(2)

    df_clientes = processamento_dados()

    # Cônjuges descobertos nesta execução (serão concatenados ao final)
    linhas_conjuge: list[dict] = []

    for index, row in df_clientes.iterrows():

        nome = row['Nome Mutuário']
        cnpj_cpf = row['CPF/CNPJ Mutuário']  # já normalizado pelo processamento_dados()
        tipo = row.get('tipo', 'titular')

        # Pula apenas os que já foram concluídos com sucesso
        if pd.notna(row['situacao']) and row['situacao'] == 'concluido':
            logger.info(f"[{index}] '{nome}' já processado. Pulando...")
            continue

        try:
            sucesso, cpf_conjuge = processar_cliente(
                cnpj_cpf=cnpj_cpf,
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

            df_clientes.at[index, 'situacao'] = 'concluido'
            logger.info(f"[{index}] '{nome}' [{tipo}]: processado com sucesso.")

            if cpf_conjuge:
                ja_existe = (
                        any(lc['CPF/CNPJ Mutuário'] == cpf_conjuge for lc in linhas_conjuge)
                        or (df_clientes['CPF/CNPJ Mutuário'] == cpf_conjuge).any()
                )

                if ja_existe:
                    logger.info(f"Cônjuge CPF '{cpf_conjuge}' já existe na tabela. Pulando.")
                else:
                    logger.info(f"Cônjuge CPF '{cpf_conjuge}' encontrado. Processando...")

                    driver.switch_to.default_content()
                    driver.get(URL_CADASTRO_CLIENTE)
                    sleep(1.5)

                    sucesso_conjuge, _ = processar_cliente(
                        cnpj_cpf=cpf_conjuge,
                        tipo='conjuge',
                        driver=driver,
                        wdw=wdw,
                        req=req,
                    )

                    linhas_conjuge.append({
                        'CPF/CNPJ Mutuário': cpf_conjuge,
                        'tipo': 'conjuge',
                        'situacao': 'concluido' if sucesso_conjuge else 'erro_conjuge',
                    })

                    logger.info(
                        f"Cônjuge '{cpf_conjuge}': "
                        f"{'concluído' if sucesso_conjuge else 'erro'}."
                    )

            sleep(1.5)

        except Exception as e:
            logger.error(f"[{index}] '{nome}': erro inesperado — {e}", exc_info=True)
            df_clientes.at[index, 'situacao'] = f'erro: {str(e)[:100]}'

        finally:
            driver.switch_to.default_content()
            salvar_progresso(df_clientes)
            driver.get(URL_CADASTRO_CLIENTE)
            sleep(1.5)

    # Consolida cônjuges descobertos nesta execução
    if linhas_conjuge:
        df_conjuges = pd.DataFrame(linhas_conjuge)
        df_clientes = pd.concat([df_clientes, df_conjuges], ignore_index=True)
        logger.info(f"{len(linhas_conjuge)} cônjuge(s) adicionado(s) ao relatório final.")

    logger.info("Processamento finalizado.")
    salvar_progresso(df_clientes)

finally:
    req.quit(driver)
