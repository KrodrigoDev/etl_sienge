"""
stages/extract/extract_usuarios.py
-----------------------------------------
Extrai o relatório de Usuários do SIENGE e salva como CSV.

Fluxo:
  1. Login via sessão salva no perfil Edge
  2. Navega para a URL de credor
  3. Clica em "Visualizar"
  4. Aguarda o donwload iniciar
  5. renomeia e move o arquivo para pasta "input/credor"
"""

from __future__ import annotations

import logging


from src.drivers.selenium_requester import BASE_URL, SeleniumRequester

logger = logging.getLogger(__name__)


URL_CREDORES = (
    f"{BASE_URL}/8/index.html"
    "#/common/page/1162"
)



