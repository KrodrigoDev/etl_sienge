"""
main.py
--------
Orquestrador do pipeline.
...
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path
from typing import Tuple

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# ── Caminhos ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent

INPUT_DIR = ROOT / "stages" / "transform" / "input"
REFERENCE_DIR = ROOT / "stages" / "transform" / "input" / "reference"
OUTPUT_DIR = ROOT / "stages" / "transform" / "output"

_DIMS_BASE = ["dim_obra.csv", "dim_insumo.csv", "dim_grupo_insumo.csv"]

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT / "logs" / "pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _checar_dims_base() -> None:
    ausentes = [d for d in _DIMS_BASE if not (OUTPUT_DIR / d).exists()]
    if ausentes:
        raise FileNotFoundError(
            f"Dims base ausentes em {OUTPUT_DIR}: {ausentes}. "
            "Execute transform_painel_compras antes de continuar."
        )


def _secao(nome: str) -> None:
    logger.info("═══ %s %s", nome, "═" * max(0, 50 - len(nome)))


def _com_retry(
        nome: str,
        fn,
        tentativas: int = 2,
        espera: int = 30,
) -> bool:
    """
    Executa fn() até `tentativas` vezes em caso de falha.

    Retorna True se concluiu com sucesso, False se todas as tentativas falharam.
    """
    for tentativa in range(1, tentativas + 1):
        try:
            logger.info(
                "Iniciando '%s' — tentativa %d/%d", nome, tentativa, tentativas
            )
            fn()
            logger.info("'%s' concluído com sucesso.", nome)
            return True
        except Exception as exc:
            logger.warning(
                "'%s' falhou na tentativa %d/%d — %s: %s",
                nome, tentativa, tentativas, type(exc).__name__, exc,
            )
            if tentativa < tentativas:
                logger.info(
                    "Aguardando %ds antes de nova tentativa de '%s'...",
                    espera, nome,
                )
                time.sleep(espera)

    logger.error(
        "'%s' falhou em todas as %d tentativas — continuando pipeline.",
        nome, tentativas,
    )
    return False


def _ja_executado_hoje(pasta: Path, extensao: str = "*.xlsx") -> bool:
    """
    Retorna True se existe algum arquivo na pasta com data de modificação = hoje.
    Usado para evitar re-execução de extracts que rodam apenas uma vez por dia.
    """
    if not pasta.exists():
        return False
    return any(
        date.fromtimestamp(f.stat().st_mtime) == date.today()
        for f in pasta.glob(extensao)
    )


def _cancelar_transforms(etapas_falhas: list[str]) -> None:
    """Loga de forma clara quais transforms foram cancelados e por quê."""
    for etapa in etapas_falhas:
        logger.error(
            "'%s' falhou no extract — transform correspondente NÃO será executado.",
            etapa,
        )
    logger.error(
        "Pipeline encerrado com falhas nos extracts acima — "
        "transforms cancelados para preservar consistência dos dados."
    )


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────────────────────

def etapa_extract_painel_compras(data_inicio: str) -> bool:
    from stages.extract.extract_painel_compras import extrair_painel_compras

    _secao("EXTRACT — painel_compras")
    return _com_retry(
        nome="extract_painel_compras",
        fn=lambda: extrair_painel_compras(
            data_inicio=data_inicio,
            destino=INPUT_DIR / "painel_compras",
        ),
    )


def etapa_extract_estoque() -> bool:
    from stages.extract.extract_estoque import extrair_estoque

    _secao("EXTRACT — estoque")
    return _com_retry(
        nome="extract_estoque",
        fn=lambda: extrair_estoque(destino=INPUT_DIR / "estoque"),
    )


def etapa_extract_servico(data_inicio: str) -> bool:
    from stages.extract.extract_servico import extrair_servicos

    _secao("EXTRACT — servico")
    return _com_retry(
        nome="extract_servico",
        fn=lambda: extrair_servicos(
            data_inicio=data_inicio,
            destino=INPUT_DIR / "servico",
        ),
    )


def etapa_extract_contrato(data_inicio: str) -> bool:
    from stages.extract.extract_contrato import extrair_contratos

    _secao("EXTRACT — contrato")
    return _com_retry(
        nome="extract_contrato",
        fn=lambda: extrair_contratos(
            data_inicio=data_inicio,
            destino=INPUT_DIR / "contrato",
        ),
    )


def etapa_extract_adiantamento(data_inicio: str) -> bool:
    from stages.extract.extract_adiantamento import extrair_adiantamento

    _secao("EXTRACT — adiantamento")
    return _com_retry(
        nome="extract_adiantamento",
        fn=lambda: extrair_adiantamento(
            data_inicio=data_inicio,
            destino=INPUT_DIR / "adiantamento",
        ),
    )


def etapa_extract_consulta_parcela(data_inicio: str) -> bool:
    from stages.extract.extract_consulta_parcela import extrair_consulta_parcela

    _secao("EXTRACT — Consulta Parcela")
    return _com_retry(
        nome="extract_consulta_parcela",
        fn=lambda: extrair_consulta_parcela(
            data_inicio=data_inicio,
            destino=INPUT_DIR / "consulta_parcela",
        ),
    )


def etapa_extract_titulo() -> tuple[bool, bool]:
    """
    Executa o extract de títulos com skip automático se já foi rodado hoje.
    Retorna True em ambos os casos (sucesso ou skip), False apenas se falhou.
    O caller pode distinguir skip vs execução real pelo segundo valor retornado.
    """
    _secao("EXTRACT — Título")

    titulo_dir = INPUT_DIR / "titulo"
    if _ja_executado_hoje(titulo_dir):
        logger.info(
            "extract_titulo — arquivos de hoje já existem em '%s', pulando.",
            titulo_dir,
        )
        return True, True  # (sucesso, foi_skip)

    from stages.extract.extract_titulo import extrair_titulo

    ok = _com_retry(
        nome="extract_titulo",
        fn=lambda: extrair_titulo(destino=titulo_dir),
    )
    return ok, False  # (sucesso, foi_skip)


def etapa_extract(data_inicio: str) -> None:
    """Roda todos os extractors na ordem definida."""
    etapa_extract_painel_compras(data_inicio)
    etapa_extract_estoque()
    etapa_extract_servico("01/01/2014")
    etapa_extract_contrato("01/01/2014")
    etapa_extract_adiantamento("01/01/2014")
    etapa_extract_consulta_parcela("01/01/2026")
    etapa_extract_titulo()


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────

def etapa_transform_painel_compras() -> None:
    from stages.transform.transform_painel_compras import executar

    _secao("TRANSFORM — painel_compras")
    executar(input_dir=INPUT_DIR, reference_dir=REFERENCE_DIR, output_dir=OUTPUT_DIR)
    logger.info("Transform painel_compras concluído.")


def etapa_transform_estoque() -> None:
    from stages.transform.transform_estoque import executar

    _secao("TRANSFORM — estoque")
    _checar_dims_base()
    executar(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR)
    logger.info("Transform estoque concluído.")


def etapa_transform_servico() -> None:
    from stages.transform.transform_servico import executar

    _secao("TRANSFORM — servico")
    _checar_dims_base()
    executar(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR)
    logger.info("Transform servico concluído.")


def etapa_transform_contrato() -> None:
    from stages.transform.transform_contratos import executar

    _secao("TRANSFORM — contrato")
    _checar_dims_base()
    executar(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR)
    logger.info("Transform contrato concluído.")


def etapa_transform_adiantamento() -> None:
    from stages.transform.transform_adiantamento import executar

    _secao("TRANSFORM — adiantamento")
    _checar_dims_base()
    executar(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR)
    logger.info("Transform adiantamento concluído.")


def etapa_transform_consulta_parcela() -> None:
    from stages.transform.transform_consulta_parcela import executar

    _secao("TRANSFORM — Consulta Parcela")
    executar(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR)
    logger.info("Transform consulta parcela concluído.")


def etapa_transform_titulo() -> None:
    from stages.transform.transform_titulo import executar

    _secao("TRANSFORM — Título")
    executar(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR)
    logger.info("Transform Título concluído.")


def etapa_transform() -> None:
    etapa_transform_painel_compras()
    etapa_transform_estoque()
    etapa_transform_servico()
    etapa_transform_contrato()
    etapa_transform_adiantamento()
    etapa_transform_consulta_parcela()
    etapa_transform_titulo()


# ─────────────────────────────────────────────────────────────────────────────
# EXECUÇÕES DESTINADAS AOS PAINEIS
# ─────────────────────────────────────────────────────────────────────────────

def painel_consultas() -> None:
    ok_parcela = etapa_extract_consulta_parcela("01/01/2026")
    ok_titulo, titulo_skip = etapa_extract_titulo()

    falhas = []
    if not ok_parcela:
        falhas.append("extract_consulta_parcela")
    if not ok_titulo:
        falhas.append("extract_titulo")

    if falhas:
        _cancelar_transforms(falhas)
        return

    etapa_transform_consulta_parcela()

    if titulo_skip:
        logger.info(
            "transform_titulo ignorado — extract_titulo não rodou hoje "
            "(output em disco já está atualizado)."
        )
    else:
        etapa_transform_titulo()


def painel_suprimentos() -> None:
    """
    Extrai todos os módulos de suprimentos.
    Só roda os transforms se todos os extracts concluírem com sucesso.
    """
    ok_painel = etapa_extract_painel_compras("01/01/2026")
    ok_estoque = etapa_extract_estoque()
    ok_servico = etapa_extract_servico("01/01/2014")
    ok_contrato = etapa_extract_contrato("01/01/2014")
    ok_adiantamento = etapa_extract_adiantamento("01/01/2014")

    falhas = []
    if not ok_painel:
        falhas.append("extract_painel_compras")
    if not ok_estoque:
        falhas.append("extract_estoque")
    if not ok_servico:
        falhas.append("extract_servico")
    if not ok_contrato:
        falhas.append("extract_contrato")
    if not ok_adiantamento:
        falhas.append("extract_adiantamento")

    if falhas:
        _cancelar_transforms(falhas)
        return

    etapa_transform_painel_compras()
    etapa_transform_estoque()
    etapa_transform_servico()
    etapa_transform_contrato()
    etapa_transform_adiantamento()


# ─────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────

_ETAPAS_VALIDAS = [
    "all",
    "extract",
    "extract_painel_compras",
    "extract_estoque",
    "extract_servico",
    "extract_contrato",
    "extract_adiantamento",
    "extract_consulta_parcela",
    "extract_titulo",
    "transform",
    "transform_painel_compras",
    "transform_estoque",
    "transform_servico",
    "transform_contrato",
    "transform_adiantamento",
    "transform_consulta_parcela",
    "transform_titulo",
    "painel_consultas",
    "painel_suprimentos",
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL Suprimentos",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--etapa",
        choices=_ETAPAS_VALIDAS,
        default="all",
        help=(
            "Etapa a executar (padrão: all)\n"
            "  all                      → extract + transform completos\n"
            "  extract                  → todos os extractors em ordem\n"
            "  transform                → todos os transforms em ordem\n"
            "  extract_<modulo>         → extração individual\n"
            "  transform_<modulo>       → transformação individual\n"
            "  painel_consultas         → consulta_parcela + titulo (extract→transform)\n"
            "  painel_suprimentos       → suprimentos completo (extract→transform)\n"
        ),
    )
    parser.add_argument(
        "--data-inicio",
        default=f"01/01/{date.today().year}",
        metavar="DD/MM/AAAA",
        help="Data de início para os relatórios com filtro temporal (padrão: 01/01/ano_atual)",
    )
    args = parser.parse_args()

    logger.info(
        "Pipeline iniciado | etapa=%s | data_inicio=%s",
        args.etapa, args.data_inicio,
    )

    # ── Extract individuais ───────────────────────────────────────────────────
    if args.etapa == "extract_painel_compras":
        etapa_extract_painel_compras(args.data_inicio)

    elif args.etapa == "extract_estoque":
        etapa_extract_estoque()

    elif args.etapa == "extract_servico":
        etapa_extract_servico(args.data_inicio)

    elif args.etapa == "extract_contrato":
        etapa_extract_contrato(args.data_inicio)

    elif args.etapa == "extract_adiantamento":
        etapa_extract_adiantamento(args.data_inicio)

    elif args.etapa == "extract_consulta_parcela":
        etapa_extract_consulta_parcela(args.data_inicio)

    elif args.etapa == "extract_titulo":
        etapa_extract_titulo()

    # ── Transform individuais ─────────────────────────────────────────────────
    elif args.etapa == "transform_painel_compras":
        etapa_transform_painel_compras()

    elif args.etapa == "transform_estoque":
        etapa_transform_estoque()

    elif args.etapa == "transform_servico":
        etapa_transform_servico()

    elif args.etapa == "transform_contrato":
        etapa_transform_contrato()

    elif args.etapa == "transform_adiantamento":
        etapa_transform_adiantamento()

    elif args.etapa == "transform_consulta_parcela":
        etapa_transform_consulta_parcela()

    elif args.etapa == "transform_titulo":
        etapa_transform_titulo()

    # ── Blocos completos ──────────────────────────────────────────────────────
    elif args.etapa == "extract":
        etapa_extract(args.data_inicio)

    elif args.etapa == "transform":
        etapa_transform()

    elif args.etapa == "all":
        etapa_extract(args.data_inicio)
        etapa_transform()

    # ── Paineis ───────────────────────────────────────────────────────────────
    elif args.etapa == "painel_consultas":
        painel_consultas()

    elif args.etapa == "painel_suprimentos":
        painel_suprimentos()

    logger.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()
