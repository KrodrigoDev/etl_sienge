"""
main.py
--------
Orquestrador do pipeline ETL de Suprimentos.

Etapas de extração (ordem obrigatória):
  1. painel_compras
  2. estoque
  3. servico
  4. contrato
  5. adiantamento
  6. consulta_parcela
  7. titulo (Se atentar porque ela depende do fato_consulta_parcela)

Etapas de transformação (após extract):
  → transform_painel_compras  (roda primeiro — gera as dims base)
  → transform_estoque         (requer dims do painel_compras)
  → transform_servico
  → transform_contrato
  → transform_adiantamento
  → transform_consulta_parcela
  → transform_titulo

Uso:
    python main.py                                # extrai tudo + transforma tudo
    python main.py --etapa extract                # só extração (todos os módulos)
    python main.py --etapa transform              # só transformação (todos os módulos)
    python main.py --etapa extract_painel_compras # extração individual
    python main.py --etapa extract_estoque
    python main.py --etapa extract_servico
    python main.py --etapa extract_contrato
    python main.py --etapa extract_adiantamento
    python main.py --etapa transform_painel_compras
    python main.py --etapa transform_estoque
    python main.py --etapa transform_servico
    python main.py --etapa transform_contrato
    python main.py --etapa transform_adiantamento
    python main.py --data-inicio 01/01/2025       # data de início customizada
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── Caminhos ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent

INPUT_DIR = ROOT / "stages" / "transform" / "input"
REFERENCE_DIR = ROOT / "stages" / "transform" / "input" / "reference"
OUTPUT_DIR = ROOT / "stages" / "transform" / "output"

# Dims base geradas pelo transform_painel_compras, exigidas pelos demais
_DIMS_BASE = ["dim_obra.csv", "dim_insumo.csv", "dim_grupo_insumo.csv"]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _checar_dims_base() -> None:
    """Garante que as dims base existam antes de rodar transforms dependentes."""
    ausentes = [d for d in _DIMS_BASE if not (OUTPUT_DIR / d).exists()]
    if ausentes:
        raise FileNotFoundError(
            f"Dims base ausentes em {OUTPUT_DIR}: {ausentes}. "
            "Execute transform_painel_compras antes de continuar."
        )


def _secao(nome: str) -> None:
    logger.info("═══ %s %s", nome, "═" * max(0, 50 - len(nome)))


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────────────────────

def etapa_extract_painel_compras(data_inicio: str) -> None:
    from stages.extract.extract_painel_compras import extrair_painel_compras

    _secao("EXTRACT — painel_compras")
    caminho = extrair_painel_compras(
        data_inicio=data_inicio,
        destino=INPUT_DIR / "painel_compras",
    )
    logger.info("Painel de compras salvo em: %s", caminho)


def etapa_extract_estoque() -> None:
    from stages.extract.extract_estoque import extrair_estoque

    _secao("EXTRACT — estoque")
    caminho = extrair_estoque(destino=INPUT_DIR / "estoque")
    logger.info("Estoque salvo em: %s", caminho)


def etapa_extract_servico(data_inicio: str) -> None:
    from stages.extract.extract_servico import extrair_servicos

    _secao("EXTRACT — servico")
    caminho = extrair_servicos(
        data_inicio=data_inicio,
        destino=INPUT_DIR / "servico",
    )
    logger.info("Serviços salvo em: %s", caminho)


def etapa_extract_contrato(data_inicio: str) -> None:
    from stages.extract.extract_contrato import extrair_contratos

    _secao("EXTRACT — contrato")
    caminho = extrair_contratos(
        data_inicio=data_inicio,
        destino=INPUT_DIR / "contrato",
    )
    logger.info("Contratos salvo em: %s", caminho)


def etapa_extract_adiantamento(data_inicio: str) -> None:
    from stages.extract.extract_adiantamento import extrair_adiantamento

    _secao("EXTRACT — adiantamento")
    extrair_adiantamento(
        data_inicio=data_inicio,
        destino=INPUT_DIR / "adiantamento",
    )
    logger.info("Adiantamento extraído.")

def etapa_extract_consulta_parcela(data_inicio: str) -> None:
    from stages.extract.extract_consulta_parcela import extrair_consulta_parcela

    _secao("EXTRACT — Consulta Parcela")
    extrair_consulta_parcela(
        data_inicio=data_inicio,
        destino=INPUT_DIR / "consulta_parcela",
    )
    logger.info("Consulta Parcela extraído.")


def etapa_extract_titulo(data_inicio: str) -> None:
    from stages.extract.extract_titulo import extrair_titulo

    _secao("EXTRACT — Título")
    extrair_titulo(
        data_inicio=data_inicio,
        destino=INPUT_DIR / "titulo",
    )
    logger.info("Consulta Título extraído.")


def etapa_extract(data_inicio: str) -> None:
    """Roda todos os extractors na ordem definida."""
    etapa_extract_painel_compras(data_inicio)

    # pegando série histórica completa
    etapa_extract_estoque()
    etapa_extract_servico("01/01/2014")
    etapa_extract_contrato("01/01/2014")
    etapa_extract_adiantamento("01/01/2014")
    etapa_extract_consulta_parcela("01/01/2026")
    etapa_extract_titulo('01/01/2024')


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────

def etapa_transform_painel_compras() -> None:
    from stages.transform.transform_painel_compras import executar

    _secao("TRANSFORM — painel_compras")
    executar(
        input_dir=INPUT_DIR,
        reference_dir=REFERENCE_DIR,
        output_dir=OUTPUT_DIR,
    )
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
    """
    Roda todos os transforms na ordem correta.
    painel_compras sempre primeiro — gera as dims base que os demais dependem.
    """
    etapa_transform_painel_compras()
    etapa_transform_estoque()
    etapa_transform_servico()
    etapa_transform_contrato()
    etapa_transform_adiantamento()
    etapa_transform_consulta_parcela()
    etapa_transform_titulo()


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
        etapa_extract_titulo(args.data_inicio)

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

    # ── Blocos completos ──────────────────────────────────────────────────────
    elif args.etapa == "extract":
        etapa_extract(args.data_inicio)

    elif args.etapa == "transform":
        etapa_transform()

    elif args.etapa == "all":
        etapa_extract(args.data_inicio)
        etapa_transform()

    logger.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()
