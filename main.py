"""
main.py
--------
Orquestrador do pipeline ETL de Suprimentos.

Etapas:
  1. EXTRACT — coleta CSVs do SIENGE via Selenium
  2. TRANSFORM — limpa, modela e gera as tabelas do DW
  3. LOAD — (próxima etapa) carga no destino final

Uso:
    python main.py                        # extrai tudo + transforma
    python main.py --etapa extract        # só extração
    python main.py --etapa transform      # só transformação (usa CSVs existentes)
    python main.py --data-inicio 01/01/2025  # data de início customizada
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
OUTPUT_DIR = ROOT / "stages" / "transform" / "output"


def etapa_extract(data_inicio: str) -> None:
    """Executa extração de todas as fontes do SIENGE."""
    from stages.extract.extract_painel_compras import extrair_painel_compras
    from stages.extract.extract_estoque import extrair_estoque

    logger.info("═══ EXTRACT ════════════════════════════════════════════")

    logger.info("Extraindo painel de compras (a partir de %s)...", data_inicio)
    csv_compras = extrair_painel_compras(
        data_inicio=data_inicio,
        destino=INPUT_DIR / "painel_compras",
    )
    logger.info("Painel de compras: %s", csv_compras)

    logger.info("Extraindo estoque de obras...")
    csv_estoque = extrair_estoque(
        destino=INPUT_DIR / "estoque",
    )
    logger.info("Estoque: %s", csv_estoque)

    logger.info("Extract concluído.")


def etapa_transform() -> None:
    """Executa transformação e gera tabelas do DW."""
    # O ETL existente (etl_suprimentos.py) será chamado aqui
    # após ser refatorado para aceitar INPUT_DIR e OUTPUT_DIR como parâmetros.
    logger.info("═══ TRANSFORM ══════════════════════════════════════════")
    logger.info("(a implementar — próxima etapa)")


def etapa_load() -> None:
    """Carrega tabelas do DW no destino final (Power BI / banco)."""
    logger.info("═══ LOAD ═══════════════════════════════════════════════")
    logger.info("(a implementar — etapa futura)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline ETL Suprimentos")
    parser.add_argument(
        "--etapa",
        choices=["extract", "transform", "load", "all"],
        default="all",
        help="Etapa a executar (padrão: all)",
    )
    parser.add_argument(
        "--data-inicio",
        default=f"01/01/{date.today().year}",
        help="Data início para extração do painel (DD/MM/AAAA)",
    )
    args = parser.parse_args()

    logger.info("Pipeline iniciado | etapa=%s | data_inicio=%s",
                args.etapa, args.data_inicio)

    if args.etapa in ("extract", "all"):
        etapa_extract(args.data_inicio)

    if args.etapa in ("transform", "all"):
        etapa_transform()

    if args.etapa in ("load", "all"):
        etapa_load()

    logger.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()
