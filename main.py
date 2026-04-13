"""
main.py
--------
Orquestrador do pipeline ETL de Suprimentos.

Etapas:
  1. EXTRACT  — coleta CSVs do SIENGE via Selenium
  2. TRANSFORM — limpa, modela e gera as tabelas do DW
               → transform_painel_compras  (roda primeiro — gera as dims base)
               → transform_estoque         (roda segundo  — expande as dims)
  3. LOAD     — (próxima etapa) carga no destino final

Uso:
    python main.py                           # extrai tudo + transforma
    python main.py --etapa extract           # só extração
    python main.py --etapa transform         # só transformação (usa CSVs existentes)
    python main.py --etapa transform_compras # só painel de compras
    python main.py --etapa transform_estoque # só estoque (requer dims já geradas)
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
ROOT          = Path(__file__).resolve().parent
INPUT_DIR     = ROOT / "stages" / "transform" / "input"
REFERENCE_DIR = ROOT / "stages" / "transform" / "input" / "reference"
OUTPUT_DIR    = ROOT / "stages" / "transform" / "output"


# ─────────────────────────────────────────────────────────────────────────────
# ETAPAS
# ─────────────────────────────────────────────────────────────────────────────

def etapa_extract(data_inicio: str) -> None:
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
    csv_estoque = extrair_estoque(destino=INPUT_DIR / "estoque")
    logger.info("Estoque: %s", csv_estoque)

    logger.info("Extract concluído.")


def etapa_transform_compras() -> None:
    """
    Transforma o painel de compras.
    SEMPRE deve rodar antes do transform de estoque.
    Gera: dim_obra, dim_insumo, dim_grupo_insumo, dim_lead_times,
          dim_fornecedor, dim_comprador, dim_solicitante, fato_solicitacao_item.
    """
    from stages.transform.transform_painel_compras import executar

    logger.info("═══ TRANSFORM — painel de compras ══════════════════════")
    executar(
        input_dir=INPUT_DIR,
        reference_dir=REFERENCE_DIR,
        output_dir=OUTPUT_DIR,
    )
    logger.info("Transform painel de compras concluído.")


def etapa_transform_estoque() -> None:
    """
    Transforma o estoque de obras.
    Requer que dim_obra, dim_insumo e dim_grupo_insumo já existam no OUTPUT_DIR
    (geradas pelo transform do painel de compras).
    Expande as dimensões existentes e gera fato_estoque.
    """
    from stages.transform.transform_estoque import executar

    # Validação de pré-condição: dims base devem existir
    for dim in ['dim_obra.csv', 'dim_insumo.csv', 'dim_grupo_insumo.csv']:
        if not (OUTPUT_DIR / dim).exists():
            raise FileNotFoundError(
                f"Arquivo '{dim}' não encontrado em {OUTPUT_DIR}. "
                f"Execute transform_compras antes de transform_estoque."
            )

    logger.info("═══ TRANSFORM — estoque ════════════════════════════════")
    executar(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR)
    logger.info("Transform estoque concluído.")


def etapa_transform() -> None:
    """Roda os dois transforms na ordem correta."""
    etapa_transform_compras()
    etapa_transform_estoque()


def etapa_load() -> None:
    logger.info("═══ LOAD ═══════════════════════════════════════════════")
    logger.info("(a implementar — etapa futura)")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline ETL Suprimentos")
    parser.add_argument(
        "--etapa",
        choices=["extract", "transform", "transform_compras",
                 "transform_estoque", "load", "all"],
        default="all",
        help="Etapa a executar (padrão: all)",
    )
    parser.add_argument(
        "--data-inicio",
        default=f"01/01/{date.today().year}",
        help="Data início para extração do painel de compras (DD/MM/AAAA)",
    )
    args = parser.parse_args()

    logger.info(
        "Pipeline iniciado | etapa=%s | data_inicio=%s",
        args.etapa, args.data_inicio,
    )

    if args.etapa in ("extract", "all"):
        etapa_extract(args.data_inicio)

    if args.etapa in ("transform", "all"):
        etapa_transform()

    if args.etapa == "transform_compras":
        etapa_transform_compras()

    if args.etapa == "transform_estoque":
        etapa_transform_estoque()

    if args.etapa in ("load", "all"):
        etapa_load()

    logger.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()