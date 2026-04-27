"""Orquestador del pipeline de datos.

Ejecuta en orden:
    1. Generación de datos sintéticos (Faker)  → data/raw/
    2. Bronze: ingesta cruda                   → data/bronze/
    3. Silver: limpieza y reglas de negocio    → data/silver/
    4. Gold: KPIs analíticos (DuckDB + SQL)    → data/gold/
    5. Validaciones de calidad                 → logs/

Uso:
    python main.py
    python main.py --skip-generate    # reusa los CSV existentes
"""

import argparse
import sys
import time
from datetime import datetime

from src.bronze import load_bronze
from src.gold import build_kpis
from src.ingestion import generate_data
from src.quality import validations
from src.silver import transform
from src.utils.logger import get_logger

logger = get_logger("pipeline")


def _step(name: str, fn) -> None:
    logger.info(f"========== {name} ==========")
    t0 = time.time()
    try:
        fn()
    except Exception:
        logger.exception(f"Falló el paso: {name}")
        raise
    logger.info(f"{name} completado en {time.time() - t0:.2f}s")


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline de datos PuntoRed")
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="No regenerar datos sintéticos (reusa los CSV existentes)",
    )
    args = parser.parse_args()

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    logger.info(f"Pipeline run_id={run_id}")
    pipeline_t0 = time.time()

    try:
        if not args.skip_generate:
            _step("1. Generacion de datos", generate_data.run)
        else:
            logger.info("Saltando generacion (--skip-generate)")

        _step("2. Bronze", lambda: load_bronze.run(run_id=run_id))
        _step("3. Silver", transform.run)
        _step("4. Gold", build_kpis.run)
        _step("5. Quality checks", validations.run)

    except Exception:
        logger.exception("Pipeline ABORTADO")
        return 1

    elapsed = time.time() - pipeline_t0
    logger.info(f"Pipeline OK en {elapsed:.2f}s (run_id={run_id})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
