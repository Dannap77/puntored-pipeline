"""Capa Bronze: ingesta de CSV crudos a parquet.

No transforma datos. Solo agrega metadata de trazabilidad
(_ingested_at, _pipeline_run_id) para auditoría.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from config.settings import DATA_BRONZE, DATA_RAW
from src.utils.logger import get_logger

logger = get_logger(__name__)


def ingest_table(table_name: str, run_id: str) -> Path:
    src = DATA_RAW / f"{table_name}.csv"
    dst = DATA_BRONZE / f"{table_name}.parquet"

    if not src.exists():
        raise FileNotFoundError(f"No existe el archivo crudo: {src}")

    df = pd.read_csv(src)
    df["_ingested_at"] = datetime.utcnow()
    df["_pipeline_run_id"] = run_id
    df["_source_file"] = src.name

    DATA_BRONZE.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dst, index=False, engine="pyarrow")

    logger.info(f"[Bronze] {table_name}: {len(df)} filas → {dst.name}")
    return dst


def run(run_id: str | None = None) -> dict[str, Path]:
    run_id = run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    logger.info(f"Iniciando ingesta Bronze (run_id={run_id})")

    tables = ["users", "transactions", "transaction_details"]
    return {t: ingest_table(t, run_id) for t in tables}


if __name__ == "__main__":
    run()
