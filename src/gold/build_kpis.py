"""Capa Gold: ejecuta los .sql en orden contra DuckDB y exporta cada tabla a parquet.

DuckDB lee directamente los parquet de Silver (sin necesidad de cargar nada).
Cada tabla resultante se materializa en data/gold/<tabla>.parquet y también
queda persistida en data/gold/warehouse.duckdb para consulta SQL ad-hoc.
"""

import os
from pathlib import Path

import duckdb

from config.settings import DATA_GOLD, DUCKDB_PATH, ROOT
from src.utils.logger import get_logger

logger = get_logger(__name__)

SQL_DIR = ROOT / "sql"

# Orden importa: enriched es la base, después los agregados.
SQL_FILES = [
    "gold_transactions_enriched.sql",
    "gold_kpi_overall.sql",
    "gold_kpi_by_user.sql",
    "gold_kpi_by_payment_method.sql",
    "gold_kpi_by_channel.sql",
    "gold_kpi_by_method_channel.sql",
    "gold_kpi_by_day.sql",
]


def run() -> dict[str, Path]:
    DATA_GOLD.mkdir(parents=True, exist_ok=True)
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # read_parquet() resuelve paths relativos contra el cwd del proceso,
    # así que nos paramos en la raíz del proyecto.
    os.chdir(ROOT)

    logger.info(f"Conectando a DuckDB en {DUCKDB_PATH}")
    con = duckdb.connect(str(DUCKDB_PATH))

    exported: dict[str, Path] = {}
    for sql_file in SQL_FILES:
        sql_path = SQL_DIR / sql_file
        sql = sql_path.read_text(encoding="utf-8")
        logger.info(f"[Gold] Ejecutando {sql_file}")
        con.execute(sql)

        # Exportar a parquet (para que el dashboard lo lea sin abrir DuckDB).
        table_name = sql_file.replace(".sql", "")
        out_path = DATA_GOLD / f"{table_name}.parquet"
        con.execute(
            f"COPY {table_name} TO '{out_path.as_posix()}' (FORMAT PARQUET)"
        )
        rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"[Gold] {table_name}: {rows} filas → {out_path.name}")
        exported[table_name] = out_path

    con.close()
    logger.info(f"Capa Gold construida en {DATA_GOLD}/")
    return exported


if __name__ == "__main__":
    run()
