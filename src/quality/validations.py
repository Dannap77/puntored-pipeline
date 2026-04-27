"""Validaciones de calidad de datos por capa (Nivel 3 de la prueba).

Cada check devuelve True/False y se loguea. Si algún check crítico falla,
levantamos DataQualityError y el pipeline aborta.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from config.settings import DATA_BRONZE, DATA_GOLD, DATA_SILVER, DUCKDB_PATH
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataQualityError(Exception):
    """Falló al menos una validación crítica de calidad."""


def _count(parquet_path: Path) -> int:
    return duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{parquet_path.as_posix()}')").fetchone()[0]


def _check(condition: bool, name: str, critical: bool = True) -> bool:
    if condition:
        logger.info(f"[QC OK]   {name}")
        return True
    msg = f"[QC FAIL] {name}"
    if critical:
        logger.error(msg)
    else:
        logger.warning(msg)
    return False


def validate_bronze() -> list[bool]:
    logger.info("Validando capa Bronze...")
    results = []
    for table in ("users", "transactions", "transaction_details"):
        path = DATA_BRONZE / f"{table}.parquet"
        results.append(_check(path.exists(), f"bronze.{table} existe"))
        if path.exists():
            results.append(_check(_count(path) > 0, f"bronze.{table} no esta vacia"))
    return results


def validate_silver() -> list[bool]:
    logger.info("Validando capa Silver...")
    results = []
    bronze_users = _count(DATA_BRONZE / "users.parquet")
    silver_users = _count(DATA_SILVER / "users.parquet")
    results.append(_check(silver_users <= bronze_users, "silver.users <= bronze.users"))

    # Reglas de negocio
    invalid_amount = duckdb.sql(
        f"SELECT COUNT(*) FROM read_parquet('{(DATA_SILVER / 'transactions.parquet').as_posix()}') WHERE amount <= 0"
    ).fetchone()[0]
    results.append(_check(invalid_amount == 0, "silver.transactions.amount > 0"))

    invalid_status = duckdb.sql(
        f"SELECT COUNT(*) FROM read_parquet('{(DATA_SILVER / 'transactions.parquet').as_posix()}') "
        "WHERE status NOT IN ('success', 'failed')"
    ).fetchone()[0]
    results.append(_check(invalid_status == 0, "silver.transactions.status in {success,failed}"))

    # Integridad referencial
    orphan_txns = duckdb.sql(f"""
        SELECT COUNT(*) FROM read_parquet('{(DATA_SILVER / 'transactions.parquet').as_posix()}') t
        LEFT JOIN read_parquet('{(DATA_SILVER / 'users.parquet').as_posix()}') u
            ON t.user_id = u.user_id
        WHERE u.user_id IS NULL
    """).fetchone()[0]
    results.append(_check(orphan_txns == 0, "silver.transactions sin huerfanas"))

    orphan_details = duckdb.sql(f"""
        SELECT COUNT(*) FROM read_parquet('{(DATA_SILVER / 'transaction_details.parquet').as_posix()}') d
        LEFT JOIN read_parquet('{(DATA_SILVER / 'transactions.parquet').as_posix()}') t
            ON d.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
    """).fetchone()[0]
    results.append(_check(orphan_details == 0, "silver.transaction_details sin huerfanas"))

    # PKs unicas
    dup_users = duckdb.sql(
        f"SELECT COUNT(*) - COUNT(DISTINCT user_id) FROM read_parquet('{(DATA_SILVER / 'users.parquet').as_posix()}')"
    ).fetchone()[0]
    results.append(_check(dup_users == 0, "silver.users sin duplicados de PK"))

    dup_txns = duckdb.sql(
        f"SELECT COUNT(*) - COUNT(DISTINCT transaction_id) FROM read_parquet('{(DATA_SILVER / 'transactions.parquet').as_posix()}')"
    ).fetchone()[0]
    results.append(_check(dup_txns == 0, "silver.transactions sin duplicados de PK"))

    return results


def validate_gold() -> list[bool]:
    logger.info("Validando capa Gold...")
    results = []
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    enriched_count = con.execute("SELECT COUNT(*) FROM gold_transactions_enriched").fetchone()[0]
    results.append(_check(enriched_count > 0, "gold_transactions_enriched no vacia"))

    # Tasa de éxito en rango razonable
    success_rate = con.execute("SELECT success_rate FROM gold_kpi_overall").fetchone()[0]
    results.append(_check(0 < success_rate < 1, f"success_rate en (0,1) -> {success_rate:.3f}"))

    # Freshness: ¿hay datos en los últimos 2 años? (los datos sintéticos cubren 2024-2025)
    last_at = con.execute("SELECT last_transaction_at FROM gold_kpi_overall").fetchone()[0]
    threshold = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=730)
    results.append(_check(
        last_at >= threshold,
        f"freshness: ultima transaccion >= {threshold.date()} (real {last_at})",
        critical=False,
    ))

    # Sin nulls en métricas clave
    null_methods = con.execute(
        "SELECT COUNT(*) FROM gold_kpi_by_payment_method WHERE payment_method IS NULL"
    ).fetchone()[0]
    results.append(_check(null_methods == 0, "gold_kpi_by_payment_method sin nulls en payment_method"))

    con.close()
    return results


def run() -> None:
    all_results: list[bool] = []
    all_results += validate_bronze()
    all_results += validate_silver()
    all_results += validate_gold()

    failed = sum(1 for r in all_results if not r)
    total = len(all_results)
    logger.info(f"Quality checks: {total - failed}/{total} OK")

    if failed:
        raise DataQualityError(f"{failed} validaciones fallaron — revisa los logs")


if __name__ == "__main__":
    run()
