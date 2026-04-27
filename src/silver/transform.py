"""Capa Silver: limpieza, validación y aplicación de reglas de negocio.

Reglas aplicadas (definidas en el PDF de la prueba):
- amount > 0
- No duplicados por PK
- status estandarizado a lowercase y dentro de {success, failed}
- Integridad referencial:
    * transactions.user_id debe existir en users
    * transaction_details.transaction_id debe existir en transactions
- Manejo de nulls en campos no obligatorios

Cada DataFrame que sale de Silver lleva las columnas _ingested_at,
_pipeline_run_id heredadas de Bronze y un nuevo _silver_processed_at.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from config.settings import DATA_BRONZE, DATA_SILVER
from src.utils.logger import get_logger

logger = get_logger(__name__)

VALID_STATUSES = {"success", "failed"}


def _read_bronze(table: str) -> pd.DataFrame:
    return pd.read_parquet(DATA_BRONZE / f"{table}.parquet")


def _write_silver(df: pd.DataFrame, table: str) -> Path:
    DATA_SILVER.mkdir(parents=True, exist_ok=True)
    dst = DATA_SILVER / f"{table}.parquet"
    df["_silver_processed_at"] = datetime.utcnow()
    df.to_parquet(dst, index=False, engine="pyarrow")
    logger.info(f"[Silver] {table}: {len(df)} filas → {dst.name}")
    return dst


def clean_users() -> pd.DataFrame:
    df = _read_bronze("users")
    initial = len(df)

    df = df.drop_duplicates(subset=["user_id"], keep="first")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["name"] = df["name"].astype(str).str.strip()
    df["email"] = df["email"].astype("object").where(df["email"].notna(), None)
    df = df[df["user_id"].notna()]

    logger.info(f"[Silver] users: {initial} → {len(df)} (descartadas {initial - len(df)})")
    return df


def clean_transactions(users_df: pd.DataFrame) -> pd.DataFrame:
    df = _read_bronze("transactions")
    initial = len(df)

    # 1. Deduplicar por PK
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")

    # 2. amount > 0
    before = len(df)
    df = df[df["amount"] > 0]
    logger.info(f"[Silver] transactions: descartadas {before - len(df)} con amount<=0")

    # 3. Estandarizar status
    df["status"] = df["status"].astype(str).str.lower().str.strip()
    before = len(df)
    df = df[df["status"].isin(VALID_STATUSES)]
    logger.info(f"[Silver] transactions: descartadas {before - len(df)} con status invalido")

    # 4. Integridad referencial: user_id debe existir
    valid_user_ids = set(users_df["user_id"])
    before = len(df)
    df = df[df["user_id"].isin(valid_user_ids)]
    logger.info(f"[Silver] transactions: descartadas {before - len(df)} huerfanas (user_id inexistente)")

    # 5. Tipos
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["amount"] = df["amount"].astype("float64")

    logger.info(f"[Silver] transactions: {initial} → {len(df)} (descartadas {initial - len(df)})")
    return df


def clean_details(transactions_df: pd.DataFrame) -> pd.DataFrame:
    df = _read_bronze("transaction_details")
    initial = len(df)

    # 1. Deduplicar por PK
    df = df.drop_duplicates(subset=["detail_id"], keep="first")

    # 2. Integridad referencial
    valid_txn_ids = set(transactions_df["transaction_id"])
    before = len(df)
    df = df[df["transaction_id"].isin(valid_txn_ids)]
    logger.info(f"[Silver] details: descartadas {before - len(df)} huerfanas (transaction_id inexistente)")

    # 3. Estandarizar valores categoricos
    df["payment_method"] = df["payment_method"].astype(str).str.lower().str.strip()
    df["channel"] = df["channel"].astype(str).str.lower().str.strip()

    # 4. processing_time_ms: si es null, dejar como null (campo no obligatorio).
    # Si no es null y es <= 0, descartarlo.
    df = df[df["processing_time_ms"].isna() | (df["processing_time_ms"] > 0)]

    logger.info(f"[Silver] details: {initial} → {len(df)} (descartadas {initial - len(df)})")
    return df


def run() -> dict[str, Path]:
    logger.info("Iniciando transformacion Silver...")
    users_df = clean_users()
    txns_df = clean_transactions(users_df)
    details_df = clean_details(txns_df)

    return {
        "users": _write_silver(users_df, "users"),
        "transactions": _write_silver(txns_df, "transactions"),
        "transaction_details": _write_silver(details_df, "transaction_details"),
    }


if __name__ == "__main__":
    run()
