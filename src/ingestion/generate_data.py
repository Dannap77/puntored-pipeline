"""Genera datos sintéticos de transacciones financieras.

Produce 3 CSV en data/raw/:
    - users.csv
    - transactions.csv
    - transaction_details.csv

Inyecta intencionalmente errores (duplicados, nulls, status en mayúsculas,
montos negativos, transacciones huérfanas) para validar la capa Silver.
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

from config.settings import (
    DATA_RAW,
    N_DETAILS_PER_TRANSACTION,
    N_TRANSACTIONS,
    N_USERS,
    RANDOM_SEED,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

PAYMENT_METHODS = ["card", "pse", "cash", "wallet", "bank_transfer"]
CHANNELS = ["web", "mobile", "api"]
STATUSES = ["success", "failed"]


def _seed():
    Faker.seed(RANDOM_SEED)
    random.seed(RANDOM_SEED)


def generate_users(n: int) -> pd.DataFrame:
    fake = Faker("es_CO")
    users = []
    base_date = datetime(2023, 1, 1)
    for i in range(1, n + 1):
        users.append({
            "user_id": f"U{i:05d}",
            "name": fake.name(),
            "email": fake.unique.email(),
            "created_at": base_date + timedelta(days=random.randint(0, 700)),
        })

    df = pd.DataFrame(users)

    # Inyectamos suciedad: ~2% de emails nulos (campo no obligatorio)
    null_idx = df.sample(frac=0.02, random_state=RANDOM_SEED).index
    df.loc[null_idx, "email"] = None

    # 5 duplicados exactos
    duplicates = df.sample(5, random_state=RANDOM_SEED)
    df = pd.concat([df, duplicates], ignore_index=True)

    return df


def generate_transactions(n: int, user_ids: list[str]) -> pd.DataFrame:
    fake = Faker()
    txns = []
    base_date = datetime(2024, 1, 1)
    for i in range(1, n + 1):
        status = random.choices(STATUSES, weights=[0.85, 0.15])[0]
        # Variamos el caso del status (success / SUCCESS / Success) para limpiar luego
        status = random.choice([status, status.upper(), status.capitalize()])
        txns.append({
            "transaction_id": f"T{i:07d}",
            "user_id": random.choice(user_ids),
            "amount": round(random.uniform(5_000, 2_500_000), 2),
            "status": status,
            "created_at": base_date + timedelta(
                days=random.randint(0, 450),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            ),
        })

    df = pd.DataFrame(txns)

    # Suciedad: 10 montos negativos (regla de negocio amount > 0)
    neg_idx = df.sample(10, random_state=RANDOM_SEED).index
    df.loc[neg_idx, "amount"] = df.loc[neg_idx, "amount"] * -1

    # 15 transacciones huérfanas (user_id que no existe)
    orphan_idx = df.sample(15, random_state=RANDOM_SEED + 1).index
    df.loc[orphan_idx, "user_id"] = "U99999"

    # 8 duplicados por transaction_id
    duplicates = df.sample(8, random_state=RANDOM_SEED + 2)
    df = pd.concat([df, duplicates], ignore_index=True)

    return df


def generate_details(transaction_ids: list[str]) -> pd.DataFrame:
    details = []
    detail_counter = 1
    for txn_id in transaction_ids:
        n_details = random.choices([1, 2], weights=[0.92, 0.08])[0]
        for _ in range(n_details):
            method = random.choices(
                PAYMENT_METHODS, weights=[0.45, 0.20, 0.10, 0.15, 0.10]
            )[0]
            channel = random.choices(CHANNELS, weights=[0.30, 0.55, 0.15])[0]

            # Tiempo de procesamiento depende del canal
            base_ms = {"web": 800, "mobile": 600, "api": 250}[channel]
            processing_ms = max(50, int(random.gauss(base_ms, base_ms * 0.4)))

            details.append({
                "detail_id": f"D{detail_counter:08d}",
                "transaction_id": txn_id,
                "payment_method": method,
                "channel": channel,
                "processing_time_ms": processing_ms,
            })
            detail_counter += 1

    df = pd.DataFrame(details)

    # 5 detalles huérfanos (transaction_id que no existe)
    orphan_idx = df.sample(5, random_state=RANDOM_SEED + 3).index
    df.loc[orphan_idx, "transaction_id"] = "T9999999"

    # ~1% de processing_time_ms nulos
    null_idx = df.sample(frac=0.01, random_state=RANDOM_SEED + 4).index
    df.loc[null_idx, "processing_time_ms"] = None

    return df


def run() -> dict[str, Path]:
    logger.info("Iniciando generación de datos sintéticos...")
    _seed()
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    users_df = generate_users(N_USERS)
    logger.info(f"Generados {len(users_df)} usuarios (incluye duplicados/nulls)")

    txns_df = generate_transactions(N_TRANSACTIONS, users_df["user_id"].unique().tolist())
    logger.info(f"Generadas {len(txns_df)} transacciones")

    details_df = generate_details(txns_df["transaction_id"].tolist())
    logger.info(f"Generados {len(details_df)} detalles")

    paths = {
        "users": DATA_RAW / "users.csv",
        "transactions": DATA_RAW / "transactions.csv",
        "transaction_details": DATA_RAW / "transaction_details.csv",
    }
    users_df.to_csv(paths["users"], index=False)
    txns_df.to_csv(paths["transactions"], index=False)
    details_df.to_csv(paths["transaction_details"], index=False)

    logger.info(f"Datos guardados en {DATA_RAW}/")
    return paths


if __name__ == "__main__":
    run()
