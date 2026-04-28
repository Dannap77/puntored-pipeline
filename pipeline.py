"""Pipeline de transacciones (Bronze -> Silver -> Gold).
Uso:
    python pipeline.py
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
from faker import Faker

ROOT = Path(__file__).resolve().parent
RAW    = ROOT / "data" / "raw"
BRONZE = ROOT / "data" / "bronze"
SILVER = ROOT / "data" / "silver"
GOLD   = ROOT / "data" / "gold"
SQL_DIR = ROOT / "sql"

N_USERS = 500
N_TRANSACTIONS = 5000
SEED = 42

PAYMENT_METHODS = ["card", "pse", "cash", "wallet", "bank_transfer"]
CHANNELS = ["web", "mobile", "api"]


def generar_datos():
    print("\n[1/5] Generando datos sintéticos con Faker...")
    Faker.seed(SEED); random.seed(SEED)
    fake = Faker("es_CO")

    users = pd.DataFrame([{
        "user_id":    f"U{i:05d}",
        "name":       fake.name(),
        "email":      fake.unique.email(),
        "created_at": datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700)),
    } for i in range(1, N_USERS + 1)])
    # Inyectamos suciedad para que la limpieza de Silver tenga algo que demostrar
    users.loc[users.sample(frac=0.02, random_state=SEED).index, "email"] = None
    users = pd.concat([users, users.sample(5, random_state=SEED)], ignore_index=True)

    user_ids = users["user_id"].unique().tolist()
    txns = []
    for i in range(1, N_TRANSACTIONS + 1):
        status = random.choices(["success", "failed"], weights=[0.85, 0.15])[0]
        # Mezclamos casing para forzar la normalización en Silver
        status = random.choice([status, status.upper(), status.capitalize()])
        txns.append({
            "transaction_id": f"T{i:07d}",
            "user_id":        random.choice(user_ids),
            "amount":         round(random.uniform(5_000, 2_500_000), 2),
            "status":         status,
            "created_at":     datetime(2024, 1, 1) + timedelta(
                days=random.randint(0, 450),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            ),
        })
    txns = pd.DataFrame(txns)
    txns.loc[txns.sample(10, random_state=SEED).index,   "amount"]  *= -1
    txns.loc[txns.sample(15, random_state=SEED+1).index, "user_id"] = "U99999"
    txns = pd.concat([txns, txns.sample(8, random_state=SEED+2)], ignore_index=True)

    details = []
    counter = 1
    for txn_id in txns["transaction_id"]:
        for _ in range(random.choices([1, 2], weights=[0.92, 0.08])[0]):
            channel = random.choices(CHANNELS, weights=[0.30, 0.55, 0.15])[0]
            base_ms = {"web": 800, "mobile": 600, "api": 250}[channel]
            details.append({
                "detail_id":          f"D{counter:08d}",
                "transaction_id":     txn_id,
                "payment_method":     random.choices(PAYMENT_METHODS, weights=[0.45, 0.20, 0.10, 0.15, 0.10])[0],
                "channel":            channel,
                "processing_time_ms": max(50, int(random.gauss(base_ms, base_ms * 0.4))),
            })
            counter += 1
    details = pd.DataFrame(details)
    details.loc[details.sample(5, random_state=SEED+3).index, "transaction_id"] = "T9999999"
    details.loc[details.sample(frac=0.01, random_state=SEED+4).index, "processing_time_ms"] = None

    RAW.mkdir(parents=True, exist_ok=True)
    users.to_csv(RAW / "users.csv", index=False)
    txns.to_csv(RAW / "transactions.csv", index=False)
    details.to_csv(RAW / "transaction_details.csv", index=False)
    print(f"      {len(users)} usuarios, {len(txns)} transacciones, {len(details)} detalles")


def cargar_bronze():
    print("\n[2/5] Bronze: copiando CSV crudos a parquet...")
    BRONZE.mkdir(parents=True, exist_ok=True)
    for tabla in ("users", "transactions", "transaction_details"):
        df = pd.read_csv(RAW / f"{tabla}.csv")
        df["_ingested_at"] = datetime.utcnow()
        df.to_parquet(BRONZE / f"{tabla}.parquet", index=False)
        print(f"      {tabla}: {len(df)} filas")


def construir_silver():
    print("\n[3/5] Silver: limpieza con pandas...")
    SILVER.mkdir(parents=True, exist_ok=True)

    users = pd.read_parquet(BRONZE / "users.parquet")
    users = users.drop_duplicates(subset=["user_id"])
    users["created_at"] = pd.to_datetime(users["created_at"])
    users.to_parquet(SILVER / "users.parquet", index=False)
    print(f"      users: {len(users)} filas")

    txns = pd.read_parquet(BRONZE / "transactions.parquet")
    n_inicial = len(txns)
    txns = txns.drop_duplicates(subset=["transaction_id"])
    txns = txns[txns["amount"] > 0]
    txns["status"] = txns["status"].str.lower().str.strip()
    txns = txns[txns["status"].isin(["success", "failed"])]
    txns = txns[txns["user_id"].isin(users["user_id"])]
    txns["created_at"] = pd.to_datetime(txns["created_at"])
    txns.to_parquet(SILVER / "transactions.parquet", index=False)
    print(f"      transactions: {n_inicial} -> {len(txns)} (descartadas {n_inicial - len(txns)})")

    details = pd.read_parquet(BRONZE / "transaction_details.parquet")
    n_inicial = len(details)
    details = details.drop_duplicates(subset=["detail_id"])
    details = details[details["transaction_id"].isin(txns["transaction_id"])]
    details["payment_method"] = details["payment_method"].str.lower()
    details["channel"] = details["channel"].str.lower()
    details.to_parquet(SILVER / "transaction_details.parquet", index=False)
    print(f"      transaction_details: {n_inicial} -> {len(details)} (descartadas {n_inicial - len(details)})")


def construir_gold():
    print("\n[4/5] Gold: ejecutando queries SQL...")
    GOLD.mkdir(parents=True, exist_ok=True)
    db = GOLD / "warehouse.duckdb"
    if db.exists():
        db.unlink()

    os.chdir(ROOT)
    con = duckdb.connect(str(db))

    archivos_sql = [
        "gold_transactions_enriched.sql",
        "gold_kpi_overall.sql",
        "gold_kpi_by_user.sql",
        "gold_kpi_by_payment_method.sql",
        "gold_kpi_by_channel.sql",
        "gold_kpi_by_method_channel.sql",
        "gold_kpi_by_day.sql",
    ]
    for archivo in archivos_sql:
        sql = (SQL_DIR / archivo).read_text(encoding="utf-8")
        con.execute(sql)
        tabla = archivo.replace(".sql", "")
        n = con.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
        con.execute(f"COPY {tabla} TO '{(GOLD / (tabla + '.parquet')).as_posix()}' (FORMAT PARQUET)")
        print(f"      {tabla}: {n} filas")

    con.close()


def validar():
    print("\n[5/5] Validaciones de calidad...")
    txns    = pd.read_parquet(SILVER / "transactions.parquet")
    users   = pd.read_parquet(SILVER / "users.parquet")
    details = pd.read_parquet(SILVER / "transaction_details.parquet")

    checks = [
        ("amount > 0",                        (txns["amount"] > 0).all()),
        ("status in {success, failed}",       txns["status"].isin(["success", "failed"]).all()),
        ("transactions sin huérfanas",        txns["user_id"].isin(users["user_id"]).all()),
        ("transaction_details sin huérfanos", details["transaction_id"].isin(txns["transaction_id"]).all()),
    ]
    for nombre, ok in checks:
        marca = "OK  " if ok else "FAIL"
        print(f"      [{marca}] {nombre}")

    if not all(ok for _, ok in checks):
        raise RuntimeError("Alguna validación falló")


if __name__ == "__main__":
    print("Pipeline PuntoRed: Bronze -> Silver -> Gold")
    generar_datos()
    cargar_bronze()
    construir_silver()
    construir_gold()
    validar()
    print("\nPipeline OK.")
    print("Dashboard: streamlit run dashboard/app.py")
    print("Reporte:   python dashboard/generate_report.py  (genera docs/reporte.html)")
