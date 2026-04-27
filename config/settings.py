"""Carga de configuración desde variables de entorno."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent

N_USERS = int(os.getenv("N_USERS", 500))
N_TRANSACTIONS = int(os.getenv("N_TRANSACTIONS", 5000))
N_DETAILS_PER_TRANSACTION = int(os.getenv("N_DETAILS_PER_TRANSACTION", 1))
RANDOM_SEED = int(os.getenv("RANDOM_SEED", 42))

DATA_RAW = ROOT / os.getenv("DATA_RAW_PATH", "data/raw")
DATA_BRONZE = ROOT / os.getenv("DATA_BRONZE_PATH", "data/bronze")
DATA_SILVER = ROOT / os.getenv("DATA_SILVER_PATH", "data/silver")
DATA_GOLD = ROOT / os.getenv("DATA_GOLD_PATH", "data/gold")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = ROOT / os.getenv("LOG_FILE", "logs/pipeline.log")

DUCKDB_PATH = ROOT / os.getenv("DUCKDB_PATH", "data/gold/warehouse.duckdb")
