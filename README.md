# Pipeline de Datos — Prueba Técnica PuntoRed

Pipeline que ingesta, transforma y expone transacciones financieras siguiendo
**arquitectura Medallion (Bronze → Silver → Gold)**, con dashboard interactivo
y validaciones de calidad automatizadas.

> 💡 **Punto de partida rápido:** corre `python main.py` y luego `streamlit run dashboard/app.py`.
> Todo se construye desde cero en menos de 1 segundo.

## Arquitectura

```
┌────────────┐   ┌──────────┐   ┌──────────┐   ┌────────┐   ┌─────────────┐
│  Faker     │──▶│  Bronze  │──▶│  Silver  │──▶│  Gold  │──▶│  Dashboard  │
│ (3 tablas) │   │ raw .pq  │   │ clean    │   │ KPIs   │   │ (Streamlit) │
└────────────┘   └────┬─────┘   └────┬─────┘   └───┬────┘   └─────────────┘
                      │              │              │
                      └──────────────┴──────────────┘
                                     │
                              ┌──────▼──────┐
                              │  Quality    │
                              │  17 checks  │
                              └─────────────┘
```

Diagrama detallado: [`docs/arquitectura.md`](docs/arquitectura.md).

## Stack

| Capa | Herramienta | Motivo |
|---|---|---|
| Lenguaje | **Python 3.11** | Estándar en data |
| Transformación | **pandas** | Sintaxis cercana a SQL |
| Storage | **Parquet** | Columnar, comprimido, tipado |
| SQL Engine | **DuckDB** | Lee parquet directo, sin servidor |
| Dashboard | **Streamlit + Plotly** | Interactivo, en Python puro |
| Datos | **Faker** | Genera 3 tablas sintéticas con suciedad inyectada |
| Calidad | Validaciones en Python + logging | 17 checks automatizados |

## Cómo correrlo

```bash
# 1. Instalar
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# Windows Git Bash / Linux / Mac:
source .venv/Scripts/activate

pip install -r requirements.txt
cp .env.example .env

# 2. Ejecutar pipeline completo (genera datos, Bronze, Silver, Gold, valida)
python main.py

# 3. Abrir dashboard
streamlit run dashboard/app.py
```

Salida esperada del pipeline:

```
========== 1. Generacion de datos ==========
[INFO] Generados 505 usuarios (incluye duplicados/nulls)
[INFO] Generadas 5008 transacciones
[INFO] Generados 5411 detalles
========== 2. Bronze ==========
[INFO] [Bronze] users: 505 filas → users.parquet
[INFO] [Bronze] transactions: 5008 filas → transactions.parquet
[INFO] [Bronze] transaction_details: 5411 filas → transaction_details.parquet
========== 3. Silver ==========
[INFO] [Silver] users: 505 → 500 (descartadas 5)
[INFO] [Silver] transactions: 5008 → 4975 (descartadas 33)
[INFO] [Silver] transaction_details: 5411 → 5380 (descartadas 31)
========== 4. Gold ==========
[INFO] [Gold] gold_transactions_enriched: 5380 filas
[INFO] [Gold] gold_kpi_overall, gold_kpi_by_user, gold_kpi_by_payment_method,
              gold_kpi_by_channel, gold_kpi_by_method_channel, gold_kpi_by_day
========== 5. Quality checks ==========
Quality checks: 17/17 OK
Pipeline OK en 0.87s
```

## Modelo de datos

**Bronze / Silver:** 3 tablas relacionadas, idénticas a las del enunciado:

```
users (user_id PK, name, email, created_at)
   │
   │ 1..N
   ▼
transactions (transaction_id PK, user_id FK, amount, status, created_at)
   │
   │ 1..N
   ▼
transaction_details (detail_id PK, transaction_id FK, payment_method, channel, processing_time_ms)
```

**Gold:** 1 fact table denormalizada + 6 tablas de KPIs agregados:

| Tabla | Filas | Propósito |
|---|---|---|
| `gold_transactions_enriched` | ~5,380 | Fact denormalizada (1 fila por detail con JOIN a tx + user) |
| `gold_kpi_overall` | 1 | Totales globales |
| `gold_kpi_by_user` | 500 | Métricas por usuario |
| `gold_kpi_by_payment_method` | 5 | Métricas por método de pago |
| `gold_kpi_by_channel` | 3 | Métricas por canal |
| `gold_kpi_by_method_channel` | 15 | Cruce método × canal |
| `gold_kpi_by_day` | ~450 | Serie temporal diaria |

Las queries SQL están en [`sql/`](sql/) — una por tabla Gold, fáciles de auditar.

## Reglas de negocio aplicadas (Silver)

Todas las reglas del enunciado están implementadas en `src/silver/transform.py` y verificadas en `src/quality/validations.py`:

- ✅ `amount > 0` (descarta 10 filas en datos de prueba)
- ✅ Sin duplicados por `transaction_id` (descarta 8 filas)
- ✅ Integridad referencial: no transacciones sin usuario (descarta 15 huérfanas)
- ✅ Integridad referencial: no detalles sin transacción
- ✅ `status` estandarizado a lowercase (`success` / `failed`)
- ✅ Manejo de nulls en campos no obligatorios (email, processing_time_ms)
- ✅ Trazabilidad Bronze → Silver → Gold con `_pipeline_run_id`

## Estructura del proyecto

```
puntored-pipeline/
├── main.py                  # ← orquestador (entry point)
├── requirements.txt
├── .env.example
├── config/
│   └── settings.py          # carga env vars
├── src/
│   ├── ingestion/           # generador Faker
│   ├── bronze/              # CSV → parquet
│   ├── silver/              # limpieza + reglas
│   ├── gold/                # ejecuta SQL
│   ├── quality/             # 17 validaciones
│   └── utils/logger.py      # logging unificado
├── sql/                     # 7 queries SQL para la capa Gold
├── data/                    # bronze/silver/gold/raw (ignorado por git)
├── dashboard/app.py         # Streamlit
├── notebooks/exploracion.ipynb
├── docs/
│   ├── arquitectura.md      # diagrama + decisiones
│   └── resumen_ejecutivo.md # 3 insights + recomendaciones
└── logs/pipeline.log        # generado en ejecución
```

## Niveles de la rúbrica

| Nivel | Cobertura | Implementación |
|---|---|---|
| **0 — Ingesta (10%)** | ✅ Completo | `src/ingestion/`, `src/bronze/`, env vars (`.env`) |
| **1 — Silver (20%)** | ✅ Completo | Todas las reglas del PDF en `src/silver/transform.py` |
| **2 — Gold (20%)** | ✅ Completo | 7 tablas en `data/gold/`, KPIs por usuario/método/canal/tiempo |
| **3 — Calidad (15%)** | ✅ Completo | 17 validaciones, logging a archivo + stdout, manejo de errores |
| **4 — Arquitectura (15%)** | 🟡 Parcial | Orquestación, modularidad, env vars. Falta: incremental + particionamiento |
| **5 — Insights (20%)** | ✅ Completo | Dashboard + 3 insights accionables en `docs/resumen_ejecutivo.md` |

## Insights ejecutivos

Ver [`docs/resumen_ejecutivo.md`](docs/resumen_ejecutivo.md). En síntesis:

1. **Wallet + Mobile** tiene 18.3% de fallo (vs. 7.7% global) — fricción puntual a auditar.
2. **Web es 3.1× más lento que API** (786ms vs 250ms) — impacta conversión.
3. **Card concentra 45% del volumen económico** — riesgo de concentración, requiere fallback.

## Decisiones técnicas clave

- **DuckDB** en lugar de PostgreSQL: cero infraestructura, lee parquet directo con SQL, escalable a Snowflake/BigQuery sin cambios.
- **Datos sintéticos con Faker**: el PDF permite elegir fuente. Generamos datos con suciedad intencional (duplicados, nulls, montos negativos, status mixto, huérfanos) para demostrar el valor de Silver.
- **Trazabilidad por capa**: cada parquet lleva `_pipeline_run_id` y `_ingested_at`; permite auditar de qué corrida vino cada fila.
- **SQL externo (no embebido)**: las queries de Gold viven en `sql/*.sql` — un analyst puede modificarlas sin tocar Python.

## Mejoras posibles (no implementadas)

- Particionamiento por fecha en Bronze/Silver (`year=2025/month=01/...`).
- Incremental loads (procesar solo nuevos `_ingested_at`).
- Reemplazar runner manual con **Prefect** o **Airflow**.
- Migrar validaciones a **Great Expectations** declarativas.
- Streaming con Kafka + Spark Structured Streaming.
