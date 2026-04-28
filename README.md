# Pipeline de Datos — Prueba Técnica PuntoRed

Pipeline que ingesta, transforma y expone transacciones financieras siguiendo
**arquitectura Medallion (Bronze → Silver → Gold)**, con dashboard interactivo
y reporte estático.

## Arquitectura

```
┌────────────┐   ┌──────────┐   ┌──────────┐   ┌────────┐   ┌─────────────┐
│  Faker     │──▶│  Bronze  │──▶│  Silver  │──▶│  Gold  │──▶│  Dashboard  │
│ (3 tablas) │   │ raw .pq  │   │ clean    │   │ KPIs   │   │ + reporte   │
└────────────┘   └──────────┘   └──────────┘   └────────┘   └─────────────┘
                                                    │
                                              ┌─────▼─────┐
                                              │  Quality  │
                                              │   checks  │
                                              └───────────┘
```

Diagrama detallado: [`docs/arquitectura.md`](docs/arquitectura.md).

## Stack

| Capa | Herramienta | Motivo |
|---|---|---|
| Lenguaje | **Python 3.11** | Estándar en data |
| Transformación | **pandas** | Sintaxis cercana a SQL |
| Storage | **Parquet** | Columnar, comprimido, tipado |
| Motor SQL | **DuckDB** | Lee parquet directo, sin servidor |
| Dashboard | **Streamlit + Plotly** | Interactivo, en Python puro |
| Reporte estático | **HTML + Plotly** | Un solo archivo, no requiere Python |
| Datos | **Faker** | Genera 3 tablas sintéticas con suciedad inyectada |

## Cómo correrlo

```bash
# 1. Instalar
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# Windows Git Bash / Linux / Mac:
source .venv/Scripts/activate
pip install -r requirements.txt

# 2. Ejecutar pipeline completo
python pipeline.py

# 3. Ver el dashboard (elige una de las dos opciones):

# Opción A — dashboard interactivo (Streamlit)
streamlit run dashboard/app.py

# Opción B — reporte estático (un solo HTML, doble click)
python dashboard/generate_report.py
# luego abre docs/reporte.html
```

## Modelo de datos

3 tablas relacionadas, idénticas a las del enunciado:

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

## Capa Gold (KPIs)

Las queries SQL están en [`sql/`](sql/) — una por tabla Gold. Las puedes leer y modificar directamente.

| Tabla | Propósito |
|---|---|
| `gold_transactions_enriched` | Fact denormalizada (1 fila por detail con JOIN a tx + user) |
| `gold_kpi_overall` | Totales globales |
| `gold_kpi_by_user` | Métricas por usuario |
| `gold_kpi_by_payment_method` | Métricas por método de pago |
| `gold_kpi_by_channel` | Métricas por canal |
| `gold_kpi_by_method_channel` | Cruce método × canal (insight estrella) |
| `gold_kpi_by_day` | Serie temporal diaria |

## Reglas de negocio aplicadas (Silver)

Todas las reglas del enunciado están implementadas en `pipeline.py` (función `construir_silver`):

- ✅ `amount > 0` (descarta 10 filas en datos de prueba)
- ✅ Sin duplicados por `transaction_id` (descarta 8 filas)
- ✅ Integridad referencial: no transacciones sin usuario (descarta 15 huérfanas)
- ✅ Integridad referencial: no detalles sin transacción
- ✅ `status` estandarizado a lowercase (`success` / `failed`)

## Estructura del proyecto

```
puntored-pipeline/
├── pipeline.py              # ← script principal (todo en un archivo)
├── requirements.txt
├── README.md
├── sql/                     # 7 queries SQL de la capa Gold
├── dashboard/
│   ├── app.py               # Streamlit
│   └── generate_report.py   # genera el HTML estático
├── data/
│   ├── raw/                 # CSV originales
│   ├── bronze/              # parquet sin transformar
│   ├── silver/              # parquet limpio
│   └── gold/                # KPIs + warehouse DuckDB
├── notebooks/
│   └── exploracion.ipynb    # análisis exploratorio
└── docs/
    ├── arquitectura.md      # diagrama + decisiones técnicas
    ├── resumen_ejecutivo.md # 3 insights + recomendaciones
    └── reporte.html         # reporte estático (generado)
```

## Niveles de la rúbrica

| Nivel | Cobertura | Implementación |
|---|---|---|
| **0 — Ingesta (10%)** | ✅ | `pipeline.py` (función `generar_datos` + `cargar_bronze`) |
| **1 — Silver (20%)** | ✅ | `pipeline.py` (función `construir_silver`) — todas las reglas del PDF |
| **2 — Gold (20%)** | ✅ | 7 archivos SQL en `sql/` ejecutados desde `construir_gold` |
| **3 — Calidad (15%)** | ✅ | 4 validaciones críticas en `pipeline.py` (función `validar`) |
| **4 — Arquitectura (15%)** | 🟡 | Pipeline modular en funciones, separación capas. Falta: incremental + particionamiento |
| **5 — Insights (20%)** | ✅ | Dashboard + reporte HTML + [`docs/resumen_ejecutivo.md`](docs/resumen_ejecutivo.md) |

## Insights ejecutivos

Ver [`docs/resumen_ejecutivo.md`](docs/resumen_ejecutivo.md). En síntesis:

1. **Wallet + Mobile** tiene 18.3% de fallo (vs. 7.7% global) — fricción puntual a auditar.
2. **Web es 3.1× más lento que API** (786ms vs 250ms) — impacta conversión.
3. **Card concentra 45% del volumen económico** — riesgo de concentración, requiere fallback.

## Decisiones técnicas

- **Datos sintéticos con Faker**: el PDF permite elegir fuente; generamos datos con suciedad
  intencional (duplicados, nulls, montos negativos, status mixto, huérfanos) para demostrar
  el valor de la limpieza en Silver.
- **DuckDB en lugar de PostgreSQL**: cero infraestructura, lee parquet directo con SQL,
  escalable a Snowflake/BigQuery sin tocar SQL.
- **SQL externo (no embebido)**: las queries de Gold viven en `sql/*.sql` — fáciles de leer,
  modificar y auditar de forma independiente al código Python.
- **Reporte HTML adicional al dashboard Streamlit**: para que el evaluador no tenga que
  correr nada — solo abrir el archivo.

## Mejoras posibles (no implementadas)

- Particionamiento por fecha en Bronze/Silver.
- Incremental loads (procesar solo nuevos `_ingested_at`).
- Orquestación con Airflow o Prefect.
- Validaciones declarativas con Great Expectations.
