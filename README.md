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

# 3. Ver el dashboard:

# Opción A — dashboard interactivo (Streamlit)
streamlit run dashboard/app.py

# Opción B — reporte estático (un solo HTML, doble click)
python dashboard/generate_report.py
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

- `amount > 0` (descarta 10 filas en datos de prueba)
- Sin duplicados por `transaction_id` (descarta 8 filas)
- Integridad referencial: no transacciones sin usuario (descarta 15 huérfanas)
- Integridad referencial: no detalles sin transacción
- status` estandarizado a lowercase (`success` / `failed`)

## Estructura del proyecto

```
puntored-pipeline/
├── pipeline.py             
├── requirements.txt
├── README.md
├── sql/                     
├── dashboard/
│   ├── app.py              
│   └── generate_report.py  
├── data/
│   ├── raw/                 
│   ├── bronze/              
│   ├── silver/             
│   └── gold/                
├── notebooks/
│   └── exploracion.ipynb    
└── docs/
    ├── arquitectura.md      
    ├── resumen_ejecutivo.md 
    └── reporte.html        
```

## Insights ejecutivos

Ver [`docs/resumen_ejecutivo.md`](docs/resumen_ejecutivo.md). En síntesis:

1. **Wallet + Mobile** tiene 18.3% de fallo (vs. 7.7% global) — fricción puntual a auditar.
2. **Web es 3.1× más lento que API** (786ms vs 250ms) — impacta conversión.
3. **Card concentra 45% del volumen económico** — riesgo de concentración, requiere fallback.