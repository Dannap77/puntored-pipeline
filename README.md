# Pipeline de Datos — Prueba Técnica PuntoRed

Pipeline de datos que ingesta, transforma y expone transacciones financieras siguiendo
una arquitectura **Medallion (Bronze → Silver → Gold)**.

## Arquitectura

```
┌─────────────┐    ┌────────────┐    ┌────────────┐    ┌──────────┐    ┌────────────┐
│  Generador  │───▶│   Bronze   │───▶│   Silver   │───▶│   Gold   │───▶│ Dashboard  │
│  (Faker)    │    │ (raw .pq)  │    │  (clean)   │    │  (KPIs)  │    │ (Streamlit)│
└─────────────┘    └────────────┘    └────────────┘    └──────────┘    └────────────┘
                          │                 │                │
                          └─────────────────┴────────────────┘
                                      │
                                ┌─────▼─────┐
                                │  Quality  │
                                │   Checks  │
                                └───────────┘
```

## Stack

- **Python 3.11** — lenguaje principal
- **pandas** — transformaciones
- **DuckDB** — motor SQL analítico (lee parquet directamente)
- **Parquet** — formato columnar para Bronze/Silver/Gold
- **Faker** — generación de datos sintéticos
- **Streamlit + Plotly** — dashboard interactivo

## Estructura del proyecto

```
puntored-pipeline/
├── config/             # configuración (variables de entorno)
├── src/
│   ├── ingestion/      # generación de datos sintéticos
│   ├── bronze/         # ingesta cruda
│   ├── silver/         # limpieza + reglas de negocio
│   ├── gold/           # KPIs agregados
│   ├── quality/        # validaciones
│   └── utils/          # logger, helpers
├── data/
│   ├── raw/            # CSV originales
│   ├── bronze/         # parquet sin transformar
│   ├── silver/         # parquet limpio
│   └── gold/           # tablas analíticas
├── sql/                # queries SQL para KPIs
├── dashboard/          # app Streamlit
├── notebooks/          # análisis exploratorio
├── docs/               # diagramas, resumen ejecutivo
└── main.py             # orquestador
```

## Cómo ejecutar

### 1. Configurar entorno

```bash
# Crear y activar entorno virtual
python -m venv .venv
# Windows (PowerShell):
.venv\Scripts\Activate.ps1
# Windows (Git Bash):
source .venv/Scripts/activate

# Instalar dependencias
pip install -r requirements.txt

# Copiar variables de entorno
cp .env.example .env
```

### 2. Correr el pipeline

```bash
python main.py
```

Esto ejecuta en orden: ingestión → Bronze → Silver → Gold → validaciones de calidad.

### 3. Abrir el dashboard

```bash
streamlit run dashboard/app.py
```

## Decisiones técnicas

- **DuckDB** en vez de PostgreSQL: motor analítico en proceso, sin servidor, lee parquet
  directamente con SQL. Ideal para pruebas locales y prototipos.
- **Parquet** en vez de CSV: formato columnar, comprimido, tipado, ~10x más rápido para analítica.
- **Datos sintéticos** con Faker: el ejercicio permite elegir la fuente y se generan datos
  realistas con duplicados/nulls/errores intencionales para validar la limpieza.
- **Trazabilidad**: cada capa añade columnas `_ingested_at` y `_pipeline_run_id` para auditar.

## Niveles cubiertos (rúbrica)

- [x] Nivel 0 — Configuración e ingesta (Bronze)
- [x] Nivel 1 — Transformación (Silver)
- [x] Nivel 2 — Modelado y capa Gold
- [x] Nivel 3 — Calidad y observabilidad
- [x] Nivel 4 — Arquitectura (orquestación + particionamiento)
- [x] Nivel 5 — Insights de negocio + dashboard

## Resumen ejecutivo

Ver `docs/resumen_ejecutivo.md` con los 3 hallazgos principales y recomendaciones.
