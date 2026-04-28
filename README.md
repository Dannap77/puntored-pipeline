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