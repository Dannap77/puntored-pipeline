# Arquitectura del Pipeline

## Diagrama de flujo

```mermaid
flowchart LR
    subgraph FUENTES["Fuente"]
        F1[Faker<br/>generador]
    end

    subgraph BRONZE["Bronze<br/>(raw)"]
        B1[users.parquet]
        B2[transactions.parquet]
        B3[transaction_details.parquet]
    end

    subgraph SILVER["Silver<br/>(clean)"]
        S1[users.parquet]
        S2[transactions.parquet]
        S3[transaction_details.parquet]
    end

    subgraph GOLD["Gold<br/>(analytics)"]
        G1[transactions_enriched]
        G2[kpi_overall]
        G3[kpi_by_user]
        G4[kpi_by_payment_method]
        G5[kpi_by_channel]
        G6[kpi_by_method_channel]
        G7[kpi_by_day]
    end

    subgraph CONSUMERS["Consumo"]
        D1[Streamlit<br/>Dashboard]
        D2[HTML estático<br/>reporte.html]
        D3[Notebook<br/>Jupyter]
    end

    QC[Quality Checks<br/>4 validaciones]

    F1 --> B1 & B2 & B3
    B1 --> S1
    B2 --> S2
    B3 --> S3
    S1 & S2 & S3 --> G1
    G1 --> G2 & G3 & G4 & G5 & G6 & G7
    G2 & G3 & G4 & G5 & G6 & G7 --> D1
    G2 & G3 & G4 & G5 & G6 & G7 --> D2
    G2 & G3 & G4 & G5 & G6 & G7 --> D3

    SILVER --> QC
```

Todo el pipeline vive en un solo archivo: [`pipeline.py`](../pipeline.py),
con cinco funciones que reflejan las cinco etapas:
`generar_datos()` → `cargar_bronze()` → `construir_silver()` → `construir_gold()` → `validar()`.


### ¿Por qué dos versiones del dashboard?
- **Streamlit** (`dashboard/app.py`): interactivo, profesional, requiere correr un servidor.
- **HTML estático** (`docs/reporte.html`): un solo archivo, doble-click y listo. Ideal
  para compartir como adjunto o cuando no se puede correr Python.

