"""Genera un reporte HTML estático autocontenido en docs/reporte.html.

Es el backup del dashboard Streamlit: un solo archivo que se abre
con doble click, sin necesidad de correr Python.

Ejecutar:
    python dashboard/generate_report.py
"""

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.io as pio

ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = ROOT / "data" / "gold" / "warehouse.duckdb"
OUTPUT = ROOT / "docs" / "reporte.html"

con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
overall = con.execute("SELECT * FROM gold_kpi_overall").fetchdf().iloc[0]
by_pm = con.execute("SELECT * FROM gold_kpi_by_payment_method").fetchdf()
by_ch = con.execute("SELECT * FROM gold_kpi_by_channel").fetchdf()
by_mc = con.execute("SELECT * FROM gold_kpi_by_method_channel").fetchdf()
by_user_top = con.execute(
    "SELECT * FROM gold_kpi_by_user ORDER BY revenue DESC LIMIT 10"
).fetchdf()
by_day = con.execute(
    "SELECT * FROM gold_kpi_by_day ORDER BY transaction_date"
).fetchdf()
con.close()

worst = by_mc.sort_values("failure_rate", ascending=False).iloc[0]
slowest = by_ch.sort_values("avg_processing_time_ms", ascending=False).iloc[0]
fastest = by_ch.sort_values("avg_processing_time_ms", ascending=True).iloc[0]
top_method = by_pm.sort_values("total_amount", ascending=False).iloc[0]
top_method_share = top_method["total_amount"] / by_pm["total_amount"].sum() * 100

by_day["transaction_date"] = pd.to_datetime(by_day["transaction_date"])
by_day["mes"] = by_day["transaction_date"].dt.to_period("M").astype(str)
monthly = by_day.groupby("mes", as_index=False).agg(
    tx=("total_transactions", "sum"),
    success_rate=("success_rate", "mean"),
)

fig_volume = px.bar(monthly, x="mes", y="tx", title="Transacciones por mes")
fig_rate = px.line(
    monthly, x="mes", y="success_rate", markers=True,
    title="Tasa de éxito mensual",
)
fig_rate.update_yaxes(tickformat=".1%")

fig_pm = px.bar(
    by_pm, x="payment_method", y="total_amount",
    color="payment_method", title="Volumen económico por método de pago",
)

pivot = by_mc.pivot_table(
    index="payment_method", columns="channel", values="success_rate", aggfunc="first"
)
fig_heatmap = px.imshow(
    pivot, text_auto=".1%", color_continuous_scale="RdYlGn",
    aspect="auto", title="Tasa de éxito: método × canal",
)

fig_ch_time = px.bar(
    by_ch, x="channel", y="avg_processing_time_ms",
    color="channel", title="Tiempo promedio de procesamiento por canal (ms)",
)

fig_top = px.bar(
    by_user_top, x="revenue", y="user_name", orientation="h",
    title="Top 10 usuarios por revenue",
)
fig_top.update_layout(yaxis={"categoryorder": "total ascending"})


def fig_html(fig, include_js=False):
    return pio.to_html(
        fig, include_plotlyjs="cdn" if include_js else False,
        full_html=False, default_height=420,
    )


def df_to_html(df, money_cols=(), pct_cols=(), int_cols=()):
    df = df.copy()
    for c in money_cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: f"${v:,.0f}")
    for c in pct_cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: f"{v*100:.1f}%")
    for c in int_cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: f"{int(v):,}")
    return df.to_html(index=False, classes="data", border=0)


html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Reporte de Transacciones | PuntoRed</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            margin: 0; padding: 32px; background: #f7f7f8; color: #1a1a1a;
            max-width: 1200px; margin: auto;
        }}
        h1 {{ color: #d62b6e; }}
        h2 {{ border-bottom: 2px solid #d62b6e; padding-bottom: 6px; margin-top: 40px; }}
        .kpis {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 20px 0; }}
        .kpi {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
        .kpi .label {{ font-size: 12px; color: #666; text-transform: uppercase; }}
        .kpi .value {{ font-size: 24px; font-weight: 600; margin-top: 4px; }}
        .insight {{ background: white; padding: 20px; border-left: 4px solid #d62b6e; border-radius: 4px; margin: 16px 0; }}
        .insight h3 {{ margin-top: 0; }}
        .reco {{ background: #fff5f8; padding: 12px; border-radius: 4px; margin-top: 8px; }}
        table.data {{ border-collapse: collapse; width: 100%; background: white; }}
        table.data th, table.data td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #eee; }}
        table.data th {{ background: #fafafa; font-weight: 600; }}
        .chart {{ background: white; border-radius: 8px; padding: 8px; margin: 16px 0; }}
        footer {{ margin-top: 60px; padding-top: 20px; border-top: 1px solid #ddd; color: #999; font-size: 12px; }}
    </style>
</head>
<body>

<h1>💳 Reporte de Transacciones — PuntoRed</h1>
<p>Análisis sobre la capa Gold del pipeline Medallion. Datos sintéticos generados con Faker.</p>

<h2>KPIs principales</h2>
<div class="kpis">
    <div class="kpi"><div class="label">Transacciones</div><div class="value">{int(overall['total_transactions']):,}</div></div>
    <div class="kpi"><div class="label">Usuarios únicos</div><div class="value">{int(overall['total_users']):,}</div></div>
    <div class="kpi"><div class="label">Monto procesado</div><div class="value">${overall['total_amount']:,.0f}</div></div>
    <div class="kpi"><div class="label">Tasa de éxito</div><div class="value">{overall['success_rate']*100:.1f}%</div></div>
    <div class="kpi"><div class="label">Ticket promedio</div><div class="value">${overall['avg_amount']:,.0f}</div></div>
    <div class="kpi"><div class="label">Revenue (success)</div><div class="value">${overall['total_amount_success']:,.0f}</div></div>
    <div class="kpi"><div class="label">Tiempo proc. promedio</div><div class="value">{overall['avg_processing_time_ms']:.0f} ms</div></div>
    <div class="kpi"><div class="label">Periodo</div><div class="value" style="font-size:14px">{pd.to_datetime(overall['first_transaction_at']).strftime('%Y-%m-%d')} → {pd.to_datetime(overall['last_transaction_at']).strftime('%Y-%m-%d')}</div></div>
</div>

<h2>📈 Tendencia temporal</h2>
<div class="chart">{fig_html(fig_volume, include_js=True)}</div>
<div class="chart">{fig_html(fig_rate)}</div>

<h2>💳 Por método de pago</h2>
<div class="chart">{fig_html(fig_pm)}</div>
{df_to_html(by_pm[['payment_method','total_transactions','total_amount','avg_ticket','success_rate','avg_processing_time_ms']],
           money_cols=['total_amount','avg_ticket'], pct_cols=['success_rate'], int_cols=['total_transactions','avg_processing_time_ms'])}

<h2>📱 Por canal</h2>
<div class="chart">{fig_html(fig_ch_time)}</div>
{df_to_html(by_ch[['channel','total_transactions','total_amount','success_rate','avg_processing_time_ms']],
           money_cols=['total_amount'], pct_cols=['success_rate'], int_cols=['total_transactions','avg_processing_time_ms'])}

<h2>🔥 Mapa de fricción: método × canal</h2>
<div class="chart">{fig_html(fig_heatmap)}</div>

<h2>👤 Top 10 usuarios por revenue</h2>
<div class="chart">{fig_html(fig_top)}</div>
{df_to_html(by_user_top[['user_name','total_transactions','revenue','avg_ticket','success_rate']],
           money_cols=['revenue','avg_ticket'], pct_cols=['success_rate'], int_cols=['total_transactions'])}

<h2>💡 Insights ejecutivos y recomendaciones</h2>

<div class="insight">
    <h3>1. Fricción puntual: {worst['payment_method']} en {worst['channel']}</h3>
    <p>La combinación con peor tasa de éxito es <strong>{worst['payment_method']} + {worst['channel']}</strong>
    con <strong>{worst['failure_rate']*100:.1f}% de rechazo</strong> sobre {int(worst['total_transactions'])} transacciones
    (vs. {(1-overall['success_rate'])*100:.1f}% de fallo global).</p>
    <div class="reco">🎯 <strong>Recomendación:</strong> auditar la integración de {worst['payment_method']} en el canal {worst['channel']} antes de la próxima campaña. Revisar logs de error específicos y compatibilidad de versiones del SDK.</div>
</div>

<div class="insight">
    <h3>2. Diferencia de performance entre canales: {slowest['channel']} vs {fastest['channel']}</h3>
    <p>El canal <strong>{slowest['channel']}</strong> procesa transacciones en <strong>{slowest['avg_processing_time_ms']:.0f} ms</strong>
    en promedio, mientras que <strong>{fastest['channel']}</strong> lo hace en <strong>{fastest['avg_processing_time_ms']:.0f} ms</strong> —
    una diferencia de <strong>{(slowest['avg_processing_time_ms']/fastest['avg_processing_time_ms']):.1f}×</strong>.</p>
    <div class="reco">🎯 <strong>Recomendación:</strong> investigar el cuello de botella en {slowest['channel']} (validaciones cliente, render de pasarela, llamadas síncronas). Bajar la latencia impacta directamente en abandono y conversión.</div>
</div>

<div class="insight">
    <h3>3. Concentración de volumen: {top_method['payment_method'].title()} = {top_method_share:.0f}% del negocio</h3>
    <p><strong>{top_method['payment_method'].title()}</strong> procesa
    <strong>${top_method['total_amount']:,.0f}</strong> ({top_method_share:.0f}% del total económico),
    con tasa de éxito {top_method['success_rate']*100:.1f}%. Riesgo de concentración:
    una caída de esta pasarela afecta casi la mitad del negocio.</p>
    <div class="reco">🎯 <strong>Recomendación:</strong> implementar fallback automático entre pasarelas. Lanzar incentivos puntuales para diversificar el mix hacia PSE y wallet.</div>
</div>

<footer>
    Generado automáticamente desde la capa Gold del pipeline · {int(overall['total_transactions']):,} transacciones procesadas ·
    última transacción: {pd.to_datetime(overall['last_transaction_at']).strftime('%Y-%m-%d')}
</footer>

</body>
</html>
"""

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
OUTPUT.write_text(html, encoding="utf-8")
print(f"Reporte generado: {OUTPUT}")
print(f"Tamaño: {OUTPUT.stat().st_size // 1024} KB")
