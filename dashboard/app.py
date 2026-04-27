"""Dashboard Streamlit que consume la capa Gold.

Ejecutar:
    streamlit run dashboard/app.py
"""

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = ROOT / "data" / "gold" / "warehouse.duckdb"


# ---------- Carga de datos (cacheada) ----------

@st.cache_data
def load_gold() -> dict[str, pd.DataFrame]:
    if not DUCKDB_PATH.exists():
        st.error(
            f"No se encuentra el warehouse en {DUCKDB_PATH}.\n"
            "Corre primero: `python main.py`"
        )
        st.stop()

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    out = {
        "overall": con.execute("SELECT * FROM gold_kpi_overall").fetchdf(),
        "by_user": con.execute("SELECT * FROM gold_kpi_by_user").fetchdf(),
        "by_payment_method": con.execute(
            "SELECT * FROM gold_kpi_by_payment_method"
        ).fetchdf(),
        "by_channel": con.execute("SELECT * FROM gold_kpi_by_channel").fetchdf(),
        "by_method_channel": con.execute(
            "SELECT * FROM gold_kpi_by_method_channel"
        ).fetchdf(),
        "by_day": con.execute(
            "SELECT * FROM gold_kpi_by_day ORDER BY transaction_date"
        ).fetchdf(),
    }
    con.close()
    return out


# ---------- Layout ----------

st.set_page_config(
    page_title="PuntoRed | Transacciones",
    page_icon="💳",
    layout="wide",
)

st.title("💳 Dashboard de Transacciones — PuntoRed")
st.caption(
    "Análisis basado en la capa Gold del pipeline Medallion. "
    "Datos sintéticos generados con Faker."
)

data = load_gold()
overall = data["overall"].iloc[0]


# ---------- KPIs principales ----------

c1, c2, c3, c4 = st.columns(4)
c1.metric("Transacciones totales", f"{overall['total_transactions']:,}")
c2.metric("Usuarios únicos", f"{overall['total_users']:,}")
c3.metric("Monto procesado", f"${overall['total_amount']:,.0f}")
c4.metric("Tasa de éxito", f"{overall['success_rate']*100:.1f}%")

c5, c6, c7, c8 = st.columns(4)
c5.metric("Ticket promedio", f"${overall['avg_amount']:,.0f}")
c6.metric("Revenue (success)", f"${overall['total_amount_success']:,.0f}")
c7.metric("Tiempo proc. promedio", f"{overall['avg_processing_time_ms']:.0f} ms")
c8.metric(
    "Periodo",
    f"{pd.to_datetime(overall['first_transaction_at']).strftime('%Y-%m-%d')} → "
    f"{pd.to_datetime(overall['last_transaction_at']).strftime('%Y-%m-%d')}",
)

st.divider()


# ---------- Tendencia temporal ----------

st.subheader("📈 Comportamiento transaccional en el tiempo")

by_day = data["by_day"].copy()
by_day["transaction_date"] = pd.to_datetime(by_day["transaction_date"])
by_day["mes"] = by_day["transaction_date"].dt.to_period("M").astype(str)
monthly = by_day.groupby("mes", as_index=False).agg(
    total_transactions=("total_transactions", "sum"),
    total_amount=("total_amount", "sum"),
    success_rate=("success_rate", "mean"),
)

col_a, col_b = st.columns(2)
with col_a:
    fig_volume = px.bar(
        monthly,
        x="mes",
        y="total_transactions",
        title="Transacciones por mes",
        labels={"total_transactions": "Transacciones", "mes": "Mes"},
    )
    st.plotly_chart(fig_volume, use_container_width=True)
with col_b:
    fig_rate = px.line(
        monthly,
        x="mes",
        y="success_rate",
        title="Tasa de éxito mensual",
        labels={"success_rate": "Tasa de éxito", "mes": "Mes"},
        markers=True,
    )
    fig_rate.update_yaxes(tickformat=".1%")
    st.plotly_chart(fig_rate, use_container_width=True)

st.divider()


# ---------- Por método de pago ----------

st.subheader("💳 Por método de pago")

by_pm = data["by_payment_method"].copy()
by_pm["success_pct"] = by_pm["success_rate"] * 100

col_a, col_b = st.columns(2)
with col_a:
    fig_pm_vol = px.bar(
        by_pm,
        x="payment_method",
        y="total_transactions",
        title="Volumen por método",
        color="payment_method",
    )
    st.plotly_chart(fig_pm_vol, use_container_width=True)
with col_b:
    fig_pm_succ = px.bar(
        by_pm.sort_values("success_pct"),
        x="payment_method",
        y="success_pct",
        title="Tasa de éxito por método (%)",
        color="success_pct",
        color_continuous_scale="RdYlGn",
    )
    st.plotly_chart(fig_pm_succ, use_container_width=True)

st.dataframe(
    by_pm[
        [
            "payment_method",
            "total_transactions",
            "total_amount",
            "avg_ticket",
            "success_rate",
            "avg_processing_time_ms",
        ]
    ].style.format(
        {
            "total_amount": "${:,.0f}",
            "avg_ticket": "${:,.0f}",
            "success_rate": "{:.1%}",
            "avg_processing_time_ms": "{:.0f}",
        }
    ),
    use_container_width=True,
)

st.divider()


# ---------- Por canal ----------

st.subheader("📱 Por canal")

by_ch = data["by_channel"].copy()
col_a, col_b = st.columns(2)
with col_a:
    fig_ch_succ = px.bar(
        by_ch,
        x="channel",
        y="success_rate",
        title="Tasa de éxito por canal",
        color="channel",
    )
    fig_ch_succ.update_yaxes(tickformat=".1%")
    st.plotly_chart(fig_ch_succ, use_container_width=True)
with col_b:
    fig_ch_time = px.bar(
        by_ch,
        x="channel",
        y="avg_processing_time_ms",
        title="Tiempo de procesamiento promedio (ms)",
        color="channel",
    )
    st.plotly_chart(fig_ch_time, use_container_width=True)

st.divider()


# ---------- Cruce método × canal (insight clave) ----------

st.subheader("🔥 Mapa de fricción: método × canal")
st.caption(
    "Identifica combinaciones método-canal con tasas de fallo elevadas. "
    "Cada celda muestra la tasa de éxito; rojo = problema."
)

pivot = data["by_method_channel"].pivot_table(
    index="payment_method",
    columns="channel",
    values="success_rate",
    aggfunc="first",
)
fig_heatmap = px.imshow(
    pivot,
    text_auto=".1%",
    color_continuous_scale="RdYlGn",
    aspect="auto",
    title="Tasa de éxito por método × canal",
    labels={"color": "Success rate"},
)
st.plotly_chart(fig_heatmap, use_container_width=True)

st.divider()


# ---------- Top usuarios ----------

st.subheader("👤 Top 10 usuarios por revenue")

top_users = (
    data["by_user"]
    .sort_values("revenue", ascending=False)
    .head(10)
    [["user_name", "total_transactions", "revenue", "avg_ticket", "success_rate"]]
)
fig_top = px.bar(
    top_users,
    x="revenue",
    y="user_name",
    orientation="h",
    title="Top 10 por revenue",
    labels={"revenue": "Revenue", "user_name": "Usuario"},
)
fig_top.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig_top, use_container_width=True)

st.dataframe(
    top_users.style.format(
        {
            "revenue": "${:,.0f}",
            "avg_ticket": "${:,.0f}",
            "success_rate": "{:.1%}",
        }
    ),
    use_container_width=True,
)

st.divider()


# ---------- Insights ejecutivos ----------

st.subheader("💡 Insights ejecutivos y recomendaciones")

# Calcular insights dinámicamente
worst_combo = data["by_method_channel"].sort_values("failure_rate", ascending=False).iloc[0]
slowest_channel = data["by_channel"].sort_values("avg_processing_time_ms", ascending=False).iloc[0]
fastest_channel = data["by_channel"].sort_values("avg_processing_time_ms", ascending=True).iloc[0]
top_method = data["by_payment_method"].sort_values("total_amount", ascending=False).iloc[0]
top_users_share = (
    data["by_user"].sort_values("revenue", ascending=False).head(50)["revenue"].sum()
    / data["by_user"]["revenue"].sum()
)

st.markdown(
    f"""
**1. Fricción puntual: {worst_combo['payment_method']} en {worst_combo['channel']}**
La combinación con peor tasa de éxito es **{worst_combo['payment_method']} + {worst_combo['channel']}** con
**{worst_combo['failure_rate']*100:.1f}% de rechazo** sobre {int(worst_combo['total_transactions'])} transacciones.
> 🎯 **Recomendación:** Auditar la integración de la pasarela de **{worst_combo['payment_method']}** en
> el canal **{worst_combo['channel']}** antes de la próxima campaña. Revisar logs de error y
> tasas de timeout específicas de esa combinación.

**2. Diferencia de performance entre canales: {slowest_channel['channel']} vs {fastest_channel['channel']}**
El canal **{slowest_channel['channel']}** procesa transacciones en **{slowest_channel['avg_processing_time_ms']:.0f} ms** en promedio,
mientras que **{fastest_channel['channel']}** lo hace en **{fastest_channel['avg_processing_time_ms']:.0f} ms** —
una diferencia de **{(slowest_channel['avg_processing_time_ms']/fastest_channel['avg_processing_time_ms']):.1f}x**.
> 🎯 **Recomendación:** Investigar por qué el canal **{slowest_channel['channel']}** es tan lento.
> Probable cuello de botella en la capa de presentación o validaciones del lado cliente.
> Mejorarlo impacta directamente en abandono y conversión.

**3. Concentración de revenue: top 50 usuarios = {top_users_share*100:.0f}% del negocio**
Los **50 usuarios más valiosos representan el {top_users_share*100:.0f}%** del revenue total.
El método dominante es **{top_method['payment_method']}** con
${top_method['total_amount']:,.0f} procesados.
> 🎯 **Recomendación:** Diseñar un programa de fidelización para el top 10% de usuarios
> (cashback, soporte prioritario). Defender este segmento es defender la mayoría del revenue.
"""
)

st.caption(
    f"Pipeline run sobre {overall['total_transactions']:,} transacciones · "
    f"última transacción: {pd.to_datetime(overall['last_transaction_at']).strftime('%Y-%m-%d')}"
)
