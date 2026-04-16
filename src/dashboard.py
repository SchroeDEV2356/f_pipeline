"""
dashboard.py — Paso 4: Visualización
======================================
Qué hace: lee datos de DuckDB y construye un dashboard
          web interactivo con Streamlit y Plotly.

Por qué Streamlit:
  - Una app web completa en Python puro: sin HTML, sin CSS, sin JS.
  - Cada vez que cambias el script, la app se recarga automáticamente.
  - Se despliega en la nube con un solo comando cuando estés listo.

Por qué Plotly (y no matplotlib):
  - Gráficos interactivos: hover, zoom, pan, descarga de imagen.
  - Plotly Express (px) genera gráficos complejos en una línea.
  - matplotlib es excelente para papers y PDFs; Plotly es para web.

Cómo correr este archivo:
  streamlit run src/dashboard.py
  (no: python src/dashboard.py — Streamlit tiene su propio runner)
"""

import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()
DB_PATH = Path(os.getenv("DB_PATH", "data/pipeline.duckdb"))


# ─────────────────────────────────────────────────────────────────────────────
# PÁGINA
# ─────────────────────────────────────────────────────────────────────────────
# st.set_page_config debe ser la PRIMERA llamada a Streamlit en el script.
# Si va después de cualquier otro st.*, Streamlit lanza un error.

st.set_page_config(
    page_title="Crypto Dashboard",
    page_icon="📊",
    layout="wide",          # usa todo el ancho de la pantalla
)

st.title("📊 Crypto Market Dashboard")
st.caption("Datos extraídos de CoinGecko API · Pipeline ETL propio")


# ─────────────────────────────────────────────────────────────────────────────
# CARGA DE DATOS
# ─────────────────────────────────────────────────────────────────────────────
# @st.cache_data es el mecanismo de caché de Streamlit.
#
# Por qué es importante:
#   Streamlit re-ejecuta el script completo cada vez que el usuario
#   interactúa con cualquier widget (un slider, un filtro, un botón).
#   Sin caché, eso significaría reconectar a DuckDB y releer todos los
#   datos en cada click — lento e innecesario.
#
#   @st.cache_data guarda el resultado de la función en memoria.
#   Solo vuelve a ejecutarla si cambian los argumentos que recibe.
#   ttl="10m" (time to live) expira la caché cada 10 minutos,
#   forzando una relectura fresca de la base de datos.

@st.cache_data(ttl="10m")
def load_data() -> pd.DataFrame:
    """
    Consulta DuckDB y devuelve el snapshot más reciente de cada moneda.

    Por qué "snapshot más reciente":
      Con el tiempo, la tabla coins_market acumulará múltiples extracciones
      de distintos momentos. Para el dashboard queremos mostrar los precios
      actuales, no el historial completo. La subconsulta con MAX(last_updated)
      filtra exactamente eso: el dato más nuevo por cada moneda.
    """
    if not DB_PATH.exists():
        return pd.DataFrame()

    con = duckdb.connect(str(DB_PATH), read_only=True)
    # read_only=True: el dashboard nunca modifica la base de datos.
    # Esto es importante cuando pipeline y dashboard corren al mismo tiempo.

    df = con.execute("""
        SELECT *
        FROM coins_market
        WHERE last_updated = (
            SELECT MAX(last_updated)
            FROM coins_market AS inner_t
            WHERE inner_t.id = coins_market.id
        )
        ORDER BY market_cap_rank ASC
    """).fetchdf()

    con.close()
    return df


# ─────────────────────────────────────────────────────────────────────────────
# GUARDADO DE ESTADO — CARGA
# ─────────────────────────────────────────────────────────────────────────────

df = load_data()

if df.empty:
    st.error(
        "No se encontró la base de datos. "
        "Ejecuta primero extract.py → transform.py → load.py."
    )
    st.stop()   # detiene la ejecución del script aquí si no hay datos


# ─────────────────────────────────────────────────────────────────────────────
# BARRA LATERAL — FILTROS
# ─────────────────────────────────────────────────────────────────────────────
# st.sidebar agrupa controles en un panel lateral.
# Cada widget devuelve el valor seleccionado por el usuario.
# Streamlit re-ejecuta el script con los nuevos valores al instante.

with st.sidebar:
    st.header("Filtros")

    top_n = st.slider(
        "Top N monedas",
        min_value=5,
        max_value=50,
        value=20,
        step=5,
        help="Número de monedas a mostrar, ordenadas por capitalización",
    )

    min_cambio, max_cambio = st.slider(
        "Cambio 24h (%)",
        min_value=float(df["price_change_percentage_24h"].min()),
        max_value=float(df["price_change_percentage_24h"].max()),
        value=(
            float(df["price_change_percentage_24h"].min()),
            float(df["price_change_percentage_24h"].max()),
        ),
        help="Filtra monedas según su variación de precio en las últimas 24h",
    )

    st.divider()
    st.caption(f"Última actualización de datos: {df['last_updated'].max()}")
    if st.button("🔄 Limpiar caché y recargar"):
        st.cache_data.clear()
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# APLICAR FILTROS
# ─────────────────────────────────────────────────────────────────────────────

df_filtered = df[
    (df["market_cap_rank"] <= top_n) &
    (df["price_change_percentage_24h"] >= min_cambio) &
    (df["price_change_percentage_24h"] <= max_cambio)
].copy()


# ─────────────────────────────────────────────────────────────────────────────
# MÉTRICAS RESUMEN
# ─────────────────────────────────────────────────────────────────────────────
# st.columns divide la fila en N columnas de igual ancho.
# st.metric muestra un número grande con un delta de cambio (↑ verde / ↓ rojo).

st.subheader("Resumen del mercado")
col1, col2, col3, col4 = st.columns(4)

with col1:
    btc = df[df["symbol"] == "btc"]
    if not btc.empty:
        precio = btc["current_price"].iloc[0]
        cambio = btc["price_change_percentage_24h"].iloc[0]
        st.metric("Bitcoin (BTC)", f"${precio:,.0f}", f"{cambio:+.2f}%")

with col2:
    eth = df[df["symbol"] == "eth"]
    if not eth.empty:
        precio = eth["current_price"].iloc[0]
        cambio = eth["price_change_percentage_24h"].iloc[0]
        st.metric("Ethereum (ETH)", f"${precio:,.2f}", f"{cambio:+.2f}%")

with col3:
    market_cap_total = df_filtered["market_cap"].sum() / 1e12
    st.metric("Market cap total", f"${market_cap_total:.2f}T")

with col4:
    ganadores = (df_filtered["price_change_percentage_24h"] > 0).sum()
    perdedores = (df_filtered["price_change_percentage_24h"] < 0).sum()
    st.metric("Ganadores / Perdedores 24h", f"{ganadores} / {perdedores}")


st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICOS
# ─────────────────────────────────────────────────────────────────────────────

col_izq, col_der = st.columns(2)

# ── Gráfico 1: Capitalización de mercado (barras horizontales) ───────────────
with col_izq:
    st.subheader("Capitalización de mercado")

    fig_cap = px.bar(
        df_filtered.sort_values("market_cap"),
        x="market_cap",
        y="symbol",
        orientation="h",
        color="price_change_percentage_24h",
        color_continuous_scale="RdYlGn",   # rojo=baja, amarillo=neutro, verde=sube
        color_continuous_midpoint=0,
        labels={
            "market_cap":                  "Market cap (USD)",
            "symbol":                      "Moneda",
            "price_change_percentage_24h": "Cambio 24h (%)",
        },
        hover_data=["name", "current_price"],
    )
    fig_cap.update_layout(
        height=450,
        margin=dict(l=0, r=0, t=10, b=0),
        coloraxis_colorbar=dict(title="24h %"),
    )
    st.plotly_chart(fig_cap, use_container_width=True)


# ── Gráfico 2: Cambio 24h (barras verticales con color) ─────────────────────
with col_der:
    st.subheader("Variación de precio 24h (%)")

    df_cambio = df_filtered.sort_values("price_change_percentage_24h")
    df_cambio["color"] = df_cambio["price_change_percentage_24h"].apply(
        lambda x: "Sube" if x >= 0 else "Baja"
    )

    fig_cambio = px.bar(
        df_cambio,
        x="symbol",
        y="price_change_percentage_24h",
        color="color",
        color_discrete_map={"Sube": "#26a641", "Baja": "#e05252"},
        labels={
            "symbol":                      "Moneda",
            "price_change_percentage_24h": "Cambio (%)",
            "color":                       "",
        },
        hover_data=["name", "current_price"],
    )
    fig_cambio.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1)
    fig_cambio.update_layout(
        height=450,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=True,
    )
    st.plotly_chart(fig_cambio, use_container_width=True)


# ── Gráfico 3: Precio vs Volumen (scatter) ───────────────────────────────────
st.subheader("Precio vs Volumen de trading (escala logarítmica)")
st.caption(
    "Cada burbuja es una moneda. "
    "Tamaño = market cap. Color = variación 24h."
)

fig_scatter = px.scatter(
    df_filtered,
    x="total_volume",
    y="current_price",
    size="market_cap",
    color="price_change_percentage_24h",
    color_continuous_scale="RdYlGn",
    color_continuous_midpoint=0,
    hover_name="name",
    hover_data={
        "symbol":                      True,
        "current_price":               ":,.4f",
        "total_volume":                ":,.0f",
        "market_cap_rank":             True,
        "price_change_percentage_24h": ":.2f",
        "market_cap":                  False,
    },
    log_x=True,    # escala logarítmica en X: muestra mejor la dispersión
    log_y=True,    # escala logarítmica en Y: bitcoin y monedas de $0.001 caben juntos
    labels={
        "total_volume":                "Volumen 24h (USD)",
        "current_price":               "Precio (USD)",
        "price_change_percentage_24h": "Cambio 24h (%)",
    },
    size_max=60,
)
fig_scatter.update_layout(
    height=500,
    margin=dict(l=0, r=0, t=10, b=0),
    coloraxis_colorbar=dict(title="24h %"),
)
st.plotly_chart(fig_scatter, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TABLA COMPLETA
# ─────────────────────────────────────────────────────────────────────────────
# st.expander agrupa contenido bajo un acordeón colapsable.
# La tabla de datos crudos es útil para depurar pero no debe dominar
# la vista — el expander la esconde hasta que el usuario la necesite.

with st.expander("Ver tabla completa de datos"):
    cols_display = [
        "market_cap_rank", "name", "symbol", "current_price",
        "market_cap", "total_volume", "price_change_percentage_24h",
        "high_24h", "low_24h",
    ]
    st.dataframe(
        df_filtered[cols_display].style.format({
            "current_price":               "${:,.4f}",
            "market_cap":                  "${:,.0f}",
            "total_volume":                "${:,.0f}",
            "price_change_percentage_24h": "{:+.2f}%",
            "high_24h":                    "${:,.4f}",
            "low_24h":                     "${:,.4f}",
        }),
        use_container_width=True,
        hide_index=True,
    )
