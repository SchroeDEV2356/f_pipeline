"""
dashboard.py — Paso 4: Visualización (versión cloud-compatible)
================================================================
Estrategia de datos dual:
  - Local:          lee de data/pipeline.duckdb  (generado por load.py)
  - Streamlit Cloud: lee de data/processed/*.parquet (commiteado en el repo)

Por qué esta dualidad:
  Streamlit Cloud no tiene acceso a tu máquina local. Cuando corre
  el dashboard en la nube, no existe pipeline.duckdb. Pero sí puede
  leer archivos del repositorio — incluido el Parquet que GitHub Actions
  genera y commitea automáticamente en cada ejecución del pipeline.
"""

import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv


load_dotenv()
DB_PATH       = Path(os.getenv("DB_PATH",            "data/pipeline.duckdb"))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR", "data/processed"))


# ─────────────────────────────────────────────────────────────────────────────
# CARGA DE DATOS — ESTRATEGIA DUAL
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl="10m")
def load_data() -> pd.DataFrame:
    """
    Intenta leer de DuckDB primero (entorno local con pipeline completo).
    Si no existe, lee el Parquet más reciente (entorno Streamlit Cloud).
    """
    if DB_PATH.exists():
        # ── Entorno local ─────────────────────────────────────────────────
        con = duckdb.connect(str(DB_PATH), read_only=True)
        df  = con.execute("""
            SELECT *
            FROM coins_market
            WHERE last_updated = (
                SELECT MAX(last_updated)
                FROM coins_market AS t
                WHERE t.id = coins_market.id
            )
            ORDER BY market_cap_rank ASC
        """).fetchdf()
        con.close()
        return df

    # ── Entorno cloud (Streamlit Cloud) ───────────────────────────────────
    # Lee el Parquet más reciente commiteado por GitHub Actions.
    parquet_files = sorted(PROCESSED_DIR.glob("coins_clean_*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    df = pd.read_parquet(parquet_files[-1])
    # En el Parquet ya están los datos del snapshot más reciente;
    # no necesitamos filtrar por last_updated como en DuckDB.
    return df.sort_values("market_cap_rank").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# PÁGINA
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Crypto Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Crypto Market Dashboard")
st.caption("Datos extraídos de CoinGecko API · Pipeline ETL propio")

df = load_data()

if df.empty:
    st.error(
        "No se encontraron datos. "
        "Ejecuta el pipeline localmente o espera a que GitHub Actions lo corra."
    )
    st.stop()


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filtros")

    top_n = st.slider("Top N monedas", 5, 50, 20, 5)

    min_cambio, max_cambio = st.slider(
        "Cambio 24h (%)",
        min_value=float(df["price_change_percentage_24h"].min()),
        max_value=float(df["price_change_percentage_24h"].max()),
        value=(
            float(df["price_change_percentage_24h"].min()),
            float(df["price_change_percentage_24h"].max()),
        ),
    )

    st.divider()
    st.caption(f"Última actualización: {df['last_updated'].max()}")
    if st.button("🔄 Recargar datos"):
        st.cache_data.clear()
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# FILTRADO
# ─────────────────────────────────────────────────────────────────────────────

df_f = df[
    (df["market_cap_rank"] <= top_n) &
    (df["price_change_percentage_24h"] >= min_cambio) &
    (df["price_change_percentage_24h"] <= max_cambio)
].copy()


# ─────────────────────────────────────────────────────────────────────────────
# MÉTRICAS
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Resumen del mercado")
col1, col2, col3, col4 = st.columns(4)

with col1:
    btc = df[df["symbol"] == "btc"]
    if not btc.empty:
        st.metric("Bitcoin (BTC)",
                  f"${btc['current_price'].iloc[0]:,.0f}",
                  f"{btc['price_change_percentage_24h'].iloc[0]:+.2f}%")

with col2:
    eth = df[df["symbol"] == "eth"]
    if not eth.empty:
        st.metric("Ethereum (ETH)",
                  f"${eth['current_price'].iloc[0]:,.2f}",
                  f"{eth['price_change_percentage_24h'].iloc[0]:+.2f}%")

with col3:
    st.metric("Market cap total",
              f"${df_f['market_cap'].sum() / 1e12:.2f}T")

with col4:
    g = (df_f["price_change_percentage_24h"] > 0).sum()
    p = (df_f["price_change_percentage_24h"] < 0).sum()
    st.metric("Ganadores / Perdedores 24h", f"{g} / {p}")

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICOS
# ─────────────────────────────────────────────────────────────────────────────

col_izq, col_der = st.columns(2)

with col_izq:
    st.subheader("Capitalización de mercado")
    fig_cap = px.bar(
        df_f.sort_values("market_cap"),
        x="market_cap", y="symbol", orientation="h",
        color="price_change_percentage_24h",
        color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
        labels={"market_cap": "Market cap (USD)", "symbol": "Moneda",
                "price_change_percentage_24h": "Cambio 24h (%)"},
        hover_data=["name", "current_price"],
    )
    fig_cap.update_layout(height=450, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig_cap, use_container_width=True)

with col_der:
    st.subheader("Variación de precio 24h (%)")
    df_f["color"] = df_f["price_change_percentage_24h"].apply(
        lambda x: "Sube" if x >= 0 else "Baja"
    )
    fig_cambio = px.bar(
        df_f.sort_values("price_change_percentage_24h"),
        x="symbol", y="price_change_percentage_24h",
        color="color",
        color_discrete_map={"Sube": "#26a641", "Baja": "#e05252"},
        labels={"symbol": "Moneda", "price_change_percentage_24h": "Cambio (%)", "color": ""},
        hover_data=["name", "current_price"],
    )
    fig_cambio.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1)
    fig_cambio.update_layout(height=450, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig_cambio, use_container_width=True)

st.subheader("Precio vs Volumen de trading (escala logarítmica)")
st.caption("Tamaño de burbuja = market cap · Color = variación 24h")
fig_scatter = px.scatter(
    df_f, x="total_volume", y="current_price",
    size="market_cap", color="price_change_percentage_24h",
    color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
    hover_name="name",
    hover_data={"symbol": True, "current_price": ":,.4f",
                "total_volume": ":,.0f", "market_cap_rank": True,
                "price_change_percentage_24h": ":.2f", "market_cap": False},
    log_x=True, log_y=True, size_max=60,
    labels={"total_volume": "Volumen 24h (USD)", "current_price": "Precio (USD)",
            "price_change_percentage_24h": "Cambio 24h (%)"},
)
fig_scatter.update_layout(height=500, margin=dict(l=0, r=0, t=10, b=0))
st.plotly_chart(fig_scatter, use_container_width=True)

with st.expander("Ver tabla completa de datos"):
    cols = ["market_cap_rank", "name", "symbol", "current_price",
            "market_cap", "total_volume", "price_change_percentage_24h",
            "high_24h", "low_24h"]
    st.dataframe(
        df_f[cols].style.format({
            "current_price":               "${:,.4f}",
            "market_cap":                  "${:,.0f}",
            "total_volume":                "${:,.0f}",
            "price_change_percentage_24h": "{:+.2f}%",
            "high_24h":                    "${:,.4f}",
            "low_24h":                     "${:,.4f}",
        }),
        use_container_width=True, hide_index=True,
    )