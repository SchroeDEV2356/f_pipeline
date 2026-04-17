"""
dashboard.py — con tendencias históricas
"""

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
DB_PATH       = Path(os.getenv("DB_PATH",            "data/pipeline.duckdb"))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR", "data/processed"))


# ─────────────────────────────────────────────────────────────────────────────
# CARGA DE DATOS — DOS CONSULTAS SEPARADAS
# ─────────────────────────────────────────────────────────────────────────────
# Por qué dos funciones y no una:
#   - load_snapshot(): solo el dato más reciente por moneda → métricas y barras
#   - load_history():  todos los registros históricos     → gráficos de tendencia
#   Cada una tiene su propio ttl de caché porque tienen propósitos distintos.

@st.cache_data(ttl="10m")
def load_snapshot() -> pd.DataFrame:
    """Snapshot más reciente de cada moneda."""
    if DB_PATH.exists():
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=True)
        df = con.execute("""
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

    parquet_files = sorted(PROCESSED_DIR.glob("coins_clean_*.parquet"))
    if not parquet_files:
        return pd.DataFrame()
    return pd.read_parquet(parquet_files[-1]).sort_values("market_cap_rank").reset_index(drop=True)


@st.cache_data(ttl="10m")
def load_history() -> pd.DataFrame:
    """
    Todos los registros históricos para gráficos de tendencia.

    En local: lee toda la tabla DuckDB (múltiples extracciones acumuladas).
    En cloud: concatena todos los Parquet commiteados por GitHub Actions.

    Por qué concatenar todos los Parquet en cloud:
      Cada ejecución del workflow commitea un Parquet nuevo al repo.
      Con el tiempo se acumula el historial. Al leer todos y concatenarlos
      obtenemos la misma tabla histórica que DuckDB tiene localmente.
    """
    if DB_PATH.exists():
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=True)
        df = con.execute("""
            SELECT id, symbol, name, current_price, market_cap,
                   market_cap_rank, total_volume, price_change_percentage_24h,
                   last_updated
            FROM coins_market
            ORDER BY last_updated ASC
        """).fetchdf()
        con.close()
        return df

    parquet_files = sorted(PROCESSED_DIR.glob("coins_clean_*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    cols = ["id", "symbol", "name", "current_price", "market_cap",
            "market_cap_rank", "total_volume", "price_change_percentage_24h",
            "last_updated"]
    frames = [pd.read_parquet(f, columns=cols) for f in parquet_files]
    df = pd.concat(frames, ignore_index=True)

    # Deduplicar por si el mismo snapshot fue commiteado más de una vez
    df = df.drop_duplicates(subset=["id", "last_updated"])
    return df.sort_values("last_updated").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# PÁGINA
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Crypto Dashboard", page_icon="📊", layout="wide")
st.title("📊 Crypto Market Dashboard")
st.caption("Datos extraídos de CoinGecko API · Pipeline ETL propio")

df_snap = load_snapshot()
df_hist = load_history()

if df_snap.empty:
    st.error("No se encontraron datos. Ejecuta el pipeline o espera a que GitHub Actions lo corra.")
    st.stop()

# Número de snapshots distintos disponibles para el historial
n_snapshots = df_hist["last_updated"].nunique() if not df_hist.empty else 0


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filtros")

    top_n = st.slider("Top N monedas", 5, 50, 20, 5)

    min_cambio, max_cambio = st.slider(
        "Cambio 24h (%)",
        min_value=float(df_snap["price_change_percentage_24h"].min()),
        max_value=float(df_snap["price_change_percentage_24h"].max()),
        value=(
            float(df_snap["price_change_percentage_24h"].min()),
            float(df_snap["price_change_percentage_24h"].max()),
        ),
    )

    st.divider()
    st.caption(f"Última actualización: {df_snap['last_updated'].max()}")
    st.caption(f"Snapshots en historial: {n_snapshots}")
    if st.button("🔄 Recargar datos"):
        st.cache_data.clear()
        st.rerun()

df_f = df_snap[
    (df_snap["market_cap_rank"] <= top_n) &
    (df_snap["price_change_percentage_24h"] >= min_cambio) &
    (df_snap["price_change_percentage_24h"] <= max_cambio)
].copy()


# ─────────────────────────────────────────────────────────────────────────────
# MÉTRICAS
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Resumen del mercado")
col1, col2, col3, col4 = st.columns(4)

with col1:
    btc = df_snap[df_snap["symbol"] == "btc"]
    if not btc.empty:
        st.metric("Bitcoin (BTC)",
                  f"${btc['current_price'].iloc[0]:,.0f}",
                  f"{btc['price_change_percentage_24h'].iloc[0]:+.2f}%")
with col2:
    eth = df_snap[df_snap["symbol"] == "eth"]
    if not eth.empty:
        st.metric("Ethereum (ETH)",
                  f"${eth['current_price'].iloc[0]:,.2f}",
                  f"{eth['price_change_percentage_24h'].iloc[0]:+.2f}%")
with col3:
    st.metric("Market cap total", f"${df_f['market_cap'].sum() / 1e12:.2f}T")
with col4:
    g = (df_f["price_change_percentage_24h"] > 0).sum()
    p = (df_f["price_change_percentage_24h"] < 0).sum()
    st.metric("Ganadores / Perdedores 24h", f"{g} / {p}")

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# SECCIÓN 1 — TENDENCIAS HISTÓRICAS
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("📈 Tendencias históricas")

if n_snapshots < 2:
    st.info(
        f"Solo hay {n_snapshots} snapshot en el historial. "
        "Las tendencias aparecen a partir de 2 ejecuciones del pipeline. "
        "GitHub Actions añade uno nuevo cada 6 horas."
    )
else:
    # Selector de monedas para el gráfico de tendencias
    # Por defecto: BTC, ETH y las 3 siguientes por capitalización
    top5_symbols = df_snap.head(5)["symbol"].tolist()
    all_symbols  = df_snap["symbol"].tolist()

    monedas_seleccionadas = st.multiselect(
        "Monedas a comparar",
        options=all_symbols,
        default=top5_symbols,
        help="Selecciona las monedas que quieres ver en los gráficos de tendencia",
    )

    if monedas_seleccionadas:
        df_trend = df_hist[df_hist["symbol"].isin(monedas_seleccionadas)].copy()

        # ── Precio en el tiempo ───────────────────────────────────────────────
        st.markdown("**Evolución del precio (USD)**")
        fig_precio = px.line(
            df_trend,
            x="last_updated",
            y="current_price",
            color="name",
            labels={
                "last_updated":  "Fecha / hora",
                "current_price": "Precio (USD)",
                "name":          "Moneda",
            },
            hover_data={"symbol": True, "current_price": ":,.4f"},
        )
        fig_precio.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=10, b=0),
            hovermode="x unified",   # muestra todas las monedas al mismo tiempo al hacer hover
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        fig_precio.update_traces(line_width=2)
        st.plotly_chart(fig_precio, use_container_width=True)

        # ── Volumen en el tiempo ──────────────────────────────────────────────
        col_vol, col_rank = st.columns(2)

        with col_vol:
            st.markdown("**Volumen de trading 24h**")
            fig_vol = px.line(
                df_trend,
                x="last_updated",
                y="total_volume",
                color="name",
                labels={
                    "last_updated": "Fecha / hora",
                    "total_volume": "Volumen USD",
                    "name":         "Moneda",
                },
            )
            fig_vol.update_layout(
                height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False,
                hovermode="x unified",
            )
            st.plotly_chart(fig_vol, use_container_width=True)

        with col_rank:
            # ── Cambio de ranking en el tiempo ───────────────────────────────
            # Por qué invertir el eje Y:
            #   Rank 1 es el mejor — visualmente queremos que subir en
            #   el ranking se vea como subir en el gráfico, no bajar.
            st.markdown("**Posición en el ranking (market cap)**")
            fig_rank = px.line(
                df_trend,
                x="last_updated",
                y="market_cap_rank",
                color="name",
                labels={
                    "last_updated":   "Fecha / hora",
                    "market_cap_rank": "Ranking",
                    "name":            "Moneda",
                },
            )
            fig_rank.update_yaxes(autorange="reversed")
            fig_rank.update_layout(
                height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False,
                hovermode="x unified",
            )
            st.plotly_chart(fig_rank, use_container_width=True)

        # ── Variación porcentual acumulada ────────────────────────────────────
        # Por qué variación acumulada y no precio absoluto:
        #   Permite comparar monedas de distinto precio en la misma escala.
        #   Bitcoin a $75,000 y una moneda a $0.01 son incomparables en precio,
        #   pero su variación % desde el inicio del periodo sí es comparable.
        st.markdown("**Variación acumulada desde el primer registro (%)**")

        frames_norm = []
        for sym in monedas_seleccionadas:
            sub = df_trend[df_trend["symbol"] == sym].sort_values("last_updated").copy()
            if len(sub) < 2:
                continue
            precio_base = sub["current_price"].iloc[0]
            if precio_base and precio_base != 0:
                sub["variacion_acumulada"] = (sub["current_price"] / precio_base - 1) * 100
                frames_norm.append(sub)

        if frames_norm:
            df_norm = pd.concat(frames_norm, ignore_index=True)
            fig_norm = px.line(
                df_norm,
                x="last_updated",
                y="variacion_acumulada",
                color="name",
                labels={
                    "last_updated":       "Fecha / hora",
                    "variacion_acumulada": "Variación acumulada (%)",
                    "name":               "Moneda",
                },
            )
            fig_norm.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1)
            fig_norm.update_layout(
                height=350,
                margin=dict(l=0, r=0, t=10, b=0),
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_norm, use_container_width=True)

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# SECCIÓN 2 — SNAPSHOT ACTUAL (igual que antes)
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("📊 Snapshot actual")

col_izq, col_der = st.columns(2)

with col_izq:
    st.markdown("**Capitalización de mercado**")
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
    st.markdown("**Variación de precio 24h (%)**")
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

st.markdown("**Precio vs Volumen (escala logarítmica)**")
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

with st.expander("Ver tabla completa"):
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