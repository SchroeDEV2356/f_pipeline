"""
dashboard.py — Empleo Formal en México (IMSS ASG 2019–2025)
=============================================================
Fuente: Datos Abiertos IMSS — Asegurados por sector económico
Cobertura: enero 2019 – diciembre 2025 (84 meses, 9 sectores)
"""

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR", "data/processed"))
PARQUET_PATH  = PROCESSED_DIR / "imss_asg_historico.parquet"

# ─────────────────────────────────────────────────────────────────────────────
# PÁGINA
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Empleo Formal México — IMSS",
    page_icon="🇲🇽",
    layout="wide",
)

st.title("🇲🇽 Empleo Formal en México")
st.caption(
    "Puestos de trabajo registrados ante el IMSS · 2019–2025 · "
    "Fuente: [Datos Abiertos IMSS](http://datos.imss.gob.mx)"
)


# ─────────────────────────────────────────────────────────────────────────────
# CARGA DE DATOS
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl="1h")
def load_data() -> pd.DataFrame:
    if not PARQUET_PATH.exists():
        return pd.DataFrame()
    df = pd.read_parquet(PARQUET_PATH)
    df["periodo"] = pd.to_datetime(df["periodo"])

    # Calcular eventuales y permanentes totales
    df["eventuales"]  = df["teu"] + df["tec"]
    df["permanentes"] = df["tpu"] + df["tpc"]

    # Ratio de eventualidad: qué % del empleo es eventual
    df["pct_eventual"] = (df["eventuales"] / df["ta"].replace(0, pd.NA) * 100).round(1)

    return df.sort_values("periodo")


df = load_data()

if df.empty:
    st.error(
        "No se encontró el archivo de datos. "
        "Ejecuta primero `download_and_process_asg.py 2019 2025`."
    )
    st.stop()

# Total nacional por período (suma de todos los sectores)
df_nacional = (
    df.groupby("periodo")[["asegurados", "ta", "eventuales", "permanentes", "masa_sal_ta"]]
    .sum()
    .reset_index()
)

# Valores de referencia para deltas
ultimo_mes   = df_nacional["periodo"].max()
penultimo    = df_nacional[df_nacional["periodo"] < ultimo_mes]["periodo"].max()
val_actual   = df_nacional[df_nacional["periodo"] == ultimo_mes]["asegurados"].iloc[0]
val_anterior = df_nacional[df_nacional["periodo"] == penultimo]["asegurados"].iloc[0]
val_2019     = df_nacional[df_nacional["periodo"] == df_nacional["periodo"].min()]["asegurados"].iloc[0]

# Sectores disponibles para filtros
sectores = sorted(df["sector_nombre"].unique())

# Período de la pandemia para anotaciones
PANDEMIA_INICIO = pd.Timestamp("2020-03-01")
PANDEMIA_FIN    = pd.Timestamp("2022-09-01")


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filtros")

    anios_disponibles = sorted(df["anio"].unique())
    anio_inicio, anio_fin = st.select_slider(
        "Rango de años",
        options=anios_disponibles,
        value=(anios_disponibles[0], anios_disponibles[-1]),
    )

    sectores_sel = st.multiselect(
        "Sectores a comparar",
        options=sectores,
        default=[
            "Industria de la construcción",
            "Servicios sociales y comunales",
            "Transportes y comunicaciones",
            "Servicios a empresas y personas",
            "Comercio",
        ],
    )

    mostrar_pandemia = st.toggle("Marcar período de pandemia", value=True)

    st.divider()
    st.caption(f"Último dato disponible: {ultimo_mes.strftime('%B %Y')}")
    st.caption(f"Fuente: Datos Abiertos IMSS · ASG")
    if st.button("🔄 Recargar datos"):
        st.cache_data.clear()
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# FILTRAR POR RANGO DE AÑOS
# ─────────────────────────────────────────────────────────────────────────────

df_f = df[(df["anio"] >= anio_inicio) & (df["anio"] <= anio_fin)]
df_nac_f = df_nacional[
    (df_nacional["periodo"].dt.year >= anio_inicio) &
    (df_nacional["periodo"].dt.year <= anio_fin)
]


# ─────────────────────────────────────────────────────────────────────────────
# MÉTRICAS
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Panorama nacional")
col1, col2, col3, col4 = st.columns(4)

with col1:
    delta_mes = val_actual - val_anterior
    st.metric(
        "Asegurados IMSS",
        f"{val_actual/1e6:.2f}M",
        f"{delta_mes:+,.0f} vs mes anterior",
    )
with col2:
    crecimiento = ((val_actual - val_2019) / val_2019 * 100)
    st.metric(
        "Crecimiento desde 2019",
        f"{crecimiento:+.1f}%",
        f"{(val_actual - val_2019)/1e6:+.2f}M puestos",
    )
with col3:
    perm_pct = df_f.groupby("periodo")["permanentes"].sum().iloc[-1] / \
               df_f.groupby("periodo")["ta"].sum().iloc[-1] * 100
    st.metric("Empleo permanente", f"{perm_pct:.1f}%", "del total registrado")
with col4:
    caida_pandemia = df_nacional[
        df_nacional["periodo"] == PANDEMIA_FIN
    ]["asegurados"].iloc[0] if not df_nacional[df_nacional["periodo"] == PANDEMIA_FIN].empty else 0
    perdida = val_2019 - caida_pandemia if caida_pandemia else 0
    st.metric(
        "Pérdida en pandemia",
        f"{perdida/1e6:.2f}M puestos",
        "julio 2020 vs enero 2019",
        delta_color="inverse",
    )

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICO 1 — EMPLEO TOTAL NACIONAL EN EL TIEMPO
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Empleo formal total — serie histórica")
st.caption("Suma de todos los sectores · puestos de trabajo registrados ante el IMSS")

fig_total = go.Figure()

fig_total.add_trace(go.Scatter(
    x=df_nac_f["periodo"],
    y=df_nac_f["asegurados"],
    mode="lines",
    line=dict(color="#1f77b4", width=2.5),
    name="Asegurados totales",
    hovertemplate="%{x|%b %Y}<br><b>%{y:,.0f}</b> asegurados<extra></extra>",
))

if mostrar_pandemia:
    fig_total.add_vrect(
        x0=PANDEMIA_INICIO, x1=PANDEMIA_FIN,
        fillcolor="rgba(255,100,100,0.12)",
        line_width=0,
    )
    fig_total.add_annotation(
        x=pd.Timestamp("2021-06-01"),
        y=1, yref="paper",
        text="Pandemia",
        showarrow=False,
        font=dict(size=11, color="#cc3333"),
        yanchor="top",
    )
fig_total.update_yaxes(tickformat=",.0f")
st.plotly_chart(fig_total, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICO 2 — EMPLEO POR SECTOR EN EL TIEMPO
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Empleo por sector económico")

if not sectores_sel:
    st.info("Selecciona al menos un sector en el panel lateral.")
else:
    df_sect = df_f[df_f["sector_nombre"].isin(sectores_sel)]

    fig_sect = px.line(
        df_sect,
        x="periodo",
        y="asegurados",
        color="sector_nombre",
        labels={
            "periodo":       "Período",
            "asegurados":    "Asegurados",
            "sector_nombre": "Sector",
        },
        hover_data={"ta": True, "permanentes": True, "eventuales": True},
    )

    if mostrar_pandemia:
        fig_sect.add_vrect(
        x0=PANDEMIA_INICIO, x1=PANDEMIA_FIN,
        fillcolor="rgba(255,100,100,0.10)",
        line_width=0,
    )
    fig_sect.add_annotation(
        x=pd.Timestamp("2021-06-01"),
        y=1, yref="paper",
        text="Pandemia",
        showarrow=False,
        font=dict(size=11, color="#cc3333"),
        yanchor="top",
    )

    fig_sect.update_layout(
        height=420,
        margin=dict(l=0, r=0, t=10, b=0),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        yaxis_tickformat=",.0f",
    )
    fig_sect.update_traces(line_width=2)
    st.plotly_chart(fig_sect, use_container_width=True)

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICO 3 — CRECIMIENTO POR SECTOR (2019 vs último mes)
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Crecimiento de empleo por sector")
st.caption(f"Comparación enero 2019 vs {ultimo_mes.strftime('%B %Y')}")

base  = df[df["periodo"] == df["periodo"].min()][["sector_nombre", "asegurados"]].set_index("sector_nombre")
final = df[df["periodo"] == ultimo_mes][["sector_nombre", "asegurados"]].set_index("sector_nombre")

df_crec = base.join(final, lsuffix="_2019", rsuffix="_actual")
df_crec["cambio_pct"] = ((df_crec["asegurados_actual"] - df_crec["asegurados_2019"]) /
                          df_crec["asegurados_2019"] * 100).round(1)
df_crec["cambio_abs"] = (df_crec["asegurados_actual"] - df_crec["asegurados_2019"])
df_crec["color"]      = df_crec["cambio_pct"].apply(lambda x: "Creció" if x >= 0 else "Cayó")
df_crec = df_crec.reset_index().sort_values("cambio_pct")

fig_crec = px.bar(
    df_crec,
    x="cambio_pct",
    y="sector_nombre",
    orientation="h",
    color="color",
    color_discrete_map={"Creció": "#26a641", "Cayó": "#e05252"},
    text="cambio_pct",
    labels={
        "cambio_pct":    "Variación (%)",
        "sector_nombre": "Sector",
        "color":         "",
    },
    hover_data={"cambio_abs": True, "asegurados_2019": True, "asegurados_actual": True},
)
fig_crec.update_traces(texttemplate="%{text:+.1f}%", textposition="outside")
fig_crec.add_vline(x=0, line_dash="dash", line_color="gray", line_width=1)
fig_crec.update_layout(
    height=380,
    margin=dict(l=0, r=10, t=10, b=0),
    showlegend=False,
    xaxis_title="Variación porcentual (%)",
)
st.plotly_chart(fig_crec, use_container_width=True)

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICO 4 — COMPOSICIÓN: PERMANENTES VS EVENTUALES
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Calidad del empleo — permanentes vs eventuales")
st.caption("Porcentaje de puestos eventuales sobre el total por sector · último mes disponible")

df_comp = df[df["periodo"] == ultimo_mes][
    ["sector_nombre", "permanentes", "eventuales", "ta", "pct_eventual"]
].sort_values("pct_eventual", ascending=False)

col_a, col_b = st.columns(2)

with col_a:
    fig_comp = px.bar(
        df_comp,
        x="sector_nombre",
        y=["permanentes", "eventuales"],
        barmode="stack",
        labels={
            "sector_nombre": "Sector",
            "value":         "Puestos",
            "variable":      "Tipo",
        },
        color_discrete_map={
            "permanentes": "#1f77b4",
            "eventuales":  "#ff7f0e",
        },
    )
    fig_comp.update_layout(
        height=380,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis_tickangle=-35,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        yaxis_tickformat=",.0f",
    )
    st.plotly_chart(fig_comp, use_container_width=True)

with col_b:
    fig_pct = px.bar(
        df_comp.sort_values("pct_eventual"),
        x="pct_eventual",
        y="sector_nombre",
        orientation="h",
        color="pct_eventual",
        color_continuous_scale="RdYlGn_r",  # rojo = más eventual (más precario)
        text="pct_eventual",
        labels={
            "pct_eventual":  "% eventual",
            "sector_nombre": "Sector",
        },
    )
    fig_pct.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig_pct.update_layout(
        height=380,
        margin=dict(l=0, r=10, t=10, b=0),
        coloraxis_showscale=False,
        xaxis_title="% de empleo eventual",
    )
    st.plotly_chart(fig_pct, use_container_width=True)

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICO 5 — MASA SALARIAL POR SECTOR
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Masa salarial por sector")
st.caption("Salario base de cotización acumulado · último mes disponible")

df_sal = df[df["periodo"] == ultimo_mes][["sector_nombre", "masa_sal_ta"]].sort_values(
    "masa_sal_ta", ascending=False
)

fig_sal = px.bar(
    df_sal,
    x="sector_nombre",
    y="masa_sal_ta",
    color="masa_sal_ta",
    color_continuous_scale="Blues",
    labels={
        "sector_nombre": "Sector",
        "masa_sal_ta":   "Masa salarial (pesos)",
    },
)
fig_sal.update_layout(
    height=360,
    margin=dict(l=0, r=0, t=10, b=0),
    xaxis_tickangle=-35,
    coloraxis_showscale=False,
    yaxis_tickformat=",.0f",
)
st.plotly_chart(fig_sal, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TABLA DE DATOS
# ─────────────────────────────────────────────────────────────────────────────

with st.expander("Ver tabla completa de datos"):
    df_tabla = df[df["periodo"] == ultimo_mes][[
        "sector_nombre", "asegurados", "ta", "permanentes",
        "eventuales", "pct_eventual", "masa_sal_ta"
    ]].sort_values("asegurados", ascending=False)

    st.dataframe(
        df_tabla.style.format({
            "asegurados":   "{:,.0f}",
            "ta":           "{:,.0f}",
            "permanentes":  "{:,.0f}",
            "eventuales":   "{:,.0f}",
            "pct_eventual": "{:.1f}%",
            "masa_sal_ta":  "${:,.0f}",
        }),
        use_container_width=True,
        hide_index=True,
    )