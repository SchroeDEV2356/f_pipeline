"""
process_imss_asg.py — Procesador de archivos ASG del IMSS
==========================================================
USO:
    python process_imss_asg.py ruta/al/asg-2024-01-31.csv

QUÉ HACE:
    1. Lee el CSV original (puede pesar ~500 MB)
    2. Extrae el año y mes del nombre del archivo
    3. Agrega los 4.6 millones de filas a ~9 filas (una por sector)
    4. Añade esas filas al Parquet histórico acumulado
    5. Te dice cuántas filas tiene el acumulado ahora

    El CSV original NO se borra automáticamente — el script solo avisa
    cuando terminó para que tú lo borres manualmente. Así tienes control
    total sobre qué archivo eliminar y cuándo.

RESULTADO:
    data/processed/imss_asg_historico.parquet
    (crece con cada CSV que procesas; pesa KB, no GB)
"""

import sys
import logging
from pathlib import Path
import pandas as pd
import re

# ─────────────────────────────────────────────────────────────────────────────
# CATÁLOGO DE SECTORES
# ─────────────────────────────────────────────────────────────────────────────
# El IMSS usa códigos numéricos de 1 dígito para las divisiones económicas.
# Este catálogo viene del Reglamento de Clasificación de Empresas del IMSS
# (Art. 9 del Reglamento de la Ley del Seguro Social en materia de Afiliación).
#
# Por qué incluirlo aquí y no en un CSV aparte:
#   Son solo 10 valores que no cambian — un catálogo externo añade
#   complejidad sin beneficio real para un conjunto tan pequeño.

CATALOGO_SECTORES = {
    0: "Agricultura, ganadería y pesca",
    1: "Industrias extractivas",
    2: "Industrias de transformación",
    3: "Industria de la construcción",
    4: "Electricidad y agua",
    5: "Comercio",
    6: "Transportes y comunicaciones",
    7: "Servicios a empresas y personas",
    8: "Servicios sociales y comunales",
    9: "No especificado",
}


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# EXTRAER FECHA DEL NOMBRE DEL ARCHIVO
# ─────────────────────────────────────────────────────────────────────────────

def extraer_periodo(filepath: Path) -> tuple[int, int]:
    """
    Del nombre 'asg-2024-01-31.csv' extrae (2024, 1).

    Por qué extraer del nombre y no del contenido:
      El CSV no tiene una columna de fecha — cada archivo representa
      el último día del mes indicado en su nombre. El nombre ES la fecha.
    """
    match = re.search(r"asg-(\d{4})-(\d{2})-\d{2}", filepath.name)
    if not match:
        raise ValueError(
            f"El nombre '{filepath.name}' no tiene el formato esperado "
            "'asg-YYYY-MM-DD.csv'"
        )
    anio, mes = int(match.group(1)), int(match.group(2))
    log.info("Período detectado: %d-%02d", anio, mes)
    return anio, mes


# ─────────────────────────────────────────────────────────────────────────────
# LEER Y AGREGAR
# ─────────────────────────────────────────────────────────────────────────────

def leer_y_agregar(filepath: Path, anio: int, mes: int) -> pd.DataFrame:
    """
    Lee el CSV completo y lo reduce a una fila por sector económico.

    Por qué low_memory=False:
      El CSV tiene columnas con tipos mixtos (números y 'NA' como texto).
      Sin este parámetro pandas lanza warnings y puede inferir tipos
      incorrectos al leer en chunks.

    Por qué usecols:
      Leer solo las columnas necesarias reduce el uso de memoria
      de ~3 GB a ~200 MB. El resto de columnas se descarta al vuelo.

    groupby('sector_economico_1').sum() colapsa 4.6M filas a ~9 filas
    sumando todas las métricas dentro de cada sector.
    """
    log.info("Leyendo %s ...", filepath.name)

    cols_usar = [
        "sector_economico_1",
        "asegurados",
        "ta",          # puestos de trabajo afiliados total
        "teu",         # eventuales urbanos
        "tec",         # eventuales del campo
        "tpu",         # permanentes urbanos
        "tpc",         # permanentes del campo
        "masa_sal_ta", # masa salarial total
    ]

    df = pd.read_csv(
        filepath,
        sep="|",
        encoding="latin1",
        low_memory=False,
        usecols=cols_usar,
    )
    log.info("Archivo leído: %d filas, %d columnas", len(df), len(df.columns))

    # Convertir a numérico — algunos valores vienen como 'NA' texto
    for col in cols_usar[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Descartar filas sin sector
    df = df.dropna(subset=["sector_economico_1"])
    df["sector_economico_1"] = df["sector_economico_1"].astype(int)

    # Agregar: una fila por sector, sumando métricas
    agg = df.groupby("sector_economico_1")[cols_usar[1:]].sum().reset_index()

    # Añadir nombre legible del sector
    agg["sector_nombre"] = agg["sector_economico_1"].map(CATALOGO_SECTORES)
    agg["sector_nombre"] = agg["sector_nombre"].fillna("Desconocido")

    # Añadir columnas de período — sin esto no sabríamos a qué mes pertenece cada fila
    agg["anio"]    = anio
    agg["mes"]     = mes
    agg["periodo"] = pd.Timestamp(year=anio, month=mes, day=1)

    log.info("Agregación completada: %d sectores", len(agg))
    return agg


# ─────────────────────────────────────────────────────────────────────────────
# GUARDAR EN PARQUET ACUMULADO
# ─────────────────────────────────────────────────────────────────────────────

def guardar_acumulado(df_nuevo: pd.DataFrame, out_path: Path) -> int:
    """
    Añade las filas nuevas al Parquet histórico existente.

    Por qué concatenar y no sobreescribir:
      Cada CSV representa un mes. Queremos acumular todos los meses
      en un solo Parquet histórico. Sobreescribir borraría el historial.

    Deduplicación por (anio, mes, sector_economico_1):
      Si procesas el mismo CSV dos veces por error, esta línea evita
      que el mes quede duplicado en el histórico.
    """
    if out_path.exists():
        df_existente = pd.read_parquet(out_path)
        df_total = pd.concat([df_existente, df_nuevo], ignore_index=True)
        df_total = df_total.drop_duplicates(
            subset=["anio", "mes", "sector_economico_1"],
            keep="last"
        )
    else:
        df_total = df_nuevo

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_total.to_parquet(out_path, index=False, engine="pyarrow")

    total_filas = len(df_total)
    periodos    = df_total.groupby(["anio", "mes"]).ngroups
    log.info(
        "Parquet actualizado: %s | %d filas | %d períodos únicos",
        out_path, total_filas, periodos
    )
    return total_filas


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Uso: python process_imss_asg.py ruta/al/asg-YYYY-MM-DD.csv")
        sys.exit(1)

    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        log.error("Archivo no encontrado: %s", csv_path)
        sys.exit(1)

    out_path = Path("data/processed/imss_asg_historico.parquet")

    log.info("═══ Procesando: %s ═══", csv_path.name)

    anio, mes   = extraer_periodo(csv_path)
    df_agregado = leer_y_agregar(csv_path, anio, mes)
    total       = guardar_acumulado(df_agregado, out_path)

    print("\n── Vista previa del mes procesado ──")
    print(df_agregado[["sector_nombre", "asegurados", "ta", "tpu", "teu"]].to_string(index=False))

    print(f"\n✓ Listo. Parquet acumulado: {out_path} ({total} filas totales)")
    print(f"  Tamaño del Parquet: {out_path.stat().st_size / 1024:.1f} KB")
    print(f"\n  Puedes borrar el CSV original:")
    print(f"  rm {csv_path}")


if __name__ == "__main__":
    main()