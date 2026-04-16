"""
transform.py — Paso 2: Transformación de datos
===============================================
Qué hace: lee el JSON crudo más reciente de data/raw/,
          lo limpia, lo tipifica correctamente, valida
          su calidad y lo guarda en data/processed/ como Parquet.

Por qué Parquet y no CSV:
  - CSV guarda todo como texto; Parquet guarda los tipos reales
    (número, fecha, booleano). No hay que reconvertir al leer.
  - Parquet comprime automáticamente: un CSV de 47 KB puede
    quedar en ~8 KB. A escala (millones de filas) la diferencia
    es enorme.
  - Es el formato estándar en la industria de datos (Spark, BigQuery,
    DuckDB, pandas, Polars… todos lo leen de forma nativa).
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

RAW_DIR       = Path(os.getenv("RAW_DATA_DIR",       "data/raw"))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR", "data/processed"))

# Columnas que nos interesan de las ~30 que devuelve la API.
# Seleccionar solo lo necesario reduce peso y hace el esquema explícito.
COLUMNS_KEEP = [
    "id",                        # identificador único (ej. "bitcoin")
    "symbol",                    # ticker corto (ej. "btc")
    "name",                      # nombre legible (ej. "Bitcoin")
    "current_price",             # precio actual en USD
    "market_cap",                # capitalización de mercado en USD
    "market_cap_rank",           # posición por capitalización
    "total_volume",              # volumen negociado en las últimas 24h
    "high_24h",                  # precio máximo de las últimas 24h
    "low_24h",                   # precio mínimo de las últimas 24h
    "price_change_percentage_24h",  # cambio porcentual en 24h
    "circulating_supply",        # monedas en circulación
    "last_updated",              # timestamp de actualización según la API
]


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
# Mismo patrón que en extract.py: consola + archivo, con timestamp.
# Reutilizamos el mismo pipeline.log para tener el historial completo
# de ambas etapas en un solo lugar.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/pipeline.log"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# LECTURA DEL RAW MÁS RECIENTE
# ─────────────────────────────────────────────────────────────────────────────

def get_latest_raw_file() -> Path:
    """
    Busca el archivo JSON más reciente en data/raw/ y lo devuelve.

    Por qué "el más reciente" y no un nombre fijo:
      - Cada ejecución de extract.py crea un archivo nuevo con timestamp.
      - sorted() + [-1] aprovecha que los nombres ISO 8601 ordenan
        cronológicamente de forma alfabética: el último es siempre el más nuevo.
      - Esto hace que transform.py funcione sin importar cuándo se ejecutó
        la extracción; simplemente toma lo más fresco que haya.
    """
    raw_files = sorted(RAW_DIR.glob("coingecko_markets_*.json"))
    if not raw_files:
        raise FileNotFoundError(
            f"No se encontraron archivos JSON en '{RAW_DIR}'. "
            "Ejecuta primero src/extract.py."
        )
    latest = raw_files[-1]
    log.info("Archivo raw seleccionado: %s", latest.name)
    return latest


def load_raw(filepath: Path) -> pd.DataFrame:
    """
    Lee el JSON y lo convierte en un DataFrame de pandas.

    Por qué DataFrame y no seguir con dicts/listas:
      - Un DataFrame es una tabla en memoria: filas = registros, columnas = campos.
      - pandas da operaciones vectorizadas (aplican a toda la columna a la vez),
        mucho más rápidas que iterar con un for.
      - Es el objeto estándar para transformar datos tabulares en Python.
    """
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    log.info("JSON cargado → %d filas, %d columnas", len(df), len(df.columns))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def select_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Conserva solo las columnas declaradas en COLUMNS_KEEP.

    Por qué hacer esto explícitamente:
      - La API puede añadir columnas nuevas mañana sin avisar.
      - Si no filtramos, esas columnas desconocidas entrarían en la base
        de datos y podrían romper el esquema del Paso 4 (Load).
      - Ser explícito sobre el esquema es una práctica defensiva.
    """
    missing = [c for c in COLUMNS_KEEP if c not in df.columns]
    if missing:
        raise ValueError(f"Columnas esperadas no encontradas en el raw: {missing}")

    df = df[COLUMNS_KEEP].copy()
    log.info("Columnas seleccionadas: %d de %d originales", len(df.columns), len(pd.DataFrame(df).columns) + len(COLUMNS_KEEP) - len(COLUMNS_KEEP))
    return df


def fix_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte cada columna al tipo de dato correcto.

    Por qué es importante el tipado:
      - JSON no distingue entre int y float: todo número puede llegar como
        cualquiera de los dos dependiendo del valor.
      - Las fechas llegan como strings ("2024-01-15T14:30:00.000Z").
        Si las dejamos como string, no podremos filtrar por rango de fechas
        ni calcular diferencias temporales en pasos posteriores.
      - pd.to_numeric(..., errors="coerce") convierte lo que pueda y
        pone NaN donde no pueda, en vez de lanzar un error. Luego
        gestionamos esos NaN en validate().

    utc=True en to_datetime: convierte el string a datetime con zona
    horaria UTC explícita, evitando ambigüedades al comparar fechas
    de distintas ejecuciones.
    """
    numeric_cols = [
        "current_price", "market_cap", "total_volume",
        "high_24h", "low_24h", "price_change_percentage_24h",
        "circulating_supply",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["market_cap_rank"] = pd.to_numeric(df["market_cap_rank"], errors="coerce").astype("Int64")
    # Int64 (con mayúscula) es el tipo entero de pandas que admite NaN.
    # El int nativo de Python no acepta NaN; si hay algún nulo explotaría.

    df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True, errors="coerce")

    log.info("Tipos de dato aplicados correctamente")
    return df


def add_metadata(df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
    """
    Añade columnas de trazabilidad: de dónde vienen estos datos y cuándo
    los procesamos nosotros.

    Por qué metadatos de trazabilidad:
      - En producción tendrás cientos de archivos procesados.
      - Si un número parece incorrecto, estas columnas te dicen exactamente
        de qué extracción vino y cuándo se transformó.
      - Es la diferencia entre "algo salió mal" y "el archivo del martes
        a las 3am tenía datos corruptos".
    """
    df["_source_file"]     = source_file.name
    df["_processed_at"]    = datetime.now(timezone.utc)

    log.info("Metadatos de trazabilidad añadidos")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# VALIDACIÓN DE CALIDAD
# ─────────────────────────────────────────────────────────────────────────────

def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Comprueba que los datos cumplen reglas de negocio básicas.
    Lanza ValueError si algo está fundamentalmente mal.
    Registra advertencias para anomalías no críticas.

    Por qué validar antes de guardar y no después:
      - Si guardas datos corruptos en la base de datos, el daño se propaga
        al dashboard, a los reportes, a las decisiones.
      - Es más fácil detectar "el raw de hoy tiene 0 filas" aquí que
        esperar a que un gráfico muestre datos vacíos.

    Separamos errores críticos (que detienen el pipeline) de advertencias
    (que lo dejan pasar pero quedan registradas para revisión).
    """
    errors   = []
    warnings = []

    # ── Errores críticos ──────────────────────────────────────────────────────

    if len(df) == 0:
        errors.append("El DataFrame está vacío — la extracción no trajo datos.")

    if df["id"].duplicated().any():
        dupes = df[df["id"].duplicated()]["id"].tolist()
        errors.append(f"IDs duplicados encontrados: {dupes}")

    negative_prices = (df["current_price"] < 0).sum()
    if negative_prices > 0:
        errors.append(f"{negative_prices} filas con precio negativo — dato imposible.")

    if errors:
        for e in errors:
            log.error("VALIDACIÓN CRÍTICA: %s", e)
        raise ValueError(f"Validación fallida con {len(errors)} error(es) críticos.")

    # ── Advertencias no críticas ──────────────────────────────────────────────

    null_prices = df["current_price"].isna().sum()
    if null_prices > 0:
        warnings.append(f"{null_prices} filas sin precio (current_price nulo)")

    null_market_cap = df["market_cap"].isna().sum()
    if null_market_cap > 0:
        warnings.append(f"{null_market_cap} filas sin market_cap")

    for w in warnings:
        log.warning("VALIDACIÓN: %s", w)

    log.info(
        "Validación completada — %d filas válidas, %d advertencia(s)",
        len(df), len(warnings)
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# PERSISTENCIA PROCESADA
# ─────────────────────────────────────────────────────────────────────────────

def save_processed(df: pd.DataFrame, source_file: Path) -> Path:
    """
    Guarda el DataFrame limpio como archivo Parquet.

    Parquet vs CSV en detalle:
      - CSV: texto plano, universalmente legible, pero pesado y sin tipos.
      - Parquet: binario columnar, preserva tipos, comprime muy bien.
        pandas, DuckDB, BigQuery, Spark, Polars lo leen sin configuración.
      - Al guardar como Parquet aquí, el Paso 4 (Load) puede leer
        directamente con los tipos correctos sin reconvertir nada.

    El nombre del archivo Parquet reutiliza el timestamp del raw original
    para mantener la trazabilidad: raw y procesado tienen el mismo ID de tiempo.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Extraemos el timestamp del nombre del raw para reutilizarlo.
    # "coingecko_markets_20240115T143022Z.json" → "20240115T143022Z"
    timestamp = source_file.stem.split("_")[-1]
    out_path  = PROCESSED_DIR / f"coins_clean_{timestamp}.parquet"

    df.to_parquet(out_path, index=False, engine="pyarrow")

    log.info(
        "Datos procesados guardados en: %s  (%d bytes)",
        out_path, out_path.stat().st_size
    )
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# ORQUESTADOR
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    """
    Encadena todas las funciones de transformación en orden.

    El patrón es siempre el mismo:
      df = paso_1(df)
      df = paso_2(df)
      df = paso_3(df)
      ...
    Cada función recibe un DataFrame y devuelve uno (posiblemente modificado).
    Esto se llama pipeline funcional: fácil de leer, fácil de depurar,
    fácil de reordenar o quitar pasos.
    """
    log.info("═══ Iniciando pipeline — Paso 2: Transformación ═══")
    start = time.perf_counter()

    try:
        source_file = get_latest_raw_file()
        df          = load_raw(source_file)
        df          = select_columns(df)
        df          = fix_types(df)
        df          = add_metadata(df, source_file)
        df          = validate(df)
        out_path    = save_processed(df, source_file)

        elapsed = time.perf_counter() - start
        log.info("Paso 2 completado en %.2f s → %s", elapsed, out_path)
        log.info("Vista previa del resultado:\n%s", df[["name", "symbol", "current_price", "market_cap_rank"]].head(5).to_string())

    except FileNotFoundError as e:
        log.error("Archivo no encontrado: %s", e)
        raise

    except ValueError as e:
        log.error("Error de validación: %s", e)
        raise

    except Exception as e:
        log.exception("Error inesperado: %s", e)
        raise


if __name__ == "__main__":
    run()
