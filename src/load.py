"""
load.py — Paso 3: Carga en base de datos
=========================================
Qué hace: lee el Parquet procesado más reciente y lo inserta
          en una tabla de DuckDB, evitando duplicados.

Por qué DuckDB y no PostgreSQL, SQLite u otro:
  - No necesita servidor: la base de datos es un archivo en disco
    (data/pipeline.duckdb). Lo abres, lo usas, lo cierras.
  - Lee Parquet de forma nativa con SQL: puedes hacer
    SELECT * FROM 'data/processed/coins_clean_*.parquet'
    sin cargar nada en memoria primero.
  - Es analítico (OLAP): optimizado para leer columnas completas
    y agregar millones de filas, exactamente lo que hace un dashboard.
  - Cuando el proyecto crezca a PostgreSQL o BigQuery, el SQL
    que escribiste aquí funciona casi sin cambios.
"""

import logging
import os
import time
from pathlib import Path

import duckdb
from dotenv import load_dotenv


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR", "data/processed"))
DB_PATH       = Path(os.getenv("DB_PATH",            "data/pipeline.duckdb"))


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

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
# LECTURA DEL PARQUET MÁS RECIENTE
# ─────────────────────────────────────────────────────────────────────────────

def get_latest_processed_file() -> Path:
    """
    Mismo patrón que en transform.py: toma el Parquet más reciente.
    sorted() + [-1] funciona porque los nombres incluyen timestamp ISO 8601.
    """
    files = sorted(PROCESSED_DIR.glob("coins_clean_*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No se encontraron archivos Parquet en '{PROCESSED_DIR}'. "
            "Ejecuta primero src/transform.py."
        )
    latest = files[-1]
    log.info("Archivo procesado seleccionado: %s", latest.name)
    return latest


# ─────────────────────────────────────────────────────────────────────────────
# CREACIÓN DE TABLA
# ─────────────────────────────────────────────────────────────────────────────

def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    """
    Crea la tabla coins_market si no existe todavía.

    Por qué CREATE TABLE IF NOT EXISTS y no CREATE OR REPLACE:
      - OR REPLACE borraría todos los datos históricos en cada ejecución.
      - IF NOT EXISTS la crea la primera vez y la deja intacta las siguientes.
      - Así acumulamos datos de múltiples extracciones en la misma tabla.

    La clave primaria compuesta (id, last_updated) define la unicidad:
      - 'id' solo no sirve: bitcoin aparece en cada extracción.
      - Necesitamos (id + momento de actualización según la API) para
        identificar un registro único e irrepetible.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS coins_market (
            id                          VARCHAR,
            symbol                      VARCHAR,
            name                        VARCHAR,
            current_price               DOUBLE,
            market_cap                  BIGINT,
            market_cap_rank             INTEGER,
            total_volume                DOUBLE,
            high_24h                    DOUBLE,
            low_24h                     DOUBLE,
            price_change_percentage_24h DOUBLE,
            circulating_supply          DOUBLE,
            last_updated                TIMESTAMPTZ,
            _source_file                VARCHAR,
            _processed_at               TIMESTAMPTZ,

            PRIMARY KEY (id, last_updated)
        )
    """)
    log.info("Tabla 'coins_market' verificada / creada")


# ─────────────────────────────────────────────────────────────────────────────
# CARGA CON DEDUPLICACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def load_parquet(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> int:
    """
    Inserta los registros del Parquet en la tabla, ignorando duplicados.

    DuckDB no tiene changes() como SQLite.
    La forma correcta es contar filas antes y después del INSERT
    para saber cuántas se insertaron realmente.
    """
    antes = con.execute("SELECT COUNT(*) FROM coins_market").fetchone()[0]

    con.execute("""
        INSERT OR IGNORE INTO coins_market
        SELECT
            id, symbol, name, current_price, market_cap, market_cap_rank,
            total_volume, high_24h, low_24h, price_change_percentage_24h,
            circulating_supply, last_updated, _source_file, _processed_at
        FROM read_parquet(?)
    """, [str(parquet_path)])

    despues  = con.execute("SELECT COUNT(*) FROM coins_market").fetchone()[0]
    inserted = despues - antes
    ignored  = 50 - inserted

    log.info("Filas insertadas: %d  (ignoradas por duplicado: %d)", inserted, ignored)
    return inserted

# ─────────────────────────────────────────────────────────────────────────────
# VERIFICACIÓN POST-CARGA
# ─────────────────────────────────────────────────────────────────────────────

def verify_load(con: duckdb.DuckDBPyConnection) -> None:
    """
    Consulta básica para confirmar que la carga tuvo efecto.

    Por qué verificar después de insertar:
      - INSERT OR IGNORE no lanza error si algo falla silenciosamente.
      - Esta consulta confirma el estado real de la tabla, no solo
        que el comando INSERT no explotó.
      - También muestra el total acumulado: útil para ver cómo crece
        la tabla con cada ejecución del pipeline.
    """
    total = con.execute("SELECT COUNT(*) FROM coins_market").fetchone()[0]
    last  = con.execute("""
        SELECT name, current_price, market_cap_rank, last_updated
        FROM coins_market
        ORDER BY _processed_at DESC, market_cap_rank ASC
        LIMIT 5
    """).fetchdf()

    log.info("Total de filas en la tabla: %d", total)
    log.info("Últimos registros cargados:\n%s", last.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# ORQUESTADOR
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    """
    Abre la conexión a DuckDB, ejecuta la carga y la cierra.

    duckdb.connect(DB_PATH) crea el archivo si no existe,
    o abre el existente si ya está ahí. Es análogo a
    abrir un archivo con open() pero para bases de datos.

    El bloque try/finally garantiza que con.close() siempre
    se ejecute, incluso si algo falla a mitad: evita dejar
    la base de datos en estado inconsistente.
    """
    log.info("═══ Iniciando pipeline — Paso 3: Carga ═══")
    start = time.perf_counter()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    try:
        parquet_path = get_latest_processed_file()
        ensure_table(con)
        load_parquet(con, parquet_path)
        verify_load(con)

        elapsed = time.perf_counter() - start
        log.info("Paso 3 completado en %.2f s → %s", elapsed, DB_PATH)

    except Exception as e:
        log.exception("Error en la carga: %s", e)
        raise

    finally:
        con.close()


if __name__ == "__main__":
    run()
